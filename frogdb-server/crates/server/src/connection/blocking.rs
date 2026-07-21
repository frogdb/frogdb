//! Blocking command handlers.
//!
//! This module handles blocking commands:
//! - BLPOP, BRPOP, BLMOVE, BLMPOP, BZPOPMIN, BZPOPMAX, BZMPOP, XREAD, XREADGROUP
//! - WAIT - Wait for replication acknowledgment

use std::time::{Duration, Instant};

use bytes::Bytes;
use frogdb_core::{BlockingOp, ShardMessage, UnblockMode, shard_for_key};
use frogdb_protocol::Response;
use tokio::sync::oneshot;

use crate::connection::ConnectionHandler;
use crate::connection::util::convert_blocking_op;

pub mod coordinator;

use coordinator::{BlockingWaitCoordinator, WaitOutcome};

impl ConnectionHandler {
    /// Handle a blocking command wait.
    ///
    /// The lifecycle is a sequence of named steps: register the wait on the
    /// owning shard, coordinate the three-way race (response / CLIENT UNBLOCK /
    /// deadline) via [`BlockingWaitCoordinator`], then clean up. The race
    /// decision lives in the coordinator and the op-aware nil shaping in
    /// [`WaitOutcome::into_response`]; this handler owns only registration and
    /// cleanup (it holds the shard senders and the client registry).
    pub(crate) async fn handle_blocking_wait(
        &mut self,
        keys: Vec<Bytes>,
        timeout: f64,
        proto_op: frogdb_protocol::BlockingOp,
    ) -> Response {
        let op = convert_blocking_op(proto_op);

        // All keys are validated onto one shard by the command, so the wait
        // targets a single response channel.
        if keys.is_empty() {
            return Response::error("ERR No keys provided for blocking command");
        }
        let target_shard = shard_for_key(&keys[0], self.num_shards);
        let deadline = (timeout > 0.0).then(|| Instant::now() + Duration::from_secs_f64(timeout));

        // Register (sends BlockWait, marks blocked, resets stale unblock).
        let response_rx = match self
            .register_wait(target_shard, &keys, op.clone(), deadline)
            .await
        {
            Ok(rx) => rx,
            Err(resp) => return resp,
        };

        // Coordinate the three-way race. The coordinator owns the decision; the
        // server stays the canonical (precise) timeout authority.
        let outcome = BlockingWaitCoordinator::wait_for_response(
            response_rx,
            deadline,
            &mut self.client_handle,
        )
        .await;

        // Clean up (clears blocked state, resets unblock, unregisters iff the
        // wait may still be live on the shard).
        self.cleanup_wait(target_shard, &outcome).await;

        outcome.into_response(&op)
    }

    /// Register a blocking wait on `target_shard`: send the `BlockWait` message,
    /// mark the connection blocked (both locally and in the registry, so CLIENT
    /// UNBLOCK can target it), and clear any stale unblock signal so the new
    /// wait starts fresh. Returns the response channel, or an error reply if the
    /// shard is unreachable.
    async fn register_wait(
        &mut self,
        target_shard: usize,
        keys: &[Bytes],
        op: BlockingOp,
        deadline: Option<Instant>,
    ) -> Result<oneshot::Receiver<Response>, Response> {
        // Defensively clear any stale CLIENT UNBLOCK signal from a previous
        // blocking command so the new wait starts fresh.
        self.admin.client_registry.reset_unblock(self.state.id);

        let (response_tx, response_rx) = oneshot::channel();

        let Some(sender) = self.core.shard_senders.get(target_shard) else {
            return Err(Response::error("ERR Internal error: invalid shard"));
        };

        if sender
            .send(ShardMessage::BlockWait {
                conn_id: self.state.id,
                keys: keys.to_vec(),
                op,
                response_tx,
                deadline,
                protocol_version: self.state.protocol_version,
            })
            .await
            .is_err()
        {
            return Err(Response::error("ERR Internal error: shard unreachable"));
        }

        self.state.begin_block(target_shard, keys.to_vec());
        self.admin
            .client_registry
            .update_blocked_state(self.state.id, true);

        Ok(response_rx)
    }

    /// Tear down a blocking wait after the race resolved: clear blocked state
    /// (local + registry), reset the unblock signal, and unregister the shard
    /// waiter unless the shard already delivered a response.
    ///
    /// The `UnregisterWait` is sent on `Timeout`/`Unblocked` (the wait may still
    /// be registered on the shard) but skipped on `Response` (the shard already
    /// removed the entry when it sent — a redundant unregister would be
    /// harmless, just wasteful).
    async fn cleanup_wait(&mut self, target_shard: usize, outcome: &WaitOutcome) {
        self.state.end_block();
        self.admin
            .client_registry
            .update_blocked_state(self.state.id, false);
        self.admin.client_registry.reset_unblock(self.state.id);

        if matches!(outcome, WaitOutcome::Timeout | WaitOutcome::Unblocked(_))
            && let Some(sender) = self.core.shard_senders.get(target_shard)
        {
            let _ = sender
                .send(ShardMessage::UnregisterWait {
                    conn_id: self.state.id,
                })
                .await;
        }
    }

    /// Handle the WAIT command at the connection level.
    ///
    /// The replication decision — offset snapshot, immediate quorum check,
    /// GETACK solicitation, quorum-or-deadline wait — is owned by the
    /// replication crate's [`frogdb_replication::WaitCoordinator`]; this
    /// handler owns only what is *connection*: argument validation, the
    /// replica/standalone rejections, blocked-state bookkeeping in the client
    /// registry, and the CLIENT UNBLOCK race.
    ///
    /// Redis semantics mirrored here (`waitCommand`, `replication.c`):
    /// - WAIT on a replica is an error, before argument parsing.
    /// - An already-satisfied quorum returns the acked count without blocking
    ///   (including `numreplicas 0`, which returns the actual count).
    /// - `timeout 0` blocks until the quorum is reached; CLIENT UNBLOCK is the
    ///   escape hatch (TIMEOUT mode returns the current count, ERROR mode the
    ///   `-UNBLOCKED` error).
    ///
    /// Documented divergence: with no replication configured (standalone —
    /// no primary handler, so no replica can ever attach), WAIT returns the
    /// count (0) immediately instead of idling out the timeout; the quorum is
    /// unreachable by construction.
    pub(crate) async fn handle_wait_command(&mut self, args: &[Bytes]) -> Response {
        // Redis rejects WAIT on replicas before looking at the arguments.
        if self.is_replica.load(std::sync::atomic::Ordering::Relaxed) {
            return Response::error(crate::commands::replication::WAIT_ON_REPLICA_ERR);
        }

        let (num_replicas, timeout_ms) = match crate::commands::replication::parse_wait_args(args) {
            Ok(parsed) => parsed,
            Err(err) => return err.to_response(),
        };

        // Standalone: no primary replication handler means no replica can ever
        // attach, so the quorum is decided now.
        let Some(primary) = self.cluster.primary_replication_handler.clone() else {
            return Response::Integer(0);
        };

        let wait = primary.wait_coordinator();
        let target = wait.target_offset();

        // Fast path (Redis: `replicationCountAcksByOffset` before blocking).
        let count = wait.count_acked(target);
        if count >= num_replicas {
            return Response::Integer(count as i64);
        }

        let deadline = (timeout_ms > 0).then(|| Instant::now() + Duration::from_millis(timeout_ms));

        // Mark the connection blocked in the registry so CLIENT UNBLOCK can
        // target this wait, clearing any stale signal first (same bookkeeping
        // as `register_wait`; there is no shard registration to pair it with).
        self.admin.client_registry.reset_unblock(self.state.id);
        self.admin
            .client_registry
            .update_blocked_state(self.state.id, true);

        // Race the coordinator (which owns the single timeout authority via its
        // internal deadline) against CLIENT UNBLOCK.
        let wait_fut = wait.wait_for_replicas(target, num_replicas, deadline, primary.as_ref());
        tokio::pin!(wait_fut);

        let response = tokio::select! {
            biased;
            verdict = &mut wait_fut => Response::Integer(verdict.count() as i64),
            mode = self.client_handle.unblocked() => match mode {
                Some(UnblockMode::Error) => {
                    Response::error("UNBLOCKED client unblocked via CLIENT UNBLOCK")
                }
                // TIMEOUT mode (and a closed signal channel) reply like a
                // timed-out WAIT: the count acked so far.
                _ => Response::Integer(wait.count_acked(target) as i64),
            },
        };

        self.admin
            .client_registry
            .update_blocked_state(self.state.id, false);
        self.admin.client_registry.reset_unblock(self.state.id);

        response
    }
}
