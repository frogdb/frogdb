//! Blocking command handlers.
//!
//! This module handles blocking commands:
//! - BLPOP, BRPOP, BLMOVE, BLMPOP, BZPOPMIN, BZPOPMAX, BZMPOP, XREAD, XREADGROUP
//! - WAIT - Wait for replication acknowledgment

use std::time::{Duration, Instant};

use bytes::Bytes;
use frogdb_core::{BlockingOp, ReplicationTracker, ShardMessage, shard_for_key};
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
    ///
    /// For WAIT the path is entirely different (replication tracker, not shard
    /// routing), so it is dispatched out before any of the above.
    pub(crate) async fn handle_blocking_wait(
        &mut self,
        keys: Vec<Bytes>,
        timeout: f64,
        proto_op: frogdb_protocol::BlockingOp,
    ) -> Response {
        // WAIT uses the replication tracker, not the blocking-wait machinery.
        if let frogdb_protocol::BlockingOp::Wait {
            num_replicas,
            timeout_ms,
        } = proto_op
        {
            return self.handle_wait_command(num_replicas, timeout_ms).await;
        }

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

    /// Handle WAIT command using the replication tracker.
    ///
    /// WAIT blocks until the specified number of replicas have acknowledged
    /// all writes up to this point, or until the timeout expires.
    pub(crate) async fn handle_wait_command(&self, num_replicas: u32, timeout_ms: u64) -> Response {
        // If no replication tracker is available, return 0 replicas
        let tracker = match &self.cluster.replication_tracker {
            Some(t) => t,
            None => {
                // No replication configured - return 0 replicas immediately
                return Response::Integer(0);
            }
        };

        // Get the current replication offset that replicas need to acknowledge
        let current_offset = tracker.current_offset();

        // If num_replicas is 0 or timeout_ms is 0, return immediately with current replica count
        // Redis behavior: timeout=0 means "check and return immediately, don't wait"
        if num_replicas == 0 || timeout_ms == 0 {
            let count = tracker.count_acked(current_offset);
            return Response::Integer(count as i64);
        }

        // Wait for replicas with timeout
        let timeout_duration = Duration::from_millis(timeout_ms);

        let wait_future = tracker.wait_for_acks(current_offset, num_replicas);
        let result = tokio::time::timeout(timeout_duration, wait_future).await;

        match result {
            Ok(count) => Response::Integer(count as i64),
            Err(_) => {
                // Timeout - return the count of replicas that did ACK
                let count = tracker.count_acked(current_offset);
                Response::Integer(count as i64)
            }
        }
    }
}
