//! Blocking command handlers.
//!
//! This module handles blocking commands:
//! - BLPOP, BRPOP, BLMOVE, BLMPOP, BZPOPMIN, BZPOPMAX, BZMPOP, XREAD, XREADGROUP
//! - WAIT - Wait for replication acknowledgment

use std::time::Duration;

use bytes::Bytes;
use frogdb_core::{BlockingMsg, BlockingOp, UnblockMode, UnregisterAck, shard_for_key};
use frogdb_protocol::Response;
use tokio::sync::oneshot;
// Blocking deadlines live on the timer's clock, not the OS clock: they are compared
// against `tokio::time::Instant::now()` on the shard and slept on with `sleep_until`,
// and under a paused runtime (turmoil) the two clocks diverge immediately.
use tokio::time::Instant;

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
        let mut response_rx = match self
            .register_wait(target_shard, &keys, op.clone(), deadline)
            .await
        {
            Ok(rx) => rx,
            Err(resp) => return resp,
        };

        // Coordinate the three-way race. The coordinator owns the decision; the
        // server stays the canonical (precise) timeout authority. `response_rx`
        // is borrowed (not consumed) so cleanup can still drain a value the
        // shard sends in the pop→deliver window after a timeout is chosen.
        let outcome = BlockingWaitCoordinator::wait_for_response(
            &mut response_rx,
            deadline,
            &mut self.client_handle,
        )
        .await;

        // Clean up (clears blocked state, resets unblock) and reconcile the
        // serve-vs-timeout race with the shard so a raced serve is delivered
        // rather than lost.
        self.cleanup_wait(target_shard, outcome, &mut response_rx, &op)
            .await
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
            .send(BlockingMsg::BlockWait {
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
    /// (local + registry), reset the unblock signal, and produce the final
    /// reply — reconciling the serve-vs-timeout race with the shard.
    ///
    /// On `Response` the shard already delivered (the value was drained by the
    /// coordinator); return it. On `Timeout`/`Unblocked` the wait may still be
    /// registered on the shard, *or* a serve may have raced the timeout and put
    /// a value on the response channel that the coordinator did not observe.
    /// [`Self::reconcile_unregister`] resolves this on the shard's serial
    /// timeline: if the waiter was already served, its value is drained and
    /// returned instead of being lost (the serve-vs-timeout race); otherwise the
    /// op-aware timeout/unblock reply is used.
    async fn cleanup_wait(
        &mut self,
        target_shard: usize,
        outcome: WaitOutcome,
        response_rx: &mut oneshot::Receiver<Response>,
        op: &BlockingOp,
    ) -> Response {
        self.state.end_block();
        self.admin
            .client_registry
            .update_blocked_state(self.state.id, false);
        self.admin.client_registry.reset_unblock(self.state.id);

        match outcome {
            WaitOutcome::Response(resp) => resp,
            WaitOutcome::Timeout | WaitOutcome::Unblocked(_) => {
                match self.reconcile_unregister(target_shard, response_rx).await {
                    Some(served) => served,
                    None => outcome.into_response(op),
                }
            }
        }
    }

    /// Send an acknowledged `UnregisterWait` and, if the shard reports the
    /// waiter was already served, drain and return the served value.
    ///
    /// The shard processes its mailbox serially, so its answer is the single
    /// source of truth for whether the serve or the timeout won:
    /// - [`UnregisterAck::Unregistered`] — the timeout won; the waiter was
    ///   removed here and no value was consumed. Return `None` (use the timeout
    ///   reply).
    /// - [`UnregisterAck::AlreadyServed`] — a serve (or the GC tick) already
    ///   removed the waiter and sent on the response channel. Drain that value
    ///   and return it; a served element is otherwise popped from the store and
    ///   delivered to nobody.
    ///
    /// Returns `None` (fall back to the timeout reply) if the shard is
    /// unreachable, the ack channel closes, or the drained channel yields no
    /// value.
    async fn reconcile_unregister(
        &mut self,
        target_shard: usize,
        response_rx: &mut oneshot::Receiver<Response>,
    ) -> Option<Response> {
        let sender = self.core.shard_senders.get(target_shard)?;
        let (ack_tx, ack_rx) = oneshot::channel();
        if sender
            .send(BlockingMsg::UnregisterWait {
                conn_id: self.state.id,
                ack: ack_tx,
            })
            .await
            .is_err()
        {
            return None;
        }

        reconcile_ack(ack_rx.await, response_rx).await
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
    /// There is no cluster-mode special case. Cluster replicas attach over the
    /// same PSYNC link and ACK into the same tracker as standalone ones (Raft
    /// carries metadata only, ADR-0001), so WAIT counts *this node's* replicas
    /// in both modes — the per-node contract Redis, Valkey and Dragonfly all
    /// implement. A cluster-wide guarantee is a client-side fan-out
    /// (`ALL_SHARDS` + `AGG_MIN`), not a server-side one; WAIT never redirects,
    /// because it takes no key.
    ///
    /// An unreachable `numreplicas` blocks to the deadline rather than
    /// early-returning (Redis parity; a replica may still be mid-attach). The
    /// only shortcut left is structural: with no primary handler wired at all
    /// no replica can ever attach, so the quorum is decided immediately. A real
    /// server always has one — the handler is built on every role — so that
    /// path is reachable only from deps that carry no replication wiring.
    pub(crate) async fn handle_wait_command(&mut self, args: &[Bytes]) -> Response {
        // Redis rejects WAIT on replicas before looking at the arguments.
        if self.is_replica.load(std::sync::atomic::Ordering::Acquire) {
            return Response::error(crate::commands::replication::WAIT_ON_REPLICA_ERR);
        }

        let (num_replicas, timeout_ms) = match crate::commands::replication::parse_wait_args(args) {
            Ok(parsed) => parsed,
            Err(err) => return err.to_response(),
        };

        // No primary replication handler means no replica can ever attach, so
        // the quorum is decided now. Not a role decision: the handler is built
        // on every role, so this is the "replication is not wired at all" case.
        let Some(primary) = self.cluster.primary_replication_handler.clone() else {
            return Response::Integer(0);
        };

        let wait = primary.wait_coordinator();

        // Subscribe to the role fence *before* concluding that this node is
        // still a primary. A demotion publishes the replica flag first and
        // bumps the fence second, so with the fence in hand one of the two
        // observations below is guaranteed to see a demotion that races this
        // WAIT — without it, a demotion landing between the check above and
        // the subscription inside the coordinator would park this wait on a
        // node that has already rejected every later WAIT.
        let fence = wait.role_fence();
        if self.is_replica.load(std::sync::atomic::Ordering::Acquire) {
            return Response::error(crate::commands::replication::WAIT_ROLE_CHANGED_ERR);
        }

        let target = wait.target_offset();

        // Fast path (Redis: `replicationCountAcksByOffset` before blocking).
        let count = wait.count_acked(target);
        if count >= num_replicas {
            return Response::Integer(count as i64);
        }

        // Timer clock, not wall clock: the coordinator hands this to
        // `tokio::time::timeout_at` (see the import note in `wait_coordinator`).
        let deadline = (timeout_ms > 0)
            .then(|| tokio::time::Instant::now() + Duration::from_millis(timeout_ms));

        // Mark the connection blocked in the registry so CLIENT UNBLOCK can
        // target this wait, clearing any stale signal first (same bookkeeping
        // as `register_wait`; there is no shard registration to pair it with).
        self.admin.client_registry.reset_unblock(self.state.id);
        self.admin
            .client_registry
            .update_blocked_state(self.state.id, true);

        // Race the coordinator (which owns the single timeout authority via its
        // internal deadline) against CLIENT UNBLOCK.
        let wait_fut =
            wait.wait_for_replicas(fence, target, num_replicas, deadline, primary.as_ref());
        tokio::pin!(wait_fut);

        let response = tokio::select! {
            biased;
            verdict = &mut wait_fut => match verdict {
                // A demotion tore down the stream this wait was parked on.
                // Redis replies with an error from `disconnectAllBlockedClients`
                // rather than a count, because the count would describe a
                // history the node no longer heads.
                crate::replication::WaitVerdict::RoleChanged(_) => {
                    Response::error(crate::commands::replication::WAIT_ROLE_CHANGED_ERR)
                }
                other => Response::Integer(other.count() as i64),
            },
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

/// Decide the reply from the shard's `UnregisterWait` ack and whatever the
/// (still open) response channel holds.
///
/// Split out of [`ConnectionHandler::reconcile_unregister`] for the same reason
/// [`BlockingWaitCoordinator`] is split out of `handle_blocking_wait`: this is
/// the whole serve-vs-timeout decision, and as a free function over two
/// in-memory channels it is unit-testable without a live shard or a whole
/// `ConnectionHandler`. `Some` means a raced serve was drained and must be
/// delivered; `None` means fall back to the op-aware timeout/unblock reply.
async fn reconcile_ack(
    ack: Result<UnregisterAck, oneshot::error::RecvError>,
    response_rx: &mut oneshot::Receiver<Response>,
) -> Option<Response> {
    match ack {
        // The shard says a serve already removed this waiter and sent on the
        // response channel. Draining it is what keeps the popped element from
        // being delivered to nobody.
        Ok(UnregisterAck::AlreadyServed) => response_rx.await.ok(),
        Ok(UnregisterAck::Unregistered) | Err(_) => None,
    }
}

#[cfg(test)]
mod reconcile_tests {
    use super::*;

    /// FM-BLOCKING-005
    ///
    /// `AlreadyServed` means the shard popped an element for this waiter after
    /// the coordinator had already chosen a timeout. The value must be drained
    /// off the still-open receiver and delivered — dropping it is exactly the
    /// "neither delivered nor in final state" loss the conservation checkers
    /// report.
    #[tokio::test]
    async fn already_served_drains_the_raced_value() {
        let (tx, mut rx) = oneshot::channel();
        tx.send(Response::Integer(42)).unwrap();
        let out = reconcile_ack(Ok(UnregisterAck::AlreadyServed), &mut rx).await;
        assert!(
            matches!(out, Some(Response::Integer(42))),
            "a serve that raced the timeout must be delivered, not dropped"
        );
    }

    /// FM-BLOCKING-005
    ///
    /// `Unregistered` is the shard's authoritative "the timeout won, nothing was
    /// consumed" — the caller keeps its op-aware timeout reply.
    #[tokio::test]
    async fn unregistered_keeps_the_timeout_reply() {
        let (_tx, mut rx) = oneshot::channel::<Response>();
        let out = reconcile_ack(Ok(UnregisterAck::Unregistered), &mut rx).await;
        assert!(out.is_none());
    }

    /// FM-BLOCKING-005
    ///
    /// A closed ack channel (shard gone) is not an excuse to hang or to
    /// fabricate a delivery: fall back to the timeout reply.
    #[tokio::test]
    async fn closed_ack_channel_keeps_the_timeout_reply() {
        let (ack_tx, ack_rx) = oneshot::channel::<UnregisterAck>();
        drop(ack_tx);
        let (_tx, mut rx) = oneshot::channel::<Response>();
        let out = reconcile_ack(ack_rx.await, &mut rx).await;
        assert!(out.is_none());
    }

    /// FM-BLOCKING-005
    ///
    /// `AlreadyServed` with a response channel that closed without a value
    /// (shard teardown between ack and send) must degrade to the timeout reply
    /// rather than blocking forever on a dead channel.
    #[tokio::test]
    async fn already_served_with_closed_response_channel_falls_back() {
        let (tx, mut rx) = oneshot::channel::<Response>();
        drop(tx);
        let out = reconcile_ack(Ok(UnregisterAck::AlreadyServed), &mut rx).await;
        assert!(out.is_none());
    }
}
