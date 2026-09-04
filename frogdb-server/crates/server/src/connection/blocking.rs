//! Blocking command handlers.
//!
//! This module handles blocking commands:
//! - BLPOP, BRPOP, BLMOVE, BLMPOP, BZPOPMIN, BZPOPMAX, BZMPOP, XREAD, XREADGROUP
//! - WAIT - Wait for replication acknowledgment

use std::collections::VecDeque;
use std::time::Duration;

use bytes::Bytes;
use frogdb_core::{BlockingMsg, BlockingOp, ClientEdge, UnblockMode, UnregisterAck, shard_for_key};
use frogdb_protocol::Response;
use futures::StreamExt;
use redis_protocol::error::RedisProtocolError;
use redis_protocol::resp2::types::BytesFrame;
use tokio::sync::oneshot;
use tokio_util::codec::Framed;
// Blocking deadlines live on the timer's clock, not the OS clock: they are compared
// against `tokio::time::Instant::now()` on the shard and slept on with `sleep_until`,
// and under a paused runtime (turmoil) the two clocks diverge immediately.
use tokio::time::Instant;

use crate::connection::codec::FrogDbResp2;
use crate::connection::util::convert_blocking_op;
use crate::connection::{ConnectionHandler, MAX_PARKED_PIPELINE_FRAMES};
use crate::net::ConnectionStream;

pub mod coordinator;

use coordinator::{BlockingWaitCoordinator, ParkedExit, WaitOutcome};

/// [`coordinator::PeerLiveness`] over a parked connection's own socket.
///
/// A blocked client is a *state on a still-readable connection*, not a suspended
/// read loop (Redis/Valkey/Dragonfly all model it that way): the wait keeps
/// reading so it can see EOF. Frames that arrive instead of EOF are pushed onto
/// the connection's parked buffer and replayed after the wait resolves, which is
/// what keeps a command pipelined behind `BLPOP` from overtaking it. At
/// [`MAX_PARKED_PIPELINE_FRAMES`] the watch stops reading entirely: the buffer
/// is held by a connection that is not making progress, so the bound is
/// backpressure, not a policy choice. Losing EOF detection past the cap is
/// acceptable — the deadline, `CLIENT KILL` and `CLIENT UNBLOCK` all still end
/// the wait, and a client that pipelined 64 commands behind a blocking one is
/// not the abandoned-socket case this exists for.
struct SocketWatch<'a> {
    framed: &'a mut Framed<ConnectionStream, FrogDbResp2>,
    parked: &'a mut VecDeque<Result<BytesFrame, RedisProtocolError>>,
}

impl coordinator::PeerLiveness for SocketWatch<'_> {
    async fn closed(&mut self) {
        loop {
            if self.parked.len() >= MAX_PARKED_PIPELINE_FRAMES {
                // Stop reading; TCP backpressure applies from here.
                std::future::pending::<()>().await;
            }
            // `StreamExt::next` on a `Framed` is cancel-safe: a partially read
            // frame stays in the codec's buffer if the select drops this branch.
            //
            // Each parked frame is copied out of the read buffer before the
            // next poll: a later `reserve` may reallocate the buffer, and a
            // slice left as the sole holder of the old allocation would
            // look unique to `detach_bytes` and pin it (invariant documented
            // on `frogdb_protocol::detach_bytes`).
            match self.framed.next().await {
                None => return,
                Some(item) => self
                    .parked
                    .push_back(item.map(frogdb_protocol::detach_frame)),
            }
        }
    }
}

/// Blocking-park boundary of the zero-copy parse path.
///
/// The shard retains the keys (and any op-embedded bytes, e.g. the BLMOVE
/// destination) for the whole park, which is unbounded — so they are copied
/// out of the connection's pooled read buffer here, before registration. The
/// buffer itself keeps serving the parked connection's socket watch and can be
/// trimmed/recycled while the wait is parked.
fn detach_wait_inputs(
    keys: Vec<Bytes>,
    proto_op: frogdb_protocol::BlockingOp,
) -> (Vec<Bytes>, BlockingOp) {
    let keys = keys
        .into_iter()
        .map(frogdb_protocol::detach_bytes)
        .collect();
    let mut op = convert_blocking_op(proto_op);
    op.detach();
    (keys, op)
}

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
        let (keys, op) = detach_wait_inputs(keys, proto_op);

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

        // Coordinate the race. The coordinator owns the decision; the server
        // stays the canonical (precise) timeout authority. `response_rx` is
        // borrowed (not consumed) so cleanup can still drain a value the shard
        // sends in the pop→deliver window after a timeout is chosen.
        //
        // The socket is watched for the whole park, so a peer that vanishes
        // mid-wait is observed rather than leaking its entry, FD and waiter
        // budget forever (TR-BLOCKING-013); frames that arrive meanwhile are
        // buffered, not executed, so pipelining keeps Redis's ordering.
        let outcome = {
            let Self {
                framed,
                client_handle,
                parked_frames,
                ..
            } = &mut *self;
            let mut peer = SocketWatch {
                framed,
                parked: parked_frames,
            };
            BlockingWaitCoordinator::wait_for_response(
                &mut response_rx,
                deadline,
                client_handle,
                &mut peer,
            )
            .await
        };

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

        let conn_id = self.state.id;
        mirror_blocked_then_register(&self.admin.client_registry, conn_id, async {
            sender
                .send(BlockingMsg::BlockWait {
                    conn_id,
                    keys: keys.to_vec(),
                    op,
                    response_tx,
                    deadline,
                    protocol_version: self.state.protocol_version,
                })
                .await
                .map_err(|_| ())
        })
        .await?;

        self.state.begin_block(target_shard, keys.to_vec());

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
        // The connection itself ended: there is no reader for any reply, and no
        // point paying for the acknowledged unregister round trip. Close the
        // response channel *first* so a serve still in flight fails its send and
        // the shard restores what it popped (TR-BLOCKING-008) rather than
        // handing an element to a socket nobody is reading; then deliberately
        // leave the blocked-flag set so `notify_connection_closed` performs the
        // unregistration on the way out (TR-BLOCKING-013, TR-BLOCKING-021).
        if let WaitOutcome::ConnectionEnded(exit) = &outcome {
            let exit = *exit;
            response_rx.close();
            self.admin.client_registry.reset_unblock(self.state.id);
            self.parked_wait_exit = Some(exit);
            // Suppressed by `process_one_command`, which sees `parked_wait_exit`
            // and breaks before any reply is buffered.
            return Response::Null;
        }

        self.state.end_block();
        self.admin
            .client_registry
            .update_blocked_state(self.state.id, false);
        self.admin.client_registry.reset_unblock(self.state.id);

        match outcome {
            // Handled above; the compiler cannot see that from the `if let`.
            WaitOutcome::ConnectionEnded(_) => Response::Null,
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
        let raced = {
            let wait_fut =
                wait.wait_for_replicas(fence, target, num_replicas, deadline, primary.as_ref());
            let Self {
                framed,
                client_handle,
                parked_frames,
                ..
            } = &mut *self;
            let mut peer = SocketWatch {
                framed,
                parked: parked_frames,
            };
            resolve_wait_race(wait_fut, client_handle, &mut peer, || {
                wait.count_acked(target)
            })
            .await
        };

        self.admin.client_registry.reset_unblock(self.state.id);

        // A `WAIT` has no shard-side `WaitEntry`, so there is nothing to
        // unregister — but the connection still has to die, and the reply still
        // has to be suppressed (TR-BLOCKING-021).
        let response = match raced {
            Ok(response) => response,
            Err(exit) => {
                self.parked_wait_exit = Some(exit);
                Response::Null
            }
        };

        self.admin
            .client_registry
            .update_blocked_state(self.state.id, false);

        response
    }
}

/// Decide the WAIT reply from the two-way race between the replication wait
/// (quorum / deadline / role fence, all resolved inside `wait_fut`) and CLIENT
/// UNBLOCK.
///
/// Split out of [`ConnectionHandler::handle_wait_command`] for the reason
/// [`reconcile_ack`] is split out of `reconcile_unregister`: as a free function
/// over two futures and a closure it is unit-testable with both arms ready in
/// the *same* poll, which is the only way to pin a tie-break that a live socket
/// can only hit by luck.
///
/// The `biased;` ordering is the contract: a `CLIENT UNBLOCK` landing in the
/// same poll as a role change loses, and the client is told the role changed.
/// The role change is the more important fact — a `-UNBLOCKED ... via CLIENT
/// UNBLOCK` reply would let the caller believe the node it was waiting on is
/// still the primary it thought it was — and, as the second tie-break in this
/// race (`wait_for_replicas`'s own fence-vs-quorum `select!` is the first,
/// FM-REPLICATION-040), it must not be left to `tokio::select!`'s unseeded
/// random choice.
///
/// `count_acked` is called only on the CLIENT UNBLOCK TIMEOUT path, where the
/// reply is the count acked so far; it is a closure so the wait future keeps
/// exclusive use of the coordinator until the race is decided.
/// `Err(exit)` means the connection ended under the wait — the same two
/// terminating edges the non-WAIT path handles, on the same terms: no reply, the
/// run loop tears the connection down.
async fn resolve_wait_race(
    wait_fut: impl std::future::Future<Output = crate::replication::WaitVerdict>,
    signals: &mut impl coordinator::ClientSignals,
    peer: &mut impl coordinator::PeerLiveness,
    count_acked: impl FnOnce() -> u32,
) -> Result<Response, ParkedExit> {
    tokio::pin!(wait_fut);

    tokio::select! {
        biased;
        verdict = &mut wait_fut => Ok(match verdict {
            // A demotion tore down the stream this wait was parked on.
            // Redis replies with an error from `disconnectAllBlockedClients`
            // rather than a count, because the count would describe a
            // history the node no longer heads.
            crate::replication::WaitVerdict::RoleChanged(_) => {
                Response::error(crate::commands::replication::WAIT_ROLE_CHANGED_ERR)
            }
            other => Response::Integer(other.count() as i64),
        }),
        edge = signals.next_edge() => match edge {
            ClientEdge::Killed => Err(ParkedExit::Killed),
            ClientEdge::Unblocked(Some(UnblockMode::Error)) => {
                Ok(Response::error("UNBLOCKED client unblocked via CLIENT UNBLOCK"))
            }
            // TIMEOUT mode (and a closed signal channel) reply like a
            // timed-out WAIT: the count acked so far.
            ClientEdge::Unblocked(_) => Ok(Response::Integer(count_acked() as i64)),
        },
        () = peer.closed() => Err(ParkedExit::PeerGone),
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

/// Set the registry's blocked mirror, then hand the wait to the shard.
///
/// The order is the point. `register` awaits — a full shard channel parks it —
/// and the connection is committed to blocking from the moment it is called, so
/// mirroring afterwards left a window in which the client was genuinely parked
/// while `CLIENT UNBLOCK` saw no [`ClientFlags::BLOCKED`](frogdb_core::ClientFlags)
/// and silently answered `0`: a reply that says "there was nothing to unblock"
/// about a client that is about to block for its full timeout.
///
/// Inverting the window is safe. An UNBLOCK landing between the mirror and the
/// registration sets the unblock signal, which the coordinator observes as soon
/// as it starts waiting; the wait then unwinds through the same `cleanup_wait`
/// path as any other unblock, and its `UnregisterWait` is ordered behind this
/// `BlockWait` on the shard channel either way. The cost is a signal arriving
/// marginally early — which the wait is built to absorb — against a `CLIENT
/// UNBLOCK` that lies about having done nothing.
async fn mirror_blocked_then_register(
    registry: &frogdb_core::ClientRegistry,
    conn_id: u64,
    register: impl Future<Output = Result<(), ()>>,
) -> Result<(), Response> {
    registry.update_blocked_state(conn_id, true);
    if register.await.is_err() {
        // No wait exists, so the mirror must not claim one: a flag left set here
        // makes the connection blocked-but-not-waiting, and every later UNBLOCK
        // would signal a wait that never runs.
        registry.update_blocked_state(conn_id, false);
        registry.reset_unblock(conn_id);
        return Err(Response::error("ERR Internal error: shard unreachable"));
    }
    Ok(())
}

#[cfg(test)]
mod registration_window_tests {
    use std::future::{pending, ready};
    use std::net::SocketAddr;
    use std::sync::Arc;

    use frogdb_core::ClientRegistry;

    use super::*;

    fn registry_with_client(id: u64) -> (Arc<ClientRegistry>, frogdb_core::ClientHandle) {
        let registry = Arc::new(ClientRegistry::new());
        let addr: SocketAddr = "127.0.0.1:6379".parse().expect("a literal socket address");
        let handle = registry.register(id, addr, None);
        (registry, handle)
    }

    // FM-BLOCKING-003
    /// `CLIENT UNBLOCK` must not answer `0` about a client that is in the middle
    /// of blocking.
    ///
    /// The registration is not instantaneous: handing `BlockWait` to the shard
    /// awaits, and a full shard channel parks it there. With the registry mirror
    /// set only after that send returned, an UNBLOCK arriving inside the window
    /// found no `BLOCKED` flag and replied `0` — "no such blocked client" — for
    /// a client that then blocked for its whole timeout. The operator's next
    /// move is to believe the reply.
    ///
    /// Pre-fix this test fails at the last assertion with `false`.
    #[tokio::test(start_paused = true)]
    async fn client_unblock_inside_the_registration_window_finds_the_client_blocked() {
        let (registry, _handle) = registry_with_client(7);
        assert!(
            !registry.unblock(7, UnblockMode::Timeout),
            "a connection that has not started blocking is genuinely not blocked"
        );

        // A registration whose send never completes: the window, held open.
        let parked = mirror_blocked_then_register(&registry, 7, pending::<Result<(), ()>>());
        assert!(
            tokio::time::timeout(Duration::from_secs(1), parked)
                .await
                .is_err(),
            "the registration is still parked in its send — this is the window"
        );

        assert!(
            registry.unblock(7, UnblockMode::Timeout),
            "an UNBLOCK inside the registration window must signal the wait, not report nothing to do"
        );
    }

    // FM-BLOCKING-003
    /// The inverted window must not leak: a registration that *failed* leaves no
    /// wait behind, so the mirror it set has to come back down. Otherwise the
    /// connection reads as blocked forever and every later UNBLOCK signals a
    /// wait that will never run.
    #[tokio::test]
    async fn a_failed_registration_clears_the_mirror_it_set() {
        let (registry, _handle) = registry_with_client(7);

        let err = mirror_blocked_then_register(&registry, 7, ready(Err(())))
            .await
            .expect_err("an unreachable shard must surface as an error reply");
        assert!(
            matches!(&err, Response::Error(msg) if msg.ends_with(b"shard unreachable")),
            "got {err:?}"
        );
        assert!(
            !registry.unblock(7, UnblockMode::Timeout),
            "a wait that never registered must not leave the connection looking blocked"
        );
    }
}

#[cfg(test)]
mod wait_race_tests {
    use std::future::{pending, ready};

    use frogdb_core::UnblockMode;

    use super::coordinator::test_support::{MockPeer, MockUnblock};
    use super::*;
    use crate::commands::replication::WAIT_ROLE_CHANGED_ERR;
    use crate::replication::WaitVerdict;

    fn error_text(resp: Response) -> String {
        match resp {
            Response::Error(msg) => String::from_utf8(msg.to_vec()).expect("utf8 error text"),
            other => panic!("expected an error reply, got {other:?}"),
        }
    }

    /// FM-REPLICATION-040
    ///
    /// The demotion release and a `CLIENT UNBLOCK ERROR` are ready in the *same*
    /// poll — the interleaving a live socket can only hit by luck, and the one
    /// the `biased;` ordering exists for. The client must be told the node
    /// stopped being a primary: `-UNBLOCKED ... via CLIENT UNBLOCK` would name a
    /// cause that is true but not the one that matters, leaving the caller
    /// believing the node it waited on still heads the history it waited for.
    /// The count must not even be computed, so `count_acked` panics if reached.
    // FM-BLOCKING-010
    #[tokio::test]
    async fn wait_released_by_a_demotion_reports_the_role_change_even_if_client_unblock_races() {
        let mut unblock = MockUnblock::fires(UnblockMode::Error);
        let resp = resolve_wait_race(
            ready(WaitVerdict::RoleChanged(2)),
            &mut unblock,
            &mut MockPeer::live(),
            || unreachable!("a role change must not consult the acked count"),
        )
        .await
        .expect("the connection did not end");
        assert_eq!(
            error_text(resp),
            WAIT_ROLE_CHANGED_ERR,
            "a role change and a CLIENT UNBLOCK in the same poll must resolve to the role change"
        );
    }

    /// A quorum reached in the same poll as a `CLIENT UNBLOCK` is the other half
    /// of the same tie-break: the wait already has its true answer, so replying
    /// `-UNBLOCKED` would discard a satisfied WAIT the client is entitled to.
    #[tokio::test]
    async fn a_reached_quorum_beats_a_client_unblock_in_the_same_poll() {
        let mut unblock = MockUnblock::fires(UnblockMode::Error);
        let resp = resolve_wait_race(
            ready(WaitVerdict::Reached(3)),
            &mut unblock,
            &mut MockPeer::live(),
            || unreachable!("a decided wait must not consult the acked count"),
        )
        .await
        .expect("the connection did not end");
        assert!(matches!(resp, Response::Integer(3)));
    }

    /// With the wait genuinely parked, `CLIENT UNBLOCK ERROR` is the escape
    /// hatch for `WAIT ... 0` and must still win — the bias is a tie-break, not
    /// a veto.
    #[tokio::test]
    async fn client_unblock_error_releases_a_parked_wait() {
        let mut unblock = MockUnblock::fires(UnblockMode::Error);
        let resp = resolve_wait_race(
            pending::<WaitVerdict>(),
            &mut unblock,
            &mut MockPeer::live(),
            || unreachable!("ERROR mode replies with the error, not a count"),
        )
        .await
        .expect("the connection did not end");
        assert_eq!(
            error_text(resp),
            "UNBLOCKED client unblocked via CLIENT UNBLOCK"
        );
    }

    /// TIMEOUT mode replies like a timed-out WAIT: the count acked so far, read
    /// at release time rather than captured when the wait was registered.
    #[tokio::test]
    async fn client_unblock_timeout_mode_reports_the_acked_count() {
        let mut unblock = MockUnblock::fires(UnblockMode::Timeout);
        let resp = resolve_wait_race(
            pending::<WaitVerdict>(),
            &mut unblock,
            &mut MockPeer::live(),
            || 4,
        )
        .await
        .expect("the connection did not end");
        assert!(matches!(resp, Response::Integer(4)));
    }

    /// A wait that resolves on its own with nobody unblocking it is the ordinary
    /// path, and answers with the verdict's count.
    #[tokio::test]
    async fn a_timed_out_wait_answers_with_its_own_count() {
        let mut unblock = MockUnblock::never();
        let resp = resolve_wait_race(
            ready(WaitVerdict::TimedOut(1)),
            &mut unblock,
            &mut MockPeer::live(),
            || unreachable!("the verdict already carries the count"),
        )
        .await
        .expect("the connection did not end");
        assert!(matches!(resp, Response::Integer(1)));
    }

    /// `WAIT numreplicas 0` parks forever, so it is the sharpest case for the
    /// two terminating edges: without them the connection is unreclaimable by
    /// either the operator or the peer.
    // TR-BLOCKING-021
    #[tokio::test]
    async fn client_kill_releases_a_parked_wait_without_replying() {
        let mut unblock = MockUnblock::kills();
        let exit = resolve_wait_race(
            pending::<WaitVerdict>(),
            &mut unblock,
            &mut MockPeer::live(),
            || unreachable!("a killed connection has no reader for a count"),
        )
        .await
        .expect_err("CLIENT KILL must end the connection, not reply");
        assert_eq!(exit, ParkedExit::Killed);
    }

    // TR-BLOCKING-013
    #[tokio::test]
    async fn a_peer_that_leaves_releases_a_parked_wait_without_replying() {
        let mut unblock = MockUnblock::never();
        let exit = resolve_wait_race(
            pending::<WaitVerdict>(),
            &mut unblock,
            &mut MockPeer::gone(),
            || unreachable!("a departed peer has no reader for a count"),
        )
        .await
        .expect_err("a departed peer must end the connection, not reply");
        assert_eq!(exit, ParkedExit::PeerGone);
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

/// The blocking-park seams of the zero-copy parse path (see
/// [`frogdb_protocol::detach_bytes`]).
#[cfg(test)]
mod zero_copy_seam_tests {
    use frogdb_protocol::Direction;

    use super::*;

    // FM-MEMORY-003
    #[test]
    fn wait_inputs_detach_from_the_shared_read_buffer() {
        let backing = Bytes::from(b"BLMOVE src dst".to_vec());
        let keys = vec![backing.slice(7..10)];
        let proto_op = frogdb_protocol::BlockingOp::BLMove {
            dest: backing.slice(11..14),
            src_dir: Direction::Left,
            dest_dir: Direction::Right,
        };

        let (keys, op) = detach_wait_inputs(keys, proto_op);

        assert!(
            backing.is_unique(),
            "a parked wait must hold no reference into the connection's read buffer"
        );
        assert_eq!(keys, vec![Bytes::from_static(b"src")]);
        match op {
            BlockingOp::BLMove { dest, .. } => assert_eq!(dest.as_ref(), b"dst"),
            other => panic!("BLMOVE converts to BLMove, got {other:?}"),
        }
    }
}

/// [`SocketWatch`] over a real loopback socket: read-ahead during a park
/// reallocates the read buffer, and the parked command's own args are what is
/// left holding the old allocation.
#[cfg(all(test, not(feature = "turmoil")))]
mod read_ahead_tests {
    use bytes::BytesMut;
    use frogdb_protocol::ParsedCommand;
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};
    use tokio_util::codec::FramedParts;

    use super::coordinator::PeerLiveness;
    use super::*;
    use crate::tls::MaybeTlsStream;

    /// Same seed size as the production read buffer (`lifecycle::READ_IDLE_TARGET`).
    const READ_BUF: usize = 8 * 1024;

    async fn loopback() -> (Framed<ConnectionStream, FrogDbResp2>, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("loopback address");
        let client = TcpStream::connect(addr).await.expect("connect loopback");
        let (server, _) = listener.accept().await.expect("accept loopback");
        let mut parts = FramedParts::new::<BytesFrame>(
            MaybeTlsStream::Plain { inner: server },
            FrogDbResp2::default(),
        );
        parts.read_buf = BytesMut::with_capacity(READ_BUF);
        (Framed::from_parts(parts), client)
    }

    fn bulk_frame(payload: &[u8]) -> Vec<u8> {
        let mut frame = format!("*1\r\n${}\r\n", payload.len()).into_bytes();
        frame.extend_from_slice(payload);
        frame.extend_from_slice(b"\r\n");
        frame
    }

    // FM-MEMORY-003
    /// Pipelined frames arriving behind a parked `BLPOP` overflow the 8 KiB
    /// read buffer, so `reserve` moves the codec onto a fresh allocation while
    /// the parked command still holds slices of the old one. Two things keep
    /// that from pinning the old allocation behind a short key: the parked
    /// frames were copied out before each poll (so they hold nothing), and
    /// the command's name co-holds the allocation with its args, so no arg is
    /// unique while the command is alive — `detach_bytes` copies.
    #[tokio::test]
    async fn read_ahead_realloc_leaves_parked_args_shared_and_parked_frames_detached() {
        let (mut framed, mut client) = loopback().await;

        client
            .write_all(b"*2\r\n$5\r\nBLPOP\r\n$1\r\nk\r\n")
            .await
            .expect("write BLPOP");
        let frame = framed
            .next()
            .await
            .expect("a frame")
            .expect("a well-formed frame");
        let cmd = ParsedCommand::try_from(frame).expect("BLPOP parses");
        assert!(
            !cmd.args[0].is_unique(),
            "name and key are slices of the one read buffer"
        );

        // 20 × ~620 B > 8 KiB, under MAX_PARKED_PIPELINE_FRAMES.
        let read_ahead = 20;
        assert!(read_ahead < MAX_PARKED_PIPELINE_FRAMES);
        for i in 0..read_ahead {
            let payload = vec![b'a' + (i as u8 % 26); 600];
            client
                .write_all(&bulk_frame(&payload))
                .await
                .expect("write pipelined frame");
        }
        client.shutdown().await.expect("client shutdown");

        let mut parked = VecDeque::new();
        SocketWatch {
            framed: &mut framed,
            parked: &mut parked,
        }
        .closed()
        .await;

        assert_eq!(parked.len(), read_ahead);
        for frame in &parked {
            match frame.as_ref().expect("a well-formed parked frame") {
                BytesFrame::Array(items) => match &items[0] {
                    BytesFrame::BulkString(payload) => assert!(
                        payload.is_unique(),
                        "each parked frame is copied out of the read buffer"
                    ),
                    other => panic!("expected a bulk string, got {other:?}"),
                },
                other => panic!("expected an array frame, got {other:?}"),
            }
        }

        // The command is alive: its key is co-held by its name, so it is not
        // unique and the retention-point copy still happens.
        let key = cmd.args[0].clone();
        assert!(!key.is_unique());
        let detached = frogdb_protocol::detach_bytes(key.clone());
        assert_ne!(
            detached.as_ptr(),
            key.as_ptr(),
            "detach_bytes copies a shared slice"
        );
        assert_eq!(detached.as_ref(), b"k");

        // Proof the read-ahead reallocated: once the rest of the command is
        // gone, the key is the sole holder of the old allocation — the codec
        // no longer references it.
        drop(cmd);
        assert!(
            key.is_unique(),
            "read-ahead past the buffer capacity must have moved the codec off the old allocation"
        );
    }
}
