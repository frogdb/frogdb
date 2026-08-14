//! Blocking-wait coordination.
//!
//! This module owns the one real server-side decision in a blocking command:
//! *which event won the race* between the shard delivering a value, a
//! `CLIENT KILL`/`CLIENT UNBLOCK` edge, the peer's socket reaching EOF, and the
//! deadline elapsing. The handler keeps registration, bookkeeping, and
//! unregistration (it owns the shard senders and the client registry); the
//! coordinator is a pure async decision over four inputs, which makes it
//! unit-testable with an in-memory `oneshot` and mock signal/peer sources.

use frogdb_core::{BlockingOp, ClientEdge, ClientHandle, UnblockMode};
use frogdb_protocol::Response;
use tokio::sync::oneshot;
// The deadline is slept on with `tokio::time::sleep_until`, so it must be stated on the
// timer's clock. A `std::time::Instant` converted here would be reinterpreted as an offset
// from the runtime's start, which under a paused clock is an arbitrarily different time.
use tokio::time::Instant;

/// Reply for a blocking wait whose shard died underneath it.
///
/// Deliberately the same vocabulary `EXEC` uses for the same underlying event
/// (`specs/txn.md` FM-TXN-032): the shard's mailbox is gone, so nothing was or
/// will be done. It is an *error*, not a nil, so a client can tell it apart from
/// an ordinary timeout instead of retrying into a dead shard forever
/// (`specs/blocking.md` FM-BLOCKING-004).
pub const SHARD_UNAVAILABLE_ERR: &str = "ERR shard unavailable";

/// Outcome of a blocking wait. Public so it can be asserted in unit tests and
/// converted to a reply with op-aware nil shaping.
#[derive(Debug)]
pub enum WaitOutcome {
    /// The shard delivered a reply (or the response channel closed).
    Response(Response),
    /// The deadline elapsed.
    Timeout,
    /// CLIENT UNBLOCK signalled this connection.
    Unblocked(UnblockMode),
    /// The connection itself ended while the wait was parked: the peer closed
    /// the socket, or `CLIENT KILL` targeted this connection. There is nobody
    /// left to reply to — the wait is *abandoned*, not resolved, and the run
    /// loop terminates the connection (`specs/blocking.md` TR-BLOCKING-013,
    /// TR-BLOCKING-021).
    ConnectionEnded(ParkedExit),
}

/// Which of the two connection-terminating edges ended a parked wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParkedExit {
    /// The peer closed the socket (EOF) while the wait was parked.
    PeerGone,
    /// `CLIENT KILL` targeted this connection while the wait was parked.
    Killed,
}

impl ParkedExit {
    /// Short label for the connection-teardown log line.
    pub fn reason(self) -> &'static str {
        match self {
            ParkedExit::PeerGone => "peer disconnected while parked",
            ParkedExit::Killed => "CLIENT KILL while parked",
        }
    }
}

impl WaitOutcome {
    /// Convert to the client reply, choosing the nil shape from the op so an
    /// array-returning op (BLPOP, BZPOPMIN, XREAD, ...) times out with a null
    /// *array* and a single-value op (BLMOVE, BRPOPLPUSH) with a null *bulk*.
    ///
    /// This is the one place the wrong-nil-shape bug is fixed: the op survives
    /// the race so the timeout reply can pick the correct RESP2 shape.
    pub fn into_response(self, op: &BlockingOp) -> Response {
        match self {
            WaitOutcome::Response(resp) => resp,
            WaitOutcome::Timeout => op.timeout_reply(),
            WaitOutcome::Unblocked(UnblockMode::Timeout) => op.timeout_reply(),
            WaitOutcome::Unblocked(UnblockMode::Error) => {
                Response::error("UNBLOCKED client unblocked via CLIENT UNBLOCK")
            }
            // Never reached in production: the caller checks for this variant
            // before it ever asks for a reply, because there is no reader left.
            WaitOutcome::ConnectionEnded(_) => Response::Null,
        }
    }
}

/// The `CLIENT UNBLOCK` and `CLIENT KILL` edges, as one seam.
///
/// One seam rather than two because in production both edges are watch
/// receivers on the same [`ClientHandle`], which a `select!` cannot borrow
/// twice; [`ClientHandle::killed_or_unblocked`] owns that split-borrow race.
/// Tests supply a mock that fires on command — the trait exists solely to cut
/// the one dependency (the registry watch channels) that otherwise forces the
/// race to be tested through a live socket.
pub trait ClientSignals {
    /// Resolves when `CLIENT KILL` or `CLIENT UNBLOCK` targets this connection.
    fn next_edge(&mut self) -> impl std::future::Future<Output = ClientEdge> + Send;
}

impl ClientSignals for ClientHandle {
    async fn next_edge(&mut self) -> ClientEdge {
        ClientHandle::killed_or_unblocked(self).await
    }
}

/// The peer's liveness, as a seam.
///
/// A parked wait must keep the socket polled — otherwise a peer that vanishes
/// mid-wait is undetectable and its wait entry, FD and waiter budget leak
/// forever (`specs/blocking.md` TR-BLOCKING-013, distsys-review CRIT-4). The
/// production adapter reads the connection's `Framed`; frames that arrive while
/// parked are *buffered*, not executed, so pipelining behind a blocking command
/// keeps Redis's ordering (a blocked client's later commands run after it
/// unblocks) instead of being reordered ahead of the wait.
pub trait PeerLiveness {
    /// Resolves when the peer's socket reaches EOF, i.e. the client is gone.
    /// Never resolves while the peer is merely idle.
    fn closed(&mut self) -> impl std::future::Future<Output = ()> + Send;
}

/// Races the shard response, the client-admin edges, the peer's socket and the
/// deadline for a single blocking wait. Stateless: the caller owns
/// register/cleanup.
pub struct BlockingWaitCoordinator;

impl BlockingWaitCoordinator {
    /// Race the shard response, `CLIENT KILL`, `CLIENT UNBLOCK`, the peer's
    /// socket and the deadline.
    ///
    /// Pure: the caller owns register/cleanup. `deadline = None` blocks forever.
    /// The `biased` ordering favours a delivered response over a simultaneous
    /// deadline, so a value that arrives exactly at the deadline is never lost
    /// to a spurious timeout.
    ///
    /// The kill and peer-liveness branches are what make a parked wait a
    /// *supervised* state rather than a suspended read loop: the two inputs the
    /// connection run loop would otherwise be polling — `CLIENT KILL` and the
    /// socket — stay live for the whole park, so every blocked state is
    /// leavable and none of them can leak its waiter (TR-BLOCKING-013,
    /// TR-BLOCKING-021).
    pub async fn wait_for_response(
        response_rx: &mut oneshot::Receiver<Response>,
        deadline: Option<Instant>,
        signals: &mut impl ClientSignals,
        peer: &mut impl PeerLiveness,
    ) -> WaitOutcome {
        // A deadline future that never resolves when there is no deadline keeps
        // the select! branch alive without artificially timing out.
        let timeout_fut = async {
            match deadline {
                Some(d) => tokio::time::sleep_until(d).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(timeout_fut);

        tokio::select! {
            biased;
            // 1. Shard delivered a value (or the channel closed). Borrowed, not
            // consumed, so the caller can drain a value the shard sends in the
            // pop→deliver window *after* a timeout is chosen — see
            // `cleanup_wait`'s `UnregisterAck::AlreadyServed` reconciliation.
            recv = &mut *response_rx => match recv {
                Ok(resp) => WaitOutcome::Response(resp),
                // A closed channel is shard death, and nothing else. Every
                // shard-side resolution — satisfy, the deadline fast-path, the
                // GC tick, admission refusal, the demotion release, the
                // disconnect drain — *sends* its reply, so sender-drop carries
                // no other meaning to confuse this with.
                Err(_) => WaitOutcome::Response(Response::error(SHARD_UNAVAILABLE_ERR)),
            },
            // 2. A CLIENT KILL / CLIENT UNBLOCK edge fired. A kill wins the tie
            // inside the seam, matching the run loop's own bias.
            edge = signals.next_edge() => match edge {
                ClientEdge::Killed => WaitOutcome::ConnectionEnded(ParkedExit::Killed),
                ClientEdge::Unblocked(Some(m)) => WaitOutcome::Unblocked(m),
                // A closed signal channel is a registry-side failure, not shard
                // health: it keeps its historical bare-nil reply rather than
                // borrowing FM-BLOCKING-004's shard-death error.
                ClientEdge::Unblocked(None) => WaitOutcome::Response(Response::Null),
            },
            // 3. The peer vanished. Ahead of the deadline because there is
            // nobody left to time out *to*; the wait is abandoned and the
            // connection torn down instead of replied to.
            () = peer.closed() => WaitOutcome::ConnectionEnded(ParkedExit::PeerGone),
            // 4. Deadline elapsed.
            _ = &mut timeout_fut => WaitOutcome::Timeout,
        }
    }
}

/// Test-only [`ClientSignals`] / [`PeerLiveness`] sources, shared with the WAIT
/// race in the parent module: both races read the same edges through the same
/// seams, so they are pinned against the same mocks rather than two that could
/// drift.
#[cfg(test)]
pub(crate) mod test_support {
    use std::future::Future;

    use frogdb_core::{ClientEdge, UnblockMode};

    use super::{ClientSignals, PeerLiveness};

    /// Mock admin-edge source. Fires `edge` once on the first poll if `Some`,
    /// otherwise pends forever (nothing is targeting this connection).
    pub(crate) struct MockUnblock {
        edge: Option<ClientEdge>,
    }

    impl MockUnblock {
        pub(crate) fn never() -> Self {
            Self { edge: None }
        }
        pub(crate) fn fires(mode: UnblockMode) -> Self {
            Self {
                edge: Some(ClientEdge::Unblocked(Some(mode))),
            }
        }
        /// `CLIENT KILL` targets this connection.
        pub(crate) fn kills() -> Self {
            Self {
                edge: Some(ClientEdge::Killed),
            }
        }
    }

    impl ClientSignals for MockUnblock {
        fn next_edge(&mut self) -> impl Future<Output = ClientEdge> + Send {
            let edge = self.edge;
            async move {
                match edge {
                    Some(e) => e,
                    None => std::future::pending().await,
                }
            }
        }
    }

    /// Mock peer socket: either already at EOF, or a peer that never leaves.
    pub(crate) struct MockPeer {
        gone: bool,
    }

    impl MockPeer {
        pub(crate) fn live() -> Self {
            Self { gone: false }
        }
        pub(crate) fn gone() -> Self {
            Self { gone: true }
        }
    }

    impl PeerLiveness for MockPeer {
        fn closed(&mut self) -> impl Future<Output = ()> + Send {
            let gone = self.gone;
            async move {
                if !gone {
                    std::future::pending::<()>().await
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::test_support::{MockPeer, MockUnblock};
    use super::*;

    // FM-BLOCKING-001
    #[tokio::test]
    async fn response_wins() {
        let (tx, mut rx) = oneshot::channel();
        tx.send(Response::Integer(7)).unwrap();
        let mut unblock = MockUnblock::never();
        let outcome = BlockingWaitCoordinator::wait_for_response(
            &mut rx,
            None,
            &mut unblock,
            &mut MockPeer::live(),
        )
        .await;
        assert!(matches!(
            outcome,
            WaitOutcome::Response(Response::Integer(7))
        ));
    }

    /// A closed response channel means the shard is gone, and says so. Every
    /// other shard-side resolution sends a reply, so this cannot be an ordinary
    /// timeout — reporting it as a nil would leave a client unable to tell "no
    /// data arrived" from "the shard serving your key died", and retrying into
    /// it forever.
    // FM-BLOCKING-004
    #[tokio::test]
    async fn channel_drop_yields_shard_unavailable_error() {
        let (tx, mut rx) = oneshot::channel::<Response>();
        drop(tx);
        let mut unblock = MockUnblock::never();
        let outcome = BlockingWaitCoordinator::wait_for_response(
            &mut rx,
            None,
            &mut unblock,
            &mut MockPeer::live(),
        )
        .await;
        let reply = outcome.into_response(&BlockingOp::BLPop);
        assert_eq!(reply, Response::error(SHARD_UNAVAILABLE_ERR));
        assert_eq!(reply, Response::error("ERR shard unavailable"));
    }

    // FM-BLOCKING-002
    #[tokio::test]
    async fn timeout_wins_when_idle() {
        // Keep the sender alive but never send, so only the deadline can fire.
        let (_tx, mut rx) = oneshot::channel::<Response>();
        let mut unblock = MockUnblock::never();
        let deadline = Instant::now() + Duration::from_millis(10);
        let outcome = BlockingWaitCoordinator::wait_for_response(
            &mut rx,
            Some(deadline),
            &mut unblock,
            &mut MockPeer::live(),
        )
        .await;
        assert!(matches!(outcome, WaitOutcome::Timeout));
    }

    // FM-BLOCKING-003
    #[tokio::test]
    async fn unblock_wins_over_idle_wait() {
        let (_tx, mut rx) = oneshot::channel::<Response>();
        let mut unblock = MockUnblock::fires(UnblockMode::Error);
        let outcome = BlockingWaitCoordinator::wait_for_response(
            &mut rx,
            None,
            &mut unblock,
            &mut MockPeer::live(),
        )
        .await;
        assert!(matches!(
            outcome,
            WaitOutcome::Unblocked(UnblockMode::Error)
        ));
    }

    // FM-BLOCKING-001
    #[tokio::test]
    async fn biased_response_beats_elapsed_deadline() {
        // Both the response and the deadline are ready: biased ordering must
        // pick the response so a value arriving at the deadline is not lost.
        let (tx, mut rx) = oneshot::channel();
        tx.send(Response::Integer(1)).unwrap();
        let mut unblock = MockUnblock::never();
        // Deadline already in the past.
        let deadline = Instant::now() - Duration::from_millis(1);
        let outcome = BlockingWaitCoordinator::wait_for_response(
            &mut rx,
            Some(deadline),
            &mut unblock,
            &mut MockPeer::live(),
        )
        .await;
        assert!(
            matches!(outcome, WaitOutcome::Response(Response::Integer(1))),
            "biased select must favour the response over a simultaneous timeout"
        );
    }

    /// A peer that vanishes mid-park must end the wait. Without this branch the
    /// park is a suspended read loop: the socket is never polled, so the EOF is
    /// invisible and the entry, FD and waiter budget leak for the life of the
    /// process.
    // TR-BLOCKING-013
    #[tokio::test]
    async fn peer_gone_while_parked_ends_the_connection() {
        // Sender alive, no deadline: only the peer branch can resolve this.
        let (_tx, mut rx) = oneshot::channel::<Response>();
        let mut unblock = MockUnblock::never();
        let outcome = BlockingWaitCoordinator::wait_for_response(
            &mut rx,
            None,
            &mut unblock,
            &mut MockPeer::gone(),
        )
        .await;
        assert!(
            matches!(outcome, WaitOutcome::ConnectionEnded(ParkedExit::PeerGone)),
            "a parked wait must observe its peer leaving, got {outcome:?}"
        );
    }

    /// A value already delivered wins over a peer that left in the same poll:
    /// the reply is genuinely available, and the connection teardown that
    /// follows the next read is the ordinary path.
    // TR-BLOCKING-013
    #[tokio::test]
    async fn response_beats_a_peer_that_left_in_the_same_poll() {
        let (tx, mut rx) = oneshot::channel();
        tx.send(Response::Integer(3)).unwrap();
        let mut unblock = MockUnblock::never();
        let outcome = BlockingWaitCoordinator::wait_for_response(
            &mut rx,
            None,
            &mut unblock,
            &mut MockPeer::gone(),
        )
        .await;
        assert!(
            matches!(outcome, WaitOutcome::Response(Response::Integer(3))),
            "biased select must favour a delivered response over the peer branch"
        );
    }

    /// `CLIENT KILL` against a parked client must terminate it. Before this
    /// branch existed the command reported success and did nothing, forever, so
    /// a node with parked clients could not be drained.
    // TR-BLOCKING-021
    // FM-BLOCKING-012
    #[tokio::test]
    async fn client_kill_while_parked_ends_the_connection() {
        let (_tx, mut rx) = oneshot::channel::<Response>();
        let mut unblock = MockUnblock::kills();
        let outcome = BlockingWaitCoordinator::wait_for_response(
            &mut rx,
            None,
            &mut unblock,
            &mut MockPeer::live(),
        )
        .await;
        assert!(
            matches!(outcome, WaitOutcome::ConnectionEnded(ParkedExit::Killed)),
            "CLIENT KILL must end a parked wait, got {outcome:?}"
        );
    }

    /// A response already in the channel outranks a pending kill: the element
    /// has left the shard's data structure, so dropping it here would lose it.
    /// The connection still ends — on the *next* poll, after the reply is
    /// written — which is what the biased ordering buys.
    // TR-BLOCKING-021
    #[tokio::test]
    async fn response_beats_a_kill_in_the_same_poll() {
        let (tx, mut rx) = oneshot::channel();
        tx.send(Response::Integer(7)).unwrap();
        let mut unblock = MockUnblock::kills();
        let outcome = BlockingWaitCoordinator::wait_for_response(
            &mut rx,
            None,
            &mut unblock,
            &mut MockPeer::live(),
        )
        .await;
        assert!(
            matches!(outcome, WaitOutcome::Response(Response::Integer(7))),
            "biased select must favour a delivered response over the kill branch, got {outcome:?}"
        );
    }

    /// A kill and an unblock landing together resolve as a kill: replying to a
    /// connection that is being torn down is worse than dropping the reply,
    /// and the operator asked for the connection to go away.
    // FM-BLOCKING-012
    #[tokio::test]
    async fn a_kill_beats_a_simultaneous_client_unblock() {
        let registry = std::sync::Arc::new(frogdb_core::ClientRegistry::new());
        let mut handle = registry.register(1, "127.0.0.1:1".parse().unwrap(), None);
        registry.update_blocked_state(1, true);
        // Both edges set before the wait is ever polled: the watches are
        // level-triggered, so this is the same observation as a live race but
        // deterministic.
        assert!(registry.kill_by_id(1));
        assert!(registry.unblock(1, UnblockMode::Error));

        let (_tx, mut rx) = oneshot::channel::<Response>();
        let outcome = BlockingWaitCoordinator::wait_for_response(
            &mut rx,
            None,
            &mut handle,
            &mut MockPeer::live(),
        )
        .await;
        assert!(
            matches!(outcome, WaitOutcome::ConnectionEnded(ParkedExit::Killed)),
            "a killed connection must terminate rather than reply, got {outcome:?}"
        );
    }

    /// The production `ClientSignals` impl must still deliver an unblock when
    /// no kill is pending — the biased kill branch must not starve it.
    // FM-BLOCKING-003
    #[tokio::test]
    async fn the_real_client_handle_reports_an_unblock_when_not_killed() {
        let registry = std::sync::Arc::new(frogdb_core::ClientRegistry::new());
        let mut handle = registry.register(2, "127.0.0.1:2".parse().unwrap(), None);
        registry.update_blocked_state(2, true);
        assert!(registry.unblock(2, UnblockMode::Timeout));

        let (_tx, mut rx) = oneshot::channel::<Response>();
        let outcome = BlockingWaitCoordinator::wait_for_response(
            &mut rx,
            None,
            &mut handle,
            &mut MockPeer::live(),
        )
        .await;
        assert!(
            matches!(outcome, WaitOutcome::Unblocked(UnblockMode::Timeout)),
            "got {outcome:?}"
        );
    }

    // FM-BLOCKING-002, FM-BLOCKING-003
    #[test]
    fn timeout_reply_picks_nil_shape_per_op() {
        use frogdb_core::Direction;

        // Array op → null array.
        let blpop = WaitOutcome::Timeout.into_response(&BlockingOp::BLPop);
        assert!(matches!(blpop, Response::NullArray));

        // Single-value op → null bulk.
        let blmove = WaitOutcome::Timeout.into_response(&BlockingOp::BLMove {
            dest: bytes::Bytes::from_static(b"d"),
            src_dir: Direction::Left,
            dest_dir: Direction::Right,
        });
        assert!(matches!(blmove, Response::Null));

        // CLIENT UNBLOCK with TIMEOUT mode uses the same op-aware nil.
        let unblocked =
            WaitOutcome::Unblocked(UnblockMode::Timeout).into_response(&BlockingOp::BZPopMin);
        assert!(matches!(unblocked, Response::NullArray));

        // CLIENT UNBLOCK with ERROR mode is an -UNBLOCKED error regardless of op.
        let err = WaitOutcome::Unblocked(UnblockMode::Error).into_response(&BlockingOp::BLPop);
        assert!(matches!(err, Response::Error(_)));
    }
}
