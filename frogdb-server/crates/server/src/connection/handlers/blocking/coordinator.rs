//! Blocking-wait coordination.
//!
//! This module owns the one real server-side decision in a blocking command:
//! *which event won the race* between the shard delivering a value, a CLIENT
//! UNBLOCK signal, and the deadline elapsing. The handler keeps registration,
//! bookkeeping, and unregistration (it owns the shard senders and the client
//! registry); the coordinator is a pure async decision over three inputs, which
//! makes it unit-testable with an in-memory `oneshot` and a mock unblock source.

use std::time::Instant;

use frogdb_core::{BlockingOp, ClientHandle, UnblockMode};
use frogdb_protocol::Response;
use tokio::sync::oneshot;

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
        }
    }
}

/// The CLIENT UNBLOCK edge, as a seam.
///
/// [`ClientHandle`] is the production adapter; tests supply a mock that fires on
/// command. This is the one new trait, and it exists solely to cut the one
/// dependency (the registry watch channel) that otherwise forces the race to be
/// tested through a live socket.
pub trait UnblockSignal {
    /// Resolves when CLIENT UNBLOCK targets this connection, yielding the mode;
    /// resolves to `None` if the signal channel closed.
    fn unblocked(&mut self) -> impl std::future::Future<Output = Option<UnblockMode>> + Send;
}

impl UnblockSignal for ClientHandle {
    async fn unblocked(&mut self) -> Option<UnblockMode> {
        ClientHandle::unblocked(self).await
    }
}

/// Races the shard response, CLIENT UNBLOCK, and the deadline for a single
/// blocking wait. Stateless: the caller owns register/cleanup.
pub struct BlockingWaitCoordinator;

impl BlockingWaitCoordinator {
    /// Race the shard response, CLIENT UNBLOCK, and the deadline.
    ///
    /// Pure: the caller owns register/cleanup. `deadline = None` blocks forever.
    /// The `biased` ordering favours a delivered response over a simultaneous
    /// deadline, so a value that arrives exactly at the deadline is never lost
    /// to a spurious timeout.
    pub async fn wait_for_response(
        response_rx: oneshot::Receiver<Response>,
        deadline: Option<Instant>,
        unblock: &mut impl UnblockSignal,
    ) -> WaitOutcome {
        // A deadline future that never resolves when there is no deadline keeps
        // the select! branch alive without artificially timing out.
        let timeout_fut = async {
            match deadline {
                Some(d) => tokio::time::sleep_until(d.into()).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(timeout_fut);

        tokio::select! {
            biased;
            // 1. Shard delivered a value (or the channel closed).
            recv = response_rx => match recv {
                Ok(resp) => WaitOutcome::Response(resp),
                Err(_) => WaitOutcome::Response(Response::Null),
            },
            // 2. CLIENT UNBLOCK signal fired.
            mode = unblock.unblocked() => match mode {
                Some(m) => WaitOutcome::Unblocked(m),
                None => WaitOutcome::Response(Response::Null),
            },
            // 3. Deadline elapsed.
            _ = &mut timeout_fut => WaitOutcome::Timeout,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::time::Duration;

    use super::*;

    /// Mock unblock source. Fires `mode` once on the first poll if `Some`,
    /// otherwise pends forever (the connection is not being CLIENT UNBLOCKed).
    struct MockUnblock {
        mode: Option<UnblockMode>,
    }

    impl MockUnblock {
        fn never() -> Self {
            Self { mode: None }
        }
        fn fires(mode: UnblockMode) -> Self {
            Self { mode: Some(mode) }
        }
    }

    impl UnblockSignal for MockUnblock {
        fn unblocked(&mut self) -> impl Future<Output = Option<UnblockMode>> + Send {
            let mode = self.mode;
            async move {
                match mode {
                    Some(m) => Some(m),
                    None => std::future::pending().await,
                }
            }
        }
    }

    #[tokio::test]
    async fn response_wins() {
        let (tx, rx) = oneshot::channel();
        tx.send(Response::Integer(7)).unwrap();
        let mut unblock = MockUnblock::never();
        let outcome = BlockingWaitCoordinator::wait_for_response(rx, None, &mut unblock).await;
        assert!(matches!(
            outcome,
            WaitOutcome::Response(Response::Integer(7))
        ));
    }

    #[tokio::test]
    async fn channel_drop_yields_null_response() {
        let (tx, rx) = oneshot::channel::<Response>();
        drop(tx);
        let mut unblock = MockUnblock::never();
        let outcome = BlockingWaitCoordinator::wait_for_response(rx, None, &mut unblock).await;
        assert!(matches!(outcome, WaitOutcome::Response(Response::Null)));
    }

    #[tokio::test]
    async fn timeout_wins_when_idle() {
        // Keep the sender alive but never send, so only the deadline can fire.
        let (_tx, rx) = oneshot::channel::<Response>();
        let mut unblock = MockUnblock::never();
        let deadline = Instant::now() + Duration::from_millis(10);
        let outcome =
            BlockingWaitCoordinator::wait_for_response(rx, Some(deadline), &mut unblock).await;
        assert!(matches!(outcome, WaitOutcome::Timeout));
    }

    #[tokio::test]
    async fn unblock_wins_over_idle_wait() {
        let (_tx, rx) = oneshot::channel::<Response>();
        let mut unblock = MockUnblock::fires(UnblockMode::Error);
        let outcome = BlockingWaitCoordinator::wait_for_response(rx, None, &mut unblock).await;
        assert!(matches!(
            outcome,
            WaitOutcome::Unblocked(UnblockMode::Error)
        ));
    }

    #[tokio::test]
    async fn biased_response_beats_elapsed_deadline() {
        // Both the response and the deadline are ready: biased ordering must
        // pick the response so a value arriving at the deadline is not lost.
        let (tx, rx) = oneshot::channel();
        tx.send(Response::Integer(1)).unwrap();
        let mut unblock = MockUnblock::never();
        // Deadline already in the past.
        let deadline = Instant::now() - Duration::from_millis(1);
        let outcome =
            BlockingWaitCoordinator::wait_for_response(rx, Some(deadline), &mut unblock).await;
        assert!(
            matches!(outcome, WaitOutcome::Response(Response::Integer(1))),
            "biased select must favour the response over a simultaneous timeout"
        );
    }

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
