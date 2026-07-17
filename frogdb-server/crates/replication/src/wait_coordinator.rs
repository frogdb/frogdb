//! The WAIT quorum decision, owned in one place.
//!
//! `WAIT numreplicas timeout` asks one question — *have `numreplicas` replicas
//! acknowledged the stream up to where it was when WAIT arrived?* — but before
//! this module the answer was assembled ad hoc on the server side: the offset
//! snapshot, the quorum count, the timeout fallback, and (missing entirely) the
//! ack solicitation each lived at the call site. [`WaitCoordinator`] mirrors
//! Redis's `waitCommand` (`replication.c`) as a single seam:
//!
//! 1. **Snapshot** the live offset via the [`OffsetCoordinator`] (Redis:
//!    `c->woff` — see the divergence note on [`WaitCoordinator::target_offset`]).
//! 2. **Immediate check**: if the quorum is already met, return the count
//!    without blocking (Redis: `replicationCountAcksByOffset` fast path).
//! 3. **Solicit acks once** when blocking with streaming replicas attached
//!    (Redis: `replicationRequestAckFromSlaves` → one `REPLCONF GETACK *`
//!    broadcast from `beforeSleep`). Without this, WAIT latency is bounded
//!    below by the replica's spontaneous 1-second ACK cadence.
//! 4. **Block** until the quorum is reached or the deadline elapses, and
//!    return the acked count either way (Redis: `blockForReplication` +
//!    `processClientsWaitingReplicas`).
//!
//! The CLIENT UNBLOCK race deliberately stays with the caller (the server's
//! connection handler owns the client registry); this module owns everything
//! that is *replication*, nothing that is *connection*.

use std::sync::Arc;
use std::time::Instant;

use frogdb_types::ReplicationTracker;

use crate::offset_coordinator::OffsetCoordinator;
use crate::primary::PrimaryReplicationHandler;
use crate::tracker::ReplicationTrackerImpl;

/// The ack-solicitation edge, as a seam.
///
/// [`PrimaryReplicationHandler`] is the production adapter (it broadcasts
/// `REPLCONF GETACK *` through the offset-stamped command stream); tests supply
/// a mock that records invocations. Mirrors the `UnblockSignal` precedent from
/// the blocking-wait coordinator: one trait, existing solely to cut the one
/// dependency that would otherwise force quorum tests through a live socket.
pub trait AckSolicitor: Sync {
    /// Ask every streaming replica to report its replication offset now.
    fn solicit_acks(&self) -> impl std::future::Future<Output = ()> + Send;
}

impl AckSolicitor for PrimaryReplicationHandler {
    async fn solicit_acks(&self) {
        self.request_acks().await;
    }
}

/// How a WAIT ended. Both arms carry the acked count, because WAIT's reply is
/// the count *regardless* of whether the quorum was reached — the verdict only
/// distinguishes the paths for observability and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitVerdict {
    /// The quorum was reached (count ≥ numreplicas).
    Reached(u32),
    /// The deadline elapsed first; count is what had acked by then.
    TimedOut(u32),
}

impl WaitVerdict {
    /// The number of replicas that acknowledged the target offset — the WAIT
    /// reply value in every outcome.
    pub fn count(&self) -> u32 {
        match self {
            WaitVerdict::Reached(n) | WaitVerdict::TimedOut(n) => *n,
        }
    }
}

/// Single owner of the WAIT quorum decision.
///
/// Reads the live offset through the [`OffsetCoordinator`] (never a raw field)
/// and counts acknowledgments through the tracker's streaming-ack projection —
/// the same projection ROLE's replica listing reads — so "which replicas count
/// and what have they acked" has exactly one definition.
pub struct WaitCoordinator {
    /// Live-offset seam: the snapshot in step 1 is `offsets.current()`.
    offsets: Arc<OffsetCoordinator>,
    /// Ack registry: quorum counting + the ACK notification channel.
    tracker: Arc<ReplicationTrackerImpl>,
}

impl WaitCoordinator {
    /// Build a coordinator over the offset seam and the ack registry.
    pub fn new(offsets: Arc<OffsetCoordinator>, tracker: Arc<ReplicationTrackerImpl>) -> Self {
        Self { offsets, tracker }
    }

    /// Snapshot the offset a WAIT must see acknowledged: the live write
    /// position at the moment WAIT arrives.
    ///
    /// Divergence from Redis (documented, deliberate): Redis snapshots
    /// `c->woff` — the offset right after *this client's* last write — so a
    /// WAIT is never delayed by other clients' subsequent writes. FrogDB does
    /// not track a per-connection write offset, so the global live offset is
    /// used instead. This is strictly conservative: the target can only be
    /// ≥ the client's own last write, so WAIT never reports a replica as
    /// caught-up when the client's writes have not been acknowledged.
    pub fn target_offset(&self) -> u64 {
        self.offsets.current()
    }

    /// Streaming replicas that have acknowledged at least `target`.
    ///
    /// Derived from [`ReplicationTrackerImpl::get_streaming_replicas`], the
    /// shared acked-offset projection, so WAIT and ROLE can never disagree
    /// about which replicas exist and what they have acknowledged.
    pub fn count_acked(&self, target: u64) -> u32 {
        self.tracker.count_acked(target)
    }

    /// Run the WAIT decision: immediate check → solicit → quorum-or-deadline.
    ///
    /// `deadline = None` blocks until the quorum is reached (Redis `timeout 0`).
    /// The caller races this future against CLIENT UNBLOCK; dropping it is safe
    /// at any point (the quorum wait holds only a broadcast subscription).
    ///
    /// Solicitation policy: one `REPLCONF GETACK *` round, sent only when the
    /// wait actually blocks *and* at least one streaming replica is attached.
    /// This matches Redis, which requests acks once per blocking WAIT and
    /// skips the stream write when no replica would consume it (GETACK is part
    /// of the command stream and advances the offset, so soliciting on a
    /// replica-less primary would grow `master_repl_offset` without writes).
    /// There is no periodic re-solicit during long waits — replicas answer
    /// GETACK immediately and spontaneously ACK every second, which also
    /// bounds the ack latency of replicas that attach mid-wait.
    pub async fn wait_for_replicas(
        &self,
        target: u64,
        num_replicas: u32,
        deadline: Option<Instant>,
        solicitor: &impl AckSolicitor,
    ) -> WaitVerdict {
        // Fast path: quorum already met (covers numreplicas = 0). Redis returns
        // the actual acked count here, which can exceed numreplicas.
        let count = self.count_acked(target);
        if count >= num_replicas {
            return WaitVerdict::Reached(count);
        }

        if self.tracker.replica_count() > 0 {
            solicitor.solicit_acks().await;
        }

        let quorum = self.tracker.wait_for_acks(target, num_replicas);
        match deadline {
            None => WaitVerdict::Reached(quorum.await),
            Some(d) => match tokio::time::timeout_at(d.into(), quorum).await {
                Ok(count) => WaitVerdict::Reached(count),
                Err(_) => WaitVerdict::TimedOut(self.count_acked(target)),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replica_session::Phase;
    use crate::state::ReplicationState;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;
    use tokio::sync::RwLock;

    /// Mock solicitor: records how many GETACK rounds were requested.
    struct MockSolicitor {
        calls: AtomicU32,
    }

    impl MockSolicitor {
        fn new() -> Self {
            Self {
                calls: AtomicU32::new(0),
            }
        }
        fn calls(&self) -> u32 {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl AckSolicitor for MockSolicitor {
        async fn solicit_acks(&self) {
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn coordinator() -> (WaitCoordinator, Arc<ReplicationTrackerImpl>) {
        let tracker = ReplicationTrackerImpl::new_arc();
        let state = Arc::new(RwLock::new(ReplicationState::new()));
        let offsets = Arc::new(OffsetCoordinator::new(tracker.clone(), state));
        (WaitCoordinator::new(offsets, tracker.clone()), tracker)
    }

    fn streaming_replica(tracker: &ReplicationTrackerImpl, port: u16) -> u64 {
        let session = tracker.register_replica(format!("127.0.0.1:{port}").parse().unwrap());
        session.force_phase_for_test(Phase::Streaming);
        session.id()
    }

    #[tokio::test]
    async fn quorum_already_met_returns_without_soliciting() {
        let (coord, tracker) = coordinator();
        let id = streaming_replica(&tracker, 6380);
        tracker.record_ack(id, 100);

        let solicitor = MockSolicitor::new();
        let verdict = coord.wait_for_replicas(100, 1, None, &solicitor).await;
        assert_eq!(verdict, WaitVerdict::Reached(1));
        assert_eq!(solicitor.calls(), 0, "satisfied WAIT must not send GETACK");
    }

    #[tokio::test]
    async fn numreplicas_zero_returns_actual_acked_count() {
        // Redis returns the real acked count on the fast path, which can
        // exceed numreplicas (WAIT 0 ... on a caught-up pair returns 1+).
        let (coord, tracker) = coordinator();
        let id = streaming_replica(&tracker, 6380);
        tracker.record_ack(id, 50);

        let solicitor = MockSolicitor::new();
        let verdict = coord.wait_for_replicas(50, 0, None, &solicitor).await;
        assert_eq!(verdict, WaitVerdict::Reached(1));
        assert_eq!(solicitor.calls(), 0);
    }

    #[tokio::test]
    async fn blocking_wait_solicits_exactly_once_then_reaches_quorum() {
        let (coord, tracker) = coordinator();
        let id = streaming_replica(&tracker, 6380);

        let solicitor = MockSolicitor::new();
        let acker = {
            let tracker = tracker.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                tracker.record_ack(id, 200);
            })
        };

        let deadline = Instant::now() + Duration::from_secs(5);
        let verdict = coord
            .wait_for_replicas(200, 1, Some(deadline), &solicitor)
            .await;
        acker.await.unwrap();

        assert_eq!(verdict, WaitVerdict::Reached(1));
        assert_eq!(
            solicitor.calls(),
            1,
            "a blocking WAIT with a streaming replica attached solicits one GETACK round"
        );
    }

    #[tokio::test]
    async fn timeout_returns_count_acked_at_target() {
        let (coord, tracker) = coordinator();
        let a = streaming_replica(&tracker, 6380);
        let _b = streaming_replica(&tracker, 6381);
        tracker.record_ack(a, 300); // one of two replicas is caught up

        let solicitor = MockSolicitor::new();
        let deadline = Instant::now() + Duration::from_millis(30);
        let verdict = coord
            .wait_for_replicas(300, 2, Some(deadline), &solicitor)
            .await;

        assert_eq!(verdict, WaitVerdict::TimedOut(1));
        assert_eq!(solicitor.calls(), 1);
    }

    #[tokio::test]
    async fn no_streaming_replicas_means_no_solicitation() {
        // GETACK is part of the command stream; a replica-less primary must
        // not advance its offset just because a client ran WAIT.
        let (coord, _tracker) = coordinator();
        let solicitor = MockSolicitor::new();
        let deadline = Instant::now() + Duration::from_millis(20);
        let verdict = coord
            .wait_for_replicas(0, 1, Some(deadline), &solicitor)
            .await;

        // target 0 with no streaming replicas: count is 0, quorum of 1 unmet.
        assert_eq!(verdict, WaitVerdict::TimedOut(0));
        assert_eq!(solicitor.calls(), 0);
    }

    #[tokio::test]
    async fn acks_below_the_target_do_not_count() {
        let (coord, tracker) = coordinator();
        let id = streaming_replica(&tracker, 6380);
        tracker.record_ack(id, 99); // one byte short of the snapshot

        let solicitor = MockSolicitor::new();
        let deadline = Instant::now() + Duration::from_millis(30);
        let verdict = coord
            .wait_for_replicas(100, 1, Some(deadline), &solicitor)
            .await;
        assert_eq!(verdict, WaitVerdict::TimedOut(0));
    }

    #[tokio::test]
    async fn no_deadline_blocks_until_quorum() {
        // deadline = None is Redis `WAIT n 0`: block until the quorum is
        // reached (the caller supplies the CLIENT UNBLOCK escape hatch).
        let (coord, tracker) = coordinator();
        let id = streaming_replica(&tracker, 6380);

        let solicitor = MockSolicitor::new();
        let acker = {
            let tracker = tracker.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                tracker.record_ack(id, 400);
            })
        };

        let verdict = coord.wait_for_replicas(400, 1, None, &solicitor).await;
        acker.await.unwrap();
        assert_eq!(verdict, WaitVerdict::Reached(1));
    }

    #[tokio::test]
    async fn target_offset_reads_the_offset_coordinator() {
        let (coord, _tracker) = coordinator();
        assert_eq!(coord.target_offset(), 0);
        coord.offsets.advance(&Bytes::from(vec![b'x'; 123]));
        assert_eq!(coord.target_offset(), 123);
    }
}
