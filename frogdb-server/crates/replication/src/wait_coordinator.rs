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

/// The WAIT deadline is a *timer* instant, not a wall-clock one: it is handed
/// straight to `tokio::time::timeout_at`. Using `std::time::Instant` here would
/// silently mis-fire whenever the tokio clock is not the real one — under
/// `tokio::time::pause()` and under turmoil the simulated clock runs ahead of
/// real time, so a std-derived deadline converts to an already-elapsed timer and
/// WAIT returns instantly instead of blocking.
use tokio::time::Instant;

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

/// How a WAIT ended. Every arm carries the acked count, because WAIT's reply is
/// the count *regardless* of whether the quorum was reached — the verdict only
/// distinguishes the paths for observability and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitVerdict {
    /// The quorum was reached (count ≥ numreplicas).
    Reached(u32),
    /// The deadline elapsed first; count is what had acked by then.
    TimedOut(u32),
    /// The node stopped being a primary while the wait was parked. The count is
    /// what had acked at that moment, but it describes a history this node no
    /// longer heads, so the caller must report an error rather than a number.
    RoleChanged(u32),
}

impl WaitVerdict {
    /// The number of replicas that acknowledged the target offset — the WAIT
    /// reply value in every outcome where a number is a truthful answer.
    pub fn count(&self) -> u32 {
        match self {
            WaitVerdict::Reached(n) | WaitVerdict::TimedOut(n) | WaitVerdict::RoleChanged(n) => *n,
        }
    }
}

/// A subscription to this node's role-change fence, taken *before* the caller
/// decides that the node is still a primary.
///
/// Ordering is the whole point. A demotion publishes the data-path replica flag
/// first and bumps the fence second (`RoleManager::demote` →
/// [`PrimaryReplicationHandler::end_primary_stint`]), and a `watch` receiver
/// only observes bumps that happen after it was created. A caller that checked
/// the role and *then* subscribed would miss a demotion landing in between and
/// park forever on a node that has already rejected every later WAIT. Taking
/// the fence first and re-reading the role afterwards closes that window: one
/// of the two observations must see the change.
pub struct RoleFence(tokio::sync::watch::Receiver<()>);

impl RoleFence {
    /// Resolve once this node's role changed since the fence was taken.
    ///
    /// A dropped sender resolves too: the coordinator that owned the fence is
    /// gone, so the stream this wait was parked on is gone with it.
    async fn changed(&mut self) {
        let _ = self.0.changed().await;
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
    /// Role-change fence: bumped when this node stops being a primary, which
    /// releases every parked wait. The payload is `()` because the signal *is*
    /// the bump: `watch` versions every send, and a subscriber only ever needs
    /// "did it change since I started" — which stays true across a node that is
    /// demoted, promoted and demoted again while one WAIT is parked. A counter
    /// here would be write-only state (nothing reads it, and `send_modify`
    /// notifies whether or not the value moved).
    role_fence: tokio::sync::watch::Sender<()>,
}

impl WaitCoordinator {
    /// Build a coordinator over the offset seam and the ack registry.
    pub fn new(offsets: Arc<OffsetCoordinator>, tracker: Arc<ReplicationTrackerImpl>) -> Self {
        Self {
            offsets,
            tracker,
            role_fence: tokio::sync::watch::Sender::new(()),
        }
    }

    /// Release every parked WAIT because this node is no longer a primary.
    ///
    /// Redis does the same from `replicationSetMaster` (via
    /// `disconnectAllBlockedClients`): a client blocked for replica
    /// acknowledgments on a node that just became a replica is waiting on a
    /// stream that no longer exists, and the honest reply is an error, not the
    /// count it happened to reach. Called from
    /// [`PrimaryReplicationHandler::end_primary_stint`], next to the downstream
    /// disconnect it belongs with.
    pub fn fence_role_change(&self) {
        // `send_modify` notifies unconditionally — the bump, not the payload,
        // is the signal (see [`Self::role_fence`]).
        self.role_fence.send_modify(|()| {});
    }

    /// Subscribe to the role-change fence.
    ///
    /// Take this *before* checking whether this node is still a primary and
    /// hand it to [`Self::wait_for_replicas`]; see [`RoleFence`] for why the
    /// order is load-bearing.
    pub fn role_fence(&self) -> RoleFence {
        RoleFence(self.role_fence.subscribe())
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
    ///
    /// `fence` is the caller's [`RoleFence`], taken before it decided this node
    /// was a primary — a demotion that landed anywhere from that decision
    /// onwards releases the wait as [`WaitVerdict::RoleChanged`].
    pub async fn wait_for_replicas(
        &self,
        mut fence: RoleFence,
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
        tokio::pin!(quorum);

        match deadline {
            None => tokio::select! {
                count = &mut quorum => WaitVerdict::Reached(count),
                _ = fence.changed() => WaitVerdict::RoleChanged(self.count_acked(target)),
            },
            Some(d) => tokio::select! {
                res = tokio::time::timeout_at(d, &mut quorum) => match res {
                    Ok(count) => WaitVerdict::Reached(count),
                    Err(_) => WaitVerdict::TimedOut(self.count_acked(target)),
                },
                _ = fence.changed() => WaitVerdict::RoleChanged(self.count_acked(target)),
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
        let identity =
            crate::identity::ReplicationIdentity::adopting(ReplicationState::new(), &tracker);
        let offsets = Arc::new(OffsetCoordinator::new(tracker.clone(), &identity));
        (WaitCoordinator::new(offsets, tracker.clone()), tracker)
    }

    fn streaming_replica(tracker: &ReplicationTrackerImpl, port: u16) -> u64 {
        let session = tracker.register_replica(format!("127.0.0.1:{port}").parse().unwrap());
        session.force_phase_for_test(Phase::Streaming);
        session.id()
    }

    // FM-REPLICATION-038
    // FM-REPLICATION-037
    #[tokio::test]
    async fn quorum_already_met_returns_without_soliciting() {
        let (coord, tracker) = coordinator();
        let id = streaming_replica(&tracker, 6380);
        tracker.record_ack(id, 100);

        let solicitor = MockSolicitor::new();
        let verdict = coord
            .wait_for_replicas(coord.role_fence(), 100, 1, None, &solicitor)
            .await;
        assert_eq!(verdict, WaitVerdict::Reached(1));
        assert_eq!(solicitor.calls(), 0, "satisfied WAIT must not send GETACK");
    }

    // FM-REPLICATION-037
    #[tokio::test]
    async fn numreplicas_zero_returns_actual_acked_count() {
        // Redis returns the real acked count on the fast path, which can
        // exceed numreplicas (WAIT 0 ... on a caught-up pair returns 1+).
        let (coord, tracker) = coordinator();
        let id = streaming_replica(&tracker, 6380);
        tracker.record_ack(id, 50);

        let solicitor = MockSolicitor::new();
        let verdict = coord
            .wait_for_replicas(coord.role_fence(), 50, 0, None, &solicitor)
            .await;
        assert_eq!(verdict, WaitVerdict::Reached(1));
        assert_eq!(solicitor.calls(), 0);
    }

    // FM-REPLICATION-038
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
            .wait_for_replicas(coord.role_fence(), 200, 1, Some(deadline), &solicitor)
            .await;
        acker.await.unwrap();

        assert_eq!(verdict, WaitVerdict::Reached(1));
        assert_eq!(
            solicitor.calls(),
            1,
            "a blocking WAIT with a streaming replica attached solicits one GETACK round"
        );
    }

    // FM-REPLICATION-037
    #[tokio::test]
    async fn timeout_returns_count_acked_at_target() {
        let (coord, tracker) = coordinator();
        let a = streaming_replica(&tracker, 6380);
        let _b = streaming_replica(&tracker, 6381);
        tracker.record_ack(a, 300); // one of two replicas is caught up

        let solicitor = MockSolicitor::new();
        let deadline = Instant::now() + Duration::from_millis(30);
        let verdict = coord
            .wait_for_replicas(coord.role_fence(), 300, 2, Some(deadline), &solicitor)
            .await;

        assert_eq!(verdict, WaitVerdict::TimedOut(1));
        assert_eq!(solicitor.calls(), 1);
    }

    // FM-REPLICATION-038
    #[tokio::test]
    async fn no_streaming_replicas_means_no_solicitation() {
        // GETACK is part of the command stream; a replica-less primary must
        // not advance its offset just because a client ran WAIT.
        let (coord, _tracker) = coordinator();
        let solicitor = MockSolicitor::new();
        let deadline = Instant::now() + Duration::from_millis(20);
        let verdict = coord
            .wait_for_replicas(coord.role_fence(), 0, 1, Some(deadline), &solicitor)
            .await;

        // target 0 with no streaming replicas: count is 0, quorum of 1 unmet.
        assert_eq!(verdict, WaitVerdict::TimedOut(0));
        assert_eq!(solicitor.calls(), 0);
    }

    // FM-REPLICATION-039
    #[tokio::test]
    async fn acks_below_the_target_do_not_count() {
        let (coord, tracker) = coordinator();
        let id = streaming_replica(&tracker, 6380);
        tracker.record_ack(id, 99); // one byte short of the snapshot

        let solicitor = MockSolicitor::new();
        let deadline = Instant::now() + Duration::from_millis(30);
        let verdict = coord
            .wait_for_replicas(coord.role_fence(), 100, 1, Some(deadline), &solicitor)
            .await;
        assert_eq!(verdict, WaitVerdict::TimedOut(0));
    }

    // FM-REPLICATION-037
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

        let verdict = coord
            .wait_for_replicas(coord.role_fence(), 400, 1, None, &solicitor)
            .await;
        acker.await.unwrap();
        assert_eq!(verdict, WaitVerdict::Reached(1));
    }

    // FM-REPLICATION-040
    #[tokio::test]
    async fn role_change_releases_a_wait_parked_forever() {
        // `WAIT n 0` has no deadline, so the fence is the only thing that can
        // release it short of the quorum.
        let (coord, tracker) = coordinator();
        let _id = streaming_replica(&tracker, 6381);
        let coord = Arc::new(coord);

        let fencer = {
            let coord = coord.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                coord.fence_role_change();
            })
        };

        let solicitor = MockSolicitor::new();
        let verdict = coord
            .wait_for_replicas(coord.role_fence(), 400, 1, None, &solicitor)
            .await;
        fencer.await.unwrap();
        assert_eq!(verdict, WaitVerdict::RoleChanged(0));
    }

    // FM-REPLICATION-040
    #[tokio::test]
    async fn role_change_releases_a_wait_with_a_deadline() {
        let (coord, tracker) = coordinator();
        let _id = streaming_replica(&tracker, 6382);
        let coord = Arc::new(coord);

        let fencer = {
            let coord = coord.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                coord.fence_role_change();
            })
        };

        let solicitor = MockSolicitor::new();
        let started = Instant::now();
        let verdict = coord
            .wait_for_replicas(
                coord.role_fence(),
                400,
                1,
                Some(Instant::now() + Duration::from_secs(30)),
                &solicitor,
            )
            .await;
        fencer.await.unwrap();
        assert_eq!(verdict, WaitVerdict::RoleChanged(0));
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the fence must win the race against a far-off deadline"
        );
    }

    // FM-REPLICATION-040
    #[tokio::test]
    async fn a_fence_from_an_earlier_stint_does_not_release_a_later_wait() {
        // The subscription is taken when the caller enters WAIT, so demotions
        // that happened before it must not count — otherwise every WAIT after
        // the first demotion would fail instantly.
        let (coord, tracker) = coordinator();
        let id = streaming_replica(&tracker, 6383);
        coord.fence_role_change();
        coord.fence_role_change();

        let solicitor = MockSolicitor::new();
        let acker = {
            let tracker = tracker.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                tracker.record_ack(id, 400);
            })
        };

        let verdict = coord
            .wait_for_replicas(coord.role_fence(), 400, 1, None, &solicitor)
            .await;
        acker.await.unwrap();
        assert_eq!(verdict, WaitVerdict::Reached(1));
    }

    // FM-REPLICATION-040
    #[tokio::test]
    async fn a_demotion_racing_the_role_check_still_releases_the_wait() {
        // The interleaving the fence-first ordering exists for: the caller has
        // read "I am a primary", the demotion lands (replica flag, then fence
        // bump), and only then does the wait park. Because the caller took the
        // fence before its role check, the bump is already pending on this
        // receiver and the wait is released instead of parking forever on a
        // node that now rejects every WAIT. Deterministic: the bump happens
        // before `wait_for_replicas` is even called, with no timing involved.
        let (coord, tracker) = coordinator();
        let _id = streaming_replica(&tracker, 6387);

        let fence = coord.role_fence();
        coord.fence_role_change();

        let solicitor = MockSolicitor::new();
        let verdict = coord
            .wait_for_replicas(fence, 400, 1, None, &solicitor)
            .await;
        assert_eq!(verdict, WaitVerdict::RoleChanged(0));
    }

    // FM-REPLICATION-039
    #[tokio::test]
    async fn a_replica_that_attaches_mid_wait_can_satisfy_the_quorum() {
        // Membership is read at count time, not snapshotted when the wait
        // starts, so a replica that finishes its handshake while a WAIT is
        // parked counts towards it. This is what makes the Redis-parity
        // "unreachable numreplicas blocks to the deadline" rule correct rather
        // than merely slow: the replica may still be mid-attach.
        let (coord, tracker) = coordinator();
        let coord = Arc::new(coord);

        let attacher = {
            let tracker = tracker.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let id = streaming_replica(&tracker, 6384);
                tracker.record_ack(id, 500);
            })
        };

        // No streaming replica exists yet, so there is nothing to solicit from.
        let solicitor = MockSolicitor::new();
        let verdict = coord
            .wait_for_replicas(
                coord.role_fence(),
                500,
                1,
                Some(Instant::now() + Duration::from_secs(5)),
                &solicitor,
            )
            .await;
        attacher.await.unwrap();

        assert_eq!(verdict, WaitVerdict::Reached(1));
        assert_eq!(
            solicitor.calls(),
            0,
            "no replica was attached when the wait blocked, so no GETACK round"
        );
    }

    // FM-REPLICATION-039
    #[tokio::test]
    async fn a_replica_that_detaches_mid_wait_stops_counting() {
        // The acked count is a projection over live sessions: a replica that
        // drops its link before the quorum is reached takes its ack with it,
        // and the deadline reports the reduced count rather than a stale one.
        let (coord, tracker) = coordinator();
        let a = streaming_replica(&tracker, 6385);
        let _b = streaming_replica(&tracker, 6386);
        tracker.record_ack(a, 600);
        assert_eq!(coord.count_acked(600), 1);

        let detacher = {
            let tracker = tracker.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                tracker.unregister_replica(a);
            })
        };

        let solicitor = MockSolicitor::new();
        let verdict = coord
            .wait_for_replicas(
                coord.role_fence(),
                600,
                2,
                Some(Instant::now() + Duration::from_millis(200)),
                &solicitor,
            )
            .await;
        detacher.await.unwrap();

        assert_eq!(
            verdict,
            WaitVerdict::TimedOut(0),
            "the detached replica's ack must not survive it"
        );
    }

    // FM-REPLICATION-037
    #[tokio::test]
    async fn target_offset_reads_the_offset_coordinator() {
        let (coord, _tracker) = coordinator();
        assert_eq!(coord.target_offset(), 0);
        coord.offsets.advance(&Bytes::from(vec![b'x'; 123]));
        assert_eq!(coord.target_offset(), 123);
    }

    /// WAIT replies with the acked count in every outcome, so each verdict has
    /// to carry its *own* number through: a timed-out WAIT that reports the
    /// quorum it never reached (or a constant) is a wrong answer to the client,
    /// not a cosmetic one.
    #[test]
    fn every_verdict_reports_the_count_it_carries() {
        assert_eq!(WaitVerdict::Reached(7).count(), 7);
        assert_eq!(WaitVerdict::TimedOut(3).count(), 3);
        assert_eq!(WaitVerdict::RoleChanged(5).count(), 5);
        assert_eq!(WaitVerdict::TimedOut(0).count(), 0);
    }

    /// The fence releases *every* wait parked before it, and a second stint
    /// releases the waits parked after the first one — the property the
    /// `watch`-versioned fence exists for, checked with two concurrent waits so
    /// a fence that only ever woke a single receiver would show up.
    #[tokio::test]
    async fn one_fence_releases_every_wait_parked_before_it() {
        let (coord, tracker) = coordinator();
        let _id = streaming_replica(&tracker, 6388);
        let coord = Arc::new(coord);

        let waits: Vec<_> = (0..2)
            .map(|_| {
                let coord = coord.clone();
                let fence = coord.role_fence();
                tokio::spawn(async move {
                    coord
                        .wait_for_replicas(fence, 400, 1, None, &MockSolicitor::new())
                        .await
                })
            })
            .collect();

        tokio::time::sleep(Duration::from_millis(20)).await;
        coord.fence_role_change();
        for wait in waits {
            assert_eq!(wait.await.unwrap(), WaitVerdict::RoleChanged(0));
        }

        // A second demotion, later in this node's life, must release a wait
        // that parked after the first one just as well.
        let fence = coord.role_fence();
        let second = {
            let coord = coord.clone();
            tokio::spawn(async move {
                coord
                    .wait_for_replicas(fence, 400, 1, None, &MockSolicitor::new())
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        coord.fence_role_change();
        assert_eq!(second.await.unwrap(), WaitVerdict::RoleChanged(0));
    }
}
