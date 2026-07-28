//! Quorum checker for replication mode.
//!
//! Monitors replica ACK freshness via the ReplicationTrackerImpl. When no
//! replica has ACKed recently, `has_quorum()` returns false, causing guards.rs
//! to reject writes with CLUSTERDOWN — fencing the primary during partitions.

use frogdb_core::ReplicationTrackerImpl;
use frogdb_core::command::QuorumChecker;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

/// Quorum checker for replication mode (primary + N replicas).
///
/// Arms itself on the first streaming replica. Once armed, requires at least
/// one streaming replica with a recent ACK to allow writes.
///
/// Both operator knobs are live: the fence toggle and the freshness window are
/// atomics read at decision time, not captured into the shape of the object. A
/// primary therefore always *has* a checker; `self-fence-on-replica-loss = no`
/// simply makes every decision permissive, so `CONFIG SET
/// replication-self-fence-on-replica-loss yes` starts fencing without a restart
/// (the checker is already installed on every shard worker and connection).
pub struct ReplicationQuorumChecker {
    tracker: Arc<ReplicationTrackerImpl>,
    /// Live `self-fence-on-replica-loss`. When false, [`has_quorum`] is
    /// unconditionally true — the pre-seam behaviour of having no checker at all.
    ///
    /// [`has_quorum`]: QuorumChecker::has_quorum
    self_fence_enabled: AtomicBool,
    /// Live `replica-freshness-timeout-ms`, in milliseconds.
    freshness_timeout_ms: AtomicU64,
    /// Once a replica reaches Streaming, this flips to true and stays true.
    armed: AtomicBool,
}

impl ReplicationQuorumChecker {
    pub fn new(
        tracker: Arc<ReplicationTrackerImpl>,
        self_fence_enabled: bool,
        freshness_timeout: Duration,
    ) -> Self {
        Self {
            tracker,
            self_fence_enabled: AtomicBool::new(self_fence_enabled),
            freshness_timeout_ms: AtomicU64::new(freshness_timeout.as_millis() as u64),
            armed: AtomicBool::new(false),
        }
    }

    /// Whether self-fencing is currently armed by configuration.
    pub fn self_fence_enabled(&self) -> bool {
        self.self_fence_enabled.load(Ordering::Relaxed)
    }

    /// Enable/disable self-fencing at the next fence decision. Reachable from
    /// `ConfigManager` for `CONFIG SET replication-self-fence-on-replica-loss`.
    pub fn set_self_fence_enabled(&self, enabled: bool) {
        self.self_fence_enabled.store(enabled, Ordering::Relaxed);
    }

    /// The current ACK-freshness window.
    pub fn freshness_timeout(&self) -> Duration {
        Duration::from_millis(self.freshness_timeout_ms.load(Ordering::Relaxed))
    }

    /// Retune the ACK-freshness window. Reachable from `ConfigManager` for
    /// `CONFIG SET replication-replica-freshness-timeout-ms`; the next freshness
    /// check uses the new value.
    pub fn set_freshness_timeout_ms(&self, ms: u64) {
        self.freshness_timeout_ms.store(ms, Ordering::Relaxed);
    }

    /// Count streaming replicas whose last ACK is within the freshness timeout.
    /// The window is loaded here, per check, not captured at construction.
    fn count_fresh_streaming_replicas(&self) -> usize {
        let freshness_timeout = self.freshness_timeout();
        self.tracker
            .get_streaming_replicas()
            .iter()
            .filter(|r| r.last_ack_time.elapsed() < freshness_timeout)
            .count()
    }
}

impl QuorumChecker for ReplicationQuorumChecker {
    fn has_quorum(&self) -> bool {
        // Check if any replica is streaming (and arm if so). Arming is tracked
        // even while fencing is disabled, so enabling the toggle on a primary
        // that has already served replicas fences immediately rather than
        // granting a fresh grace period.
        let streaming = self.tracker.get_streaming_replicas();
        if !streaming.is_empty() && !self.armed.load(Ordering::Relaxed) {
            self.armed.store(true, Ordering::Relaxed);
        }

        // Fence decision point: read the live toggle. Disabled = never fence.
        if !self.self_fence_enabled() {
            return true;
        }

        // Before any replica has ever streamed, allow all writes
        if !self.armed.load(Ordering::Relaxed) {
            return true;
        }

        // Armed: require at least 1 fresh streaming replica
        self.count_fresh_streaming_replicas() >= 1
    }

    fn count_reachable_nodes(&self) -> usize {
        // 1 (self) + fresh streaming replicas
        1 + self.count_fresh_streaming_replicas()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_core::ReplicationTracker;
    use frogdb_replication::Phase;
    use std::net::SocketAddr;
    use std::time::Duration;

    fn make_tracker() -> Arc<ReplicationTrackerImpl> {
        Arc::new(ReplicationTrackerImpl::new())
    }

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    #[test]
    fn unarmed_allows_writes() {
        let tracker = make_tracker();
        let checker = ReplicationQuorumChecker::new(tracker, true, Duration::from_secs(3));
        assert!(checker.has_quorum());
        assert_eq!(checker.count_reachable_nodes(), 1);
    }

    #[test]
    fn armed_with_fresh_replica_allows_writes() {
        let tracker = make_tracker();
        let session = tracker.register_replica(addr(9001));
        session.force_phase_for_test(Phase::Streaming);
        tracker.record_ack(session.id(), 100);

        let checker = ReplicationQuorumChecker::new(tracker, true, Duration::from_secs(3));
        assert!(checker.has_quorum());
        assert_eq!(checker.count_reachable_nodes(), 2);
    }

    #[test]
    fn armed_with_stale_replica_rejects_writes() {
        let tracker = make_tracker();
        let session = tracker.register_replica(addr(9001));
        session.force_phase_for_test(Phase::Streaming);
        tracker.record_ack(session.id(), 100);

        // Use a tiny freshness timeout so the replica is immediately stale
        let checker = ReplicationQuorumChecker::new(tracker, true, Duration::from_nanos(1));
        // First call arms the checker (streaming replica exists)
        // But the replica is already stale
        std::thread::sleep(Duration::from_millis(1));
        assert!(!checker.has_quorum());
        assert_eq!(checker.count_reachable_nodes(), 1);
    }

    #[test]
    fn armed_with_no_replicas_rejects_writes() {
        let tracker = make_tracker();
        let session = tracker.register_replica(addr(9001));
        session.force_phase_for_test(Phase::Streaming);
        tracker.record_ack(session.id(), 100);

        let checker = ReplicationQuorumChecker::new(tracker.clone(), true, Duration::from_secs(3));
        // Arm the checker
        assert!(checker.has_quorum());

        // Remove the replica
        tracker.unregister_replica(session.id());
        assert!(!checker.has_quorum());
        assert_eq!(checker.count_reachable_nodes(), 1);
    }

    #[test]
    fn arming_transition() {
        let tracker = make_tracker();
        let checker = ReplicationQuorumChecker::new(tracker.clone(), true, Duration::from_secs(3));

        // Not armed yet — quorum is true
        assert!(checker.has_quorum());
        assert!(!checker.armed.load(Ordering::Relaxed));

        // Register a replica in Connecting state — still not armed
        let session = tracker.register_replica(addr(9001));
        assert!(checker.has_quorum());
        assert!(!checker.armed.load(Ordering::Relaxed));

        // Move to Streaming — now it arms
        session.force_phase_for_test(Phase::Streaming);
        tracker.record_ack(session.id(), 0);
        assert!(checker.has_quorum());
        assert!(checker.armed.load(Ordering::Relaxed));

        // Remove replica — armed stays true, quorum lost
        tracker.unregister_replica(session.id());
        assert!(!checker.has_quorum());
        assert!(checker.armed.load(Ordering::Relaxed));
    }

    /// Propagation truth for `replication-self-fence-on-replica-loss`: the
    /// toggle is read at the fence decision, so flipping it on a *live* checker
    /// changes the very next `has_quorum()` — no restart, no re-construction.
    #[test]
    fn self_fence_toggle_is_live() {
        let tracker = make_tracker();
        let session = tracker.register_replica(addr(9001));
        session.force_phase_for_test(Phase::Streaming);
        tracker.record_ack(session.id(), 100);

        // Booted with fencing OFF: a lost replica must not fence writes. This is
        // also the shape a primary boots in today when the operator disables the
        // knob — a checker that is present but permissive.
        let checker = ReplicationQuorumChecker::new(tracker.clone(), false, Duration::from_secs(3));
        assert!(checker.has_quorum());
        tracker.unregister_replica(session.id());
        assert!(
            checker.has_quorum(),
            "fencing disabled: replica loss must not reject writes"
        );
        // Arming still happened while disabled, so enabling does not hand out a
        // fresh grace period.
        assert!(checker.armed.load(Ordering::Relaxed));

        // CONFIG SET replication-self-fence-on-replica-loss yes.
        checker.set_self_fence_enabled(true);
        assert!(checker.self_fence_enabled());
        assert!(
            !checker.has_quorum(),
            "enabling the fence must reject writes on the same checker instance"
        );

        // ...and back off again, same instance.
        checker.set_self_fence_enabled(false);
        assert!(checker.has_quorum());
    }

    /// Propagation truth for `replica-freshness-timeout-ms`: the window is
    /// loaded per freshness check, so shrinking it makes an already-registered
    /// replica stale (and widening it makes it fresh again) without a restart.
    #[test]
    fn freshness_timeout_is_live() {
        let tracker = make_tracker();
        let session = tracker.register_replica(addr(9001));
        session.force_phase_for_test(Phase::Streaming);
        tracker.record_ack(session.id(), 100);

        let checker = ReplicationQuorumChecker::new(tracker, true, Duration::from_secs(3600));
        assert!(checker.has_quorum());
        assert_eq!(checker.count_reachable_nodes(), 2);
        assert_eq!(checker.freshness_timeout(), Duration::from_secs(3600));

        // Let a measurable amount of ACK age accumulate, then CONFIG SET the
        // window below it.
        std::thread::sleep(Duration::from_millis(5));
        checker.set_freshness_timeout_ms(1);
        assert_eq!(checker.freshness_timeout(), Duration::from_millis(1));
        assert!(
            !checker.has_quorum(),
            "a shrunk freshness window must stale the replica on the next check"
        );
        assert_eq!(checker.count_reachable_nodes(), 1);

        // Widening it again restores quorum on the same instance.
        checker.set_freshness_timeout_ms(3_600_000);
        assert!(checker.has_quorum());
        assert_eq!(checker.count_reachable_nodes(), 2);
    }
}
