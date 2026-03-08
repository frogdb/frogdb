//! Quorum checker for replication mode.
//!
//! Monitors replica ACK freshness via the ReplicationTrackerImpl. When no
//! replica has ACKed recently, `has_quorum()` returns false, causing guards.rs
//! to reject writes with CLUSTERDOWN — fencing the primary during partitions.

use frogdb_core::ReplicationTrackerImpl;
use frogdb_core::command::QuorumChecker;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// Quorum checker for replication mode (primary + N replicas).
///
/// Arms itself on the first streaming replica. Once armed, requires at least
/// one streaming replica with a recent ACK to allow writes.
pub struct ReplicationQuorumChecker {
    tracker: Arc<ReplicationTrackerImpl>,
    freshness_timeout: Duration,
    /// Once a replica reaches Streaming, this flips to true and stays true.
    armed: AtomicBool,
}

impl ReplicationQuorumChecker {
    pub fn new(tracker: Arc<ReplicationTrackerImpl>, freshness_timeout: Duration) -> Self {
        Self {
            tracker,
            freshness_timeout,
            armed: AtomicBool::new(false),
        }
    }

    /// Count streaming replicas whose last ACK is within the freshness timeout.
    fn count_fresh_streaming_replicas(&self) -> usize {
        self.tracker
            .get_streaming_replicas()
            .iter()
            .filter(|r| r.last_ack_time.elapsed() < self.freshness_timeout)
            .count()
    }
}

impl QuorumChecker for ReplicationQuorumChecker {
    fn has_quorum(&self) -> bool {
        // Check if any replica is streaming (and arm if so)
        let streaming = self.tracker.get_streaming_replicas();
        if !streaming.is_empty() && !self.armed.load(Ordering::Relaxed) {
            self.armed.store(true, Ordering::Relaxed);
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
    use frogdb_replication::tracker::ReplicaState;
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
        let checker = ReplicationQuorumChecker::new(tracker, Duration::from_secs(3));
        assert!(checker.has_quorum());
        assert_eq!(checker.count_reachable_nodes(), 1);
    }

    #[test]
    fn armed_with_fresh_replica_allows_writes() {
        let tracker = make_tracker();
        let rid = tracker.register_replica(addr(9001));
        tracker.set_state(rid, ReplicaState::Streaming);
        tracker.record_ack(rid, 100);

        let checker = ReplicationQuorumChecker::new(tracker, Duration::from_secs(3));
        assert!(checker.has_quorum());
        assert_eq!(checker.count_reachable_nodes(), 2);
    }

    #[test]
    fn armed_with_stale_replica_rejects_writes() {
        let tracker = make_tracker();
        let rid = tracker.register_replica(addr(9001));
        tracker.set_state(rid, ReplicaState::Streaming);
        tracker.record_ack(rid, 100);

        // Use a tiny freshness timeout so the replica is immediately stale
        let checker = ReplicationQuorumChecker::new(tracker, Duration::from_nanos(1));
        // First call arms the checker (streaming replica exists)
        // But the replica is already stale
        std::thread::sleep(Duration::from_millis(1));
        assert!(!checker.has_quorum());
        assert_eq!(checker.count_reachable_nodes(), 1);
    }

    #[test]
    fn armed_with_no_replicas_rejects_writes() {
        let tracker = make_tracker();
        let rid = tracker.register_replica(addr(9001));
        tracker.set_state(rid, ReplicaState::Streaming);
        tracker.record_ack(rid, 100);

        let checker = ReplicationQuorumChecker::new(tracker.clone(), Duration::from_secs(3));
        // Arm the checker
        assert!(checker.has_quorum());

        // Remove the replica
        tracker.unregister_replica(rid);
        assert!(!checker.has_quorum());
        assert_eq!(checker.count_reachable_nodes(), 1);
    }

    #[test]
    fn arming_transition() {
        let tracker = make_tracker();
        let checker = ReplicationQuorumChecker::new(tracker.clone(), Duration::from_secs(3));

        // Not armed yet — quorum is true
        assert!(checker.has_quorum());
        assert!(!checker.armed.load(Ordering::Relaxed));

        // Register a replica in Connecting state — still not armed
        let rid = tracker.register_replica(addr(9001));
        assert!(checker.has_quorum());
        assert!(!checker.armed.load(Ordering::Relaxed));

        // Move to Streaming — now it arms
        tracker.set_state(rid, ReplicaState::Streaming);
        tracker.record_ack(rid, 0);
        assert!(checker.has_quorum());
        assert!(checker.armed.load(Ordering::Relaxed));

        // Remove replica — armed stays true, quorum lost
        tracker.unregister_replica(rid);
        assert!(!checker.has_quorum());
        assert!(checker.armed.load(Ordering::Relaxed));
    }
}
