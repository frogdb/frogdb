//! Replication tracker: registry of per-replica sessions and cross-replica state.
//!
//! The tracker holds the session map plus the cross-replica fields (current
//! replication offset, ACK notification channel, lag-disconnect cooldowns) and
//! implements the [`frogdb_types::ReplicationTracker`] trait so that consumers
//! (WAIT, INFO, cluster bus) can read tracker state without knowing about
//! per-replica sessions directly.
//!
//! Per-replica state lives on [`crate::replica_session::ReplicaSession`].

use frogdb_types::ReplicationTracker;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::broadcast;

use crate::replica_session::{Phase, ReplicaInfo, ReplicaSession};

/// Registry of replica sessions and cross-replica replication state.
pub struct ReplicationTrackerImpl {
    /// Per-replica sessions keyed by id.
    replicas: RwLock<HashMap<u64, Arc<ReplicaSession>>>,

    /// Next replica id to allocate.
    next_replica_id: AtomicU64,

    /// Current replication offset (primary's write position). A *borrowed*
    /// clone of the atomic owned by [`crate::offset_coordinator::OffsetCoordinator`]
    /// (its canonical home, and the sole vendor of the cluster-bus handle). The
    /// tracker keeps this handle only for its INFO/ROLE read + lag accessors;
    /// the coordinator, not the tracker, advances it and hands it to the bus.
    current_offset: Arc<AtomicU64>,

    /// Channel for notifying WAIT waiters about new ACKs.
    ack_notify: broadcast::Sender<(u64, u64)>, // (replica_id, offset)

    /// Timestamps of proactive lag disconnects, keyed by socket address.
    /// Address-based (not replica_id) because replica IDs change on reconnect.
    lag_disconnect_times: RwLock<HashMap<SocketAddr, Instant>>,
}

impl Default for ReplicationTrackerImpl {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicationTrackerImpl {
    pub fn new() -> Self {
        let (ack_notify, _) = broadcast::channel(1024);
        Self {
            replicas: RwLock::new(HashMap::new()),
            next_replica_id: AtomicU64::new(1),
            current_offset: Arc::new(AtomicU64::new(0)),
            ack_notify,
            lag_disconnect_times: RwLock::new(HashMap::new()),
        }
    }

    pub fn new_arc() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Hand the offset atomic to the [`crate::offset_coordinator::OffsetCoordinator`]
    /// so it can adopt it as its canonical `live` handle at construction.
    ///
    /// This is the ONLY place the tracker vends its offset atomic: the public
    /// cluster-bus handle is vended by the coordinator
    /// ([`crate::offset_coordinator::OffsetCoordinator::shared_offset`]), the
    /// single owner, not here.
    pub(crate) fn offset_handle(&self) -> Arc<AtomicU64> {
        self.current_offset.clone()
    }

    /// Register a new replica connection and return the owning session handle.
    ///
    /// The caller drives the session via [`ReplicaSession::run`]; the tracker
    /// keeps an Arc so consumers can query it via [`Self::get_streaming_replicas`]
    /// and friends until [`Self::unregister_replica`] is called.
    pub fn register_replica(&self, address: SocketAddr) -> Arc<ReplicaSession> {
        let id = self.next_replica_id.fetch_add(1, Ordering::Relaxed);
        let session = ReplicaSession::new(id, address);
        self.replicas.write().insert(id, session.clone());
        tracing::info!(
            replica_id = id,
            address = %address,
            "Registered new replica"
        );
        session
    }

    /// Drop the replica's session from the registry.
    pub fn unregister_replica(&self, replica_id: u64) {
        if let Some(session) = self.replicas.write().remove(&replica_id) {
            tracing::info!(
                replica_id = replica_id,
                address = %session.address(),
                "Unregistered replica"
            );
        }
    }

    /// Look up the session for a given replica id, if it's still registered.
    pub fn get_session(&self, replica_id: u64) -> Option<Arc<ReplicaSession>> {
        self.replicas.read().get(&replica_id).cloned()
    }

    /// Snapshot of a single replica.
    pub fn get_replica(&self, replica_id: u64) -> Option<ReplicaInfo> {
        self.replicas.read().get(&replica_id).map(|s| s.snapshot())
    }

    /// Snapshots of all registered replicas (any phase).
    pub fn get_all_replicas(&self) -> Vec<ReplicaInfo> {
        self.replicas
            .read()
            .values()
            .map(|s| s.snapshot())
            .collect()
    }

    /// Snapshots of replicas currently in the live-streaming phase.
    ///
    /// This is THE acked-offset projection: WAIT's quorum count
    /// ([`Self::count_acked`]), [`Self::min_acked_offset`], and ROLE's replica
    /// listing all read this one accessor, so "which replicas count and what
    /// have they acknowledged" has a single definition.
    pub fn get_streaming_replicas(&self) -> Vec<ReplicaInfo> {
        self.replicas
            .read()
            .values()
            .filter(|s| matches!(s.phase(), Phase::Streaming))
            .map(|s| s.snapshot())
            .collect()
    }

    /// Count streaming replicas whose last ACK is within `max_lag` — the "good"
    /// replicas for Redis's `min-replicas-to-write` gate. A `max_lag` of zero
    /// disables the freshness filter (every streaming replica counts), matching
    /// Redis's `min-replicas-max-lag 0` semantics.
    pub fn count_good_replicas(&self, max_lag: Duration) -> u32 {
        self.get_streaming_replicas()
            .iter()
            .filter(|r| max_lag.is_zero() || r.last_ack_time.elapsed() < max_lag)
            .count() as u32
    }

    /// Set the current replication offset.
    ///
    /// Used at wiring time to seed the offset from recovered state. The live
    /// advance is owned by the coordinator's `advance` gate, which writes the
    /// same (shared) atomic.
    pub fn set_offset(&self, offset: u64) {
        self.current_offset.store(offset, Ordering::Release);
    }

    /// Get the current replication offset.
    ///
    /// Reads the borrowed handle for INFO/ROLE reporting and lag; the
    /// coordinator's `current()` reads the same atomic.
    pub fn current_offset(&self) -> u64 {
        self.current_offset.load(Ordering::Acquire)
    }

    /// Minimum acknowledged offset across streaming replicas.
    ///
    /// Derived from the [`Self::get_streaming_replicas`] projection.
    pub fn min_acked_offset(&self) -> Option<u64> {
        self.get_streaming_replicas()
            .iter()
            .map(|r| r.acked_offset)
            .min()
    }

    /// Count streaming replicas that have ACKed at least `offset`.
    ///
    /// Derived from the [`Self::get_streaming_replicas`] projection — the same
    /// replicas ROLE lists are the ones WAIT counts.
    pub fn count_acked(&self, offset: u64) -> u32 {
        self.get_streaming_replicas()
            .iter()
            .filter(|r| r.acked_offset >= offset)
            .count() as u32
    }

    /// Lag in bytes for a specific replica (current_offset - acked_offset).
    pub fn replica_lag(&self, replica_id: u64) -> Option<u64> {
        let current = self.current_offset();
        self.replicas
            .read()
            .get(&replica_id)
            .map(|s| current.saturating_sub(s.acked_offset()))
    }

    /// Subscribe to ACK notifications. Yields `(replica_id, offset)` for any
    /// ACK that advances the replica's offset.
    pub fn subscribe_acks(&self) -> broadcast::Receiver<(u64, u64)> {
        self.ack_notify.subscribe()
    }

    /// Time-based lag for a specific replica (seconds since last ACK).
    pub fn replica_lag_secs(&self, replica_id: u64) -> Option<f64> {
        self.replicas
            .read()
            .get(&replica_id)
            .map(|s| s.last_ack_time().elapsed().as_secs_f64())
    }

    /// Record that a replica was proactively disconnected due to lag.
    pub fn record_lag_disconnect(&self, replica_id: u64) {
        if let Some(session) = self.replicas.read().get(&replica_id) {
            self.lag_disconnect_times
                .write()
                .insert(session.address(), Instant::now());
        }
    }

    /// Seed a replica's initial acked position when its session enters the
    /// streaming phase — the offset it resumed from (its PSYNC offset for a
    /// partial resync, or the checkpoint's `snapshot_offset` for a full resync).
    ///
    /// This is the primary recording where the replica *started*, not the
    /// replica acknowledging an offset — the distinction from
    /// [`ReplicationTracker::record_ack`] is the source of the value (primary
    /// bookkeeping vs. a wire ACK), not the effect. It shares the session's
    /// monotonic `acked_offset` atomic (so no second source of truth is
    /// introduced) and, like `record_ack`, notifies WAIT waiters when the seed
    /// actually advances that offset: a replica that reconnects via partial
    /// resync already at/past a blocked WAIT's target must wake it immediately
    /// rather than park for up to a spontaneous-ACK cadence. A stale/duplicate
    /// seed (offset <= current) does not advance the offset and does not notify.
    pub fn seed_acked_position(&self, replica_id: u64, offset: u64) {
        let Some(session) = self.replicas.read().get(&replica_id).cloned() else {
            return;
        };
        if session.seed_acked_position(offset) {
            let _ = self.ack_notify.send((replica_id, offset));
            tracing::trace!(
                replica_id = replica_id,
                offset = offset,
                "Seeded replica acked position (advance; notified WAIT waiters)"
            );
        }
    }

    /// True iff a replica's address is within the cooldown window after a
    /// proactive lag disconnect.
    pub fn is_in_lag_cooldown(&self, replica_id: u64, cooldown: Duration) -> bool {
        let addr = match self.replicas.read().get(&replica_id) {
            Some(session) => session.address(),
            None => return false,
        };
        self.lag_disconnect_times
            .read()
            .get(&addr)
            .is_some_and(|t| t.elapsed() < cooldown)
    }
}

impl ReplicationTracker for ReplicationTrackerImpl {
    /// Wait for replicas to acknowledge up to the given sequence number.
    async fn wait_for_acks(&self, sequence: u64, min_replicas: u32) -> u32 {
        let current_count = self.count_acked(sequence);
        if current_count >= min_replicas {
            return current_count;
        }
        let mut rx = self.ack_notify.subscribe();
        loop {
            let count = self.count_acked(sequence);
            if count >= min_replicas {
                return count;
            }
            match rx.recv().await {
                Ok(_) => continue,
                Err(broadcast::error::RecvError::Closed) => {
                    return self.count_acked(sequence);
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
            }
        }
    }

    /// Record an acknowledgment from a replica.
    ///
    /// Routes to the per-session bookkeeping; only newer ACKs notify waiters.
    fn record_ack(&self, replica_id: u64, sequence: u64) {
        let session = match self.replicas.read().get(&replica_id) {
            Some(s) => s.clone(),
            None => return,
        };
        if session.record_ack(sequence) {
            let _ = self.ack_notify.send((replica_id, sequence));
            tracing::trace!(
                replica_id = replica_id,
                offset = sequence,
                "Recorded replica ACK"
            );
        }
    }

    /// Number of replicas currently in the streaming phase.
    fn replica_count(&self) -> usize {
        self.replicas
            .read()
            .values()
            .filter(|s| matches!(s.phase(), Phase::Streaming))
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn test_addr() -> SocketAddr {
        "127.0.0.1:6380".parse().unwrap()
    }

    #[test]
    fn test_register_unregister_replica() {
        let tracker = ReplicationTrackerImpl::new();
        let session = tracker.register_replica(test_addr());
        assert_eq!(tracker.replica_count(), 0); // Not streaming yet
        session.force_phase_for_test(Phase::Streaming);
        assert_eq!(tracker.replica_count(), 1);
        tracker.unregister_replica(session.id());
        assert_eq!(tracker.replica_count(), 0);
    }

    #[test]
    fn test_record_ack() {
        let tracker = ReplicationTrackerImpl::new();
        let session = tracker.register_replica(test_addr());
        session.force_phase_for_test(Phase::Streaming);
        tracker.record_ack(session.id(), 100);
        assert_eq!(tracker.count_acked(100), 1);
        assert_eq!(tracker.count_acked(101), 0);
        tracker.record_ack(session.id(), 200);
        assert_eq!(tracker.count_acked(200), 1);
    }

    #[test]
    fn test_replica_lag() {
        let tracker = ReplicationTrackerImpl::new();
        let session = tracker.register_replica(test_addr());
        session.force_phase_for_test(Phase::Streaming);
        tracker.set_offset(1000);
        tracker.record_ack(session.id(), 800);
        assert_eq!(tracker.replica_lag(session.id()), Some(200));
        tracker.record_ack(session.id(), 1000);
        assert_eq!(tracker.replica_lag(session.id()), Some(0));
    }

    /// Seeding regression (round-7 follow-up to proposal 57): seeding the
    /// resume position advances the acked offset (so WAIT quorum counting and
    /// the lag monitor start from where the replica resumed) AND notifies WAIT
    /// waiters exactly like a genuine ACK, but only when the seed actually
    /// advances the offset — a stale/duplicate seed stays silent.
    #[test]
    fn seed_acked_position_notifies_wait_waiters_on_advance() {
        let tracker = ReplicationTrackerImpl::new();
        let session = tracker.register_replica(test_addr());
        session.force_phase_for_test(Phase::Streaming);
        let mut acks = tracker.subscribe_acks();

        // Seed: position advances, notification fires.
        tracker.seed_acked_position(session.id(), 100);
        assert_eq!(session.acked_offset(), 100);
        assert_eq!(tracker.count_acked(100), 1);
        assert_eq!(
            acks.try_recv().unwrap(),
            (session.id(), 100),
            "an advancing seed must notify WAIT waiters"
        );

        // A stale seed never regresses the monotonic offset, and does not notify.
        tracker.seed_acked_position(session.id(), 50);
        assert_eq!(session.acked_offset(), 100);
        assert!(
            acks.try_recv().is_err(),
            "a stale/duplicate seed must not notify WAIT waiters"
        );

        // Genuine ACK at a higher offset still notifies as before.
        tracker.record_ack(session.id(), 200);
        assert_eq!(acks.try_recv().unwrap(), (session.id(), 200));
    }

    #[test]
    fn test_min_acked_offset() {
        let tracker = ReplicationTrackerImpl::new();
        assert_eq!(tracker.min_acked_offset(), None);
        let s1 = tracker.register_replica("127.0.0.1:6380".parse().unwrap());
        let s2 = tracker.register_replica("127.0.0.1:6381".parse().unwrap());
        s1.force_phase_for_test(Phase::Streaming);
        s2.force_phase_for_test(Phase::Streaming);
        tracker.record_ack(s1.id(), 100);
        tracker.record_ack(s2.id(), 200);
        assert_eq!(tracker.min_acked_offset(), Some(100));
    }

    #[tokio::test]
    async fn test_wait_for_acks_immediate() {
        let tracker = ReplicationTrackerImpl::new();
        let session = tracker.register_replica(test_addr());
        session.force_phase_for_test(Phase::Streaming);
        tracker.record_ack(session.id(), 100);
        let count = tracker.wait_for_acks(100, 1).await;
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_wait_for_acks_with_timeout() {
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let session = tracker.register_replica(test_addr());
        session.force_phase_for_test(Phase::Streaming);
        let tracker_clone = tracker.clone();
        let id = session.id();
        let wait_handle = tokio::spawn(async move {
            tokio::time::timeout(
                Duration::from_millis(100),
                tracker_clone.wait_for_acks(100, 1),
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        tracker.record_ack(id, 100);
        let result = wait_handle.await.unwrap();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
    }

    /// Reconnect-during-WAIT regression (round-7 follow-up to proposal 57): a
    /// replica that resumes via partial resync at/past a blocked WAIT's target
    /// must wake the waiter immediately via `seed_acked_position`, not leave it
    /// parked until the next spontaneous ACK (up to ~1s in production).
    #[tokio::test]
    async fn seed_acked_position_wakes_blocked_wait_for_acks() {
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let session = tracker.register_replica(test_addr());
        session.force_phase_for_test(Phase::Streaming);
        let tracker_clone = tracker.clone();
        let id = session.id();
        let wait_handle = tokio::spawn(async move {
            tokio::time::timeout(
                Duration::from_millis(100),
                tracker_clone.wait_for_acks(100, 1),
            )
            .await
        });
        // Give the waiter time to park before seeding.
        tokio::time::sleep(Duration::from_millis(10)).await;
        tracker.seed_acked_position(id, 100);
        let result = wait_handle.await.unwrap();
        assert!(
            result.is_ok(),
            "seeding an advance must wake the blocked WAIT well within the timeout"
        );
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn test_get_streaming_replicas() {
        let tracker = ReplicationTrackerImpl::new();
        let s1 = tracker.register_replica("127.0.0.1:6380".parse().unwrap());
        let s2 = tracker.register_replica("127.0.0.1:6381".parse().unwrap());
        let s3 = tracker.register_replica("127.0.0.1:6382".parse().unwrap());
        s1.force_phase_for_test(Phase::Streaming);
        s2.force_phase_for_test(Phase::PreparingCheckpoint);
        s3.force_phase_for_test(Phase::Streaming);
        let streaming = tracker.get_streaming_replicas();
        assert_eq!(streaming.len(), 2);
    }

    #[test]
    fn test_replica_lag_secs() {
        let tracker = ReplicationTrackerImpl::new();
        assert!(tracker.replica_lag_secs(999).is_none());
        let session = tracker.register_replica(test_addr());
        session.force_phase_for_test(Phase::Streaming);
        let lag = tracker.replica_lag_secs(session.id()).unwrap();
        assert!(lag < 1.0);
    }

    #[test]
    fn test_lag_disconnect_cooldown() {
        let tracker = ReplicationTrackerImpl::new();
        let session = tracker.register_replica(test_addr());
        session.force_phase_for_test(Phase::Streaming);
        let cooldown = Duration::from_secs(60);
        assert!(!tracker.is_in_lag_cooldown(session.id(), cooldown));
        tracker.record_lag_disconnect(session.id());
        assert!(tracker.is_in_lag_cooldown(session.id(), cooldown));
        assert!(!tracker.is_in_lag_cooldown(session.id(), Duration::ZERO));
    }

    #[test]
    fn test_lag_cooldown_address_based() {
        let tracker = ReplicationTrackerImpl::new();
        let addr: SocketAddr = "127.0.0.1:6380".parse().unwrap();
        let s1 = tracker.register_replica(addr);
        s1.force_phase_for_test(Phase::Streaming);
        tracker.record_lag_disconnect(s1.id());
        tracker.unregister_replica(s1.id());
        let s2 = tracker.register_replica(addr);
        s2.force_phase_for_test(Phase::Streaming);
        // Cooldown still applies — same address, fresh id.
        assert!(tracker.is_in_lag_cooldown(s2.id(), Duration::from_secs(60)));
    }

    #[test]
    fn test_capabilities_parsing() {
        use crate::replica_session::ReplicaCapabilities;
        let caps = ReplicaCapabilities::parse_capa(&["eof", "psync2"]);
        assert!(caps.eof);
        assert!(caps.psync2);
        let caps = ReplicaCapabilities::parse_capa(&["eof"]);
        assert!(caps.eof);
        assert!(!caps.psync2);
        let caps = ReplicaCapabilities::parse_capa(&["unknown"]);
        assert!(!caps.eof);
        assert!(!caps.psync2);
    }
}
