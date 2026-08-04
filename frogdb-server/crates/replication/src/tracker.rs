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

use crate::primary::ring_buffer::{BacklogGeometry, ReplicationRingBuffer};
use crate::replica_session::{
    Phase, ReplicaAnnouncement, ReplicaInfo, ReplicaSession, ack_age_secs,
};
use crate::sync_counters::{SyncCounters, SyncCountersSnapshot, SyncOutcome};

/// Whether an ACK that landed `ack_age` ago is still inside a freshness
/// `window`.
///
/// The one place the "recently ACKing" comparison is spelled. Both write gates
/// ask it: [`ReplicationTrackerImpl::count_good_replicas`] for Redis's
/// `min-replicas-max-lag`, and the self-fence's
/// `ReplicationQuorumChecker::count_fresh_streaming_replicas` for
/// `replica-freshness-timeout-ms`. Two gates measuring the same thing must not
/// be able to disagree about where its boundary is.
///
/// The comparison is strict, and the age is a *parameter* rather than a clock
/// read inside: an ACK exactly `window` old is stale, which is what makes a
/// zero window mean "nothing is ever fresh" instead of "everything is" — and
/// therefore why `min-replicas-max-lag 0` needs the explicit disable disjunct in
/// [`ReplicationTrackerImpl::count_good_replicas`] rather than falling out of
/// the arithmetic. Taking the age in is what makes that boundary testable at
/// all; `Instant::elapsed()` can be sampled but never made to land exactly on
/// the window.
pub fn ack_is_fresh(ack_age: Duration, window: Duration) -> bool {
    ack_age < window
}

/// Whether a proactive lag disconnect that landed `age` ago is still inside the
/// `cooldown` window.
///
/// Extracted from [`ReplicationTrackerImpl::is_in_lag_cooldown`] for the same
/// reason [`ack_is_fresh`] is extracted from the freshness gates: the boundary
/// is strict (`age == cooldown` is *out* of cooldown, so a zero cooldown means
/// "no suppression at all"), and a comparison written against a live
/// `Instant::elapsed()` can never be made to land exactly on the window — which
/// leaves `<` vs `<=` untestable inside the accessor.
fn lag_cooldown_active(age: Duration, cooldown: Duration) -> bool {
    age < cooldown
}

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

    /// Lifetime tally of how each `PSYNC` resolved — `INFO`'s `sync_full`,
    /// `sync_partial_ok` and `sync_partial_err`. Kept here rather than on a
    /// session because it must outlive every session: the number an operator
    /// cares about is how often replicas *had* to resync, and the sessions that
    /// resynced are long gone.
    sync_counters: SyncCounters,

    /// The replication backlog, published here by
    /// [`crate::primary::PrimaryReplicationHandler::new`] so `INFO
    /// replication` can read the live window in either role.
    ///
    /// Kept here for the same reason [`Self::sync_counters`] is: the tracker is
    /// the one object both INFO renderers reach (FM-REPLICATION-050), and the
    /// shard-local renderer has no route to the handler at all. `None` only
    /// before publication (and in unit tests that build a bare tracker), which
    /// reads as "this node has no backlog" — the honest answer for a tracker
    /// that never got one.
    backlog: RwLock<Option<Arc<ReplicationRingBuffer>>>,
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
            sync_counters: SyncCounters::default(),
            backlog: RwLock::new(None),
        }
    }

    /// Equivalent-mutant note: `Arc::new(Self::new())` and
    /// `Arc::new(Default::default())` are the same expression here — the
    /// [`Default`] impl above is *defined* as `Self::new()` — so no test can
    /// distinguish them.
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
        self.register_announced_replica(address, ReplicaAnnouncement::default())
    }

    /// Register a replica that announced its identity over `REPLCONF` before
    /// sending `PSYNC`.
    ///
    /// The announcement is seeded into the session at construction, so a
    /// session is never visible to `INFO replication` / `ROLE` carrying a
    /// placeholder `port=0` (FM-REPLICATION-049).
    pub fn register_announced_replica(
        &self,
        address: SocketAddr,
        announcement: ReplicaAnnouncement,
    ) -> Arc<ReplicaSession> {
        let id = self.next_replica_id.fetch_add(1, Ordering::Relaxed);
        let session = ReplicaSession::announced(id, address, announcement);
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

    /// Ask every registered session to tear down — Redis's `disconnectSlaves`.
    ///
    /// Called when this node stops being a primary (see
    /// [`crate::primary::PrimaryReplicationHandler::end_primary_stint`]). Only
    /// signals; each session unregisters itself through its own exit handler, so
    /// the registry is not mutated here and a session that is already exiting is
    /// harmless to signal. Returns how many were signalled.
    pub fn disconnect_all_replicas(&self) -> usize {
        let sessions: Vec<Arc<ReplicaSession>> = self.replicas.read().values().cloned().collect();
        for session in &sessions {
            session.request_disconnect();
        }
        sessions.len()
    }

    /// Look up the session for a given replica id, if it's still registered.
    pub fn get_session(&self, replica_id: u64) -> Option<Arc<ReplicaSession>> {
        self.replicas.read().get(&replica_id).cloned()
    }

    /// Snapshot of a single replica.
    pub fn get_replica(&self, replica_id: u64) -> Option<ReplicaInfo> {
        self.replicas.read().get(&replica_id).map(|s| s.snapshot())
    }

    /// Snapshots of all registered replicas (any phase), in attach order.
    ///
    /// This is the feed `INFO replication` renders `slaveN:` lines from
    /// (FM-REPLICATION-060), which is why the order matters: the registry is a
    /// `HashMap`, so without the sort two consecutive `INFO` calls against an
    /// unchanged set of replicas could number the same replica `slave0:` and
    /// `slave2:`. Replica ids are handed out by a monotonic counter, so sorting
    /// by id *is* attach order.
    pub fn get_all_replicas(&self) -> Vec<ReplicaInfo> {
        let mut replicas: Vec<ReplicaInfo> = self
            .replicas
            .read()
            .values()
            .map(|s| s.snapshot())
            .collect();
        replicas.sort_unstable_by_key(|r| r.id);
        replicas
    }

    /// Snapshots of replicas currently in the live-streaming phase.
    ///
    /// This is THE acked-offset projection: WAIT's quorum count
    /// ([`Self::count_acked`]), [`Self::min_acked_offset`], and ROLE's replica
    /// listing all read this one accessor, so "which replicas count and what
    /// have they acknowledged" has a single definition.
    pub fn get_streaming_replicas(&self) -> Vec<ReplicaInfo> {
        let mut replicas: Vec<ReplicaInfo> = self
            .replicas
            .read()
            .values()
            .filter(|s| matches!(s.phase(), Phase::Streaming))
            .map(|s| s.snapshot())
            .collect();
        // Same reason as [`Self::get_all_replicas`]: `ROLE`'s replica array is
        // positional on the wire, so hash order would reshuffle it between two
        // calls that observed no change at all.
        replicas.sort_unstable_by_key(|r| r.id);
        replicas
    }

    /// Whether *any* replica is currently in the live-streaming phase.
    ///
    /// The existence-only counterpart of [`Self::get_streaming_replicas`], for
    /// callers that only need the boolean and would otherwise pay a `Vec` of
    /// snapshots per call. Sits on the per-write path of the primary self-fence
    /// arming check, so it short-circuits and allocates nothing.
    pub fn has_streaming_replica(&self) -> bool {
        self.replicas
            .read()
            .values()
            .any(|s| matches!(s.phase(), Phase::Streaming))
    }

    /// Count streaming replicas whose last ACK is within `max_lag` — the "good"
    /// replicas for Redis's `min-replicas-to-write` gate.
    ///
    /// A `max_lag` of zero counts every streaming replica, however long it has
    /// been silent. That is a *decision*, not a degenerate-duration guard: Redis
    /// documents `min-replicas-max-lag 0` as "no lag check", and inverting it
    /// (excluding everybody, which is what a bare [`ack_is_fresh`] would do at a
    /// zero window) would fence a healthy primary. Because the sentinel is
    /// load-bearing, no CONFIG path may reach it by accident — the seconds-valued
    /// `min-replicas-max-lag` rounds a sub-second window *up* rather than
    /// truncating it to `0`.
    pub fn count_good_replicas(&self, max_lag: Duration) -> u32 {
        self.get_streaming_replicas()
            .iter()
            // The disable check is first, so it short-circuits the clock read.
            .filter(|r| max_lag.is_zero() || ack_is_fresh(r.last_ack_time.elapsed(), max_lag))
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
            .map(|s| ack_age_secs(s.last_ack_time()))
    }

    /// Record how one `PSYNC` resolved.
    ///
    /// Refusals are recorded by the primary at the `+FULLRESYNC` / `+CONTINUE`
    /// fork, before the session is registered, so they move for every resolved
    /// handshake — including ones that never reach a streaming session. A
    /// *granted* partial is recorded later, by the streaming session, once the
    /// backlog tail it promised has actually been extracted: the window can
    /// close between the two (FM-REPLICATION-012), and a resume that was
    /// abandoned served no data.
    pub fn record_sync_outcome(&self, outcome: SyncOutcome) {
        self.sync_counters.record(outcome);
    }

    /// Read `sync_full` / `sync_partial_ok` / `sync_partial_err` as one
    /// consistent triple, for `INFO`.
    pub fn sync_counters(&self) -> SyncCountersSnapshot {
        self.sync_counters.snapshot()
    }

    /// Publish the replication backlog so `INFO replication` can report its
    /// real geometry.
    ///
    /// Called once, from [`crate::primary::PrimaryReplicationHandler::new`],
    /// which owns the ring. Re-publishing replaces the handle rather than
    /// panicking: a node can build a second handler (test wiring, and the
    /// cluster's per-shard init), and the newest ring is the one INFO should
    /// describe.
    pub fn publish_backlog(&self, backlog: Arc<ReplicationRingBuffer>) {
        *self.backlog.write() = Some(backlog);
    }

    /// The backlog window as INFO reports it, measured against this tracker's
    /// live offset so the pair cannot describe different instants.
    ///
    /// All-zero when no backlog has been published — see [`Self::backlog`].
    pub fn backlog_geometry(&self) -> BacklogGeometry {
        let current = self.current_offset();
        self.backlog
            .read()
            .as_ref()
            .map(|backlog| backlog.geometry(current))
            .unwrap_or_default()
    }

    /// Zero the three resync counters — `CONFIG RESETSTAT`.
    ///
    /// Deliberately narrower than "reset the tracker": the registered replicas,
    /// the offset, and the lag-disconnect history are live state describing the
    /// stream *right now*, and an operator resetting statistics is not asking to
    /// forget who is attached. Only the three lifetime tallies move.
    pub fn reset_sync_counters(&self) {
        self.sync_counters.reset();
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
            .is_some_and(|t| lag_cooldown_active(t.elapsed(), cooldown))
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

    // FM-REPLICATION-039
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

    // FM-REPLICATION-058
    /// `CONFIG RESETSTAT` reaches the counters through the tracker, and stops
    /// there: the attached replicas and the stream offset are live state, not
    /// statistics, and survive the reset.
    #[test]
    fn resetting_the_sync_counters_leaves_the_replica_registry_and_offset_alone() {
        let tracker = ReplicationTrackerImpl::new();
        let session = tracker.register_replica(test_addr());
        session.force_phase_for_test(Phase::Streaming);
        tracker.set_offset(1000);
        tracker.record_ack(session.id(), 800);
        tracker.record_sync_outcome(SyncOutcome::PartialRefused);
        assert_eq!(tracker.sync_counters().full, 1);

        tracker.reset_sync_counters();

        assert_eq!(tracker.sync_counters(), SyncCountersSnapshot::default());
        assert_eq!(tracker.replica_count(), 1);
        assert_eq!(tracker.current_offset(), 1000);
        assert_eq!(tracker.replica_lag(session.id()), Some(200));
    }

    // FM-REPLICATION-043
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

    // FM-REPLICATION-039
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

    // FM-REPLICATION-039
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

    // FM-REPLICATION-039
    #[tokio::test]
    async fn test_wait_for_acks_immediate() {
        let tracker = ReplicationTrackerImpl::new();
        let session = tracker.register_replica(test_addr());
        session.force_phase_for_test(Phase::Streaming);
        tracker.record_ack(session.id(), 100);
        let count = tracker.wait_for_acks(100, 1).await;
        assert_eq!(count, 1);
    }

    // FM-REPLICATION-039
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

    // FM-REPLICATION-039
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

    // FM-REPLICATION-042
    // FM-REPLICATION-039
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

    // FM-REPLICATION-043
    #[test]
    fn test_replica_lag_secs() {
        let tracker = ReplicationTrackerImpl::new();
        assert!(tracker.replica_lag_secs(999).is_none());
        let session = tracker.register_replica(test_addr());
        session.force_phase_for_test(Phase::Streaming);
        let lag = tracker.replica_lag_secs(session.id()).unwrap();
        assert!(lag < 1.0);
    }

    // FM-REPLICATION-043
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

    // FM-REPLICATION-043
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

    /// The cooldown boundary is exclusive, exactly like the freshness one: a
    /// disconnect that landed exactly `cooldown` ago no longer suppresses the
    /// next one, which is what makes `Duration::ZERO` mean "no cooldown".
    ///
    /// Asserted on the pure predicate for the reason spelled out on
    /// [`lag_cooldown_active`]: `Instant::elapsed()` cannot be made to land on
    /// the window, so `<` vs `<=` is invisible from the accessor.
    #[test]
    fn lag_cooldown_excludes_a_disconnect_exactly_at_the_window() {
        let cooldown = Duration::from_millis(500);
        assert!(lag_cooldown_active(Duration::from_millis(499), cooldown));
        assert!(
            !lag_cooldown_active(cooldown, cooldown),
            "a disconnect exactly `cooldown` old has aged out; `<=` here would \
             suppress one extra reconnect at every boundary"
        );
        assert!(!lag_cooldown_active(Duration::from_millis(501), cooldown));
        assert!(!lag_cooldown_active(Duration::ZERO, Duration::ZERO));
        assert!(lag_cooldown_active(Duration::ZERO, Duration::from_nanos(1)));
    }

    /// The two single-replica lookups answer from the live registry, and answer
    /// `None` only for an id that is not in it.
    #[test]
    fn single_replica_lookups_see_the_registered_session() {
        let tracker = ReplicationTrackerImpl::new();
        assert!(tracker.get_session(1).is_none());
        assert!(tracker.get_replica(1).is_none());

        let session = tracker.register_replica(test_addr());
        session.force_phase_for_test(Phase::Streaming);
        tracker.record_ack(session.id(), 700);

        let looked_up = tracker
            .get_session(session.id())
            .expect("a registered replica has a session");
        assert_eq!(looked_up.id(), session.id());
        assert!(Arc::ptr_eq(&looked_up, &session), "same session, not a copy");

        let info = tracker
            .get_replica(session.id())
            .expect("a registered replica has a snapshot");
        assert_eq!(info.id, session.id());
        assert_eq!(info.address, test_addr());
        assert_eq!(info.acked_offset, 700);
        assert_eq!(info.phase, Phase::Streaming);

        tracker.unregister_replica(session.id());
        assert!(tracker.get_session(session.id()).is_none());
        assert!(tracker.get_replica(session.id()).is_none());
    }

    // FM-REPLICATION-042
    /// The allocation-free existence check must agree with the projection it
    /// stands in for: a registry with only handshaking replicas has no
    /// streaming replica, and one that lost its last streaming replica goes
    /// back to `false`.
    #[test]
    fn has_streaming_replica_tracks_the_streaming_projection() {
        let tracker = ReplicationTrackerImpl::new();
        assert!(!tracker.has_streaming_replica(), "an empty registry has none");

        let handshaking = tracker.register_replica("127.0.0.1:6380".parse().unwrap());
        assert!(
            !tracker.has_streaming_replica(),
            "a replica mid-handshake is not streaming"
        );
        assert_eq!(tracker.get_streaming_replicas().len(), 0);

        let streaming = tracker.register_replica("127.0.0.1:6381".parse().unwrap());
        streaming.force_phase_for_test(Phase::Streaming);
        assert!(tracker.has_streaming_replica());
        assert_eq!(tracker.get_streaming_replicas().len(), 1);

        handshaking.force_phase_for_test(Phase::Disconnecting);
        assert!(
            tracker.has_streaming_replica(),
            "an unrelated phase change must not clear the answer"
        );

        tracker.unregister_replica(streaming.id());
        assert!(
            !tracker.has_streaming_replica(),
            "the last streaming replica leaving flips it back"
        );
    }

    // FM-REPLICATION-046
    /// The freshness boundary is exclusive: an ACK exactly `window` old is
    /// already stale.
    ///
    /// Asserted on the pure predicate rather than through a live
    /// `Instant::elapsed()`, because a wall clock cannot be made to land exactly
    /// on the window — which is why the `<` inside the two counters used to be
    /// an unkillable mutant. With the comparison extracted, `<` vs `<=` is a
    /// one-line behavioural difference this test sees.
    #[test]
    fn ack_is_fresh_excludes_an_ack_exactly_at_the_window() {
        let window = Duration::from_millis(500);
        assert!(ack_is_fresh(Duration::from_millis(499), window));
        assert!(
            !ack_is_fresh(window, window),
            "an ACK exactly `window` old has aged out; `<=` here would keep a \
             replica 'good' for one extra tick at every boundary"
        );
        assert!(!ack_is_fresh(Duration::from_millis(501), window));
        // A brand-new ACK is fresh under any non-zero window, and nothing is
        // fresh under a zero window — the disable is the *caller's* job, so this
        // predicate must not smuggle it in.
        assert!(ack_is_fresh(Duration::ZERO, Duration::from_nanos(1)));
        assert!(!ack_is_fresh(Duration::ZERO, Duration::ZERO));
    }

    // FM-REPLICATION-046 FM-REPLICATION-042
    /// `count_good_replicas` filters on ACK freshness at a real window, and
    /// treats a zero window as Redis's documented "no lag check" disable rather
    /// than as "nothing is fresh".
    ///
    /// The stale replica is backdated rather than slept, so the window stays a
    /// window and the test stays instant.
    #[test]
    fn count_good_replicas_excludes_a_stale_replica_but_zero_disables_the_check() {
        let tracker = ReplicationTrackerImpl::new();

        let fresh = tracker.register_replica("127.0.0.1:6380".parse().unwrap());
        fresh.force_phase_for_test(Phase::Streaming);
        let stale = tracker.register_replica("127.0.0.1:6381".parse().unwrap());
        stale.force_phase_for_test(Phase::Streaming);
        stale.backdate_last_ack_for_test(Duration::from_secs(3600));

        let window = Duration::from_millis(500);
        assert_eq!(
            tracker.count_good_replicas(window),
            1,
            "a replica silent for an hour is not a good replica at a 500ms window"
        );
        assert_eq!(
            tracker.count_good_replicas(Duration::ZERO),
            2,
            "`min-replicas-max-lag 0` disables the lag check (Redis parity), so \
             every streaming replica counts however long it has been silent"
        );

        // A fresh ACK rehabilitates the stale replica without touching config.
        tracker.record_ack(stale.id(), 1);
        assert_eq!(tracker.count_good_replicas(window), 2);
    }

    // FM-REPLICATION-046 FM-REPLICATION-042
    /// Only *streaming* replicas can be good, freshness notwithstanding: a
    /// handshaking replica with a brand-new `last_ack_time` must not satisfy
    /// `min-replicas-to-write`.
    #[test]
    fn count_good_replicas_ignores_non_streaming_replicas() {
        let tracker = ReplicationTrackerImpl::new();
        let handshaking = tracker.register_replica(test_addr());
        assert_eq!(handshaking.phase(), Phase::Connecting);
        assert_eq!(tracker.count_good_replicas(Duration::from_millis(500)), 0);
        assert_eq!(tracker.count_good_replicas(Duration::ZERO), 0);
        handshaking.force_phase_for_test(Phase::Streaming);
        assert_eq!(tracker.count_good_replicas(Duration::from_millis(500)), 1);
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
