use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};

use crate::cluster::{ClusterNetworkFactory, ClusterRaft, ClusterState};
use crate::command::QuorumChecker;
use crate::eviction::{
    EvictionCandidate, EvictionConfig, EvictionPolicy, EvictionPool, EvictionRanker,
};
use crate::functions::SharedFunctionRegistry;
use crate::latency::LatencyMonitor;
use crate::persistence::{
    RecoveryStats, RocksStore, SnapshotCoordinator, WalFailurePolicy, WalSink,
};
use crate::registry::CommandRegistry;
use crate::replication::{ReplicationTrackerImpl, SharedBroadcaster};
use crate::scripting::{ScriptExecutor, ScriptingConfig};
use crate::slowlog::SlowLog;
use crate::store::ExpiryIndexAnomaly;
use bytes::Bytes;
use frogdb_protocol::Response;

use super::counters::OperationCounters;
use super::message::{ScatterOp, ShardSender};

// ============================================================================
// ShardWorker Sub-Structs
// ============================================================================

/// Immutable shard identity.
pub(crate) struct ShardIdentity {
    shard_id: usize,
    num_shards: usize,
    /// Pre-formatted shard ID label for metrics (avoids per-message allocation).
    shard_label: String,
    is_replica: Arc<AtomicBool>,
    /// Server data directory (for search indexes, etc.).
    data_dir: Option<std::path::PathBuf>,
    /// Handle to request a runtime role transition (`REPLICAOF`). Shared,
    /// server-wide; `None` until the `RoleManager` is wired in during startup.
    role_controller: Option<Arc<dyn crate::command::RoleController>>,
}

impl ShardIdentity {
    pub(crate) fn new(shard_id: usize, num_shards: usize, is_replica: bool) -> Self {
        Self {
            shard_id,
            num_shards,
            shard_label: shard_id.to_string(),
            is_replica: Arc::new(AtomicBool::new(is_replica)),
            data_dir: None,
            role_controller: None,
        }
    }

    pub(crate) fn shard_id(&self) -> usize {
        self.shard_id
    }

    pub(crate) fn num_shards(&self) -> usize {
        self.num_shards
    }

    /// Pre-formatted shard-id metric label.
    pub(crate) fn shard_label(&self) -> &str {
        &self.shard_label
    }

    pub(crate) fn data_dir(&self) -> Option<&std::path::PathBuf> {
        self.data_dir.as_ref()
    }

    pub(crate) fn set_data_dir(&mut self, dir: std::path::PathBuf) {
        self.data_dir = Some(dir);
    }

    /// Whether this shard currently belongs to a replica server.
    pub(crate) fn is_replica(&self) -> bool {
        self.is_replica.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Set the replica flag (shared with the acceptor and connection handlers).
    pub(crate) fn set_is_replica(&self, is_replica: bool) {
        self.is_replica
            .store(is_replica, std::sync::atomic::Ordering::Relaxed);
    }

    /// The shared replica flag (clone to hand out to other holders).
    pub(crate) fn is_replica_flag(&self) -> &Arc<AtomicBool> {
        &self.is_replica
    }

    /// Replace the replica flag with a server-wide shared one.
    pub(crate) fn set_is_replica_flag(&mut self, flag: Arc<AtomicBool>) {
        self.is_replica = flag;
    }

    /// The primary host this server currently replicates from, if any.
    ///
    /// Derived live from the shared [`RoleController`](crate::command::RoleController)
    /// — the RoleManager, cloned into every shard's identity — rather than a
    /// per-shard copy: there is exactly one source of truth for the current
    /// replication target, and it is always fresh (seeded at boot, updated by
    /// every runtime Role Demotion), so `ROLE` / INFO can never report a
    /// stale primary after `REPLICAOF host port`.
    pub(crate) fn master_host(&self) -> Option<String> {
        self.primary_target().map(|addr| addr.ip().to_string())
    }

    /// The primary port this server currently replicates from, if any. See
    /// [`Self::master_host`].
    pub(crate) fn master_port(&self) -> Option<u16> {
        self.primary_target().map(|addr| addr.port())
    }

    /// Whether the replication link to [`Self::master_host`] is currently up
    /// (connected past PSYNC and streaming), for INFO's `master_link_status`.
    /// Derived live from the same [`RoleController`](crate::command::RoleController)
    /// as `master_host`/`master_port` — see their docs for why this is a live
    /// derivation rather than a per-shard copy. `false` with no controller
    /// wired or on a primary/standalone node.
    pub(crate) fn master_link_up(&self) -> bool {
        self.role_controller
            .as_ref()
            .is_some_and(|c| c.master_link_up())
    }

    /// Why the inbound replication stream gave up, if it did, for INFO's
    /// `master_sync_error`. Derived live from the same
    /// [`RoleController`](crate::command::RoleController) as `master_link_up`.
    /// `None` with no controller wired, and on a link that is merely down and
    /// still retrying.
    pub(crate) fn master_sync_error(&self) -> Option<String> {
        self.role_controller.as_ref()?.sync_refusal()
    }

    fn primary_target(&self) -> Option<std::net::SocketAddr> {
        self.role_controller.as_ref()?.primary_target()
    }

    /// The shared role-transition controller (clone into each `CommandContext`).
    pub(crate) fn role_controller(&self) -> Option<&Arc<dyn crate::command::RoleController>> {
        self.role_controller.as_ref()
    }

    /// Install the server-wide role-transition controller.
    pub(crate) fn set_role_controller(
        &mut self,
        controller: Arc<dyn crate::command::RoleController>,
    ) {
        self.role_controller = Some(controller);
    }
}

/// Observability: metrics, slowlog, latency, counters, queue depth, peak memory.
pub(crate) struct ShardObservability {
    metrics_recorder: Arc<dyn crate::noop::MetricsRecorder>,
    /// Process-wide keyspace hit/miss accumulator, shared with the server so
    /// `INFO stats` reads it and `CONFIG RESETSTAT` advances its baseline.
    keyspace_stats: Arc<crate::KeyspaceStats>,
    slowlog: SlowLog,
    latency_monitor: LatencyMonitor,
    operation_counters: OperationCounters,
    peak_memory: u64,
    evicted_keys: u64,
    /// Total number of objects freed via lazyfree operations (UNLINK, FLUSHALL ASYNC, etc.).
    lazyfreed_objects: u64,
    /// Shared per-shard memory usage vec, indexed by shard_id.
    /// Read by SystemMetricsCollector for fragmentation ratio calculation.
    shard_memory_used: Option<Arc<Vec<AtomicU64>>>,
}

impl ShardObservability {
    /// Assemble observability state around the shard's shared collaborators.
    pub(crate) fn new(
        metrics_recorder: Arc<dyn crate::noop::MetricsRecorder>,
        slowlog: SlowLog,
    ) -> Self {
        Self {
            metrics_recorder,
            keyspace_stats: Arc::new(crate::KeyspaceStats::new()),
            slowlog,
            latency_monitor: LatencyMonitor::default_monitor(),
            operation_counters: OperationCounters::new(),
            peak_memory: 0,
            evicted_keys: 0,
            lazyfreed_objects: 0,
            shard_memory_used: None,
        }
    }

    /// Reset the transient stats surfaced by `CONFIG RESETSTAT`.
    pub(crate) fn reset_stats(&mut self) {
        self.latency_monitor.reset(&[]);
        self.slowlog.reset();
        self.peak_memory = 0;
        self.evicted_keys = 0;
        self.lazyfreed_objects = 0;
    }

    /// The metrics recorder as a trait object (the common call form).
    pub(crate) fn metrics(&self) -> &dyn crate::noop::MetricsRecorder {
        &*self.metrics_recorder
    }

    /// The metrics recorder as a shared handle (for APIs that clone/share it).
    pub(crate) fn metrics_arc(&self) -> &Arc<dyn crate::noop::MetricsRecorder> {
        &self.metrics_recorder
    }

    /// Shared keyspace hit/miss accumulator.
    pub(crate) fn keyspace_stats(&self) -> &crate::KeyspaceStats {
        &self.keyspace_stats
    }

    /// Replace the shared keyspace hit/miss accumulator.
    pub(crate) fn set_keyspace_stats(&mut self, stats: Arc<crate::KeyspaceStats>) {
        self.keyspace_stats = stats;
    }

    pub(crate) fn slowlog(&self) -> &SlowLog {
        &self.slowlog
    }

    pub(crate) fn slowlog_mut(&mut self) -> &mut SlowLog {
        &mut self.slowlog
    }

    pub(crate) fn latency_monitor(&self) -> &LatencyMonitor {
        &self.latency_monitor
    }

    pub(crate) fn latency_monitor_mut(&mut self) -> &mut LatencyMonitor {
        &mut self.latency_monitor
    }

    pub(crate) fn operation_counters_mut(&mut self) -> &mut OperationCounters {
        &mut self.operation_counters
    }

    /// High-water mark of memory used by this shard.
    pub(crate) fn peak_memory(&self) -> u64 {
        self.peak_memory
    }

    /// Raise the peak-memory high-water mark if `used` exceeds it.
    pub(crate) fn observe_peak_memory(&mut self, used: u64) {
        if used > self.peak_memory {
            self.peak_memory = used;
        }
    }

    /// Total keys evicted on this shard.
    pub(crate) fn evicted_keys(&self) -> u64 {
        self.evicted_keys
    }

    /// Record a single evicted key.
    pub(crate) fn record_evicted(&mut self) {
        self.evicted_keys += 1;
    }

    /// Total objects freed via lazyfree on this shard.
    pub(crate) fn lazyfreed_objects(&self) -> u64 {
        self.lazyfreed_objects
    }

    /// Record `count` objects freed via lazyfree.
    pub(crate) fn record_lazyfreed(&mut self, count: u64) {
        self.lazyfreed_objects += count;
    }

    /// Shared per-shard memory-usage vec, if the server wired one in.
    pub(crate) fn shard_memory_used(&self) -> Option<&Arc<Vec<AtomicU64>>> {
        self.shard_memory_used.as_ref()
    }

    /// Wire in the shared per-shard memory-usage vec.
    pub(crate) fn set_shard_memory_used(&mut self, shared: Arc<Vec<AtomicU64>>) {
        self.shard_memory_used = Some(shared);
    }
}

/// Memory management: eviction config, sampling pool, per-shard memory limit.
pub(crate) struct ShardEviction {
    config: EvictionConfig,
    pool: EvictionPool,
    memory_limit: u64,
}

impl ShardEviction {
    /// Build eviction state, deriving this shard's slice of the global
    /// `maxmemory` limit (0 = unlimited).
    pub(crate) fn new(config: EvictionConfig, num_shards: usize) -> Self {
        Self {
            memory_limit: Self::per_shard_limit(&config, num_shards),
            pool: EvictionPool::new(),
            config,
        }
    }

    fn per_shard_limit(config: &EvictionConfig, num_shards: usize) -> u64 {
        if config.maxmemory > 0 {
            config.maxmemory / num_shards as u64
        } else {
            0
        }
    }

    pub(crate) fn update_config(&mut self, config: EvictionConfig, num_shards: usize) {
        self.memory_limit = Self::per_shard_limit(&config, num_shards);
        self.config = config;
    }

    /// Per-shard memory limit in bytes (0 = unlimited).
    pub(crate) fn memory_limit(&self) -> u64 {
        self.memory_limit
    }

    /// The active eviction policy.
    pub(crate) fn policy(&self) -> EvictionPolicy {
        self.config.policy
    }

    /// Metric-label form of the active policy.
    pub(crate) fn policy_label(&self) -> String {
        self.config.policy.to_string()
    }

    /// True when the policy rejects writes rather than evicting.
    pub(crate) fn is_no_eviction(&self) -> bool {
        self.config.policy == EvictionPolicy::NoEviction
    }

    /// Number of keys to sample per eviction pass.
    pub(crate) fn maxmemory_samples(&self) -> usize {
        self.config.maxmemory_samples
    }

    /// Offer a candidate to the sampling pool under the given ranker.
    pub(crate) fn consider_candidate<R: EvictionRanker>(
        &mut self,
        candidate: EvictionCandidate,
        ranker: &R,
    ) {
        self.pool.maybe_insert_with_ranker(candidate, ranker);
    }

    /// Pop the worst-ranked candidate currently in the pool.
    pub(crate) fn take_worst_candidate(&mut self) -> Option<EvictionCandidate> {
        self.pool.pop_worst()
    }

    /// Drop a key from the sampling pool (it is being deleted/spilled).
    pub(crate) fn forget_key(&mut self, key: &[u8]) {
        self.pool.remove(key);
    }
}

/// Reply to a write on a shard whose WAL is poisoned under a refusing failure
/// policy (FM-PERSISTENCE-055).
///
/// `MISCONF` is Redis's "the server is configured such that it cannot persist"
/// error, which is precisely the situation: the operator asked for
/// `rollback`/`readonly` durability and the storage can no longer supply it.
/// The message says *restart*, not "wait": the latch is deliberately not
/// self-clearing, because a later successful write is no evidence the lost one
/// reached the device.
pub(crate) const WAL_POISONED_ERROR: &str = "MISCONF FrogDB is unable to persist writes: \
     the WAL failed and this shard is fenced. Fix storage, then restart the node.";

/// WAL writer + snapshot coordinator for this shard.
///
/// The shard's RocksDB handle is not stored here: it is captured by the
/// [`RocksWalWriter`](crate::persistence::RocksWalWriter) and the
/// [`SnapshotCoordinator`], and wired into the store
/// as a warm tier at spawn time, so a separate copy would be write-only.
pub(crate) struct ShardPersistence {
    wal_writer: Option<Box<dyn WalSink>>,
    snapshot_coordinator: Arc<dyn SnapshotCoordinator>,
    /// WAL failure policy, encoded via [`WalFailurePolicy::as_u8`]. Shared
    /// with ConfigManager for runtime CONFIG SET support.
    failure_policy: Arc<std::sync::atomic::AtomicU8>,
    /// Whether the configured durability mode is
    /// [`DurabilityMode::Sync`](crate::persistence::DurabilityMode::Sync).
    ///
    /// Orthogonal to [`Self::failure_policy`]: the mode decides whether the ack
    /// waits for the commit, the policy decides what a failed wait does. Boot
    /// time only — `persistence.durability-mode` is not a live-retunable
    /// parameter (its `CONFIG SET` arm updates the reported value and nothing
    /// else), so unlike the policy this needs no shared atomic.
    sync_durability: bool,
    /// The full-sync flush hold this shard's WAL flush thread honours, when
    /// persistence is enabled.
    ///
    /// Lives here rather than behind the [`WalSink`] trait because it is not a
    /// WAL operation: the shard *arms* it at a `FULLRESYNC` drain, and a
    /// different thread — the full-sync coordinator, through this same `Arc` —
    /// releases it after the checkpoint cut. Sending the release down the WAL
    /// channel would deadlock behind the explicit `Flush` the hold is blocking.
    flush_hold: Option<Arc<crate::persistence::FlushHold>>,
    /// This node's boot-time recovery outcome, set once after construction
    /// (recovery finishes before any shard worker exists — there is nothing
    /// to reconstruct it from at `ShardPersistence::new` time) via
    /// [`ShardWorker::set_recovery_stats`](super::worker::ShardWorker::set_recovery_stats).
    /// `Arc::new(RecoveryStats::default())` until then, matching every other
    /// unit/test default on this struct.
    recovery_stats: Arc<RecoveryStats>,
}

impl ShardPersistence {
    pub(crate) fn new(
        wal_writer: Option<Box<dyn WalSink>>,
        snapshot_coordinator: Arc<dyn SnapshotCoordinator>,
        failure_policy: Arc<std::sync::atomic::AtomicU8>,
        sync_durability: bool,
        flush_hold: Option<Arc<crate::persistence::FlushHold>>,
    ) -> Self {
        Self {
            wal_writer,
            snapshot_coordinator,
            failure_policy,
            sync_durability,
            flush_hold,
            recovery_stats: Arc::new(RecoveryStats::default()),
        }
    }

    /// The WAL writer for this shard, if persistence is enabled.
    pub(crate) fn wal_writer(&self) -> Option<&dyn WalSink> {
        self.wal_writer.as_deref()
    }

    /// The full-sync flush hold for this shard, if persistence is enabled.
    pub(crate) fn flush_hold(&self) -> Option<&Arc<crate::persistence::FlushHold>> {
        self.flush_hold.as_ref()
    }

    /// Returns true if a WAL writer is configured for this shard.
    pub(crate) fn has_wal(&self) -> bool {
        self.wal_writer.is_some()
    }

    /// The snapshot coordinator (BGSAVE) for this shard.
    pub(crate) fn snapshot_coordinator(&self) -> &Arc<dyn SnapshotCoordinator> {
        &self.snapshot_coordinator
    }

    /// This node's boot-time recovery outcome (issue 42 / FM-PERSISTENCE-022).
    pub(crate) fn recovery_stats(&self) -> &Arc<RecoveryStats> {
        &self.recovery_stats
    }

    /// Replace the recovery outcome, called once at shard-worker spawn time
    /// with the node-wide `RecoveryStats` every shard shares.
    pub(crate) fn set_recovery_stats(&mut self, stats: Arc<RecoveryStats>) {
        self.recovery_stats = stats;
    }

    /// Replace the shared WAL failure-policy flag (from ConfigManager).
    pub(crate) fn set_failure_policy(&mut self, flag: Arc<std::sync::atomic::AtomicU8>) {
        self.failure_policy = flag;
    }

    /// The live WAL failure policy.
    fn failure_policy(&self) -> WalFailurePolicy {
        WalFailurePolicy::from_u8(
            self.failure_policy
                .load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    /// Returns true if a failed durability confirmation undoes the write —
    /// `rollback`, and `readonly`, which is `rollback` plus a standing refusal.
    pub(crate) fn should_rollback(&self) -> bool {
        self.failure_policy().rolls_back()
    }

    /// Whether this shard's WAL has lost an entry and not been reset since
    /// (FM-PERSISTENCE-053). False when there is no WAL: nothing was promised,
    /// so nothing was broken.
    pub(crate) fn wal_poisoned(&self) -> bool {
        self.wal_writer.as_ref().is_some_and(|w| w.poisoned())
    }

    /// Whether this shard must refuse writes outright (FM-PERSISTENCE-055).
    ///
    /// The fail-stop of `wal-failure-policy = rollback`/`readonly`: once the
    /// WAL has lost an entry it persists nothing further, so every subsequent
    /// write would be accepted into memory and lost on restart. Refusing before
    /// execution is the only answer that does not silently downgrade the shard
    /// to `continue`. `continue` itself never refuses — trading durability for
    /// availability is exactly what it was chosen for.
    pub(crate) fn write_refused(&self) -> bool {
        self.has_wal()
            && self.failure_policy().refuses_writes_when_poisoned()
            && self.wal_poisoned()
    }

    /// Returns true if this shard's durability mode is `sync`.
    pub(crate) fn sync_durability(&self) -> bool {
        self.sync_durability
    }

    /// Whether a write must be staged with
    /// [`Durability::Committed`](super::persistence::Durability::Committed) — i.e.
    /// whether the acknowledgement waits for the commit.
    ///
    /// Two orthogonal knobs, either of which is sufficient (FM-PERSISTENCE-002):
    /// `rollback` needs the wait so a failure can be undone before the client
    /// is told anything, and `sync` needs it because the whole contract of the
    /// mode is that an acked write is on the device. The policy alone deciding
    /// this was the gap issue 01 closed — `durability-mode = sync` under the
    /// default `continue` policy used to ack with only a `FireAndForget` stage
    /// behind it.
    pub(crate) fn should_confirm(&self) -> bool {
        self.should_rollback() || self.sync_durability()
    }
}

/// Per-shard VLL state machine.
///
/// Type alias over [`crate::vll::VllShardState`] specialized to `ScatterOp`,
/// so callers access the deepened API directly through `self.vll.<method>()`.
pub(crate) type ShardVll = crate::vll::VllShardState<ScatterOp>;

/// Client tracking: invalidation registry, tracking table, broadcast table.
pub(crate) struct ShardTracking {
    /// Client tracking: invalidation registry (conn_id → sender + metadata).
    invalidation_registry: crate::tracking::InvalidationRegistry,
    /// Client tracking: key → interested connections table.
    tracking_table: crate::tracking::TrackingTable,
    /// BCAST tracking: prefix → interested connections table.
    broadcast_table: crate::tracking::BroadcastTable,
}

impl Default for ShardTracking {
    fn default() -> Self {
        Self {
            invalidation_registry: crate::tracking::InvalidationRegistry::default(),
            tracking_table: crate::tracking::TrackingTable::new(
                crate::tracking::DEFAULT_TRACKING_TABLE_MAX_KEYS,
            ),
            broadcast_table: crate::tracking::BroadcastTable::default(),
        }
    }
}

impl ShardTracking {
    pub(crate) fn has_tracking_clients(&self) -> bool {
        !self.invalidation_registry.is_empty()
    }

    /// True if any client is tracking, in either default (key) or BCAST
    /// (prefix) mode — the guard write paths use before emitting invalidations.
    pub(crate) fn has_any_tracking_clients(&self) -> bool {
        self.has_tracking_clients() || !self.broadcast_table.is_empty()
    }

    /// Register a default (key-based) tracking client.
    pub(crate) fn register(&mut self, conn_id: u64, conn: crate::tracking::TrackedConnection) {
        self.invalidation_registry.register(conn_id, conn);
    }

    /// Register a BCAST (prefix-based) tracking client.
    pub(crate) fn register_broadcast(
        &mut self,
        conn_id: u64,
        conn: crate::tracking::TrackedConnection,
        prefixes: &[Bytes],
    ) {
        self.invalidation_registry.register(conn_id, conn);
        self.broadcast_table.register(conn_id, prefixes);
    }

    /// Drop a connection from every tracking table (default + BCAST + registry).
    pub(crate) fn unregister(&mut self, conn_id: u64) {
        self.tracking_table.remove_connection(conn_id);
        self.broadcast_table.remove_connection(conn_id);
        self.invalidation_registry.unregister(conn_id);
    }

    pub(crate) fn record_read(&mut self, key: &[u8], conn_id: u64) {
        self.tracking_table
            .record_read(key, conn_id, &self.invalidation_registry);
    }

    pub(crate) fn invalidate_keys(&mut self, keys: &[&[u8]], conn_id: u64) {
        self.tracking_table
            .invalidate_keys(keys, conn_id, &self.invalidation_registry);
    }

    /// Invalidate `keys` across both tracking modes (default key-based and BCAST
    /// prefix-based). The single seam every write path uses so the two modes can
    /// never drift apart again.
    pub(crate) fn invalidate_keys_all_modes(&mut self, keys: &[&[u8]], conn_id: u64) {
        if keys.is_empty() {
            return;
        }
        if self.has_tracking_clients() {
            self.invalidate_keys(keys, conn_id);
        }
        if !self.broadcast_table.is_empty() {
            self.broadcast_table
                .invalidate_matching(keys, conn_id, &self.invalidation_registry);
        }
    }

    pub(crate) fn flush_all_tracking(&mut self) {
        self.tracking_table.flush_all(&self.invalidation_registry);
    }

    /// Approximate heap footprint of the tracking table (key/client indices +
    /// LRU order, stale entries included), for memory accounting.
    pub(crate) fn memory_usage(&self) -> usize {
        self.tracking_table.memory_usage()
    }
}

/// Scripting: Lua script executor, function registry.
#[derive(Default)]
pub(crate) struct ShardScripting {
    /// Script executor for this shard.
    executor: Option<ScriptExecutor>,
    /// Function registry (shared across all shards).
    function_registry: Option<SharedFunctionRegistry>,
}

impl ShardScripting {
    pub(crate) fn new(
        executor: Option<ScriptExecutor>,
        function_registry: Option<SharedFunctionRegistry>,
    ) -> Self {
        Self {
            executor,
            function_registry,
        }
    }

    /// True if a Lua executor is available on this shard.
    pub(crate) fn has_executor(&self) -> bool {
        self.executor.is_some()
    }

    pub(crate) fn executor(&self) -> Option<&ScriptExecutor> {
        self.executor.as_ref()
    }

    pub(crate) fn executor_mut(&mut self) -> Option<&mut ScriptExecutor> {
        self.executor.as_mut()
    }

    /// Move the executor out (put it back with [`set_executor`](Self::set_executor)).
    ///
    /// Used by the EVAL path so `self` is free to build a `CommandContext` while
    /// the executor runs.
    pub(crate) fn take_executor(&mut self) -> Option<ScriptExecutor> {
        self.executor.take()
    }

    pub(crate) fn set_executor(&mut self, executor: ScriptExecutor) {
        self.executor = Some(executor);
    }

    pub(crate) fn function_registry(&self) -> Option<&SharedFunctionRegistry> {
        self.function_registry.as_ref()
    }

    pub(crate) fn set_function_registry(&mut self, registry: SharedFunctionRegistry) {
        self.function_registry = Some(registry);
    }
}

/// Cluster: raft, cluster state, node ID, network factory, quorum checker, replication.
pub(crate) struct ShardCluster {
    raft: Option<Arc<ClusterRaft>>,
    cluster_state: Option<Arc<ClusterState>>,
    node_id: Option<u64>,
    network_factory: Option<Arc<ClusterNetworkFactory>>,
    quorum_checker: Option<Arc<dyn QuorumChecker>>,
    replication_tracker: Option<Arc<ReplicationTrackerImpl>>,
}

impl ShardCluster {
    /// Build cluster state from the handles resolved by the builder. The
    /// replication tracker is wired in later via [`set_replication_tracker`].
    pub(crate) fn new(
        raft: Option<Arc<ClusterRaft>>,
        cluster_state: Option<Arc<ClusterState>>,
        node_id: Option<u64>,
        network_factory: Option<Arc<ClusterNetworkFactory>>,
        quorum_checker: Option<Arc<dyn QuorumChecker>>,
    ) -> Self {
        Self {
            raft,
            cluster_state,
            node_id,
            network_factory,
            quorum_checker,
            replication_tracker: None,
        }
    }

    /// True when this shard participates in a cluster.
    pub(crate) fn is_cluster_mode(&self) -> bool {
        self.cluster_state.is_some()
    }

    pub(crate) fn raft(&self) -> Option<&Arc<ClusterRaft>> {
        self.raft.as_ref()
    }

    pub(crate) fn cluster_state(&self) -> Option<&Arc<ClusterState>> {
        self.cluster_state.as_ref()
    }

    pub(crate) fn node_id(&self) -> Option<u64> {
        self.node_id
    }

    pub(crate) fn network_factory(&self) -> Option<&Arc<ClusterNetworkFactory>> {
        self.network_factory.as_ref()
    }

    pub(crate) fn quorum_checker(&self) -> Option<&dyn QuorumChecker> {
        self.quorum_checker.as_deref()
    }

    /// The same checker as an owned handle, for the
    /// [`ShardWriteSeam`](crate::write_seam::ShardWriteSeam), which outlives the
    /// `&self` borrow (it is held for the span of one script execution).
    pub(crate) fn quorum_checker_owned(&self) -> Option<Arc<dyn QuorumChecker>> {
        self.quorum_checker.clone()
    }

    pub(crate) fn replication_tracker(&self) -> Option<&Arc<ReplicationTrackerImpl>> {
        self.replication_tracker.as_ref()
    }

    pub(crate) fn set_raft(&mut self, raft: Arc<ClusterRaft>) {
        self.raft = Some(raft);
    }

    pub(crate) fn set_cluster_state(&mut self, cluster_state: Arc<ClusterState>) {
        self.cluster_state = Some(cluster_state);
    }

    pub(crate) fn set_node_id(&mut self, node_id: u64) {
        self.node_id = Some(node_id);
    }

    pub(crate) fn set_network_factory(&mut self, network_factory: Arc<ClusterNetworkFactory>) {
        self.network_factory = Some(network_factory);
    }

    pub(crate) fn set_quorum_checker(&mut self, quorum_checker: Arc<dyn QuorumChecker>) {
        self.quorum_checker = Some(quorum_checker);
    }

    pub(crate) fn set_replication_tracker(&mut self, tracker: Arc<ReplicationTrackerImpl>) {
        self.replication_tracker = Some(tracker);
    }
}

// ============================================================================
// Dependency Groups for ShardWorkerBuilder
// ============================================================================

/// Core dependencies required for shard operation.
#[derive(Clone)]
pub struct ShardCoreDeps {
    /// Senders to all shards for cross-shard operations.
    pub shard_senders: Arc<Vec<ShardSender>>,

    /// Command registry for looking up command implementations.
    pub registry: Arc<CommandRegistry>,

    /// Metrics recorder for observability.
    pub metrics_recorder: Arc<dyn crate::noop::MetricsRecorder>,

    /// Slowlog next ID counter (shared across shards).
    pub slowlog_next_id: Arc<AtomicU64>,

    /// Replication broadcaster for propagating writes.
    pub replication_broadcaster: SharedBroadcaster,
}

/// Dependencies for persistence (optional).
#[derive(Clone, Default)]
pub struct ShardPersistenceDeps {
    /// RocksDB store for persistence.
    pub rocks_store: Option<Arc<RocksStore>>,

    /// Snapshot coordinator for BGSAVE operations.
    pub snapshot_coordinator: Option<Arc<dyn SnapshotCoordinator>>,

    /// This node's boot-time recovery outcome, for INFO persistence's
    /// `rdb_last_load_keys_*` fields (issue 42 / FM-PERSISTENCE-022).
    pub recovery_stats: Option<Arc<RecoveryStats>>,
}

/// Dependencies for cluster mode (optional).
#[derive(Clone, Default)]
pub struct ShardClusterDeps {
    /// Cluster state with slot assignments.
    pub cluster_state: Option<Arc<ClusterState>>,

    /// This node's ID in the cluster.
    pub node_id: Option<u64>,

    /// Raft instance for consensus operations.
    pub raft: Option<Arc<ClusterRaft>>,

    /// Network factory for cluster communications.
    pub network_factory: Option<Arc<ClusterNetworkFactory>>,

    /// Quorum checker for cluster health.
    pub quorum_checker: Option<Arc<dyn QuorumChecker>>,
}

impl ShardClusterDeps {
    /// Create empty cluster deps (standalone mode).
    pub fn standalone() -> Self {
        Self::default()
    }

    /// Check if cluster mode is enabled.
    pub fn is_cluster_mode(&self) -> bool {
        self.cluster_state.is_some()
    }
}

/// Configuration for shard behavior.
#[derive(Clone, Default)]
pub struct ShardConfig {
    /// Eviction configuration.
    pub eviction: EvictionConfig,

    /// Scripting configuration.
    pub scripting: ScriptingConfig,

    /// Enable VLL (Virtual Lock Loom) for transaction coordination.
    pub enable_vll: bool,
}

// ============================================================================
// Response / metadata types
// ============================================================================

/// Reply from a shard for a scatter-gather operation.
///
/// One variant per reply *shape*, mirroring the [`ScatterOp`] that produces it
/// so request and reply are typed in lockstep. This replaces the former
/// "`Vec<(Bytes, Response)>` + optional `ft`" struct, which forced the non-keyed
/// broadcast ops (SCAN/DBSIZE/RANDOMKEY/FLUSHDB/COPY) to smuggle control data
/// through fabricated sentinel keys and a positional array.
#[derive(Debug)]
pub enum PartialResult {
    /// Keyed `(key, response)` pairs: MGET/MSET/DEL/EXISTS/TOUCH/UNLINK/KEYS/
    /// DUMP/CopySet, the FT.* ops that reply per key, and the shard-error
    /// fallback path (`dispatch_core.rs`).
    Keyed(Vec<(Bytes, Response)>),

    /// SCAN: this shard's next cursor (`0` = exhausted) and the keys found this
    /// step.
    Scan {
        /// Resume cursor for the next SCAN step on this shard; `0` when done.
        next_cursor: u64,
        /// Keys found by this scan step.
        keys: Vec<Bytes>,
    },

    /// DBSIZE: this shard's key count.
    Count(i64),

    /// RANDOMKEY: a random key from this shard, or `None` if it is empty.
    RandomKey(Option<Bytes>),

    /// FLUSHDB acknowledgement (no payload).
    Flushed,

    /// COPY read phase: the source value + out-of-band expiry, or `None` if the
    /// source key is absent.
    Copy(Option<CopyPayload>),

    /// Typed payload for the FT.* query fan-outs (search hits / partial
    /// aggregates).
    Ft(frogdb_search::FtShardReply),

    /// A shard rejected the scatter part outright — it never executed the
    /// operation, so this reply carries *no* data, only the error. Emitted by
    /// [`scatter_error_reply`](crate::shard::ShardWorker) for **keyless**
    /// scatter ops (KEYS/DBSIZE/SCAN/FLUSHDB, and the FT admin/single-shard ops)
    /// whose per-key `Keyed` fallback would otherwise be *empty* and drop the
    /// error silently. A `ShardError` is fatal: the coordinator must surface it
    /// (fail the whole command) rather than fold it into a truncated success.
    /// [`ScatterGather::run`](../../../server) recognizes it centrally so every
    /// broadcast merge — present and future — aborts on it uniformly.
    ShardError(Response),
}

/// The source payload a cross-shard COPY read phase ships to the coordinator.
#[derive(Debug)]
pub struct CopyPayload {
    /// Self-describing persistence frame (no separate type tag). The COPY frame
    /// header is expiry-free; the TTL rides out-of-band in [`Self::expiry_ms`].
    pub value: Bytes,
    /// Expiry in ms since the epoch; `None` = no expiry.
    pub expiry_ms: Option<i64>,
}

impl Default for PartialResult {
    /// The empty keyed reply — matches the former `#[derive(Default)]` on the
    /// struct (empty `results`). `#[default]` cannot attach to a data-carrying
    /// variant, so this is hand-written; the live call site is the VLL
    /// dequeue-miss empty reply (`vll.rs`).
    fn default() -> Self {
        PartialResult::Keyed(Vec::new())
    }
}

impl PartialResult {
    /// A conventional keyed-response reply.
    pub fn keyed(results: Vec<(Bytes, Response)>) -> Self {
        PartialResult::Keyed(results)
    }

    /// A typed FT.* reply.
    pub fn ft(reply: frogdb_search::FtShardReply) -> Self {
        PartialResult::Ft(reply)
    }

    /// A fatal shard-rejection reply carrying only the error (no data). Used by
    /// the keyless-scatter conflict path so the error survives the merge.
    pub fn shard_error(err: Response) -> Self {
        PartialResult::ShardError(err)
    }

    /// Borrow the fatal shard error, if this is a [`PartialResult::ShardError`].
    /// Non-`ShardError` variants yield `None`. Lets the non-`run` coordinator
    /// seams (SCAN / RANDOMKEY / the shard-0-direct FT reads) surface a
    /// conflict rejection instead of returning a truncated success.
    pub fn as_shard_error(&self) -> Option<&Response> {
        match self {
            PartialResult::ShardError(err) => Some(err),
            _ => None,
        }
    }

    /// The keyed `(key, response)` pairs, consuming the reply. Non-keyed
    /// variants yield an empty vec — used by the genuinely-keyed consumers
    /// (MGET/DEL/DUMP/CopySet fan-out merges and single-shard FT/persistence
    /// replies).
    pub fn into_keyed_results(self) -> Vec<(Bytes, Response)> {
        match self {
            PartialResult::Keyed(results) => results,
            _ => Vec::new(),
        }
    }

    /// Borrow the keyed `(key, response)` pairs; empty slice for non-keyed
    /// variants.
    pub fn keyed_slice(&self) -> &[(Bytes, Response)] {
        match self {
            PartialResult::Keyed(results) => results,
            _ => &[],
        }
    }
}

/// Memory statistics for a single shard.
#[derive(Debug, Clone, Default)]
pub struct ShardMemoryStats {
    /// Shard identifier.
    pub shard_id: usize,
    /// Total memory used by data (bytes).
    pub data_memory: usize,
    /// Number of keys in the shard.
    pub keys: usize,
    /// Peak memory usage (high-water mark).
    pub peak_memory: u64,
    /// Memory limit for this shard (0 = unlimited).
    pub memory_limit: u64,
    /// Overhead estimate (allocator, metadata, etc).
    pub overhead_estimate: usize,
    /// Total number of keys evicted.
    pub evicted_keys: u64,
    /// Total number of keys expired.
    pub expired_keys: u64,
    /// Total number of objects freed via lazyfree operations.
    pub lazyfreed_objects: u64,
}

/// Information about a large key.
#[derive(Debug, Clone)]
pub struct BigKeyInfo {
    /// The key name.
    pub key: Bytes,
    /// Type of the value (e.g., "string", "hash", "list").
    pub key_type: String,
    /// Memory usage in bytes.
    pub memory_bytes: usize,
}

/// Response from big key scanning.
#[derive(Debug, Clone, Default)]
pub struct BigKeysScanResponse {
    /// Shard identifier.
    pub shard_id: usize,
    /// List of big keys found.
    pub big_keys: Vec<BigKeyInfo>,
    /// Total number of keys scanned.
    pub keys_scanned: usize,
    /// Whether the scan was truncated due to max_keys limit.
    pub truncated: bool,
}

/// Response for WAL lag statistics query.
#[derive(Debug, Clone, Default)]
pub struct WalLagStatsResponse {
    /// Shard identifier.
    pub shard_id: usize,
    /// Whether persistence is enabled for this shard.
    pub persistence_enabled: bool,
    /// Lag statistics (None if persistence is disabled).
    pub lag_stats: Option<crate::persistence::WalLagStats>,
}

/// Tiered-storage counters for a single shard (INFO `# Tiered` section).
#[derive(Debug, Clone, Default)]
pub struct TieredCounts {
    /// Number of keys resident in the hot tier.
    pub hot_keys: usize,
    /// Number of keys resident in the warm tier.
    pub warm_keys: usize,
    /// Total unspills (warm -> hot) performed.
    pub unspills: u64,
    /// Total spills (hot -> warm) performed.
    pub spills: u64,
    /// Keys found expired while being unspilled.
    pub expired_on_unspill: u64,
}

/// Everything INFO needs from a single shard, gathered in one fleet scatter.
///
/// This replaces INFO's previous two passes (a `MemoryStats` scatter plus a
/// separate `KeysizesSnapshot` loop) with a single combined reply. Adding a new
/// per-shard INFO field is a new field here, not a new round trip. The
/// connection-level INFO builder folds these per-shard replies into its
/// aggregate (summing eviction counters, merging keysize histograms, and
/// picking the local shard's values for shard-scoped fields).
#[derive(Debug, Clone, Default)]
pub struct InfoShardSnapshot {
    /// Shard identifier.
    pub shard_id: usize,
    /// Memory + eviction/expiry counters (identical to a `MemoryStats` reply).
    pub memory: ShardMemoryStats,
    /// `rdb_changes_since_last_save` source (this shard's dirty counter).
    pub dirty: u64,
    /// Tiered-storage counters for this shard.
    pub tiered: TieredCounts,
    /// Per-type key size histograms for this shard (merged across shards).
    pub keysizes: crate::histogram::KeysizeHistograms,
    /// WAL durability lag (None when persistence is disabled for this shard).
    pub wal_lag: Option<crate::persistence::WalLagStats>,
    /// Primary host, set when this shard is running as a replica.
    pub master_host: Option<String>,
    /// Primary port, set when this shard is running as a replica.
    pub master_port: Option<u16>,
    /// Whether the replication link to the primary is connected and
    /// streaming (mirrors `RoleController::master_link_up`).
    pub master_link_up: bool,
    /// Why the inbound replication stream gave up, if it did (mirrors
    /// `RoleController::sync_refusal`). `None` on a link that is merely down
    /// and still retrying.
    pub master_sync_error: Option<String>,
}

/// Response for VLL queue info query.
#[derive(Debug, Clone, Default)]
pub struct VllQueueInfo {
    /// Shard identifier.
    pub shard_id: usize,
    /// Number of pending operations in the queue.
    pub queue_depth: usize,
    /// Transaction ID currently executing (if any).
    pub executing_txid: Option<u64>,
    /// Continuation lock info (if held).
    pub continuation_lock: Option<VllContinuationLockInfo>,
    /// Pending operations in the queue.
    pub pending_ops: Vec<VllPendingOpInfo>,
    /// Intent table state.
    pub intent_table: Vec<VllKeyIntentInfo>,
}

/// Information about a pending VLL operation.
#[derive(Debug, Clone)]
pub struct VllPendingOpInfo {
    /// Transaction ID.
    pub txid: u64,
    /// Operation type as string.
    pub operation: String,
    /// Number of keys involved.
    pub key_count: usize,
    /// Current state.
    pub state: String,
    /// Age in milliseconds.
    pub age_ms: u64,
}

/// Information about a continuation lock.
#[derive(Debug, Clone)]
pub struct VllContinuationLockInfo {
    /// Transaction ID holding the lock.
    pub txid: u64,
    /// Connection ID that owns the lock.
    pub conn_id: u64,
    /// Age in milliseconds.
    pub age_ms: u64,
}

/// Information about key intents.
#[derive(Debug, Clone)]
pub struct VllKeyIntentInfo {
    /// Key (may be truncated).
    pub key: String,
    /// Transaction IDs with intents on this key.
    pub txids: Vec<u64>,
    /// Lock state as string.
    pub lock_state: String,
}

/// Response for `DEBUG LOCKTABLE` — a per-shard VLL lock-table snapshot.
#[derive(Debug, Clone, Default)]
pub struct LockTableInfo {
    /// Shard identifier.
    pub shard_id: usize,
    /// Per-key intents (txids + grant state), reusing the VLL intent view.
    pub intents: Vec<VllKeyIntentInfo>,
    /// The continuation lock, if one is held.
    pub continuation_lock: Option<VllContinuationLockInfo>,
}

/// Response for `DEBUG WAITQUEUE` — a per-shard blocking-waiter snapshot.
#[derive(Debug, Clone, Default)]
pub struct WaitQueueInfo {
    /// Shard identifier.
    pub shard_id: usize,
    /// Total active waiters on this shard.
    pub total_waiters: usize,
    /// Waiters grouped by key (keys sorted; waiters in registration order).
    pub keys: Vec<WaitQueueKeyInfo>,
}

/// Waiters blocked on one key.
#[derive(Debug, Clone)]
pub struct WaitQueueKeyInfo {
    /// The key (lossy UTF-8 for display).
    pub key: String,
    /// Waiters in registration (FIFO) order.
    pub waiters: Vec<WaitQueueWaiterInfo>,
}

/// One blocked waiter's view.
#[derive(Debug, Clone)]
pub struct WaitQueueWaiterInfo {
    /// Connection id of the blocked client.
    pub conn_id: u64,
    /// Blocking command name (e.g. "BLPOP").
    pub op: String,
    /// Queue-wide monotonic registration ordinal (smaller = earlier).
    pub registration_seq: u64,
    /// Whether the waiter has a finite deadline.
    pub has_deadline: bool,
}

/// Response for `DEBUG WAITQUEUE-LOG` — a per-shard journal of every blocking
/// registration the shard recorded, in registration order.
///
/// `DEBUG WAITQUEUE` can only show waiters that are *still parked* when it is
/// read, so sampling it misses every waiter that parked and was served between
/// two samples. This journal is written at registration time, so it is complete
/// by construction (unless `truncated`).
#[derive(Debug, Clone, Default)]
pub struct WaitQueueLogInfo {
    /// Shard identifier.
    pub shard_id: usize,
    /// True if the journal hit its cap and stopped recording — the entries are
    /// then a prefix, not the whole registration history.
    pub truncated: bool,
    /// Registrations in order, one per (waiter, key) pair.
    pub entries: Vec<WaitQueueLogEntryInfo>,
}

/// One journaled registration.
#[derive(Debug, Clone)]
pub struct WaitQueueLogEntryInfo {
    /// Shard-wide monotonic registration ordinal (smaller = registered earlier).
    pub registration_seq: u64,
    /// Connection id of the waiter.
    pub conn_id: u64,
    /// The key parked on (lossy UTF-8 for display).
    pub key: String,
    /// Blocking command name (e.g. "BLPOP").
    pub op: String,
}

/// Response for `DEBUG MEMORY-CHECK` — tracked vs recomputed live footprint.
#[derive(Debug, Clone, Default)]
pub struct MemoryCheckInfo {
    /// Shard identifier.
    pub shard_id: usize,
    /// The running `memory_used` counter.
    pub tracked_bytes: usize,
    /// Recomputed live sum over all entries.
    pub recomputed_bytes: usize,
}

/// Response for `DEBUG EXPIRY-INDEX-CHECK` — index-vs-entry inconsistencies.
#[derive(Debug, Clone, Default)]
pub struct ExpiryIndexCheckInfo {
    /// Shard identifier.
    pub shard_id: usize,
    /// Number of key-level expiry-index entries examined.
    pub total_entries: usize,
    /// Inconsistencies found (empty = consistent).
    pub anomalies: Vec<ExpiryIndexAnomaly>,
}

/// Response for `DEBUG OBJECT <key>` — the internals of one live key, gathered
/// on the shard that owns it.
///
/// Every field is a fact the store actually holds. Redis's `at:<ptr>` and its
/// `ql_*` quicklist counters have no truthful analogue here (a heap address is
/// an ASLR information leak, and FrogDB's list is not a quicklist), so they are
/// absent from this struct rather than fabricated at the formatting seam.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectInfo {
    /// References to the value — always
    /// [`Value::REPORTED_REFCOUNT`](frogdb_types::Value::REPORTED_REFCOUNT), the
    /// same answer `OBJECT REFCOUNT` gives.
    pub refcount: i64,
    /// The value's encoding name, from
    /// [`Value::encoding_name`](frogdb_types::Value::encoding_name) — the same
    /// answer `OBJECT ENCODING` gives.
    pub encoding: &'static str,
    /// Byte length of the value's persisted payload
    /// ([`frogdb_persistence::serialization::serialized_payload_len`]).
    pub serialized_length: usize,
    /// The key's last-access stamp as Unix seconds — FrogDB's LRU clock. Redis
    /// reports a 24-bit truncation of the same quantity.
    pub lru: i64,
    /// Seconds since the key was last accessed — the same answer
    /// `OBJECT IDLETIME` gives.
    pub lru_seconds_idle: u64,
}

/// Pub/Sub limits info for a shard.
#[derive(Debug, Clone, Default)]
pub struct PubSubLimitsInfo {
    /// Total subscriptions across all connections on this shard.
    pub total_subscriptions: usize,
    /// Number of unique channels with at least one subscriber.
    pub unique_channels: usize,
    /// Number of unique patterns with at least one subscriber.
    pub unique_patterns: usize,
}

/// Result from executing a transaction.
#[derive(Debug)]
pub enum TransactionResult {
    /// Transaction executed successfully.
    Success(Vec<Response>),
    /// Transaction aborted due to WATCH conflict.
    WatchAborted,
    /// The routing generation the batch was validated against is no longer the
    /// live one, so the shard refused the apply **before running any command**
    /// (`specs/txn.md` TR-TXN-020).
    ///
    /// Not an error and not a redirect: the shard cannot name the new owner
    /// (that is connection-side knowledge) and nothing was applied, so the
    /// coordinator re-validates against a fresh snapshot and either retries or
    /// answers with whatever the fresh verdict says. Distinct from
    /// [`Self::Error`] precisely so it cannot be mistaken for a terminal answer
    /// and surfaced to the client as one.
    TopologyChanged,
    /// Every watch on this shard still holds, and here is a generation handle
    /// per watched key so the batch's target shard can say so again *at commit
    /// time* (`specs/txn.md` TR-TXN-028).
    ///
    /// The answer to a [`WatchFenceRole::Mint`] round-trip, which carries no
    /// commands — it is the coordinator asking a non-target shard about its
    /// watches. A dirty verdict still comes back as [`Self::WatchAborted`]:
    /// fences are only minted when there is nothing to abort on yet.
    ///
    /// [`WatchFenceRole::Mint`]: crate::WatchFenceRole
    WatchesFenced(Vec<crate::WatchFence>),
    /// Transaction failed with an error.
    Error(String),
}

#[cfg(test)]
mod eviction_tests {
    use super::*;

    fn config(maxmemory: u64, policy: EvictionPolicy) -> EvictionConfig {
        EvictionConfig {
            maxmemory,
            policy,
            ..EvictionConfig::default()
        }
    }

    #[test]
    fn new_divides_maxmemory_across_shards() {
        let ev = ShardEviction::new(config(1000, EvictionPolicy::AllkeysLru), 4);
        assert_eq!(ev.memory_limit(), 250);
        assert_eq!(ev.policy(), EvictionPolicy::AllkeysLru);
        assert!(!ev.is_no_eviction());
    }

    #[test]
    fn zero_maxmemory_is_unlimited() {
        let ev = ShardEviction::new(config(0, EvictionPolicy::NoEviction), 4);
        assert_eq!(ev.memory_limit(), 0);
        assert!(ev.is_no_eviction());
    }

    #[test]
    fn update_config_recomputes_limit_and_policy() {
        let mut ev = ShardEviction::new(config(0, EvictionPolicy::NoEviction), 8);
        assert_eq!(ev.memory_limit(), 0);
        assert!(ev.is_no_eviction());

        ev.update_config(config(800, EvictionPolicy::VolatileLru), 8);
        assert_eq!(ev.memory_limit(), 100);
        assert!(!ev.is_no_eviction());
        assert_eq!(ev.policy(), EvictionPolicy::VolatileLru);
        assert_eq!(ev.policy_label(), EvictionPolicy::VolatileLru.to_string());
    }
}

#[cfg(test)]
mod observability_tests {
    use super::*;

    fn observability() -> ShardObservability {
        let slowlog = SlowLog::new(
            crate::slowlog::DEFAULT_SLOWLOG_MAX_LEN,
            crate::slowlog::DEFAULT_SLOWLOG_MAX_ARG_LEN,
            Arc::new(AtomicU64::new(0)),
        );
        ShardObservability::new(Arc::new(crate::noop::NoopMetricsRecorder::new()), slowlog)
    }

    #[test]
    fn observe_peak_memory_tracks_high_water_mark() {
        let mut obs = observability();
        assert_eq!(obs.peak_memory(), 0);
        obs.observe_peak_memory(100);
        assert_eq!(obs.peak_memory(), 100);
        // A lower reading does not lower the high-water mark.
        obs.observe_peak_memory(40);
        assert_eq!(obs.peak_memory(), 100);
        obs.observe_peak_memory(250);
        assert_eq!(obs.peak_memory(), 250);
    }

    #[test]
    fn eviction_and_lazyfree_counters_accumulate() {
        let mut obs = observability();
        obs.record_evicted();
        obs.record_evicted();
        assert_eq!(obs.evicted_keys(), 2);

        obs.record_lazyfreed(5);
        obs.record_lazyfreed(3);
        assert_eq!(obs.lazyfreed_objects(), 8);
    }

    #[test]
    fn reset_stats_clears_transient_counters() {
        let mut obs = observability();
        obs.observe_peak_memory(500);
        obs.record_evicted();
        obs.record_lazyfreed(9);

        obs.reset_stats();

        assert_eq!(obs.peak_memory(), 0);
        assert_eq!(obs.evicted_keys(), 0);
        assert_eq!(obs.lazyfreed_objects(), 0);
    }
}

#[cfg(test)]
mod persistence_tests {
    use super::*;
    use crate::persistence::NoopSnapshotCoordinator;

    fn persistence() -> ShardPersistence {
        persistence_with_durability(false)
    }

    fn persistence_with_durability(sync_durability: bool) -> ShardPersistence {
        ShardPersistence::new(
            None,
            Arc::new(NoopSnapshotCoordinator::new()),
            Arc::new(std::sync::atomic::AtomicU8::new(
                WalFailurePolicy::default().as_u8(),
            )),
            sync_durability,
            None,
        )
    }

    #[test]
    fn no_wal_without_writer() {
        let p = persistence();
        assert!(!p.has_wal());
        assert!(p.wal_writer().is_none());
    }

    // FM-PERSISTENCE-005
    #[test]
    fn should_rollback_follows_shared_flag() {
        let mut p = persistence();
        assert!(!p.should_rollback(), "default policy continues");

        let flag = Arc::new(std::sync::atomic::AtomicU8::new(
            WalFailurePolicy::Rollback.as_u8(),
        ));
        p.set_failure_policy(flag);
        assert!(p.should_rollback());
    }

    // FM-PERSISTENCE-002
    // The two knobs are orthogonal, and either one alone selects `Confirm`:
    // `sync` durability under the default `continue` policy must wait for the
    // commit exactly as `rollback` does, and the policy must keep its own
    // meaning (what a failed wait does) in both modes.
    #[test]
    fn sync_durability_selects_confirm_independently_of_the_failure_policy() {
        let rollback = Arc::new(std::sync::atomic::AtomicU8::new(
            WalFailurePolicy::Rollback.as_u8(),
        ));

        // continue + non-sync: the one combination that acks without waiting.
        let p = persistence_with_durability(false);
        assert!(!p.should_rollback());
        assert!(!p.sync_durability());
        assert!(!p.should_confirm());

        // continue + sync: the gap issue 01 closed — the mode gates the ack even
        // though the policy would not.
        let p = persistence_with_durability(true);
        assert!(!p.should_rollback(), "still the default `continue` policy");
        assert!(
            p.should_confirm(),
            "`sync` durability must gate the ack on its own"
        );

        // rollback + non-sync: the policy still selects `Confirm` on its own.
        let mut p = persistence_with_durability(false);
        p.set_failure_policy(rollback.clone());
        assert!(p.should_confirm());

        // rollback + sync: both, and the policy is unchanged by the mode.
        let mut p = persistence_with_durability(true);
        p.set_failure_policy(rollback);
        assert!(p.should_confirm());
        assert!(p.should_rollback());
    }

    fn persistence_with_sink(sink: crate::persistence::FakeWalSink) -> ShardPersistence {
        ShardPersistence::new(
            Some(Box::new(sink)),
            Arc::new(NoopSnapshotCoordinator::new()),
            Arc::new(std::sync::atomic::AtomicU8::new(
                WalFailurePolicy::default().as_u8(),
            )),
            false,
            None,
        )
    }

    fn policy_flag(policy: WalFailurePolicy) -> Arc<std::sync::atomic::AtomicU8> {
        Arc::new(std::sync::atomic::AtomicU8::new(policy.as_u8()))
    }

    // FM-PERSISTENCE-055
    /// The refusal needs *both* halves, and the shard must not invent a third.
    /// A poisoned WAL under `continue` keeps serving writes — that policy was
    /// chosen to trade durability for availability, and fencing it would
    /// silently promote every user of the default to fail-stop. A healthy WAL
    /// under `readonly` serves writes too: the fence is a consequence of a
    /// real loss, not of the setting.
    #[test]
    fn a_poisoned_shard_refuses_writes_only_under_a_refusing_policy() {
        // No WAL at all: nothing was promised, so nothing can be refused.
        let p = persistence();
        assert!(!p.wal_poisoned());
        assert!(!p.write_refused());

        let sink = crate::persistence::FakeWalSink::new(0);
        let mut p = persistence_with_sink(sink);
        assert!(!p.wal_poisoned(), "a fresh WAL is healthy");
        assert!(!p.write_refused());

        // Healthy WAL, refusing policy: still serving.
        p.set_failure_policy(policy_flag(WalFailurePolicy::Readonly));
        assert!(!p.write_refused(), "a healthy shard refuses nothing");

        // The loss is what fences the shard.
        let sink = crate::persistence::FakeWalSink::new(0);
        sink.poison();
        let mut p = persistence_with_sink(sink);
        assert!(p.wal_poisoned());
        assert!(
            !p.write_refused(),
            "`continue` acknowledges the loss and carries on — that is its whole point"
        );

        p.set_failure_policy(policy_flag(WalFailurePolicy::Rollback));
        assert!(p.write_refused(), "rollback fails a write it cannot undo");

        p.set_failure_policy(policy_flag(WalFailurePolicy::Readonly));
        assert!(p.write_refused());

        // The policy is live, so the fence lifts with it — the latch does not.
        p.set_failure_policy(policy_flag(WalFailurePolicy::Continue));
        assert!(!p.write_refused());
        assert!(p.wal_poisoned());
    }
}

#[cfg(test)]
mod scripting_tests {
    use super::*;

    #[test]
    fn defaults_have_no_executor_or_registry() {
        let s = ShardScripting::default();
        assert!(!s.has_executor());
        assert!(s.executor().is_none());
        assert!(s.function_registry().is_none());
    }

    #[test]
    fn set_and_take_executor_round_trip() {
        let executor = ScriptExecutor::new(ScriptingConfig::default())
            .expect("script executor initializes in tests");
        let mut s = ShardScripting::new(Some(executor), None);
        assert!(s.has_executor());

        let taken = s.take_executor().expect("executor present");
        assert!(!s.has_executor(), "take leaves the slot empty");

        s.set_executor(taken);
        assert!(s.has_executor(), "set restores the executor");
    }
}

#[cfg(test)]
mod cluster_tests {
    use super::*;

    #[test]
    fn standalone_has_no_cluster_handles() {
        let cluster = ShardCluster::new(None, None, None, None, None);
        assert!(!cluster.is_cluster_mode());
        assert_eq!(cluster.node_id(), None);
        assert!(cluster.raft().is_none());
        assert!(cluster.cluster_state().is_none());
        assert!(cluster.network_factory().is_none());
        assert!(cluster.quorum_checker().is_none());
        assert!(cluster.replication_tracker().is_none());
    }

    #[test]
    fn set_node_id_is_observed() {
        let mut cluster = ShardCluster::new(None, None, None, None, None);
        assert_eq!(cluster.node_id(), None);
        cluster.set_node_id(7);
        assert_eq!(cluster.node_id(), Some(7));
    }
}

/// A [`RoleController`](crate::command::RoleController) that answers with
/// whatever it was built with, so a test can assert that a surface *derives*
/// from the controller instead of keeping its own copy.
#[cfg(test)]
pub(crate) struct FixedRoleController {
    pub primary: Option<std::net::SocketAddr>,
    pub link_up: bool,
    pub refusal: Option<String>,
}

#[cfg(test)]
impl FixedRoleController {
    /// A controller that has refused nothing — the ordinary case.
    pub(crate) fn new(primary: Option<std::net::SocketAddr>, link_up: bool) -> Self {
        Self {
            primary,
            link_up,
            refusal: None,
        }
    }
}

#[cfg(test)]
impl crate::command::RoleController for FixedRoleController {
    fn request_promote(&self) -> Result<(), crate::error::CommandError> {
        Ok(())
    }
    fn request_demote(&self, _primary: std::net::SocketAddr) {}
    fn primary_target(&self) -> Option<std::net::SocketAddr> {
        self.primary
    }
    fn is_replica(&self) -> bool {
        // None of the `ShardIdentity` fixtures exercise the stranded-promotion
        // divergence (`primary_target() == None` while still flagged a
        // replica) — they only ever construct a genuine replica or a genuine
        // primary, for which this agrees with `primary.is_some()`.
        self.primary.is_some()
    }
    fn master_link_up(&self) -> bool {
        self.link_up
    }
    fn sync_refusal(&self) -> Option<String> {
        self.refusal.clone()
    }
}

#[cfg(test)]
mod identity_tests {
    use super::*;

    #[test]
    fn new_primary_defaults() {
        let id = ShardIdentity::new(3, 8, false);
        assert_eq!(id.shard_id(), 3);
        assert_eq!(id.num_shards(), 8);
        assert_eq!(id.shard_label(), "3");
        assert!(!id.is_replica());
        assert_eq!(id.master_host(), None);
        assert_eq!(id.master_port(), None);
        assert!(id.data_dir().is_none());
    }

    #[test]
    fn replica_flag_is_shared_and_toggleable() {
        let id = ShardIdentity::new(0, 1, true);
        assert!(id.is_replica());
        let shared = id.is_replica_flag().clone();
        id.set_is_replica(false);
        assert!(!id.is_replica());
        // The handed-out handle observes the same atomic.
        assert!(!shared.load(std::sync::atomic::Ordering::Relaxed));
    }

    /// `master_host`/`master_port` are derived live from the shared
    /// `RoleController` (the RoleManager), not a per-shard copy: wiring the
    /// controller is enough for both getters to report its current target.
    #[test]
    fn master_address_derives_from_role_controller() {
        let mut id = ShardIdentity::new(0, 1, false);
        let target: std::net::SocketAddr = "10.0.0.5:6390".parse().unwrap();
        id.set_role_controller(Arc::new(FixedRoleController::new(Some(target), false)));
        assert_eq!(id.master_host().as_deref(), Some("10.0.0.5"));
        assert_eq!(id.master_port(), Some(6390));
    }

    /// No role controller wired (e.g. a bare test harness) -> no master
    /// address, rather than a stale or fabricated one.
    #[test]
    fn master_address_absent_without_role_controller() {
        let id = ShardIdentity::new(0, 1, true);
        assert_eq!(id.master_host(), None);
        assert_eq!(id.master_port(), None);
    }

    // FM-REPLICATION-061
    /// The refusal is derived live from the role controller too, and is a
    /// *separate* fact from the link being down: a down-and-retrying link
    /// reports no refusal, and only a controller that has given up does.
    /// Copying it per shard would let one shard answer `INFO` with a stale
    /// "still trying" after the stream gave up.
    #[test]
    fn master_sync_error_derives_from_role_controller() {
        let target: std::net::SocketAddr = "10.0.0.5:6390".parse().unwrap();

        let mut retrying = ShardIdentity::new(0, 1, true);
        retrying.set_role_controller(Arc::new(FixedRoleController::new(Some(target), false)));
        assert_eq!(
            retrying.master_sync_error(),
            None,
            "a link that is down but still retrying has refused nothing"
        );

        let mut refused = ShardIdentity::new(0, 1, true);
        refused.set_role_controller(Arc::new(FixedRoleController {
            primary: Some(target),
            link_up: false,
            refusal: Some("shard-count mismatch: 4 vs 2".to_string()),
        }));
        assert_eq!(
            refused.master_sync_error().as_deref(),
            Some("shard-count mismatch: 4 vs 2")
        );

        assert_eq!(
            ShardIdentity::new(0, 1, true).master_sync_error(),
            None,
            "no controller wired -> nothing to report, not a fabricated reason"
        );
    }

    /// `master_link_up` is derived the same way as `master_host`/`master_port`
    /// — live from the role controller — and must not be conflated with
    /// merely having a target: a replica can know its primary's address while
    /// still mid-handshake or fully disconnected.
    #[test]
    fn master_link_up_derives_from_role_controller() {
        let mut id = ShardIdentity::new(0, 1, false);
        let target: std::net::SocketAddr = "10.0.0.5:6390".parse().unwrap();
        id.set_role_controller(Arc::new(FixedRoleController::new(Some(target), true)));
        assert!(id.master_link_up());

        let mut down = ShardIdentity::new(0, 1, false);
        down.set_role_controller(Arc::new(FixedRoleController::new(Some(target), false)));
        assert!(!down.master_link_up());
    }

    /// No role controller wired -> down, not a fabricated `up`.
    #[test]
    fn master_link_up_false_without_role_controller() {
        let id = ShardIdentity::new(0, 1, true);
        assert!(!id.master_link_up());
    }
}

#[cfg(test)]
mod tracking_tests {
    use super::*;

    #[test]
    fn default_has_no_tracking_clients() {
        let t = ShardTracking::default();
        assert!(!t.has_tracking_clients());
        assert!(!t.has_any_tracking_clients());
    }

    #[test]
    fn unregister_unknown_connection_is_noop() {
        let mut t = ShardTracking::default();
        t.unregister(999);
        assert!(!t.has_any_tracking_clients());
    }
}
