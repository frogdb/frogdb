use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};

use crate::cluster::{ClusterNetworkFactory, ClusterRaft, ClusterState};
use crate::command::QuorumChecker;
use crate::eviction::{
    EvictionCandidate, EvictionConfig, EvictionPolicy, EvictionPool, EvictionRanker,
};
use crate::functions::SharedFunctionRegistry;
use crate::latency::LatencyMonitor;
use crate::persistence::{RocksStore, SnapshotCoordinator, WalFailurePolicy, WalSink};
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
        self.is_replica.load(std::sync::atomic::Ordering::Relaxed)
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
    queue_depth: Arc<AtomicUsize>,
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
        queue_depth: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            metrics_recorder,
            keyspace_stats: Arc::new(crate::KeyspaceStats::new()),
            slowlog,
            latency_monitor: LatencyMonitor::default_monitor(),
            operation_counters: OperationCounters::new(),
            queue_depth,
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

    /// Current shard queue depth (shared with the connection layer).
    pub(crate) fn queue_depth(&self) -> usize {
        self.queue_depth.load(std::sync::atomic::Ordering::Relaxed)
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
}

impl ShardPersistence {
    pub(crate) fn new(
        wal_writer: Option<Box<dyn WalSink>>,
        snapshot_coordinator: Arc<dyn SnapshotCoordinator>,
        failure_policy: Arc<std::sync::atomic::AtomicU8>,
    ) -> Self {
        Self {
            wal_writer,
            snapshot_coordinator,
            failure_policy,
        }
    }

    /// The WAL writer for this shard, if persistence is enabled.
    pub(crate) fn wal_writer(&self) -> Option<&dyn WalSink> {
        self.wal_writer.as_deref()
    }

    /// Returns true if a WAL writer is configured for this shard.
    pub(crate) fn has_wal(&self) -> bool {
        self.wal_writer.is_some()
    }

    /// The snapshot coordinator (BGSAVE) for this shard.
    pub(crate) fn snapshot_coordinator(&self) -> &Arc<dyn SnapshotCoordinator> {
        &self.snapshot_coordinator
    }

    /// Replace the shared WAL failure-policy flag (from ConfigManager).
    pub(crate) fn set_failure_policy(&mut self, flag: Arc<std::sync::atomic::AtomicU8>) {
        self.failure_policy = flag;
    }

    /// Returns true if the WAL failure policy is set to Rollback.
    pub(crate) fn should_rollback(&self) -> bool {
        WalFailurePolicy::from_u8(
            self.failure_policy
                .load(std::sync::atomic::Ordering::Relaxed),
        ) == WalFailurePolicy::Rollback
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

/// Result from a shard for scatter-gather operations.
#[derive(Debug, Default)]
pub struct PartialResult {
    /// Results keyed by original key position.
    pub results: Vec<(Bytes, Response)>,
    /// Typed payload for the FT.* query fan-outs (search hits / partial
    /// aggregates); `None` for every other scatter op.
    pub ft: Option<frogdb_search::FtShardReply>,
}

impl PartialResult {
    /// A conventional keyed-response reply.
    pub fn from_results(results: Vec<(Bytes, Response)>) -> Self {
        Self { results, ft: None }
    }

    /// A typed FT.* reply (no keyed responses).
    pub fn from_ft(reply: frogdb_search::FtShardReply) -> Self {
        Self {
            results: Vec::new(),
            ft: Some(reply),
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
    use std::sync::atomic::AtomicUsize;

    fn observability() -> ShardObservability {
        let slowlog = SlowLog::new(
            crate::slowlog::DEFAULT_SLOWLOG_MAX_LEN,
            crate::slowlog::DEFAULT_SLOWLOG_MAX_ARG_LEN,
            Arc::new(AtomicU64::new(0)),
        );
        ShardObservability::new(
            Arc::new(crate::noop::NoopMetricsRecorder::new()),
            slowlog,
            Arc::new(AtomicUsize::new(0)),
        )
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
        ShardPersistence::new(
            None,
            Arc::new(NoopSnapshotCoordinator::new()),
            Arc::new(std::sync::atomic::AtomicU8::new(
                WalFailurePolicy::default().as_u8(),
            )),
        )
    }

    #[test]
    fn no_wal_without_writer() {
        let p = persistence();
        assert!(!p.has_wal());
        assert!(p.wal_writer().is_none());
    }

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

#[cfg(test)]
pub(crate) struct FixedRoleController(pub Option<std::net::SocketAddr>);

#[cfg(test)]
impl crate::command::RoleController for FixedRoleController {
    fn request_promote(&self) {}
    fn request_demote(&self, _primary: std::net::SocketAddr) {}
    fn primary_target(&self) -> Option<std::net::SocketAddr> {
        self.0
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
        id.set_role_controller(Arc::new(FixedRoleController(Some(target))));
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
