use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::Arc;

use bytes::Bytes;
use frogdb_protocol::Response;
use tokio::sync::mpsc;

use crate::cluster::{ClusterNetworkFactory, ClusterRaft, ClusterState};
use crate::command::QuorumChecker;
use crate::eviction::{EvictionConfig, EvictionPool};
use crate::functions::SharedFunctionRegistry;
use crate::latency::LatencyMonitor;
use crate::persistence::{
    NoopSnapshotCoordinator, RocksStore, RocksWalWriter, SnapshotCoordinator, WalConfig,
};
use crate::pubsub::ShardSubscriptions;
use crate::registry::CommandRegistry;
use crate::replication::{NoopBroadcaster, SharedBroadcaster};
use crate::scripting::{ScriptExecutor, ScriptingConfig};
use crate::slowlog::SlowLog;
use crate::store::HashMapStore;

use super::connection::NewConnection;
use super::counters::OperationCounters;
use super::message::ShardMessage;
use super::wait_queue::ShardWaitQueue;

/// A shard worker that owns a partition of the data.
pub struct ShardWorker {
    /// Shard ID.
    pub shard_id: usize,

    /// Total number of shards.
    pub num_shards: usize,

    /// Local data store.
    pub store: HashMapStore,

    /// Receiver for shard messages.
    pub message_rx: mpsc::Receiver<ShardMessage>,

    /// Receiver for new connections.
    pub new_conn_rx: mpsc::Receiver<NewConnection>,

    /// Senders to all shards (for cross-shard operations).
    pub shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,

    /// Command registry.
    pub registry: Arc<CommandRegistry>,

    /// Optional RocksDB store for persistence.
    pub rocks_store: Option<Arc<RocksStore>>,

    /// WAL writer for this shard.
    pub wal_writer: Option<RocksWalWriter>,

    /// Snapshot coordinator for BGSAVE.
    pub snapshot_coordinator: Arc<dyn SnapshotCoordinator>,

    /// Monotonically increasing version for WATCH detection.
    /// Reset to 0 on server restart.
    pub(crate) shard_version: u64,

    /// Pub/Sub subscriptions for this shard.
    pub(crate) subscriptions: ShardSubscriptions,

    /// Script executor for this shard.
    pub(crate) script_executor: Option<ScriptExecutor>,

    /// Function registry (shared across all shards).
    pub(crate) function_registry: Option<SharedFunctionRegistry>,

    /// Eviction configuration.
    pub(crate) eviction_config: EvictionConfig,

    /// Eviction pool for maintaining best eviction candidates.
    pub(crate) eviction_pool: EvictionPool,

    /// Current memory limit for this shard (0 = unlimited).
    /// This is maxmemory / num_shards.
    pub(crate) memory_limit: u64,

    /// Metrics recorder for observability.
    pub(crate) metrics_recorder: Arc<dyn crate::noop::MetricsRecorder>,

    /// Peak memory usage for this shard (high-water mark).
    pub(crate) peak_memory: u64,

    /// Wait queue for blocking commands.
    pub(crate) wait_queue: ShardWaitQueue,

    /// Slow query log for this shard.
    pub(crate) slowlog: SlowLog,

    /// Latency monitor for this shard.
    pub(crate) latency_monitor: LatencyMonitor,

    /// VLL intent table for tracking pending key access.
    pub(crate) intent_table: Option<crate::vll::IntentTable>,

    /// VLL transaction queue for ordering operations.
    pub(crate) tx_queue: Option<crate::vll::TransactionQueue>,

    /// VLL continuation lock (for MULTI/EXEC and Lua scripts).
    pub(crate) continuation_lock: Option<crate::vll::ContinuationLock>,

    /// Pending release receiver for continuation lock.
    /// When Some, we poll this to detect when the lock should be released.
    pub(crate) pending_continuation_release: Option<tokio::sync::oneshot::Receiver<()>>,

    /// Replication broadcaster for streaming writes to replicas.
    pub(crate) replication_broadcaster: SharedBroadcaster,

    /// Optional Raft instance for cluster command execution.
    pub(crate) raft: Option<Arc<ClusterRaft>>,

    /// Optional cluster state for cluster commands and routing.
    pub(crate) cluster_state: Option<Arc<ClusterState>>,

    /// This node's ID (for cluster mode).
    pub(crate) node_id: Option<u64>,

    /// Operation counters for hot shard detection.
    pub(crate) operation_counters: OperationCounters,

    /// Current queue depth (messages waiting to be processed).
    /// Shared so it can be read from outside the worker.
    pub(crate) queue_depth: Arc<AtomicUsize>,

    /// Optional network factory for cluster node management.
    pub(crate) network_factory: Option<Arc<ClusterNetworkFactory>>,

    /// Optional quorum checker for local cluster health detection.
    pub(crate) quorum_checker: Option<Arc<dyn QuorumChecker>>,
}

impl ShardWorker {
    /// Create a new shard worker without persistence.
    pub fn new(
        shard_id: usize,
        num_shards: usize,
        message_rx: mpsc::Receiver<ShardMessage>,
        new_conn_rx: mpsc::Receiver<NewConnection>,
        shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
        registry: Arc<CommandRegistry>,
    ) -> Self {
        Self::with_eviction(
            shard_id,
            num_shards,
            message_rx,
            new_conn_rx,
            shard_senders,
            registry,
            EvictionConfig::default(),
            Arc::new(crate::noop::NoopMetricsRecorder::new()),
            Arc::new(AtomicU64::new(0)),
            Arc::new(NoopBroadcaster),
        )
    }

    /// Create a new shard worker without persistence but with eviction config.
    #[allow(clippy::too_many_arguments)]
    pub fn with_eviction(
        shard_id: usize,
        num_shards: usize,
        message_rx: mpsc::Receiver<ShardMessage>,
        new_conn_rx: mpsc::Receiver<NewConnection>,
        shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
        registry: Arc<CommandRegistry>,
        eviction_config: EvictionConfig,
        metrics_recorder: Arc<dyn crate::noop::MetricsRecorder>,
        slowlog_next_id: Arc<AtomicU64>,
        replication_broadcaster: SharedBroadcaster,
    ) -> Self {
        // Try to create script executor
        let script_executor = ScriptExecutor::new(ScriptingConfig::default())
            .map_err(|e| {
                tracing::warn!(shard_id, error = %e, "Failed to initialize script executor");
            })
            .ok();

        // Calculate per-shard memory limit
        let memory_limit = if eviction_config.maxmemory > 0 {
            eviction_config.maxmemory / num_shards as u64
        } else {
            0
        };

        Self {
            shard_id,
            num_shards,
            store: HashMapStore::new(),
            message_rx,
            new_conn_rx,
            shard_senders,
            registry,
            rocks_store: None,
            wal_writer: None,
            snapshot_coordinator: Arc::new(NoopSnapshotCoordinator::new()),
            shard_version: 0,
            subscriptions: ShardSubscriptions::new(),
            script_executor,
            function_registry: None,
            eviction_config,
            eviction_pool: EvictionPool::new(),
            memory_limit,
            metrics_recorder,
            peak_memory: 0,
            wait_queue: ShardWaitQueue::new(),
            slowlog: SlowLog::new(
                crate::slowlog::DEFAULT_SLOWLOG_MAX_LEN,
                crate::slowlog::DEFAULT_SLOWLOG_MAX_ARG_LEN,
                slowlog_next_id,
            ),
            latency_monitor: LatencyMonitor::default_monitor(),
            intent_table: None,
            tx_queue: None,
            continuation_lock: None,
            pending_continuation_release: None,
            replication_broadcaster,
            raft: None,
            cluster_state: None,
            node_id: None,
            operation_counters: OperationCounters::new(),
            queue_depth: Arc::new(AtomicUsize::new(0)),
            network_factory: None,
            quorum_checker: None,
        }
    }

    /// Create a new shard worker with persistence.
    #[allow(clippy::too_many_arguments)]
    pub fn with_persistence(
        shard_id: usize,
        num_shards: usize,
        store: HashMapStore,
        message_rx: mpsc::Receiver<ShardMessage>,
        new_conn_rx: mpsc::Receiver<NewConnection>,
        shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
        registry: Arc<CommandRegistry>,
        rocks_store: Arc<RocksStore>,
        wal_config: WalConfig,
        snapshot_coordinator: Arc<dyn SnapshotCoordinator>,
        eviction_config: EvictionConfig,
        metrics_recorder: Arc<dyn crate::noop::MetricsRecorder>,
        slowlog_next_id: Arc<AtomicU64>,
        replication_broadcaster: SharedBroadcaster,
    ) -> Self {
        let wal_writer = RocksWalWriter::new(
            rocks_store.clone(),
            shard_id,
            wal_config,
            metrics_recorder.clone(),
        );

        // Try to create script executor
        let script_executor = ScriptExecutor::new(ScriptingConfig::default())
            .map_err(|e| {
                tracing::warn!(shard_id, error = %e, "Failed to initialize script executor");
            })
            .ok();

        // Calculate per-shard memory limit
        let memory_limit = if eviction_config.maxmemory > 0 {
            eviction_config.maxmemory / num_shards as u64
        } else {
            0
        };

        Self {
            shard_id,
            num_shards,
            store,
            message_rx,
            new_conn_rx,
            shard_senders,
            registry,
            rocks_store: Some(rocks_store),
            wal_writer: Some(wal_writer),
            snapshot_coordinator,
            shard_version: 0,
            subscriptions: ShardSubscriptions::new(),
            script_executor,
            function_registry: None,
            eviction_config,
            eviction_pool: EvictionPool::new(),
            memory_limit,
            metrics_recorder,
            peak_memory: 0,
            wait_queue: ShardWaitQueue::new(),
            slowlog: SlowLog::new(
                crate::slowlog::DEFAULT_SLOWLOG_MAX_LEN,
                crate::slowlog::DEFAULT_SLOWLOG_MAX_ARG_LEN,
                slowlog_next_id,
            ),
            latency_monitor: LatencyMonitor::default_monitor(),
            intent_table: None,
            tx_queue: None,
            continuation_lock: None,
            pending_continuation_release: None,
            replication_broadcaster,
            raft: None,
            cluster_state: None,
            node_id: None,
            operation_counters: OperationCounters::new(),
            queue_depth: Arc::new(AtomicUsize::new(0)),
            network_factory: None,
            quorum_checker: None,
        }
    }

    /// Set the function registry for this shard.
    pub fn set_function_registry(&mut self, registry: SharedFunctionRegistry) {
        self.function_registry = Some(registry);
    }

    /// Set the replication broadcaster for this shard.
    pub fn set_replication_broadcaster(&mut self, broadcaster: SharedBroadcaster) {
        self.replication_broadcaster = broadcaster;
    }

    /// Set the Raft instance for cluster commands.
    pub fn set_raft(&mut self, raft: Arc<ClusterRaft>) {
        self.raft = Some(raft);
    }

    /// Set the cluster state for cluster commands.
    pub fn set_cluster_state(&mut self, cluster_state: Arc<ClusterState>) {
        self.cluster_state = Some(cluster_state);
    }

    /// Set this node's ID for cluster mode.
    pub fn set_node_id(&mut self, node_id: u64) {
        self.node_id = Some(node_id);
    }

    /// Set the network factory for cluster node management.
    pub fn set_network_factory(&mut self, network_factory: Arc<ClusterNetworkFactory>) {
        self.network_factory = Some(network_factory);
    }

    /// Set the quorum checker for local cluster health detection.
    pub fn set_quorum_checker(&mut self, quorum_checker: Arc<dyn QuorumChecker>) {
        self.quorum_checker = Some(quorum_checker);
    }

    /// Get the snapshot coordinator.
    pub fn snapshot_coordinator(&self) -> &Arc<dyn SnapshotCoordinator> {
        &self.snapshot_coordinator
    }

    /// Increment shard version (call on any write operation).
    pub(crate) fn increment_version(&mut self) {
        self.shard_version = self.shard_version.wrapping_add(1);
    }

    /// Get version for a key.
    ///
    /// Phase 1: Returns per-shard version (simple, some false positives).
    /// Phase 2 (future): Can be changed to return per-key version.
    pub(crate) fn get_key_version(&self, _key: &[u8]) -> u64 {
        self.shard_version
    }

    /// Check if watched keys have changed since they were watched.
    pub(crate) fn check_watches(&self, watches: &[(Bytes, u64)]) -> bool {
        watches
            .iter()
            .all(|(key, watched_ver)| self.get_key_version(key) == *watched_ver)
    }

    /// Check if this connection can execute during a continuation lock.
    ///
    /// When a continuation lock is held, only the lock owner can execute commands.
    /// Returns Ok(()) if execution is allowed, Err(Response) otherwise.
    pub(crate) fn can_execute_during_lock(&self, conn_id: u64) -> Result<(), Response> {
        if let Some(ref lock) = self.continuation_lock {
            if lock.conn_id != conn_id {
                return Err(Response::error("ERR shard busy with continuation lock"));
            }
        }
        Ok(())
    }
}
