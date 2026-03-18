use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize};

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
use super::types::{
    ShardCluster, ShardEviction, ShardIdentity, ShardObservability, ShardPersistence, ShardVll,
};
use super::wait_queue::ShardWaitQueue;

/// A shard worker that owns a partition of the data.
pub struct ShardWorker {
    /// Immutable shard identity.
    pub(crate) identity: ShardIdentity,

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

    /// Monotonically increasing version for WATCH detection.
    pub(crate) shard_version: u64,

    /// Persistence: RocksDB, WAL, snapshots.
    pub(crate) persistence: ShardPersistence,

    /// Observability: metrics, slowlog, latency, counters.
    pub(crate) observability: ShardObservability,

    /// Memory management: eviction config, pool, memory limit.
    pub(crate) eviction: ShardEviction,

    /// VLL: intent table, tx queue, continuation lock.
    pub(crate) vll: ShardVll,

    /// Cluster: raft, cluster state, node ID, network factory.
    pub(crate) cluster: ShardCluster,

    /// Pub/Sub subscriptions for this shard.
    pub(crate) subscriptions: ShardSubscriptions,

    /// Client tracking: invalidation registry (conn_id → sender + metadata).
    pub(crate) invalidation_registry: crate::tracking::InvalidationRegistry,

    /// Client tracking: key → interested connections table.
    pub(crate) tracking_table: crate::tracking::TrackingTable,

    /// BCAST tracking: prefix → interested connections table.
    pub(crate) broadcast_table: crate::tracking::BroadcastTable,

    /// Script executor for this shard.
    pub(crate) script_executor: Option<ScriptExecutor>,

    /// Function registry (shared across all shards).
    pub(crate) function_registry: Option<SharedFunctionRegistry>,

    /// Wait queue for blocking commands.
    pub(crate) wait_queue: ShardWaitQueue,

    /// Replication broadcaster for streaming writes to replicas.
    pub(crate) replication_broadcaster: SharedBroadcaster,

    /// Whether per-request tracing spans are enabled.
    pub per_request_spans: Arc<AtomicBool>,

    /// Whether active key expiry is paused (true during CLIENT PAUSE ALL).
    pub(crate) expiry_paused: Arc<AtomicBool>,

    /// Per-shard search indexes (index_name -> ShardSearchIndex).
    pub search_indexes: std::collections::HashMap<String, frogdb_search::ShardSearchIndex>,

    /// Search index aliases (alias_name -> index_name).
    pub index_aliases: std::collections::HashMap<String, String>,

    /// Search dictionaries for FT.SPELLCHECK (dict_name -> terms).
    pub search_dictionaries: std::collections::HashMap<String, std::collections::HashSet<String>>,

    /// Search configuration parameters (param_name -> value).
    pub search_config: std::collections::HashMap<String, String>,
}

impl ShardWorker {
    /// Get the shard ID.
    pub fn shard_id(&self) -> usize {
        self.identity.shard_id
    }

    /// Get the total number of shards.
    pub fn num_shards(&self) -> usize {
        self.identity.num_shards
    }

    /// Get the data directory for this server.
    pub fn data_dir(&self) -> std::path::PathBuf {
        self.identity
            .data_dir
            .clone()
            .unwrap_or_else(|| std::path::PathBuf::from("data"))
    }

    /// Set the data directory.
    pub fn set_data_dir(&mut self, dir: std::path::PathBuf) {
        self.identity.data_dir = Some(dir);
    }

    /// Set whether this shard belongs to a replica server.
    pub fn set_is_replica(&mut self, is_replica: bool) {
        self.identity
            .is_replica
            .store(is_replica, std::sync::atomic::Ordering::Relaxed);
    }

    /// Get a shared handle to the is_replica flag.
    pub fn is_replica_flag(&self) -> Arc<AtomicBool> {
        self.identity.is_replica.clone()
    }

    /// Replace this shard's is_replica flag with a shared one.
    ///
    /// This allows all shards, the acceptor, and connection handlers to share
    /// a single `Arc<AtomicBool>` so that `REPLICAOF NO ONE` can toggle replica
    /// status server-wide with a single atomic store.
    pub fn set_is_replica_flag(&mut self, flag: Arc<AtomicBool>) {
        self.identity.is_replica = flag;
    }

    /// Replace this shard's expiry_paused flag with a shared one from the ClientRegistry.
    pub fn set_expiry_paused_flag(&mut self, flag: Arc<AtomicBool>) {
        self.expiry_paused = flag;
    }

    /// Replace this shard's WAL failure policy flag with a shared one from ConfigManager.
    pub fn set_wal_failure_policy_flag(&mut self, flag: Arc<AtomicU8>) {
        self.persistence.failure_policy = flag;
    }

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
            identity: ShardIdentity {
                shard_id,
                num_shards,
                is_replica: Arc::new(AtomicBool::new(false)),
                master_host: None,
                master_port: None,
                data_dir: None,
            },
            store: HashMapStore::new(),
            message_rx,
            new_conn_rx,
            shard_senders,
            registry,
            shard_version: 0,
            persistence: ShardPersistence {
                rocks_store: None,
                wal_writer: None,
                snapshot_coordinator: Arc::new(NoopSnapshotCoordinator::new()),
                failure_policy: Arc::new(AtomicU8::new(0)),
            },
            observability: ShardObservability {
                metrics_recorder,
                slowlog: SlowLog::new(
                    crate::slowlog::DEFAULT_SLOWLOG_MAX_LEN,
                    crate::slowlog::DEFAULT_SLOWLOG_MAX_ARG_LEN,
                    slowlog_next_id,
                ),
                latency_monitor: LatencyMonitor::default_monitor(),
                operation_counters: OperationCounters::new(),
                queue_depth: Arc::new(AtomicUsize::new(0)),
                peak_memory: 0,
                evicted_keys: 0,
            },
            eviction: ShardEviction {
                config: eviction_config,
                pool: EvictionPool::new(),
                memory_limit,
            },
            vll: ShardVll {
                intent_table: None,
                tx_queue: None,
                continuation_lock: None,
                pending_continuation_release: None,
            },
            cluster: ShardCluster {
                raft: None,
                cluster_state: None,
                node_id: None,
                network_factory: None,
                quorum_checker: None,
                replication_tracker: None,
            },
            subscriptions: ShardSubscriptions::new(),
            invalidation_registry: crate::tracking::InvalidationRegistry::default(),
            tracking_table: crate::tracking::TrackingTable::new(
                crate::tracking::DEFAULT_TRACKING_TABLE_MAX_KEYS,
            ),
            broadcast_table: crate::tracking::BroadcastTable::default(),
            script_executor,
            function_registry: None,
            wait_queue: ShardWaitQueue::new(),
            replication_broadcaster,
            per_request_spans: Arc::new(AtomicBool::new(false)),
            expiry_paused: Arc::new(AtomicBool::new(false)),
            search_indexes: std::collections::HashMap::new(),
            index_aliases: std::collections::HashMap::new(),
            search_dictionaries: std::collections::HashMap::new(),
            search_config: std::collections::HashMap::new(),
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
            identity: ShardIdentity {
                shard_id,
                num_shards,
                is_replica: Arc::new(AtomicBool::new(false)),
                master_host: None,
                master_port: None,
                data_dir: None,
            },
            store,
            message_rx,
            new_conn_rx,
            shard_senders,
            registry,
            shard_version: 0,
            persistence: ShardPersistence {
                rocks_store: Some(rocks_store),
                wal_writer: Some(wal_writer),
                snapshot_coordinator,
                failure_policy: Arc::new(AtomicU8::new(0)),
            },
            observability: ShardObservability {
                metrics_recorder,
                slowlog: SlowLog::new(
                    crate::slowlog::DEFAULT_SLOWLOG_MAX_LEN,
                    crate::slowlog::DEFAULT_SLOWLOG_MAX_ARG_LEN,
                    slowlog_next_id,
                ),
                latency_monitor: LatencyMonitor::default_monitor(),
                operation_counters: OperationCounters::new(),
                queue_depth: Arc::new(AtomicUsize::new(0)),
                peak_memory: 0,
                evicted_keys: 0,
            },
            eviction: ShardEviction {
                config: eviction_config,
                pool: EvictionPool::new(),
                memory_limit,
            },
            vll: ShardVll {
                intent_table: None,
                tx_queue: None,
                continuation_lock: None,
                pending_continuation_release: None,
            },
            cluster: ShardCluster {
                raft: None,
                cluster_state: None,
                node_id: None,
                network_factory: None,
                quorum_checker: None,
                replication_tracker: None,
            },
            subscriptions: ShardSubscriptions::new(),
            invalidation_registry: crate::tracking::InvalidationRegistry::default(),
            tracking_table: crate::tracking::TrackingTable::new(
                crate::tracking::DEFAULT_TRACKING_TABLE_MAX_KEYS,
            ),
            broadcast_table: crate::tracking::BroadcastTable::default(),
            script_executor,
            function_registry: None,
            wait_queue: ShardWaitQueue::new(),
            replication_broadcaster,
            per_request_spans: Arc::new(AtomicBool::new(false)),
            expiry_paused: Arc::new(AtomicBool::new(false)),
            search_indexes: std::collections::HashMap::new(),
            index_aliases: std::collections::HashMap::new(),
            search_dictionaries: std::collections::HashMap::new(),
            search_config: std::collections::HashMap::new(),
        }
    }

    /// Replace the script executor with one using the given scripting config.
    pub fn set_scripting_config(&mut self, config: ScriptingConfig) {
        match ScriptExecutor::new(config) {
            Ok(executor) => self.script_executor = Some(executor),
            Err(e) => {
                tracing::warn!(
                    shard_id = self.identity.shard_id,
                    error = %e,
                    "Failed to reinitialize script executor with new config"
                );
            }
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
        self.cluster.raft = Some(raft);
    }

    /// Set the cluster state for cluster commands.
    pub fn set_cluster_state(&mut self, cluster_state: Arc<ClusterState>) {
        self.cluster.cluster_state = Some(cluster_state);
    }

    /// Set this node's ID for cluster mode.
    pub fn set_node_id(&mut self, node_id: u64) {
        self.cluster.node_id = Some(node_id);
    }

    /// Set the network factory for cluster node management.
    pub fn set_network_factory(&mut self, network_factory: Arc<ClusterNetworkFactory>) {
        self.cluster.network_factory = Some(network_factory);
    }

    /// Set the quorum checker for local cluster health detection.
    pub fn set_quorum_checker(&mut self, quorum_checker: Arc<dyn QuorumChecker>) {
        self.cluster.quorum_checker = Some(quorum_checker);
    }

    /// Set the replication tracker for INFO replication / WAIT support.
    pub fn set_replication_tracker(
        &mut self,
        tracker: Arc<crate::replication::ReplicationTrackerImpl>,
    ) {
        self.cluster.replication_tracker = Some(tracker);
    }

    /// Set the primary address for INFO replication (replica mode).
    pub fn set_master_address(&mut self, host: String, port: u16) {
        self.identity.master_host = Some(host);
        self.identity.master_port = Some(port);
    }

    /// Get the snapshot coordinator.
    pub fn snapshot_coordinator(&self) -> &Arc<dyn SnapshotCoordinator> {
        &self.persistence.snapshot_coordinator
    }

    /// Increment shard version (call on any write operation).
    pub(crate) fn increment_version(&mut self) {
        self.shard_version = self.shard_version.wrapping_add(1);
    }

    /// Get version for a key.
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
    #[allow(clippy::result_large_err)]
    pub(crate) fn can_execute_during_lock(&self, conn_id: u64) -> Result<(), Response> {
        if let Some(ref lock) = self.vll.continuation_lock
            && lock.conn_id != conn_id
        {
            return Err(Response::error("ERR shard busy with continuation lock"));
        }
        Ok(())
    }
}
