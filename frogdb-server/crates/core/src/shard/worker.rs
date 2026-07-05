use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, AtomicUsize};

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
    WalFailurePolicy,
};
use crate::pubsub::ShardSubscriptions;
use crate::registry::CommandRegistry;
use crate::replication::{NoopBroadcaster, SharedBroadcaster};
use crate::scripting::{ScriptExecutor, ScriptingConfig};
use crate::slowlog::SlowLog;
use crate::store::HashMapStore;

use super::active_expiry::ActiveExpiryCoordinator;
use super::connection::NewConnection;
use super::counters::OperationCounters;
use super::keyspace_coordinator::KeyspaceNotificationCoordinator;
use super::message::{ShardReceiver, ShardSender};
use super::search::lifecycle::IndexLifecycleManager;
use super::types::{
    ShardCluster, ShardEviction, ShardIdentity, ShardObservability, ShardPersistence,
    ShardScripting, ShardTracking, ShardVll,
};
use super::wait_queue::ShardWaitQueue;

/// A shard worker that owns a partition of the data.
pub struct ShardWorker {
    /// Immutable shard identity.
    pub(crate) identity: ShardIdentity,

    /// Local data store.
    pub store: HashMapStore,

    /// Receiver for shard messages.
    pub(crate) message_rx: ShardReceiver,

    /// Receiver for new connections.
    pub(crate) new_conn_rx: mpsc::Receiver<NewConnection>,

    /// Senders to all shards (for cross-shard operations).
    pub(crate) shard_senders: Arc<Vec<ShardSender>>,

    /// Command registry.
    pub(crate) registry: Arc<CommandRegistry>,

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

    /// Owns the emit→subscriber routing decision for keyspace notifications:
    /// broadcast subscribers register on the coordinator shard (shard 0), so an
    /// event emitted on the key-owner shard is routed there instead of into the
    /// emitting shard's own (subscriber-less) table.
    pub(crate) keyspace_notify: KeyspaceNotificationCoordinator,

    /// Client tracking: invalidation registry, tracking table, broadcast table.
    pub(crate) tracking: ShardTracking,

    /// Scripting: Lua script executor, function registry.
    pub(crate) scripting: ShardScripting,

    /// Wait queue for blocking commands.
    pub(crate) wait_queue: ShardWaitQueue,

    /// Replication broadcaster for streaming writes to replicas.
    pub(crate) replication_broadcaster: SharedBroadcaster,

    /// Whether per-request tracing spans are enabled.
    pub(crate) per_request_spans: Arc<AtomicBool>,

    /// Whether active key expiry is paused (true during CLIENT PAUSE ALL).
    pub(crate) expiry_paused: Arc<AtomicBool>,

    /// Shared keyspace notification event flags (from CONFIG notify-keyspace-events).
    /// Zero means disabled. Read atomically from the shard worker on every write.
    pub(crate) notify_keyspace_events: Arc<AtomicU32>,

    /// Whether active expiry is disabled via DEBUG SET-ACTIVE-EXPIRE 0.
    pub(crate) debug_active_expire_disabled: bool,

    /// Search: indexes, aliases, dictionaries, config.
    pub(crate) search: IndexLifecycleManager,

    /// Active-expiry decision + deletion engine (TTL key sweep + hash field
    /// sweep under a time budget). Side effects are applied shard-side from the
    /// returned `ExpiryResult`.
    pub(crate) expiry: ActiveExpiryCoordinator,
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
        self.search.set_data_dir(dir.clone());
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

    /// Set the shared per-shard memory usage vec.
    /// Used by SystemMetricsCollector to compute fragmentation ratio.
    pub fn set_shard_memory_used(&mut self, shared: Arc<Vec<AtomicU64>>) {
        self.observability.shard_memory_used = Some(shared);
    }

    /// Share the process-wide keyspace hit/miss accumulator with this worker.
    ///
    /// The same `Arc` is held by the server so `INFO stats` reads it and
    /// `CONFIG RESETSTAT` advances its baseline.
    pub fn set_keyspace_stats(&mut self, stats: Arc<crate::KeyspaceStats>) {
        self.observability.keyspace_stats = stats;
    }

    /// Build a fully-populated [`CommandContext`](crate::command::CommandContext)
    /// for executing a command against this shard's local store.
    ///
    /// This is the single place that wires a command context from the shard
    /// worker. Cross-shard senders, cluster/replication handles, replica
    /// identity (`is_replica` / `master_host` / `master_port`), and the command
    /// registry are all sourced from `self` here — so every command-execution
    /// seam (normal dispatch, EVAL / EVALSHA / FCALL, and cross-shard script
    /// sub-commands) observes the *same* context and cannot drift out of sync
    /// (e.g. a Lua script reporting the wrong replica role via ROLE / INFO).
    pub(crate) fn command_context(
        &mut self,
        conn_id: u64,
        protocol_version: frogdb_protocol::ProtocolVersion,
    ) -> crate::command::CommandContext<'_> {
        // Prefer the dynamic self_node_id from ClusterState (updated by HARD
        // reset) over the static node_id captured at connection creation time.
        let node_id = self
            .cluster
            .cluster_state
            .as_ref()
            .and_then(|cs| cs.self_node_id())
            .or(self.cluster.node_id);
        let is_replica = self
            .identity
            .is_replica
            .load(std::sync::atomic::Ordering::Relaxed);

        crate::command::CommandContext {
            store: &mut self.store,
            shard_senders: &self.shard_senders,
            shard_id: self.identity.shard_id,
            num_shards: self.identity.num_shards,
            conn_id,
            protocol_version,
            replication_tracker: self.cluster.replication_tracker.as_ref(),
            cluster_state: self.cluster.cluster_state.as_ref(),
            node_id,
            raft: self.cluster.raft.as_ref(),
            network_factory: self.cluster.network_factory.as_ref(),
            quorum_checker: self.cluster.quorum_checker.as_ref().map(|q| q.as_ref()),
            command_registry: Some(&self.registry),
            is_replica,
            is_replica_flag: Some(self.identity.is_replica.clone()),
            master_host: self.identity.master_host.clone(),
            master_port: self.identity.master_port,
            dirty_delta: 0,
            lazyfreed_delta: 0,
            keyspace_hits: 0,
            keyspace_misses: 0,
        }
    }

    /// Create a new shard worker without persistence.
    pub fn new(
        shard_id: usize,
        num_shards: usize,
        message_rx: ShardReceiver,
        new_conn_rx: mpsc::Receiver<NewConnection>,
        shard_senders: Arc<Vec<ShardSender>>,
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
        message_rx: ShardReceiver,
        new_conn_rx: mpsc::Receiver<NewConnection>,
        shard_senders: Arc<Vec<ShardSender>>,
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

        // Decide keyspace-notification routing once: Local on shard 0 / single
        // shard, else forward to the coordinator shard's mailbox.
        let keyspace_notify = KeyspaceNotificationCoordinator::new(
            shard_id,
            num_shards,
            &shard_senders,
            metrics_recorder.clone(),
        );

        Self {
            identity: ShardIdentity {
                shard_id,
                num_shards,
                shard_label: shard_id.to_string(),
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
                failure_policy: Arc::new(AtomicU8::new(WalFailurePolicy::default().as_u8())),
            },
            observability: ShardObservability {
                metrics_recorder,
                keyspace_stats: Arc::new(crate::KeyspaceStats::new()),
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
                lazyfreed_objects: 0,
                shard_memory_used: None,
            },
            eviction: ShardEviction {
                config: eviction_config,
                pool: EvictionPool::new(),
                memory_limit,
            },
            vll: ShardVll::default(),
            cluster: ShardCluster {
                raft: None,
                cluster_state: None,
                node_id: None,
                network_factory: None,
                quorum_checker: None,
                replication_tracker: None,
            },
            subscriptions: ShardSubscriptions::new(),
            keyspace_notify,
            tracking: ShardTracking::default(),
            scripting: ShardScripting {
                executor: script_executor,
                ..Default::default()
            },
            wait_queue: ShardWaitQueue::new(),
            replication_broadcaster,
            per_request_spans: Arc::new(AtomicBool::new(false)),
            expiry_paused: Arc::new(AtomicBool::new(false)),
            notify_keyspace_events: Arc::new(AtomicU32::new(0)),
            debug_active_expire_disabled: false,
            search: IndexLifecycleManager::new(shard_id, std::path::PathBuf::from("data"), None),
            expiry: ActiveExpiryCoordinator::default(),
        }
    }

    /// Create a new shard worker with persistence.
    #[allow(clippy::too_many_arguments)]
    pub fn with_persistence(
        shard_id: usize,
        num_shards: usize,
        store: HashMapStore,
        message_rx: ShardReceiver,
        new_conn_rx: mpsc::Receiver<NewConnection>,
        shard_senders: Arc<Vec<ShardSender>>,
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

        let search = IndexLifecycleManager::new(
            shard_id,
            std::path::PathBuf::from("data"),
            Some(rocks_store.clone()),
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

        // Decide keyspace-notification routing once: Local on shard 0 / single
        // shard, else forward to the coordinator shard's mailbox.
        let keyspace_notify = KeyspaceNotificationCoordinator::new(
            shard_id,
            num_shards,
            &shard_senders,
            metrics_recorder.clone(),
        );

        Self {
            identity: ShardIdentity {
                shard_id,
                num_shards,
                shard_label: shard_id.to_string(),
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
                failure_policy: Arc::new(AtomicU8::new(WalFailurePolicy::default().as_u8())),
            },
            observability: ShardObservability {
                metrics_recorder,
                keyspace_stats: Arc::new(crate::KeyspaceStats::new()),
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
                lazyfreed_objects: 0,
                shard_memory_used: None,
            },
            eviction: ShardEviction {
                config: eviction_config,
                pool: EvictionPool::new(),
                memory_limit,
            },
            vll: ShardVll::default(),
            cluster: ShardCluster {
                raft: None,
                cluster_state: None,
                node_id: None,
                network_factory: None,
                quorum_checker: None,
                replication_tracker: None,
            },
            subscriptions: ShardSubscriptions::new(),
            keyspace_notify,
            tracking: ShardTracking::default(),
            scripting: ShardScripting {
                executor: script_executor,
                ..Default::default()
            },
            wait_queue: ShardWaitQueue::new(),
            replication_broadcaster,
            per_request_spans: Arc::new(AtomicBool::new(false)),
            expiry_paused: Arc::new(AtomicBool::new(false)),
            notify_keyspace_events: Arc::new(AtomicU32::new(0)),
            debug_active_expire_disabled: false,
            search,
            expiry: ActiveExpiryCoordinator::default(),
        }
    }

    /// Replace the script executor with one using the given scripting config.
    pub fn set_scripting_config(&mut self, config: ScriptingConfig) {
        match ScriptExecutor::new(config) {
            Ok(executor) => self.scripting.executor = Some(executor),
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
        self.scripting.function_registry = Some(registry);
    }

    /// Set the wait queue limits from blocking config.
    pub fn set_wait_queue_limits(
        &mut self,
        max_waiters_per_key: usize,
        max_blocked_connections: usize,
    ) {
        self.wait_queue = ShardWaitQueue::with_limits(max_waiters_per_key, max_blocked_connections);
    }

    /// Set the per-request spans flag (shared with connections and ConfigManager).
    pub fn set_per_request_spans(&mut self, flag: Arc<AtomicBool>) {
        self.per_request_spans = flag;
    }

    /// Set the shared keyspace notification event flags (from ConfigManager).
    pub fn set_notify_keyspace_events(&mut self, flag: Arc<AtomicU32>) {
        self.notify_keyspace_events = flag;
    }

    /// Install a search index lifecycle manager, replacing the worker's current
    /// one. Used during server startup recovery: the manager is built by
    /// [`IndexLifecycleManager::recover`] at spawn time (so its non-`Send` index
    /// handles never cross a thread boundary) and installed into the worker it
    /// was built for.
    pub fn install_search_manager(&mut self, manager: IndexLifecycleManager) {
        self.search = manager;
    }

    /// Get a mutable reference to the search indexes.
    pub fn search_indexes_mut(
        &mut self,
    ) -> &mut std::collections::HashMap<String, frogdb_search::ShardSearchIndex> {
        &mut self.search.indexes
    }

    /// Get a reference to the search indexes.
    pub fn search_indexes(
        &self,
    ) -> &std::collections::HashMap<String, frogdb_search::ShardSearchIndex> {
        &self.search.indexes
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
        if let Some(owner) = self.vll.continuation_lock_owner()
            && owner != conn_id
        {
            return Err(Response::error("ERR shard busy with continuation lock"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod command_context_tests {
    use super::*;
    use crate::registry::CommandRegistry;
    use crate::shard::builder::ShardWorkerBuilder;
    use crate::shard::connection::NewConnection;
    use crate::shard::message::{Envelope, ShardReceiver};
    use frogdb_protocol::ProtocolVersion;

    fn minimal_worker() -> ShardWorker {
        let (_mtx, mrx) = mpsc::channel::<Envelope>(1);
        let (_ntx, nrx) = mpsc::channel::<NewConnection>(1);
        ShardWorkerBuilder::new(0, 1)
            .with_message_rx(ShardReceiver::new(mrx))
            .with_new_conn_rx(nrx)
            .with_shard_senders(Arc::new(vec![]))
            .with_registry(Arc::new(CommandRegistry::new()))
            .build()
    }

    /// The builder must carry the shard's replica identity into every context —
    /// the fields EVAL/EVALSHA/FCALL previously dropped.
    #[test]
    fn command_context_carries_replica_identity() {
        let mut worker = minimal_worker();
        worker.set_is_replica(true);
        worker.identity.master_host = Some("primary.local".to_string());
        worker.identity.master_port = Some(6390);

        let ctx = worker.command_context(42, ProtocolVersion::Resp2);
        assert!(ctx.is_replica, "built context must report replica role");
        assert_eq!(ctx.master_host.as_deref(), Some("primary.local"));
        assert_eq!(ctx.master_port, Some(6390));
        assert_eq!(ctx.conn_id, 42);
        assert!(ctx.command_registry.is_some(), "registry must be wired");
        assert!(
            ctx.is_replica_flag.is_some(),
            "shared replica flag must be wired"
        );
    }

    /// On a primary the built context reports the primary role and no master.
    #[test]
    fn command_context_reports_primary_by_default() {
        let mut worker = minimal_worker();
        let ctx = worker.command_context(1, ProtocolVersion::Resp2);
        assert!(!ctx.is_replica);
        assert_eq!(ctx.master_host, None);
        assert_eq!(ctx.master_port, None);
    }
}
