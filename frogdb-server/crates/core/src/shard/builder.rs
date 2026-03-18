use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};

use tokio::sync::mpsc;

use crate::cluster::{ClusterNetworkFactory, ClusterRaft, ClusterState};
use crate::command::QuorumChecker;
use crate::eviction::EvictionConfig;
use crate::functions::SharedFunctionRegistry;
use crate::persistence::{
    NoopSnapshotCoordinator, RocksStore, RocksWalWriter, SnapshotCoordinator, WalConfig,
    WalFailurePolicy,
};
use crate::registry::CommandRegistry;
use crate::replication::{NoopBroadcaster, SharedBroadcaster};
use crate::scripting::ScriptingConfig;

use super::connection::NewConnection;
use super::message::ShardMessage;
use super::types::{ShardClusterDeps, ShardCoreDeps, ShardPersistenceDeps};
use super::worker::ShardWorker;

// ============================================================================
// ShardWorker Builder
// ============================================================================

/// Error returned when building a [`ShardWorker`] fails due to missing required fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardBuilderError {
    /// A required field was not set.
    MissingField(&'static str),
}

impl std::fmt::Display for ShardBuilderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShardBuilderError::MissingField(field) => {
                write!(f, "missing required field: {}", field)
            }
        }
    }
}

impl std::error::Error for ShardBuilderError {}

/// Builder for creating [`ShardWorker`] instances with a fluent API.
///
/// This provides a cleaner way to construct shard workers with optional
/// features like persistence, VLL, and cluster support.
///
/// # Example
///
/// ```rust,ignore
/// let worker = ShardWorkerBuilder::new(shard_id, num_shards, message_rx, new_conn_rx)
///     .with_registry(registry)
///     .with_shard_senders(shard_senders)
///     .with_metrics(metrics_recorder)
///     .with_eviction(eviction_config)
///     .with_persistence(rocks_store, wal_config)
///     .enable_vll()
///     .build();
/// ```
pub struct ShardWorkerBuilder {
    shard_id: usize,
    num_shards: usize,
    message_rx: Option<mpsc::Receiver<ShardMessage>>,
    new_conn_rx: Option<mpsc::Receiver<NewConnection>>,
    shard_senders: Option<Arc<Vec<mpsc::Sender<ShardMessage>>>>,
    registry: Option<Arc<CommandRegistry>>,
    metrics_recorder: Option<Arc<dyn crate::noop::MetricsRecorder>>,
    slowlog_next_id: Option<Arc<AtomicU64>>,
    replication_broadcaster: Option<SharedBroadcaster>,
    eviction_config: EvictionConfig,
    scripting_config: ScriptingConfig,
    rocks_store: Option<Arc<RocksStore>>,
    wal_config: Option<WalConfig>,
    snapshot_coordinator: Option<Arc<dyn SnapshotCoordinator>>,
    function_registry: Option<SharedFunctionRegistry>,
    cluster_state: Option<Arc<ClusterState>>,
    node_id: Option<u64>,
    raft: Option<Arc<ClusterRaft>>,
    network_factory: Option<Arc<ClusterNetworkFactory>>,
    quorum_checker: Option<Arc<dyn QuorumChecker>>,
    enable_vll: bool,
    queue_depth: Option<Arc<AtomicUsize>>,
    per_request_spans: Option<Arc<AtomicBool>>,
    wal_failure_policy: Option<Arc<std::sync::atomic::AtomicU8>>,
    is_replica: bool,
}

impl ShardWorkerBuilder {
    /// Create a new builder with required shard identity.
    pub fn new(shard_id: usize, num_shards: usize) -> Self {
        Self {
            shard_id,
            num_shards,
            message_rx: None,
            new_conn_rx: None,
            shard_senders: None,
            registry: None,
            metrics_recorder: None,
            slowlog_next_id: None,
            replication_broadcaster: None,
            eviction_config: EvictionConfig::default(),
            scripting_config: ScriptingConfig::default(),
            rocks_store: None,
            wal_config: None,
            snapshot_coordinator: None,
            function_registry: None,
            cluster_state: None,
            node_id: None,
            raft: None,
            network_factory: None,
            quorum_checker: None,
            enable_vll: false,
            queue_depth: None,
            per_request_spans: None,
            wal_failure_policy: None,
            is_replica: false,
        }
    }

    /// Set the message receiver for shard commands.
    pub fn with_message_rx(mut self, rx: mpsc::Receiver<ShardMessage>) -> Self {
        self.message_rx = Some(rx);
        self
    }

    /// Set the new connection receiver.
    pub fn with_new_conn_rx(mut self, rx: mpsc::Receiver<NewConnection>) -> Self {
        self.new_conn_rx = Some(rx);
        self
    }

    /// Set shard senders for cross-shard operations.
    pub fn with_shard_senders(mut self, senders: Arc<Vec<mpsc::Sender<ShardMessage>>>) -> Self {
        self.shard_senders = Some(senders);
        self
    }

    /// Set the command registry.
    pub fn with_registry(mut self, registry: Arc<CommandRegistry>) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Set the metrics recorder.
    pub fn with_metrics(mut self, recorder: Arc<dyn crate::noop::MetricsRecorder>) -> Self {
        self.metrics_recorder = Some(recorder);
        self
    }

    /// Set the slowlog next ID counter.
    pub fn with_slowlog_id(mut self, id: Arc<AtomicU64>) -> Self {
        self.slowlog_next_id = Some(id);
        self
    }

    /// Set the replication broadcaster.
    pub fn with_replication(mut self, broadcaster: SharedBroadcaster) -> Self {
        self.replication_broadcaster = Some(broadcaster);
        self
    }

    /// Set eviction configuration.
    pub fn with_eviction(mut self, config: EvictionConfig) -> Self {
        self.eviction_config = config;
        self
    }

    /// Set scripting configuration.
    pub fn with_scripting(mut self, config: ScriptingConfig) -> Self {
        self.scripting_config = config;
        self
    }

    /// Enable persistence with RocksDB.
    pub fn with_persistence(mut self, rocks_store: Arc<RocksStore>, wal_config: WalConfig) -> Self {
        self.rocks_store = Some(rocks_store);
        self.wal_config = Some(wal_config);
        self
    }

    /// Set the snapshot coordinator.
    pub fn with_snapshot_coordinator(mut self, coordinator: Arc<dyn SnapshotCoordinator>) -> Self {
        self.snapshot_coordinator = Some(coordinator);
        self
    }

    /// Set the function registry for FUNCTION/FCALL commands.
    pub fn with_function_registry(mut self, registry: SharedFunctionRegistry) -> Self {
        self.function_registry = Some(registry);
        self
    }

    /// Enable cluster mode with the given dependencies.
    pub fn with_cluster(
        mut self,
        cluster_state: Arc<ClusterState>,
        node_id: u64,
        raft: Arc<ClusterRaft>,
        network_factory: Arc<ClusterNetworkFactory>,
    ) -> Self {
        self.cluster_state = Some(cluster_state);
        self.node_id = Some(node_id);
        self.raft = Some(raft);
        self.network_factory = Some(network_factory);
        self
    }

    /// Set the quorum checker for cluster health detection.
    pub fn with_quorum_checker(mut self, checker: Arc<dyn QuorumChecker>) -> Self {
        self.quorum_checker = Some(checker);
        self
    }

    /// Enable VLL (Virtual Lock Loom) for transaction coordination.
    pub fn enable_vll(mut self) -> Self {
        self.enable_vll = true;
        self
    }

    /// Set the shared queue depth counter.
    pub fn with_queue_depth(mut self, depth: Arc<AtomicUsize>) -> Self {
        self.queue_depth = Some(depth);
        self
    }

    /// Mark this shard as belonging to a replica server.
    pub fn as_replica(mut self) -> Self {
        self.is_replica = true;
        self
    }

    /// Set the per-request spans toggle (shared with connections and ConfigManager).
    pub fn with_per_request_spans(mut self, flag: Arc<AtomicBool>) -> Self {
        self.per_request_spans = Some(flag);
        self
    }

    /// Set the WAL failure policy toggle (shared with ConfigManager for runtime CONFIG SET).
    pub fn with_wal_failure_policy(mut self, policy: Arc<std::sync::atomic::AtomicU8>) -> Self {
        self.wal_failure_policy = Some(policy);
        self
    }

    /// Set core dependencies from a bundle.
    pub fn with_core_deps(mut self, core: ShardCoreDeps) -> Self {
        self.shard_senders = Some(core.shard_senders);
        self.registry = Some(core.registry);
        self.metrics_recorder = Some(core.metrics_recorder);
        self.slowlog_next_id = Some(core.slowlog_next_id);
        self.replication_broadcaster = Some(core.replication_broadcaster);
        self
    }

    /// Set persistence dependencies from a bundle.
    pub fn with_persistence_deps(mut self, persistence: ShardPersistenceDeps) -> Self {
        self.rocks_store = persistence.rocks_store;
        self.snapshot_coordinator = persistence.snapshot_coordinator;
        self
    }

    /// Set cluster dependencies from a bundle.
    pub fn with_cluster_deps(mut self, cluster: ShardClusterDeps) -> Self {
        self.cluster_state = cluster.cluster_state;
        self.node_id = cluster.node_id;
        self.raft = cluster.raft;
        self.network_factory = cluster.network_factory;
        self.quorum_checker = cluster.quorum_checker;
        self
    }

    /// Try to build the ShardWorker, returning an error if required fields are missing.
    ///
    /// This is the fallible version of [`build()`](Self::build) that returns a
    /// `Result` instead of panicking on missing required fields.
    pub fn try_build(self) -> Result<ShardWorker, ShardBuilderError> {
        let message_rx = self
            .message_rx
            .ok_or(ShardBuilderError::MissingField("message_rx"))?;
        let new_conn_rx = self
            .new_conn_rx
            .ok_or(ShardBuilderError::MissingField("new_conn_rx"))?;
        let shard_senders = self
            .shard_senders
            .ok_or(ShardBuilderError::MissingField("shard_senders"))?;
        let registry = self
            .registry
            .ok_or(ShardBuilderError::MissingField("registry"))?;
        let metrics_recorder = self
            .metrics_recorder
            .unwrap_or_else(|| Arc::new(crate::noop::NoopMetricsRecorder::new()));
        let slowlog_next_id = self
            .slowlog_next_id
            .unwrap_or_else(|| Arc::new(AtomicU64::new(0)));
        let replication_broadcaster = self
            .replication_broadcaster
            .unwrap_or_else(|| Arc::new(NoopBroadcaster));
        let snapshot_coordinator: Arc<dyn SnapshotCoordinator> = self
            .snapshot_coordinator
            .unwrap_or_else(|| Arc::new(NoopSnapshotCoordinator::new()));

        // Create the worker using the existing with_eviction constructor
        let mut worker = ShardWorker::with_eviction(
            self.shard_id,
            self.num_shards,
            message_rx,
            new_conn_rx,
            shard_senders,
            registry,
            self.eviction_config,
            metrics_recorder,
            slowlog_next_id,
            replication_broadcaster,
        );

        // Apply optional configurations
        if let Some(rocks_store) = self.rocks_store {
            worker.persistence.rocks_store = Some(rocks_store.clone());
            if let Some(ref wal_config) = self.wal_config {
                // Set failure policy from config or shared atomic
                if let Some(shared_policy) = self.wal_failure_policy {
                    worker.persistence.failure_policy = shared_policy;
                } else {
                    let policy_val = match wal_config.failure_policy {
                        WalFailurePolicy::Continue => 0u8,
                        WalFailurePolicy::Rollback => 1u8,
                    };
                    worker
                        .persistence
                        .failure_policy
                        .store(policy_val, std::sync::atomic::Ordering::Relaxed);
                }

                let wal_writer = RocksWalWriter::new(
                    rocks_store,
                    worker.shard_id(),
                    self.wal_config.unwrap(),
                    worker.observability.metrics_recorder.clone(),
                );
                worker.persistence.wal_writer = Some(wal_writer);
            }
        }

        worker.persistence.snapshot_coordinator = snapshot_coordinator;
        worker.scripting.function_registry = self.function_registry;
        worker.cluster.cluster_state = self.cluster_state;
        worker.cluster.node_id = self.node_id;
        worker.cluster.raft = self.raft;
        worker.cluster.network_factory = self.network_factory;
        worker.cluster.quorum_checker = self.quorum_checker;

        if let Some(queue_depth) = self.queue_depth {
            worker.observability.queue_depth = queue_depth;
        }
        if let Some(per_request_spans) = self.per_request_spans {
            worker.per_request_spans = per_request_spans;
        }

        worker
            .identity
            .is_replica
            .store(self.is_replica, std::sync::atomic::Ordering::Relaxed);

        // VLL initialization is handled separately via enable_vll() method on ShardWorker
        // since it requires runtime configuration

        Ok(worker)
    }

    /// Build the ShardWorker.
    ///
    /// # Panics
    ///
    /// Panics if required dependencies are not set:
    /// - `message_rx`
    /// - `new_conn_rx`
    /// - `shard_senders`
    /// - `registry`
    pub fn build(self) -> ShardWorker {
        self.try_build()
            .expect("ShardWorkerBuilder: missing required fields")
    }
}
