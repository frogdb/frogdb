//! Main server implementation.

mod cluster_init;
mod init;
mod listeners;
mod register;
mod replication_init;
mod runtime;
mod shards;
mod startup;
mod subsystems;
mod util;

pub use listeners::{BoundListeners, bind_listeners};
pub use register::register_commands;
pub use util::{next_conn_id, next_txid};

use anyhow::Result;
use frogdb_core::persistence::{RocksStore, SnapshotCoordinator};
use frogdb_core::sync::{Arc, AtomicU64};
use frogdb_core::{
    AclManager, ClientRegistry, ClusterNetworkFactory, ClusterRaft, ClusterState, CommandRegistry,
    MetricsRecorder, ReplicationTrackerImpl, ShardSender,
};
use frogdb_telemetry::{HealthChecker, PrometheusRecorder, SharedTracer};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::info;

use crate::config::{Config, ConfigExt, TracingConfigExt};
use crate::failure_detector::FailureDetector;
use crate::latency_test::LatencyTestResult;
use crate::net::TcpListener;
use crate::replication::{PrimaryReplicationHandler, ReplicaReplicationHandler};
use crate::runtime_config::{ConfigManager, ShardConfigNotifier};

use util::shutdown_signal;

/// Optional pre-bound listeners for server subsystems.
///
/// When a listener is provided, `Server` uses it directly instead of binding
/// from config. This eliminates TOCTOU port races: the caller can bind port 0,
/// read the actual port, then hand the listener to Server.
#[derive(Default)]
pub struct ServerListeners {
    /// Pre-bound RESP protocol listener for client connections.
    pub resp: Option<TcpListener>,
    /// Pre-bound admin RESP protocol listener.
    pub admin_resp: Option<TcpListener>,
    /// Pre-bound metrics HTTP listener.
    pub metrics: Option<tokio::net::TcpListener>,
    /// Pre-bound admin HTTP API listener.
    pub admin_http: Option<tokio::net::TcpListener>,
    /// Pre-bound cluster bus (Raft RPC) listener.
    pub cluster_bus: Option<TcpListener>,
}

/// FrogDB server.
pub struct Server {
    /// Server configuration.
    config: Config,

    /// TCP listener for client connections.
    /// Wrapped in `Option` so `start_subsystems()` can `.take()` it.
    listener: Option<TcpListener>,

    /// Optional TCP listener for admin connections.
    admin_listener: Option<TcpListener>,

    /// Optional pre-bound TCP listener for the metrics/observability HTTP server.
    /// Held here from `new()` so the port is never released before `run_until()`.
    metrics_listener: Option<tokio::net::TcpListener>,

    /// Optional pre-bound TCP listener for the admin HTTP API server.
    /// Held here from `new()` so the port is never released before `run_until()`.
    admin_http_listener: Option<tokio::net::TcpListener>,

    /// Optional pre-bound TCP listener for the cluster bus (Raft RPC) server.
    /// Uses `crate::net::TcpListener` so Turmoil can intercept it in simulations.
    cluster_bus_listener: Option<TcpListener>,

    /// Command registry.
    registry: Arc<CommandRegistry>,

    /// Client registry for CLIENT commands.
    client_registry: Arc<ClientRegistry>,

    /// Configuration manager for CONFIG commands.
    config_manager: Arc<ConfigManager>,

    /// Shard message senders.
    shard_senders: Arc<Vec<ShardSender>>,

    /// New connection senders (one per shard).
    new_conn_senders: Vec<mpsc::Sender<frogdb_core::shard::NewConnection>>,

    /// Shard worker handles.
    shard_handles: Vec<crate::net::JoinHandle<()>>,

    /// Optional RocksDB store for persistence.
    rocks_store: Option<Arc<RocksStore>>,

    /// Optional periodic sync task handle.
    periodic_sync_handle: Option<crate::net::JoinHandle<()>>,

    /// Optional periodic snapshot task handle.
    periodic_snapshot_handle: Option<crate::net::JoinHandle<()>>,

    /// Snapshot coordinator (shared across all shards).
    snapshot_coordinator: Arc<dyn SnapshotCoordinator>,

    /// Metrics recorder.
    metrics_recorder: Arc<dyn MetricsRecorder>,

    /// Prometheus recorder (for HTTP endpoint).
    prometheus_recorder: Option<Arc<PrometheusRecorder>>,

    /// Health checker.
    health_checker: HealthChecker,

    /// ACL manager for authentication and authorization.
    acl_manager: Arc<AclManager>,

    /// Function registry (shared across all shards).
    function_registry: frogdb_core::SharedFunctionRegistry,

    /// Optional shared tracer for distributed tracing.
    shared_tracer: Option<SharedTracer>,

    /// Optional replication tracker for WAIT command (only when running as primary).
    replication_tracker: Option<Arc<ReplicationTrackerImpl>>,

    /// Optional cluster state (only when cluster mode is enabled).
    cluster_state: Option<Arc<ClusterState>>,

    /// This node's ID (for cluster mode).
    node_id: Option<u64>,

    /// Optional Raft instance (only when cluster mode is enabled).
    raft: Option<Arc<ClusterRaft>>,

    /// Latency baseline from startup test (if enabled).
    latency_baseline: Option<LatencyTestResult>,

    /// Optional network factory for cluster node management.
    network_factory: Option<Arc<ClusterNetworkFactory>>,

    /// Optional failure detector (only when cluster mode is enabled).
    failure_detector: Option<Arc<FailureDetector>>,

    /// Optional failure detector task handle (only when cluster mode is enabled).
    failure_detector_handle: Option<crate::net::JoinHandle<()>>,

    /// Optional replica replication handler (only when running as replica).
    replica_handler: Option<Arc<ReplicaReplicationHandler>>,

    /// Optional replica frame receiver (only when running as replica).
    replica_frame_rx: Option<mpsc::Receiver<frogdb_core::ReplicationFrame>>,

    /// Optional primary replication handler (only when running as primary).
    /// Used for PSYNC connection handoff.
    primary_replication_handler: Option<Arc<PrimaryReplicationHandler>>,

    /// Optional replication quorum checker (only when running as primary with self-fencing).
    replication_quorum_checker: Option<Arc<dyn frogdb_core::command::QuorumChecker>>,

    /// Optional connection task monitor for tokio-metrics instrumentation.
    conn_monitor: Option<tokio_metrics::TaskMonitor>,

    /// Background handle for the tokio-metrics task monitor collector.
    _task_monitor_handle: Option<tokio::task::JoinHandle<()>>,

    /// Shared replication offset for cluster bus HealthProbe responses.
    shared_replication_offset: Option<Arc<AtomicU64>>,

    /// Shared is_replica flag. Toggled by REPLICAOF NO ONE to promote from
    /// replica to primary. Shared across all shard workers, acceptors, and
    /// connection handlers.
    is_replica_flag: Arc<std::sync::atomic::AtomicBool>,

    /// Shared maxmemory value for SystemMetricsCollector.
    shared_maxmemory: Arc<AtomicU64>,

    /// Per-shard memory usage atomics for SystemMetricsCollector fragmentation ratio.
    shard_memory_used: Arc<Vec<AtomicU64>>,
}

impl Server {
    /// Create a new server instance, binding all listeners from config.
    pub async fn new(
        config: Config,
        log_reload_handle: crate::runtime_config::LogReloadHandle,
    ) -> Result<Self> {
        Self::with_listeners(config, ServerListeners::default(), Some(log_reload_handle)).await
    }

    /// Create a new server instance, optionally accepting pre-bound listeners.
    ///
    /// If a pre-bound listener is provided in `listeners`, Server uses it
    /// directly instead of binding from config. This eliminates TOCTOU port
    /// races for cluster bus addresses.
    pub async fn with_listeners(
        config: Config,
        listeners: ServerListeners,
        log_reload_handle: Option<crate::runtime_config::LogReloadHandle>,
    ) -> Result<Self> {
        // Phase 1: Infrastructure init (metrics, listeners, registries, persistence, channels)
        let infra = init::init_infrastructure(&config, listeners, log_reload_handle).await?;

        // Phase 2: Replication handler setup
        let repl = replication_init::init_replication(
            &config,
            &infra.rocks_store,
            &infra.metrics_recorder,
        )?;

        // Phase 3: Cluster/Raft initialization + background tasks
        let shared_replication_offset = repl.shared_replication_offset;
        let cluster = cluster_init::init_cluster(
            &config,
            &infra.listener,
            &infra.cluster_bus_listener,
            &infra.shard_senders,
            infra.num_shards,
            &repl.replication_broadcaster,
            &repl.replication_tracker,
            &infra.metrics_recorder,
        )
        .await?;

        // Phase 4: Spawn shard workers
        let shard_handles = shards::spawn_shard_workers(shards::ShardSpawnContext {
            config: config.clone(),
            num_shards: infra.num_shards,
            shard_receivers: infra.shard_receivers,
            new_conn_receivers: infra.new_conn_receivers,
            shard_senders: infra.shard_senders.clone(),
            registry: infra.registry.clone(),
            rocks_store: infra.rocks_store.clone(),
            recovered_stores: infra.recovered_stores,
            wal_config: infra.wal_config,
            eviction_config: infra.eviction_config,
            snapshot_coordinator: infra.snapshot_coordinator.clone(),
            metrics_recorder: infra.metrics_recorder.clone(),
            slowlog_next_id: infra.slowlog_next_id,
            function_registry: infra.function_registry.clone(),
            replication_broadcaster: repl.replication_broadcaster,
            replication_tracker: repl.replication_tracker.clone(),
            raft: cluster.raft.clone(),
            cluster_state: cluster.cluster_state.clone(),
            node_id: cluster.node_id,
            network_factory: cluster.network_factory.clone(),
            failure_detector: cluster.failure_detector.clone(),
            replication_quorum_checker: repl.replication_quorum_checker.clone(),
            is_replica_flag: cluster.is_replica_flag.clone(),
            client_registry: infra.client_registry.clone(),
            config_manager: infra.config_manager.clone(),
            shard_memory_used: infra.shard_memory_used.clone(),
            shard_monitor: infra.shard_monitor,
        });

        // Create ACL manager
        let acl_manager = AclManager::new(config.to_acl_config());

        // Initialize distributed tracer if enabled
        let shared_tracer = if config.tracing.enabled {
            let tracing_config = config.tracing.to_metrics_config();
            match frogdb_telemetry::create_tracer(&tracing_config) {
                Ok(tracer) => {
                    info!(
                        endpoint = %config.tracing.otlp_endpoint,
                        sampling_rate = %config.tracing.sampling_rate,
                        "Distributed tracing enabled"
                    );
                    Some(tracer)
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to initialize tracer, continuing without tracing");
                    None
                }
            }
        } else {
            None
        };

        // Create shard config notifier for propagating runtime config changes
        let shard_notifier = Arc::new(ShardConfigNotifier::new(
            infra.shard_senders.clone(),
            infra.config_manager.runtime_ref(),
            infra.num_shards,
        ));
        infra.config_manager.set_shard_notifier(shard_notifier);

        // Spawn task monitor collector (tokio-metrics)
        let task_monitor_handle = infra
            .task_registry
            .spawn_collector(infra.metrics_recorder.clone(), Duration::from_secs(10));

        Ok(Self {
            config,
            listener: Some(infra.listener),
            admin_listener: infra.admin_listener,
            metrics_listener: infra.metrics_listener,
            admin_http_listener: infra.admin_http_listener,
            cluster_bus_listener: infra.cluster_bus_listener,
            registry: infra.registry,
            client_registry: infra.client_registry,
            config_manager: infra.config_manager,
            shard_senders: infra.shard_senders,
            new_conn_senders: infra.new_conn_senders,
            shard_handles,
            rocks_store: infra.rocks_store,
            periodic_sync_handle: infra.periodic_sync_handle,
            periodic_snapshot_handle: infra.periodic_snapshot_handle,
            snapshot_coordinator: infra.snapshot_coordinator,
            metrics_recorder: infra.metrics_recorder,
            prometheus_recorder: infra.prometheus_recorder,
            health_checker: infra.health_checker,
            acl_manager,
            function_registry: infra.function_registry,
            shared_tracer,
            replication_tracker: repl.replication_tracker,
            cluster_state: cluster.cluster_state,
            node_id: cluster.node_id,
            raft: cluster.raft,
            latency_baseline: None,
            network_factory: cluster.network_factory,
            failure_detector: cluster.failure_detector,
            failure_detector_handle: cluster.failure_detector_handle,
            replica_handler: repl.replica_handler,
            replica_frame_rx: repl.replica_frame_rx,
            primary_replication_handler: repl.primary_replication_handler,
            replication_quorum_checker: repl.replication_quorum_checker,
            conn_monitor: Some(infra.conn_monitor),
            _task_monitor_handle: Some(task_monitor_handle),
            shared_replication_offset,
            is_replica_flag: cluster.is_replica_flag,
            shared_maxmemory: infra.shared_maxmemory,
            shard_memory_used: infra.shard_memory_used,
        })
    }

    // run_until() is in runtime.rs

    /// Run the server (production - waits for OS signals).
    pub async fn run(self) -> Result<()> {
        self.run_until(shutdown_signal()).await
    }

    /// Get the snapshot coordinator.
    pub fn snapshot_coordinator(&self) -> &Arc<dyn SnapshotCoordinator> {
        &self.snapshot_coordinator
    }

    /// Get the local address of the RESP TCP listener.
    ///
    /// After `Server::new()`, the listener is bound and this returns the actual
    /// address (including the OS-assigned port when `config.server.port` was 0).
    pub fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.listener
            .as_ref()
            .expect("listener not yet taken")
            .local_addr()
    }

    /// Get the local address of the admin RESP TCP listener, if enabled.
    pub fn admin_resp_addr(&self) -> Option<std::io::Result<std::net::SocketAddr>> {
        self.admin_listener.as_ref().map(|l| l.local_addr())
    }

    /// Get the local address of the metrics/observability HTTP listener, if enabled.
    pub fn metrics_addr(&self) -> Option<std::io::Result<std::net::SocketAddr>> {
        self.metrics_listener.as_ref().map(|l| l.local_addr())
    }

    /// Get the local address of the admin HTTP API listener, if enabled.
    pub fn admin_http_addr(&self) -> Option<std::io::Result<std::net::SocketAddr>> {
        self.admin_http_listener.as_ref().map(|l| l.local_addr())
    }

    /// Get the local address of the cluster bus listener, if cluster mode is enabled.
    pub fn cluster_bus_addr(&self) -> Option<std::io::Result<std::net::SocketAddr>> {
        self.cluster_bus_listener.as_ref().map(|l| l.local_addr())
    }

    /// Get the Raft instance, if cluster mode is enabled.
    pub fn raft(&self) -> Option<&Arc<ClusterRaft>> {
        self.raft.as_ref()
    }

    /// Get the cluster state, if cluster mode is enabled.
    pub fn cluster_state(&self) -> Option<&Arc<ClusterState>> {
        self.cluster_state.as_ref()
    }

    /// Get this node's ID, if cluster mode is enabled.
    pub fn node_id(&self) -> Option<u64> {
        self.node_id
    }

    /// Get the latency baseline result from startup test (if available).
    pub fn latency_baseline(&self) -> Option<&LatencyTestResult> {
        self.latency_baseline.as_ref()
    }

    /// Get the client registry (for testing blocked-client counts, etc.).
    pub fn client_registry(&self) -> &Arc<ClientRegistry> {
        &self.client_registry
    }
}
