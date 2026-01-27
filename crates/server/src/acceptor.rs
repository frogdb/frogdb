//! TCP connection acceptor.

use anyhow::Result;
use frogdb_core::sync::{Arc, AtomicUsize, Ordering};
use std::sync::atomic::AtomicI64;
use frogdb_core::{
    persistence::SnapshotCoordinator, shard::NewConnection, AclManager, ClientRegistry,
    ClusterNetworkFactory, ClusterRaft, ClusterState, CommandRegistry, MetricsRecorder,
    ReplicationTrackerImpl, ShardMessage, SharedFunctionRegistry,
};
use frogdb_metrics::{metric_names, SharedTracer};

use crate::config::TracingConfig;
use crate::replication::PrimaryReplicationHandler;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

use crate::connection::ConnectionHandler;
use crate::net::{spawn, TcpListener};
use crate::runtime_config::ConfigManager;
use crate::server::next_conn_id;

/// Round-robin connection assigner.
struct RoundRobinAssigner {
    next: AtomicUsize,
    num_shards: usize,
}

impl RoundRobinAssigner {
    fn new(num_shards: usize) -> Self {
        Self {
            next: AtomicUsize::new(0),
            num_shards,
        }
    }

    fn assign(&self) -> usize {
        let idx = self.next.fetch_add(1, Ordering::Relaxed);
        idx % self.num_shards
    }
}

/// TCP acceptor that distributes connections to shard workers.
pub struct Acceptor {
    /// TCP listener.
    listener: TcpListener,

    /// New connection senders (one per shard).
    /// Reserved for future use when connections are routed to shard workers.
    #[allow(dead_code)]
    new_conn_senders: Vec<mpsc::Sender<NewConnection>>,

    /// Shard message senders.
    shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,

    /// Command registry.
    registry: Arc<CommandRegistry>,

    /// Client registry for CLIENT commands.
    client_registry: Arc<ClientRegistry>,

    /// Configuration manager for CONFIG commands.
    config_manager: Arc<ConfigManager>,

    /// Connection assigner.
    assigner: RoundRobinAssigner,

    /// Allow cross-slot operations.
    allow_cross_slot: bool,

    /// Scatter-gather timeout in milliseconds.
    scatter_gather_timeout_ms: u64,

    /// Metrics recorder.
    metrics_recorder: Arc<dyn MetricsRecorder>,

    /// Current connection count (shared for decrement on drop).
    current_connections: Arc<AtomicI64>,

    /// ACL manager for authentication and authorization.
    acl_manager: Arc<AclManager>,

    /// Snapshot coordinator for BGSAVE/LASTSAVE commands.
    snapshot_coordinator: Arc<dyn SnapshotCoordinator>,

    /// Function registry for FUNCTION/FCALL commands.
    function_registry: SharedFunctionRegistry,

    /// Optional shared tracer for distributed tracing.
    shared_tracer: Option<SharedTracer>,

    /// Tracing configuration.
    tracing_config: TracingConfig,

    /// Optional replication tracker for WAIT command.
    replication_tracker: Option<Arc<ReplicationTrackerImpl>>,

    /// Optional cluster state (only when cluster mode is enabled).
    cluster_state: Option<Arc<ClusterState>>,

    /// This node's ID (for cluster mode).
    node_id: Option<u64>,

    /// Whether this acceptor handles admin connections.
    is_admin: bool,

    /// Whether admin port separation is enabled (admin commands blocked on regular port).
    admin_enabled: bool,

    /// Hot shard detection configuration.
    hotshards_config: frogdb_metrics::HotShardConfig,

    /// Memory diagnostics configuration.
    memory_diag_config: frogdb_metrics::MemoryDiagConfig,

    /// Optional latency band tracker for SLO monitoring.
    band_tracker: Option<Arc<frogdb_metrics::LatencyBandTracker>>,

    /// Optional Raft instance (only when cluster mode is enabled).
    raft: Option<Arc<ClusterRaft>>,

    /// Optional network factory for cluster node management.
    network_factory: Option<Arc<ClusterNetworkFactory>>,

    /// Optional primary replication handler for PSYNC connection handoff.
    primary_replication_handler: Option<Arc<PrimaryReplicationHandler>>,
}

impl Acceptor {
    /// Create a new acceptor.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        listener: TcpListener,
        new_conn_senders: Vec<mpsc::Sender<NewConnection>>,
        shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
        registry: Arc<CommandRegistry>,
        client_registry: Arc<ClientRegistry>,
        config_manager: Arc<ConfigManager>,
        allow_cross_slot: bool,
        scatter_gather_timeout_ms: u64,
        metrics_recorder: Arc<dyn MetricsRecorder>,
        acl_manager: Arc<AclManager>,
        snapshot_coordinator: Arc<dyn SnapshotCoordinator>,
        function_registry: SharedFunctionRegistry,
        shared_tracer: Option<SharedTracer>,
        tracing_config: TracingConfig,
        replication_tracker: Option<Arc<ReplicationTrackerImpl>>,
        cluster_state: Option<Arc<ClusterState>>,
        node_id: Option<u64>,
        is_admin: bool,
        admin_enabled: bool,
        hotshards_config: frogdb_metrics::HotShardConfig,
        memory_diag_config: frogdb_metrics::MemoryDiagConfig,
        band_tracker: Option<Arc<frogdb_metrics::LatencyBandTracker>>,
        raft: Option<Arc<ClusterRaft>>,
        network_factory: Option<Arc<ClusterNetworkFactory>>,
        primary_replication_handler: Option<Arc<PrimaryReplicationHandler>>,
    ) -> Self {
        let num_shards = new_conn_senders.len();
        Self {
            listener,
            new_conn_senders,
            shard_senders,
            registry,
            client_registry,
            config_manager,
            assigner: RoundRobinAssigner::new(num_shards),
            allow_cross_slot,
            scatter_gather_timeout_ms,
            metrics_recorder,
            current_connections: Arc::new(AtomicI64::new(0)),
            acl_manager,
            snapshot_coordinator,
            function_registry,
            shared_tracer,
            tracing_config,
            replication_tracker,
            cluster_state,
            node_id,
            is_admin,
            admin_enabled,
            hotshards_config,
            memory_diag_config,
            band_tracker,
            raft,
            network_factory,
            primary_replication_handler,
        }
    }

    /// Run the acceptor loop.
    pub async fn run(self) -> Result<()> {
        info!("Acceptor started");

        loop {
            match self.listener.accept().await {
                Ok((socket, addr)) => {
                    let conn_id = next_conn_id();
                    let shard_id = self.assigner.assign();

                    // Get local address
                    let local_addr = socket.local_addr().ok();

                    // Register connection with client registry
                    let client_handle = self.client_registry.register(conn_id, addr, local_addr);

                    // Record connection metrics
                    self.metrics_recorder
                        .increment_counter(metric_names::CONNECTIONS_TOTAL, 1, &[]);
                    let current = self.current_connections.fetch_add(1, Ordering::SeqCst) + 1;
                    self.metrics_recorder
                        .record_gauge(metric_names::CONNECTIONS_CURRENT, current as f64, &[]);

                    debug!(
                        conn_id,
                        shard_id,
                        addr = %addr,
                        "Accepted connection"
                    );

                    // Build grouped dependencies for the connection handler
                    use crate::connection::deps::{
                        AdminDeps, ClusterDeps, ConnectionConfig, CoreDeps, ObservabilityDeps,
                    };
                    use std::time::Duration;

                    let core = CoreDeps {
                        registry: self.registry.clone(),
                        shard_senders: self.shard_senders.clone(),
                        metrics_recorder: self.metrics_recorder.clone(),
                        acl_manager: self.acl_manager.clone(),
                    };
                    let admin = AdminDeps {
                        client_registry: self.client_registry.clone(),
                        config_manager: self.config_manager.clone(),
                        snapshot_coordinator: self.snapshot_coordinator.clone(),
                        function_registry: self.function_registry.clone(),
                    };
                    let cluster = ClusterDeps {
                        cluster_state: self.cluster_state.clone(),
                        node_id: self.node_id,
                        raft: self.raft.clone(),
                        network_factory: self.network_factory.clone(),
                        replication_tracker: self.replication_tracker.clone(),
                        primary_replication_handler: self.primary_replication_handler.clone(),
                    };
                    let config = ConnectionConfig {
                        num_shards: self.shard_senders.len(),
                        allow_cross_slot: self.allow_cross_slot,
                        scatter_gather_timeout: Duration::from_millis(self.scatter_gather_timeout_ms),
                        is_admin: self.is_admin,
                        admin_enabled: self.admin_enabled,
                        hotshards_config: self.hotshards_config.clone(),
                        memory_diag_config: self.memory_diag_config.clone(),
                    };
                    let observability = ObservabilityDeps {
                        shared_tracer: self.shared_tracer.clone(),
                        tracing_config: self.tracing_config.clone(),
                        band_tracker: self.band_tracker.clone(),
                    };

                    let metrics_recorder = self.metrics_recorder.clone();
                    let current_connections = self.current_connections.clone();

                    spawn(async move {
                        let handler = ConnectionHandler::from_deps(
                            socket,
                            addr,
                            conn_id,
                            shard_id,
                            client_handle,
                            core,
                            admin,
                            cluster,
                            config,
                            observability,
                        );

                        if let Err(e) = handler.run().await {
                            debug!(conn_id, error = %e, "Connection ended with error");
                        }

                        // Decrement connection count when handler finishes
                        let current = current_connections.fetch_sub(1, Ordering::SeqCst) - 1;
                        metrics_recorder
                            .record_gauge(metric_names::CONNECTIONS_CURRENT, current as f64, &[]);
                    });
                }
                Err(e) => {
                    error!(error = %e, "Failed to accept connection");
                }
            }
        }
    }
}
