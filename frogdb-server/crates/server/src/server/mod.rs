//! Main server implementation.

mod register;
mod util;

pub use register::register_commands;
pub use util::{next_conn_id, next_txid};

use anyhow::Result;
use frogdb_core::persistence::{
    NoopSnapshotCoordinator, RocksConfig, RocksSnapshotCoordinator, RocksStore,
    SnapshotCoordinator, recover_all_shards, spawn_periodic_sync,
};
use frogdb_core::sync::{Arc, AtomicU64};
use frogdb_core::{
    AclManager, ClientRegistry, ClusterNetworkFactory, ClusterRaft, ClusterState,
    ClusterStateMachine, ClusterStorage, CommandRegistry, MetricsRecorder, NoopBroadcaster,
    ReplicationTrackerImpl, ShardMessage, ShardWorker, SharedBroadcaster,
};
use frogdb_debug::{ConfigEntry, DebugState, ServerInfo};
use frogdb_telemetry::{
    HealthChecker, PrometheusRecorder, SharedTracer, StatusCollector, SystemMetricsCollector,
    TaskMonitorRegistry,
};

use crate::observability_server::ObservabilityServer;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::acceptor::Acceptor;
use crate::config::{Config, PersistenceConfig};
use crate::failure_detector::{
    FailureDetector, FailureDetectorConfig, spawn_failure_detector_task,
};
use crate::latency_test::{self, LatencyTestResult};
use crate::net::{TcpListener, spawn, tcp_listener_reusable};
use crate::replication::{
    LagThresholdConfig, PrimaryReplicationHandler, ReplicaCommandExecutor,
    ReplicaReplicationHandler, SplitBrainBufferConfig, consume_frames,
};
use crate::runtime_config::{ConfigManager, ShardConfigNotifier};

use util::{
    NEW_CONN_CHANNEL_CAPACITY, PersistenceInitResult, SHARD_CHANNEL_CAPACITY,
    build_eviction_config, build_wal_config, hash_addr_to_node_id, num_cpus, parse_compression,
    shutdown_signal,
};

/// Optional pre-bound listeners for server subsystems.
///
/// When a listener is provided, `Server` uses it directly instead of binding
/// from config. This eliminates TOCTOU port races: the caller can bind port 0,
/// read the actual port, then hand the listener to Server.
#[derive(Default)]
pub struct ServerListeners {
    /// Pre-bound cluster bus (Raft RPC) listener.
    pub cluster_bus: Option<TcpListener>,
}

/// FrogDB server.
pub struct Server {
    /// Server configuration.
    config: Config,

    /// TCP listener for client connections.
    listener: TcpListener,

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
    shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,

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

    /// Optional latency band tracker for SLO monitoring.
    band_tracker: Option<Arc<frogdb_telemetry::LatencyBandTracker>>,

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

    /// Optional connection task monitor for tokio-metrics instrumentation.
    conn_monitor: Option<tokio_metrics::TaskMonitor>,

    /// Background handle for the tokio-metrics task monitor collector.
    _task_monitor_handle: Option<tokio::task::JoinHandle<()>>,
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
        // Configure sorted set index backend before any data structures are created
        let score_index_backend = match config.server.sorted_set_index {
            crate::config::server::SortedSetIndexConfig::Btreemap => {
                frogdb_core::ScoreIndexBackend::BTree
            }
            crate::config::server::SortedSetIndexConfig::Skiplist => {
                frogdb_core::ScoreIndexBackend::SkipList
            }
        };
        frogdb_core::set_default_score_index(score_index_backend);

        // Initialize metrics
        let health_checker = HealthChecker::new();
        let (metrics_recorder, prometheus_recorder): (
            Arc<dyn MetricsRecorder>,
            Option<Arc<PrometheusRecorder>>,
        ) = if config.metrics.enabled {
            let recorder = Arc::new(PrometheusRecorder::new());
            // Record server info
            recorder.record_gauge(
                frogdb_telemetry::metric_names::INFO,
                1.0,
                &[
                    ("version", env!("CARGO_PKG_VERSION")),
                    ("mode", "standalone"),
                ],
            );
            // Record maxmemory at startup
            if config.memory.maxmemory > 0 {
                recorder.record_gauge(
                    frogdb_telemetry::metric_names::MEMORY_MAXMEMORY_BYTES,
                    config.memory.maxmemory as f64,
                    &[],
                );
            }
            (recorder.clone(), Some(recorder))
        } else {
            (Arc::new(frogdb_core::NoopMetricsRecorder::new()), None)
        };

        // Bind TCP listener with SO_REUSEADDR for rapid restarts
        let bind_addr: std::net::SocketAddr = config.bind_addr().parse()?;
        let listener = tcp_listener_reusable(bind_addr).await?;

        info!(
            addr = %bind_addr,
            "TCP listener bound"
        );

        // Bind admin TCP listener if enabled
        let admin_listener = if config.admin.enabled {
            let admin_bind_addr: std::net::SocketAddr = config.admin.bind_addr().parse()?;
            let admin_listener = tcp_listener_reusable(admin_bind_addr).await?;
            info!(
                addr = %config.admin.bind_addr(),
                "Admin TCP listener bound"
            );
            Some(admin_listener)
        } else {
            None
        };

        // Bind metrics HTTP listener if metrics are enabled
        let metrics_listener = if config.metrics.enabled {
            let metrics_bind_addr: std::net::SocketAddr = config.metrics.bind_addr().parse()?;
            let listener = tokio::net::TcpListener::bind(metrics_bind_addr).await?;
            info!(
                addr = %listener.local_addr()?,
                "Metrics listener bound"
            );
            Some(listener)
        } else {
            None
        };

        // Bind admin HTTP listener if admin API is enabled
        let admin_http_listener = if config.admin.enabled {
            let admin_http_bind_addr: std::net::SocketAddr = config.admin.bind_addr().parse()?;
            let listener = tokio::net::TcpListener::bind(admin_http_bind_addr).await?;
            info!(
                addr = %listener.local_addr()?,
                "Admin HTTP listener bound"
            );
            Some(listener)
        } else {
            None
        };

        // Bind cluster bus listener if cluster mode is enabled.
        // Uses crate::net::TcpListener (turmoil-compatible) so simulations can intercept it.
        // If a pre-bound listener was provided via ServerListeners, use it directly.
        let cluster_bus_listener = if let Some(l) = listeners.cluster_bus {
            info!(addr = %l.local_addr()?, "Cluster bus using pre-bound listener");
            Some(l)
        } else if config.cluster.enabled {
            let cluster_bus_addr = config.cluster.cluster_bus_socket_addr();
            let listener = tcp_listener_reusable(cluster_bus_addr).await?;
            info!(
                addr = %listener.local_addr()?,
                "Cluster bus listener bound"
            );
            Some(listener)
        } else {
            None
        };

        // Create command registry
        let mut registry = CommandRegistry::new();
        crate::register_commands(&mut registry);
        let registry = Arc::new(registry);

        // Create client registry
        let client_registry = Arc::new(ClientRegistry::new());

        // Create configuration manager
        let mut config_manager = ConfigManager::new(&config);
        if let Some(handle) = log_reload_handle {
            config_manager.set_log_reload_handle(handle);
        }
        let config_manager = Arc::new(config_manager);

        // Determine number of shards
        let num_shards = if config.server.num_shards == 0 {
            std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(1)
        } else {
            config.server.num_shards
        };

        info!(num_shards, "Initializing shards");

        // Create task monitor registry for tokio-metrics instrumentation
        let mut task_registry = TaskMonitorRegistry::new();
        let shard_monitor = task_registry.register("shard_worker");
        let conn_monitor = task_registry.register("connection");
        let wal_sync_monitor = task_registry.register("wal_sync");

        // Initialize persistence if enabled
        let (rocks_store, recovered_stores, periodic_sync_handle) = if config.persistence.enabled {
            let (rocks, stores, sync_handle) =
                Self::init_persistence(&config.persistence, num_shards, Some(wal_sync_monitor))?;
            (Some(rocks), Some(stores), sync_handle)
        } else {
            info!("Persistence disabled");
            (None, None, None)
        };

        // Create snapshot coordinator
        let (snapshot_coordinator, periodic_snapshot_handle): (
            Arc<dyn SnapshotCoordinator>,
            Option<crate::net::JoinHandle<()>>,
        ) = if let Some(ref rocks) = rocks_store {
            // Real snapshot coordinator with RocksDB
            match RocksSnapshotCoordinator::new(
                rocks.clone(),
                config.snapshot.to_core_config(),
                metrics_recorder.clone(),
            ) {
                Ok(coordinator) => {
                    let coordinator = Arc::new(coordinator);

                    // Spawn periodic snapshot task if enabled
                    let snapshot_handle = if config.snapshot.snapshot_interval_secs > 0 {
                        Some(Self::spawn_periodic_snapshot_task(
                            coordinator.clone(),
                            config.snapshot.snapshot_interval_secs,
                        ))
                    } else {
                        None
                    };

                    (coordinator, snapshot_handle)
                }
                Err(e) => {
                    warn!(error = %e, "Failed to create snapshot coordinator, using noop");
                    (Arc::new(NoopSnapshotCoordinator::new()), None)
                }
            }
        } else {
            // No persistence - use noop coordinator
            (Arc::new(NoopSnapshotCoordinator::new()), None)
        };

        // Create channels for each shard
        let mut shard_senders = Vec::with_capacity(num_shards);
        let mut shard_receivers = Vec::with_capacity(num_shards);
        let mut new_conn_senders = Vec::with_capacity(num_shards);
        let mut new_conn_receivers = Vec::with_capacity(num_shards);

        for _ in 0..num_shards {
            let (msg_tx, msg_rx) = mpsc::channel(SHARD_CHANNEL_CAPACITY);
            let (conn_tx, conn_rx) = mpsc::channel(NEW_CONN_CHANNEL_CAPACITY);

            shard_senders.push(msg_tx);
            shard_receivers.push(msg_rx);
            new_conn_senders.push(conn_tx);
            new_conn_receivers.push(conn_rx);
        }

        let shard_senders = Arc::new(shard_senders);

        // Spawn shard workers
        let mut shard_handles = Vec::with_capacity(num_shards);
        let wal_config = build_wal_config(&config.persistence);

        // Build eviction config from memory settings
        let eviction_config = build_eviction_config(&config.memory);

        // Create shared slowlog ID counter for global ordering across shards
        let slowlog_next_id = Arc::new(AtomicU64::new(0));

        // Create shared function registry
        let function_registry = frogdb_core::new_shared_registry();

        // Load persisted functions from disk (if persistence is enabled)
        if config.persistence.enabled {
            let functions_path = config.persistence.data_dir.join("functions.fdb");
            match frogdb_core::load_from_file(&functions_path) {
                Ok(libraries) if !libraries.is_empty() => {
                    info!(count = libraries.len(), "Loading persisted functions");
                    let mut registry = function_registry.write().unwrap();
                    for (name, code) in libraries {
                        match frogdb_core::load_library(&code) {
                            Ok(library) => {
                                if let Err(e) = registry.load_library(library, false) {
                                    warn!(library = %name, error = %e, "Failed to load persisted function library");
                                }
                            }
                            Err(e) => {
                                warn!(library = %name, error = %e, "Failed to parse persisted function library");
                            }
                        }
                    }
                    info!("Persisted functions loaded");
                }
                Ok(_) => {
                    // No functions to load, that's fine
                }
                Err(e) => {
                    warn!(error = %e, "Failed to load persisted functions");
                }
            }
        }

        // Convert recovered stores to an iterator if available
        let mut recovered_iter = recovered_stores.map(|v| v.into_iter());

        // Create replication broadcaster, tracker, and replica handler
        let mut replica_handler: Option<Arc<ReplicaReplicationHandler>> = None;
        let mut replica_frame_rx: Option<mpsc::Receiver<frogdb_core::ReplicationFrame>> = None;
        let mut primary_replication_handler: Option<Arc<PrimaryReplicationHandler>> = None;

        let (replication_broadcaster, replication_tracker): (
            SharedBroadcaster,
            Option<Arc<ReplicationTrackerImpl>>,
        ) = if config.replication.is_primary() {
            // Initialize PrimaryReplicationHandler for primary role
            let state_path = config
                .persistence
                .data_dir
                .join(&config.replication.state_file);
            let repl_state = frogdb_core::ReplicationState::load_or_create(&state_path)
                .map_err(|e| anyhow::anyhow!("Failed to load replication state: {}", e))?;

            info!(
                replication_id = %repl_state.replication_id,
                offset = repl_state.replication_offset,
                "Initialized primary replication state"
            );

            let tracker = Arc::new(frogdb_core::ReplicationTrackerImpl::new());
            tracker.set_offset(repl_state.replication_offset);

            let handler = Arc::new(PrimaryReplicationHandler::new(
                repl_state,
                tracker.clone(),
                rocks_store.clone(),
                config.persistence.data_dir.clone(),
                LagThresholdConfig {
                    threshold_bytes: config.replication.replication_lag_threshold_bytes,
                    threshold_secs: config.replication.replication_lag_threshold_secs,
                    cooldown: Duration::from_secs(config.replication.fullresync_cooldown_secs),
                },
                SplitBrainBufferConfig {
                    enabled: config.replication.split_brain_log_enabled,
                    max_entries: config.replication.split_brain_buffer_size,
                    max_bytes: config.replication.split_brain_buffer_max_mb * 1024 * 1024,
                },
            ));

            // Store a reference for PSYNC connection handoff
            primary_replication_handler = Some(handler.clone());

            (handler as SharedBroadcaster, Some(tracker))
        } else if config.replication.is_replica() {
            // Initialize ReplicaReplicationHandler for replica role
            let primary_addr = format!(
                "{}:{}",
                config.replication.primary_host, config.replication.primary_port
            )
            .parse::<std::net::SocketAddr>()
            .map_err(|e| anyhow::anyhow!("Invalid primary address: {}", e))?;

            let state_path = config
                .persistence
                .data_dir
                .join(&config.replication.state_file);
            let repl_state = frogdb_core::ReplicationState::load_or_create(&state_path)
                .map_err(|e| anyhow::anyhow!("Failed to load replication state: {}", e))?;

            info!(
                primary = %primary_addr,
                replication_id = %repl_state.replication_id,
                offset = repl_state.replication_offset,
                "Initialized replica replication state"
            );

            let (handler, frame_rx) = ReplicaReplicationHandler::new(
                primary_addr,
                config.server.port,
                repl_state,
                config.persistence.data_dir.clone(),
            );

            replica_handler = Some(Arc::new(handler));
            replica_frame_rx = Some(frame_rx);

            // Replicas use NoopBroadcaster (they don't broadcast to other replicas)
            (Arc::new(NoopBroadcaster), None)
        } else {
            // Standalone mode
            (Arc::new(NoopBroadcaster), None)
        };

        // Initialize cluster state and Raft if cluster mode is enabled
        // NOTE: This must happen before shard creation so we can pass raft to workers
        let (cluster_state, node_id, raft, network_factory) = if config.cluster.enabled {
            // Derive node_id from cluster_bus address for deterministic IDs
            let cluster_addr = config.cluster.cluster_bus_socket_addr();
            let node_id = if config.cluster.node_id != 0 {
                config.cluster.node_id
            } else {
                hash_addr_to_node_id(&cluster_addr)
            };

            info!(
                node_id = node_id,
                cluster_bus_addr = %config.cluster.cluster_bus_addr,
                "Cluster mode enabled"
            );

            // Initialize Raft storage
            let raft_path = config.persistence.data_dir.join("raft");
            let raft_storage = ClusterStorage::open(&raft_path)
                .map_err(|e| anyhow::anyhow!("Failed to open Raft storage: {}", e))?;

            // Initialize Raft state machine with cluster state
            let cluster = ClusterState::new();
            let mut state_machine = ClusterStateMachine::with_state(cluster.clone());

            // Enable demotion detection for split-brain logging
            let demotion_rx = if config.replication.split_brain_log_enabled {
                Some(state_machine.enable_demotion_detection(node_id))
            } else {
                None
            };

            // Initialize Raft network factory
            let network_factory = ClusterNetworkFactory::new();

            // Process initial_nodes and register addresses
            let mut initial_members: std::collections::BTreeMap<u64, openraft::BasicNode> =
                std::collections::BTreeMap::new();

            for addr_str in &config.cluster.initial_nodes {
                if let Ok(peer_cluster_addr) = addr_str.parse::<std::net::SocketAddr>() {
                    let peer_node_id = hash_addr_to_node_id(&peer_cluster_addr);
                    network_factory.register_node(peer_node_id, peer_cluster_addr);
                    initial_members.insert(
                        peer_node_id,
                        openraft::BasicNode {
                            addr: peer_cluster_addr.to_string(),
                        },
                    );
                    info!(peer_node_id = peer_node_id, addr = %peer_cluster_addr, "Registered initial cluster peer");
                }
            }

            // Register this node's address
            let cluster_bus_addr = config.cluster.cluster_bus_socket_addr();
            network_factory.register_node(node_id, cluster_bus_addr);

            // Ensure this node is in initial_members
            initial_members
                .entry(node_id)
                .or_insert_with(|| openraft::BasicNode {
                    addr: cluster_bus_addr.to_string(),
                });

            // Determine if this node should bootstrap (lowest node_id)
            let should_bootstrap = initial_members.keys().next().copied() == Some(node_id);

            // Create Raft config
            let raft_config = openraft::Config {
                election_timeout_min: config.cluster.election_timeout_ms,
                election_timeout_max: config.cluster.election_timeout_ms * 2,
                heartbeat_interval: config.cluster.heartbeat_interval_ms,
                ..Default::default()
            };

            // Clone network factory before passing to Raft (so we can use it later for CLUSTER MEET/FORGET)
            let network_factory_clone = network_factory.clone();

            // Initialize Raft instance
            let raft = openraft::Raft::new(
                node_id,
                Arc::new(raft_config),
                network_factory,
                raft_storage,
                state_machine,
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to initialize Raft: {}", e))?;

            info!(node_id = node_id, "Raft initialized");

            // Bootstrap Raft cluster if this is the bootstrap node
            if should_bootstrap && !initial_members.is_empty() {
                // Check if already initialized (restart case)
                let metrics = raft.metrics().borrow().clone();
                let already_initialized =
                    metrics.membership_config.membership().nodes().count() > 0;

                if !already_initialized {
                    info!(
                        node_id = node_id,
                        member_count = initial_members.len(),
                        "Bootstrapping Raft cluster"
                    );
                    if let Err(e) = raft.initialize(initial_members.clone()).await {
                        warn!(error = %e, "Raft initialization error (may be already initialized)");
                    }
                } else {
                    info!(
                        node_id = node_id,
                        "Raft already initialized, skipping bootstrap"
                    );
                }
            }

            // Add all initial nodes to cluster_state
            for (peer_id, basic_node) in &initial_members {
                if let Ok(peer_cluster_addr) = basic_node.addr.parse::<std::net::SocketAddr>() {
                    let client_port = peer_cluster_addr.port().saturating_sub(10000);
                    let client_addr =
                        std::net::SocketAddr::new(peer_cluster_addr.ip(), client_port);
                    let peer_node = frogdb_core::cluster::NodeInfo::new_primary(
                        *peer_id,
                        client_addr,
                        peer_cluster_addr,
                    );
                    cluster.add_node(peer_node);
                }
            }

            // Add this node to the cluster state (ensure it's there even if not in initial_members)
            // Use the actual bound addresses (not config) to handle OS-assigned ports (port 0).
            let client_addr = listener.local_addr()?;
            let cluster_bus_addr = cluster_bus_listener
                .as_ref()
                .map(|l| l.local_addr())
                .transpose()?
                .unwrap_or_else(|| config.cluster.cluster_bus_socket_addr());
            let this_node =
                frogdb_core::cluster::NodeInfo::new_primary(node_id, client_addr, cluster_bus_addr);
            cluster.add_node(this_node);
            info!(node_id = node_id, "Node added to cluster state");

            // Auto-assign slots evenly on bootstrap
            // Only the bootstrap node assigns slots to avoid conflicts
            if should_bootstrap && !initial_members.is_empty() && !cluster.all_slots_assigned() {
                let node_ids: Vec<u64> = initial_members.keys().copied().collect();
                let num_nodes = node_ids.len();
                let slots_per_node = 16384 / num_nodes;

                for (i, &nid) in node_ids.iter().enumerate() {
                    let start = i * slots_per_node;
                    let end = if i == num_nodes - 1 {
                        16384 // Last node gets remainder
                    } else {
                        (i + 1) * slots_per_node
                    };
                    cluster.assign_slots(nid, (start as u16)..(end as u16));
                }
                info!(
                    node_count = num_nodes,
                    slots_per_node = slots_per_node,
                    "Auto-assigned slots to cluster nodes"
                );
            }

            let raft = Arc::new(raft);

            // Spawn self-registration via Raft so all nodes converge on correct peer
            // addresses. The initial add_node() calls above use guessed client ports
            // (cluster_port - 10000), which are wrong when ports are OS-assigned.
            // Each node knows its own real addresses, so it proposes AddNode for itself
            // via Raft consensus to correct the cluster state.
            // Leaders propose directly; followers forward through the cluster bus.
            {
                let raft_clone = raft.clone();
                let network_factory = network_factory_clone.clone();
                let self_node = frogdb_core::cluster::NodeInfo::new_primary(
                    node_id,
                    client_addr,
                    cluster_bus_addr,
                );
                tokio::spawn(async move {
                    for attempt in 0..30 {
                        let cmd = frogdb_core::cluster::ClusterCommand::AddNode {
                            node: self_node.clone(),
                        };
                        match raft_clone.client_write(cmd).await {
                            Ok(_) => {
                                info!(
                                    node_id = node_id,
                                    "Registered self in cluster state via Raft"
                                );
                                return;
                            }
                            Err(e) => {
                                // Check if this is a ForwardToLeader error
                                use openraft::error::{ClientWriteError, RaftError};
                                if let RaftError::APIError(ClientWriteError::ForwardToLeader(fwd)) =
                                    &e
                                    && let Some(leader_id) = fwd.leader_id
                                    && let Some(leader_addr) =
                                        network_factory.get_node_addr(leader_id)
                                {
                                    let net = frogdb_core::cluster::ClusterNetwork::new(
                                        leader_id,
                                        leader_addr,
                                    );
                                    let fwd_cmd = frogdb_core::cluster::ClusterCommand::AddNode {
                                        node: self_node.clone(),
                                    };
                                    if net.forward_write(fwd_cmd).await.is_ok() {
                                        info!(
                                            node_id = node_id,
                                            leader_id = leader_id,
                                            "Registered self via leader forward"
                                        );
                                        return;
                                    }
                                }
                                if attempt < 29 {
                                    tokio::time::sleep(Duration::from_millis(500)).await;
                                } else {
                                    warn!(
                                        node_id = node_id,
                                        error = %e,
                                        "Failed to self-register after 30 attempts"
                                    );
                                }
                            }
                        }
                    }
                });
            }

            // Spawn split-brain demotion handler if enabled
            if let Some(mut demotion_rx) = demotion_rx {
                let data_dir = config.persistence.data_dir.clone();
                let broadcaster: SharedBroadcaster = replication_broadcaster.clone();
                let tracker = replication_tracker.clone();
                let metrics = metrics_recorder.clone();
                spawn(async move {
                    while let Some(event) = demotion_rx.recv().await {
                        tracing::warn!(
                            demoted_node = event.demoted_node_id,
                            new_primary = ?event.new_primary_id,
                            epoch = event.epoch,
                            "Split-brain demotion detected"
                        );

                        // Determine divergence boundary
                        let min_acked = tracker
                            .as_ref()
                            .and_then(|t| t.min_acked_offset())
                            .unwrap_or(0);
                        let current = broadcaster.current_offset();

                        if current > min_acked {
                            let divergent = broadcaster.extract_divergent_writes(min_acked);
                            if !divergent.is_empty() {
                                let header =
                                    frogdb_replication::split_brain_log::SplitBrainLogHeader {
                                        timestamp: String::new(),
                                        old_primary: format!("{:x}", event.demoted_node_id),
                                        new_primary: event
                                            .new_primary_id
                                            .map(|id| format!("{:x}", id))
                                            .unwrap_or_else(|| "unknown".to_string()),
                                        epoch_old: event.epoch,
                                        epoch_new: event.epoch.saturating_add(1),
                                        seq_diverge_start: min_acked,
                                        seq_diverge_end: current,
                                        ops_discarded: divergent.len(),
                                    };

                                match frogdb_replication::split_brain_log::write_log(
                                    &data_dir, header, &divergent,
                                ) {
                                    Ok(path) => {
                                        tracing::warn!(
                                            ops = divergent.len(),
                                            path = %path.display(),
                                            "Split-brain: {} divergent writes logged to {}",
                                            divergent.len(),
                                            path.display()
                                        );
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            error = %e,
                                            "Failed to write split-brain log"
                                        );
                                    }
                                }

                                frogdb_telemetry::definitions::SplitBrainEventsTotal::inc(
                                    &*metrics,
                                );
                                frogdb_telemetry::definitions::SplitBrainOpsDiscardedTotal::inc_by(
                                    &*metrics,
                                    divergent.len() as u64,
                                );
                                frogdb_telemetry::definitions::SplitBrainRecoveryPending::set(
                                    &*metrics, 1.0,
                                );
                            }
                        }
                    }
                });
            }

            (
                Some(Arc::new(cluster)),
                Some(node_id),
                Some(raft),
                Some(Arc::new(network_factory_clone)),
            )
        } else {
            (None, None, None, None)
        };

        // Create failure detector early so we can pass it to shards
        let (failure_detector, failure_detector_handle) = if let (
            Some(raft_arc),
            Some(state_arc),
            Some(nid),
        ) = (&raft, &cluster_state, node_id)
        {
            let detector_config = FailureDetectorConfig {
                check_interval_ms: config.cluster.heartbeat_interval_ms,
                connect_timeout_ms: config.cluster.heartbeat_interval_ms / 2,
                fail_threshold: config.cluster.fail_threshold,
                auto_failover: config.cluster.auto_failover,
            };

            let detector = Arc::new(FailureDetector::new(
                nid,
                detector_config,
                state_arc.clone(),
                raft_arc.clone(),
            ));

            info!(
                node_id = nid,
                auto_failover = config.cluster.auto_failover,
                fail_threshold = config.cluster.fail_threshold,
                "Failure detector initialized"
            );

            let handle = spawn_failure_detector_task(detector.clone());
            (Some(detector), Some(handle))
        } else {
            (None, None)
        };

        for (shard_id, (msg_rx, conn_rx)) in shard_receivers
            .into_iter()
            .zip(new_conn_receivers.into_iter())
            .enumerate()
        {
            let mut worker = if let Some(ref rocks) = rocks_store {
                // Get recovered store for this shard
                let (store, _expiry_index) = recovered_iter
                    .as_mut()
                    .and_then(|iter| iter.next())
                    .unwrap_or_default();

                ShardWorker::with_persistence(
                    shard_id,
                    num_shards,
                    store,
                    msg_rx,
                    conn_rx,
                    shard_senders.clone(),
                    registry.clone(),
                    rocks.clone(),
                    wal_config.clone(),
                    snapshot_coordinator.clone(),
                    eviction_config.clone(),
                    metrics_recorder.clone(),
                    slowlog_next_id.clone(),
                    replication_broadcaster.clone(),
                )
            } else {
                ShardWorker::with_eviction(
                    shard_id,
                    num_shards,
                    msg_rx,
                    conn_rx,
                    shard_senders.clone(),
                    registry.clone(),
                    eviction_config.clone(),
                    metrics_recorder.clone(),
                    slowlog_next_id.clone(),
                    replication_broadcaster.clone(),
                )
            };

            // Set function registry on each shard
            worker.set_function_registry(function_registry.clone());

            // Set cluster-related fields if cluster mode is enabled
            if let Some(ref raft_instance) = raft {
                worker.set_raft(raft_instance.clone());
            }
            if let Some(ref state) = cluster_state {
                worker.set_cluster_state(state.clone());
            }
            if let Some(id) = node_id {
                worker.set_node_id(id);
            }
            if let Some(ref factory) = network_factory {
                worker.set_network_factory(factory.clone());
            }
            if let Some(ref detector) = failure_detector {
                worker.set_quorum_checker(detector.clone());
            }

            // Share the per-request spans toggle with shard workers
            worker.per_request_spans = config_manager.per_request_spans_flag();

            let handle = spawn(shard_monitor.instrument(async move {
                worker.run().await;
            }));

            shard_handles.push(handle);
        }

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
                    warn!(error = %e, "Failed to initialize tracer, continuing without tracing");
                    None
                }
            }
        } else {
            None
        };

        // Create shard config notifier for propagating runtime config changes
        let shard_notifier = Arc::new(ShardConfigNotifier::new(
            shard_senders.clone(),
            config_manager.runtime_ref(),
            num_shards,
        ));
        config_manager.set_shard_notifier(shard_notifier);

        // Spawn task monitor collector (tokio-metrics)
        let task_monitor_handle =
            task_registry.spawn_collector(metrics_recorder.clone(), Duration::from_secs(10));

        // Create latency band tracker if enabled
        let band_tracker = if config.latency_bands.enabled {
            let tracker =
                frogdb_telemetry::LatencyBandTracker::new(config.latency_bands.bands.clone());
            info!(
                bands = ?config.latency_bands.bands,
                "Latency band tracking enabled"
            );
            Some(Arc::new(tracker))
        } else {
            None
        };

        Ok(Self {
            config,
            listener,
            admin_listener,
            metrics_listener,
            admin_http_listener,
            cluster_bus_listener,
            registry,
            client_registry,
            config_manager,
            shard_senders,
            new_conn_senders,
            shard_handles,
            rocks_store,
            periodic_sync_handle,
            periodic_snapshot_handle,
            snapshot_coordinator,
            metrics_recorder,
            prometheus_recorder,
            health_checker,
            acl_manager,
            function_registry,
            shared_tracer,
            replication_tracker,
            cluster_state,
            node_id,
            raft,
            latency_baseline: None,
            band_tracker,
            network_factory,
            failure_detector,
            failure_detector_handle,
            replica_handler,
            replica_frame_rx,
            primary_replication_handler,
            conn_monitor: Some(conn_monitor),
            _task_monitor_handle: Some(task_monitor_handle),
        })
    }

    /// Spawn periodic snapshot task.
    fn spawn_periodic_snapshot_task(
        coordinator: Arc<dyn SnapshotCoordinator>,
        interval_secs: u64,
    ) -> crate::net::JoinHandle<()> {
        info!(interval_secs, "Starting periodic snapshot task");

        spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));

            loop {
                interval.tick().await;

                if coordinator.in_progress() {
                    tracing::debug!("Skipping periodic snapshot - already in progress");
                    continue;
                }

                match coordinator.start_snapshot() {
                    Ok(handle) => {
                        tracing::info!(epoch = handle.epoch(), "Periodic snapshot started");
                        // Handle completes when background task finishes
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "Periodic snapshot failed to start");
                    }
                }
            }
        })
    }

    /// Initialize persistence layer.
    fn init_persistence(
        config: &PersistenceConfig,
        num_shards: usize,
        wal_sync_monitor: Option<tokio_metrics::TaskMonitor>,
    ) -> Result<PersistenceInitResult> {
        use std::fs;

        info!(
            data_dir = %config.data_dir.display(),
            durability_mode = %config.durability_mode,
            "Initializing persistence"
        );

        // Ensure data directory exists
        fs::create_dir_all(&config.data_dir)?;

        // Check for and load staged checkpoint from replica full sync
        match RocksStore::load_staged_checkpoint(&config.data_dir) {
            Ok(true) => {
                info!("Loaded staged checkpoint from replica full sync");
            }
            Ok(false) => {
                // No checkpoint to load, continue normally
            }
            Err(e) => {
                error!(error = %e, "Failed to load staged checkpoint");
                return Err(anyhow::anyhow!("Failed to load staged checkpoint: {}", e));
            }
        }

        // Build RocksDB config
        let rocks_config = RocksConfig {
            write_buffer_size: config.write_buffer_size_mb * 1024 * 1024,
            compression: parse_compression(&config.compression),
            max_background_jobs: num_cpus::get() as i32,
            create_if_missing: true,
            block_cache_size: config.block_cache_size_mb * 1024 * 1024,
            bloom_filter_bits: config.bloom_filter_bits,
            max_write_buffer_number: config.max_write_buffer_number,
            level0_file_num_compaction_trigger: 8,
            target_file_size_base: 128 * 1024 * 1024,
            max_bytes_for_level_base: 512 * 1024 * 1024,
            compaction_rate_limit_mb: if config.compaction_rate_limit_mb > 0 {
                Some(config.compaction_rate_limit_mb)
            } else {
                None
            },
        };

        // Open RocksDB
        let rocks = Arc::new(RocksStore::open(
            &config.data_dir,
            num_shards,
            &rocks_config,
        )?);

        // Recover data if database has existing data
        let recovered = if rocks.has_data() {
            info!("Recovering data from RocksDB...");
            let (stores, stats) = recover_all_shards(&rocks)?;
            info!(
                keys_loaded = stats.keys_loaded,
                keys_expired = stats.keys_expired_skipped,
                bytes = stats.bytes_loaded,
                duration_ms = stats.duration_ms,
                "Recovery complete"
            );
            stores
        } else {
            info!("No existing data found, starting fresh");
            (0..num_shards).map(|_| Default::default()).collect()
        };

        // Start periodic sync if using periodic durability mode
        let sync_handle = if config.durability_mode.to_lowercase() == "periodic" {
            info!(
                interval_ms = config.sync_interval_ms,
                "Starting periodic WAL sync"
            );
            Some(spawn_periodic_sync(rocks.clone(), config.sync_interval_ms, wal_sync_monitor))
        } else {
            None
        };

        Ok((rocks, recovered, sync_handle))
    }

    /// Run the server until the provided future completes.
    ///
    /// Use this for testing where OS signals aren't available (e.g., Turmoil simulation).
    pub async fn run_until<F>(mut self, shutdown: F) -> Result<()>
    where
        F: std::future::Future<Output = ()>,
    {
        // Check for pending split-brain logs and set metric
        if frogdb_replication::split_brain_log::has_pending_logs(&self.config.persistence.data_dir)
        {
            warn!("Unprocessed split-brain log files found in data directory");
            frogdb_telemetry::definitions::SplitBrainRecoveryPending::set(
                &*self.metrics_recorder,
                1.0,
            );
        }

        // Run startup latency test if configured
        if self.config.latency.startup_test {
            info!(
                "Running startup latency test for {} seconds...",
                self.config.latency.startup_test_duration_secs
            );

            let result = latency_test::run_intrinsic_latency_test(
                self.config.latency.startup_test_duration_secs,
                None,
            );

            // Check against warning threshold
            if result.max_us > self.config.latency.warning_threshold_us {
                warn!(
                    max_latency_us = result.max_us,
                    threshold_us = self.config.latency.warning_threshold_us,
                    "High intrinsic latency detected. This may indicate virtualization \
                     overhead or system contention."
                );
            }

            info!(
                min_us = result.min_us,
                max_us = result.max_us,
                avg_us = format!("{:.1}", result.avg_us),
                p99_us = result.p99_us,
                samples = result.samples,
                "Latency baseline established"
            );

            // Store globally for INFO command access
            latency_test::set_global_baseline(
                result.clone(),
                self.config.latency.warning_threshold_us,
            );
            self.latency_baseline = Some(result);
        }

        // Capture server start time
        let start_time = std::time::Instant::now();

        // Start metrics server if enabled
        let metrics_server_handle = if let Some(ref prometheus) = self.prometheus_recorder {
            // Create debug state for the debug web UI
            let config_entries = vec![
                ConfigEntry {
                    name: "bind".into(),
                    value: self.config.server.bind.clone(),
                },
                ConfigEntry {
                    name: "port".into(),
                    value: self.config.server.port.to_string(),
                },
                ConfigEntry {
                    name: "num_shards".into(),
                    value: self.shard_senders.len().to_string(),
                },
                ConfigEntry {
                    name: "metrics_bind".into(),
                    value: self.config.metrics.bind.clone(),
                },
                ConfigEntry {
                    name: "metrics_port".into(),
                    value: self.config.metrics.port.to_string(),
                },
            ];
            let debug_state = DebugState::new(
                ServerInfo {
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    start_time,
                    num_shards: self.shard_senders.len(),
                    bind_addr: self.config.server.bind.clone(),
                    port: self.config.server.port,
                },
                config_entries,
            );

            // Create status collector for /status/json endpoint
            let status_collector_config = self.config.status.to_collector_config();
            let mode = if self.config.cluster.enabled {
                "cluster".to_string()
            } else if self.config.replication.is_primary() {
                "primary".to_string()
            } else if self.config.replication.is_replica() {
                "replica".to_string()
            } else {
                "standalone".to_string()
            };
            let status_collector = Arc::new(StatusCollector::new(
                status_collector_config,
                self.health_checker.clone(),
                self.shard_senders.clone(),
                self.client_registry.clone(),
                prometheus.clone(),
                start_time,
                0, // max_clients (0 = unlimited for now)
                self.config.memory.maxmemory,
                self.config.persistence.enabled,
                self.config.persistence.durability_mode.clone(),
                mode,
            ));

            // SAFETY: metrics_listener is Some when prometheus_recorder is Some
            // (both are gated on config.metrics.enabled in Server::new()).
            let metrics_listener = self
                .metrics_listener
                .take()
                .expect("metrics_listener must be set when metrics are enabled");
            let metrics_bound_addr = metrics_listener.local_addr()?;

            let metrics_config = crate::config::MetricsConfig {
                bind: self.config.metrics.bind.clone(),
                port: metrics_bound_addr.port(),
                enabled: true,
                ..Default::default()
            };
            let mut server = ObservabilityServer::new(
                metrics_config,
                prometheus.clone(),
                self.health_checker.clone(),
            )
            .with_listener(metrics_listener)
            .with_debug_state(debug_state)
            .with_status_collector(status_collector);

            // Add band tracker if configured
            if let Some(tracker) = &self.band_tracker {
                server = server.with_band_tracker(tracker.clone());
            }

            info!(
                addr = %metrics_bound_addr,
                debug_ui = %format!("http://{}/debug", metrics_bound_addr),
                status_json = %format!("http://{}/status/json", metrics_bound_addr),
                "Metrics server starting"
            );

            Some(server.spawn())
        } else {
            None
        };

        // Start system metrics collector if metrics enabled
        let system_collector_handle = if self.prometheus_recorder.is_some() {
            Some(SystemMetricsCollector::spawn_collector(
                self.metrics_recorder.clone(),
                Duration::from_secs(15),
            ))
        } else {
            None
        };


        // Determine if admin port is enabled (used for both acceptors)
        let admin_enabled = self.config.admin.enabled;

        // Start admin server if enabled
        let admin_server_handle = if self.config.admin.enabled {
            use crate::admin::server::AdminState;

            let admin_state = AdminState {
                cluster_state: self.cluster_state.clone(),
                replication_tracker: self.replication_tracker.clone(),
                node_id: self.node_id,
                client_addr: self.config.bind_addr(),
                cluster_bus_addr: if self.config.cluster.enabled {
                    Some(self.config.cluster.cluster_bus_addr.clone())
                } else {
                    None
                },
            };

            // SAFETY: admin_http_listener is Some when config.admin.enabled is true
            // (both are gated on the same condition in Server::new()).
            let admin_http_listener = self
                .admin_http_listener
                .take()
                .expect("admin_http_listener must be set when admin API is enabled");

            let admin_server =
                crate::admin::AdminServer::new(self.config.admin.clone(), admin_state)
                    .with_listener(admin_http_listener);

            Some(spawn(async move {
                if let Err(e) = admin_server.run().await {
                    error!(error = %e, "Admin server error");
                }
            }))
        } else {
            None
        };

        // Start cluster bus TCP server if cluster mode is enabled
        let cluster_bus_handle = if let Some(ref raft) = self.raft {
            // SAFETY: cluster_bus_listener is Some when raft is Some
            // (both gated on config.cluster.enabled in Server::new()).
            let cluster_bus_listener = self
                .cluster_bus_listener
                .take()
                .expect("cluster_bus_listener must be set when cluster is enabled");
            let raft = raft.clone();
            Some(spawn(async move {
                if let Err(e) = crate::cluster_bus::run(cluster_bus_listener, raft).await {
                    error!(error = %e, "Cluster bus server error");
                }
            }))
        } else {
            None
        };

        // Start replica replication if running as replica
        let replica_handle = if let (Some(handler), Some(frame_rx)) =
            (self.replica_handler.take(), self.replica_frame_rx.take())
        {
            let shard_senders = self.shard_senders.clone();
            let num_shards = self.config.server.num_shards.max(1);

            // Spawn replication connection task (connects to primary and receives frames)
            let handler_clone = handler.clone();
            let repl_conn_handle = spawn(async move {
                if let Err(e) = handler_clone.start().await {
                    error!(error = %e, "Replica replication connection error");
                }
            });

            // Spawn frame consumer task (applies replicated commands to shards)
            let executor = ReplicaCommandExecutor::new(shard_senders, num_shards);
            let frame_consumer_handle = spawn(async move {
                consume_frames(frame_rx, executor).await;
            });

            info!("Replica replication tasks started");

            Some((repl_conn_handle, frame_consumer_handle))
        } else {
            None
        };

        // Create quorum checker for self-fencing (write rejection on quorum loss)
        let quorum_checker: Option<Arc<dyn frogdb_core::command::QuorumChecker>> =
            if self.config.cluster.self_fence_on_quorum_loss {
                self.failure_detector
                    .clone()
                    .map(|fd| fd as Arc<dyn frogdb_core::command::QuorumChecker>)
            } else {
                None
            };

        // Create main acceptor (regular client connections)
        // When admin port is enabled, this acceptor blocks admin commands
        let is_replica = self.config.replication.is_replica();
        let acceptor = Acceptor::new(
            self.listener,
            self.new_conn_senders.clone(),
            self.shard_senders.clone(),
            self.registry.clone(),
            self.client_registry.clone(),
            self.config_manager.clone(),
            self.config.server.allow_cross_slot_standalone,
            self.config.server.scatter_gather_timeout_ms,
            self.metrics_recorder.clone(),
            self.acl_manager.clone(),
            self.snapshot_coordinator.clone(),
            self.function_registry.clone(),
            self.shared_tracer.clone(),
            self.config.tracing.clone(),
            self.replication_tracker.clone(),
            self.cluster_state.clone(),
            self.node_id,
            false, // is_admin = false for regular port
            admin_enabled,
            self.config.hotshards.to_collector_config(),
            self.config.memory.to_diag_config(),
            self.band_tracker.clone(),
            self.raft.clone(),
            self.network_factory.clone(),
            self.primary_replication_handler.clone(),
            is_replica,
            quorum_checker.clone(),
            self.conn_monitor.clone(),
        );

        // Spawn main acceptor task
        let acceptor_handle = spawn(async move {
            if let Err(e) = acceptor.run().await {
                error!(error = %e, "Acceptor error");
            }
        });

        // Spawn admin acceptor if admin port is enabled
        let admin_acceptor_handle = if let Some(admin_listener) = self.admin_listener.take() {
            let admin_acceptor = Acceptor::new(
                admin_listener,
                self.new_conn_senders,
                self.shard_senders.clone(),
                self.registry.clone(),
                self.client_registry.clone(),
                self.config_manager.clone(),
                self.config.server.allow_cross_slot_standalone,
                self.config.server.scatter_gather_timeout_ms,
                self.metrics_recorder.clone(),
                self.acl_manager.clone(),
                self.snapshot_coordinator.clone(),
                self.function_registry.clone(),
                self.shared_tracer.clone(),
                self.config.tracing.clone(),
                self.replication_tracker.clone(),
                self.cluster_state.clone(),
                self.node_id,
                true, // is_admin = true for admin port
                admin_enabled,
                self.config.hotshards.to_collector_config(),
                self.config.memory.to_diag_config(),
                self.band_tracker.clone(),
                self.raft.clone(),
                self.network_factory.clone(),
                self.primary_replication_handler.clone(),
                is_replica,
                quorum_checker.clone(),
                self.conn_monitor.clone(),
            );

            Some(spawn(async move {
                if let Err(e) = admin_acceptor.run().await {
                    error!(error = %e, "Admin acceptor error");
                }
            }))
        } else {
            None
        };

        // Mark server as ready
        self.health_checker.set_ready();

        info!(
            addr = %self.config.bind_addr(),
            "FrogDB server ready"
        );

        // Wait for shutdown signal
        shutdown.await;

        info!("Shutdown signal received, stopping server...");

        // Mark server as not ready during shutdown
        self.health_checker.shutdown();

        // Send shutdown to all shards
        for sender in self.shard_senders.iter() {
            let _ = sender.send(ShardMessage::Shutdown).await;
        }

        // Wait for shard workers to finish
        for handle in self.shard_handles {
            let _ = handle.await;
        }

        // Stop periodic sync task if running
        if let Some(handle) = self.periodic_sync_handle {
            handle.abort();
        }

        // Stop periodic snapshot task if running
        if let Some(handle) = self.periodic_snapshot_handle {
            handle.abort();
        }

        // Wait for any in-progress snapshot to complete before final flush
        if self.snapshot_coordinator.in_progress() {
            info!("Waiting for in-progress snapshot to complete...");
            while self.snapshot_coordinator.in_progress() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            info!("Snapshot completed");
        }

        // Stop metrics server, system collector, admin server, and cluster bus
        if let Some(handle) = metrics_server_handle {
            handle.abort();
        }
        if let Some(handle) = system_collector_handle {
            handle.abort();
        }
        if let Some(handle) = admin_server_handle {
            handle.abort();
        }
        if let Some(handle) = cluster_bus_handle {
            handle.abort();
        }

        // Stop replica replication tasks if running
        if let Some((conn_handle, consumer_handle)) = replica_handle {
            conn_handle.abort();
            consumer_handle.abort();
        }

        // Stop failure detector task if running
        if let Some(handle) = self.failure_detector_handle {
            handle.abort();
        }

        // Shutdown tracer and flush pending spans
        if let Some(ref tracer) = self.shared_tracer {
            info!("Shutting down distributed tracer...");
            tracer.shutdown();
        }

        // Final flush of RocksDB
        if let Some(ref rocks) = self.rocks_store {
            if let Err(e) = rocks.flush() {
                error!(error = %e, "Failed to flush RocksDB on shutdown");
            } else {
                info!("RocksDB flushed successfully");
            }
        }

        // Abort acceptors
        acceptor_handle.abort();
        if let Some(handle) = admin_acceptor_handle {
            handle.abort();
        }

        info!("Server shutdown complete");

        Ok(())
    }

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
        self.listener.local_addr()
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
}
