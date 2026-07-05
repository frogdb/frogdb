//! TCP connection acceptor.

use anyhow::Result;
use frogdb_core::sync::{Arc, AtomicUsize, Ordering};
use frogdb_core::{
    AclManager, ClientRegistry, ClusterNetworkFactory, ClusterRaft, ClusterState,
    CommandLatencyHistograms, CommandRegistry, MetricsRecorder, ReplicationTrackerImpl,
    ShardSender, SharedFunctionRegistry, SharedHotkeySession, command::QuorumChecker,
    persistence::SnapshotCoordinator, shard::NewConnection,
};

use crate::cluster_pubsub::ClusterPubSubForwarder;
use frogdb_telemetry::SharedTracer;
use frogdb_telemetry::definitions::{
    ConnectionsCurrent, ConnectionsRejected, ConnectionsTotal, TlsHandshakeErrors,
};
use frogdb_telemetry::labels::{RejectionReason, TlsHandshakeError};
use std::sync::atomic::AtomicI64;

use crate::config::TracingConfig;
use crate::replication::PrimaryReplicationHandler;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

use crate::connection::ConnectionHandler;
use crate::connection::deps::{AdminDeps, ClusterDeps, CoreDeps, ObservabilityDeps};
use crate::net::{TcpListener, spawn};
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
    shard_senders: Arc<Vec<ShardSender>>,

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

    /// Cursor store for FT.AGGREGATE WITHCURSOR / FT.CURSOR.
    cursor_store: Arc<crate::cursor_store::AggregateCursorStore>,

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
    hotshards_config: frogdb_debug::HotShardConfig,

    /// Memory diagnostics configuration.
    memory_diag_config: frogdb_debug::MemoryDiagConfig,

    /// Optional Raft instance (only when cluster mode is enabled).
    raft: Option<Arc<ClusterRaft>>,

    /// Optional network factory for cluster node management.
    network_factory: Option<Arc<ClusterNetworkFactory>>,

    /// Optional slot migration coordinator (only when cluster mode is enabled).
    slot_migration: Option<Arc<crate::slot_migration::SlotMigrationCoordinator>>,

    /// Optional primary replication handler for PSYNC connection handoff.
    primary_replication_handler: Option<Arc<PrimaryReplicationHandler>>,

    /// Shared replication state (IDs + offset) for INFO `master_replid`.
    ///
    /// Holds the active role's `Arc<RwLock<ReplicationState>>` (primary or
    /// replica). `None` in standalone and pure cluster mode.
    replication_state: Option<Arc<tokio::sync::RwLock<frogdb_core::ReplicationState>>>,

    /// Maximum simultaneous client connections (0 = unlimited). Admin exempt.
    max_clients: Arc<std::sync::atomic::AtomicU64>,

    /// Whether per-request tracing spans are enabled.
    per_request_spans: Arc<std::sync::atomic::AtomicBool>,

    /// Whether this server is a replica (rejects write commands from clients).
    /// Shared across all connections so REPLICAOF NO ONE takes effect immediately.
    is_replica: Arc<std::sync::atomic::AtomicBool>,

    /// Optional quorum checker for self-fencing (write rejection on quorum loss).
    quorum_checker: Option<Arc<dyn QuorumChecker>>,

    /// Optional task monitor for connection handler tasks.
    conn_monitor: Option<tokio_metrics::TaskMonitor>,

    /// Optional pub/sub forwarder for cross-node message delivery.
    pubsub_forwarder: Option<Arc<ClusterPubSubForwarder>>,

    /// MONITOR command broadcaster.
    monitor_broadcaster: Arc<crate::monitor::MonitorBroadcaster>,

    /// Server-wide per-command latency histograms for INFO latencystats.
    latency_histograms: Arc<CommandLatencyHistograms>,

    /// Server-wide hotkey sampling session.
    hotkey_session: SharedHotkeySession,

    /// Process-wide keyspace hit/miss accumulator, shared with the shard
    /// workers (INFO reads it, RESETSTAT rebases it).
    keyspace_stats: Arc<frogdb_core::KeyspaceStats>,

    /// Chaos testing configuration (turmoil simulation only).
    #[cfg(feature = "turmoil")]
    chaos_config: Arc<crate::config::ChaosConfig>,

    /// Optional TLS manager for accepting TLS connections.
    #[cfg(not(feature = "turmoil"))]
    tls_manager: Option<Arc<crate::tls::TlsManager>>,

    /// TLS handshake timeout duration.
    #[cfg(not(feature = "turmoil"))]
    tls_handshake_timeout: std::time::Duration,
}

/// Dependencies shared by every acceptor (main, admin, and TLS ports).
///
/// The three ports differ only in which listener they own, whether they
/// treat connections as admin connections, and whether they perform a TLS
/// handshake (see [`PortSpec`]) — everything else is identical. Built once
/// per server start in `start_subsystems` and cheaply cloned (all fields are
/// `Arc`/`Copy`/small) for each [`Acceptor::bind`] call, instead of being
/// threaded positionally through three near-identical ~38-argument
/// constructor calls.
#[derive(Clone)]
pub struct AcceptorContext {
    /// Core dependencies for command routing (registry, shard senders, ACLs).
    pub core: CoreDeps,

    /// Dependencies for admin-only functionality (CLIENT, CONFIG, snapshots, ...).
    pub admin: AdminDeps,

    /// Cluster-mode dependencies. `Default` (all `None`) in standalone mode.
    pub cluster: ClusterDeps,

    /// Observability dependencies (metrics, tracing, MONITOR, hotkeys, ...).
    pub observability: ObservabilityDeps,

    /// New connection senders (one per shard).
    pub new_conn_senders: Vec<mpsc::Sender<NewConnection>>,

    /// Allow cross-slot operations.
    pub allow_cross_slot: bool,

    /// Scatter-gather timeout in milliseconds.
    pub scatter_gather_timeout_ms: u64,

    /// Whether admin port separation is enabled (admin commands blocked on
    /// the regular port).
    pub admin_enabled: bool,

    /// Hot shard detection configuration.
    pub hotshards_config: frogdb_debug::HotShardConfig,

    /// Memory diagnostics configuration.
    pub memory_diag_config: frogdb_debug::MemoryDiagConfig,

    /// Maximum simultaneous client connections (0 = unlimited). Admin exempt.
    pub max_clients: Arc<std::sync::atomic::AtomicU64>,

    /// Whether this server is a replica (rejects write commands from clients).
    pub is_replica: Arc<std::sync::atomic::AtomicBool>,

    /// Optional task monitor for connection handler tasks.
    pub conn_monitor: Option<tokio_metrics::TaskMonitor>,

    /// Chaos testing configuration (turmoil simulation only).
    #[cfg(feature = "turmoil")]
    pub chaos_config: Arc<crate::config::ChaosConfig>,

    /// TLS handshake timeout duration (shared across whichever ports enable TLS).
    #[cfg(not(feature = "turmoil"))]
    pub tls_handshake_timeout: std::time::Duration,
}

/// Per-listener configuration: the pieces that actually differ between the
/// main, admin, and TLS ports.
pub struct PortSpec {
    /// The TCP listener this acceptor owns.
    pub listener: TcpListener,

    /// Whether this acceptor handles admin connections.
    pub is_admin: bool,

    /// TLS manager to perform a handshake with, if this port accepts TLS.
    #[cfg(not(feature = "turmoil"))]
    pub tls_manager: Option<Arc<crate::tls::TlsManager>>,
}

impl Acceptor {
    /// Bind an acceptor to a listener, combining the shared [`AcceptorContext`]
    /// with the per-listener [`PortSpec`].
    pub fn bind(ctx: AcceptorContext, spec: PortSpec) -> Self {
        let num_shards = ctx.new_conn_senders.len();
        let per_request_spans = ctx.admin.config_manager.per_request_spans_flag();
        Self {
            listener: spec.listener,
            new_conn_senders: ctx.new_conn_senders,
            shard_senders: ctx.core.shard_senders,
            registry: ctx.core.registry,
            client_registry: ctx.admin.client_registry,
            config_manager: ctx.admin.config_manager,
            assigner: RoundRobinAssigner::new(num_shards),
            allow_cross_slot: ctx.allow_cross_slot,
            scatter_gather_timeout_ms: ctx.scatter_gather_timeout_ms,
            metrics_recorder: ctx.observability.metrics_recorder,
            current_connections: Arc::new(AtomicI64::new(0)),
            acl_manager: ctx.core.acl_manager,
            snapshot_coordinator: ctx.admin.snapshot_coordinator,
            function_registry: ctx.admin.function_registry,
            cursor_store: ctx.admin.cursor_store,
            shared_tracer: ctx.observability.shared_tracer,
            tracing_config: ctx.observability.tracing_config,
            replication_tracker: ctx.cluster.replication_tracker,
            cluster_state: ctx.cluster.cluster_state,
            node_id: ctx.cluster.node_id,
            is_admin: spec.is_admin,
            admin_enabled: ctx.admin_enabled,
            hotshards_config: ctx.hotshards_config,
            memory_diag_config: ctx.memory_diag_config,
            raft: ctx.cluster.raft,
            network_factory: ctx.cluster.network_factory,
            slot_migration: ctx.cluster.slot_migration,
            primary_replication_handler: ctx.cluster.primary_replication_handler,
            replication_state: ctx.cluster.replication_state,
            max_clients: ctx.max_clients,
            per_request_spans,
            is_replica: ctx.is_replica,
            quorum_checker: ctx.cluster.quorum_checker,
            conn_monitor: ctx.conn_monitor,
            pubsub_forwarder: ctx.cluster.pubsub_forwarder,
            monitor_broadcaster: ctx.observability.monitor_broadcaster,
            latency_histograms: ctx.observability.latency_histograms,
            hotkey_session: ctx.observability.hotkey_session,
            keyspace_stats: ctx.observability.keyspace_stats,
            #[cfg(feature = "turmoil")]
            chaos_config: ctx.chaos_config,
            #[cfg(not(feature = "turmoil"))]
            tls_manager: spec.tls_manager,
            #[cfg(not(feature = "turmoil"))]
            tls_handshake_timeout: ctx.tls_handshake_timeout,
        }
    }

    /// Run the acceptor loop.
    pub async fn run(self) -> Result<()> {
        info!("Acceptor started");

        loop {
            match self.listener.accept().await {
                Ok((mut socket, addr)) => {
                    // Disable Nagle's algorithm for lower latency on small writes
                    if let Err(e) = socket.set_nodelay(true) {
                        error!(error = %e, "Failed to set TCP_NODELAY");
                    }

                    // Check maxclients limit (admin port is exempt)
                    if !self.is_admin {
                        let limit = self.max_clients.load(std::sync::atomic::Ordering::Relaxed);
                        if limit > 0 {
                            let current = self.current_connections.load(Ordering::SeqCst);
                            if current >= limit as i64 {
                                ConnectionsRejected::inc(
                                    &*self.metrics_recorder,
                                    RejectionReason::MaxClients,
                                );
                                // Best-effort error write, then graceful close
                                spawn(async move {
                                    use tokio::io::AsyncWriteExt;
                                    let _ = socket
                                        .write_all(b"-ERR max number of clients reached\r\n")
                                        .await;
                                    let _ = socket.shutdown().await;
                                });
                                continue;
                            }
                        }
                    }

                    let conn_id = next_conn_id();
                    let shard_id = self.assigner.assign();

                    // Get local address before wrapping in MaybeTlsStream
                    let local_addr = socket.local_addr().ok();

                    // Wrap the raw TCP socket in ConnectionStream.
                    // If a TLS manager is configured, perform TLS handshake.
                    // Otherwise, wrap as plain TCP.
                    #[cfg(not(feature = "turmoil"))]
                    let socket: crate::net::ConnectionStream =
                        if let Some(ref tls_mgr) = self.tls_manager {
                            let acceptor = tls_mgr.acceptor();
                            match tokio::time::timeout(
                                self.tls_handshake_timeout,
                                acceptor.accept(socket),
                            )
                            .await
                            {
                                Ok(Ok(tls_stream)) => {
                                    crate::tls::MaybeTlsStream::Tls { inner: tls_stream }
                                }
                                Ok(Err(e)) => {
                                    debug!(addr = %addr, error = %e, "TLS handshake failed");
                                    TlsHandshakeErrors::inc(
                                        &*self.metrics_recorder,
                                        TlsHandshakeError::HandshakeError,
                                    );
                                    continue;
                                }
                                Err(_) => {
                                    debug!(addr = %addr, "TLS handshake timed out");
                                    TlsHandshakeErrors::inc(
                                        &*self.metrics_recorder,
                                        TlsHandshakeError::Timeout,
                                    );
                                    continue;
                                }
                            }
                        } else {
                            crate::tls::MaybeTlsStream::Plain { inner: socket }
                        };

                    // Register connection with client registry
                    let client_handle = self.client_registry.register(conn_id, addr, local_addr);

                    // Record connection metrics
                    ConnectionsTotal::inc(&*self.metrics_recorder);
                    let current = self.current_connections.fetch_add(1, Ordering::SeqCst) + 1;
                    ConnectionsCurrent::set(&*self.metrics_recorder, current as f64);

                    // Fire USDT probe: connection-accept
                    frogdb_core::probes::fire_connection_accept(conn_id, &addr.to_string());

                    debug!(
                        conn_id,
                        shard_id,
                        addr = %addr,
                        "Accepted connection"
                    );

                    // Build grouped dependencies for the connection handler
                    use crate::connection::deps::ConnectionConfig;
                    use std::time::Duration;

                    let core = CoreDeps {
                        registry: self.registry.clone(),
                        shard_senders: self.shard_senders.clone(),
                        acl_manager: self.acl_manager.clone(),
                    };
                    let admin = AdminDeps {
                        client_registry: self.client_registry.clone(),
                        config_manager: self.config_manager.clone(),
                        snapshot_coordinator: self.snapshot_coordinator.clone(),
                        function_registry: self.function_registry.clone(),
                        cursor_store: self.cursor_store.clone(),
                    };
                    let cluster = ClusterDeps {
                        cluster_state: self.cluster_state.clone(),
                        node_id: self.node_id,
                        raft: self.raft.clone(),
                        network_factory: self.network_factory.clone(),
                        slot_migration: self.slot_migration.clone(),
                        replication_tracker: self.replication_tracker.clone(),
                        primary_replication_handler: self.primary_replication_handler.clone(),
                        replication_state: self.replication_state.clone(),
                        quorum_checker: self.quorum_checker.clone(),
                        pubsub_forwarder: self.pubsub_forwarder.clone(),
                    };
                    let config = ConnectionConfig {
                        num_shards: self.shard_senders.len(),
                        allow_cross_slot: self.allow_cross_slot,
                        scatter_gather_timeout: Duration::from_millis(
                            self.scatter_gather_timeout_ms,
                        ),
                        is_admin: self.is_admin,
                        admin_enabled: self.admin_enabled,
                        hotshards_config: self.hotshards_config.clone(),
                        memory_diag_config: self.memory_diag_config.clone(),
                        per_request_spans: self.per_request_spans.clone(),
                        is_replica: self.is_replica.clone(),
                        enable_debug_command: self.config_manager.enable_debug_command(),
                        #[cfg(feature = "turmoil")]
                        chaos_config: self.chaos_config.clone(),
                    };
                    let observability = ObservabilityDeps {
                        metrics_recorder: self.metrics_recorder.clone(),
                        shared_tracer: self.shared_tracer.clone(),
                        tracing_config: self.tracing_config.clone(),
                        monitor_broadcaster: self.monitor_broadcaster.clone(),
                        latency_histograms: self.latency_histograms.clone(),
                        hotkey_session: self.hotkey_session.clone(),
                        keyspace_stats: self.keyspace_stats.clone(),
                    };

                    let metrics_recorder = self.metrics_recorder.clone();
                    let current_connections = self.current_connections.clone();

                    let conn_future = async move {
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
                        ConnectionsCurrent::set(&*metrics_recorder, current as f64);
                    };

                    if let Some(ref monitor) = self.conn_monitor {
                        spawn(monitor.instrument(conn_future));
                    } else {
                        spawn(conn_future);
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to accept connection");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_core::NoopSnapshotCoordinator;
    use std::sync::atomic::{AtomicBool, AtomicU64};

    /// Build an `AcceptorContext` with minimal test doubles (empty command
    /// registry, no-op snapshot coordinator, default/standalone cluster and
    /// observability deps) — enough to exercise `Acceptor::bind` without a
    /// running server.
    fn test_context() -> AcceptorContext {
        let registry = Arc::new(CommandRegistry::new());
        let (shard_tx, _shard_rx) = mpsc::channel(1);
        let shard_senders = Arc::new(vec![ShardSender::new(shard_tx)]);
        let acl_manager = AclManager::new(Default::default());
        let client_registry = Arc::new(ClientRegistry::new());
        let config_manager = Arc::new(ConfigManager::new(&crate::config::Config::default()));
        let snapshot_coordinator: Arc<dyn SnapshotCoordinator> =
            Arc::new(NoopSnapshotCoordinator::new());
        let function_registry = SharedFunctionRegistry::default();
        let (conn_tx, _conn_rx) = mpsc::channel(1);

        AcceptorContext {
            core: CoreDeps {
                registry,
                shard_senders,
                acl_manager,
            },
            admin: AdminDeps {
                client_registry,
                config_manager,
                snapshot_coordinator,
                function_registry,
                cursor_store: Arc::new(crate::cursor_store::AggregateCursorStore::new()),
            },
            cluster: ClusterDeps::default(),
            observability: ObservabilityDeps::default(),
            new_conn_senders: vec![conn_tx],
            allow_cross_slot: false,
            scatter_gather_timeout_ms: 5000,
            admin_enabled: true,
            hotshards_config: frogdb_debug::HotShardConfig::default(),
            memory_diag_config: frogdb_debug::MemoryDiagConfig::default(),
            max_clients: Arc::new(AtomicU64::new(0)),
            is_replica: Arc::new(AtomicBool::new(false)),
            conn_monitor: None,
            #[cfg(feature = "turmoil")]
            chaos_config: Arc::new(crate::config::ChaosConfig::default()),
            #[cfg(not(feature = "turmoil"))]
            tls_handshake_timeout: std::time::Duration::from_millis(3000),
        }
    }

    /// The same `AcceptorContext` is shared by every port; only the
    /// `PortSpec` should differ per listener. Verify `is_admin` threads
    /// through correctly and shared fields stay identical across ports.
    #[tokio::test]
    async fn bind_threads_is_admin_per_port() {
        let ctx = test_context();

        let main_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let main = Acceptor::bind(
            ctx.clone(),
            PortSpec {
                listener: main_listener,
                is_admin: false,
                #[cfg(not(feature = "turmoil"))]
                tls_manager: None,
            },
        );
        assert!(!main.is_admin);
        #[cfg(not(feature = "turmoil"))]
        assert!(main.tls_manager.is_none());

        let admin_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let admin = Acceptor::bind(
            ctx.clone(),
            PortSpec {
                listener: admin_listener,
                is_admin: true,
                #[cfg(not(feature = "turmoil"))]
                tls_manager: None,
            },
        );
        assert!(admin.is_admin);

        // Fields sourced from the shared context must be identical across
        // ports; only `is_admin` (and TLS, tested separately) may differ.
        assert_eq!(
            admin.scatter_gather_timeout_ms,
            main.scatter_gather_timeout_ms
        );
        assert_eq!(admin.admin_enabled, main.admin_enabled);
    }

    /// The TLS port gets a `tls_manager`; the plaintext port does not, even
    /// though both are built from the same context.
    #[cfg(not(feature = "turmoil"))]
    #[tokio::test]
    async fn bind_threads_tls_manager_per_port() {
        use frogdb_test_harness::tls::TlsFixture;

        let fixture = TlsFixture::generate();
        let tls_config = crate::config::TlsConfig {
            enabled: true,
            cert_file: fixture.server_cert.clone(),
            key_file: fixture.server_key.clone(),
            ..Default::default()
        };
        let tls_manager = Arc::new(crate::tls::TlsManager::new(&tls_config).unwrap());

        let ctx = test_context();

        let plain_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let plain = Acceptor::bind(
            ctx.clone(),
            PortSpec {
                listener: plain_listener,
                is_admin: false,
                tls_manager: None,
            },
        );
        assert!(plain.tls_manager.is_none());

        let tls_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tls = Acceptor::bind(
            ctx,
            PortSpec {
                listener: tls_listener,
                is_admin: false,
                tls_manager: Some(tls_manager.clone()),
            },
        );
        assert!(tls.tls_manager.is_some());
        assert!(!tls.is_admin);
    }
}
