//! TCP connection acceptor.

use anyhow::Result;
use frogdb_core::shard::NewConnection;
use frogdb_core::sync::{Arc, AtomicUsize, Ordering};

use frogdb_telemetry::definitions::{ConnectionsCurrent, ConnectionsRejected, ConnectionsTotal};
use frogdb_telemetry::labels::RejectionReason;
// TLS handshake metrics are only referenced by the non-turmoil TLS accept path
// below (`#[cfg(not(feature = "turmoil"))]`); importing them under turmoil is an
// unused-import error under `-D warnings`.
#[cfg(not(feature = "turmoil"))]
use frogdb_telemetry::definitions::TlsHandshakeErrors;
#[cfg(not(feature = "turmoil"))]
use frogdb_telemetry::labels::TlsHandshakeError;
use std::sync::atomic::AtomicI64;

use tokio::sync::mpsc;
use tracing::{debug, error, info};

use crate::connection::ConnectionHandler;
use crate::connection::deps::{
    AdminDeps, ClusterDeps, ConnectionConfig, ConnectionDeps, CoreDeps, ObservabilityDeps,
};
use crate::net::{ShardPlacement, TcpListener, spawn};
use crate::server::next_conn_id;

/// A freshly accepted socket on its way to its connection task.
///
/// A tokio `TcpStream` is bound to the IO driver of the runtime it was created
/// on: polling it from a different runtime silently never wakes. Thread-per-core
/// placement moves the connection task to the shard's own current-thread
/// runtime, so the socket must be detached here and re-registered *there*.
/// Connections that stay on the acceptor's runtime skip the round trip.
enum PendingSocket {
    /// Still registered with the acceptor runtime's IO driver.
    Registered(crate::net::TcpStream),

    /// Detached, awaiting registration on the shard runtime's IO driver.
    #[cfg(not(feature = "turmoil"))]
    Unregistered(std::net::TcpStream),
}

impl PendingSocket {
    /// Register the socket with the *current* runtime's IO driver.
    ///
    /// Must be called from inside the task that will drive the connection.
    fn register(self) -> std::io::Result<crate::net::TcpStream> {
        match self {
            Self::Registered(socket) => Ok(socket),
            #[cfg(not(feature = "turmoil"))]
            Self::Unregistered(socket) => crate::net::TcpStream::from_std(socket),
        }
    }
}

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

    /// Connection assigner.
    assigner: RoundRobinAssigner,

    /// Per-shard runtime handles for thread-per-core connection placement.
    ///
    /// Empty for executors that do not own dedicated runtimes (turmoil, and any
    /// future in-process executor), in which case connections stay on the
    /// acceptor's runtime exactly as before.
    shard_placement: ShardPlacement,

    /// Current connection count for this port (shared across every connection
    /// on the port for the maxclients gate and decremented on drop).
    ///
    /// Minted fresh per [`Acceptor::bind`] and deliberately kept *out* of the
    /// cloned [`ConnectionDeps`] bundle: if it were cloned per connection, each
    /// connection would count against its own zeroed counter and the gate would
    /// never fire.
    current_connections: Arc<AtomicI64>,

    /// Maximum simultaneous client connections (0 = unlimited). Admin exempt.
    /// Never enters the handler — it is the acceptor-local maxclients gate.
    max_clients: Arc<std::sync::atomic::AtomicU64>,

    /// Optional task monitor for connection handler tasks.
    conn_monitor: Option<tokio_metrics::TaskMonitor>,

    /// Optional TLS manager for accepting TLS connections (per-port).
    #[cfg(not(feature = "turmoil"))]
    tls_manager: Option<Arc<crate::tls::TlsManager>>,

    /// Live TLS handshake timeout, read per handshake (see
    /// [`crate::tls_runtime::HandshakeTimeout`]).
    #[cfg(not(feature = "turmoil"))]
    tls_handshake_timeout: crate::tls_runtime::HandshakeTimeout,

    /// The five dependency groups, pre-assembled once at [`Acceptor::bind`] and
    /// cloned (Arc-cheap) per accepted connection to hand to the handler.
    deps: ConnectionDeps,
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

    /// Hard limit (bytes) on pub/sub messages buffered for a slow subscriber
    /// before it is disconnected. `0` disables the bound.
    pub pubsub_output_buffer_hard_limit: usize,

    /// Whether admin port separation is enabled (admin commands blocked on
    /// the regular port).
    pub admin_enabled: bool,

    /// Memory diagnostics configuration.
    pub memory_diag_config: frogdb_debug::MemoryDiagConfig,

    /// Maximum simultaneous client connections (0 = unlimited). Admin exempt.
    pub max_clients: Arc<std::sync::atomic::AtomicU64>,

    /// Whether this server is a replica (rejects write commands from clients).
    pub is_replica: Arc<std::sync::atomic::AtomicBool>,

    /// Optional task monitor for connection handler tasks.
    pub conn_monitor: Option<tokio_metrics::TaskMonitor>,

    /// Per-shard runtime handles for thread-per-core connection placement.
    ///
    /// See [`ShardPlacement`]. Default (`unpinned`) leaves every connection on
    /// the acceptor's runtime.
    pub shard_placement: ShardPlacement,

    /// Chaos testing configuration (turmoil simulation only).
    #[cfg(feature = "turmoil")]
    pub chaos_config: Arc<crate::config::ChaosConfig>,

    /// Live TLS handshake timeout, shared across whichever ports enable TLS
    /// and read per handshake rather than snapshotted at bind time.
    #[cfg(not(feature = "turmoil"))]
    pub tls_handshake_timeout: crate::tls_runtime::HandshakeTimeout,
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
        let num_shards = ctx.core.shard_senders.len();

        // core/admin/cluster/observability move in wholesale — they are already
        // grouped exactly as `from_deps` wants them. Only `ConnectionConfig` is
        // *assembled* here, because it is not present in `AcceptorContext`: two
        // of its members are knowable only at bind time (`spec.is_admin`, which
        // is per-port) and two derive from the `ConfigManager`.
        let config = ConnectionConfig {
            num_shards,
            allow_cross_slot: ctx.allow_cross_slot,
            scatter_gather_timeout: std::time::Duration::from_millis(ctx.scatter_gather_timeout_ms),
            is_admin: spec.is_admin,
            admin_enabled: ctx.admin_enabled,
            memory_diag_config: ctx.memory_diag_config,
            per_request_spans: ctx.admin.config_manager.per_request_spans_flag(),
            is_replica: ctx.is_replica,
            enable_debug_command: ctx.admin.config_manager.enable_debug_command(),
            pubsub_output_buffer_hard_limit: ctx.pubsub_output_buffer_hard_limit,
            #[cfg(feature = "turmoil")]
            chaos_config: ctx.chaos_config,
        };

        Self {
            listener: spec.listener,
            assigner: RoundRobinAssigner::new(num_shards),
            shard_placement: ctx.shard_placement,
            current_connections: Arc::new(AtomicI64::new(0)),
            max_clients: ctx.max_clients,
            conn_monitor: ctx.conn_monitor,
            #[cfg(not(feature = "turmoil"))]
            tls_manager: spec.tls_manager,
            #[cfg(not(feature = "turmoil"))]
            tls_handshake_timeout: ctx.tls_handshake_timeout,
            deps: ConnectionDeps {
                core: ctx.core,
                admin: ctx.admin,
                cluster: ctx.cluster,
                config,
                observability: ctx.observability,
            },
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
                    if !self.deps.config.is_admin {
                        let limit = self.max_clients.load(std::sync::atomic::Ordering::Relaxed);
                        if limit > 0 {
                            let current = self.current_connections.load(Ordering::SeqCst);
                            if current >= limit as i64 {
                                ConnectionsRejected::inc(
                                    &*self.deps.observability.metrics_recorder,
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

                    // The TLS manager and the live handshake timeout travel with
                    // the connection; both are still read *per connection*, just
                    // inside the spawned task rather than in this loop.
                    #[cfg(not(feature = "turmoil"))]
                    let tls = self
                        .tls_manager
                        .clone()
                        .map(|mgr| (mgr, self.tls_handshake_timeout.clone()));

                    // Clone the pre-assembled dep bundle for this connection.
                    // Every member is an `Arc`/`Copy`/small owned value, so this
                    // is the same Arc-bumping the old regroup did — minus the
                    // field-by-field restatement.
                    let ConnectionDeps {
                        core,
                        admin,
                        cluster,
                        config,
                        observability,
                    } = self.deps.clone();

                    let metrics_recorder = observability.metrics_recorder.clone();
                    let current_connections = self.current_connections.clone();

                    // Placement rule: the round-robin shard assigned above also
                    // decides which core runs this connection, and the
                    // connection *never moves* for its lifetime. Commands whose
                    // key hashes to the owning shard are then served without
                    // leaving the thread; the rest pay the cross-core hop the
                    // routing channel already implies.
                    #[cfg(not(feature = "turmoil"))]
                    let (socket, shard_runtime) =
                        match self.shard_placement.runtime_for(shard_id).cloned() {
                            Some(runtime) => match socket.into_std() {
                                Ok(detached) => {
                                    (PendingSocket::Unregistered(detached), Some(runtime))
                                }
                                Err(e) => {
                                    error!(
                                        conn_id,
                                        shard_id,
                                        error = %e,
                                        "Failed to detach socket for shard placement"
                                    );
                                    continue;
                                }
                            },
                            None => (PendingSocket::Registered(socket), None),
                        };
                    #[cfg(feature = "turmoil")]
                    let socket = PendingSocket::Registered(socket);

                    let conn_future = async move {
                        // Re-register the socket on whichever runtime ended up
                        // driving this task (a no-op when it never left the
                        // acceptor's).
                        let socket = match socket.register() {
                            Ok(socket) => socket,
                            Err(e) => {
                                error!(
                                    conn_id,
                                    shard_id,
                                    error = %e,
                                    "Failed to register socket on shard runtime"
                                );
                                return;
                            }
                        };

                        // The TLS handshake is awaited *here*, not in the accept
                        // loop. Awaiting it there stalls the listener for as long
                        // as one peer takes to complete (up to the full handshake
                        // timeout), so a client that connects and sends nothing —
                        // repeated a few times a second — blocks every other
                        // connection on the port. Handshaking per task also means
                        // a rejected handshake never touches the registry or the
                        // connection gauge, exactly as before.
                        #[cfg(not(feature = "turmoil"))]
                        let socket: crate::net::ConnectionStream = match tls {
                            Some((tls_mgr, handshake_timeout)) => {
                                let acceptor = tls_mgr.acceptor();
                                match tokio::time::timeout(
                                    handshake_timeout.get(),
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
                                            &*metrics_recorder,
                                            TlsHandshakeError::HandshakeError,
                                        );
                                        return;
                                    }
                                    Err(_) => {
                                        debug!(addr = %addr, "TLS handshake timed out");
                                        TlsHandshakeErrors::inc(
                                            &*metrics_recorder,
                                            TlsHandshakeError::Timeout,
                                        );
                                        return;
                                    }
                                }
                            }
                            None => crate::tls::MaybeTlsStream::Plain { inner: socket },
                        };

                        // Register connection with client registry
                        let client_handle =
                            admin.client_registry.register(conn_id, addr, local_addr);

                        // Record connection metrics
                        ConnectionsTotal::inc(&*metrics_recorder);
                        let current = current_connections.fetch_add(1, Ordering::SeqCst) + 1;
                        ConnectionsCurrent::set(&*metrics_recorder, current as f64);

                        // Fire USDT probe: connection-accept
                        frogdb_core::probes::fire_connection_accept(conn_id, &addr.to_string());

                        debug!(
                            conn_id,
                            shard_id,
                            addr = %addr,
                            "Accepted connection"
                        );

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

                    #[cfg(not(feature = "turmoil"))]
                    match (shard_runtime, self.conn_monitor.as_ref()) {
                        (Some(runtime), Some(monitor)) => {
                            runtime.spawn(monitor.instrument(conn_future));
                        }
                        (Some(runtime), None) => {
                            runtime.spawn(conn_future);
                        }
                        (None, Some(monitor)) => {
                            spawn(monitor.instrument(conn_future));
                        }
                        (None, None) => {
                            spawn(conn_future);
                        }
                    }
                    #[cfg(feature = "turmoil")]
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

// Every test here hardcodes `tokio::net::TcpListener`, which is incompatible
// with the turmoil-typed `PortSpec.listener` (and a turmoil listener cannot
// bind outside a sim World). None is a simulation test, so the whole module is
// excluded under the `turmoil` feature — otherwise `test_context` and its
// imports are dead code under `-D warnings`.
#[cfg(all(test, not(feature = "turmoil")))]
mod tests {
    use super::*;
    use crate::runtime_config::ConfigManager;
    use frogdb_core::{
        AclManager, ClientRegistry, CommandRegistry, NoopSnapshotCoordinator, ShardSender,
        SharedFunctionRegistry, persistence::SnapshotCoordinator,
    };
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
                recovery_stats: Default::default(),
            },
            cluster: ClusterDeps::default(),
            observability: ObservabilityDeps::default(),
            new_conn_senders: vec![conn_tx],
            allow_cross_slot: false,
            scatter_gather_timeout_ms: 5000,
            admin_enabled: true,
            memory_diag_config: frogdb_debug::MemoryDiagConfig::default(),
            max_clients: Arc::new(AtomicU64::new(0)),
            is_replica: Arc::new(AtomicBool::new(false)),
            pubsub_output_buffer_hard_limit: frogdb_core::DEFAULT_PUBSUB_OUTPUT_BUFFER_HARD_LIMIT,
            conn_monitor: None,
            shard_placement: ShardPlacement::unpinned(),
            #[cfg(feature = "turmoil")]
            chaos_config: Arc::new(crate::config::ChaosConfig::default()),
            #[cfg(not(feature = "turmoil"))]
            tls_handshake_timeout: crate::tls_runtime::HandshakeTimeout::new(3000),
        }
    }

    /// The same `AcceptorContext` is shared by every port; only the
    /// `PortSpec` should differ per listener. Verify `is_admin` threads
    /// through correctly and shared fields stay identical across ports.
    // Hardcodes `tokio::net::TcpListener`, which is incompatible with the
    // turmoil-typed `PortSpec.listener` under the `turmoil` feature (and a
    // turmoil listener cannot bind outside a sim World). Not a simulation test.
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
        assert!(!main.deps.config.is_admin);
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
        assert!(admin.deps.config.is_admin);

        // Fields sourced from the shared context must be identical across
        // ports; only `is_admin` (and TLS, tested separately) may differ.
        assert_eq!(
            admin.deps.config.scatter_gather_timeout,
            main.deps.config.scatter_gather_timeout
        );
        assert_eq!(
            admin.deps.config.admin_enabled,
            main.deps.config.admin_enabled
        );
    }

    /// Pre-assembling the dep bundle at `bind` makes "the deps the handler
    /// receives" inspectable without accepting a socket. Two ports built from
    /// the same context must *share* their Arc dep members (pointer-equal), not
    /// copy them — while `config.is_admin` still tracks the per-port `PortSpec`.
    // See `bind_threads_is_admin_per_port`: tokio-listener bind is incompatible
    // with the turmoil-typed `PortSpec.listener`. Not a simulation test.
    #[tokio::test]
    async fn bind_shares_deps_across_ports() {
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

        let admin_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let admin = Acceptor::bind(
            ctx,
            PortSpec {
                listener: admin_listener,
                is_admin: true,
                #[cfg(not(feature = "turmoil"))]
                tls_manager: None,
            },
        );

        // Per-port config differs.
        assert!(!main.deps.config.is_admin);
        assert!(admin.deps.config.is_admin);

        // Shared Arc dep members are the same allocation, not per-port copies.
        assert!(Arc::ptr_eq(
            &main.deps.core.registry,
            &admin.deps.core.registry
        ));
        assert!(Arc::ptr_eq(
            &main.deps.core.shard_senders,
            &admin.deps.core.shard_senders
        ));
        assert!(Arc::ptr_eq(
            &main.deps.admin.client_registry,
            &admin.deps.admin.client_registry
        ));
        assert!(Arc::ptr_eq(
            &main.deps.admin.config_manager,
            &admin.deps.admin.config_manager
        ));

        // `current_connections` is minted fresh per bind — NOT shared — so the
        // maxclients gate counts per port.
        assert!(!Arc::ptr_eq(
            &main.current_connections,
            &admin.current_connections
        ));
    }

    /// The TLS port gets a `tls_manager`; the plaintext port does not, even
    /// though both are built from the same context.
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
        assert!(!tls.deps.config.is_admin);
    }

    /// Thread-per-core placement must *actually* move the connection task onto
    /// the shard's runtime — which means detaching the socket from the
    /// acceptor's IO driver and re-registering it on the shard's. Skipping that
    /// re-registration is silent: the socket simply never becomes readable and
    /// the connection hangs with no error anywhere. So this asserts the reply
    /// comes back, which is only possible if the move succeeded.
    #[tokio::test]
    async fn a_pinned_connection_is_served_from_the_shard_runtime() {
        use crate::net::{RealShardExecutor, ShardExecutor};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // One shard on its own thread with its own current-thread runtime. The
        // body parks forever: all this test needs from the shard is a live
        // runtime to place the connection on.
        let mut executor = RealShardExecutor::new();
        let _shard = executor
            .launch(0, Box::pin(std::future::pending::<()>()))
            .expect("shard launch");
        let placement = ShardPlacement::collect(&executor, 1);
        assert!(
            placement.is_pinned(),
            "the real executor must expose a runtime per shard"
        );

        let mut ctx = test_context();
        ctx.shard_placement = placement;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let acceptor = Acceptor::bind(
            ctx,
            PortSpec {
                listener,
                is_admin: false,
                tls_manager: None,
            },
        );
        tokio::spawn(async move {
            let _ = acceptor.run().await;
        });

        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();

        let mut buf = [0u8; 64];
        let n = tokio::time::timeout(std::time::Duration::from_secs(10), client.read(&mut buf))
            .await
            .expect("pinned connection never answered: socket was not re-registered")
            .expect("pinned connection read failed");
        assert!(n > 0, "pinned connection closed without answering");
    }
}
