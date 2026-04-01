//! TestServer helper for integration tests.
//!
//! This module provides a single TestServer abstraction for spawning FrogDB
//! servers in tests with dynamic port allocation and proper cleanup.

#![allow(dead_code)]

use bytes::Bytes;
use frogdb_core::ClusterState;
use frogdb_core::cluster::ClusterRaft;
use frogdb_protocol::Response;
use frogdb_server::net::tcp_listener_reusable;
use frogdb_server::{Config, Server, ServerListeners};
use futures::{SinkExt, StreamExt};
use redis_protocol::codec::{Resp2, Resp3};
use redis_protocol::resp2::types::BytesFrame;
use redis_protocol::resp3::types::BytesFrame as Resp3Frame;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio_util::codec::Framed;

/// Server role for test configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServerRole {
    /// Standalone server (no replication)
    Standalone,
    /// Primary server (accepts replica connections)
    Primary,
    /// Replica server (connects to a primary)
    Replica,
}

/// Configuration options for test servers.
///
/// Cannot derive `Clone` because `TcpListener` is not `Clone` — has manual
/// Clone impl that sets `cluster_bus_listener` to `None`.
#[derive(Default)]
pub struct TestServerConfig {
    // --- Server core ---
    /// Number of shards (default: 4).
    pub num_shards: Option<usize>,

    // --- Persistence ---
    /// Enable RocksDB persistence (default: false).
    pub persistence: bool,
    /// Data directory for persistence (auto-generated temp dir if None).
    pub data_dir: Option<PathBuf>,

    // --- Logging ---
    /// Log level (default: "warn" to reduce test noise).
    pub log_level: Option<String>,

    // --- Security ---
    /// Password required for authentication (default: None).
    pub requirepass: Option<String>,

    // --- Admin ---
    /// Enable admin port (default: false).
    pub admin_enabled: bool,

    // --- Replication ---
    /// Replication role: "standalone", "primary", or "replica".
    pub replication_role: Option<String>,
    /// Primary host for replica mode.
    pub replication_primary_host: Option<String>,
    /// Primary port for replica mode.
    pub replication_primary_port: Option<u16>,

    // --- Cluster ---
    /// Enable cluster mode (default: false).
    pub cluster_enabled: bool,
    /// Cluster node ID (0 = auto-generate).
    pub cluster_node_id: Option<u64>,
    /// Initial nodes for cluster formation.
    pub cluster_initial_nodes: Option<Vec<String>>,
    /// Cluster data directory.
    pub cluster_data_dir: Option<PathBuf>,
    /// Election timeout in ms.
    pub cluster_election_timeout_ms: Option<u64>,
    /// Heartbeat interval in ms.
    pub cluster_heartbeat_interval_ms: Option<u64>,
    /// Connection timeout in ms.
    pub cluster_connect_timeout_ms: Option<u64>,
    /// Request timeout in ms.
    pub cluster_request_timeout_ms: Option<u64>,
    /// Pre-bound cluster bus listener. When provided, cluster_bus_addr is
    /// derived from `listener.local_addr()`. When None and cluster_enabled=true,
    /// start_with_config auto-binds one on port 0.
    pub cluster_bus_listener: Option<frogdb_server::net::TcpListener>,

    // --- Sorted set index ---
    /// Sorted set index backend (default: server default).
    pub sorted_set_index: Option<frogdb_server::config::server::SortedSetIndexConfig>,

    // --- Connection limits ---
    /// Maximum simultaneous client connections (None = use server default).
    pub max_clients: Option<u32>,

    // --- TLS ---
    /// Path to TLS certificate file.
    pub tls_cert_file: Option<PathBuf>,
    /// Path to TLS private key file.
    pub tls_key_file: Option<PathBuf>,
    /// Path to TLS CA certificate file (for mTLS).
    pub tls_ca_file: Option<PathBuf>,
    /// Client certificate authentication mode.
    pub tls_client_auth: Option<frogdb_server::config::ClientCertMode>,
    /// TLS handshake timeout in ms.
    pub tls_handshake_timeout_ms: Option<u64>,
    /// Encrypt replication connections.
    pub tls_replication: bool,
    /// Encrypt cluster bus connections.
    pub tls_cluster: bool,
    /// Enable dual-accept migration mode for cluster bus.
    pub tls_cluster_migration: bool,
    /// Keep HTTP server plaintext even when TLS enabled (default: true).
    pub tls_no_tls_on_http: Option<bool>,
    /// Path to client certificate for outgoing TLS (replication/cluster).
    pub tls_client_cert_file: Option<PathBuf>,
    /// Path to client private key for outgoing TLS (replication/cluster).
    pub tls_client_key_file: Option<PathBuf>,
}

impl Clone for TestServerConfig {
    fn clone(&self) -> Self {
        Self {
            num_shards: self.num_shards,
            persistence: self.persistence,
            data_dir: self.data_dir.clone(),
            log_level: self.log_level.clone(),
            requirepass: self.requirepass.clone(),
            admin_enabled: self.admin_enabled,
            replication_role: self.replication_role.clone(),
            replication_primary_host: self.replication_primary_host.clone(),
            replication_primary_port: self.replication_primary_port,
            cluster_enabled: self.cluster_enabled,
            cluster_node_id: self.cluster_node_id,
            cluster_initial_nodes: self.cluster_initial_nodes.clone(),
            cluster_data_dir: self.cluster_data_dir.clone(),
            cluster_election_timeout_ms: self.cluster_election_timeout_ms,
            cluster_heartbeat_interval_ms: self.cluster_heartbeat_interval_ms,
            cluster_connect_timeout_ms: self.cluster_connect_timeout_ms,
            cluster_request_timeout_ms: self.cluster_request_timeout_ms,
            // TcpListener is not Clone; cloned configs always self-bind.
            cluster_bus_listener: None,
            sorted_set_index: self.sorted_set_index,
            max_clients: self.max_clients,
            tls_cert_file: self.tls_cert_file.clone(),
            tls_key_file: self.tls_key_file.clone(),
            tls_ca_file: self.tls_ca_file.clone(),
            tls_client_auth: self.tls_client_auth.clone(),
            tls_handshake_timeout_ms: self.tls_handshake_timeout_ms,
            tls_replication: self.tls_replication,
            tls_cluster: self.tls_cluster,
            tls_cluster_migration: self.tls_cluster_migration,
            tls_no_tls_on_http: self.tls_no_tls_on_http,
            tls_client_cert_file: self.tls_client_cert_file.clone(),
            tls_client_key_file: self.tls_client_key_file.clone(),
        }
    }
}

// Default is derived via #[derive(Default)] on the struct.

/// A test server instance that can be used in integration tests.
pub struct TestServer {
    /// RESP protocol port (for client commands)
    port: u16,
    /// Metrics server port
    metrics_port: u16,
    /// Admin RESP port (None if admin disabled)
    admin_port: Option<u16>,
    /// Admin HTTP port (None if admin disabled)
    admin_http_port: Option<u16>,
    /// Cluster bus port (None if cluster disabled)
    cluster_bus_port: Option<u16>,
    /// Server role for reference
    role: ServerRole,
    /// Shutdown signal sender
    shutdown_tx: Option<oneshot::Sender<()>>,
    /// Server task handle
    handle: tokio::task::JoinHandle<()>,
    /// Data directory path (managed by the test)
    data_dir: Option<PathBuf>,
    /// Raft instance (cluster mode only)
    raft: Option<Arc<ClusterRaft>>,
    /// Cluster state (cluster mode only)
    cluster_state: Option<Arc<ClusterState>>,
    /// Client registry for querying blocked-client counts, etc.
    client_registry: Arc<frogdb_core::ClientRegistry>,
    /// TLS port (None if TLS disabled)
    tls_port: Option<u16>,
}

impl TestServer {
    /// Create a unique temp directory for test data.
    pub fn create_temp_dir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        // Nest the actual data dir one level deep so that each test server
        // has its own isolated parent directory.  The replication checkpoint
        // staging area (`checkpoint_ready`) is placed as a sibling of the
        // data dir (in its parent), so without isolation parallel tests race
        // on a shared `checkpoint_ready`.
        let dir = std::env::temp_dir().join(format!(
            "frogdb_test_{}_{}_{}/data",
            timestamp,
            std::process::id(),
            id
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    // -----------------------------------------------------------------------
    // Convenience constructors (thin wrappers around start_with_config)
    // -----------------------------------------------------------------------

    /// Start a standalone server on an OS-assigned port.
    pub async fn start_standalone() -> Self {
        Self::start_standalone_with_config(TestServerConfig::default()).await
    }

    /// Start a standalone server with security (password required).
    pub async fn start_with_security(requirepass: &str) -> Self {
        Self::start_standalone_with_config(TestServerConfig {
            requirepass: Some(requirepass.to_string()),
            ..Default::default()
        })
        .await
    }

    /// Start standalone server with admin port enabled.
    pub async fn start_with_admin_port() -> Self {
        Self::start_standalone_with_config(TestServerConfig {
            admin_enabled: true,
            ..Default::default()
        })
        .await
    }

    /// Start a standalone server with custom configuration.
    pub async fn start_standalone_with_config(test_config: TestServerConfig) -> Self {
        Self::start_with_config(test_config, ServerRole::Standalone).await
    }

    /// Start a primary server on an OS-assigned port.
    pub async fn start_primary() -> Self {
        Self::start_primary_with_config(TestServerConfig::default()).await
    }

    /// Start a primary server with custom configuration.
    pub async fn start_primary_with_config(test_config: TestServerConfig) -> Self {
        Self::start_with_config(test_config, ServerRole::Primary).await
    }

    /// Start a replica server connecting to the given primary.
    pub async fn start_replica(primary: &TestServer) -> Self {
        Self::start_replica_with_config(primary, TestServerConfig::default()).await
    }

    /// Start a replica server with custom configuration.
    pub async fn start_replica_with_config(
        primary: &TestServer,
        mut test_config: TestServerConfig,
    ) -> Self {
        test_config.replication_primary_host = Some("127.0.0.1".to_string());
        test_config.replication_primary_port = Some(primary.port());
        Self::start_with_config(test_config, ServerRole::Replica).await
    }

    // -----------------------------------------------------------------------
    // Unified start
    // -----------------------------------------------------------------------

    /// Start a server with the given configuration and role.
    pub async fn start_with_config(test_config: TestServerConfig, role: ServerRole) -> Self {
        let (owned_dir, data_dir) = match test_config.data_dir {
            Some(dir) => (None, dir),
            None => {
                let dir = Self::create_temp_dir();
                (Some(dir.clone()), dir)
            }
        };

        let mut config = Config::default();
        config.server.bind = "127.0.0.1".to_string();
        config.server.port = 0; // OS assigns
        config.server.num_shards = test_config.num_shards.unwrap_or(4);
        config.logging.level = test_config.log_level.unwrap_or_else(|| "warn".to_string());
        config.persistence.enabled = test_config.persistence;
        config.persistence.data_dir = data_dir;
        config.http.bind = "127.0.0.1".to_string();
        config.http.port = 0; // OS assigns

        // Security
        if let Some(ref pass) = test_config.requirepass {
            config.security.requirepass = pass.clone();
        }

        // Admin
        if test_config.admin_enabled {
            config.admin.enabled = true;
            config.admin.bind = "127.0.0.1".to_string();
            config.admin.port = 0; // OS assigns
        }

        // Replication role
        match role {
            ServerRole::Standalone => {
                config.replication.role = test_config
                    .replication_role
                    .unwrap_or_else(|| "standalone".to_string());
            }
            ServerRole::Primary => {
                config.replication.role = "primary".to_string();
            }
            ServerRole::Replica => {
                config.replication.role = "replica".to_string();
                if let Some(host) = test_config.replication_primary_host {
                    config.replication.primary_host = host;
                }
                if let Some(port) = test_config.replication_primary_port {
                    config.replication.primary_port = port;
                }
            }
        }

        // Pre-bind all listeners before creating Server to eliminate TOCTOU
        // races and avoid SO_REUSEPORT cross-talk between concurrent tests.
        let mut listeners = ServerListeners::default();

        let resp_listener = tcp_listener_reusable("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        listeners.resp = Some(resp_listener);

        if config.http.enabled {
            let http_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            listeners.http = Some(http_listener);
        }

        if test_config.admin_enabled {
            let admin_resp_listener = tcp_listener_reusable("127.0.0.1:0".parse().unwrap())
                .await
                .unwrap();
            listeners.admin_resp = Some(admin_resp_listener);
        }

        // Cluster configuration
        if test_config.cluster_enabled {
            config.cluster.enabled = true;

            if let Some(node_id) = test_config.cluster_node_id {
                config.cluster.node_id = node_id;
            }
            if let Some(initial_nodes) = test_config.cluster_initial_nodes {
                config.cluster.initial_nodes = initial_nodes;
            }
            if let Some(cluster_data_dir) = test_config.cluster_data_dir {
                config.cluster.data_dir = cluster_data_dir;
            } else {
                let cluster_dir = config.persistence.data_dir.join("cluster");
                std::fs::create_dir_all(&cluster_dir).unwrap();
                config.cluster.data_dir = cluster_dir;
            }
            if let Some(ms) = test_config.cluster_election_timeout_ms {
                config.cluster.election_timeout_ms = ms;
            }
            if let Some(ms) = test_config.cluster_heartbeat_interval_ms {
                config.cluster.heartbeat_interval_ms = ms;
            }
            if let Some(ms) = test_config.cluster_connect_timeout_ms {
                config.cluster.connect_timeout_ms = ms;
            }
            if let Some(ms) = test_config.cluster_request_timeout_ms {
                config.cluster.request_timeout_ms = ms;
            }

            // Handle cluster bus listener: use provided or auto-bind
            let bus_listener = if let Some(l) = test_config.cluster_bus_listener {
                l
            } else {
                tcp_listener_reusable("127.0.0.1:0".parse().unwrap())
                    .await
                    .unwrap()
            };
            let bus_addr = bus_listener.local_addr().unwrap();
            config.cluster.cluster_bus_addr = bus_addr.to_string();

            listeners.cluster_bus = Some(bus_listener);
        }

        // Sorted set index backend
        if let Some(idx) = test_config.sorted_set_index {
            config.server.sorted_set_index = idx;
        }

        // Max clients
        if let Some(max) = test_config.max_clients {
            config.server.max_clients = max;
        }

        // TLS configuration
        if let Some(ref cert_file) = test_config.tls_cert_file {
            config.tls.enabled = true;
            config.tls.cert_file = cert_file.clone();
            config.tls.key_file = test_config
                .tls_key_file
                .clone()
                .expect("tls_key_file required when tls_cert_file is set");
            config.tls.tls_port = 0; // OS assigns
            if let Some(ref ca_file) = test_config.tls_ca_file {
                config.tls.ca_file = Some(ca_file.clone());
            }
            if let Some(ref mode) = test_config.tls_client_auth {
                config.tls.require_client_cert = mode.clone();
            }
            if let Some(ms) = test_config.tls_handshake_timeout_ms {
                config.tls.handshake_timeout_ms = ms;
            }
            if test_config.tls_replication {
                config.tls.tls_replication = true;
            }
            if test_config.tls_cluster {
                config.tls.tls_cluster = true;
            }
            if test_config.tls_cluster_migration {
                config.tls.tls_cluster_migration = true;
            }
            if let Some(no_tls_http) = test_config.tls_no_tls_on_http {
                config.tls.no_tls_on_http = no_tls_http;
            }
            if let Some(ref cert) = test_config.tls_client_cert_file {
                config.tls.client_cert_file = Some(cert.clone());
            }
            if let Some(ref key) = test_config.tls_client_key_file {
                config.tls.client_key_file = Some(key.clone());
            }

            // Pre-bind TLS listener
            let tls_listener = tcp_listener_reusable("127.0.0.1:0".parse().unwrap())
                .await
                .unwrap();
            listeners.tls = Some(tls_listener);
        }

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        // Construct server *before* spawning so we can read the bound addresses.
        let server = Server::with_listeners(config, listeners, None)
            .await
            .unwrap();
        let port = server.local_addr().unwrap().port();
        let metrics_port = server
            .http_addr()
            .and_then(|r| r.ok())
            .map(|a| a.port())
            .unwrap_or(0);
        let admin_port = server
            .admin_resp_addr()
            .and_then(|r| r.ok())
            .map(|a| a.port());
        let admin_http_port = server.http_addr().and_then(|r| r.ok()).map(|a| a.port());
        let cluster_bus_port = server
            .cluster_bus_addr()
            .and_then(|r| r.ok())
            .map(|a| a.port());
        let tls_port = server.tls_addr().and_then(|r| r.ok()).map(|a| a.port());
        let raft = server.raft().cloned();
        let cluster_state = server.cluster_state().cloned();
        let client_registry = server.client_registry().clone();

        let handle = tokio::spawn(async move {
            let _ = server
                .run_until(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        // Wait for server to be ready (acceptor starts in run_until)
        Self::wait_for_ready(port).await;

        Self {
            port,
            metrics_port,
            admin_port,
            admin_http_port,
            cluster_bus_port,
            role,
            shutdown_tx: Some(shutdown_tx),
            handle,
            data_dir: owned_dir,
            raft,
            cluster_state,
            client_registry,
            tls_port,
        }
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    /// Get the server's role.
    pub fn role(&self) -> ServerRole {
        self.role
    }

    /// Get the RESP server port.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Get the metrics server port.
    pub fn metrics_port(&self) -> u16 {
        self.metrics_port
    }

    /// Get the admin RESP port (panics if admin not enabled).
    pub fn admin_port(&self) -> u16 {
        self.admin_port.expect("Admin port not enabled")
    }

    /// Get the admin HTTP port (panics if admin not enabled).
    pub fn admin_http_port(&self) -> u16 {
        self.admin_http_port.expect("Admin HTTP port not enabled")
    }

    /// Check if admin port is enabled.
    pub fn has_admin_port(&self) -> bool {
        self.admin_port.is_some()
    }

    /// Get admin RESP socket address.
    pub fn admin_socket_addr(&self) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], self.admin_port()))
    }

    /// Get admin HTTP socket address.
    pub fn admin_http_socket_addr(&self) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], self.admin_http_port()))
    }

    /// Get the cluster bus port (panics if cluster not enabled).
    pub fn cluster_bus_port(&self) -> u16 {
        self.cluster_bus_port
            .expect("Cluster bus port not available")
    }

    /// Get the cluster bus address as "host:port".
    pub fn cluster_bus_addr(&self) -> String {
        format!("127.0.0.1:{}", self.cluster_bus_port())
    }

    /// Get the RESP address as "host:port".
    pub fn addr(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    /// Get the TLS port (panics if TLS not enabled).
    pub fn tls_port(&self) -> u16 {
        self.tls_port.expect("TLS not enabled on this test server")
    }

    /// Get the RESP address as SocketAddr.
    pub fn socket_addr(&self) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], self.port))
    }

    /// Get the TLS address as SocketAddr (panics if TLS not enabled).
    pub fn tls_socket_addr(&self) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], self.tls_port()))
    }

    /// Get the metrics address as SocketAddr.
    pub fn metrics_addr(&self) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], self.metrics_port))
    }

    /// Get the metrics URL for a given path.
    pub fn metrics_url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{}", self.metrics_port, path)
    }

    /// Get the Raft instance (cluster mode only).
    pub fn raft(&self) -> Option<&Arc<ClusterRaft>> {
        self.raft.as_ref()
    }

    /// Get the cluster state (cluster mode only).
    pub fn cluster_state(&self) -> Option<&Arc<ClusterState>> {
        self.cluster_state.as_ref()
    }

    /// Return the current number of blocked clients.
    pub fn blocked_client_count(&self) -> usize {
        self.client_registry.blocked_client_count()
    }

    /// Wait until the server has exactly `expected` blocked clients.
    /// Polls every 10ms, panics after 5 seconds.
    pub async fn wait_for_blocked_clients(&self, expected: usize) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let count = self.client_registry.blocked_client_count();
            if count == expected {
                return;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("timed out waiting for {expected} blocked clients, currently {count}");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Check if the server task has finished.
    pub fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    /// Get the data directory path.
    pub fn data_dir(&self) -> Option<&PathBuf> {
        self.data_dir.as_ref()
    }

    /// Try to connect to this server, returning an error instead of panicking.
    pub async fn try_connect(&self) -> std::io::Result<TestClient> {
        let stream = TcpStream::connect(self.socket_addr()).await?;
        let framed = Framed::new(stream, Resp2::default());
        Ok(TestClient { framed })
    }

    /// Fetch metrics as raw Prometheus text format.
    pub async fn fetch_metrics(&self) -> String {
        reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .get(self.metrics_url("/metrics"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap()
    }

    // -----------------------------------------------------------------------
    // Connection helpers
    // -----------------------------------------------------------------------

    /// Start a standalone server with TLS enabled using ephemeral test certs.
    pub async fn start_with_tls(fixture: &crate::tls::TlsFixture) -> Self {
        Self::start_with_config(
            TestServerConfig {
                tls_cert_file: Some(fixture.server_cert.clone()),
                tls_key_file: Some(fixture.server_key.clone()),
                ..Default::default()
            },
            ServerRole::Standalone,
        )
        .await
    }

    /// Start a standalone server with mTLS enabled.
    pub async fn start_with_mtls(
        fixture: &crate::tls::TlsFixture,
        mode: frogdb_server::config::ClientCertMode,
    ) -> Self {
        Self::start_with_config(
            TestServerConfig {
                tls_cert_file: Some(fixture.server_cert.clone()),
                tls_key_file: Some(fixture.server_key.clone()),
                tls_ca_file: Some(fixture.ca_cert.clone()),
                tls_client_auth: Some(mode),
                ..Default::default()
            },
            ServerRole::Standalone,
        )
        .await
    }

    /// Connect to the TLS port and return a TlsTestClient.
    pub async fn connect_tls(&self, fixture: &crate::tls::TlsFixture) -> TlsTestClient {
        TlsTestClient::connect(self.tls_socket_addr(), &fixture.ca_cert_der, None, None).await
    }

    /// Connect to the TLS port with a client certificate (for mTLS).
    pub async fn connect_tls_with_client_cert(
        &self,
        fixture: &crate::tls::TlsFixture,
    ) -> TlsTestClient {
        TlsTestClient::connect(
            self.tls_socket_addr(),
            &fixture.ca_cert_der,
            Some(&fixture.client_cert_der),
            Some(&fixture.client_key_der),
        )
        .await
    }

    /// Try to connect to the TLS port (returns Result for negative tests).
    pub async fn try_connect_tls(
        &self,
        ca_cert_der: &[u8],
        client_cert_der: Option<&[u8]>,
        client_key_der: Option<&[u8]>,
    ) -> Result<TlsTestClient, Box<dyn std::error::Error>> {
        TlsTestClient::try_connect(
            self.tls_socket_addr(),
            ca_cert_der,
            client_cert_der,
            client_key_der,
        )
        .await
    }

    /// Build a `TestServerConfig` with TLS enabled from a fixture.
    ///
    /// Returns a config with cert/key/CA set. Callers can modify fields
    /// (e.g. `tls_replication`, `tls_cluster`) before starting the server.
    pub fn tls_config(fixture: &crate::tls::TlsFixture) -> TestServerConfig {
        TestServerConfig {
            tls_cert_file: Some(fixture.server_cert.clone()),
            tls_key_file: Some(fixture.server_key.clone()),
            tls_ca_file: Some(fixture.ca_cert.clone()),
            tls_client_cert_file: Some(fixture.client_cert.clone()),
            tls_client_key_file: Some(fixture.client_key.clone()),
            ..Default::default()
        }
    }

    /// Start a primary with TLS enabled.
    pub async fn start_primary_with_tls(fixture: &crate::tls::TlsFixture) -> Self {
        Self::start_with_config(Self::tls_config(fixture), ServerRole::Primary).await
    }

    /// Start a replica that connects to the primary over TLS.
    pub async fn start_replica_with_tls(
        primary: &TestServer,
        fixture: &crate::tls::TlsFixture,
    ) -> Self {
        let mut config = Self::tls_config(fixture);
        config.tls_replication = true;
        config.replication_role = Some("replica".to_string());
        config.replication_primary_host = Some("127.0.0.1".to_string());
        // When tls_replication=true, replica connects to the TLS port
        config.replication_primary_port = Some(primary.tls_port());
        Self::start_with_config(config, ServerRole::Replica).await
    }

    /// Start a standalone server with TLS and HTTPS enabled.
    pub async fn start_with_https(fixture: &crate::tls::TlsFixture) -> Self {
        let mut config = Self::tls_config(fixture);
        config.tls_no_tls_on_http = Some(false);
        Self::start_with_config(config, ServerRole::Standalone).await
    }

    /// Fetch a URL from the HTTPS metrics/admin endpoint.
    pub async fn fetch_https(
        &self,
        fixture: &crate::tls::TlsFixture,
        path: &str,
    ) -> reqwest::Response {
        let client = build_https_client(&fixture.ca_cert_der);
        let url = format!("https://127.0.0.1:{}{}", self.metrics_port(), path);
        client.get(&url).send().await.unwrap()
    }

    /// Connect to this server and return a TestClient (RESP2).
    pub async fn connect(&self) -> TestClient {
        let stream = TcpStream::connect(self.socket_addr()).await.unwrap();
        let framed = Framed::new(stream, Resp2::default());
        TestClient { framed }
    }

    /// Connect to the admin port.
    pub async fn connect_admin(&self) -> TestClient {
        let stream = TcpStream::connect(self.admin_socket_addr()).await.unwrap();
        let framed = Framed::new(stream, Resp2::default());
        TestClient { framed }
    }

    /// Connect to this server with RESP3 codec.
    pub async fn connect_resp3(&self) -> Resp3TestClient {
        let stream = TcpStream::connect(self.socket_addr()).await.unwrap();
        let framed = Framed::new(stream, Resp3::default());
        Resp3TestClient { framed }
    }

    /// Send a command and get response (convenience method).
    pub async fn send(&self, cmd: &str, args: &[&str]) -> Response {
        let mut client = self.connect().await;
        let mut all_args = vec![cmd];
        all_args.extend(args);
        client.command(&all_args).await
    }

    /// Send raw bytes and get raw response.
    pub async fn send_raw(&self, data: &[u8]) -> Vec<u8> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut stream = TcpStream::connect(self.socket_addr()).await.unwrap();
        stream.write_all(data).await.unwrap();
        let mut buf = vec![0u8; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        buf.truncate(n);
        buf
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /// Wait for the server to be ready to accept connections.
    pub async fn wait_for_ready(port: u16) {
        for _ in 0..50 {
            if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("Server failed to start on port {}", port);
    }

    /// Shutdown the test server (non-consuming).
    /// Shuts down raft first if present, then sends the shutdown signal
    /// and waits for the server task to fully complete (including RocksDB
    /// flush and lock release).
    pub async fn shutdown_mut(&mut self) {
        if let Some(ref raft) = self.raft {
            let _ = raft.shutdown().await;
        }
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        // Wait for the server task to fully complete so all resources
        // (RocksDB locks, file handles, etc.) are released before any
        // restart on the same data directory.
        let _ = timeout(Duration::from_secs(10), &mut self.handle).await;
    }

    /// Shutdown the test server (consuming).
    pub async fn shutdown(mut self) {
        self.shutdown_mut().await;
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        self.handle.abort();
        if let Some(ref dir) = self.data_dir
            && let Some(parent) = dir.parent()
        {
            let _ = std::fs::remove_dir_all(parent);
        }
    }
}

// ===========================================================================
// RESP2 TestClient
// ===========================================================================

/// A test client for sending RESP2 commands.
pub struct TestClient {
    /// The framed connection (public for direct access in some tests).
    pub framed: Framed<TcpStream, Resp2>,
}

impl TestClient {
    /// Send a command and receive a response.
    pub async fn command(&mut self, args: &[&str]) -> Response {
        // Build command frame
        let frame = BytesFrame::Array(
            args.iter()
                .map(|s| BytesFrame::BulkString(Bytes::from(s.to_string())))
                .collect(),
        );

        // Send
        self.framed.send(frame).await.unwrap();

        // Receive - use 15 second timeout to accommodate WAIT commands
        let response_frame = timeout(Duration::from_secs(15), self.framed.next())
            .await
            .expect("timeout")
            .expect("connection closed")
            .expect("frame error");

        frame_to_response(response_frame)
    }

    /// Send a command with raw Bytes arguments (for binary data like FUNCTION DUMP).
    pub async fn command_raw(&mut self, args: &[&Bytes]) -> Response {
        let frame = BytesFrame::Array(
            args.iter()
                .map(|b| BytesFrame::BulkString((*b).clone()))
                .collect(),
        );

        self.framed.send(frame).await.unwrap();

        let response_frame = timeout(Duration::from_secs(5), self.framed.next())
            .await
            .expect("timeout")
            .expect("connection closed")
            .expect("frame error");

        frame_to_response(response_frame)
    }

    /// Send a command without waiting for response.
    pub async fn send_only(&mut self, args: &[&str]) {
        let frame = BytesFrame::Array(
            args.iter()
                .map(|s| BytesFrame::BulkString(Bytes::from(s.to_string())))
                .collect(),
        );
        self.framed.send(frame).await.unwrap();
    }

    /// Read a response with timeout (useful for async operations).
    pub async fn read_response(&mut self, timeout_duration: Duration) -> Option<Response> {
        match timeout(timeout_duration, self.framed.next()).await {
            Ok(Some(Ok(frame))) => Some(frame_to_response(frame)),
            _ => None,
        }
    }

    /// Read a pushed message (for pub/sub subscribers).
    /// Alias for `read_response` for compatibility with pub/sub test patterns.
    pub async fn read_message(&mut self, timeout_duration: Duration) -> Option<Response> {
        self.read_response(timeout_duration).await
    }
}

// ===========================================================================
// RESP3 TestClient
// ===========================================================================

/// A test client for sending RESP3 commands.
pub struct Resp3TestClient {
    /// The framed connection (public for direct access in some tests).
    pub framed: Framed<TcpStream, Resp3>,
}

impl Resp3TestClient {
    /// Send a command as RESP3 array and receive RESP3 response.
    pub async fn command(&mut self, args: &[&str]) -> Resp3Frame {
        let frame = Resp3Frame::Array {
            data: args
                .iter()
                .map(|s| Resp3Frame::BlobString {
                    data: Bytes::from(s.to_string()),
                    attributes: None,
                })
                .collect(),
            attributes: None,
        };
        self.framed.send(frame).await.unwrap();

        timeout(Duration::from_secs(5), self.framed.next())
            .await
            .expect("timeout")
            .expect("connection closed")
            .expect("frame error")
    }

    /// Read a pushed message for pub/sub.
    pub async fn read_message(&mut self, timeout_duration: Duration) -> Option<Resp3Frame> {
        match timeout(timeout_duration, self.framed.next()).await {
            Ok(Some(Ok(frame))) => Some(frame),
            _ => None,
        }
    }
}

// ===========================================================================
// Response helpers
// ===========================================================================

/// Convert a BytesFrame to our Response type.
pub fn frame_to_response(frame: BytesFrame) -> Response {
    match frame {
        BytesFrame::SimpleString(s) => Response::Simple(s),
        BytesFrame::Error(e) => Response::Error(e.into_inner()),
        BytesFrame::Integer(n) => Response::Integer(n),
        BytesFrame::BulkString(b) => Response::Bulk(Some(b)),
        BytesFrame::Null => Response::Bulk(None),
        BytesFrame::Array(items) => {
            Response::Array(items.into_iter().map(frame_to_response).collect())
        }
    }
}

/// Parse a simple string response.
pub fn parse_simple_string(response: &Response) -> Option<&str> {
    match response {
        Response::Simple(s) => std::str::from_utf8(s).ok(),
        _ => None,
    }
}

/// Parse a bulk string response.
pub fn parse_bulk_string(response: &Response) -> Option<&[u8]> {
    match response {
        Response::Bulk(Some(s)) => Some(s),
        _ => None,
    }
}

/// Parse an integer response.
pub fn parse_integer(response: &Response) -> Option<i64> {
    match response {
        Response::Integer(n) => Some(*n),
        _ => None,
    }
}

/// Check if response is an OK simple string.
pub fn is_ok(response: &Response) -> bool {
    matches!(response, Response::Simple(s) if s == "OK")
}

/// Check if response is an error.
pub fn is_error(response: &Response) -> bool {
    matches!(response, Response::Error(_))
}

/// Extract error message from response.
pub fn get_error_message(response: &Response) -> Option<&str> {
    match response {
        Response::Error(e) => std::str::from_utf8(e).ok(),
        _ => None,
    }
}

// ===========================================================================
// TLS TestClient
// ===========================================================================

/// A test client for sending RESP2 commands over TLS.
pub struct TlsTestClient {
    pub framed: Framed<tokio_rustls::client::TlsStream<TcpStream>, Resp2>,
}

impl TlsTestClient {
    /// Connect to a TLS server with the given CA cert and optional client cert.
    pub async fn connect(
        addr: SocketAddr,
        ca_cert_der: &[u8],
        client_cert_der: Option<&[u8]>,
        client_key_der: Option<&[u8]>,
    ) -> Self {
        Self::try_connect(addr, ca_cert_der, client_cert_der, client_key_der)
            .await
            .expect("TLS connection failed")
    }

    /// Try to connect, returning Result for negative tests.
    pub async fn try_connect(
        addr: SocketAddr,
        ca_cert_der: &[u8],
        client_cert_der: Option<&[u8]>,
        client_key_der: Option<&[u8]>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};

        // Ensure crypto provider is installed
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        // Build root cert store from CA cert
        let mut root_store = rustls::RootCertStore::empty();
        root_store.add(CertificateDer::from(ca_cert_der.to_vec()))?;

        // Build client config
        let builder = rustls::ClientConfig::builder().with_root_certificates(root_store);

        let client_config =
            if let (Some(cert_der), Some(key_der)) = (client_cert_der, client_key_der) {
                let certs = vec![CertificateDer::from(cert_der.to_vec())];
                let key = PrivateKeyDer::try_from(key_der.to_vec())?;
                builder.with_client_auth_cert(certs, key)?
            } else {
                builder.with_no_client_auth()
            };

        let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));
        let tcp_stream = TcpStream::connect(addr).await?;
        let server_name = ServerName::try_from("localhost")?;
        let tls_stream = connector.connect(server_name, tcp_stream).await?;

        Ok(Self {
            framed: Framed::new(tls_stream, Resp2::default()),
        })
    }

    /// Send a command and receive a response.
    pub async fn command(&mut self, args: &[&str]) -> Response {
        let frame = BytesFrame::Array(
            args.iter()
                .map(|s| BytesFrame::BulkString(Bytes::from(s.to_string())))
                .collect(),
        );
        self.framed.send(frame).await.unwrap();

        timeout(Duration::from_secs(15), self.framed.next())
            .await
            .expect("timeout waiting for response")
            .expect("connection closed")
            .map(frame_to_response)
            .expect("frame error")
    }
}

/// Build a `reqwest::Client` that trusts the given CA certificate for HTTPS.
fn build_https_client(ca_cert_der: &[u8]) -> reqwest::Client {
    let ca_cert = reqwest::Certificate::from_der(ca_cert_der).expect("invalid CA DER");
    reqwest::Client::builder()
        .add_root_certificate(ca_cert)
        .no_proxy()
        .danger_accept_invalid_hostnames(false)
        .build()
        .expect("failed to build HTTPS client")
}
