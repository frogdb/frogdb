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
    /// Admin port (None if admin disabled)
    admin_port: Option<u16>,
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
}

impl TestServer {
    /// Create a unique temp directory for test data.
    /// Uses /tmp/claude/ which is writable in the sandbox.
    pub fn create_temp_dir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let dir = PathBuf::from(format!("/tmp/claude/frogdb_test_{}_{}", timestamp, id));
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
        config.metrics.bind = "127.0.0.1".to_string();
        config.metrics.port = 0; // OS assigns

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

        // Cluster configuration
        let mut listeners = ServerListeners::default();

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

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        // Construct server *before* spawning so we can read the bound addresses.
        let server = Server::with_listeners(config, listeners, None)
            .await
            .unwrap();
        let port = server.local_addr().unwrap().port();
        let metrics_port = server
            .metrics_addr()
            .and_then(|r| r.ok())
            .map(|a| a.port())
            .unwrap_or(0);
        let admin_port = server
            .admin_resp_addr()
            .and_then(|r| r.ok())
            .map(|a| a.port());
        let cluster_bus_port = server
            .cluster_bus_addr()
            .and_then(|r| r.ok())
            .map(|a| a.port());
        let raft = server.raft().cloned();
        let cluster_state = server.cluster_state().cloned();

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
            cluster_bus_port,
            role,
            shutdown_tx: Some(shutdown_tx),
            handle,
            data_dir: owned_dir,
            raft,
            cluster_state,
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

    /// Get the admin port (panics if admin not enabled).
    pub fn admin_port(&self) -> u16 {
        self.admin_port.expect("Admin port not enabled")
    }

    /// Check if admin port is enabled.
    pub fn has_admin_port(&self) -> bool {
        self.admin_port.is_some()
    }

    /// Get admin socket address.
    pub fn admin_socket_addr(&self) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], self.admin_port()))
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

    /// Get the RESP address as SocketAddr.
    pub fn socket_addr(&self) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], self.port))
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
        let framed = Framed::new(stream, Resp2);
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

    /// Connect to this server and return a TestClient (RESP2).
    pub async fn connect(&self) -> TestClient {
        let stream = TcpStream::connect(self.socket_addr()).await.unwrap();
        let framed = Framed::new(stream, Resp2);
        TestClient { framed }
    }

    /// Connect to the admin port.
    pub async fn connect_admin(&self) -> TestClient {
        let stream = TcpStream::connect(self.admin_socket_addr()).await.unwrap();
        let framed = Framed::new(stream, Resp2);
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
    /// Shuts down raft first if present, then sends the shutdown signal.
    pub async fn shutdown_mut(&mut self) {
        if let Some(ref raft) = self.raft {
            let _ = raft.shutdown().await;
        }
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        // Give the server a moment to shut down gracefully
        tokio::time::sleep(Duration::from_millis(50)).await;
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
