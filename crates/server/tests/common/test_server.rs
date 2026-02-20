//! TestServer helper for replication integration tests.
//!
//! This module provides a TestServer abstraction for spawning FrogDB servers
//! in tests with dynamic port allocation and proper cleanup.

use bytes::Bytes;
use frogdb_protocol::Response;
use frogdb_server::{Config, Server};
use futures::{SinkExt, StreamExt};
use redis_protocol::codec::Resp2;
use redis_protocol::resp2::types::BytesFrame;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::path::PathBuf;
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
#[derive(Clone, Default)]
pub struct TestServerConfig {
    /// Number of shards (default: 4)
    pub num_shards: Option<usize>,
    /// Enable RocksDB persistence (default: false)
    pub persistence: bool,
    /// Data directory for persistence (auto-generated temp dir if None)
    pub data_dir: Option<PathBuf>,
    /// Log level (default: "warn" to reduce test noise)
    pub log_level: Option<String>,
    /// Password required for authentication (default: None)
    pub requirepass: Option<String>,
    /// Enable admin port (default: false)
    pub admin_enabled: bool,
}

/// A test server instance that can be used in integration tests.
pub struct TestServer {
    /// RESP protocol port (for client commands)
    port: u16,
    /// Metrics server port
    metrics_port: u16,
    /// Admin port (None if admin disabled)
    admin_port: Option<u16>,
    /// Server role for reference
    role: ServerRole,
    /// Shutdown signal sender
    shutdown_tx: Option<oneshot::Sender<()>>,
    /// Server task handle
    #[allow(dead_code)]
    handle: tokio::task::JoinHandle<()>,
    /// Data directory path (managed by the test)
    #[allow(dead_code)]
    data_dir: Option<PathBuf>,
}

impl TestServer {
    /// Allocate a free port from the OS.
    fn allocate_port() -> u16 {
        // Bind to port 0 - OS assigns a free ephemeral port
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        // Drop the listener to free the port for the server
        drop(listener);
        port
    }

    /// Create a unique temp directory for test data.
    /// Uses /tmp/claude/ which is writable in the sandbox.
    fn create_temp_dir() -> PathBuf {
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

    /// Start a standalone server on an OS-assigned port.
    pub async fn start_standalone() -> Self {
        Self::start_standalone_with_config(TestServerConfig::default()).await
    }

    /// Start a standalone server with security (password required).
    pub async fn start_with_security(requirepass: &str) -> Self {
        let mut config = TestServerConfig::default();
        config.requirepass = Some(requirepass.to_string());
        Self::start_standalone_with_config(config).await
    }

    /// Start a standalone server with custom configuration.
    pub async fn start_standalone_with_config(test_config: TestServerConfig) -> Self {
        let port = Self::allocate_port();
        let metrics_port = Self::allocate_port();
        let (owned_dir, data_dir) = match test_config.data_dir {
            Some(dir) => (None, dir),
            None => {
                let dir = Self::create_temp_dir();
                (Some(dir.clone()), dir)
            }
        };

        // Allocate admin port if admin is enabled
        let admin_port = if test_config.admin_enabled {
            Some(Self::allocate_port())
        } else {
            None
        };

        let mut config = Config::default();
        config.server.bind = "127.0.0.1".to_string();
        config.server.port = port;
        config.server.num_shards = test_config.num_shards.unwrap_or(4);
        config.logging.level = test_config.log_level.unwrap_or_else(|| "warn".to_string());
        config.persistence.enabled = test_config.persistence;
        config.persistence.data_dir = data_dir;
        config.metrics.bind = "127.0.0.1".to_string();
        config.metrics.port = metrics_port;
        config.replication.role = "standalone".to_string();
        if let Some(ref pass) = test_config.requirepass {
            config.security.requirepass = pass.clone();
        }

        // Configure admin port if enabled
        if let Some(admin_p) = admin_port {
            config.admin.enabled = true;
            config.admin.bind = "127.0.0.1".to_string();
            config.admin.port = admin_p;
        }

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let handle = tokio::spawn(async move {
            let server = Server::new(config).await.unwrap();
            let _ = server
                .run_until(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        // Wait for server to be ready
        Self::wait_for_ready(port).await;

        Self {
            port,
            metrics_port,
            admin_port,
            role: ServerRole::Standalone,
            shutdown_tx: Some(shutdown_tx),
            handle,
            data_dir: owned_dir,
        }
    }

    /// Start a primary server on an OS-assigned port.
    pub async fn start_primary() -> Self {
        Self::start_primary_with_config(TestServerConfig::default()).await
    }

    /// Start a primary server with custom configuration.
    pub async fn start_primary_with_config(test_config: TestServerConfig) -> Self {
        let port = Self::allocate_port();
        let metrics_port = Self::allocate_port();
        let (owned_dir, data_dir) = match test_config.data_dir {
            Some(dir) => (None, dir),
            None => {
                let dir = Self::create_temp_dir();
                (Some(dir.clone()), dir)
            }
        };

        let mut config = Config::default();
        config.server.bind = "127.0.0.1".to_string();
        config.server.port = port;
        config.server.num_shards = test_config.num_shards.unwrap_or(4);
        config.logging.level = test_config.log_level.unwrap_or_else(|| "warn".to_string());
        config.persistence.enabled = test_config.persistence;
        config.persistence.data_dir = data_dir;
        config.metrics.bind = "127.0.0.1".to_string();
        config.metrics.port = metrics_port;
        config.replication.role = "primary".to_string();

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let handle = tokio::spawn(async move {
            let server = Server::new(config).await.unwrap();
            let _ = server
                .run_until(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        // Wait for server to be ready
        Self::wait_for_ready(port).await;

        Self {
            port,
            metrics_port,
            admin_port: None,
            role: ServerRole::Primary,
            shutdown_tx: Some(shutdown_tx),
            handle,
            data_dir: owned_dir,
        }
    }

    /// Start a replica server connecting to the given primary.
    pub async fn start_replica(primary: &TestServer) -> Self {
        Self::start_replica_with_config(primary, TestServerConfig::default()).await
    }

    /// Start a replica server with custom configuration.
    pub async fn start_replica_with_config(
        primary: &TestServer,
        test_config: TestServerConfig,
    ) -> Self {
        let port = Self::allocate_port();
        let metrics_port = Self::allocate_port();
        let (owned_dir, data_dir) = match test_config.data_dir {
            Some(dir) => (None, dir),
            None => {
                let dir = Self::create_temp_dir();
                (Some(dir.clone()), dir)
            }
        };

        let mut config = Config::default();
        config.server.bind = "127.0.0.1".to_string();
        config.server.port = port;
        config.server.num_shards = test_config.num_shards.unwrap_or(4);
        config.logging.level = test_config.log_level.unwrap_or_else(|| "warn".to_string());
        config.persistence.enabled = test_config.persistence;
        config.persistence.data_dir = data_dir;
        config.metrics.bind = "127.0.0.1".to_string();
        config.metrics.port = metrics_port;
        config.replication.role = "replica".to_string();
        config.replication.primary_host = "127.0.0.1".to_string();
        config.replication.primary_port = primary.port();

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let handle = tokio::spawn(async move {
            let server = Server::new(config).await.unwrap();
            let _ = server
                .run_until(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        // Wait for server to be ready
        Self::wait_for_ready(port).await;

        Self {
            port,
            metrics_port,
            admin_port: None,
            role: ServerRole::Replica,
            shutdown_tx: Some(shutdown_tx),
            handle,
            data_dir: owned_dir,
        }
    }

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

    /// Connect to the admin port.
    pub async fn connect_admin(&self) -> TestClient {
        let stream = TcpStream::connect(self.admin_socket_addr()).await.unwrap();
        let framed = Framed::new(stream, Resp2);
        TestClient { framed }
    }

    /// Start standalone server with admin port enabled.
    pub async fn start_with_admin_port() -> Self {
        let mut config = TestServerConfig::default();
        config.admin_enabled = true;
        Self::start_standalone_with_config(config).await
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

    /// Connect to this server and return a TestClient.
    pub async fn connect(&self) -> TestClient {
        let stream = TcpStream::connect(self.socket_addr()).await.unwrap();
        let framed = Framed::new(stream, Resp2);
        TestClient { framed }
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

    /// Wait for the server to be ready to accept connections.
    async fn wait_for_ready(port: u16) {
        for _ in 0..50 {
            if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("Server failed to start on port {}", port);
    }

    /// Shutdown the test server.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        // Give the server a moment to shut down gracefully
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

/// A test client for sending RESP commands.
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
