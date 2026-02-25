//! Cluster test harness for integration testing.
//!
//! Provides types and utilities for testing multi-node cluster operations:
//! - ClusterNodeConfig: Configuration for individual cluster nodes
//! - ClusterTestNode: Wrapper for a single cluster node

#![allow(dead_code)]
//! - ClusterTestHarness: Orchestrates multi-node cluster testing

use super::cluster_helpers::{ClusterError, ClusterInfo, parse_cluster_info};
use super::test_server::TestClient;
use frogdb_protocol::Response;
use frogdb_server::net::tcp_listener_reusable;
use frogdb_server::{Config, Server, ServerListeners};
use redis_protocol::codec::Resp2;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio_util::codec::Framed;

/// Configuration for a cluster test node.
#[derive(Clone, Debug)]
pub struct ClusterNodeConfig {
    /// Node ID (0 = auto-generate).
    pub node_id: u64,
    /// Number of shards (default: 4).
    pub num_shards: Option<usize>,
    /// Enable persistence (default: false).
    pub persistence: bool,
    /// Data directory (auto temp dir if None).
    pub data_dir: Option<PathBuf>,
    /// Election timeout in ms (default: 300 for fast tests).
    pub election_timeout_ms: u64,
    /// Heartbeat interval in ms (default: 100 for fast tests).
    pub heartbeat_interval_ms: u64,
    /// Connection timeout in ms (default: 2000).
    pub connect_timeout_ms: u64,
    /// Request timeout in ms (default: 5000).
    pub request_timeout_ms: u64,
    /// Log level (default: "warn").
    pub log_level: Option<String>,
}

impl Default for ClusterNodeConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            num_shards: Some(4),
            persistence: false,
            data_dir: None,
            election_timeout_ms: 300,
            heartbeat_interval_ms: 100,
            connect_timeout_ms: 2000,
            request_timeout_ms: 5000,
            log_level: Some("warn".to_string()),
        }
    }
}

/// Internal state stored for potential restart.
#[derive(Clone)]
struct NodeRestartInfo {
    node_id: u64,
    client_port: u16,
    cluster_port: u16,
    metrics_port: u16,
    data_dir: PathBuf,
    config: ClusterNodeConfig,
    initial_nodes: Vec<String>,
}

/// A single cluster node for testing.
pub struct ClusterTestNode {
    node_id: u64,
    client_port: u16,
    cluster_port: u16,
    metrics_port: u16,
    shutdown_tx: Option<oneshot::Sender<()>>,
    handle: Option<tokio::task::JoinHandle<()>>,
    data_dir: Option<PathBuf>,
    running: Arc<AtomicBool>,
    restart_info: Option<NodeRestartInfo>,
}

impl ClusterTestNode {
    /// Create a unique temp directory for test data.
    fn create_temp_dir() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = PathBuf::from(format!(
            "/tmp/claude/frogdb_cluster_test_{}_{}",
            timestamp, id
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Generate a node ID from cluster port (deterministic).
    fn generate_node_id_from_port(cluster_port: u16) -> u64 {
        use std::hash::{Hash, Hasher};
        let addr: std::net::SocketAddr = format!("127.0.0.1:{}", cluster_port).parse().unwrap();
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        addr.hash(&mut hasher);
        hasher.finish()
    }

    /// Start a cluster node with the given configuration.
    pub async fn start(config: ClusterNodeConfig, initial_nodes: Vec<String>) -> Self {
        // Pre-bind cluster bus listener on port 0 to avoid TOCTOU race
        let cluster_bus_listener =
            tcp_listener_reusable("127.0.0.1:0".parse::<SocketAddr>().unwrap())
                .await
                .unwrap();
        let cluster_port = cluster_bus_listener.local_addr().unwrap().port();
        let cluster_bus_addr_str = format!("127.0.0.1:{}", cluster_port);

        let data_dir = config
            .data_dir
            .clone()
            .unwrap_or_else(Self::create_temp_dir);
        let node_id = if config.node_id == 0 {
            Self::generate_node_id_from_port(cluster_port)
        } else {
            config.node_id
        };

        // Create cluster data directory
        let cluster_data_dir = data_dir.join("cluster");
        std::fs::create_dir_all(&cluster_data_dir).unwrap();

        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();

        // Build server config (client and metrics ports use 0 — read actual ports after construction)
        let mut server_config = Config::default();
        server_config.server.bind = "127.0.0.1".to_string();
        server_config.server.port = 0;
        server_config.server.num_shards = config.num_shards.unwrap_or(4);
        server_config.logging.level = config
            .log_level
            .clone()
            .unwrap_or_else(|| "warn".to_string());
        server_config.persistence.enabled = config.persistence;
        server_config.persistence.data_dir = data_dir.clone();
        server_config.metrics.bind = "127.0.0.1".to_string();
        server_config.metrics.port = 0;

        // Cluster configuration
        server_config.cluster.enabled = true;
        server_config.cluster.node_id = node_id;
        server_config.cluster.cluster_bus_addr = cluster_bus_addr_str;
        server_config.cluster.initial_nodes = initial_nodes.clone();
        server_config.cluster.data_dir = cluster_data_dir;
        server_config.cluster.election_timeout_ms = config.election_timeout_ms;
        server_config.cluster.heartbeat_interval_ms = config.heartbeat_interval_ms;
        server_config.cluster.connect_timeout_ms = config.connect_timeout_ms;
        server_config.cluster.request_timeout_ms = config.request_timeout_ms;

        // Construct server OUTSIDE spawn so we can read bound addresses
        let listeners = ServerListeners {
            cluster_bus: Some(cluster_bus_listener),
        };
        let server = Server::with_listeners(server_config, listeners)
            .await
            .unwrap();
        let client_port = server.local_addr().unwrap().port();
        let metrics_port = server
            .metrics_addr()
            .and_then(|r| r.ok())
            .map(|a| a.port())
            .unwrap_or(0);

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let handle = tokio::spawn(async move {
            let _ = server
                .run_until(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
            running_clone.store(false, Ordering::SeqCst);
        });

        // Wait for server to be ready
        Self::wait_for_ready(client_port).await;

        let restart_info = NodeRestartInfo {
            node_id,
            client_port,
            cluster_port,
            metrics_port,
            data_dir: data_dir.clone(),
            config: config.clone(),
            initial_nodes,
        };

        Self {
            node_id,
            client_port,
            cluster_port,
            metrics_port,
            shutdown_tx: Some(shutdown_tx),
            handle: Some(handle),
            data_dir: Some(data_dir),
            running,
            restart_info: Some(restart_info),
        }
    }

    /// Wait for the server to be ready to accept connections.
    async fn wait_for_ready(port: u16) {
        for _ in 0..100 {
            if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("Cluster node failed to start on port {}", port);
    }

    /// Get the node ID.
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// Get the client address.
    pub fn client_addr(&self) -> String {
        format!("127.0.0.1:{}", self.client_port)
    }

    /// Get the cluster bus address.
    pub fn cluster_addr(&self) -> String {
        format!("127.0.0.1:{}", self.cluster_port)
    }

    /// Get the client port.
    pub fn client_port(&self) -> u16 {
        self.client_port
    }

    /// Get the cluster port.
    pub fn cluster_port(&self) -> u16 {
        self.cluster_port
    }

    /// Check if the node is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Connect to this node and return a TestClient.
    /// Retries on transient errors (e.g. EADDRNOTAVAIL during rapid restarts).
    pub async fn connect(&self) -> TestClient {
        let mut last_err = None;
        for _ in 0..10 {
            match TcpStream::connect(("127.0.0.1", self.client_port)).await {
                Ok(stream) => {
                    let framed = Framed::new(stream, Resp2);
                    return TestClient { framed };
                }
                Err(e) => {
                    last_err = Some(e);
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
        panic!(
            "Failed to connect to node on port {} after 10 attempts: {}",
            self.client_port,
            last_err.unwrap()
        );
    }

    /// Send a command and get response.
    pub async fn send(&self, cmd: &str, args: &[&str]) -> Response {
        let mut client = self.connect().await;
        let mut all_args = vec![cmd];
        all_args.extend(args);
        client.command(&all_args).await
    }

    /// Gracefully shutdown the node.
    pub async fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            let _ = timeout(Duration::from_secs(5), handle).await;
        }
        self.running.store(false, Ordering::SeqCst);
        // Give the server a moment to clean up
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    /// Immediately stop the node (simulate crash).
    /// Triggers shutdown and briefly waits for cleanup so RocksDB in-process
    /// locks are released. In a real crash the OS handles this; in single-process
    /// tests we must do it ourselves.
    pub async fn kill(&mut self) {
        // Send shutdown signal to allow the server to clean up child tasks
        // (Raft, replication, etc.) that hold RocksDB handles.
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        // Wait briefly for the server to terminate and release locks
        if let Some(handle) = self.handle.take() {
            let _ = timeout(Duration::from_millis(2000), handle).await;
        }
        self.running.store(false, Ordering::SeqCst);
    }

    /// Restart the node with the same configuration.
    pub async fn restart(&mut self) -> Result<(), ClusterError> {
        let restart_info = self
            .restart_info
            .clone()
            .ok_or_else(|| ClusterError::new("No restart info available"))?;

        // Make sure the node is stopped
        if self.is_running() {
            self.shutdown().await;
        }

        // Wait for old server task to fully terminate (handles both shutdown()
        // and kill() cases — ensures RocksDB locks are released)
        if let Some(handle) = self.handle.take() {
            let _ = timeout(Duration::from_secs(5), handle).await;
        }

        // Retry binding with backoff — OS may need time to release TIME_WAIT sockets
        let cluster_bus_addr: SocketAddr = format!("127.0.0.1:{}", restart_info.cluster_port)
            .parse()
            .unwrap();
        let cluster_bus_listener = {
            let mut listener = None;
            let mut last_err = None;
            for attempt in 0..10 {
                tokio::time::sleep(Duration::from_millis(500)).await;
                match tcp_listener_reusable(cluster_bus_addr).await {
                    Ok(l) => {
                        listener = Some(l);
                        break;
                    }
                    Err(e) => {
                        eprintln!(
                            "Cluster bus bind attempt {}/10 for port {} failed: {}",
                            attempt + 1,
                            restart_info.cluster_port,
                            e
                        );
                        last_err = Some(e);
                    }
                }
            }
            listener.ok_or_else(|| {
                ClusterError::new(format!(
                    "Failed to bind cluster bus after 10 attempts: {}",
                    last_err.unwrap()
                ))
            })?
        };

        // Create cluster data directory if needed
        let cluster_data_dir = restart_info.data_dir.join("cluster");
        std::fs::create_dir_all(&cluster_data_dir).ok();

        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();

        // Build server config (client and metrics use port 0 — read actual after construction)
        let mut server_config = Config::default();
        server_config.server.bind = "127.0.0.1".to_string();
        server_config.server.port = 0;
        server_config.server.num_shards = restart_info.config.num_shards.unwrap_or(4);
        server_config.logging.level = restart_info
            .config
            .log_level
            .clone()
            .unwrap_or_else(|| "warn".to_string());
        server_config.persistence.enabled = restart_info.config.persistence;
        server_config.persistence.data_dir = restart_info.data_dir.clone();
        server_config.metrics.bind = "127.0.0.1".to_string();
        server_config.metrics.port = 0;

        // Cluster configuration
        server_config.cluster.enabled = true;
        server_config.cluster.node_id = restart_info.node_id;
        server_config.cluster.cluster_bus_addr = format!("127.0.0.1:{}", restart_info.cluster_port);
        server_config.cluster.initial_nodes = restart_info.initial_nodes.clone();
        server_config.cluster.data_dir = cluster_data_dir;
        server_config.cluster.election_timeout_ms = restart_info.config.election_timeout_ms;
        server_config.cluster.heartbeat_interval_ms = restart_info.config.heartbeat_interval_ms;
        server_config.cluster.connect_timeout_ms = restart_info.config.connect_timeout_ms;
        server_config.cluster.request_timeout_ms = restart_info.config.request_timeout_ms;

        // Construct server OUTSIDE spawn to read bound addresses
        let listeners = ServerListeners {
            cluster_bus: Some(cluster_bus_listener),
        };
        let server = Server::with_listeners(server_config, listeners)
            .await
            .map_err(|e| ClusterError::new(format!("Failed to start server: {}", e)))?;
        let client_port = server.local_addr().unwrap().port();
        let metrics_port = server
            .metrics_addr()
            .and_then(|r| r.ok())
            .map(|a| a.port())
            .unwrap_or(0);

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let handle = tokio::spawn(async move {
            let _ = server
                .run_until(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
            running_clone.store(false, Ordering::SeqCst);
        });

        // Wait for server to be ready
        Self::wait_for_ready(client_port).await;

        // Update ports (client/metrics may change since we use port 0)
        self.client_port = client_port;
        self.metrics_port = metrics_port;
        self.shutdown_tx = Some(shutdown_tx);
        self.handle = Some(handle);
        self.running = running;

        // Update restart_info with new ports
        self.restart_info = Some(NodeRestartInfo {
            client_port,
            metrics_port,
            ..restart_info
        });

        Ok(())
    }
}

impl Drop for ClusterTestNode {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

/// Orchestrates a multi-node cluster for testing.
pub struct ClusterTestHarness {
    nodes: HashMap<u64, ClusterTestNode>,
    node_order: Vec<u64>,
    base_config: ClusterNodeConfig,
}

impl ClusterTestHarness {
    /// Create a new harness with default configuration.
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            node_order: Vec::new(),
            base_config: ClusterNodeConfig::default(),
        }
    }

    /// Create a new harness with custom base configuration.
    pub fn with_config(config: ClusterNodeConfig) -> Self {
        Self {
            nodes: HashMap::new(),
            node_order: Vec::new(),
            base_config: config,
        }
    }

    /// Start a cluster with the specified number of nodes.
    pub async fn start_cluster(&mut self, num_nodes: usize) -> Result<(), ClusterError> {
        if num_nodes == 0 {
            return Err(ClusterError::new("Cluster must have at least 1 node"));
        }

        // Pre-bind cluster bus listeners for ALL nodes (no TOCTOU!)
        let mut bus_listeners = Vec::new();
        let mut bus_addrs = Vec::new();
        let mut node_ids = Vec::new();
        for _ in 0..num_nodes {
            let listener = tcp_listener_reusable("127.0.0.1:0".parse::<SocketAddr>().unwrap())
                .await
                .unwrap();
            let addr = listener.local_addr().unwrap();
            let cluster_port = addr.port();
            let node_id = ClusterTestNode::generate_node_id_from_port(cluster_port);
            bus_addrs.push(addr.to_string());
            node_ids.push(node_id);
            bus_listeners.push(listener);
        }

        // Build initial_nodes list with all cluster addresses
        let initial_nodes: Vec<String> = bus_addrs.clone();

        // Start all nodes
        for (i, bus_listener) in bus_listeners.into_iter().enumerate() {
            let node_id = node_ids[i];
            let cluster_port = bus_listener.local_addr().unwrap().port();

            let mut config = self.base_config.clone();
            config.node_id = node_id;

            let data_dir = ClusterTestNode::create_temp_dir();
            config.data_dir = Some(data_dir.clone());

            // Create cluster data directory
            let cluster_data_dir = data_dir.join("cluster");
            std::fs::create_dir_all(&cluster_data_dir).unwrap();

            let running = Arc::new(AtomicBool::new(true));
            let running_clone = running.clone();

            // Build server config (client and metrics ports use 0)
            let mut server_config = Config::default();
            server_config.server.bind = "127.0.0.1".to_string();
            server_config.server.port = 0;
            server_config.server.num_shards = config.num_shards.unwrap_or(4);
            server_config.logging.level = config
                .log_level
                .clone()
                .unwrap_or_else(|| "warn".to_string());
            server_config.persistence.enabled = config.persistence;
            server_config.persistence.data_dir = data_dir.clone();
            server_config.metrics.bind = "127.0.0.1".to_string();
            server_config.metrics.port = 0;

            // Cluster configuration
            server_config.cluster.enabled = true;
            server_config.cluster.node_id = node_id;
            server_config.cluster.cluster_bus_addr = bus_addrs[i].clone();
            server_config.cluster.initial_nodes = initial_nodes.clone();
            server_config.cluster.data_dir = cluster_data_dir;
            server_config.cluster.election_timeout_ms = config.election_timeout_ms;
            server_config.cluster.heartbeat_interval_ms = config.heartbeat_interval_ms;
            server_config.cluster.connect_timeout_ms = config.connect_timeout_ms;
            server_config.cluster.request_timeout_ms = config.request_timeout_ms;

            // Construct server OUTSIDE spawn to read bound addresses
            let listeners = ServerListeners {
                cluster_bus: Some(bus_listener),
            };
            let server = Server::with_listeners(server_config, listeners)
                .await
                .unwrap();
            let client_port = server.local_addr().unwrap().port();
            let metrics_port = server
                .metrics_addr()
                .and_then(|r| r.ok())
                .map(|a| a.port())
                .unwrap_or(0);

            let (shutdown_tx, shutdown_rx) = oneshot::channel();

            let handle = tokio::spawn(async move {
                let _ = server
                    .run_until(async move {
                        let _ = shutdown_rx.await;
                    })
                    .await;
                running_clone.store(false, Ordering::SeqCst);
            });

            let restart_info = NodeRestartInfo {
                node_id,
                client_port,
                cluster_port,
                metrics_port,
                data_dir: data_dir.clone(),
                config: config.clone(),
                initial_nodes: initial_nodes.clone(),
            };

            let node = ClusterTestNode {
                node_id,
                client_port,
                cluster_port,
                metrics_port,
                shutdown_tx: Some(shutdown_tx),
                handle: Some(handle),
                data_dir: Some(data_dir),
                running,
                restart_info: Some(restart_info),
            };

            self.nodes.insert(node_id, node);
            self.node_order.push(node_id);
        }

        // Wait for all nodes to be ready
        for node_id in &self.node_order {
            let node = self.nodes.get(node_id).unwrap();
            ClusterTestNode::wait_for_ready(node.client_port).await;
        }

        Ok(())
    }

    /// Add a new node to the cluster.
    pub async fn add_node(&mut self) -> Result<u64, ClusterError> {
        if self.nodes.is_empty() {
            return Err(ClusterError::new("Cannot add node to empty cluster"));
        }

        // Get existing cluster addresses
        let initial_nodes: Vec<String> = self.nodes.values().map(|n| n.cluster_addr()).collect();

        let config = self.base_config.clone();
        let node = ClusterTestNode::start(config, initial_nodes).await;
        let node_id = node.node_id();
        let new_node_client_port = node.client_port();
        let new_node_cluster_port = node.cluster_port();

        self.nodes.insert(node_id, node);
        self.node_order.push(node_id);

        // Issue CLUSTER MEET from an existing node to add the new node to the cluster
        // This registers the new node in the Raft membership and network factory
        // CLUSTER MEET <ip> <client-port> [<cluster-bus-port>]
        // Try each existing node until one succeeds (handles leader forwarding)
        let mut last_error = None;
        let existing_node_ids: Vec<u64> = self
            .node_order
            .iter()
            .filter(|&&id| id != node_id)
            .copied()
            .collect();

        for existing_node_id in existing_node_ids {
            if let Some(existing_node) = self.nodes.get(&existing_node_id) {
                let response = existing_node
                    .send(
                        "CLUSTER",
                        &[
                            "MEET",
                            "127.0.0.1",
                            &new_node_client_port.to_string(),
                            &new_node_cluster_port.to_string(),
                        ],
                    )
                    .await;
                // Check if CLUSTER MEET succeeded
                match &response {
                    frogdb_protocol::Response::Simple(s) if s.as_ref() == b"OK" => {
                        return Ok(node_id);
                    }
                    frogdb_protocol::Response::Error(e) => {
                        let error_msg = String::from_utf8_lossy(e);
                        // Check if this is a REDIRECT error - try to find the leader
                        if error_msg.starts_with("REDIRECT ") {
                            // Parse "REDIRECT <node_id> <addr>" and find the leader
                            let parts: Vec<&str> = error_msg.split_whitespace().collect();
                            if parts.len() >= 3 {
                                let leader_addr = parts[2];
                                // Find the node with this address and retry
                                for (&nid, n) in &self.nodes {
                                    if nid != node_id && n.client_addr() == leader_addr {
                                        let retry_response = n
                                            .send(
                                                "CLUSTER",
                                                &[
                                                    "MEET",
                                                    "127.0.0.1",
                                                    &new_node_client_port.to_string(),
                                                    &new_node_cluster_port.to_string(),
                                                ],
                                            )
                                            .await;
                                        match &retry_response {
                                            frogdb_protocol::Response::Simple(s)
                                                if s.as_ref() == b"OK" =>
                                            {
                                                return Ok(node_id);
                                            }
                                            _ => {} // will set last_error below
                                        }
                                        break;
                                    }
                                }
                            }
                        }
                        last_error = Some(format!("CLUSTER MEET failed: {}", error_msg));
                    }
                    _ => {
                        last_error =
                            Some(format!("Unexpected CLUSTER MEET response: {:?}", response));
                    }
                }
            }
        }

        // If we get here, none of the attempts succeeded
        Err(ClusterError::new(last_error.unwrap_or_else(|| {
            "No existing nodes to send CLUSTER MEET".to_string()
        })))
    }

    /// Remove a node from the cluster.
    pub async fn remove_node(&mut self, node_id: u64) -> Result<(), ClusterError> {
        let mut node = self
            .nodes
            .remove(&node_id)
            .ok_or_else(|| ClusterError::new(format!("Node {} not found", node_id)))?;

        self.node_order.retain(|&id| id != node_id);
        node.shutdown().await;

        Ok(())
    }

    /// Shutdown all nodes.
    pub async fn shutdown_all(&mut self) {
        for node in self.nodes.values_mut() {
            node.shutdown().await;
        }
        self.nodes.clear();
        self.node_order.clear();
    }

    /// Get a reference to a node.
    pub fn node(&self, node_id: u64) -> Option<&ClusterTestNode> {
        self.nodes.get(&node_id)
    }

    /// Get a mutable reference to a node.
    pub fn node_mut(&mut self, node_id: u64) -> Option<&mut ClusterTestNode> {
        self.nodes.get_mut(&node_id)
    }

    /// Get all node IDs in the order they were added.
    pub fn node_ids(&self) -> Vec<u64> {
        self.node_order.clone()
    }

    /// Get cluster info from a specific node.
    pub async fn get_cluster_info(&self, node_id: u64) -> Result<ClusterInfo, ClusterError> {
        let node = self
            .node(node_id)
            .ok_or_else(|| ClusterError::new(format!("Node {} not found", node_id)))?;

        let response = node.send("CLUSTER", &["INFO"]).await;
        parse_cluster_info(&response)
    }

    /// Determine the current Raft leader by probing a running node with a
    /// Raft-requiring command. If the node is the leader the command succeeds;
    /// if not, the server returns a REDIRECT with the actual leader's node_id.
    pub async fn get_leader(&self) -> Option<u64> {
        for &node_id in &self.node_order {
            let Some(node) = self.nodes.get(&node_id) else {
                continue;
            };
            if !node.is_running() {
                continue;
            }

            // CLUSTER SETSLOT 0 STABLE is a safe no-op probe: it cancels
            // migration on slot 0, which is virtually never migrating.
            let resp = node.send("CLUSTER", &["SETSLOT", "0", "STABLE"]).await;
            match &resp {
                // This node handled the write → it is the leader.
                Response::Simple(_) => return Some(node_id),
                // Follower forwarded to leader → parse "REDIRECT <id> <addr>".
                Response::Error(e) => {
                    let msg = String::from_utf8_lossy(e);
                    if let Some(rest) = msg.strip_prefix("REDIRECT ")
                        && let Some(id_str) = rest.split_whitespace().next()
                        && let Ok(leader_id) = id_str.parse::<u64>()
                        && self.nodes.contains_key(&leader_id)
                    {
                        return Some(leader_id);
                    }
                    // "CLUSTERDOWN No leader available" → keep trying other nodes.
                }
                _ => {}
            }
        }
        None
    }

    /// Wait for a leader to be elected.
    pub async fn wait_for_leader(&self, timeout_duration: Duration) -> Result<u64, ClusterError> {
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            if let Some(leader) = self.get_leader().await {
                return Ok(leader);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Err(ClusterError::new("Timeout waiting for leader election"))
    }

    /// Wait for cluster convergence (all nodes report same state).
    pub async fn wait_for_cluster_convergence(
        &self,
        timeout_duration: Duration,
    ) -> Result<(), ClusterError> {
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            let mut all_ok = true;
            let mut known_nodes_counts = Vec::new();

            for &node_id in &self.node_order {
                if let Some(node) = self.nodes.get(&node_id) {
                    if !node.is_running() {
                        continue;
                    }

                    match self.get_cluster_info(node_id).await {
                        Ok(info) => {
                            if info.cluster_state != "ok" {
                                all_ok = false;
                                break;
                            }
                            known_nodes_counts.push(info.cluster_known_nodes);
                        }
                        Err(_) => {
                            all_ok = false;
                            break;
                        }
                    }
                }
            }

            // Check if all running nodes agree on the cluster size
            if all_ok && !known_nodes_counts.is_empty() {
                let first = known_nodes_counts[0];
                if known_nodes_counts.iter().all(|&c| c == first) {
                    return Ok(());
                }
            }

            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        Err(ClusterError::new("Timeout waiting for cluster convergence"))
    }

    /// Wait for a specific node to be recognized by all other nodes.
    pub async fn wait_for_node_recognized(
        &self,
        node_id: u64,
        timeout_duration: Duration,
    ) -> Result<(), ClusterError> {
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            let mut recognized = true;

            for &other_id in &self.node_order {
                if other_id == node_id {
                    continue;
                }

                if let Some(node) = self.nodes.get(&other_id) {
                    if !node.is_running() {
                        continue;
                    }

                    // Check if this node can reach the target
                    match self.get_cluster_info(other_id).await {
                        Ok(info) => {
                            // We expect at least as many nodes as we have
                            let expected = self.nodes.values().filter(|n| n.is_running()).count();
                            if info.cluster_known_nodes < expected {
                                recognized = false;
                                break;
                            }
                        }
                        Err(_) => {
                            recognized = false;
                            break;
                        }
                    }
                }
            }

            if recognized {
                return Ok(());
            }

            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        Err(ClusterError::new(format!(
            "Timeout waiting for node {} to be recognized",
            node_id
        )))
    }

    /// Shutdown a specific node (graceful).
    pub async fn shutdown_node(&mut self, node_id: u64) {
        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.shutdown().await;
        }
    }

    /// Kill a specific node (immediate, simulates crash).
    pub async fn kill_node(&mut self, node_id: u64) {
        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.kill().await;
        }
    }

    /// Restart a specific node.
    pub async fn restart_node(&mut self, node_id: u64) -> Result<(), ClusterError> {
        let node = self
            .nodes
            .get_mut(&node_id)
            .ok_or_else(|| ClusterError::new(format!("Node {} not found", node_id)))?;

        node.restart().await
    }
}

impl Default for ClusterTestHarness {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ClusterTestHarness {
    fn drop(&mut self) {
        // Nodes will be dropped and shut down via their Drop impl
    }
}
