//! Cluster test harness for integration testing.
//!
//! Provides types and utilities for testing multi-node cluster operations:
//! - ClusterNodeConfig: Configuration for individual cluster nodes
//! - ClusterTestNode: Wrapper for a single cluster node (delegates to TestServer)
//! - ClusterTestHarness: Orchestrates multi-node cluster testing

#![allow(dead_code)]

use crate::cluster_helpers::{ClusterError, ClusterInfo};
use crate::server::{TestClient, TestServer, TestServerConfig};
use frogdb_core::ClusterState;
use frogdb_core::cluster::ClusterRaft;
use frogdb_protocol::Response;
use frogdb_server::net::tcp_listener_reusable;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

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
    cluster_port: u16,
    data_dir: PathBuf,
    config: ClusterNodeConfig,
    initial_nodes: Vec<String>,
}

/// A single cluster node for testing.
///
/// Wraps a `TestServer` and adds cluster-specific identity and lifecycle.
/// `node_id` and `cluster_port` are constant across restarts.
pub struct ClusterTestNode {
    node_id: u64,
    cluster_port: u16,
    server: Option<TestServer>,
    restart_info: Option<NodeRestartInfo>,
}

impl ClusterTestNode {
    /// Generate a node ID from cluster port (deterministic).
    pub fn generate_node_id_from_port(cluster_port: u16) -> u64 {
        use std::hash::{Hash, Hasher};
        let addr: SocketAddr = format!("127.0.0.1:{}", cluster_port).parse().unwrap();
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        addr.hash(&mut hasher);
        hasher.finish()
    }

    /// Build a `TestServerConfig` from `ClusterNodeConfig` plus cluster-specific fields.
    fn build_test_server_config(
        config: &ClusterNodeConfig,
        node_id: u64,
        data_dir: &Path,
        initial_nodes: Vec<String>,
        cluster_bus_listener: frogdb_server::net::TcpListener,
    ) -> TestServerConfig {
        let cluster_data_dir = data_dir.join("cluster");
        std::fs::create_dir_all(&cluster_data_dir).ok();

        TestServerConfig {
            num_shards: config.num_shards,
            persistence: config.persistence,
            data_dir: Some(data_dir.to_path_buf()),
            log_level: config.log_level.clone(),
            cluster_enabled: true,
            cluster_node_id: Some(node_id),
            cluster_initial_nodes: Some(initial_nodes),
            cluster_data_dir: Some(cluster_data_dir),
            cluster_election_timeout_ms: Some(config.election_timeout_ms),
            cluster_heartbeat_interval_ms: Some(config.heartbeat_interval_ms),
            cluster_connect_timeout_ms: Some(config.connect_timeout_ms),
            cluster_request_timeout_ms: Some(config.request_timeout_ms),
            cluster_bus_listener: Some(cluster_bus_listener),
            ..Default::default()
        }
    }

    /// Start a cluster node with the given configuration.
    pub async fn start(config: ClusterNodeConfig, initial_nodes: Vec<String>) -> Self {
        // Pre-bind cluster bus listener on port 0 to avoid TOCTOU race
        let cluster_bus_listener =
            tcp_listener_reusable("127.0.0.1:0".parse::<SocketAddr>().unwrap())
                .await
                .unwrap();
        let cluster_port = cluster_bus_listener.local_addr().unwrap().port();

        let data_dir = config
            .data_dir
            .clone()
            .unwrap_or_else(TestServer::create_temp_dir);
        let node_id = if config.node_id == 0 {
            Self::generate_node_id_from_port(cluster_port)
        } else {
            config.node_id
        };

        let test_config = Self::build_test_server_config(
            &config,
            node_id,
            &data_dir,
            initial_nodes.clone(),
            cluster_bus_listener,
        );

        use crate::server::ServerRole;
        let server = TestServer::start_with_config(test_config, ServerRole::Standalone).await;

        let restart_info = NodeRestartInfo {
            node_id,
            cluster_port,
            data_dir,
            config,
            initial_nodes,
        };

        Self {
            node_id,
            cluster_port,
            server: Some(server),
            restart_info: Some(restart_info),
        }
    }

    /// Get the node ID.
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// Get the client address.
    pub fn client_addr(&self) -> String {
        self.server
            .as_ref()
            .map(|s| s.addr())
            .unwrap_or_default()
    }

    /// Get the cluster bus address.
    pub fn cluster_addr(&self) -> String {
        format!("127.0.0.1:{}", self.cluster_port)
    }

    /// Get the client port.
    pub fn client_port(&self) -> u16 {
        self.server.as_ref().map(|s| s.port()).unwrap_or(0)
    }

    /// Get the cluster port.
    pub fn cluster_port(&self) -> u16 {
        self.cluster_port
    }

    /// Check if the node is running.
    pub fn is_running(&self) -> bool {
        self.server
            .as_ref()
            .is_some_and(|s| !s.is_finished())
    }

    /// Get the Raft instance, if cluster mode is enabled.
    pub fn raft(&self) -> Option<&Arc<ClusterRaft>> {
        self.server.as_ref().and_then(|s| s.raft())
    }

    /// Get the cluster state, if cluster mode is enabled.
    pub fn cluster_state(&self) -> Option<&Arc<ClusterState>> {
        self.server.as_ref().and_then(|s| s.cluster_state())
    }

    /// Connect to this node and return a TestClient.
    /// Retries on transient errors (e.g. EADDRNOTAVAIL during rapid restarts).
    pub async fn connect(&self) -> TestClient {
        let server = self.server.as_ref().expect("Node not running");
        let mut last_err = None;
        for _ in 0..10 {
            match server.try_connect().await {
                Ok(client) => return client,
                Err(e) => {
                    last_err = Some(e);
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
        panic!(
            "Failed to connect to node on port {} after 10 attempts: {}",
            self.client_port(),
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
        if let Some(mut server) = self.server.take() {
            server.shutdown_mut().await;
            // Wait for the server task to fully terminate
            // (re-take handle timeout like original)
        }
        // Give the server a moment to clean up
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    /// Immediately stop the node (simulate crash).
    pub async fn kill(&mut self) {
        if let Some(mut server) = self.server.take() {
            server.shutdown_mut().await;
        }
    }

    /// Restart the node with the same configuration.
    pub async fn restart(&mut self) -> Result<(), ClusterError> {
        let restart_info = self
            .restart_info
            .clone()
            .ok_or_else(|| ClusterError::new("No restart info available"))?;

        // Make sure the node is stopped
        if self.server.is_some() {
            self.shutdown().await;
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

        let test_config = Self::build_test_server_config(
            &restart_info.config,
            restart_info.node_id,
            &restart_info.data_dir,
            restart_info.initial_nodes.clone(),
            cluster_bus_listener,
        );

        use crate::server::ServerRole;
        let server = TestServer::start_with_config(test_config, ServerRole::Standalone).await;
        self.server = Some(server);

        Ok(())
    }
}

// Drop is a no-op — TestServer::Drop handles shutdown + abort.

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

            let data_dir = TestServer::create_temp_dir();
            config.data_dir = Some(data_dir.clone());

            let test_config = ClusterTestNode::build_test_server_config(
                &config,
                node_id,
                &data_dir,
                initial_nodes.clone(),
                bus_listener,
            );

            use crate::server::ServerRole;
            let server = TestServer::start_with_config(test_config, ServerRole::Standalone).await;

            let restart_info = NodeRestartInfo {
                node_id,
                cluster_port,
                data_dir,
                config,
                initial_nodes: initial_nodes.clone(),
            };

            let node = ClusterTestNode {
                node_id,
                cluster_port,
                server: Some(server),
                restart_info: Some(restart_info),
            };

            self.nodes.insert(node_id, node);
            self.node_order.push(node_id);
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
                match &response {
                    frogdb_protocol::Response::Simple(s) if s.as_ref() == b"OK" => {
                        return Ok(node_id);
                    }
                    frogdb_protocol::Response::Error(e) => {
                        let error_msg = String::from_utf8_lossy(e);
                        if error_msg.starts_with("REDIRECT ") {
                            let parts: Vec<&str> = error_msg.split_whitespace().collect();
                            if parts.len() >= 3 {
                                let leader_addr = parts[2];
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
                                            _ => {}
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

    /// Get cluster info from a specific node via direct state access (no network round-trip).
    pub fn get_cluster_info(&self, node_id: u64) -> Result<ClusterInfo, ClusterError> {
        let node = self
            .node(node_id)
            .ok_or_else(|| ClusterError::new(format!("Node {} not found", node_id)))?;
        let cs = node
            .cluster_state()
            .ok_or_else(|| ClusterError::new("No cluster state"))?;
        let snapshot = cs.snapshot();

        let slots_assigned = snapshot.slot_assignment.len() as u16;
        Ok(ClusterInfo {
            cluster_state: "ok".to_string(),
            cluster_slots_assigned: slots_assigned,
            cluster_slots_ok: slots_assigned,
            cluster_slots_pfail: 0,
            cluster_slots_fail: 0,
            cluster_known_nodes: snapshot.nodes.len(),
            cluster_size: snapshot.nodes.len(),
            cluster_current_epoch: snapshot.config_epoch,
            cluster_my_epoch: snapshot.config_epoch,
        })
    }

    /// Get the cluster node ID string (40-char hex) for a node, matching CLUSTER MYID format.
    pub fn get_node_id_str(&self, node_id: u64) -> Option<String> {
        let node = self.nodes.get(&node_id)?;
        Some(format!("{:040x}", node.node_id()))
    }

    /// Try to determine the current Raft leader by reading metrics from running nodes.
    pub async fn get_leader(&self) -> Option<u64> {
        for &node_id in &self.node_order {
            if let Some(node) = self.nodes.get(&node_id)
                && node.is_running()
                && let Some(raft) = node.raft()
            {
                let leader = raft.metrics().borrow().current_leader;
                if let Some(leader_id) = leader
                    && self.nodes.get(&leader_id).is_some_and(|n| n.is_running())
                {
                    return Some(leader_id);
                }
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

    /// Wait for a new leader to be elected, excluding the given node ID.
    pub async fn wait_for_new_leader(
        &self,
        exclude: u64,
        timeout_duration: Duration,
    ) -> Result<u64, ClusterError> {
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            if let Some(leader) = self.get_leader().await
                && leader != exclude
            {
                return Ok(leader);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Err(ClusterError::new("Timeout waiting for new leader election"))
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

                    match self.get_cluster_info(node_id) {
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

                    match self.get_cluster_info(other_id) {
                        Ok(info) => {
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
