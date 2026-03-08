//! Cluster configuration.

use anyhow::Result;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::ServerConfig;

/// Cluster configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct ClusterConfigSection {
    /// Whether cluster mode is enabled.
    #[serde(default)]
    pub enabled: bool,

    /// This node's unique ID (0 = auto-generate from timestamp).
    #[serde(default)]
    pub node_id: u64,

    /// Address for client connections (host:port).
    /// Defaults to server.bind:server.port if not specified.
    #[serde(default)]
    pub client_addr: String,

    /// Address for cluster bus (Raft) communication.
    /// Typically server port + 10000 (e.g., 16379 for 6379).
    #[serde(default = "default_cluster_bus_addr")]
    pub cluster_bus_addr: String,

    /// Initial cluster nodes to connect to (for joining existing cluster).
    /// Format: ["host1:port1", "host2:port2"]
    #[serde(default)]
    pub initial_nodes: Vec<String>,

    /// Directory for storing cluster state (Raft logs, snapshots).
    #[serde(default = "default_cluster_data_dir")]
    pub data_dir: std::path::PathBuf,

    /// Election timeout in milliseconds.
    /// A leader must receive heartbeats within this time or election starts.
    #[serde(default = "default_election_timeout_ms")]
    pub election_timeout_ms: u64,

    /// Heartbeat interval in milliseconds.
    /// Leader sends heartbeats at this interval.
    #[serde(default = "default_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u64,

    /// Connection timeout for cluster bus in milliseconds.
    #[serde(default = "default_cluster_connect_timeout_ms")]
    pub connect_timeout_ms: u64,

    /// Request timeout for cluster bus RPCs in milliseconds.
    #[serde(default = "default_cluster_request_timeout_ms")]
    pub request_timeout_ms: u64,

    /// Enable automatic failover when a primary fails (default: false).
    /// When enabled, the leader will automatically promote a replica to primary
    /// if the primary becomes unreachable.
    #[serde(default)]
    pub auto_failover: bool,

    /// Number of consecutive failures before marking a node as FAIL (default: 5).
    #[serde(default = "default_fail_threshold")]
    pub fail_threshold: u32,

    /// Reject write commands when this node cannot form a quorum with reachable nodes.
    /// When enabled, writes return CLUSTERDOWN if quorum is lost, preventing
    /// split-brain data divergence. Reads remain available. (default: true)
    #[serde(default = "default_self_fence_on_quorum_loss")]
    pub self_fence_on_quorum_loss: bool,

    /// Priority for replica promotion during auto-failover (default: 100).
    /// Lower values are preferred. 0 means this replica will never be promoted.
    #[serde(default = "default_replica_priority")]
    pub replica_priority: u32,
}

pub const DEFAULT_CLUSTER_BUS_ADDR: &str = "127.0.0.1:16379";
pub const DEFAULT_ELECTION_TIMEOUT_MS: u64 = 1000;
pub const DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 250;
pub const DEFAULT_CLUSTER_CONNECT_TIMEOUT_MS: u64 = 5000;
pub const DEFAULT_CLUSTER_REQUEST_TIMEOUT_MS: u64 = 10000;
pub const DEFAULT_FAIL_THRESHOLD: u32 = 5;
pub const DEFAULT_REPLICA_PRIORITY: u32 = 100;

fn default_self_fence_on_quorum_loss() -> bool {
    true
}

fn default_cluster_bus_addr() -> String {
    DEFAULT_CLUSTER_BUS_ADDR.to_string()
}

fn default_cluster_data_dir() -> std::path::PathBuf {
    std::path::PathBuf::from("./frogdb-cluster")
}

fn default_election_timeout_ms() -> u64 {
    DEFAULT_ELECTION_TIMEOUT_MS
}

fn default_heartbeat_interval_ms() -> u64 {
    DEFAULT_HEARTBEAT_INTERVAL_MS
}

fn default_cluster_connect_timeout_ms() -> u64 {
    DEFAULT_CLUSTER_CONNECT_TIMEOUT_MS
}

fn default_cluster_request_timeout_ms() -> u64 {
    DEFAULT_CLUSTER_REQUEST_TIMEOUT_MS
}

fn default_fail_threshold() -> u32 {
    DEFAULT_FAIL_THRESHOLD
}

fn default_replica_priority() -> u32 {
    DEFAULT_REPLICA_PRIORITY
}

impl Default for ClusterConfigSection {
    fn default() -> Self {
        Self {
            enabled: false,
            node_id: 0,
            client_addr: String::new(),
            cluster_bus_addr: default_cluster_bus_addr(),
            initial_nodes: Vec::new(),
            data_dir: default_cluster_data_dir(),
            election_timeout_ms: default_election_timeout_ms(),
            heartbeat_interval_ms: default_heartbeat_interval_ms(),
            connect_timeout_ms: default_cluster_connect_timeout_ms(),
            request_timeout_ms: default_cluster_request_timeout_ms(),
            auto_failover: false,
            fail_threshold: default_fail_threshold(),
            self_fence_on_quorum_loss: default_self_fence_on_quorum_loss(),
            replica_priority: default_replica_priority(),
        }
    }
}

impl ClusterConfigSection {
    /// Validate the cluster configuration.
    pub fn validate(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        // Validate cluster bus address
        if self.cluster_bus_addr.is_empty() {
            anyhow::bail!(
                "cluster.cluster_bus_addr must be specified when cluster mode is enabled"
            );
        }

        // Parse and validate cluster bus address
        if self
            .cluster_bus_addr
            .parse::<std::net::SocketAddr>()
            .is_err()
        {
            anyhow::bail!(
                "invalid cluster.cluster_bus_addr '{}', expected host:port format",
                self.cluster_bus_addr
            );
        }

        // Validate timeouts
        if self.election_timeout_ms == 0 {
            anyhow::bail!("cluster.election_timeout_ms must be > 0");
        }
        if self.heartbeat_interval_ms == 0 {
            anyhow::bail!("cluster.heartbeat_interval_ms must be > 0");
        }
        if self.heartbeat_interval_ms >= self.election_timeout_ms {
            anyhow::bail!(
                "cluster.heartbeat_interval_ms ({}) must be less than election_timeout_ms ({})",
                self.heartbeat_interval_ms,
                self.election_timeout_ms
            );
        }

        // Validate initial nodes format
        for node in &self.initial_nodes {
            if node.parse::<std::net::SocketAddr>().is_err() {
                anyhow::bail!(
                    "invalid cluster.initial_nodes entry '{}', expected host:port format",
                    node
                );
            }
        }

        Ok(())
    }

    /// Generate a node ID from timestamp if not specified.
    pub fn effective_node_id(&self) -> u64 {
        if self.node_id != 0 {
            self.node_id
        } else {
            // Generate from timestamp + random bits
            use std::time::{SystemTime, UNIX_EPOCH};
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            // Use lower 48 bits of timestamp + 16 random bits
            let random_bits = rand::random::<u16>() as u64;
            (timestamp << 16) | random_bits
        }
    }

    /// Get the effective client address (from config or server config).
    pub fn effective_client_addr(&self, server_config: &ServerConfig) -> std::net::SocketAddr {
        if !self.client_addr.is_empty() {
            self.client_addr.parse().unwrap_or_else(|_| {
                format!("{}:{}", server_config.bind, server_config.port)
                    .parse()
                    .unwrap()
            })
        } else {
            format!("{}:{}", server_config.bind, server_config.port)
                .parse()
                .unwrap()
        }
    }

    /// Get the cluster bus address.
    pub fn cluster_bus_socket_addr(&self) -> std::net::SocketAddr {
        self.cluster_bus_addr.parse().unwrap()
    }

    /// Convert to core ClusterConfig.
    pub fn to_core_config(&self, server_config: &ServerConfig) -> frogdb_core::ClusterConfig {
        frogdb_core::ClusterConfig {
            node_id: self.effective_node_id(),
            addr: self.effective_client_addr(server_config),
            cluster_addr: self.cluster_bus_socket_addr(),
            initial_nodes: self
                .initial_nodes
                .iter()
                .filter_map(|s| s.parse().ok())
                .collect(),
            data_dir: self.data_dir.clone(),
            election_timeout_ms: self.election_timeout_ms,
            heartbeat_interval_ms: self.heartbeat_interval_ms,
        }
    }
}
