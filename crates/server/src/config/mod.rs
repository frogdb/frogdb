//! Configuration handling via Figment.

pub mod validators;

use anyhow::{Context, Result};
use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

/// Main configuration struct.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Server configuration.
    #[serde(default)]
    pub server: ServerConfig,

    /// Logging configuration.
    #[serde(default)]
    pub logging: LoggingConfig,

    /// Persistence configuration.
    #[serde(default)]
    pub persistence: PersistenceConfig,

    /// Snapshot configuration.
    #[serde(default)]
    pub snapshot: SnapshotConfig,

    /// Metrics configuration.
    #[serde(default)]
    pub metrics: MetricsConfig,

    /// Admin port configuration.
    #[serde(default)]
    pub admin: AdminConfig,

    /// Distributed tracing configuration.
    #[serde(default)]
    pub tracing: TracingConfig,

    /// Memory configuration.
    #[serde(default)]
    pub memory: MemoryConfig,

    /// Security configuration.
    #[serde(default)]
    pub security: SecurityConfig,

    /// ACL configuration.
    #[serde(default)]
    pub acl: AclFileConfig,

    /// Blocking commands configuration.
    #[serde(default)]
    pub blocking: BlockingConfig,

    /// VLL (Very Lightweight Locking) configuration.
    #[serde(default)]
    pub vll: VllConfig,

    /// Replication configuration.
    #[serde(default)]
    pub replication: ReplicationConfigSection,

    /// Slow query log configuration.
    #[serde(default)]
    pub slowlog: SlowlogConfig,

    /// JSON configuration.
    #[serde(default)]
    pub json: JsonConfig,

    /// Cluster configuration.
    #[serde(default)]
    pub cluster: ClusterConfigSection,

    /// Status endpoint configuration.
    #[serde(default)]
    pub status: StatusConfig,

    /// Hot shard detection configuration.
    #[serde(default)]
    pub hotshards: HotShardsConfig,

    /// Latency testing configuration.
    #[serde(default)]
    pub latency: LatencyConfig,
}

/// Latency testing configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LatencyConfig {
    /// Run intrinsic latency test at startup before accepting connections.
    #[serde(default)]
    pub startup_test: bool,

    /// Duration of the startup latency test in seconds.
    #[serde(default = "default_latency_test_duration_secs")]
    pub startup_test_duration_secs: u64,

    /// Warning threshold for intrinsic latency in microseconds.
    /// If max latency exceeds this, a warning is logged.
    #[serde(default = "default_latency_warning_threshold_us")]
    pub warning_threshold_us: u64,
}

fn default_latency_test_duration_secs() -> u64 {
    5
}

fn default_latency_warning_threshold_us() -> u64 {
    2000 // 2ms
}

impl Default for LatencyConfig {
    fn default() -> Self {
        Self {
            startup_test: false,
            startup_test_duration_secs: default_latency_test_duration_secs(),
            warning_threshold_us: default_latency_warning_threshold_us(),
        }
    }
}

/// Status endpoint configuration for health thresholds.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StatusConfig {
    /// Threshold percentage for memory warning (0-100).
    #[serde(default = "default_memory_warning_percent")]
    pub memory_warning_percent: u8,

    /// Threshold percentage for connection warning (0-100).
    #[serde(default = "default_connection_warning_percent")]
    pub connection_warning_percent: u8,
}

fn default_memory_warning_percent() -> u8 {
    90
}

fn default_connection_warning_percent() -> u8 {
    90
}

impl Default for StatusConfig {
    fn default() -> Self {
        Self {
            memory_warning_percent: default_memory_warning_percent(),
            connection_warning_percent: default_connection_warning_percent(),
        }
    }
}

impl StatusConfig {
    /// Convert to StatusCollectorConfig.
    pub fn to_collector_config(&self) -> frogdb_metrics::StatusCollectorConfig {
        frogdb_metrics::StatusCollectorConfig {
            memory_warning_percent: self.memory_warning_percent,
            connection_warning_percent: self.connection_warning_percent,
        }
    }
}

/// Hot shard detection configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HotShardsConfig {
    /// Threshold percentage for "HOT" status (default: 20.0).
    #[serde(default = "default_hot_threshold_percent")]
    pub hot_threshold_percent: f64,

    /// Threshold percentage for "WARM" status (default: 15.0).
    #[serde(default = "default_warm_threshold_percent")]
    pub warm_threshold_percent: f64,

    /// Default period for stats collection in seconds (default: 10).
    #[serde(default = "default_hotshards_period_secs")]
    pub default_period_secs: u64,
}

fn default_hot_threshold_percent() -> f64 {
    20.0
}

fn default_warm_threshold_percent() -> f64 {
    15.0
}

fn default_hotshards_period_secs() -> u64 {
    10
}

impl Default for HotShardsConfig {
    fn default() -> Self {
        Self {
            hot_threshold_percent: default_hot_threshold_percent(),
            warm_threshold_percent: default_warm_threshold_percent(),
            default_period_secs: default_hotshards_period_secs(),
        }
    }
}

impl HotShardsConfig {
    /// Convert to HotShardConfig for the metrics crate.
    pub fn to_collector_config(&self) -> frogdb_metrics::HotShardConfig {
        frogdb_metrics::HotShardConfig {
            hot_threshold_percent: self.hot_threshold_percent,
            warm_threshold_percent: self.warm_threshold_percent,
            default_period_secs: self.default_period_secs,
        }
    }
}

/// Security configuration.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SecurityConfig {
    /// Legacy password for the default user (like Redis requirepass).
    /// If set, clients must AUTH with this password before running commands.
    #[serde(default)]
    pub requirepass: String,
}

/// ACL configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AclFileConfig {
    /// Path to the ACL file for SAVE/LOAD operations.
    /// If empty, ACL SAVE/LOAD will return an error.
    #[serde(default)]
    pub aclfile: String,

    /// Maximum number of entries in the ACL LOG.
    #[serde(default = "default_acl_log_max_len")]
    pub log_max_len: usize,
}

impl Default for AclFileConfig {
    fn default() -> Self {
        Self {
            aclfile: String::new(),
            log_max_len: default_acl_log_max_len(),
        }
    }
}

fn default_acl_log_max_len() -> usize {
    128
}

/// Blocking commands configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BlockingConfig {
    /// Maximum waiters per key (0 = unlimited).
    #[serde(default = "default_max_waiters_per_key")]
    pub max_waiters_per_key: usize,

    /// Maximum total blocked connections (0 = unlimited).
    #[serde(default = "default_max_blocked_connections")]
    pub max_blocked_connections: usize,
}

/// VLL (Very Lightweight Locking) configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VllConfig {
    /// Maximum queue depth per shard before rejecting new operations.
    #[serde(default = "default_vll_max_queue_depth")]
    pub max_queue_depth: usize,

    /// Timeout for acquiring locks on all shards (ms).
    #[serde(default = "default_vll_lock_acquisition_timeout_ms")]
    pub lock_acquisition_timeout_ms: u64,

    /// Per-shard lock acquisition timeout (ms).
    #[serde(default = "default_vll_per_shard_lock_timeout_ms")]
    pub per_shard_lock_timeout_ms: u64,

    /// Interval for checking/cleaning up expired operations (ms).
    #[serde(default = "default_vll_timeout_check_interval_ms")]
    pub timeout_check_interval_ms: u64,

    /// Maximum time a continuation lock can be held (ms).
    #[serde(default = "default_vll_max_continuation_lock_ms")]
    pub max_continuation_lock_ms: u64,
}

fn default_vll_max_queue_depth() -> usize {
    10000
}

fn default_vll_lock_acquisition_timeout_ms() -> u64 {
    4000
}

fn default_vll_per_shard_lock_timeout_ms() -> u64 {
    2000
}

fn default_vll_timeout_check_interval_ms() -> u64 {
    100
}

fn default_vll_max_continuation_lock_ms() -> u64 {
    65000
}

impl Default for VllConfig {
    fn default() -> Self {
        Self {
            max_queue_depth: default_vll_max_queue_depth(),
            lock_acquisition_timeout_ms: default_vll_lock_acquisition_timeout_ms(),
            per_shard_lock_timeout_ms: default_vll_per_shard_lock_timeout_ms(),
            timeout_check_interval_ms: default_vll_timeout_check_interval_ms(),
            max_continuation_lock_ms: default_vll_max_continuation_lock_ms(),
        }
    }
}

/// Replication configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicationConfigSection {
    /// Replication role: "standalone", "primary", or "replica".
    /// - standalone: No replication (default)
    /// - primary: Accept replica connections
    /// - replica: Connect to a primary
    #[serde(default = "default_replication_role")]
    pub role: String,

    /// Primary host (for replica role).
    /// When role is "replica", this specifies the primary to connect to.
    #[serde(default)]
    pub primary_host: String,

    /// Primary port (for replica role).
    #[serde(default = "default_primary_port")]
    pub primary_port: u16,

    /// Minimum replicas required to acknowledge writes (for primary role).
    /// If set > 0, writes will wait for this many replicas to acknowledge
    /// before returning success.
    #[serde(default)]
    pub min_replicas_to_write: u32,

    /// Timeout for min_replicas_to_write in milliseconds.
    /// If replicas don't acknowledge within this time, the write still succeeds
    /// but returns with fewer acknowledged replicas.
    #[serde(default = "default_min_replicas_timeout_ms")]
    pub min_replicas_timeout_ms: u64,

    /// ACK interval - how often replicas send ACKs to primary (milliseconds).
    #[serde(default = "default_ack_interval_ms")]
    pub ack_interval_ms: u64,

    /// Full sync timeout (seconds).
    /// Maximum time to wait for a full sync operation.
    #[serde(default = "default_fullsync_timeout_secs")]
    pub fullsync_timeout_secs: u64,

    /// Maximum memory for full sync buffering (MB).
    /// If exceeded, FULLRESYNC requests will be rejected.
    #[serde(default = "default_fullsync_max_memory_mb")]
    pub fullsync_max_memory_mb: usize,

    /// Replication state file path.
    /// Stores replication ID and offset for partial sync recovery.
    #[serde(default = "default_replication_state_file")]
    pub state_file: String,

    /// Connection timeout for replica connecting to primary (milliseconds).
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,

    /// Handshake timeout during replication setup (milliseconds).
    #[serde(default = "default_handshake_timeout_ms")]
    pub handshake_timeout_ms: u64,

    /// Reconnection backoff - initial delay (milliseconds).
    #[serde(default = "default_reconnect_backoff_initial_ms")]
    pub reconnect_backoff_initial_ms: u64,

    /// Reconnection backoff - maximum delay (milliseconds).
    #[serde(default = "default_reconnect_backoff_max_ms")]
    pub reconnect_backoff_max_ms: u64,
}

fn default_replication_role() -> String {
    "standalone".to_string()
}

fn default_primary_port() -> u16 {
    6379
}

fn default_min_replicas_timeout_ms() -> u64 {
    5000
}

fn default_ack_interval_ms() -> u64 {
    1000
}

fn default_fullsync_timeout_secs() -> u64 {
    300
}

fn default_fullsync_max_memory_mb() -> usize {
    512
}

fn default_replication_state_file() -> String {
    "replication_state.json".to_string()
}

fn default_connect_timeout_ms() -> u64 {
    5000
}

fn default_handshake_timeout_ms() -> u64 {
    10000
}

fn default_reconnect_backoff_initial_ms() -> u64 {
    100
}

fn default_reconnect_backoff_max_ms() -> u64 {
    30000
}

impl Default for ReplicationConfigSection {
    fn default() -> Self {
        Self {
            role: default_replication_role(),
            primary_host: String::new(),
            primary_port: default_primary_port(),
            min_replicas_to_write: 0,
            min_replicas_timeout_ms: default_min_replicas_timeout_ms(),
            ack_interval_ms: default_ack_interval_ms(),
            fullsync_timeout_secs: default_fullsync_timeout_secs(),
            fullsync_max_memory_mb: default_fullsync_max_memory_mb(),
            state_file: default_replication_state_file(),
            connect_timeout_ms: default_connect_timeout_ms(),
            handshake_timeout_ms: default_handshake_timeout_ms(),
            reconnect_backoff_initial_ms: default_reconnect_backoff_initial_ms(),
            reconnect_backoff_max_ms: default_reconnect_backoff_max_ms(),
        }
    }
}

impl ReplicationConfigSection {
    /// Validate the replication configuration.
    pub fn validate(&self) -> anyhow::Result<()> {
        let valid_roles = ["standalone", "primary", "replica"];
        if !valid_roles.contains(&self.role.to_lowercase().as_str()) {
            anyhow::bail!(
                "invalid replication role '{}', expected one of: {}",
                self.role,
                valid_roles.join(", ")
            );
        }

        // If replica, primary_host must be specified
        if self.role.to_lowercase() == "replica" && self.primary_host.is_empty() {
            anyhow::bail!("primary_host must be specified when role is 'replica'");
        }

        // Validate timeouts
        if self.fullsync_timeout_secs == 0 {
            anyhow::bail!("fullsync_timeout_secs must be > 0");
        }

        if self.fullsync_max_memory_mb == 0 {
            anyhow::bail!("fullsync_max_memory_mb must be > 0");
        }

        if self.ack_interval_ms == 0 {
            anyhow::bail!("ack_interval_ms must be > 0");
        }

        Ok(())
    }

    /// Check if this node is a primary.
    pub fn is_primary(&self) -> bool {
        self.role.to_lowercase() == "primary"
    }

    /// Check if this node is a replica.
    pub fn is_replica(&self) -> bool {
        self.role.to_lowercase() == "replica"
    }

    /// Check if this node is standalone.
    pub fn is_standalone(&self) -> bool {
        self.role.to_lowercase() == "standalone"
    }

    /// Convert to core ReplicationConfig.
    pub fn to_core_config(&self) -> frogdb_core::ReplicationConfig {
        match self.role.to_lowercase().as_str() {
            "primary" => frogdb_core::ReplicationConfig::Primary {
                min_replicas_to_write: self.min_replicas_to_write,
            },
            "replica" => frogdb_core::ReplicationConfig::Replica {
                primary_addr: format!("{}:{}", self.primary_host, self.primary_port),
            },
            _ => frogdb_core::ReplicationConfig::Standalone,
        }
    }
}

fn default_max_waiters_per_key() -> usize {
    10000
}

fn default_max_blocked_connections() -> usize {
    50000
}

impl Default for BlockingConfig {
    fn default() -> Self {
        Self {
            max_waiters_per_key: default_max_waiters_per_key(),
            max_blocked_connections: default_max_blocked_connections(),
        }
    }
}

/// Slow query log configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SlowlogConfig {
    /// Threshold in microseconds. Commands slower than this are logged.
    /// Set to 0 to log all commands, -1 to disable logging.
    #[serde(default = "default_slowlog_log_slower_than")]
    pub log_slower_than: i64,

    /// Maximum number of entries per shard.
    #[serde(default = "default_slowlog_max_len")]
    pub max_len: usize,

    /// Maximum characters per argument before truncation.
    #[serde(default = "default_slowlog_max_arg_len")]
    pub max_arg_len: usize,
}

fn default_slowlog_log_slower_than() -> i64 {
    frogdb_core::DEFAULT_SLOWLOG_LOG_SLOWER_THAN
}

fn default_slowlog_max_len() -> usize {
    frogdb_core::DEFAULT_SLOWLOG_MAX_LEN
}

fn default_slowlog_max_arg_len() -> usize {
    frogdb_core::DEFAULT_SLOWLOG_MAX_ARG_LEN
}

impl Default for SlowlogConfig {
    fn default() -> Self {
        Self {
            log_slower_than: default_slowlog_log_slower_than(),
            max_len: default_slowlog_max_len(),
            max_arg_len: default_slowlog_max_arg_len(),
        }
    }
}

/// JSON configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JsonConfig {
    /// Maximum nesting depth for JSON documents.
    #[serde(default = "default_json_max_depth")]
    pub max_depth: usize,

    /// Maximum size in bytes for JSON documents.
    #[serde(default = "default_json_max_size")]
    pub max_size: usize,
}

fn default_json_max_depth() -> usize {
    frogdb_core::DEFAULT_JSON_MAX_DEPTH
}

fn default_json_max_size() -> usize {
    frogdb_core::DEFAULT_JSON_MAX_SIZE
}

impl Default for JsonConfig {
    fn default() -> Self {
        Self {
            max_depth: default_json_max_depth(),
            max_size: default_json_max_size(),
        }
    }
}

impl JsonConfig {
    /// Convert to JsonLimits.
    pub fn to_limits(&self) -> frogdb_core::JsonLimits {
        frogdb_core::JsonLimits {
            max_depth: self.max_depth,
            max_size: self.max_size,
        }
    }
}

/// Cluster configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
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
}

fn default_cluster_bus_addr() -> String {
    "127.0.0.1:16379".to_string()
}

fn default_cluster_data_dir() -> std::path::PathBuf {
    std::path::PathBuf::from("./frogdb-cluster")
}

fn default_election_timeout_ms() -> u64 {
    1000
}

fn default_heartbeat_interval_ms() -> u64 {
    250
}

fn default_cluster_connect_timeout_ms() -> u64 {
    5000
}

fn default_cluster_request_timeout_ms() -> u64 {
    10000
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
        }
    }
}

impl ClusterConfigSection {
    /// Validate the cluster configuration.
    pub fn validate(&self) -> anyhow::Result<()> {
        if !self.enabled {
            return Ok(());
        }

        // Validate cluster bus address
        if self.cluster_bus_addr.is_empty() {
            anyhow::bail!("cluster.cluster_bus_addr must be specified when cluster mode is enabled");
        }

        // Parse and validate cluster bus address
        if self.cluster_bus_addr.parse::<std::net::SocketAddr>().is_err() {
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

/// Admin API configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AdminConfig {
    /// Whether the admin API is enabled.
    #[serde(default)]
    pub enabled: bool,

    /// Port for the admin HTTP server.
    #[serde(default = "default_admin_port")]
    pub port: u16,

    /// Bind address for the admin HTTP server.
    #[serde(default = "default_admin_bind")]
    pub bind: String,
}

fn default_admin_port() -> u16 {
    6380
}

fn default_admin_bind() -> String {
    "127.0.0.1".to_string()
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            port: default_admin_port(),
            bind: default_admin_bind(),
        }
    }
}

impl AdminConfig {
    /// Get the full bind address.
    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.bind, self.port)
    }

    /// Validate the admin configuration.
    pub fn validate(&self) -> anyhow::Result<()> {
        if !self.enabled {
            return Ok(());
        }

        if self.port == 0 {
            anyhow::bail!("admin.port cannot be 0");
        }

        Ok(())
    }
}

/// Chaos testing configuration for latency and failure injection.
/// Only available when compiled with `turmoil` feature.
#[cfg(feature = "turmoil")]
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChaosConfig {
    /// Delay (ms) between scatter sends to different shards.
    /// Useful for testing interleaving of concurrent operations.
    #[serde(default)]
    pub scatter_inter_send_delay_ms: u64,

    /// Per-shard latency overrides (shard_id -> delay_ms).
    /// Applied before sending to each shard.
    #[serde(default)]
    pub shard_delays_ms: std::collections::HashMap<usize, u64>,

    /// Random jitter range (0 to jitter_ms).
    /// Added on top of other delays for more realistic simulation.
    #[serde(default)]
    pub jitter_ms: u64,

    /// Delay (ms) before single-shard command execution.
    #[serde(default)]
    pub single_shard_delay_ms: u64,

    /// Delay (ms) before transaction EXEC processing.
    #[serde(default)]
    pub transaction_delay_ms: u64,

    // === Failure Injection Fields ===

    /// Shard IDs that simulate being unavailable (requests timeout).
    /// When a shard is in this set, scatter-gather operations to it will fail.
    #[serde(default)]
    pub unavailable_shards: std::collections::HashSet<usize>,

    /// Probability (0.0-1.0) of simulating connection reset during operations.
    /// Applied per-operation to simulate network instability.
    #[serde(default)]
    pub connection_reset_probability: f64,

    /// Shard IDs that return errors instead of successful responses.
    /// Maps shard_id -> error message to return.
    #[serde(default)]
    pub error_shards: std::collections::HashMap<usize, String>,
}

#[cfg(feature = "turmoil")]
impl ChaosConfig {
    /// Get jitter delay (0 to jitter_ms, random).
    pub fn get_jitter(&self) -> std::time::Duration {
        if self.jitter_ms == 0 {
            std::time::Duration::ZERO
        } else {
            use rand::Rng;
            let jitter = rand::thread_rng().gen_range(0..=self.jitter_ms);
            std::time::Duration::from_millis(jitter)
        }
    }

    /// Apply configured delay with optional jitter.
    pub async fn apply_delay(&self, base_ms: u64) {
        if base_ms > 0 || self.jitter_ms > 0 {
            let total = std::time::Duration::from_millis(base_ms) + self.get_jitter();
            if !total.is_zero() {
                tokio::time::sleep(total).await;
            }
        }
    }

    /// Check if a shard is configured as unavailable.
    pub fn is_shard_unavailable(&self, shard_id: usize) -> bool {
        self.unavailable_shards.contains(&shard_id)
    }

    /// Get the error message for a shard if it's configured to return errors.
    /// Returns None if the shard should operate normally.
    pub fn get_shard_error(&self, shard_id: usize) -> Option<&str> {
        self.error_shards.get(&shard_id).map(|s| s.as_str())
    }

    /// Check if a connection reset should be simulated based on probability.
    /// Returns true if the operation should be aborted with a connection error.
    pub fn should_simulate_connection_reset(&self) -> bool {
        if self.connection_reset_probability <= 0.0 {
            return false;
        }
        if self.connection_reset_probability >= 1.0 {
            return true;
        }
        use rand::Rng;
        rand::thread_rng().gen::<f64>() < self.connection_reset_probability
    }

    /// Check if any failure injection is configured.
    pub fn has_failure_injection(&self) -> bool {
        !self.unavailable_shards.is_empty()
            || !self.error_shards.is_empty()
            || self.connection_reset_probability > 0.0
    }
}


/// Metrics configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsConfig {
    /// Whether metrics are enabled.
    #[serde(default = "default_metrics_enabled")]
    pub enabled: bool,

    /// Bind address for the metrics HTTP server.
    #[serde(default = "default_metrics_bind")]
    pub bind: String,

    /// Port for the metrics HTTP server.
    #[serde(default = "default_metrics_port")]
    pub port: u16,

    /// Whether OTLP export is enabled.
    #[serde(default)]
    pub otlp_enabled: bool,

    /// OTLP endpoint URL.
    #[serde(default = "default_otlp_endpoint")]
    pub otlp_endpoint: String,

    /// OTLP push interval in seconds.
    #[serde(default = "default_otlp_interval_secs")]
    pub otlp_interval_secs: u64,
}

/// Distributed tracing configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TracingConfig {
    /// Whether distributed tracing is enabled.
    #[serde(default)]
    pub enabled: bool,

    /// OTLP endpoint for trace export.
    #[serde(default = "default_tracing_endpoint")]
    pub otlp_endpoint: String,

    /// Sampling rate (0.0 to 1.0). 1.0 = sample all, 0.1 = sample 10%.
    #[serde(default = "default_sampling_rate")]
    pub sampling_rate: f64,

    /// Service name in traces.
    #[serde(default = "default_service_name")]
    pub service_name: String,

    /// Enable scatter-gather operation spans (child spans per shard for MGET/MSET).
    #[serde(default)]
    pub scatter_gather_spans: bool,

    /// Enable shard execution spans (spans inside shard workers).
    #[serde(default)]
    pub shard_spans: bool,

    /// Enable persistence spans (WAL writes, snapshots).
    #[serde(default)]
    pub persistence_spans: bool,
}

/// Server-specific configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// Bind address.
    #[serde(default = "default_bind")]
    pub bind: String,

    /// Listen port.
    #[serde(default = "default_port")]
    pub port: u16,

    /// Number of shards (0 = auto-detect CPU cores).
    #[serde(default = "default_num_shards")]
    pub num_shards: usize,

    /// Allow cross-slot operations in standalone mode.
    /// When enabled, multi-key commands like MGET/MSET can operate across different
    /// hash slots using scatter-gather. MSETNX always requires same-slot.
    #[serde(default = "default_allow_cross_slot_standalone")]
    pub allow_cross_slot_standalone: bool,

    /// Timeout for scatter-gather operations in milliseconds.
    #[serde(default = "default_scatter_gather_timeout_ms")]
    pub scatter_gather_timeout_ms: u64,
}

/// Logging configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LoggingConfig {
    /// Log level (trace, debug, info, warn, error).
    #[serde(default = "default_log_level")]
    pub level: String,

    /// Log format (pretty, json).
    #[serde(default = "default_log_format")]
    pub format: String,
}

/// Persistence configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PersistenceConfig {
    /// Whether persistence is enabled.
    #[serde(default = "default_persistence_enabled")]
    pub enabled: bool,

    /// Directory for data files.
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,

    /// Durability mode: "async", "periodic", or "sync".
    #[serde(default = "default_durability_mode")]
    pub durability_mode: String,

    /// Sync interval in milliseconds (for periodic mode).
    #[serde(default = "default_sync_interval_ms")]
    pub sync_interval_ms: u64,

    /// RocksDB write buffer size in MB.
    #[serde(default = "default_write_buffer_size_mb")]
    pub write_buffer_size_mb: usize,

    /// Compression type: "none", "snappy", "lz4", "zstd".
    #[serde(default = "default_compression")]
    pub compression: String,

    /// Batch size threshold in KB before flushing.
    #[serde(default = "default_batch_size_threshold_kb")]
    pub batch_size_threshold_kb: usize,

    /// Batch timeout in milliseconds before flushing.
    #[serde(default = "default_batch_timeout_ms")]
    pub batch_timeout_ms: u64,
}

/// Snapshot configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotConfig {
    /// Directory for storing snapshots.
    #[serde(default = "default_snapshot_dir")]
    pub snapshot_dir: PathBuf,

    /// Interval between automatic snapshots in seconds (0 = disabled).
    #[serde(default = "default_snapshot_interval_secs")]
    pub snapshot_interval_secs: u64,

    /// Maximum number of snapshots to retain (0 = unlimited).
    #[serde(default = "default_max_snapshots")]
    pub max_snapshots: usize,
}

fn default_snapshot_dir() -> PathBuf {
    PathBuf::from("./snapshots")
}

fn default_snapshot_interval_secs() -> u64 {
    3600 // 1 hour
}

fn default_max_snapshots() -> usize {
    5
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            snapshot_dir: default_snapshot_dir(),
            snapshot_interval_secs: default_snapshot_interval_secs(),
            max_snapshots: default_max_snapshots(),
        }
    }
}

impl SnapshotConfig {
    /// Convert to the core SnapshotConfig type.
    pub fn to_core_config(&self) -> frogdb_core::persistence::SnapshotConfig {
        frogdb_core::persistence::SnapshotConfig {
            snapshot_dir: self.snapshot_dir.clone(),
            snapshot_interval_secs: self.snapshot_interval_secs,
            max_snapshots: self.max_snapshots,
        }
    }
}

/// Memory management configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryConfig {
    /// Maximum memory limit in bytes. 0 means unlimited.
    /// Can use human-readable formats like "100mb", "1gb" in config files.
    #[serde(default = "default_maxmemory")]
    pub maxmemory: u64,

    /// Eviction policy when maxmemory is reached.
    /// Options: noeviction, volatile-lru, allkeys-lru, volatile-lfu, allkeys-lfu,
    ///          volatile-random, allkeys-random, volatile-ttl
    #[serde(default = "default_maxmemory_policy")]
    pub maxmemory_policy: String,

    /// Number of keys to sample when looking for eviction candidates.
    #[serde(default = "default_maxmemory_samples")]
    pub maxmemory_samples: usize,

    /// LFU log factor - higher values make counter increment less likely.
    #[serde(default = "default_lfu_log_factor")]
    pub lfu_log_factor: u8,

    /// LFU decay time in minutes - counter decays by 1 every N minutes.
    #[serde(default = "default_lfu_decay_time")]
    pub lfu_decay_time: u64,

    /// Threshold in bytes for MEMORY DOCTOR big key detection.
    /// Keys larger than this will be flagged. Default: 1MB (1048576 bytes).
    #[serde(default = "default_doctor_big_key_threshold")]
    pub doctor_big_key_threshold: u64,

    /// Maximum number of big keys to report per shard in MEMORY DOCTOR.
    /// Default: 100.
    #[serde(default = "default_doctor_max_big_keys")]
    pub doctor_max_big_keys: usize,

    /// Threshold for shard memory imbalance detection (coefficient of variation).
    /// Shards with memory CV higher than this will trigger a warning. Default: 25%.
    #[serde(default = "default_doctor_imbalance_threshold")]
    pub doctor_imbalance_threshold: f64,
}

fn default_bind() -> String {
    "127.0.0.1".to_string()
}

fn default_port() -> u16 {
    6379
}

fn default_num_shards() -> usize {
    1 // Start with 1 shard as per the plan
}

fn default_allow_cross_slot_standalone() -> bool {
    false
}

fn default_scatter_gather_timeout_ms() -> u64 {
    5000
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_log_format() -> String {
    "pretty".to_string()
}

fn default_persistence_enabled() -> bool {
    true
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("./frogdb-data")
}

fn default_durability_mode() -> String {
    "periodic".to_string()
}

fn default_sync_interval_ms() -> u64 {
    1000
}

fn default_write_buffer_size_mb() -> usize {
    64
}

fn default_compression() -> String {
    "lz4".to_string()
}

fn default_batch_size_threshold_kb() -> usize {
    4096 // 4MB
}

fn default_batch_timeout_ms() -> u64 {
    10
}

fn default_metrics_enabled() -> bool {
    true
}

fn default_metrics_bind() -> String {
    "0.0.0.0".to_string()
}

fn default_metrics_port() -> u16 {
    9090
}

fn default_otlp_endpoint() -> String {
    "http://localhost:4317".to_string()
}

fn default_otlp_interval_secs() -> u64 {
    15
}

fn default_maxmemory() -> u64 {
    0 // Unlimited
}

fn default_maxmemory_policy() -> String {
    "noeviction".to_string()
}

fn default_maxmemory_samples() -> usize {
    5
}

fn default_lfu_log_factor() -> u8 {
    10
}

fn default_lfu_decay_time() -> u64 {
    1
}

fn default_doctor_big_key_threshold() -> u64 {
    1_048_576 // 1MB
}

fn default_doctor_max_big_keys() -> usize {
    100
}

fn default_doctor_imbalance_threshold() -> f64 {
    25.0 // 25% coefficient of variation
}

fn default_tracing_endpoint() -> String {
    "http://localhost:4317".to_string()
}

fn default_sampling_rate() -> f64 {
    1.0
}

fn default_service_name() -> String {
    "frogdb".to_string()
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            port: default_port(),
            num_shards: default_num_shards(),
            allow_cross_slot_standalone: default_allow_cross_slot_standalone(),
            scatter_gather_timeout_ms: default_scatter_gather_timeout_ms(),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
        }
    }
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            enabled: default_persistence_enabled(),
            data_dir: default_data_dir(),
            durability_mode: default_durability_mode(),
            sync_interval_ms: default_sync_interval_ms(),
            write_buffer_size_mb: default_write_buffer_size_mb(),
            compression: default_compression(),
            batch_size_threshold_kb: default_batch_size_threshold_kb(),
            batch_timeout_ms: default_batch_timeout_ms(),
        }
    }
}

impl PersistenceConfig {
    /// Validate the persistence configuration.
    pub fn validate(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let valid_compressions = ["none", "snappy", "lz4", "zstd"];
        if !valid_compressions.contains(&self.compression.to_lowercase().as_str()) {
            anyhow::bail!(
                "invalid compression type '{}', expected one of: {}",
                self.compression,
                valid_compressions.join(", ")
            );
        }

        let valid_modes = ["async", "periodic", "sync"];
        if !valid_modes.contains(&self.durability_mode.to_lowercase().as_str()) {
            anyhow::bail!(
                "invalid durability_mode '{}', expected one of: {}",
                self.durability_mode,
                valid_modes.join(", ")
            );
        }

        Ok(())
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: default_metrics_enabled(),
            bind: default_metrics_bind(),
            port: default_metrics_port(),
            otlp_enabled: false,
            otlp_endpoint: default_otlp_endpoint(),
            otlp_interval_secs: default_otlp_interval_secs(),
        }
    }
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            otlp_endpoint: default_tracing_endpoint(),
            sampling_rate: default_sampling_rate(),
            service_name: default_service_name(),
            scatter_gather_spans: false,
            shard_spans: false,
            persistence_spans: false,
        }
    }
}

impl TracingConfig {
    /// Validate the tracing configuration.
    pub fn validate(&self) -> Result<()> {
        if self.enabled && self.otlp_endpoint.is_empty() {
            anyhow::bail!("OTLP endpoint must be specified when tracing is enabled");
        }

        if self.sampling_rate < 0.0 || self.sampling_rate > 1.0 {
            anyhow::bail!("sampling_rate must be between 0.0 and 1.0");
        }

        Ok(())
    }

    /// Convert to frogdb_metrics::TracingConfig.
    pub fn to_metrics_config(&self) -> frogdb_metrics::TracingConfig {
        frogdb_metrics::TracingConfig {
            enabled: self.enabled,
            otlp_endpoint: self.otlp_endpoint.clone(),
            sampling_rate: self.sampling_rate,
            service_name: self.service_name.clone(),
            scatter_gather_spans: self.scatter_gather_spans,
            shard_spans: self.shard_spans,
            persistence_spans: self.persistence_spans,
        }
    }
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            maxmemory: default_maxmemory(),
            maxmemory_policy: default_maxmemory_policy(),
            maxmemory_samples: default_maxmemory_samples(),
            lfu_log_factor: default_lfu_log_factor(),
            lfu_decay_time: default_lfu_decay_time(),
            doctor_big_key_threshold: default_doctor_big_key_threshold(),
            doctor_max_big_keys: default_doctor_max_big_keys(),
            doctor_imbalance_threshold: default_doctor_imbalance_threshold(),
        }
    }
}

impl MemoryConfig {
    /// Validate the memory configuration.
    pub fn validate(&self) -> Result<()> {
        // Validate policy string
        let valid_policies = [
            "noeviction",
            "volatile-lru",
            "allkeys-lru",
            "volatile-lfu",
            "allkeys-lfu",
            "volatile-random",
            "allkeys-random",
            "volatile-ttl",
        ];

        if !valid_policies.contains(&self.maxmemory_policy.to_lowercase().as_str()) {
            anyhow::bail!(
                "invalid maxmemory_policy '{}', expected one of: {}",
                self.maxmemory_policy,
                valid_policies.join(", ")
            );
        }

        // Validate samples
        if self.maxmemory_samples == 0 {
            anyhow::bail!("maxmemory_samples must be > 0");
        }

        Ok(())
    }

    /// Check if memory limit is enabled.
    pub fn has_limit(&self) -> bool {
        self.maxmemory > 0
    }

    /// Convert to MemoryDiagConfig for the metrics crate.
    pub fn to_diag_config(&self) -> frogdb_metrics::MemoryDiagConfig {
        frogdb_metrics::MemoryDiagConfig {
            big_key_threshold_bytes: self.doctor_big_key_threshold as usize,
            max_big_keys_per_shard: self.doctor_max_big_keys,
            imbalance_threshold_percent: self.doctor_imbalance_threshold,
        }
    }
}

impl MetricsConfig {
    /// Get the full bind address.
    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.bind, self.port)
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<()> {
        if self.port == 0 {
            anyhow::bail!("metrics port cannot be 0");
        }

        if self.otlp_enabled && self.otlp_endpoint.is_empty() {
            anyhow::bail!("OTLP endpoint must be specified when OTLP is enabled");
        }

        if self.otlp_interval_secs == 0 {
            anyhow::bail!("OTLP interval must be > 0");
        }

        Ok(())
    }
}

/// Validate a bind address (IP address or hostname).
fn validate_bind_address(addr: &str, field_name: &str) -> Result<()> {
    use std::net::IpAddr;

    if addr.parse::<IpAddr>().is_ok() {
        return Ok(());
    }

    // Validate as hostname
    if addr.is_empty() {
        anyhow::bail!("{}: bind address cannot be empty", field_name);
    }
    if addr.len() > 253 {
        anyhow::bail!("{}: hostname too long (max 253 chars)", field_name);
    }
    for label in addr.split('.') {
        if label.is_empty() || label.len() > 63 {
            anyhow::bail!(
                "{}: invalid hostname '{}' - labels must be 1-63 chars",
                field_name,
                addr
            );
        }
        if label.starts_with('-') || label.ends_with('-') {
            anyhow::bail!(
                "{}: invalid hostname '{}' - labels cannot start or end with hyphen",
                field_name,
                addr
            );
        }
        if !label.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
            anyhow::bail!(
                "{}: invalid hostname '{}' - contains invalid characters",
                field_name,
                addr
            );
        }
    }
    Ok(())
}

/// Validate that a path's parent directory exists and is writable.
fn validate_path_parent(path: &Path, field_name: &str) -> Result<()> {
    let parent = path.parent().unwrap_or(Path::new("."));

    if !parent.exists() {
        anyhow::bail!(
            "{}: parent directory '{}' does not exist",
            field_name,
            parent.display()
        );
    }
    if !parent.is_dir() {
        anyhow::bail!(
            "{}: parent path '{}' is not a directory",
            field_name,
            parent.display()
        );
    }

    // Check writability by creating temp file
    let test_file = parent.join(format!(".frogdb_write_test_{}", std::process::id()));
    match std::fs::File::create(&test_file) {
        Ok(_) => {
            let _ = std::fs::remove_file(&test_file);
            Ok(())
        }
        Err(e) => anyhow::bail!(
            "{}: parent directory '{}' is not writable: {}",
            field_name,
            parent.display(),
            e
        ),
    }
}

impl Config {
    /// Load configuration from multiple sources.
    ///
    /// Priority (highest to lowest):
    /// 1. CLI arguments
    /// 2. Environment variables (FROGDB_ prefix)
    /// 3. TOML config file
    /// 4. Built-in defaults
    pub fn load(
        config_path: Option<&Path>,
        bind: Option<String>,
        port: Option<u16>,
        shards: Option<String>,
        log_level: Option<String>,
        log_format: Option<String>,
        admin_bind: Option<String>,
        admin_port: Option<u16>,
    ) -> Result<Self> {
        let mut figment = Figment::new()
            .merge(Serialized::defaults(Config::default()));

        // Merge config file if provided
        if let Some(path) = config_path {
            if !path.exists() {
                anyhow::bail!("config file not found: {}", path.display());
            }
            figment = figment.merge(Toml::file(path));
        } else {
            // Try default config file
            let default_path = Path::new("frogdb.toml");
            if default_path.exists() {
                figment = figment.merge(Toml::file(default_path).nested());
            } else {
                tracing::warn!("Default config file 'frogdb.toml' not found, using defaults");
            }
        }

        // Merge environment variables
        figment = figment.merge(Env::prefixed("FROGDB_").split("__"));

        // Build CLI overrides
        let mut cli_overrides = Config::default();

        if let Some(ref bind) = bind {
            cli_overrides.server.bind = bind.clone();
        }

        if let Some(port) = port {
            cli_overrides.server.port = port;
        }

        if let Some(ref shards) = shards {
            cli_overrides.server.num_shards = if shards == "auto" {
                std::thread::available_parallelism()
                    .map(|p| p.get())
                    .unwrap_or(1)
            } else {
                shards.parse().context("Invalid shard count")?
            };
        }

        if let Some(ref level) = log_level {
            cli_overrides.logging.level = level.clone();
        }

        if let Some(ref format) = log_format {
            cli_overrides.logging.format = format.clone();
        }

        // Merge CLI overrides (only non-default values)
        // For simplicity, we'll re-merge specific values
        let mut config: Config = figment.extract().context("Failed to load configuration")?;

        // Apply CLI overrides explicitly
        if bind.is_some() {
            config.server.bind = cli_overrides.server.bind;
        }
        if port.is_some() {
            config.server.port = cli_overrides.server.port;
        }
        if shards.is_some() {
            config.server.num_shards = cli_overrides.server.num_shards;
        }
        if log_level.is_some() {
            config.logging.level = cli_overrides.logging.level;
        }
        if log_format.is_some() {
            config.logging.format = cli_overrides.logging.format;
        }

        // Apply admin CLI overrides
        // --admin-port implies admin.enabled=true
        if admin_port.is_some() {
            config.admin.enabled = true;
            config.admin.port = admin_port.unwrap();
        }
        if let Some(ref bind) = admin_bind {
            config.admin.bind = bind.clone();
        }

        // Validate
        config.validate()?;

        Ok(config)
    }

    /// Validate configuration values.
    fn validate(&self) -> Result<()> {
        // Validate port
        if self.server.port == 0 {
            anyhow::bail!("Port cannot be 0");
        }

        // Validate log level
        let valid_levels = ["trace", "debug", "info", "warn", "error"];
        if !valid_levels.contains(&self.logging.level.to_lowercase().as_str()) {
            anyhow::bail!(
                "Invalid log level '{}', expected one of: {:?}",
                self.logging.level,
                valid_levels
            );
        }

        // Validate log format
        let valid_formats = ["pretty", "json"];
        if !valid_formats.contains(&self.logging.format.to_lowercase().as_str()) {
            anyhow::bail!(
                "Invalid log format '{}', expected one of: {:?}",
                self.logging.format,
                valid_formats
            );
        }

        // Validate metrics config
        self.metrics.validate()?;

        // Validate memory config
        self.memory.validate()?;

        // Validate replication config
        self.replication.validate()?;

        // Validate tracing config
        self.tracing.validate()?;

        // Validate persistence config
        self.persistence.validate()?;

        // Validate cluster config
        self.cluster.validate()?;

        // Validate admin config
        self.admin.validate()?;

        // Validate bind addresses
        validate_bind_address(&self.server.bind, "server.bind")?;
        if self.metrics.enabled {
            validate_bind_address(&self.metrics.bind, "metrics.bind")?;
        }
        if self.admin.enabled {
            validate_bind_address(&self.admin.bind, "admin.bind")?;
        }

        // Validate paths (only if features are enabled)
        if self.persistence.enabled {
            validate_path_parent(&self.persistence.data_dir, "persistence.data_dir")?;
        }
        if self.snapshot.snapshot_interval_secs > 0 {
            validate_path_parent(&self.snapshot.snapshot_dir, "snapshot.snapshot_dir")?;
        }
        if !self.acl.aclfile.is_empty() {
            validate_path_parent(Path::new(&self.acl.aclfile), "acl.aclfile")?;
        }

        // Run cross-field validators
        let report = validators::run_all_validators(self);
        report.log_non_errors();
        report.into_result()?;

        Ok(())
    }

    /// Initialize logging based on configuration.
    pub fn init_logging(&self) -> Result<()> {
        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new(&self.logging.level));

        match self.logging.format.to_lowercase().as_str() {
            "json" => {
                tracing_subscriber::registry()
                    .with(filter)
                    .with(fmt::layer().json())
                    .init();
            }
            _ => {
                tracing_subscriber::registry()
                    .with(filter)
                    .with(fmt::layer())
                    .init();
            }
        }

        Ok(())
    }

    /// Generate default TOML configuration.
    pub fn default_toml() -> String {
        r#"# FrogDB Configuration File

[server]
# Bind address
bind = "127.0.0.1"

# Listen port
port = 6379

# Number of shards (0 = auto-detect CPU cores)
num_shards = 1

# Allow cross-slot operations in standalone mode.
# When enabled, multi-key commands like MGET/MSET can operate across different
# hash slots using scatter-gather. MSETNX always requires same-slot for atomicity.
allow_cross_slot_standalone = false

# Timeout for scatter-gather operations in milliseconds.
scatter_gather_timeout_ms = 5000

[logging]
# Log level (trace, debug, info, warn, error)
level = "info"

# Log format (pretty, json)
format = "pretty"

[persistence]
# Whether persistence is enabled
enabled = true

# Directory for data files
data_dir = "./frogdb-data"

# Durability mode: "async" (no fsync), "periodic" (fsync at interval), "sync" (fsync every write)
durability_mode = "periodic"

# Sync interval in milliseconds (for periodic mode)
sync_interval_ms = 1000

# RocksDB write buffer size in MB
write_buffer_size_mb = 64

# Compression type: "none", "snappy", "lz4", "zstd"
compression = "lz4"

# Batch size threshold in KB before flushing
batch_size_threshold_kb = 4096

# Batch timeout in milliseconds before flushing
batch_timeout_ms = 10

[snapshot]
# Directory for storing point-in-time snapshots
snapshot_dir = "./snapshots"

# Interval between automatic snapshots in seconds (0 = disabled)
snapshot_interval_secs = 3600

# Maximum number of snapshots to retain (0 = unlimited)
max_snapshots = 5

[metrics]
# Whether metrics are enabled
enabled = true

# Bind address for the metrics HTTP server
bind = "0.0.0.0"

# Port for the metrics HTTP server
port = 9090

# Whether OTLP export is enabled
otlp_enabled = false

# OTLP endpoint URL
otlp_endpoint = "http://localhost:4317"

# OTLP push interval in seconds
otlp_interval_secs = 15

[tracing]
# Whether distributed tracing is enabled
enabled = false

# OTLP endpoint for trace export
otlp_endpoint = "http://localhost:4317"

# Sampling rate (0.0 to 1.0). 1.0 = sample all, 0.1 = sample 10%
sampling_rate = 1.0

# Service name in traces
service_name = "frogdb"

# Enable scatter-gather operation spans (child spans per shard for MGET/MSET)
scatter_gather_spans = false

# Enable shard execution spans (spans inside shard workers)
shard_spans = false

# Enable persistence spans (WAL writes, snapshots)
persistence_spans = false

[memory]
# Maximum memory limit in bytes. 0 means unlimited.
# When exceeded, behavior depends on maxmemory_policy.
maxmemory = 0

# Eviction policy when maxmemory is reached:
# - noeviction: Return OOM error on writes
# - volatile-lru: Evict least recently used keys with TTL
# - allkeys-lru: Evict least recently used keys (any)
# - volatile-lfu: Evict least frequently used keys with TTL
# - allkeys-lfu: Evict least frequently used keys (any)
# - volatile-random: Evict random keys with TTL
# - allkeys-random: Evict random keys (any)
# - volatile-ttl: Evict keys with shortest TTL
maxmemory_policy = "noeviction"

# Number of keys to sample when looking for eviction candidates.
# Higher values give better accuracy but cost more CPU.
maxmemory_samples = 5

# LFU log factor - higher values make counter increment less likely.
# This affects how quickly the access counter grows.
lfu_log_factor = 10

# LFU decay time in minutes - counter decays by 1 every N minutes.
# This allows old hot keys to eventually become evictable.
lfu_decay_time = 1

[security]
# Legacy password for the default user.
# If set, clients must AUTH with this password before running commands.
# Leave empty to allow connections without authentication.
requirepass = ""

[acl]
# Path to the ACL file for SAVE/LOAD operations.
# If empty, ACL SAVE/LOAD will return an error.
aclfile = ""

# Maximum number of entries in the ACL LOG.
log_max_len = 128

[slowlog]
# Threshold in microseconds. Commands slower than this are logged.
# Set to 0 to log all commands, -1 to disable logging.
log_slower_than = 10000

# Maximum number of entries per shard.
max_len = 128

# Maximum characters per argument before truncation.
max_arg_len = 128

[json]
# Maximum nesting depth for JSON documents.
max_depth = 128

# Maximum size in bytes for JSON documents (64MB).
max_size = 67108864

[vll]
# VLL (Very Lightweight Locking) configuration for multi-shard atomicity.

# Maximum queue depth per shard before rejecting new operations.
max_queue_depth = 10000

# Timeout for acquiring locks on all shards (ms).
lock_acquisition_timeout_ms = 4000

# Per-shard lock acquisition timeout (ms).
per_shard_lock_timeout_ms = 2000

# Interval for checking/cleaning up expired operations (ms).
timeout_check_interval_ms = 100

# Maximum time a continuation lock can be held (ms).
max_continuation_lock_ms = 65000

[replication]
# Replication role: "standalone", "primary", or "replica".
# - standalone: No replication (default)
# - primary: Accept replica connections and stream WAL updates
# - replica: Connect to a primary and receive updates
role = "standalone"

# Primary host (for replica role).
# When role is "replica", this specifies the primary to connect to.
primary_host = ""

# Primary port (for replica role).
primary_port = 6379

# Minimum replicas required to acknowledge writes (for primary role).
# If set > 0, writes will wait for this many replicas to acknowledge
# before returning success. Set to 0 to disable synchronous replication.
min_replicas_to_write = 0

# Timeout for min_replicas_to_write in milliseconds.
# If replicas don't acknowledge within this time, the write still succeeds
# but returns with fewer acknowledged replicas.
min_replicas_timeout_ms = 5000

# ACK interval - how often replicas send ACKs to primary (milliseconds).
ack_interval_ms = 1000

# Full sync timeout (seconds).
# Maximum time to wait for a full sync operation.
fullsync_timeout_secs = 300

# Maximum memory for full sync buffering (MB).
# If exceeded, FULLRESYNC requests will be rejected.
fullsync_max_memory_mb = 512

# Replication state file path (relative to data_dir).
# Stores replication ID and offset for partial sync recovery.
state_file = "replication_state.json"

# Connection timeout for replica connecting to primary (milliseconds).
connect_timeout_ms = 5000

# Handshake timeout during replication setup (milliseconds).
handshake_timeout_ms = 10000

# Reconnection backoff - initial delay (milliseconds).
reconnect_backoff_initial_ms = 100

# Reconnection backoff - maximum delay (milliseconds).
reconnect_backoff_max_ms = 30000

[cluster]
# Whether cluster mode is enabled.
# When enabled, FrogDB runs as part of a Raft-coordinated cluster.
enabled = false

# This node's unique ID (0 = auto-generate from timestamp).
node_id = 0

# Address for client connections (host:port).
# Defaults to server.bind:server.port if not specified.
client_addr = ""

# Address for cluster bus (Raft) communication.
# Typically server port + 10000 (e.g., 16379 for 6379).
cluster_bus_addr = "127.0.0.1:16379"

# Initial cluster nodes to connect to (for joining existing cluster).
# Format: ["host1:port1", "host2:port2"]
initial_nodes = []

# Directory for storing cluster state (Raft logs, snapshots).
data_dir = "./frogdb-cluster"

# Election timeout in milliseconds.
# A leader must receive heartbeats within this time or election starts.
election_timeout_ms = 1000

# Heartbeat interval in milliseconds.
# Leader sends heartbeats at this interval.
heartbeat_interval_ms = 250

# Connection timeout for cluster bus in milliseconds.
connect_timeout_ms = 5000

# Request timeout for cluster bus RPCs in milliseconds.
request_timeout_ms = 10000

[admin]
# Whether the admin HTTP API is enabled.
# The admin API provides cluster management and health check endpoints.
enabled = false

# Port for the admin HTTP server.
port = 6380

# Bind address for the admin HTTP server.
bind = "127.0.0.1"

[status]
# Threshold percentage for memory warning.
# Health status will show a warning when memory usage exceeds this threshold.
memory_warning_percent = 90

# Threshold percentage for connection warning.
# Health status will show a warning when client connections exceed this threshold.
connection_warning_percent = 90

[latency]
# Run intrinsic latency test at startup before accepting connections.
# This measures the system's inherent scheduling latency (OS/hypervisor overhead).
startup_test = false

# Duration of the startup latency test in seconds.
startup_test_duration_secs = 5

# Warning threshold for intrinsic latency in microseconds.
# If max latency exceeds this, a warning is logged but startup continues.
# Results under 500us are typical for bare metal; over 2ms suggests virtualization.
warning_threshold_us = 2000
"#
        .to_string()
    }

    /// Convert to AclConfig for AclManager initialization.
    pub fn to_acl_config(&self) -> frogdb_core::AclConfig {
        use std::path::PathBuf;

        frogdb_core::AclConfig {
            aclfile: if self.acl.aclfile.is_empty() {
                None
            } else {
                Some(PathBuf::from(&self.acl.aclfile))
            },
            log_max_len: self.acl.log_max_len,
            requirepass: if self.security.requirepass.is_empty() {
                None
            } else {
                Some(self.security.requirepass.clone())
            },
        }
    }

    /// Get the full bind address.
    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.server.bind, self.server.port)
    }

    /// Serialize config to JSON for logging.
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|_| "{}".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.server.bind, "127.0.0.1");
        assert_eq!(config.server.port, 6379);
        assert_eq!(config.server.num_shards, 1);
        assert_eq!(config.logging.level, "info");
        assert_eq!(config.logging.format, "pretty");
        assert!(config.persistence.enabled);
        assert_eq!(config.persistence.durability_mode, "periodic");
        assert_eq!(config.persistence.sync_interval_ms, 1000);
    }

    #[test]
    fn test_validate_invalid_log_level() {
        let mut config = Config::default();
        config.logging.level = "invalid".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_invalid_log_format() {
        let mut config = Config::default();
        config.logging.format = "xml".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_bind_addr() {
        let config = Config::default();
        assert_eq!(config.bind_addr(), "127.0.0.1:6379");
    }

    #[test]
    fn test_default_metrics_config() {
        let config = MetricsConfig::default();
        assert!(config.enabled);
        assert_eq!(config.bind, "0.0.0.0");
        assert_eq!(config.port, 9090);
        assert!(!config.otlp_enabled);
        assert_eq!(config.otlp_endpoint, "http://localhost:4317");
        assert_eq!(config.otlp_interval_secs, 15);
    }

    #[test]
    fn test_metrics_bind_addr() {
        let config = MetricsConfig::default();
        assert_eq!(config.bind_addr(), "0.0.0.0:9090");
    }

    #[test]
    fn test_validate_metrics_zero_port() {
        let mut config = Config::default();
        config.metrics.port = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_metrics_otlp_without_endpoint() {
        let mut config = Config::default();
        config.metrics.otlp_enabled = true;
        config.metrics.otlp_endpoint = String::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_default_memory_config() {
        let config = MemoryConfig::default();
        assert_eq!(config.maxmemory, 0);
        assert_eq!(config.maxmemory_policy, "noeviction");
        assert_eq!(config.maxmemory_samples, 5);
        assert_eq!(config.lfu_log_factor, 10);
        assert_eq!(config.lfu_decay_time, 1);
    }

    #[test]
    fn test_memory_config_has_limit() {
        let mut config = MemoryConfig::default();
        assert!(!config.has_limit());

        config.maxmemory = 1024 * 1024;
        assert!(config.has_limit());
    }

    #[test]
    fn test_validate_memory_invalid_policy() {
        let mut config = Config::default();
        config.memory.maxmemory_policy = "invalid".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_memory_valid_policies() {
        let policies = [
            "noeviction",
            "volatile-lru",
            "allkeys-lru",
            "volatile-lfu",
            "allkeys-lfu",
            "volatile-random",
            "allkeys-random",
            "volatile-ttl",
        ];

        for policy in policies {
            let mut config = Config::default();
            config.memory.maxmemory_policy = policy.to_string();
            assert!(config.validate().is_ok(), "Policy {} should be valid", policy);
        }
    }

    #[test]
    fn test_validate_memory_zero_samples() {
        let mut config = Config::default();
        config.memory.maxmemory_samples = 0;
        assert!(config.validate().is_err());
    }

    // ===== LoggingConfig Tests =====

    #[test]
    fn test_logging_config_defaults() {
        let config = LoggingConfig::default();
        assert_eq!(config.level, "info");
        assert_eq!(config.format, "pretty");
    }

    // ===== ReplicationConfigSection Tests =====

    #[test]
    fn test_default_replication_config() {
        let config = ReplicationConfigSection::default();
        assert_eq!(config.role, "standalone");
        assert!(config.primary_host.is_empty());
        assert_eq!(config.primary_port, 6379);
        assert_eq!(config.min_replicas_to_write, 0);
        assert_eq!(config.min_replicas_timeout_ms, 5000);
        assert_eq!(config.ack_interval_ms, 1000);
        assert_eq!(config.fullsync_timeout_secs, 300);
        assert_eq!(config.fullsync_max_memory_mb, 512);
        assert_eq!(config.state_file, "replication_state.json");
        assert_eq!(config.connect_timeout_ms, 5000);
        assert_eq!(config.handshake_timeout_ms, 10000);
        assert_eq!(config.reconnect_backoff_initial_ms, 100);
        assert_eq!(config.reconnect_backoff_max_ms, 30000);
    }

    #[test]
    fn test_replication_config_role_helpers() {
        let mut config = ReplicationConfigSection::default();
        assert!(config.is_standalone());
        assert!(!config.is_primary());
        assert!(!config.is_replica());

        config.role = "primary".to_string();
        assert!(!config.is_standalone());
        assert!(config.is_primary());
        assert!(!config.is_replica());

        config.role = "replica".to_string();
        config.primary_host = "127.0.0.1".to_string();
        assert!(!config.is_standalone());
        assert!(!config.is_primary());
        assert!(config.is_replica());
    }

    #[test]
    fn test_replication_config_validate_invalid_role() {
        let mut config = ReplicationConfigSection::default();
        config.role = "invalid".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_replication_config_validate_valid_roles() {
        for role in ["standalone", "primary", "replica"] {
            let mut config = ReplicationConfigSection::default();
            config.role = role.to_string();
            if role == "replica" {
                config.primary_host = "127.0.0.1".to_string();
            }
            assert!(config.validate().is_ok(), "Role {} should be valid", role);
        }
    }

    #[test]
    fn test_replication_config_validate_replica_without_host() {
        let mut config = ReplicationConfigSection::default();
        config.role = "replica".to_string();
        config.primary_host = String::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_replication_config_validate_zero_fullsync_timeout() {
        let mut config = ReplicationConfigSection::default();
        config.fullsync_timeout_secs = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_replication_config_validate_zero_fullsync_memory() {
        let mut config = ReplicationConfigSection::default();
        config.fullsync_max_memory_mb = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_replication_config_validate_zero_ack_interval() {
        let mut config = ReplicationConfigSection::default();
        config.ack_interval_ms = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_replication_config_to_core_config() {
        // Test standalone
        let config = ReplicationConfigSection::default();
        let core = config.to_core_config();
        assert!(matches!(core, frogdb_core::ReplicationConfig::Standalone));

        // Test primary
        let mut config = ReplicationConfigSection::default();
        config.role = "primary".to_string();
        config.min_replicas_to_write = 2;
        let core = config.to_core_config();
        assert!(matches!(
            core,
            frogdb_core::ReplicationConfig::Primary { min_replicas_to_write: 2 }
        ));

        // Test replica
        let mut config = ReplicationConfigSection::default();
        config.role = "replica".to_string();
        config.primary_host = "192.168.1.1".to_string();
        config.primary_port = 6380;
        let core = config.to_core_config();
        if let frogdb_core::ReplicationConfig::Replica { primary_addr } = core {
            assert_eq!(primary_addr, "192.168.1.1:6380");
        } else {
            panic!("Expected Replica config");
        }
    }

    // ===== TracingConfig Tests =====

    #[test]
    fn test_tracing_config_defaults() {
        let config = TracingConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.otlp_endpoint, "http://localhost:4317");
        assert_eq!(config.sampling_rate, 1.0);
        assert_eq!(config.service_name, "frogdb");
        assert!(!config.scatter_gather_spans);
        assert!(!config.shard_spans);
        assert!(!config.persistence_spans);
    }

    #[test]
    fn test_validate_tracing_enabled_without_endpoint() {
        let mut config = Config::default();
        config.tracing.enabled = true;
        config.tracing.otlp_endpoint = String::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_tracing_invalid_sampling_rate() {
        let mut config = Config::default();
        config.tracing.enabled = true;
        config.tracing.sampling_rate = 1.5;
        assert!(config.validate().is_err());

        config.tracing.sampling_rate = -0.1;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_tracing_valid_config() {
        let mut config = Config::default();
        config.tracing.enabled = true;
        config.tracing.otlp_endpoint = "http://localhost:4317".to_string();
        config.tracing.sampling_rate = 0.5;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_tracing_config_to_metrics_config() {
        let config = TracingConfig {
            enabled: true,
            otlp_endpoint: "http://example.com:4317".to_string(),
            sampling_rate: 0.1,
            service_name: "test-service".to_string(),
            scatter_gather_spans: true,
            shard_spans: false,
            persistence_spans: true,
        };

        let metrics_config = config.to_metrics_config();
        assert!(metrics_config.enabled);
        assert_eq!(metrics_config.otlp_endpoint, "http://example.com:4317");
        assert_eq!(metrics_config.sampling_rate, 0.1);
        assert_eq!(metrics_config.service_name, "test-service");
        assert!(metrics_config.scatter_gather_spans);
        assert!(!metrics_config.shard_spans);
        assert!(metrics_config.persistence_spans);
    }

    // ===== PersistenceConfig Validation Tests =====

    #[test]
    fn test_validate_invalid_compression() {
        let mut config = PersistenceConfig::default();
        config.compression = "invalid".to_string();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid compression type"));
    }

    #[test]
    fn test_validate_valid_compression_types() {
        for compression in ["none", "snappy", "lz4", "zstd", "NONE", "Snappy", "LZ4", "ZSTD"] {
            let mut config = PersistenceConfig::default();
            config.compression = compression.to_string();
            assert!(
                config.validate().is_ok(),
                "Compression {} should be valid",
                compression
            );
        }
    }

    #[test]
    fn test_validate_invalid_durability_mode() {
        let mut config = PersistenceConfig::default();
        config.durability_mode = "invalid".to_string();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid durability_mode"));
    }

    #[test]
    fn test_validate_valid_durability_modes() {
        for mode in ["async", "periodic", "sync", "ASYNC", "Periodic", "SYNC"] {
            let mut config = PersistenceConfig::default();
            config.durability_mode = mode.to_string();
            assert!(
                config.validate().is_ok(),
                "Durability mode {} should be valid",
                mode
            );
        }
    }

    #[test]
    fn test_validate_persistence_disabled_skips_validation() {
        let mut config = PersistenceConfig::default();
        config.enabled = false;
        config.compression = "invalid".to_string();
        config.durability_mode = "invalid".to_string();
        // Should pass because persistence is disabled
        assert!(config.validate().is_ok());
    }

    // ===== Bind Address Validation Tests =====

    #[test]
    fn test_validate_valid_bind_addresses() {
        // Valid IP addresses
        assert!(validate_bind_address("127.0.0.1", "test").is_ok());
        assert!(validate_bind_address("0.0.0.0", "test").is_ok());
        assert!(validate_bind_address("192.168.1.1", "test").is_ok());
        assert!(validate_bind_address("::1", "test").is_ok());
        assert!(validate_bind_address("::", "test").is_ok());

        // Valid hostnames
        assert!(validate_bind_address("localhost", "test").is_ok());
        assert!(validate_bind_address("example.com", "test").is_ok());
        assert!(validate_bind_address("my-host", "test").is_ok());
        assert!(validate_bind_address("server1.example.com", "test").is_ok());
    }

    #[test]
    fn test_validate_invalid_bind_addresses() {
        // Empty address
        let result = validate_bind_address("", "test");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot be empty"));

        // Hostname starting with hyphen
        let result = validate_bind_address("-invalid", "test");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot start or end with hyphen"));

        // Hostname ending with hyphen
        let result = validate_bind_address("invalid-", "test");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot start or end with hyphen"));

        // Invalid characters
        let result = validate_bind_address("host_name", "test");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid characters"));

        // Empty label (consecutive dots)
        let result = validate_bind_address("host..name", "test");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("labels must be 1-63 chars"));
    }

    // ===== Path Validation Tests =====

    #[test]
    fn test_validate_data_dir_nonexistent_parent() {
        let path = Path::new("/nonexistent/path/data");
        let result = validate_path_parent(path, "test.path");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not exist"));
    }

    #[test]
    fn test_validate_path_skipped_when_feature_disabled() {
        // When persistence is disabled, path validation should be skipped
        let mut config = Config::default();
        config.persistence.enabled = false;
        config.persistence.data_dir = PathBuf::from("/nonexistent/path/data");
        // This should not fail because persistence is disabled
        // (The validate() call will still run other validations, but path validation
        // for persistence.data_dir is skipped)
        // Note: We can't easily test this in isolation, but the logic is in Config::validate()
    }

    // ===== Config File Loading Tests =====

    #[test]
    fn test_load_explicit_config_file_not_found() {
        let nonexistent_path = Path::new("/nonexistent/config.toml");
        let result = Config::load(
            Some(nonexistent_path),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("config file not found"));
    }

    // ===== Unknown Fields Rejection Tests =====

    #[test]
    fn test_reject_unknown_fields_in_server() {
        let toml = r#"
            [server]
            unknown_field = "value"
        "#;
        let result: Result<Config, _> = toml::from_str(toml);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unknown field"));
    }

    #[test]
    fn test_reject_unknown_fields_in_snapshot() {
        let toml = r#"
            [snapshot]
            enabled = false
        "#;
        let result: Result<Config, _> = toml::from_str(toml);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unknown field"));
    }

    #[test]
    fn test_reject_unknown_fields_at_root() {
        let toml = r#"
            [unknown_section]
            key = "value"
        "#;
        let result: Result<Config, _> = toml::from_str(toml);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unknown field"));
    }

    #[test]
    fn test_accept_valid_config() {
        let toml = r#"
            [server]
            port = 6380
            bind = "0.0.0.0"

            [logging]
            level = "debug"
        "#;
        let result: Result<Config, _> = toml::from_str(toml);
        assert!(result.is_ok());
        let config = result.unwrap();
        assert_eq!(config.server.port, 6380);
        assert_eq!(config.server.bind, "0.0.0.0");
        assert_eq!(config.logging.level, "debug");
    }
}
