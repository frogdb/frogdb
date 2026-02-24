//! Configuration handling via Figment.

pub mod validators;

// Config section modules
pub mod admin;
pub mod blocking;
#[cfg(feature = "turmoil")]
pub mod chaos;
pub mod cluster;
pub mod compat;
pub mod debug_bundle;
pub mod distributed_tracing;
pub mod json;
pub mod latency;
pub mod logging;
pub mod memory;
pub mod metrics;
pub mod persistence;
pub mod replication;
pub mod security;
pub mod server;
pub mod slowlog;
pub mod status;
pub mod vll;

// Re-export all config types
pub use admin::AdminConfig;
pub use blocking::BlockingConfig;
#[cfg(feature = "turmoil")]
pub use chaos::ChaosConfig;
pub use cluster::ClusterConfigSection;
pub use compat::CompatConfig;
pub use debug_bundle::DebugBundleConfig;
pub use distributed_tracing::TracingConfig;
pub use json::JsonConfig;
pub use latency::{LatencyBandsConfig, LatencyConfig};
pub use logging::LoggingConfig;
pub use memory::MemoryConfig;
pub use metrics::MetricsConfig;
pub use persistence::{PersistenceConfig, SnapshotConfig};
pub use replication::ReplicationConfigSection;
pub use security::{AclFileConfig, SecurityConfig};
pub use server::ServerConfig;
pub use slowlog::SlowlogConfig;
pub use status::{HotShardsConfig, StatusConfig};
pub use vll::VllConfig;

use anyhow::{Context, Result};
use figment::{
    Figment,
    providers::{Env, Format, Serialized, Toml},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::Path;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

/// Main configuration struct.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
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

    /// Latency bands configuration for SLO monitoring.
    #[serde(default)]
    pub latency_bands: LatencyBandsConfig,

    /// Debug bundle configuration.
    #[serde(default)]
    pub debug_bundle: DebugBundleConfig,

    /// Compatibility configuration.
    #[serde(default)]
    pub compat: CompatConfig,
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
    #[allow(clippy::too_many_arguments)]
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
        let mut figment = Figment::new().merge(Serialized::defaults(Config::default()));

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
        if let Some(port) = admin_port {
            config.admin.enabled = true;
            config.admin.port = port;
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

# RocksDB block cache size in MB (0 = disabled)
block_cache_size_mb = 256

# RocksDB bloom filter bits per key (0 = disabled)
bloom_filter_bits = 10

# Maximum number of RocksDB write buffers
max_write_buffer_number = 4

# RocksDB compaction rate limit in MB/s (0 = unlimited)
compaction_rate_limit_mb = 0

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

# Enable automatic failover when a primary fails.
# When enabled, the leader will automatically promote a replica to primary
# if the primary becomes unreachable.
auto_failover = false

# Number of consecutive failures before marking a node as FAIL.
# Used by the failure detection system to determine when a node is down.
fail_threshold = 5

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

[latency_bands]
# Whether SLO-focused latency band tracking is enabled.
# When enabled, tracks cumulative request counts per latency bucket for SLO monitoring.
enabled = false

# Latency band thresholds in milliseconds.
# Requests are counted in cumulative buckets (<=1ms, <=5ms, etc.)
# Use LATENCY BANDS command to view counts and percentages.
bands = [1, 5, 10, 50, 100, 500]
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
    fn test_validate_memory_invalid_policy() {
        let mut config = Config::default();
        config.memory.maxmemory_policy = "invalid".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_memory_zero_samples() {
        let mut config = Config::default();
        config.memory.maxmemory_samples = 0;
        assert!(config.validate().is_err());
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
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("cannot start or end with hyphen")
        );

        // Hostname ending with hyphen
        let result = validate_bind_address("invalid-", "test");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("cannot start or end with hyphen")
        );

        // Invalid characters
        let result = validate_bind_address("host_name", "test");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid characters")
        );

        // Empty label (consecutive dots)
        let result = validate_bind_address("host..name", "test");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("labels must be 1-63 chars")
        );
    }

    // ===== Path Validation Tests =====

    #[test]
    fn test_validate_data_dir_nonexistent_parent() {
        let path = Path::new("/nonexistent/path/data");
        let result = validate_path_parent(path, "test.path");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not exist"));
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
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("config file not found")
        );
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
