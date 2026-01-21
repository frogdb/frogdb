//! Configuration handling via Figment.

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

    /// Slow query log configuration.
    #[serde(default)]
    pub slowlog: SlowlogConfig,
}

/// Security configuration.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct SecurityConfig {
    /// Legacy password for the default user (like Redis requirepass).
    /// If set, clients must AUTH with this password before running commands.
    #[serde(default)]
    pub requirepass: String,
}

/// ACL configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
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
pub struct BlockingConfig {
    /// Maximum waiters per key (0 = unlimited).
    #[serde(default = "default_max_waiters_per_key")]
    pub max_waiters_per_key: usize,

    /// Maximum total blocked connections (0 = unlimited).
    #[serde(default = "default_max_blocked_connections")]
    pub max_blocked_connections: usize,
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

/// Metrics configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
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

/// Server-specific configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
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

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            maxmemory: default_maxmemory(),
            maxmemory_policy: default_maxmemory_policy(),
            maxmemory_samples: default_maxmemory_samples(),
            lfu_log_factor: default_lfu_log_factor(),
            lfu_decay_time: default_lfu_decay_time(),
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
    ) -> Result<Self> {
        let mut figment = Figment::new()
            .merge(Serialized::defaults(Config::default()));

        // Merge config file if provided
        if let Some(path) = config_path {
            figment = figment.merge(Toml::file(path));
        } else {
            // Try default config file
            figment = figment.merge(Toml::file("frogdb.toml").nested());
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
        assert_eq!(config.output, "stdout");
        assert!(config.file_path.is_none());
        assert_eq!(config.max_size_mb, 100);
        assert_eq!(config.max_files, 5);
        assert!(!config.compress);
    }

    #[test]
    fn test_logging_config_validate_stdout_no_path_ok() {
        let config = LoggingConfig {
            output: "stdout".to_string(),
            file_path: None,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_logging_config_validate_file_requires_path() {
        let config = LoggingConfig {
            output: "file".to_string(),
            file_path: None,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_logging_config_validate_both_requires_path() {
        let config = LoggingConfig {
            output: "both".to_string(),
            file_path: None,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_logging_config_validate_file_with_path_ok() {
        let config = LoggingConfig {
            output: "file".to_string(),
            file_path: Some(PathBuf::from("/tmp/test.log")),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_logging_config_validate_both_with_path_ok() {
        let config = LoggingConfig {
            output: "both".to_string(),
            file_path: Some(PathBuf::from("/tmp/test.log")),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_logging_config_validate_invalid_output() {
        let config = LoggingConfig {
            output: "invalid".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_logging_config_validate_case_insensitive() {
        // Test that output validation is case-insensitive
        let config = LoggingConfig {
            output: "STDOUT".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_ok());

        let config = LoggingConfig {
            output: "FILE".to_string(),
            file_path: Some(PathBuf::from("/tmp/test.log")),
            ..Default::default()
        };
        assert!(config.validate().is_ok());

        let config = LoggingConfig {
            output: "BOTH".to_string(),
            file_path: Some(PathBuf::from("/tmp/test.log")),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }
}
