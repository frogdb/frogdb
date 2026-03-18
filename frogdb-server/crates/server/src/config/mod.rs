//! Configuration handling via Figment.

mod loader;
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
pub mod monitor;
pub mod persistence;
pub mod replication;
pub mod security;
pub mod server;
pub mod slowlog;
pub mod status;
pub mod tiered;
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
pub use logging::{LogOutput, LoggingConfig, LoggingGuard, RotationConfig, RotationFrequency};
pub use memory::MemoryConfig;
pub use metrics::MetricsConfig;
pub use monitor::MonitorConfig;
pub use persistence::{PersistenceConfig, SnapshotConfig};
pub use replication::ReplicationConfigSection;
pub use security::{AclFileConfig, SecurityConfig};
pub use server::ServerConfig;
pub use slowlog::SlowlogConfig;
pub use status::{HotShardsConfig, StatusConfig};
pub use tiered::TieredStorageConfig;
pub use vll::VllConfig;

use anyhow::Result;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::Path;

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

    /// Tiered storage configuration (hot/warm two-tier storage).
    #[serde(default)]
    pub tiered_storage: TieredStorageConfig,

    /// MONITOR command configuration.
    #[serde(default)]
    pub monitor: MonitorConfig,

    /// Chaos testing configuration (turmoil simulation only).
    #[cfg(feature = "turmoil")]
    #[serde(default, skip)]
    #[schemars(skip)]
    pub chaos: ChaosConfig,
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
        assert_eq!(config.server.bind, server::DEFAULT_BIND);
        assert_eq!(config.server.port, server::DEFAULT_PORT);
        assert_eq!(config.server.num_shards, server::DEFAULT_NUM_SHARDS);
        assert_eq!(config.logging.level, logging::DEFAULT_LOG_LEVEL);
        assert_eq!(config.logging.format, logging::DEFAULT_LOG_FORMAT);
        assert!(config.persistence.enabled);
        assert_eq!(config.persistence.durability_mode, "periodic");
        assert_eq!(
            config.persistence.sync_interval_ms,
            persistence::DEFAULT_SYNC_INTERVAL_MS
        );
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

    // ===== File Logging Tests =====

    #[test]
    fn test_build_file_writer_none_when_no_file_path() {
        let config = Config::default();
        let (writer, guard) = config.build_file_writer().unwrap();
        assert!(writer.is_none());
        assert!(guard.is_none());
    }

    #[test]
    fn test_build_file_writer_plain_file() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");

        let mut config = Config::default();
        config.logging.file_path = Some(log_path.clone());

        let (writer, guard) = config.build_file_writer().unwrap();
        assert!(writer.is_some());
        assert!(guard.is_some());

        // Write through the non-blocking writer
        use std::io::Write;
        let mut nb = writer.unwrap();
        writeln!(nb, "hello from test").unwrap();

        // Drop guard to flush
        drop(guard);

        let contents = std::fs::read_to_string(&log_path).unwrap();
        assert!(contents.contains("hello from test"));
    }

    #[test]
    fn test_build_file_writer_with_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");

        let mut config = Config::default();
        config.logging.file_path = Some(log_path.clone());
        config.logging.rotation = Some(RotationConfig {
            max_size_mb: 100,
            frequency: RotationFrequency::Daily,
            max_files: 3,
        });

        let (writer, guard) = config.build_file_writer().unwrap();
        assert!(writer.is_some());
        assert!(guard.is_some());

        use std::io::Write;
        let mut nb = writer.unwrap();
        writeln!(nb, "rotated log line").unwrap();

        drop(guard);

        // The rolling file appender writes to a file with a suffix
        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(
            !files.is_empty(),
            "should have created at least one log file"
        );
    }

    #[test]
    fn test_build_file_writer_nonexistent_parent_fails() {
        let mut config = Config::default();
        config.logging.file_path = Some(std::path::PathBuf::from("/nonexistent/dir/test.log"));

        let result = config.build_file_writer();
        assert!(result.is_err());
    }

    #[test]
    fn test_accept_config_with_file_logging() {
        let toml = r#"
            [logging]
            level = "info"
            format = "json"
            output = "none"
            file_path = "/tmp/frogdb-test.log"

            [logging.rotation]
            max_size_mb = 50
            frequency = "hourly"
            max_files = 10
        "#;
        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(config.logging.output, LogOutput::None);
        assert_eq!(
            config.logging.file_path,
            Some(std::path::PathBuf::from("/tmp/frogdb-test.log"))
        );
        let rotation = config.logging.rotation.unwrap();
        assert_eq!(rotation.max_size_mb, 50);
        assert_eq!(rotation.frequency, RotationFrequency::Hourly);
        assert_eq!(rotation.max_files, 10);
    }

    #[test]
    fn test_validate_rotation_without_file_path_fails() {
        let mut config = Config::default();
        config.logging.rotation = Some(RotationConfig::default());
        assert!(config.validate().is_err());
    }
}
