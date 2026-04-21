//! FrogDB configuration types and validation.
//!
//! This crate contains all configuration type definitions and validators,
//! decoupled from the heavy server runtime so lightweight consumers
//! (operator, helm-gen) don't pull in RocksDB/mlua/tantivy.

pub mod admin;
pub mod blocking;
#[cfg(feature = "turmoil")]
pub mod chaos;
pub mod cluster;
pub mod compat;
pub mod debug_bundle;
pub mod distributed_tracing;
pub mod http;
pub mod json;
pub mod latency;
pub mod logging;
pub mod memory;
pub mod metrics;
pub mod monitor;
pub mod params;
pub mod persistence;
pub mod replication;
pub mod security;
pub mod server;
pub mod slowlog;
pub mod status;
pub mod tiered;
pub mod tls;
pub mod validators;
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
pub use http::HttpConfig;
pub use json::JsonConfig;
pub use latency::{LatencyBandsConfig, LatencyConfig};
pub use logging::{LogOutput, LoggingConfig, RotationConfig, RotationFrequency};
pub use memory::MemoryConfig;
pub use metrics::MetricsConfig;
pub use monitor::MonitorConfig;
pub use params::{ConfigParamInfo, config_param_registry};
pub use persistence::{PersistenceConfig, SnapshotConfig};
pub use replication::ReplicationConfigSection;
pub use security::{AclFileConfig, SecurityConfig};
pub use server::ServerConfig;
pub use slowlog::SlowlogConfig;
pub use status::{HotShardsConfig, StatusConfig};
pub use tiered::TieredStorageConfig;
pub use tls::{ClientCertMode, TlsConfig, TlsProtocol};
pub use vll::VllConfig;

use anyhow::Result;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Main configuration struct.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct Config {
    /// Path to the TOML config file used at startup (None if defaults only).
    /// Skipped from serde and schema since it is set programmatically.
    #[serde(skip)]
    #[schemars(skip)]
    pub config_source_path: Option<PathBuf>,

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

    /// HTTP server configuration (metrics, health, debug UI, admin REST API).
    #[serde(default)]
    pub http: HttpConfig,

    /// Metrics configuration (OTLP export settings).
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

    /// TLS configuration.
    #[serde(default)]
    pub tls: TlsConfig,

    /// Chaos testing configuration (turmoil simulation only).
    #[cfg(feature = "turmoil")]
    #[serde(default, skip)]
    #[schemars(skip)]
    pub chaos: ChaosConfig,
}

/// Validate a bind address (IP address or hostname).
pub fn validate_bind_address(addr: &str, field_name: &str) -> Result<()> {
    use std::net::IpAddr;

    if addr.parse::<IpAddr>().is_ok() {
        return Ok(());
    }

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
pub fn validate_path_parent(path: &Path, field_name: &str) -> Result<()> {
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
    /// Validate the configuration.
    pub fn validate(&self) -> Result<()> {
        if self.server.port == 0 {
            anyhow::bail!("Port cannot be 0");
        }
        let valid_levels = ["trace", "debug", "info", "warn", "error"];
        if !valid_levels.contains(&self.logging.level.to_lowercase().as_str()) {
            anyhow::bail!(
                "Invalid log level '{}', expected one of: {:?}",
                self.logging.level,
                valid_levels
            );
        }
        let valid_formats = ["pretty", "json"];
        if !valid_formats.contains(&self.logging.format.to_lowercase().as_str()) {
            anyhow::bail!(
                "Invalid log format '{}', expected one of: {:?}",
                self.logging.format,
                valid_formats
            );
        }
        self.logging.validate()?;
        self.http.validate()?;
        self.metrics.validate()?;
        self.memory.validate()?;
        self.replication.validate()?;
        self.tracing.validate()?;
        self.persistence.validate()?;
        self.cluster.validate()?;
        self.admin.validate()?;
        self.tls.validate()?;
        self.json.validate()?;
        self.vll.validate()?;
        self.status.validate()?;
        self.hotshards.validate()?;
        self.tiered_storage.validate()?;
        if self.tiered_storage.enabled && !self.persistence.enabled {
            anyhow::bail!("tiered_storage.enabled=true requires persistence.enabled=true");
        }
        let policy = self.memory.maxmemory_policy.to_lowercase();
        if (policy == "tiered-lru" || policy == "tiered-lfu") && !self.tiered_storage.enabled {
            anyhow::bail!(
                "maxmemory_policy '{}' requires tiered_storage.enabled=true",
                self.memory.maxmemory_policy
            );
        }
        validate_bind_address(&self.server.bind, "server.bind")?;
        if self.http.enabled {
            validate_bind_address(&self.http.bind, "http.bind")?;
        }
        if self.admin.enabled {
            validate_bind_address(&self.admin.bind, "admin.bind")?;
        }
        if self.persistence.enabled {
            validate_path_parent(&self.persistence.data_dir, "persistence.data_dir")?;
        }
        if self.snapshot.snapshot_interval_secs > 0 {
            validate_path_parent(&self.snapshot.snapshot_dir, "snapshot.snapshot_dir")?;
        }
        if !self.acl.aclfile.is_empty() {
            validate_path_parent(Path::new(&self.acl.aclfile), "acl.aclfile")?;
        }
        if let Some(ref file_path) = self.logging.file_path {
            validate_path_parent(file_path, "logging.file_path")?;
        }
        let report = validators::run_all_validators(self);
        report.log_non_errors();
        report.into_result()?;
        Ok(())
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
    fn test_bind_addr() {
        let config = Config::default();
        assert_eq!(config.bind_addr(), "127.0.0.1:6379");
    }

    #[test]
    fn test_validate_valid_bind_addresses() {
        assert!(validate_bind_address("127.0.0.1", "test").is_ok());
        assert!(validate_bind_address("0.0.0.0", "test").is_ok());
        assert!(validate_bind_address("::1", "test").is_ok());
        assert!(validate_bind_address("localhost", "test").is_ok());
        assert!(validate_bind_address("example.com", "test").is_ok());
    }

    #[test]
    fn test_validate_invalid_bind_addresses() {
        assert!(validate_bind_address("", "test").is_err());
        assert!(validate_bind_address("-invalid", "test").is_err());
        assert!(validate_bind_address("host_name", "test").is_err());
    }

    #[test]
    fn test_reject_unknown_fields_in_server() {
        let toml = r#"
            [server]
            unknown-field = "value"
        "#;
        let result: Result<Config, _> = toml::from_str(toml);
        assert!(result.is_err());
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
