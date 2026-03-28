//! Configuration handling via Figment.
//!
//! Config types are defined in `frogdb-config` and re-exported here.
//! This module adds runtime loading (loader.rs), logging initialization,
//! and conversion methods to core/telemetry/debug types.

mod loader;
pub use loader::ConfigLoader;

// Re-export everything from frogdb-config
pub use frogdb_config::*;

/// Guard that keeps the non-blocking file writer alive.
/// When dropped, the background writer thread flushes remaining logs.
pub struct LoggingGuard {
    pub(crate) _file_guard: Option<tracing_appender::non_blocking::WorkerGuard>,
}

// === Extension traits for conversion methods ===
// These depend on heavy crates (frogdb-core, frogdb-debug, frogdb-telemetry)
// and thus cannot live in the lightweight frogdb-config crate.

/// Extension trait for Config conversion methods.
pub trait ConfigExt {
    /// Convert to AclConfig for AclManager initialization.
    fn to_acl_config(&self) -> frogdb_core::AclConfig;
}

impl ConfigExt for Config {
    fn to_acl_config(&self) -> frogdb_core::AclConfig {
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
}

/// Extension trait for ReplicationConfigSection conversion methods.
pub trait ReplicationConfigExt {
    /// Convert to core ReplicationConfig.
    fn to_core_config(&self) -> frogdb_core::ReplicationConfig;
}

impl ReplicationConfigExt for ReplicationConfigSection {
    fn to_core_config(&self) -> frogdb_core::ReplicationConfig {
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

/// Extension trait for ClusterConfigSection conversion methods.
pub trait ClusterConfigExt {
    /// Convert to core ClusterConfig.
    fn to_core_config(&self, server_config: &ServerConfig) -> frogdb_core::ClusterConfig;
}

impl ClusterConfigExt for ClusterConfigSection {
    fn to_core_config(
        &self,
        server_config: &ServerConfig,
    ) -> frogdb_core::ClusterConfig {
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

/// Extension trait for SnapshotConfig conversion methods.
pub trait SnapshotConfigExt {
    /// Convert to the core SnapshotConfig type.
    fn to_core_config(&self) -> frogdb_core::persistence::SnapshotConfig;
}

impl SnapshotConfigExt for SnapshotConfig {
    fn to_core_config(&self) -> frogdb_core::persistence::SnapshotConfig {
        frogdb_core::persistence::SnapshotConfig {
            snapshot_dir: self.snapshot_dir.clone(),
            snapshot_interval_secs: self.snapshot_interval_secs,
            max_snapshots: self.max_snapshots,
        }
    }
}

/// Extension trait for JsonConfig conversion methods.
pub trait JsonConfigExt {
    /// Convert to JsonLimits.
    fn to_limits(&self) -> frogdb_core::JsonLimits;
}

impl JsonConfigExt for JsonConfig {
    fn to_limits(&self) -> frogdb_core::JsonLimits {
        frogdb_core::JsonLimits {
            max_depth: self.max_depth,
            max_size: self.max_size,
        }
    }
}

/// Extension trait for MemoryConfig conversion methods.
pub trait MemoryConfigExt {
    /// Convert to MemoryDiagConfig for the debug crate.
    fn to_diag_config(&self) -> frogdb_debug::MemoryDiagConfig;
}

impl MemoryConfigExt for MemoryConfig {
    fn to_diag_config(&self) -> frogdb_debug::MemoryDiagConfig {
        frogdb_debug::MemoryDiagConfig {
            big_key_threshold_bytes: self.doctor_big_key_threshold as usize,
            max_big_keys_per_shard: self.doctor_max_big_keys,
            imbalance_threshold_percent: self.doctor_imbalance_threshold,
        }
    }
}

/// Extension trait for StatusConfig conversion methods.
pub trait StatusConfigExt {
    /// Convert to StatusCollectorConfig.
    fn to_collector_config(&self) -> frogdb_telemetry::StatusCollectorConfig;
}

impl StatusConfigExt for StatusConfig {
    fn to_collector_config(&self) -> frogdb_telemetry::StatusCollectorConfig {
        frogdb_telemetry::StatusCollectorConfig {
            memory_warning_percent: self.memory_warning_percent,
            connection_warning_percent: self.connection_warning_percent,
            durability_lag_warning_ms: self.durability_lag_warning_ms,
            durability_lag_critical_ms: self.durability_lag_critical_ms,
        }
    }
}

/// Extension trait for HotShardsConfig conversion methods.
pub trait HotShardsConfigExt {
    /// Convert to HotShardConfig for the debug crate.
    fn to_collector_config(&self) -> frogdb_debug::HotShardConfig;
}

impl HotShardsConfigExt for HotShardsConfig {
    fn to_collector_config(&self) -> frogdb_debug::HotShardConfig {
        frogdb_debug::HotShardConfig {
            hot_threshold_percent: self.hot_threshold_percent,
            warm_threshold_percent: self.warm_threshold_percent,
            default_period_secs: self.default_period_secs,
        }
    }
}

/// Extension trait for TracingConfig conversion methods.
pub trait TracingConfigExt {
    /// Convert to frogdb_telemetry::TracingConfig.
    fn to_metrics_config(&self) -> frogdb_telemetry::TracingConfig;
}

impl TracingConfigExt for frogdb_config::TracingConfig {
    fn to_metrics_config(&self) -> frogdb_telemetry::TracingConfig {
        frogdb_telemetry::TracingConfig {
            enabled: self.enabled,
            otlp_endpoint: self.otlp_endpoint.clone(),
            sampling_rate: self.sampling_rate,
            service_name: self.service_name.clone(),
            scatter_gather_spans: self.scatter_gather_spans,
            shard_spans: self.shard_spans,
            persistence_spans: self.persistence_spans,
            recent_traces_max: self.recent_traces_max,
        }
    }
}

/// Extension trait for DebugBundleConfig conversion methods.
pub trait DebugBundleConfigExt {
    /// Convert to BundleConfig for the debug crate.
    fn to_bundle_config(&self) -> frogdb_debug::BundleConfig;
}

impl DebugBundleConfigExt for DebugBundleConfig {
    fn to_bundle_config(&self) -> frogdb_debug::BundleConfig {
        frogdb_debug::BundleConfig {
            directory: std::path::PathBuf::from(&self.directory),
            max_bundles: self.max_bundles,
            bundle_ttl_secs: self.bundle_ttl_secs,
            max_slowlog_entries: self.max_slowlog_entries,
            max_trace_entries: self.max_trace_entries,
        }
    }
}

// ChaosConfig runtime methods (turmoil only, depend on tokio/rand)
#[cfg(feature = "turmoil")]
pub trait ChaosConfigExt {
    fn get_jitter(&self) -> std::time::Duration;
    async fn apply_delay(&self, base_ms: u64);
    fn is_shard_unavailable(&self, shard_id: usize) -> bool;
    fn get_shard_error(&self, shard_id: usize) -> Option<&str>;
    fn should_simulate_connection_reset(&self) -> bool;
    fn has_failure_injection(&self) -> bool;
}

#[cfg(feature = "turmoil")]
impl ChaosConfigExt for ChaosConfig {
    fn get_jitter(&self) -> std::time::Duration {
        if self.jitter_ms == 0 {
            std::time::Duration::ZERO
        } else {
            use rand::Rng;
            let jitter = rand::thread_rng().gen_range(0..=self.jitter_ms);
            std::time::Duration::from_millis(jitter)
        }
    }

    async fn apply_delay(&self, base_ms: u64) {
        if base_ms > 0 || self.jitter_ms > 0 {
            let total = std::time::Duration::from_millis(base_ms) + self.get_jitter();
            if !total.is_zero() {
                tokio::time::sleep(total).await;
            }
        }
    }

    fn is_shard_unavailable(&self, shard_id: usize) -> bool {
        self.unavailable_shards.contains(&shard_id)
    }

    fn get_shard_error(&self, shard_id: usize) -> Option<&str> {
        self.error_shards.get(&shard_id).map(|s| s.as_str())
    }

    fn should_simulate_connection_reset(&self) -> bool {
        if self.connection_reset_probability <= 0.0 {
            return false;
        }
        if self.connection_reset_probability >= 1.0 {
            return true;
        }
        use rand::Rng;
        rand::thread_rng().r#gen::<f64>() < self.connection_reset_probability
    }

    fn has_failure_injection(&self) -> bool {
        !self.unavailable_shards.is_empty()
            || !self.error_shards.is_empty()
            || self.connection_reset_probability > 0.0
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

    #[test]
    fn test_validate_valid_bind_addresses() {
        assert!(validate_bind_address("127.0.0.1", "test").is_ok());
        assert!(validate_bind_address("0.0.0.0", "test").is_ok());
        assert!(validate_bind_address("192.168.1.1", "test").is_ok());
        assert!(validate_bind_address("::1", "test").is_ok());
        assert!(validate_bind_address("::", "test").is_ok());
        assert!(validate_bind_address("localhost", "test").is_ok());
        assert!(validate_bind_address("example.com", "test").is_ok());
        assert!(validate_bind_address("my-host", "test").is_ok());
    }

    #[test]
    fn test_validate_invalid_bind_addresses() {
        let result = validate_bind_address("", "test");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot be empty"));

        let result = validate_bind_address("-invalid", "test");
        assert!(result.is_err());

        let result = validate_bind_address("host_name", "test");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_data_dir_nonexistent_parent() {
        let path = std::path::Path::new("/nonexistent/path/data");
        let result = validate_path_parent(path, "test.path");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not exist"));
    }

    #[test]
    fn test_load_explicit_config_file_not_found() {
        let nonexistent_path = std::path::Path::new("/nonexistent/config.toml");
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

    #[test]
    fn test_reject_unknown_fields_in_server() {
        let toml = r#"
            [server]
            unknown_field = "value"
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
    }

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

        use std::io::Write;
        let mut nb = writer.unwrap();
        writeln!(nb, "hello from test").unwrap();
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

        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(!files.is_empty());
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

    #[test]
    fn test_replication_config_to_core_config() {
        let config = ReplicationConfigSection::default();
        let core = config.to_core_config();
        assert!(matches!(core, frogdb_core::ReplicationConfig::Standalone));

        let config = ReplicationConfigSection {
            role: "primary".to_string(),
            min_replicas_to_write: 2,
            ..Default::default()
        };
        let core = config.to_core_config();
        assert!(matches!(
            core,
            frogdb_core::ReplicationConfig::Primary {
                min_replicas_to_write: 2
            }
        ));

        let config = ReplicationConfigSection {
            role: "replica".to_string(),
            primary_host: "192.168.1.1".to_string(),
            primary_port: 6380,
            ..Default::default()
        };
        let core = config.to_core_config();
        if let frogdb_core::ReplicationConfig::Replica { primary_addr } = core {
            assert_eq!(primary_addr, "192.168.1.1:6380");
        } else {
            panic!("Expected Replica config");
        }
    }

    #[test]
    fn test_tracing_config_to_metrics_config() {
        let config = frogdb_config::TracingConfig {
            enabled: true,
            otlp_endpoint: "http://example.com:4317".to_string(),
            sampling_rate: 0.1,
            service_name: "test-service".to_string(),
            scatter_gather_spans: true,
            shard_spans: false,
            persistence_spans: true,
            recent_traces_max: 50,
        };

        let metrics_config = config.to_metrics_config();
        assert!(metrics_config.enabled);
        assert_eq!(metrics_config.otlp_endpoint, "http://example.com:4317");
        assert_eq!(metrics_config.sampling_rate, 0.1);
    }
}
