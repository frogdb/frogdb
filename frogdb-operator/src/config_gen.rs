//! Generate frogdb.toml content from FrogDB CR spec.
//!
//! Config is built by populating the real `frogdb_config` section structs and
//! serializing them with `toml::to_string`. Because those structs carry the
//! server schema's `#[serde(rename_all = "kebab-case")]` renames, the emitted
//! keys always match what the server accepts, and any future server-side
//! rename/addition becomes a **compile error** here rather than a runtime parse
//! failure on a deployed pod.

use std::path::PathBuf;

use frogdb_config::{
    ClusterConfigSection, LogOutput, LoggingConfig, MemoryConfig, MetricsConfig, PersistenceConfig,
    ServerConfig,
};
use serde::Serialize;

use crate::crd::FrogDBConfigSpec;

/// Serializable projection of `frogdb_config::Config` covering exactly the
/// sections the operator controls. Field names match `frogdb_config::Config`'s
/// section names so the emitted TOML deserializes back through the real server
/// schema unchanged.
#[derive(Serialize)]
struct GeneratedConfig {
    server: ServerConfig,
    logging: LoggingConfig,
    persistence: PersistenceConfig,
    metrics: MetricsConfig,
    memory: MemoryConfig,
}

/// Serializable projection for the cluster overlay (the `[cluster]` section).
#[derive(Serialize)]
struct GeneratedClusterConfig {
    cluster: ClusterConfigSection,
}

/// Generate a frogdb.toml string from the CR config spec.
pub fn generate_toml(config: &FrogDBConfigSpec) -> String {
    let generated = GeneratedConfig {
        server: ServerConfig {
            // Fixed operator choice: listen on all interfaces inside the pod.
            bind: "0.0.0.0".to_string(),
            port: config.port,
            num_shards: config.num_shards,
            ..Default::default()
        },
        logging: LoggingConfig {
            level: config.logging.level.clone(),
            // Fixed operator choice: structured logs to stdout for log collectors.
            format: "json".to_string(),
            output: LogOutput::Stdout,
            ..Default::default()
        },
        persistence: PersistenceConfig {
            enabled: config.persistence.enabled,
            // Fixed operator choice: data lives on the mounted PVC at /data.
            data_dir: PathBuf::from("/data"),
            // `durability-mode` is a plain String in the server schema (validated
            // at server startup against DURABILITY_MODES), so the CRD value is
            // passed through verbatim; an invalid value surfaces as a boot-time
            // validation error, not a generation error.
            durability_mode: config.persistence.durability_mode.clone(),
            ..Default::default()
        },
        metrics: MetricsConfig {
            // The server schema expresses "metrics disabled" as `enabled = false`,
            // so we always emit the section and carry the CRD flag through. (The
            // old template omitted the section when disabled, which would have let
            // the server default `enabled = true` win — a semantic bug.)
            enabled: config.metrics.enabled,
            bind: "0.0.0.0".to_string(),
            port: config.metrics.port,
            ..Default::default()
        },
        memory: MemoryConfig {
            maxmemory: config.memory.maxmemory,
            // `maxmemory-policy` is likewise a plain String validated at startup.
            maxmemory_policy: config.memory.policy.clone(),
            ..Default::default()
        },
    };

    toml::to_string(&generated).expect("frogdb config sections always serialize to TOML")
}

/// Generate cluster-specific configuration overrides (the `[cluster]` section).
#[allow(dead_code)]
pub fn cluster_env_toml(
    bus_port: u16,
    election_timeout_ms: u64,
    heartbeat_interval_ms: u64,
    auto_failover: bool,
) -> String {
    let generated = GeneratedClusterConfig {
        cluster: ClusterConfigSection {
            enabled: true,
            cluster_bus_addr: format!("0.0.0.0:{bus_port}"),
            election_timeout_ms,
            heartbeat_interval_ms,
            auto_failover,
            ..Default::default()
        },
    };

    toml::to_string(&generated).expect("cluster config always serializes to TOML")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Round-trip a generated config string back through the real server schema.
    fn parse(toml_str: &str) -> frogdb_config::Config {
        toml::from_str(toml_str).expect("generated TOML must parse as frogdb_config::Config")
    }

    #[test]
    fn test_generate_toml_defaults() {
        let spec = FrogDBConfigSpec::default();
        let config = parse(&generate_toml(&spec));

        assert_eq!(config.server.bind, "0.0.0.0");
        assert_eq!(config.server.port, 6379);
        assert_eq!(config.server.num_shards, 1);
        assert_eq!(config.logging.level, "info");
        assert_eq!(config.logging.format, "json");
        assert_eq!(config.logging.output, LogOutput::Stdout);
        assert!(config.persistence.enabled);
        assert_eq!(config.persistence.data_dir, PathBuf::from("/data"));
        assert_eq!(config.persistence.durability_mode, "periodic");
        assert!(config.metrics.enabled);
        assert_eq!(config.metrics.port, 9090);
        assert_eq!(config.memory.maxmemory, 0);
        assert_eq!(config.memory.maxmemory_policy, "noeviction");
    }

    #[test]
    fn test_generate_toml_custom() {
        let spec = FrogDBConfigSpec {
            port: 6380,
            num_shards: 4,
            ..Default::default()
        };
        let config = parse(&generate_toml(&spec));

        assert_eq!(config.server.port, 6380);
        assert_eq!(config.server.num_shards, 4);
    }

    #[test]
    fn test_cluster_env_toml() {
        let config = parse(&cluster_env_toml(16379, 1000, 250, true));

        assert!(config.cluster.enabled);
        assert_eq!(config.cluster.cluster_bus_addr, "0.0.0.0:16379");
        assert_eq!(config.cluster.election_timeout_ms, 1000);
        assert_eq!(config.cluster.heartbeat_interval_ms, 250);
        assert!(config.cluster.auto_failover);
    }
}
