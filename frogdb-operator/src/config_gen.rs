//! Generate frogdb.toml content from FrogDB CR spec.

use crate::crd::FrogDBConfigSpec;

/// Generate a frogdb.toml string from the CR config spec.
pub fn generate_toml(config: &FrogDBConfigSpec) -> String {
    let mut sections = Vec::new();

    sections.push(format!(
        r#"[server]
bind = "0.0.0.0"
port = {}
num_shards = {}"#,
        config.port, config.num_shards
    ));

    sections.push(format!(
        r#"[logging]
level = "{}"
format = "json"
output = "stdout""#,
        config.logging.level
    ));

    sections.push(format!(
        r#"[persistence]
enabled = {}
data_dir = "/data"
durability_mode = "{}""#,
        config.persistence.enabled, config.persistence.durability_mode
    ));

    if config.metrics.enabled {
        sections.push(format!(
            r#"[metrics]
enabled = true
bind = "0.0.0.0"
port = {}"#,
            config.metrics.port
        ));
    }

    sections.push(format!(
        r#"[memory]
maxmemory = {}
maxmemory_policy = "{}""#,
        config.memory.maxmemory, config.memory.policy
    ));

    sections.join("\n\n")
}

/// Generate cluster-specific environment variable overrides.
#[allow(dead_code)]
pub fn cluster_env_toml(
    bus_port: u16,
    election_timeout_ms: u64,
    heartbeat_interval_ms: u64,
    auto_failover: bool,
) -> String {
    format!(
        r#"[cluster]
enabled = true
cluster_bus_addr = "0.0.0.0:{bus_port}"
election_timeout_ms = {election_timeout_ms}
heartbeat_interval_ms = {heartbeat_interval_ms}
auto_failover = {auto_failover}"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_toml_defaults() {
        let config = FrogDBConfigSpec::default();
        let toml = generate_toml(&config);
        assert!(toml.contains("port = 6379"));
        assert!(toml.contains("num_shards = 1"));
        assert!(toml.contains("enabled = true"));
        assert!(toml.contains(r#"level = "info""#));
        assert!(toml.contains(r#"maxmemory_policy = "noeviction""#));
    }

    #[test]
    fn test_generate_toml_custom() {
        let config = FrogDBConfigSpec {
            port: 6380,
            num_shards: 4,
            ..Default::default()
        };
        let toml = generate_toml(&config);
        assert!(toml.contains("port = 6380"));
        assert!(toml.contains("num_shards = 4"));
    }

    #[test]
    fn test_cluster_env_toml() {
        let toml = cluster_env_toml(16379, 1000, 250, true);
        assert!(toml.contains("enabled = true"));
        assert!(toml.contains("cluster_bus_addr = \"0.0.0.0:16379\""));
        assert!(toml.contains("auto_failover = true"));
    }
}
