//! CONFIG GET/SET parameter metadata registry.
//!
//! This module provides a lightweight registry mapping CONFIG GET/SET
//! parameter names to their TOML config paths and runtime mutability.
//! It is the single source of truth consumed by:
//! - `docs-gen` (documentation generation)
//! - `runtime_config.rs` (runtime CONFIG command handling)

/// Metadata for a CONFIG GET/SET parameter.
#[derive(Debug, Clone, Copy)]
pub struct ConfigParamInfo {
    /// CONFIG GET/SET parameter name (kebab-case).
    pub name: &'static str,
    /// TOML section this param belongs to (e.g., "memory", "persistence").
    /// `None` for Redis compat no-op params that don't map to a TOML field.
    pub section: Option<&'static str>,
    /// TOML field name within the section (kebab-case, matching serde rename).
    /// `None` for no-op params or params that don't map 1:1 to a config field.
    pub field: Option<&'static str>,
    /// Whether this parameter can be changed at runtime via CONFIG SET.
    pub mutable: bool,
    /// Whether this is a Redis compatibility no-op (not used by FrogDB internally).
    pub noop: bool,
}

/// Returns the complete CONFIG GET/SET parameter registry.
///
/// This is the single source of truth for which parameters are exposed
/// via CONFIG GET/SET, whether they are mutable, and how they map to
/// TOML configuration fields.
pub fn config_param_registry() -> &'static [ConfigParamInfo] {
    &[
        // === Mutable parameters backed by TOML config fields ===
        ConfigParamInfo {
            name: "maxmemory",
            section: Some("memory"),
            field: Some("maxmemory"),
            mutable: true,
            noop: false,
        },
        ConfigParamInfo {
            name: "maxmemory-policy",
            section: Some("memory"),
            field: Some("maxmemory-policy"),
            mutable: true,
            noop: false,
        },
        ConfigParamInfo {
            name: "maxmemory-samples",
            section: Some("memory"),
            field: Some("maxmemory-samples"),
            mutable: true,
            noop: false,
        },
        ConfigParamInfo {
            name: "lfu-log-factor",
            section: Some("memory"),
            field: Some("lfu-log-factor"),
            mutable: true,
            noop: false,
        },
        ConfigParamInfo {
            name: "lfu-decay-time",
            section: Some("memory"),
            field: Some("lfu-decay-time"),
            mutable: true,
            noop: false,
        },
        ConfigParamInfo {
            name: "loglevel",
            section: Some("logging"),
            field: Some("level"),
            mutable: true,
            noop: false,
        },
        ConfigParamInfo {
            name: "durability-mode",
            section: Some("persistence"),
            field: Some("durability-mode"),
            mutable: true,
            noop: false,
        },
        ConfigParamInfo {
            name: "wal-failure-policy",
            section: Some("persistence"),
            field: Some("wal-failure-policy"),
            mutable: true,
            noop: false,
        },
        ConfigParamInfo {
            name: "sync-interval-ms",
            section: Some("persistence"),
            field: Some("sync-interval-ms"),
            mutable: true,
            noop: false,
        },
        ConfigParamInfo {
            name: "batch-timeout-ms",
            section: Some("persistence"),
            field: Some("batch-timeout-ms"),
            mutable: true,
            noop: false,
        },
        ConfigParamInfo {
            name: "scatter-gather-timeout-ms",
            section: Some("server"),
            field: Some("scatter-gather-timeout-ms"),
            mutable: true,
            noop: false,
        },
        ConfigParamInfo {
            name: "min-replicas-to-write",
            section: Some("replication"),
            field: Some("min-replicas-to-write"),
            mutable: true,
            noop: false,
        },
        ConfigParamInfo {
            name: "min-replicas-max-lag",
            section: Some("replication"),
            field: Some("min-replicas-timeout-ms"),
            mutable: true,
            noop: false,
        },
        ConfigParamInfo {
            name: "slowlog-log-slower-than",
            section: Some("slowlog"),
            field: Some("log-slower-than"),
            mutable: true,
            noop: false,
        },
        ConfigParamInfo {
            name: "slowlog-max-len",
            section: Some("slowlog"),
            field: Some("max-len"),
            mutable: true,
            noop: false,
        },
        ConfigParamInfo {
            name: "slowlog-max-arg-len",
            section: Some("slowlog"),
            field: Some("max-arg-len"),
            mutable: true,
            noop: false,
        },
        ConfigParamInfo {
            name: "per-request-spans",
            section: Some("logging"),
            field: Some("per-request-spans"),
            mutable: true,
            noop: false,
        },
        ConfigParamInfo {
            name: "lua-time-limit",
            section: None,
            field: None,
            mutable: true,
            noop: false,
        },
        ConfigParamInfo {
            name: "maxclients",
            section: Some("server"),
            field: Some("max-clients"),
            mutable: true,
            noop: false,
        },
        // === Mutable listpack/encoding threshold params ===
        ConfigParamInfo {
            name: "set-max-listpack-entries",
            section: None,
            field: None,
            mutable: true,
            noop: false,
        },
        ConfigParamInfo {
            name: "set-max-listpack-value",
            section: None,
            field: None,
            mutable: true,
            noop: false,
        },
        ConfigParamInfo {
            name: "hash-max-ziplist-entries",
            section: None,
            field: None,
            mutable: true,
            noop: false,
        },
        ConfigParamInfo {
            name: "hash-max-ziplist-value",
            section: None,
            field: None,
            mutable: true,
            noop: false,
        },
        ConfigParamInfo {
            name: "hash-max-listpack-entries",
            section: None,
            field: None,
            mutable: true,
            noop: false,
        },
        ConfigParamInfo {
            name: "hash-max-listpack-value",
            section: None,
            field: None,
            mutable: true,
            noop: false,
        },
        // === Security / ACL params ===
        ConfigParamInfo {
            name: "requirepass",
            section: Some("security"),
            field: Some("requirepass"),
            mutable: true,
            noop: false,
        },
        // === Redis compat no-op mutable params ===
        ConfigParamInfo {
            name: "save",
            section: None,
            field: None,
            mutable: true,
            noop: true,
        },
        ConfigParamInfo {
            name: "set-max-intset-entries",
            section: None,
            field: None,
            mutable: true,
            noop: true,
        },
        ConfigParamInfo {
            name: "list-max-listpack-size",
            section: None,
            field: None,
            mutable: true,
            noop: true,
        },
        ConfigParamInfo {
            name: "list-compress-depth",
            section: None,
            field: None,
            mutable: true,
            noop: true,
        },
        ConfigParamInfo {
            name: "list-max-ziplist-size",
            section: None,
            field: None,
            mutable: true,
            noop: true,
        },
        ConfigParamInfo {
            name: "latency-tracking",
            section: None,
            field: None,
            mutable: true,
            noop: false,
        },
        ConfigParamInfo {
            name: "latency-tracking-info-percentiles",
            section: None,
            field: None,
            mutable: true,
            noop: false,
        },
        ConfigParamInfo {
            name: "latency-monitor-threshold",
            section: None,
            field: None,
            mutable: true,
            noop: true,
        },
        ConfigParamInfo {
            name: "busy-reply-threshold",
            section: None,
            field: None,
            mutable: true,
            noop: true,
        },
        ConfigParamInfo {
            name: "hz",
            section: None,
            field: None,
            mutable: true,
            noop: true,
        },
        ConfigParamInfo {
            name: "activedefrag",
            section: None,
            field: None,
            mutable: true,
            noop: true,
        },
        ConfigParamInfo {
            name: "close-on-oom",
            section: None,
            field: None,
            mutable: true,
            noop: true,
        },
        ConfigParamInfo {
            name: "zset-max-ziplist-entries",
            section: None,
            field: None,
            mutable: true,
            noop: true,
        },
        ConfigParamInfo {
            name: "zset-max-ziplist-value",
            section: None,
            field: None,
            mutable: true,
            noop: true,
        },
        ConfigParamInfo {
            name: "zset-max-listpack-entries",
            section: None,
            field: None,
            mutable: true,
            noop: true,
        },
        ConfigParamInfo {
            name: "zset-max-listpack-value",
            section: None,
            field: None,
            mutable: true,
            noop: true,
        },
        // === Immutable parameters backed by TOML config fields ===
        ConfigParamInfo {
            name: "bind",
            section: Some("server"),
            field: Some("bind"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "port",
            section: Some("server"),
            field: Some("port"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "num-shards",
            section: Some("server"),
            field: Some("num-shards"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "dir",
            section: Some("persistence"),
            field: Some("data-dir"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "persistence-enabled",
            section: Some("persistence"),
            field: Some("enabled"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "metrics-enabled",
            section: Some("metrics"),
            field: Some("enabled"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "metrics-port",
            section: Some("metrics"),
            field: Some("port"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "tls-port",
            section: Some("tls"),
            field: Some("tls-port"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "tls-cert-file",
            section: Some("tls"),
            field: Some("cert-file"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "tls-key-file",
            section: Some("tls"),
            field: Some("key-file"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "tls-ca-cert-file",
            section: Some("tls"),
            field: Some("ca-file"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "tls-auth-clients",
            section: Some("tls"),
            field: Some("require-client-cert"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "tls-replication",
            section: Some("tls"),
            field: Some("tls-replication"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "tls-cluster",
            section: Some("tls"),
            field: Some("tls-cluster"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "tls-protocols",
            section: Some("tls"),
            field: Some("protocols"),
            mutable: false,
            noop: false,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_duplicate_param_names() {
        let registry = config_param_registry();
        let mut seen = std::collections::HashSet::new();
        for param in registry {
            assert!(
                seen.insert(param.name),
                "duplicate param name: {}",
                param.name
            );
        }
    }

    #[test]
    fn test_noop_params_are_mutable() {
        for param in config_param_registry() {
            if param.noop {
                assert!(
                    param.mutable,
                    "noop param '{}' should be mutable",
                    param.name
                );
            }
        }
    }

    #[test]
    fn test_noop_params_have_no_config_path() {
        for param in config_param_registry() {
            if param.noop {
                assert!(
                    param.section.is_none() && param.field.is_none(),
                    "noop param '{}' should not map to a config field",
                    param.name
                );
            }
        }
    }
}
