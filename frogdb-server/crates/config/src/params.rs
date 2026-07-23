//! CONFIG GET/SET parameter metadata registry.
//!
//! This module provides a lightweight registry mapping CONFIG GET/SET
//! parameter names to their TOML config paths and runtime mutability.
//! It is the single source of truth consumed by:
//! - `docs-gen` (documentation generation)
//! - `runtime_config.rs` (runtime CONFIG command handling)
//!
//! # Assembly (derive-macro migration, complete)
//!
//! Historically this file was one hand-maintained 61-row table, independent of
//! the serde section structs — a field could be added to a struct with no
//! matching row and nothing failed. Every struct-backed section now carries
//! `#[derive(ConfigParams)]`, where every field must declare `#[param(...)]` or
//! `#[param(skip)]` (an unannotated field is a compile error). Each struct emits
//! a `PARAMS` table, and [`config_param_registry`] assembles the final list from:
//! 1. the derived per-section `PARAMS` tables (`MemoryConfig`, `ServerConfig`,
//!    `PersistenceConfig`, `LoggingConfig`, `ReplicationConfigSection`,
//!    `SlowlogConfig`, `SecurityConfig`, `MetricsConfig`, `TlsConfig`), and
//! 2. the hand-maintained [`VIRTUAL_PARAMS`] rows.
//!
//! [`VIRTUAL_PARAMS`] is the single home for the rows that have no serde backing
//! field (`field: None`); these can never be derived from a struct.
//!
//! Row **order** is load-bearing (it fixes CONFIG HELP output). Because the
//! historical order interleaves individual section rows with virtual rows, the
//! assembly splices them by hand; the result is pinned byte-for-byte against a
//! snapshot of the original 61-row table by
//! `tests::test_registry_matches_golden_snapshot`.

/// Metadata for a CONFIG GET/SET parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

/// "Virtual" CONFIG params — rows with no serde-backed config field
/// (`section: None`, `field: None`). These cannot be produced by
/// `#[derive(ConfigParams)]` (there is no struct field to attach `#[param]` to),
/// so they stay a hand-maintained list. This is now the **single** home of these
/// rows: [`config_param_registry`] splices slices of this list in between the
/// derived per-section `PARAMS` tables to reproduce the historical ordering.
/// [`VIRTUAL_PARAMS`] is validated as a faithful subset of the assembled registry
/// by `tests::test_virtual_params_are_field_none_subset`.
pub const VIRTUAL_PARAMS: &[ConfigParamInfo] = &[
    ConfigParamInfo {
        name: "lua-time-limit",
        section: None,
        field: None,
        mutable: true,
        noop: false,
    },
    ConfigParamInfo {
        name: "notify-keyspace-events",
        section: None,
        field: None,
        mutable: true,
        noop: false,
    },
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
        name: "key-memory-histograms",
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
];

/// Look up a single derived row by CONFIG name within a section's `PARAMS`.
///
/// Used by [`config_param_registry`] to splice individual struct-derived rows
/// into the historical order (which interleaves sections rather than grouping
/// them). Panics if the name is absent — that can only happen if a `#[param]`
/// annotation was removed or renamed, which the golden-snapshot test also
/// catches, so a clear panic here is the friendlier failure.
fn pick(params: &'static [ConfigParamInfo], name: &str) -> ConfigParamInfo {
    *params
        .iter()
        .find(|p| p.name == name)
        .unwrap_or_else(|| panic!("CONFIG param '{name}' not found in derived section PARAMS"))
}

/// Returns the complete CONFIG GET/SET parameter registry.
///
/// This is the single source of truth for which parameters are exposed
/// via CONFIG GET/SET, whether they are mutable, and how they map to
/// TOML configuration fields.
///
/// The list is assembled once from two sources: the derived per-section `PARAMS`
/// tables (`#[derive(ConfigParams)]`) and the hand-maintained [`VIRTUAL_PARAMS`]
/// rows that have no serde backing. The historical CONFIG HELP order interleaves
/// individual section rows with virtual rows, so the assembly splices them by
/// hand (via [`pick`] for single rows and slices of [`VIRTUAL_PARAMS`]); the
/// exact order is pinned by `tests::test_registry_matches_golden_snapshot`.
pub fn config_param_registry() -> &'static [ConfigParamInfo] {
    use std::sync::LazyLock;

    static REGISTRY: LazyLock<Vec<ConfigParamInfo>> = LazyLock::new(|| {
        use crate::admin::AdminConfig;
        use crate::cluster::ClusterConfigSection;
        use crate::distributed_tracing::TracingConfig;
        use crate::http::HttpConfig;
        use crate::json::JsonConfig;
        use crate::latency::LatencyBandsConfig;
        use crate::logging::LoggingConfig;
        use crate::memory::MemoryConfig;
        use crate::metrics::MetricsConfig;
        use crate::persistence::{PersistenceConfig, SnapshotConfig};
        use crate::replication::ReplicationConfigSection;
        use crate::security::{AclFileConfig, SecurityConfig};
        use crate::server::ServerConfig;
        use crate::slowlog::SlowlogConfig;
        use crate::status::StatusConfig;
        use crate::tls::TlsConfig;

        let mut rows: Vec<ConfigParamInfo> = Vec::new();

        // The row order below is load-bearing: it reproduces the original
        // 61-row hand table byte-for-byte (see the golden-snapshot test). It
        // deliberately interleaves sections and virtual rows.

        // memory (all six rows, contiguous)
        rows.extend_from_slice(MemoryConfig::PARAMS);

        rows.push(pick(LoggingConfig::PARAMS, "loglevel"));
        rows.push(pick(PersistenceConfig::PARAMS, "durability-mode"));
        rows.push(pick(PersistenceConfig::PARAMS, "wal-failure-policy"));
        rows.push(pick(PersistenceConfig::PARAMS, "sync-interval-ms"));
        rows.push(pick(PersistenceConfig::PARAMS, "batch-timeout-ms"));
        rows.push(pick(ServerConfig::PARAMS, "scatter-gather-timeout-ms"));

        // replication: the two mutable rows only. 13-01 Pass 2b promoted four
        // more replication fields to immutable; they are spliced by name in the
        // Pass-2b block below, so this pair is picked by name (not by slice) to
        // keep the historical first-61 order byte-identical.
        rows.push(pick(
            ReplicationConfigSection::PARAMS,
            "min-replicas-to-write",
        ));
        rows.push(pick(
            ReplicationConfigSection::PARAMS,
            "min-replicas-max-lag",
        ));
        // slowlog (all three rows, contiguous and in field order)
        rows.extend_from_slice(SlowlogConfig::PARAMS);

        rows.push(pick(LoggingConfig::PARAMS, "per-request-spans"));

        rows.extend_from_slice(&VIRTUAL_PARAMS[0..2]); // lua-time-limit, notify-keyspace-events
        rows.push(pick(ServerConfig::PARAMS, "maxclients"));
        rows.extend_from_slice(&VIRTUAL_PARAMS[2..8]); // set/hash listpack threshold rows
        rows.push(pick(SecurityConfig::PARAMS, "requirepass"));
        rows.extend_from_slice(&VIRTUAL_PARAMS[8..25]); // save … zset-max-listpack-value

        rows.push(pick(ServerConfig::PARAMS, "bind"));
        rows.push(pick(ServerConfig::PARAMS, "port"));
        rows.push(pick(ServerConfig::PARAMS, "num-shards"));
        rows.push(pick(PersistenceConfig::PARAMS, "dir"));
        rows.push(pick(PersistenceConfig::PARAMS, "persistence-enabled"));
        rows.push(pick(PersistenceConfig::PARAMS, "flush-compact-range"));

        // metrics: the two originally-registered rows only, picked by name.
        // The issue-14 wire pass promoted three metrics OTLP fields to immutable;
        // they are spliced in the issue-14 block below (not by whole-section
        // slice) so the historical first-104 order stays byte-identical.
        rows.push(pick(MetricsConfig::PARAMS, "metrics-enabled"));
        rows.push(pick(MetricsConfig::PARAMS, "metrics-port"));

        // tls (8 rows; field order differs from the historical order, so splice
        // by name rather than by slice)
        rows.push(pick(TlsConfig::PARAMS, "tls-port"));
        rows.push(pick(TlsConfig::PARAMS, "tls-cert-file"));
        rows.push(pick(TlsConfig::PARAMS, "tls-key-file"));
        rows.push(pick(TlsConfig::PARAMS, "tls-ca-cert-file"));
        rows.push(pick(TlsConfig::PARAMS, "tls-auth-clients"));
        rows.push(pick(TlsConfig::PARAMS, "tls-replication"));
        rows.push(pick(TlsConfig::PARAMS, "tls-cluster"));
        rows.push(pick(TlsConfig::PARAMS, "tls-protocols"));

        // --- 13-01 Pass 2a: 22 promote-immutable rows appended below. ---
        // The 61 rows above keep their historical relative order untouched; the
        // audit's newly-exposed immutable (CONFIG GET-only) params are grouped by
        // section and appended here so the golden snapshot's first 61 rows are
        // stable. (26 rows were classified promote-immutable in Pass 1; 4 metrics
        // OTLP/bind rows were downgraded to justify as dead config — the OTLP
        // recorder is never wired and the metrics listener is superseded by the
        // `http` section — leaving 22 exposed here.)
        rows.push(pick(ServerConfig::PARAMS, "sorted-set-index"));
        rows.push(pick(ServerConfig::PARAMS, "enable-debug-command"));
        rows.push(pick(PersistenceConfig::PARAMS, "write-buffer-size-mb"));
        rows.push(pick(PersistenceConfig::PARAMS, "compression"));
        rows.push(pick(PersistenceConfig::PARAMS, "block-cache-size-mb"));
        rows.push(pick(PersistenceConfig::PARAMS, "bloom-filter-bits"));
        rows.push(pick(PersistenceConfig::PARAMS, "max-write-buffer-number"));
        // Pass 2a rows whose sections gained *more* params in Pass 2b are now
        // picked by name (not by whole-section slice) so the 22 Pass-2a rows keep
        // their exact positions and the Pass-2b additions land only in the
        // appended block below.
        rows.push(pick(SnapshotConfig::PARAMS, "snapshot-dir"));
        rows.extend_from_slice(HttpConfig::PARAMS); // http-enabled, http-bind, http-port
        rows.extend_from_slice(AdminConfig::PARAMS); // admin-enabled, admin-port, admin-bind
        rows.push(pick(TracingConfig::PARAMS, "tracing-enabled"));
        rows.push(pick(TracingConfig::PARAMS, "tracing-otlp-endpoint"));
        rows.push(pick(AclFileConfig::PARAMS, "aclfile"));
        rows.push(pick(ClusterConfigSection::PARAMS, "cluster-enabled"));
        rows.push(pick(ClusterConfigSection::PARAMS, "cluster-data-dir"));
        rows.push(pick(LatencyBandsConfig::PARAMS, "latency-bands"));
        rows.push(pick(TlsConfig::PARAMS, "tls-enabled"));
        rows.push(pick(LoggingConfig::PARAMS, "logfile"));

        // --- 13-01 Pass 2b: 20 promote-immutable (startup-consumed, CONFIG
        // GET-only) rows + 1 promote-mutable row (`acllog-max-len`), appended
        // after the Pass-2a block so the golden snapshot's first 83 rows stay
        // byte-identical. Every promotion here passed the Pass-2b propagation
        // audit: the 20 immutable rows are consumed once at startup (their
        // startup value is honest to report via GET, but a runtime SET has no
        // seam to reach the subsystem); `acllog-max-len` is genuinely live —
        // the ACL log length is re-read per append via the already-injected
        // `Arc<AclManager>`. (14 further Pass-1 promote-mutable candidates were
        // dead config and stayed `#[param(skip)]`.)
        rows.push(pick(PersistenceConfig::PARAMS, "compaction-rate-limit-mb"));
        rows.push(pick(PersistenceConfig::PARAMS, "batch-size-threshold-kb"));
        rows.push(pick(SnapshotConfig::PARAMS, "snapshot-interval-secs"));
        rows.push(pick(
            ReplicationConfigSection::PARAMS,
            "replication-lag-threshold-bytes",
        ));
        rows.push(pick(
            ReplicationConfigSection::PARAMS,
            "replication-lag-threshold-secs",
        ));
        rows.push(pick(
            ReplicationConfigSection::PARAMS,
            "self-fence-on-replica-loss",
        ));
        rows.push(pick(
            ReplicationConfigSection::PARAMS,
            "replica-freshness-timeout-ms",
        ));
        rows.push(pick(ClusterConfigSection::PARAMS, "cluster-auto-failover"));
        rows.push(pick(
            ClusterConfigSection::PARAMS,
            "cluster-self-fence-on-quorum-loss",
        ));
        rows.push(pick(ClusterConfigSection::PARAMS, "replica-priority"));
        rows.push(pick(TlsConfig::PARAMS, "tls-cluster-migration"));
        rows.push(pick(TlsConfig::PARAMS, "tls-client-cert-file"));
        rows.push(pick(TlsConfig::PARAMS, "tls-client-key-file"));
        rows.push(pick(TlsConfig::PARAMS, "tls-handshake-timeout-ms"));
        rows.push(pick(TracingConfig::PARAMS, "tracing-sampling-rate"));
        rows.extend_from_slice(StatusConfig::PARAMS); // status-{memory,connection}-warning-percent, status-durability-lag-{warning,critical}-ms
        rows.push(pick(LatencyBandsConfig::PARAMS, "latency-bands-enabled"));
        rows.push(pick(AclFileConfig::PARAMS, "acllog-max-len")); // mutable

        // --- issue-14 wire pass: 7 promote-immutable (startup-consumed, CONFIG
        // GET-only) rows, appended after the Pass-2b block so the golden
        // snapshot's first 104 rows stay byte-identical. Each field was wired to
        // a real startup consumer in the five config-wiring lanes — the metrics
        // OTLP recorder, the JSON handler limits (via CommandContext.json_limits),
        // the replica ACK tick cadence, and the TLS manager's ciphersuite
        // selection — and is consumed once at startup: GET reports the honest
        // startup value, SET has no runtime seam. ---
        rows.push(pick(MetricsConfig::PARAMS, "metrics-otlp-enabled"));
        rows.push(pick(MetricsConfig::PARAMS, "metrics-otlp-endpoint"));
        rows.push(pick(MetricsConfig::PARAMS, "metrics-otlp-interval-secs"));
        rows.extend_from_slice(JsonConfig::PARAMS); // json-max-depth, json-max-size
        rows.push(pick(
            ReplicationConfigSection::PARAMS,
            "repl-ack-interval-ms",
        ));
        rows.push(pick(TlsConfig::PARAMS, "tls-ciphersuites"));

        // --- issue-29: pub/sub slow-subscriber output-buffer bound. One
        // promote-immutable (startup-consumed, CONFIG GET-only) row, appended
        // after the issue-14 block so the golden snapshot's first 111 rows stay
        // byte-identical. Consumed once when a connection lazily allocates its
        // pub/sub channel (`PubSubIo::ensure_pubsub_channel`): GET reports the
        // honest startup value, SET has no runtime seam (existing channels keep
        // their limit). ---
        rows.push(pick(
            ServerConfig::PARAMS,
            "pubsub-output-buffer-hard-limit",
        ));

        rows
    });

    &REGISTRY
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Independent snapshot of the registry, captured verbatim. The first 61 rows
    /// are the original hand-written table captured before the derive-macro
    /// migration; 13-01 Pass 2a appended 22 promote-immutable rows (see the
    /// trailing block below). The assembled [`config_param_registry`] must equal
    /// this exactly — same rows, same order — so no migration step can silently
    /// change the registry a client sees.
    const GOLDEN_SNAPSHOT: &[ConfigParamInfo] = &[
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
            name: "maxmemory-clients",
            section: Some("memory"),
            field: Some("maxmemory-clients"),
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
            name: "notify-keyspace-events",
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
        ConfigParamInfo {
            name: "requirepass",
            section: Some("security"),
            field: Some("requirepass"),
            mutable: true,
            noop: false,
        },
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
            name: "key-memory-histograms",
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
            name: "flush-compact-range",
            section: Some("persistence"),
            field: Some("flush-compact-range"),
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
        // --- 13-01 Pass 2a: 22 promote-immutable rows (all CONFIG GET-only,
        // mutable: false), appended after the original 61 so their relative order
        // is stable and the first 61 rows above are byte-for-byte unchanged. ---
        ConfigParamInfo {
            name: "sorted-set-index",
            section: Some("server"),
            field: Some("sorted-set-index"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "enable-debug-command",
            section: Some("server"),
            field: Some("enable-debug-command"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "write-buffer-size-mb",
            section: Some("persistence"),
            field: Some("write-buffer-size-mb"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "compression",
            section: Some("persistence"),
            field: Some("compression"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "block-cache-size-mb",
            section: Some("persistence"),
            field: Some("block-cache-size-mb"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "bloom-filter-bits",
            section: Some("persistence"),
            field: Some("bloom-filter-bits"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "max-write-buffer-number",
            section: Some("persistence"),
            field: Some("max-write-buffer-number"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "snapshot-dir",
            section: Some("snapshot"),
            field: Some("snapshot-dir"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "http-enabled",
            section: Some("http"),
            field: Some("enabled"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "http-bind",
            section: Some("http"),
            field: Some("bind"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "http-port",
            section: Some("http"),
            field: Some("port"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "admin-enabled",
            section: Some("admin"),
            field: Some("enabled"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "admin-port",
            section: Some("admin"),
            field: Some("port"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "admin-bind",
            section: Some("admin"),
            field: Some("bind"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "tracing-enabled",
            section: Some("tracing"),
            field: Some("enabled"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "tracing-otlp-endpoint",
            section: Some("tracing"),
            field: Some("otlp-endpoint"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "aclfile",
            section: Some("acl"),
            field: Some("aclfile"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "cluster-enabled",
            section: Some("cluster"),
            field: Some("enabled"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "cluster-data-dir",
            section: Some("cluster"),
            field: Some("data-dir"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "latency-bands",
            section: Some("latency-bands"),
            field: Some("bands"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "tls-enabled",
            section: Some("tls"),
            field: Some("enabled"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "logfile",
            section: Some("logging"),
            field: Some("file-path"),
            mutable: false,
            noop: false,
        },
        // --- 13-01 Pass 2b: 20 promote-immutable rows + 1 promote-mutable row
        // (`acllog-max-len`), appended after the Pass-2a block. ---
        ConfigParamInfo {
            name: "compaction-rate-limit-mb",
            section: Some("persistence"),
            field: Some("compaction-rate-limit-mb"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "batch-size-threshold-kb",
            section: Some("persistence"),
            field: Some("batch-size-threshold-kb"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "snapshot-interval-secs",
            section: Some("snapshot"),
            field: Some("snapshot-interval-secs"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "replication-lag-threshold-bytes",
            section: Some("replication"),
            field: Some("replication-lag-threshold-bytes"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "replication-lag-threshold-secs",
            section: Some("replication"),
            field: Some("replication-lag-threshold-secs"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "self-fence-on-replica-loss",
            section: Some("replication"),
            field: Some("self-fence-on-replica-loss"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "replica-freshness-timeout-ms",
            section: Some("replication"),
            field: Some("replica-freshness-timeout-ms"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "cluster-auto-failover",
            section: Some("cluster"),
            field: Some("auto-failover"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "cluster-self-fence-on-quorum-loss",
            section: Some("cluster"),
            field: Some("self-fence-on-quorum-loss"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "replica-priority",
            section: Some("cluster"),
            field: Some("replica-priority"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "tls-cluster-migration",
            section: Some("tls"),
            field: Some("tls-cluster-migration"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "tls-client-cert-file",
            section: Some("tls"),
            field: Some("client-cert-file"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "tls-client-key-file",
            section: Some("tls"),
            field: Some("client-key-file"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "tls-handshake-timeout-ms",
            section: Some("tls"),
            field: Some("handshake-timeout-ms"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "tracing-sampling-rate",
            section: Some("tracing"),
            field: Some("sampling-rate"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "status-memory-warning-percent",
            section: Some("status"),
            field: Some("memory-warning-percent"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "status-connection-warning-percent",
            section: Some("status"),
            field: Some("connection-warning-percent"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "status-durability-lag-warning-ms",
            section: Some("status"),
            field: Some("durability-lag-warning-ms"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "status-durability-lag-critical-ms",
            section: Some("status"),
            field: Some("durability-lag-critical-ms"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "latency-bands-enabled",
            section: Some("latency-bands"),
            field: Some("enabled"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "acllog-max-len",
            section: Some("acl"),
            field: Some("log-max-len"),
            mutable: true,
            noop: false,
        },
        // --- issue-14 wire pass: 7 promote-immutable rows (all CONFIG GET-only,
        // mutable: false), appended after the Pass-2b block so the first 104 rows
        // above stay byte-for-byte unchanged. ---
        ConfigParamInfo {
            name: "metrics-otlp-enabled",
            section: Some("metrics"),
            field: Some("otlp-enabled"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "metrics-otlp-endpoint",
            section: Some("metrics"),
            field: Some("otlp-endpoint"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "metrics-otlp-interval-secs",
            section: Some("metrics"),
            field: Some("otlp-interval-secs"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "json-max-depth",
            section: Some("json"),
            field: Some("max-depth"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "json-max-size",
            section: Some("json"),
            field: Some("max-size"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "repl-ack-interval-ms",
            section: Some("replication"),
            field: Some("ack-interval-ms"),
            mutable: false,
            noop: false,
        },
        ConfigParamInfo {
            name: "tls-ciphersuites",
            section: Some("tls"),
            field: Some("ciphersuites"),
            mutable: false,
            noop: false,
        },
        // issue-29: pub/sub slow-subscriber output-buffer bound (immutable).
        ConfigParamInfo {
            name: "pubsub-output-buffer-hard-limit",
            section: Some("server"),
            field: Some("pubsub-output-buffer-hard-limit"),
            mutable: false,
            noop: false,
        },
    ];

    #[test]
    fn test_registry_matches_golden_snapshot() {
        let registry = config_param_registry();
        assert_eq!(
            registry.len(),
            GOLDEN_SNAPSHOT.len(),
            "assembled registry has {} rows, golden snapshot has {}",
            registry.len(),
            GOLDEN_SNAPSHOT.len()
        );
        for (i, (got, want)) in registry.iter().zip(GOLDEN_SNAPSHOT.iter()).enumerate() {
            assert_eq!(
                got, want,
                "registry row {i} diverged from golden snapshot:\n  got:  {got:?}\n  want: {want:?}"
            );
        }
    }

    #[test]
    fn test_golden_snapshot_row_count() {
        // Guards against accidental edits to the snapshot itself. The original
        // migration captured 61 rows; 13-01 Pass 2a appended 22 promote-immutable
        // rows (26 classified, minus 4 metrics OTLP/bind rows downgraded to justify
        // as dead config), giving 83. 13-01 Pass 2b appended 21 more (20
        // promote-immutable startup-consumed rows + the mutable `acllog-max-len`),
        // giving 104. The issue-14 wire pass appended 7 more promote-immutable
        // rows (metrics OTLP ×3, json limits ×2, replica ACK cadence, TLS
        // ciphersuites), giving 111. Issue-29 appended 1 more promote-immutable
        // row (`pubsub-output-buffer-hard-limit`), giving 112.
        assert_eq!(GOLDEN_SNAPSHOT.len(), 112);
    }

    #[test]
    fn test_virtual_params_are_field_none_subset() {
        // Every virtual param must have no serde backing, and must appear in the
        // live registry exactly as declared. This keeps the Phase 2 scaffold
        // honest before it is wired into the assembly.
        let registry = config_param_registry();
        for vp in VIRTUAL_PARAMS {
            assert!(
                vp.section.is_none() && vp.field.is_none(),
                "virtual param '{}' must have section: None and field: None",
                vp.name
            );
            assert!(
                registry.contains(vp),
                "virtual param '{}' not found (identically) in the assembled registry",
                vp.name
            );
        }
    }

    /// UI-lite exercise of `#[derive(ConfigParams)]` defaults, `mutable`,
    /// `name` override, `#[serde(rename)]` field-name resolution, `noop`
    /// (section/field cleared), and `skip` (row omitted).
    #[derive(serde::Serialize, serde::Deserialize, frogdb_config_derive::ConfigParams)]
    #[params(section = "smoke")]
    #[serde(rename_all = "kebab-case")]
    #[allow(dead_code)]
    struct DeriveSmoke {
        #[param(mutable)]
        some_flag: u64,
        #[param]
        plain_field: u64,
        #[serde(rename = "renamed-field")]
        #[param(mutable, name = "custom-name")]
        weird: u64,
        #[param(mutable, noop)]
        legacy_knob: u64,
        #[param(skip)]
        internal: u64,
    }

    #[test]
    fn test_derive_smoke_params() {
        let expected = &[
            ConfigParamInfo {
                name: "some-flag",
                section: Some("smoke"),
                field: Some("some-flag"),
                mutable: true,
                noop: false,
            },
            ConfigParamInfo {
                name: "plain-field",
                section: Some("smoke"),
                field: Some("plain-field"),
                mutable: false,
                noop: false,
            },
            ConfigParamInfo {
                name: "custom-name",
                section: Some("smoke"),
                field: Some("renamed-field"),
                mutable: true,
                noop: false,
            },
            ConfigParamInfo {
                name: "legacy-knob",
                section: None,
                field: None,
                mutable: true,
                noop: true,
            },
        ];
        assert_eq!(DeriveSmoke::PARAMS, expected);
    }

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

    /// Per-STRUCT coverage guard (issue 13-02).
    ///
    /// `#[derive(ConfigSections)]` on the root [`crate::Config`] emits
    /// `Config::SECTION_PARAMS` — one `PARAMS` slice per `#[section]` field. This
    /// asserts the hand-spliced [`config_param_registry`] covers **exactly** that
    /// derived set of section rows (the serde-backed rows; virtual rows are
    /// excluded, being disjoint by `section: None`).
    ///
    /// This closes the gap the derive's compile-time completeness link cannot
    /// catch: the link only forces a `#[section]` field's type to *have*
    /// `PARAMS`, not that the assembly actually *splices* it in. A new section
    /// struct classified `#[section]` but never wired into `config_param_registry`
    /// leaves its rows in `SECTION_PARAMS` yet absent from the registry, turning
    /// the silent hole into a red test.
    #[test]
    fn test_registry_covers_derived_sections() {
        let registry = config_param_registry();

        // Registry rows with a serde backing (non-virtual). Virtual rows have
        // `section: None` and are disjoint from section rows, so filtering them
        // out by identity leaves exactly the section-derived rows.
        let mut registry_section_rows: Vec<ConfigParamInfo> = registry
            .iter()
            .filter(|p| !VIRTUAL_PARAMS.contains(p))
            .copied()
            .collect();

        // Union of every derived section's PARAMS (sections with all-`skip`
        // fields contribute an empty slice and drop out).
        let mut derived_rows: Vec<ConfigParamInfo> = crate::Config::SECTION_PARAMS
            .iter()
            .flat_map(|params| params.iter())
            .copied()
            .collect();

        // Param names are globally unique, so sorting by name yields a stable
        // total order and set-equality reduces to sorted-Vec equality.
        registry_section_rows.sort_by_key(|p| p.name);
        derived_rows.sort_by_key(|p| p.name);

        assert_eq!(
            registry_section_rows, derived_rows,
            "config_param_registry() must cover exactly the section set derived from \
             Config::SECTION_PARAMS — a new #[section] struct not wired into the \
             assembly (or an assembly row from a non-derived section) breaks this",
        );
    }

    /// Guards the [`crate::Config`] `#[derive(ConfigSections)]` against silently
    /// dropping a section, and documents the current section count.
    #[test]
    fn test_section_params_counts_every_section() {
        #[cfg(not(feature = "turmoil"))]
        assert_eq!(
            crate::Config::SECTION_PARAMS.len(),
            24,
            "expected one SECTION_PARAMS entry per #[section] field of Config"
        );
        #[cfg(feature = "turmoil")]
        assert_eq!(
            crate::Config::SECTION_PARAMS.len(),
            25,
            "expected one SECTION_PARAMS entry per #[section] field of Config (incl. chaos)"
        );
    }

    /// UI-lite exercise of `#[derive(ConfigSections)]`: `#[section(skip)]`
    /// contributes nothing, and `#[section]` contributes exactly its field type's
    /// `PARAMS`. A field missing both attributes is a compile error (the point of
    /// the derive); that path is documented rather than unit-tested, since the
    /// crate has no trybuild harness.
    #[derive(frogdb_config_derive::ConfigSections)]
    #[allow(dead_code)]
    struct SectionsSmoke {
        #[section(skip)]
        not_a_section: u64,
        #[section]
        smoke: DeriveSmoke,
    }

    #[test]
    fn test_derive_sections_smoke() {
        assert_eq!(SectionsSmoke::SECTION_PARAMS.len(), 1);
        assert_eq!(SectionsSmoke::SECTION_PARAMS[0], DeriveSmoke::PARAMS);
    }
}
