//! Runtime configuration for CONFIG GET/SET commands.
//!
//! This module provides:
//! - `RuntimeConfig` - mutable parameters that can be changed at runtime
//! - `ConfigManager` - main interface for CONFIG commands
//! - `ShardConfigNotifier` - propagates config changes to shards
//! - Parameter registry with metadata for each configurable parameter

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering};

use frogdb_core::persistence::WalFailurePolicy;
use frogdb_core::{
    EvictionConfig, EvictionPolicy, KeyspaceEventFlags, ObservabilityMsg, ShardSender, glob_match,
};
use tokio::sync::oneshot;
use toml_edit::Value as TomlValue;
use tracing::{info, warn};

use crate::config::Config;
use crate::config_persister::{ConfigPersister, ConfigUpdate};

use frogdb_config::{
    ClientCertMode, ConfigParam, DynParam, ImmutableParamId, MutableParamId, Propagation,
    TlsProtocol,
};

/// CONFIG error type, defined alongside the parameter lifecycle in `frogdb-config`
/// and re-exported here so existing `runtime_config::ConfigError` paths keep working.
pub use frogdb_config::ConfigError;

/// The context type the parameter-lifecycle closures reach state through.
///
/// `ConfigParam`/`DynParam` are generic over this so the lightweight config crate
/// need not name a server type; the server supplies its own [`ConfigManager`].
///
/// Stored as `Box<dyn TomlRenderable>` rather than `Box<dyn DynParam<ConfigManager>>`
/// directly: `TomlRenderable` is a supertrait of `DynParam<ConfigManager>` (so every
/// existing `.name()`/`.get()`/`.set()`/`.propagation()` call site is unaffected),
/// and it additionally lets CONFIG REWRITE ask each parameter for a genuinely-typed
/// [`toml_edit::Value`] instead of re-guessing one from a formatted string.
type Param = Box<dyn TomlRenderable>;

/// A runtime value that knows how to render itself as a correctly-typed TOML value.
///
/// `frogdb-config` (where [`ConfigParam`] lives) intentionally has no `toml_edit`
/// dependency, so this conversion -- and the [`TomlRenderable`] blanket impl that
/// uses it -- stays local to the server crate. This is what lets CONFIG REWRITE
/// render a TOML bool/int/string from each parameter's own `T` instead of the old
/// `string_to_toml_value` heuristic, which re-guessed the type from a formatted
/// string and would e.g. coerce a `String`-typed value like `maxmemory-clients = "0"`
/// into a TOML integer.
trait ToTomlValue {
    /// The value as TOML, or `None` when the parameter is *unset* and its
    /// config-file key must therefore be absent (see [`OptionalPathValue`]).
    fn to_toml_value(&self) -> Option<TomlValue>;
}

/// Implements [`ToTomlValue`] for integer types by widening to `i64`, the only
/// integer representation `toml_edit::Value` has.
macro_rules! impl_to_toml_value_via_i64 {
    ($($t:ty),+ $(,)?) => {
        $(
            impl ToTomlValue for $t {
                fn to_toml_value(&self) -> Option<TomlValue> {
                    Some(TomlValue::from(*self as i64))
                }
            }
        )+
    };
}
impl_to_toml_value_via_i64!(u8, u16, u32, u64, usize, i32, i64);

impl ToTomlValue for f64 {
    fn to_toml_value(&self) -> Option<TomlValue> {
        Some(TomlValue::from(*self))
    }
}

impl ToTomlValue for bool {
    fn to_toml_value(&self) -> Option<TomlValue> {
        Some(TomlValue::from(*self))
    }
}

impl ToTomlValue for String {
    fn to_toml_value(&self) -> Option<TomlValue> {
        Some(TomlValue::from(self.as_str()))
    }
}

impl ToTomlValue for EvictionPolicy {
    fn to_toml_value(&self) -> Option<TomlValue> {
        Some(TomlValue::from(self.as_str()))
    }
}

impl ToTomlValue for ClientCertMode {
    /// Renders the *file* encoding (`#[serde(rename_all = "lowercase")]`:
    /// `"none"`/`"optional"`/`"required"`), which differs from the Redis-style
    /// CONFIG GET display value (`"no"`/`"optional"`/`"yes"`, see
    /// [`StaticConfig::from_config`]) -- they serve different protocols.
    fn to_toml_value(&self) -> Option<TomlValue> {
        let s = match self {
            ClientCertMode::None => "none",
            ClientCertMode::Optional => "optional",
            ClientCertMode::Required => "required",
        };
        Some(TomlValue::from(s))
    }
}

impl ToTomlValue for Vec<TlsProtocol> {
    fn to_toml_value(&self) -> Option<TomlValue> {
        Some(
            self.iter()
                .map(|p| match p {
                    TlsProtocol::Tls12 => "1.2",
                    TlsProtocol::Tls13 => "1.3",
                })
                .collect(),
        )
    }
}

impl ToTomlValue for Vec<f64> {
    /// Renders a TOML array of floats. Only `latency-tracking-info-percentiles`
    /// has this type; it carries no file mapping (`section`/`field` are `None`),
    /// so this is never reached by CONFIG REWRITE -- it exists solely to satisfy
    /// the `TomlRenderable` bound shared by every entry in `typed_params`.
    fn to_toml_value(&self) -> Option<TomlValue> {
        Some(self.iter().copied().collect())
    }
}

impl ToTomlValue for Vec<u64> {
    /// Renders a TOML array of integers. Backs the immutable `latency-bands`
    /// param (13-01), whose file field `latency-bands.bands` is a TOML int array.
    fn to_toml_value(&self) -> Option<TomlValue> {
        Some(self.iter().map(|&v| v as i64).collect())
    }
}

impl ToTomlValue for Vec<String> {
    /// Renders a TOML array of strings. Backs the immutable `tls-ciphersuites`
    /// param (issue-14), whose file field `tls.ciphersuites` is a TOML string
    /// array of rustls IANA ciphersuite names.
    fn to_toml_value(&self) -> Option<TomlValue> {
        Some(self.iter().map(|s| s.as_str()).collect())
    }
}

/// Extension of [`DynParam`] that additionally renders a parameter's live value
/// as a genuinely-typed [`toml_edit::Value`] for CONFIG REWRITE.
///
/// The blanket impl below reaches back into each [`ConfigParam`]'s own `T` via
/// [`ToTomlValue`], so this is never a re-guess from a rendered string: a bool
/// param renders a TOML bool, an int param a TOML int, and so on.
trait TomlRenderable: DynParam<ConfigManager> {
    /// Render the live value as a properly-typed TOML value, or `None` when
    /// the parameter is unset and its config-file key must be absent.
    fn toml_value(&self, ctx: &ConfigManager) -> Option<TomlValue>;
}

impl<T> TomlRenderable for ConfigParam<T, ConfigManager>
where
    T: ToTomlValue + 'static,
{
    fn toml_value(&self, ctx: &ConfigManager) -> Option<TomlValue> {
        (self.get)(ctx).to_toml_value()
    }
}

/// A path-valued parameter whose *config-file* field is an `Option<PathBuf>`
/// (`tls.ca-file`, the TLS client pair, `logging.file-path`).
///
/// CONFIG GET/SET represent "unset" as the empty string, which is the only
/// choice the Redis protocol offers. The file cannot use the same encoding:
/// serde reads `ca-file = ""` back as `Some("")`, i.e. a file literally named
/// `""`, and boot validation then rejects the config the server itself wrote.
/// Wrapping the wire string in this type moves that knowledge into the
/// parameter's own type, so [`ToTomlValue`] can answer `None` and CONFIG
/// REWRITE removes the key instead of writing an empty one.
#[derive(Debug, Clone)]
struct OptionalPathValue(String);

impl ToTomlValue for OptionalPathValue {
    fn to_toml_value(&self) -> Option<TomlValue> {
        (!self.0.is_empty()).then(|| TomlValue::from(self.0.as_str()))
    }
}

/// The `min-replicas-max-lag` runtime value, in seconds.
///
/// CONFIG GET/SET operate in seconds (Redis compatibility), but the backing
/// TOML field is `min-replicas-timeout-ms` (milliseconds). Wrapping the
/// seconds value in this distinct type -- rather than a bare `u64` -- lets
/// [`ToTomlValue`] carry the seconds->ms conversion as part of *this
/// parameter's own type*, alongside its `parse`/`get`/`apply`/`render` in
/// [`ConfigManager::build_typed_params`], instead of the file writer having to
/// special-case this one parameter by name.
#[derive(Debug, Clone, Copy)]
struct MinReplicasMaxLagSecs(u64);

impl ToTomlValue for MinReplicasMaxLagSecs {
    fn to_toml_value(&self) -> Option<TomlValue> {
        self.0.saturating_mul(1000).to_toml_value()
    }
}

/// Type-erased closure for reloading the log filter.
type ReloadFn = Box<dyn Fn(&str) -> Result<(), String> + Send + Sync>;

/// Handle for reloading the log filter at runtime.
///
/// Uses a type-erased closure internally so it works with both `LevelFilter`
/// (production fast-path) and `EnvFilter` (RUST_LOG developer mode) regardless
/// of the subscriber layer stack.
pub struct LogReloadHandle {
    reload_fn: ReloadFn,
}

impl LogReloadHandle {
    /// Create a new reload handle wrapping a closure.
    pub fn new(reload_fn: ReloadFn) -> Self {
        Self { reload_fn }
    }

    /// Create a no-op handle (for tests or when logging isn't reloadable).
    pub fn noop() -> Self {
        Self {
            reload_fn: Box::new(|_| Ok(())),
        }
    }

    /// Reload the log filter with a new level string (e.g. "info", "debug").
    pub fn reload_level(&self, level: &str) -> Result<(), String> {
        (self.reload_fn)(level)
    }
}

/// Mutable runtime configuration values.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    // Memory settings
    pub maxmemory: u64,
    pub maxmemory_policy: EvictionPolicy,
    pub maxmemory_samples: usize,
    pub lfu_log_factor: u8,
    pub lfu_decay_time: u64,

    // Logging settings
    pub loglevel: String,

    // Persistence settings
    pub durability_mode: String,
    pub sync_interval_ms: u64,
    pub batch_timeout_ms: u64,

    // Server settings
    pub scatter_gather_timeout_ms: u64,

    // Replication settings
    pub min_replicas_to_write: u32,
    pub min_replicas_timeout_ms: u64,

    // Slowlog settings
    pub slowlog_log_slower_than: i64,
    pub slowlog_max_len: usize,
    pub slowlog_max_arg_len: usize,

    // Client memory limit
    pub maxmemory_clients: String,
}

impl RuntimeConfig {
    /// Create from the initial config.
    pub fn from_config(config: &Config) -> Self {
        Self {
            maxmemory: config.memory.maxmemory,
            // The policy string is validated at startup (config loader), so this
            // parse always succeeds; default to NoEviction defensively otherwise.
            maxmemory_policy: config.memory.maxmemory_policy.parse().unwrap_or_default(),
            maxmemory_samples: config.memory.maxmemory_samples,
            lfu_log_factor: config.memory.lfu_log_factor,
            lfu_decay_time: config.memory.lfu_decay_time,
            loglevel: config.logging.level.clone(),
            durability_mode: config.persistence.durability_mode.clone(),
            sync_interval_ms: config.persistence.sync_interval_ms,
            batch_timeout_ms: config.persistence.batch_timeout_ms,
            scatter_gather_timeout_ms: config.server.scatter_gather_timeout_ms,
            min_replicas_to_write: config.replication.min_replicas_to_write,
            min_replicas_timeout_ms: config.replication.min_replicas_timeout_ms,
            slowlog_log_slower_than: config.slowlog.log_slower_than,
            slowlog_max_len: config.slowlog.max_len,
            slowlog_max_arg_len: config.slowlog.max_arg_len,
            maxmemory_clients: config.memory.maxmemory_clients.clone(),
        }
    }
}

/// Immutable configuration values (for reference only).
#[derive(Debug, Clone)]
pub struct StaticConfig {
    pub bind: String,
    pub port: u16,
    pub num_shards: usize,
    pub data_dir: String,
    pub persistence_enabled: bool,
    /// Whether FLUSHDB/FLUSHALL is followed by an eager async
    /// DeleteFilesInRange + CompactRange to reclaim disk (proposal 48).
    pub flush_compact_range: bool,
    pub enable_debug_command: bool,
    pub metrics_enabled: bool,
    pub metrics_port: u16,
    pub strict_config: bool,
    pub tls_enabled: bool,
    pub tls_port: u16,
    pub tls_cert_file: String,
    pub tls_key_file: String,
    pub tls_ca_file: String,
    /// Redis-style CONFIG GET display value ("no"/"optional"/"yes").
    pub tls_auth_clients: String,
    /// The underlying typed value, kept alongside `tls_auth_clients` so CONFIG
    /// REWRITE can render the TOML file's own encoding ("none"/"optional"/
    /// "required") instead of the CONFIG GET display string.
    pub tls_require_client_cert: ClientCertMode,
    pub tls_replication: bool,
    pub tls_cluster: bool,
    /// Redis-style CONFIG GET display value ("TLSv1.2 TLSv1.3").
    pub tls_protocols: String,
    /// The underlying typed list, kept alongside `tls_protocols` so CONFIG
    /// REWRITE can render a proper TOML array in the file's own encoding
    /// ("1.2"/"1.3") instead of the CONFIG GET display string.
    pub tls_protocol_list: Vec<TlsProtocol>,

    // --- 13-01 Pass 2a: immutable (CONFIG GET-only) startup-fixed params ---
    /// Sorted-set index backend, rendered in the file's own encoding
    /// ("skiplist"/"btreemap") for both CONFIG GET and REWRITE.
    pub sorted_set_index: String,
    /// RocksDB write buffer size in MB (applied at DB open).
    pub write_buffer_size_mb: usize,
    /// RocksDB column-family compression ("none"/"snappy"/"lz4"/"zstd").
    pub compression: String,
    /// RocksDB block cache size in MB (applied at DB open).
    pub block_cache_size_mb: usize,
    /// RocksDB bloom filter bits per key (0 = disabled).
    pub bloom_filter_bits: i32,
    /// RocksDB maximum number of write buffers.
    pub max_write_buffer_number: i32,
    /// Snapshot output directory.
    pub snapshot_dir: String,
    /// Whether the HTTP observability/admin server is enabled.
    pub http_enabled: bool,
    /// HTTP server bind address.
    pub http_bind: String,
    /// HTTP server port.
    pub http_port: u16,
    /// Whether the admin RESP listener is enabled.
    pub admin_enabled: bool,
    /// Admin RESP listener port.
    pub admin_port: u16,
    /// Admin RESP listener bind address.
    pub admin_bind: String,
    /// Whether distributed tracing is enabled.
    pub tracing_enabled: bool,
    /// Distributed-tracing OTLP export endpoint.
    pub tracing_otlp_endpoint: String,
    /// ACL file path (empty when unset).
    pub aclfile: String,
    /// Whether cluster mode is enabled.
    pub cluster_enabled: bool,
    /// Cluster (Raft) state directory.
    pub cluster_data_dir: String,
    /// Latency-band thresholds in milliseconds (SLO monitoring).
    pub latency_bands: Vec<u64>,
    /// Log file path (empty when logging to console only).
    pub logfile: String,

    // --- 13-01 Pass 2b: immutable (CONFIG GET-only) startup-consumed params ---
    // Each is copied once from `Config` at startup; CONFIG GET reports that
    // startup value (honest — it is what the server runs with), but they carry
    // no runtime-SET seam, so they live here rather than in the mutable registry.
    /// RocksDB background-compaction rate limit in MB/s (0 = unlimited).
    pub compaction_rate_limit_mb: u64,
    /// WAL flush batch-size threshold in KB.
    pub batch_size_threshold_kb: usize,
    /// Periodic snapshot interval in seconds (0 = disabled).
    pub snapshot_interval_secs: u64,
    /// Replica byte-lag disconnect threshold.
    pub replication_lag_threshold_bytes: u64,
    /// Replica time-lag disconnect threshold in seconds.
    pub replication_lag_threshold_secs: u64,
    /// Whether the primary self-fences (refuses writes) on replica loss.
    pub self_fence_on_replica_loss: bool,
    /// Replica freshness window in ms used by self-fencing.
    pub replica_freshness_timeout_ms: u64,
    /// Whether dual-accept TLS cluster migration mode is enabled.
    pub tls_cluster_migration: bool,
    /// Outgoing (replication/cluster) client certificate path (empty when unset).
    pub tls_client_cert_file: String,
    /// Outgoing client private-key path (empty when unset).
    pub tls_client_key_file: String,
    /// TLS handshake timeout in ms.
    pub tls_handshake_timeout_ms: u64,

    // --- issue-14 wire pass: immutable (CONFIG GET-only) startup-consumed params ---
    /// Whether metrics OTLP export is enabled.
    pub metrics_otlp_enabled: bool,
    /// Metrics OTLP export endpoint URL.
    pub metrics_otlp_endpoint: String,
    /// Metrics OTLP push interval in seconds.
    pub metrics_otlp_interval_secs: u64,
    /// Maximum JSON document nesting depth.
    pub json_max_depth: usize,
    /// Maximum JSON document size in bytes.
    pub json_max_size: usize,
    /// Replica -> primary ACK cadence in milliseconds.
    pub repl_ack_interval_ms: u64,
    /// Allowed TLS ciphersuites (rustls IANA names; empty = rustls defaults).
    pub tls_ciphersuites: Vec<String>,
    /// Hard limit (bytes) on the per-connection pub/sub output buffer for a slow
    /// subscriber before messages are dropped and the connection is torn down.
    pub pubsub_output_buffer_hard_limit: usize,

    // --- config-mutability round: newly-exposed immutable params ---
    /// Whether the certificate file watcher is running. Immutable: the watcher
    /// task is spawned (or not) once at startup.
    pub tls_watch_certs: bool,
    /// Cert-watcher debounce window in ms. Immutable: consumed when the watcher
    /// task is spawned.
    pub tls_watch_debounce_ms: u64,
}

impl StaticConfig {
    /// Create from the initial config.
    pub fn from_config(config: &Config) -> Self {
        Self {
            bind: config.server.bind.clone(),
            port: config.server.port,
            num_shards: config.server.num_shards,
            data_dir: config.persistence.data_dir.display().to_string(),
            persistence_enabled: config.persistence.enabled,
            flush_compact_range: config.persistence.flush_compact_range,
            enable_debug_command: config.server.enable_debug_command,
            metrics_enabled: config.http.enabled,
            metrics_port: config.http.port,
            strict_config: config.compat.strict_config,
            tls_enabled: config.tls.enabled,
            tls_port: config.tls.tls_port,
            tls_cert_file: config.tls.cert_file.display().to_string(),
            tls_key_file: config.tls.key_file.display().to_string(),
            tls_ca_file: config
                .tls
                .ca_file
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_default(),
            tls_auth_clients: match config.tls.require_client_cert {
                ClientCertMode::None => "no".to_string(),
                ClientCertMode::Optional => "optional".to_string(),
                ClientCertMode::Required => "yes".to_string(),
            },
            tls_require_client_cert: config.tls.require_client_cert.clone(),
            tls_replication: config.tls.tls_replication,
            tls_cluster: config.tls.tls_cluster,
            tls_protocols: config
                .tls
                .protocols
                .iter()
                .map(|p| match p {
                    TlsProtocol::Tls12 => "TLSv1.2",
                    TlsProtocol::Tls13 => "TLSv1.3",
                })
                .collect::<Vec<_>>()
                .join(" "),
            tls_protocol_list: config.tls.protocols.clone(),
            // --- 13-01 Pass 2a: immutable startup-fixed params ---
            sorted_set_index: match config.server.sorted_set_index {
                crate::config::server::SortedSetIndexConfig::Skiplist => "skiplist".to_string(),
                crate::config::server::SortedSetIndexConfig::Btreemap => "btreemap".to_string(),
            },
            write_buffer_size_mb: config.persistence.write_buffer_size_mb,
            compression: config.persistence.compression.clone(),
            block_cache_size_mb: config.persistence.block_cache_size_mb,
            bloom_filter_bits: config.persistence.bloom_filter_bits,
            max_write_buffer_number: config.persistence.max_write_buffer_number,
            snapshot_dir: config.snapshot.snapshot_dir.display().to_string(),
            http_enabled: config.http.enabled,
            http_bind: config.http.bind.clone(),
            http_port: config.http.port,
            admin_enabled: config.admin.enabled,
            admin_port: config.admin.port,
            admin_bind: config.admin.bind.clone(),
            tracing_enabled: config.tracing.enabled,
            tracing_otlp_endpoint: config.tracing.otlp_endpoint.clone(),
            aclfile: config.acl.aclfile.clone(),
            cluster_enabled: config.cluster.enabled,
            cluster_data_dir: config.cluster.data_dir.display().to_string(),
            latency_bands: config.latency_bands.bands.clone(),
            logfile: config
                .logging
                .file_path
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_default(),
            // --- 13-01 Pass 2b: immutable startup-consumed params ---
            compaction_rate_limit_mb: config.persistence.compaction_rate_limit_mb,
            batch_size_threshold_kb: config.persistence.batch_size_threshold_kb,
            snapshot_interval_secs: config.snapshot.snapshot_interval_secs,
            replication_lag_threshold_bytes: config.replication.replication_lag_threshold_bytes,
            replication_lag_threshold_secs: config.replication.replication_lag_threshold_secs,
            self_fence_on_replica_loss: config.replication.self_fence_on_replica_loss,
            replica_freshness_timeout_ms: config.replication.replica_freshness_timeout_ms,
            tls_cluster_migration: config.tls.tls_cluster_migration,
            tls_client_cert_file: config
                .tls
                .client_cert_file
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_default(),
            tls_client_key_file: config
                .tls
                .client_key_file
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_default(),
            tls_handshake_timeout_ms: config.tls.handshake_timeout_ms,
            // --- issue-14 wire pass: immutable startup-consumed params ---
            metrics_otlp_enabled: config.metrics.otlp_enabled,
            metrics_otlp_endpoint: config.metrics.otlp_endpoint.clone(),
            metrics_otlp_interval_secs: config.metrics.otlp_interval_secs,
            json_max_depth: config.json.max_depth,
            json_max_size: config.json.max_size,
            repl_ack_interval_ms: config.replication.ack_interval_ms,
            tls_ciphersuites: config.tls.ciphersuites.clone(),
            pubsub_output_buffer_hard_limit: config.server.pubsub_output_buffer_hard_limit,
            // --- config-mutability round: newly-exposed immutable params ---
            tls_watch_certs: config.tls.watch_certs,
            tls_watch_debounce_ms: config.tls.watch_debounce_ms,
        }
    }
}

/// Render a bool as the Redis-style CONFIG GET display string ("yes"/"no").
fn yes_no(v: bool) -> String {
    if v { "yes" } else { "no" }.to_string()
}

/// Parse the Redis-style boolean spellings accepted by `CONFIG SET`.
fn parse_yes_no(param: &'static str, s: &str) -> Result<bool, ConfigError> {
    match s.to_lowercase().as_str() {
        "yes" | "true" | "1" | "on" => Ok(true),
        "no" | "false" | "0" | "off" => Ok(false),
        _ => Err(ConfigError::InvalidValue {
            param: param.to_string(),
            message: "must be yes/no".to_string(),
        }),
    }
}

/// Enforce the 1-100 bound the `[status]` section validator applies to its
/// warning percentages, so CONFIG SET and the config file agree on what is legal.
fn validate_percent(param: &'static str, v: u8) -> Result<(), ConfigError> {
    if v == 0 || v > 100 {
        Err(ConfigError::InvalidValue {
            param: param.to_string(),
            message: "must be between 1 and 100".to_string(),
        })
    } else {
        Ok(())
    }
}

/// Parse a 0-100 percentage for the `[hotshards]` thresholds, applying the same
/// range check as `HotShardsConfig::validate`.
fn parse_percent_f64(param: &'static str, s: &str) -> Result<f64, ConfigError> {
    let v = s.parse::<f64>().map_err(|_| ConfigError::InvalidValue {
        param: param.to_string(),
        message: "must be a number between 0 and 100".to_string(),
    })?;
    if !(0.0..=100.0).contains(&v) {
        return Err(ConfigError::InvalidValue {
            param: param.to_string(),
            message: "must be between 0 and 100".to_string(),
        });
    }
    Ok(v)
}

/// Rejection message shared by every TLS parameter when no TLS runtime exists.
const TLS_NOT_RUNNING: &str =
    "TLS is not running on this server; enable [tls] in the config file and restart";

/// One live TLS change, as requested by a TLS parameter's `apply`.
///
/// See [`ConfigManager::apply_tls`] for why this is a value rather than a
/// closure over `TlsRuntimeHandle`.
// The turmoil build compiles no TLS, so `apply_tls` rejects every mutation there
// without reading its payload.
#[cfg_attr(feature = "turmoil", allow(dead_code))]
enum TlsMutation {
    CertFile(PathBuf),
    KeyFile(PathBuf),
    CaFile(Option<PathBuf>),
    ClientCertFile(Option<PathBuf>),
    ClientKeyFile(Option<PathBuf>),
    Ciphersuites(Vec<String>),
    HandshakeTimeoutMs(u64),
    ClusterMigration(bool),
}

/// Interpret a CONFIG SET path value: an empty string clears an optional path.
fn optional_path(s: &str) -> Option<PathBuf> {
    if s.is_empty() {
        None
    } else {
        Some(PathBuf::from(s))
    }
}

/// Render an optional path the way CONFIG GET reports "unset": the empty string.
fn render_optional_path(p: &Option<PathBuf>) -> String {
    p.as_ref()
        .map(|p| p.display().to_string())
        .unwrap_or_default()
}

/// Read-only metadata for an immutable parameter served by CONFIG GET.
///
/// Every *mutable* parameter's full parse/validate/apply/render/propagation
/// lifecycle lives in the typed registry ([`ConfigManager::build_typed_params`]);
/// only immutable, restart-required parameters remain here, and they need nothing
/// but a string getter. Existence, mutability, and no-op gating come from the
/// config-crate metadata registry ([`frogdb_config::config_param_registry`]).
pub struct ParamMeta {
    /// Redis-style parameter name.
    pub name: &'static str,
    /// Get the current value as a string (CONFIG GET rendering).
    pub getter: fn(&ConfigManager) -> String,
    /// Render the current value as a correctly-typed TOML value (CONFIG
    /// REWRITE). Distinct from `getter` because the two protocols sometimes
    /// disagree on representation (e.g. `tls-auth-clients` reports Redis-style
    /// "no"/"yes" via `getter` but the TOML field needs "none"/"required").
    pub toml_getter: fn(&ConfigManager) -> Option<TomlValue>,
}

/// A Redis-compatibility no-op parameter.
///
/// Accepts any value on CONFIG SET (ignoring it) and reports a fixed Redis
/// default on CONFIG GET, so Redis test suites can set encoding thresholds
/// without aborting. FrogDB does not use these internally. Strict-config gating
/// still hides them via the metadata registry's `noop` flag.
///
/// Unlike [`ConfigParam`], the reported value is per-instance data, so this is a
/// small dedicated [`DynParam`] impl rather than a literal with function
/// pointers (which cannot capture the value).
struct NoopParam {
    name: &'static str,
    value: &'static str,
}

impl DynParam<ConfigManager> for NoopParam {
    fn name(&self) -> &'static str {
        self.name
    }

    fn get(&self, _ctx: &ConfigManager) -> String {
        self.value.to_string()
    }

    fn set(&self, _ctx: &ConfigManager, _raw: &str) -> Result<(), ConfigError> {
        Ok(())
    }

    fn propagation(&self) -> Propagation {
        Propagation::None
    }

    fn is_noop(&self) -> bool {
        true
    }
}

impl TomlRenderable for NoopParam {
    /// Never actually invoked: every no-op param in the metadata registry has
    /// `section: None, field: None`, so `ConfigManager::config_updates` filters
    /// them out before any renderer would be called. Implemented anyway (as a
    /// string, matching `get` above) so `NoopParam` satisfies the same trait
    /// object bound as every other entry in `typed_params`.
    fn toml_value(&self, _ctx: &ConfigManager) -> Option<TomlValue> {
        Some(TomlValue::from(self.value))
    }
}

/// Shared atomic listpack encoding thresholds.
///
/// These are read lock-free by shard workers during command execution
/// and written by CONFIG SET through the param registry.
pub struct ListpackAtomicConfig {
    pub hash_max_entries: AtomicU64,
    pub hash_max_value: AtomicU64,
    pub set_max_entries: AtomicU64,
    pub set_max_value: AtomicU64,
}

/// Configuration manager for CONFIG GET/SET commands.
pub struct ConfigManager {
    /// Mutable runtime configuration.
    runtime: Arc<RwLock<RuntimeConfig>>,
    /// Immutable static configuration.
    static_config: StaticConfig,
    /// Path to the TOML config file (None if using defaults only).
    config_file_path: RwLock<Option<PathBuf>>,
    /// Log level reload handle (optional, not available in tests).
    log_reload_handle: Option<LogReloadHandle>,
    /// Whether per-request tracing spans are enabled.
    /// Shared with all connections and shard workers via Arc.
    per_request_spans: Arc<AtomicBool>,
    /// Shared lua-time-limit value (readable by LuaVm timeout hooks).
    lua_time_limit: Arc<AtomicU64>,
    /// Listpack encoding thresholds (shared atomics, readable by shard workers).
    listpack: Arc<ListpackAtomicConfig>,
    /// WAL failure policy (0 = Continue, 1 = Rollback). Shared with shard workers.
    wal_failure_policy: Arc<AtomicU8>,
    /// Maximum simultaneous client connections (0 = unlimited). Shared with Acceptor.
    max_clients: Arc<AtomicU64>,
    /// Keyspace notification event flags (readable by shard workers without locking).
    /// Disabled (0) by default.
    notify_keyspace_events: Arc<AtomicU32>,
    /// ACL manager for requirepass CONFIG SET/GET support. Injected at
    /// construction so `requirepass` never silently no-ops.
    acl_manager: Arc<frogdb_core::AclManager>,
    /// Server-wide latency histograms. Injected at construction so
    /// `latency-tracking` toggles always reach the live histograms.
    latency_histograms: Arc<frogdb_core::CommandLatencyHistograms>,
    /// Configured percentiles for latency-tracking-info-percentiles.
    latency_tracking_percentiles: RwLock<Vec<f64>>,
    /// Key-memory histograms state.
    /// 0 = enabled (startup default), 1 = disabled at startup, 2 = disabled at runtime.
    key_memory_histograms_state: AtomicU8,
    /// Read-only parameter registry: string getters for immutable,
    /// restart-required parameters. All mutable parameters live in `typed_params`.
    params: Vec<ParamMeta>,
    /// Typed parameter-lifecycle registry. Each entry owns one parameter's whole
    /// parse/validate/apply/render/propagation lifecycle in a single literal.
    typed_params: Vec<Param>,
    /// Notifier for propagating eviction/histogram config changes to shards.
    /// Injected at construction so CONFIG SET propagation never silently no-ops.
    shard_notifier: Arc<ShardConfigNotifier>,
    /// Client registry for maxmemory-clients eviction on CONFIG SET. Injected at
    /// construction so eviction always fires.
    client_eviction_registry: Arc<frogdb_core::ClientRegistry>,
    /// Live `[cluster]` decision flags (auto-failover, self-fence, replica
    /// priority). Shared with the failure detector and the self-fence gate,
    /// which read them at decision time.
    cluster_flags: Arc<crate::cluster_flags::ClusterRuntimeFlags>,
    /// Live `[status]` health thresholds. Shared with the status collector, which
    /// classifies each `/status` report against the current values.
    status_thresholds: Arc<frogdb_telemetry::StatusThresholds>,
    /// Live OpenTelemetry sampling rate. Shared with the tracer's sampler, which
    /// reads it per sampling decision.
    tracing_sampling_rate: Arc<frogdb_telemetry::SamplingRate>,
    /// Latency-band tracker backing `latency-bands-enabled`. Injected at
    /// construction (the metrics recorder is built before this manager) so the
    /// toggle always reaches the live tracker.
    latency_band_tracker: Arc<frogdb_telemetry::LatencyBandTracker>,
    /// Live TLS runtime handle, published by server init once the TLS manager
    /// exists (which is after this manager is constructed and `Arc`-wrapped).
    ///
    /// This is the entry point for TLS CONFIG SET support: a param's `apply`
    /// closure calls [`ConfigManager::tls_runtime`] and, when it is present,
    /// invokes the matching `set_*` method on the handle
    /// (`set_cert_file`, `set_ciphersuites`, ...). `None` means TLS is
    /// disabled, so the corresponding CONFIG SET has nothing live to change.
    #[cfg(not(feature = "turmoil"))]
    tls_runtime: std::sync::OnceLock<Arc<crate::tls_runtime::TlsRuntimeHandle>>,
    /// Live hot-shard classification thresholds.
    ///
    /// Owned here from construction and *adopted* by the `HotShardCollector`
    /// (which is built later, in `start_subsystems`) via
    /// [`frogdb_debug::HotShardCollector::with_shared_config`], so both sides
    /// share one cell: `CONFIG SET hotshards-*` retunes the running collector
    /// and CONFIG GET reads back what the collector actually classifies with.
    hotshards: Arc<frogdb_debug::SharedHotShardConfig>,
    /// Live WAL flush batch-size threshold, in **bytes**.
    ///
    /// The wire parameter `batch-size-threshold-kb` is in KiB; this cell holds
    /// the byte value the flush threads compare against. Adopted by every
    /// `RocksWalWriter` through `WalConfig::batch_size_threshold_handle`, so all
    /// shards retune together. When persistence is disabled nothing adopts it
    /// and it simply records the configured value for GET/REWRITE.
    wal_batch_size_threshold: Arc<AtomicUsize>,
    /// Configured periodic-snapshot cadence, in seconds.
    ///
    /// This is the *authority* for CONFIG GET/REWRITE. The snapshot coordinator
    /// is built after this manager, so `apply` writes here first and then pushes
    /// the new cadence into [`Self::snapshot_coordinator`] when it is published.
    snapshot_interval_secs: Arc<AtomicU64>,
    /// Live snapshot coordinator, published by server init once persistence is
    /// up. `None` only before that point (and in unit tests).
    snapshot_coordinator:
        std::sync::OnceLock<Arc<dyn frogdb_core::persistence::SnapshotCoordinator>>,
    /// Configured primary replication lag thresholds (bytes / seconds).
    ///
    /// Authority for CONFIG GET/REWRITE on *every* node. The live
    /// [`frogdb_replication::LagThresholds`] exists only on primaries, so on a
    /// replica a SET is recorded here and takes effect if the node is later
    /// re-initialised as a primary.
    replication_lag_threshold_bytes: Arc<AtomicU64>,
    replication_lag_threshold_secs: Arc<AtomicU64>,
    /// Live primary-side lag thresholds; absent on replicas.
    replication_lag_thresholds: std::sync::OnceLock<Arc<frogdb_replication::LagThresholds>>,
    /// Configured replica-loss self-fence policy and freshness window.
    ///
    /// Same primary-only story as the lag thresholds: authority for GET/REWRITE
    /// here, pushed into the live quorum checker when one exists.
    self_fence_on_replica_loss: Arc<AtomicBool>,
    replica_freshness_timeout_ms: Arc<AtomicU64>,
    /// Live replication self-fence quorum checker; absent on replicas.
    replication_self_fence:
        std::sync::OnceLock<Arc<crate::replication_quorum::ReplicationQuorumChecker>>,
}

/// Bundle of live collaborators injected into [`ConfigManager`] at construction.
///
/// Passing these in (rather than wiring them through post-construction setters)
/// makes them non-optional: the side-effecting CONFIG SET paths -- requirepass
/// (ACL), maxmemory-clients (client eviction), latency-tracking (histograms),
/// and shard propagation -- can no longer silently no-op because a collaborator
/// was never wired.
pub struct ConfigCollaborators {
    /// ACL manager backing `requirepass` CONFIG GET/SET.
    pub acl_manager: Arc<frogdb_core::AclManager>,
    /// Server-wide latency histograms backing `latency-tracking`.
    pub latency_histograms: Arc<frogdb_core::CommandLatencyHistograms>,
    /// Client registry driving maxmemory-clients eviction.
    pub client_eviction_registry: Arc<frogdb_core::ClientRegistry>,
    /// Notifier propagating eviction/histogram changes to shards.
    ///
    /// This one borrows the ConfigManager's own runtime `Arc` (via
    /// [`ShardConfigNotifier::new`]), so the caller must build it from the same
    /// runtime handle passed to [`ConfigManager::with_collaborators`].
    pub shard_notifier: Arc<ShardConfigNotifier>,
    /// Latency-band tracker backing `latency-bands-enabled`.
    ///
    /// Injected (rather than built here) because the Prometheus recorder that
    /// records into it is constructed before the config manager exists.
    pub latency_band_tracker: Arc<frogdb_telemetry::LatencyBandTracker>,
}

impl ConfigCollaborators {
    /// Build a set of standalone default collaborators for tests and any caller
    /// that does not wire real subsystems.
    ///
    /// These are genuine null objects, not absent options: a fresh ACL manager,
    /// enabled histograms, an empty client registry, and a zero-shard notifier
    /// (which propagates to no shards but is a real, non-panicking notifier).
    /// The notifier shares `runtime` so its view stays consistent with the
    /// manager's.
    pub fn defaults(runtime: &Arc<RwLock<RuntimeConfig>>) -> Self {
        Self {
            acl_manager: frogdb_core::AclManager::new(Default::default()),
            latency_histograms: Arc::new(frogdb_core::CommandLatencyHistograms::new(true)),
            client_eviction_registry: Arc::new(frogdb_core::ClientRegistry::new()),
            shard_notifier: Arc::new(ShardConfigNotifier::new(
                Arc::new(Vec::new()),
                runtime.clone(),
                0,
            )),
            latency_band_tracker: Arc::new(frogdb_telemetry::LatencyBandTracker::new(
                Vec::new(),
                false,
            )),
        }
    }
}

impl ConfigManager {
    /// Create a new ConfigManager wired with standalone default collaborators.
    ///
    /// Used by tests and any caller that does not inject real subsystems. The
    /// production path is [`with_collaborators`](Self::with_collaborators), which
    /// supplies the live ACL manager, histograms, client registry, and shard
    /// notifier.
    pub fn new(config: &Config) -> Self {
        let runtime = Arc::new(RwLock::new(RuntimeConfig::from_config(config)));
        let collaborators = ConfigCollaborators::defaults(&runtime);
        Self::with_collaborators(config, runtime, collaborators)
    }

    /// Create a ConfigManager from the initial config and injected collaborators.
    ///
    /// `runtime` must be the same `Arc` used to build
    /// `collaborators.shard_notifier` (see [`ConfigCollaborators::shard_notifier`]),
    /// so the notifier and manager observe identical runtime state.
    pub fn with_collaborators(
        config: &Config,
        runtime: Arc<RwLock<RuntimeConfig>>,
        collaborators: ConfigCollaborators,
    ) -> Self {
        let static_config = StaticConfig::from_config(config);

        let wal_failure_policy_val =
            WalFailurePolicy::from_config_str(&config.persistence.wal_failure_policy).as_u8();

        let ConfigCollaborators {
            acl_manager,
            latency_histograms,
            client_eviction_registry,
            shard_notifier,
            latency_band_tracker,
        } = collaborators;

        Self {
            runtime,
            static_config,
            config_file_path: RwLock::new(config.config_source_path.clone()),
            log_reload_handle: None,
            per_request_spans: Arc::new(AtomicBool::new(config.logging.per_request_spans)),
            lua_time_limit: Arc::new(AtomicU64::new(5000)),
            listpack: Arc::new(ListpackAtomicConfig {
                hash_max_entries: AtomicU64::new(128),
                hash_max_value: AtomicU64::new(64),
                set_max_entries: AtomicU64::new(128),
                set_max_value: AtomicU64::new(64),
            }),
            wal_failure_policy: Arc::new(AtomicU8::new(wal_failure_policy_val)),
            max_clients: Arc::new(AtomicU64::new(config.server.max_clients as u64)),
            notify_keyspace_events: Arc::new(AtomicU32::new(0)),
            acl_manager,
            latency_histograms,
            latency_tracking_percentiles: RwLock::new(vec![50.0, 99.0, 99.9]),
            key_memory_histograms_state: AtomicU8::new(0), // enabled by default
            params: Self::build_param_registry(),
            typed_params: Self::build_typed_params(),
            shard_notifier,
            client_eviction_registry,
            cluster_flags: crate::cluster_flags::ClusterRuntimeFlags::from_config(&config.cluster),
            status_thresholds: crate::config::StatusConfigExt::to_thresholds(&config.status),
            tracing_sampling_rate: Arc::new(frogdb_telemetry::SamplingRate::new(
                config.tracing.sampling_rate,
            )),
            latency_band_tracker,
            #[cfg(not(feature = "turmoil"))]
            tls_runtime: std::sync::OnceLock::new(),
            hotshards: Arc::new(frogdb_debug::SharedHotShardConfig::new(
                &crate::config::HotShardsConfigExt::to_collector_config(&config.hotshards),
            )),
            wal_batch_size_threshold: Arc::new(AtomicUsize::new(
                config.persistence.batch_size_threshold_kb * 1024,
            )),
            snapshot_interval_secs: Arc::new(AtomicU64::new(
                config.snapshot.snapshot_interval_secs,
            )),
            snapshot_coordinator: std::sync::OnceLock::new(),
            replication_lag_threshold_bytes: Arc::new(AtomicU64::new(
                config.replication.replication_lag_threshold_bytes,
            )),
            replication_lag_threshold_secs: Arc::new(AtomicU64::new(
                config.replication.replication_lag_threshold_secs,
            )),
            replication_lag_thresholds: std::sync::OnceLock::new(),
            self_fence_on_replica_loss: Arc::new(AtomicBool::new(
                config.replication.self_fence_on_replica_loss,
            )),
            replica_freshness_timeout_ms: Arc::new(AtomicU64::new(
                config.replication.replica_freshness_timeout_ms,
            )),
            replication_self_fence: std::sync::OnceLock::new(),
        }
    }

    /// Live hot-shard thresholds, for the collector to adopt at startup.
    pub fn hotshard_config(&self) -> Arc<frogdb_debug::SharedHotShardConfig> {
        self.hotshards.clone()
    }

    /// Live WAL flush batch-size threshold (bytes), for `WalConfig` to adopt.
    pub fn wal_batch_size_threshold_handle(&self) -> Arc<AtomicUsize> {
        self.wal_batch_size_threshold.clone()
    }

    /// Publish the live snapshot coordinator.
    ///
    /// Takes `&self` for the same reason as [`Self::set_tls_runtime`]: the
    /// coordinator is built after this manager is `Arc`-wrapped. Called at most
    /// once; a second call is ignored. On publish the coordinator is
    /// immediately synced to the configured cadence, so a CONFIG SET that
    /// landed before publication is not lost.
    pub fn set_snapshot_coordinator(
        &self,
        coordinator: Arc<dyn frogdb_core::persistence::SnapshotCoordinator>,
    ) {
        if self.snapshot_coordinator.set(coordinator).is_ok() {
            let secs = self.snapshot_interval_secs.load(Ordering::Relaxed);
            if let Some(c) = self.snapshot_coordinator.get() {
                c.set_periodic_interval_secs(secs);
            }
        }
    }

    /// Publish the live primary-side replication lag thresholds.
    ///
    /// Primary-only: replicas never call this. Syncs the configured values into
    /// the handle on publish.
    pub fn set_replication_lag_thresholds(
        &self,
        thresholds: Arc<frogdb_replication::LagThresholds>,
    ) {
        if self.replication_lag_thresholds.set(thresholds).is_ok()
            && let Some(t) = self.replication_lag_thresholds.get()
        {
            t.set_threshold_bytes(self.replication_lag_threshold_bytes.load(Ordering::Relaxed));
            t.set_threshold_secs(self.replication_lag_threshold_secs.load(Ordering::Relaxed));
        }
    }

    /// Publish the live replication self-fence quorum checker.
    ///
    /// Primary-only: replicas never call this. Syncs the configured values into
    /// the checker on publish.
    pub fn set_replication_self_fence(
        &self,
        checker: Arc<crate::replication_quorum::ReplicationQuorumChecker>,
    ) {
        if self.replication_self_fence.set(checker).is_ok()
            && let Some(c) = self.replication_self_fence.get()
        {
            c.set_self_fence_enabled(self.self_fence_on_replica_loss.load(Ordering::Relaxed));
            c.set_freshness_timeout_ms(self.replica_freshness_timeout_ms.load(Ordering::Relaxed));
        }
    }

    /// Live `[cluster]` decision flags, for the failure detector and fence gate.
    pub fn cluster_flags(&self) -> Arc<crate::cluster_flags::ClusterRuntimeFlags> {
        self.cluster_flags.clone()
    }

    /// Live `[status]` health thresholds, for the status collector.
    pub fn status_thresholds(&self) -> Arc<frogdb_telemetry::StatusThresholds> {
        self.status_thresholds.clone()
    }

    /// Live OpenTelemetry sampling rate, for the tracer's sampler.
    pub fn tracing_sampling_rate_handle(&self) -> Arc<frogdb_telemetry::SamplingRate> {
        self.tracing_sampling_rate.clone()
    }

    /// Latency-band tracker backing `latency-bands-enabled`.
    pub fn latency_band_tracker(&self) -> Arc<frogdb_telemetry::LatencyBandTracker> {
        self.latency_band_tracker.clone()
    }

    /// Get the shared per_request_spans flag for connections and shard workers.
    pub fn per_request_spans_flag(&self) -> Arc<AtomicBool> {
        self.per_request_spans.clone()
    }

    /// Get the shared WAL failure policy flag for shard workers.
    /// Encoded via [`WalFailurePolicy::as_u8`].
    pub fn wal_failure_policy_flag(&self) -> Arc<AtomicU8> {
        self.wal_failure_policy.clone()
    }

    /// Set the log reload handle for dynamic log level changes.
    pub fn set_log_reload_handle(&mut self, handle: LogReloadHandle) {
        self.log_reload_handle = Some(handle);
    }

    /// Publish the live TLS runtime handle.
    ///
    /// Takes `&self` (not `&mut self`) because the TLS manager is built later in
    /// server init than the `ConfigManager`, by which point the manager is
    /// already behind an `Arc`. Called at most once; a second call is ignored.
    #[cfg(not(feature = "turmoil"))]
    pub fn set_tls_runtime(&self, handle: Arc<crate::tls_runtime::TlsRuntimeHandle>) {
        let _ = self.tls_runtime.set(handle);
    }

    /// The live TLS runtime handle, or `None` when TLS is disabled (or the
    /// manager was built without server init, as in unit tests).
    #[cfg(not(feature = "turmoil"))]
    pub fn tls_runtime(&self) -> Option<&Arc<crate::tls_runtime::TlsRuntimeHandle>> {
        self.tls_runtime.get()
    }

    /// The TLS configuration the running server is actually serving, or `None`
    /// when no TLS runtime exists (TLS disabled, a unit-test manager, or the
    /// `turmoil` build, which compiles no TLS at all).
    ///
    /// Every TLS parameter's CONFIG GET reads through this so a value that was
    /// changed by `CONFIG SET` is never reported from the startup snapshot.
    fn live_tls_config(&self) -> Option<frogdb_config::TlsConfig> {
        #[cfg(not(feature = "turmoil"))]
        {
            self.tls_runtime.get().map(|h| h.current_config())
        }
        #[cfg(feature = "turmoil")]
        {
            None
        }
    }

    /// Apply a TLS mutation to the live runtime handle.
    ///
    /// A missing handle is an *error*, not a silent no-op: TLS parameters only
    /// exist as live rustls state, so accepting the set with nothing to change
    /// would make the next CONFIG GET (which reads the live handle) report the
    /// old value. Handle errors -- a bad certificate path, an unparsable key, an
    /// unknown ciphersuite -- are surfaced verbatim; `TlsRuntimeHandle::apply`
    /// is build-then-commit, so a failed set leaves both the stored config and
    /// the certificates being served untouched.
    ///
    /// The mutation is passed as a [`TlsMutation`] value rather than a closure
    /// over the handle so that the `turmoil` build -- which compiles no TLS
    /// module at all -- needs a single `cfg` here instead of one per parameter.
    fn apply_tls(&self, param: &'static str, mutation: TlsMutation) -> Result<(), ConfigError> {
        let unavailable = || ConfigError::InvalidValue {
            param: param.to_string(),
            message: TLS_NOT_RUNNING.to_string(),
        };
        #[cfg(not(feature = "turmoil"))]
        {
            let handle = self.tls_runtime.get().ok_or_else(unavailable)?;
            let result = match mutation {
                TlsMutation::CertFile(p) => handle.set_cert_file(p),
                TlsMutation::KeyFile(p) => handle.set_key_file(p),
                TlsMutation::CaFile(p) => handle.set_ca_file(p),
                TlsMutation::ClientCertFile(p) => handle.set_client_cert_file(p),
                TlsMutation::ClientKeyFile(p) => handle.set_client_key_file(p),
                TlsMutation::Ciphersuites(s) => handle.set_ciphersuites(s),
                TlsMutation::HandshakeTimeoutMs(ms) => {
                    handle.set_handshake_timeout_ms(ms);
                    Ok(())
                }
                TlsMutation::ClusterMigration(on) => {
                    handle.set_cluster_migration(on);
                    Ok(())
                }
            };
            result.map_err(|e| ConfigError::InvalidValue {
                param: param.to_string(),
                message: e.to_string(),
            })
        }
        #[cfg(feature = "turmoil")]
        {
            let _ = mutation;
            Err(unavailable())
        }
    }

    /// Get the config file path.
    pub fn config_file_path(&self) -> Option<PathBuf> {
        self.config_file_path.read().unwrap().clone()
    }

    /// Set the config file path (used for CONFIG REWRITE).
    pub fn set_config_file_path(&self, path: PathBuf) {
        *self.config_file_path.write().unwrap() = Some(path);
    }

    /// Build the full set of already-typed CONFIG REWRITE updates from the
    /// param registry.
    ///
    /// For each registry entry with a TOML mapping, dispatches to the typed
    /// mutable-param renderer ([`typed_param_toml`](Self::typed_param_toml))
    /// or the immutable-param renderer
    /// ([`readonly_param`](Self::readonly_param)`.toml_getter`), so every
    /// value written to disk is genuinely typed by the parameter that owns
    /// it -- never re-guessed from a formatted string.
    fn config_updates(&self) -> Vec<ConfigUpdate> {
        frogdb_config::config_param_registry()
            .iter()
            .filter(|param| !param.noop)
            .filter_map(|param| {
                let (section, field) = match (param.section, param.field) {
                    (Some(s), Some(f)) => (s, f),
                    _ => return None,
                };
                // `None` here is not "skip this parameter": it is the value
                // *unset*, which `ConfigPersister::merge` renders by removing
                // the key (a previously-set path that has since been cleared
                // must not survive in the file).
                let value = if let Some(typed) = self.typed_param_toml(param.name) {
                    typed.toml_value(self)
                } else {
                    (self.readonly_param(param.name)?.toml_getter)(self)
                };
                Some(ConfigUpdate {
                    section,
                    field,
                    value,
                })
            })
            .collect()
    }

    /// Rewrite the config file, merging current runtime values into the TOML document.
    ///
    /// Preserves comments, formatting, and key ordering in the original file.
    /// Uses atomic write (temp file + fsync + rename) for safety.
    pub fn rewrite_config(&self) -> Result<(), String> {
        let config_path = self
            .config_file_path
            .read()
            .unwrap()
            .clone()
            .ok_or_else(|| "ERR The server is running without a config file".to_string())?;

        let contents = std::fs::read_to_string(&config_path).map_err(|e| {
            format!(
                "ERR failed to read config file '{}': {}",
                config_path.display(),
                e
            )
        })?;

        let merged = ConfigPersister::merge(&contents, self.config_updates()).map_err(|e| {
            format!(
                "ERR failed to parse config file '{}': {}",
                config_path.display(),
                e
            )
        })?;

        ConfigPersister::atomic_write(&config_path, &merged)?;

        info!(path = %config_path.display(), "Config file rewritten");
        Ok(())
    }

    /// Get the configured latency tracking percentiles.
    pub fn latency_tracking_percentiles(&self) -> Vec<f64> {
        self.latency_tracking_percentiles.read().unwrap().clone()
    }

    /// Get the data directory path.
    pub fn data_dir(&self) -> &str {
        &self.static_config.data_dir
    }

    /// Get current listpack configuration for hash/set encoding thresholds.
    pub fn listpack_config(&self) -> frogdb_core::ListpackConfig {
        frogdb_core::ListpackConfig {
            hash_max_entries: self.listpack.hash_max_entries.load(Ordering::Relaxed) as usize,
            hash_max_value: self.listpack.hash_max_value.load(Ordering::Relaxed) as usize,
            set_max_entries: self.listpack.set_max_entries.load(Ordering::Relaxed) as usize,
            set_max_value: self.listpack.set_max_value.load(Ordering::Relaxed) as usize,
        }
    }

    /// Check if persistence is enabled.
    pub fn persistence_enabled(&self) -> bool {
        self.static_config.persistence_enabled
    }

    /// Build the parameter registry.
    fn build_param_registry() -> Vec<ParamMeta> {
        ImmutableParamId::ALL
            .iter()
            .map(|&id| Self::readonly_param_meta(id))
            .collect()
    }

    /// Build the read-only [`ParamMeta`] getter for a single immutable parameter.
    ///
    /// Exhaustive over [`ImmutableParamId`]: a new immutable identity with no arm
    /// is a `non-exhaustive patterns` compile error, and a duplicated arm is an
    /// unreachable-pattern error. This is the compile-time replacement for the
    /// former runtime "every immutable metadata row is served by
    /// build_param_registry" partition check. The wire name comes from
    /// `id.name()`, pinning each literal to its identity.
    fn readonly_param_meta(id: ImmutableParamId) -> ParamMeta {
        use ImmutableParamId::*;
        match id {
            // Every mutable parameter now lives in the typed registry
            // (`build_typed_params`); only immutable, read-only parameters remain
            // in this legacy string-getter registry.
            Bind => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.bind.clone(),
                toml_getter: |mgr| mgr.static_config.bind.to_toml_value(),
            },
            Port => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.port.to_string(),
                toml_getter: |mgr| mgr.static_config.port.to_toml_value(),
            },
            NumShards => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.num_shards.to_string(),
                toml_getter: |mgr| mgr.static_config.num_shards.to_toml_value(),
            },
            Dir => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.data_dir.clone(),
                toml_getter: |mgr| mgr.static_config.data_dir.to_toml_value(),
            },
            PersistenceEnabled => ParamMeta {
                name: id.name(),
                getter: |mgr| {
                    if mgr.static_config.persistence_enabled {
                        "yes".to_string()
                    } else {
                        "no".to_string()
                    }
                },
                toml_getter: |mgr| mgr.static_config.persistence_enabled.to_toml_value(),
            },
            FlushCompactRange => ParamMeta {
                name: id.name(),
                getter: |mgr| {
                    if mgr.static_config.flush_compact_range {
                        "yes".to_string()
                    } else {
                        "no".to_string()
                    }
                },
                toml_getter: |mgr| mgr.static_config.flush_compact_range.to_toml_value(),
            },
            MetricsEnabled => ParamMeta {
                name: id.name(),
                getter: |mgr| {
                    if mgr.static_config.metrics_enabled {
                        "yes".to_string()
                    } else {
                        "no".to_string()
                    }
                },
                toml_getter: |mgr| mgr.static_config.metrics_enabled.to_toml_value(),
            },
            MetricsPort => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.metrics_port.to_string(),
                toml_getter: |mgr| mgr.static_config.metrics_port.to_toml_value(),
            },
            // TLS parameters (all read-only)
            TlsPort => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.tls_port.to_string(),
                toml_getter: |mgr| mgr.static_config.tls_port.to_toml_value(),
            },
            TlsAuthClients => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.tls_auth_clients.clone(),
                // Renders the TOML file's own enum encoding ("none"/"optional"/
                // "required"), not the CONFIG GET display string above -- see
                // `StaticConfig::tls_require_client_cert`.
                toml_getter: |mgr| mgr.static_config.tls_require_client_cert.to_toml_value(),
            },
            TlsReplication => ParamMeta {
                name: id.name(),
                getter: |mgr| {
                    if mgr.static_config.tls_replication {
                        "yes".to_string()
                    } else {
                        "no".to_string()
                    }
                },
                toml_getter: |mgr| mgr.static_config.tls_replication.to_toml_value(),
            },
            TlsCluster => ParamMeta {
                name: id.name(),
                getter: |mgr| {
                    if mgr.static_config.tls_cluster {
                        "yes".to_string()
                    } else {
                        "no".to_string()
                    }
                },
                toml_getter: |mgr| mgr.static_config.tls_cluster.to_toml_value(),
            },
            TlsProtocols => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.tls_protocols.clone(),
                // Renders a proper TOML array in the file's own encoding
                // ("1.2"/"1.3"), not the space-joined CONFIG GET display string
                // above -- see `StaticConfig::tls_protocol_list`.
                toml_getter: |mgr| mgr.static_config.tls_protocol_list.to_toml_value(),
            },

            // === 13-01 Pass 2a: promote-immutable params (CONFIG GET-only) ===
            SortedSetIndex => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.sorted_set_index.clone(),
                toml_getter: |mgr| mgr.static_config.sorted_set_index.to_toml_value(),
            },
            EnableDebugCommand => ParamMeta {
                name: id.name(),
                getter: |mgr| yes_no(mgr.static_config.enable_debug_command),
                toml_getter: |mgr| mgr.static_config.enable_debug_command.to_toml_value(),
            },
            WriteBufferSizeMb => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.write_buffer_size_mb.to_string(),
                toml_getter: |mgr| mgr.static_config.write_buffer_size_mb.to_toml_value(),
            },
            Compression => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.compression.clone(),
                toml_getter: |mgr| mgr.static_config.compression.to_toml_value(),
            },
            BlockCacheSizeMb => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.block_cache_size_mb.to_string(),
                toml_getter: |mgr| mgr.static_config.block_cache_size_mb.to_toml_value(),
            },
            BloomFilterBits => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.bloom_filter_bits.to_string(),
                toml_getter: |mgr| mgr.static_config.bloom_filter_bits.to_toml_value(),
            },
            MaxWriteBufferNumber => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.max_write_buffer_number.to_string(),
                toml_getter: |mgr| mgr.static_config.max_write_buffer_number.to_toml_value(),
            },
            SnapshotDir => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.snapshot_dir.clone(),
                toml_getter: |mgr| mgr.static_config.snapshot_dir.to_toml_value(),
            },
            HttpEnabled => ParamMeta {
                name: id.name(),
                getter: |mgr| yes_no(mgr.static_config.http_enabled),
                toml_getter: |mgr| mgr.static_config.http_enabled.to_toml_value(),
            },
            HttpBind => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.http_bind.clone(),
                toml_getter: |mgr| mgr.static_config.http_bind.to_toml_value(),
            },
            HttpPort => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.http_port.to_string(),
                toml_getter: |mgr| mgr.static_config.http_port.to_toml_value(),
            },
            AdminEnabled => ParamMeta {
                name: id.name(),
                getter: |mgr| yes_no(mgr.static_config.admin_enabled),
                toml_getter: |mgr| mgr.static_config.admin_enabled.to_toml_value(),
            },
            AdminPort => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.admin_port.to_string(),
                toml_getter: |mgr| mgr.static_config.admin_port.to_toml_value(),
            },
            AdminBind => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.admin_bind.clone(),
                toml_getter: |mgr| mgr.static_config.admin_bind.to_toml_value(),
            },
            TracingEnabled => ParamMeta {
                name: id.name(),
                getter: |mgr| yes_no(mgr.static_config.tracing_enabled),
                toml_getter: |mgr| mgr.static_config.tracing_enabled.to_toml_value(),
            },
            TracingOtlpEndpoint => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.tracing_otlp_endpoint.clone(),
                toml_getter: |mgr| mgr.static_config.tracing_otlp_endpoint.to_toml_value(),
            },
            Aclfile => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.aclfile.clone(),
                toml_getter: |mgr| mgr.static_config.aclfile.to_toml_value(),
            },
            ClusterEnabled => ParamMeta {
                name: id.name(),
                getter: |mgr| yes_no(mgr.static_config.cluster_enabled),
                toml_getter: |mgr| mgr.static_config.cluster_enabled.to_toml_value(),
            },
            ClusterDataDir => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.cluster_data_dir.clone(),
                toml_getter: |mgr| mgr.static_config.cluster_data_dir.to_toml_value(),
            },
            LatencyBands => ParamMeta {
                name: id.name(),
                // CONFIG GET renders the thresholds space-joined (Redis-style,
                // like `latency-tracking-info-percentiles`); CONFIG REWRITE writes
                // the file's own TOML int array via `Vec<u64>::to_toml_value`.
                getter: |mgr| {
                    mgr.static_config
                        .latency_bands
                        .iter()
                        .map(|b| b.to_string())
                        .collect::<Vec<_>>()
                        .join(" ")
                },
                toml_getter: |mgr| mgr.static_config.latency_bands.to_toml_value(),
            },
            TlsEnabled => ParamMeta {
                name: id.name(),
                getter: |mgr| yes_no(mgr.static_config.tls_enabled),
                toml_getter: |mgr| mgr.static_config.tls_enabled.to_toml_value(),
            },
            Logfile => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.logfile.clone(),
                // `logging.file-path` is `Option<PathBuf>`: no logfile means the
                // key is absent, not `file-path = ""` (which would ask the next
                // boot to log to a file named `""`).
                toml_getter: |mgr| {
                    OptionalPathValue(mgr.static_config.logfile.clone()).to_toml_value()
                },
            },
            // --- 13-01 Pass 2b: immutable startup-consumed params (GET-only) ---
            CompactionRateLimitMb => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.compaction_rate_limit_mb.to_string(),
                toml_getter: |mgr| mgr.static_config.compaction_rate_limit_mb.to_toml_value(),
            },
            // --- issue-14 wire pass: promote-immutable params (GET-only) ---
            MetricsOtlpEnabled => ParamMeta {
                name: id.name(),
                getter: |mgr| yes_no(mgr.static_config.metrics_otlp_enabled),
                toml_getter: |mgr| mgr.static_config.metrics_otlp_enabled.to_toml_value(),
            },
            MetricsOtlpEndpoint => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.metrics_otlp_endpoint.clone(),
                toml_getter: |mgr| mgr.static_config.metrics_otlp_endpoint.to_toml_value(),
            },
            MetricsOtlpIntervalSecs => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.metrics_otlp_interval_secs.to_string(),
                toml_getter: |mgr| mgr.static_config.metrics_otlp_interval_secs.to_toml_value(),
            },
            JsonMaxDepth => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.json_max_depth.to_string(),
                toml_getter: |mgr| mgr.static_config.json_max_depth.to_toml_value(),
            },
            JsonMaxSize => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.json_max_size.to_string(),
                toml_getter: |mgr| mgr.static_config.json_max_size.to_toml_value(),
            },
            ReplAckIntervalMs => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.repl_ack_interval_ms.to_string(),
                toml_getter: |mgr| mgr.static_config.repl_ack_interval_ms.to_toml_value(),
            },
            PubsubOutputBufferHardLimit => ParamMeta {
                name: id.name(),
                getter: |mgr| {
                    mgr.static_config
                        .pubsub_output_buffer_hard_limit
                        .to_string()
                },
                toml_getter: |mgr| {
                    mgr.static_config
                        .pubsub_output_buffer_hard_limit
                        .to_toml_value()
                },
            },
            // --- config-mutability round: newly-exposed immutable params ---
            // The cert watcher is a task spawned once at startup from these two
            // values; there is no live handle to retune, so both stay GET-only.
            TlsWatchCerts => ParamMeta {
                name: id.name(),
                getter: |mgr| yes_no(mgr.static_config.tls_watch_certs),
                toml_getter: |mgr| mgr.static_config.tls_watch_certs.to_toml_value(),
            },
            TlsWatchDebounceMs => ParamMeta {
                name: id.name(),
                getter: |mgr| mgr.static_config.tls_watch_debounce_ms.to_string(),
                toml_getter: |mgr| mgr.static_config.tls_watch_debounce_ms.to_toml_value(),
            },
        }
    }

    /// Build the typed parameter-lifecycle registry.
    ///
    /// Each entry is one [`ConfigParam`] literal that owns the whole lifecycle of
    /// one parameter (parse → validate → apply, plus render and propagation).
    /// This registry holds every mutable parameter; immutable, read-only
    /// parameters live in [`build_param_registry`](Self::build_param_registry).
    fn build_typed_params() -> Vec<Param> {
        MutableParamId::ALL
            .iter()
            .map(|&id| Self::build_typed_param(id))
            .collect()
    }

    /// Build the [`DynParam`] lifecycle for a single mutable parameter.
    ///
    /// Exhaustive over [`MutableParamId`]: a new mutable identity with no arm is
    /// a `non-exhaustive patterns` compile error, and a duplicated arm is an
    /// unreachable-pattern error. This is the compile-time replacement for the
    /// former runtime "every mutable metadata row is served by build_typed_params"
    /// partition check. The wire name comes from `id.name()`, pinning each literal
    /// to its identity.
    fn build_typed_param(id: MutableParamId) -> Param {
        use MutableParamId::*;
        match id {
            // === Memory / eviction family ===
            Maxmemory => Box::new(ConfigParam::<u64, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<u64>().map_err(|_| ConfigError::InvalidValue {
                        param: "maxmemory".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })
                },
                validate: ConfigParam::no_validate,
                default: || 0,
                get: |mgr| mgr.runtime.read().unwrap().maxmemory,
                apply: |mgr, v| {
                    mgr.runtime.write().unwrap().maxmemory = v;
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::Eviction,
            }),
            MaxmemoryPolicy => Box::new(ConfigParam::<EvictionPolicy, ConfigManager> {
                name: id.name(),
                // Legal values = whatever `EvictionPolicy::from_str` accepts; the
                // enum is the single source of truth. The message lists
                // `all_names()` to stay byte-identical with the prior setter.
                parse: |s| {
                    s.parse::<EvictionPolicy>()
                        .map_err(|_| ConfigError::InvalidValue {
                            param: "maxmemory-policy".to_string(),
                            message: format!(
                                "must be one of: {}",
                                EvictionPolicy::all_names().join(", ")
                            ),
                        })
                },
                validate: ConfigParam::no_validate,
                default: EvictionPolicy::default,
                get: |mgr| mgr.runtime.read().unwrap().maxmemory_policy,
                apply: |mgr, p| {
                    mgr.runtime.write().unwrap().maxmemory_policy = p;
                    Ok(())
                },
                render: |p| p.as_str().to_string(),
                propagation: Propagation::Eviction,
            }),
            MaxmemorySamples => Box::new(ConfigParam::<usize, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<usize>().map_err(|_| ConfigError::InvalidValue {
                        param: "maxmemory-samples".to_string(),
                        message: "must be a positive integer".to_string(),
                    })
                },
                validate: |v, _ctx| {
                    if *v == 0 {
                        Err(ConfigError::InvalidValue {
                            param: "maxmemory-samples".to_string(),
                            message: "must be > 0".to_string(),
                        })
                    } else {
                        Ok(())
                    }
                },
                default: || frogdb_config::memory::DEFAULT_MAXMEMORY_SAMPLES,
                get: |mgr| mgr.runtime.read().unwrap().maxmemory_samples,
                apply: |mgr, v| {
                    mgr.runtime.write().unwrap().maxmemory_samples = v;
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::Eviction,
            }),
            LfuLogFactor => Box::new(ConfigParam::<u8, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<u8>().map_err(|_| ConfigError::InvalidValue {
                        param: "lfu-log-factor".to_string(),
                        message: "must be an integer 0-255".to_string(),
                    })
                },
                validate: ConfigParam::no_validate,
                default: || frogdb_config::memory::DEFAULT_LFU_LOG_FACTOR,
                get: |mgr| mgr.runtime.read().unwrap().lfu_log_factor,
                apply: |mgr, v| {
                    mgr.runtime.write().unwrap().lfu_log_factor = v;
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::Eviction,
            }),
            LfuDecayTime => Box::new(ConfigParam::<u64, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<u64>().map_err(|_| ConfigError::InvalidValue {
                        param: "lfu-decay-time".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })
                },
                validate: ConfigParam::no_validate,
                default: || frogdb_config::memory::DEFAULT_LFU_DECAY_TIME,
                get: |mgr| mgr.runtime.read().unwrap().lfu_decay_time,
                apply: |mgr, v| {
                    mgr.runtime.write().unwrap().lfu_decay_time = v;
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::Eviction,
            }),
            MaxmemoryClients => Box::new(ConfigParam::<String, ConfigManager> {
                name: id.name(),
                // `parse` only checks the value is well-formed; `apply` re-resolves
                // against live maxmemory and triggers eviction.
                parse: |s| {
                    if frogdb_config::parse_maxmemory_clients(s, 0).is_none() {
                        return Err(ConfigError::InvalidValue {
                            param: "maxmemory-clients".to_string(),
                            message: "must be 0 (disabled), a byte value (e.g. 100mb), or a percentage (e.g. 5%)".to_string(),
                        });
                    }
                    Ok(s.to_string())
                },
                validate: ConfigParam::no_validate,
                default: || "0".to_string(),
                get: |mgr| mgr.runtime.read().unwrap().maxmemory_clients.clone(),
                apply: |mgr, v| {
                    let maxmemory = mgr.runtime.read().unwrap().maxmemory;
                    mgr.runtime.write().unwrap().maxmemory_clients = v.clone();
                    // Trigger immediate eviction check via the injected client
                    // registry (always present -- no silent no-op).
                    let limit = frogdb_config::parse_maxmemory_clients(&v, maxmemory).unwrap_or(0);
                    if limit > 0 {
                        let evicted = mgr.client_eviction_registry.try_evict_clients(limit);
                        if evicted > 0 {
                            info!(
                                evicted,
                                limit, "Client eviction triggered by CONFIG SET maxmemory-clients"
                            );
                        }
                    }
                    Ok(())
                },
                render: |v| v.clone(),
                propagation: Propagation::None,
            }),
            // === Logging family ===
            Loglevel => Box::new(ConfigParam::<String, ConfigManager> {
                name: id.name(),
                // Legal values = `frogdb_config::logging::LOG_LEVELS`, the single
                // source of truth shared with config-file startup validation.
                parse: |s| {
                    let lower = s.to_lowercase();
                    if !frogdb_config::logging::LOG_LEVELS.contains(&lower.as_str()) {
                        return Err(ConfigError::InvalidValue {
                            param: "loglevel".to_string(),
                            message: format!(
                                "must be one of: {}",
                                frogdb_config::logging::LOG_LEVELS.join(", ")
                            ),
                        });
                    }
                    Ok(lower)
                },
                validate: ConfigParam::no_validate,
                default: || frogdb_config::logging::DEFAULT_LOG_LEVEL.to_string(),
                get: |mgr| mgr.runtime.read().unwrap().loglevel.clone(),
                apply: |mgr, v| {
                    mgr.runtime.write().unwrap().loglevel = v.clone();
                    // Apply the level change if a reload handle is wired up.
                    if let Some(ref handle) = mgr.log_reload_handle
                        && let Err(e) = handle.reload_level(&v)
                    {
                        warn!(error = %e, "Failed to reload log level");
                    }
                    Ok(())
                },
                render: |v| v.clone(),
                propagation: Propagation::None,
            }),
            PerRequestSpans => Box::new(ConfigParam::<bool, ConfigManager> {
                name: id.name(),
                parse: |s| match s.to_lowercase().as_str() {
                    "yes" | "true" | "1" | "on" => Ok(true),
                    "no" | "false" | "0" | "off" => Ok(false),
                    _ => Err(ConfigError::InvalidValue {
                        param: "per-request-spans".to_string(),
                        message: "must be yes/no".to_string(),
                    }),
                },
                validate: ConfigParam::no_validate,
                default: || false,
                get: |mgr| mgr.per_request_spans.load(Ordering::Relaxed),
                apply: |mgr, enabled| {
                    mgr.per_request_spans.store(enabled, Ordering::Relaxed);
                    info!(enabled, "Per-request tracing spans toggled");
                    Ok(())
                },
                render: |v| {
                    if *v {
                        "yes".to_string()
                    } else {
                        "no".to_string()
                    }
                },
                propagation: Propagation::None,
            }),
            // === Persistence family ===
            DurabilityMode => Box::new(ConfigParam::<String, ConfigManager> {
                name: id.name(),
                // Legal values = `frogdb_config::persistence::DURABILITY_MODES`,
                // shared with `PersistenceConfig::validate`.
                parse: |s| {
                    let lower = s.to_lowercase();
                    if !frogdb_config::persistence::DURABILITY_MODES.contains(&lower.as_str()) {
                        return Err(ConfigError::InvalidValue {
                            param: "durability-mode".to_string(),
                            message: format!(
                                "must be one of: {}",
                                frogdb_config::persistence::DURABILITY_MODES.join(", ")
                            ),
                        });
                    }
                    Ok(lower)
                },
                validate: ConfigParam::no_validate,
                default: || "periodic".to_string(),
                get: |mgr| mgr.runtime.read().unwrap().durability_mode.clone(),
                apply: |mgr, v| {
                    mgr.runtime.write().unwrap().durability_mode = v;
                    Ok(())
                },
                render: |v| v.clone(),
                propagation: Propagation::None,
            }),
            WalFailurePolicy => Box::new(ConfigParam::<String, ConfigManager> {
                name: id.name(),
                // Legal values = `frogdb_config::persistence::WAL_FAILURE_POLICIES`,
                // shared with `PersistenceConfig::validate`.
                parse: |s| {
                    let lower = s.to_lowercase();
                    if !frogdb_config::persistence::WAL_FAILURE_POLICIES.contains(&lower.as_str()) {
                        return Err(ConfigError::InvalidValue {
                            param: "wal-failure-policy".to_string(),
                            message: format!(
                                "must be one of: {}",
                                frogdb_config::persistence::WAL_FAILURE_POLICIES.join(", ")
                            ),
                        });
                    }
                    Ok(lower)
                },
                validate: ConfigParam::no_validate,
                default: || "continue".to_string(),
                get: |mgr| {
                    // Fully qualified: the `use MutableParamId::*` above shadows
                    // the bare `WalFailurePolicy` type with the same-named identity
                    // variant.
                    frogdb_core::persistence::WalFailurePolicy::from_u8(
                        mgr.wal_failure_policy.load(Ordering::Relaxed),
                    )
                    .as_config_str()
                    .to_string()
                },
                apply: |mgr, v| {
                    let policy_val =
                        frogdb_core::persistence::WalFailurePolicy::from_config_str(&v).as_u8();
                    mgr.wal_failure_policy.store(policy_val, Ordering::Relaxed);
                    info!(policy = %v, "WAL failure policy updated");
                    Ok(())
                },
                render: |v| v.clone(),
                propagation: Propagation::None,
            }),
            SyncIntervalMs => Box::new(ConfigParam::<u64, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<u64>().map_err(|_| ConfigError::InvalidValue {
                        param: "sync-interval-ms".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })
                },
                validate: ConfigParam::no_validate,
                default: || frogdb_config::persistence::DEFAULT_SYNC_INTERVAL_MS,
                get: |mgr| mgr.runtime.read().unwrap().sync_interval_ms,
                apply: |mgr, v| {
                    mgr.runtime.write().unwrap().sync_interval_ms = v;
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),
            BatchTimeoutMs => Box::new(ConfigParam::<u64, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<u64>().map_err(|_| ConfigError::InvalidValue {
                        param: "batch-timeout-ms".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })
                },
                validate: ConfigParam::no_validate,
                default: || frogdb_config::persistence::DEFAULT_BATCH_TIMEOUT_MS,
                get: |mgr| mgr.runtime.read().unwrap().batch_timeout_ms,
                apply: |mgr, v| {
                    mgr.runtime.write().unwrap().batch_timeout_ms = v;
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),
            // === Server family ===
            ScatterGatherTimeoutMs => Box::new(ConfigParam::<u64, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<u64>().map_err(|_| ConfigError::InvalidValue {
                        param: "scatter-gather-timeout-ms".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })
                },
                validate: ConfigParam::no_validate,
                default: || frogdb_config::server::DEFAULT_SCATTER_GATHER_TIMEOUT_MS,
                get: |mgr| mgr.runtime.read().unwrap().scatter_gather_timeout_ms,
                apply: |mgr, v| {
                    mgr.runtime.write().unwrap().scatter_gather_timeout_ms = v;
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),
            // === Replication family ===
            MinReplicasToWrite => Box::new(ConfigParam::<u32, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<u32>().map_err(|_| ConfigError::InvalidValue {
                        param: "min-replicas-to-write".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })
                },
                validate: ConfigParam::no_validate,
                default: || 0,
                get: |mgr| mgr.runtime.read().unwrap().min_replicas_to_write,
                apply: |mgr, v| {
                    mgr.runtime.write().unwrap().min_replicas_to_write = v;
                    info!(min_replicas_to_write = v, "min-replicas-to-write updated");
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),
            MinReplicasMaxLag => Box::new(ConfigParam::<MinReplicasMaxLagSecs, ConfigManager> {
                name: id.name(),
                // Redis reports/accepts this in seconds; stored internally in ms.
                // `get`/`render` round-trip in seconds; `apply` converts to ms.
                // The seconds->ms conversion needed to persist this into the
                // `min-replicas-timeout-ms` TOML field lives on
                // `MinReplicasMaxLagSecs::to_toml_value`, right next to this
                // parameter's own definition, rather than as a name check in
                // the file writer.
                parse: |s| {
                    s.parse::<u64>().map(MinReplicasMaxLagSecs).map_err(|_| {
                        ConfigError::InvalidValue {
                            param: "min-replicas-max-lag".to_string(),
                            message: "must be a non-negative integer (seconds)".to_string(),
                        }
                    })
                },
                validate: ConfigParam::no_validate,
                default: || {
                    MinReplicasMaxLagSecs(
                        frogdb_config::replication::DEFAULT_MIN_REPLICAS_TIMEOUT_MS / 1000,
                    )
                },
                get: |mgr| {
                    MinReplicasMaxLagSecs(
                        mgr.runtime.read().unwrap().min_replicas_timeout_ms / 1000,
                    )
                },
                apply: |mgr, MinReplicasMaxLagSecs(secs)| {
                    mgr.runtime.write().unwrap().min_replicas_timeout_ms = secs * 1000;
                    info!(
                        min_replicas_max_lag_secs = secs,
                        "min-replicas-max-lag updated"
                    );
                    Ok(())
                },
                render: |MinReplicasMaxLagSecs(secs)| secs.to_string(),
                propagation: Propagation::None,
            }),
            // === Slowlog family ===
            SlowlogLogSlowerThan => Box::new(ConfigParam::<i64, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<i64>().map_err(|_| ConfigError::InvalidValue {
                        param: "slowlog-log-slower-than".to_string(),
                        message: "must be an integer".to_string(),
                    })
                },
                validate: ConfigParam::no_validate,
                default: || frogdb_config::slowlog::DEFAULT_SLOWLOG_LOG_SLOWER_THAN,
                get: |mgr| mgr.runtime.read().unwrap().slowlog_log_slower_than,
                apply: |mgr, v| {
                    mgr.runtime.write().unwrap().slowlog_log_slower_than = v;
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),
            SlowlogMaxLen => Box::new(ConfigParam::<usize, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<usize>().map_err(|_| ConfigError::InvalidValue {
                        param: "slowlog-max-len".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })
                },
                validate: ConfigParam::no_validate,
                default: || frogdb_config::slowlog::DEFAULT_SLOWLOG_MAX_LEN,
                get: |mgr| mgr.runtime.read().unwrap().slowlog_max_len,
                apply: |mgr, v| {
                    mgr.runtime.write().unwrap().slowlog_max_len = v;
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),
            SlowlogMaxArgLen => Box::new(ConfigParam::<usize, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<usize>().map_err(|_| ConfigError::InvalidValue {
                        param: "slowlog-max-arg-len".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })
                },
                validate: ConfigParam::no_validate,
                default: || frogdb_config::slowlog::DEFAULT_SLOWLOG_MAX_ARG_LEN,
                get: |mgr| mgr.runtime.read().unwrap().slowlog_max_arg_len,
                apply: |mgr, v| {
                    mgr.runtime.write().unwrap().slowlog_max_arg_len = v;
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),
            // === Encoding-threshold family (listpack atomics, read lock-free by
            // shard workers) ===
            SetMaxListpackEntries => Box::new(ConfigParam::<u64, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<u64>().map_err(|_| ConfigError::InvalidValue {
                        param: "set-max-listpack-entries".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })
                },
                validate: ConfigParam::no_validate,
                default: || 128,
                get: |mgr| mgr.listpack.set_max_entries.load(Ordering::Relaxed),
                apply: |mgr, v| {
                    mgr.listpack.set_max_entries.store(v, Ordering::Relaxed);
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),
            SetMaxListpackValue => Box::new(ConfigParam::<u64, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<u64>().map_err(|_| ConfigError::InvalidValue {
                        param: "set-max-listpack-value".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })
                },
                validate: ConfigParam::no_validate,
                default: || 64,
                get: |mgr| mgr.listpack.set_max_value.load(Ordering::Relaxed),
                apply: |mgr, v| {
                    mgr.listpack.set_max_value.store(v, Ordering::Relaxed);
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),
            HashMaxZiplistEntries => Box::new(ConfigParam::<u64, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<u64>().map_err(|_| ConfigError::InvalidValue {
                        param: "hash-max-ziplist-entries".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })
                },
                validate: ConfigParam::no_validate,
                default: || 128,
                get: |mgr| mgr.listpack.hash_max_entries.load(Ordering::Relaxed),
                apply: |mgr, v| {
                    mgr.listpack.hash_max_entries.store(v, Ordering::Relaxed);
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),
            HashMaxZiplistValue => Box::new(ConfigParam::<u64, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<u64>().map_err(|_| ConfigError::InvalidValue {
                        param: "hash-max-ziplist-value".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })
                },
                validate: ConfigParam::no_validate,
                default: || 64,
                get: |mgr| mgr.listpack.hash_max_value.load(Ordering::Relaxed),
                apply: |mgr, v| {
                    mgr.listpack.hash_max_value.store(v, Ordering::Relaxed);
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),
            HashMaxListpackEntries => Box::new(ConfigParam::<u64, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<u64>().map_err(|_| ConfigError::InvalidValue {
                        param: "hash-max-listpack-entries".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })
                },
                validate: ConfigParam::no_validate,
                default: || 128,
                get: |mgr| mgr.listpack.hash_max_entries.load(Ordering::Relaxed),
                apply: |mgr, v| {
                    mgr.listpack.hash_max_entries.store(v, Ordering::Relaxed);
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),
            HashMaxListpackValue => Box::new(ConfigParam::<u64, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<u64>().map_err(|_| ConfigError::InvalidValue {
                        param: "hash-max-listpack-value".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })
                },
                validate: ConfigParam::no_validate,
                default: || 64,
                get: |mgr| mgr.listpack.hash_max_value.load(Ordering::Relaxed),
                apply: |mgr, v| {
                    mgr.listpack.hash_max_value.store(v, Ordering::Relaxed);
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),
            // === Misc runtime family ===
            LuaTimeLimit => Box::new(ConfigParam::<u64, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<u64>().map_err(|_| ConfigError::InvalidValue {
                        param: "lua-time-limit".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })
                },
                validate: ConfigParam::no_validate,
                default: || 5000,
                get: |mgr| mgr.lua_time_limit.load(Ordering::Relaxed),
                apply: |mgr, v| {
                    mgr.lua_time_limit.store(v, Ordering::Relaxed);
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),
            Maxclients => Box::new(ConfigParam::<u64, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<u64>().map_err(|_| ConfigError::InvalidValue {
                        param: "maxclients".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })
                },
                validate: ConfigParam::no_validate,
                default: || frogdb_config::server::DEFAULT_MAX_CLIENTS as u64,
                get: |mgr| mgr.max_clients.load(Ordering::Relaxed),
                apply: |mgr, v| {
                    mgr.max_clients.store(v, Ordering::Relaxed);
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),
            LatencyTracking => Box::new(ConfigParam::<bool, ConfigManager> {
                name: id.name(),
                parse: |s| match s.to_lowercase().as_str() {
                    "yes" | "1" | "true" => Ok(true),
                    "no" | "0" | "false" => Ok(false),
                    _ => Err(ConfigError::InvalidValue {
                        param: "latency-tracking".to_string(),
                        message: "must be yes or no".to_string(),
                    }),
                },
                validate: ConfigParam::no_validate,
                default: || true,
                get: |mgr| mgr.latency_histograms.is_enabled(),
                apply: |mgr, enabled| {
                    mgr.latency_histograms.set_enabled(enabled);
                    Ok(())
                },
                render: |v| {
                    if *v {
                        "yes".to_string()
                    } else {
                        "no".to_string()
                    }
                },
                propagation: Propagation::None,
            }),
            LatencyTrackingInfoPercentiles => Box::new(ConfigParam::<Vec<f64>, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    let trimmed = s.trim();
                    if trimmed.is_empty() {
                        return Ok(Vec::new());
                    }
                    let mut percentiles = Vec::new();
                    for part in trimmed.split_whitespace() {
                        let p: f64 = part.parse().map_err(|_| ConfigError::InvalidValue {
                            param: "latency-tracking-info-percentiles".to_string(),
                            message: format!("'{}' is not a valid percentile", part),
                        })?;
                        if !(0.0..=100.0).contains(&p) {
                            return Err(ConfigError::InvalidValue {
                                param: "latency-tracking-info-percentiles".to_string(),
                                message: format!("'{}' is not between 0 and 100", part),
                            });
                        }
                        percentiles.push(p);
                    }
                    Ok(percentiles)
                },
                validate: ConfigParam::no_validate,
                default: || vec![50.0, 99.0, 99.9],
                get: |mgr| mgr.latency_tracking_percentiles.read().unwrap().clone(),
                apply: |mgr, v| {
                    *mgr.latency_tracking_percentiles.write().unwrap() = v;
                    Ok(())
                },
                render: |v| {
                    v.iter()
                        .map(|p| {
                            if *p == p.floor() {
                                format!("{}", *p as u64)
                            } else {
                                format!("{}", p)
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(" ")
                },
                propagation: Propagation::None,
            }),
            NotifyKeyspaceEvents => Box::new(ConfigParam::<u32, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    let flags = KeyspaceEventFlags::from_flag_string(s).ok_or_else(|| {
                        ConfigError::InvalidValue {
                            param: "notify-keyspace-events".to_string(),
                            message: "invalid flag characters".to_string(),
                        }
                    })?;
                    Ok(flags.bits())
                },
                validate: ConfigParam::no_validate,
                default: || 0,
                get: |mgr| mgr.notify_keyspace_events.load(Ordering::Relaxed),
                apply: |mgr, bits| {
                    mgr.notify_keyspace_events.store(bits, Ordering::Relaxed);
                    Ok(())
                },
                render: |v| KeyspaceEventFlags::from_bits_truncate(*v).to_flag_string(),
                propagation: Propagation::None,
            }),
            Requirepass => Box::new(ConfigParam::<String, ConfigManager> {
                name: id.name(),
                // Any value is accepted; the ACL manager performs the real
                // validation and storage in `apply`.
                parse: |s| Ok(s.to_string()),
                validate: ConfigParam::no_validate,
                default: String::new,
                get: |mgr| mgr.acl_manager.get_requirepass(),
                apply: |mgr, v| {
                    mgr.acl_manager
                        .set_requirepass(&v)
                        .map_err(|e| ConfigError::InvalidValue {
                            param: "requirepass".to_string(),
                            message: e.to_string(),
                        })
                },
                render: |v| v.clone(),
                propagation: Propagation::None,
            }),
            KeyMemoryHistograms => Box::new(ConfigParam::<bool, ConfigManager> {
                name: id.name(),
                parse: |s| match s.to_lowercase().as_str() {
                    "yes" | "1" | "true" => Ok(true),
                    "no" | "0" | "false" => Ok(false),
                    _ => Err(ConfigError::InvalidValue {
                        param: "key-memory-histograms".to_string(),
                        message: "must be yes or no".to_string(),
                    }),
                },
                validate: |want_enabled, mgr| {
                    // Cannot enable at runtime if disabled at startup (state=1) or
                    // after a prior runtime disable (state=2).
                    if *want_enabled && mgr.key_memory_histograms_state.load(Ordering::Relaxed) != 0
                    {
                        return Err(ConfigError::InvalidValue {
                            param: "key-memory-histograms".to_string(),
                            message: "can't enable key-memory-histograms at runtime".to_string(),
                        });
                    }
                    Ok(())
                },
                default: || true,
                get: |mgr| mgr.key_memory_histograms_state.load(Ordering::Relaxed) == 0,
                apply: |mgr, want_enabled| {
                    // Disable: transition 0 -> 2 (runtime disable). Enabling is a
                    // no-op here (already enabled; `validate` rejected enabling
                    // from a disabled state).
                    if !want_enabled && mgr.key_memory_histograms_state.load(Ordering::Relaxed) == 0
                    {
                        mgr.key_memory_histograms_state.store(2, Ordering::Relaxed);
                    }
                    Ok(())
                },
                render: |v| {
                    if *v {
                        "yes".to_string()
                    } else {
                        "no".to_string()
                    }
                },
                propagation: Propagation::KeyMemoryHistograms,
            }),
            // === Redis-compatibility no-op parameters ===
            // Accepted for compatibility with Redis test suites; ignored by
            // FrogDB. Strict-config gating hides them via the metadata registry.
            Save => Box::new(NoopParam {
                name: id.name(),
                value: "",
            }),
            SetMaxIntsetEntries => Box::new(NoopParam {
                name: id.name(),
                value: "512",
            }),
            ListMaxListpackSize => Box::new(NoopParam {
                name: id.name(),
                value: "-2",
            }),
            ListCompressDepth => Box::new(NoopParam {
                name: id.name(),
                value: "0",
            }),
            ListMaxZiplistSize => Box::new(NoopParam {
                name: id.name(),
                value: "-2",
            }),
            LatencyMonitorThreshold => Box::new(NoopParam {
                name: id.name(),
                value: "0",
            }),
            BusyReplyThreshold => Box::new(NoopParam {
                name: id.name(),
                value: "5000",
            }),
            Hz => Box::new(NoopParam {
                name: id.name(),
                value: "10",
            }),
            Activedefrag => Box::new(NoopParam {
                name: id.name(),
                value: "no",
            }),
            CloseOnOom => Box::new(NoopParam {
                name: id.name(),
                value: "no",
            }),
            ZsetMaxZiplistEntries => Box::new(NoopParam {
                name: id.name(),
                value: "128",
            }),
            ZsetMaxZiplistValue => Box::new(NoopParam {
                name: id.name(),
                value: "64",
            }),
            ZsetMaxListpackEntries => Box::new(NoopParam {
                name: id.name(),
                value: "128",
            }),
            ZsetMaxListpackValue => Box::new(NoopParam {
                name: id.name(),
                value: "64",
            }),
            // === 13-01 Pass 2b: genuinely-live mutable param ===
            // The ACL log length is re-read on every append; apply/get reach it
            // through the already-injected `Arc<AclManager>`, so CONFIG SET
            // actually changes runtime behavior (the log trims to the new bound).
            AcllogMaxLen => Box::new(ConfigParam::<usize, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<usize>().map_err(|_| ConfigError::InvalidValue {
                        param: "acllog-max-len".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })
                },
                validate: ConfigParam::no_validate,
                default: || frogdb_config::security::DEFAULT_ACL_LOG_MAX_LEN,
                get: |mgr| mgr.acl_manager.log().max_len(),
                apply: |mgr, v| {
                    mgr.acl_manager.log().set_max_len(v);
                    info!(acllog_max_len = v, "ACL log max length updated");
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),

            // === config-mutability round: TLS live-reload family ===
            // Every getter reads the *running* TLS config (falling back to the
            // startup snapshot only when no TLS runtime exists), and every
            // setter goes through `apply_tls`, which is build-then-commit: a
            // rejected certificate leaves the served identity untouched.
            TlsCertFile => Box::new(ConfigParam::<String, ConfigManager> {
                name: id.name(),
                parse: |s| Ok(s.to_string()),
                validate: ConfigParam::no_validate,
                default: String::new,
                get: |mgr| match mgr.live_tls_config() {
                    Some(c) => c.cert_file.display().to_string(),
                    None => mgr.static_config.tls_cert_file.clone(),
                },
                apply: |mgr, v| {
                    mgr.apply_tls("tls-cert-file", TlsMutation::CertFile(PathBuf::from(v)))?;
                    info!("TLS server certificate reloaded");
                    Ok(())
                },
                render: |v| v.clone(),
                propagation: Propagation::None,
            }),
            TlsKeyFile => Box::new(ConfigParam::<String, ConfigManager> {
                name: id.name(),
                parse: |s| Ok(s.to_string()),
                validate: ConfigParam::no_validate,
                default: String::new,
                get: |mgr| match mgr.live_tls_config() {
                    Some(c) => c.key_file.display().to_string(),
                    None => mgr.static_config.tls_key_file.clone(),
                },
                apply: |mgr, v| {
                    mgr.apply_tls("tls-key-file", TlsMutation::KeyFile(PathBuf::from(v)))?;
                    info!("TLS server private key reloaded");
                    Ok(())
                },
                render: |v| v.clone(),
                propagation: Propagation::None,
            }),
            // `OptionalPathValue`, not `String`: the file field is
            // `Option<PathBuf>`, so "unset" must be rendered by CONFIG REWRITE
            // as an *absent* key rather than `ca-file = ""`.
            TlsCaCertFile => Box::new(ConfigParam::<OptionalPathValue, ConfigManager> {
                name: id.name(),
                parse: |s| Ok(OptionalPathValue(s.to_string())),
                validate: ConfigParam::no_validate,
                default: || OptionalPathValue(String::new()),
                get: |mgr| {
                    OptionalPathValue(match mgr.live_tls_config() {
                        Some(c) => render_optional_path(&c.ca_file),
                        None => mgr.static_config.tls_ca_file.clone(),
                    })
                },
                apply: |mgr, v| {
                    mgr.apply_tls("tls-ca-cert-file", TlsMutation::CaFile(optional_path(&v.0)))?;
                    info!("TLS CA bundle reloaded");
                    Ok(())
                },
                render: |v| v.0.clone(),
                propagation: Propagation::None,
            }),
            TlsClientCertFile => Box::new(ConfigParam::<OptionalPathValue, ConfigManager> {
                name: id.name(),
                parse: |s| Ok(OptionalPathValue(s.to_string())),
                validate: ConfigParam::no_validate,
                default: || OptionalPathValue(String::new()),
                get: |mgr| {
                    OptionalPathValue(match mgr.live_tls_config() {
                        Some(c) => render_optional_path(&c.client_cert_file),
                        None => mgr.static_config.tls_client_cert_file.clone(),
                    })
                },
                apply: |mgr, v| {
                    mgr.apply_tls(
                        "tls-client-cert-file",
                        TlsMutation::ClientCertFile(optional_path(&v.0)),
                    )?;
                    info!("TLS outgoing client certificate reloaded");
                    Ok(())
                },
                render: |v| v.0.clone(),
                propagation: Propagation::None,
            }),
            TlsClientKeyFile => Box::new(ConfigParam::<OptionalPathValue, ConfigManager> {
                name: id.name(),
                parse: |s| Ok(OptionalPathValue(s.to_string())),
                validate: ConfigParam::no_validate,
                default: || OptionalPathValue(String::new()),
                get: |mgr| {
                    OptionalPathValue(match mgr.live_tls_config() {
                        Some(c) => render_optional_path(&c.client_key_file),
                        None => mgr.static_config.tls_client_key_file.clone(),
                    })
                },
                apply: |mgr, v| {
                    mgr.apply_tls(
                        "tls-client-key-file",
                        TlsMutation::ClientKeyFile(optional_path(&v.0)),
                    )?;
                    info!("TLS outgoing client key reloaded");
                    Ok(())
                },
                render: |v| v.0.clone(),
                propagation: Propagation::None,
            }),
            TlsCiphersuites => Box::new(ConfigParam::<Vec<String>, ConfigManager> {
                name: id.name(),
                // Space-separated on the wire (Redis-style, like `tls-protocols`);
                // the empty string means "rustls defaults". Names are not checked
                // here -- `set_ciphersuites` rebuilds rustls from them and reports
                // an unknown name as a set error, so there is one authority.
                parse: |s| {
                    Ok(s.split_whitespace()
                        .map(|c| c.to_string())
                        .collect::<Vec<_>>())
                },
                validate: ConfigParam::no_validate,
                default: Vec::new,
                get: |mgr| match mgr.live_tls_config() {
                    Some(c) => c.ciphersuites.clone(),
                    None => mgr.static_config.tls_ciphersuites.clone(),
                },
                apply: |mgr, v| {
                    mgr.apply_tls("tls-ciphersuites", TlsMutation::Ciphersuites(v))?;
                    info!("TLS ciphersuites updated");
                    Ok(())
                },
                render: |v| v.join(" "),
                propagation: Propagation::None,
            }),
            TlsHandshakeTimeoutMs => Box::new(ConfigParam::<u64, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<u64>().map_err(|_| ConfigError::InvalidValue {
                        param: "tls-handshake-timeout-ms".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })
                },
                validate: |v, _ctx| {
                    if *v == 0 {
                        Err(ConfigError::InvalidValue {
                            param: "tls-handshake-timeout-ms".to_string(),
                            message: "must be > 0".to_string(),
                        })
                    } else {
                        Ok(())
                    }
                },
                default: || 10000,
                get: |mgr| match mgr.live_tls_config() {
                    Some(c) => c.handshake_timeout_ms,
                    None => mgr.static_config.tls_handshake_timeout_ms,
                },
                apply: |mgr, v| {
                    mgr.apply_tls(
                        "tls-handshake-timeout-ms",
                        TlsMutation::HandshakeTimeoutMs(v),
                    )?;
                    info!(handshake_timeout_ms = v, "TLS handshake timeout updated");
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),
            TlsClusterMigration => Box::new(ConfigParam::<bool, ConfigManager> {
                name: id.name(),
                parse: |s| parse_yes_no("tls-cluster-migration", s),
                validate: ConfigParam::no_validate,
                default: || false,
                get: |mgr| match mgr.live_tls_config() {
                    Some(c) => c.tls_cluster_migration,
                    None => mgr.static_config.tls_cluster_migration,
                },
                apply: |mgr, v| {
                    mgr.apply_tls("tls-cluster-migration", TlsMutation::ClusterMigration(v))?;
                    info!(enabled = v, "TLS cluster-bus dual-accept toggled");
                    Ok(())
                },
                render: |v| yes_no(*v),
                propagation: Propagation::None,
            }),

            // === config-mutability round: [cluster] decision flags ===
            // Read by the failure detector and the self-fence gate at decision
            // time through the shared `ClusterRuntimeFlags`.
            ClusterAutoFailover => Box::new(ConfigParam::<bool, ConfigManager> {
                name: id.name(),
                parse: |s| parse_yes_no("cluster-auto-failover", s),
                validate: ConfigParam::no_validate,
                default: || false,
                get: |mgr| mgr.cluster_flags.auto_failover(),
                apply: |mgr, v| {
                    mgr.cluster_flags.set_auto_failover(v);
                    info!(enabled = v, "Cluster automatic failover toggled");
                    Ok(())
                },
                render: |v| yes_no(*v),
                propagation: Propagation::None,
            }),
            ClusterSelfFenceOnQuorumLoss => Box::new(ConfigParam::<bool, ConfigManager> {
                name: id.name(),
                parse: |s| parse_yes_no("cluster-self-fence-on-quorum-loss", s),
                validate: ConfigParam::no_validate,
                default: || true,
                get: |mgr| mgr.cluster_flags.self_fence_on_quorum_loss(),
                apply: |mgr, v| {
                    mgr.cluster_flags.set_self_fence_on_quorum_loss(v);
                    info!(enabled = v, "Cluster quorum-loss self-fencing toggled");
                    Ok(())
                },
                render: |v| yes_no(*v),
                propagation: Propagation::None,
            }),
            // Partially live: this node's own election scoring picks the new
            // priority up immediately, but peers only learn it when the node
            // next advertises itself, so a change is not instantly global.
            ReplicaPriority => Box::new(ConfigParam::<u32, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<u32>().map_err(|_| ConfigError::InvalidValue {
                        param: "replica-priority".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })
                },
                validate: ConfigParam::no_validate,
                default: || frogdb_config::cluster::DEFAULT_REPLICA_PRIORITY,
                get: |mgr| mgr.cluster_flags.replica_priority(),
                apply: |mgr, v| {
                    mgr.cluster_flags.set_replica_priority(v);
                    info!(replica_priority = v, "Replica failover priority updated");
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),

            // === config-mutability round: [status] health thresholds ===
            // The status collector classifies every `/status` render against
            // these, so a set changes the next report.
            StatusMemoryWarningPercent => Box::new(ConfigParam::<u8, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<u8>().map_err(|_| ConfigError::InvalidValue {
                        param: "status-memory-warning-percent".to_string(),
                        message: "must be an integer 1-100".to_string(),
                    })
                },
                // Same bound as `StatusConfig::validate`, so a value CONFIG SET
                // accepts is a value the config file would also accept.
                validate: |v, _ctx| validate_percent("status-memory-warning-percent", *v),
                default: || frogdb_config::status::DEFAULT_MEMORY_WARNING_PERCENT,
                get: |mgr| mgr.status_thresholds.memory_warning_percent(),
                apply: |mgr, v| {
                    mgr.status_thresholds.set_memory_warning_percent(v);
                    info!(percent = v, "Status memory warning threshold updated");
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),
            StatusConnectionWarningPercent => Box::new(ConfigParam::<u8, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<u8>().map_err(|_| ConfigError::InvalidValue {
                        param: "status-connection-warning-percent".to_string(),
                        message: "must be an integer 1-100".to_string(),
                    })
                },
                validate: |v, _ctx| validate_percent("status-connection-warning-percent", *v),
                default: || frogdb_config::status::DEFAULT_CONNECTION_WARNING_PERCENT,
                get: |mgr| mgr.status_thresholds.connection_warning_percent(),
                apply: |mgr, v| {
                    mgr.status_thresholds.set_connection_warning_percent(v);
                    info!(percent = v, "Status connection warning threshold updated");
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),
            StatusDurabilityLagWarningMs => Box::new(ConfigParam::<u64, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<u64>().map_err(|_| ConfigError::InvalidValue {
                        param: "status-durability-lag-warning-ms".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })
                },
                // `StatusConfig::validate` requires warning < critical; enforce it
                // here against the *live* critical value so the pair can never be
                // driven into an ordering the config file would reject.
                validate: |v, mgr| {
                    let critical = mgr.status_thresholds.durability_lag_critical_ms();
                    if *v >= critical {
                        Err(ConfigError::InvalidValue {
                            param: "status-durability-lag-warning-ms".to_string(),
                            message: format!(
                                "must be less than status-durability-lag-critical-ms ({critical})"
                            ),
                        })
                    } else {
                        Ok(())
                    }
                },
                default: || frogdb_config::status::DEFAULT_DURABILITY_LAG_WARNING_MS,
                get: |mgr| mgr.status_thresholds.durability_lag_warning_ms(),
                apply: |mgr, v| {
                    mgr.status_thresholds.set_durability_lag_warning_ms(v);
                    info!(ms = v, "Status durability-lag warning threshold updated");
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),
            StatusDurabilityLagCriticalMs => Box::new(ConfigParam::<u64, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<u64>().map_err(|_| ConfigError::InvalidValue {
                        param: "status-durability-lag-critical-ms".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })
                },
                validate: |v, mgr| {
                    let warning = mgr.status_thresholds.durability_lag_warning_ms();
                    if *v <= warning {
                        Err(ConfigError::InvalidValue {
                            param: "status-durability-lag-critical-ms".to_string(),
                            message: format!(
                                "must be greater than status-durability-lag-warning-ms ({warning})"
                            ),
                        })
                    } else {
                        Ok(())
                    }
                },
                default: || frogdb_config::status::DEFAULT_DURABILITY_LAG_CRITICAL_MS,
                get: |mgr| mgr.status_thresholds.durability_lag_critical_ms(),
                apply: |mgr, v| {
                    mgr.status_thresholds.set_durability_lag_critical_ms(v);
                    info!(ms = v, "Status durability-lag critical threshold updated");
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),

            // === config-mutability round: telemetry ===
            TracingSamplingRate => Box::new(ConfigParam::<f64, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<f64>().map_err(|_| ConfigError::InvalidValue {
                        param: "tracing-sampling-rate".to_string(),
                        message: "must be a number between 0.0 and 1.0".to_string(),
                    })
                },
                // Same bound as `TracingConfig::validate`.
                validate: |v, _ctx| {
                    if !(0.0..=1.0).contains(v) {
                        Err(ConfigError::InvalidValue {
                            param: "tracing-sampling-rate".to_string(),
                            message: "must be between 0.0 and 1.0".to_string(),
                        })
                    } else {
                        Ok(())
                    }
                },
                default: || 1.0,
                get: |mgr| mgr.tracing_sampling_rate.get(),
                apply: |mgr, v| {
                    mgr.tracing_sampling_rate.set(v);
                    info!(sampling_rate = v, "Trace sampling rate updated");
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),
            LatencyBandsEnabled => Box::new(ConfigParam::<bool, ConfigManager> {
                name: id.name(),
                parse: |s| parse_yes_no("latency-bands-enabled", s),
                validate: ConfigParam::no_validate,
                default: || false,
                get: |mgr| mgr.latency_band_tracker.is_enabled(),
                apply: |mgr, v| {
                    mgr.latency_band_tracker.set_enabled(v);
                    info!(enabled = v, "Latency band tracking toggled");
                    Ok(())
                },
                render: |v| yes_no(*v),
                propagation: Propagation::None,
            }),

            // === config-mutability round: persistence / replication ===
            // The periodic-snapshot task re-reads its cadence each iteration, so
            // a set retimes the next snapshot (0 idles the task rather than
            // stopping it). `snapshot_interval_secs` is the authority for
            // GET/REWRITE; the coordinator is pushed to when it exists.
            SnapshotIntervalSecs => Box::new(ConfigParam::<u64, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<u64>().map_err(|_| ConfigError::InvalidValue {
                        param: "snapshot-interval-secs".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })
                },
                validate: ConfigParam::no_validate,
                default: || frogdb_config::persistence::DEFAULT_SNAPSHOT_INTERVAL_SECS,
                get: |mgr| mgr.snapshot_interval_secs.load(Ordering::Relaxed),
                apply: |mgr, v| {
                    mgr.snapshot_interval_secs.store(v, Ordering::Relaxed);
                    if let Some(c) = mgr.snapshot_coordinator.get() {
                        c.set_periodic_interval_secs(v);
                    }
                    info!(interval_secs = v, "Periodic snapshot cadence updated");
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),
            // KiB on the wire, bytes in the shared cell every shard's WAL flush
            // thread compares against, so all shards retune together.
            BatchSizeThresholdKb => Box::new(ConfigParam::<usize, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<usize>().map_err(|_| ConfigError::InvalidValue {
                        param: "batch-size-threshold-kb".to_string(),
                        message: "must be a positive integer".to_string(),
                    })
                },
                validate: |v, _ctx| {
                    if *v == 0 {
                        Err(ConfigError::InvalidValue {
                            param: "batch-size-threshold-kb".to_string(),
                            message: "must be > 0".to_string(),
                        })
                    } else {
                        Ok(())
                    }
                },
                default: || frogdb_config::persistence::DEFAULT_BATCH_SIZE_THRESHOLD_KB,
                get: |mgr| mgr.wal_batch_size_threshold.load(Ordering::Relaxed) / 1024,
                apply: |mgr, v| {
                    mgr.wal_batch_size_threshold
                        .store(v * 1024, Ordering::Relaxed);
                    info!(
                        batch_size_threshold_kb = v,
                        "WAL flush batch threshold updated"
                    );
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),
            // Primary-only live seam: the lag thresholds exist inside the primary
            // replication handler. On a replica the value is still recorded (and
            // reported by GET/REWRITE) so it takes effect if the node is later
            // initialised as a primary.
            ReplicationLagThresholdBytes => Box::new(ConfigParam::<u64, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<u64>().map_err(|_| ConfigError::InvalidValue {
                        param: "replication-lag-threshold-bytes".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })
                },
                validate: ConfigParam::no_validate,
                default: || 0,
                get: |mgr| mgr.replication_lag_threshold_bytes.load(Ordering::Relaxed),
                apply: |mgr, v| {
                    mgr.replication_lag_threshold_bytes
                        .store(v, Ordering::Relaxed);
                    if let Some(t) = mgr.replication_lag_thresholds.get() {
                        t.set_threshold_bytes(v);
                    }
                    info!(
                        threshold_bytes = v,
                        "Replication lag byte threshold updated"
                    );
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),
            ReplicationLagThresholdSecs => Box::new(ConfigParam::<u64, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<u64>().map_err(|_| ConfigError::InvalidValue {
                        param: "replication-lag-threshold-secs".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })
                },
                validate: ConfigParam::no_validate,
                default: || 0,
                get: |mgr| mgr.replication_lag_threshold_secs.load(Ordering::Relaxed),
                apply: |mgr, v| {
                    mgr.replication_lag_threshold_secs
                        .store(v, Ordering::Relaxed);
                    if let Some(t) = mgr.replication_lag_thresholds.get() {
                        t.set_threshold_secs(v);
                    }
                    info!(threshold_secs = v, "Replication lag time threshold updated");
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),
            // Primary-only live seam, same story as the lag thresholds: the
            // quorum checker only exists on a primary.
            SelfFenceOnReplicaLoss => Box::new(ConfigParam::<bool, ConfigManager> {
                name: id.name(),
                parse: |s| parse_yes_no("self-fence-on-replica-loss", s),
                validate: ConfigParam::no_validate,
                default: || frogdb_config::replication::DEFAULT_SELF_FENCE_ON_REPLICA_LOSS,
                get: |mgr| mgr.self_fence_on_replica_loss.load(Ordering::Relaxed),
                apply: |mgr, v| {
                    mgr.self_fence_on_replica_loss.store(v, Ordering::Relaxed);
                    if let Some(c) = mgr.replication_self_fence.get() {
                        c.set_self_fence_enabled(v);
                    }
                    info!(enabled = v, "Replica-loss self-fencing toggled");
                    Ok(())
                },
                render: |v| yes_no(*v),
                propagation: Propagation::None,
            }),
            ReplicaFreshnessTimeoutMs => Box::new(ConfigParam::<u64, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<u64>().map_err(|_| ConfigError::InvalidValue {
                        param: "replica-freshness-timeout-ms".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })
                },
                validate: |v, _ctx| {
                    if *v == 0 {
                        Err(ConfigError::InvalidValue {
                            param: "replica-freshness-timeout-ms".to_string(),
                            message: "must be > 0".to_string(),
                        })
                    } else {
                        Ok(())
                    }
                },
                default: || frogdb_config::replication::DEFAULT_REPLICA_FRESHNESS_TIMEOUT_MS,
                get: |mgr| mgr.replica_freshness_timeout_ms.load(Ordering::Relaxed),
                apply: |mgr, v| {
                    mgr.replica_freshness_timeout_ms.store(v, Ordering::Relaxed);
                    if let Some(c) = mgr.replication_self_fence.get() {
                        c.set_freshness_timeout_ms(v);
                    }
                    info!(timeout_ms = v, "Replica freshness window updated");
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),

            // === config-mutability round: [hotshards] thresholds ===
            // The collector re-reads all three once per `collect()`, so a set
            // retunes FROGDB.HOTSHARDS, `/status` and the debug UI at once. The
            // warm <= hot invariant from `HotShardsConfig::validate` is enforced
            // against the live sibling value.
            HotshardsHotThresholdPercent => Box::new(ConfigParam::<f64, ConfigManager> {
                name: id.name(),
                parse: |s| parse_percent_f64("hotshards-hot-threshold-percent", s),
                validate: |v, mgr| {
                    let warm = mgr.hotshards.warm_threshold_percent();
                    if *v < warm {
                        Err(ConfigError::InvalidValue {
                            param: "hotshards-hot-threshold-percent".to_string(),
                            message: format!(
                                "must not be below hotshards-warm-threshold-percent ({warm})"
                            ),
                        })
                    } else {
                        Ok(())
                    }
                },
                default: || frogdb_config::hotshards::DEFAULT_HOT_THRESHOLD_PERCENT,
                get: |mgr| mgr.hotshards.hot_threshold_percent(),
                apply: |mgr, v| {
                    mgr.hotshards.set_hot_threshold_percent(v);
                    info!(percent = v, "Hot-shard HOT threshold updated");
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),
            HotshardsWarmThresholdPercent => Box::new(ConfigParam::<f64, ConfigManager> {
                name: id.name(),
                parse: |s| parse_percent_f64("hotshards-warm-threshold-percent", s),
                validate: |v, mgr| {
                    let hot = mgr.hotshards.hot_threshold_percent();
                    if *v > hot {
                        Err(ConfigError::InvalidValue {
                            param: "hotshards-warm-threshold-percent".to_string(),
                            message: format!(
                                "must not exceed hotshards-hot-threshold-percent ({hot})"
                            ),
                        })
                    } else {
                        Ok(())
                    }
                },
                default: || frogdb_config::hotshards::DEFAULT_WARM_THRESHOLD_PERCENT,
                get: |mgr| mgr.hotshards.warm_threshold_percent(),
                apply: |mgr, v| {
                    mgr.hotshards.set_warm_threshold_percent(v);
                    info!(percent = v, "Hot-shard WARM threshold updated");
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),
            HotshardsDefaultPeriodSecs => Box::new(ConfigParam::<u64, ConfigManager> {
                name: id.name(),
                parse: |s| {
                    s.parse::<u64>().map_err(|_| ConfigError::InvalidValue {
                        param: "hotshards-default-period-secs".to_string(),
                        message: "must be a positive integer".to_string(),
                    })
                },
                validate: |v, _ctx| {
                    if *v == 0 {
                        Err(ConfigError::InvalidValue {
                            param: "hotshards-default-period-secs".to_string(),
                            message: "must be > 0".to_string(),
                        })
                    } else {
                        Ok(())
                    }
                },
                default: || frogdb_config::hotshards::DEFAULT_DEFAULT_PERIOD_SECS,
                get: |mgr| mgr.hotshards.default_period_secs(),
                apply: |mgr, v| {
                    mgr.hotshards.set_default_period_secs(v);
                    info!(period_secs = v, "Hot-shard default sampling window updated");
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),
        }
    }

    /// Look up a mutable parameter's typed lifecycle by (already-normalized) name.
    fn typed_param(&self, name: &str) -> Option<&dyn DynParam<ConfigManager>> {
        self.typed_params
            .iter()
            .map(|b| b.as_ref() as &dyn DynParam<ConfigManager>)
            .find(|p| p.name() == name)
    }

    /// Look up a mutable parameter's CONFIG REWRITE renderer by (already-normalized) name.
    ///
    /// Same lookup as [`typed_param`](Self::typed_param), but returns the
    /// [`TomlRenderable`] view so CONFIG REWRITE can ask the parameter for a
    /// genuinely-typed [`toml_edit::Value`] rather than a display string.
    fn typed_param_toml(&self, name: &str) -> Option<&dyn TomlRenderable> {
        self.typed_params
            .iter()
            .map(|b| b.as_ref())
            .find(|p| p.name() == name)
    }

    /// Look up an immutable, read-only parameter's getter by name.
    fn readonly_param(&self, name: &str) -> Option<&ParamMeta> {
        self.params.iter().find(|p| p.name == name)
    }

    /// Read a parameter's current value as a string, checking the typed
    /// (mutable) registry first and then the read-only one.
    fn value_of(&self, name: &str) -> Option<String> {
        if let Some(p) = self.typed_param(name) {
            return Some(p.get(self));
        }
        self.readonly_param(name).map(|p| (p.getter)(self))
    }

    /// Get parameters matching a glob pattern.
    ///
    /// Returns a vector of (name, value) pairs.
    /// When `strict_config` is enabled, no-op compatibility params are hidden.
    ///
    /// Iteration is driven by the config-crate metadata registry (the single
    /// source of truth for which parameters exist and their `mutable`/`noop`
    /// flags); values come from whichever server registry owns the lifecycle.
    pub fn get(&self, pattern: &str) -> Vec<(String, String)> {
        let strict = self.static_config.strict_config;
        let pattern_bytes = pattern.as_bytes();
        frogdb_config::config_param_registry()
            .iter()
            .filter(|info| !(strict && info.noop))
            .filter(|info| glob_match(pattern_bytes, info.name.as_bytes()))
            .filter_map(|info| self.value_of(info.name).map(|v| (info.name.to_string(), v)))
            .collect()
    }

    /// Set a configuration parameter.
    ///
    /// Returns Ok(()) on success, or an error if the parameter is immutable,
    /// unknown, or the value is invalid.
    /// When `strict_config` is enabled, no-op compatibility params are rejected.
    pub fn set(&self, name: &str, value: &str) -> Result<(), ConfigError> {
        // Normalize name (lowercase, allow underscores as dashes)
        let normalized = name.to_lowercase().replace('_', "-");

        // Existence + mutability + no-op gating come from the metadata registry.
        let info = frogdb_config::config_param_registry()
            .iter()
            .find(|p| p.name == normalized)
            .ok_or_else(|| {
                warn!(param = %name, "Unknown config parameter");
                ConfigError::UnknownParameter(name.to_string())
            })?;

        // When strict_config is enabled, reject no-op compatibility params
        if self.static_config.strict_config && info.noop {
            warn!(param = %name, "No-op config parameter rejected (strict_config=true)");
            return Err(ConfigError::UnknownParameter(name.to_string()));
        }

        if !info.mutable {
            warn!(param = %name, "Attempted to change immutable config");
            return Err(ConfigError::ImmutableParameter(name.to_string()));
        }

        // Get old value before change
        let old_value = self.value_of(&normalized).unwrap_or_default();

        // Every mutable parameter owns its parse/validate/apply lifecycle in the
        // typed registry. A name that passed the mutability gate above but has no
        // typed entry is an internal registry inconsistency (caught by
        // `test_param_registry_consistency`), so treat it as immutable.
        let param = self
            .typed_param(&normalized)
            .ok_or_else(|| ConfigError::ImmutableParameter(name.to_string()))?;
        param.set(self, value).map_err(|e| {
            warn!(param = %name, value = %value, error = %e, "Invalid config value rejected");
            e
        })?;

        // Get new value after change
        let new_value = self.value_of(&normalized).unwrap_or_default();

        info!(param = %name, old_value = %old_value, new_value = %new_value, "Config parameter changed");

        Ok(())
    }

    /// Get all parameter names.
    pub fn all_param_names(&self) -> Vec<&'static str> {
        frogdb_config::config_param_registry()
            .iter()
            .map(|p| p.name)
            .collect()
    }

    /// Get mutable parameter names.
    pub fn mutable_param_names(&self) -> Vec<&'static str> {
        frogdb_config::config_param_registry()
            .iter()
            .filter(|p| p.mutable)
            .map(|p| p.name)
            .collect()
    }

    /// Get immutable parameter names.
    pub fn immutable_param_names(&self) -> Vec<&'static str> {
        frogdb_config::config_param_registry()
            .iter()
            .filter(|p| !p.mutable)
            .map(|p| p.name)
            .collect()
    }

    /// Get the current runtime config snapshot.
    pub fn runtime_snapshot(&self) -> RuntimeConfig {
        self.runtime.read().unwrap().clone()
    }

    /// Get the current maxmemory value.
    pub fn maxmemory(&self) -> u64 {
        self.runtime.read().unwrap().maxmemory
    }

    /// Get the current maxmemory policy.
    pub fn maxmemory_policy(&self) -> EvictionPolicy {
        self.runtime.read().unwrap().maxmemory_policy
    }

    /// Minimum number of "good" replicas required before a write is accepted
    /// (Redis `min-replicas-to-write`). `0` disables the gate. Read live on the
    /// write path so `CONFIG SET min-replicas-to-write` takes effect at once.
    pub fn min_replicas_to_write(&self) -> u32 {
        self.runtime.read().unwrap().min_replicas_to_write
    }

    /// Maximum replica ACK lag (ms) for a replica to count as "good" toward
    /// [`Self::min_replicas_to_write`] (Redis `min-replicas-max-lag`, kept in
    /// ms internally).
    pub fn min_replicas_timeout_ms(&self) -> u64 {
        self.runtime.read().unwrap().min_replicas_timeout_ms
    }

    /// Get the slowlog threshold in microseconds.
    /// Returns -1 if disabled, 0 to log all, or positive value for threshold.
    pub fn slowlog_log_slower_than(&self) -> i64 {
        self.runtime.read().unwrap().slowlog_log_slower_than
    }

    /// Get the slowlog max entries per shard.
    pub fn slowlog_max_len(&self) -> usize {
        self.runtime.read().unwrap().slowlog_max_len
    }

    /// Get the slowlog max argument length.
    pub fn slowlog_max_arg_len(&self) -> usize {
        self.runtime.read().unwrap().slowlog_max_arg_len
    }

    /// Generate CONFIG HELP output.
    ///
    /// The mutable/immutable parameter lists are auto-generated from the
    /// parameter registry so they stay in sync as parameters are added.
    pub fn help_text(&self) -> Vec<String> {
        let registry = frogdb_config::config_param_registry();
        let mutable: Vec<&str> = registry
            .iter()
            .filter(|p| p.mutable && !p.noop)
            .map(|p| p.name)
            .collect();
        let immutable: Vec<&str> = registry
            .iter()
            .filter(|p| !p.mutable)
            .map(|p| p.name)
            .collect();

        vec![
            "CONFIG <subcommand> [<arg> ...]. Subcommands are:".to_string(),
            "GET <pattern>".to_string(),
            "    Return parameters matching <pattern>.".to_string(),
            "SET <param> <value>".to_string(),
            "    Set a mutable configuration parameter.".to_string(),
            "HELP".to_string(),
            "    Print this help.".to_string(),
            String::new(),
            format!("Mutable parameters: {}", mutable.join(", ")),
            String::new(),
            format!(
                "Immutable parameters (require restart): {}",
                immutable.join(", ")
            ),
        ]
    }

    /// Resolve the current maxmemory-clients limit in bytes.
    /// Returns 0 if disabled.
    pub fn resolve_maxmemory_clients(&self) -> u64 {
        let runtime = self.runtime.read().unwrap();
        frogdb_config::parse_maxmemory_clients(&runtime.maxmemory_clients, runtime.maxmemory)
            .unwrap_or(0)
    }

    /// Get a reference to the runtime config Arc.
    pub fn runtime_ref(&self) -> Arc<RwLock<RuntimeConfig>> {
        self.runtime.clone()
    }

    /// Get the number of shards from static config.
    pub fn num_shards(&self) -> usize {
        self.static_config.num_shards
    }

    /// Whether DEBUG SLEEP (and other unsafe DEBUG subcommands) is enabled.
    pub fn enable_debug_command(&self) -> bool {
        self.static_config.enable_debug_command
    }

    /// Get the shared lua-time-limit atomic for use in ScriptingConfig.
    pub fn lua_time_limit(&self) -> Arc<AtomicU64> {
        self.lua_time_limit.clone()
    }

    /// Get the shared max_clients flag for the Acceptor.
    pub fn max_clients_flag(&self) -> Arc<AtomicU64> {
        self.max_clients.clone()
    }

    /// Read the current max_clients value.
    pub fn max_clients(&self) -> u64 {
        self.max_clients.load(Ordering::Relaxed)
    }

    /// Get the shared notify-keyspace-events flags for shard workers.
    pub fn notify_keyspace_events_flags(&self) -> Arc<AtomicU32> {
        self.notify_keyspace_events.clone()
    }

    /// Check if key-memory histograms are enabled.
    pub fn key_memory_histograms_enabled(&self) -> bool {
        self.key_memory_histograms_state.load(Ordering::Relaxed) == 0
    }

    /// Get the configured WAL durability mode (e.g. "periodic", "sync", "async").
    pub fn durability_mode(&self) -> String {
        self.runtime.read().unwrap().durability_mode.clone()
    }

    /// Mark key-memory histograms as disabled at startup.
    pub fn set_key_memory_histograms_disabled_at_startup(&self) {
        self.key_memory_histograms_state.store(1, Ordering::Relaxed);
    }

    /// Set a config parameter, notifying shards if needed (async).
    ///
    /// This is the async version of `set` that also propagates eviction config
    /// changes to all shards and waits for acknowledgment.
    pub async fn set_async(&self, name: &str, value: &str) -> Result<(), ConfigError> {
        // First, apply the change (sync)
        self.set(name, value)?;

        // The parameter definition decides whether (and how) a change propagates
        // to shards — there is no out-of-band name list. (An immutable name never
        // reaches here: `set` above rejects it before any propagation.)
        let normalized = name.to_lowercase().replace('_', "-");
        let propagation = self
            .typed_param(&normalized)
            .map(|p| p.propagation())
            .unwrap_or(Propagation::None);

        match propagation {
            Propagation::None => {}
            Propagation::Eviction => {
                self.shard_notifier.notify_eviction_change().await?;
            }
            Propagation::KeyMemoryHistograms => {
                let enabled = self.key_memory_histograms_enabled();
                self.shard_notifier
                    .notify_key_memory_histograms(enabled)
                    .await?;
            }
        }

        Ok(())
    }
}

/// Notifies shards of configuration changes synchronously.
///
/// This notifier is used to propagate runtime config changes (like maxmemory,
/// maxmemory-policy, etc.) to all shard workers. It sends UpdateConfig messages
/// to each shard and waits for all shards to acknowledge the update before returning.
pub struct ShardConfigNotifier {
    /// Senders to all shard workers.
    shard_senders: Arc<Vec<ShardSender>>,
    /// Reference to the runtime config for building eviction config.
    runtime: Arc<RwLock<RuntimeConfig>>,
    /// Number of shards.
    num_shards: usize,
}

impl ShardConfigNotifier {
    /// Create a new shard config notifier.
    pub fn new(
        shard_senders: Arc<Vec<ShardSender>>,
        runtime: Arc<RwLock<RuntimeConfig>>,
        num_shards: usize,
    ) -> Self {
        Self {
            shard_senders,
            runtime,
            num_shards,
        }
    }

    /// Notify all shards of an eviction config change.
    ///
    /// This method builds the new EvictionConfig from the current RuntimeConfig,
    /// sends UpdateConfig messages to all shards, and waits for all shards to
    /// acknowledge the update before returning.
    pub async fn notify_eviction_change(&self) -> Result<(), ConfigError> {
        // Build eviction config from current runtime config. The policy is stored
        // as a typed `EvictionPolicy` (validated at the set seam), so there is no
        // re-parse and no fallback here.
        let eviction_config = {
            let config = self.runtime.read().unwrap();
            EvictionConfig {
                maxmemory: config.maxmemory,
                policy: config.maxmemory_policy,
                maxmemory_samples: config.maxmemory_samples,
                lfu_log_factor: config.lfu_log_factor,
                lfu_decay_time: config.lfu_decay_time,
            }
        };

        let mut receivers = Vec::with_capacity(self.num_shards);

        // Send UpdateConfig to all shards
        for sender in self.shard_senders.iter() {
            let (tx, rx) = oneshot::channel();
            if let Err(e) = sender
                .send(ObservabilityMsg::UpdateConfig {
                    eviction_config: Some(eviction_config.clone()),
                    response_tx: tx,
                })
                .await
            {
                return Err(ConfigError::InvalidValue {
                    param: "internal".to_string(),
                    message: format!("failed to send config update to shard: {}", e),
                });
            }
            receivers.push(rx);
        }

        // Wait for all shards to acknowledge
        for rx in receivers {
            if let Err(e) = rx.await {
                return Err(ConfigError::InvalidValue {
                    param: "internal".to_string(),
                    message: format!("shard failed to acknowledge config update: {}", e),
                });
            }
        }

        tracing::info!(
            maxmemory = eviction_config.maxmemory,
            policy = ?eviction_config.policy,
            "Eviction config propagated to all shards"
        );

        Ok(())
    }

    /// Notify all shards of a key-memory-histograms config change.
    pub async fn notify_key_memory_histograms(&self, enabled: bool) -> Result<(), ConfigError> {
        let mut receivers = Vec::with_capacity(self.num_shards);

        for sender in self.shard_senders.iter() {
            let (tx, rx) = oneshot::channel();
            if let Err(e) = sender
                .send(ObservabilityMsg::SetKeyMemoryHistograms {
                    enabled,
                    response_tx: tx,
                })
                .await
            {
                return Err(ConfigError::InvalidValue {
                    param: "key-memory-histograms".to_string(),
                    message: format!("failed to send to shard: {}", e),
                });
            }
            receivers.push(rx);
        }

        for rx in receivers {
            if let Err(e) = rx.await {
                return Err(ConfigError::InvalidValue {
                    param: "key-memory-histograms".to_string(),
                    message: format!("shard failed to acknowledge: {}", e),
                });
            }
        }

        tracing::info!(enabled, "key-memory-histograms propagated to all shards");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> Config {
        Config::default()
    }

    #[test]
    fn test_config_get_all() {
        let config = test_config();
        let manager = ConfigManager::new(&config);

        let results = manager.get("*");
        assert!(!results.is_empty());
        assert!(results.iter().any(|(k, _)| k == "maxmemory"));
        assert!(results.iter().any(|(k, _)| k == "bind"));
    }

    #[test]
    fn test_config_get_pattern() {
        let config = test_config();
        let manager = ConfigManager::new(&config);

        let results = manager.get("max*");
        assert!(results.iter().all(|(k, _)| k.starts_with("max")));
        assert!(results.iter().any(|(k, _)| k == "maxmemory"));
        assert!(results.iter().any(|(k, _)| k == "maxmemory-policy"));
    }

    #[test]
    fn test_config_set_mutable() {
        let config = test_config();
        let manager = ConfigManager::new(&config);

        assert!(manager.set("maxmemory", "1048576").is_ok());
        let results = manager.get("maxmemory");
        assert_eq!(results[0].1, "1048576");
    }

    #[test]
    fn test_config_set_immutable() {
        let config = test_config();
        let manager = ConfigManager::new(&config);

        let result = manager.set("bind", "0.0.0.0");
        assert!(matches!(result, Err(ConfigError::ImmutableParameter(_))));
    }

    /// 13-01 Pass 2a: the newly-promoted immutable params are CONFIG GET-visible
    /// with their startup values and rejected by CONFIG SET with
    /// `ImmutableParameter`. Samples span persistence, snapshot, http, admin,
    /// cluster, tracing, server, tls, logging, latency-bands and acl so a
    /// regression in any one section's wiring is caught.
    #[test]
    fn test_config_get_promoted_immutable_params() {
        let config = test_config(); // Config::default()
        let manager = ConfigManager::new(&config);

        // (param name, expected CONFIG GET value at defaults).
        let expected: &[(&str, &str)] = &[
            ("write-buffer-size-mb", "64"),           // persistence
            ("compression", "lz4"),                   // persistence
            ("bloom-filter-bits", "10"),              // persistence
            ("snapshot-dir", "./snapshots"),          // snapshot
            ("http-enabled", "yes"),                  // http
            ("http-port", "9090"),                    // http
            ("admin-enabled", "no"),                  // admin
            ("admin-port", "6382"),                   // admin
            ("cluster-enabled", "no"),                // cluster
            ("cluster-data-dir", "./frogdb-cluster"), // cluster
            ("tracing-enabled", "no"),                // tracing
            ("sorted-set-index", "skiplist"),         // server
            ("enable-debug-command", "no"),           // server
            ("tls-enabled", "no"),                    // tls
            ("latency-bands", "1 5 10 50 100 500"),   // latency-bands
            ("aclfile", ""),                          // acl (empty by default)
            ("logfile", ""),                          // logging (console-only by default)
        ];

        for (name, want) in expected {
            let got = manager.get(name);
            assert_eq!(
                got.len(),
                1,
                "CONFIG GET {name} should return exactly one row"
            );
            assert_eq!(&got[0].0, name, "CONFIG GET returned wrong key for {name}");
            assert_eq!(&got[0].1, want, "CONFIG GET {name} value mismatch");

            // Every promoted param is immutable: CONFIG SET must be rejected.
            let set = manager.set(name, "1");
            assert!(
                matches!(set, Err(ConfigError::ImmutableParameter(_))),
                "CONFIG SET {name} should be rejected as ImmutableParameter, got {set:?}"
            );
        }

        // They are also reported under the immutable-name list, not the mutable one.
        let immutable = manager.immutable_param_names();
        for (name, _) in expected {
            assert!(
                immutable.contains(name),
                "{name} should be listed among immutable params"
            );
        }
    }

    /// 13-01 Pass 2b originally downgraded 20 startup-consumed params to
    /// promote-immutable. The config-mutability round promoted 19 of them once
    /// each had a live runtime seam; `compaction-rate-limit-mb` is the sole
    /// survivor, because librocksdb-sys exposes neither `rocksdb_set_db_options`
    /// nor the rate limiter's `SetBytesPerSecond`, so there is nothing to retune.
    #[test]
    fn test_config_get_promoted_immutable_params_pass2b() {
        let config = test_config(); // Config::default()
        let manager = ConfigManager::new(&config);

        let name = "compaction-rate-limit-mb";
        let got = manager.get(name);
        assert_eq!(got.len(), 1, "CONFIG GET {name} should return one row");
        assert_eq!(&got[0].0, name, "CONFIG GET returned wrong key for {name}");
        assert_eq!(&got[0].1, "0", "CONFIG GET {name} value mismatch");
        let set = manager.set(name, "1");
        assert!(
            matches!(set, Err(ConfigError::ImmutableParameter(_))),
            "CONFIG SET {name} should be rejected as ImmutableParameter, got {set:?}"
        );
        assert!(
            manager.immutable_param_names().contains(&name),
            "{name} should be listed among immutable params"
        );

        // The other 19 are now mutable and no longer reported as immutable.
        let promoted: &[&str] = &[
            "batch-size-threshold-kb",
            "snapshot-interval-secs",
            "replication-lag-threshold-bytes",
            "replication-lag-threshold-secs",
            "self-fence-on-replica-loss",
            "replica-freshness-timeout-ms",
            "cluster-auto-failover",
            "cluster-self-fence-on-quorum-loss",
            "replica-priority",
            "tls-cluster-migration",
            "tls-client-cert-file",
            "tls-client-key-file",
            "tls-handshake-timeout-ms",
            "tracing-sampling-rate",
            "status-memory-warning-percent",
            "status-connection-warning-percent",
            "status-durability-lag-warning-ms",
            "status-durability-lag-critical-ms",
            "latency-bands-enabled",
        ];
        let immutable = manager.immutable_param_names();
        for name in promoted {
            assert_eq!(
                manager.get(name).len(),
                1,
                "CONFIG GET {name} should return one row"
            );
            assert!(
                !immutable.contains(name),
                "{name} was promoted to mutable and must not be listed as immutable"
            );
        }
    }

    /// issue-14 wire pass: 6 of the 7 newly-wired config fields promoted to
    /// immutable are CONFIG GET-visible (reporting their honest startup value)
    /// and reject CONFIG SET with `ImmutableParameter`. Spans metrics OTLP, json
    /// limits and replication ACK cadence. (`tls-ciphersuites`, the seventh, is
    /// mutable as of the config-mutability round -- rustls is rebuilt from the
    /// new suite list -- and is covered by the TLS propagation-truth tests.)
    #[test]
    fn test_config_get_wired_immutable_params_issue14() {
        let config = test_config(); // Config::default()
        let manager = ConfigManager::new(&config);

        // (param name, expected CONFIG GET value at defaults).
        let expected: &[(&str, &str)] = &[
            ("metrics-otlp-enabled", "no"),                     // metrics (bool)
            ("metrics-otlp-endpoint", "http://localhost:4317"), // metrics
            ("metrics-otlp-interval-secs", "15"),               // metrics
            ("json-max-depth", "128"),                          // json
            ("json-max-size", "67108864"),                      // json (64 MiB)
            ("repl-ack-interval-ms", "1000"),                   // replication
        ];

        for (name, want) in expected {
            let got = manager.get(name);
            assert_eq!(
                got.len(),
                1,
                "CONFIG GET {name} should return exactly one row"
            );
            assert_eq!(&got[0].0, name, "CONFIG GET returned wrong key for {name}");
            assert_eq!(&got[0].1, want, "CONFIG GET {name} value mismatch");

            // Every promoted param is immutable: CONFIG SET must be rejected.
            let set = manager.set(name, "1");
            assert!(
                matches!(set, Err(ConfigError::ImmutableParameter(_))),
                "CONFIG SET {name} should be rejected as ImmutableParameter, got {set:?}"
            );

            // Reported under the immutable-name list, not the mutable one.
            assert!(
                manager.immutable_param_names().contains(name),
                "{name} should be listed among immutable params"
            );
        }
    }

    /// 13-01 Pass 2b: `acllog-max-len` is the sole promote-mutable survivor of the
    /// propagation-truth audit. CONFIG SET must (a) be accepted, (b) be reflected
    /// by CONFIG GET, and (c) actually change the propagated target -- the live
    /// ACL-log length atomic the manager re-reads on every append.
    #[test]
    fn test_config_set_acllog_max_len_roundtrip() {
        let config = test_config();
        let manager = ConfigManager::new(&config);

        // Default is visible via GET and mutable (listed as a settable param).
        let before = manager.get("acllog-max-len");
        assert_eq!(before.len(), 1);
        assert_eq!(before[0].0, "acllog-max-len");
        assert_eq!(
            before[0].1,
            frogdb_config::security::DEFAULT_ACL_LOG_MAX_LEN.to_string()
        );

        // (a) SET accepted.
        assert!(manager.set("acllog-max-len", "7").is_ok());
        // (b) GET returns the new value.
        assert_eq!(manager.get("acllog-max-len")[0].1, "7");
        // (c) the propagated target changed: GET reads through the shared
        // Arc<AclManager> atomic, so this value came from the live ACL log.
        // Exercise the behavioral effect end-to-end: pushing more than 7 events
        // trims the log to the new bound.
        let log = manager.acl_manager.log();
        assert_eq!(log.max_len(), 7);
        for i in 0..20 {
            log.log_command_denied(&format!("user{i}"), "127.0.0.1:1", "GET");
        }
        assert_eq!(log.len(), 7, "ACL log should trim to the CONFIG SET bound");

        // Rejects a non-integer value.
        assert!(matches!(
            manager.set("acllog-max-len", "not-a-number"),
            Err(ConfigError::InvalidValue { .. })
        ));
    }

    /// 13-01 Pass 2b: CONFIG REWRITE persists the new mutable `acllog-max-len`
    /// into the `[acl]` section using the file's own field name (`log-max-len`).
    #[test]
    fn test_rewrite_config_acllog_max_len() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("frogdb.toml");
        std::fs::write(
            &config_path,
            r#"[server]
bind = "127.0.0.1"
port = 6379

[acl]
log-max-len = 128
"#,
        )
        .unwrap();

        let mut config = test_config();
        config.config_source_path = Some(config_path.clone());
        let manager = ConfigManager::new(&config);

        manager.set("acllog-max-len", "42").unwrap();
        assert!(manager.rewrite_config().is_ok());

        let contents = std::fs::read_to_string(&config_path).unwrap();
        assert!(
            contents.contains("log-max-len = 42"),
            "acllog-max-len not rewritten into [acl]; file:\n{contents}"
        );
    }

    #[test]
    fn test_config_set_unknown() {
        let config = test_config();
        let manager = ConfigManager::new(&config);

        let result = manager.set("unknown-param", "value");
        assert!(matches!(result, Err(ConfigError::UnknownParameter(_))));
    }

    #[test]
    fn test_config_set_invalid_value() {
        let config = test_config();
        let manager = ConfigManager::new(&config);

        let result = manager.set("maxmemory", "not-a-number");
        assert!(matches!(result, Err(ConfigError::InvalidValue { .. })));
    }

    #[test]
    fn test_config_set_invalid_policy() {
        let config = test_config();
        let manager = ConfigManager::new(&config);

        let result = manager.set("maxmemory-policy", "invalid-policy");
        assert!(matches!(result, Err(ConfigError::InvalidValue { .. })));
    }

    #[test]
    fn test_config_set_valid_policy() {
        let config = test_config();
        let manager = ConfigManager::new(&config);

        assert!(manager.set("maxmemory-policy", "allkeys-lru").is_ok());
        let results = manager.get("maxmemory-policy");
        assert_eq!(results[0].1, "allkeys-lru");
    }

    #[test]
    fn test_config_set_loglevel() {
        let config = test_config();
        let manager = ConfigManager::new(&config);

        assert!(manager.set("loglevel", "debug").is_ok());
        let results = manager.get("loglevel");
        assert_eq!(results[0].1, "debug");
    }

    #[test]
    fn test_config_set_invalid_loglevel() {
        let config = test_config();
        let manager = ConfigManager::new(&config);

        let result = manager.set("loglevel", "invalid");
        assert!(matches!(result, Err(ConfigError::InvalidValue { .. })));
    }

    #[test]
    fn test_parameter_name_mapping() {
        let config = test_config();
        let manager = ConfigManager::new(&config);

        // Test underscore to dash conversion
        assert!(manager.set("maxmemory_policy", "allkeys-lfu").is_ok());

        // Test case insensitivity
        assert!(manager.set("MAXMEMORY", "2048").is_ok());
    }

    #[test]
    fn test_maxmemory_samples_validation() {
        let config = test_config();
        let manager = ConfigManager::new(&config);

        let result = manager.set("maxmemory-samples", "0");
        assert!(matches!(result, Err(ConfigError::InvalidValue { .. })));

        assert!(manager.set("maxmemory-samples", "10").is_ok());
    }

    #[test]
    fn test_help_text() {
        let config = test_config();
        let manager = ConfigManager::new(&config);
        let help = manager.help_text();
        assert!(!help.is_empty());
        assert!(help[0].contains("CONFIG"));
        // Verify auto-generated param lists contain known params
        let mutable_line = help.iter().find(|l| l.starts_with("Mutable")).unwrap();
        assert!(mutable_line.contains("maxmemory"));
        assert!(mutable_line.contains("loglevel"));
        let immutable_line = help.iter().find(|l| l.starts_with("Immutable")).unwrap();
        assert!(immutable_line.contains("bind"));
        assert!(immutable_line.contains("port"));
    }

    #[test]
    fn test_param_registry_consistency() {
        // What this test used to guard is now enforced earlier and more strongly,
        // so only the residual gap remains here:
        //
        // * The mutable/immutable **partition** (every mutable metadata row is
        //   served by the typed registry and never the legacy one, and vice
        //   versa) is now a *compile-time* guarantee. `build_typed_params` is an
        //   exhaustive `match` over `MutableParamId::ALL` and `build_param_registry`
        //   an exhaustive `match` over `ImmutableParamId::ALL`, so a missing
        //   handler is a `non-exhaustive patterns` error. That the two identity
        //   rosters equal the registry's mutable/immutable partitions (and are
        //   disjoint) is pinned by `frogdb_config::param_id`'s own tests. Together
        //   these make the former name-set and partition assertions redundant.
        //
        // * The **noop ⟺ NoopParam** correspondence is *not* compiler-enforced:
        //   nothing stops a `#[param(noop)]`/virtual-noop identity's match arm
        //   from building a real `ConfigParam` (or vice versa). Guard only that
        //   here, keyed off the derived metadata `noop` flag and the runtime
        //   `DynParam::is_noop()` accessor.
        let typed = ConfigManager::build_typed_params();
        let config_params = frogdb_config::config_param_registry();

        // Names of the typed entries that are Redis-compat no-ops.
        let noop_names: Vec<&str> = typed
            .iter()
            .filter(|p| p.is_noop())
            .map(|p| p.name())
            .collect();

        for info in config_params {
            // noop ⟺ the serving typed entry is a NoopParam.
            if info.noop {
                assert!(
                    noop_names.contains(&info.name),
                    "'{}' is noop in metadata but its typed entry is not a NoopParam",
                    info.name
                );
            } else {
                assert!(
                    !noop_names.contains(&info.name),
                    "'{}' is not noop in metadata but its typed entry is a NoopParam",
                    info.name
                );
            }
        }
    }

    #[test]
    fn test_maxmemory_policy_matches_eviction_policy_enum() {
        // The CONFIG SET legal-value set for maxmemory-policy *is*
        // `EvictionPolicy::from_str`, so every variant round-trips and the config
        // crate's startup validation list cannot silently drift from the enum.
        let config = test_config();
        let manager = ConfigManager::new(&config);
        for name in EvictionPolicy::all_names() {
            assert!(
                manager.set("maxmemory-policy", name).is_ok(),
                "EvictionPolicy variant '{}' should be accepted by CONFIG SET",
                name
            );
            let got = &manager.get("maxmemory-policy")[0].1;
            assert_eq!(got, name, "round-trip mismatch for policy '{}'", name);
        }
        assert!(manager.set("maxmemory-policy", "bogus-policy").is_err());

        // Pin the config crate's startup validation to the same enum, so the two
        // legal-value sources cannot drift apart across the crate boundary.
        for name in EvictionPolicy::all_names() {
            let cfg = frogdb_config::MemoryConfig {
                maxmemory_policy: name.to_string(),
                ..Default::default()
            };
            assert!(
                cfg.validate().is_ok(),
                "MemoryConfig::validate should accept EvictionPolicy variant '{}'",
                name
            );
        }
        let bad = frogdb_config::MemoryConfig {
            maxmemory_policy: "bogus-policy".to_string(),
            ..Default::default()
        };
        assert!(bad.validate().is_err());
    }

    #[test]
    fn test_rewrite_config_no_file_path() {
        let config = test_config();
        let manager = ConfigManager::new(&config);
        // No config file path set, should error
        let result = manager.rewrite_config();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("without a config file"));
    }

    #[test]
    fn test_rewrite_config_basic() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("frogdb.toml");
        std::fs::write(
            &config_path,
            r#"# Test config
[server]
bind = "127.0.0.1"
port = 6379

[memory]
maxmemory = 0
maxmemory-policy = "noeviction"
"#,
        )
        .unwrap();

        let mut config = test_config();
        config.config_source_path = Some(config_path.clone());
        let manager = ConfigManager::new(&config);

        // Change maxmemory at runtime
        manager.set("maxmemory", "1048576").unwrap();

        // Rewrite config
        let result = manager.rewrite_config();
        assert!(result.is_ok(), "rewrite failed: {:?}", result);

        // Verify the file was updated
        let contents = std::fs::read_to_string(&config_path).unwrap();
        assert!(
            contents.contains("1048576"),
            "maxmemory not updated in file"
        );
        // Verify comments are preserved
        assert!(contents.contains("# Test config"), "comment not preserved");
    }

    #[test]
    fn test_rewrite_config_preserves_comments() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("frogdb.toml");
        std::fs::write(
            &config_path,
            r#"# FrogDB Configuration
# This is important

[server]
bind = "127.0.0.1"  # Listen address
port = 6379  # Redis-compatible port
num-shards = 1

[logging]
# Log level configuration
level = "info"

[memory]
maxmemory = 0  # 0 means no limit
maxmemory-policy = "noeviction"
"#,
        )
        .unwrap();

        let mut config = test_config();
        config.config_source_path = Some(config_path.clone());
        let manager = ConfigManager::new(&config);

        // Change log level
        manager.set("loglevel", "debug").unwrap();

        let result = manager.rewrite_config();
        assert!(result.is_ok());

        let contents = std::fs::read_to_string(&config_path).unwrap();
        // Check comments are preserved
        assert!(contents.contains("# FrogDB Configuration"));
        assert!(contents.contains("# This is important"));
        // The value was updated
        assert!(contents.contains("\"debug\""));
    }

    #[test]
    fn test_rewrite_config_creates_missing_sections() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("frogdb.toml");
        // Write a minimal file with no [memory] section
        std::fs::write(
            &config_path,
            r#"[server]
bind = "127.0.0.1"
port = 6379
"#,
        )
        .unwrap();

        let mut config = test_config();
        config.config_source_path = Some(config_path.clone());
        let manager = ConfigManager::new(&config);

        let result = manager.rewrite_config();
        assert!(result.is_ok());

        let contents = std::fs::read_to_string(&config_path).unwrap();
        // Memory section should have been created
        assert!(contents.contains("[memory]"));
    }

    #[test]
    fn test_rewrite_config_noop_params_not_written() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("frogdb.toml");
        std::fs::write(
            &config_path,
            r#"[server]
bind = "127.0.0.1"
port = 6379
"#,
        )
        .unwrap();

        let mut config = test_config();
        config.config_source_path = Some(config_path.clone());
        let manager = ConfigManager::new(&config);

        // Set a no-op param
        manager.set("save", "900 1").unwrap();

        let result = manager.rewrite_config();
        assert!(result.is_ok());

        let contents = std::fs::read_to_string(&config_path).unwrap();
        // No-op params should not appear in the file
        assert!(!contents.contains("save"));
    }

    #[test]
    fn test_rewrite_config_min_replicas_max_lag_conversion() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("frogdb.toml");
        std::fs::write(
            &config_path,
            r#"[replication]
min-replicas-to-write = 0
min-replicas-timeout-ms = 5000
"#,
        )
        .unwrap();

        let mut config = test_config();
        config.config_source_path = Some(config_path.clone());
        let manager = ConfigManager::new(&config);

        // Set min-replicas-max-lag to 10 seconds
        manager.set("min-replicas-max-lag", "10").unwrap();

        let result = manager.rewrite_config();
        assert!(result.is_ok());

        let contents = std::fs::read_to_string(&config_path).unwrap();
        // Should be written as 10000 ms in the TOML file
        assert!(
            contents.contains("min-replicas-timeout-ms = 10000"),
            "expected 10000ms, got: {}",
            contents
        );
    }

    // === Per-type `ToTomlValue` coercion tests ===
    //
    // These replace the old `string_to_toml_value` tests: instead of asserting
    // that a heuristic *guesses* the right TOML type from a formatted string,
    // they assert that each parameter's own type renders itself correctly --
    // so a numeric-looking `String` value never gets coerced to a TOML
    // integer, and so on.

    /// A set value's TOML rendering. Panics on `None`, which every type
    /// exercised below is expected never to produce (only
    /// [`OptionalPathValue`] can be unset).
    fn toml(v: impl ToTomlValue) -> TomlValue {
        v.to_toml_value().expect("value is set")
    }

    #[test]
    fn to_toml_value_bool_renders_as_toml_bool() {
        assert_eq!(toml(true).as_bool(), Some(true));
        assert_eq!(toml(false).as_bool(), Some(false));
    }

    #[test]
    fn to_toml_value_integer_types_render_as_toml_integer() {
        assert_eq!(toml(42u64).as_integer(), Some(42));
        assert_eq!(toml(7u8).as_integer(), Some(7));
        assert_eq!(toml(-1i64).as_integer(), Some(-1));
    }

    #[test]
    fn to_toml_value_string_never_coerces_to_bool_or_integer() {
        // The exact bug class named in the task: `maxmemory-clients` is a
        // `String`-typed parameter (accepts "50%" as well as a byte count),
        // so a value that merely *looks* like an integer or a boolean must
        // still render as a TOML string, not get re-guessed into another type.
        let v = toml("42".to_string());
        assert!(v.is_str(), "expected TOML string, got: {v:?}");
        assert_eq!(v.as_str(), Some("42"));

        let v = toml("yes".to_string());
        assert!(v.is_str(), "expected TOML string, got: {v:?}");
        assert_eq!(v.as_str(), Some("yes"));
    }

    #[test]
    fn to_toml_value_eviction_policy_renders_as_toml_string() {
        let v = toml(EvictionPolicy::AllkeysLru);
        assert!(v.is_str());
        assert_eq!(v.as_str(), Some("allkeys-lru"));
    }

    #[test]
    fn to_toml_value_min_replicas_max_lag_converts_seconds_to_ms() {
        // The unit conversion now lives on `MinReplicasMaxLagSecs` itself,
        // rather than as a name check in the file writer.
        assert_eq!(toml(MinReplicasMaxLagSecs(10)).as_integer(), Some(10_000));
    }

    #[test]
    fn to_toml_value_client_cert_mode_renders_file_encoding() {
        // Distinct from the CONFIG GET display value ("no"/"optional"/"yes"):
        // the TOML file encodes this as "none"/"optional"/"required".
        assert_eq!(toml(ClientCertMode::None).as_str(), Some("none"));
        assert_eq!(toml(ClientCertMode::Optional).as_str(), Some("optional"));
        assert_eq!(toml(ClientCertMode::Required).as_str(), Some("required"));
    }

    #[test]
    fn to_toml_value_tls_protocols_renders_toml_array_in_file_encoding() {
        // Distinct from the CONFIG GET display value ("TLSv1.2 TLSv1.3"): the
        // TOML file encodes this as an array of "1.2"/"1.3".
        let v = toml(vec![TlsProtocol::Tls12, TlsProtocol::Tls13]);
        let arr = v.as_array().expect("expected a TOML array");
        let rendered: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
        assert_eq!(rendered, vec!["1.2", "1.3"]);
    }

    #[test]
    fn to_toml_value_optional_path_renders_unset_as_absent() {
        // C1: the empty wire value means "unset", which REWRITE must express by
        // *removing* the key. Rendering `""` would round-trip as `Some("")` and
        // fail the next boot with "does not exist".
        assert!(OptionalPathValue(String::new()).to_toml_value().is_none());
        assert_eq!(
            toml(OptionalPathValue("/etc/frogdb/ca.pem".to_string())).as_str(),
            Some("/etc/frogdb/ca.pem")
        );
    }

    #[test]
    fn test_config_file_path_getter_setter() {
        let config = test_config();
        let manager = ConfigManager::new(&config);

        assert!(manager.config_file_path().is_none());

        let path = PathBuf::from("/tmp/test.toml");
        manager.set_config_file_path(path.clone());
        assert_eq!(manager.config_file_path(), Some(path));
    }

    #[test]
    fn test_rewrite_config_output_is_valid_toml() {
        // Minimal config file - rewrite should produce valid TOML
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("frogdb.toml");
        std::fs::write(
            &config_path,
            r#"[server]
bind = "127.0.0.1"
port = 6379

[memory]
maxmemory = 0
"#,
        )
        .unwrap();

        let mut config = test_config();
        config.config_source_path = Some(config_path.clone());
        let manager = ConfigManager::new(&config);

        let result = manager.rewrite_config();
        assert!(result.is_ok(), "rewrite failed: {:?}", result);

        let contents = std::fs::read_to_string(&config_path).unwrap();
        // Verify it parses as valid TOML using the toml_edit parser
        let parsed: Result<toml_edit::DocumentMut, _> = contents.parse();
        assert!(
            parsed.is_ok(),
            "Output is not valid TOML:\n{}\nError: {:?}",
            contents,
            parsed.err()
        );
    }

    #[test]
    fn test_rewrite_config_output_is_valid_toml_value() {
        // Same test but parse with toml::Value (the way integration tests do it)
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("frogdb.toml");
        std::fs::write(
            &config_path,
            r#"[server]
bind = "127.0.0.1"
port = 6379

[memory]
maxmemory = 0
"#,
        )
        .unwrap();

        let mut config = test_config();
        config.config_source_path = Some(config_path.clone());
        let manager = ConfigManager::new(&config);

        let result = manager.rewrite_config();
        assert!(result.is_ok(), "rewrite failed: {:?}", result);

        let contents = std::fs::read_to_string(&config_path).unwrap();
        // Verify the output is valid TOML syntax by re-parsing with toml_edit
        let reparsed: Result<toml_edit::DocumentMut, _> = contents.parse();
        assert!(
            reparsed.is_ok(),
            "Output is not valid TOML:\n{}\nError: {:?}",
            contents,
            reparsed.err()
        );
    }

    // === Injected-collaborator side-effect tests ===
    //
    // These prove that the side-effecting CONFIG SET paths reach the real,
    // non-optional collaborators injected at construction -- the exact behavior
    // that the old `RwLock<Option<Arc<..>>>` + post-construction setters made
    // easy to silently drop (an unwired collaborator meant a quiet no-op).

    /// Build a ConfigManager over `config` whose collaborators are the defaults
    /// except for the fields supplied via `f`, which mutates the bundle so the
    /// test can retain handles to the exact injected instances.
    fn manager_with(
        config: &Config,
        f: impl FnOnce(&Arc<RwLock<RuntimeConfig>>, &mut ConfigCollaborators),
    ) -> ConfigManager {
        let runtime = Arc::new(RwLock::new(RuntimeConfig::from_config(config)));
        let mut collaborators = ConfigCollaborators::defaults(&runtime);
        f(&runtime, &mut collaborators);
        ConfigManager::with_collaborators(config, runtime, collaborators)
    }

    #[test]
    fn test_requirepass_routes_to_injected_acl_manager() {
        let config = test_config();
        let acl = frogdb_core::AclManager::new(Default::default());
        let manager = manager_with(&config, |_rt, c| c.acl_manager = acl.clone());

        // Before: a fresh default user is `nopass`, so it accepts any password.
        assert!(acl.authenticate("default", "wrong", "test").is_ok());

        manager.set("requirepass", "s3cret").unwrap();

        // After: the password now lives in the *injected* ACL manager -- the
        // correct password authenticates and a wrong one is rejected, proving
        // the `requirepass` apply closure reached the real collaborator (it
        // flipped `nopass` off and installed the hash) rather than silently
        // no-opping.
        assert!(acl.authenticate("default", "s3cret", "test").is_ok());
        assert!(acl.authenticate("default", "wrong", "test").is_err());
    }

    #[test]
    fn test_latency_tracking_toggles_injected_histograms() {
        let config = test_config();
        let histograms = Arc::new(frogdb_core::CommandLatencyHistograms::new(true));
        let manager = manager_with(&config, |_rt, c| {
            c.latency_histograms = histograms.clone();
        });

        assert!(histograms.is_enabled());

        manager.set("latency-tracking", "no").unwrap();
        assert!(
            !histograms.is_enabled(),
            "CONFIG SET latency-tracking no must disable the injected histograms"
        );

        manager.set("latency-tracking", "yes").unwrap();
        assert!(
            histograms.is_enabled(),
            "CONFIG SET latency-tracking yes must re-enable the injected histograms"
        );
    }

    #[test]
    fn test_latency_tracking_get_reads_injected_histograms() {
        let config = test_config();
        // Histograms constructed disabled: CONFIG GET must reflect that, not the
        // old `.unwrap_or(true)` default that hid an absent collaborator.
        let histograms = Arc::new(frogdb_core::CommandLatencyHistograms::new(false));
        let manager = manager_with(&config, |_rt, c| {
            c.latency_histograms = histograms.clone();
        });

        let got = manager.get("latency-tracking");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].1, "no");
    }

    #[test]
    fn test_maxmemory_clients_triggers_eviction_on_injected_registry() {
        let config = test_config();
        let registry = Arc::new(frogdb_core::ClientRegistry::new());
        let manager = manager_with(&config, |_rt, c| {
            c.client_eviction_registry = registry.clone();
        });

        // Register a client whose memory far exceeds the limit we set below.
        let handle = registry.register(1, "127.0.0.1:1000".parse().unwrap(), None);
        registry.update_memory(
            1,
            frogdb_core::ClientMemoryUsage {
                query_buf_size: 10_000,
                ..Default::default()
            },
        );
        assert!(!handle.is_killed());

        // A 1-byte limit forces eviction of the 10KB+ client. If the registry
        // were an absent Option (the old bug), this would silently no-op.
        manager.set("maxmemory-clients", "1").unwrap();

        assert!(
            handle.is_killed(),
            "CONFIG SET maxmemory-clients must evict via the injected client registry"
        );
    }

    #[test]
    fn test_maxmemory_clients_disabled_does_not_evict() {
        let config = test_config();
        let registry = Arc::new(frogdb_core::ClientRegistry::new());
        let manager = manager_with(&config, |_rt, c| {
            c.client_eviction_registry = registry.clone();
        });

        let handle = registry.register(1, "127.0.0.1:1000".parse().unwrap(), None);
        registry.update_memory(
            1,
            frogdb_core::ClientMemoryUsage {
                query_buf_size: 10_000,
                ..Default::default()
            },
        );

        // "0" disables the limit -> eviction must not fire.
        manager.set("maxmemory-clients", "0").unwrap();
        assert!(!handle.is_killed());
    }

    // === config-mutability round: propagation-truth tests ===
    //
    // Each promoted parameter is proved twice: CONFIG GET round-trips the new
    // value, and the *live* runtime object the server actually reads at decision
    // time observes the change. A test that only asserted GET would pass against
    // a parameter that stores into the manager and reaches nothing.

    #[test]
    fn cluster_flag_sets_reach_the_live_flags() {
        let config = test_config();
        let manager = ConfigManager::new(&config);
        let flags = manager.cluster_flags();

        assert!(!flags.auto_failover());
        manager.set("cluster-auto-failover", "yes").unwrap();
        assert!(flags.auto_failover(), "failover flag must flip live");
        assert_eq!(manager.get("cluster-auto-failover")[0].1, "yes");

        assert!(flags.self_fence_on_quorum_loss());
        manager
            .set("cluster-self-fence-on-quorum-loss", "no")
            .unwrap();
        assert!(!flags.self_fence_on_quorum_loss());
        assert_eq!(manager.get("cluster-self-fence-on-quorum-loss")[0].1, "no");

        manager.set("replica-priority", "42").unwrap();
        assert_eq!(flags.replica_priority(), 42);
        assert_eq!(manager.get("replica-priority")[0].1, "42");
    }

    #[test]
    fn status_threshold_sets_reach_the_live_thresholds() {
        let config = test_config();
        let manager = ConfigManager::new(&config);
        let thresholds = manager.status_thresholds();

        manager.set("status-memory-warning-percent", "75").unwrap();
        assert_eq!(thresholds.memory_warning_percent(), 75);
        assert_eq!(manager.get("status-memory-warning-percent")[0].1, "75");

        manager
            .set("status-connection-warning-percent", "80")
            .unwrap();
        assert_eq!(thresholds.connection_warning_percent(), 80);

        // Raise critical first: warning must stay strictly below it.
        manager
            .set("status-durability-lag-critical-ms", "60000")
            .unwrap();
        assert_eq!(thresholds.durability_lag_critical_ms(), 60000);
        manager
            .set("status-durability-lag-warning-ms", "10000")
            .unwrap();
        assert_eq!(thresholds.durability_lag_warning_ms(), 10000);
    }

    #[test]
    fn status_threshold_sets_enforce_the_section_validator_bounds() {
        let config = test_config();
        let manager = ConfigManager::new(&config);

        // 0 and >100 are what `StatusConfig::validate` rejects.
        assert!(manager.set("status-memory-warning-percent", "0").is_err());
        assert!(manager.set("status-memory-warning-percent", "101").is_err());
        assert!(
            manager
                .set("status-connection-warning-percent", "0")
                .is_err()
        );

        // warning < critical, checked against the live sibling in both
        // directions so the pair cannot be driven into an illegal ordering.
        let critical = manager.status_thresholds().durability_lag_critical_ms();
        assert!(
            manager
                .set("status-durability-lag-warning-ms", &critical.to_string())
                .is_err()
        );
        let warning = manager.status_thresholds().durability_lag_warning_ms();
        assert!(
            manager
                .set("status-durability-lag-critical-ms", &warning.to_string())
                .is_err()
        );
        // Rejected sets change nothing.
        assert_eq!(
            manager.status_thresholds().durability_lag_critical_ms(),
            critical
        );
    }

    #[test]
    fn tracing_sampling_rate_set_reaches_the_live_sampler_handle() {
        let config = test_config();
        let manager = ConfigManager::new(&config);
        let rate = manager.tracing_sampling_rate_handle();

        manager.set("tracing-sampling-rate", "0.25").unwrap();
        assert_eq!(rate.get(), 0.25);
        assert_eq!(manager.get("tracing-sampling-rate")[0].1, "0.25");

        // Same 0.0..=1.0 bound as `TracingConfig::validate`.
        assert!(manager.set("tracing-sampling-rate", "1.5").is_err());
        assert!(manager.set("tracing-sampling-rate", "-0.1").is_err());
        assert_eq!(rate.get(), 0.25, "a rejected set must not change the rate");
    }

    #[test]
    fn latency_bands_enabled_set_reaches_the_live_tracker() {
        let config = test_config();
        let tracker = Arc::new(frogdb_telemetry::LatencyBandTracker::new(
            vec![1000, 10_000],
            false,
        ));
        let manager = manager_with(&config, |_rt, c| {
            c.latency_band_tracker = tracker.clone();
        });

        manager.set("latency-bands-enabled", "yes").unwrap();
        assert!(tracker.is_enabled());
        assert_eq!(manager.get("latency-bands-enabled")[0].1, "yes");

        manager.set("latency-bands-enabled", "no").unwrap();
        assert!(!tracker.is_enabled());
    }

    #[test]
    fn hotshard_threshold_sets_reach_the_collector_handle() {
        let config = test_config();
        let manager = ConfigManager::new(&config);
        // The handle the collector adopts at startup.
        let shared = manager.hotshard_config();

        manager
            .set("hotshards-hot-threshold-percent", "40")
            .unwrap();
        assert_eq!(shared.hot_threshold_percent(), 40.0);
        manager
            .set("hotshards-warm-threshold-percent", "30")
            .unwrap();
        assert_eq!(shared.warm_threshold_percent(), 30.0);
        manager.set("hotshards-default-period-secs", "5").unwrap();
        assert_eq!(shared.default_period_secs(), 5);

        // `snapshot()` is what the collector classifies with.
        let snap = shared.snapshot();
        assert_eq!(snap.hot_threshold_percent, 40.0);
        assert_eq!(snap.warm_threshold_percent, 30.0);
        assert_eq!(snap.default_period_secs, 5);
    }

    #[test]
    fn hotshard_threshold_sets_enforce_the_section_validator_bounds() {
        let config = test_config();
        let manager = ConfigManager::new(&config);
        let shared = manager.hotshard_config();

        // 0..=100 range, as in `HotShardsConfig::validate`.
        assert!(
            manager
                .set("hotshards-hot-threshold-percent", "101")
                .is_err()
        );
        assert!(
            manager
                .set("hotshards-warm-threshold-percent", "-1")
                .is_err()
        );
        // warm <= hot, checked against the live sibling from both sides.
        assert!(
            manager
                .set("hotshards-warm-threshold-percent", "99")
                .is_err()
        );
        assert!(manager.set("hotshards-hot-threshold-percent", "1").is_err());
        // A zero sampling window would make every report empty.
        assert!(manager.set("hotshards-default-period-secs", "0").is_err());

        assert_eq!(
            shared.snapshot().hot_threshold_percent,
            frogdb_config::hotshards::DEFAULT_HOT_THRESHOLD_PERCENT,
            "rejected sets must leave the collector untouched"
        );
    }

    #[test]
    fn snapshot_interval_set_reaches_the_published_coordinator() {
        use frogdb_core::SnapshotCoordinator;

        let config = test_config();
        let manager = ConfigManager::new(&config);
        let coordinator = Arc::new(frogdb_core::persistence::NoopSnapshotCoordinator::new());
        manager.set_snapshot_coordinator(coordinator.clone());

        manager.set("snapshot-interval-secs", "120").unwrap();
        assert_eq!(
            coordinator.periodic_interval_secs(),
            120,
            "the periodic task reads its cadence from the coordinator"
        );
        assert_eq!(manager.get("snapshot-interval-secs")[0].1, "120");

        // 0 disables periodic saves without stopping the task.
        manager.set("snapshot-interval-secs", "0").unwrap();
        assert_eq!(coordinator.periodic_interval_secs(), 0);
    }

    #[test]
    fn snapshot_interval_set_before_publication_is_not_lost() {
        use frogdb_core::SnapshotCoordinator;

        let config = test_config();
        let manager = ConfigManager::new(&config);

        // A set that lands before server init publishes the coordinator.
        manager.set("snapshot-interval-secs", "900").unwrap();

        let coordinator = Arc::new(frogdb_core::persistence::NoopSnapshotCoordinator::new());
        manager.set_snapshot_coordinator(coordinator.clone());
        assert_eq!(
            coordinator.periodic_interval_secs(),
            900,
            "publication must sync the coordinator to the configured cadence"
        );
    }

    #[test]
    fn batch_size_threshold_set_reaches_the_shared_wal_cell() {
        let config = test_config();
        let manager = ConfigManager::new(&config);
        // The exact cell every RocksWalWriter adopts through
        // `WalConfig::batch_size_threshold_handle`.
        let cell = manager.wal_batch_size_threshold_handle();

        manager.set("batch-size-threshold-kb", "512").unwrap();
        assert_eq!(
            cell.load(Ordering::Relaxed),
            512 * 1024,
            "the wire value is KiB; flush threads compare bytes"
        );
        assert_eq!(manager.get("batch-size-threshold-kb")[0].1, "512");

        // A zero threshold would flush every single entry.
        assert!(manager.set("batch-size-threshold-kb", "0").is_err());
        assert_eq!(cell.load(Ordering::Relaxed), 512 * 1024);
    }

    #[test]
    fn replication_lag_threshold_sets_reach_the_published_thresholds() {
        let config = test_config();
        let manager = ConfigManager::new(&config);
        let thresholds = Arc::new(frogdb_replication::LagThresholds::new(0, 0));
        manager.set_replication_lag_thresholds(thresholds.clone());

        manager
            .set("replication-lag-threshold-bytes", "1048576")
            .unwrap();
        assert_eq!(thresholds.threshold_bytes(), 1_048_576);
        assert_eq!(
            manager.get("replication-lag-threshold-bytes")[0].1,
            "1048576"
        );

        manager.set("replication-lag-threshold-secs", "30").unwrap();
        assert_eq!(thresholds.threshold_secs(), 30);
        assert_eq!(manager.get("replication-lag-threshold-secs")[0].1, "30");
    }

    #[test]
    fn replication_lag_thresholds_are_recorded_without_a_primary_handler() {
        // On a replica there is no primary-side lag machinery to retune. The set
        // must still be accepted and reported, so it takes effect if the node is
        // later initialised as a primary.
        let config = test_config();
        let manager = ConfigManager::new(&config);

        manager
            .set("replication-lag-threshold-bytes", "4096")
            .unwrap();
        assert_eq!(manager.get("replication-lag-threshold-bytes")[0].1, "4096");

        let thresholds = Arc::new(frogdb_replication::LagThresholds::new(0, 0));
        manager.set_replication_lag_thresholds(thresholds.clone());
        assert_eq!(
            thresholds.threshold_bytes(),
            4096,
            "publication must carry the recorded value into the live handle"
        );
    }

    #[test]
    fn self_fence_sets_reach_the_published_quorum_checker() {
        let config = test_config();
        let manager = ConfigManager::new(&config);
        let checker = Arc::new(crate::replication_quorum::ReplicationQuorumChecker::new(
            Arc::new(frogdb_core::ReplicationTrackerImpl::new()),
            true,
            std::time::Duration::from_millis(3000),
        ));
        manager.set_replication_self_fence(checker.clone());

        manager.set("self-fence-on-replica-loss", "no").unwrap();
        assert!(!checker.self_fence_enabled());
        assert_eq!(manager.get("self-fence-on-replica-loss")[0].1, "no");

        manager.set("replica-freshness-timeout-ms", "7500").unwrap();
        assert_eq!(checker.freshness_timeout().as_millis(), 7500);
        assert_eq!(manager.get("replica-freshness-timeout-ms")[0].1, "7500");

        // A zero freshness window would make every replica instantly stale.
        assert!(manager.set("replica-freshness-timeout-ms", "0").is_err());
        assert_eq!(checker.freshness_timeout().as_millis(), 7500);
    }

    // === TLS propagation truth ===
    //
    // TLS parameters are the only family whose live state is rustls itself, so
    // these drive a real `TlsRuntimeHandle` and complete real loopback
    // handshakes: the assertion is which certificate the server presents, not
    // merely what the manager remembers.

    #[cfg(not(feature = "turmoil"))]
    fn manager_with_tls(
        config: &Config,
        cert: std::path::PathBuf,
        key: std::path::PathBuf,
    ) -> (ConfigManager, Arc<crate::tls_runtime::TlsRuntimeHandle>) {
        let tls_config = frogdb_config::TlsConfig {
            enabled: true,
            cert_file: cert,
            key_file: key,
            ..Default::default()
        };
        let handle = Arc::new(crate::tls_runtime::TlsRuntimeHandle::new(&tls_config).unwrap());
        let manager = ConfigManager::new(config);
        manager.set_tls_runtime(handle.clone());
        (manager, handle)
    }

    #[cfg(not(feature = "turmoil"))]
    #[tokio::test]
    async fn tls_identity_sets_are_atomic_against_a_mismatched_pair() {
        use crate::tls_runtime::test_support::{handshake_leaf, write_identity};

        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_identity(dir.path(), "first");
        let (other_cert, other_key) = write_identity(dir.path(), "second");
        let (manager, handle) = manager_with_tls(&test_config(), cert.clone(), key.clone());

        let before = handshake_leaf(&handle).await;

        // Each set rebuilds rustls from the full config, and `CertifiedKey`
        // checks the key against the leaf. Pointing one half of the pair at a
        // different identity is therefore rejected instead of producing a
        // server that fails every later handshake.
        assert!(
            manager
                .set("tls-key-file", other_key.to_str().unwrap())
                .is_err()
        );
        assert!(
            manager
                .set("tls-cert-file", other_cert.to_str().unwrap())
                .is_err()
        );

        assert_eq!(
            before,
            handshake_leaf(&handle).await,
            "rejected sets must leave the served identity untouched"
        );
        // CONFIG GET reads the live handle, so it still reports the running pair.
        assert_eq!(
            manager.get("tls-cert-file")[0].1,
            cert.display().to_string()
        );
        assert_eq!(manager.get("tls-key-file")[0].1, key.display().to_string());
    }

    #[cfg(not(feature = "turmoil"))]
    #[tokio::test]
    async fn tls_cert_rotation_in_place_is_visible_to_config_get_and_clients() {
        use crate::tls_runtime::test_support::{handshake_leaf, rotate_in_place, write_identity};

        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_identity(dir.path(), "server");
        let (manager, handle) = manager_with_tls(&test_config(), cert.clone(), key.clone());

        let before = handshake_leaf(&handle).await;
        let rotated_der = rotate_in_place(&cert, &key);

        // Re-setting the same path re-reads the file: the operator-visible
        // "rotate under the configured path" flow, driven by CONFIG SET.
        manager
            .set("tls-cert-file", cert.to_str().unwrap())
            .unwrap();

        let after = handshake_leaf(&handle).await;
        assert_ne!(before, after, "clients must be served the rotated leaf");
        assert_eq!(after, rotated_der);
        assert_eq!(
            manager.get("tls-cert-file")[0].1,
            cert.display().to_string()
        );
    }

    #[cfg(not(feature = "turmoil"))]
    #[tokio::test]
    async fn tls_cert_file_set_to_a_bad_path_errors_and_keeps_serving() {
        use crate::tls_runtime::test_support::{handshake_leaf, write_identity};

        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_identity(dir.path(), "server");
        let (manager, handle) = manager_with_tls(&test_config(), cert.clone(), key);

        let before = handshake_leaf(&handle).await;
        let err = manager
            .set(
                "tls-cert-file",
                dir.path().join("nope.crt").to_str().unwrap(),
            )
            .unwrap_err();
        assert!(
            matches!(err, ConfigError::InvalidValue { ref param, .. } if param == "tls-cert-file"),
            "unexpected error: {err:?}"
        );

        // Build-then-commit: neither the stored config nor the served leaf moved.
        assert_eq!(
            manager.get("tls-cert-file")[0].1,
            cert.display().to_string()
        );
        assert_eq!(before, handshake_leaf(&handle).await);
    }

    #[cfg(not(feature = "turmoil"))]
    #[test]
    fn tls_ciphersuite_set_is_validated_by_rustls_and_round_trips() {
        use crate::tls_runtime::test_support::write_identity;

        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_identity(dir.path(), "server");
        let (manager, handle) = manager_with_tls(&test_config(), cert, key);

        manager
            .set("tls-ciphersuites", "TLS13_AES_256_GCM_SHA384")
            .unwrap();
        assert_eq!(
            handle.current_config().ciphersuites,
            vec!["TLS13_AES_256_GCM_SHA384".to_string()]
        );
        assert_eq!(
            manager.get("tls-ciphersuites")[0].1,
            "TLS13_AES_256_GCM_SHA384",
            "CONFIG GET renders the live list space-joined"
        );

        // An unknown name is rejected by the rustls rebuild, not by a duplicated
        // allow-list in the config layer.
        let err = manager.set("tls-ciphersuites", "TLS_NOPE").unwrap_err();
        assert!(format!("{err:?}").contains("unknown tls.ciphersuites"));
        assert_eq!(
            handle.current_config().ciphersuites,
            vec!["TLS13_AES_256_GCM_SHA384".to_string()],
            "a rejected suite list must not be committed"
        );
    }

    #[cfg(not(feature = "turmoil"))]
    #[test]
    fn tls_handshake_timeout_and_cluster_migration_sets_reach_live_flags() {
        use crate::tls_runtime::test_support::write_identity;

        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_identity(dir.path(), "server");
        let (manager, handle) = manager_with_tls(&test_config(), cert, key);

        manager.set("tls-handshake-timeout-ms", "2500").unwrap();
        assert_eq!(
            handle.handshake_timeout().millis(),
            2500,
            "accept loops hold this shared timeout"
        );
        assert_eq!(manager.get("tls-handshake-timeout-ms")[0].1, "2500");
        assert!(manager.set("tls-handshake-timeout-ms", "0").is_err());

        let flag = handle.cluster_migration_flag();
        manager.set("tls-cluster-migration", "yes").unwrap();
        assert!(
            flag.load(Ordering::Relaxed),
            "the cluster bus reads this flag per inbound connection"
        );
        assert_eq!(manager.get("tls-cluster-migration")[0].1, "yes");
    }

    #[cfg(not(feature = "turmoil"))]
    #[test]
    fn tls_optional_path_sets_round_trip_and_clear_on_empty() {
        use crate::tls_runtime::test_support::write_identity;

        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_identity(dir.path(), "server");
        let (client_cert, client_key) = write_identity(dir.path(), "client");
        let (manager, handle) = manager_with_tls(&test_config(), cert, key);

        manager
            .set("tls-client-cert-file", client_cert.to_str().unwrap())
            .unwrap();
        manager
            .set("tls-client-key-file", client_key.to_str().unwrap())
            .unwrap();
        assert_eq!(handle.current_config().client_cert_file, Some(client_cert));
        assert_eq!(handle.current_config().client_key_file, Some(client_key));

        // The empty string is how CONFIG SET clears an optional path.
        manager.set("tls-client-cert-file", "").unwrap();
        manager.set("tls-client-key-file", "").unwrap();
        assert_eq!(handle.current_config().client_cert_file, None);
        assert_eq!(manager.get("tls-client-cert-file")[0].1, "");
        assert_eq!(manager.get("tls-client-key-file")[0].1, "");

        // The CA bundle follows the same optional-path contract.
        let (ca, _ca_key) = write_identity(dir.path(), "ca");
        manager
            .set("tls-ca-cert-file", ca.to_str().unwrap())
            .unwrap();
        assert_eq!(handle.current_config().ca_file, Some(ca.clone()));
        assert_eq!(
            manager.get("tls-ca-cert-file")[0].1,
            ca.display().to_string()
        );
        manager.set("tls-ca-cert-file", "").unwrap();
        assert_eq!(handle.current_config().ca_file, None);
        assert_eq!(manager.get("tls-ca-cert-file")[0].1, "");
    }

    /// With `require-client-cert = none` the CA bundle is only recorded, so the
    /// file is not read until a client verifier is built. Under mutual TLS it is
    /// loaded on every rebuild, and a bad path must fail the set rather than
    /// leave the server unable to verify clients.
    #[cfg(not(feature = "turmoil"))]
    #[test]
    fn tls_ca_cert_file_set_is_validated_under_mutual_tls() {
        use crate::tls_runtime::test_support::write_identity;

        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_identity(dir.path(), "server");
        let (ca, _ca_key) = write_identity(dir.path(), "ca");
        let tls_config = frogdb_config::TlsConfig {
            enabled: true,
            cert_file: cert,
            key_file: key,
            ca_file: Some(ca.clone()),
            require_client_cert: frogdb_config::ClientCertMode::Required,
            ..Default::default()
        };
        let handle = Arc::new(crate::tls_runtime::TlsRuntimeHandle::new(&tls_config).unwrap());
        let manager = ConfigManager::new(&test_config());
        manager.set_tls_runtime(handle.clone());

        let (ca2, _ca2_key) = write_identity(dir.path(), "ca2");
        manager
            .set("tls-ca-cert-file", ca2.to_str().unwrap())
            .unwrap();
        assert_eq!(handle.current_config().ca_file, Some(ca2.clone()));

        assert!(
            manager
                .set(
                    "tls-ca-cert-file",
                    dir.path().join("absent.pem").to_str().unwrap()
                )
                .is_err()
        );
        assert_eq!(
            handle.current_config().ca_file,
            Some(ca2),
            "a failed CA reload must not be committed"
        );

        // Clearing the bundle while client certs are required is refused by the
        // rustls rebuild: there would be nothing to verify against.
        assert!(manager.set("tls-ca-cert-file", "").is_err());
    }

    #[cfg(not(feature = "turmoil"))]
    #[test]
    fn tls_sets_are_rejected_when_no_tls_runtime_is_running() {
        // With TLS off there is no rustls state to change. Accepting the set
        // would make the next CONFIG GET report a value the server is not using,
        // so the set is refused outright.
        let manager = ConfigManager::new(&test_config());
        for name in [
            "tls-cert-file",
            "tls-key-file",
            "tls-ca-cert-file",
            "tls-client-cert-file",
            "tls-client-key-file",
            "tls-ciphersuites",
            "tls-handshake-timeout-ms",
            "tls-cluster-migration",
        ] {
            let err = manager.set(name, "1").unwrap_err();
            assert!(
                format!("{err:?}").contains("TLS is not running"),
                "{name}: unexpected error {err:?}"
            );
        }
    }

    #[test]
    fn rewrite_emits_the_values_set_on_promoted_params() {
        // REWRITE must serialise what the running server is using. A promoted
        // parameter whose GET reads a live seam but whose rewrite path still
        // read the startup snapshot would silently persist the old value.
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("frogdb.toml");
        std::fs::write(
            &config_path,
            "[server]\nbind = \"127.0.0.1\"\nport = 6379\n",
        )
        .unwrap();

        let mut config = test_config();
        config.config_source_path = Some(config_path.clone());
        let manager = ConfigManager::new(&config);

        let coordinator = Arc::new(frogdb_core::persistence::NoopSnapshotCoordinator::new());
        manager.set_snapshot_coordinator(coordinator);

        for (param, value) in [
            ("cluster-auto-failover", "yes"),
            ("replica-priority", "7"),
            ("status-memory-warning-percent", "77"),
            ("tracing-sampling-rate", "0.5"),
            ("latency-bands-enabled", "yes"),
            ("hotshards-hot-threshold-percent", "44"),
            ("hotshards-default-period-secs", "9"),
            ("snapshot-interval-secs", "1234"),
            ("batch-size-threshold-kb", "256"),
            ("replication-lag-threshold-secs", "17"),
            ("self-fence-on-replica-loss", "no"),
            ("replica-freshness-timeout-ms", "8000"),
        ] {
            manager
                .set(param, value)
                .unwrap_or_else(|e| panic!("{param}: {e:?}"));
        }
        manager.rewrite_config().unwrap();

        let contents = std::fs::read_to_string(&config_path).unwrap();
        for needle in [
            "auto-failover = true",
            "replica-priority = 7",
            "memory-warning-percent = 77",
            "sampling-rate = 0.5",
            "hot-threshold-percent = 44",
            "default-period-secs = 9",
            "snapshot-interval-secs = 1234",
            "batch-size-threshold-kb = 256",
            "replication-lag-threshold-secs = 17",
            "self-fence-on-replica-loss = false",
            "replica-freshness-timeout-ms = 8000",
        ] {
            assert!(
                contents.contains(needle),
                "missing `{needle}` after rewrite; file:\n{contents}"
            );
        }
    }

    #[test]
    fn tls_watch_params_stay_immutable() {
        // The cert watcher is spawned once at startup from these two values.
        let manager = ConfigManager::new(&test_config());
        for (name, want) in [("tls-watch-certs", "yes"), ("tls-watch-debounce-ms", "500")] {
            let got = manager.get(name);
            assert_eq!(got.len(), 1);
            assert_eq!(got[0].1, want);
            assert!(matches!(
                manager.set(name, "1"),
                Err(ConfigError::ImmutableParameter(_))
            ));
        }
    }
}
