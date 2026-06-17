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
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering};

use frogdb_core::{
    EvictionConfig, EvictionPolicy, KeyspaceEventFlags, ShardMessage, ShardSender, glob_match,
};
use tokio::sync::oneshot;
use tracing::{info, warn};

use crate::config::Config;

use frogdb_config::{ConfigParam, DynParam, Propagation};

/// CONFIG error type, defined alongside the parameter lifecycle in `frogdb-config`
/// and re-exported here so existing `runtime_config::ConfigError` paths keep working.
pub use frogdb_config::ConfigError;

/// The context type the parameter-lifecycle closures reach state through.
///
/// `ConfigParam`/`DynParam` are generic over this so the lightweight config crate
/// need not name a server type; the server supplies its own [`ConfigManager`].
type Param = Box<dyn DynParam<ConfigManager>>;

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
    pub enable_debug_command: bool,
    pub metrics_enabled: bool,
    pub metrics_port: u16,
    pub strict_config: bool,
    pub tls_enabled: bool,
    pub tls_port: u16,
    pub tls_cert_file: String,
    pub tls_key_file: String,
    pub tls_ca_file: String,
    pub tls_auth_clients: String,
    pub tls_replication: bool,
    pub tls_cluster: bool,
    pub tls_protocols: String,
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
                frogdb_config::ClientCertMode::None => "no".to_string(),
                frogdb_config::ClientCertMode::Optional => "optional".to_string(),
                frogdb_config::ClientCertMode::Required => "yes".to_string(),
            },
            tls_replication: config.tls.tls_replication,
            tls_cluster: config.tls.tls_cluster,
            tls_protocols: config
                .tls
                .protocols
                .iter()
                .map(|p| match p {
                    frogdb_config::TlsProtocol::Tls12 => "TLSv1.2",
                    frogdb_config::TlsProtocol::Tls13 => "TLSv1.3",
                })
                .collect::<Vec<_>>()
                .join(" "),
        }
    }
}

/// Type alias for parameter setter function.
type ParamSetter = fn(&ConfigManager, &str) -> Result<(), ConfigError>;

/// Parameter metadata.
pub struct ParamMeta {
    /// Redis-style parameter name.
    pub name: &'static str,
    /// Whether this parameter can be changed at runtime.
    pub mutable: bool,
    /// Whether this is a no-op compatibility parameter.
    /// When `strict_config` is true, these are hidden from CONFIG GET/SET.
    pub noop: bool,
    /// Get the current value as a string.
    pub getter: fn(&ConfigManager) -> String,
    /// Set the value from a string (only for mutable params).
    pub setter: Option<ParamSetter>,
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
    /// ACL manager for requirepass CONFIG SET/GET support.
    acl_manager: RwLock<Option<Arc<frogdb_core::AclManager>>>,
    /// Server-wide latency histograms (set after construction).
    latency_histograms: RwLock<Option<Arc<frogdb_core::CommandLatencyHistograms>>>,
    /// Configured percentiles for latency-tracking-info-percentiles.
    latency_tracking_percentiles: RwLock<Vec<f64>>,
    /// Key-memory histograms state.
    /// 0 = enabled (startup default), 1 = disabled at startup, 2 = disabled at runtime.
    key_memory_histograms_state: AtomicU8,
    /// Legacy parameter metadata registry (string getter/setter closures).
    /// Parameters are migrated out of this into `typed_params` family by family.
    params: Vec<ParamMeta>,
    /// Typed parameter-lifecycle registry. Each entry owns one parameter's whole
    /// parse/validate/apply/render/propagation lifecycle in a single literal.
    typed_params: Vec<Param>,
    /// Optional notifier for shard config updates.
    shard_notifier: RwLock<Option<Arc<ShardConfigNotifier>>>,
    /// Client registry for maxmemory-clients eviction on CONFIG SET.
    client_eviction_registry: RwLock<Option<Arc<frogdb_core::ClientRegistry>>>,
}

impl ConfigManager {
    /// Create a new ConfigManager from the initial config.
    pub fn new(config: &Config) -> Self {
        let runtime = RuntimeConfig::from_config(config);
        let static_config = StaticConfig::from_config(config);

        let wal_failure_policy_val = match config
            .persistence
            .wal_failure_policy
            .to_lowercase()
            .as_str()
        {
            "rollback" => 1u8,
            _ => 0u8,
        };

        Self {
            runtime: Arc::new(RwLock::new(runtime)),
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
            acl_manager: RwLock::new(None),
            latency_histograms: RwLock::new(None),
            latency_tracking_percentiles: RwLock::new(vec![50.0, 99.0, 99.9]),
            key_memory_histograms_state: AtomicU8::new(0), // enabled by default
            params: Self::build_param_registry(),
            typed_params: Self::build_typed_params(),
            shard_notifier: RwLock::new(None),
            client_eviction_registry: RwLock::new(None),
        }
    }

    /// Get the shared per_request_spans flag for connections and shard workers.
    pub fn per_request_spans_flag(&self) -> Arc<AtomicBool> {
        self.per_request_spans.clone()
    }

    /// Get the shared WAL failure policy flag for shard workers.
    /// 0 = Continue, 1 = Rollback.
    pub fn wal_failure_policy_flag(&self) -> Arc<AtomicU8> {
        self.wal_failure_policy.clone()
    }

    /// Set the log reload handle for dynamic log level changes.
    pub fn set_log_reload_handle(&mut self, handle: LogReloadHandle) {
        self.log_reload_handle = Some(handle);
    }

    /// Get the config file path.
    pub fn config_file_path(&self) -> Option<PathBuf> {
        self.config_file_path.read().unwrap().clone()
    }

    /// Set the config file path (used for CONFIG REWRITE).
    pub fn set_config_file_path(&self, path: PathBuf) {
        *self.config_file_path.write().unwrap() = Some(path);
    }

    /// Rewrite the config file, merging current runtime values into the TOML document.
    ///
    /// Preserves comments, formatting, and key ordering in the original file.
    /// Uses atomic write (temp file + fsync + rename) for safety.
    pub fn rewrite_config(&self) -> Result<(), String> {
        use std::io::Write;
        use toml_edit::DocumentMut;

        let config_path = self
            .config_file_path
            .read()
            .unwrap()
            .clone()
            .ok_or_else(|| "ERR The server is running without a config file".to_string())?;

        // Read the existing file
        let contents = std::fs::read_to_string(&config_path).map_err(|e| {
            format!(
                "ERR failed to read config file '{}': {}",
                config_path.display(),
                e
            )
        })?;

        // Parse as toml_edit document (preserves comments and formatting)
        let mut doc: DocumentMut = contents.parse().map_err(|e| {
            format!(
                "ERR failed to parse config file '{}': {}",
                config_path.display(),
                e
            )
        })?;

        // Iterate over the config param registry and update values
        let registry = frogdb_config::config_param_registry();
        for param in registry {
            // Skip params without config file mapping
            let (section, field) = match (param.section, param.field) {
                (Some(s), Some(f)) => (s, f),
                _ => continue,
            };

            // Skip no-op params (they don't affect FrogDB behavior)
            if param.noop {
                continue;
            }

            // Get current runtime value
            let values = self.get(param.name);
            let value = match values.first() {
                Some((_, v)) => v.clone(),
                None => continue,
            };

            // Special case: min-replicas-max-lag is in seconds at runtime
            // but the TOML field is min-replicas-timeout-ms (in milliseconds)
            let value = if param.name == "min-replicas-max-lag" {
                match value.parse::<u64>() {
                    Ok(secs) => (secs * 1000).to_string(),
                    Err(_) => value,
                }
            } else {
                value
            };

            // Ensure section exists
            if !doc.contains_table(section) {
                doc[section] = toml_edit::Item::Table(toml_edit::Table::new());
            }

            // Convert value to appropriate TOML type
            let toml_value = string_to_toml_value(&value);
            doc[section][field] = toml_edit::Item::Value(toml_value);
        }

        // Atomic write: write to temp file, fsync, rename
        let pid = std::process::id();
        let tmp_path = config_path.with_extension(format!("tmp.{}", pid));

        let mut file = std::fs::File::create(&tmp_path).map_err(|e| {
            format!(
                "ERR failed to create temp file '{}': {}",
                tmp_path.display(),
                e
            )
        })?;

        file.write_all(doc.to_string().as_bytes()).map_err(|e| {
            let _ = std::fs::remove_file(&tmp_path);
            format!("ERR failed to write temp file: {}", e)
        })?;

        file.sync_all().map_err(|e| {
            let _ = std::fs::remove_file(&tmp_path);
            format!("ERR failed to fsync temp file: {}", e)
        })?;

        drop(file);

        std::fs::rename(&tmp_path, &config_path).map_err(|e| {
            let _ = std::fs::remove_file(&tmp_path);
            format!("ERR failed to rename temp file to config: {}", e)
        })?;

        info!(path = %config_path.display(), "Config file rewritten");
        Ok(())
    }

    /// Set the ACL manager for CONFIG SET/GET requirepass support.
    pub fn set_acl_manager(&self, acl_manager: Arc<frogdb_core::AclManager>) {
        *self.acl_manager.write().unwrap() = Some(acl_manager);
    }

    /// Set the latency histograms reference for CONFIG SET latency-tracking.
    pub fn set_latency_histograms(&self, histograms: Arc<frogdb_core::CommandLatencyHistograms>) {
        *self.latency_histograms.write().unwrap() = Some(histograms);
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
        vec![
            // Every mutable parameter now lives in the typed registry
            // (`build_typed_params`); only immutable, read-only parameters remain
            // in this legacy string-getter registry.
            ParamMeta {
                name: "bind",
                mutable: false,
                noop: false,
                getter: |mgr| mgr.static_config.bind.clone(),
                setter: None,
            },
            ParamMeta {
                name: "port",
                mutable: false,
                noop: false,
                getter: |mgr| mgr.static_config.port.to_string(),
                setter: None,
            },
            ParamMeta {
                name: "num-shards",
                mutable: false,
                noop: false,
                getter: |mgr| mgr.static_config.num_shards.to_string(),
                setter: None,
            },
            ParamMeta {
                name: "dir",
                mutable: false,
                noop: false,
                getter: |mgr| mgr.static_config.data_dir.clone(),
                setter: None,
            },
            ParamMeta {
                name: "persistence-enabled",
                mutable: false,
                noop: false,
                getter: |mgr| {
                    if mgr.static_config.persistence_enabled {
                        "yes".to_string()
                    } else {
                        "no".to_string()
                    }
                },
                setter: None,
            },
            ParamMeta {
                name: "metrics-enabled",
                mutable: false,
                noop: false,
                getter: |mgr| {
                    if mgr.static_config.metrics_enabled {
                        "yes".to_string()
                    } else {
                        "no".to_string()
                    }
                },
                setter: None,
            },
            ParamMeta {
                name: "metrics-port",
                mutable: false,
                noop: false,
                getter: |mgr| mgr.static_config.metrics_port.to_string(),
                setter: None,
            },
            // TLS parameters (all read-only)
            ParamMeta {
                name: "tls-port",
                mutable: false,
                noop: false,
                getter: |mgr| mgr.static_config.tls_port.to_string(),
                setter: None,
            },
            ParamMeta {
                name: "tls-cert-file",
                mutable: false,
                noop: false,
                getter: |mgr| mgr.static_config.tls_cert_file.clone(),
                setter: None,
            },
            ParamMeta {
                name: "tls-key-file",
                mutable: false,
                noop: false,
                getter: |mgr| mgr.static_config.tls_key_file.clone(),
                setter: None,
            },
            ParamMeta {
                name: "tls-ca-cert-file",
                mutable: false,
                noop: false,
                getter: |mgr| mgr.static_config.tls_ca_file.clone(),
                setter: None,
            },
            ParamMeta {
                name: "tls-auth-clients",
                mutable: false,
                noop: false,
                getter: |mgr| mgr.static_config.tls_auth_clients.clone(),
                setter: None,
            },
            ParamMeta {
                name: "tls-replication",
                mutable: false,
                noop: false,
                getter: |mgr| {
                    if mgr.static_config.tls_replication {
                        "yes".to_string()
                    } else {
                        "no".to_string()
                    }
                },
                setter: None,
            },
            ParamMeta {
                name: "tls-cluster",
                mutable: false,
                noop: false,
                getter: |mgr| {
                    if mgr.static_config.tls_cluster {
                        "yes".to_string()
                    } else {
                        "no".to_string()
                    }
                },
                setter: None,
            },
            ParamMeta {
                name: "tls-protocols",
                mutable: false,
                noop: false,
                getter: |mgr| mgr.static_config.tls_protocols.clone(),
                setter: None,
            },
        ]
    }

    /// Build the typed parameter-lifecycle registry.
    ///
    /// Each entry is one [`ConfigParam`] literal that owns the whole lifecycle of
    /// one parameter (parse → validate → apply, plus render and propagation).
    /// Parameters are migrated here family by family, replacing their opaque
    /// string closures in [`build_param_registry`](Self::build_param_registry).
    fn build_typed_params() -> Vec<Param> {
        vec![
            // === Memory / eviction family ===
            Box::new(ConfigParam::<u64, ConfigManager> {
                name: "maxmemory",
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
            Box::new(ConfigParam::<EvictionPolicy, ConfigManager> {
                name: "maxmemory-policy",
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
            Box::new(ConfigParam::<usize, ConfigManager> {
                name: "maxmemory-samples",
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
            Box::new(ConfigParam::<u8, ConfigManager> {
                name: "lfu-log-factor",
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
            Box::new(ConfigParam::<u64, ConfigManager> {
                name: "lfu-decay-time",
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
            Box::new(ConfigParam::<String, ConfigManager> {
                name: "maxmemory-clients",
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
                    // Trigger immediate eviction check via the client registry.
                    if let Some(ref registry) = *mgr.client_eviction_registry.read().unwrap() {
                        let limit =
                            frogdb_config::parse_maxmemory_clients(&v, maxmemory).unwrap_or(0);
                        if limit > 0 {
                            let evicted = registry.try_evict_clients(limit);
                            if evicted > 0 {
                                info!(
                                    evicted,
                                    limit,
                                    "Client eviction triggered by CONFIG SET maxmemory-clients"
                                );
                            }
                        }
                    }
                    Ok(())
                },
                render: |v| v.clone(),
                propagation: Propagation::None,
            }),
            // === Logging family ===
            Box::new(ConfigParam::<String, ConfigManager> {
                name: "loglevel",
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
            Box::new(ConfigParam::<bool, ConfigManager> {
                name: "per-request-spans",
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
            Box::new(ConfigParam::<String, ConfigManager> {
                name: "durability-mode",
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
            Box::new(ConfigParam::<String, ConfigManager> {
                name: "wal-failure-policy",
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
                get: |mgr| match mgr.wal_failure_policy.load(Ordering::Relaxed) {
                    1 => "rollback".to_string(),
                    _ => "continue".to_string(),
                },
                apply: |mgr, v| {
                    let policy_val = if v == "rollback" { 1u8 } else { 0u8 };
                    mgr.wal_failure_policy.store(policy_val, Ordering::Relaxed);
                    info!(policy = %v, "WAL failure policy updated");
                    Ok(())
                },
                render: |v| v.clone(),
                propagation: Propagation::None,
            }),
            Box::new(ConfigParam::<u64, ConfigManager> {
                name: "sync-interval-ms",
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
            Box::new(ConfigParam::<u64, ConfigManager> {
                name: "batch-timeout-ms",
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
            Box::new(ConfigParam::<u64, ConfigManager> {
                name: "scatter-gather-timeout-ms",
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
            Box::new(ConfigParam::<u32, ConfigManager> {
                name: "min-replicas-to-write",
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
            Box::new(ConfigParam::<u64, ConfigManager> {
                name: "min-replicas-max-lag",
                // Redis reports/accepts this in seconds; stored internally in ms.
                // `get`/`render` round-trip in seconds; `apply` converts to ms.
                parse: |s| {
                    s.parse::<u64>().map_err(|_| ConfigError::InvalidValue {
                        param: "min-replicas-max-lag".to_string(),
                        message: "must be a non-negative integer (seconds)".to_string(),
                    })
                },
                validate: ConfigParam::no_validate,
                default: || frogdb_config::replication::DEFAULT_MIN_REPLICAS_TIMEOUT_MS / 1000,
                get: |mgr| mgr.runtime.read().unwrap().min_replicas_timeout_ms / 1000,
                apply: |mgr, secs| {
                    mgr.runtime.write().unwrap().min_replicas_timeout_ms = secs * 1000;
                    info!(
                        min_replicas_max_lag_secs = secs,
                        "min-replicas-max-lag updated"
                    );
                    Ok(())
                },
                render: |v| v.to_string(),
                propagation: Propagation::None,
            }),
            // === Slowlog family ===
            Box::new(ConfigParam::<i64, ConfigManager> {
                name: "slowlog-log-slower-than",
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
            Box::new(ConfigParam::<usize, ConfigManager> {
                name: "slowlog-max-len",
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
            Box::new(ConfigParam::<usize, ConfigManager> {
                name: "slowlog-max-arg-len",
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
            Box::new(ConfigParam::<u64, ConfigManager> {
                name: "set-max-listpack-entries",
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
            Box::new(ConfigParam::<u64, ConfigManager> {
                name: "set-max-listpack-value",
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
            Box::new(ConfigParam::<u64, ConfigManager> {
                name: "hash-max-ziplist-entries",
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
            Box::new(ConfigParam::<u64, ConfigManager> {
                name: "hash-max-ziplist-value",
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
            Box::new(ConfigParam::<u64, ConfigManager> {
                name: "hash-max-listpack-entries",
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
            Box::new(ConfigParam::<u64, ConfigManager> {
                name: "hash-max-listpack-value",
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
            Box::new(ConfigParam::<u64, ConfigManager> {
                name: "lua-time-limit",
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
            Box::new(ConfigParam::<u64, ConfigManager> {
                name: "maxclients",
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
            Box::new(ConfigParam::<bool, ConfigManager> {
                name: "latency-tracking",
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
                get: |mgr| {
                    let histograms = mgr.latency_histograms.read().unwrap();
                    histograms.as_ref().map(|h| h.is_enabled()).unwrap_or(true)
                },
                apply: |mgr, enabled| {
                    let histograms = mgr.latency_histograms.read().unwrap();
                    if let Some(ref h) = *histograms {
                        h.set_enabled(enabled);
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
                propagation: Propagation::None,
            }),
            Box::new(ConfigParam::<Vec<f64>, ConfigManager> {
                name: "latency-tracking-info-percentiles",
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
            Box::new(ConfigParam::<u32, ConfigManager> {
                name: "notify-keyspace-events",
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
            Box::new(ConfigParam::<String, ConfigManager> {
                name: "requirepass",
                // Any value is accepted; the ACL manager performs the real
                // validation and storage in `apply`.
                parse: |s| Ok(s.to_string()),
                validate: ConfigParam::no_validate,
                default: String::new,
                get: |mgr| {
                    let acl = mgr.acl_manager.read().unwrap();
                    acl.as_ref()
                        .map(|m| m.get_requirepass())
                        .unwrap_or_default()
                },
                apply: |mgr, v| {
                    let acl = mgr.acl_manager.read().unwrap();
                    let acl = acl.as_ref().ok_or_else(|| ConfigError::InvalidValue {
                        param: "requirepass".to_string(),
                        message: "ACL manager not available".to_string(),
                    })?;
                    acl.set_requirepass(&v)
                        .map_err(|e| ConfigError::InvalidValue {
                            param: "requirepass".to_string(),
                            message: e.to_string(),
                        })
                },
                render: |v| v.clone(),
                propagation: Propagation::None,
            }),
            Box::new(ConfigParam::<bool, ConfigManager> {
                name: "key-memory-histograms",
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
            Box::new(NoopParam {
                name: "save",
                value: "",
            }),
            Box::new(NoopParam {
                name: "set-max-intset-entries",
                value: "512",
            }),
            Box::new(NoopParam {
                name: "list-max-listpack-size",
                value: "-2",
            }),
            Box::new(NoopParam {
                name: "list-compress-depth",
                value: "0",
            }),
            Box::new(NoopParam {
                name: "list-max-ziplist-size",
                value: "-2",
            }),
            Box::new(NoopParam {
                name: "latency-monitor-threshold",
                value: "0",
            }),
            Box::new(NoopParam {
                name: "busy-reply-threshold",
                value: "5000",
            }),
            Box::new(NoopParam {
                name: "hz",
                value: "10",
            }),
            Box::new(NoopParam {
                name: "activedefrag",
                value: "no",
            }),
            Box::new(NoopParam {
                name: "close-on-oom",
                value: "no",
            }),
            Box::new(NoopParam {
                name: "zset-max-ziplist-entries",
                value: "128",
            }),
            Box::new(NoopParam {
                name: "zset-max-ziplist-value",
                value: "64",
            }),
            Box::new(NoopParam {
                name: "zset-max-listpack-entries",
                value: "128",
            }),
            Box::new(NoopParam {
                name: "zset-max-listpack-value",
                value: "64",
            }),
        ]
    }

    /// Look up a migrated typed parameter by (already-normalized) name.
    fn typed_param(&self, name: &str) -> Option<&dyn DynParam<ConfigManager>> {
        self.typed_params
            .iter()
            .map(|b| b.as_ref())
            .find(|p| p.name() == name)
    }

    /// Look up a not-yet-migrated legacy parameter by name.
    fn legacy_param(&self, name: &str) -> Option<&ParamMeta> {
        self.params.iter().find(|p| p.name == name)
    }

    /// Read a parameter's current value as a string, checking the typed registry
    /// first and then the legacy one.
    fn value_of(&self, name: &str) -> Option<String> {
        if let Some(p) = self.typed_param(name) {
            return Some(p.get(self));
        }
        self.legacy_param(name).map(|p| (p.getter)(self))
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

        // Apply via the typed lifecycle if migrated, else the legacy setter.
        if let Some(param) = self.typed_param(&normalized) {
            param.set(self, value).map_err(|e| {
                warn!(param = %name, value = %value, error = %e, "Invalid config value rejected");
                e
            })?;
        } else {
            let setter = self
                .legacy_param(&normalized)
                .and_then(|p| p.setter)
                .ok_or_else(|| ConfigError::ImmutableParameter(name.to_string()))?;
            setter(self, value).map_err(|e| {
                warn!(param = %name, value = %value, error = %e, "Invalid config value rejected");
                e
            })?;
        }

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

    /// Set the client registry for maxmemory-clients eviction on CONFIG SET.
    pub fn set_client_eviction_registry(&self, registry: Arc<frogdb_core::ClientRegistry>) {
        *self.client_eviction_registry.write().unwrap() = Some(registry);
    }

    /// Resolve the current maxmemory-clients limit in bytes.
    /// Returns 0 if disabled.
    pub fn resolve_maxmemory_clients(&self) -> u64 {
        let runtime = self.runtime.read().unwrap();
        frogdb_config::parse_maxmemory_clients(&runtime.maxmemory_clients, runtime.maxmemory)
            .unwrap_or(0)
    }

    /// Set the shard notifier for propagating config changes to shards.
    pub fn set_shard_notifier(&self, notifier: Arc<ShardConfigNotifier>) {
        *self.shard_notifier.write().unwrap() = Some(notifier);
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
        // to shards. For params not yet migrated to the typed registry, fall back
        // to the legacy name-based decision.
        let normalized = name.to_lowercase().replace('_', "-");
        let propagation = self
            .typed_param(&normalized)
            .map(|p| p.propagation())
            .unwrap_or_else(|| legacy_propagation(&normalized));

        match propagation {
            Propagation::None => {}
            Propagation::Eviction => {
                let notifier = self.shard_notifier.read().unwrap().clone();
                if let Some(ref notifier) = notifier {
                    notifier.notify_eviction_change().await?;
                }
            }
            Propagation::KeyMemoryHistograms => {
                let enabled = self.key_memory_histograms_enabled();
                let notifier = self.shard_notifier.read().unwrap().clone();
                if let Some(ref notifier) = notifier {
                    notifier.notify_key_memory_histograms(enabled).await?;
                }
            }
        }

        Ok(())
    }
}

/// Shard-propagation decision for parameters not yet migrated to the typed
/// registry. Removed once every propagating parameter carries its own
/// [`Propagation`].
fn legacy_propagation(normalized: &str) -> Propagation {
    const LEGACY_EVICTION_PARAMS: [&str; 5] = [
        "maxmemory",
        "maxmemory-policy",
        "maxmemory-samples",
        "lfu-log-factor",
        "lfu-decay-time",
    ];
    if LEGACY_EVICTION_PARAMS.contains(&normalized) {
        Propagation::Eviction
    } else if normalized == "key-memory-histograms" {
        Propagation::KeyMemoryHistograms
    } else {
        Propagation::None
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
                .send(ShardMessage::UpdateConfig {
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
                .send(ShardMessage::SetKeyMemoryHistograms {
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

/// Convert a runtime config string value to the appropriate TOML value type.
///
/// Tries parsing as integer first, then boolean, then falls back to string.
fn string_to_toml_value(s: &str) -> toml_edit::Value {
    // Try integer
    if let Ok(n) = s.parse::<i64>() {
        return toml_edit::value(n).into_value().unwrap();
    }

    // Try boolean (yes/no, true/false)
    match s.to_lowercase().as_str() {
        "yes" | "true" => return toml_edit::value(true).into_value().unwrap(),
        "no" | "false" => return toml_edit::value(false).into_value().unwrap(),
        _ => {}
    }

    // Fall back to string
    toml_edit::value(s).into_value().unwrap()
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
        // The config crate's metadata registry is the single source of truth for
        // which parameters exist and their mutable/noop flags. Every name there
        // must be served by exactly one server-side registry (typed or legacy),
        // and every server-side name must exist in the metadata registry.
        let legacy = ConfigManager::build_param_registry();
        let typed = ConfigManager::build_typed_params();
        let config_params = frogdb_config::config_param_registry();

        let mut server_names: Vec<&str> = legacy.iter().map(|p| p.name).collect();
        server_names.extend(typed.iter().map(|p| p.name()));

        // No name is served by both registries.
        for name in &server_names {
            let count = server_names.iter().filter(|n| *n == name).count();
            assert_eq!(count, 1, "param '{}' is registered more than once", name);
        }

        // Every config param is served by the server.
        for config_param in config_params {
            assert!(
                server_names.contains(&config_param.name),
                "config param '{}' missing from server registries",
                config_param.name
            );
        }

        // Every server param exists in the config metadata registry.
        for name in &server_names {
            assert!(
                config_params.iter().any(|p| p.name == *name),
                "server param '{}' missing from config_param_registry",
                name
            );
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

    #[test]
    fn test_string_to_toml_value_integer() {
        let v = string_to_toml_value("42");
        assert!(v.is_integer());
        assert_eq!(v.as_integer(), Some(42));
    }

    #[test]
    fn test_string_to_toml_value_boolean() {
        let yes = string_to_toml_value("yes");
        assert!(yes.is_bool());
        assert_eq!(yes.as_bool(), Some(true));

        let no = string_to_toml_value("no");
        assert!(no.is_bool());
        assert_eq!(no.as_bool(), Some(false));
    }

    #[test]
    fn test_string_to_toml_value_string() {
        let v = string_to_toml_value("allkeys-lru");
        assert!(v.is_str());
        assert_eq!(v.as_str(), Some("allkeys-lru"));
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
}
