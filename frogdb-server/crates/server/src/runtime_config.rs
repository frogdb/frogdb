//! Runtime configuration for CONFIG GET/SET commands.
//!
//! This module provides:
//! - `RuntimeConfig` - mutable parameters that can be changed at runtime
//! - `ConfigManager` - main interface for CONFIG commands
//! - `ShardConfigNotifier` - propagates config changes to shards
//! - Parameter registry with metadata for each configurable parameter

use std::fmt;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

use frogdb_core::{EvictionConfig, EvictionPolicy, ShardMessage, ShardSender, glob_match};
use tokio::sync::oneshot;
use tracing::{info, warn};

use crate::config::Config;

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

/// Error type for CONFIG operations.
#[derive(Debug, Clone)]
pub enum ConfigError {
    /// Parameter is not mutable at runtime.
    ImmutableParameter(String),
    /// Parameter does not exist.
    UnknownParameter(String),
    /// Invalid value for the parameter.
    InvalidValue { param: String, message: String },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::ImmutableParameter(name) => {
                write!(f, "ERR CONFIG parameter '{}' is not mutable", name)
            }
            ConfigError::UnknownParameter(name) => {
                write!(f, "ERR Unknown CONFIG parameter '{}'", name)
            }
            ConfigError::InvalidValue { param, message } => {
                write!(f, "ERR Invalid value for '{}': {}", param, message)
            }
        }
    }
}

impl std::error::Error for ConfigError {}

/// Mutable runtime configuration values.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    // Memory settings
    pub maxmemory: u64,
    pub maxmemory_policy: String,
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

    // Slowlog settings
    pub slowlog_log_slower_than: i64,
    pub slowlog_max_len: usize,
    pub slowlog_max_arg_len: usize,
}

impl RuntimeConfig {
    /// Create from the initial config.
    pub fn from_config(config: &Config) -> Self {
        Self {
            maxmemory: config.memory.maxmemory,
            maxmemory_policy: config.memory.maxmemory_policy.clone(),
            maxmemory_samples: config.memory.maxmemory_samples,
            lfu_log_factor: config.memory.lfu_log_factor,
            lfu_decay_time: config.memory.lfu_decay_time,
            loglevel: config.logging.level.clone(),
            durability_mode: config.persistence.durability_mode.clone(),
            sync_interval_ms: config.persistence.sync_interval_ms,
            batch_timeout_ms: config.persistence.batch_timeout_ms,
            scatter_gather_timeout_ms: config.server.scatter_gather_timeout_ms,
            slowlog_log_slower_than: config.slowlog.log_slower_than,
            slowlog_max_len: config.slowlog.max_len,
            slowlog_max_arg_len: config.slowlog.max_arg_len,
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
    pub metrics_enabled: bool,
    pub metrics_port: u16,
    pub strict_config: bool,
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
            metrics_enabled: config.metrics.enabled,
            metrics_port: config.metrics.port,
            strict_config: config.compat.strict_config,
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
    /// Parameter metadata registry.
    params: Vec<ParamMeta>,
    /// Optional notifier for shard config updates.
    shard_notifier: RwLock<Option<Arc<ShardConfigNotifier>>>,
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
            params: Self::build_param_registry(),
            shard_notifier: RwLock::new(None),
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
            // Mutable parameters
            ParamMeta {
                name: "maxmemory",
                mutable: true,
                noop: false,
                getter: |mgr| mgr.runtime.read().unwrap().maxmemory.to_string(),
                setter: Some(|mgr, val| {
                    let parsed: u64 = val.parse().map_err(|_| ConfigError::InvalidValue {
                        param: "maxmemory".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })?;
                    mgr.runtime.write().unwrap().maxmemory = parsed;
                    Ok(())
                }),
            },
            ParamMeta {
                name: "maxmemory-policy",
                mutable: true,
                noop: false,
                getter: |mgr| mgr.runtime.read().unwrap().maxmemory_policy.clone(),
                setter: Some(|mgr, val| {
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
                    let lower = val.to_lowercase();
                    if !valid_policies.contains(&lower.as_str()) {
                        return Err(ConfigError::InvalidValue {
                            param: "maxmemory-policy".to_string(),
                            message: format!("must be one of: {}", valid_policies.join(", ")),
                        });
                    }
                    mgr.runtime.write().unwrap().maxmemory_policy = lower;
                    Ok(())
                }),
            },
            ParamMeta {
                name: "maxmemory-samples",
                mutable: true,
                noop: false,
                getter: |mgr| mgr.runtime.read().unwrap().maxmemory_samples.to_string(),
                setter: Some(|mgr, val| {
                    let parsed: usize = val.parse().map_err(|_| ConfigError::InvalidValue {
                        param: "maxmemory-samples".to_string(),
                        message: "must be a positive integer".to_string(),
                    })?;
                    if parsed == 0 {
                        return Err(ConfigError::InvalidValue {
                            param: "maxmemory-samples".to_string(),
                            message: "must be > 0".to_string(),
                        });
                    }
                    mgr.runtime.write().unwrap().maxmemory_samples = parsed;
                    Ok(())
                }),
            },
            ParamMeta {
                name: "lfu-log-factor",
                mutable: true,
                noop: false,
                getter: |mgr| mgr.runtime.read().unwrap().lfu_log_factor.to_string(),
                setter: Some(|mgr, val| {
                    let parsed: u8 = val.parse().map_err(|_| ConfigError::InvalidValue {
                        param: "lfu-log-factor".to_string(),
                        message: "must be an integer 0-255".to_string(),
                    })?;
                    mgr.runtime.write().unwrap().lfu_log_factor = parsed;
                    Ok(())
                }),
            },
            ParamMeta {
                name: "lfu-decay-time",
                mutable: true,
                noop: false,
                getter: |mgr| mgr.runtime.read().unwrap().lfu_decay_time.to_string(),
                setter: Some(|mgr, val| {
                    let parsed: u64 = val.parse().map_err(|_| ConfigError::InvalidValue {
                        param: "lfu-decay-time".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })?;
                    mgr.runtime.write().unwrap().lfu_decay_time = parsed;
                    Ok(())
                }),
            },
            ParamMeta {
                name: "loglevel",
                mutable: true,
                noop: false,
                getter: |mgr| mgr.runtime.read().unwrap().loglevel.clone(),
                setter: Some(|mgr, val| {
                    let valid_levels = ["trace", "debug", "info", "warn", "error"];
                    let lower = val.to_lowercase();
                    if !valid_levels.contains(&lower.as_str()) {
                        return Err(ConfigError::InvalidValue {
                            param: "loglevel".to_string(),
                            message: format!("must be one of: {}", valid_levels.join(", ")),
                        });
                    }
                    mgr.runtime.write().unwrap().loglevel = lower.clone();

                    // Apply log level change if handle is available
                    if let Some(ref handle) = mgr.log_reload_handle
                        && let Err(e) = handle.reload_level(&lower)
                    {
                        warn!(error = %e, "Failed to reload log level");
                    }

                    Ok(())
                }),
            },
            ParamMeta {
                name: "durability-mode",
                mutable: true,
                noop: false,
                getter: |mgr| mgr.runtime.read().unwrap().durability_mode.clone(),
                setter: Some(|mgr, val| {
                    let valid_modes = ["async", "periodic", "sync"];
                    let lower = val.to_lowercase();
                    if !valid_modes.contains(&lower.as_str()) {
                        return Err(ConfigError::InvalidValue {
                            param: "durability-mode".to_string(),
                            message: format!("must be one of: {}", valid_modes.join(", ")),
                        });
                    }
                    mgr.runtime.write().unwrap().durability_mode = lower;
                    Ok(())
                }),
            },
            ParamMeta {
                name: "wal-failure-policy",
                mutable: true,
                noop: false,
                getter: |mgr| match mgr.wal_failure_policy.load(Ordering::Relaxed) {
                    1 => "rollback".to_string(),
                    _ => "continue".to_string(),
                },
                setter: Some(|mgr, val| {
                    let valid = ["continue", "rollback"];
                    let lower = val.to_lowercase();
                    if !valid.contains(&lower.as_str()) {
                        return Err(ConfigError::InvalidValue {
                            param: "wal-failure-policy".to_string(),
                            message: format!("must be one of: {}", valid.join(", ")),
                        });
                    }
                    let policy_val = if lower == "rollback" { 1u8 } else { 0u8 };
                    mgr.wal_failure_policy.store(policy_val, Ordering::Relaxed);
                    info!(policy = %lower, "WAL failure policy updated");
                    Ok(())
                }),
            },
            ParamMeta {
                name: "sync-interval-ms",
                mutable: true,
                noop: false,
                getter: |mgr| mgr.runtime.read().unwrap().sync_interval_ms.to_string(),
                setter: Some(|mgr, val| {
                    let parsed: u64 = val.parse().map_err(|_| ConfigError::InvalidValue {
                        param: "sync-interval-ms".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })?;
                    mgr.runtime.write().unwrap().sync_interval_ms = parsed;
                    Ok(())
                }),
            },
            ParamMeta {
                name: "batch-timeout-ms",
                mutable: true,
                noop: false,
                getter: |mgr| mgr.runtime.read().unwrap().batch_timeout_ms.to_string(),
                setter: Some(|mgr, val| {
                    let parsed: u64 = val.parse().map_err(|_| ConfigError::InvalidValue {
                        param: "batch-timeout-ms".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })?;
                    mgr.runtime.write().unwrap().batch_timeout_ms = parsed;
                    Ok(())
                }),
            },
            ParamMeta {
                name: "scatter-gather-timeout-ms",
                mutable: true,
                noop: false,
                getter: |mgr| {
                    mgr.runtime
                        .read()
                        .unwrap()
                        .scatter_gather_timeout_ms
                        .to_string()
                },
                setter: Some(|mgr, val| {
                    let parsed: u64 = val.parse().map_err(|_| ConfigError::InvalidValue {
                        param: "scatter-gather-timeout-ms".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })?;
                    mgr.runtime.write().unwrap().scatter_gather_timeout_ms = parsed;
                    Ok(())
                }),
            },
            // Slowlog parameters
            ParamMeta {
                name: "slowlog-log-slower-than",
                mutable: true,
                noop: false,
                getter: |mgr| {
                    mgr.runtime
                        .read()
                        .unwrap()
                        .slowlog_log_slower_than
                        .to_string()
                },
                setter: Some(|mgr, val| {
                    let parsed: i64 = val.parse().map_err(|_| ConfigError::InvalidValue {
                        param: "slowlog-log-slower-than".to_string(),
                        message: "must be an integer".to_string(),
                    })?;
                    mgr.runtime.write().unwrap().slowlog_log_slower_than = parsed;
                    Ok(())
                }),
            },
            ParamMeta {
                name: "slowlog-max-len",
                mutable: true,
                noop: false,
                getter: |mgr| mgr.runtime.read().unwrap().slowlog_max_len.to_string(),
                setter: Some(|mgr, val| {
                    let parsed: usize = val.parse().map_err(|_| ConfigError::InvalidValue {
                        param: "slowlog-max-len".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })?;
                    mgr.runtime.write().unwrap().slowlog_max_len = parsed;
                    Ok(())
                }),
            },
            ParamMeta {
                name: "slowlog-max-arg-len",
                mutable: true,
                noop: false,
                getter: |mgr| mgr.runtime.read().unwrap().slowlog_max_arg_len.to_string(),
                setter: Some(|mgr, val| {
                    let parsed: usize = val.parse().map_err(|_| ConfigError::InvalidValue {
                        param: "slowlog-max-arg-len".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })?;
                    mgr.runtime.write().unwrap().slowlog_max_arg_len = parsed;
                    Ok(())
                }),
            },
            ParamMeta {
                name: "per-request-spans",
                mutable: true,
                noop: false,
                getter: |mgr| {
                    if mgr.per_request_spans.load(Ordering::Relaxed) {
                        "yes".to_string()
                    } else {
                        "no".to_string()
                    }
                },
                setter: Some(|mgr, val| {
                    let lower = val.to_lowercase();
                    let enabled = match lower.as_str() {
                        "yes" | "true" | "1" | "on" => true,
                        "no" | "false" | "0" | "off" => false,
                        _ => {
                            return Err(ConfigError::InvalidValue {
                                param: "per-request-spans".to_string(),
                                message: "must be yes/no".to_string(),
                            });
                        }
                    };
                    mgr.per_request_spans.store(enabled, Ordering::Relaxed);
                    info!(enabled, "Per-request tracing spans toggled");
                    Ok(())
                }),
            },
            // No-op mutable parameters (accept any value, return Redis defaults)
            // These exist so that Redis test suites can CONFIG SET encoding thresholds
            // without aborting.  FrogDB does not use these internally.
            // When compat.strict_config = true, these are treated as unknown.
            ParamMeta {
                name: "save",
                mutable: true,
                noop: true,
                getter: |_| "".to_string(),
                setter: Some(|_, _| Ok(())),
            },
            ParamMeta {
                name: "set-max-intset-entries",
                mutable: true,
                noop: true,
                getter: |_| "512".to_string(),
                setter: Some(|_, _| Ok(())),
            },
            ParamMeta {
                name: "set-max-listpack-entries",
                mutable: true,
                noop: false,
                getter: |mgr| {
                    mgr.listpack
                        .set_max_entries
                        .load(Ordering::Relaxed)
                        .to_string()
                },
                setter: Some(|mgr, val| {
                    let v: u64 = val.parse().map_err(|_| ConfigError::InvalidValue {
                        param: "set-max-listpack-entries".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })?;
                    mgr.listpack.set_max_entries.store(v, Ordering::Relaxed);
                    Ok(())
                }),
            },
            ParamMeta {
                name: "hash-max-ziplist-entries",
                mutable: true,
                noop: false,
                getter: |mgr| {
                    mgr.listpack
                        .hash_max_entries
                        .load(Ordering::Relaxed)
                        .to_string()
                },
                setter: Some(|mgr, val| {
                    let v: u64 = val.parse().map_err(|_| ConfigError::InvalidValue {
                        param: "hash-max-ziplist-entries".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })?;
                    mgr.listpack.hash_max_entries.store(v, Ordering::Relaxed);
                    Ok(())
                }),
            },
            ParamMeta {
                name: "hash-max-ziplist-value",
                mutable: true,
                noop: false,
                getter: |mgr| {
                    mgr.listpack
                        .hash_max_value
                        .load(Ordering::Relaxed)
                        .to_string()
                },
                setter: Some(|mgr, val| {
                    let v: u64 = val.parse().map_err(|_| ConfigError::InvalidValue {
                        param: "hash-max-ziplist-value".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })?;
                    mgr.listpack.hash_max_value.store(v, Ordering::Relaxed);
                    Ok(())
                }),
            },
            ParamMeta {
                name: "hash-max-listpack-entries",
                mutable: true,
                noop: false,
                getter: |mgr| {
                    mgr.listpack
                        .hash_max_entries
                        .load(Ordering::Relaxed)
                        .to_string()
                },
                setter: Some(|mgr, val| {
                    let v: u64 = val.parse().map_err(|_| ConfigError::InvalidValue {
                        param: "hash-max-listpack-entries".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })?;
                    mgr.listpack.hash_max_entries.store(v, Ordering::Relaxed);
                    Ok(())
                }),
            },
            ParamMeta {
                name: "hash-max-listpack-value",
                mutable: true,
                noop: false,
                getter: |mgr| {
                    mgr.listpack
                        .hash_max_value
                        .load(Ordering::Relaxed)
                        .to_string()
                },
                setter: Some(|mgr, val| {
                    let v: u64 = val.parse().map_err(|_| ConfigError::InvalidValue {
                        param: "hash-max-listpack-value".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })?;
                    mgr.listpack.hash_max_value.store(v, Ordering::Relaxed);
                    Ok(())
                }),
            },
            ParamMeta {
                name: "list-max-listpack-size",
                mutable: true,
                noop: true,
                getter: |_| "-2".to_string(),
                setter: Some(|_, _| Ok(())),
            },
            ParamMeta {
                name: "list-compress-depth",
                mutable: true,
                noop: true,
                getter: |_| "0".to_string(),
                setter: Some(|_, _| Ok(())),
            },
            ParamMeta {
                name: "list-max-ziplist-size",
                mutable: true,
                noop: true,
                getter: |_| "-2".to_string(),
                setter: Some(|_, _| Ok(())),
            },
            ParamMeta {
                name: "latency-monitor-threshold",
                mutable: true,
                noop: true,
                getter: |_| "0".to_string(),
                setter: Some(|_, _| Ok(())),
            },
            ParamMeta {
                name: "lua-time-limit",
                mutable: true,
                noop: false,
                getter: |mgr| mgr.lua_time_limit.load(Ordering::Relaxed).to_string(),
                setter: Some(|mgr, val| {
                    let parsed: u64 = val.parse().map_err(|_| ConfigError::InvalidValue {
                        param: "lua-time-limit".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })?;
                    mgr.lua_time_limit.store(parsed, Ordering::Relaxed);
                    Ok(())
                }),
            },
            ParamMeta {
                name: "maxclients",
                mutable: true,
                noop: false,
                getter: |mgr| mgr.max_clients.load(Ordering::Relaxed).to_string(),
                setter: Some(|mgr, val| {
                    let parsed: u64 = val.parse().map_err(|_| ConfigError::InvalidValue {
                        param: "maxclients".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })?;
                    mgr.max_clients.store(parsed, Ordering::Relaxed);
                    Ok(())
                }),
            },
            ParamMeta {
                name: "busy-reply-threshold",
                mutable: true,
                noop: true,
                getter: |_| "5000".to_string(),
                setter: Some(|_, _| Ok(())),
            },
            ParamMeta {
                name: "hz",
                mutable: true,
                noop: true,
                getter: |_| "10".to_string(),
                setter: Some(|_, _| Ok(())),
            },
            ParamMeta {
                name: "activedefrag",
                mutable: true,
                noop: true,
                getter: |_| "no".to_string(),
                setter: Some(|_, _| Ok(())),
            },
            ParamMeta {
                name: "close-on-oom",
                mutable: true,
                noop: true,
                getter: |_| "no".to_string(),
                setter: Some(|_, _| Ok(())),
            },
            ParamMeta {
                name: "set-max-listpack-value",
                mutable: true,
                noop: false,
                getter: |mgr| {
                    mgr.listpack
                        .set_max_value
                        .load(Ordering::Relaxed)
                        .to_string()
                },
                setter: Some(|mgr, val| {
                    let v: u64 = val.parse().map_err(|_| ConfigError::InvalidValue {
                        param: "set-max-listpack-value".to_string(),
                        message: "must be a non-negative integer".to_string(),
                    })?;
                    mgr.listpack.set_max_value.store(v, Ordering::Relaxed);
                    Ok(())
                }),
            },
            ParamMeta {
                name: "zset-max-ziplist-entries",
                mutable: true,
                noop: true,
                getter: |_| "128".to_string(),
                setter: Some(|_, _| Ok(())),
            },
            ParamMeta {
                name: "zset-max-ziplist-value",
                mutable: true,
                noop: true,
                getter: |_| "64".to_string(),
                setter: Some(|_, _| Ok(())),
            },
            ParamMeta {
                name: "zset-max-listpack-entries",
                mutable: true,
                noop: true,
                getter: |_| "128".to_string(),
                setter: Some(|_, _| Ok(())),
            },
            ParamMeta {
                name: "zset-max-listpack-value",
                mutable: true,
                noop: true,
                getter: |_| "64".to_string(),
                setter: Some(|_, _| Ok(())),
            },
            // Immutable parameters
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
        ]
    }

    /// Get parameters matching a glob pattern.
    ///
    /// Returns a vector of (name, value) pairs.
    /// When `strict_config` is enabled, no-op compatibility params are hidden.
    pub fn get(&self, pattern: &str) -> Vec<(String, String)> {
        let strict = self.static_config.strict_config;
        let pattern_bytes = pattern.as_bytes();
        self.params
            .iter()
            .filter(|param| {
                if strict && param.noop {
                    return false;
                }
                glob_match(pattern_bytes, param.name.as_bytes())
            })
            .map(|param| (param.name.to_string(), (param.getter)(self)))
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

        let param = self
            .params
            .iter()
            .find(|p| p.name == normalized)
            .ok_or_else(|| {
                warn!(param = %name, "Unknown config parameter");
                ConfigError::UnknownParameter(name.to_string())
            })?;

        // When strict_config is enabled, reject no-op compatibility params
        if self.static_config.strict_config && param.noop {
            warn!(param = %name, "No-op config parameter rejected (strict_config=true)");
            return Err(ConfigError::UnknownParameter(name.to_string()));
        }

        if !param.mutable {
            warn!(param = %name, "Attempted to change immutable config");
            return Err(ConfigError::ImmutableParameter(name.to_string()));
        }

        let setter = param
            .setter
            .ok_or_else(|| ConfigError::ImmutableParameter(name.to_string()))?;

        // Get old value before change
        let old_value = (param.getter)(self);

        // Apply the change
        setter(self, value).map_err(|e| {
            warn!(param = %name, value = %value, error = %e, "Invalid config value rejected");
            e
        })?;

        // Get new value after change
        let new_value = (param.getter)(self);

        info!(param = %name, old_value = %old_value, new_value = %new_value, "Config parameter changed");

        Ok(())
    }

    /// Get all parameter names.
    pub fn all_param_names(&self) -> Vec<&'static str> {
        self.params.iter().map(|p| p.name).collect()
    }

    /// Get mutable parameter names.
    pub fn mutable_param_names(&self) -> Vec<&'static str> {
        self.params
            .iter()
            .filter(|p| p.mutable)
            .map(|p| p.name)
            .collect()
    }

    /// Get immutable parameter names.
    pub fn immutable_param_names(&self) -> Vec<&'static str> {
        self.params
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
    pub fn maxmemory_policy(&self) -> String {
        self.runtime.read().unwrap().maxmemory_policy.clone()
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
    pub fn help_text() -> Vec<String> {
        vec![
            "CONFIG <subcommand> [<arg> ...]. Subcommands are:".to_string(),
            "GET <pattern>".to_string(),
            "    Return parameters matching <pattern>.".to_string(),
            "SET <param> <value>".to_string(),
            "    Set a mutable configuration parameter.".to_string(),
            "HELP".to_string(),
            "    Print this help.".to_string(),
            String::new(),
            "Mutable parameters: maxmemory, maxmemory-policy, maxmemory-samples, lfu-log-factor, lfu-decay-time, loglevel, durability-mode, sync-interval-ms, batch-timeout-ms, scatter-gather-timeout-ms, slowlog-log-slower-than, slowlog-max-len, slowlog-max-arg-len".to_string(),
            String::new(),
            "Immutable parameters (require restart): bind, port, num-shards, dir, persistence-enabled, metrics-enabled, metrics-port".to_string(),
        ]
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

    /// Set a config parameter, notifying shards if needed (async).
    ///
    /// This is the async version of `set` that also propagates eviction config
    /// changes to all shards and waits for acknowledgment.
    pub async fn set_async(&self, name: &str, value: &str) -> Result<(), ConfigError> {
        // First, apply the change (sync)
        self.set(name, value)?;

        // Check if this is an eviction param that needs shard notification
        let eviction_params = [
            "maxmemory",
            "maxmemory-policy",
            "maxmemory-samples",
            "lfu-log-factor",
            "lfu-decay-time",
        ];
        let normalized = name.to_lowercase().replace('_', "-");

        if eviction_params.contains(&normalized.as_str()) {
            // Notify shards of eviction config change
            let notifier = self.shard_notifier.read().unwrap().clone();
            if let Some(ref notifier) = notifier {
                notifier.notify_eviction_change().await?;
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
        // Build eviction config from current runtime config
        let eviction_config = {
            let config = self.runtime.read().unwrap();
            EvictionConfig {
                maxmemory: config.maxmemory,
                policy: config
                    .maxmemory_policy
                    .parse::<EvictionPolicy>()
                    .unwrap_or(EvictionPolicy::NoEviction),
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
        let help = ConfigManager::help_text();
        assert!(!help.is_empty());
        assert!(help[0].contains("CONFIG"));
    }
}
