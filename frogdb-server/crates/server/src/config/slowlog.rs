//! Slow query log configuration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Slow query log configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SlowlogConfig {
    /// Threshold in microseconds. Commands slower than this are logged.
    /// Set to 0 to log all commands, -1 to disable logging.
    #[serde(default = "default_slowlog_log_slower_than")]
    pub log_slower_than: i64,

    /// Maximum number of entries per shard.
    #[serde(default = "default_slowlog_max_len")]
    pub max_len: usize,

    /// Maximum characters per argument before truncation.
    #[serde(default = "default_slowlog_max_arg_len")]
    pub max_arg_len: usize,
}

fn default_slowlog_log_slower_than() -> i64 {
    frogdb_core::DEFAULT_SLOWLOG_LOG_SLOWER_THAN
}

fn default_slowlog_max_len() -> usize {
    frogdb_core::DEFAULT_SLOWLOG_MAX_LEN
}

fn default_slowlog_max_arg_len() -> usize {
    frogdb_core::DEFAULT_SLOWLOG_MAX_ARG_LEN
}

impl Default for SlowlogConfig {
    fn default() -> Self {
        Self {
            log_slower_than: default_slowlog_log_slower_than(),
            max_len: default_slowlog_max_len(),
            max_arg_len: default_slowlog_max_arg_len(),
        }
    }
}
