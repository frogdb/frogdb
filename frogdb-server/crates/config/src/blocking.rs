//! Blocking commands configuration.

use frogdb_config_derive::ConfigParams;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Blocking commands configuration.
//
// No fields are exposed as CONFIG GET/SET parameters; each carries an explicit
// `#[param(skip)]` to satisfy the per-field coverage guarantee.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, ConfigParams)]
#[params(section = "blocking")]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct BlockingConfig {
    /// Maximum waiters per key (0 = unlimited).
    #[serde(default = "default_max_waiters_per_key")]
    #[param(skip)]
    pub max_waiters_per_key: usize,

    /// Maximum total blocked connections (0 = unlimited).
    #[serde(default = "default_max_blocked_connections")]
    #[param(skip)]
    pub max_blocked_connections: usize,
}

pub const DEFAULT_MAX_WAITERS_PER_KEY: usize = 10000;
pub const DEFAULT_MAX_BLOCKED_CONNECTIONS: usize = 50000;

fn default_max_waiters_per_key() -> usize {
    DEFAULT_MAX_WAITERS_PER_KEY
}

fn default_max_blocked_connections() -> usize {
    DEFAULT_MAX_BLOCKED_CONNECTIONS
}

impl Default for BlockingConfig {
    fn default() -> Self {
        Self {
            max_waiters_per_key: default_max_waiters_per_key(),
            max_blocked_connections: default_max_blocked_connections(),
        }
    }
}
