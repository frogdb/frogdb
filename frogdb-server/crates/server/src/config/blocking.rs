//! Blocking commands configuration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Blocking commands configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BlockingConfig {
    /// Maximum waiters per key (0 = unlimited).
    #[serde(default = "default_max_waiters_per_key")]
    pub max_waiters_per_key: usize,

    /// Maximum total blocked connections (0 = unlimited).
    #[serde(default = "default_max_blocked_connections")]
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
