//! VLL (Very Lightweight Locking) configuration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// VLL (Very Lightweight Locking) configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct VllConfig {
    /// Maximum queue depth per shard before rejecting new operations.
    #[serde(default = "default_vll_max_queue_depth")]
    pub max_queue_depth: usize,

    /// Timeout for acquiring locks on all shards (ms).
    #[serde(default = "default_vll_lock_acquisition_timeout_ms")]
    pub lock_acquisition_timeout_ms: u64,

    /// Per-shard lock acquisition timeout (ms).
    #[serde(default = "default_vll_per_shard_lock_timeout_ms")]
    pub per_shard_lock_timeout_ms: u64,

    /// Interval for checking/cleaning up expired operations (ms).
    #[serde(default = "default_vll_timeout_check_interval_ms")]
    pub timeout_check_interval_ms: u64,

    /// Maximum time a continuation lock can be held (ms).
    #[serde(default = "default_vll_max_continuation_lock_ms")]
    pub max_continuation_lock_ms: u64,
}

pub const DEFAULT_VLL_MAX_QUEUE_DEPTH: usize = 10000;
pub const DEFAULT_VLL_LOCK_ACQUISITION_TIMEOUT_MS: u64 = 4000;
pub const DEFAULT_VLL_PER_SHARD_LOCK_TIMEOUT_MS: u64 = 2000;
pub const DEFAULT_VLL_TIMEOUT_CHECK_INTERVAL_MS: u64 = 100;
pub const DEFAULT_VLL_MAX_CONTINUATION_LOCK_MS: u64 = 65000;

fn default_vll_max_queue_depth() -> usize {
    DEFAULT_VLL_MAX_QUEUE_DEPTH
}

fn default_vll_lock_acquisition_timeout_ms() -> u64 {
    DEFAULT_VLL_LOCK_ACQUISITION_TIMEOUT_MS
}

fn default_vll_per_shard_lock_timeout_ms() -> u64 {
    DEFAULT_VLL_PER_SHARD_LOCK_TIMEOUT_MS
}

fn default_vll_timeout_check_interval_ms() -> u64 {
    DEFAULT_VLL_TIMEOUT_CHECK_INTERVAL_MS
}

fn default_vll_max_continuation_lock_ms() -> u64 {
    DEFAULT_VLL_MAX_CONTINUATION_LOCK_MS
}

impl Default for VllConfig {
    fn default() -> Self {
        Self {
            max_queue_depth: default_vll_max_queue_depth(),
            lock_acquisition_timeout_ms: default_vll_lock_acquisition_timeout_ms(),
            per_shard_lock_timeout_ms: default_vll_per_shard_lock_timeout_ms(),
            timeout_check_interval_ms: default_vll_timeout_check_interval_ms(),
            max_continuation_lock_ms: default_vll_max_continuation_lock_ms(),
        }
    }
}
