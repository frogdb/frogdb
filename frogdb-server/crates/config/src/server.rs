//! Server configuration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Sorted set index backend selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum SortedSetIndexConfig {
    Btreemap,
    Skiplist,
}

/// Server-specific configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// Bind address.
    #[serde(default = "default_bind")]
    pub bind: String,

    /// Listen port.
    #[serde(default = "default_port")]
    pub port: u16,

    /// Number of shards (0 = auto-detect CPU cores).
    #[serde(default = "default_num_shards")]
    pub num_shards: usize,

    /// Allow cross-slot operations in standalone mode.
    /// When enabled, multi-key commands like MGET/MSET can operate across different
    /// hash slots using scatter-gather. MSETNX always requires same-slot.
    #[serde(default = "default_allow_cross_slot_standalone")]
    pub allow_cross_slot_standalone: bool,

    /// Timeout for scatter-gather operations in milliseconds.
    #[serde(default = "default_scatter_gather_timeout_ms")]
    pub scatter_gather_timeout_ms: u64,

    /// Sorted set index backend: "skiplist" (default) or "btreemap".
    /// SkipList provides O(log n) rank queries; BTreeMap provides lower memory usage.
    /// Requires server restart to change.
    #[serde(default = "default_sorted_set_index")]
    pub sorted_set_index: SortedSetIndexConfig,

    /// Maximum number of simultaneous client connections (0 = unlimited).
    /// Admin port connections are exempt from this limit.
    #[serde(default = "default_max_clients")]
    pub max_clients: u32,
}

pub const DEFAULT_BIND: &str = "127.0.0.1";
pub const DEFAULT_PORT: u16 = 6379;
pub const DEFAULT_NUM_SHARDS: usize = 1;
pub const DEFAULT_SCATTER_GATHER_TIMEOUT_MS: u64 = 5000;
pub const DEFAULT_MAX_CLIENTS: u32 = 10000;

fn default_bind() -> String {
    DEFAULT_BIND.to_string()
}

fn default_port() -> u16 {
    DEFAULT_PORT
}

fn default_num_shards() -> usize {
    DEFAULT_NUM_SHARDS
}

fn default_allow_cross_slot_standalone() -> bool {
    false
}

fn default_scatter_gather_timeout_ms() -> u64 {
    DEFAULT_SCATTER_GATHER_TIMEOUT_MS
}

fn default_sorted_set_index() -> SortedSetIndexConfig {
    SortedSetIndexConfig::Skiplist
}

fn default_max_clients() -> u32 {
    DEFAULT_MAX_CLIENTS
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            port: default_port(),
            num_shards: default_num_shards(),
            allow_cross_slot_standalone: default_allow_cross_slot_standalone(),
            scatter_gather_timeout_ms: default_scatter_gather_timeout_ms(),
            sorted_set_index: default_sorted_set_index(),
            max_clients: default_max_clients(),
        }
    }
}
