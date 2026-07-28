//! Server configuration.

use frogdb_config_derive::ConfigParams;
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
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, ConfigParams)]
#[params(section = "server")]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct ServerConfig {
    /// Bind address.
    #[serde(default = "default_bind")]
    #[param]
    pub bind: String,

    /// Listen port.
    #[serde(default = "default_port")]
    #[param]
    pub port: u16,

    /// Number of shards (0 = auto-detect CPU cores).
    #[serde(default = "default_num_shards")]
    #[param]
    pub num_shards: usize,

    /// Allow cross-slot operations in standalone mode.
    /// When enabled, multi-key commands like MGET/MSET can operate across different
    /// hash slots using scatter-gather. MSETNX always requires same-slot.
    #[serde(default = "default_allow_cross_slot_standalone")]
    #[param(skip)]
    // skip: borderline: changes multi-key command semantics; startup-fixed behavior flag, no Redis analogue
    pub allow_cross_slot_standalone: bool,

    /// Timeout for scatter-gather operations in milliseconds.
    #[serde(default = "default_scatter_gather_timeout_ms")]
    #[param(mutable)]
    pub scatter_gather_timeout_ms: u64,

    /// Sorted set index backend: "skiplist" (default) or "btreemap".
    /// SkipList provides O(log n) rank queries; BTreeMap provides lower memory usage.
    /// Requires server restart to change.
    #[serde(default = "default_sorted_set_index")]
    #[param]
    pub sorted_set_index: SortedSetIndexConfig,

    /// Maximum number of simultaneous client connections (0 = unlimited).
    /// Admin port connections are exempt from this limit.
    #[serde(default = "default_max_clients")]
    #[param(mutable, name = "maxclients")]
    pub max_clients: u32,

    /// Enable the DEBUG family of subcommands that are unsafe in production.
    ///
    /// Currently gates `DEBUG SLEEP`, which parks the connection task for an
    /// arbitrary duration and is a trivial denial-of-service vector if
    /// exposed to untrusted clients. Default: `false`. The test harness
    /// defaults it to `true` so existing test-only DEBUG commands keep
    /// working.
    #[serde(default = "default_enable_debug_command")]
    #[param]
    pub enable_debug_command: bool,

    /// Hard limit, in bytes, on the pub/sub messages buffered server-side for a
    /// single slow / non-reading subscriber. Once a connection's pending
    /// delivery queue would exceed this, further messages are dropped and the
    /// connection is torn down, so a stalled subscriber cannot force unbounded
    /// memory growth. Mirrors Redis's `client-output-buffer-limit pubsub` hard
    /// limit (default 32mb). `0` disables the bound (legacy unbounded delivery).
    ///
    /// Startup-fixed: consumed when a connection lazily allocates its pub/sub
    /// channel, so `CONFIG GET` reports the honest startup value and `CONFIG
    /// SET` is rejected (immutable), matching the `json-max-size` treatment.
    #[serde(default = "default_pubsub_output_buffer_hard_limit")]
    #[param(name = "pubsub-output-buffer-hard-limit")]
    pub pubsub_output_buffer_hard_limit: usize,
}

pub const DEFAULT_BIND: &str = "127.0.0.1";
pub const DEFAULT_PORT: u16 = 6379;
pub const DEFAULT_NUM_SHARDS: usize = 1;
pub const DEFAULT_SCATTER_GATHER_TIMEOUT_MS: u64 = 5000;
pub const DEFAULT_MAX_CLIENTS: u32 = 10000;
/// Default pub/sub output-buffer hard limit (32 MiB), matching Redis's
/// `client-output-buffer-limit pubsub 32mb` hard limit.
pub const DEFAULT_PUBSUB_OUTPUT_BUFFER_HARD_LIMIT: usize = 32 * 1024 * 1024;

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

fn default_enable_debug_command() -> bool {
    false
}

fn default_pubsub_output_buffer_hard_limit() -> usize {
    DEFAULT_PUBSUB_OUTPUT_BUFFER_HARD_LIMIT
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
            enable_debug_command: default_enable_debug_command(),
            pubsub_output_buffer_hard_limit: default_pubsub_output_buffer_hard_limit(),
        }
    }
}
