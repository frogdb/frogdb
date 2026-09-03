//! Server configuration.

use frogdb_config_derive::ConfigParams;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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

    /// Run each client connection on the OS thread that owns its shard.
    ///
    /// This is the second half of thread-per-core: the shard executor gives every
    /// shard its own thread and runtime, and this setting decides whether the
    /// connections assigned to a shard are served *from* that runtime. When they
    /// are, a command for the connection's own shard is a same-thread handoff
    /// instead of a cross-core wakeup.
    ///
    /// The win assumes the process owns the machine's cores and `num-shards` is
    /// the core count, because colocation also caps a node's client-side CPU at
    /// `num-shards` cores. Turn it off where that assumption does not hold — most
    /// notably when several servers share one process, as the multi-node test
    /// harness does — and connections go back to the ambient runtime.
    #[serde(default = "default_colocate_connections")]
    #[param(skip)]
    // skip: startup-fixed execution-shape flag (threads are already running), no Redis analogue
    pub colocate_connections: bool,

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

    /// Per-class limits on the reply bytes buffered for one client, in Redis's
    /// `client-output-buffer-limit` spelling: whitespace-separated groups of
    /// `<class> <hard> <soft> <soft-seconds>` over the classes `normal`,
    /// `replica` (alias `slave`) and `pubsub`. `0` disables a limit; byte counts
    /// take Redis's `k`/`kb`/`m`/`mb`/`g`/`gb` suffixes. A class the value does
    /// not name keeps its default, so `normal 0 0 0 replica 268435456 67108864
    /// 60 pubsub 33554432 8388608 60` is what the default means.
    ///
    /// Startup-fixed: a connection reads the limits once when it is built, so
    /// `CONFIG GET` reports the honest startup value and `CONFIG SET` is
    /// rejected, matching the `json-max-size` treatment.
    ///
    /// This is the *only* client output-buffer knob: the `pubsub` triple bounds
    /// a subscriber's undelivered delivery queue as well as its socket buffers,
    /// which is what Redis's one `pubsub` class has always meant.
    ///
    /// **The `replica` class is not enforced on the replication feed yet.** A
    /// connection is judged against it only while it is still a client
    /// connection — after `PSYNC` hands the socket to the replication feed, the
    /// feed's buffering is outside this accounting entirely. Setting the
    /// `replica` triple therefore bounds a replica's *pre-handoff* connection
    /// and nothing after it. Use replication's own backlog settings to bound a
    /// slow replica until this is closed.
    #[serde(default = "default_client_output_buffer_limit")]
    #[param(name = "client-output-buffer-limit")]
    pub client_output_buffer_limit: String,
}

pub const DEFAULT_BIND: &str = "127.0.0.1";
pub const DEFAULT_PORT: u16 = 6379;
pub const DEFAULT_NUM_SHARDS: usize = 1;
pub const DEFAULT_SCATTER_GATHER_TIMEOUT_MS: u64 = 5000;
pub const DEFAULT_MAX_CLIENTS: u32 = 10000;
/// Default `client-output-buffer-limit`: Redis's three shipped triples, spelled
/// in bytes so `CONFIG GET` and this default agree character for character.
pub const DEFAULT_CLIENT_OUTPUT_BUFFER_LIMIT: &str =
    "normal 0 0 0 replica 268435456 67108864 60 pubsub 33554432 8388608 60";

fn default_bind() -> String {
    DEFAULT_BIND.to_string()
}

fn default_port() -> u16 {
    DEFAULT_PORT
}

fn default_num_shards() -> usize {
    DEFAULT_NUM_SHARDS
}

fn default_colocate_connections() -> bool {
    true
}

fn default_allow_cross_slot_standalone() -> bool {
    false
}

fn default_scatter_gather_timeout_ms() -> u64 {
    DEFAULT_SCATTER_GATHER_TIMEOUT_MS
}

fn default_max_clients() -> u32 {
    DEFAULT_MAX_CLIENTS
}

fn default_enable_debug_command() -> bool {
    false
}

fn default_client_output_buffer_limit() -> String {
    DEFAULT_CLIENT_OUTPUT_BUFFER_LIMIT.to_string()
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            port: default_port(),
            num_shards: default_num_shards(),
            colocate_connections: default_colocate_connections(),
            allow_cross_slot_standalone: default_allow_cross_slot_standalone(),
            scatter_gather_timeout_ms: default_scatter_gather_timeout_ms(),
            max_clients: default_max_clients(),
            enable_debug_command: default_enable_debug_command(),
            client_output_buffer_limit: default_client_output_buffer_limit(),
        }
    }
}
