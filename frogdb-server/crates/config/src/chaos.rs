//! Chaos testing configuration (turmoil feature only).

use serde::{Deserialize, Serialize};

/// Chaos testing configuration for latency and failure injection.
/// Only available when compiled with `turmoil` feature.
#[cfg(feature = "turmoil")]
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct ChaosConfig {
    /// Delay (ms) between scatter sends to different shards.
    /// Useful for testing interleaving of concurrent operations.
    #[serde(default)]
    pub scatter_inter_send_delay_ms: u64,

    /// Per-shard latency overrides (shard_id -> delay_ms).
    /// Applied before sending to each shard.
    #[serde(default)]
    pub shard_delays_ms: std::collections::HashMap<usize, u64>,

    /// Random jitter range (0 to jitter_ms).
    /// Added on top of other delays for more realistic simulation.
    #[serde(default)]
    pub jitter_ms: u64,

    /// Delay (ms) before single-shard command execution.
    #[serde(default)]
    pub single_shard_delay_ms: u64,

    /// Delay (ms) before transaction EXEC processing.
    #[serde(default)]
    pub transaction_delay_ms: u64,

    // === Failure Injection Fields ===
    /// Shard IDs that simulate being unavailable (requests timeout).
    /// When a shard is in this set, scatter-gather operations to it will fail.
    #[serde(default)]
    pub unavailable_shards: std::collections::HashSet<usize>,

    /// Probability (0.0-1.0) of simulating connection reset during operations.
    /// Applied per-operation to simulate network instability.
    #[serde(default)]
    pub connection_reset_probability: f64,

    /// Shard IDs that return errors instead of successful responses.
    /// Maps shard_id -> error message to return.
    #[serde(default)]
    pub error_shards: std::collections::HashMap<usize, String>,
}
