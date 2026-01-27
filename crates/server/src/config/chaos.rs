//! Chaos testing configuration (turmoil feature only).

use serde::{Deserialize, Serialize};

/// Chaos testing configuration for latency and failure injection.
/// Only available when compiled with `turmoil` feature.
#[cfg(feature = "turmoil")]
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
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

#[cfg(feature = "turmoil")]
impl ChaosConfig {
    /// Get jitter delay (0 to jitter_ms, random).
    pub fn get_jitter(&self) -> std::time::Duration {
        if self.jitter_ms == 0 {
            std::time::Duration::ZERO
        } else {
            use rand::Rng;
            let jitter = rand::thread_rng().gen_range(0..=self.jitter_ms);
            std::time::Duration::from_millis(jitter)
        }
    }

    /// Apply configured delay with optional jitter.
    pub async fn apply_delay(&self, base_ms: u64) {
        if base_ms > 0 || self.jitter_ms > 0 {
            let total = std::time::Duration::from_millis(base_ms) + self.get_jitter();
            if !total.is_zero() {
                tokio::time::sleep(total).await;
            }
        }
    }

    /// Check if a shard is configured as unavailable.
    pub fn is_shard_unavailable(&self, shard_id: usize) -> bool {
        self.unavailable_shards.contains(&shard_id)
    }

    /// Get the error message for a shard if it's configured to return errors.
    /// Returns None if the shard should operate normally.
    pub fn get_shard_error(&self, shard_id: usize) -> Option<&str> {
        self.error_shards.get(&shard_id).map(|s| s.as_str())
    }

    /// Check if a connection reset should be simulated based on probability.
    /// Returns true if the operation should be aborted with a connection error.
    pub fn should_simulate_connection_reset(&self) -> bool {
        if self.connection_reset_probability <= 0.0 {
            return false;
        }
        if self.connection_reset_probability >= 1.0 {
            return true;
        }
        use rand::Rng;
        rand::thread_rng().gen::<f64>() < self.connection_reset_probability
    }

    /// Check if any failure injection is configured.
    pub fn has_failure_injection(&self) -> bool {
        !self.unavailable_shards.is_empty()
            || !self.error_shards.is_empty()
            || self.connection_reset_probability > 0.0
    }
}
