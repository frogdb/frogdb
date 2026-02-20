//! Chaos testing presets for simulation tests.
//!
//! This module provides named chaos configurations for parameterized testing,
//! enabling Cartesian products of delay values and failure modes.

use frogdb_server::config::ChaosConfig;
use std::collections::{HashMap, HashSet};

/// Standard delay values for parameterized tests (in milliseconds).
pub const DELAY_VALUES: [u64; 4] = [0, 50, 100, 250];

/// Tier 1 delay values for quick CI tests.
pub const QUICK_DELAY_VALUES: [u64; 3] = [0, 50, 100];

/// Named chaos presets for simulation tests.
///
/// Each preset represents a specific chaos scenario that can be applied
/// to the server during tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChaosPreset {
    /// No chaos - baseline testing.
    None,

    /// Delay between scatter sends to different shards.
    /// Good for testing interleaving of concurrent scatter-gather operations.
    ScatterDelay(u64),

    /// Delay applied to single-shard commands.
    /// Good for testing command latency handling.
    SingleShardDelay(u64),

    /// Delay applied before transaction EXEC processing.
    /// Good for testing transaction timing.
    TransactionDelay(u64),

    /// Combined delays on scatter, single-shard, and transaction operations.
    AllDelays(u64),

    /// Simulate a single shard being unavailable.
    /// The shard_id will be shard 0 by default (or can be configured via `to_config_with_shard`).
    ShardUnavailable,

    /// Simulate partial failure where one shard returns errors.
    /// The shard_id will be shard 0 by default.
    PartialFailure,

    /// Simulate intermittent connection resets with low probability.
    ConnectionReset,

    /// Simulate intermittent connection resets with high probability.
    ConnectionResetHigh,
}

impl ChaosPreset {
    /// Convert this preset to a ChaosConfig.
    ///
    /// # Arguments
    /// * `num_shards` - Total number of shards in the server (used for failure injection)
    pub fn to_config(&self, num_shards: usize) -> ChaosConfig {
        self.to_config_with_shard(num_shards, 0)
    }

    /// Convert this preset to a ChaosConfig with a specific target shard for failures.
    ///
    /// # Arguments
    /// * `num_shards` - Total number of shards in the server
    /// * `target_shard` - Which shard to target for failure injection
    pub fn to_config_with_shard(&self, _num_shards: usize, target_shard: usize) -> ChaosConfig {
        match self {
            ChaosPreset::None => ChaosConfig::default(),

            ChaosPreset::ScatterDelay(ms) => ChaosConfig {
                scatter_inter_send_delay_ms: *ms,
                ..Default::default()
            },

            ChaosPreset::SingleShardDelay(ms) => ChaosConfig {
                single_shard_delay_ms: *ms,
                ..Default::default()
            },

            ChaosPreset::TransactionDelay(ms) => ChaosConfig {
                transaction_delay_ms: *ms,
                ..Default::default()
            },

            ChaosPreset::AllDelays(ms) => ChaosConfig {
                scatter_inter_send_delay_ms: *ms,
                single_shard_delay_ms: *ms,
                transaction_delay_ms: *ms,
                ..Default::default()
            },

            ChaosPreset::ShardUnavailable => {
                let mut unavailable = HashSet::new();
                unavailable.insert(target_shard);
                ChaosConfig {
                    unavailable_shards: unavailable,
                    ..Default::default()
                }
            }

            ChaosPreset::PartialFailure => {
                let mut error_shards = HashMap::new();
                error_shards.insert(target_shard, "ERR simulated shard error".to_string());
                ChaosConfig {
                    error_shards,
                    ..Default::default()
                }
            }

            ChaosPreset::ConnectionReset => ChaosConfig {
                connection_reset_probability: 0.1, // 10% chance
                ..Default::default()
            },

            ChaosPreset::ConnectionResetHigh => ChaosConfig {
                connection_reset_probability: 0.5, // 50% chance
                ..Default::default()
            },
        }
    }

    /// Get a human-readable description of this preset.
    pub fn description(&self) -> &'static str {
        match self {
            ChaosPreset::None => "no chaos",
            ChaosPreset::ScatterDelay(_) => "scatter inter-send delay",
            ChaosPreset::SingleShardDelay(_) => "single-shard command delay",
            ChaosPreset::TransactionDelay(_) => "transaction delay",
            ChaosPreset::AllDelays(_) => "all delays combined",
            ChaosPreset::ShardUnavailable => "shard unavailable",
            ChaosPreset::PartialFailure => "partial shard failure",
            ChaosPreset::ConnectionReset => "connection reset (10%)",
            ChaosPreset::ConnectionResetHigh => "connection reset (50%)",
        }
    }

    /// Returns true if this preset involves failure injection (not just delays).
    pub fn is_failure_mode(&self) -> bool {
        matches!(
            self,
            ChaosPreset::ShardUnavailable
                | ChaosPreset::PartialFailure
                | ChaosPreset::ConnectionReset
                | ChaosPreset::ConnectionResetHigh
        )
    }

    /// Get all delay-based presets for a given delay value.
    pub fn delay_presets(delay_ms: u64) -> Vec<ChaosPreset> {
        vec![
            ChaosPreset::ScatterDelay(delay_ms),
            ChaosPreset::SingleShardDelay(delay_ms),
            ChaosPreset::TransactionDelay(delay_ms),
            ChaosPreset::AllDelays(delay_ms),
        ]
    }

    /// Get all failure mode presets.
    pub fn failure_presets() -> Vec<ChaosPreset> {
        vec![
            ChaosPreset::ShardUnavailable,
            ChaosPreset::PartialFailure,
            ChaosPreset::ConnectionReset,
        ]
    }
}

impl std::fmt::Display for ChaosPreset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChaosPreset::None => write!(f, "none"),
            ChaosPreset::ScatterDelay(ms) => write!(f, "scatter_{}ms", ms),
            ChaosPreset::SingleShardDelay(ms) => write!(f, "single_{}ms", ms),
            ChaosPreset::TransactionDelay(ms) => write!(f, "txn_{}ms", ms),
            ChaosPreset::AllDelays(ms) => write!(f, "all_{}ms", ms),
            ChaosPreset::ShardUnavailable => write!(f, "shard_unavail"),
            ChaosPreset::PartialFailure => write!(f, "partial_fail"),
            ChaosPreset::ConnectionReset => write!(f, "conn_reset"),
            ChaosPreset::ConnectionResetHigh => write!(f, "conn_reset_high"),
        }
    }
}

/// Builder for creating custom ChaosConfig instances.
///
/// Provides a fluent API for combining multiple chaos parameters.
#[derive(Debug, Clone, Default)]
pub struct ChaosConfigBuilder {
    config: ChaosConfig,
}

impl ChaosConfigBuilder {
    /// Create a new builder with default (no chaos) settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the scatter inter-send delay.
    pub fn scatter_delay(mut self, ms: u64) -> Self {
        self.config.scatter_inter_send_delay_ms = ms;
        self
    }

    /// Set the single-shard command delay.
    pub fn single_shard_delay(mut self, ms: u64) -> Self {
        self.config.single_shard_delay_ms = ms;
        self
    }

    /// Set the transaction delay.
    pub fn transaction_delay(mut self, ms: u64) -> Self {
        self.config.transaction_delay_ms = ms;
        self
    }

    /// Set jitter range.
    pub fn jitter(mut self, ms: u64) -> Self {
        self.config.jitter_ms = ms;
        self
    }

    /// Add a per-shard delay.
    pub fn shard_delay(mut self, shard_id: usize, ms: u64) -> Self {
        self.config.shard_delays_ms.insert(shard_id, ms);
        self
    }

    /// Mark a shard as unavailable.
    pub fn unavailable_shard(mut self, shard_id: usize) -> Self {
        self.config.unavailable_shards.insert(shard_id);
        self
    }

    /// Configure a shard to return errors.
    pub fn error_shard(mut self, shard_id: usize, error_msg: impl Into<String>) -> Self {
        self.config.error_shards.insert(shard_id, error_msg.into());
        self
    }

    /// Set connection reset probability.
    pub fn connection_reset_prob(mut self, probability: f64) -> Self {
        self.config.connection_reset_probability = probability.clamp(0.0, 1.0);
        self
    }

    /// Build the final ChaosConfig.
    pub fn build(self) -> ChaosConfig {
        self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_preset_none() {
        let config = ChaosPreset::None.to_config(4);
        assert_eq!(config.scatter_inter_send_delay_ms, 0);
        assert_eq!(config.single_shard_delay_ms, 0);
        assert!(config.unavailable_shards.is_empty());
    }

    #[test]
    fn test_preset_scatter_delay() {
        let config = ChaosPreset::ScatterDelay(50).to_config(4);
        assert_eq!(config.scatter_inter_send_delay_ms, 50);
        assert_eq!(config.single_shard_delay_ms, 0);
    }

    #[test]
    fn test_preset_all_delays() {
        let config = ChaosPreset::AllDelays(100).to_config(4);
        assert_eq!(config.scatter_inter_send_delay_ms, 100);
        assert_eq!(config.single_shard_delay_ms, 100);
        assert_eq!(config.transaction_delay_ms, 100);
    }

    #[test]
    fn test_preset_shard_unavailable() {
        let config = ChaosPreset::ShardUnavailable.to_config(4);
        assert!(config.unavailable_shards.contains(&0));
    }

    #[test]
    fn test_preset_shard_unavailable_with_target() {
        let config = ChaosPreset::ShardUnavailable.to_config_with_shard(4, 2);
        assert!(config.unavailable_shards.contains(&2));
        assert!(!config.unavailable_shards.contains(&0));
    }

    #[test]
    fn test_preset_partial_failure() {
        let config = ChaosPreset::PartialFailure.to_config(4);
        assert!(config.error_shards.contains_key(&0));
    }

    #[test]
    fn test_preset_connection_reset() {
        let config = ChaosPreset::ConnectionReset.to_config(4);
        assert!((config.connection_reset_probability - 0.1).abs() < f64::EPSILON);
    }

    #[test]
    fn test_builder() {
        let config = ChaosConfigBuilder::new()
            .scatter_delay(50)
            .single_shard_delay(25)
            .unavailable_shard(1)
            .error_shard(2, "test error")
            .build();

        assert_eq!(config.scatter_inter_send_delay_ms, 50);
        assert_eq!(config.single_shard_delay_ms, 25);
        assert!(config.unavailable_shards.contains(&1));
        assert!(config.error_shards.contains_key(&2));
    }

    #[test]
    fn test_preset_display() {
        assert_eq!(format!("{}", ChaosPreset::None), "none");
        assert_eq!(format!("{}", ChaosPreset::ScatterDelay(50)), "scatter_50ms");
        assert_eq!(
            format!("{}", ChaosPreset::ShardUnavailable),
            "shard_unavail"
        );
    }

    #[test]
    fn test_is_failure_mode() {
        assert!(!ChaosPreset::None.is_failure_mode());
        assert!(!ChaosPreset::ScatterDelay(50).is_failure_mode());
        assert!(ChaosPreset::ShardUnavailable.is_failure_mode());
        assert!(ChaosPreset::PartialFailure.is_failure_mode());
        assert!(ChaosPreset::ConnectionReset.is_failure_mode());
    }
}
