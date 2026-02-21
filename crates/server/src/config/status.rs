//! Status endpoint and hot shards configuration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Status endpoint configuration for health thresholds.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StatusConfig {
    /// Threshold percentage for memory warning (0-100).
    #[serde(default = "default_memory_warning_percent")]
    pub memory_warning_percent: u8,

    /// Threshold percentage for connection warning (0-100).
    #[serde(default = "default_connection_warning_percent")]
    pub connection_warning_percent: u8,

    /// Durability lag warning threshold in milliseconds (default: 5000 = 5 seconds).
    #[serde(default = "default_durability_lag_warning_ms")]
    pub durability_lag_warning_ms: u64,

    /// Durability lag critical threshold in milliseconds (default: 30000 = 30 seconds).
    #[serde(default = "default_durability_lag_critical_ms")]
    pub durability_lag_critical_ms: u64,
}

fn default_memory_warning_percent() -> u8 {
    90
}

fn default_connection_warning_percent() -> u8 {
    90
}

fn default_durability_lag_warning_ms() -> u64 {
    5000 // 5 seconds
}

fn default_durability_lag_critical_ms() -> u64 {
    30000 // 30 seconds
}

impl Default for StatusConfig {
    fn default() -> Self {
        Self {
            memory_warning_percent: default_memory_warning_percent(),
            connection_warning_percent: default_connection_warning_percent(),
            durability_lag_warning_ms: default_durability_lag_warning_ms(),
            durability_lag_critical_ms: default_durability_lag_critical_ms(),
        }
    }
}

impl StatusConfig {
    /// Convert to StatusCollectorConfig.
    pub fn to_collector_config(&self) -> frogdb_telemetry::StatusCollectorConfig {
        frogdb_telemetry::StatusCollectorConfig {
            memory_warning_percent: self.memory_warning_percent,
            connection_warning_percent: self.connection_warning_percent,
            durability_lag_warning_ms: self.durability_lag_warning_ms,
            durability_lag_critical_ms: self.durability_lag_critical_ms,
        }
    }
}

/// Hot shard detection configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HotShardsConfig {
    /// Threshold percentage for "HOT" status (default: 20.0).
    #[serde(default = "default_hot_threshold_percent")]
    pub hot_threshold_percent: f64,

    /// Threshold percentage for "WARM" status (default: 15.0).
    #[serde(default = "default_warm_threshold_percent")]
    pub warm_threshold_percent: f64,

    /// Default period for stats collection in seconds (default: 10).
    #[serde(default = "default_hotshards_period_secs")]
    pub default_period_secs: u64,
}

fn default_hot_threshold_percent() -> f64 {
    20.0
}

fn default_warm_threshold_percent() -> f64 {
    15.0
}

fn default_hotshards_period_secs() -> u64 {
    10
}

impl Default for HotShardsConfig {
    fn default() -> Self {
        Self {
            hot_threshold_percent: default_hot_threshold_percent(),
            warm_threshold_percent: default_warm_threshold_percent(),
            default_period_secs: default_hotshards_period_secs(),
        }
    }
}

impl HotShardsConfig {
    /// Convert to HotShardConfig for the metrics crate.
    pub fn to_collector_config(&self) -> frogdb_debug::HotShardConfig {
        frogdb_debug::HotShardConfig {
            hot_threshold_percent: self.hot_threshold_percent,
            warm_threshold_percent: self.warm_threshold_percent,
            default_period_secs: self.default_period_secs,
        }
    }
}
