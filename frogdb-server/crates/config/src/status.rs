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

pub const DEFAULT_MEMORY_WARNING_PERCENT: u8 = 90;
pub const DEFAULT_CONNECTION_WARNING_PERCENT: u8 = 90;
pub const DEFAULT_DURABILITY_LAG_WARNING_MS: u64 = 5000;
pub const DEFAULT_DURABILITY_LAG_CRITICAL_MS: u64 = 30000;

fn default_memory_warning_percent() -> u8 {
    DEFAULT_MEMORY_WARNING_PERCENT
}

fn default_connection_warning_percent() -> u8 {
    DEFAULT_CONNECTION_WARNING_PERCENT
}

fn default_durability_lag_warning_ms() -> u64 {
    DEFAULT_DURABILITY_LAG_WARNING_MS
}

fn default_durability_lag_critical_ms() -> u64 {
    DEFAULT_DURABILITY_LAG_CRITICAL_MS
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
    /// Validate the status configuration.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.memory_warning_percent == 0 || self.memory_warning_percent > 100 {
            anyhow::bail!("status.memory_warning_percent must be between 1 and 100");
        }
        if self.connection_warning_percent == 0 || self.connection_warning_percent > 100 {
            anyhow::bail!("status.connection_warning_percent must be between 1 and 100");
        }
        if self.durability_lag_warning_ms >= self.durability_lag_critical_ms {
            anyhow::bail!(
                "status.durability_lag_warning_ms ({}) must be less than durability_lag_critical_ms ({})",
                self.durability_lag_warning_ms,
                self.durability_lag_critical_ms
            );
        }
        Ok(())
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

pub const DEFAULT_HOT_THRESHOLD_PERCENT: f64 = 20.0;
pub const DEFAULT_WARM_THRESHOLD_PERCENT: f64 = 15.0;
pub const DEFAULT_HOTSHARDS_PERIOD_SECS: u64 = 10;

fn default_hot_threshold_percent() -> f64 {
    DEFAULT_HOT_THRESHOLD_PERCENT
}

fn default_warm_threshold_percent() -> f64 {
    DEFAULT_WARM_THRESHOLD_PERCENT
}

fn default_hotshards_period_secs() -> u64 {
    DEFAULT_HOTSHARDS_PERIOD_SECS
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
    /// Validate the hot shards configuration.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.hot_threshold_percent <= 0.0 || self.hot_threshold_percent > 100.0 {
            anyhow::bail!("hotshards.hot_threshold_percent must be > 0.0 and <= 100.0");
        }
        if self.warm_threshold_percent <= 0.0 || self.warm_threshold_percent > 100.0 {
            anyhow::bail!("hotshards.warm_threshold_percent must be > 0.0 and <= 100.0");
        }
        if self.warm_threshold_percent >= self.hot_threshold_percent {
            anyhow::bail!(
                "hotshards.warm_threshold_percent ({}) must be less than hot_threshold_percent ({})",
                self.warm_threshold_percent,
                self.hot_threshold_percent
            );
        }
        Ok(())
    }
}
