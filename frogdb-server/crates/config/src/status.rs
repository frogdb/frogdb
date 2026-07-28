//! Status endpoint and hot shards configuration.

use frogdb_config_derive::ConfigParams;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Status endpoint configuration for health thresholds.
//
// The four health thresholds are exposed CONFIG GET-only (immutable) as of
// 13-01 Pass 2b: the status endpoint reads them from a startup snapshot
// (`StatusCollectorConfig`), so their startup values are honest to report but a
// runtime SET would not reach the collector without new propagation wiring.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, ConfigParams)]
#[params(section = "status")]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct StatusConfig {
    /// Threshold percentage for memory warning (0-100).
    #[serde(default = "default_memory_warning_percent")]
    #[param(name = "status-memory-warning-percent")]
    pub memory_warning_percent: u8,

    /// Threshold percentage for connection warning (0-100).
    #[serde(default = "default_connection_warning_percent")]
    #[param(name = "status-connection-warning-percent")]
    pub connection_warning_percent: u8,

    /// Durability lag warning threshold in milliseconds.
    #[serde(default = "default_durability_lag_warning_ms")]
    #[param(name = "status-durability-lag-warning-ms")]
    pub durability_lag_warning_ms: u64,

    /// Durability lag critical threshold in milliseconds.
    #[serde(default = "default_durability_lag_critical_ms")]
    #[param(name = "status-durability-lag-critical-ms")]
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
