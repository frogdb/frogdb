//! Hot shard detection configuration.
//!
//! Tunes the hot-shard collector (see `frogdb_debug::HotShardCollector`) that
//! flags shards receiving a disproportionate share of traffic in FrogDB's
//! shared-nothing, thread-per-core architecture.
//
// All four fields are live-mutable CONFIG params (`hotshards-enabled`,
// `hotshards-hot-threshold-percent`, `hotshards-warm-threshold-percent`,
// `hotshards-default-period-secs`). The collector holds them in a shared
// `SharedHotShardConfig` (atomics) that it re-reads on every `collect()`, so a
// runtime CONFIG SET retunes every hot-shard surface at once — FROGDB.HOTSHARDS,
// the `/status` JSON, and the debug web UI panel.

use frogdb_config_derive::ConfigParams;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Hot shard detection thresholds and defaults.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, ConfigParams)]
#[params(section = "hotshards")]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct HotShardsConfig {
    /// Traffic-share percentage (0-100) at or above which a shard is classified
    /// HOT.
    #[serde(default = "default_hot_threshold_percent")]
    #[param(mutable, name = "hotshards-hot-threshold-percent")]
    pub hot_threshold_percent: f64,

    /// Traffic-share percentage (0-100) at or above which a shard is classified
    /// WARM (must be below `hot_threshold_percent`).
    #[serde(default = "default_warm_threshold_percent")]
    #[param(mutable, name = "hotshards-warm-threshold-percent")]
    pub warm_threshold_percent: f64,

    /// Default window, in seconds, over which per-shard op-rates are computed
    /// when a request does not specify one.
    #[serde(default = "default_default_period_secs")]
    #[param(mutable, name = "hotshards-default-period-secs")]
    pub default_period_secs: u64,

    /// Whether per-shard op-rate accounting runs at all.
    ///
    /// The kill switch for the feature: with this off, shard workers skip the
    /// windowed counters entirely (one relaxed atomic load per dispatched
    /// command) and every hot-shard surface reports no shards rather than a
    /// stale window. Re-enabling starts a fresh window.
    #[serde(default = "default_enabled")]
    #[param(mutable, name = "hotshards-enabled")]
    pub enabled: bool,
}

pub const DEFAULT_HOT_THRESHOLD_PERCENT: f64 = 20.0;
pub const DEFAULT_WARM_THRESHOLD_PERCENT: f64 = 15.0;
pub const DEFAULT_DEFAULT_PERIOD_SECS: u64 = 10;
pub const DEFAULT_ENABLED: bool = true;

fn default_hot_threshold_percent() -> f64 {
    DEFAULT_HOT_THRESHOLD_PERCENT
}

fn default_warm_threshold_percent() -> f64 {
    DEFAULT_WARM_THRESHOLD_PERCENT
}

fn default_default_period_secs() -> u64 {
    DEFAULT_DEFAULT_PERIOD_SECS
}

fn default_enabled() -> bool {
    DEFAULT_ENABLED
}

impl Default for HotShardsConfig {
    fn default() -> Self {
        Self {
            hot_threshold_percent: default_hot_threshold_percent(),
            warm_threshold_percent: default_warm_threshold_percent(),
            default_period_secs: default_default_period_secs(),
            enabled: default_enabled(),
        }
    }
}

impl HotShardsConfig {
    /// Validate the hot shard configuration.
    pub fn validate(&self) -> anyhow::Result<()> {
        if !(0.0..=100.0).contains(&self.hot_threshold_percent) {
            anyhow::bail!("hotshards.hot_threshold_percent must be between 0 and 100");
        }
        if !(0.0..=100.0).contains(&self.warm_threshold_percent) {
            anyhow::bail!("hotshards.warm_threshold_percent must be between 0 and 100");
        }
        if self.warm_threshold_percent > self.hot_threshold_percent {
            anyhow::bail!(
                "hotshards.warm_threshold_percent ({}) must not exceed hot_threshold_percent ({})",
                self.warm_threshold_percent,
                self.hot_threshold_percent
            );
        }
        if self.default_period_secs == 0 {
            anyhow::bail!("hotshards.default_period_secs must be greater than 0");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_collector() {
        let cfg = HotShardsConfig::default();
        assert_eq!(cfg.hot_threshold_percent, 20.0);
        assert_eq!(cfg.warm_threshold_percent, 15.0);
        assert_eq!(cfg.default_period_secs, 10);
        assert!(cfg.enabled, "hot-shard accounting is on by default");
    }

    #[test]
    fn validate_rejects_inverted_thresholds() {
        let cfg = HotShardsConfig {
            hot_threshold_percent: 10.0,
            warm_threshold_percent: 20.0,
            default_period_secs: 10,
            enabled: true,
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_period() {
        let cfg = HotShardsConfig {
            default_period_secs: 0,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_accepts_defaults() {
        assert!(HotShardsConfig::default().validate().is_ok());
    }
}
