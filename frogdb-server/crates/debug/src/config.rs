//! Configuration structs for debug diagnostics.

use serde::{Deserialize, Serialize};

/// Configuration for memory diagnostics (MEMORY DOCTOR).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryDiagConfig {
    /// Threshold in bytes for big key detection (default: 1MB).
    pub big_key_threshold_bytes: usize,
    /// Maximum number of big keys to report per shard (default: 100).
    pub max_big_keys_per_shard: usize,
    /// Threshold percentage for shard memory imbalance detection (default: 25%).
    pub imbalance_threshold_percent: f64,
}

impl Default for MemoryDiagConfig {
    fn default() -> Self {
        Self {
            big_key_threshold_bytes: 1_048_576, // 1MB
            max_big_keys_per_shard: 100,
            imbalance_threshold_percent: 25.0,
        }
    }
}

/// Configuration for hot shard detection (`FROGDB.HOTSHARDS`).
///
/// The single hot-shard config struct: the server's `[hotshards]` TOML section
/// (`frogdb_config::HotShardsConfig`) converts into this, and
/// [`crate::HotShardCollector`] holds it behind live-tunable shared state (see
/// [`crate::SharedHotShardConfig`]) so a runtime CONFIG SET can retune a running
/// collector. Defaults here are the source of truth the TOML section's defaults
/// are asserted against.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HotShardConfig {
    /// Traffic-share percentage at or above which a shard is classified HOT.
    pub hot_threshold_percent: f64,
    /// Traffic-share percentage at or above which a shard is classified WARM.
    pub warm_threshold_percent: f64,
    /// Window, in seconds, used when a request does not specify one.
    pub default_period_secs: u64,
    /// Whether per-shard op-rate accounting runs at all. With this off, shard
    /// workers skip the windowed counters and reports carry no shards.
    pub enabled: bool,
}

impl Default for HotShardConfig {
    fn default() -> Self {
        Self {
            hot_threshold_percent: 20.0,
            warm_threshold_percent: 15.0,
            default_period_secs: 10,
            enabled: true,
        }
    }
}
