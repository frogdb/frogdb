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
