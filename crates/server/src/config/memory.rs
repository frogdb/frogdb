//! Memory management configuration.

use anyhow::Result;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Memory management configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MemoryConfig {
    /// Maximum memory limit in bytes. 0 means unlimited.
    /// Can use human-readable formats like "100mb", "1gb" in config files.
    #[serde(default = "default_maxmemory")]
    pub maxmemory: u64,

    /// Eviction policy when maxmemory is reached.
    /// Options: noeviction, volatile-lru, allkeys-lru, volatile-lfu, allkeys-lfu,
    ///          volatile-random, allkeys-random, volatile-ttl
    #[serde(default = "default_maxmemory_policy")]
    pub maxmemory_policy: String,

    /// Number of keys to sample when looking for eviction candidates.
    #[serde(default = "default_maxmemory_samples")]
    pub maxmemory_samples: usize,

    /// LFU log factor - higher values make counter increment less likely.
    #[serde(default = "default_lfu_log_factor")]
    pub lfu_log_factor: u8,

    /// LFU decay time in minutes - counter decays by 1 every N minutes.
    #[serde(default = "default_lfu_decay_time")]
    pub lfu_decay_time: u64,

    /// Threshold in bytes for MEMORY DOCTOR big key detection.
    /// Keys larger than this will be flagged. Default: 1MB (1048576 bytes).
    #[serde(default = "default_doctor_big_key_threshold")]
    pub doctor_big_key_threshold: u64,

    /// Maximum number of big keys to report per shard in MEMORY DOCTOR.
    /// Default: 100.
    #[serde(default = "default_doctor_max_big_keys")]
    pub doctor_max_big_keys: usize,

    /// Threshold for shard memory imbalance detection (coefficient of variation).
    /// Shards with memory CV higher than this will trigger a warning. Default: 25%.
    #[serde(default = "default_doctor_imbalance_threshold")]
    pub doctor_imbalance_threshold: f64,
}

fn default_maxmemory() -> u64 {
    0 // Unlimited
}

fn default_maxmemory_policy() -> String {
    "noeviction".to_string()
}

fn default_maxmemory_samples() -> usize {
    5
}

fn default_lfu_log_factor() -> u8 {
    10
}

fn default_lfu_decay_time() -> u64 {
    1
}

fn default_doctor_big_key_threshold() -> u64 {
    1_048_576 // 1MB
}

fn default_doctor_max_big_keys() -> usize {
    100
}

fn default_doctor_imbalance_threshold() -> f64 {
    25.0 // 25% coefficient of variation
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            maxmemory: default_maxmemory(),
            maxmemory_policy: default_maxmemory_policy(),
            maxmemory_samples: default_maxmemory_samples(),
            lfu_log_factor: default_lfu_log_factor(),
            lfu_decay_time: default_lfu_decay_time(),
            doctor_big_key_threshold: default_doctor_big_key_threshold(),
            doctor_max_big_keys: default_doctor_max_big_keys(),
            doctor_imbalance_threshold: default_doctor_imbalance_threshold(),
        }
    }
}

impl MemoryConfig {
    /// Validate the memory configuration.
    pub fn validate(&self) -> Result<()> {
        // Validate policy string
        let valid_policies = [
            "noeviction",
            "volatile-lru",
            "allkeys-lru",
            "volatile-lfu",
            "allkeys-lfu",
            "volatile-random",
            "allkeys-random",
            "volatile-ttl",
        ];

        if !valid_policies.contains(&self.maxmemory_policy.to_lowercase().as_str()) {
            anyhow::bail!(
                "invalid maxmemory_policy '{}', expected one of: {}",
                self.maxmemory_policy,
                valid_policies.join(", ")
            );
        }

        // Validate samples
        if self.maxmemory_samples == 0 {
            anyhow::bail!("maxmemory_samples must be > 0");
        }

        Ok(())
    }

    /// Check if memory limit is enabled.
    pub fn has_limit(&self) -> bool {
        self.maxmemory > 0
    }

    /// Convert to MemoryDiagConfig for the metrics crate.
    pub fn to_diag_config(&self) -> frogdb_debug::MemoryDiagConfig {
        frogdb_debug::MemoryDiagConfig {
            big_key_threshold_bytes: self.doctor_big_key_threshold as usize,
            max_big_keys_per_shard: self.doctor_max_big_keys,
            imbalance_threshold_percent: self.doctor_imbalance_threshold,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_memory_config() {
        let config = MemoryConfig::default();
        assert_eq!(config.maxmemory, 0);
        assert_eq!(config.maxmemory_policy, "noeviction");
        assert_eq!(config.maxmemory_samples, 5);
        assert_eq!(config.lfu_log_factor, 10);
        assert_eq!(config.lfu_decay_time, 1);
    }

    #[test]
    fn test_memory_config_has_limit() {
        let mut config = MemoryConfig::default();
        assert!(!config.has_limit());

        config.maxmemory = 1024 * 1024;
        assert!(config.has_limit());
    }

    #[test]
    fn test_validate_memory_valid_policies() {
        let policies = [
            "noeviction",
            "volatile-lru",
            "allkeys-lru",
            "volatile-lfu",
            "allkeys-lfu",
            "volatile-random",
            "allkeys-random",
            "volatile-ttl",
        ];

        for policy in policies {
            let config = MemoryConfig {
                maxmemory_policy: policy.to_string(),
                ..Default::default()
            };
            assert!(
                config.validate().is_ok(),
                "Policy {} should be valid",
                policy
            );
        }
    }
}
