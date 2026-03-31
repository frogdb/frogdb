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

pub const DEFAULT_MAXMEMORY_SAMPLES: usize = 5;
pub const DEFAULT_LFU_LOG_FACTOR: u8 = 10;
pub const DEFAULT_LFU_DECAY_TIME: u64 = 1;
pub const DEFAULT_DOCTOR_BIG_KEY_THRESHOLD: u64 = 1_048_576;
pub const DEFAULT_DOCTOR_MAX_BIG_KEYS: usize = 100;
pub const DEFAULT_DOCTOR_IMBALANCE_THRESHOLD: f64 = 25.0;

fn default_maxmemory_samples() -> usize {
    DEFAULT_MAXMEMORY_SAMPLES
}

fn default_lfu_log_factor() -> u8 {
    DEFAULT_LFU_LOG_FACTOR
}

fn default_lfu_decay_time() -> u64 {
    DEFAULT_LFU_DECAY_TIME
}

fn default_doctor_big_key_threshold() -> u64 {
    DEFAULT_DOCTOR_BIG_KEY_THRESHOLD
}

fn default_doctor_max_big_keys() -> usize {
    DEFAULT_DOCTOR_MAX_BIG_KEYS
}

fn default_doctor_imbalance_threshold() -> f64 {
    DEFAULT_DOCTOR_IMBALANCE_THRESHOLD
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
        let valid_policies = [
            "noeviction",
            "volatile-lru",
            "allkeys-lru",
            "volatile-lfu",
            "allkeys-lfu",
            "volatile-random",
            "allkeys-random",
            "volatile-ttl",
            "tiered-lru",
            "tiered-lfu",
        ];

        if !valid_policies.contains(&self.maxmemory_policy.to_lowercase().as_str()) {
            anyhow::bail!(
                "invalid maxmemory_policy '{}', expected one of: {}",
                self.maxmemory_policy,
                valid_policies.join(", ")
            );
        }

        if self.maxmemory_samples == 0 {
            anyhow::bail!("maxmemory_samples must be > 0");
        }

        if self.doctor_imbalance_threshold <= 0.0 || self.doctor_imbalance_threshold > 100.0 {
            anyhow::bail!("memory.doctor_imbalance_threshold must be > 0.0 and <= 100.0");
        }

        Ok(())
    }

    /// Check if memory limit is enabled.
    pub fn has_limit(&self) -> bool {
        self.maxmemory > 0
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
        assert_eq!(config.maxmemory_samples, DEFAULT_MAXMEMORY_SAMPLES);
        assert_eq!(config.lfu_log_factor, DEFAULT_LFU_LOG_FACTOR);
        assert_eq!(config.lfu_decay_time, DEFAULT_LFU_DECAY_TIME);
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
