//! Memory eviction module for FrogDB.
//!
//! This module implements Redis-compatible eviction policies for managing memory limits.
//! When `maxmemory` is exceeded, either writes are rejected (OOM error) or keys are
//! evicted based on the configured policy.
//!
//! # Eviction Policies
//!
//! - `NoEviction`: Reject writes with OOM error when memory limit exceeded
//! - `VolatileLru`: Evict least recently used keys that have TTL set
//! - `AllkeysLru`: Evict least recently used keys (any key)
//! - `VolatileLfu`: Evict least frequently used keys that have TTL set
//! - `AllkeysLfu`: Evict least frequently used keys (any key)
//! - `VolatileRandom`: Evict random keys that have TTL set
//! - `AllkeysRandom`: Evict random keys (any key)
//! - `VolatileTtl`: Evict keys with shortest remaining TTL
//!
//! # Implementation
//!
//! Eviction uses approximate algorithms based on sampling:
//! - Keys are randomly sampled (configurable `maxmemory_samples`)
//! - Best candidate from sample is selected based on policy
//! - An eviction pool maintains candidates across samples for better accuracy

mod lfu;
mod policy;
mod pool;

pub use lfu::{lfu_decay, lfu_log_incr};
pub use policy::EvictionPolicy;
pub use pool::{EvictionCandidate, EvictionPool};

/// Default number of samples to take when looking for eviction candidates.
pub const DEFAULT_MAXMEMORY_SAMPLES: usize = 5;

/// Default LFU log factor (controls probability of counter increment).
pub const DEFAULT_LFU_LOG_FACTOR: u8 = 10;

/// Default LFU decay time in minutes.
pub const DEFAULT_LFU_DECAY_TIME: u64 = 1;

/// Size of the eviction pool (per shard).
pub const EVICTION_POOL_SIZE: usize = 16;

/// Configuration for memory eviction.
#[derive(Debug, Clone)]
pub struct EvictionConfig {
    /// Maximum memory limit in bytes (0 = unlimited).
    pub maxmemory: u64,

    /// Eviction policy to use when memory limit is exceeded.
    pub policy: EvictionPolicy,

    /// Number of keys to sample when looking for eviction candidates.
    pub maxmemory_samples: usize,

    /// LFU log factor - higher values make counter increment less likely.
    pub lfu_log_factor: u8,

    /// LFU decay time in minutes - counter decays by 1 every N minutes.
    pub lfu_decay_time: u64,
}

impl Default for EvictionConfig {
    fn default() -> Self {
        Self {
            maxmemory: 0, // unlimited
            policy: EvictionPolicy::NoEviction,
            maxmemory_samples: DEFAULT_MAXMEMORY_SAMPLES,
            lfu_log_factor: DEFAULT_LFU_LOG_FACTOR,
            lfu_decay_time: DEFAULT_LFU_DECAY_TIME,
        }
    }
}

impl EvictionConfig {
    /// Create a new eviction config with specified memory limit and policy.
    pub fn new(maxmemory: u64, policy: EvictionPolicy) -> Self {
        Self {
            maxmemory,
            policy,
            ..Default::default()
        }
    }

    /// Check if memory limit is enabled.
    pub fn has_limit(&self) -> bool {
        self.maxmemory > 0
    }

    /// Check if eviction is enabled (not NoEviction policy).
    pub fn eviction_enabled(&self) -> bool {
        self.has_limit() && self.policy != EvictionPolicy::NoEviction
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eviction_config_default() {
        let config = EvictionConfig::default();
        assert_eq!(config.maxmemory, 0);
        assert_eq!(config.policy, EvictionPolicy::NoEviction);
        assert_eq!(config.maxmemory_samples, DEFAULT_MAXMEMORY_SAMPLES);
        assert_eq!(config.lfu_log_factor, DEFAULT_LFU_LOG_FACTOR);
        assert_eq!(config.lfu_decay_time, DEFAULT_LFU_DECAY_TIME);
    }

    #[test]
    fn test_eviction_config_has_limit() {
        let mut config = EvictionConfig::default();
        assert!(!config.has_limit());

        config.maxmemory = 1024 * 1024; // 1MB
        assert!(config.has_limit());
    }

    #[test]
    fn test_eviction_config_eviction_enabled() {
        let mut config = EvictionConfig::default();
        assert!(!config.eviction_enabled());

        config.maxmemory = 1024 * 1024;
        assert!(!config.eviction_enabled()); // Still NoEviction

        config.policy = EvictionPolicy::AllkeysLru;
        assert!(config.eviction_enabled());
    }
}
