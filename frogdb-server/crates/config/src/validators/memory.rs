//! Memory-related configuration validators.

use super::{ConfigValidator, ValidationResult};
use crate::Config;

/// Warns when an eviction policy is set but maxmemory is 0 (unlimited).
pub struct EvictionPolicyWithoutLimitValidator;

impl ConfigValidator for EvictionPolicyWithoutLimitValidator {
    fn name(&self) -> &'static str {
        "eviction-policy-without-limit"
    }

    fn validate(&self, config: &Config) -> ValidationResult {
        let policy = config.memory.maxmemory_policy.to_lowercase();

        if config.memory.maxmemory == 0 && policy != "noeviction" {
            return ValidationResult::Warning(format!(
                "memory.maxmemory_policy is '{}' but memory.maxmemory is 0 (unlimited); \
                 the eviction policy will have no effect until a memory limit is set",
                config.memory.maxmemory_policy
            ));
        }

        ValidationResult::Ok
    }
}

/// Warns when num_shards is significantly larger than available CPUs.
pub struct ShardCountVsCpusValidator;

impl ConfigValidator for ShardCountVsCpusValidator {
    fn name(&self) -> &'static str {
        "shard-count-vs-cpus"
    }

    fn validate(&self, config: &Config) -> ValidationResult {
        let num_shards = config.server.num_shards;

        if num_shards == 0 {
            return ValidationResult::Ok;
        }

        let available_cpus = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1);

        let threshold = available_cpus * 2;

        if num_shards > threshold {
            return ValidationResult::Warning(format!(
                "server.num_shards ({}) is more than 2x the available CPUs ({}); \
                 this may lead to excessive context switching and reduced performance",
                num_shards, available_cpus
            ));
        }

        ValidationResult::Ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eviction_policy_without_limit_noeviction() {
        let mut config = Config::default();
        config.memory.maxmemory = 0;
        config.memory.maxmemory_policy = "noeviction".to_string();

        let validator = EvictionPolicyWithoutLimitValidator;
        assert!(validator.validate(&config).is_ok());
    }

    #[test]
    fn test_eviction_policy_without_limit_warning() {
        let mut config = Config::default();
        config.memory.maxmemory = 0;
        config.memory.maxmemory_policy = "allkeys-lru".to_string();

        let validator = EvictionPolicyWithoutLimitValidator;
        assert!(validator.validate(&config).is_warning());
    }

    #[test]
    fn test_shard_count_auto_detect() {
        let mut config = Config::default();
        config.server.num_shards = 0;

        let validator = ShardCountVsCpusValidator;
        assert!(validator.validate(&config).is_ok());
    }
}
