//! Timeout-related configuration validators.

use super::{ConfigValidator, ValidationResult};
use crate::config::Config;

/// Validates VLL timeout ordering.
///
/// Rule: `per_shard_lock_timeout_ms` < `lock_acquisition_timeout_ms` < `scatter_gather_timeout_ms`
///
/// This ensures that per-shard timeouts fire before the overall lock acquisition timeout,
/// which in turn fires before the scatter-gather timeout.
pub struct VllTimeoutOrderingValidator;

impl ConfigValidator for VllTimeoutOrderingValidator {
    fn name(&self) -> &'static str {
        "vll-timeout-ordering"
    }

    fn validate(&self, config: &Config) -> ValidationResult {
        let per_shard = config.vll.per_shard_lock_timeout_ms;
        let lock_acq = config.vll.lock_acquisition_timeout_ms;
        let scatter = config.server.scatter_gather_timeout_ms;

        if per_shard >= lock_acq {
            return ValidationResult::Error(format!(
                "vll.per_shard_lock_timeout_ms ({}) must be less than vll.lock_acquisition_timeout_ms ({})",
                per_shard, lock_acq
            ));
        }

        if lock_acq >= scatter {
            return ValidationResult::Error(format!(
                "vll.lock_acquisition_timeout_ms ({}) must be less than server.scatter_gather_timeout_ms ({})",
                lock_acq, scatter
            ));
        }

        ValidationResult::Ok
    }
}

/// Validates replication backoff ordering.
///
/// Rule: `reconnect_backoff_initial_ms` <= `reconnect_backoff_max_ms`
pub struct ReplicationBackoffOrderingValidator;

impl ConfigValidator for ReplicationBackoffOrderingValidator {
    fn name(&self) -> &'static str {
        "replication-backoff-ordering"
    }

    fn validate(&self, config: &Config) -> ValidationResult {
        let initial = config.replication.reconnect_backoff_initial_ms;
        let max = config.replication.reconnect_backoff_max_ms;

        if initial > max {
            return ValidationResult::Error(format!(
                "replication.reconnect_backoff_initial_ms ({}) must be <= replication.reconnect_backoff_max_ms ({})",
                initial, max
            ));
        }

        ValidationResult::Ok
    }
}

/// Validates replication timeout ordering (warning only).
///
/// Rule: `connect_timeout_ms` should typically be < `handshake_timeout_ms`
pub struct ReplicationTimeoutOrderingValidator;

impl ConfigValidator for ReplicationTimeoutOrderingValidator {
    fn name(&self) -> &'static str {
        "replication-timeout-ordering"
    }

    fn validate(&self, config: &Config) -> ValidationResult {
        // Only validate if replication is configured as replica
        if !config.replication.is_replica() {
            return ValidationResult::Ok;
        }

        let connect = config.replication.connect_timeout_ms;
        let handshake = config.replication.handshake_timeout_ms;

        if connect >= handshake {
            return ValidationResult::Warning(format!(
                "replication.connect_timeout_ms ({}) is >= replication.handshake_timeout_ms ({}); \
                 typically connection timeout should be shorter than handshake timeout",
                connect, handshake
            ));
        }

        ValidationResult::Ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vll_timeout_ordering_valid() {
        let mut config = Config::default();
        config.vll.per_shard_lock_timeout_ms = 1000;
        config.vll.lock_acquisition_timeout_ms = 2000;
        config.server.scatter_gather_timeout_ms = 3000;

        let validator = VllTimeoutOrderingValidator;
        assert!(validator.validate(&config).is_ok());
    }

    #[test]
    fn test_vll_timeout_ordering_per_shard_too_large() {
        let mut config = Config::default();
        config.vll.per_shard_lock_timeout_ms = 3000;
        config.vll.lock_acquisition_timeout_ms = 2000;
        config.server.scatter_gather_timeout_ms = 5000;

        let validator = VllTimeoutOrderingValidator;
        assert!(validator.validate(&config).is_error());
    }

    #[test]
    fn test_vll_timeout_ordering_lock_acq_too_large() {
        let mut config = Config::default();
        config.vll.per_shard_lock_timeout_ms = 1000;
        config.vll.lock_acquisition_timeout_ms = 6000;
        config.server.scatter_gather_timeout_ms = 5000;

        let validator = VllTimeoutOrderingValidator;
        assert!(validator.validate(&config).is_error());
    }

    #[test]
    fn test_replication_backoff_ordering_valid() {
        let mut config = Config::default();
        config.replication.reconnect_backoff_initial_ms = 100;
        config.replication.reconnect_backoff_max_ms = 30000;

        let validator = ReplicationBackoffOrderingValidator;
        assert!(validator.validate(&config).is_ok());
    }

    #[test]
    fn test_replication_backoff_ordering_equal() {
        let mut config = Config::default();
        config.replication.reconnect_backoff_initial_ms = 1000;
        config.replication.reconnect_backoff_max_ms = 1000;

        let validator = ReplicationBackoffOrderingValidator;
        assert!(validator.validate(&config).is_ok());
    }

    #[test]
    fn test_replication_backoff_ordering_invalid() {
        let mut config = Config::default();
        config.replication.reconnect_backoff_initial_ms = 50000;
        config.replication.reconnect_backoff_max_ms = 30000;

        let validator = ReplicationBackoffOrderingValidator;
        assert!(validator.validate(&config).is_error());
    }

    #[test]
    fn test_replication_timeout_ordering_standalone() {
        // Should skip validation for standalone
        let config = Config::default();

        let validator = ReplicationTimeoutOrderingValidator;
        assert!(validator.validate(&config).is_ok());
    }

    #[test]
    fn test_replication_timeout_ordering_replica_valid() {
        let mut config = Config::default();
        config.replication.role = "replica".to_string();
        config.replication.primary_host = "127.0.0.1".to_string();
        config.replication.connect_timeout_ms = 5000;
        config.replication.handshake_timeout_ms = 10000;

        let validator = ReplicationTimeoutOrderingValidator;
        assert!(validator.validate(&config).is_ok());
    }

    #[test]
    fn test_replication_timeout_ordering_replica_warning() {
        let mut config = Config::default();
        config.replication.role = "replica".to_string();
        config.replication.primary_host = "127.0.0.1".to_string();
        config.replication.connect_timeout_ms = 15000;
        config.replication.handshake_timeout_ms = 10000;

        let validator = ReplicationTimeoutOrderingValidator;
        assert!(validator.validate(&config).is_warning());
    }
}
