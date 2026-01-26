//! Network-related configuration validators.

use super::{ConfigValidator, ValidationResult};
use crate::config::Config;

/// Validates that server and metrics ports don't conflict.
///
/// Rule: `server.port` must not equal `metrics.port` when metrics are enabled.
pub struct PortConflictValidator;

impl ConfigValidator for PortConflictValidator {
    fn name(&self) -> &'static str {
        "port-conflict"
    }

    fn validate(&self, config: &Config) -> ValidationResult {
        // Only check if metrics are enabled
        if !config.metrics.enabled {
            return ValidationResult::Ok;
        }

        // Check if both bind to the same interface
        let same_interface = config.server.bind == config.metrics.bind
            || config.metrics.bind == "0.0.0.0"
            || config.server.bind == "0.0.0.0";

        if same_interface && config.server.port == config.metrics.port {
            return ValidationResult::Error(format!(
                "server.port ({}) conflicts with metrics.port ({}); they cannot be the same when binding to overlapping interfaces",
                config.server.port, config.metrics.port
            ));
        }

        ValidationResult::Ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_port_conflict_different_ports() {
        let mut config = Config::default();
        config.server.port = 6379;
        config.metrics.port = 9090;
        config.metrics.enabled = true;

        let validator = PortConflictValidator;
        assert!(validator.validate(&config).is_ok());
    }

    #[test]
    fn test_port_conflict_same_port_same_interface() {
        let mut config = Config::default();
        config.server.bind = "127.0.0.1".to_string();
        config.server.port = 6379;
        config.metrics.bind = "127.0.0.1".to_string();
        config.metrics.port = 6379;
        config.metrics.enabled = true;

        let validator = PortConflictValidator;
        assert!(validator.validate(&config).is_error());
    }

    #[test]
    fn test_port_conflict_same_port_wildcard_metrics() {
        let mut config = Config::default();
        config.server.bind = "127.0.0.1".to_string();
        config.server.port = 6379;
        config.metrics.bind = "0.0.0.0".to_string();
        config.metrics.port = 6379;
        config.metrics.enabled = true;

        let validator = PortConflictValidator;
        assert!(validator.validate(&config).is_error());
    }

    #[test]
    fn test_port_conflict_same_port_wildcard_server() {
        let mut config = Config::default();
        config.server.bind = "0.0.0.0".to_string();
        config.server.port = 6379;
        config.metrics.bind = "192.168.1.1".to_string();
        config.metrics.port = 6379;
        config.metrics.enabled = true;

        let validator = PortConflictValidator;
        assert!(validator.validate(&config).is_error());
    }

    #[test]
    fn test_port_conflict_metrics_disabled() {
        let mut config = Config::default();
        config.server.port = 6379;
        config.metrics.port = 6379;
        config.metrics.enabled = false;

        let validator = PortConflictValidator;
        assert!(validator.validate(&config).is_ok());
    }

    #[test]
    fn test_port_conflict_different_interfaces() {
        let mut config = Config::default();
        config.server.bind = "127.0.0.1".to_string();
        config.server.port = 6379;
        config.metrics.bind = "192.168.1.1".to_string();
        config.metrics.port = 6379;
        config.metrics.enabled = true;

        let validator = PortConflictValidator;
        assert!(validator.validate(&config).is_ok());
    }
}
