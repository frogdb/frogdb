//! Network-related configuration validators.

use super::{ConfigValidator, ValidationResult};
use crate::config::Config;

/// Validates that server and metrics ports don't conflict.
///
/// Rule: `server.port` must not equal `metrics.port` when metrics are enabled.
pub struct PortConflictValidator;

/// Validates that admin port doesn't conflict with server or metrics ports.
///
/// Rule: When admin is enabled, `admin.port` must not equal `server.port` or `metrics.port`
/// when they bind to overlapping interfaces.
pub struct AdminPortConflictValidator;

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

impl ConfigValidator for AdminPortConflictValidator {
    fn name(&self) -> &'static str {
        "admin-port-conflict"
    }

    fn validate(&self, config: &Config) -> ValidationResult {
        // Only check if admin port is enabled
        if !config.admin.enabled {
            return ValidationResult::Ok;
        }

        // Helper to check if two binds overlap
        let binds_overlap = |bind1: &str, bind2: &str| -> bool {
            bind1 == bind2 || bind1 == "0.0.0.0" || bind2 == "0.0.0.0"
        };

        // Check conflict with server port
        if binds_overlap(&config.admin.bind, &config.server.bind)
            && config.admin.port == config.server.port
        {
            return ValidationResult::Error(format!(
                "admin.port ({}) conflicts with server.port ({}); they cannot be the same when binding to overlapping interfaces",
                config.admin.port, config.server.port
            ));
        }

        // Check conflict with metrics port (only if metrics enabled)
        if config.metrics.enabled
            && binds_overlap(&config.admin.bind, &config.metrics.bind)
            && config.admin.port == config.metrics.port
        {
            return ValidationResult::Error(format!(
                "admin.port ({}) conflicts with metrics.port ({}); they cannot be the same when binding to overlapping interfaces",
                config.admin.port, config.metrics.port
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

    // ===== AdminPortConflictValidator Tests =====

    #[test]
    fn test_admin_port_conflict_disabled() {
        let mut config = Config::default();
        config.admin.enabled = false;
        config.admin.port = 6379; // Same as server port, but disabled

        let validator = AdminPortConflictValidator;
        assert!(validator.validate(&config).is_ok());
    }

    #[test]
    fn test_admin_port_conflict_with_server() {
        let mut config = Config::default();
        config.admin.enabled = true;
        config.admin.bind = "127.0.0.1".to_string();
        config.admin.port = 6379;
        config.server.bind = "127.0.0.1".to_string();
        config.server.port = 6379;

        let validator = AdminPortConflictValidator;
        assert!(validator.validate(&config).is_error());
    }

    #[test]
    fn test_admin_port_conflict_with_metrics() {
        let mut config = Config::default();
        config.admin.enabled = true;
        config.admin.bind = "0.0.0.0".to_string();
        config.admin.port = 9090;
        config.metrics.enabled = true;
        config.metrics.bind = "0.0.0.0".to_string();
        config.metrics.port = 9090;

        let validator = AdminPortConflictValidator;
        assert!(validator.validate(&config).is_error());
    }

    #[test]
    fn test_admin_port_no_conflict() {
        let mut config = Config::default();
        config.admin.enabled = true;
        config.admin.bind = "127.0.0.1".to_string();
        config.admin.port = 6380;
        config.server.bind = "127.0.0.1".to_string();
        config.server.port = 6379;
        config.metrics.enabled = true;
        config.metrics.bind = "127.0.0.1".to_string();
        config.metrics.port = 9090;

        let validator = AdminPortConflictValidator;
        assert!(validator.validate(&config).is_ok());
    }

    #[test]
    fn test_admin_port_different_interfaces() {
        let mut config = Config::default();
        config.admin.enabled = true;
        config.admin.bind = "192.168.1.1".to_string();
        config.admin.port = 6379;
        config.server.bind = "127.0.0.1".to_string();
        config.server.port = 6379;

        let validator = AdminPortConflictValidator;
        assert!(validator.validate(&config).is_ok());
    }
}
