//! Network-related configuration validators.

use super::{ConfigValidator, ValidationResult};
use crate::Config;

/// Validates that server and HTTP ports don't conflict.
pub struct PortConflictValidator;

/// Validates that admin port doesn't conflict with server or HTTP ports.
pub struct AdminPortConflictValidator;

impl ConfigValidator for PortConflictValidator {
    fn name(&self) -> &'static str {
        "port-conflict"
    }

    fn validate(&self, config: &Config) -> ValidationResult {
        if !config.http.enabled {
            return ValidationResult::Ok;
        }

        let same_interface = config.server.bind == config.http.bind
            || config.http.bind == "0.0.0.0"
            || config.server.bind == "0.0.0.0";

        if same_interface && config.server.port == config.http.port {
            return ValidationResult::Error(format!(
                "server.port ({}) conflicts with http.port ({}); they cannot be the same when binding to overlapping interfaces",
                config.server.port, config.http.port
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
        if !config.admin.enabled {
            return ValidationResult::Ok;
        }

        let binds_overlap = |bind1: &str, bind2: &str| -> bool {
            bind1 == bind2 || bind1 == "0.0.0.0" || bind2 == "0.0.0.0"
        };

        if binds_overlap(&config.admin.bind, &config.server.bind)
            && config.admin.port == config.server.port
        {
            return ValidationResult::Error(format!(
                "admin.port ({}) conflicts with server.port ({}); they cannot be the same when binding to overlapping interfaces",
                config.admin.port, config.server.port
            ));
        }

        if config.http.enabled
            && binds_overlap(&config.admin.bind, &config.http.bind)
            && config.admin.port == config.http.port
        {
            return ValidationResult::Error(format!(
                "admin.port ({}) conflicts with http.port ({}); they cannot be the same when binding to overlapping interfaces",
                config.admin.port, config.http.port
            ));
        }

        ValidationResult::Ok
    }
}

/// Validates that the TLS port doesn't conflict with other ports.
pub struct TlsPortConflictValidator;

impl ConfigValidator for TlsPortConflictValidator {
    fn name(&self) -> &'static str {
        "tls-port-conflict"
    }

    fn validate(&self, config: &Config) -> ValidationResult {
        if !config.tls.enabled {
            return ValidationResult::Ok;
        }

        let binds_overlap = |bind1: &str, bind2: &str| -> bool {
            bind1 == bind2 || bind1 == "0.0.0.0" || bind2 == "0.0.0.0"
        };

        // TLS port is always on server.bind
        let tls_bind = &config.server.bind;

        if config.tls.tls_port == config.server.port {
            return ValidationResult::Error(format!(
                "tls.tls_port ({}) conflicts with server.port ({}); they cannot be the same",
                config.tls.tls_port, config.server.port
            ));
        }

        if config.admin.enabled
            && binds_overlap(tls_bind, &config.admin.bind)
            && config.tls.tls_port == config.admin.port
        {
            return ValidationResult::Error(format!(
                "tls.tls_port ({}) conflicts with admin.port ({}); they cannot be the same when binding to overlapping interfaces",
                config.tls.tls_port, config.admin.port
            ));
        }

        if config.admin.enabled {
            let admin_http_bind = config.admin.http_bind.as_deref().unwrap_or(&config.admin.bind);
            if binds_overlap(tls_bind, admin_http_bind)
                && config.tls.tls_port == config.admin.http_port
            {
                return ValidationResult::Error(format!(
                    "tls.tls_port ({}) conflicts with admin.http_port ({}); they cannot be the same when binding to overlapping interfaces",
                    config.tls.tls_port, config.admin.http_port
                ));
            }
        }

        if config.metrics.enabled
            && binds_overlap(tls_bind, &config.metrics.bind)
            && config.tls.tls_port == config.metrics.port
        {
            return ValidationResult::Error(format!(
                "tls.tls_port ({}) conflicts with metrics.port ({}); they cannot be the same when binding to overlapping interfaces",
                config.tls.tls_port, config.metrics.port
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
        config.http.port = 9090;
        config.http.enabled = true;

        let validator = PortConflictValidator;
        assert!(validator.validate(&config).is_ok());
    }

    #[test]
    fn test_port_conflict_same_port_same_interface() {
        let mut config = Config::default();
        config.server.bind = "127.0.0.1".to_string();
        config.server.port = 6379;
        config.http.bind = "127.0.0.1".to_string();
        config.http.port = 6379;
        config.http.enabled = true;

        let validator = PortConflictValidator;
        assert!(validator.validate(&config).is_error());
    }

    #[test]
    fn test_admin_port_conflict_disabled() {
        let mut config = Config::default();
        config.admin.enabled = false;
        config.admin.port = 6379;

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
    fn test_tls_port_conflicts_with_server() {
        let mut config = Config::default();
        config.tls.enabled = true;
        config.tls.tls_port = 6379;
        config.server.port = 6379;

        let validator = TlsPortConflictValidator;
        assert!(validator.validate(&config).is_error());
    }

    #[test]
    fn test_tls_port_conflicts_with_admin() {
        let mut config = Config::default();
        config.tls.enabled = true;
        config.tls.tls_port = 6382;
        config.admin.enabled = true;
        config.admin.bind = "127.0.0.1".to_string();
        config.admin.port = 6382;
        config.server.bind = "127.0.0.1".to_string();

        let validator = TlsPortConflictValidator;
        assert!(validator.validate(&config).is_error());
    }

    #[test]
    fn test_tls_port_no_conflict_different_ports() {
        let mut config = Config::default();
        config.tls.enabled = true;
        config.tls.tls_port = 6380;
        config.server.port = 6379;

        let validator = TlsPortConflictValidator;
        assert!(validator.validate(&config).is_ok());
    }

    #[test]
    fn test_tls_disabled_no_conflict_check() {
        let mut config = Config::default();
        config.tls.enabled = false;
        config.tls.tls_port = 6379;
        config.server.port = 6379;

        let validator = TlsPortConflictValidator;
        assert!(validator.validate(&config).is_ok());
    }
}
