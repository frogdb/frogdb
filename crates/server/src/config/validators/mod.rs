//! Configuration validation framework.
//!
//! This module provides cross-field validation for configuration values,
//! catching semantic errors that cannot be caught by serde alone.

pub mod memory;
pub mod network;
pub mod persistence;
pub mod timeouts;

use crate::config::Config;

/// Result of a single validation check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationResult {
    /// Validation passed.
    Ok,
    /// Validation failed with a blocking error (prevents startup).
    Error(String),
    /// Validation passed with a warning (logged but allows startup).
    Warning(String),
    /// Validation passed with an informational message.
    Info(String),
}

impl ValidationResult {
    /// Returns true if this is an error.
    pub fn is_error(&self) -> bool {
        matches!(self, ValidationResult::Error(_))
    }

    /// Returns true if this is a warning.
    pub fn is_warning(&self) -> bool {
        matches!(self, ValidationResult::Warning(_))
    }

    /// Returns true if this is info.
    pub fn is_info(&self) -> bool {
        matches!(self, ValidationResult::Info(_))
    }

    /// Returns true if this is Ok.
    pub fn is_ok(&self) -> bool {
        matches!(self, ValidationResult::Ok)
    }
}

/// Trait for configuration validators.
///
/// Each validator checks a specific cross-field constraint and returns
/// a ValidationResult indicating success, error, warning, or info.
pub trait ConfigValidator: Send + Sync {
    /// A short name for this validator (used in error messages).
    fn name(&self) -> &'static str;

    /// Validate the configuration.
    fn validate(&self, config: &Config) -> ValidationResult;
}

/// Aggregates validation results from multiple validators.
#[derive(Debug, Default)]
pub struct ValidationReport {
    /// Blocking errors that prevent startup.
    pub errors: Vec<String>,
    /// Warnings that are logged but don't prevent startup.
    pub warnings: Vec<String>,
    /// Informational messages.
    pub infos: Vec<String>,
}

impl ValidationReport {
    /// Create a new empty report.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a validation result to the report.
    pub fn add(&mut self, validator_name: &str, result: ValidationResult) {
        match result {
            ValidationResult::Ok => {}
            ValidationResult::Error(msg) => {
                self.errors.push(format!("[{}] {}", validator_name, msg));
            }
            ValidationResult::Warning(msg) => {
                self.warnings.push(format!("[{}] {}", validator_name, msg));
            }
            ValidationResult::Info(msg) => {
                self.infos.push(format!("[{}] {}", validator_name, msg));
            }
        }
    }

    /// Returns true if there are any blocking errors.
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// Returns true if there are any warnings.
    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }

    /// Log all warnings and infos using tracing.
    pub fn log_non_errors(&self) {
        for warning in &self.warnings {
            tracing::warn!("Config validation warning: {}", warning);
        }
        for info in &self.infos {
            tracing::info!("Config validation info: {}", info);
        }
    }

    /// Convert errors to an anyhow error if any exist.
    pub fn into_result(self) -> anyhow::Result<()> {
        if self.has_errors() {
            anyhow::bail!(
                "Configuration validation failed:\n  - {}",
                self.errors.join("\n  - ")
            );
        }
        Ok(())
    }
}

/// Run all registered validators against the configuration.
pub fn run_all_validators(config: &Config) -> ValidationReport {
    let validators: Vec<Box<dyn ConfigValidator>> = vec![
        // Timeout validators
        Box::new(timeouts::VllTimeoutOrderingValidator),
        Box::new(timeouts::ReplicationBackoffOrderingValidator),
        Box::new(timeouts::ReplicationTimeoutOrderingValidator),
        // Network validators
        Box::new(network::PortConflictValidator),
        Box::new(network::AdminPortConflictValidator),
        // Persistence validators
        Box::new(persistence::DirectorySeparationValidator),
        Box::new(persistence::SyncIntervalIgnoredValidator),
        // Memory validators
        Box::new(memory::EvictionPolicyWithoutLimitValidator),
        Box::new(memory::ShardCountVsCpusValidator),
    ];

    let mut report = ValidationReport::new();

    for validator in &validators {
        let result = validator.validate(config);
        report.add(validator.name(), result);
    }

    report
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_result_methods() {
        assert!(ValidationResult::Ok.is_ok());
        assert!(!ValidationResult::Ok.is_error());
        assert!(!ValidationResult::Ok.is_warning());
        assert!(!ValidationResult::Ok.is_info());

        assert!(ValidationResult::Error("test".to_string()).is_error());
        assert!(ValidationResult::Warning("test".to_string()).is_warning());
        assert!(ValidationResult::Info("test".to_string()).is_info());
    }

    #[test]
    fn test_validation_report_add() {
        let mut report = ValidationReport::new();
        assert!(!report.has_errors());
        assert!(!report.has_warnings());

        report.add("test", ValidationResult::Ok);
        assert!(!report.has_errors());

        report.add("test", ValidationResult::Warning("warn".to_string()));
        assert!(report.has_warnings());
        assert!(!report.has_errors());

        report.add("test", ValidationResult::Error("err".to_string()));
        assert!(report.has_errors());
    }

    #[test]
    fn test_validation_report_into_result() {
        let report = ValidationReport::new();
        assert!(report.into_result().is_ok());

        let mut report = ValidationReport::new();
        report.add("test", ValidationResult::Error("failed".to_string()));
        assert!(report.into_result().is_err());
    }

    #[test]
    fn test_default_config_passes_all_validators() {
        let config = Config::default();
        let report = run_all_validators(&config);
        assert!(
            !report.has_errors(),
            "Default config should pass all validators: {:?}",
            report.errors
        );
    }

    #[test]
    fn test_run_all_validators_collects_warnings() {
        let mut config = Config::default();
        // Set eviction policy without memory limit - should produce warning
        config.memory.maxmemory = 0;
        config.memory.maxmemory_policy = "allkeys-lru".to_string();

        let report = run_all_validators(&config);
        assert!(!report.has_errors());
        assert!(report.has_warnings());
    }
}
