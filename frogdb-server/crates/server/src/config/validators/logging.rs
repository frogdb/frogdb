//! Logging-related configuration validators.

use super::{ConfigValidator, ValidationResult};
use crate::config::Config;

/// Warns when file_path is set without rotation, meaning the log file will grow unbounded.
pub struct FileWithoutRotationValidator;

impl ConfigValidator for FileWithoutRotationValidator {
    fn name(&self) -> &'static str {
        "file-without-rotation"
    }

    fn validate(&self, config: &Config) -> ValidationResult {
        if config.logging.file_path.is_some() && config.logging.rotation.is_none() {
            return ValidationResult::Warning(
                "logging.file_path is set without logging.rotation; \
                 the log file will grow unbounded"
                    .to_string(),
            );
        }
        ValidationResult::Ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_file_without_rotation_warns() {
        let mut config = Config::default();
        config.logging.file_path = Some(PathBuf::from("/tmp/test.log"));
        config.logging.rotation = None;

        let validator = FileWithoutRotationValidator;
        assert!(validator.validate(&config).is_warning());
    }

    #[test]
    fn test_file_with_rotation_ok() {
        let mut config = Config::default();
        config.logging.file_path = Some(PathBuf::from("/tmp/test.log"));
        config.logging.rotation = Some(crate::config::RotationConfig::default());

        let validator = FileWithoutRotationValidator;
        assert!(validator.validate(&config).is_ok());
    }

    #[test]
    fn test_no_file_ok() {
        let config = Config::default();

        let validator = FileWithoutRotationValidator;
        assert!(validator.validate(&config).is_ok());
    }
}
