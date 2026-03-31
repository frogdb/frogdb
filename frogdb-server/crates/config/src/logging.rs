//! Logging configuration.

use std::path::PathBuf;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Console output destination.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogOutput {
    #[default]
    Stdout,
    Stderr,
    None,
}

/// Time-based rotation frequency.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RotationFrequency {
    #[default]
    Daily,
    Hourly,
    Never,
}

/// Log file rotation settings.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct RotationConfig {
    /// Maximum file size in MB before rotation (0 = no size rotation).
    #[serde(default = "default_max_size_mb")]
    pub max_size_mb: u64,

    /// Time-based rotation frequency.
    #[serde(default)]
    pub frequency: RotationFrequency,

    /// Maximum number of rotated files to retain (0 = unlimited).
    #[serde(default = "default_max_files")]
    pub max_files: u32,
}

impl Default for RotationConfig {
    fn default() -> Self {
        Self {
            max_size_mb: default_max_size_mb(),
            frequency: RotationFrequency::default(),
            max_files: default_max_files(),
        }
    }
}

fn default_max_size_mb() -> u64 {
    100
}

fn default_max_files() -> u32 {
    5
}

/// Logging configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct LoggingConfig {
    /// Log level (trace, debug, info, warn, error).
    #[serde(default = "default_log_level")]
    pub level: String,

    /// Log format (pretty, json).
    #[serde(default = "default_log_format")]
    pub format: String,

    /// Console output destination (stdout, stderr, none).
    #[serde(default)]
    pub output: LogOutput,

    /// Enable per-request tracing spans (cmd_read, cmd_execute, cmd_route, etc.).
    /// Disabled by default for production performance (~7% CPU savings).
    /// Enable for debugging or when distributed tracing is needed.
    #[serde(default)]
    pub per_request_spans: bool,

    /// File path for log output. When set, logs are written to this file
    /// in addition to console output. Use `output = "none"` to disable console.
    #[serde(default)]
    pub file_path: Option<PathBuf>,

    /// Log file rotation settings. Only meaningful when `file_path` is set.
    #[serde(default)]
    pub rotation: Option<RotationConfig>,
}

pub const DEFAULT_LOG_LEVEL: &str = "info";
pub const DEFAULT_LOG_FORMAT: &str = "pretty";

fn default_log_level() -> String {
    DEFAULT_LOG_LEVEL.to_string()
}

fn default_log_format() -> String {
    DEFAULT_LOG_FORMAT.to_string()
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
            output: LogOutput::default(),
            per_request_spans: false,
            file_path: None,
            rotation: None,
        }
    }
}

impl LoggingConfig {
    /// Validate logging-specific configuration.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.rotation.is_some() && self.file_path.is_none() {
            anyhow::bail!(
                "logging.rotation is set but logging.file_path is not; \
                 rotation settings require a file path"
            );
        }

        if let Some(ref rotation) = self.rotation
            && rotation.max_size_mb == 0
            && rotation.frequency == RotationFrequency::Never
        {
            anyhow::bail!(
                "logging.rotation has max_size_mb=0 and frequency=never; \
                 at least one rotation trigger must be configured"
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_logging_config_defaults() {
        let config = LoggingConfig::default();
        assert_eq!(config.level, DEFAULT_LOG_LEVEL);
        assert_eq!(config.format, DEFAULT_LOG_FORMAT);
        assert_eq!(config.output, LogOutput::Stdout);
        assert!(config.file_path.is_none());
        assert!(config.rotation.is_none());
    }

    #[test]
    fn test_rotation_config_defaults() {
        let config = RotationConfig::default();
        assert_eq!(config.max_size_mb, 100);
        assert_eq!(config.frequency, RotationFrequency::Daily);
        assert_eq!(config.max_files, 5);
    }

    #[test]
    fn test_validate_rotation_without_file_path() {
        let config = LoggingConfig {
            rotation: Some(RotationConfig::default()),
            file_path: None,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_rotation_no_trigger() {
        let config = LoggingConfig {
            file_path: Some(PathBuf::from("/tmp/test.log")),
            rotation: Some(RotationConfig {
                max_size_mb: 0,
                frequency: RotationFrequency::Never,
                max_files: 5,
            }),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_valid_rotation() {
        let config = LoggingConfig {
            file_path: Some(PathBuf::from("/tmp/test.log")),
            rotation: Some(RotationConfig::default()),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_file_without_rotation() {
        let config = LoggingConfig {
            file_path: Some(PathBuf::from("/tmp/test.log")),
            rotation: None,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_no_file_no_rotation() {
        let config = LoggingConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_deserialize_output_variants() {
        let toml = r#"output = "stdout""#;
        let config: LoggingConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.output, LogOutput::Stdout);

        let toml = r#"output = "stderr""#;
        let config: LoggingConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.output, LogOutput::Stderr);

        let toml = r#"output = "none""#;
        let config: LoggingConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.output, LogOutput::None);
    }

    #[test]
    fn test_deserialize_rotation_frequency() {
        let toml = r#"frequency = "daily""#;
        let config: RotationConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.frequency, RotationFrequency::Daily);

        let toml = r#"frequency = "hourly""#;
        let config: RotationConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.frequency, RotationFrequency::Hourly);

        let toml = r#"frequency = "never""#;
        let config: RotationConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.frequency, RotationFrequency::Never);
    }

    #[test]
    fn test_deserialize_full_logging_config() {
        let toml = r#"
            level = "debug"
            format = "json"
            output = "stderr"
            per-request-spans = true
            file-path = "/var/log/frogdb/frogdb.log"

            [rotation]
            max-size-mb = 50
            frequency = "hourly"
            max-files = 10
        "#;
        let config: LoggingConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.level, "debug");
        assert_eq!(config.format, "json");
        assert_eq!(config.output, LogOutput::Stderr);
        assert!(config.per_request_spans);
        assert_eq!(
            config.file_path.unwrap(),
            PathBuf::from("/var/log/frogdb/frogdb.log")
        );
        let rotation = config.rotation.unwrap();
        assert_eq!(rotation.max_size_mb, 50);
        assert_eq!(rotation.frequency, RotationFrequency::Hourly);
        assert_eq!(rotation.max_files, 10);
    }
}
