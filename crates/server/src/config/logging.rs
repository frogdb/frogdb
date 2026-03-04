//! Logging configuration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Logging configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LoggingConfig {
    /// Log level (trace, debug, info, warn, error).
    #[serde(default = "default_log_level")]
    pub level: String,

    /// Log format (pretty, json).
    #[serde(default = "default_log_format")]
    pub format: String,

    /// Enable per-request tracing spans (cmd_read, cmd_execute, cmd_route, etc.).
    /// Disabled by default for production performance (~7% CPU savings).
    /// Enable for debugging or when distributed tracing is needed.
    #[serde(default)]
    pub per_request_spans: bool,
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_log_format() -> String {
    "pretty".to_string()
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
            per_request_spans: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_logging_config_defaults() {
        let config = LoggingConfig::default();
        assert_eq!(config.level, "info");
        assert_eq!(config.format, "pretty");
    }
}
