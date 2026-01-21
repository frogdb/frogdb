//! Metrics configuration.

use serde::{Deserialize, Serialize};

/// Metrics configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MetricsConfig {
    /// Whether metrics are enabled.
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Bind address for the metrics HTTP server.
    #[serde(default = "default_bind")]
    pub bind: String,

    /// Port for the metrics HTTP server.
    #[serde(default = "default_port")]
    pub port: u16,

    /// Whether OTLP export is enabled.
    #[serde(default)]
    pub otlp_enabled: bool,

    /// OTLP endpoint URL.
    #[serde(default = "default_otlp_endpoint")]
    pub otlp_endpoint: String,

    /// OTLP push interval in seconds.
    #[serde(default = "default_otlp_interval_secs")]
    pub otlp_interval_secs: u64,
}

fn default_enabled() -> bool {
    true
}

fn default_bind() -> String {
    "0.0.0.0".to_string()
}

fn default_port() -> u16 {
    9090
}

fn default_otlp_endpoint() -> String {
    "http://localhost:4317".to_string()
}

fn default_otlp_interval_secs() -> u64 {
    15
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            bind: default_bind(),
            port: default_port(),
            otlp_enabled: false,
            otlp_endpoint: default_otlp_endpoint(),
            otlp_interval_secs: default_otlp_interval_secs(),
        }
    }
}

impl MetricsConfig {
    /// Get the full bind address.
    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.bind, self.port)
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.port == 0 {
            return Err("metrics port cannot be 0".to_string());
        }

        if self.otlp_enabled && self.otlp_endpoint.is_empty() {
            return Err("OTLP endpoint must be specified when OTLP is enabled".to_string());
        }

        if self.otlp_interval_secs == 0 {
            return Err("OTLP interval must be > 0".to_string());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = MetricsConfig::default();
        assert!(config.enabled);
        assert_eq!(config.bind, "0.0.0.0");
        assert_eq!(config.port, 9090);
        assert!(!config.otlp_enabled);
        assert_eq!(config.otlp_endpoint, "http://localhost:4317");
        assert_eq!(config.otlp_interval_secs, 15);
    }

    #[test]
    fn test_bind_addr() {
        let config = MetricsConfig::default();
        assert_eq!(config.bind_addr(), "0.0.0.0:9090");
    }

    #[test]
    fn test_validate_valid_config() {
        let config = MetricsConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_zero_port() {
        let config = MetricsConfig {
            port: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_otlp_without_endpoint() {
        let config = MetricsConfig {
            otlp_enabled: true,
            otlp_endpoint: String::new(),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }
}
