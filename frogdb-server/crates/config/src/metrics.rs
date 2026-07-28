//! Metrics configuration.

use anyhow::Result;
use frogdb_config_derive::ConfigParams;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Metrics configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, ConfigParams)]
#[params(section = "metrics")]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct MetricsConfig {
    /// Whether metrics are enabled.
    #[serde(default = "default_metrics_enabled")]
    #[param(name = "metrics-enabled")]
    pub enabled: bool,

    /// Port for the metrics HTTP server.
    #[serde(default = "default_metrics_port")]
    #[param(name = "metrics-port")]
    pub port: u16,

    /// Whether OTLP export is enabled.
    #[serde(default)]
    // issue-14: consumed at startup (OTLP recorder construction); immutable.
    #[param(name = "metrics-otlp-enabled")]
    pub otlp_enabled: bool,

    /// OTLP endpoint URL.
    #[serde(default = "default_otlp_endpoint")]
    // issue-14: consumed at startup (OTLP recorder construction); immutable.
    #[param(name = "metrics-otlp-endpoint")]
    pub otlp_endpoint: String,

    /// OTLP push interval in seconds.
    #[serde(default = "default_otlp_interval_secs")]
    // issue-14: consumed at startup (OTLP recorder construction); immutable.
    #[param(name = "metrics-otlp-interval-secs")]
    pub otlp_interval_secs: u64,
}

fn default_metrics_enabled() -> bool {
    true
}

pub const DEFAULT_METRICS_PORT: u16 = 9090;
pub const DEFAULT_OTLP_INTERVAL_SECS: u64 = 15;

fn default_metrics_port() -> u16 {
    DEFAULT_METRICS_PORT
}

fn default_otlp_endpoint() -> String {
    "http://localhost:4317".to_string()
}

fn default_otlp_interval_secs() -> u64 {
    DEFAULT_OTLP_INTERVAL_SECS
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: default_metrics_enabled(),
            port: default_metrics_port(),
            otlp_enabled: false,
            otlp_endpoint: default_otlp_endpoint(),
            otlp_interval_secs: default_otlp_interval_secs(),
        }
    }
}

impl MetricsConfig {
    /// Validate the configuration.
    pub fn validate(&self) -> Result<()> {
        if self.port == 0 {
            anyhow::bail!("metrics port cannot be 0");
        }

        if self.otlp_enabled && self.otlp_endpoint.is_empty() {
            anyhow::bail!("OTLP endpoint must be specified when OTLP is enabled");
        }

        if self.otlp_interval_secs == 0 {
            anyhow::bail!("OTLP interval must be > 0");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_metrics_config() {
        let config = MetricsConfig::default();
        assert!(config.enabled);
        assert_eq!(config.port, DEFAULT_METRICS_PORT);
        assert!(!config.otlp_enabled);
        assert_eq!(config.otlp_endpoint, "http://localhost:4317");
        assert_eq!(config.otlp_interval_secs, DEFAULT_OTLP_INTERVAL_SECS);
    }
}
