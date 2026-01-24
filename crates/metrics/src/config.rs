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

/// Distributed tracing configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TracingConfig {
    /// Whether distributed tracing is enabled.
    #[serde(default)]
    pub enabled: bool,

    /// OTLP endpoint for trace export.
    #[serde(default = "default_tracing_endpoint")]
    pub otlp_endpoint: String,

    /// Sampling rate (0.0 to 1.0). 1.0 = sample all, 0.1 = sample 10%.
    #[serde(default = "default_sampling_rate")]
    pub sampling_rate: f64,

    /// Service name in traces.
    #[serde(default = "default_service_name")]
    pub service_name: String,

    /// Enable scatter-gather operation spans (child spans per shard for MGET/MSET).
    #[serde(default)]
    pub scatter_gather_spans: bool,

    /// Enable shard execution spans (spans inside shard workers).
    #[serde(default)]
    pub shard_spans: bool,

    /// Enable persistence spans (WAL writes, snapshots).
    #[serde(default)]
    pub persistence_spans: bool,
}

fn default_tracing_endpoint() -> String {
    "http://localhost:4317".to_string()
}

fn default_sampling_rate() -> f64 {
    1.0
}

fn default_service_name() -> String {
    "frogdb".to_string()
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            otlp_endpoint: default_tracing_endpoint(),
            sampling_rate: default_sampling_rate(),
            service_name: default_service_name(),
            scatter_gather_spans: false,
            shard_spans: false,
            persistence_spans: false,
        }
    }
}

impl TracingConfig {
    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.enabled && self.otlp_endpoint.is_empty() {
            return Err("OTLP endpoint must be specified when tracing is enabled".to_string());
        }

        if self.sampling_rate < 0.0 || self.sampling_rate > 1.0 {
            return Err("sampling_rate must be between 0.0 and 1.0".to_string());
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

    // ===== TracingConfig Tests =====

    #[test]
    fn test_tracing_default_config() {
        let config = TracingConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.otlp_endpoint, "http://localhost:4317");
        assert_eq!(config.sampling_rate, 1.0);
        assert_eq!(config.service_name, "frogdb");
        assert!(!config.scatter_gather_spans);
        assert!(!config.shard_spans);
        assert!(!config.persistence_spans);
    }

    #[test]
    fn test_tracing_validate_valid() {
        let config = TracingConfig {
            enabled: true,
            otlp_endpoint: "http://localhost:4317".to_string(),
            sampling_rate: 0.5,
            service_name: "test".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_tracing_validate_enabled_no_endpoint() {
        let config = TracingConfig {
            enabled: true,
            otlp_endpoint: String::new(),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_tracing_validate_invalid_sampling_rate_high() {
        let config = TracingConfig {
            sampling_rate: 1.5,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_tracing_validate_invalid_sampling_rate_low() {
        let config = TracingConfig {
            sampling_rate: -0.1,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_tracing_validate_disabled_no_endpoint_ok() {
        // When disabled, empty endpoint is OK
        let config = TracingConfig {
            enabled: false,
            otlp_endpoint: String::new(),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }
}
