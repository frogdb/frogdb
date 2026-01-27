//! Distributed tracing configuration.

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Distributed tracing configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
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

    /// Maximum number of recent traces to retain for DEBUG TRACING RECENT.
    #[serde(default = "default_recent_traces_max")]
    pub recent_traces_max: usize,
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

fn default_recent_traces_max() -> usize {
    100
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
            recent_traces_max: default_recent_traces_max(),
        }
    }
}

impl TracingConfig {
    /// Validate the tracing configuration.
    pub fn validate(&self) -> Result<()> {
        if self.enabled && self.otlp_endpoint.is_empty() {
            anyhow::bail!("OTLP endpoint must be specified when tracing is enabled");
        }

        if self.sampling_rate < 0.0 || self.sampling_rate > 1.0 {
            anyhow::bail!("sampling_rate must be between 0.0 and 1.0");
        }

        Ok(())
    }

    /// Convert to frogdb_metrics::TracingConfig.
    pub fn to_metrics_config(&self) -> frogdb_metrics::TracingConfig {
        frogdb_metrics::TracingConfig {
            enabled: self.enabled,
            otlp_endpoint: self.otlp_endpoint.clone(),
            sampling_rate: self.sampling_rate,
            service_name: self.service_name.clone(),
            scatter_gather_spans: self.scatter_gather_spans,
            shard_spans: self.shard_spans,
            persistence_spans: self.persistence_spans,
            recent_traces_max: self.recent_traces_max,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tracing_config_defaults() {
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
    fn test_tracing_config_to_metrics_config() {
        let config = TracingConfig {
            enabled: true,
            otlp_endpoint: "http://example.com:4317".to_string(),
            sampling_rate: 0.1,
            service_name: "test-service".to_string(),
            scatter_gather_spans: true,
            shard_spans: false,
            persistence_spans: true,
            recent_traces_max: 50,
        };

        let metrics_config = config.to_metrics_config();
        assert!(metrics_config.enabled);
        assert_eq!(metrics_config.otlp_endpoint, "http://example.com:4317");
        assert_eq!(metrics_config.sampling_rate, 0.1);
        assert_eq!(metrics_config.service_name, "test-service");
        assert!(metrics_config.scatter_gather_spans);
        assert!(!metrics_config.shard_spans);
        assert!(metrics_config.persistence_spans);
        assert_eq!(metrics_config.recent_traces_max, 50);
    }
}
