//! Distributed tracing configuration.

use anyhow::Result;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Distributed tracing configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
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

pub const DEFAULT_RECENT_TRACES_MAX: usize = 100;

fn default_recent_traces_max() -> usize {
    DEFAULT_RECENT_TRACES_MAX
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
}
