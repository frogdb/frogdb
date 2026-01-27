//! Latency testing and SLO monitoring configuration.

use serde::{Deserialize, Serialize};

/// Latency testing configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LatencyConfig {
    /// Run intrinsic latency test at startup before accepting connections.
    #[serde(default)]
    pub startup_test: bool,

    /// Duration of the startup latency test in seconds.
    #[serde(default = "default_latency_test_duration_secs")]
    pub startup_test_duration_secs: u64,

    /// Warning threshold for intrinsic latency in microseconds.
    /// If max latency exceeds this, a warning is logged.
    #[serde(default = "default_latency_warning_threshold_us")]
    pub warning_threshold_us: u64,
}

fn default_latency_test_duration_secs() -> u64 {
    5
}

fn default_latency_warning_threshold_us() -> u64 {
    2000 // 2ms
}

impl Default for LatencyConfig {
    fn default() -> Self {
        Self {
            startup_test: false,
            startup_test_duration_secs: default_latency_test_duration_secs(),
            warning_threshold_us: default_latency_warning_threshold_us(),
        }
    }
}

/// Latency bands configuration for SLO-focused monitoring.
///
/// Tracks cumulative request counts in configurable latency buckets,
/// enabling direct SLO monitoring without external aggregation.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LatencyBandsConfig {
    /// Whether latency band tracking is enabled.
    #[serde(default)]
    pub enabled: bool,

    /// Latency band thresholds in milliseconds.
    /// Requests are counted in cumulative buckets (<=1ms, <=5ms, etc.)
    #[serde(default = "default_latency_bands")]
    pub bands: Vec<u64>,
}

fn default_latency_bands() -> Vec<u64> {
    vec![1, 5, 10, 50, 100, 500]
}

impl Default for LatencyBandsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bands: default_latency_bands(),
        }
    }
}
