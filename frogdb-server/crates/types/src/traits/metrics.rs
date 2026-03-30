//! Metrics recording traits.
//!
//! These traits define the interface for recording metrics (counters, gauges,
//! histograms) in an OpenTelemetry-compatible way.

/// Metrics recorder trait (OpenTelemetry-ready).
///
/// Implementations record counters, gauges, and histograms to a metrics
/// backend like Prometheus or OTLP.
pub trait MetricsRecorder: Send + Sync {
    /// Increment a counter.
    ///
    /// Counters are monotonically increasing values (e.g., total requests).
    fn increment_counter(&self, name: &str, value: u64, labels: &[(&str, &str)]);

    /// Record a gauge value.
    ///
    /// Gauges represent current values that can go up or down
    /// (e.g., current connections).
    fn record_gauge(&self, name: &str, value: f64, labels: &[(&str, &str)]);

    /// Record a histogram observation.
    ///
    /// Histograms track distributions of values (e.g., latencies).
    fn record_histogram(&self, name: &str, value: f64, labels: &[(&str, &str)]);

    /// Record a command latency for SLO band tracking.
    ///
    /// Implementations with latency band support will bucket this into
    /// configured SLO bands. Default is a no-op.
    fn record_command_latency_ms(&self, _latency_ms: u64) {}

    /// Whether latency band tracking is enabled.
    fn latency_bands_enabled(&self) -> bool {
        false
    }

    /// Total number of requests tracked by latency bands.
    fn latency_band_total(&self) -> u64 {
        0
    }

    /// Per-band percentages: (label, count, percentage).
    fn latency_band_percentages(&self) -> Vec<(String, u64, f64)> {
        vec![]
    }

    /// Reset all latency band counters.
    fn reset_latency_bands(&self) {}
}

/// Noop metrics recorder.
///
/// Use this when metrics are disabled or for testing.
#[derive(Debug, Default)]
pub struct NoopMetricsRecorder;

impl NoopMetricsRecorder {
    /// Create a new noop metrics recorder.
    pub fn new() -> Self {
        Self
    }
}

impl MetricsRecorder for NoopMetricsRecorder {
    fn increment_counter(&self, name: &str, value: u64, _labels: &[(&str, &str)]) {
        tracing::trace!(name, value, "Noop counter increment");
    }

    fn record_gauge(&self, name: &str, value: f64, _labels: &[(&str, &str)]) {
        tracing::trace!(name, value, "Noop gauge record");
    }

    fn record_histogram(&self, name: &str, value: f64, _labels: &[(&str, &str)]) {
        tracing::trace!(name, value, "Noop histogram record");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_noop_metrics_recorder() {
        let recorder = NoopMetricsRecorder::new();

        // These should not panic
        recorder.increment_counter("requests_total", 1, &[("method", "GET")]);
        recorder.record_gauge("connections_current", 42.0, &[]);
        recorder.record_histogram("request_latency_ms", 15.5, &[("path", "/api")]);
    }
}
