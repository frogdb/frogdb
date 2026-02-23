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
