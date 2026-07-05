//! OTLP (OpenTelemetry Protocol) metrics export.

use crate::config::MetricsConfig;
use crate::interned_registry::InternedRegistry;
use opentelemetry::metrics::{Counter, Gauge, Histogram, MeterProvider};
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

/// Convert `(key, value)` label pairs into OTLP `KeyValue` attributes.
fn labels_to_attributes(labels: &[(&str, &str)]) -> Vec<opentelemetry::KeyValue> {
    labels
        .iter()
        .map(|(k, v)| opentelemetry::KeyValue::new(k.to_string(), v.to_string()))
        .collect()
}

/// OTLP metrics recorder that exports metrics via OpenTelemetry.
pub struct OtlpRecorder {
    meter_provider: SdkMeterProvider,
    counters: InternedRegistry<Counter<u64>>,
    gauges: InternedRegistry<Gauge<f64>>,
    histograms: InternedRegistry<Histogram<f64>>,
}

impl OtlpRecorder {
    /// Create a new OTLP recorder.
    ///
    /// Returns None if OTLP is not enabled or initialization fails.
    pub fn new(config: &MetricsConfig) -> Option<Self> {
        if !config.otlp_enabled {
            return None;
        }

        info!(
            endpoint = %config.otlp_endpoint,
            interval_secs = config.otlp_interval_secs,
            "Initializing OTLP metrics export"
        );

        let exporter = match MetricExporter::builder()
            .with_tonic()
            .with_endpoint(&config.otlp_endpoint)
            .build()
        {
            Ok(e) => e,
            Err(e) => {
                error!(error = %e, "Failed to create OTLP exporter");
                return None;
            }
        };

        let reader = PeriodicReader::builder(exporter)
            .with_interval(Duration::from_secs(config.otlp_interval_secs))
            .build();

        let meter_provider = SdkMeterProvider::builder().with_reader(reader).build();

        Some(Self {
            meter_provider,
            counters: InternedRegistry::new(),
            gauges: InternedRegistry::new(),
            histograms: InternedRegistry::new(),
        })
    }

    /// Increment a counter.
    pub fn increment_counter(&self, name: &str, value: u64, labels: &[(&str, &str)]) {
        let counter = self.counters.get_or_create(name, || {
            self.meter_provider
                .meter("frogdb")
                .u64_counter(name.to_string())
                .build()
        });
        counter.add(value, &labels_to_attributes(labels));
    }

    /// Record a gauge value.
    pub fn record_gauge(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        let gauge = self.gauges.get_or_create(name, || {
            self.meter_provider
                .meter("frogdb")
                .f64_gauge(name.to_string())
                .build()
        });
        gauge.record(value, &labels_to_attributes(labels));
    }

    /// Record a histogram observation.
    pub fn record_histogram(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        let histogram = self.histograms.get_or_create(name, || {
            self.meter_provider
                .meter("frogdb")
                .f64_histogram(name.to_string())
                .build()
        });
        histogram.record(value, &labels_to_attributes(labels));
    }

    /// Shutdown the OTLP recorder, flushing any pending metrics.
    pub fn shutdown(&self) {
        if let Err(e) = self.meter_provider.shutdown() {
            warn!(error = %e, "Error shutting down OTLP meter provider");
        }
    }
}

impl Drop for OtlpRecorder {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Composite recorder that writes to multiple backends.
pub struct CompositeRecorder {
    recorders: Vec<Arc<dyn frogdb_core::MetricsRecorder>>,
}

impl CompositeRecorder {
    /// Create a new composite recorder.
    pub fn new(recorders: Vec<Arc<dyn frogdb_core::MetricsRecorder>>) -> Self {
        Self { recorders }
    }
}

impl frogdb_core::MetricsRecorder for CompositeRecorder {
    fn increment_counter(&self, name: &str, value: u64, labels: &[(&str, &str)]) {
        for recorder in &self.recorders {
            recorder.increment_counter(name, value, labels);
        }
    }

    fn record_gauge(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        for recorder in &self.recorders {
            recorder.record_gauge(name, value, labels);
        }
    }

    fn record_histogram(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        for recorder in &self.recorders {
            recorder.record_histogram(name, value, labels);
        }
    }

    fn counter_value(&self, name: &str) -> Option<u64> {
        // Return the first backend that can read this counter back (e.g. the
        // Prometheus recorder); OTLP/no-op backends return None.
        self.recorders
            .iter()
            .find_map(|recorder| recorder.counter_value(name))
    }

    fn record_command_latency_ms(&self, latency_ms: u64) {
        for recorder in &self.recorders {
            recorder.record_command_latency_ms(latency_ms);
        }
    }

    fn latency_bands_enabled(&self) -> bool {
        self.recorders.iter().any(|r| r.latency_bands_enabled())
    }

    fn latency_band_total(&self) -> u64 {
        self.recorders
            .iter()
            .find(|r| r.latency_bands_enabled())
            .map_or(0, |r| r.latency_band_total())
    }

    fn latency_band_percentages(&self) -> Vec<(String, u64, f64)> {
        self.recorders
            .iter()
            .find(|r| r.latency_bands_enabled())
            .map_or_else(Vec::new, |r| r.latency_band_percentages())
    }

    fn reset_latency_bands(&self) {
        for recorder in &self.recorders {
            recorder.reset_latency_bands();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_core::MetricsRecorder;

    #[test]
    fn test_otlp_disabled_returns_none() {
        let config = MetricsConfig {
            otlp_enabled: false,
            ..Default::default()
        };
        assert!(OtlpRecorder::new(&config).is_none());
    }

    #[test]
    fn test_composite_recorder() {
        use frogdb_core::NoopMetricsRecorder;

        let recorder1 = Arc::new(NoopMetricsRecorder::new()) as Arc<dyn MetricsRecorder>;
        let recorder2 = Arc::new(NoopMetricsRecorder::new()) as Arc<dyn MetricsRecorder>;

        let composite = CompositeRecorder::new(vec![recorder1, recorder2]);

        // Should not panic
        composite.increment_counter("test", 1, &[]);
        composite.record_gauge("test", 1.0, &[]);
        composite.record_histogram("test", 0.1, &[]);
    }

    #[test]
    fn test_composite_recorder_with_prometheus() {
        use crate::prometheus_recorder::PrometheusRecorder;

        // Create the prometheus recorder and keep a reference to it
        let prom = Arc::new(PrometheusRecorder::new());
        let prom_trait = prom.clone() as Arc<dyn MetricsRecorder>;
        let noop = Arc::new(frogdb_core::NoopMetricsRecorder::new()) as Arc<dyn MetricsRecorder>;

        let composite = CompositeRecorder::new(vec![prom_trait, noop]);

        // Record through composite
        composite.increment_counter("test_counter", 5, &[("label", "value")]);
        composite.record_gauge("test_gauge", 42.0, &[]);
        composite.record_histogram("test_histogram", 0.123, &[]);

        // Verify prometheus backend received the metrics using our original Arc
        let output = prom.encode();
        assert!(output.contains("test_counter"));
        assert!(output.contains("test_gauge"));
        assert!(output.contains("test_histogram"));
    }

    #[test]
    fn test_composite_recorder_empty() {
        let composite = CompositeRecorder::new(vec![]);

        // Should not panic with no backends
        composite.increment_counter("test", 1, &[]);
        composite.record_gauge("test", 1.0, &[]);
        composite.record_histogram("test", 0.1, &[]);
    }

    #[test]
    fn test_composite_recorder_with_labels() {
        use frogdb_core::NoopMetricsRecorder;

        let noop = Arc::new(NoopMetricsRecorder::new()) as Arc<dyn MetricsRecorder>;
        let composite = CompositeRecorder::new(vec![noop]);

        // Should handle various label configurations
        composite.increment_counter("counter", 1, &[("a", "1"), ("b", "2")]);
        composite.record_gauge("gauge", 1.0, &[("cmd", "GET")]);
        composite.record_histogram("histogram", 0.1, &[("shard", "0"), ("op", "read")]);
    }
}
