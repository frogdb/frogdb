//! OTLP (OpenTelemetry Protocol) metrics export.

use crate::config::MetricsConfig;
use opentelemetry::metrics::{Counter, Gauge, Histogram, MeterProvider};
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::{
    metrics::{PeriodicReader, SdkMeterProvider},
    runtime,
};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tracing::{error, info, warn};

/// OTLP metrics recorder that exports metrics via OpenTelemetry.
pub struct OtlpRecorder {
    meter_provider: SdkMeterProvider,
    counters: RwLock<HashMap<String, Counter<u64>>>,
    gauges: RwLock<HashMap<String, Gauge<f64>>>,
    histograms: RwLock<HashMap<String, Histogram<f64>>>,
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

        let reader = PeriodicReader::builder(exporter, runtime::Tokio)
            .with_interval(Duration::from_secs(config.otlp_interval_secs))
            .build();

        let meter_provider = SdkMeterProvider::builder().with_reader(reader).build();

        Some(Self {
            meter_provider,
            counters: RwLock::new(HashMap::new()),
            gauges: RwLock::new(HashMap::new()),
            histograms: RwLock::new(HashMap::new()),
        })
    }

    /// Get or create a counter.
    fn get_or_create_counter(&self, name: &str) -> Counter<u64> {
        {
            let counters = self.counters.read().unwrap();
            if let Some(counter) = counters.get(name) {
                return counter.clone();
            }
        }

        let meter = self.meter_provider.meter("frogdb");
        let counter = meter.u64_counter(name.to_string()).build();

        let mut counters = self.counters.write().unwrap();
        counters.insert(name.to_string(), counter.clone());
        counter
    }

    /// Get or create a gauge.
    fn get_or_create_gauge(&self, name: &str) -> Gauge<f64> {
        {
            let gauges = self.gauges.read().unwrap();
            if let Some(gauge) = gauges.get(name) {
                return gauge.clone();
            }
        }

        let meter = self.meter_provider.meter("frogdb");
        let gauge = meter.f64_gauge(name.to_string()).build();

        let mut gauges = self.gauges.write().unwrap();
        gauges.insert(name.to_string(), gauge.clone());
        gauge
    }

    /// Get or create a histogram.
    fn get_or_create_histogram(&self, name: &str) -> Histogram<f64> {
        {
            let histograms = self.histograms.read().unwrap();
            if let Some(histogram) = histograms.get(name) {
                return histogram.clone();
            }
        }

        let meter = self.meter_provider.meter("frogdb");
        let histogram = meter.f64_histogram(name.to_string()).build();

        let mut histograms = self.histograms.write().unwrap();
        histograms.insert(name.to_string(), histogram.clone());
        histogram
    }

    /// Increment a counter.
    pub fn increment_counter(&self, name: &str, value: u64, labels: &[(&str, &str)]) {
        let counter = self.get_or_create_counter(name);
        let attributes: Vec<opentelemetry::KeyValue> = labels
            .iter()
            .map(|(k, v)| opentelemetry::KeyValue::new(k.to_string(), v.to_string()))
            .collect();
        counter.add(value, &attributes);
    }

    /// Record a gauge value.
    pub fn record_gauge(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        let gauge = self.get_or_create_gauge(name);
        let attributes: Vec<opentelemetry::KeyValue> = labels
            .iter()
            .map(|(k, v)| opentelemetry::KeyValue::new(k.to_string(), v.to_string()))
            .collect();
        gauge.record(value, &attributes);
    }

    /// Record a histogram observation.
    pub fn record_histogram(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        let histogram = self.get_or_create_histogram(name);
        let attributes: Vec<opentelemetry::KeyValue> = labels
            .iter()
            .map(|(k, v)| opentelemetry::KeyValue::new(k.to_string(), v.to_string()))
            .collect();
        histogram.record(value, &attributes);
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
