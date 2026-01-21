//! Prometheus metrics recorder implementation.

use dashmap::DashMap;
use frogdb_core::MetricsRecorder;
use prometheus::{
    Counter, CounterVec, Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, Opts,
    Registry, TextEncoder,
};
/// Labels key type for DashMap lookups.
type LabelsKey = Vec<(String, String)>;

/// Prometheus-based metrics recorder.
///
/// This implementation lazily creates metrics on first use and stores them
/// in thread-safe DashMaps for concurrent access.
pub struct PrometheusRecorder {
    registry: Registry,
    counters: DashMap<String, CounterVec>,
    gauges: DashMap<String, GaugeVec>,
    histograms: DashMap<String, HistogramVec>,
    // Cache for specific label combinations (avoids label vec allocation on hot path)
    counter_cache: DashMap<(String, LabelsKey), Counter>,
    gauge_cache: DashMap<(String, LabelsKey), Gauge>,
    histogram_cache: DashMap<(String, LabelsKey), Histogram>,
}

impl Default for PrometheusRecorder {
    fn default() -> Self {
        Self::new()
    }
}

impl PrometheusRecorder {
    /// Create a new Prometheus recorder with a custom registry.
    pub fn new() -> Self {
        Self {
            registry: Registry::new(),
            counters: DashMap::new(),
            gauges: DashMap::new(),
            histograms: DashMap::new(),
            counter_cache: DashMap::new(),
            gauge_cache: DashMap::new(),
            histogram_cache: DashMap::new(),
        }
    }

    /// Encode all metrics to Prometheus text format.
    pub fn encode(&self) -> String {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    }

    /// Get the Prometheus registry.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Get or create a counter vec.
    fn get_or_create_counter_vec(&self, name: &str, label_names: &[&str]) -> CounterVec {
        if let Some(counter) = self.counters.get(name) {
            return counter.clone();
        }

        let opts = Opts::new(name, name);
        let counter = CounterVec::new(opts, label_names).expect("Failed to create counter");
        let _ = self.registry.register(Box::new(counter.clone()));
        self.counters.insert(name.to_string(), counter.clone());
        counter
    }

    /// Get or create a gauge vec.
    fn get_or_create_gauge_vec(&self, name: &str, label_names: &[&str]) -> GaugeVec {
        if let Some(gauge) = self.gauges.get(name) {
            return gauge.clone();
        }

        let opts = Opts::new(name, name);
        let gauge = GaugeVec::new(opts, label_names).expect("Failed to create gauge");
        let _ = self.registry.register(Box::new(gauge.clone()));
        self.gauges.insert(name.to_string(), gauge.clone());
        gauge
    }

    /// Get or create a histogram vec with default buckets optimized for latency measurements.
    fn get_or_create_histogram_vec(&self, name: &str, label_names: &[&str]) -> HistogramVec {
        if let Some(histogram) = self.histograms.get(name) {
            return histogram.clone();
        }

        // Default buckets optimized for command latency (microseconds to seconds)
        let buckets = vec![
            0.000_01,  // 10µs
            0.000_05,  // 50µs
            0.000_1,   // 100µs
            0.000_25,  // 250µs
            0.000_5,   // 500µs
            0.001,     // 1ms
            0.002_5,   // 2.5ms
            0.005,     // 5ms
            0.01,      // 10ms
            0.025,     // 25ms
            0.05,      // 50ms
            0.1,       // 100ms
            0.25,      // 250ms
            0.5,       // 500ms
            1.0,       // 1s
            2.5,       // 2.5s
            5.0,       // 5s
            10.0,      // 10s
        ];

        let opts = HistogramOpts::new(name, name).buckets(buckets);
        let histogram = HistogramVec::new(opts, label_names).expect("Failed to create histogram");
        let _ = self.registry.register(Box::new(histogram.clone()));
        self.histograms.insert(name.to_string(), histogram.clone());
        histogram
    }

    /// Convert labels slice to cache key.
    fn labels_to_key(labels: &[(&str, &str)]) -> LabelsKey {
        labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Convert labels slice to label values vec.
    fn labels_to_values<'a>(labels: &'a [(&'a str, &'a str)]) -> Vec<&'a str> {
        labels.iter().map(|(_, v)| *v).collect()
    }

    /// Extract label names from labels slice.
    fn labels_to_names<'a>(labels: &'a [(&'a str, &'a str)]) -> Vec<&'a str> {
        labels.iter().map(|(k, _)| *k).collect()
    }
}

impl MetricsRecorder for PrometheusRecorder {
    fn increment_counter(&self, name: &str, value: u64, labels: &[(&str, &str)]) {
        let key = (name.to_string(), Self::labels_to_key(labels));

        // Fast path: check cache first
        if let Some(counter) = self.counter_cache.get(&key) {
            counter.inc_by(value as f64);
            return;
        }

        // Slow path: create or get counter vec and cache specific counter
        let label_names = Self::labels_to_names(labels);
        let label_values = Self::labels_to_values(labels);
        let counter_vec = self.get_or_create_counter_vec(name, &label_names);
        let counter = counter_vec.with_label_values(&label_values);
        counter.inc_by(value as f64);
        self.counter_cache.insert(key, counter);
    }

    fn record_gauge(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        let key = (name.to_string(), Self::labels_to_key(labels));

        // Fast path: check cache first
        if let Some(gauge) = self.gauge_cache.get(&key) {
            gauge.set(value);
            return;
        }

        // Slow path: create or get gauge vec and cache specific gauge
        let label_names = Self::labels_to_names(labels);
        let label_values = Self::labels_to_values(labels);
        let gauge_vec = self.get_or_create_gauge_vec(name, &label_names);
        let gauge = gauge_vec.with_label_values(&label_values);
        gauge.set(value);
        self.gauge_cache.insert(key, gauge);
    }

    fn record_histogram(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        let key = (name.to_string(), Self::labels_to_key(labels));

        // Fast path: check cache first
        if let Some(histogram) = self.histogram_cache.get(&key) {
            histogram.observe(value);
            return;
        }

        // Slow path: create or get histogram vec and cache specific histogram
        let label_names = Self::labels_to_names(labels);
        let label_values = Self::labels_to_values(labels);
        let histogram_vec = self.get_or_create_histogram_vec(name, &label_names);
        let histogram = histogram_vec.with_label_values(&label_values);
        histogram.observe(value);
        self.histogram_cache.insert(key, histogram);
    }
}

// Safety: PrometheusRecorder uses DashMap which is Send + Sync
unsafe impl Send for PrometheusRecorder {}
unsafe impl Sync for PrometheusRecorder {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter_increment() {
        let recorder = PrometheusRecorder::new();
        recorder.increment_counter("test_counter", 1, &[("label", "value")]);
        recorder.increment_counter("test_counter", 5, &[("label", "value")]);

        let output = recorder.encode();
        assert!(output.contains("test_counter"));
        assert!(output.contains("6")); // 1 + 5
    }

    #[test]
    fn test_gauge_set() {
        let recorder = PrometheusRecorder::new();
        recorder.record_gauge("test_gauge", 42.0, &[("shard", "0")]);

        let output = recorder.encode();
        assert!(output.contains("test_gauge"));
        assert!(output.contains("42"));
    }

    #[test]
    fn test_histogram_observe() {
        let recorder = PrometheusRecorder::new();
        recorder.record_histogram("test_histogram", 0.001, &[("command", "GET")]);
        recorder.record_histogram("test_histogram", 0.005, &[("command", "GET")]);

        let output = recorder.encode();
        assert!(output.contains("test_histogram"));
        assert!(output.contains("_bucket"));
        assert!(output.contains("_count"));
        assert!(output.contains("_sum"));
    }

    #[test]
    fn test_multiple_label_combinations() {
        let recorder = PrometheusRecorder::new();
        recorder.increment_counter("cmd_total", 1, &[("cmd", "GET")]);
        recorder.increment_counter("cmd_total", 2, &[("cmd", "SET")]);
        recorder.increment_counter("cmd_total", 3, &[("cmd", "GET")]);

        let output = recorder.encode();
        assert!(output.contains(r#"cmd="GET""#));
        assert!(output.contains(r#"cmd="SET""#));
    }

    #[test]
    fn test_encode_empty() {
        let recorder = PrometheusRecorder::new();
        let output = recorder.encode();
        assert!(output.is_empty() || output.trim().is_empty());
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let recorder = Arc::new(PrometheusRecorder::new());
        let mut handles = vec![];

        for i in 0..10 {
            let r = Arc::clone(&recorder);
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    r.increment_counter("concurrent_counter", 1, &[("thread", &i.to_string())]);
                    r.record_gauge("concurrent_gauge", j as f64, &[("thread", &i.to_string())]);
                    r.record_histogram(
                        "concurrent_histogram",
                        0.001 * j as f64,
                        &[("thread", &i.to_string())],
                    );
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let output = recorder.encode();
        assert!(output.contains("concurrent_counter"));
        assert!(output.contains("concurrent_gauge"));
        assert!(output.contains("concurrent_histogram"));
    }
}
