//! Prometheus metrics recorder implementation.

use crate::latency_bands::LatencyBandTracker;
use crate::metric_names;
use dashmap::DashMap;
use frogdb_core::MetricsRecorder;
use prometheus::{
    Counter, CounterVec, Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, Opts,
    Registry, TextEncoder,
};
use serde::Serialize;

/// Labels key type for DashMap lookups.
type LabelsKey = Vec<(String, String)>;

/// A point-in-time snapshot of all dashboard-relevant metrics.
#[derive(Debug, Clone, Serialize)]
pub struct DashboardMetrics {
    pub timestamp: u64,
    // Commands (counters — client computes rates)
    pub commands_total: f64,
    pub commands_errors_total: f64,
    // Connections
    pub connections_current: f64,
    pub connections_total: f64,
    // Keyspace
    pub keys_total: f64,
    pub keys_with_expiry: f64,
    pub keys_expired_total: f64,
    pub keyspace_hits_total: f64,
    pub keyspace_misses_total: f64,
    // Memory
    pub memory_used_bytes: f64,
    pub memory_rss_bytes: f64,
    pub memory_peak_bytes: f64,
    pub memory_max_bytes: f64,
    pub memory_fragmentation: f64,
    // Evictions (counters)
    pub eviction_keys_total: f64,
    pub eviction_bytes_total: f64,
    // CPU (counters — client computes rates)
    pub cpu_user_seconds: f64,
    pub cpu_system_seconds: f64,
    // Network (counters)
    pub net_input_bytes_total: f64,
    pub net_output_bytes_total: f64,
    // Per-shard data
    pub shards: Vec<ShardSnapshot>,
    // Top commands by count
    pub top_commands: Vec<CommandSnapshot>,
}

/// Per-shard metrics snapshot.
#[derive(Debug, Clone, Serialize)]
pub struct ShardSnapshot {
    pub shard_id: String,
    pub keys: f64,
    pub memory_bytes: f64,
    pub queue_depth: f64,
}

/// Per-command counter snapshot.
#[derive(Debug, Clone, Serialize)]
pub struct CommandSnapshot {
    pub command: String,
    pub count: f64,
}

/// Prometheus-based metrics recorder.
///
/// This implementation lazily creates metrics on first use and stores them
/// in thread-safe DashMaps for concurrent access. Optionally embeds a
/// `LatencyBandTracker` for SLO monitoring of command latencies.
pub struct PrometheusRecorder {
    registry: Registry,
    counters: DashMap<String, CounterVec>,
    gauges: DashMap<String, GaugeVec>,
    histograms: DashMap<String, HistogramVec>,
    // Cache for specific label combinations (avoids label vec allocation on hot path)
    counter_cache: DashMap<(String, LabelsKey), Counter>,
    gauge_cache: DashMap<(String, LabelsKey), Gauge>,
    histogram_cache: DashMap<(String, LabelsKey), Histogram>,
    // Optional latency band tracker for SLO monitoring
    band_tracker: Option<LatencyBandTracker>,
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
            band_tracker: None,
        }
    }

    /// Enable latency band tracking with the given thresholds (in milliseconds).
    pub fn with_latency_bands(mut self, bands: Vec<u64>) -> Self {
        self.band_tracker = Some(LatencyBandTracker::new(bands));
        self
    }

    /// Encode all metrics to Prometheus text format.
    ///
    /// If latency band tracking is enabled, band gauges are updated
    /// in the registry before encoding.
    pub fn encode(&self) -> String {
        // Flush latency band data into gauges before encoding
        if let Some(tracker) = &self.band_tracker {
            for (label, count) in tracker.get_counts() {
                self.record_gauge(
                    metric_names::LATENCY_BAND_REQUESTS,
                    count as f64,
                    &[("le", &label)],
                );
            }
        }

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
            0.000_01, // 10µs
            0.000_05, // 50µs
            0.000_1,  // 100µs
            0.000_25, // 250µs
            0.000_5,  // 500µs
            0.001,    // 1ms
            0.002_5,  // 2.5ms
            0.005,    // 5ms
            0.01,     // 10ms
            0.025,    // 25ms
            0.05,     // 50ms
            0.1,      // 100ms
            0.25,     // 250ms
            0.5,      // 500ms
            1.0,      // 1s
            2.5,      // 2.5s
            5.0,      // 5s
            10.0,     // 10s
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

    /// Get the current value of a counter (summed across all label combinations).
    ///
    /// Returns None if the counter doesn't exist.
    pub fn get_counter_value(&self, name: &str) -> Option<f64> {
        let metric_families = self.registry.gather();
        for mf in metric_families {
            if mf.name() == name {
                let mut total = 0.0;
                for m in mf.get_metric() {
                    total += m.get_counter().value();
                }
                return Some(total);
            }
        }
        None
    }

    /// Get the current value of a gauge (returns the first one found, or summed if requested).
    ///
    /// Returns None if the gauge doesn't exist.
    pub fn get_gauge_value(&self, name: &str) -> Option<f64> {
        let metric_families = self.registry.gather();
        for mf in metric_families {
            if mf.name() == name {
                let mut total = 0.0;
                for m in mf.get_metric() {
                    total += m.get_gauge().value();
                }
                return Some(total);
            }
        }
        None
    }

    /// Collect a structured snapshot of all dashboard-relevant metrics.
    ///
    /// Gathers the Prometheus registry once and extracts all values in a single pass,
    /// returning them in a typed struct instead of requiring callers to use string-based lookups.
    pub fn dashboard_snapshot(&self) -> DashboardMetrics {
        use crate::metric_names;

        let metric_families = self.registry.gather();

        // Helper closures that search the pre-gathered families
        let gauge_val = |name: &str| -> f64 {
            for mf in &metric_families {
                if mf.name() == name {
                    let mut total = 0.0;
                    for m in mf.get_metric() {
                        total += m.get_gauge().value();
                    }
                    return total;
                }
            }
            0.0
        };

        let counter_val = |name: &str| -> f64 {
            for mf in &metric_families {
                if mf.name() == name {
                    let mut total = 0.0;
                    for m in mf.get_metric() {
                        total += m.get_counter().value();
                    }
                    return total;
                }
            }
            0.0
        };

        // Extract per-shard data from labeled gauges
        let mut shards = Vec::new();
        let mut shard_keys_map: std::collections::HashMap<String, f64> =
            std::collections::HashMap::new();
        let mut shard_mem_map: std::collections::HashMap<String, f64> =
            std::collections::HashMap::new();
        let mut shard_queue_map: std::collections::HashMap<String, f64> =
            std::collections::HashMap::new();

        for mf in &metric_families {
            let name = mf.name();
            if name == metric_names::SHARD_KEYS {
                for m in mf.get_metric() {
                    for lp in m.get_label() {
                        if lp.name() == "shard" {
                            shard_keys_map
                                .insert(lp.value().to_string(), m.get_gauge().value());
                        }
                    }
                }
            } else if name == metric_names::SHARD_MEMORY_BYTES {
                for m in mf.get_metric() {
                    for lp in m.get_label() {
                        if lp.name() == "shard" {
                            shard_mem_map
                                .insert(lp.value().to_string(), m.get_gauge().value());
                        }
                    }
                }
            } else if name == metric_names::SHARD_QUEUE_DEPTH {
                for m in mf.get_metric() {
                    for lp in m.get_label() {
                        if lp.name() == "shard" {
                            shard_queue_map
                                .insert(lp.value().to_string(), m.get_gauge().value());
                        }
                    }
                }
            }
        }

        // Merge shard maps into snapshots
        let mut all_shard_ids: std::collections::BTreeSet<String> =
            std::collections::BTreeSet::new();
        all_shard_ids.extend(shard_keys_map.keys().cloned());
        all_shard_ids.extend(shard_mem_map.keys().cloned());
        for shard_id in all_shard_ids {
            shards.push(ShardSnapshot {
                keys: shard_keys_map.get(&shard_id).copied().unwrap_or(0.0),
                memory_bytes: shard_mem_map.get(&shard_id).copied().unwrap_or(0.0),
                queue_depth: shard_queue_map.get(&shard_id).copied().unwrap_or(0.0),
                shard_id,
            });
        }

        // Extract per-command counters (top commands)
        let mut top_commands = Vec::new();
        for mf in &metric_families {
            if mf.name() == metric_names::COMMANDS_TOTAL {
                for m in mf.get_metric() {
                    let count = m.get_counter().value();
                    let mut cmd_name = String::new();
                    for lp in m.get_label() {
                        if lp.name() == "command" {
                            cmd_name = lp.value().to_string();
                        }
                    }
                    if !cmd_name.is_empty() && count > 0.0 {
                        top_commands.push(CommandSnapshot {
                            command: cmd_name,
                            count,
                        });
                    }
                }
            }
        }
        // Sort by count descending, take top 20
        top_commands.sort_by(|a, b| b.count.partial_cmp(&a.count).unwrap_or(std::cmp::Ordering::Equal));
        top_commands.truncate(20);

        DashboardMetrics {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            commands_total: counter_val(metric_names::COMMANDS_TOTAL),
            commands_errors_total: counter_val(metric_names::COMMANDS_ERRORS),
            connections_current: gauge_val(metric_names::CONNECTIONS_CURRENT),
            connections_total: counter_val(metric_names::CONNECTIONS_TOTAL),
            keys_total: gauge_val(metric_names::KEYS_TOTAL),
            keys_with_expiry: gauge_val(metric_names::KEYS_WITH_EXPIRY),
            keys_expired_total: counter_val(metric_names::KEYS_EXPIRED),
            keyspace_hits_total: counter_val(metric_names::KEYSPACE_HITS),
            keyspace_misses_total: counter_val(metric_names::KEYSPACE_MISSES),
            memory_used_bytes: gauge_val(metric_names::MEMORY_USED_BYTES),
            memory_rss_bytes: gauge_val(metric_names::MEMORY_RSS_BYTES),
            memory_peak_bytes: gauge_val(metric_names::MEMORY_PEAK_BYTES),
            memory_max_bytes: gauge_val(metric_names::MEMORY_MAXMEMORY_BYTES),
            memory_fragmentation: gauge_val(metric_names::MEMORY_FRAGMENTATION_RATIO),
            eviction_keys_total: counter_val(metric_names::EVICTION_KEYS_TOTAL),
            eviction_bytes_total: counter_val(metric_names::EVICTION_BYTES_TOTAL),
            cpu_user_seconds: counter_val(metric_names::CPU_USER_SECONDS),
            cpu_system_seconds: counter_val(metric_names::CPU_SYSTEM_SECONDS),
            net_input_bytes_total: counter_val(metric_names::NET_INPUT_BYTES),
            net_output_bytes_total: counter_val(metric_names::NET_OUTPUT_BYTES),
            shards,
            top_commands,
        }
    }

    /// Get histogram quantiles for a metric.
    ///
    /// Returns (p50, p95, p99) or None if the histogram doesn't exist.
    pub fn get_histogram_quantiles(&self, name: &str) -> Option<(f64, f64, f64)> {
        let metric_families = self.registry.gather();
        for mf in metric_families {
            if mf.name() == name
                && let Some(m) = mf.get_metric().first()
            {
                let h = m.get_histogram();
                let count = h.sample_count();
                if count == 0 {
                    return Some((0.0, 0.0, 0.0));
                }

                // Calculate approximate quantiles from buckets
                let buckets = h.get_bucket();
                let p50_target = count as f64 * 0.5;
                let p95_target = count as f64 * 0.95;
                let p99_target = count as f64 * 0.99;

                let mut p50 = 0.0;
                let mut p95 = 0.0;
                let mut p99 = 0.0;
                let mut p50_found = false;
                let mut p95_found = false;
                let mut p99_found = false;

                for bucket in buckets {
                    let upper = bucket.upper_bound();
                    let cumulative = bucket.cumulative_count() as f64;

                    if !p50_found && cumulative >= p50_target {
                        p50 = upper * 1000.0; // Convert to ms
                        p50_found = true;
                    }
                    if !p95_found && cumulative >= p95_target {
                        p95 = upper * 1000.0;
                        p95_found = true;
                    }
                    if !p99_found && cumulative >= p99_target {
                        p99 = upper * 1000.0;
                        p99_found = true;
                    }
                }

                return Some((p50, p95, p99));
            }
        }
        None
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

    fn record_command_latency_ms(&self, latency_ms: u64) {
        if let Some(tracker) = &self.band_tracker {
            tracker.record(latency_ms);
        }
    }

    fn latency_bands_enabled(&self) -> bool {
        self.band_tracker.is_some()
    }

    fn latency_band_total(&self) -> u64 {
        self.band_tracker.as_ref().map_or(0, |t| t.total())
    }

    fn latency_band_percentages(&self) -> Vec<(String, u64, f64)> {
        self.band_tracker
            .as_ref()
            .map_or_else(Vec::new, |t| t.get_percentages())
    }

    fn reset_latency_bands(&self) {
        if let Some(tracker) = &self.band_tracker {
            tracker.reset();
        }
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
