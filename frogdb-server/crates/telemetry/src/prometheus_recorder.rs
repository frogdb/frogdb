//! Prometheus metrics recorder implementation.

use crate::latency_bands::LatencyBandTracker;
use dashmap::DashMap;
use frogdb_core::MetricsRecorder;
use frogdb_types::metrics::{MetricType, definition_for, definitions};
use prometheus::proto::MetricType as ProtoMetricType;
use prometheus::{
    Counter, CounterVec, Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, Opts,
    Registry, TextEncoder,
};
use serde::Serialize;
use std::collections::HashMap;

/// Labels key type for DashMap lookups.
type LabelsKey = Vec<(String, String)>;

/// A point-in-time snapshot of all dashboard-relevant metrics.
///
/// This stays a named-field struct rather than a registry-derived map because
/// its serialized field names are the JSON contract with the debug web UI
/// (`/debug/api/metrics`); the field *values* are populated from the same
/// registry-defined metric names the typed handles emit, in one pass over the
/// gathered families.
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
    // CPU (cumulative seconds — client computes rates)
    pub cpu_user_seconds: f64,
    pub cpu_system_seconds: f64,
    // WAL / Persistence
    pub wal_writes_total: f64,
    pub wal_bytes_total: f64,
    pub wal_pending_ops: f64,
    pub wal_durability_lag_ms: f64,
    // Blocking
    pub blocked_clients: f64,
    // Pub/Sub
    pub pubsub_channels: f64,
    pub pubsub_patterns: f64,
    pub pubsub_subscribers: f64,
    pub pubsub_messages_total: f64,
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
/// Metrics are created lazily on first emission. For metrics present in the
/// typed registry (`frogdb_types::metrics::ALL_METRICS`), creation takes the
/// help text, metric type, and label schema from the definition — the first
/// caller cannot fix a wrong arity, and `/metrics` carries real HELP text.
/// Unknown names (tests, ad-hoc probes) fall back to caller-supplied labels
/// with the name as help. Optionally embeds a `LatencyBandTracker` for SLO
/// monitoring of command latencies.
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

/// Check an emission against the typed registry.
///
/// Returns `false` (drop the sample, log an error) when the metric is
/// registered with a different type or label schema than the caller supplied —
/// recording it anyway would either panic in the prometheus crate (arity
/// mismatch on an already-created vec) or silently create a second, never
/// -scraped metric family (type mismatch). Typed handles always pass; only
/// raw string-name emissions can trip this.
fn emission_matches_definition(name: &str, expected: MetricType, labels: &[(&str, &str)]) -> bool {
    let Some(def) = definition_for(name) else {
        // Unknown to the registry: allowed (tests, ad-hoc), caller shape wins.
        return true;
    };
    if def.metric_type != expected {
        tracing::error!(
            metric = name,
            registered = %def.metric_type,
            emitted = %expected,
            "metric emitted with wrong type; sample dropped — emit through the typed handle"
        );
        return false;
    }
    if def.labels.len() != labels.len() || def.labels.iter().zip(labels).any(|(d, (k, _))| d != k) {
        tracing::error!(
            metric = name,
            registered = ?def.labels,
            emitted = ?labels.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
            "metric emitted with wrong label schema; sample dropped — emit through the typed handle"
        );
        return false;
    }
    true
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
                definitions::LatencyBandRequests::set(self, count as f64, &label);
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

    /// Resolve help text and label names for a metric: registry definition if
    /// present, otherwise the caller's shape with the name as help.
    fn opts_for<'a>(name: &'a str, caller_labels: &[&'a str]) -> (&'a str, Vec<&'a str>) {
        match definition_for(name) {
            Some(def) => (def.help, def.labels.to_vec()),
            None => (name, caller_labels.to_vec()),
        }
    }

    /// Get or create a counter vec.
    fn get_or_create_counter_vec(&self, name: &str, label_names: &[&str]) -> CounterVec {
        if let Some(counter) = self.counters.get(name) {
            return counter.clone();
        }

        let (help, label_names) = Self::opts_for(name, label_names);
        let opts = Opts::new(name, help);
        let counter = CounterVec::new(opts, &label_names).expect("Failed to create counter");
        let _ = self.registry.register(Box::new(counter.clone()));
        self.counters.insert(name.to_string(), counter.clone());
        counter
    }

    /// Get or create a gauge vec.
    fn get_or_create_gauge_vec(&self, name: &str, label_names: &[&str]) -> GaugeVec {
        if let Some(gauge) = self.gauges.get(name) {
            return gauge.clone();
        }

        let (help, label_names) = Self::opts_for(name, label_names);
        let opts = Opts::new(name, help);
        let gauge = GaugeVec::new(opts, &label_names).expect("Failed to create gauge");
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

        let (help, label_names) = Self::opts_for(name, label_names);
        let opts = HistogramOpts::new(name, help).buckets(buckets);
        let histogram = HistogramVec::new(opts, &label_names).expect("Failed to create histogram");
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

    /// Get the current value of a gauge (summed across all label combinations).
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
    /// Gathers the Prometheus registry once and folds every family into a
    /// name → summed-value map in a single pass (reading counter or gauge
    /// values according to each family's registered type), then fills the
    /// typed struct from that map using the registry's metric-name constants.
    pub fn dashboard_snapshot(&self) -> DashboardMetrics {
        let metric_families = self.registry.gather();

        // name → value summed across label sets, one pass over all families.
        let mut values: HashMap<&str, f64> = HashMap::with_capacity(metric_families.len());
        // Per-shard gauge extraction and per-command counters, same pass.
        let mut shard_keys_map: HashMap<String, f64> = HashMap::new();
        let mut shard_mem_map: HashMap<String, f64> = HashMap::new();
        let mut shard_queue_map: HashMap<String, f64> = HashMap::new();
        let mut top_commands: Vec<CommandSnapshot> = Vec::new();

        for mf in &metric_families {
            let name = mf.name();
            let field_type = mf.get_field_type();
            let mut total = 0.0;
            for m in mf.get_metric() {
                let value = match field_type {
                    ProtoMetricType::COUNTER => m.get_counter().value(),
                    ProtoMetricType::GAUGE => m.get_gauge().value(),
                    _ => continue,
                };
                total += value;

                if name == definitions::ShardKeys::NAME
                    || name == definitions::ShardMemoryBytes::NAME
                    || name == definitions::ShardQueueDepth::NAME
                {
                    for lp in m.get_label() {
                        if lp.name() == "shard" {
                            let map = if name == definitions::ShardKeys::NAME {
                                &mut shard_keys_map
                            } else if name == definitions::ShardMemoryBytes::NAME {
                                &mut shard_mem_map
                            } else {
                                &mut shard_queue_map
                            };
                            map.insert(lp.value().to_string(), value);
                        }
                    }
                } else if name == definitions::CommandsTotal::NAME && value > 0.0 {
                    for lp in m.get_label() {
                        if lp.name() == "command" && !lp.value().is_empty() {
                            top_commands.push(CommandSnapshot {
                                command: lp.value().to_string(),
                                count: value,
                            });
                        }
                    }
                }
            }
            values.insert(name, total);
        }

        let val = |name: &str| -> f64 { values.get(name).copied().unwrap_or(0.0) };

        // Merge shard maps into snapshots
        let mut shards = Vec::new();
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

        // Sort by count descending, take top 20
        top_commands.sort_by(|a, b| {
            b.count
                .partial_cmp(&a.count)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        top_commands.truncate(20);

        DashboardMetrics {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            commands_total: val(definitions::CommandsTotal::NAME),
            commands_errors_total: val(definitions::CommandsErrors::NAME),
            connections_current: val(definitions::ConnectionsCurrent::NAME),
            connections_total: val(definitions::ConnectionsTotal::NAME),
            keys_total: val(definitions::KeysTotal::NAME),
            keys_with_expiry: val(definitions::KeysWithExpiry::NAME),
            keys_expired_total: val(definitions::KeysExpired::NAME),
            keyspace_hits_total: val(definitions::KeyspaceHits::NAME),
            keyspace_misses_total: val(definitions::KeyspaceMisses::NAME),
            memory_used_bytes: val(definitions::MemoryUsedBytes::NAME),
            memory_rss_bytes: val(definitions::MemoryRssBytes::NAME),
            memory_peak_bytes: val(definitions::MemoryPeakBytes::NAME),
            memory_max_bytes: val(definitions::MemoryMaxmemoryBytes::NAME),
            memory_fragmentation: val(definitions::MemoryFragmentationRatio::NAME),
            eviction_keys_total: val(definitions::EvictionKeysTotal::NAME),
            eviction_bytes_total: val(definitions::EvictionBytesTotal::NAME),
            cpu_user_seconds: val(definitions::CpuUserSeconds::NAME),
            cpu_system_seconds: val(definitions::CpuSystemSeconds::NAME),
            wal_writes_total: val(definitions::WalWrites::NAME),
            wal_bytes_total: val(definitions::WalBytes::NAME),
            wal_pending_ops: val(definitions::WalPendingOps::NAME),
            wal_durability_lag_ms: val(definitions::WalDurabilityLagMs::NAME),
            blocked_clients: val(definitions::BlockedClients::NAME),
            pubsub_channels: val(definitions::PubsubChannels::NAME),
            pubsub_patterns: val(definitions::PubsubPatterns::NAME),
            pubsub_subscribers: val(definitions::PubsubSubscribers::NAME),
            pubsub_messages_total: val(definitions::PubsubMessages::NAME),
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

        // Slow path: validate against the registry, then create or get the
        // counter vec and cache the specific counter.
        if !emission_matches_definition(name, MetricType::Counter, labels) {
            return;
        }
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

        // Slow path: validate against the registry, then create or get the
        // gauge vec and cache the specific gauge.
        if !emission_matches_definition(name, MetricType::Gauge, labels) {
            return;
        }
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

        // Slow path: validate against the registry, then create or get the
        // histogram vec and cache the specific histogram.
        if !emission_matches_definition(name, MetricType::Histogram, labels) {
            return;
        }
        let label_names = Self::labels_to_names(labels);
        let label_values = Self::labels_to_values(labels);
        let histogram_vec = self.get_or_create_histogram_vec(name, &label_names);
        let histogram = histogram_vec.with_label_values(&label_values);
        histogram.observe(value);
        self.histogram_cache.insert(key, histogram);
    }

    fn counter_value(&self, name: &str) -> Option<u64> {
        // Reuse the registry-gathering reader so INFO reports exactly what
        // Prometheus would scrape for this counter.
        self.get_counter_value(name).map(|v| v as u64)
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
    fn test_registered_metric_gets_definition_help_text() {
        use frogdb_types::metrics::definitions::ConnectionsTotal;

        let recorder = PrometheusRecorder::new();
        ConnectionsTotal::inc(&recorder);

        let output = recorder.encode();
        let help_line = format!(
            "# HELP {} {}",
            ConnectionsTotal::NAME,
            ConnectionsTotal::HELP
        );
        assert!(
            output.contains(&help_line),
            "expected real HELP text from the registry, got:\n{output}"
        );
    }

    #[test]
    fn test_registered_metric_rejects_wrong_label_schema() {
        use frogdb_types::metrics::definitions::CommandsTotal;

        let recorder = PrometheusRecorder::new();
        // Correct schema via the typed handle.
        CommandsTotal::inc(&recorder, "GET");
        // Raw emission with a label set that contradicts the registry: dropped.
        recorder.increment_counter(CommandsTotal::NAME, 5, &[("bogus", "x")]);
        recorder.increment_counter(CommandsTotal::NAME, 5, &[]);

        assert_eq!(recorder.get_counter_value(CommandsTotal::NAME), Some(1.0));
        assert!(!recorder.encode().contains("bogus"));
    }

    #[test]
    fn test_registered_metric_rejects_wrong_type() {
        use frogdb_types::metrics::definitions::ConnectionsCurrent;

        let recorder = PrometheusRecorder::new();
        // ConnectionsCurrent is a gauge; a counter emission must be dropped,
        // not registered as a shadow family.
        recorder.increment_counter(ConnectionsCurrent::NAME, 1, &[]);
        assert!(
            recorder
                .get_counter_value(ConnectionsCurrent::NAME)
                .is_none()
        );

        ConnectionsCurrent::set(&recorder, 7.0);
        assert_eq!(
            recorder.get_gauge_value(ConnectionsCurrent::NAME),
            Some(7.0)
        );
    }

    #[test]
    fn test_unknown_metric_falls_back_to_caller_shape() {
        let recorder = PrometheusRecorder::new();
        recorder.increment_counter("adhoc_metric_total", 3, &[("kind", "x")]);
        assert_eq!(recorder.get_counter_value("adhoc_metric_total"), Some(3.0));
        // Unknown names get the name as help text.
        assert!(
            recorder
                .encode()
                .contains("# HELP adhoc_metric_total adhoc_metric_total")
        );
    }

    #[test]
    fn test_dashboard_snapshot_reads_typed_emissions() {
        use frogdb_types::metrics::definitions::{
            CommandsTotal, ConnectionsCurrent, CpuUserSeconds, KeysTotal, ShardKeys,
        };

        let recorder = PrometheusRecorder::new();
        CommandsTotal::inc_by(&recorder, 4, "GET");
        CommandsTotal::inc_by(&recorder, 2, "SET");
        ConnectionsCurrent::set(&recorder, 3.0);
        CpuUserSeconds::set(&recorder, 1.5);
        KeysTotal::set(&recorder, 10.0, "0");
        KeysTotal::set(&recorder, 20.0, "1");
        ShardKeys::set(&recorder, 10.0, "0");
        ShardKeys::set(&recorder, 20.0, "1");

        let snap = recorder.dashboard_snapshot();
        assert_eq!(snap.commands_total, 6.0);
        assert_eq!(snap.connections_current, 3.0);
        // CPU seconds are recorded as gauges; the snapshot must read them as
        // such instead of reporting a permanent 0.
        assert_eq!(snap.cpu_user_seconds, 1.5);
        // Per-shard gauges are summed across label sets.
        assert_eq!(snap.keys_total, 30.0);
        assert_eq!(snap.shards.len(), 2);
        assert_eq!(snap.top_commands[0].command, "GET");
        assert_eq!(snap.top_commands[0].count, 4.0);
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
