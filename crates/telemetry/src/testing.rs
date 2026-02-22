//! Test utilities for asserting on metric values.
//!
//! Only available when the `testing` feature is enabled.
//!
//! # Example
//!
//! ```rust,ignore
//! use frogdb_telemetry::testing::*;
//!
//! let metrics = fetch_metrics().await;
//! let count = get_counter(&metrics, "frogdb_commands_total", &[("command", "SET")]);
//! assert_eq!(count, 1.0);
//! ```
//!
//! # Snapshot-based Testing
//!
//! For before/after comparisons, use `MetricsSnapshot` and `MetricsDelta`:
//!
//! ```rust,ignore
//! use frogdb_telemetry::testing::{MetricsSnapshot, MetricsDelta};
//!
//! let before = MetricsSnapshot::new(server.fetch_metrics().await);
//! // ... perform operations ...
//! let after = MetricsSnapshot::new(server.fetch_metrics().await);
//!
//! MetricsDelta::new(before, after)
//!     .assert_counter_increased("frogdb_commands_total", &[("command", "SET")], 1.0)
//!     .assert_histogram_recorded("frogdb_commands_duration_seconds", &[("command", "SET")], 1);
//! ```

use std::collections::HashMap;

/// A parsed metric sample with its name, labels, and value.
#[derive(Debug, Clone)]
pub struct MetricSample {
    pub name: String,
    pub labels: HashMap<String, String>,
    pub value: f64,
}

/// Parse Prometheus text format into metric samples.
///
/// Handles:
/// - Comment lines (# HELP, # TYPE) - skipped
/// - Metric lines with optional labels
/// - Label value escaping (quotes, backslashes, newlines)
/// - Histogram suffixes (_bucket, _count, _sum)
///
/// # Example
///
/// ```
/// use frogdb_telemetry::testing::parse_prometheus;
///
/// let text = r#"
/// # HELP my_counter A counter metric
/// # TYPE my_counter counter
/// my_counter{label="value"} 42
/// my_gauge 3.14
/// "#;
///
/// let samples = parse_prometheus(text);
/// assert_eq!(samples.len(), 2);
/// ```
pub fn parse_prometheus(text: &str) -> Vec<MetricSample> {
    let mut samples = Vec::new();

    for line in text.lines() {
        let line = line.trim();

        // Skip empty lines and comments
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if let Some(sample) = parse_metric_line(line) {
            samples.push(sample);
        }
    }

    samples
}

/// Parse a single metric line.
fn parse_metric_line(line: &str) -> Option<MetricSample> {
    // Find the metric name (everything up to { or space)
    let (name, rest) = if let Some(brace_pos) = line.find('{') {
        (&line[..brace_pos], &line[brace_pos..])
    } else if let Some(space_pos) = line.find(' ') {
        (&line[..space_pos], &line[space_pos..])
    } else {
        return None;
    };

    let name = name.trim().to_string();

    // Parse labels if present
    let (labels, value_str) = if rest.starts_with('{') {
        parse_labels_and_value(rest)?
    } else {
        (HashMap::new(), rest.trim())
    };

    // Parse value
    let value = value_str.trim().parse::<f64>().ok()?;

    Some(MetricSample {
        name,
        labels,
        value,
    })
}

/// Parse labels from `{label="value",...} 123` format.
/// Returns the labels and the remaining value string.
fn parse_labels_and_value(s: &str) -> Option<(HashMap<String, String>, &str)> {
    let close_brace = find_closing_brace(s)?;
    let labels_str = &s[1..close_brace]; // Skip opening brace
    let value_str = &s[close_brace + 1..]; // After closing brace

    let labels = parse_labels_str(labels_str);
    Some((labels, value_str))
}

/// Find the closing brace, accounting for escaped characters in label values.
fn find_closing_brace(s: &str) -> Option<usize> {
    let mut in_quotes = false;
    let mut prev_backslash = false;

    for (i, c) in s.char_indices() {
        if c == '"' && !prev_backslash {
            in_quotes = !in_quotes;
        } else if c == '}' && !in_quotes {
            return Some(i);
        }

        prev_backslash = c == '\\' && !prev_backslash;
    }

    None
}

/// Parse the labels portion: `key="value",key2="value2"`
fn parse_labels_str(s: &str) -> HashMap<String, String> {
    let mut labels = HashMap::new();
    let mut remaining = s.trim();

    while !remaining.is_empty() {
        // Find the key (up to =)
        let eq_pos = match remaining.find('=') {
            Some(pos) => pos,
            None => break,
        };

        let key = remaining[..eq_pos].trim().to_string();
        remaining = &remaining[eq_pos + 1..];

        // Value should start with "
        if !remaining.starts_with('"') {
            break;
        }
        remaining = &remaining[1..]; // Skip opening quote

        // Find the closing quote, handling escapes
        let (value, rest) = parse_quoted_value(remaining);
        labels.insert(key, value);

        remaining = rest.trim_start_matches(',').trim();
    }

    labels
}

/// Parse a quoted value, handling escape sequences.
/// Returns the unescaped value and the remaining string.
fn parse_quoted_value(s: &str) -> (String, &str) {
    let mut value = String::new();
    let chars = s.char_indices();
    let mut prev_backslash = false;
    let mut end_pos = s.len();

    for (i, c) in chars {
        if prev_backslash {
            // Handle escape sequences
            match c {
                'n' => value.push('\n'),
                '\\' => value.push('\\'),
                '"' => value.push('"'),
                _ => {
                    value.push('\\');
                    value.push(c);
                }
            }
            prev_backslash = false;
        } else if c == '\\' {
            prev_backslash = true;
        } else if c == '"' {
            end_pos = i;
            break;
        } else {
            value.push(c);
        }
    }

    (value, &s[end_pos + 1..])
}

/// Find a metric by name and labels.
///
/// Labels are matched exactly - all specified labels must match,
/// but the metric may have additional labels.
///
/// # Arguments
///
/// * `samples` - The parsed metric samples
/// * `name` - The metric name to find
/// * `labels` - Label key-value pairs that must match
///
/// # Returns
///
/// The first matching sample, or None if not found.
pub fn find_metric<'a>(
    samples: &'a [MetricSample],
    name: &str,
    labels: &[(&str, &str)],
) -> Option<&'a MetricSample> {
    samples.iter().find(|sample| {
        if sample.name != name {
            return false;
        }

        // All specified labels must match
        for (key, value) in labels {
            match sample.labels.get(*key) {
                Some(v) if v == *value => {}
                _ => return false,
            }
        }

        true
    })
}

/// Get a counter value by name and labels.
///
/// Returns 0.0 if the metric is not found.
///
/// # Example
///
/// ```
/// use frogdb_telemetry::testing::get_counter;
///
/// let text = r#"my_counter{cmd="GET"} 42"#;
/// let value = get_counter(text, "my_counter", &[("cmd", "GET")]);
/// assert_eq!(value, 42.0);
/// ```
pub fn get_counter(text: &str, name: &str, labels: &[(&str, &str)]) -> f64 {
    let samples = parse_prometheus(text);
    find_metric(&samples, name, labels)
        .map(|s| s.value)
        .unwrap_or(0.0)
}

/// Get a gauge value by name and labels.
///
/// Returns 0.0 if the metric is not found.
/// This is functionally identical to `get_counter` but provided
/// for semantic clarity in tests.
pub fn get_gauge(text: &str, name: &str, labels: &[(&str, &str)]) -> f64 {
    get_counter(text, name, labels)
}

/// Get a histogram count (_count suffix) by name and labels.
///
/// # Example
///
/// ```
/// use frogdb_telemetry::testing::get_histogram_count;
///
/// let text = r#"
/// my_histogram_count{cmd="GET"} 100
/// my_histogram_sum{cmd="GET"} 5.5
/// "#;
/// let count = get_histogram_count(text, "my_histogram", &[("cmd", "GET")]);
/// assert_eq!(count, 100);
/// ```
pub fn get_histogram_count(text: &str, name: &str, labels: &[(&str, &str)]) -> u64 {
    let count_name = format!("{}_count", name);
    get_counter(text, &count_name, labels) as u64
}

/// Get a histogram sum (_sum suffix) by name and labels.
pub fn get_histogram_sum(text: &str, name: &str, labels: &[(&str, &str)]) -> f64 {
    let sum_name = format!("{}_sum", name);
    get_counter(text, &sum_name, labels)
}

/// Get all histogram bucket values for a metric.
///
/// Returns a vector of (le, count) pairs sorted by le value.
pub fn get_histogram_buckets(text: &str, name: &str, labels: &[(&str, &str)]) -> Vec<(f64, u64)> {
    let bucket_name = format!("{}_bucket", name);
    let samples = parse_prometheus(text);

    let mut buckets: Vec<(f64, u64)> = samples
        .iter()
        .filter(|s| {
            if s.name != bucket_name {
                return false;
            }
            // Check all specified labels match
            for (key, value) in labels {
                match s.labels.get(*key) {
                    Some(v) if v == *value => {}
                    _ => return false,
                }
            }
            true
        })
        .filter_map(|s| {
            let le = s.labels.get("le")?;
            let le_val = if le == "+Inf" {
                f64::INFINITY
            } else {
                le.parse::<f64>().ok()?
            };
            Some((le_val, s.value as u64))
        })
        .collect();

    buckets.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    buckets
}

/// Assert that a counter has the expected value.
///
/// # Panics
///
/// Panics if the counter value doesn't match the expected value.
#[macro_export]
macro_rules! assert_counter_eq {
    ($text:expr, $name:expr, $labels:expr, $expected:expr) => {{
        let labels: &[(&str, &str)] = $labels;
        let actual = $crate::testing::get_counter($text, $name, labels);
        assert!(
            (actual - $expected as f64).abs() < 0.001,
            "Counter {} with labels {:?} expected {}, got {}",
            $name,
            labels,
            $expected,
            actual
        );
    }};
}

/// Assert that a counter is greater than or equal to a minimum value.
///
/// # Panics
///
/// Panics if the counter value is less than the minimum.
#[macro_export]
macro_rules! assert_counter_gte {
    ($text:expr, $name:expr, $labels:expr, $min:expr) => {{
        let labels: &[(&str, &str)] = $labels;
        let actual = $crate::testing::get_counter($text, $name, labels);
        assert!(
            actual >= $min as f64,
            "Counter {} with labels {:?} expected >= {}, got {}",
            $name,
            labels,
            $min,
            actual
        );
    }};
}

/// Assert that a gauge has the expected value.
///
/// # Panics
///
/// Panics if the gauge value doesn't match the expected value.
#[macro_export]
macro_rules! assert_gauge_eq {
    ($text:expr, $name:expr, $labels:expr, $expected:expr) => {{
        let labels: &[(&str, &str)] = $labels;
        let actual = $crate::testing::get_gauge($text, $name, labels);
        assert!(
            (actual - $expected as f64).abs() < 0.001,
            "Gauge {} with labels {:?} expected {}, got {}",
            $name,
            labels,
            $expected,
            actual
        );
    }};
}

/// Assert that a gauge is greater than or equal to a minimum value.
#[macro_export]
macro_rules! assert_gauge_gte {
    ($text:expr, $name:expr, $labels:expr, $min:expr) => {{
        let labels: &[(&str, &str)] = $labels;
        let actual = $crate::testing::get_gauge($text, $name, labels);
        assert!(
            actual >= $min as f64,
            "Gauge {} with labels {:?} expected >= {}, got {}",
            $name,
            labels,
            $min,
            actual
        );
    }};
}

/// Assert that a histogram count matches the expected value.
#[macro_export]
macro_rules! assert_histogram_count_eq {
    ($text:expr, $name:expr, $labels:expr, $expected:expr) => {{
        let labels: &[(&str, &str)] = $labels;
        let actual = $crate::testing::get_histogram_count($text, $name, labels);
        assert_eq!(
            actual, $expected as u64,
            "Histogram {} count with labels {:?} expected {}, got {}",
            $name, labels, $expected, actual
        );
    }};
}

/// Assert that a histogram count is greater than or equal to a minimum value.
#[macro_export]
macro_rules! assert_histogram_count_gte {
    ($text:expr, $name:expr, $labels:expr, $min:expr) => {{
        let labels: &[(&str, &str)] = $labels;
        let actual = $crate::testing::get_histogram_count($text, $name, labels);
        assert!(
            actual >= $min as u64,
            "Histogram {} count with labels {:?} expected >= {}, got {}",
            $name,
            labels,
            $min,
            actual
        );
    }};
}

// =============================================================================
// Snapshot-based Testing Helpers
// =============================================================================

/// A snapshot of metrics for comparison.
///
/// Wraps raw Prometheus text format metrics with helper methods for
/// extracting specific metric values.
///
/// # Example
///
/// ```rust,ignore
/// let snapshot = MetricsSnapshot::new(server.fetch_metrics().await);
/// let count = snapshot.counter("frogdb_commands_total", &[("command", "SET")]);
/// ```
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    raw: String,
}

/// Fetch metrics from an HTTP endpoint.
///
/// # Example
///
/// ```rust,ignore
/// use frogdb_telemetry::testing::fetch_metrics;
///
/// let metrics = fetch_metrics(server.metrics_addr).await;
/// ```
#[cfg(feature = "testing")]
pub async fn fetch_metrics(addr: std::net::SocketAddr) -> String {
    reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .get(format!("http://{}/metrics", addr))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap()
}

impl MetricsSnapshot {
    /// Create a new metrics snapshot from raw Prometheus text format.
    pub fn new(raw: String) -> Self {
        Self { raw }
    }

    /// Fetch metrics from an HTTP endpoint and create a snapshot.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use frogdb_telemetry::testing::MetricsSnapshot;
    ///
    /// let snapshot = MetricsSnapshot::fetch(server.metrics_addr).await;
    /// ```
    #[cfg(feature = "testing")]
    pub async fn fetch(addr: std::net::SocketAddr) -> Self {
        Self::new(fetch_metrics(addr).await)
    }

    /// Get the raw metrics text.
    pub fn raw(&self) -> &str {
        &self.raw
    }

    /// Get a counter value by name and labels.
    ///
    /// Returns 0.0 if the metric is not found.
    pub fn counter(&self, name: &str, labels: &[(&str, &str)]) -> f64 {
        get_counter(&self.raw, name, labels)
    }

    /// Get a gauge value by name and labels.
    ///
    /// Returns 0.0 if the metric is not found.
    pub fn gauge(&self, name: &str, labels: &[(&str, &str)]) -> f64 {
        get_gauge(&self.raw, name, labels)
    }

    /// Get a histogram count (_count suffix) by name and labels.
    pub fn histogram_count(&self, name: &str, labels: &[(&str, &str)]) -> u64 {
        get_histogram_count(&self.raw, name, labels)
    }

    /// Get a histogram sum (_sum suffix) by name and labels.
    pub fn histogram_sum(&self, name: &str, labels: &[(&str, &str)]) -> f64 {
        get_histogram_sum(&self.raw, name, labels)
    }
}

/// Helper for asserting metric changes between snapshots.
///
/// Provides fluent API for asserting that metrics changed as expected
/// between a "before" and "after" snapshot.
///
/// # Example
///
/// ```rust,ignore
/// let before = MetricsSnapshot::new(server.fetch_metrics().await);
/// // ... perform operations ...
/// let after = MetricsSnapshot::new(server.fetch_metrics().await);
///
/// MetricsDelta::new(before, after)
///     .assert_counter_increased("frogdb_commands_total", &[("command", "SET")], 1.0)
///     .assert_counter_increased_gte("frogdb_keyspace_hits_total", &[], 1.0)
///     .assert_histogram_recorded("frogdb_commands_duration_seconds", &[("command", "SET")], 1);
/// ```
#[derive(Debug)]
pub struct MetricsDelta {
    before: MetricsSnapshot,
    after: MetricsSnapshot,
}

impl MetricsDelta {
    /// Create a new delta from before and after snapshots.
    pub fn new(before: MetricsSnapshot, after: MetricsSnapshot) -> Self {
        Self { before, after }
    }

    /// Assert that a counter increased by exactly the expected amount.
    ///
    /// # Panics
    ///
    /// Panics if the counter didn't increase by the expected amount.
    pub fn assert_counter_increased(
        &self,
        name: &str,
        labels: &[(&str, &str)],
        expected_increase: f64,
    ) -> &Self {
        let before = self.before.counter(name, labels);
        let after = self.after.counter(name, labels);
        let actual_increase = after - before;

        assert!(
            (actual_increase - expected_increase).abs() < 0.001,
            "Counter {} with labels {:?} expected increase of {}, got {} (before={}, after={})",
            name,
            labels,
            expected_increase,
            actual_increase,
            before,
            after
        );
        self
    }

    /// Assert that a counter increased by at least the minimum amount.
    ///
    /// Useful for metrics that may be incremented by background processes
    /// or when the exact count varies.
    ///
    /// # Panics
    ///
    /// Panics if the counter didn't increase by at least the minimum.
    pub fn assert_counter_increased_gte(
        &self,
        name: &str,
        labels: &[(&str, &str)],
        min_increase: f64,
    ) -> &Self {
        let before = self.before.counter(name, labels);
        let after = self.after.counter(name, labels);
        let actual_increase = after - before;

        assert!(
            actual_increase >= min_increase - 0.001,
            "Counter {} with labels {:?} expected increase >= {}, got {} (before={}, after={})",
            name,
            labels,
            min_increase,
            actual_increase,
            before,
            after
        );
        self
    }

    /// Assert that a histogram recorded exactly the expected number of observations.
    ///
    /// # Panics
    ///
    /// Panics if the histogram count didn't increase by the expected amount.
    pub fn assert_histogram_recorded(
        &self,
        name: &str,
        labels: &[(&str, &str)],
        expected_count: u64,
    ) -> &Self {
        let before = self.before.histogram_count(name, labels);
        let after = self.after.histogram_count(name, labels);
        let actual_increase = after - before;

        assert_eq!(
            actual_increase, expected_count,
            "Histogram {} with labels {:?} expected {} new observations, got {} (before={}, after={})",
            name, labels, expected_count, actual_increase, before, after
        );
        self
    }

    /// Assert that a histogram recorded at least the minimum number of observations.
    ///
    /// # Panics
    ///
    /// Panics if the histogram count didn't increase by at least the minimum.
    pub fn assert_histogram_recorded_gte(
        &self,
        name: &str,
        labels: &[(&str, &str)],
        min_count: u64,
    ) -> &Self {
        let before = self.before.histogram_count(name, labels);
        let after = self.after.histogram_count(name, labels);
        let actual_increase = after - before;

        assert!(
            actual_increase >= min_count,
            "Histogram {} with labels {:?} expected >= {} new observations, got {} (before={}, after={})",
            name,
            labels,
            min_count,
            actual_increase,
            before,
            after
        );
        self
    }

    /// Assert that a gauge has the expected value after operations.
    ///
    /// # Panics
    ///
    /// Panics if the gauge doesn't have the expected value.
    pub fn assert_gauge_eq(&self, name: &str, labels: &[(&str, &str)], expected: f64) -> &Self {
        let actual = self.after.gauge(name, labels);
        assert!(
            (actual - expected).abs() < 0.001,
            "Gauge {} with labels {:?} expected {}, got {}",
            name,
            labels,
            expected,
            actual
        );
        self
    }

    /// Assert that a gauge is at least the minimum value after operations.
    ///
    /// # Panics
    ///
    /// Panics if the gauge is less than the minimum.
    pub fn assert_gauge_gte(&self, name: &str, labels: &[(&str, &str)], min: f64) -> &Self {
        let actual = self.after.gauge(name, labels);
        assert!(
            actual >= min - 0.001,
            "Gauge {} with labels {:?} expected >= {}, got {}",
            name,
            labels,
            min,
            actual
        );
        self
    }

    /// Get the counter delta (after - before).
    pub fn counter_delta(&self, name: &str, labels: &[(&str, &str)]) -> f64 {
        self.after.counter(name, labels) - self.before.counter(name, labels)
    }

    /// Get the histogram count delta (after - before).
    pub fn histogram_count_delta(&self, name: &str, labels: &[(&str, &str)]) -> u64 {
        self.after.histogram_count(name, labels) - self.before.histogram_count(name, labels)
    }
}

/// Assert a counter delta between before and after metrics.
///
/// This macro is useful for inline delta assertions without creating
/// a full `MetricsDelta` struct.
///
/// # Example
///
/// ```rust,ignore
/// let before = server.fetch_metrics().await;
/// client.command(&["SET", "key", "value"]).await;
/// let after = server.fetch_metrics().await;
///
/// assert_counter_delta!(&before, &after, "frogdb_commands_total", &[("command", "SET")], 1.0);
/// ```
#[macro_export]
macro_rules! assert_counter_delta {
    ($before:expr, $after:expr, $name:expr, $labels:expr, $expected:expr) => {{
        let labels: &[(&str, &str)] = $labels;
        let before_val = $crate::testing::get_counter($before, $name, labels);
        let after_val = $crate::testing::get_counter($after, $name, labels);
        let actual_delta = after_val - before_val;
        assert!(
            (actual_delta - $expected as f64).abs() < 0.001,
            "Counter {} with labels {:?} expected delta of {}, got {} (before={}, after={})",
            $name,
            labels,
            $expected,
            actual_delta,
            before_val,
            after_val
        );
    }};
}

/// Assert a counter delta is at least the minimum between before and after metrics.
#[macro_export]
macro_rules! assert_counter_delta_gte {
    ($before:expr, $after:expr, $name:expr, $labels:expr, $min:expr) => {{
        let labels: &[(&str, &str)] = $labels;
        let before_val = $crate::testing::get_counter($before, $name, labels);
        let after_val = $crate::testing::get_counter($after, $name, labels);
        let actual_delta = after_val - before_val;
        assert!(
            actual_delta >= $min as f64 - 0.001,
            "Counter {} with labels {:?} expected delta >= {}, got {} (before={}, after={})",
            $name,
            labels,
            $min,
            actual_delta,
            before_val,
            after_val
        );
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_metric() {
        let text = "my_metric 42";
        let samples = parse_prometheus(text);
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].name, "my_metric");
        assert_eq!(samples[0].value, 42.0);
        assert!(samples[0].labels.is_empty());
    }

    #[test]
    fn test_parse_metric_with_labels() {
        let text = r#"my_metric{foo="bar",baz="qux"} 123.45"#;
        let samples = parse_prometheus(text);
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].name, "my_metric");
        assert_eq!(samples[0].value, 123.45);
        assert_eq!(samples[0].labels.get("foo"), Some(&"bar".to_string()));
        assert_eq!(samples[0].labels.get("baz"), Some(&"qux".to_string()));
    }

    #[test]
    fn test_parse_metric_with_escaped_quotes() {
        let text = r#"my_metric{msg="say \"hello\""} 1"#;
        let samples = parse_prometheus(text);
        assert_eq!(samples.len(), 1);
        assert_eq!(
            samples[0].labels.get("msg"),
            Some(&r#"say "hello""#.to_string())
        );
    }

    #[test]
    fn test_parse_metric_with_escaped_backslash() {
        let text = r#"my_metric{path="C:\\Users\\test"} 1"#;
        let samples = parse_prometheus(text);
        assert_eq!(samples.len(), 1);
        assert_eq!(
            samples[0].labels.get("path"),
            Some(&r#"C:\Users\test"#.to_string())
        );
    }

    #[test]
    fn test_parse_metric_with_newline_escape() {
        let text = r#"my_metric{msg="line1\nline2"} 1"#;
        let samples = parse_prometheus(text);
        assert_eq!(samples.len(), 1);
        assert_eq!(
            samples[0].labels.get("msg"),
            Some(&"line1\nline2".to_string())
        );
    }

    #[test]
    fn test_parse_skips_comments() {
        let text = r#"
# HELP my_metric A test metric
# TYPE my_metric counter
my_metric 42
        "#;
        let samples = parse_prometheus(text);
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].name, "my_metric");
    }

    #[test]
    fn test_parse_multiple_metrics() {
        let text = r#"
metric_a 1
metric_b{label="value"} 2
metric_c 3
        "#;
        let samples = parse_prometheus(text);
        assert_eq!(samples.len(), 3);
    }

    #[test]
    fn test_find_metric_exact_labels() {
        let text = r#"
my_metric{cmd="GET",status="ok"} 10
my_metric{cmd="SET",status="ok"} 20
my_metric{cmd="GET",status="error"} 5
        "#;
        let samples = parse_prometheus(text);

        let found = find_metric(&samples, "my_metric", &[("cmd", "GET"), ("status", "ok")]);
        assert!(found.is_some());
        assert_eq!(found.unwrap().value, 10.0);
    }

    #[test]
    fn test_find_metric_partial_labels() {
        let text = r#"my_metric{cmd="GET",extra="ignored"} 42"#;
        let samples = parse_prometheus(text);

        let found = find_metric(&samples, "my_metric", &[("cmd", "GET")]);
        assert!(found.is_some());
        assert_eq!(found.unwrap().value, 42.0);
    }

    #[test]
    fn test_find_metric_not_found() {
        let text = r#"my_metric{cmd="GET"} 42"#;
        let samples = parse_prometheus(text);

        let found = find_metric(&samples, "my_metric", &[("cmd", "SET")]);
        assert!(found.is_none());
    }

    #[test]
    fn test_get_counter() {
        let text = r#"requests_total{method="GET"} 100"#;
        let value = get_counter(text, "requests_total", &[("method", "GET")]);
        assert_eq!(value, 100.0);
    }

    #[test]
    fn test_get_counter_not_found_returns_zero() {
        let text = r#"requests_total{method="GET"} 100"#;
        let value = get_counter(text, "requests_total", &[("method", "POST")]);
        assert_eq!(value, 0.0);
    }

    #[test]
    fn test_get_histogram_count() {
        let text = r#"
request_duration_seconds_bucket{le="0.1"} 10
request_duration_seconds_bucket{le="0.5"} 50
request_duration_seconds_bucket{le="+Inf"} 100
request_duration_seconds_count 100
request_duration_seconds_sum 25.5
        "#;

        let count = get_histogram_count(text, "request_duration_seconds", &[]);
        assert_eq!(count, 100);

        let sum = get_histogram_sum(text, "request_duration_seconds", &[]);
        assert_eq!(sum, 25.5);
    }

    #[test]
    fn test_get_histogram_buckets() {
        let text = r#"
my_histogram_bucket{le="0.1"} 10
my_histogram_bucket{le="0.5"} 50
my_histogram_bucket{le="1.0"} 80
my_histogram_bucket{le="+Inf"} 100
        "#;

        let buckets = get_histogram_buckets(text, "my_histogram", &[]);
        assert_eq!(buckets.len(), 4);
        assert_eq!(buckets[0], (0.1, 10));
        assert_eq!(buckets[1], (0.5, 50));
        assert_eq!(buckets[2], (1.0, 80));
        assert_eq!(buckets[3], (f64::INFINITY, 100));
    }

    #[test]
    fn test_get_histogram_buckets_with_labels() {
        let text = r#"
my_histogram_bucket{cmd="GET",le="0.1"} 10
my_histogram_bucket{cmd="GET",le="1.0"} 50
my_histogram_bucket{cmd="SET",le="0.1"} 5
my_histogram_bucket{cmd="SET",le="1.0"} 20
        "#;

        let get_buckets = get_histogram_buckets(text, "my_histogram", &[("cmd", "GET")]);
        assert_eq!(get_buckets.len(), 2);
        assert_eq!(get_buckets[0], (0.1, 10));
        assert_eq!(get_buckets[1], (1.0, 50));

        let set_buckets = get_histogram_buckets(text, "my_histogram", &[("cmd", "SET")]);
        assert_eq!(set_buckets.len(), 2);
        assert_eq!(set_buckets[0], (0.1, 5));
    }

    #[test]
    fn test_assert_counter_eq_macro() {
        let text = r#"my_counter{label="value"} 42"#;
        assert_counter_eq!(&text, "my_counter", &[("label", "value")], 42);
    }

    #[test]
    fn test_assert_counter_gte_macro() {
        let text = r#"my_counter 100"#;
        assert_counter_gte!(&text, "my_counter", &[], 50);
        assert_counter_gte!(&text, "my_counter", &[], 100);
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_assert_gauge_eq_macro() {
        let text = r#"my_gauge 3.14"#;
        assert_gauge_eq!(&text, "my_gauge", &[], 3.14);
    }

    #[test]
    fn test_parse_scientific_notation() {
        let text = "my_metric 1.5e-3";
        let samples = parse_prometheus(text);
        assert_eq!(samples.len(), 1);
        assert!((samples[0].value - 0.0015).abs() < 1e-10);
    }

    #[test]
    fn test_parse_negative_value() {
        let text = "my_metric -42.5";
        let samples = parse_prometheus(text);
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].value, -42.5);
    }

    #[test]
    fn test_parse_inf_values() {
        let text = r#"
my_metric_pos +Inf
my_metric_neg -Inf
        "#;
        let samples = parse_prometheus(text);
        assert_eq!(samples.len(), 2);
        assert!(samples[0].value.is_infinite() && samples[0].value > 0.0);
        assert!(samples[1].value.is_infinite() && samples[1].value < 0.0);
    }

    // =========================================================================
    // MetricsSnapshot Tests
    // =========================================================================

    #[test]
    fn test_metrics_snapshot_counter() {
        let text = r#"my_counter{cmd="SET"} 42"#.to_string();
        let snapshot = MetricsSnapshot::new(text);

        assert_eq!(snapshot.counter("my_counter", &[("cmd", "SET")]), 42.0);
        assert_eq!(snapshot.counter("my_counter", &[("cmd", "GET")]), 0.0);
        assert_eq!(snapshot.counter("nonexistent", &[]), 0.0);
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_metrics_snapshot_gauge() {
        let text = r#"my_gauge 3.14"#.to_string();
        let snapshot = MetricsSnapshot::new(text);

        assert!((snapshot.gauge("my_gauge", &[]) - 3.14).abs() < 0.001);
    }

    #[test]
    fn test_metrics_snapshot_histogram() {
        let text = r#"
my_histogram_count{cmd="GET"} 100
my_histogram_sum{cmd="GET"} 25.5
        "#
        .to_string();
        let snapshot = MetricsSnapshot::new(text);

        assert_eq!(
            snapshot.histogram_count("my_histogram", &[("cmd", "GET")]),
            100
        );
        assert!((snapshot.histogram_sum("my_histogram", &[("cmd", "GET")]) - 25.5).abs() < 0.001);
    }

    // =========================================================================
    // MetricsDelta Tests
    // =========================================================================

    #[test]
    fn test_metrics_delta_counter_increased() {
        let before = MetricsSnapshot::new(r#"my_counter{cmd="SET"} 10"#.to_string());
        let after = MetricsSnapshot::new(r#"my_counter{cmd="SET"} 15"#.to_string());

        let delta = MetricsDelta::new(before, after);
        delta.assert_counter_increased("my_counter", &[("cmd", "SET")], 5.0);
    }

    #[test]
    fn test_metrics_delta_counter_increased_gte() {
        let before = MetricsSnapshot::new(r#"my_counter{cmd="SET"} 10"#.to_string());
        let after = MetricsSnapshot::new(r#"my_counter{cmd="SET"} 20"#.to_string());

        let delta = MetricsDelta::new(before, after);
        delta.assert_counter_increased_gte("my_counter", &[("cmd", "SET")], 5.0);
        delta.assert_counter_increased_gte("my_counter", &[("cmd", "SET")], 10.0);
    }

    #[test]
    fn test_metrics_delta_histogram_recorded() {
        let before = MetricsSnapshot::new(r#"my_histogram_count{cmd="GET"} 50"#.to_string());
        let after = MetricsSnapshot::new(r#"my_histogram_count{cmd="GET"} 55"#.to_string());

        let delta = MetricsDelta::new(before, after);
        delta.assert_histogram_recorded("my_histogram", &[("cmd", "GET")], 5);
    }

    #[test]
    fn test_metrics_delta_histogram_recorded_gte() {
        let before = MetricsSnapshot::new(r#"my_histogram_count{cmd="GET"} 50"#.to_string());
        let after = MetricsSnapshot::new(r#"my_histogram_count{cmd="GET"} 60"#.to_string());

        let delta = MetricsDelta::new(before, after);
        delta.assert_histogram_recorded_gte("my_histogram", &[("cmd", "GET")], 5);
        delta.assert_histogram_recorded_gte("my_histogram", &[("cmd", "GET")], 10);
    }

    #[test]
    fn test_metrics_delta_gauge_eq() {
        let before = MetricsSnapshot::new(r#"my_gauge 5"#.to_string());
        let after = MetricsSnapshot::new(r#"my_gauge 10"#.to_string());

        let delta = MetricsDelta::new(before, after);
        delta.assert_gauge_eq("my_gauge", &[], 10.0);
    }

    #[test]
    fn test_metrics_delta_chaining() {
        let before = MetricsSnapshot::new(
            r#"
counter_a 10
counter_b 20
histogram_count 5
        "#
            .to_string(),
        );
        let after = MetricsSnapshot::new(
            r#"
counter_a 15
counter_b 25
histogram_count 10
        "#
            .to_string(),
        );

        // Test fluent chaining
        MetricsDelta::new(before, after)
            .assert_counter_increased("counter_a", &[], 5.0)
            .assert_counter_increased("counter_b", &[], 5.0)
            .assert_histogram_recorded("histogram", &[], 5);
    }

    #[test]
    fn test_metrics_delta_getters() {
        let before = MetricsSnapshot::new(r#"my_counter 10"#.to_string());
        let after = MetricsSnapshot::new(r#"my_counter 25"#.to_string());

        let delta = MetricsDelta::new(before, after);
        assert_eq!(delta.counter_delta("my_counter", &[]), 15.0);
    }

    #[test]
    fn test_assert_counter_delta_macro() {
        let before = r#"my_counter{cmd="SET"} 10"#;
        let after = r#"my_counter{cmd="SET"} 15"#;

        assert_counter_delta!(&before, &after, "my_counter", &[("cmd", "SET")], 5.0);
    }

    #[test]
    fn test_assert_counter_delta_gte_macro() {
        let before = r#"my_counter 10"#;
        let after = r#"my_counter 20"#;

        assert_counter_delta_gte!(&before, &after, "my_counter", &[], 5.0);
        assert_counter_delta_gte!(&before, &after, "my_counter", &[], 10.0);
    }
}
