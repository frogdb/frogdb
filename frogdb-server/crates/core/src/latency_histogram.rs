//! Server-wide per-command latency histograms for INFO latencystats.
//!
//! Uses a fixed-bucket histogram with 64 buckets covering 1us to ~8.6 seconds
//! using quarter-power-of-2 granularity. Each bucket uses an AtomicU64 counter
//! for lock-free concurrent recording from many connection tasks.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use dashmap::DashMap;

/// Number of histogram buckets (quarter-power-of-2 from 1us to ~8.6s).
const NUM_BUCKETS: usize = 64;

/// Fixed-bucket latency histogram using atomic counters.
pub struct LatencyHistogram {
    /// Bucket counts. Bucket i covers [bucket_lower_bound(i), bucket_lower_bound(i+1)).
    buckets: [AtomicU64; NUM_BUCKETS],
    /// Total number of samples recorded.
    count: AtomicU64,
}

impl LatencyHistogram {
    /// Create a new empty histogram.
    pub fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
        }
    }

    /// Record a latency sample in microseconds.
    pub fn record(&self, latency_us: u64) {
        let idx = bucket_index(latency_us);
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Compute the value at a given percentile (0.0-100.0).
    /// Returns the value in microseconds as a float.
    pub fn percentile(&self, p: f64) -> f64 {
        let total = self.count.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }

        let target = ((p / 100.0) * total as f64).ceil() as u64;
        let mut cumulative: u64 = 0;

        for i in 0..NUM_BUCKETS {
            cumulative += self.buckets[i].load(Ordering::Relaxed);
            if cumulative >= target {
                // Return the geometric mean of the bucket bounds for better accuracy
                let lower = bucket_lower_bound(i);
                let upper = if i + 1 < NUM_BUCKETS {
                    bucket_lower_bound(i + 1)
                } else {
                    lower * 2.0_f64.powf(0.25)
                };
                return (lower + upper) / 2.0;
            }
        }

        // Fallback: return upper bound of last bucket
        bucket_lower_bound(NUM_BUCKETS - 1)
    }

    /// Get the total number of samples.
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Reset all counters.
    pub fn reset(&self) {
        for bucket in &self.buckets {
            bucket.store(0, Ordering::Relaxed);
        }
        self.count.store(0, Ordering::Relaxed);
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

/// Converts a latency in microseconds to a bucket index.
///
/// Buckets use quarter-power-of-2 spacing: bucket i covers
/// [2^(i/4), 2^((i+1)/4)) microseconds.
fn bucket_index(latency_us: u64) -> usize {
    if latency_us <= 1 {
        return 0;
    }
    // index = floor(4 * log2(latency_us))
    let log2 = (latency_us as f64).log2();
    let idx = (log2 * 4.0).floor() as usize;
    idx.min(NUM_BUCKETS - 1)
}

/// Returns the lower bound (in microseconds) for a given bucket.
fn bucket_lower_bound(index: usize) -> f64 {
    2.0_f64.powf(index as f64 / 4.0)
}

/// Server-wide per-command latency histograms.
pub struct CommandLatencyHistograms {
    /// Map from lowercase command name to its histogram.
    histograms: DashMap<String, LatencyHistogram>,
    /// Whether tracking is enabled (checked before recording).
    enabled: AtomicBool,
}

impl CommandLatencyHistograms {
    /// Create a new histogram container.
    pub fn new(enabled: bool) -> Self {
        Self {
            histograms: DashMap::new(),
            enabled: AtomicBool::new(enabled),
        }
    }

    /// Record a command latency sample.
    /// `cmd_name` should be lowercase (e.g., "get", "client|id").
    pub fn record(&self, cmd_name: &str, latency_us: u64) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }
        let cmd_lower = cmd_name.to_ascii_lowercase();
        self.histograms
            .entry(cmd_lower)
            .or_default()
            .record(latency_us);
    }

    /// Check if tracking is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Set the enabled state.
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    /// Reset all histograms (clear all data).
    pub fn reset(&self) {
        self.histograms.clear();
    }

    /// Get percentiles for a specific command.
    /// Returns None if no data exists for the command.
    pub fn percentiles_for(&self, cmd_name: &str, percentiles: &[f64]) -> Option<Vec<(f64, f64)>> {
        let hist = self.histograms.get(cmd_name)?;
        if hist.count() == 0 {
            return None;
        }
        Some(
            percentiles
                .iter()
                .map(|&p| (p, hist.percentile(p)))
                .collect(),
        )
    }

    /// Get all command names that have recorded data.
    pub fn all_commands(&self) -> Vec<String> {
        self.histograms
            .iter()
            .filter(|entry| entry.value().count() > 0)
            .map(|entry| entry.key().clone())
            .collect()
    }
}

impl Default for CommandLatencyHistograms {
    fn default() -> Self {
        Self::new(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucket_index_boundaries() {
        assert_eq!(bucket_index(0), 0);
        assert_eq!(bucket_index(1), 0);
        // 2^(1/4) ~= 1.19, so latency 2 should be in bucket 4 (2^(4/4) = 2)
        assert_eq!(bucket_index(2), 4);
        // 2^(8/4) = 4
        assert_eq!(bucket_index(4), 8);
        // 2^(40/4) = 1024
        assert_eq!(bucket_index(1024), 40);
    }

    #[test]
    fn test_bucket_index_clamped() {
        // Very large values should clamp to last bucket
        assert_eq!(bucket_index(u64::MAX), NUM_BUCKETS - 1);
    }

    #[test]
    fn test_histogram_record_and_percentile() {
        let hist = LatencyHistogram::new();

        // Record 100 samples: 1, 2, 3, ..., 100
        for i in 1..=100 {
            hist.record(i);
        }

        assert_eq!(hist.count(), 100);

        // p50 should be around 50us
        let p50 = hist.percentile(50.0);
        assert!(p50 > 30.0 && p50 < 70.0, "p50 = {}", p50);

        // p99 should be around 99us
        let p99 = hist.percentile(99.0);
        assert!(p99 > 60.0 && p99 < 130.0, "p99 = {}", p99);
    }

    #[test]
    fn test_histogram_reset() {
        let hist = LatencyHistogram::new();
        hist.record(100);
        hist.record(200);
        assert_eq!(hist.count(), 2);

        hist.reset();
        assert_eq!(hist.count(), 0);
        assert_eq!(hist.percentile(50.0), 0.0);
    }

    #[test]
    fn test_command_histograms_enabled() {
        let histograms = CommandLatencyHistograms::new(true);
        histograms.record("get", 100);
        histograms.record("set", 200);

        let cmds = histograms.all_commands();
        assert!(cmds.contains(&"get".to_string()));
        assert!(cmds.contains(&"set".to_string()));
    }

    #[test]
    fn test_command_histograms_disabled() {
        let histograms = CommandLatencyHistograms::new(false);
        histograms.record("get", 100);

        let cmds = histograms.all_commands();
        assert!(cmds.is_empty());
    }

    #[test]
    fn test_command_histograms_toggle() {
        let histograms = CommandLatencyHistograms::new(true);
        histograms.record("get", 100);
        assert_eq!(histograms.all_commands().len(), 1);

        histograms.set_enabled(false);
        histograms.record("set", 200);
        // set should not appear since tracking was disabled
        assert!(!histograms.all_commands().contains(&"set".to_string()));

        histograms.set_enabled(true);
        histograms.record("set", 200);
        assert!(histograms.all_commands().contains(&"set".to_string()));
    }

    #[test]
    fn test_command_histograms_reset() {
        let histograms = CommandLatencyHistograms::new(true);
        histograms.record("get", 100);
        histograms.record("set", 200);

        histograms.reset();
        assert!(histograms.all_commands().is_empty());
    }

    #[test]
    fn test_percentiles_for() {
        let histograms = CommandLatencyHistograms::new(true);
        for i in 1..=1000 {
            histograms.record("get", i);
        }

        let result = histograms.percentiles_for("get", &[50.0, 99.0, 99.9]);
        assert!(result.is_some());
        let vals = result.unwrap();
        assert_eq!(vals.len(), 3);
        // Each value should be (percentile, microseconds)
        assert_eq!(vals[0].0, 50.0);
        assert_eq!(vals[1].0, 99.0);
        assert_eq!(vals[2].0, 99.9);
    }

    #[test]
    fn test_percentiles_for_missing_command() {
        let histograms = CommandLatencyHistograms::new(true);
        assert!(histograms.percentiles_for("nonexistent", &[50.0]).is_none());
    }

    #[test]
    fn test_case_insensitive_recording() {
        let histograms = CommandLatencyHistograms::new(true);
        histograms.record("GET", 100);
        histograms.record("Get", 200);
        histograms.record("get", 300);

        // All should go to the same histogram
        let cmds = histograms.all_commands();
        assert_eq!(cmds.len(), 1);
        assert!(cmds.contains(&"get".to_string()));
    }
}
