//! Latency monitoring for tracking event latencies and command execution times.
//!
//! Each shard maintains its own LatencyMonitor to avoid cross-shard locking.
//! Events are recorded when they exceed a configurable threshold.

use std::collections::{HashMap, VecDeque};

/// Default threshold in milliseconds. Events taking longer are recorded.
/// Set to 0 to record all events.
pub const DEFAULT_LATENCY_THRESHOLD_MS: u64 = 0;

/// Default maximum history entries per event type.
pub const DEFAULT_LATENCY_HISTORY_LEN: usize = 160;

/// Types of latency events that can be tracked.
/// These match Redis's latency event types for compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LatencyEvent {
    /// Command execution latency.
    Command,
    /// Fork operation (for background saves).
    Fork,
    /// AOF fsync operation.
    AofFsync,
    /// Expire cycle (key expiration).
    ExpireCycle,
    /// Eviction cycle (memory eviction).
    EvictionCycle,
    /// Snapshot I/O operations.
    SnapshotIo,
}

impl LatencyEvent {
    /// Get the string name of this event type.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Command => "command",
            Self::Fork => "fork",
            Self::AofFsync => "aof-fsync",
            Self::ExpireCycle => "expire-cycle",
            Self::EvictionCycle => "eviction-cycle",
            Self::SnapshotIo => "snapshot-io",
        }
    }

    /// Parse an event type from a string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "command" => Some(Self::Command),
            "fork" => Some(Self::Fork),
            "aof-fsync" | "aof_fsync" => Some(Self::AofFsync),
            "expire-cycle" | "expire_cycle" => Some(Self::ExpireCycle),
            "eviction-cycle" | "eviction_cycle" => Some(Self::EvictionCycle),
            "snapshot-io" | "snapshot_io" => Some(Self::SnapshotIo),
            _ => None,
        }
    }

    /// Get all event types.
    pub fn all() -> &'static [Self] {
        &[
            Self::Command,
            Self::Fork,
            Self::AofFsync,
            Self::ExpireCycle,
            Self::EvictionCycle,
            Self::SnapshotIo,
        ]
    }
}

impl std::fmt::Display for LatencyEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A single latency sample.
#[derive(Debug, Clone, Copy)]
pub struct LatencySample {
    /// Unix timestamp in seconds when the sample was recorded.
    pub timestamp: i64,
    /// Latency in milliseconds.
    pub latency_ms: u64,
}

impl LatencySample {
    /// Create a new sample with the current timestamp.
    pub fn new(latency_ms: u64) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        Self {
            timestamp,
            latency_ms,
        }
    }

    /// Create a new sample with a specific timestamp (for testing).
    pub fn with_timestamp(timestamp: i64, latency_ms: u64) -> Self {
        Self {
            timestamp,
            latency_ms,
        }
    }
}

/// Circular buffer for event history.
#[derive(Debug)]
pub struct EventHistory {
    /// Samples, newest first.
    samples: VecDeque<LatencySample>,
    /// Maximum number of samples to keep.
    max_len: usize,
}

impl EventHistory {
    /// Create a new event history with the specified capacity.
    pub fn new(max_len: usize) -> Self {
        Self {
            samples: VecDeque::with_capacity(max_len.min(256)),
            max_len,
        }
    }

    /// Add a sample to the history.
    pub fn add(&mut self, sample: LatencySample) {
        if self.samples.len() >= self.max_len {
            self.samples.pop_back();
        }
        self.samples.push_front(sample);
    }

    /// Get all samples, newest first.
    pub fn get_all(&self) -> Vec<LatencySample> {
        self.samples.iter().copied().collect()
    }

    /// Get the latest sample, if any.
    pub fn latest(&self) -> Option<LatencySample> {
        self.samples.front().copied()
    }

    /// Get the maximum latency in the history.
    pub fn max_latency(&self) -> Option<u64> {
        self.samples.iter().map(|s| s.latency_ms).max()
    }

    /// Get the minimum latency in the history.
    pub fn min_latency(&self) -> Option<u64> {
        self.samples.iter().map(|s| s.latency_ms).min()
    }

    /// Get the average latency in the history.
    pub fn avg_latency(&self) -> Option<u64> {
        if self.samples.is_empty() {
            return None;
        }
        let sum: u64 = self.samples.iter().map(|s| s.latency_ms).sum();
        Some(sum / self.samples.len() as u64)
    }

    /// Get the number of samples.
    pub fn len(&self) -> usize {
        self.samples.len()
    }

    /// Check if the history is empty.
    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    /// Clear all samples.
    pub fn reset(&mut self) {
        self.samples.clear();
    }
}

/// Per-shard latency monitor.
pub struct LatencyMonitor {
    /// Histories for each event type.
    histories: HashMap<LatencyEvent, EventHistory>,
    /// Threshold in milliseconds. Events below this are not recorded.
    threshold_ms: u64,
    /// Maximum history length per event type.
    history_len: usize,
}

impl LatencyMonitor {
    /// Create a new latency monitor.
    pub fn new(threshold_ms: u64, history_len: usize) -> Self {
        Self {
            histories: HashMap::new(),
            threshold_ms,
            history_len,
        }
    }

    /// Create a monitor with default settings.
    pub fn default_monitor() -> Self {
        Self::new(DEFAULT_LATENCY_THRESHOLD_MS, DEFAULT_LATENCY_HISTORY_LEN)
    }

    /// Record a latency event if it exceeds the threshold.
    pub fn record(&mut self, event: LatencyEvent, latency_ms: u64) {
        if latency_ms >= self.threshold_ms {
            let history = self
                .histories
                .entry(event)
                .or_insert_with(|| EventHistory::new(self.history_len));
            history.add(LatencySample::new(latency_ms));
        }
    }

    /// Record a latency event with a specific timestamp (for testing).
    pub fn record_with_timestamp(&mut self, event: LatencyEvent, timestamp: i64, latency_ms: u64) {
        if latency_ms >= self.threshold_ms {
            let history = self
                .histories
                .entry(event)
                .or_insert_with(|| EventHistory::new(self.history_len));
            history.add(LatencySample::with_timestamp(timestamp, latency_ms));
        }
    }

    /// Get the latest sample for each event type that has data.
    pub fn latest(&self) -> Vec<(LatencyEvent, LatencySample)> {
        let mut result = Vec::new();
        for event in LatencyEvent::all() {
            if let Some(history) = self.histories.get(event) {
                if let Some(sample) = history.latest() {
                    result.push((*event, sample));
                }
            }
        }
        result
    }

    /// Get the history for a specific event type.
    pub fn history(&self, event: LatencyEvent) -> Vec<LatencySample> {
        self.histories
            .get(&event)
            .map(|h| h.get_all())
            .unwrap_or_default()
    }

    /// Reset history for specific events, or all if empty.
    pub fn reset(&mut self, events: &[LatencyEvent]) {
        if events.is_empty() {
            // Reset all
            for history in self.histories.values_mut() {
                history.reset();
            }
        } else {
            // Reset specific events
            for event in events {
                if let Some(history) = self.histories.get_mut(event) {
                    history.reset();
                }
            }
        }
    }

    /// Set the threshold in milliseconds.
    pub fn set_threshold(&mut self, threshold_ms: u64) {
        self.threshold_ms = threshold_ms;
    }

    /// Get the current threshold.
    pub fn threshold(&self) -> u64 {
        self.threshold_ms
    }

    /// Get statistics for an event type.
    pub fn stats(&self, event: LatencyEvent) -> Option<EventStats> {
        self.histories.get(&event).and_then(|h| {
            if h.is_empty() {
                None
            } else {
                Some(EventStats {
                    count: h.len(),
                    min_ms: h.min_latency().unwrap_or(0),
                    max_ms: h.max_latency().unwrap_or(0),
                    avg_ms: h.avg_latency().unwrap_or(0),
                })
            }
        })
    }
}

impl std::fmt::Debug for LatencyMonitor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LatencyMonitor")
            .field("threshold_ms", &self.threshold_ms)
            .field("history_len", &self.history_len)
            .field("active_events", &self.histories.len())
            .finish()
    }
}

/// Statistics for an event type.
#[derive(Debug, Clone, Copy)]
pub struct EventStats {
    /// Number of samples.
    pub count: usize,
    /// Minimum latency in milliseconds.
    pub min_ms: u64,
    /// Maximum latency in milliseconds.
    pub max_ms: u64,
    /// Average latency in milliseconds.
    pub avg_ms: u64,
}

/// Exponential bucket histogram for command latencies.
/// Buckets are powers of 2: [0-1), [1-2), [2-4), [4-8), ...
pub struct CommandHistogram {
    /// Bucket counts. Index 0 = [0-1)ms, index 1 = [1-2)ms, index 2 = [2-4)ms, etc.
    buckets: Vec<u64>,
    /// Total count of all samples.
    total_count: u64,
    /// Sum of all latencies (for average calculation).
    total_latency_us: u64,
    /// Minimum latency observed (microseconds).
    min_latency_us: Option<u64>,
    /// Maximum latency observed (microseconds).
    max_latency_us: Option<u64>,
}

impl CommandHistogram {
    /// Number of buckets (covers up to 2^32 ms ~= 49 days).
    const NUM_BUCKETS: usize = 32;

    /// Create a new histogram.
    pub fn new() -> Self {
        Self {
            buckets: vec![0; Self::NUM_BUCKETS],
            total_count: 0,
            total_latency_us: 0,
            min_latency_us: None,
            max_latency_us: None,
        }
    }

    /// Record a latency in microseconds.
    pub fn record(&mut self, latency_us: u64) {
        // Update min/max
        self.min_latency_us = Some(
            self.min_latency_us
                .map(|m| m.min(latency_us))
                .unwrap_or(latency_us),
        );
        self.max_latency_us = Some(
            self.max_latency_us
                .map(|m| m.max(latency_us))
                .unwrap_or(latency_us),
        );

        // Update totals
        self.total_count += 1;
        self.total_latency_us += latency_us;

        // Convert to milliseconds for bucketing
        let latency_ms = latency_us / 1000;

        // Find bucket index (log2 of latency_ms, with 0ms going to bucket 0)
        let bucket_idx = if latency_ms == 0 {
            0
        } else {
            // 64 - leading_zeros gives us floor(log2(x)) + 1
            // For latency_ms=1, this gives bucket 1 ([1-2)ms)
            let idx = (64 - latency_ms.leading_zeros()) as usize;
            idx.min(Self::NUM_BUCKETS - 1)
        };

        self.buckets[bucket_idx] += 1;
    }

    /// Get bucket data: (lower_bound_ms, upper_bound_ms, count) for each non-empty bucket.
    pub fn get_buckets(&self) -> Vec<(u64, u64, u64)> {
        self.buckets
            .iter()
            .enumerate()
            .filter(|(_, &count)| count > 0)
            .map(|(idx, &count)| {
                let lower = if idx == 0 { 0 } else { 1u64 << (idx - 1) };
                let upper = 1u64 << idx;
                (lower, upper, count)
            })
            .collect()
    }

    /// Get the total count of samples.
    pub fn total_count(&self) -> u64 {
        self.total_count
    }

    /// Get the minimum latency observed (microseconds).
    pub fn min_latency_us(&self) -> Option<u64> {
        self.min_latency_us
    }

    /// Get the maximum latency observed (microseconds).
    pub fn max_latency_us(&self) -> Option<u64> {
        self.max_latency_us
    }

    /// Get the average latency (microseconds).
    pub fn avg_latency_us(&self) -> Option<u64> {
        if self.total_count == 0 {
            None
        } else {
            Some(self.total_latency_us / self.total_count)
        }
    }

    /// Reset the histogram.
    pub fn reset(&mut self) {
        self.buckets.fill(0);
        self.total_count = 0;
        self.total_latency_us = 0;
        self.min_latency_us = None;
        self.max_latency_us = None;
    }

    /// Merge another histogram into this one.
    pub fn merge(&mut self, other: &CommandHistogram) {
        for (i, &count) in other.buckets.iter().enumerate() {
            self.buckets[i] += count;
        }
        self.total_count += other.total_count;
        self.total_latency_us += other.total_latency_us;

        if let Some(other_min) = other.min_latency_us {
            self.min_latency_us = Some(
                self.min_latency_us
                    .map(|m| m.min(other_min))
                    .unwrap_or(other_min),
            );
        }
        if let Some(other_max) = other.max_latency_us {
            self.max_latency_us = Some(
                self.max_latency_us
                    .map(|m| m.max(other_max))
                    .unwrap_or(other_max),
            );
        }
    }
}

impl Default for CommandHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for CommandHistogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandHistogram")
            .field("total_count", &self.total_count)
            .field("min_latency_us", &self.min_latency_us)
            .field("max_latency_us", &self.max_latency_us)
            .finish()
    }
}

/// Generate an ASCII latency graph for a series of samples.
/// Returns a multi-line string suitable for display.
pub fn generate_latency_graph(event: LatencyEvent, samples: &[LatencySample]) -> String {
    if samples.is_empty() {
        return format!("{} - no data available", event.as_str());
    }

    let mut lines = Vec::new();

    // Header
    lines.push(format!("{} - {} samples", event.as_str(), samples.len()));

    // Find min/max for scaling
    let min_latency = samples.iter().map(|s| s.latency_ms).min().unwrap_or(0);
    let max_latency = samples.iter().map(|s| s.latency_ms).max().unwrap_or(0);

    if max_latency == 0 {
        lines.push("(all samples have 0ms latency)".to_string());
        return lines.join("\n");
    }

    // Graph dimensions
    const GRAPH_HEIGHT: usize = 8;
    const GRAPH_WIDTH: usize = 60;

    // Sample the data if we have too many points
    let display_samples: Vec<LatencySample> = if samples.len() <= GRAPH_WIDTH {
        samples.to_vec()
    } else {
        // Sample evenly
        let step = samples.len() as f64 / GRAPH_WIDTH as f64;
        (0..GRAPH_WIDTH)
            .map(|i| samples[(i as f64 * step) as usize])
            .collect()
    };

    // Build the graph grid
    let scale = max_latency as f64 / GRAPH_HEIGHT as f64;
    let mut grid: Vec<Vec<char>> = vec![vec![' '; display_samples.len()]; GRAPH_HEIGHT];

    for (x, sample) in display_samples.iter().enumerate() {
        let normalized = if max_latency > 0 {
            (sample.latency_ms as f64 / max_latency as f64 * (GRAPH_HEIGHT - 1) as f64) as usize
        } else {
            0
        };
        // Fill from bottom up to the value
        for y in 0..=normalized {
            grid[GRAPH_HEIGHT - 1 - y][x] = if y == normalized { '#' } else { '|' };
        }
    }

    // Add y-axis labels and render
    for (i, row) in grid.iter().enumerate() {
        let label_value = ((GRAPH_HEIGHT - 1 - i) as f64 * scale) as u64;
        let row_str: String = row.iter().collect();
        if i == 0 {
            lines.push(format!("{:>6}ms |{}", max_latency, row_str));
        } else if i == GRAPH_HEIGHT - 1 {
            lines.push(format!("{:>6}ms |{}", min_latency, row_str));
        } else if i == GRAPH_HEIGHT / 2 {
            lines.push(format!("{:>6}ms |{}", label_value, row_str));
        } else {
            lines.push(format!("        |{}", row_str));
        }
    }

    // X-axis
    lines.push(format!("        +{}", "-".repeat(display_samples.len())));

    // Time range
    if let (Some(oldest), Some(newest)) = (samples.last(), samples.first()) {
        lines.push(format!(
            "        oldest {:>10} newest {:>10}",
            oldest.timestamp, newest.timestamp
        ));
    }

    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_latency_event_from_str() {
        assert_eq!(LatencyEvent::from_str("command"), Some(LatencyEvent::Command));
        assert_eq!(LatencyEvent::from_str("COMMAND"), Some(LatencyEvent::Command));
        assert_eq!(LatencyEvent::from_str("expire-cycle"), Some(LatencyEvent::ExpireCycle));
        assert_eq!(LatencyEvent::from_str("expire_cycle"), Some(LatencyEvent::ExpireCycle));
        assert_eq!(LatencyEvent::from_str("unknown"), None);
    }

    #[test]
    fn test_latency_event_as_str() {
        assert_eq!(LatencyEvent::Command.as_str(), "command");
        assert_eq!(LatencyEvent::ExpireCycle.as_str(), "expire-cycle");
    }

    #[test]
    fn test_event_history_add_and_get() {
        let mut history = EventHistory::new(10);

        history.add(LatencySample::with_timestamp(1000, 50));
        history.add(LatencySample::with_timestamp(1001, 100));
        history.add(LatencySample::with_timestamp(1002, 75));

        assert_eq!(history.len(), 3);

        let samples = history.get_all();
        assert_eq!(samples.len(), 3);
        // Newest first
        assert_eq!(samples[0].latency_ms, 75);
        assert_eq!(samples[1].latency_ms, 100);
        assert_eq!(samples[2].latency_ms, 50);
    }

    #[test]
    fn test_event_history_capacity() {
        let mut history = EventHistory::new(3);

        for i in 0..5 {
            history.add(LatencySample::with_timestamp(1000 + i, (i + 1) as u64 * 10));
        }

        assert_eq!(history.len(), 3);

        let samples = history.get_all();
        // Should have the 3 newest
        assert_eq!(samples[0].latency_ms, 50);
        assert_eq!(samples[1].latency_ms, 40);
        assert_eq!(samples[2].latency_ms, 30);
    }

    #[test]
    fn test_event_history_statistics() {
        let mut history = EventHistory::new(10);

        history.add(LatencySample::with_timestamp(1000, 50));
        history.add(LatencySample::with_timestamp(1001, 100));
        history.add(LatencySample::with_timestamp(1002, 75));

        assert_eq!(history.min_latency(), Some(50));
        assert_eq!(history.max_latency(), Some(100));
        assert_eq!(history.avg_latency(), Some(75)); // (50 + 100 + 75) / 3 = 75
    }

    #[test]
    fn test_event_history_latest() {
        let mut history = EventHistory::new(10);

        history.add(LatencySample::with_timestamp(1000, 50));
        history.add(LatencySample::with_timestamp(1001, 100));

        let latest = history.latest().unwrap();
        assert_eq!(latest.latency_ms, 100);
        assert_eq!(latest.timestamp, 1001);
    }

    #[test]
    fn test_event_history_reset() {
        let mut history = EventHistory::new(10);

        history.add(LatencySample::with_timestamp(1000, 50));
        history.add(LatencySample::with_timestamp(1001, 100));

        assert_eq!(history.len(), 2);

        history.reset();

        assert!(history.is_empty());
        assert_eq!(history.len(), 0);
    }

    #[test]
    fn test_latency_monitor_record_and_latest() {
        let mut monitor = LatencyMonitor::new(0, 10);

        monitor.record_with_timestamp(LatencyEvent::Command, 1000, 50);
        monitor.record_with_timestamp(LatencyEvent::ExpireCycle, 1001, 100);

        let latest = monitor.latest();
        assert_eq!(latest.len(), 2);

        // Find Command event
        let cmd_sample = latest
            .iter()
            .find(|(e, _)| *e == LatencyEvent::Command)
            .map(|(_, s)| s);
        assert!(cmd_sample.is_some());
        assert_eq!(cmd_sample.unwrap().latency_ms, 50);
    }

    #[test]
    fn test_latency_monitor_threshold() {
        let mut monitor = LatencyMonitor::new(100, 10);

        // Below threshold - should not be recorded
        monitor.record_with_timestamp(LatencyEvent::Command, 1000, 50);
        // At threshold - should be recorded
        monitor.record_with_timestamp(LatencyEvent::Command, 1001, 100);
        // Above threshold - should be recorded
        monitor.record_with_timestamp(LatencyEvent::Command, 1002, 150);

        let history = monitor.history(LatencyEvent::Command);
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].latency_ms, 150);
        assert_eq!(history[1].latency_ms, 100);
    }

    #[test]
    fn test_latency_monitor_history() {
        let mut monitor = LatencyMonitor::new(0, 10);

        monitor.record_with_timestamp(LatencyEvent::Command, 1000, 50);
        monitor.record_with_timestamp(LatencyEvent::Command, 1001, 100);
        monitor.record_with_timestamp(LatencyEvent::Command, 1002, 75);

        let history = monitor.history(LatencyEvent::Command);
        assert_eq!(history.len(), 3);

        // Empty history for unrecorded event
        let empty_history = monitor.history(LatencyEvent::Fork);
        assert!(empty_history.is_empty());
    }

    #[test]
    fn test_latency_monitor_reset_all() {
        let mut monitor = LatencyMonitor::new(0, 10);

        monitor.record_with_timestamp(LatencyEvent::Command, 1000, 50);
        monitor.record_with_timestamp(LatencyEvent::ExpireCycle, 1001, 100);

        // Reset all
        monitor.reset(&[]);

        assert!(monitor.history(LatencyEvent::Command).is_empty());
        assert!(monitor.history(LatencyEvent::ExpireCycle).is_empty());
    }

    #[test]
    fn test_latency_monitor_reset_specific() {
        let mut monitor = LatencyMonitor::new(0, 10);

        monitor.record_with_timestamp(LatencyEvent::Command, 1000, 50);
        monitor.record_with_timestamp(LatencyEvent::ExpireCycle, 1001, 100);

        // Reset only Command
        monitor.reset(&[LatencyEvent::Command]);

        assert!(monitor.history(LatencyEvent::Command).is_empty());
        assert_eq!(monitor.history(LatencyEvent::ExpireCycle).len(), 1);
    }

    #[test]
    fn test_latency_monitor_stats() {
        let mut monitor = LatencyMonitor::new(0, 10);

        monitor.record_with_timestamp(LatencyEvent::Command, 1000, 50);
        monitor.record_with_timestamp(LatencyEvent::Command, 1001, 100);
        monitor.record_with_timestamp(LatencyEvent::Command, 1002, 75);

        let stats = monitor.stats(LatencyEvent::Command).unwrap();
        assert_eq!(stats.count, 3);
        assert_eq!(stats.min_ms, 50);
        assert_eq!(stats.max_ms, 100);
        assert_eq!(stats.avg_ms, 75);

        // No stats for unrecorded event
        assert!(monitor.stats(LatencyEvent::Fork).is_none());
    }

    #[test]
    fn test_command_histogram_record() {
        let mut hist = CommandHistogram::new();

        // Record some latencies in microseconds
        hist.record(500); // 0.5ms -> bucket 0
        hist.record(1500); // 1.5ms -> bucket 1
        hist.record(3000); // 3ms -> bucket 2
        hist.record(5000); // 5ms -> bucket 3
        hist.record(10000); // 10ms -> bucket 4

        assert_eq!(hist.total_count(), 5);
        assert_eq!(hist.min_latency_us(), Some(500));
        assert_eq!(hist.max_latency_us(), Some(10000));

        let buckets = hist.get_buckets();
        assert!(!buckets.is_empty());
    }

    #[test]
    fn test_command_histogram_buckets() {
        let mut hist = CommandHistogram::new();

        // All samples in [1-2)ms range
        for _ in 0..10 {
            hist.record(1500); // 1.5ms
        }

        let buckets = hist.get_buckets();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].2, 10); // count = 10
    }

    #[test]
    fn test_command_histogram_merge() {
        let mut hist1 = CommandHistogram::new();
        let mut hist2 = CommandHistogram::new();

        hist1.record(1000);
        hist1.record(2000);

        hist2.record(500);
        hist2.record(3000);

        hist1.merge(&hist2);

        assert_eq!(hist1.total_count(), 4);
        assert_eq!(hist1.min_latency_us(), Some(500));
        assert_eq!(hist1.max_latency_us(), Some(3000));
    }

    #[test]
    fn test_command_histogram_reset() {
        let mut hist = CommandHistogram::new();

        hist.record(1000);
        hist.record(2000);

        assert_eq!(hist.total_count(), 2);

        hist.reset();

        assert_eq!(hist.total_count(), 0);
        assert!(hist.min_latency_us().is_none());
        assert!(hist.max_latency_us().is_none());
    }

    #[test]
    fn test_generate_latency_graph_empty() {
        let graph = generate_latency_graph(LatencyEvent::Command, &[]);
        assert!(graph.contains("no data available"));
    }

    #[test]
    fn test_generate_latency_graph_with_data() {
        let samples = vec![
            LatencySample::with_timestamp(1000, 50),
            LatencySample::with_timestamp(1001, 100),
            LatencySample::with_timestamp(1002, 75),
        ];

        let graph = generate_latency_graph(LatencyEvent::Command, &samples);

        // Should contain the event name and sample count
        assert!(graph.contains("command"));
        assert!(graph.contains("3 samples"));
        // Should contain some graph characters
        assert!(graph.contains("#"));
    }
}
