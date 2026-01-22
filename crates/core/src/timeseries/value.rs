//! TimeSeriesValue - the core data structure for time series storage.
//!
//! Implements:
//! - BTreeMap for active (uncompressed) samples
//! - Vec<CompressedChunk> for historical data
//! - Configurable retention, duplicate policy, and chunk size
//! - Labels for metadata and filtering

use std::collections::BTreeMap;

use crate::timeseries::aggregation::{aggregate_by_bucket, Aggregation};
use crate::timeseries::chunk::CompressedChunk;

/// Default chunk size (samples before compression).
pub const DEFAULT_CHUNK_SIZE: usize = 256;

/// Default retention (0 = no retention limit).
pub const DEFAULT_RETENTION_MS: u64 = 0;

/// Policy for handling duplicate timestamps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DuplicatePolicy {
    /// Error on duplicate timestamp.
    Block,
    /// Keep the first value.
    First,
    /// Keep the last value (default).
    #[default]
    Last,
    /// Keep the minimum value.
    Min,
    /// Keep the maximum value.
    Max,
    /// Sum the values.
    Sum,
}

impl DuplicatePolicy {
    /// Parse duplicate policy from string.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "BLOCK" => Some(DuplicatePolicy::Block),
            "FIRST" => Some(DuplicatePolicy::First),
            "LAST" => Some(DuplicatePolicy::Last),
            "MIN" => Some(DuplicatePolicy::Min),
            "MAX" => Some(DuplicatePolicy::Max),
            "SUM" => Some(DuplicatePolicy::Sum),
            _ => None,
        }
    }

    /// Get the string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            DuplicatePolicy::Block => "BLOCK",
            DuplicatePolicy::First => "FIRST",
            DuplicatePolicy::Last => "LAST",
            DuplicatePolicy::Min => "MIN",
            DuplicatePolicy::Max => "MAX",
            DuplicatePolicy::Sum => "SUM",
        }
    }
}

/// Error when adding a sample fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddError {
    /// Duplicate timestamp with BLOCK policy.
    DuplicateTimestamp,
    /// Timestamp is older than retention allows.
    TimestampTooOld,
}

/// A time series value with compression and retention.
#[derive(Debug, Clone)]
pub struct TimeSeriesValue {
    /// Active (uncompressed) samples, sorted by timestamp.
    active_samples: BTreeMap<i64, f64>,
    /// Compressed historical chunks.
    chunks: Vec<CompressedChunk>,
    /// Labels (metadata key-value pairs).
    labels: Vec<(String, String)>,
    /// Retention period in milliseconds (0 = unlimited).
    retention_ms: u64,
    /// Policy for handling duplicate timestamps.
    duplicate_policy: DuplicatePolicy,
    /// Number of samples before compression.
    chunk_size: usize,
    /// Total number of samples (including compressed).
    total_samples: u64,
    /// Last timestamp added.
    last_timestamp: Option<i64>,
}

impl TimeSeriesValue {
    /// Create a new empty time series.
    pub fn new() -> Self {
        Self {
            active_samples: BTreeMap::new(),
            chunks: Vec::new(),
            labels: Vec::new(),
            retention_ms: DEFAULT_RETENTION_MS,
            duplicate_policy: DuplicatePolicy::default(),
            chunk_size: DEFAULT_CHUNK_SIZE,
            total_samples: 0,
            last_timestamp: None,
        }
    }

    /// Create a new time series with specific settings.
    pub fn with_options(
        retention_ms: u64,
        duplicate_policy: DuplicatePolicy,
        chunk_size: usize,
        labels: Vec<(String, String)>,
    ) -> Self {
        Self {
            active_samples: BTreeMap::new(),
            chunks: Vec::new(),
            labels,
            retention_ms,
            duplicate_policy,
            chunk_size: chunk_size.max(1),
            total_samples: 0,
            last_timestamp: None,
        }
    }

    /// Create from raw components (for deserialization).
    pub fn from_raw(
        active_samples: BTreeMap<i64, f64>,
        chunks: Vec<CompressedChunk>,
        labels: Vec<(String, String)>,
        retention_ms: u64,
        duplicate_policy: DuplicatePolicy,
        chunk_size: usize,
    ) -> Self {
        let total_samples = active_samples.len() as u64
            + chunks.iter().map(|c| c.sample_count() as u64).sum::<u64>();
        let last_timestamp = active_samples.keys().last().copied().or_else(|| {
            chunks.last().map(|c| c.end_time())
        });

        Self {
            active_samples,
            chunks,
            labels,
            retention_ms,
            duplicate_policy,
            chunk_size,
            total_samples,
            last_timestamp,
        }
    }

    /// Add a sample to the time series.
    ///
    /// Returns the timestamp if successful.
    pub fn add(&mut self, timestamp: i64, value: f64) -> Result<i64, AddError> {
        // Handle duplicate timestamps
        if let Some(&existing) = self.active_samples.get(&timestamp) {
            match self.duplicate_policy {
                DuplicatePolicy::Block => return Err(AddError::DuplicateTimestamp),
                DuplicatePolicy::First => return Ok(timestamp),
                DuplicatePolicy::Last => {
                    self.active_samples.insert(timestamp, value);
                }
                DuplicatePolicy::Min => {
                    if value < existing {
                        self.active_samples.insert(timestamp, value);
                    }
                }
                DuplicatePolicy::Max => {
                    if value > existing {
                        self.active_samples.insert(timestamp, value);
                    }
                }
                DuplicatePolicy::Sum => {
                    self.active_samples.insert(timestamp, existing + value);
                }
            }
        } else {
            // Also check if timestamp exists in chunks (for BLOCK policy)
            if self.duplicate_policy == DuplicatePolicy::Block {
                for chunk in &self.chunks {
                    if chunk.overlaps(timestamp, timestamp) {
                        let samples = chunk.range(timestamp, timestamp);
                        if !samples.is_empty() {
                            return Err(AddError::DuplicateTimestamp);
                        }
                    }
                }
            }

            self.active_samples.insert(timestamp, value);
            self.total_samples += 1;
        }

        // Update last timestamp
        if self.last_timestamp.is_none_or(|last| timestamp > last) {
            self.last_timestamp = Some(timestamp);
        }

        // Compress if needed
        if self.active_samples.len() >= self.chunk_size {
            self.compress_active();
        }

        // Enforce retention
        if self.retention_ms > 0 {
            if let Some(last_ts) = self.last_timestamp {
                self.enforce_retention(last_ts);
            }
        }

        Ok(timestamp)
    }

    /// Get the most recent sample.
    pub fn get_last(&self) -> Option<(i64, f64)> {
        // Check active samples first
        if let Some((&ts, &val)) = self.active_samples.iter().next_back() {
            return Some((ts, val));
        }

        // Check the last chunk
        if let Some(chunk) = self.chunks.last() {
            let samples = chunk.decompress();
            return samples.last().copied();
        }

        None
    }

    /// Get a sample at a specific timestamp.
    pub fn get(&self, timestamp: i64) -> Option<f64> {
        // Check active samples
        if let Some(&val) = self.active_samples.get(&timestamp) {
            return Some(val);
        }

        // Check chunks
        for chunk in &self.chunks {
            if chunk.overlaps(timestamp, timestamp) {
                let samples = chunk.range(timestamp, timestamp);
                if let Some((_, val)) = samples.first() {
                    return Some(*val);
                }
            }
        }

        None
    }

    /// Query samples in a time range.
    ///
    /// Returns samples where `from <= timestamp <= to`.
    pub fn range(&self, from: i64, to: i64) -> Vec<(i64, f64)> {
        let mut result = Vec::new();

        // Collect from chunks
        for chunk in &self.chunks {
            if chunk.overlaps(from, to) {
                result.extend(chunk.range(from, to));
            }
        }

        // Collect from active samples
        for (&ts, &val) in self.active_samples.range(from..=to) {
            result.push((ts, val));
        }

        // Sort by timestamp (chunks might overlap)
        result.sort_by_key(|&(ts, _)| ts);
        result
    }

    /// Query samples in reverse order.
    pub fn revrange(&self, from: i64, to: i64) -> Vec<(i64, f64)> {
        let mut result = self.range(from, to);
        result.reverse();
        result
    }

    /// Query samples with aggregation.
    pub fn range_aggregated(
        &self,
        from: i64,
        to: i64,
        bucket_duration_ms: i64,
        agg: Aggregation,
    ) -> Vec<(i64, f64)> {
        let samples = self.range(from, to);
        aggregate_by_bucket(&samples, bucket_duration_ms, agg)
    }

    /// Delete samples in a time range.
    ///
    /// Returns the number of samples deleted.
    pub fn delete_range(&mut self, from: i64, to: i64) -> u64 {
        let mut deleted = 0u64;

        // Delete from active samples
        let keys_to_remove: Vec<i64> = self.active_samples
            .range(from..=to)
            .map(|(&k, _)| k)
            .collect();
        deleted += keys_to_remove.len() as u64;
        for key in keys_to_remove {
            self.active_samples.remove(&key);
        }

        // For chunks, we need to decompress, filter, and recompress
        let mut new_chunks = Vec::new();
        for chunk in self.chunks.drain(..) {
            if !chunk.overlaps(from, to) {
                new_chunks.push(chunk);
            } else {
                // Decompress and filter
                let samples: Vec<(i64, f64)> = chunk.decompress()
                    .into_iter()
                    .filter(|&(ts, _)| ts < from || ts > to)
                    .collect();

                deleted += chunk.sample_count() as u64 - samples.len() as u64;

                if !samples.is_empty() {
                    new_chunks.push(CompressedChunk::from_samples(&samples));
                }
            }
        }
        self.chunks = new_chunks;

        self.total_samples = self.total_samples.saturating_sub(deleted);

        deleted
    }

    /// Enforce retention policy by removing old samples.
    pub fn enforce_retention(&mut self, current_time: i64) {
        if self.retention_ms == 0 {
            return;
        }

        let cutoff = current_time - self.retention_ms as i64;

        // Remove from active samples
        let keys_to_remove: Vec<i64> = self.active_samples
            .range(..cutoff)
            .map(|(&k, _)| k)
            .collect();
        let removed = keys_to_remove.len() as u64;
        for key in keys_to_remove {
            self.active_samples.remove(&key);
        }
        self.total_samples = self.total_samples.saturating_sub(removed);

        // Remove old chunks entirely, or trim partially
        let mut new_chunks = Vec::new();
        for chunk in self.chunks.drain(..) {
            if chunk.end_time() < cutoff {
                // Entire chunk is too old, remove it
                self.total_samples = self.total_samples.saturating_sub(chunk.sample_count() as u64);
            } else if chunk.start_time() >= cutoff {
                // Entire chunk is within retention
                new_chunks.push(chunk);
            } else {
                // Chunk partially overlaps, trim it
                let samples: Vec<(i64, f64)> = chunk.decompress()
                    .into_iter()
                    .filter(|&(ts, _)| ts >= cutoff)
                    .collect();

                let removed = chunk.sample_count() as u64 - samples.len() as u64;
                self.total_samples = self.total_samples.saturating_sub(removed);

                if !samples.is_empty() {
                    new_chunks.push(CompressedChunk::from_samples(&samples));
                }
            }
        }
        self.chunks = new_chunks;
    }

    /// Compress active samples into a chunk.
    fn compress_active(&mut self) {
        if self.active_samples.is_empty() {
            return;
        }

        let samples: Vec<(i64, f64)> = self.active_samples
            .iter()
            .map(|(&ts, &val)| (ts, val))
            .collect();

        let chunk = CompressedChunk::from_samples(&samples);
        self.chunks.push(chunk);
        self.active_samples.clear();
    }

    /// Increment value at timestamp.
    pub fn incrby(&mut self, timestamp: i64, delta: f64) -> Result<f64, AddError> {
        let current = self.get(timestamp).unwrap_or(0.0);
        let new_value = current + delta;

        // Remove old value if it exists in active samples
        self.active_samples.remove(&timestamp);

        // Use LAST policy internally for INCRBY
        let old_policy = self.duplicate_policy;
        self.duplicate_policy = DuplicatePolicy::Last;
        self.add(timestamp, new_value)?;
        self.duplicate_policy = old_policy;

        Ok(new_value)
    }

    /// Get the labels.
    pub fn labels(&self) -> &[(String, String)] {
        &self.labels
    }

    /// Set the labels (replaces all existing labels).
    pub fn set_labels(&mut self, labels: Vec<(String, String)>) {
        self.labels = labels;
    }

    /// Get a specific label value.
    pub fn get_label(&self, name: &str) -> Option<&str> {
        self.labels.iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }

    /// Get the retention period in milliseconds.
    pub fn retention_ms(&self) -> u64 {
        self.retention_ms
    }

    /// Set the retention period.
    pub fn set_retention_ms(&mut self, retention_ms: u64) {
        self.retention_ms = retention_ms;
    }

    /// Get the duplicate policy.
    pub fn duplicate_policy(&self) -> DuplicatePolicy {
        self.duplicate_policy
    }

    /// Set the duplicate policy.
    pub fn set_duplicate_policy(&mut self, policy: DuplicatePolicy) {
        self.duplicate_policy = policy;
    }

    /// Get the chunk size.
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    /// Set the chunk size.
    pub fn set_chunk_size(&mut self, size: usize) {
        self.chunk_size = size.max(1);
    }

    /// Get the total number of samples.
    pub fn total_samples(&self) -> u64 {
        self.total_samples
    }

    /// Get the last timestamp.
    pub fn last_timestamp(&self) -> Option<i64> {
        self.last_timestamp
    }

    /// Get the first timestamp.
    pub fn first_timestamp(&self) -> Option<i64> {
        // Check chunks first (they contain older data)
        if let Some(chunk) = self.chunks.first() {
            return Some(chunk.start_time());
        }

        // Then check active samples
        self.active_samples.keys().next().copied()
    }

    /// Get the number of compressed chunks.
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// Get the active samples (for serialization).
    pub fn active_samples(&self) -> &BTreeMap<i64, f64> {
        &self.active_samples
    }

    /// Get the chunks (for serialization).
    pub fn chunks(&self) -> &[CompressedChunk] {
        &self.chunks
    }

    /// Calculate approximate memory size.
    pub fn memory_size(&self) -> usize {
        let active_size = self.active_samples.len() * (8 + 8); // i64 + f64
        let chunks_size: usize = self.chunks.iter().map(|c| c.memory_size()).sum();
        let labels_size: usize = self.labels.iter()
            .map(|(k, v)| k.len() + v.len())
            .sum();

        std::mem::size_of::<Self>() + active_size + chunks_size + labels_size
    }
}

impl Default for TimeSeriesValue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_timeseries() {
        let ts = TimeSeriesValue::new();
        assert_eq!(ts.total_samples(), 0);
        assert!(ts.get_last().is_none());
    }

    #[test]
    fn test_add_samples() {
        let mut ts = TimeSeriesValue::new();

        ts.add(1000, 10.0).unwrap();
        ts.add(2000, 20.0).unwrap();
        ts.add(3000, 30.0).unwrap();

        assert_eq!(ts.total_samples(), 3);
        assert_eq!(ts.get_last(), Some((3000, 30.0)));
        assert_eq!(ts.get(2000), Some(20.0));
    }

    #[test]
    fn test_duplicate_policy_block() {
        let mut ts = TimeSeriesValue::with_options(0, DuplicatePolicy::Block, 256, vec![]);

        ts.add(1000, 10.0).unwrap();
        let result = ts.add(1000, 20.0);
        assert_eq!(result, Err(AddError::DuplicateTimestamp));
        assert_eq!(ts.get(1000), Some(10.0)); // Value unchanged
    }

    #[test]
    fn test_duplicate_policy_last() {
        let mut ts = TimeSeriesValue::with_options(0, DuplicatePolicy::Last, 256, vec![]);

        ts.add(1000, 10.0).unwrap();
        ts.add(1000, 20.0).unwrap();
        assert_eq!(ts.get(1000), Some(20.0));
    }

    #[test]
    fn test_duplicate_policy_first() {
        let mut ts = TimeSeriesValue::with_options(0, DuplicatePolicy::First, 256, vec![]);

        ts.add(1000, 10.0).unwrap();
        ts.add(1000, 20.0).unwrap();
        assert_eq!(ts.get(1000), Some(10.0));
    }

    #[test]
    fn test_duplicate_policy_min() {
        let mut ts = TimeSeriesValue::with_options(0, DuplicatePolicy::Min, 256, vec![]);

        ts.add(1000, 20.0).unwrap();
        ts.add(1000, 10.0).unwrap();
        ts.add(1000, 30.0).unwrap();
        assert_eq!(ts.get(1000), Some(10.0));
    }

    #[test]
    fn test_duplicate_policy_max() {
        let mut ts = TimeSeriesValue::with_options(0, DuplicatePolicy::Max, 256, vec![]);

        ts.add(1000, 10.0).unwrap();
        ts.add(1000, 30.0).unwrap();
        ts.add(1000, 20.0).unwrap();
        assert_eq!(ts.get(1000), Some(30.0));
    }

    #[test]
    fn test_duplicate_policy_sum() {
        let mut ts = TimeSeriesValue::with_options(0, DuplicatePolicy::Sum, 256, vec![]);

        ts.add(1000, 10.0).unwrap();
        ts.add(1000, 20.0).unwrap();
        ts.add(1000, 30.0).unwrap();
        assert_eq!(ts.get(1000), Some(60.0)); // 10 + 20 + 30
    }

    #[test]
    fn test_range_query() {
        let mut ts = TimeSeriesValue::new();

        for i in 0..10 {
            ts.add(1000 + i * 100, i as f64).unwrap();
        }

        let range = ts.range(1200, 1500);
        assert_eq!(range.len(), 4);
        assert_eq!(range[0], (1200, 2.0));
        assert_eq!(range[3], (1500, 5.0));
    }

    #[test]
    fn test_revrange() {
        let mut ts = TimeSeriesValue::new();

        ts.add(1000, 1.0).unwrap();
        ts.add(2000, 2.0).unwrap();
        ts.add(3000, 3.0).unwrap();

        let range = ts.revrange(1000, 3000);
        assert_eq!(range[0], (3000, 3.0));
        assert_eq!(range[2], (1000, 1.0));
    }

    #[test]
    fn test_delete_range() {
        let mut ts = TimeSeriesValue::new();

        for i in 0..10 {
            ts.add(1000 + i * 100, i as f64).unwrap();
        }

        let deleted = ts.delete_range(1300, 1600);
        assert_eq!(deleted, 4); // timestamps 1300, 1400, 1500, 1600

        let remaining = ts.range(0, 10000);
        assert_eq!(remaining.len(), 6);
    }

    #[test]
    fn test_retention() {
        let mut ts = TimeSeriesValue::with_options(1000, DuplicatePolicy::Last, 256, vec![]);

        ts.add(1000, 1.0).unwrap();
        ts.add(1500, 1.5).unwrap();
        ts.add(2000, 2.0).unwrap();

        // After adding at 2500, samples before 1500 should be removed
        ts.add(2500, 2.5).unwrap();

        let samples = ts.range(0, 10000);
        assert!(samples.iter().all(|&(ts, _)| ts >= 1500));
    }

    #[test]
    fn test_compression() {
        let mut ts = TimeSeriesValue::with_options(0, DuplicatePolicy::Last, 10, vec![]);

        // Add 25 samples, should create 2 chunks (10 + 10) and 5 active
        for i in 0..25 {
            ts.add(1000 + i * 100, i as f64).unwrap();
        }

        assert_eq!(ts.chunk_count(), 2);
        assert_eq!(ts.total_samples(), 25);

        // Verify all samples are still accessible
        let range = ts.range(0, 100000);
        assert_eq!(range.len(), 25);
    }

    #[test]
    fn test_labels() {
        let labels = vec![
            ("location".to_string(), "kitchen".to_string()),
            ("sensor".to_string(), "temp".to_string()),
        ];
        let ts = TimeSeriesValue::with_options(0, DuplicatePolicy::Last, 256, labels);

        assert_eq!(ts.get_label("location"), Some("kitchen"));
        assert_eq!(ts.get_label("sensor"), Some("temp"));
        assert_eq!(ts.get_label("nonexistent"), None);
    }

    #[test]
    fn test_incrby() {
        let mut ts = TimeSeriesValue::new();

        let val = ts.incrby(1000, 10.0).unwrap();
        assert_eq!(val, 10.0);

        let val = ts.incrby(1000, 5.0).unwrap();
        assert_eq!(val, 15.0);

        let val = ts.incrby(1000, -3.0).unwrap();
        assert_eq!(val, 12.0);
    }

    #[test]
    fn test_first_last_timestamp() {
        let mut ts = TimeSeriesValue::new();

        assert!(ts.first_timestamp().is_none());
        assert!(ts.last_timestamp().is_none());

        ts.add(2000, 2.0).unwrap();
        ts.add(1000, 1.0).unwrap();
        ts.add(3000, 3.0).unwrap();

        assert_eq!(ts.first_timestamp(), Some(1000));
        assert_eq!(ts.last_timestamp(), Some(3000));
    }

    #[test]
    fn test_aggregated_range() {
        let mut ts = TimeSeriesValue::new();

        // Add samples with 100ms intervals
        for i in 0..10 {
            ts.add(1000 + i * 100, (i + 1) as f64 * 10.0).unwrap();
        }

        // Aggregate with 300ms buckets
        let result = ts.range_aggregated(1000, 2000, 300, Aggregation::Avg);

        // Should have buckets at 900, 1200, 1500, 1800 (bucket boundaries)
        assert!(!result.is_empty());
    }
}
