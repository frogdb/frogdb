//! Compressed chunk storage for time series data.
//!
//! A chunk represents a compressed block of time series samples.
//! When the active sample buffer reaches a certain size, it gets
//! compressed into a chunk for more efficient storage.

use crate::timeseries::compression::{decode_samples, encode_samples};

/// A compressed chunk of time series samples.
#[derive(Debug, Clone)]
pub struct CompressedChunk {
    /// Compressed data bytes.
    data: Vec<u8>,
    /// Start timestamp (first sample in chunk).
    start_time: i64,
    /// End timestamp (last sample in chunk).
    end_time: i64,
    /// Number of samples in this chunk.
    sample_count: u32,
}

impl CompressedChunk {
    /// Create a new compressed chunk from samples.
    ///
    /// The samples must be sorted by timestamp.
    pub fn from_samples(samples: &[(i64, f64)]) -> Self {
        if samples.is_empty() {
            return Self {
                data: Vec::new(),
                start_time: 0,
                end_time: 0,
                sample_count: 0,
            };
        }

        let start_time = samples.first().map(|s| s.0).unwrap_or(0);
        let end_time = samples.last().map(|s| s.0).unwrap_or(0);
        let sample_count = samples.len() as u32;
        let data = encode_samples(samples);

        Self {
            data,
            start_time,
            end_time,
            sample_count,
        }
    }

    /// Create a chunk from raw serialized data.
    pub fn from_raw(data: Vec<u8>, start_time: i64, end_time: i64, sample_count: u32) -> Self {
        Self {
            data,
            start_time,
            end_time,
            sample_count,
        }
    }

    /// Decompress and return all samples in this chunk.
    pub fn decompress(&self) -> Vec<(i64, f64)> {
        if self.data.is_empty() {
            return Vec::new();
        }
        decode_samples(&self.data)
    }

    /// Get samples within a time range.
    ///
    /// Returns samples where `from <= timestamp <= to`.
    pub fn range(&self, from: i64, to: i64) -> Vec<(i64, f64)> {
        // Quick check: if chunk doesn't overlap the range at all, return empty
        if self.end_time < from || self.start_time > to {
            return Vec::new();
        }

        // Decompress and filter
        self.decompress()
            .into_iter()
            .filter(|&(ts, _)| ts >= from && ts <= to)
            .collect()
    }

    /// Check if this chunk overlaps with a time range.
    pub fn overlaps(&self, from: i64, to: i64) -> bool {
        self.start_time <= to && self.end_time >= from
    }

    /// Get the start timestamp.
    pub fn start_time(&self) -> i64 {
        self.start_time
    }

    /// Get the end timestamp.
    pub fn end_time(&self) -> i64 {
        self.end_time
    }

    /// Get the number of samples in this chunk.
    pub fn sample_count(&self) -> u32 {
        self.sample_count
    }

    /// Get the compressed data size in bytes.
    pub fn data_size(&self) -> usize {
        self.data.len()
    }

    /// Get the raw compressed data.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Calculate approximate memory size.
    pub fn memory_size(&self) -> usize {
        std::mem::size_of::<Self>() + self.data.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_chunk() {
        let chunk = CompressedChunk::from_samples(&[]);
        assert_eq!(chunk.sample_count(), 0);
        assert!(chunk.decompress().is_empty());
    }

    #[test]
    fn test_single_sample_chunk() {
        let samples = vec![(1000i64, 42.5f64)];
        let chunk = CompressedChunk::from_samples(&samples);

        assert_eq!(chunk.sample_count(), 1);
        assert_eq!(chunk.start_time(), 1000);
        assert_eq!(chunk.end_time(), 1000);

        let decoded = chunk.decompress();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].0, 1000);
        assert!((decoded[0].1 - 42.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_multiple_samples_chunk() {
        let samples: Vec<(i64, f64)> = (0..100)
            .map(|i| (1000 + i * 60, 20.0 + i as f64 * 0.1))
            .collect();
        let chunk = CompressedChunk::from_samples(&samples);

        assert_eq!(chunk.sample_count(), 100);
        assert_eq!(chunk.start_time(), 1000);
        assert_eq!(chunk.end_time(), 1000 + 99 * 60);

        let decoded = chunk.decompress();
        assert_eq!(decoded.len(), 100);
        for (i, (ts, val)) in decoded.iter().enumerate() {
            assert_eq!(*ts, samples[i].0);
            assert!((val - samples[i].1).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn test_range_query() {
        let samples: Vec<(i64, f64)> = (0..10).map(|i| (1000 + i * 100, i as f64)).collect();
        let chunk = CompressedChunk::from_samples(&samples);

        // Query middle range
        let range = chunk.range(1200, 1500);
        assert_eq!(range.len(), 4); // timestamps 1200, 1300, 1400, 1500

        // Query outside range
        let range = chunk.range(2000, 3000);
        assert!(range.is_empty());

        // Query overlapping start
        let range = chunk.range(900, 1100);
        assert_eq!(range.len(), 2); // timestamps 1000, 1100
    }

    #[test]
    fn test_overlaps() {
        let samples = vec![(1000i64, 1.0f64), (2000i64, 2.0f64)];
        let chunk = CompressedChunk::from_samples(&samples);

        assert!(chunk.overlaps(500, 1500)); // Overlaps start
        assert!(chunk.overlaps(1500, 2500)); // Overlaps end
        assert!(chunk.overlaps(1200, 1800)); // Inside
        assert!(chunk.overlaps(500, 2500)); // Contains
        assert!(!chunk.overlaps(2500, 3000)); // After
        assert!(!chunk.overlaps(0, 500)); // Before
    }

    #[test]
    fn test_compression_saves_space() {
        // Regular time series data should compress well
        let samples: Vec<(i64, f64)> = (0..1000)
            .map(|i| (1706000000000 + i * 60000, 23.5 + (i % 10) as f64 * 0.1))
            .collect();
        let chunk = CompressedChunk::from_samples(&samples);

        let uncompressed_size = samples.len() * 16; // 8 + 8 bytes per sample
        let compressed_size = chunk.data_size();

        // Should achieve at least 2x compression for regular data
        assert!(
            compressed_size < uncompressed_size / 2,
            "Compressed size {} should be less than half of uncompressed {}",
            compressed_size,
            uncompressed_size
        );
    }
}
