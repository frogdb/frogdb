//! Latency band tracking for SLO monitoring.
//!
//! Tracks cumulative request counts in configurable latency buckets,
//! inspired by FoundationDB's approach to SLO monitoring.

use std::sync::atomic::{AtomicU64, Ordering};

/// Tracks request latencies across configurable bands for SLO monitoring.
///
/// Each band represents a cumulative bucket (e.g., "<=5ms"). When a request
/// completes, it's counted in all bands whose threshold is >= the request latency.
/// This enables direct SLO monitoring (e.g., "99% of requests under 10ms").
pub struct LatencyBandTracker {
    /// Sorted thresholds in milliseconds.
    bands: Vec<u64>,
    /// Cumulative counters: one per band + overflow bucket.
    /// counters[i] = requests with latency <= bands[i]
    /// counters[bands.len()] = total requests (including overflow)
    counters: Vec<AtomicU64>,
}

impl LatencyBandTracker {
    /// Create a new tracker with the given band thresholds (in milliseconds).
    ///
    /// Bands are automatically sorted. Duplicates are removed.
    pub fn new(mut bands: Vec<u64>) -> Self {
        bands.sort();
        bands.dedup();

        // One counter per band + one for total
        let counters = (0..=bands.len()).map(|_| AtomicU64::new(0)).collect();

        Self { bands, counters }
    }

    /// Record a request latency in milliseconds.
    ///
    /// The latency is counted in all bands whose threshold is >= the latency.
    pub fn record(&self, latency_ms: u64) {
        // Increment total counter
        self.counters[self.bands.len()].fetch_add(1, Ordering::Relaxed);

        // Increment all bands where latency <= threshold
        for (i, &threshold) in self.bands.iter().enumerate() {
            if latency_ms <= threshold {
                self.counters[i].fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Get the band thresholds.
    pub fn bands(&self) -> &[u64] {
        &self.bands
    }

    /// Get the total request count.
    pub fn total(&self) -> u64 {
        self.counters[self.bands.len()].load(Ordering::Relaxed)
    }

    /// Get cumulative counts for each band.
    ///
    /// Returns (label, cumulative_count) pairs, plus an overflow bucket.
    /// Labels are formatted as "<=Xms" or ">Xms" for overflow.
    pub fn get_counts(&self) -> Vec<(String, u64)> {
        let mut result = Vec::with_capacity(self.bands.len() + 1);

        for (i, &threshold) in self.bands.iter().enumerate() {
            let count = self.counters[i].load(Ordering::Relaxed);
            let label = format!("<={}ms", threshold);
            result.push((label, count));
        }

        // Overflow bucket: total - last band count
        let total = self.total();
        let last_band_count = if self.bands.is_empty() {
            0
        } else {
            self.counters[self.bands.len() - 1].load(Ordering::Relaxed)
        };
        let overflow = total.saturating_sub(last_band_count);
        let overflow_label = if self.bands.is_empty() {
            ">0ms".to_string()
        } else {
            format!(">{}ms", self.bands.last().unwrap())
        };
        result.push((overflow_label, overflow));

        result
    }

    /// Get per-band counts (not cumulative).
    ///
    /// Returns (label, count) pairs where count is the number of requests
    /// that fell into that specific band.
    pub fn get_band_counts(&self) -> Vec<(String, u64)> {
        let mut result = Vec::with_capacity(self.bands.len() + 1);
        let total = self.total();

        let mut prev_cumulative = 0u64;
        for (i, &threshold) in self.bands.iter().enumerate() {
            let cumulative = self.counters[i].load(Ordering::Relaxed);
            let band_count = cumulative.saturating_sub(prev_cumulative);
            let label = format!("<={}ms", threshold);
            result.push((label, band_count));
            prev_cumulative = cumulative;
        }

        // Overflow bucket
        let overflow = total.saturating_sub(prev_cumulative);
        let overflow_label = if self.bands.is_empty() {
            ">0ms".to_string()
        } else {
            format!(">{}ms", self.bands.last().unwrap())
        };
        result.push((overflow_label, overflow));

        result
    }

    /// Get percentages for each band.
    ///
    /// Returns (label, percentage) pairs. Percentage is 0.0-100.0.
    pub fn get_percentages(&self) -> Vec<(String, u64, f64)> {
        let total = self.total();
        if total == 0 {
            return self
                .bands
                .iter()
                .map(|&t| (format!("<={}ms", t), 0, 0.0))
                .chain(std::iter::once((
                    if self.bands.is_empty() {
                        ">0ms".to_string()
                    } else {
                        format!(">{}ms", self.bands.last().unwrap())
                    },
                    0,
                    0.0,
                )))
                .collect();
        }

        let mut result = Vec::with_capacity(self.bands.len() + 1);
        let mut prev_cumulative = 0u64;

        for (i, &threshold) in self.bands.iter().enumerate() {
            let cumulative = self.counters[i].load(Ordering::Relaxed);
            let band_count = cumulative.saturating_sub(prev_cumulative);
            let percentage = (band_count as f64 / total as f64) * 100.0;
            let label = format!("<={}ms", threshold);
            result.push((label, band_count, percentage));
            prev_cumulative = cumulative;
        }

        // Overflow bucket
        let overflow = total.saturating_sub(prev_cumulative);
        let overflow_pct = (overflow as f64 / total as f64) * 100.0;
        let overflow_label = if self.bands.is_empty() {
            ">0ms".to_string()
        } else {
            format!(">{}ms", self.bands.last().unwrap())
        };
        result.push((overflow_label, overflow, overflow_pct));

        result
    }

    /// Reset all counters to zero.
    pub fn reset(&self) {
        for counter in &self.counters {
            counter.store(0, Ordering::Relaxed);
        }
    }
}

// Safety: AtomicU64 is Send + Sync
unsafe impl Send for LatencyBandTracker {}
unsafe impl Sync for LatencyBandTracker {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_sorts_and_dedupes_bands() {
        let tracker = LatencyBandTracker::new(vec![100, 10, 50, 10, 5]);
        assert_eq!(tracker.bands(), &[5, 10, 50, 100]);
    }

    #[test]
    fn test_record_increments_correct_bands() {
        let tracker = LatencyBandTracker::new(vec![1, 5, 10, 50]);

        // Record 3ms latency - should increment <=5ms, <=10ms, <=50ms, and total
        tracker.record(3);
        assert_eq!(tracker.total(), 1);

        let counts = tracker.get_counts();
        assert_eq!(counts[0], ("<=1ms".to_string(), 0)); // 3 > 1
        assert_eq!(counts[1], ("<=5ms".to_string(), 1)); // 3 <= 5
        assert_eq!(counts[2], ("<=10ms".to_string(), 1)); // 3 <= 10
        assert_eq!(counts[3], ("<=50ms".to_string(), 1)); // 3 <= 50
        assert_eq!(counts[4], (">50ms".to_string(), 0)); // no overflow
    }

    #[test]
    fn test_overflow_bucket() {
        let tracker = LatencyBandTracker::new(vec![10, 50]);

        tracker.record(100); // Should only go to overflow

        let counts = tracker.get_counts();
        assert_eq!(counts[0], ("<=10ms".to_string(), 0));
        assert_eq!(counts[1], ("<=50ms".to_string(), 0));
        assert_eq!(counts[2], (">50ms".to_string(), 1));
        assert_eq!(tracker.total(), 1);
    }

    #[test]
    fn test_get_band_counts_not_cumulative() {
        let tracker = LatencyBandTracker::new(vec![1, 5, 10]);

        tracker.record(0); // <=1ms
        tracker.record(3); // <=5ms
        tracker.record(3); // <=5ms
        tracker.record(8); // <=10ms
        tracker.record(100); // overflow

        let band_counts = tracker.get_band_counts();
        assert_eq!(band_counts[0], ("<=1ms".to_string(), 1)); // 1 request <= 1ms
        assert_eq!(band_counts[1], ("<=5ms".to_string(), 2)); // 2 requests in (1, 5]
        assert_eq!(band_counts[2], ("<=10ms".to_string(), 1)); // 1 request in (5, 10]
        assert_eq!(band_counts[3], (">10ms".to_string(), 1)); // 1 overflow
    }

    #[test]
    fn test_get_percentages() {
        let tracker = LatencyBandTracker::new(vec![10, 50]);

        for _ in 0..50 {
            tracker.record(5); // <=10ms
        }
        for _ in 0..30 {
            tracker.record(25); // <=50ms but >10ms
        }
        for _ in 0..20 {
            tracker.record(100); // overflow
        }

        let percentages = tracker.get_percentages();
        assert_eq!(percentages[0].0, "<=10ms");
        assert_eq!(percentages[0].1, 50); // count
        assert!((percentages[0].2 - 50.0).abs() < 0.01); // 50%

        assert_eq!(percentages[1].0, "<=50ms");
        assert_eq!(percentages[1].1, 30); // count
        assert!((percentages[1].2 - 30.0).abs() < 0.01); // 30%

        assert_eq!(percentages[2].0, ">50ms");
        assert_eq!(percentages[2].1, 20); // count
        assert!((percentages[2].2 - 20.0).abs() < 0.01); // 20%
    }

    #[test]
    fn test_reset() {
        let tracker = LatencyBandTracker::new(vec![10, 50]);

        tracker.record(5);
        tracker.record(25);
        tracker.record(100);

        assert_eq!(tracker.total(), 3);

        tracker.reset();

        assert_eq!(tracker.total(), 0);
        let counts = tracker.get_counts();
        assert!(counts.iter().all(|(_, c)| *c == 0));
    }

    #[test]
    fn test_empty_bands() {
        let tracker = LatencyBandTracker::new(vec![]);

        tracker.record(100);

        assert_eq!(tracker.total(), 1);
        let counts = tracker.get_counts();
        assert_eq!(counts.len(), 1);
        assert_eq!(counts[0], (">0ms".to_string(), 1));
    }

    #[test]
    fn test_exact_threshold_match() {
        let tracker = LatencyBandTracker::new(vec![5, 10]);

        tracker.record(5); // Exactly on threshold

        let counts = tracker.get_counts();
        assert_eq!(counts[0], ("<=5ms".to_string(), 1)); // Should be included
        assert_eq!(counts[1], ("<=10ms".to_string(), 1)); // Also included
    }

    #[test]
    fn test_zero_latency() {
        let tracker = LatencyBandTracker::new(vec![1, 5]);

        tracker.record(0);

        let counts = tracker.get_counts();
        assert_eq!(counts[0], ("<=1ms".to_string(), 1)); // 0 <= 1
        assert_eq!(counts[1], ("<=5ms".to_string(), 1)); // 0 <= 5
    }

    #[test]
    fn test_concurrent_recording() {
        use std::sync::Arc;
        use std::thread;

        let tracker = Arc::new(LatencyBandTracker::new(vec![10, 50, 100]));
        let mut handles = vec![];

        for _ in 0..10 {
            let t = Arc::clone(&tracker);
            handles.push(thread::spawn(move || {
                for i in 0..1000 {
                    t.record(i % 150);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(tracker.total(), 10_000);
    }
}
