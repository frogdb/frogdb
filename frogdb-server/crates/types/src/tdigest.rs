//! T-Digest probabilistic data structure.
//!
//! Provides approximate quantile estimation on streaming data using the
//! merging digest algorithm with k1 scale function. Useful for computing
//! percentiles (e.g., p99 latency) without storing all values.

use std::f64::consts::PI;

/// A centroid in the t-digest.
#[derive(Debug, Clone)]
pub struct Centroid {
    pub mean: f64,
    pub weight: f64,
}

/// T-Digest value for approximate quantile estimation.
#[derive(Debug, Clone)]
pub struct TDigestValue {
    /// Compression parameter (default 100). Controls accuracy vs memory tradeoff.
    compression: f64,
    /// Sorted, merged centroids.
    centroids: Vec<Centroid>,
    /// Buffer of unmerged values.
    unmerged: Vec<Centroid>,
    /// Minimum observed value.
    min: f64,
    /// Maximum observed value.
    max: f64,
    /// Total weight of merged centroids.
    merged_weight: f64,
    /// Total weight of unmerged centroids.
    unmerged_weight: f64,
}

impl TDigestValue {
    /// Create a new t-digest with the given compression.
    pub fn new(compression: f64) -> Self {
        Self {
            compression,
            centroids: Vec::new(),
            unmerged: Vec::new(),
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            merged_weight: 0.0,
            unmerged_weight: 0.0,
        }
    }

    /// Create from raw data (for deserialization).
    pub fn from_raw(
        compression: f64,
        centroids: Vec<Centroid>,
        unmerged: Vec<Centroid>,
        min: f64,
        max: f64,
        merged_weight: f64,
        unmerged_weight: f64,
    ) -> Self {
        Self {
            compression,
            centroids,
            unmerged,
            min,
            max,
            merged_weight,
            unmerged_weight,
        }
    }

    /// Add a value with weight 1.
    pub fn add(&mut self, value: f64) {
        self.add_weighted(value, 1.0);
    }

    /// Add a value with a given weight.
    pub fn add_weighted(&mut self, value: f64, weight: f64) {
        if weight <= 0.0 || value.is_nan() {
            return;
        }
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
        self.unmerged.push(Centroid {
            mean: value,
            weight,
        });
        self.unmerged_weight += weight;

        let buffer_limit = (self.compression / 2.0).ceil() as usize;
        if self.unmerged.len() >= buffer_limit.max(1) {
            self.compress();
        }
    }

    /// Compress unmerged centroids into the main digest.
    pub fn compress(&mut self) {
        if self.unmerged.is_empty() {
            return;
        }

        // Combine merged and unmerged centroids
        let mut all: Vec<Centroid> = Vec::with_capacity(self.centroids.len() + self.unmerged.len());
        all.append(&mut self.centroids);
        all.append(&mut self.unmerged);

        let total_weight = self.merged_weight + self.unmerged_weight;

        // Sort by mean
        all.sort_by(|a, b| {
            a.mean
                .partial_cmp(&b.mean)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Merge using k1 scale function
        let mut result: Vec<Centroid> = Vec::new();
        let mut weight_so_far = 0.0;

        for centroid in all {
            if result.is_empty() {
                result.push(centroid);
                continue;
            }

            let last = result.last().unwrap();
            let proposed_weight = last.weight + centroid.weight;
            let q0 = weight_so_far / total_weight;
            let q1 = (weight_so_far + proposed_weight) / total_weight;

            // k1 scale function: k(q) = (compression / (2*pi)) * asin(2q - 1)
            let k0 = self.k1(q0);
            let k1 = self.k1(q1);

            if k1 - k0 <= 1.0 {
                // Merge into last centroid
                let last = result.last_mut().unwrap();
                let new_weight = last.weight + centroid.weight;
                last.mean += (centroid.mean - last.mean) * centroid.weight / new_weight;
                last.weight = new_weight;
            } else {
                weight_so_far += result.last().unwrap().weight;
                result.push(centroid);
            }
        }

        self.centroids = result;
        self.merged_weight = total_weight;
        self.unmerged_weight = 0.0;
    }

    /// k1 scale function: k(q) = (compression / (2*pi)) * asin(2q - 1)
    fn k1(&self, q: f64) -> f64 {
        (self.compression / (2.0 * PI)) * (2.0 * q - 1.0).asin()
    }

    /// Ensure all pending values are merged before querying.
    fn flush(&mut self) {
        if !self.unmerged.is_empty() {
            self.compress();
        }
    }

    /// Merge values from another t-digest into this one.
    pub fn merge_from(&mut self, other: &TDigestValue) {
        if other.total_weight() == 0.0 {
            return;
        }

        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }

        for c in &other.centroids {
            self.unmerged.push(Centroid {
                mean: c.mean,
                weight: c.weight,
            });
            self.unmerged_weight += c.weight;
        }
        for c in &other.unmerged {
            self.unmerged.push(Centroid {
                mean: c.mean,
                weight: c.weight,
            });
            self.unmerged_weight += c.weight;
        }

        self.compress();
    }

    /// Estimate the value at the given quantile (0.0 to 1.0).
    pub fn quantile(&mut self, q: f64) -> f64 {
        self.flush();

        if self.centroids.is_empty() {
            return f64::NAN;
        }

        if q <= 0.0 {
            return self.min;
        }
        if q >= 1.0 {
            return self.max;
        }

        let total = self.merged_weight;
        let target = q * total;
        let mut cumulative = 0.0;

        // Handle edge case: target is in the first half of the first centroid
        let first = &self.centroids[0];
        if target < first.weight / 2.0 {
            // Interpolate between min and first centroid mean
            let inner = target / (first.weight / 2.0);
            return self.min + inner * (first.mean - self.min);
        }

        // Handle edge case: target is in the last half of the last centroid
        let last = &self.centroids[self.centroids.len() - 1];
        if target > total - last.weight / 2.0 {
            let tail = total - target;
            let inner = tail / (last.weight / 2.0);
            return self.max - inner * (self.max - last.mean);
        }

        // Interpolate between centroids
        for i in 0..self.centroids.len() {
            let c = &self.centroids[i];
            let upper = cumulative + c.weight;

            if target < upper {
                // target is within this centroid
                let mid = cumulative + c.weight / 2.0;

                if i == 0 {
                    // In the first centroid, interpolate from min
                    if target <= mid {
                        let inner = (target - cumulative) / (c.weight / 2.0);
                        return self.min + inner * (c.mean - self.min);
                    }
                    if i + 1 < self.centroids.len() {
                        let next = &self.centroids[i + 1];
                        let delta = (target - mid) / ((c.weight + next.weight) / 2.0);
                        return c.mean + delta * (next.mean - c.mean);
                    }
                    return c.mean;
                }

                if i == self.centroids.len() - 1 {
                    // In the last centroid, interpolate to max
                    if target >= mid {
                        let inner = (target - mid) / (c.weight / 2.0);
                        return c.mean + inner * (self.max - c.mean);
                    }
                    let prev = &self.centroids[i - 1];
                    let delta = (target - (cumulative - prev.weight / 2.0))
                        / ((prev.weight + c.weight) / 2.0);
                    return prev.mean + delta * (c.mean - prev.mean);
                }

                // Interior centroid
                if target <= mid {
                    if i > 0 {
                        let prev = &self.centroids[i - 1];
                        let prev_mid = cumulative - prev.weight / 2.0;
                        let delta = (target - prev_mid) / ((prev.weight + c.weight) / 2.0);
                        return prev.mean + delta * (c.mean - prev.mean);
                    }
                    return c.mean;
                } else {
                    let next = &self.centroids[i + 1];
                    let delta = (target - mid) / ((c.weight + next.weight) / 2.0);
                    return c.mean + delta * (next.mean - c.mean);
                }
            }

            cumulative = upper;
        }

        self.max
    }

    /// Estimate the CDF value (fraction of values <= the given value).
    pub fn cdf(&mut self, value: f64) -> f64 {
        self.flush();

        if self.centroids.is_empty() {
            return f64::NAN;
        }

        let total = self.merged_weight;

        if value <= self.min {
            return 0.0;
        }
        if value >= self.max {
            return 1.0;
        }

        let mut cumulative = 0.0;

        for i in 0..self.centroids.len() {
            let c = &self.centroids[i];

            if i == 0 {
                // Interpolation between min and first centroid
                if value < c.mean {
                    let frac = (value - self.min) / (c.mean - self.min);
                    return frac * (c.weight / 2.0) / total;
                }
            }

            if i == self.centroids.len() - 1 {
                // Interpolation between last centroid and max
                if value >= c.mean {
                    let frac = (value - c.mean) / (self.max - c.mean);
                    return (cumulative + c.weight / 2.0 + frac * (c.weight / 2.0)) / total;
                }
            }

            if i + 1 < self.centroids.len() {
                let next = &self.centroids[i + 1];
                if value >= c.mean && value < next.mean {
                    let mid = cumulative + c.weight / 2.0;
                    let frac = (value - c.mean) / (next.mean - c.mean);
                    return (mid + frac * ((c.weight + next.weight) / 2.0)) / total;
                }
            }

            cumulative += c.weight;
        }

        1.0
    }

    /// Estimate the rank (number of values <= the given value).
    pub fn rank(&mut self, value: f64) -> i64 {
        let cdf = self.cdf(value);
        if cdf.is_nan() {
            return -1;
        }
        (cdf * self.total_weight()).round() as i64
    }

    /// Estimate the reverse rank (number of values > the given value).
    pub fn revrank(&mut self, value: f64) -> i64 {
        let total = self.total_weight();
        if total == 0.0 {
            return -1;
        }
        let cdf = self.cdf(value);
        if cdf.is_nan() {
            return -1;
        }
        let rank = (cdf * total).round() as i64;
        (total as i64) - rank
    }

    /// Compute the trimmed mean between quantiles low_q and high_q.
    pub fn trimmed_mean(&mut self, low_q: f64, high_q: f64) -> f64 {
        self.flush();

        if self.centroids.is_empty() {
            return f64::NAN;
        }

        let total = self.merged_weight;
        let low_cut = low_q * total;
        let high_cut = high_q * total;

        if low_cut >= high_cut {
            return f64::NAN;
        }

        let mut cumulative = 0.0;
        let mut weighted_sum = 0.0;
        let mut included_weight = 0.0;

        for c in &self.centroids {
            let upper = cumulative + c.weight;

            // Compute overlap of centroid [cumulative, upper) with [low_cut, high_cut)
            let lo = cumulative.max(low_cut);
            let hi = upper.min(high_cut);

            if lo < hi {
                let fraction = (hi - lo) / c.weight;
                weighted_sum += c.mean * fraction * c.weight;
                included_weight += fraction * c.weight;
            }

            cumulative = upper;
        }

        if included_weight == 0.0 {
            f64::NAN
        } else {
            weighted_sum / included_weight
        }
    }

    /// Reset the digest, clearing all data.
    pub fn reset(&mut self) {
        self.centroids.clear();
        self.unmerged.clear();
        self.min = f64::INFINITY;
        self.max = f64::NEG_INFINITY;
        self.merged_weight = 0.0;
        self.unmerged_weight = 0.0;
    }

    /// Get the minimum observed value.
    pub fn min(&self) -> f64 {
        if self.merged_weight == 0.0 && self.unmerged_weight == 0.0 {
            f64::NAN
        } else {
            self.min
        }
    }

    /// Get the maximum observed value.
    pub fn max(&self) -> f64 {
        if self.merged_weight == 0.0 && self.unmerged_weight == 0.0 {
            f64::NAN
        } else {
            self.max
        }
    }

    /// Get the total weight (count) of all values.
    pub fn total_weight(&self) -> f64 {
        self.merged_weight + self.unmerged_weight
    }

    /// Get the compression parameter.
    pub fn compression(&self) -> f64 {
        self.compression
    }

    /// Get the merged centroids.
    pub fn centroids(&self) -> &[Centroid] {
        &self.centroids
    }

    /// Get the unmerged buffer.
    pub fn unmerged(&self) -> &[Centroid] {
        &self.unmerged
    }

    /// Get the merged weight.
    pub fn merged_weight(&self) -> f64 {
        self.merged_weight
    }

    /// Get the unmerged weight.
    pub fn unmerged_weight(&self) -> f64 {
        self.unmerged_weight
    }

    /// Get the raw min value (for serialization).
    pub fn raw_min(&self) -> f64 {
        self.min
    }

    /// Get the raw max value (for serialization).
    pub fn raw_max(&self) -> f64 {
        self.max
    }

    /// Number of merged centroids.
    pub fn num_centroids(&self) -> usize {
        self.centroids.len()
    }

    /// Set the compression parameter.
    pub fn set_compression(&mut self, compression: f64) {
        self.compression = compression;
    }

    /// Calculate approximate memory size.
    pub fn memory_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.centroids.len() * std::mem::size_of::<Centroid>()
            + self.unmerged.len() * std::mem::size_of::<Centroid>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_sketch() {
        let mut td = TDigestValue::new(100.0);
        assert!(td.min().is_nan());
        assert!(td.max().is_nan());
        assert!(td.quantile(0.5).is_nan());
        assert!(td.cdf(1.0).is_nan());
        assert_eq!(td.total_weight(), 0.0);
    }

    #[test]
    fn test_single_value() {
        let mut td = TDigestValue::new(100.0);
        td.add(42.0);
        assert_eq!(td.min(), 42.0);
        assert_eq!(td.max(), 42.0);
        assert_eq!(td.total_weight(), 1.0);
        let q50 = td.quantile(0.5);
        assert!((q50 - 42.0).abs() < 1e-10, "quantile(0.5) = {}", q50);
    }

    #[test]
    fn test_uniform_distribution_accuracy() {
        let mut td = TDigestValue::new(100.0);
        let n = 10000;
        for i in 0..n {
            td.add(i as f64);
        }

        // Check quantiles at key points
        let q50 = td.quantile(0.5);
        assert!(
            (q50 - 5000.0).abs() < 100.0,
            "p50 = {}, expected ~5000",
            q50
        );

        let q99 = td.quantile(0.99);
        assert!(
            (q99 - 9900.0).abs() < 100.0,
            "p99 = {}, expected ~9900",
            q99
        );

        let q01 = td.quantile(0.01);
        assert!((q01 - 100.0).abs() < 100.0, "p1 = {}, expected ~100", q01);
    }

    #[test]
    fn test_min_max_tracking() {
        let mut td = TDigestValue::new(100.0);
        td.add(10.0);
        td.add(-5.0);
        td.add(100.0);
        td.add(42.0);

        assert_eq!(td.min(), -5.0);
        assert_eq!(td.max(), 100.0);
    }

    #[test]
    fn test_cdf() {
        let mut td = TDigestValue::new(100.0);
        for i in 0..1000 {
            td.add(i as f64);
        }

        let cdf_500 = td.cdf(500.0);
        assert!(
            (cdf_500 - 0.5).abs() < 0.05,
            "cdf(500) = {}, expected ~0.5",
            cdf_500
        );

        assert_eq!(td.cdf(-1.0), 0.0);
        assert_eq!(td.cdf(1000.0), 1.0);
    }

    #[test]
    fn test_rank() {
        let mut td = TDigestValue::new(100.0);
        for i in 0..100 {
            td.add(i as f64);
        }

        let rank = td.rank(50.0);
        assert!((rank - 51).abs() <= 5, "rank(50) = {}, expected ~51", rank);
    }

    #[test]
    fn test_revrank() {
        let mut td = TDigestValue::new(100.0);
        for i in 0..100 {
            td.add(i as f64);
        }

        let revrank = td.revrank(50.0);
        let rank = td.rank(50.0);
        assert_eq!(
            rank + revrank,
            100,
            "rank({}) + revrank({}) = {} != 100",
            rank,
            revrank,
            rank + revrank
        );
    }

    #[test]
    fn test_merge() {
        let mut td1 = TDigestValue::new(100.0);
        for i in 0..500 {
            td1.add(i as f64);
        }

        let mut td2 = TDigestValue::new(100.0);
        for i in 500..1000 {
            td2.add(i as f64);
        }

        td1.merge_from(&td2);
        assert_eq!(td1.total_weight(), 1000.0);
        assert_eq!(td1.min(), 0.0);
        assert_eq!(td1.max(), 999.0);

        let q50 = td1.quantile(0.5);
        assert!(
            (q50 - 500.0).abs() < 50.0,
            "merged p50 = {}, expected ~500",
            q50
        );
    }

    #[test]
    fn test_reset() {
        let mut td = TDigestValue::new(100.0);
        td.add(1.0);
        td.add(2.0);
        td.add(3.0);
        td.reset();

        assert!(td.min().is_nan());
        assert!(td.max().is_nan());
        assert_eq!(td.total_weight(), 0.0);
        assert!(td.quantile(0.5).is_nan());
    }

    #[test]
    fn test_trimmed_mean() {
        let mut td = TDigestValue::new(100.0);
        for i in 0..1000 {
            td.add(i as f64);
        }

        // Trimmed mean between 25th and 75th percentile should be ~500
        let tm = td.trimmed_mean(0.25, 0.75);
        assert!(
            (tm - 500.0).abs() < 50.0,
            "trimmed_mean(0.25, 0.75) = {}, expected ~500",
            tm
        );
    }

    #[test]
    fn test_compression_effect() {
        // Higher compression = more centroids = more accuracy
        let mut td_low = TDigestValue::new(10.0);
        let mut td_high = TDigestValue::new(500.0);

        for i in 0..10000 {
            let v = i as f64;
            td_low.add(v);
            td_high.add(v);
        }

        td_low.flush();
        td_high.flush();

        // Higher compression should produce more centroids
        assert!(
            td_high.num_centroids() > td_low.num_centroids(),
            "high compression centroids ({}) should be > low ({})",
            td_high.num_centroids(),
            td_low.num_centroids()
        );
    }
}
