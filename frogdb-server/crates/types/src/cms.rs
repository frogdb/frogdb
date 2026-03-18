//! Count-Min Sketch probabilistic data structure.
//!
//! A space-efficient 2D counter array (depth × width) for frequency estimation
//! in data streams. Each row uses a different hash function; incrementing hashes
//! an item per row and bumps those counters; querying returns the minimum counter
//! across rows.

/// Count-Min Sketch data structure.
#[derive(Debug, Clone)]
pub struct CountMinSketchValue {
    width: u32,
    depth: u32,
    count: u64,
    counters: Vec<Vec<u64>>,
}

/// Compute xxhash64.
fn xxhash64(data: &[u8], seed: u64) -> u64 {
    xxhash_rust::xxh64::xxh64(data, seed)
}

impl CountMinSketchValue {
    /// Create a new Count-Min Sketch with the given dimensions.
    pub fn new(width: u32, depth: u32) -> Self {
        let counters = vec![vec![0u64; width as usize]; depth as usize];
        Self {
            width,
            depth,
            count: 0,
            counters,
        }
    }

    /// Create from error rate and probability parameters.
    /// width = ⌈e/error⌉, depth = ⌈ln(1/probability)⌉
    pub fn from_error_and_prob(error: f64, probability: f64) -> Self {
        let width = (std::f64::consts::E / error).ceil() as u32;
        let depth = (1.0_f64 / probability).ln().ceil() as u32;
        Self::new(width, depth)
    }

    /// Reconstruct from raw components (for deserialization).
    pub fn from_raw(width: u32, depth: u32, count: u64, counters: Vec<Vec<u64>>) -> Self {
        Self {
            width,
            depth,
            count,
            counters,
        }
    }

    /// Increment the count for an item.
    pub fn increment(&mut self, item: &[u8], count: u64) {
        for d in 0..self.depth as usize {
            let j = (xxhash64(item, d as u64 + 1) % self.width as u64) as usize;
            self.counters[d][j] = self.counters[d][j].saturating_add(count);
        }
        self.count = self.count.saturating_add(count);
    }

    /// Query the estimated count of an item (minimum across rows).
    pub fn query(&self, item: &[u8]) -> u64 {
        let mut min = u64::MAX;
        for d in 0..self.depth as usize {
            let j = (xxhash64(item, d as u64 + 1) % self.width as u64) as usize;
            min = min.min(self.counters[d][j]);
        }
        min
    }

    /// Get the width.
    pub fn width(&self) -> u32 {
        self.width
    }

    /// Get the depth.
    pub fn depth(&self) -> u32 {
        self.depth
    }

    /// Get the total count of all increments.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Get raw counter data for serialization.
    pub fn counters_raw(&self) -> &[Vec<u64>] {
        &self.counters
    }

    /// Calculate approximate memory size.
    pub fn memory_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.depth as usize * self.width as usize * std::mem::size_of::<u64>()
            + self.depth as usize * std::mem::size_of::<Vec<u64>>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cms_from_error_and_prob() {
        let cms = CountMinSketchValue::from_error_and_prob(0.01, 0.001);
        // width = ceil(e / 0.01) = ceil(271.828...) = 272
        assert_eq!(cms.width(), 272);
        // depth = ceil(ln(1/0.001)) = ceil(6.907...) = 7
        assert_eq!(cms.depth(), 7);
    }

    #[test]
    fn test_cms_increment_and_query() {
        let mut cms = CountMinSketchValue::new(100, 5);

        cms.increment(b"apple", 3);
        cms.increment(b"banana", 7);
        cms.increment(b"apple", 2);

        // apple was incremented by 3 + 2 = 5
        assert!(cms.query(b"apple") >= 5);
        // banana was incremented by 7
        assert!(cms.query(b"banana") >= 7);
        // total count
        assert_eq!(cms.count(), 12);
    }

    #[test]
    fn test_cms_query_missing() {
        let cms = CountMinSketchValue::new(100, 5);
        assert_eq!(cms.query(b"nonexistent"), 0);
    }

    #[test]
    fn test_cms_memory_size() {
        let cms = CountMinSketchValue::new(100, 5);
        assert!(cms.memory_size() > 0);
    }

    #[test]
    fn test_cms_from_raw() {
        let mut cms = CountMinSketchValue::new(10, 3);
        cms.increment(b"test", 5);

        let reconstructed = CountMinSketchValue::from_raw(
            cms.width(),
            cms.depth(),
            cms.count(),
            cms.counters_raw().to_vec(),
        );

        assert_eq!(reconstructed.width(), cms.width());
        assert_eq!(reconstructed.depth(), cms.depth());
        assert_eq!(reconstructed.count(), cms.count());
        assert_eq!(reconstructed.query(b"test"), cms.query(b"test"));
    }
}
