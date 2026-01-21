//! Scalable Bloom filter implementation.
//!
//! A Bloom filter is a space-efficient probabilistic data structure that
//! can answer "is element X in the set?" with:
//! - Definite NO (element definitely not in set)
//! - Probable YES (element probably in set, with configurable false positive rate)
//!
//! This implementation supports automatic scaling by adding new layers when
//! the filter reaches capacity.

use bitvec::prelude::*;
use std::io::Cursor;

/// A single bloom filter layer.
#[derive(Debug, Clone)]
pub struct BloomLayer {
    /// Bit vector storing the filter data.
    bits: BitVec<u8, Lsb0>,
    /// Number of hash functions.
    k: u32,
    /// Number of items added to this layer.
    count: u64,
    /// Capacity of this layer.
    capacity: u64,
}

impl BloomLayer {
    /// Create a new bloom filter layer with the given parameters.
    pub fn new(capacity: u64, error_rate: f64) -> Self {
        // Calculate optimal number of bits: m = -n * ln(p) / (ln(2)^2)
        let ln2_sq = std::f64::consts::LN_2 * std::f64::consts::LN_2;
        let m = (-(capacity as f64) * error_rate.ln() / ln2_sq).ceil() as usize;
        let m = m.max(8); // Minimum size

        // Calculate optimal number of hash functions: k = (m/n) * ln(2)
        let k = ((m as f64 / capacity as f64) * std::f64::consts::LN_2).ceil() as u32;
        let k = k.max(1).min(32); // Reasonable bounds

        Self {
            bits: bitvec![u8, Lsb0; 0; m],
            k,
            count: 0,
            capacity,
        }
    }

    /// Create a layer from raw data (for deserialization).
    pub fn from_raw(bits: BitVec<u8, Lsb0>, k: u32, count: u64, capacity: u64) -> Self {
        Self {
            bits,
            k,
            count,
            capacity,
        }
    }

    /// Add an item to the filter.
    ///
    /// Returns true if the item was probably already present.
    pub fn add(&mut self, item: &[u8]) -> bool {
        let was_present = self.contains(item);
        let m = self.bits.len();

        for i in 0..self.k {
            let hash = double_hash(item, i, m);
            self.bits.set(hash, true);
        }

        if !was_present {
            self.count += 1;
        }

        was_present
    }

    /// Check if an item might be in the filter.
    pub fn contains(&self, item: &[u8]) -> bool {
        let m = self.bits.len();

        for i in 0..self.k {
            let hash = double_hash(item, i, m);
            if !self.bits[hash] {
                return false;
            }
        }

        true
    }

    /// Check if this layer has reached capacity.
    pub fn is_full(&self) -> bool {
        self.count >= self.capacity
    }

    /// Get the number of items in this layer.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Get the capacity of this layer.
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Get the number of hash functions.
    pub fn k(&self) -> u32 {
        self.k
    }

    /// Get the size of the bit vector in bits.
    pub fn size_bits(&self) -> usize {
        self.bits.len()
    }

    /// Get the raw bits as a byte slice.
    pub fn bits_as_bytes(&self) -> &[u8] {
        self.bits.as_raw_slice()
    }

    /// Calculate memory size.
    pub fn memory_size(&self) -> usize {
        std::mem::size_of::<Self>() + self.bits.as_raw_slice().len()
    }
}

/// Scalable bloom filter that automatically adds layers when capacity is reached.
#[derive(Debug, Clone)]
pub struct BloomFilterValue {
    /// Bloom filter layers (newest last).
    layers: Vec<BloomLayer>,
    /// Base error rate.
    error_rate: f64,
    /// Growth factor for each new layer's capacity.
    expansion: u32,
    /// Whether scaling is disabled.
    non_scaling: bool,
}

impl BloomFilterValue {
    /// Create a new scalable bloom filter.
    pub fn new(capacity: u64, error_rate: f64) -> Self {
        Self {
            layers: vec![BloomLayer::new(capacity, error_rate)],
            error_rate,
            expansion: 2,
            non_scaling: false,
        }
    }

    /// Create with specific options.
    pub fn with_options(capacity: u64, error_rate: f64, expansion: u32, non_scaling: bool) -> Self {
        Self {
            layers: vec![BloomLayer::new(capacity, error_rate)],
            error_rate,
            expansion: expansion.max(1),
            non_scaling,
        }
    }

    /// Create from raw layers (for deserialization).
    pub fn from_raw(
        layers: Vec<BloomLayer>,
        error_rate: f64,
        expansion: u32,
        non_scaling: bool,
    ) -> Self {
        Self {
            layers,
            error_rate,
            expansion,
            non_scaling,
        }
    }

    /// Add an item to the filter.
    ///
    /// Returns true if the item was added (not previously present).
    pub fn add(&mut self, item: &[u8]) -> bool {
        // Check if already present in any layer
        if self.contains(item) {
            return false;
        }

        // Find a layer with capacity, or create a new one
        let layer_idx = if let Some(idx) = self.layers.iter().position(|l| !l.is_full()) {
            idx
        } else if self.non_scaling {
            // Non-scaling: use the last layer even if full
            self.layers.len() - 1
        } else {
            // Create a new layer with expanded capacity and tighter error rate
            let last = self.layers.last().unwrap();
            let new_capacity = last.capacity() * self.expansion as u64;
            // Tighter error rate for newer layers to maintain overall rate
            let layer_count = self.layers.len() as f64;
            let new_error_rate = self.error_rate * 0.5f64.powf(layer_count);
            self.layers.push(BloomLayer::new(new_capacity, new_error_rate));
            self.layers.len() - 1
        };

        self.layers[layer_idx].add(item);
        true
    }

    /// Check if an item might be in the filter.
    pub fn contains(&self, item: &[u8]) -> bool {
        self.layers.iter().any(|layer| layer.contains(item))
    }

    /// Get the total number of items.
    pub fn count(&self) -> u64 {
        self.layers.iter().map(|l| l.count()).sum()
    }

    /// Get the total capacity.
    pub fn capacity(&self) -> u64 {
        self.layers.iter().map(|l| l.capacity()).sum()
    }

    /// Get the error rate.
    pub fn error_rate(&self) -> f64 {
        self.error_rate
    }

    /// Get the expansion factor.
    pub fn expansion(&self) -> u32 {
        self.expansion
    }

    /// Get the number of layers.
    pub fn num_layers(&self) -> usize {
        self.layers.len()
    }

    /// Get read access to layers.
    pub fn layers(&self) -> &[BloomLayer] {
        &self.layers
    }

    /// Check if scaling is disabled.
    pub fn is_non_scaling(&self) -> bool {
        self.non_scaling
    }

    /// Calculate memory size.
    pub fn memory_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.layers.iter().map(|l| l.memory_size()).sum::<usize>()
    }
}

/// Double hashing: hash_i = (hash1 + i * hash2) % m
fn double_hash(item: &[u8], i: u32, m: usize) -> usize {
    let hash1 = murmur3_hash(item, 0);
    let hash2 = xxhash64(item, 0);

    let combined = hash1.wrapping_add((i as u64).wrapping_mul(hash2));
    (combined % m as u64) as usize
}

/// Compute murmur3 hash.
fn murmur3_hash(data: &[u8], seed: u32) -> u64 {
    let mut cursor = Cursor::new(data);
    murmur3::murmur3_x64_128(&mut cursor, seed).unwrap_or(0) as u64
}

/// Compute xxhash64.
fn xxhash64(data: &[u8], seed: u64) -> u64 {
    xxhash_rust::xxh64::xxh64(data, seed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_layer_basic() {
        let mut layer = BloomLayer::new(100, 0.01);

        // Add items
        assert!(!layer.add(b"hello"));
        assert!(!layer.add(b"world"));

        // Check presence
        assert!(layer.contains(b"hello"));
        assert!(layer.contains(b"world"));
        assert!(!layer.contains(b"foo"));

        // Re-adding should return true (was present)
        assert!(layer.add(b"hello"));
    }

    #[test]
    fn test_bloom_filter_scaling() {
        let mut bf = BloomFilterValue::new(10, 0.01);

        // Add more than capacity
        for i in 0..25 {
            bf.add(format!("item{}", i).as_bytes());
        }

        // Should have created additional layers
        assert!(bf.num_layers() > 1);

        // All items should still be found
        for i in 0..25 {
            assert!(bf.contains(format!("item{}", i).as_bytes()));
        }
    }

    #[test]
    fn test_bloom_filter_non_scaling() {
        let mut bf = BloomFilterValue::with_options(10, 0.01, 2, true);

        // Add more than capacity
        for i in 0..25 {
            bf.add(format!("item{}", i).as_bytes());
        }

        // Should not have created additional layers
        assert_eq!(bf.num_layers(), 1);
    }

    #[test]
    fn test_bloom_false_positive_rate() {
        let mut bf = BloomFilterValue::new(1000, 0.01);

        // Add items
        for i in 0..1000 {
            bf.add(format!("item{}", i).as_bytes());
        }

        // Check false positives
        let mut false_positives = 0;
        let test_count = 10000;
        for i in 1000..1000 + test_count {
            if bf.contains(format!("item{}", i).as_bytes()) {
                false_positives += 1;
            }
        }

        let fp_rate = false_positives as f64 / test_count as f64;
        // Allow some margin since this is probabilistic
        assert!(
            fp_rate < 0.05,
            "False positive rate {} exceeds threshold",
            fp_rate
        );
    }

    #[test]
    fn test_bloom_info() {
        let bf = BloomFilterValue::with_options(100, 0.01, 4, false);

        assert_eq!(bf.capacity(), 100);
        assert!((bf.error_rate() - 0.01).abs() < 0.0001);
        assert_eq!(bf.expansion(), 4);
        assert_eq!(bf.num_layers(), 1);
        assert!(!bf.is_non_scaling());
    }
}
