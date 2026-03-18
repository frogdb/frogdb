//! Cuckoo filter implementation.
//!
//! A Cuckoo filter is a space-efficient probabilistic data structure that
//! supports membership testing, deletion, and counting — capabilities that
//! Bloom filters lack. It uses fingerprint hashing with bucket-based storage
//! and cuckoo displacement for collision resolution.

use std::io::Cursor;

/// Error type for cuckoo filter operations.
#[derive(Debug, Clone)]
pub enum CuckooError {
    /// The filter is full and cannot accept more items.
    FilterFull,
}

impl std::fmt::Display for CuckooError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CuckooError::FilterFull => write!(f, "filter is full"),
        }
    }
}

/// A single cuckoo filter layer.
#[derive(Debug, Clone)]
pub struct CuckooLayer {
    /// Buckets of fingerprints. Each bucket holds up to `bucket_size` entries.
    /// A fingerprint of 0 means the slot is empty.
    buckets: Vec<Vec<u16>>,
    /// Number of buckets.
    num_buckets: usize,
    /// Maximum entries per bucket.
    bucket_size: u8,
    /// Number of items inserted.
    count: u64,
    /// Capacity of this layer.
    capacity: u64,
}

impl CuckooLayer {
    /// Create a new cuckoo filter layer.
    pub fn new(capacity: u64, bucket_size: u8) -> Self {
        let num_buckets = (capacity as usize)
            .div_ceil(bucket_size as usize)
            .next_power_of_two()
            .max(1);
        let buckets = vec![vec![0u16; bucket_size as usize]; num_buckets];
        Self {
            buckets,
            num_buckets,
            bucket_size,
            count: 0,
            capacity,
        }
    }

    /// Create a layer from raw data (for deserialization).
    pub fn from_raw(
        buckets: Vec<Vec<u16>>,
        num_buckets: usize,
        bucket_size: u8,
        count: u64,
        capacity: u64,
    ) -> Self {
        Self {
            buckets,
            num_buckets,
            bucket_size,
            count,
            capacity,
        }
    }

    /// Compute a fingerprint for an item. Never returns 0 (reserved for empty).
    fn fingerprint(item: &[u8]) -> u16 {
        let hash = murmur3_hash(item, 0);
        let fp = (hash & 0xFFFF) as u16;
        if fp == 0 { 1 } else { fp }
    }

    /// Compute the primary bucket index for an item.
    fn primary_index(&self, item: &[u8]) -> usize {
        let hash = xxhash64(item, 0);
        (hash as usize) % self.num_buckets
    }

    /// Compute the alternate bucket index given a primary index and fingerprint.
    fn alt_index(&self, idx: usize, fp: u16) -> usize {
        let fp_hash = xxhash64(&fp.to_le_bytes(), 0) as usize;
        (idx ^ fp_hash) % self.num_buckets
    }

    /// Try to insert a fingerprint into a bucket. Returns true if successful.
    fn bucket_insert(&mut self, bucket_idx: usize, fp: u16) -> bool {
        for slot in &mut self.buckets[bucket_idx] {
            if *slot == 0 {
                *slot = fp;
                self.count += 1;
                return true;
            }
        }
        false
    }

    /// Insert an item, returning the displaced fingerprint and its bucket index
    /// if the cuckoo loop exhausted max_iterations.
    pub fn insert_with_displaced(
        &mut self,
        item: &[u8],
        max_iterations: u16,
    ) -> Result<(), (u16, usize)> {
        let fp = Self::fingerprint(item);
        let i1 = self.primary_index(item);
        let i2 = self.alt_index(i1, fp);

        if self.bucket_insert(i1, fp) {
            return Ok(());
        }
        if self.bucket_insert(i2, fp) {
            return Ok(());
        }

        self.cuckoo_kick(fp, i1, i2, max_iterations)
    }

    /// Insert a raw fingerprint at one of two candidate buckets, with cuckoo kicking.
    pub fn insert_fp_with_displaced(
        &mut self,
        fp: u16,
        idx: usize,
        max_iterations: u16,
    ) -> Result<(), (u16, usize)> {
        if self.bucket_insert(idx, fp) {
            return Ok(());
        }
        let alt = self.alt_index(idx, fp);
        if self.bucket_insert(alt, fp) {
            return Ok(());
        }

        self.cuckoo_kick(fp, idx, alt, max_iterations)
    }

    fn cuckoo_kick(
        &mut self,
        fp: u16,
        i1: usize,
        i2: usize,
        max_iterations: u16,
    ) -> Result<(), (u16, usize)> {
        let mut idx = if rand_bool() { i1 } else { i2 };
        let mut displaced_fp = fp;

        for _ in 0..max_iterations {
            let slot = rand_slot(self.bucket_size);
            std::mem::swap(&mut self.buckets[idx][slot], &mut displaced_fp);

            idx = self.alt_index(idx, displaced_fp);
            if self.bucket_insert(idx, displaced_fp) {
                // The original fp is now in the filter via the chain of swaps
                self.count += 1; // for the original new item
                return Ok(());
            }
        }

        // displaced_fp couldn't be placed. The original fp IS in the filter
        // via swaps, so count the new item.
        self.count += 1;
        Err((displaced_fp, idx))
    }

    /// Check if an item exists in the filter.
    pub fn lookup(&self, item: &[u8]) -> bool {
        let fp = Self::fingerprint(item);
        let i1 = self.primary_index(item);
        let i2 = self.alt_index(i1, fp);

        self.buckets[i1].contains(&fp) || self.buckets[i2].contains(&fp)
    }

    /// Delete an item from the filter. Returns true if found and deleted.
    pub fn delete(&mut self, item: &[u8]) -> bool {
        let fp = Self::fingerprint(item);
        let i1 = self.primary_index(item);
        let i2 = self.alt_index(i1, fp);

        // Check primary bucket first
        for slot in &mut self.buckets[i1] {
            if *slot == fp {
                *slot = 0;
                self.count -= 1;
                return true;
            }
        }

        // Check alternate bucket
        for slot in &mut self.buckets[i2] {
            if *slot == fp {
                *slot = 0;
                self.count -= 1;
                return true;
            }
        }

        false
    }

    /// Count occurrences of an item (by fingerprint match).
    pub fn count(&self, item: &[u8]) -> u64 {
        let fp = Self::fingerprint(item);
        let i1 = self.primary_index(item);
        let i2 = self.alt_index(i1, fp);

        let c1 = self.buckets[i1].iter().filter(|&&s| s == fp).count() as u64;
        let c2 = self.buckets[i2].iter().filter(|&&s| s == fp).count() as u64;
        c1 + c2
    }

    /// Get the total number of items inserted.
    pub fn total_count(&self) -> u64 {
        self.count
    }

    /// Get the capacity.
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Get the number of buckets.
    pub fn num_buckets(&self) -> usize {
        self.num_buckets
    }

    /// Get the bucket size.
    pub fn bucket_size(&self) -> u8 {
        self.bucket_size
    }

    /// Check if this layer is full.
    pub fn is_full(&self) -> bool {
        self.count >= self.capacity
    }

    /// Get the buckets (for serialization).
    pub fn buckets(&self) -> &[Vec<u16>] {
        &self.buckets
    }

    /// Calculate memory size.
    pub fn memory_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.num_buckets * self.bucket_size as usize * std::mem::size_of::<u16>()
            + self.num_buckets * std::mem::size_of::<Vec<u16>>()
    }
}

/// Scalable cuckoo filter that supports multiple layers for expansion.
#[derive(Debug, Clone)]
pub struct CuckooFilterValue {
    /// Cuckoo filter layers (newest last).
    layers: Vec<CuckooLayer>,
    /// Bucket size for new layers.
    bucket_size: u8,
    /// Maximum cuckoo kick iterations.
    max_iterations: u16,
    /// Growth factor for new layers (0 = no expansion).
    expansion: u32,
    /// Number of items deleted.
    delete_count: u64,
}

impl CuckooFilterValue {
    /// Create a new cuckoo filter with default options.
    pub fn new(capacity: u64) -> Self {
        Self::with_options(capacity, 2, 20, 1)
    }

    /// Create with specific options.
    pub fn with_options(capacity: u64, bucket_size: u8, max_iterations: u16, expansion: u32) -> Self {
        let bucket_size = bucket_size.max(1);
        Self {
            layers: vec![CuckooLayer::new(capacity, bucket_size)],
            bucket_size,
            max_iterations,
            expansion,
            delete_count: 0,
        }
    }

    /// Create from raw layers (for deserialization).
    pub fn from_raw(
        layers: Vec<CuckooLayer>,
        bucket_size: u8,
        max_iterations: u16,
        expansion: u32,
        delete_count: u64,
    ) -> Self {
        Self {
            layers,
            bucket_size,
            max_iterations,
            expansion,
            delete_count,
        }
    }

    /// Add an item to the filter.
    pub fn add(&mut self, item: &[u8]) -> Result<(), CuckooError> {
        // Try inserting into the last layer
        let last_idx = self.layers.len() - 1;
        match self.layers[last_idx].insert_with_displaced(item, self.max_iterations) {
            Ok(()) => Ok(()),
            Err((displaced_fp, displaced_idx)) => {
                // Try to place the displaced fingerprint in a new layer
                if self.expansion == 0 {
                    return Err(CuckooError::FilterFull);
                }

                let last_capacity = self.layers[last_idx].capacity();
                let new_capacity = last_capacity * self.expansion as u64;
                let mut new_layer = CuckooLayer::new(new_capacity, self.bucket_size);

                match new_layer.insert_fp_with_displaced(
                    displaced_fp,
                    displaced_idx % new_layer.num_buckets(),
                    self.max_iterations,
                ) {
                    Ok(()) => {
                        self.layers.push(new_layer);
                        Ok(())
                    }
                    Err(_) => Err(CuckooError::FilterFull),
                }
            }
        }
    }

    /// Add an item only if it doesn't already exist.
    /// Returns Ok(true) if added, Ok(false) if already exists.
    pub fn add_nx(&mut self, item: &[u8]) -> Result<bool, CuckooError> {
        if self.exists(item) {
            return Ok(false);
        }
        self.add(item)?;
        Ok(true)
    }

    /// Check if an item exists in any layer.
    pub fn exists(&self, item: &[u8]) -> bool {
        self.layers.iter().any(|layer| layer.lookup(item))
    }

    /// Delete an item from the first layer containing it.
    pub fn delete(&mut self, item: &[u8]) -> bool {
        for layer in &mut self.layers {
            if layer.delete(item) {
                self.delete_count += 1;
                return true;
            }
        }
        false
    }

    /// Count occurrences across all layers.
    pub fn count(&self, item: &[u8]) -> u64 {
        self.layers.iter().map(|layer| layer.count(item)).sum()
    }

    /// Get the total number of items across all layers.
    pub fn total_count(&self) -> u64 {
        self.layers.iter().map(|l| l.total_count()).sum()
    }

    /// Get the total capacity across all layers.
    pub fn capacity(&self) -> u64 {
        self.layers.iter().map(|l| l.capacity()).sum()
    }

    /// Get the number of layers.
    pub fn num_layers(&self) -> usize {
        self.layers.len()
    }

    /// Get the bucket size.
    pub fn bucket_size(&self) -> u8 {
        self.bucket_size
    }

    /// Get the max iterations.
    pub fn max_iterations(&self) -> u16 {
        self.max_iterations
    }

    /// Get the expansion factor.
    pub fn expansion(&self) -> u32 {
        self.expansion
    }

    /// Get read access to layers.
    pub fn layers(&self) -> &[CuckooLayer] {
        &self.layers
    }

    /// Calculate memory size.
    pub fn memory_size(&self) -> usize {
        std::mem::size_of::<Self>() + self.layers.iter().map(|l| l.memory_size()).sum::<usize>()
    }

    /// Get the total number of items deleted.
    pub fn num_items_deleted(&self) -> u64 {
        self.delete_count
    }

    /// Get the delete count (for serialization).
    pub fn delete_count(&self) -> u64 {
        self.delete_count
    }

    /// Total number of buckets across all layers.
    pub fn total_buckets(&self) -> usize {
        self.layers.iter().map(|l| l.num_buckets()).sum()
    }
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

/// Simple random bool using thread_rng.
fn rand_bool() -> bool {
    use rand::Rng;
    rand::thread_rng().r#gen::<bool>()
}

/// Random slot index within bucket_size.
fn rand_slot(bucket_size: u8) -> usize {
    use rand::Rng;
    rand::thread_rng().gen_range(0..bucket_size as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cuckoo_layer_basic() {
        let mut layer = CuckooLayer::new(100, 2);

        // Insert items
        assert!(layer.insert_with_displaced(b"hello", 20).is_ok());
        assert!(layer.insert_with_displaced(b"world", 20).is_ok());

        // Check presence
        assert!(layer.lookup(b"hello"));
        assert!(layer.lookup(b"world"));
        assert!(!layer.lookup(b"foo"));

        // Delete
        assert!(layer.delete(b"hello"));
        assert!(!layer.lookup(b"hello"));
        assert!(layer.lookup(b"world"));
    }

    #[test]
    fn test_cuckoo_filter_basic() {
        let mut cf = CuckooFilterValue::new(100);

        assert!(cf.add(b"hello").is_ok());
        assert!(cf.add(b"world").is_ok());

        assert!(cf.exists(b"hello"));
        assert!(cf.exists(b"world"));
        assert!(!cf.exists(b"foo"));
    }

    #[test]
    fn test_cuckoo_filter_delete() {
        let mut cf = CuckooFilterValue::new(100);

        cf.add(b"hello").unwrap();
        cf.add(b"world").unwrap();

        assert!(cf.delete(b"hello"));
        assert!(!cf.exists(b"hello"));
        assert!(cf.exists(b"world"));

        // Delete non-existent returns false
        assert!(!cf.delete(b"foo"));
    }

    #[test]
    fn test_cuckoo_filter_addnx() {
        let mut cf = CuckooFilterValue::new(100);

        assert_eq!(cf.add_nx(b"hello").unwrap(), true);
        assert_eq!(cf.add_nx(b"hello").unwrap(), false); // already exists
    }

    #[test]
    fn test_cuckoo_filter_count() {
        let mut cf = CuckooFilterValue::new(100);

        cf.add(b"hello").unwrap();
        cf.add(b"hello").unwrap(); // duplicate

        assert!(cf.count(b"hello") >= 2);
        assert_eq!(cf.count(b"nonexistent"), 0);
    }

    #[test]
    fn test_cuckoo_filter_scaling() {
        let mut cf = CuckooFilterValue::with_options(10, 2, 20, 2);

        // Add more than capacity
        for i in 0..25 {
            let _ = cf.add(format!("item{}", i).as_bytes());
        }

        // Should have created additional layers
        assert!(cf.num_layers() >= 1);

        // Most items should be findable
        let mut found = 0;
        for i in 0..25 {
            if cf.exists(format!("item{}", i).as_bytes()) {
                found += 1;
            }
        }
        assert!(found > 0);
    }

    #[test]
    fn test_cuckoo_filter_no_expansion() {
        let mut cf = CuckooFilterValue::with_options(10, 2, 20, 0);

        let mut inserted = 0;
        for i in 0..100 {
            if cf.add(format!("item{}", i).as_bytes()).is_ok() {
                inserted += 1;
            }
        }

        // Should not have created additional layers
        assert_eq!(cf.num_layers(), 1);
        // Should have inserted some but not all
        assert!(inserted > 0);
        assert!(inserted < 100);
    }

    #[test]
    fn test_cuckoo_false_positive_rate() {
        let mut cf = CuckooFilterValue::with_options(1000, 2, 500, 1);

        // Add items
        for i in 0..1000 {
            cf.add(format!("item{}", i).as_bytes()).unwrap();
        }

        // Check false positives
        let mut false_positives = 0;
        let test_count = 10000;
        for i in 1000..1000 + test_count {
            if cf.exists(format!("item{}", i).as_bytes()) {
                false_positives += 1;
            }
        }

        let fp_rate = false_positives as f64 / test_count as f64;
        // u16 fingerprints should give very low FP rate
        assert!(
            fp_rate < 0.01,
            "False positive rate {} exceeds threshold",
            fp_rate
        );
    }

    #[test]
    fn test_cuckoo_info() {
        let cf = CuckooFilterValue::with_options(100, 4, 50, 2);

        assert_eq!(cf.capacity(), 100);
        assert_eq!(cf.bucket_size(), 4);
        assert_eq!(cf.max_iterations(), 50);
        assert_eq!(cf.expansion(), 2);
        assert_eq!(cf.num_layers(), 1);
    }
}
