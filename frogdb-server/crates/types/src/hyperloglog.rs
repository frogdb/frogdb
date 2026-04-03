//! HyperLogLog probabilistic cardinality estimation.
//!
//! A HyperLogLog is a space-efficient probabilistic data structure that
//! estimates the cardinality (number of unique elements) in a set.
//!
//! This implementation uses:
//! - 16384 registers (2^14) with 6 bits each
//! - Sparse encoding for small sets (< ~3000 elements)
//! - Dense encoding (12KB fixed) for larger sets
//! - ~0.81% standard error

use std::io::Cursor;

/// Number of HyperLogLog registers (2^14 = 16384).
pub const HLL_REGISTERS: usize = 16384;

/// Number of bits per register (max value 63).
pub const HLL_BITS: u8 = 6;

/// Size of the dense representation in bytes (16384 * 6 / 8 = 12288).
pub const HLL_DENSE_SIZE: usize = 12288;

/// Threshold for promoting sparse to dense encoding.
/// When sparse has more than this many registers set, promote to dense.
const SPARSE_TO_DENSE_THRESHOLD: usize = 3000;

/// Alpha constant for bias correction with 16384 registers.
const ALPHA_16384: f64 = 0.7213 / (1.0 + 1.079 / 16384.0);

/// Redis-compatible Murmur3 seed ("_avre" in ASCII).
const MURMUR3_SEED: u32 = 0x5f617672;

/// HyperLogLog encoding representation.
#[derive(Debug, Clone)]
enum HllEncoding {
    /// Sparse: list of (register_index, value) pairs.
    /// Efficient for small sets with few registers set.
    Sparse(Vec<(u16, u8)>),
    /// Dense: all 16384 registers packed into 12KB.
    /// 6 bits per register, packed 4 registers into 3 bytes.
    Dense(Box<[u8; HLL_DENSE_SIZE]>),
}

/// HyperLogLog value for probabilistic cardinality estimation.
#[derive(Debug, Clone)]
pub struct HyperLogLogValue {
    /// The encoding representation (sparse or dense).
    encoding: HllEncoding,
    /// Cached cardinality estimate (invalidated on updates).
    cached_cardinality: Option<u64>,
}

impl HyperLogLogValue {
    /// Create a new empty HyperLogLog with sparse encoding.
    pub fn new() -> Self {
        Self {
            encoding: HllEncoding::Sparse(Vec::new()),
            cached_cardinality: Some(0),
        }
    }

    /// Create a HyperLogLog from a sparse representation.
    pub fn from_sparse(pairs: Vec<(u16, u8)>) -> Self {
        let mut hll = Self {
            encoding: HllEncoding::Sparse(pairs),
            cached_cardinality: None,
        };
        hll.maybe_promote();
        hll
    }

    /// Create a HyperLogLog from a dense representation.
    pub fn from_dense(registers: Box<[u8; HLL_DENSE_SIZE]>) -> Self {
        Self {
            encoding: HllEncoding::Dense(registers),
            cached_cardinality: None,
        }
    }

    /// Add an element to the HyperLogLog.
    ///
    /// Returns true if any register was updated (internal state changed).
    pub fn add(&mut self, element: &[u8]) -> bool {
        let hash = murmur3_hash(element);

        // First 14 bits = register index (0-16383)
        let index = (hash & 0x3FFF) as u16;

        // Remaining 50 bits used to count leading zeros
        let remaining = hash >> 14;
        // Count leading zeros in the remaining 50 bits
        // 64 - 14 = 50 bits, so we subtract 14 from leading_zeros
        let zeros = remaining.leading_zeros().saturating_sub(14);
        // Value is leading zeros + 1, capped at 63 (6 bits max)
        let value = ((zeros + 1) as u8).min(63);

        let changed = self.set_register_if_greater(index, value);
        if changed {
            self.cached_cardinality = None;
        }
        changed
    }

    /// Get the estimated cardinality.
    ///
    /// For single HLL, this returns the cached value if available (O(1)).
    /// Otherwise computes the estimate (O(N) where N = number of registers).
    pub fn count(&mut self) -> u64 {
        if let Some(cached) = self.cached_cardinality {
            return cached;
        }

        let estimate = self.compute_cardinality();
        self.cached_cardinality = Some(estimate);
        estimate
    }

    /// Compute the cardinality without caching (for multi-key operations).
    pub fn count_no_cache(&self) -> u64 {
        if let Some(cached) = self.cached_cardinality {
            return cached;
        }
        self.compute_cardinality()
    }

    /// Merge another HyperLogLog into this one (per-register MAX).
    ///
    /// Returns true if any register was updated.
    pub fn merge(&mut self, other: &HyperLogLogValue) -> bool {
        let mut changed = false;

        match &other.encoding {
            HllEncoding::Sparse(pairs) => {
                // Iterate only the non-zero registers in the sparse representation.
                // This is O(N) instead of O(16384 × N) for the old approach.
                for &(index, value) in pairs {
                    if self.set_register_if_greater(index, value) {
                        changed = true;
                    }
                }
            }
            HllEncoding::Dense(registers) => {
                for i in 0..HLL_REGISTERS {
                    let other_val = dense_get_register(registers, i);
                    if other_val > 0 && self.set_register_if_greater(i as u16, other_val) {
                        changed = true;
                    }
                }
            }
        }

        if changed {
            self.cached_cardinality = None;
        }
        changed
    }

    /// Get a register value by index.
    pub fn get_register(&self, index: u16) -> u8 {
        match &self.encoding {
            HllEncoding::Sparse(pairs) => pairs
                .iter()
                .find(|(i, _)| *i == index)
                .map(|(_, v)| *v)
                .unwrap_or(0),
            HllEncoding::Dense(registers) => dense_get_register(registers, index as usize),
        }
    }

    /// Set a register value if the new value is greater than the current.
    ///
    /// Returns true if the register was updated.
    fn set_register_if_greater(&mut self, index: u16, value: u8) -> bool {
        match &mut self.encoding {
            HllEncoding::Sparse(pairs) => {
                if let Some(pos) = pairs.iter().position(|(i, _)| *i == index) {
                    if value > pairs[pos].1 {
                        pairs[pos].1 = value;
                        return true;
                    }
                    return false;
                }
                // New register
                pairs.push((index, value));
                self.maybe_promote();
                true
            }
            HllEncoding::Dense(registers) => {
                let current = dense_get_register(registers, index as usize);
                if value > current {
                    dense_set_register(registers, index as usize, value);
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Maybe promote sparse to dense encoding.
    fn maybe_promote(&mut self) {
        if let HllEncoding::Sparse(pairs) = &self.encoding {
            // Promote when:
            // - More than SPARSE_TO_DENSE_THRESHOLD unique registers are set
            // - Or when sparse encoding would exceed dense size
            if pairs.len() > SPARSE_TO_DENSE_THRESHOLD || pairs.len() * 3 > HLL_DENSE_SIZE {
                self.promote_to_dense();
            }
        }
    }

    /// Promote from sparse to dense encoding.
    fn promote_to_dense(&mut self) {
        if let HllEncoding::Sparse(pairs) = &self.encoding {
            let mut dense = Box::new([0u8; HLL_DENSE_SIZE]);
            for (index, value) in pairs {
                dense_set_register(&mut dense, *index as usize, *value);
            }
            self.encoding = HllEncoding::Dense(dense);
        }
    }

    /// Compute the cardinality estimate using the HyperLogLog algorithm.
    fn compute_cardinality(&self) -> u64 {
        let m = HLL_REGISTERS as f64;

        // Compute the harmonic mean of 2^(-register_value)
        let mut sum = 0.0;
        let mut zeros = 0u32;

        for i in 0..HLL_REGISTERS {
            let reg_val = self.get_register(i as u16);
            sum += 2.0_f64.powi(-(reg_val as i32));
            if reg_val == 0 {
                zeros += 1;
            }
        }

        // Raw HyperLogLog estimate
        let raw_estimate = ALPHA_16384 * m * m / sum;

        // Apply bias corrections
        let estimate = if raw_estimate <= 2.5 * m {
            // Small range correction: linear counting if there are zeros
            if zeros > 0 {
                // Linear counting: m * ln(m/zeros)
                m * (m / zeros as f64).ln()
            } else {
                raw_estimate
            }
        } else if raw_estimate <= (1u64 << 32) as f64 / 30.0 {
            // Intermediate range: no correction needed
            raw_estimate
        } else {
            // Large range correction
            let two32 = (1u64 << 32) as f64;
            -two32 * (1.0 - raw_estimate / two32).ln()
        };

        estimate.round() as u64
    }

    /// Check if this HyperLogLog uses sparse encoding.
    pub fn is_sparse(&self) -> bool {
        matches!(self.encoding, HllEncoding::Sparse(_))
    }

    /// Get the encoding type as a string.
    pub fn encoding_str(&self) -> &'static str {
        match &self.encoding {
            HllEncoding::Sparse(_) => "sparse",
            HllEncoding::Dense(_) => "dense",
        }
    }

    /// Get the number of registers set (for sparse) or total registers (for dense).
    pub fn num_registers(&self) -> usize {
        match &self.encoding {
            HllEncoding::Sparse(pairs) => pairs.len(),
            HllEncoding::Dense(_) => HLL_REGISTERS,
        }
    }

    /// Calculate the approximate memory size.
    pub fn memory_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + match &self.encoding {
                HllEncoding::Sparse(pairs) => pairs.len() * std::mem::size_of::<(u16, u8)>(),
                HllEncoding::Dense(_) => HLL_DENSE_SIZE,
            }
    }

    /// Get a reference to sparse pairs (if sparse encoding).
    pub fn as_sparse(&self) -> Option<&[(u16, u8)]> {
        match &self.encoding {
            HllEncoding::Sparse(pairs) => Some(pairs),
            HllEncoding::Dense(_) => None,
        }
    }

    /// Get a reference to dense registers (if dense encoding).
    pub fn as_dense(&self) -> Option<&[u8; HLL_DENSE_SIZE]> {
        match &self.encoding {
            HllEncoding::Sparse(_) => None,
            HllEncoding::Dense(registers) => Some(registers),
        }
    }

    /// Serialize the HyperLogLog for cross-shard copy.
    /// Format: encoding_byte || data
    /// - encoding_byte: 0 = sparse, 1 = dense
    /// - For sparse: num_pairs(4 bytes) || (index(2 bytes) || value(1 byte))*
    /// - For dense: 12288 bytes of register data
    pub fn serialize(&self) -> Vec<u8> {
        match &self.encoding {
            HllEncoding::Sparse(pairs) => {
                let mut buf = Vec::with_capacity(1 + 4 + pairs.len() * 3);
                buf.push(0); // sparse encoding
                buf.extend_from_slice(&(pairs.len() as u32).to_le_bytes());
                for (index, value) in pairs {
                    buf.extend_from_slice(&index.to_le_bytes());
                    buf.push(*value);
                }
                buf
            }
            HllEncoding::Dense(registers) => {
                let mut buf = Vec::with_capacity(1 + HLL_DENSE_SIZE);
                buf.push(1); // dense encoding
                buf.extend_from_slice(registers.as_ref());
                buf
            }
        }
    }

    /// Deserialize a HyperLogLog from cross-shard copy data.
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.is_empty() {
            return None;
        }

        match data[0] {
            0 => {
                // Sparse encoding
                if data.len() < 5 {
                    return None;
                }
                let num_pairs = u32::from_le_bytes(data[1..5].try_into().ok()?) as usize;
                let expected_len = 5 + num_pairs * 3;
                if data.len() < expected_len {
                    return None;
                }

                let mut pairs = Vec::with_capacity(num_pairs);
                let mut pos = 5;
                for _ in 0..num_pairs {
                    let index = u16::from_le_bytes(data[pos..pos + 2].try_into().ok()?);
                    let value = data[pos + 2];
                    pairs.push((index, value));
                    pos += 3;
                }
                Some(Self::from_sparse(pairs))
            }
            1 => {
                // Dense encoding
                if data.len() < 1 + HLL_DENSE_SIZE {
                    return None;
                }
                let mut registers = Box::new([0u8; HLL_DENSE_SIZE]);
                registers.copy_from_slice(&data[1..1 + HLL_DENSE_SIZE]);
                Some(Self::from_dense(registers))
            }
            _ => None,
        }
    }
}

impl Default for HyperLogLogValue {
    fn default() -> Self {
        Self::new()
    }
}

/// Get a 6-bit register value from dense encoding.
///
/// Registers are packed 4 per 3 bytes:
/// - Register 0: bits 0-5 of byte 0
/// - Register 1: bits 6-7 of byte 0 + bits 0-3 of byte 1
/// - Register 2: bits 4-7 of byte 1 + bits 0-1 of byte 2
/// - Register 3: bits 2-7 of byte 2
fn dense_get_register(registers: &[u8; HLL_DENSE_SIZE], index: usize) -> u8 {
    let byte_pos = (index * 6) / 8;
    let bit_pos = (index * 6) % 8;

    if bit_pos <= 2 {
        // Register fits in one byte
        (registers[byte_pos] >> bit_pos) & 0x3F
    } else {
        // Register spans two bytes
        let low = registers[byte_pos] >> bit_pos;
        let high = registers[byte_pos + 1] << (8 - bit_pos);
        (low | high) & 0x3F
    }
}

/// Set a 6-bit register value in dense encoding.
fn dense_set_register(registers: &mut [u8; HLL_DENSE_SIZE], index: usize, value: u8) {
    let value = value & 0x3F; // Ensure 6 bits
    let byte_pos = (index * 6) / 8;
    let bit_pos = (index * 6) % 8;

    if bit_pos <= 2 {
        // Register fits in one byte
        let mask = !(0x3F << bit_pos);
        registers[byte_pos] = (registers[byte_pos] & mask) | (value << bit_pos);
    } else {
        // Register spans two bytes
        let low_mask = !((0xFF >> bit_pos) << bit_pos);
        let high_bits = 6 - (8 - bit_pos);
        let high_mask = (1 << high_bits) - 1;

        registers[byte_pos] = (registers[byte_pos] & low_mask) | (value << bit_pos);
        registers[byte_pos + 1] = (registers[byte_pos + 1] & !high_mask) | (value >> (8 - bit_pos));
    }
}

/// Compute Murmur3 64-bit hash.
fn murmur3_hash(data: &[u8]) -> u64 {
    let mut cursor = Cursor::new(data);
    murmur3::murmur3_x64_128(&mut cursor, MURMUR3_SEED).unwrap_or(0) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_hll_is_sparse() {
        let hll = HyperLogLogValue::new();
        assert!(hll.is_sparse());
        assert_eq!(hll.num_registers(), 0);
    }

    #[test]
    fn test_add_single_element() {
        let mut hll = HyperLogLogValue::new();
        let changed = hll.add(b"hello");
        assert!(changed);
        assert!(hll.count() > 0);
    }

    #[test]
    fn test_add_duplicate_is_idempotent() {
        let mut hll = HyperLogLogValue::new();
        hll.add(b"hello");
        let initial_count = hll.count();

        // Adding the same element shouldn't change the count
        let changed = hll.add(b"hello");
        assert!(!changed);
        assert_eq!(hll.count(), initial_count);
    }

    #[test]
    fn test_cardinality_estimation_accuracy() {
        let mut hll = HyperLogLogValue::new();

        // Add 1000 unique elements
        for i in 0..1000 {
            hll.add(format!("element:{}", i).as_bytes());
        }

        let estimate = hll.count();
        // Allow 5% error margin (HLL has ~0.81% standard error, but we allow more for tests)
        let lower = 950;
        let upper = 1050;
        assert!(
            estimate >= lower && estimate <= upper,
            "Estimate {} not in range [{}, {}]",
            estimate,
            lower,
            upper
        );
    }

    #[test]
    fn test_sparse_encoding_for_small_sets() {
        let mut hll = HyperLogLogValue::new();

        // Add a few elements
        for i in 0..10 {
            hll.add(format!("item:{}", i).as_bytes());
        }

        // Should still be sparse
        assert!(hll.is_sparse());
    }

    #[test]
    fn test_promotion_to_dense() {
        let mut hll = HyperLogLogValue::new();

        // Add many unique elements to force promotion
        for i in 0..5000 {
            hll.add(format!("element:{}", i).as_bytes());
        }

        // Should have promoted to dense
        assert!(!hll.is_sparse());
        assert_eq!(hll.encoding_str(), "dense");
    }

    #[test]
    fn test_dense_register_get_set() {
        let mut registers = Box::new([0u8; HLL_DENSE_SIZE]);

        // Test various register positions
        for i in [0, 1, 2, 3, 4, 100, 1000, 16383] {
            dense_set_register(&mut registers, i, 42);
            assert_eq!(dense_get_register(&registers, i), 42);
        }

        // Test max value (63)
        dense_set_register(&mut registers, 500, 63);
        assert_eq!(dense_get_register(&registers, 500), 63);
    }

    #[test]
    fn test_merge_disjoint() {
        let mut hll1 = HyperLogLogValue::new();
        let mut hll2 = HyperLogLogValue::new();

        // Add different elements to each
        for i in 0..100 {
            hll1.add(format!("set1:{}", i).as_bytes());
            hll2.add(format!("set2:{}", i).as_bytes());
        }

        let count1 = hll1.count();
        let count2 = hll2.count();

        // Merge hll2 into hll1
        hll1.merge(&hll2);

        // Merged count should be approximately count1 + count2
        let merged = hll1.count();
        let expected = count1 + count2;
        let lower = (expected as f64 * 0.9) as u64;
        let upper = (expected as f64 * 1.1) as u64;
        assert!(
            merged >= lower && merged <= upper,
            "Merged {} not in expected range [{}, {}]",
            merged,
            lower,
            upper
        );
    }

    #[test]
    fn test_merge_overlapping() {
        let mut hll1 = HyperLogLogValue::new();
        let mut hll2 = HyperLogLogValue::new();

        // Add overlapping elements
        for i in 0..100 {
            hll1.add(format!("item:{}", i).as_bytes());
        }
        for i in 50..150 {
            hll2.add(format!("item:{}", i).as_bytes());
        }

        hll1.merge(&hll2);

        // Should count ~150 unique items (0-149)
        let merged = hll1.count();
        let lower = 140;
        let upper = 160;
        assert!(
            merged >= lower && merged <= upper,
            "Merged {} not in expected range [{}, {}]",
            merged,
            lower,
            upper
        );
    }

    #[test]
    fn test_memory_size() {
        let hll_sparse = HyperLogLogValue::new();
        let sparse_size = hll_sparse.memory_size();

        let mut hll_dense = HyperLogLogValue::new();
        for i in 0..5000 {
            hll_dense.add(format!("e:{}", i).as_bytes());
        }
        let dense_size = hll_dense.memory_size();

        // Dense should be larger (includes the full 12KB)
        assert!(dense_size > sparse_size);
        assert!(dense_size >= HLL_DENSE_SIZE);
    }

    #[test]
    fn test_from_sparse() {
        let pairs = vec![(0, 5), (100, 10), (1000, 15)];
        let hll = HyperLogLogValue::from_sparse(pairs);
        assert!(hll.is_sparse());
        assert_eq!(hll.get_register(0), 5);
        assert_eq!(hll.get_register(100), 10);
        assert_eq!(hll.get_register(1000), 15);
    }

    #[test]
    fn test_from_dense() {
        let mut registers = Box::new([0u8; HLL_DENSE_SIZE]);
        dense_set_register(&mut registers, 0, 5);
        dense_set_register(&mut registers, 100, 10);

        let hll = HyperLogLogValue::from_dense(registers);
        assert!(!hll.is_sparse());
        assert_eq!(hll.get_register(0), 5);
        assert_eq!(hll.get_register(100), 10);
    }

    #[test]
    fn test_empty_hll_count() {
        let mut hll = HyperLogLogValue::new();
        assert_eq!(hll.count(), 0);
    }

    #[test]
    fn test_large_cardinality() {
        let mut hll = HyperLogLogValue::new();

        // Add 100K unique elements
        for i in 0..100_000 {
            hll.add(format!("x:{}", i).as_bytes());
        }

        let estimate = hll.count();
        // Allow 2% error
        let lower = 98_000;
        let upper = 102_000;
        assert!(
            estimate >= lower && estimate <= upper,
            "Estimate {} not in range [{}, {}]",
            estimate,
            lower,
            upper
        );
    }
}
