//! Power-of-2 histogram for keysize and key-memory tracking.
//!
//! Tracks element counts (or byte lengths) in power-of-2 bins.
//! Bin `i` counts values with size in `[2^i, 2^(i+1) - 1]`.
//! Bin 0 is the special case for size 0 or 1 (empty collections or single elements).

use std::fmt;

/// Number of bins in the histogram (covers up to 2^63).
const NUM_BINS: usize = 64;

/// Power-of-2 histogram with fixed-size bin array.
///
/// Each bin `i` counts the number of keys whose logical size falls in
/// `[2^i, 2^(i+1) - 1]`. Bin 0 covers sizes 0 and 1.
#[derive(Clone)]
pub struct PowerOfTwoHistogram {
    bins: [u64; NUM_BINS],
}

impl Default for PowerOfTwoHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for PowerOfTwoHistogram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PowerOfTwoHistogram")
            .field("total", &self.total())
            .finish()
    }
}

impl PowerOfTwoHistogram {
    /// Create a new empty histogram.
    pub fn new() -> Self {
        Self {
            bins: [0; NUM_BINS],
        }
    }

    /// Compute the bin index for a given size.
    ///
    /// Bin 0 covers sizes 0 and 1. For size >= 2, uses the position of
    /// the highest set bit: bin = ceil(log2(size)).
    #[inline]
    pub fn bin_for(size: usize) -> usize {
        if size <= 1 {
            0
        } else {
            (usize::BITS - (size - 1).leading_zeros()) as usize
        }
    }

    /// Increment the count for the bin corresponding to `size`.
    #[inline]
    pub fn increment(&mut self, size: usize) {
        let bin = Self::bin_for(size);
        self.bins[bin] += 1;
    }

    /// Decrement the count for the bin corresponding to `size`.
    #[inline]
    pub fn decrement(&mut self, size: usize) {
        let bin = Self::bin_for(size);
        self.bins[bin] = self.bins[bin].saturating_sub(1);
    }

    /// Migrate a key from one size bin to another (decrement old, increment new).
    #[inline]
    pub fn migrate(&mut self, old_size: usize, new_size: usize) {
        let old_bin = Self::bin_for(old_size);
        let new_bin = Self::bin_for(new_size);
        if old_bin != new_bin {
            self.bins[old_bin] = self.bins[old_bin].saturating_sub(1);
            self.bins[new_bin] += 1;
        }
    }

    /// Clear all bins.
    pub fn clear(&mut self) {
        self.bins = [0; NUM_BINS];
    }

    /// Returns true if all bins are zero.
    pub fn is_empty(&self) -> bool {
        self.bins.iter().all(|&c| c == 0)
    }

    /// Get the count for a specific bin index.
    #[inline]
    pub fn get_bin(&self, bin: usize) -> u64 {
        if bin < NUM_BINS { self.bins[bin] } else { 0 }
    }

    /// Total count across all bins.
    pub fn total(&self) -> u64 {
        self.bins.iter().sum()
    }

    /// Access the raw bins array (for scatter-gather merge).
    pub fn bins(&self) -> &[u64; NUM_BINS] {
        &self.bins
    }

    /// Merge another histogram's bins into this one (additive).
    pub fn merge(&mut self, other: &PowerOfTwoHistogram) {
        for (dst, src) in self.bins.iter_mut().zip(other.bins.iter()) {
            *dst += src;
        }
    }

    /// Format bins in Redis-compatible format.
    ///
    /// Produces a string like `0=3,2=5,8=1,1K=2` where only non-zero bins
    /// are emitted. The bin label is the human-readable power-of-2 value:
    /// 0, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1K, 2K, 4K, 8K, 16K, 32K,
    /// 64K, 128K, 256K, 512K, 1M, 2M, 4M, ...
    pub fn format_bins(&self) -> String {
        let mut parts = Vec::new();
        for (i, &count) in self.bins.iter().enumerate() {
            if count > 0 {
                parts.push(format!("{}={}", bin_label(i), count));
            }
        }
        parts.join(",")
    }
}

/// Format a bin index as a human-readable power-of-2 label.
///
/// Bin 0 = "0", bin 1 = "2", bin 2 = "4", ..., bin 10 = "1K",
/// bin 20 = "1M", bin 30 = "1G", bin 40 = "1T", etc.
fn bin_label(bin: usize) -> String {
    if bin == 0 {
        return "0".to_string();
    }
    let value: u64 = 1u64 << bin;
    if value < 1024 {
        format!("{}", value)
    } else if value < 1024 * 1024 {
        let k = value / 1024;
        format!("{}K", k)
    } else if value < 1024 * 1024 * 1024 {
        let m = value / (1024 * 1024);
        format!("{}M", m)
    } else if value < 1024 * 1024 * 1024 * 1024 {
        let g = value / (1024 * 1024 * 1024);
        format!("{}G", g)
    } else {
        let t = value / (1024 * 1024 * 1024 * 1024);
        format!("{}T", t)
    }
}

/// Names for the histogram types, matching Redis's INFO keysizes format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeysizeType {
    /// String byte length: `distrib_strings_sizes`
    Strings,
    /// List element count: `distrib_lists_items`
    Lists,
    /// Set cardinality: `distrib_sets_items`
    Sets,
    /// Hash field count: `distrib_hashes_items`
    Hashes,
    /// Sorted set cardinality: `distrib_zsets_items`
    Zsets,
    /// Stream entry count: `distrib_streams_items`
    Streams,
    /// HLL estimated cardinality: `distrib_strings_sizes` (HLL tracked as string)
    Hlls,
}

impl KeysizeType {
    /// Returns the INFO keysizes field name for this type.
    pub fn info_field_name(&self) -> &'static str {
        match self {
            KeysizeType::Strings => "distrib_strings_sizes",
            KeysizeType::Lists => "distrib_lists_items",
            KeysizeType::Sets => "distrib_sets_items",
            KeysizeType::Hashes => "distrib_hashes_items",
            KeysizeType::Zsets => "distrib_zsets_items",
            KeysizeType::Streams => "distrib_streams_items",
            KeysizeType::Hlls => "distrib_strings_sizes_hll",
        }
    }

    /// Returns the DEBUG KEYSIZES-HIST-ASSERT type name (lowercase).
    pub fn debug_name(&self) -> &'static str {
        match self {
            KeysizeType::Strings => "strings",
            KeysizeType::Lists => "lists",
            KeysizeType::Sets => "sets",
            KeysizeType::Hashes => "hashes",
            KeysizeType::Zsets => "zsets",
            KeysizeType::Streams => "streams",
            KeysizeType::Hlls => "hlls",
        }
    }

    /// Parse a type name from DEBUG command input.
    pub fn from_debug_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "strings" => Some(KeysizeType::Strings),
            "lists" => Some(KeysizeType::Lists),
            "sets" => Some(KeysizeType::Sets),
            "hashes" => Some(KeysizeType::Hashes),
            "zsets" => Some(KeysizeType::Zsets),
            "streams" => Some(KeysizeType::Streams),
            "hlls" => Some(KeysizeType::Hlls),
            _ => None,
        }
    }

    /// All keysize types in order.
    pub const ALL: &'static [KeysizeType] = &[
        KeysizeType::Strings,
        KeysizeType::Lists,
        KeysizeType::Sets,
        KeysizeType::Hashes,
        KeysizeType::Zsets,
        KeysizeType::Streams,
        KeysizeType::Hlls,
    ];
}

/// Collection of per-type keysize histograms.
#[derive(Clone, Debug)]
pub struct KeysizeHistograms {
    pub strings: PowerOfTwoHistogram,
    pub lists: PowerOfTwoHistogram,
    pub sets: PowerOfTwoHistogram,
    pub hashes: PowerOfTwoHistogram,
    pub zsets: PowerOfTwoHistogram,
    pub streams: PowerOfTwoHistogram,
    pub hlls: PowerOfTwoHistogram,
    /// Key memory histogram (tracks total memory per key).
    pub key_memory: PowerOfTwoHistogram,
    /// Whether key memory histograms are enabled.
    pub key_memory_enabled: bool,
}

impl Default for KeysizeHistograms {
    fn default() -> Self {
        Self::new()
    }
}

impl KeysizeHistograms {
    /// Create a new set of histograms with key-memory enabled.
    pub fn new() -> Self {
        Self {
            strings: PowerOfTwoHistogram::new(),
            lists: PowerOfTwoHistogram::new(),
            sets: PowerOfTwoHistogram::new(),
            hashes: PowerOfTwoHistogram::new(),
            zsets: PowerOfTwoHistogram::new(),
            streams: PowerOfTwoHistogram::new(),
            hlls: PowerOfTwoHistogram::new(),
            key_memory: PowerOfTwoHistogram::new(),
            key_memory_enabled: true,
        }
    }

    /// Create histograms with key-memory disabled.
    pub fn new_without_memory() -> Self {
        Self {
            key_memory_enabled: false,
            ..Self::new()
        }
    }

    /// Get the histogram for a given keysize type.
    pub fn get(&self, ty: KeysizeType) -> &PowerOfTwoHistogram {
        match ty {
            KeysizeType::Strings => &self.strings,
            KeysizeType::Lists => &self.lists,
            KeysizeType::Sets => &self.sets,
            KeysizeType::Hashes => &self.hashes,
            KeysizeType::Zsets => &self.zsets,
            KeysizeType::Streams => &self.streams,
            KeysizeType::Hlls => &self.hlls,
        }
    }

    /// Get a mutable histogram for a given keysize type.
    pub fn get_mut(&mut self, ty: KeysizeType) -> &mut PowerOfTwoHistogram {
        match ty {
            KeysizeType::Strings => &mut self.strings,
            KeysizeType::Lists => &mut self.lists,
            KeysizeType::Sets => &mut self.sets,
            KeysizeType::Hashes => &mut self.hashes,
            KeysizeType::Zsets => &mut self.zsets,
            KeysizeType::Streams => &mut self.streams,
            KeysizeType::Hlls => &mut self.hlls,
        }
    }

    /// Clear all histograms.
    pub fn clear(&mut self) {
        self.strings.clear();
        self.lists.clear();
        self.sets.clear();
        self.hashes.clear();
        self.zsets.clear();
        self.streams.clear();
        self.hlls.clear();
        self.key_memory.clear();
    }

    /// Merge another set of histograms into this one (additive, for scatter-gather).
    pub fn merge(&mut self, other: &KeysizeHistograms) {
        self.strings.merge(&other.strings);
        self.lists.merge(&other.lists);
        self.sets.merge(&other.sets);
        self.hashes.merge(&other.hashes);
        self.zsets.merge(&other.zsets);
        self.streams.merge(&other.streams);
        self.hlls.merge(&other.hlls);
        self.key_memory.merge(&other.key_memory);
    }

    /// Disable key-memory tracking. Once disabled, cannot be re-enabled.
    pub fn disable_key_memory(&mut self) {
        self.key_memory_enabled = false;
        self.key_memory.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bin_for() {
        assert_eq!(PowerOfTwoHistogram::bin_for(0), 0);
        assert_eq!(PowerOfTwoHistogram::bin_for(1), 0);
        assert_eq!(PowerOfTwoHistogram::bin_for(2), 1);
        assert_eq!(PowerOfTwoHistogram::bin_for(3), 2);
        assert_eq!(PowerOfTwoHistogram::bin_for(4), 2);
        assert_eq!(PowerOfTwoHistogram::bin_for(5), 3);
        assert_eq!(PowerOfTwoHistogram::bin_for(8), 3);
        assert_eq!(PowerOfTwoHistogram::bin_for(9), 4);
        assert_eq!(PowerOfTwoHistogram::bin_for(16), 4);
        assert_eq!(PowerOfTwoHistogram::bin_for(17), 5);
        assert_eq!(PowerOfTwoHistogram::bin_for(1023), 10);
        assert_eq!(PowerOfTwoHistogram::bin_for(1024), 10);
        assert_eq!(PowerOfTwoHistogram::bin_for(1025), 11);
    }

    #[test]
    fn test_increment_decrement() {
        let mut h = PowerOfTwoHistogram::new();
        assert!(h.is_empty());

        h.increment(5);
        assert_eq!(h.get_bin(3), 1);
        assert!(!h.is_empty());

        h.increment(7);
        assert_eq!(h.get_bin(3), 2);

        h.decrement(5);
        assert_eq!(h.get_bin(3), 1);

        h.decrement(7);
        assert!(h.is_empty());
    }

    #[test]
    fn test_migrate() {
        let mut h = PowerOfTwoHistogram::new();
        h.increment(5); // bin 3
        assert_eq!(h.get_bin(3), 1);

        h.migrate(5, 20); // bin 3 -> bin 5
        assert_eq!(h.get_bin(3), 0);
        assert_eq!(h.get_bin(5), 1);
    }

    #[test]
    fn test_migrate_same_bin() {
        let mut h = PowerOfTwoHistogram::new();
        h.increment(5); // bin 3
        h.migrate(5, 6); // both in bin 3, no change
        assert_eq!(h.get_bin(3), 1);
    }

    #[test]
    fn test_format_bins() {
        let mut h = PowerOfTwoHistogram::new();
        assert_eq!(h.format_bins(), "");

        h.increment(0); // bin 0
        assert_eq!(h.format_bins(), "0=1");

        h.increment(5); // bin 3 (8)
        assert_eq!(h.format_bins(), "0=1,8=1");

        h.increment(1024); // bin 10 (1K)
        assert_eq!(h.format_bins(), "0=1,8=1,1K=1");
    }

    #[test]
    fn test_bin_labels() {
        assert_eq!(bin_label(0), "0");
        assert_eq!(bin_label(1), "2");
        assert_eq!(bin_label(2), "4");
        assert_eq!(bin_label(3), "8");
        assert_eq!(bin_label(10), "1K");
        assert_eq!(bin_label(11), "2K");
        assert_eq!(bin_label(20), "1M");
        assert_eq!(bin_label(30), "1G");
        assert_eq!(bin_label(40), "1T");
    }

    #[test]
    fn test_merge() {
        let mut h1 = PowerOfTwoHistogram::new();
        let mut h2 = PowerOfTwoHistogram::new();

        h1.increment(5);
        h2.increment(5);
        h2.increment(100);

        h1.merge(&h2);
        assert_eq!(h1.get_bin(3), 2); // bin for 5
        assert_eq!(h1.get_bin(7), 1); // bin for 100
    }

    #[test]
    fn test_clear() {
        let mut h = PowerOfTwoHistogram::new();
        h.increment(5);
        h.increment(100);
        assert!(!h.is_empty());

        h.clear();
        assert!(h.is_empty());
    }
}
