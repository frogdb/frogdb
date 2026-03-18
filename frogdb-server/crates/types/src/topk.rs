//! Top-K probabilistic data structure using the HeavyKeeper algorithm.
//!
//! Maintains an approximate list of the k most frequent items in a data stream.
//! Uses a Count-Min Sketch-like 2D bucket array with exponential decay plus a
//! min-heap to track the top-k items.

use bytes::Bytes;
use std::collections::HashMap;

/// A single bucket in the HeavyKeeper array.
#[derive(Debug, Clone)]
struct TopKBucket {
    fingerprint: u32,
    counter: u32,
}

/// Top-K data structure using the HeavyKeeper algorithm.
#[derive(Debug, Clone)]
pub struct TopKValue {
    k: u32,
    width: u32,
    depth: u32,
    decay: f64,
    buckets: Vec<Vec<TopKBucket>>,
    /// Min-heap by count (size <= k). Stored as vec, heapified manually.
    heap: Vec<(Bytes, u64)>,
    /// Parallel index for O(1) query/count lookup.
    item_counts: HashMap<Bytes, u64>,
}

/// Compute xxhash64.
fn xxhash64(data: &[u8], seed: u64) -> u64 {
    xxhash_rust::xxh64::xxh64(data, seed)
}

impl TopKValue {
    /// Create a new Top-K with the given parameters.
    pub fn new(k: u32, width: u32, depth: u32, decay: f64) -> Self {
        let buckets = (0..depth)
            .map(|_| {
                (0..width)
                    .map(|_| TopKBucket {
                        fingerprint: 0,
                        counter: 0,
                    })
                    .collect()
            })
            .collect();

        Self {
            k,
            width,
            depth,
            decay,
            buckets,
            heap: Vec::with_capacity(k as usize),
            item_counts: HashMap::new(),
        }
    }

    /// Reconstruct from raw components (for deserialization).
    pub fn from_raw(
        k: u32,
        width: u32,
        depth: u32,
        decay: f64,
        buckets: Vec<Vec<(u32, u32)>>,
        heap_items: Vec<(Bytes, u64)>,
    ) -> Self {
        let buckets = buckets
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|(fingerprint, counter)| TopKBucket {
                        fingerprint,
                        counter,
                    })
                    .collect()
            })
            .collect();

        let item_counts: HashMap<Bytes, u64> =
            heap_items.iter().map(|(k, v)| (k.clone(), *v)).collect();

        let mut value = Self {
            k,
            width,
            depth,
            decay,
            buckets,
            heap: heap_items,
            item_counts,
        };
        // Rebuild heap property
        value.rebuild_heap();
        value
    }

    /// Add an item with increment, returning any expelled item.
    pub fn add(&mut self, item: &[u8], increment: u64) -> Option<Bytes> {
        let fingerprint = xxhash64(item, 0) as u32;
        let mut max_count: u64 = 0;

        for d in 0..self.depth as usize {
            let j = (xxhash64(item, d as u64 + 1) % self.width as u64) as usize;
            let bucket = &mut self.buckets[d][j];

            if bucket.counter == 0 || bucket.fingerprint == fingerprint {
                // Empty bucket or same fingerprint — increment
                bucket.fingerprint = fingerprint;
                bucket.counter = bucket.counter.saturating_add(increment as u32);
                max_count = max_count.max(bucket.counter as u64);
            } else {
                // Different fingerprint — decay
                let mut decremented = 0u32;
                for _ in 0..increment {
                    let prob = self.decay.powi(bucket.counter as i32);
                    if rand::random::<f64>() < prob {
                        decremented += 1;
                    }
                }
                if decremented >= bucket.counter {
                    // Replace the bucket
                    bucket.fingerprint = fingerprint;
                    bucket.counter = increment as u32;
                    max_count = max_count.max(bucket.counter as u64);
                } else {
                    bucket.counter -= decremented;
                }
            }
        }

        if max_count == 0 {
            return None;
        }

        let item_bytes = Bytes::copy_from_slice(item);

        // If item already in heap, update its count
        if let Some(existing_count) = self.item_counts.get_mut(&item_bytes) {
            *existing_count = max_count;
            // Update heap entry and re-heapify
            if let Some(pos) = self.heap.iter().position(|(k, _)| k == &item_bytes) {
                self.heap[pos].1 = max_count;
            }
            self.rebuild_heap();
            return None;
        }

        // If heap not full, just push
        if (self.heap.len() as u32) < self.k {
            self.item_counts.insert(item_bytes.clone(), max_count);
            self.heap.push((item_bytes, max_count));
            self.rebuild_heap();
            return None;
        }

        // If count > heap min, swap out the min
        if let Some(&(_, min_count)) = self.heap.first()
            && max_count > min_count
        {
            let expelled = self.heap[0].0.clone();
            self.item_counts.remove(&expelled);
            self.heap[0] = (item_bytes.clone(), max_count);
            self.item_counts.insert(item_bytes, max_count);
            self.rebuild_heap();
            return Some(expelled);
        }

        None
    }

    /// Check if an item is in the top-k.
    pub fn query(&self, item: &[u8]) -> bool {
        let item_bytes = Bytes::copy_from_slice(item);
        self.item_counts.contains_key(&item_bytes)
    }

    /// Get the estimated count of an item.
    pub fn count(&self, item: &[u8]) -> u64 {
        let item_bytes = Bytes::copy_from_slice(item);
        self.item_counts.get(&item_bytes).copied().unwrap_or(0)
    }

    /// List all items in the top-k with their counts, sorted by count descending.
    pub fn list(&self) -> Vec<(&Bytes, u64)> {
        let mut items: Vec<(&Bytes, u64)> = self.heap.iter().map(|(k, v)| (k, *v)).collect();
        items.sort_by(|a, b| b.1.cmp(&a.1));
        items
    }

    /// Get k.
    pub fn k(&self) -> u32 {
        self.k
    }

    /// Get width.
    pub fn width(&self) -> u32 {
        self.width
    }

    /// Get depth.
    pub fn depth(&self) -> u32 {
        self.depth
    }

    /// Get decay.
    pub fn decay(&self) -> f64 {
        self.decay
    }

    /// Get raw bucket data for serialization: (fingerprint, counter) pairs.
    pub fn buckets_raw(&self) -> Vec<Vec<(u32, u32)>> {
        self.buckets
            .iter()
            .map(|row| {
                row.iter()
                    .map(|b| (b.fingerprint, b.counter))
                    .collect()
            })
            .collect()
    }

    /// Get heap items for serialization.
    pub fn heap_items(&self) -> &[(Bytes, u64)] {
        &self.heap
    }

    /// Calculate approximate memory size.
    pub fn memory_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + (self.depth as usize * self.width as usize * std::mem::size_of::<TopKBucket>())
            + self
                .heap
                .iter()
                .map(|(k, _)| k.len() + std::mem::size_of::<(Bytes, u64)>())
                .sum::<usize>()
            + self
                .item_counts
                .keys()
                .map(|k| k.len() + std::mem::size_of::<(Bytes, u64)>() + 32)
                .sum::<usize>()
    }

    /// Rebuild min-heap property (by count, ascending).
    fn rebuild_heap(&mut self) {
        let len = self.heap.len();
        if len <= 1 {
            return;
        }
        // Build min-heap from the bottom up
        for i in (0..len / 2).rev() {
            self.sift_down(i);
        }
    }

    fn sift_down(&mut self, mut idx: usize) {
        let len = self.heap.len();
        loop {
            let left = 2 * idx + 1;
            let right = 2 * idx + 2;
            let mut smallest = idx;

            if left < len && self.heap[left].1 < self.heap[smallest].1 {
                smallest = left;
            }
            if right < len && self.heap[right].1 < self.heap[smallest].1 {
                smallest = right;
            }

            if smallest == idx {
                break;
            }
            self.heap.swap(idx, smallest);
            idx = smallest;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topk_basic_add() {
        let mut tk = TopKValue::new(3, 8, 4, 0.9);

        // Add items
        let expelled = tk.add(b"apple", 1);
        assert!(expelled.is_none());
        assert!(tk.query(b"apple"));
        assert!(tk.count(b"apple") > 0);
    }

    #[test]
    fn test_topk_incrby() {
        let mut tk = TopKValue::new(3, 8, 4, 0.9);

        tk.add(b"apple", 10);
        assert!(tk.count(b"apple") >= 10);
    }

    #[test]
    fn test_topk_expulsion() {
        let mut tk = TopKValue::new(2, 8, 4, 0.9);

        // Fill the heap
        for _ in 0..20 {
            tk.add(b"high", 1);
        }
        for _ in 0..10 {
            tk.add(b"medium", 1);
        }

        // Both should be in
        assert!(tk.query(b"high"));
        assert!(tk.query(b"medium"));

        // Adding a third item with low count — should be expelled or not enter
        tk.add(b"low", 1);

        // The top 2 should remain (high and medium)
        let items = tk.list();
        assert!(items.len() <= 2);
    }

    #[test]
    fn test_topk_query_missing() {
        let tk = TopKValue::new(3, 8, 4, 0.9);
        assert!(!tk.query(b"nonexistent"));
        assert_eq!(tk.count(b"nonexistent"), 0);
    }

    #[test]
    fn test_topk_list() {
        let mut tk = TopKValue::new(5, 8, 4, 0.9);

        tk.add(b"a", 5);
        tk.add(b"b", 3);
        tk.add(b"c", 1);

        let items = tk.list();
        assert!(!items.is_empty());
        // Should be sorted descending by count
        for i in 1..items.len() {
            assert!(items[i - 1].1 >= items[i].1);
        }
    }

    #[test]
    fn test_topk_info() {
        let tk = TopKValue::new(10, 50, 5, 0.92);
        assert_eq!(tk.k(), 10);
        assert_eq!(tk.width(), 50);
        assert_eq!(tk.depth(), 5);
        assert!((tk.decay() - 0.92).abs() < 0.001);
    }

    #[test]
    fn test_topk_memory_size() {
        let tk = TopKValue::new(5, 8, 4, 0.9);
        assert!(tk.memory_size() > 0);
    }
}
