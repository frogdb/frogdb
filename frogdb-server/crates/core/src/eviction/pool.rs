//! Eviction pool for maintaining best eviction candidates.
//!
//! The eviction pool is a fixed-size collection that maintains the best
//! eviction candidates found during sampling. This improves eviction
//! quality by accumulating good candidates across multiple samples.
//!
//! Per Redis design:
//! - Pool size is 16 entries
//! - Candidates are ranked by their "idle time" (for LRU) or frequency (for LFU)
//! - When pool is full, worst candidate is replaced if new one is better

use bytes::Bytes;
use std::time::{Duration, Instant};

use super::EVICTION_POOL_SIZE;

/// A candidate for eviction with ranking information.
#[derive(Debug, Clone)]
pub struct EvictionCandidate {
    /// The key to potentially evict.
    pub key: Bytes,

    /// Idle time for LRU policies (time since last access).
    pub idle_time: Duration,

    /// LFU counter value for LFU policies (lower = more likely to evict).
    pub lfu_value: u8,

    /// TTL remaining for volatile-ttl policy (None = no TTL).
    pub ttl_remaining: Option<Duration>,
}

impl EvictionCandidate {
    /// Create a new eviction candidate.
    pub fn new(
        key: Bytes,
        idle_time: Duration,
        lfu_value: u8,
        ttl_remaining: Option<Duration>,
    ) -> Self {
        Self {
            key,
            idle_time,
            lfu_value,
            ttl_remaining,
        }
    }

    /// Create a candidate from key metadata.
    pub fn from_metadata(
        key: Bytes,
        last_access: Instant,
        lfu_counter: u8,
        expires_at: Option<Instant>,
        now: Instant,
    ) -> Self {
        let idle_time = now.duration_since(last_access);
        let ttl_remaining = expires_at.map(|exp| {
            if exp > now {
                exp.duration_since(now)
            } else {
                Duration::ZERO
            }
        });

        Self::new(key, idle_time, lfu_counter, ttl_remaining)
    }

    /// Compare for LRU ordering (higher idle time = worse, should be evicted first).
    pub fn lru_rank(&self) -> u64 {
        self.idle_time.as_micros() as u64
    }

    /// Compare for LFU ordering (lower counter = worse, should be evicted first).
    /// We invert so higher rank = worse candidate.
    pub fn lfu_rank(&self) -> u64 {
        255 - self.lfu_value as u64
    }

    /// Compare for TTL ordering (lower remaining TTL = worse, should be evicted first).
    /// Returns max value if no TTL set.
    pub fn ttl_rank(&self) -> u64 {
        match self.ttl_remaining {
            Some(ttl) => u64::MAX - ttl.as_micros() as u64,
            None => 0, // No TTL means this key shouldn't be evicted by volatile-ttl
        }
    }
}

/// Pool of eviction candidates.
///
/// Maintains up to `EVICTION_POOL_SIZE` candidates, keeping the best
/// (worst from data's perspective) candidates for eviction.
#[derive(Debug)]
pub struct EvictionPool {
    /// Candidates sorted by rank (worst candidates first).
    candidates: Vec<EvictionCandidate>,
}

impl Default for EvictionPool {
    fn default() -> Self {
        Self::new()
    }
}

impl EvictionPool {
    /// Create a new empty eviction pool.
    pub fn new() -> Self {
        Self {
            candidates: Vec::with_capacity(EVICTION_POOL_SIZE),
        }
    }

    /// Get the number of candidates in the pool.
    pub fn len(&self) -> usize {
        self.candidates.len()
    }

    /// Check if the pool is empty.
    pub fn is_empty(&self) -> bool {
        self.candidates.is_empty()
    }

    /// Check if the pool is full.
    pub fn is_full(&self) -> bool {
        self.candidates.len() >= EVICTION_POOL_SIZE
    }

    /// Clear all candidates from the pool.
    pub fn clear(&mut self) {
        self.candidates.clear();
    }

    /// Remove a specific key from the pool.
    ///
    /// Used when a key is deleted/evicted and should no longer be a candidate.
    pub fn remove(&mut self, key: &[u8]) {
        self.candidates.retain(|c| c.key.as_ref() != key);
    }

    /// Try to insert a candidate for LRU eviction.
    ///
    /// The candidate is inserted if:
    /// - The pool is not full, or
    /// - The candidate has higher idle time than the best candidate in pool
    ///
    /// Returns true if the candidate was inserted.
    pub fn maybe_insert_lru(&mut self, candidate: EvictionCandidate) -> bool {
        // Don't add duplicates
        if self.candidates.iter().any(|c| c.key == candidate.key) {
            return false;
        }

        let rank = candidate.lru_rank();

        if self.is_full() {
            // Find the best (lowest rank) candidate to potentially replace
            let min_rank = self
                .candidates
                .iter()
                .map(|c| c.lru_rank())
                .min()
                .unwrap_or(0);
            if rank <= min_rank {
                return false;
            }

            // Remove the best candidate (least idle)
            if let Some(pos) = self
                .candidates
                .iter()
                .position(|c| c.lru_rank() == min_rank)
            {
                self.candidates.remove(pos);
            }
        }

        // Insert in sorted position (worst first)
        let pos = self
            .candidates
            .iter()
            .position(|c| c.lru_rank() < rank)
            .unwrap_or(self.candidates.len());
        self.candidates.insert(pos, candidate);
        true
    }

    /// Try to insert a candidate for LFU eviction.
    ///
    /// The candidate is inserted if:
    /// - The pool is not full, or
    /// - The candidate has lower LFU counter than the best candidate in pool
    ///
    /// Returns true if the candidate was inserted.
    pub fn maybe_insert_lfu(&mut self, candidate: EvictionCandidate) -> bool {
        // Don't add duplicates
        if self.candidates.iter().any(|c| c.key == candidate.key) {
            return false;
        }

        let rank = candidate.lfu_rank();

        if self.is_full() {
            // Find the best (lowest rank = highest counter) candidate
            let min_rank = self
                .candidates
                .iter()
                .map(|c| c.lfu_rank())
                .min()
                .unwrap_or(0);
            if rank <= min_rank {
                return false;
            }

            // Remove the best candidate (highest counter)
            if let Some(pos) = self
                .candidates
                .iter()
                .position(|c| c.lfu_rank() == min_rank)
            {
                self.candidates.remove(pos);
            }
        }

        // Insert in sorted position (worst first)
        let pos = self
            .candidates
            .iter()
            .position(|c| c.lfu_rank() < rank)
            .unwrap_or(self.candidates.len());
        self.candidates.insert(pos, candidate);
        true
    }

    /// Try to insert a candidate for TTL-based eviction.
    ///
    /// Only considers candidates with TTL set. Prefers keys with shorter
    /// remaining TTL.
    ///
    /// Returns true if the candidate was inserted.
    pub fn maybe_insert_ttl(&mut self, candidate: EvictionCandidate) -> bool {
        // Only consider keys with TTL
        if candidate.ttl_remaining.is_none() {
            return false;
        }

        // Don't add duplicates
        if self.candidates.iter().any(|c| c.key == candidate.key) {
            return false;
        }

        let rank = candidate.ttl_rank();

        if self.is_full() {
            // Find the best (lowest rank = longest TTL) candidate
            let min_rank = self
                .candidates
                .iter()
                .map(|c| c.ttl_rank())
                .min()
                .unwrap_or(0);
            if rank <= min_rank {
                return false;
            }

            // Remove the best candidate
            if let Some(pos) = self
                .candidates
                .iter()
                .position(|c| c.ttl_rank() == min_rank)
            {
                self.candidates.remove(pos);
            }
        }

        // Insert in sorted position (worst first)
        let pos = self
            .candidates
            .iter()
            .position(|c| c.ttl_rank() < rank)
            .unwrap_or(self.candidates.len());
        self.candidates.insert(pos, candidate);
        true
    }

    /// Pop the worst candidate (best to evict) from the pool.
    ///
    /// Returns the candidate with the highest rank (worst from data's perspective).
    pub fn pop_worst(&mut self) -> Option<EvictionCandidate> {
        if self.candidates.is_empty() {
            None
        } else {
            // First candidate is the worst
            Some(self.candidates.remove(0))
        }
    }

    /// Peek at the worst candidate without removing it.
    pub fn peek_worst(&self) -> Option<&EvictionCandidate> {
        self.candidates.first()
    }

    /// Get an iterator over all candidates.
    pub fn iter(&self) -> impl Iterator<Item = &EvictionCandidate> {
        self.candidates.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_candidate(key: &str, idle_ms: u64, lfu: u8, ttl_ms: Option<u64>) -> EvictionCandidate {
        EvictionCandidate::new(
            Bytes::from(key.to_string()),
            Duration::from_millis(idle_ms),
            lfu,
            ttl_ms.map(Duration::from_millis),
        )
    }

    #[test]
    fn test_pool_new() {
        let pool = EvictionPool::new();
        assert!(pool.is_empty());
        assert!(!pool.is_full());
        assert_eq!(pool.len(), 0);
    }

    #[test]
    fn test_pool_insert_lru() {
        let mut pool = EvictionPool::new();

        // Insert first candidate
        let c1 = make_candidate("key1", 1000, 5, None);
        assert!(pool.maybe_insert_lru(c1));
        assert_eq!(pool.len(), 1);

        // Insert second candidate with higher idle time (worse)
        let c2 = make_candidate("key2", 2000, 5, None);
        assert!(pool.maybe_insert_lru(c2));
        assert_eq!(pool.len(), 2);

        // Worst candidate should be key2 (higher idle time)
        let worst = pool.peek_worst().unwrap();
        assert_eq!(worst.key.as_ref(), b"key2");
    }

    #[test]
    fn test_pool_insert_lru_no_duplicates() {
        let mut pool = EvictionPool::new();

        let c1 = make_candidate("key1", 1000, 5, None);
        assert!(pool.maybe_insert_lru(c1.clone()));
        assert!(!pool.maybe_insert_lru(c1)); // Duplicate rejected
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn test_pool_insert_lfu() {
        let mut pool = EvictionPool::new();

        // Insert candidate with high counter (good, less likely to evict)
        let c1 = make_candidate("key1", 1000, 100, None);
        assert!(pool.maybe_insert_lfu(c1));

        // Insert candidate with low counter (bad, more likely to evict)
        let c2 = make_candidate("key2", 1000, 5, None);
        assert!(pool.maybe_insert_lfu(c2));

        // Worst candidate should be key2 (lower counter)
        let worst = pool.peek_worst().unwrap();
        assert_eq!(worst.key.as_ref(), b"key2");
    }

    #[test]
    fn test_pool_insert_ttl() {
        let mut pool = EvictionPool::new();

        // Insert candidate with long TTL
        let c1 = make_candidate("key1", 1000, 5, Some(10000));
        assert!(pool.maybe_insert_ttl(c1));

        // Insert candidate with short TTL (worse)
        let c2 = make_candidate("key2", 1000, 5, Some(1000));
        assert!(pool.maybe_insert_ttl(c2));

        // Candidate without TTL should be rejected
        let c3 = make_candidate("key3", 1000, 5, None);
        assert!(!pool.maybe_insert_ttl(c3));

        // Worst candidate should be key2 (shorter TTL)
        let worst = pool.peek_worst().unwrap();
        assert_eq!(worst.key.as_ref(), b"key2");
    }

    #[test]
    fn test_pool_capacity() {
        let mut pool = EvictionPool::new();

        // Fill the pool
        for i in 0..EVICTION_POOL_SIZE {
            let c = make_candidate(&format!("key{}", i), i as u64 * 100, 5, None);
            assert!(pool.maybe_insert_lru(c));
        }
        assert!(pool.is_full());

        // Try to insert a worse candidate (should succeed, replacing best)
        let c = make_candidate("new_key", 10000, 5, None);
        assert!(pool.maybe_insert_lru(c));
        assert!(pool.is_full());

        // Try to insert a better candidate (should fail)
        let c = make_candidate("another_key", 1, 5, None);
        assert!(!pool.maybe_insert_lru(c));
    }

    #[test]
    fn test_pool_pop_worst() {
        let mut pool = EvictionPool::new();

        pool.maybe_insert_lru(make_candidate("key1", 1000, 5, None));
        pool.maybe_insert_lru(make_candidate("key2", 2000, 5, None));
        pool.maybe_insert_lru(make_candidate("key3", 500, 5, None));

        // Pop should return worst first (key2)
        let c = pool.pop_worst().unwrap();
        assert_eq!(c.key.as_ref(), b"key2");
        assert_eq!(pool.len(), 2);

        // Next worst is key1
        let c = pool.pop_worst().unwrap();
        assert_eq!(c.key.as_ref(), b"key1");
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn test_pool_remove() {
        let mut pool = EvictionPool::new();

        pool.maybe_insert_lru(make_candidate("key1", 1000, 5, None));
        pool.maybe_insert_lru(make_candidate("key2", 2000, 5, None));
        assert_eq!(pool.len(), 2);

        pool.remove(b"key1");
        assert_eq!(pool.len(), 1);
        assert_eq!(pool.peek_worst().unwrap().key.as_ref(), b"key2");
    }

    #[test]
    fn test_pool_clear() {
        let mut pool = EvictionPool::new();

        pool.maybe_insert_lru(make_candidate("key1", 1000, 5, None));
        pool.maybe_insert_lru(make_candidate("key2", 2000, 5, None));
        assert_eq!(pool.len(), 2);

        pool.clear();
        assert!(pool.is_empty());
    }

    #[test]
    fn test_candidate_from_metadata() {
        let now = Instant::now();
        let last_access = now - Duration::from_secs(10);
        let expires_at = now + Duration::from_secs(60);

        let c = EvictionCandidate::from_metadata(
            Bytes::from("test"),
            last_access,
            50,
            Some(expires_at),
            now,
        );

        assert_eq!(c.key.as_ref(), b"test");
        assert!(c.idle_time >= Duration::from_secs(9)); // Allow some tolerance
        assert!(c.idle_time <= Duration::from_secs(11));
        assert_eq!(c.lfu_value, 50);
        assert!(c.ttl_remaining.unwrap() >= Duration::from_secs(59));
        assert!(c.ttl_remaining.unwrap() <= Duration::from_secs(61));
    }
}
