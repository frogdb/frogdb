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

use super::{EVICTION_POOL_SIZE, EvictionRanker};

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

    /// Try to insert a candidate, ordered by the given [`EvictionRanker`].
    ///
    /// This is the single home of the pool replace-the-best algorithm shared by
    /// every ranking policy. The candidate is inserted if:
    /// - the ranker admits it (`rank` returns `Some`), and
    /// - it is not already in the pool, and
    /// - the pool has room, or it outranks the best (least evictable) candidate.
    ///
    /// Returns true if the candidate was inserted.
    pub fn maybe_insert_with_ranker<R: EvictionRanker>(
        &mut self,
        candidate: EvictionCandidate,
        ranker: &R,
    ) -> bool {
        // Admit guard (replaces volatile-ttl's `ttl_remaining.is_none()` early return).
        let Some(rank) = ranker.rank(&candidate) else {
            return false;
        };

        // Don't add duplicates.
        if self.candidates.iter().any(|c| c.key == candidate.key) {
            return false;
        }

        if self.is_full() {
            // Pooled candidates were all admitted, so `rank()` is always `Some`
            // here; `filter_map` drops the impossible `None` without affecting
            // the min.
            let min_rank = self
                .candidates
                .iter()
                .filter_map(|c| ranker.rank(c))
                .min()
                .unwrap_or(0);
            if rank <= min_rank {
                return false;
            }

            // Remove the best (least evictable) candidate.
            if let Some(pos) = self
                .candidates
                .iter()
                .position(|c| ranker.rank(c) == Some(min_rank))
            {
                self.candidates.remove(pos);
            }
        }

        // Insert in sorted position (worst first). Pooled ranks are `Some`, so
        // `ranker.rank(c) < Some(rank)` preserves the old `c.X_rank() < rank`
        // ordering exactly.
        let pos = self
            .candidates
            .iter()
            .position(|c| ranker.rank(c) < Some(rank))
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

    // The pool-maintenance algorithm is identical for every policy and is
    // exercised once, parametrized over each ranker (replacing the old per-policy
    // `test_pool_insert_lru/_lfu/_ttl` + `test_pool_capacity` copies).
    #[test]
    fn pool_contract_lru() {
        check_pool_contract(&LruRanker, lru_cand);
    }

    #[test]
    fn pool_contract_lfu() {
        check_pool_contract(&LfuRanker, lfu_cand);
    }

    #[test]
    fn pool_contract_ttl() {
        check_pool_contract(&TtlRanker, ttl_cand);
    }

    /// The volatile-ttl admit guard: a candidate with no TTL is never pooled.
    #[test]
    fn pool_ttl_rejects_keys_without_ttl() {
        let mut pool = EvictionPool::new();
        assert!(!pool.maybe_insert_with_ranker(make_candidate("k", 1000, 5, None), &TtlRanker));
        assert!(pool.is_empty());
    }

    #[test]
    fn test_pool_pop_worst() {
        let mut pool = EvictionPool::new();

        ins(&mut pool, make_candidate("key1", 1000, 5, None));
        ins(&mut pool, make_candidate("key2", 2000, 5, None));
        ins(&mut pool, make_candidate("key3", 500, 5, None));

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

        ins(&mut pool, make_candidate("key1", 1000, 5, None));
        ins(&mut pool, make_candidate("key2", 2000, 5, None));
        assert_eq!(pool.len(), 2);

        pool.remove(b"key1");
        assert_eq!(pool.len(), 1);
        assert_eq!(pool.peek_worst().unwrap().key.as_ref(), b"key2");
    }

    #[test]
    fn test_pool_clear() {
        let mut pool = EvictionPool::new();

        ins(&mut pool, make_candidate("key1", 1000, 5, None));
        ins(&mut pool, make_candidate("key2", 2000, 5, None));
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

    use super::super::{LfuRanker, LruRanker, TtlRanker};

    /// Insert with the LRU ranker; used by the ranker-agnostic mechanics tests.
    fn ins(pool: &mut EvictionPool, c: EvictionCandidate) -> bool {
        pool.maybe_insert_with_ranker(c, &LruRanker)
    }

    fn keys(pool: &EvictionPool) -> Vec<Bytes> {
        pool.iter().map(|c| c.key.clone()).collect()
    }

    // Candidate builders whose rank under the matching ranker rises with
    // `badness` (higher badness = worse = evicted first). They let one contract
    // test cover every ranker.
    fn lru_cand(key: &str, badness: u64) -> EvictionCandidate {
        make_candidate(key, badness, 5, None)
    }

    fn lfu_cand(key: &str, badness: u64) -> EvictionCandidate {
        // Lower counter = worse; rank() == 255 - counter == badness.
        make_candidate(key, 0, (255 - badness) as u8, None)
    }

    fn ttl_cand(key: &str, badness: u64) -> EvictionCandidate {
        // Shorter remaining TTL = worse.
        make_candidate(key, 0, 5, Some(100_000 - badness))
    }

    /// The pool-maintenance contract, identical for every ranker: duplicate
    /// rejection, worst-first sorted insert, fill-to-capacity, replace-worse,
    /// reject-better.
    fn check_pool_contract<R: EvictionRanker>(
        ranker: &R,
        mk: impl Fn(&str, u64) -> EvictionCandidate,
    ) {
        // Duplicate rejection.
        let mut pool = EvictionPool::new();
        let dup = mk("dup", 10);
        assert!(pool.maybe_insert_with_ranker(dup.clone(), ranker));
        assert!(!pool.maybe_insert_with_ranker(dup, ranker));
        assert_eq!(pool.len(), 1);

        // Sorted insert: worst (highest badness) first, regardless of insert order.
        let mut pool = EvictionPool::new();
        assert!(pool.maybe_insert_with_ranker(mk("a", 10), ranker));
        assert!(pool.maybe_insert_with_ranker(mk("b", 30), ranker));
        assert!(pool.maybe_insert_with_ranker(mk("c", 20), ranker));
        assert_eq!(
            keys(&pool),
            vec![
                Bytes::from_static(b"b"),
                Bytes::from_static(b"c"),
                Bytes::from_static(b"a"),
            ]
        );

        // Fill to capacity, then replace-worse and reject-better.
        let mut pool = EvictionPool::new();
        for i in 0..EVICTION_POOL_SIZE {
            assert!(pool.maybe_insert_with_ranker(mk(&format!("k{i}"), 100 + i as u64), ranker));
        }
        assert!(pool.is_full());
        // Worse than the current best (min badness 100) -> admitted, replacing it.
        assert!(pool.maybe_insert_with_ranker(mk("worse", 200), ranker));
        assert!(pool.is_full());
        // Better than every pooled candidate -> rejected.
        assert!(!pool.maybe_insert_with_ranker(mk("better", 1), ranker));
    }
}
