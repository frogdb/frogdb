//! Eviction ranking seam.
//!
//! Each `maxmemory-policy` that uses the sample-rank-pool machinery (LRU, LFU,
//! volatile-TTL) differs only in *how it orders candidates* — and whether a
//! candidate is eligible at all. [`EvictionRanker`] captures that single varying
//! decision so the sampling loop, the pool-maintenance algorithm, and the sample
//! metric each live in exactly one place. A new policy becomes one
//! `EvictionRanker` impl plus one `evict_one` match arm — not a copy-pasted
//! sampler + pool inserter + evict method.

use super::EvictionCandidate;

/// Total-order key for eviction: higher = worse = evicted first.
///
/// Matches [`EvictionPool::pop_worst`](super::EvictionPool::pop_worst), which
/// removes the highest-ranked candidate.
pub type RankKey = u64;

/// How a `maxmemory-policy` orders eviction candidates.
///
/// `rank` returns `None` for a candidate the policy refuses to evict at all
/// (volatile-ttl rejects keys with no TTL), folding the old TTL admit-guard into
/// the ranking decision. Everything else about sampling and pool maintenance is
/// policy-independent and lives behind the generic pool/sampler methods.
pub trait EvictionRanker {
    /// Rank a candidate; `None` means the policy will not evict it.
    fn rank(&self, candidate: &EvictionCandidate) -> Option<RankKey>;
}

/// Least-recently-used ordering: higher idle time = worse.
pub struct LruRanker;

impl EvictionRanker for LruRanker {
    #[inline]
    fn rank(&self, c: &EvictionCandidate) -> Option<RankKey> {
        Some(c.idle_time.as_micros() as u64)
    }
}

/// Least-frequently-used ordering: lower counter = worse.
///
/// The counter is inverted so that, like every ranker, a higher [`RankKey`]
/// means a worse candidate.
pub struct LfuRanker;

impl EvictionRanker for LfuRanker {
    #[inline]
    fn rank(&self, c: &EvictionCandidate) -> Option<RankKey> {
        Some(255 - c.lfu_value as u64)
    }
}

/// Volatile-TTL ordering: shorter remaining TTL = worse.
///
/// Keys without a TTL are ineligible (`None`), which is exactly the old
/// `ttl_remaining.is_none()` admit guard.
pub struct TtlRanker;

impl EvictionRanker for TtlRanker {
    #[inline]
    fn rank(&self, c: &EvictionCandidate) -> Option<RankKey> {
        c.ttl_remaining.map(|ttl| u64::MAX - ttl.as_micros() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::time::Duration;

    fn candidate(idle_ms: u64, lfu: u8, ttl_ms: Option<u64>) -> EvictionCandidate {
        EvictionCandidate::new(
            Bytes::from_static(b"k"),
            Duration::from_millis(idle_ms),
            lfu,
            ttl_ms.map(Duration::from_millis),
        )
    }

    #[test]
    fn lru_rank_monotonic_in_idle_time() {
        let cold = candidate(2000, 5, None);
        let warm = candidate(1000, 5, None);
        // Higher idle time = worse = higher rank.
        assert!(LruRanker.rank(&cold) > LruRanker.rank(&warm));
        assert_eq!(
            LruRanker.rank(&warm),
            Some(Duration::from_millis(1000).as_micros() as u64)
        );
    }

    #[test]
    fn lfu_rank_inverted_in_counter() {
        let rare = candidate(0, 5, None);
        let popular = candidate(0, 200, None);
        // Lower counter = worse = higher rank.
        assert!(LfuRanker.rank(&rare) > LfuRanker.rank(&popular));
        assert_eq!(LfuRanker.rank(&popular), Some(255 - 200));
    }

    #[test]
    fn ttl_rank_none_without_ttl() {
        assert_eq!(TtlRanker.rank(&candidate(0, 5, None)), None);
    }

    #[test]
    fn ttl_rank_monotone_decreasing_in_remaining() {
        let soon = candidate(0, 5, Some(1000));
        let later = candidate(0, 5, Some(10000));
        // Shorter remaining TTL = worse = higher rank.
        assert!(TtlRanker.rank(&soon) > TtlRanker.rank(&later));
    }
}
