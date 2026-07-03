//! Single source of truth for keyspace hit/miss accounting.
//!
//! `keyspace_hits` / `keyspace_misses` answer one question — of the reads that
//! resolved a named key, how many found it? The *decision* to count and the
//! *classification* of hit vs. miss live on the command spec
//! ([`crate::command_spec::LookupSpec`]) and are applied once at the execution
//! seam; this type owns only the *aggregate*.
//!
//! Two views over the same cumulative totals:
//!
//! - **Prometheus** scrapes the monotonic cumulative counters
//!   (`frogdb_keyspace_{hits,misses}_total`), which NEVER decrease, so `rate()`
//!   / `increase()` stay exact across a `CONFIG RESETSTAT`.
//! - **INFO** reports `cumulative − baseline`. `CONFIG RESETSTAT` advances the
//!   baseline to the current cumulative, so the operator-visible value drops to
//!   zero while the Prometheus counter is untouched.
//!
//! See `todo/proposals/24-keyspace-stats-accounting.md`.

use std::sync::atomic::{AtomicU64, Ordering};

/// Process-wide accumulator for keyspace hit/miss accounting.
///
/// Shared behind an `Arc`: every shard feeds it, `INFO stats` reads it, and
/// `CONFIG RESETSTAT` advances its baselines. Lives independently of the
/// metrics recorder so `INFO` reports correct values even when Prometheus is
/// disabled.
#[derive(Debug, Default)]
pub struct KeyspaceStats {
    /// Cumulative hits since process start. Feeds the monotonic Prometheus
    /// counter; NEVER decreases.
    hits: AtomicU64,
    /// Cumulative misses since process start.
    misses: AtomicU64,
    /// Hits snapshot taken at the last `CONFIG RESETSTAT`. `INFO` reports
    /// `hits − hits_baseline`.
    hits_baseline: AtomicU64,
    /// Misses snapshot taken at the last `CONFIG RESETSTAT`.
    misses_baseline: AtomicU64,
}

impl KeyspaceStats {
    /// Create a fresh accumulator with all totals and baselines at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record `hits` keyspace hits and `misses` keyspace misses in one shot.
    ///
    /// A no-op when both are zero, so callers need not guard the common case.
    #[inline]
    pub fn record(&self, hits: u64, misses: u64) {
        if hits > 0 {
            self.hits.fetch_add(hits, Ordering::Relaxed);
        }
        if misses > 0 {
            self.misses.fetch_add(misses, Ordering::Relaxed);
        }
    }

    /// Cumulative hits since process start (the value Prometheus exports).
    #[inline]
    pub fn cumulative_hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Cumulative misses since process start (the value Prometheus exports).
    #[inline]
    pub fn cumulative_misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Operator-visible hits: `cumulative − baseline`, i.e. hits since the last
    /// `CONFIG RESETSTAT`. This is what `INFO stats` reports.
    #[inline]
    pub fn reported_hits(&self) -> u64 {
        self.hits
            .load(Ordering::Relaxed)
            .saturating_sub(self.hits_baseline.load(Ordering::Relaxed))
    }

    /// Operator-visible misses since the last `CONFIG RESETSTAT`.
    #[inline]
    pub fn reported_misses(&self) -> u64 {
        self.misses
            .load(Ordering::Relaxed)
            .saturating_sub(self.misses_baseline.load(Ordering::Relaxed))
    }

    /// Advance the baselines to the current cumulative totals.
    ///
    /// After this the operator-visible ([`Self::reported_hits`] /
    /// [`Self::reported_misses`]) values read zero, while the cumulative totals
    /// feeding Prometheus are left strictly monotonic and untouched. Called by
    /// `CONFIG RESETSTAT`.
    pub fn reset(&self) {
        // Load-then-store: any concurrent `record` between the two loads is
        // simply attributed to the new interval, which is the desired behavior.
        self.hits_baseline
            .store(self.hits.load(Ordering::Relaxed), Ordering::Relaxed);
        self.misses_baseline
            .store(self.misses.load(Ordering::Relaxed), Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_accumulates_cumulative_and_reported() {
        let stats = KeyspaceStats::new();
        stats.record(3, 1);
        assert_eq!(stats.cumulative_hits(), 3);
        assert_eq!(stats.cumulative_misses(), 1);
        assert_eq!(stats.reported_hits(), 3);
        assert_eq!(stats.reported_misses(), 1);
    }

    #[test]
    fn record_zero_is_noop() {
        let stats = KeyspaceStats::new();
        stats.record(0, 0);
        assert_eq!(stats.cumulative_hits(), 0);
        assert_eq!(stats.cumulative_misses(), 0);
    }

    #[test]
    fn reset_zeroes_reported_but_not_cumulative() {
        let stats = KeyspaceStats::new();
        stats.record(3, 1);
        stats.reset();
        // Prometheus-facing totals are monotonic and untouched.
        assert_eq!(stats.cumulative_hits(), 3);
        assert_eq!(stats.cumulative_misses(), 1);
        // Operator-facing values reset to zero.
        assert_eq!(stats.reported_hits(), 0);
        assert_eq!(stats.reported_misses(), 0);
    }

    #[test]
    fn reported_tracks_deltas_after_reset() {
        // The migration-plan pin: record(hit×3, miss×1), RESETSTAT, record(hit×1)
        // => INFO sees hits=1, misses=0 while Prometheus _total is hits=4, miss=1.
        let stats = KeyspaceStats::new();
        stats.record(3, 1);
        stats.reset();
        stats.record(1, 0);
        assert_eq!(stats.reported_hits(), 1);
        assert_eq!(stats.reported_misses(), 0);
        assert_eq!(stats.cumulative_hits(), 4);
        assert_eq!(stats.cumulative_misses(), 1);
    }

    #[test]
    fn repeated_reset_is_idempotent_on_reported() {
        let stats = KeyspaceStats::new();
        stats.record(5, 2);
        stats.reset();
        stats.reset();
        assert_eq!(stats.reported_hits(), 0);
        assert_eq!(stats.reported_misses(), 0);
        assert_eq!(stats.cumulative_hits(), 5);
    }
}
