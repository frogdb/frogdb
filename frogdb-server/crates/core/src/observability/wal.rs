//! Cross-shard WAL durability-lag aggregation.
//!
//! This is the *single* place that folds per-shard [`WalLagStats`] into a
//! node-wide view. It used to be aggregated twice — once by INFO's
//! `WalAggregate` (`server/src/info/mod.rs`) and once by telemetry's
//! `gather_wal_lag_stats` (`telemetry/src/status.rs`) — through two
//! hand-written loops that had already drifted apart (telemetry surfaced an
//! average and per-shard rows; INFO surfaced neither, but tracked the oldest
//! last-flush timestamp telemetry lacked). Both surfaces now render from this
//! one aggregate, so they can never disagree.
//!
//! Aggregation rules (each renderer picks the subset it surfaces):
//! - `pending_ops` / `pending_bytes` / `flush_failures` / `lost_ops`: **sum**.
//! - `max_durability_lag_ms`: **max** — the worst shard bounds durability.
//! - `avg_durability_lag_ms`: **mean** over participating shards.
//! - `last_flush_ok`: **AND** — ok only if every shard's last flush succeeded.
//! - `last_flush_time_ms`: **min** — the oldest shard's last flush, i.e. every
//!   shard has flushed at least as of the reported time.
//! - `per_shard`: retained verbatim for surfaces that show per-shard detail.

use crate::persistence::WalLagStats;

/// One shard's contribution to the WAL-lag aggregate, retained verbatim so a
/// renderer can surface per-shard rows.
#[derive(Debug, Clone)]
pub struct ShardWalLag {
    /// Shard identifier.
    pub shard_id: usize,
    /// Operations buffered but not yet flushed on this shard.
    pub pending_ops: usize,
    /// Bytes buffered but not yet flushed on this shard.
    pub pending_bytes: usize,
    /// Durability lag in milliseconds for this shard.
    pub durability_lag_ms: u64,
    /// Failed flush attempts since startup on this shard.
    pub flush_failures: u64,
    /// WAL entries dropped in failed flushes since startup on this shard.
    pub lost_ops: u64,
    /// Whether this shard's most recent flush attempt succeeded.
    pub last_flush_ok: bool,
}

impl From<&WalLagStats> for ShardWalLag {
    fn from(lag: &WalLagStats) -> Self {
        Self {
            shard_id: lag.shard_id,
            pending_ops: lag.pending_ops,
            pending_bytes: lag.pending_bytes,
            durability_lag_ms: lag.durability_lag_ms,
            flush_failures: lag.flush_failures,
            lost_ops: lag.lost_ops,
            last_flush_ok: lag.last_flush_ok,
        }
    }
}

/// WAL durability lag aggregated across every persistent shard.
///
/// Construct incrementally with [`WalLagAggregate::absorb`] (INFO folds shard
/// replies as they arrive) or in one shot with [`WalLagAggregate::from_shards`]
/// (telemetry collects all replies first). Both paths produce identical values.
#[derive(Debug, Clone)]
pub struct WalLagAggregate {
    /// Total operations buffered but not yet flushed.
    pub pending_ops: usize,
    /// Total bytes buffered but not yet flushed.
    pub pending_bytes: usize,
    /// Worst per-shard durability lag (ms since last flush).
    pub max_durability_lag_ms: u64,
    /// Total failed flush attempts across shards since startup.
    pub flush_failures: u64,
    /// Total WAL entries dropped in failed flushes across shards since startup.
    /// Losses are permanent; this never decreases.
    pub lost_ops: u64,
    /// False if any shard's most recent flush attempt failed.
    pub last_flush_ok: bool,
    /// Oldest per-shard last-flush wall-clock time (unix ms).
    pub last_flush_time_ms: u64,
    /// Per-shard detail, in fold order.
    pub per_shard: Vec<ShardWalLag>,
}

impl Default for WalLagAggregate {
    fn default() -> Self {
        Self::new()
    }
}

impl WalLagAggregate {
    /// An empty aggregate seeded so that `max`/`min`/`AND` fold correctly on
    /// the first [`absorb`](Self::absorb). Not observable on its own: an
    /// aggregate that never absorbed a shard has an empty `per_shard` and
    /// should be treated as "no WAL" (see [`from_shards`](Self::from_shards)).
    pub fn new() -> Self {
        Self {
            pending_ops: 0,
            pending_bytes: 0,
            max_durability_lag_ms: 0,
            flush_failures: 0,
            lost_ops: 0,
            last_flush_ok: true,
            last_flush_time_ms: u64::MAX,
            per_shard: Vec::new(),
        }
    }

    /// Fold one shard's lag stats into the aggregate.
    pub fn absorb(&mut self, lag: &WalLagStats) {
        self.pending_ops += lag.pending_ops;
        self.pending_bytes += lag.pending_bytes;
        self.max_durability_lag_ms = self.max_durability_lag_ms.max(lag.durability_lag_ms);
        self.flush_failures += lag.flush_failures;
        self.lost_ops += lag.lost_ops;
        self.last_flush_ok &= lag.last_flush_ok;
        self.last_flush_time_ms = self.last_flush_time_ms.min(lag.last_flush_timestamp_ms);
        self.per_shard.push(ShardWalLag::from(lag));
    }

    /// Fold all shards at once, returning `None` when no shard reported WAL
    /// stats (persistence disabled everywhere).
    pub fn from_shards<'a, I>(shards: I) -> Option<Self>
    where
        I: IntoIterator<Item = &'a WalLagStats>,
    {
        let mut agg = Self::new();
        for lag in shards {
            agg.absorb(lag);
        }
        if agg.per_shard.is_empty() {
            None
        } else {
            Some(agg)
        }
    }

    /// Mean durability lag across participating shards (0.0 when none).
    pub fn avg_durability_lag_ms(&self) -> f64 {
        if self.per_shard.is_empty() {
            return 0.0;
        }
        let total: u64 = self.per_shard.iter().map(|s| s.durability_lag_ms).sum();
        total as f64 / self.per_shard.len() as f64
    }

    /// Oldest last-flush timestamp, or `0` when no shard has flushed.
    ///
    /// The internal seed is `u64::MAX` so `min` folds correctly; this accessor
    /// normalizes the never-absorbed case to `0` for rendering.
    pub fn last_flush_time_ms(&self) -> u64 {
        if self.last_flush_time_ms == u64::MAX {
            0
        } else {
            self.last_flush_time_ms
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lag(shard_id: usize, pending: usize, dlag: u64, flush_ts: u64, ok: bool) -> WalLagStats {
        WalLagStats {
            pending_ops: pending,
            pending_bytes: pending * 10,
            durability_lag_ms: dlag,
            sequence: 0,
            durable_sequence: 0,
            flush_failures: 1,
            lost_ops: 2,
            lost_bytes: 200,
            last_flush_ok: ok,
            shard_id,
            last_flush_timestamp_ms: flush_ts,
        }
    }

    #[test]
    fn from_shards_none_when_empty() {
        assert!(WalLagAggregate::from_shards(std::iter::empty()).is_none());
    }

    #[test]
    fn folds_sum_max_min_and_avg() {
        let shards = [lag(0, 3, 50, 1_000, true), lag(1, 4, 80, 800, false)];
        let agg = WalLagAggregate::from_shards(shards.iter()).expect("present");
        assert_eq!(agg.pending_ops, 7, "pending sums");
        assert_eq!(agg.pending_bytes, 70, "bytes sum");
        assert_eq!(agg.max_durability_lag_ms, 80, "lag takes worst shard");
        assert_eq!(agg.flush_failures, 2, "failures sum");
        assert_eq!(agg.lost_ops, 4, "lost ops sum");
        assert!(!agg.last_flush_ok, "one failing shard makes aggregate err");
        assert_eq!(
            agg.last_flush_time_ms(),
            800,
            "flush time takes oldest shard"
        );
        assert_eq!(agg.avg_durability_lag_ms(), 65.0, "avg of 50 and 80");
        assert_eq!(agg.per_shard.len(), 2, "per-shard rows retained");
    }

    #[test]
    fn absorb_matches_from_shards() {
        let shards = [lag(0, 3, 50, 1_000, true), lag(1, 4, 80, 800, false)];
        let mut incremental = WalLagAggregate::new();
        for l in &shards {
            incremental.absorb(l);
        }
        let batch = WalLagAggregate::from_shards(shards.iter()).unwrap();
        assert_eq!(incremental.pending_ops, batch.pending_ops);
        assert_eq!(
            incremental.max_durability_lag_ms,
            batch.max_durability_lag_ms
        );
        assert_eq!(incremental.last_flush_time_ms(), batch.last_flush_time_ms());
        assert_eq!(
            incremental.avg_durability_lag_ms(),
            batch.avg_durability_lag_ms()
        );
    }
}
