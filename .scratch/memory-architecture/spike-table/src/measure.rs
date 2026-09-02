//! Allocator truth and timing helpers.
//!
//! Live-byte accounting comes from jemalloc's `stats.allocated` (the same counter
//! the phase-1 spike used for R2), read after an `epoch` advance. Because it is a
//! *live bytes* gauge, transient allocations during a build do not show up — only
//! what the structure still holds.

use tikv_jemalloc_ctl::{epoch, stats};

/// Advances the stats epoch and returns process-wide live bytes.
pub fn allocated() -> usize {
    epoch::advance().expect("jemalloc epoch");
    stats::allocated::read().expect("jemalloc stats.allocated")
}

/// Live bytes added by `f`, measured across an epoch advance on either side.
pub fn measure<T>(f: impl FnOnce() -> T) -> (T, usize) {
    let before = allocated();
    let out = f();
    let after = allocated();
    (out, after.saturating_sub(before))
}

/// Median of the samples, taken by sorting a copy.
pub fn median(mut xs: Vec<f64>) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    if xs.is_empty() {
        return 0.0;
    }
    xs[xs.len() / 2]
}

/// Percentile of an already-collected sample set (nearest rank).
pub fn percentile(xs: &mut [u64], p: f64) -> u64 {
    if xs.is_empty() {
        return 0;
    }
    xs.sort_unstable();
    let rank = ((p / 100.0) * xs.len() as f64).ceil() as usize;
    xs[rank.saturating_sub(1).min(xs.len() - 1)]
}
