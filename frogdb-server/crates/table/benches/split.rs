//! Split stall: how long one insert takes when it is the insert that splits.
//!
//! The stall this structure exists to remove. The spike measured a `str7` split
//! at **44 375 ns p50**, scanning 808 slots to move 404 — every one of them
//! rehashed, because the spike's slots did not store enough of the hash to say
//! which half an entry belonged in. This crate stores 16 route bits per slot for
//! exactly that reason, so a split should be a copy and the stall should fall
//! with it.
//!
//! # Why this is not a Criterion bench
//!
//! Criterion reports a distribution over *repetitions of a fixed workload*. A
//! split is a one-shot event inside a growing table: it cannot be repeated
//! without rebuilding the table, and rebuilding changes which insert splits. What
//! is wanted is the distribution over the ~2 000 splits of one fill, which is a
//! histogram of individually timed inserts — so the bench times every insert,
//! separates the ones that split from the ones that did not, and prints
//! percentiles of each.
//!
//! Run: `just bench-table-split`. The p50 is the number to read; the tail is
//! contention on a laptop, as the spike's own report says of its p99.

use std::time::Instant;

use frogdb_table::word::ValueWord;
use frogdb_table::{Table, TableSeed};

mod workload;
use workload::{Shape, Value};

/// The spike's fill size, so the split count is comparable.
const KEYS: usize = 1_000_000;

/// The p50 the spike measured for `str7`, in nanoseconds.
const SPIKE_SPLIT_P50_NS: u128 = 44_375;

fn percentile(sorted: &[u128], p: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx]
}

fn main() {
    let entries = workload::generate(Shape::RedisFeel, KEYS);
    println!(
        "shape redis-feel, {KEYS} keys, mean key {:.1} B",
        workload::mean_key_len(&entries)
    );

    let mut t: Table<ValueWord> = Table::with_seed(TableSeed::from_u64(11));
    let mut plain: Vec<u128> = Vec::with_capacity(KEYS);
    let mut split: Vec<u128> = Vec::new();

    let mut splits_before = 0u64;
    for e in &entries {
        let value = match &e.value {
            Value::Int(i) => ValueWord::from_int(*i),
            Value::Bytes(b) => ValueWord::from_bytes(b),
        };
        let start = Instant::now();
        t.insert(&e.key, value);
        let elapsed = start.elapsed().as_nanos();

        let splits_now = t.stats().splits;
        if splits_now == splits_before {
            plain.push(elapsed);
        } else {
            split.push(elapsed);
        }
        splits_before = splits_now;
    }

    plain.sort_unstable();
    split.sort_unstable();

    let s = t.stats();
    let per_split = |n: u64| {
        if s.splits == 0 {
            0.0
        } else {
            n as f64 / s.splits as f64
        }
    };

    let split_p50 = percentile(&split, 0.50);
    let moved_per_split = per_split(s.split_moved);

    println!();
    println!("insert p50 (no split) : {} ns", percentile(&plain, 0.50));
    println!("insert p99 (no split) : {} ns", percentile(&plain, 0.99));
    println!("split  p50            : {split_p50} ns   (spike str7: {SPIKE_SPLIT_P50_NS})");
    println!("split  p99            : {} ns", percentile(&split, 0.99));
    println!("split  max            : {} ns", percentile(&split, 1.0));
    println!();
    println!("splits                : {}", s.splits);
    println!("doublings             : {}", s.doublings);
    println!("scanned/split         : {:.1}", per_split(s.split_scanned));
    println!("moved/split           : {moved_per_split:.1}");
    println!("rehashed/split        : {:.3}", per_split(s.split_rehashed));
    println!(
        "leftovers/split       : {:.3}",
        per_split(s.split_leftovers)
    );
    println!("dir writes/split      : {:.1}", per_split(s.dir_writes));
    if moved_per_split > 0.0 {
        println!(
            "ns per moved entry    : {:.0}",
            split_p50 as f64 / moved_per_split
        );
    }
    println!();
    println!(
        "occupancy {:.3}, {:.1} structural B/entry over {} segments",
        t.occupancy(),
        t.structural_bytes_per_entry(),
        t.segment_count()
    );

    // The acceptance criteria, checked here rather than left to a reader: a
    // benchmark that can silently stop meeting them is a benchmark nobody reads.
    assert_eq!(
        s.split_rehashed, 0,
        "a split rehashed {} keys — the split bit should come from stored route bits",
        s.split_rehashed
    );
    assert!(
        split_p50 < SPIKE_SPLIT_P50_NS,
        "split p50 {split_p50} ns did not beat the spike's {SPIKE_SPLIT_P50_NS} ns"
    );
}
