//! Lookup: the segmented table against the incumbent `griddle::HashMap`.
//!
//! This is the measurement issue 11's swap decision turns on. The spike found
//! lookups 2.0× slower on `redis-feel`, 2.4× on `sessions` and 3.2× on
//! `counters`, and set a ship gate of **within 1.25× on `redis-feel` and
//! `sessions`**; the residual it blamed was the scalar fingerprint loop, which
//! this crate replaces with a SIMD match.
//!
//! # Keeping the comparison honest
//!
//! - **Hasher parity.** Both sides hash with `ahash` under the same four seed
//!   words, so what is measured is the structure and not the hash.
//! - **Same keys, same order.** Both are filled from one generated workload, and
//!   probed in one fixed shuffled order, so neither side gets a friendlier
//!   access pattern.
//! - **Same value type.** Both store a [`ValueWord`]. The store seam's real value
//!   is a 64-byte `Entry`, which no 8-byte word can inline — see the report — but
//!   putting the *same* value on both sides is what makes this a structure
//!   comparison.
//! - **Every probe's result is consumed** through `black_box`, so neither side's
//!   probe can be optimised away.
//!
//! Run: `just bench-table`. On macOS these are an upper bound on the ratio;
//! the ship-gate reading is the testbox one.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use frogdb_table::word::ValueWord;
use frogdb_table::{Table, TableSeed};

mod workload;
use workload::{Shape, Value};

/// Entries per shape. Big enough that the table is several hundred segments deep
/// and neither side fits in L2, which is where the two structures differ.
const KEYS: usize = 200_000;

/// The seed both sides hash with.
const SEED: u64 = 11;

fn value_word(v: &Value) -> ValueWord {
    match v {
        Value::Int(i) => ValueWord::from_int(*i),
        Value::Bytes(b) => ValueWord::from_bytes(b),
    }
}

/// The incumbent, keyed with the table's seed so the hash is not the variable.
fn griddle_map(
    entries: &[workload::Entry],
) -> griddle::HashMap<Vec<u8>, ValueWord, ahash::RandomState> {
    let [a, b, c, d] = TableSeed::from_u64(SEED).words();
    let mut m = griddle::HashMap::with_hasher(ahash::RandomState::with_seeds(a, b, c, d));
    for e in entries {
        m.insert(e.key.clone(), value_word(&e.value));
    }
    m
}

fn table(entries: &[workload::Entry]) -> Table<ValueWord> {
    let mut t = Table::with_seed(TableSeed::from_u64(SEED));
    for e in entries {
        t.insert(&e.key, value_word(&e.value));
    }
    t
}

/// A fixed probe order, shuffled so the walk is not the insertion order and both
/// sides see the same misses in the same places.
fn probe_order(entries: &[workload::Entry]) -> Vec<Vec<u8>> {
    // A fixed-stride walk rather than a shuffle: deterministic, and the stride is
    // a prime that does not divide the length, so it visits every key exactly
    // once while defeating any prefetcher that would reward insertion order.
    let n = entries.len();
    const STRIDE: usize = 104_729;
    assert_ne!(n % STRIDE, 0, "stride must be coprime with the key count");
    (0..n)
        .map(|i| entries[(i * STRIDE) % n].key.clone())
        .collect()
}

fn bench_lookup(c: &mut Criterion) {
    for shape in Shape::ALL {
        let entries = workload::generate(shape, KEYS);
        let probes = probe_order(&entries);
        let absent = workload::absent_keys(shape, KEYS);
        let g = griddle_map(&entries);
        let t = table(&entries);

        assert_eq!(g.len(), t.len(), "{} filled unequally", shape.name());

        let mut hit = c.benchmark_group(format!("lookup-hit/{}", shape.name()));
        hit.bench_with_input(BenchmarkId::new("griddle", KEYS), &probes, |bencher, ks| {
            bencher.iter(|| {
                for k in ks {
                    black_box(g.get(k.as_slice()));
                }
            });
        });
        hit.bench_with_input(BenchmarkId::new("table", KEYS), &probes, |bencher, ks| {
            bencher.iter(|| {
                for k in ks {
                    black_box(t.get(k));
                }
            });
        });
        hit.finish();

        let mut miss = c.benchmark_group(format!("lookup-miss/{}", shape.name()));
        miss.bench_with_input(BenchmarkId::new("griddle", KEYS), &absent, |bencher, ks| {
            bencher.iter(|| {
                for k in ks {
                    black_box(g.get(k.as_slice()));
                }
            });
        });
        miss.bench_with_input(BenchmarkId::new("table", KEYS), &absent, |bencher, ks| {
            bencher.iter(|| {
                for k in ks {
                    black_box(t.get(k));
                }
            });
        });
        miss.finish();
    }
}

criterion_group!(benches, bench_lookup);
criterion_main!(benches);
