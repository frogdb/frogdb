//! The R5 slot-layout sweep: five slot layouts × three workload shapes, against the
//! shipped `griddle::HashMap<Bytes, Entry>` baseline.
//!
//! ```bash
//! cargo run --release --bin sweep            # 1,000,000 entries per cell
//! cargo run --release --bin sweep -- 200000  # smaller run
//! ```

use std::time::Instant;

use memarch_spike_table::baseline;
use memarch_spike_table::measure::{allocated, percentile};
use memarch_spike_table::segment::{
    BUCKETS, BUCKET_BYTES, HEADER_BYTES, META_BYTES, R9_RESERVED_BYTES, REGULAR_BUCKETS,
    STASH_BUCKETS,
};
use memarch_spike_table::table::{Stats, Table, Val};
use memarch_spike_table::word::{W8Int, W8Ptr, Word, W16, W8};
use memarch_spike_table::workload::{generate, summarize, Pair, Shape};

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

struct Row {
    variant: &'static str,
    inline_keys: f64,
    inline_values: f64,
    live_bytes_per_entry: f64,
    structural_bytes_per_entry: f64,
    dir_bytes_per_entry: f64,
    occupancy: f64,
    bucket_fill: f64,
    full_buckets: f64,
    stash_load: f64,
    probe_hit: f64,
    probe_miss: f64,
    probe_max: u32,
    insert_ns: f64,
    lookup_hit_ns: f64,
    lookup_miss_ns: f64,
    iterate_ns: f64,
    segments: usize,
    dir_entries: usize,
    splits: u64,
    doublings: u64,
}

fn val_of(p: &Pair) -> Val<'_> {
    match p.int {
        Some(i) => Val::Int(i),
        None => Val::Bytes(&p.bytes),
    }
}

fn run_variant<K: Word, V: Word, const N: usize>(
    variant: &'static str,
    pairs: &[Pair],
    misses: &[Vec<u8>],
) -> Row {
    let n = pairs.len();
    let mut t: Table<K, V, N> = Table::new();

    let before = allocated();
    let t0 = Instant::now();
    for p in pairs {
        t.insert(&p.key, val_of(p));
    }
    let insert_ns = t0.elapsed().as_nanos() as f64 / n as f64;
    let live = allocated().saturating_sub(before);
    assert_eq!(t.len(), n, "{variant}: duplicate keys in workload");

    let build = t.stats;
    t.stats = Stats::default();
    let t0 = Instant::now();
    let mut hits = 0usize;
    for p in pairs {
        if t.contains(&p.key) {
            hits += 1;
        }
    }
    let lookup_hit_ns = t0.elapsed().as_nanos() as f64 / n as f64;
    assert_eq!(hits, n);
    let hit_stats = t.stats;

    t.stats = Stats::default();
    let t0 = Instant::now();
    let mut found = 0usize;
    for k in misses {
        if t.contains(k) {
            found += 1;
        }
    }
    let lookup_miss_ns = t0.elapsed().as_nanos() as f64 / misses.len() as f64;
    assert_eq!(found, 0, "{variant}: miss keys collided with the workload");
    let miss_stats = t.stats;

    let t0 = Instant::now();
    let mut bytes = 0usize;
    t.for_each(|k| bytes += k.len());
    let iterate_ns = t0.elapsed().as_nanos() as f64 / n as f64;
    assert!(bytes > 0);

    let (bucket_fill, full_buckets) = t.bucket_fill();
    Row {
        variant,
        inline_keys: build.inline_keys as f64 / n as f64,
        inline_values: build.inline_values as f64 / n as f64,
        live_bytes_per_entry: live as f64 / n as f64,
        structural_bytes_per_entry: t.structural_bytes() as f64 / n as f64,
        dir_bytes_per_entry: t.directory_bytes() as f64 / n as f64,
        occupancy: t.occupancy(),
        bucket_fill,
        full_buckets,
        stash_load: t.stash_load(),
        probe_hit: hit_stats.probe_buckets as f64 / hit_stats.lookups as f64,
        probe_miss: miss_stats.probe_buckets as f64 / miss_stats.lookups as f64,
        probe_max: hit_stats.probe_max.max(miss_stats.probe_max),
        insert_ns,
        lookup_hit_ns,
        lookup_miss_ns,
        iterate_ns,
        segments: t.segments(),
        dir_entries: t.directory_entries(),
        splits: build.splits,
        doublings: build.dir_doublings,
    }
}

fn run_baseline(pairs: &[Pair], misses: &[Vec<u8>]) -> Row {
    let n = pairs.len();
    let mut map = baseline::Baseline::new();

    let before = allocated();
    let t0 = Instant::now();
    for p in pairs {
        baseline::insert(&mut map, &p.key, p.int, &p.bytes);
    }
    let insert_ns = t0.elapsed().as_nanos() as f64 / n as f64;
    let live = allocated().saturating_sub(before);
    assert_eq!(map.len(), n);

    let t0 = Instant::now();
    let mut hits = 0usize;
    for p in pairs {
        if map.contains_key(p.key.as_slice()) {
            hits += 1;
        }
    }
    let lookup_hit_ns = t0.elapsed().as_nanos() as f64 / n as f64;
    assert_eq!(hits, n);

    let t0 = Instant::now();
    let mut found = 0usize;
    for k in misses {
        if map.contains_key(k.as_slice()) {
            found += 1;
        }
    }
    let lookup_miss_ns = t0.elapsed().as_nanos() as f64 / misses.len() as f64;
    assert_eq!(found, 0);

    let t0 = Instant::now();
    let mut bytes = 0usize;
    for (k, _) in map.iter() {
        bytes += k.len();
    }
    let iterate_ns = t0.elapsed().as_nanos() as f64 / n as f64;
    assert!(bytes > 0);

    // griddle's own table: capacity is in entries, each entry a `(Bytes, Entry)` pair
    // plus a control byte. This is the incumbent's structural cost, the number our
    // segment bytes are the counterpart of. During an incremental resize griddle also
    // holds the old table, which the live-byte figure sees and this one does not.
    let (_, _, pair_sz, _) = baseline::sizes();
    let table_bytes = map.capacity() * (pair_sz + 1);

    Row {
        variant: "griddle",
        inline_keys: 0.0,
        inline_values: 0.0,
        live_bytes_per_entry: live as f64 / n as f64,
        structural_bytes_per_entry: table_bytes as f64 / n as f64,
        dir_bytes_per_entry: f64::NAN,
        occupancy: f64::NAN,
        bucket_fill: f64::NAN,
        full_buckets: f64::NAN,
        stash_load: f64::NAN,
        probe_hit: f64::NAN,
        probe_miss: f64::NAN,
        probe_max: 0,
        insert_ns,
        lookup_hit_ns,
        lookup_miss_ns,
        iterate_ns,
        segments: 0,
        dir_entries: 0,
        splits: 0,
        doublings: 0,
    }
}

/// Worst single-operation stall. Every insert is individually timed and bucketed by
/// what it did — plain, split, or split + directory doubling — so the split cost is a
/// distribution over the ~2 000 real splits rather than an amortised average or a
/// single outlier the OS scheduler produced.
fn split_stall<K: Word, V: Word, const N: usize>(variant: &str, pairs: &[Pair]) {
    let mut t: Table<K, V, N> = Table::new();
    let mut plain = Vec::with_capacity(pairs.len());
    let mut splits = Vec::new();
    let mut doublings = Vec::new();
    for p in pairs {
        let before_splits = t.stats.splits;
        let before_doublings = t.stats.dir_doublings;
        let t0 = Instant::now();
        t.insert(&p.key, val_of(p));
        let ns = t0.elapsed().as_nanos() as u64;
        if t.stats.dir_doublings > before_doublings {
            doublings.push(ns);
        } else if t.stats.splits > before_splits {
            splits.push(ns);
        } else {
            plain.push(ns);
        }
    }
    // Split-carrying inserts are reported by their own median: on a shared laptop the
    // maximum of a million timed inserts is a scheduler artefact, not a split.
    let p50 = percentile(&mut plain.clone(), 50.0);
    let p999 = percentile(&mut plain.clone(), 99.9);
    let plain_max = percentile(&mut plain, 100.0);
    let split_p50 = percentile(&mut splits.clone(), 50.0);
    let split_p99 = percentile(&mut splits.clone(), 99.0);
    let split_max = percentile(&mut splits, 100.0);
    let dbl_p50 = percentile(&mut doublings.clone(), 50.0);
    let dbl_max = percentile(&mut doublings, 100.0);
    println!(
        "| {variant} | {p50} | {p999} | {plain_max} | {split_p50} | {split_p99} | {split_max} | {dbl_p50} | {dbl_max} | {} | {} |",
        t.stats.splits, t.stats.dir_doublings
    );
}

fn header(row: &str) {
    println!("{row}");
}

fn print_rows(title: &str, rows: &[Row]) {
    println!("\n### {title}\n");
    println!(
        "| variant | inline K | inline V | live B/e | struct B/e | dir B/e | occup. | fill/14 | full bkt | stash | probe hit | probe miss | pmax | ins ns | hit ns | miss ns | iter ns | segs | dir | splits | x2 |"
    );
    println!(
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    );
    for r in rows {
        println!(
            "| {} | {:.1}% | {:.1}% | {:.1} | {:.1} | {:.3} | {:.3} | {:.2} | {:.1}% | {:.2}% | {:.2} | {:.2} | {} | {:.0} | {:.0} | {:.0} | {:.1} | {} | {} | {} | {} |",
            r.variant,
            r.inline_keys * 100.0,
            r.inline_values * 100.0,
            r.live_bytes_per_entry,
            r.structural_bytes_per_entry,
            r.dir_bytes_per_entry,
            r.occupancy,
            r.bucket_fill,
            r.full_buckets * 100.0,
            r.stash_load * 100.0,
            r.probe_hit,
            r.probe_miss,
            r.probe_max,
            r.insert_ns,
            r.lookup_hit_ns,
            r.lookup_miss_ns,
            r.iterate_ns,
            r.segments,
            r.dir_entries,
            r.splits,
            r.doublings,
        );
    }
}

fn main() {
    let n: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);

    println!("# R5 slot-layout sweep — {n} entries per cell\n");

    println!("## Layout\n");
    println!("| variant | key word | value word | slot B | slots/bucket | bucket B | segment B | segment slots | inline key ≤ | inline val ≤ | inline int bits |");
    println!("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |");
    macro_rules! layout {
        ($label:literal, $k:ty, $v:ty, $n:expr) => {
            println!(
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
                $label,
                <$k>::NAME,
                <$v>::NAME,
                Table::<$k, $v, $n>::SLOT_BYTES,
                Table::<$k, $v, $n>::SLOTS_PER_BUCKET,
                META_BYTES + Table::<$k, $v, $n>::SLOT_BYTES * $n,
                Table::<$k, $v, $n>::SEGMENT_BYTES,
                Table::<$k, $v, $n>::SEGMENT_CAPACITY,
                Table::<$k, $v, $n>::KEY_INLINE_MAX,
                Table::<$k, $v, $n>::VALUE_INLINE_MAX,
                Table::<$k, $v, $n>::VALUE_INT_BITS,
            );
        };
    }
    layout!("ptr8", W8Ptr, W8Ptr, 14);
    layout!("int8", W8Ptr, W8Int, 14);
    layout!("str7", W8, W8, 14);
    layout!("str15w", W16, W16, 7);
    layout!("hybrid", W8, W16, 9);

    let (bytes_sz, entry_sz, pair_sz, value_sz) = baseline::sizes();
    println!(
        "\nBaseline sizes: `Bytes` {bytes_sz} B, `Entry` {entry_sz} B, `(Bytes, Entry)` {pair_sz} B, \
         `Value` (String arm only) {value_sz} B."
    );
    println!(
        "Segment header {HEADER_BYTES} B; {REGULAR_BUCKETS} regular + {STASH_BUCKETS} stash = \
         {BUCKETS} buckets of {BUCKET_BYTES} B; R9 reserves {R9_RESERVED_BYTES} B of the header."
    );

    for shape in Shape::ALL {
        let pairs = generate(shape, n);
        let s = summarize(&pairs);
        let misses: Vec<Vec<u8>> = (0..n.min(200_000))
            .map(|i| format!("miss-{}-{i}", shape.name()).into_bytes())
            .collect();

        header(&format!(
            "\n## Shape `{}` — mean key {:.1} B ({:.1}% ≤ 7 B, {:.1}% ≤ 15 B), \
             mean value {:.1} B ({:.1}% integers, {:.1}% ≤ 7 B, {:.1}% ≤ 15 B)",
            shape.name(),
            s.key_mean,
            s.key_le7 * 100.0,
            s.key_le15 * 100.0,
            s.val_mean,
            s.int_values * 100.0,
            s.val_le7 * 100.0,
            s.val_le15 * 100.0,
        ));

        let rows = vec![
            run_baseline(&pairs, &misses),
            run_variant::<W8Ptr, W8Ptr, 14>("ptr8", &pairs, &misses),
            run_variant::<W8Ptr, W8Int, 14>("int8", &pairs, &misses),
            run_variant::<W8, W8, 14>("str7", &pairs, &misses),
            run_variant::<W16, W16, 7>("str15w", &pairs, &misses),
            run_variant::<W8, W16, 9>("hybrid", &pairs, &misses),
        ];
        print_rows(shape.name(), &rows);
    }

    println!("\n## Split stall — per-insert timing, `redis-feel`, {n} inserts\n");
    println!("Inserts are bucketed by what they did: plain, split, or split + directory doubling.");
    println!("| variant | plain p50 | plain p99.9 | plain max | split p50 | split p99 | split max | x2 p50 | x2 max | splits | doublings |");
    println!("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |");
    let pairs = generate(Shape::RedisFeel, n);
    split_stall::<W8Ptr, W8Ptr, 14>("ptr8", &pairs);
    split_stall::<W8Ptr, W8Int, 14>("int8", &pairs);
    split_stall::<W8, W8, 14>("str7", &pairs);
    split_stall::<W16, W16, 7>("str15w", &pairs);
    split_stall::<W8, W16, 9>("hybrid", &pairs);
}
