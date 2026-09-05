//! Where per-key metadata lives, decided by building the table both ways.
//!
//! Every key carries metadata beyond its value — expiry, encoding, LRU/LFU state.
//! Issue 11 leaves two places to put it and asks for the choice to be *measured*:
//!
//! - **(a) inside the entry.** The slot stays two words, 16 B: a key word and a
//!   value word, the value word pointing at a record that carries the metadata
//!   alongside the payload. 256 B of bucket minus 48 B of metadata block leaves
//!   208 B, so 13 slots per bucket.
//! - **(b) a third slot word.** Metadata gets its own 8 B beside the key and the
//!   value, so a slot is 24 B. The same 256 B bucket then holds 9 slots, and the
//!   metadata block shrinks with the slot count.
//!
//! The choice is a per-entry byte cost, and it is not the slot arithmetic alone:
//! a wider slot means fewer slots per segment, so the fixed 16 KB size class and
//! the directory are amortised over fewer live entries, and Dash displacement
//! reaches a different occupancy at a different width. That is why this is a test
//! that fills two real tables rather than a comment doing division.
//!
//! Both tables are filled with the same keys through the same code path, and the
//! figure compared is structural bytes per live entry at the **allocated size
//! class** — what the process pays, not what the struct declares.

use frogdb_table::layout::{BUCKETS, SLOTS_PER_BUCKET};
use frogdb_table::word::ValueWord;
use frogdb_table::{Table, TableSeed};

/// Slots per bucket once a third 8-byte word joins the key and the value.
///
/// 24 B slots and a 40 B metadata block (the fingerprint, route, bitmap and
/// stash-count arrays all shrink with the slot count): 40 + 9 × 24 = 256 exactly.
const WIDE_SLOTS_PER_BUCKET: usize = 9;

/// Option (b)'s value: the value word plus a metadata word of its own.
///
/// The contents do not matter to a layout measurement — only that it is the 8
/// bytes option (b) would spend and that it is dropped like a real value.
#[derive(Debug)]
struct ValueAndMetadata {
    _value: ValueWord,
    _metadata: u64,
}

const KEYS: usize = 200_000;

fn keys() -> Vec<String> {
    (0..KEYS).map(|i| format!("key:{i}")).collect()
}

/// Fills option (a): metadata inside the entry, 16 B slots, 13 per bucket.
fn narrow(keys: &[String]) -> Table<ValueWord, SLOTS_PER_BUCKET> {
    let mut t = Table::with_seed(TableSeed::from_u64(11));
    for (i, k) in keys.iter().enumerate() {
        t.insert(k.as_bytes(), ValueWord::from_int(i as i64));
    }
    t
}

/// Fills option (b): metadata in a third slot word, 24 B slots, 9 per bucket.
fn wide(keys: &[String]) -> Table<ValueAndMetadata, WIDE_SLOTS_PER_BUCKET> {
    let mut t = Table::with_seed(TableSeed::from_u64(11));
    for (i, k) in keys.iter().enumerate() {
        t.insert(
            k.as_bytes(),
            ValueAndMetadata {
                _value: ValueWord::from_int(i as i64),
                _metadata: i as u64,
            },
        );
    }
    t
}

/// Both widths must fill a 256 B bucket exactly, or the comparison is between a
/// tuned layout and an untuned one rather than between two metadata placements.
#[test]
fn both_slot_widths_fill_a_bucket_exactly() {
    use frogdb_table::bucket::{Bucket, Slot};
    use std::mem::size_of;

    assert_eq!(size_of::<Slot<ValueWord>>(), 16, "option (a) slot");
    assert_eq!(size_of::<Slot<ValueAndMetadata>>(), 24, "option (b) slot");

    assert_eq!(
        size_of::<Bucket<ValueWord, SLOTS_PER_BUCKET>>(),
        256,
        "option (a) bucket"
    );
    assert_eq!(
        size_of::<Bucket<ValueAndMetadata, WIDE_SLOTS_PER_BUCKET>>(),
        256,
        "option (b) bucket"
    );

    // And neither wastes a slot: one more would overflow the bucket.
    assert!(size_of::<Bucket<ValueWord, { SLOTS_PER_BUCKET + 1 }>>() > 256);
    assert!(size_of::<Bucket<ValueAndMetadata, { WIDE_SLOTS_PER_BUCKET + 1 }>>() > 256);
}

/// The measurement issue 11 asks for. Metadata in the entry wins, and this
/// records by how much.
#[test]
fn metadata_in_the_entry_costs_fewer_bytes_per_key_than_a_third_slot_word() {
    let keys = keys();
    let a = narrow(&keys);
    let b = wide(&keys);

    assert_eq!(a.len(), KEYS);
    assert_eq!(b.len(), KEYS);

    let a_bpe = a.structural_bytes_per_entry();
    let b_bpe = b.structural_bytes_per_entry();

    println!(
        "option (a) metadata in the entry   : slots/bucket {SLOTS_PER_BUCKET}, slots/segment {}, segments {}, occupancy {:.3}, {a_bpe:.1} B/entry",
        BUCKETS * SLOTS_PER_BUCKET,
        a.segment_count(),
        a.occupancy()
    );
    println!(
        "option (b) metadata in a slot word : slots/bucket {WIDE_SLOTS_PER_BUCKET}, slots/segment {}, segments {}, occupancy {:.3}, {b_bpe:.1} B/entry",
        BUCKETS * WIDE_SLOTS_PER_BUCKET,
        b.segment_count(),
        b.occupancy()
    );
    println!("delta: {:+.1} B/entry for option (b)", b_bpe - a_bpe);

    assert!(
        b_bpe > a_bpe,
        "option (b) was not the more expensive layout: {b_bpe:.1} vs {a_bpe:.1} B/entry"
    );

    // The gap is a whole slot word amortised over the segment, not noise. If a
    // future change ever narrows it to under 4 B/entry the decision is worth
    // revisiting, so fail rather than let it drift silently.
    assert!(
        b_bpe - a_bpe > 4.0,
        "the two layouts came within {:.1} B/entry — re-take the decision",
        b_bpe - a_bpe
    );
}

/// The three statistics of the occupancy cycle, and the per-entry cost at each.
struct Cycle {
    peak_occupancy: f64,
    trough_occupancy: f64,
    mean_occupancy: f64,
    /// The cost at the peak — the lowest, because occupancy is the denominator.
    best_bpe: f64,
    /// The cost at the trough: the worst a table of this shape ever costs.
    worst_bpe: f64,
    mean_bpe: f64,
    end_occupancy: f64,
    end_bpe: f64,
}

/// Fills a table to `n` keys, sampling the cycle as it goes.
///
/// Sampled, not every insert: `occupancy` and `structural_bytes_per_entry` are
/// both O(1), but a round of splits is thousands of inserts wide, so one sample
/// in 64 resolves the top and the bottom of a round with room to spare. The
/// first 10 000 keys are skipped: a table with a handful of segments has not
/// reached its steady-state cycle yet, and its early troughs are an artifact of
/// starting from one segment rather than a property of the layout.
fn measure_cycle(n: usize) -> Cycle {
    let mut t: Table<ValueWord, SLOTS_PER_BUCKET> = Table::with_seed(TableSeed::from_u64(11));

    let mut peak_occupancy = 0.0f64;
    let mut trough_occupancy = f64::INFINITY;
    let mut best_bpe = f64::INFINITY;
    let mut worst_bpe = 0.0f64;
    let mut occupancy_sum = 0.0f64;
    let mut bpe_sum = 0.0f64;
    let mut samples = 0u32;

    for i in 0..n {
        t.insert(format!("key:{i}").as_bytes(), ValueWord::from_int(i as i64));
        if i % 64 == 0 && i > 10_000 {
            let (occ, bpe) = (t.occupancy(), t.structural_bytes_per_entry());
            peak_occupancy = peak_occupancy.max(occ);
            trough_occupancy = trough_occupancy.min(occ);
            best_bpe = best_bpe.min(bpe);
            worst_bpe = worst_bpe.max(bpe);
            occupancy_sum += occ;
            bpe_sum += bpe;
            samples += 1;
        }
    }

    let samples = f64::from(samples);
    Cycle {
        peak_occupancy,
        trough_occupancy,
        mean_occupancy: occupancy_sum / samples,
        best_bpe,
        worst_bpe,
        mean_bpe: bpe_sum / samples,
        end_occupancy: t.occupancy(),
        end_bpe: t.structural_bytes_per_entry(),
    }
}

/// The chosen layout has to hold the occupancy the issue asks for, and hold the
/// per-entry cost under what the spike measured.
///
/// # Which spike number is comparable to which of these
///
/// The spike's `str7` run reported **0.581 occupancy and 33.6 B/entry** for a
/// *settled* table — one figure, taken at the end of a fill, with no
/// displacement. The comparable statistic here is the settled one
/// (`end_occupancy` / `end_bpe`) or, phase-independently, the cycle mean. The
/// peak is **not** comparable to it: it is the top of a cycle the spike's
/// single measurement never reported, and quoting the peak against a settled
/// figure overstates the win. All four are printed for that reason.
///
/// What the honest comparison says, on the numbers this test prints:
///
/// - the **cycle mean** is 0.716 / 28.7 B/entry at 200 000 keys and
///   0.685 / 30.1 at 1 M — a real but modest win over 0.581 / 33.6;
/// - the **settled** figure at 1 M is 0.596 / 33.6, which is *indistinguishable
///   from the spike*. At 200 000 it is 0.738 / 27.1. The difference between
///   those two is phase, not structure;
/// - at the **trough** the layout costs 38.9 B/entry, which is *worse* than the
///   spike's settled 33.6. That is why no assertion below claims otherwise.
///
/// The win this layout is actually being kept for is the peak — 0.913, which is
/// what bounds how much memory a shard needs to have on hand before a split —
/// and the fact that growth is incremental at all. It is not a large steady-state
/// bytes-per-entry win.
///
/// # Why there is a cycle at all
///
/// Occupancy in an extendible-hash table oscillates, and the amplitude is a
/// property of the geometry rather than of the workload. A segment holds 819
/// slots, so the number of keys landing in one is binomial with a relative spread
/// near `1/sqrt(819)`, about 3.5 %: every segment fills at nearly the same rate,
/// they reach their split threshold at nearly the same time, and the table
/// therefore runs in rounds. Occupancy climbs to its peak, a round of splits
/// halves it, and it climbs again.
///
/// So "occupancy at 200 000 keys" measures which phase of that cycle 200 000
/// happens to fall in — a different key count gives a different answer with no
/// change to the structure. The peak is how full a segment gets before it is
/// forced to split, and it is the statistic displacement moves; the trough is
/// what a table sitting at an unlucky size actually costs; the mean is what a
/// table of unknown size costs in expectation.
#[test]
fn the_chosen_layout_beats_the_spike_it_replaces() {
    let c = measure_cycle(KEYS);

    println!("chosen layout, {KEYS} keys");
    println!(
        "  peak of cycle    : occupancy {:.3}, {:.1} B/entry",
        c.peak_occupancy, c.best_bpe
    );
    println!(
        "  trough of cycle  : occupancy {:.3}, {:.1} B/entry",
        c.trough_occupancy, c.worst_bpe
    );
    println!(
        "  cycle average    : occupancy {:.3}, {:.1} B/entry",
        c.mean_occupancy, c.mean_bpe
    );
    println!(
        "  settled at {KEYS}: occupancy {:.3}, {:.1} B/entry",
        c.end_occupancy, c.end_bpe
    );
    println!("  spike, settled, no displacement: occupancy 0.581, 33.6 B/entry");

    assert!(
        c.peak_occupancy >= 0.85,
        "peak occupancy {:.3} is under the 0.85 the issue asks for",
        c.peak_occupancy
    );

    // The trough bound is measured, not aspirational. Provenance: this test's
    // own printed line, which reports 0.515 both at 200 000 keys and at 1 M (see
    // `the_occupancy_cycle_does_not_drift_with_the_key_count`). A round of splits
    // halves a table's occupancy, so a trough a little over half the 0.913 peak
    // is the structure behaving as designed. The bound is set at 0.50 — about
    // 3 % under the measurement, enough for seed and key-count variation, tight
    // enough to fail if displacement ever stops carrying the trough back up.
    assert!(
        c.trough_occupancy >= 0.50,
        "trough occupancy {:.3} fell below the measured 0.50 floor",
        c.trough_occupancy
    );

    // The phase-independent comparison against the spike's settled 33.6. The
    // cycle mean is the statistic to hold: the peak beats 33.6 comfortably but
    // is not comparable to a settled figure, and the trough (38.9 B/entry) is
    // *worse* than the spike — asserting on either alone would be picking the
    // flattering phase.
    assert!(
        c.mean_bpe < 33.6,
        "{:.1} B/entry averaged over the cycle does not beat the spike's 33.6",
        c.mean_bpe
    );
    // And the occupancy the whole displacement machinery exists to buy, on the
    // same phase-independent footing.
    assert!(
        c.mean_occupancy > 0.581,
        "cycle-average occupancy {:.3} does not beat the spike's settled 0.581",
        c.mean_occupancy
    );
}

/// The same three statistics an order of magnitude further out.
///
/// The cycle is a property of the geometry, so it must not drift with the key
/// count — if the peak, trough and mean at 1 M keys differed materially from
/// those at 200 000, the 200 000-key figures would be a coincidence of size
/// rather than a description of the layout. This is the run the report's §3
/// table quotes alongside the 200 000-key one.
///
/// It is also where the settled figure stops flattering the layout: settled at
/// 1 M the table reports 0.596 / 33.6 B/entry against the spike's 0.581 / 33.6.
/// Those are the same numbers. The 200 000-key settled figure (0.738 / 27.1)
/// looks better only because 200 000 lands higher in the cycle.
#[test]
fn the_occupancy_cycle_does_not_drift_with_the_key_count() {
    let small = measure_cycle(KEYS);
    let large = measure_cycle(1_000_000);

    println!(
        "  1M peak    : occupancy {:.3}, {:.1} B/entry",
        large.peak_occupancy, large.best_bpe
    );
    println!(
        "  1M trough  : occupancy {:.3}, {:.1} B/entry",
        large.trough_occupancy, large.worst_bpe
    );
    println!(
        "  1M average : occupancy {:.3}, {:.1} B/entry",
        large.mean_occupancy, large.mean_bpe
    );
    println!(
        "  1M settled : occupancy {:.3}, {:.1} B/entry",
        large.end_occupancy, large.end_bpe
    );

    assert!(
        (large.peak_occupancy - small.peak_occupancy).abs() < 0.05,
        "peak moved with the key count: {:.3} at {KEYS} vs {:.3} at 1M",
        small.peak_occupancy,
        large.peak_occupancy
    );
    assert!(
        (large.mean_occupancy - small.mean_occupancy).abs() < 0.05,
        "cycle mean moved with the key count: {:.3} at {KEYS} vs {:.3} at 1M",
        small.mean_occupancy,
        large.mean_occupancy
    );
    assert!(
        large.trough_occupancy >= 0.50,
        "trough occupancy {:.3} at 1M fell below the measured 0.50 floor",
        large.trough_occupancy
    );
    assert!(
        large.mean_bpe < 33.6,
        "{:.1} B/entry averaged over the 1M cycle does not beat the spike's 33.6",
        large.mean_bpe
    );
}
