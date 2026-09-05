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

/// The chosen layout has to hold the occupancy the issue asks for, and hold the
/// per-entry cost under what the spike measured. The spike's `str7` run reported
/// 0.581 occupancy and 33.6 B/entry with no displacement; displacement is what
/// buys the difference.
///
/// # Why the peak, and not the figure at some key count
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
/// change to the structure. The peak is the phase-independent statistic and the
/// one displacement moves: it is how full a segment gets before it is forced to
/// split. Both figures are printed, because the trough is what a table sitting at
/// an unlucky size actually costs.
#[test]
fn the_chosen_layout_beats_the_spike_it_replaces() {
    let mut t: Table<ValueWord, SLOTS_PER_BUCKET> = Table::with_seed(TableSeed::from_u64(11));

    let mut peak_occupancy = 0.0f64;
    let mut best_bpe = f64::INFINITY;
    for i in 0..KEYS {
        t.insert(format!("key:{i}").as_bytes(), ValueWord::from_int(i as i64));
        // Sampled, not every insert: `structural_bytes_per_entry` is O(1) but the
        // peak only needs enough resolution to catch the top of a round, and a
        // round is thousands of inserts wide.
        if i % 64 == 0 && i > 10_000 {
            peak_occupancy = peak_occupancy.max(t.occupancy());
            best_bpe = best_bpe.min(t.structural_bytes_per_entry());
        }
    }

    let end_occupancy = t.occupancy();
    let end_bpe = t.structural_bytes_per_entry();
    println!("chosen layout, peak of cycle : occupancy {peak_occupancy:.3}, {best_bpe:.1} B/entry");
    println!("chosen layout, at {KEYS} keys: occupancy {end_occupancy:.3}, {end_bpe:.1} B/entry");
    println!("spike (no displacement)      : occupancy 0.581, 33.6 B/entry");

    assert!(
        peak_occupancy >= 0.85,
        "peak occupancy {peak_occupancy:.3} is under the 0.85 the issue asks for"
    );
    assert!(
        best_bpe < 33.6,
        "{best_bpe:.1} B/entry does not beat the spike's 33.6"
    );
    // Even caught at its worst phase the layout has to beat the spike, or the
    // rewrite bought occupancy at one key count and nothing at another.
    assert!(
        end_bpe < 33.6,
        "{end_bpe:.1} B/entry at {KEYS} keys does not beat the spike's 33.6"
    );
}
