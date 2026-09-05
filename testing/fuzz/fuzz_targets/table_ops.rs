//! The segmented keyspace table against a `HashMap` model.
//!
//! The table is the structure the whole keyspace will sit in, it is full of
//! `unsafe` (tagged words, `MaybeUninit` slots, a hand-laid-out bucket), and its
//! hardest invariants only show up after a split has moved entries around. A
//! model check is the cheapest way to reach those states: drive the table and a
//! `HashMap` with the same operations and assert they agree.
//!
//! Three properties, in rising order of what they are worth:
//!
//! 1. **Contents agree.** Every get, insert and remove returns the *value* the
//!    model says, not merely whether a key was present, and `len`/`iter` agree
//!    at the end. Presence alone would pass a table that returned the right
//!    key's neighbour — precisely what a mis-split or a bad displacement does.
//! 2. **SCAN returns every stable key.** Redis's guarantee, and the reason the
//!    cursor is reverse-binary at the *local* depth. Checked on a quiet table.
//! 3. **SCAN keeps that guarantee across splits.** The interesting one: keys are
//!    inserted *between* scan steps, so the table splits underneath a live
//!    cursor, and every key present for the whole scan must still be returned.
//!
//! Growth is real, not simulated: the op stream inserts enough keys to push the
//! table through several directory doublings, so splits happen mid-scan rather
//! than only between runs.

#![no_main]

use std::collections::{HashMap, HashSet};

use frogdb_table::word::{Decoded, ValueWord};
use frogdb_table::{Table, TableSeed};
use libfuzzer_sys::arbitrary::{self, Arbitrary};
use libfuzzer_sys::fuzz_target;

#[derive(Arbitrary, Debug)]
enum Op {
    Insert { key: u16, value: i64 },
    Remove { key: u16 },
    Get { key: u16 },
    /// Insert a run of fresh keys, to force the table to grow and split.
    Grow { count: u16 },
    /// Walk the whole table with SCAN and check every key comes back.
    Scan { count: u8 },
    /// Walk with SCAN while inserting between steps, so the table splits under
    /// the cursor.
    ScanUnderChurn { count: u8, churn: u8 },
}

#[derive(Arbitrary, Debug)]
struct Input {
    seed: u64,
    ops: Vec<Op>,
}

/// Keys are short and drawn from a small space so collisions, overwrites and
/// removes of present keys all happen often. The `:` keeps them inline-eligible
/// at some lengths and not others.
fn key_of(k: u16) -> Vec<u8> {
    format!("k:{k}").into_bytes()
}

fn slot_key(slot: &frogdb_table::Slot<ValueWord>) -> Vec<u8> {
    let mut buf = [0u8; 16];
    slot.key.bytes(&mut buf).to_vec()
}

/// The integer a value word holds.
///
/// Every value this target stores is written with `ValueWord::from_int`, which
/// has two encodings: inline for the 61-bit range, and an 8-byte little-endian
/// record for anything wider. Both are read back here, so the comparison covers
/// out-of-line values — the ones that own a refcounted record and are therefore
/// the interesting half for split and displacement bugs.
fn value_of(v: &ValueWord) -> i64 {
    let mut buf = [0u8; 16];
    match v.decode(&mut buf) {
        Decoded::Int(i) => i,
        Decoded::Bytes(b) => {
            let bytes: [u8; 8] = b
                .try_into()
                .unwrap_or_else(|_| panic!("a stored integer came back as {} bytes", b.len()));
            i64::from_le_bytes(bytes)
        }
    }
}

/// Walks the table with SCAN, calling `between` after each step, and returns
/// every key seen. Bounded so a pathological cursor cannot hang the fuzzer.
fn scan_all(
    t: &mut Table<ValueWord>,
    count: usize,
    mut between: impl FnMut(&mut Table<ValueWord>),
) -> Vec<Vec<u8>> {
    let mut seen = Vec::new();
    let mut cursor = 0u64;
    // A step returns at least one segment, so the walk cannot need more steps
    // than there are directory entries, plus slack for the directory growing
    // under it.
    let mut budget = 4 * (1usize << t.global_depth()) + 64;
    loop {
        cursor = t.scan(cursor, count, |slot| seen.push(slot_key(slot)));
        if cursor == 0 {
            return seen;
        }
        budget -= 1;
        assert!(budget > 0, "SCAN did not terminate");
        between(t);
    }
}

fuzz_target!(|input: Input| {
    if input.ops.len() > 64 {
        return;
    }

    let mut t: Table<ValueWord> = Table::with_seed(TableSeed::from_u64(input.seed));
    let mut model: HashMap<Vec<u8>, i64> = HashMap::new();
    // Fresh keys for `Grow`, kept out of the `k:` space so they never collide
    // with the small-space ops above.
    let mut fresh = 0u32;

    for op in &input.ops {
        match op {
            Op::Insert { key, value } => {
                let k = key_of(*key);
                let previous = t.insert(&k, ValueWord::from_int(*value));
                let expected = model.insert(k.clone(), *value);
                assert_eq!(
                    previous.as_ref().map(value_of),
                    expected,
                    "insert returned the wrong previous value for {k:?}"
                );
            }
            Op::Remove { key } => {
                let k = key_of(*key);
                let removed = t.remove(&k);
                let expected = model.remove(&k);
                assert_eq!(
                    removed.as_ref().map(value_of),
                    expected,
                    "remove returned the wrong value for {k:?}"
                );
            }
            Op::Get { key } => {
                let k = key_of(*key);
                assert_eq!(
                    t.get(&k).map(value_of),
                    model.get(&k).copied(),
                    "get returned the wrong value for {k:?}"
                );
            }
            Op::Grow { count } => {
                // Capped so one op cannot dominate a fuzz iteration's runtime,
                // but large enough that a handful of these cross a doubling.
                for _ in 0..(*count % 4096) {
                    let k = format!("grow:{fresh}").into_bytes();
                    fresh += 1;
                    t.insert(&k, ValueWord::from_int(i64::from(fresh)));
                    model.insert(k, i64::from(fresh));
                }
            }
            Op::Scan { count } => {
                let seen = scan_all(&mut t, 1 + *count as usize, |_| {});
                let unique: HashSet<Vec<u8>> = seen.into_iter().collect();
                for k in model.keys() {
                    assert!(unique.contains(k), "SCAN missed {k:?} on a quiet table");
                }
                assert_eq!(
                    unique.len(),
                    model.len(),
                    "SCAN returned keys the table does not hold"
                );
            }
            Op::ScanUnderChurn { count, churn } => {
                // Everything present for the whole scan must be returned. Keys
                // added mid-scan may or may not be, exactly as in Redis, so the
                // check is against the set taken *before* the walk.
                let before: Vec<Vec<u8>> = model.keys().cloned().collect();
                let churn = 1 + *churn as usize;
                // The closure cannot touch the model (it borrows the table), so
                // the churn records what it wrote and the model learns it after
                // the walk — with the real value, so the end-of-run value check
                // still has something to compare against.
                let mut churned: Vec<(Vec<u8>, i64)> = Vec::new();
                let seen = scan_all(&mut t, 1 + *count as usize, |t| {
                    for _ in 0..churn {
                        let k = format!("grow:{fresh}").into_bytes();
                        fresh += 1;
                        let v = i64::from(fresh);
                        t.insert(&k, ValueWord::from_int(v));
                        churned.push((k, v));
                    }
                });
                let seen_set: HashSet<Vec<u8>> = seen.into_iter().collect();
                for k in &before {
                    assert!(
                        seen_set.contains(k),
                        "SCAN lost {k:?}, which was present for the whole walk"
                    );
                }
                for (k, v) in churned {
                    model.insert(k, v);
                }
            }
        }
    }

    // Contents agree at the end, walked from the table's side so an entry the
    // model never knew about is caught too.
    assert_eq!(t.len(), model.len(), "length disagreed");
    let mut walked = 0;
    for slot in t.iter() {
        let k = slot_key(slot);
        match model.get(&k) {
            Some(v) => assert_eq!(
                value_of(&slot.val),
                *v,
                "table holds the wrong value for {k:?}"
            ),
            None => panic!("table holds {k:?}, model does not"),
        }
        walked += 1;
    }
    assert_eq!(walked, model.len(), "iter yielded the wrong number of slots");
});
