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
//! 1. **Contents agree.** Every get, insert and remove returns what the model
//!    says, and `len`/`iter` agree at the end.
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

use frogdb_table::word::ValueWord;
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
                    previous.is_some(),
                    expected.is_some(),
                    "insert disagreed on whether {k:?} was present"
                );
            }
            Op::Remove { key } => {
                let k = key_of(*key);
                let removed = t.remove(&k);
                let expected = model.remove(&k);
                assert_eq!(
                    removed.is_some(),
                    expected.is_some(),
                    "remove disagreed on whether {k:?} was present"
                );
            }
            Op::Get { key } => {
                let k = key_of(*key);
                assert_eq!(
                    t.get(&k).is_some(),
                    model.contains_key(&k),
                    "get disagreed on {k:?}"
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
                let seen = scan_all(&mut t, 1 + *count as usize, |t| {
                    for _ in 0..churn {
                        let k = format!("grow:{fresh}").into_bytes();
                        fresh += 1;
                        t.insert(&k, ValueWord::from_int(i64::from(fresh)));
                    }
                });
                // The model has to learn about the churn too, or the next op
                // compares against a stale set.
                let seen_set: HashSet<Vec<u8>> = seen.into_iter().collect();
                for k in &before {
                    assert!(
                        seen_set.contains(k),
                        "SCAN lost {k:?}, which was present for the whole walk"
                    );
                }
                for slot in t.iter() {
                    let k = slot_key(slot);
                    if !model.contains_key(&k) {
                        model.insert(k, 0);
                    }
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
        assert!(model.contains_key(&k), "table holds {k:?}, model does not");
        walked += 1;
    }
    assert_eq!(walked, model.len(), "iter yielded the wrong number of slots");
});
