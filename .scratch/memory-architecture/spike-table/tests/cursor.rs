//! The SCAN-cursor proof (brief item 3).
//!
//! Redis-compat hard requirement: a SCAN that runs to completion while the table
//! splits underneath it must return every key that was present for the whole scan
//! **exactly once**. These tests are the executable form of that guarantee — and one
//! of them is the counter-example that shows why the reverse-binary scheme is needed
//! rather than the obvious directory walk.

use std::collections::HashMap;

use ahash::RandomState;
use memarch_spike_table::table::{Table, Val};
use memarch_spike_table::word::{W16, W8};

/// The table's default hasher (griddle's own, so the sweep compares like with like)
/// seeds itself from process randomness, which would make a failing scan
/// unreproducible. These tests pin the seed instead: same keys, same hash, same
/// split points, every run.
const SEEDS: [u64; 4] = [
    0x9E37_79B9_7F4A_7C15,
    0xBF58_476D_1CE4_E5B9,
    0x94D0_49BB_1331_11EB,
    0xD1B5_4A32_D192_ED03,
];

fn fixed_hasher() -> RandomState {
    RandomState::with_seeds(SEEDS[0], SEEDS[1], SEEDS[2], SEEDS[3])
}

/// `TableStr7`/`TableHybrid` with the seed pinned.
type Str7 = Table<W8, W8, 14, RandomState>;
type Hybrid = Table<W8, W16, 9, RandomState>;

fn str7() -> Str7 {
    Str7::with_hasher(fixed_hasher())
}

/// Runs a full SCAN with `step`, inserting `churn` fresh keys between every step so
/// the table splits mid-iteration. Returns how many times each key was emitted.
fn scan_with_churn<F>(
    table: &mut Str7,
    preexisting: usize,
    churn: usize,
    mut step: F,
) -> (HashMap<Vec<u8>, u32>, usize, usize)
where
    F: FnMut(&Str7, u64, &mut Vec<Vec<u8>>) -> u64,
{
    for i in 0..preexisting {
        let key = format!("pre:{i}").into_bytes();
        assert!(table.insert(&key, Val::Int(i as i64)));
    }
    let splits_before = table.stats.splits;

    let mut seen: HashMap<Vec<u8>, u32> = HashMap::new();
    let mut cursor = 0u64;
    let mut steps = 0usize;
    let mut fresh = 0usize;
    loop {
        let mut out = Vec::new();
        cursor = step(table, cursor, &mut out);
        for k in out {
            *seen.entry(k).or_insert(0) += 1;
        }
        steps += 1;
        assert!(steps < 100_000, "scan did not terminate");

        for _ in 0..churn {
            let key = format!("new:{fresh}").into_bytes();
            table.insert(&key, Val::Int(fresh as i64));
            fresh += 1;
        }
        if cursor == 0 {
            break;
        }
    }
    let splits = (table.stats.splits - splits_before) as usize;
    (seen, steps, splits)
}

#[test]
fn reverse_binary_cursor_sees_every_preexisting_key_exactly_once() {
    let preexisting = 8_000;
    let mut table = str7();
    let (seen, steps, splits) =
        scan_with_churn(&mut table, preexisting, 400, |t, c, out| t.scan(c, out));

    assert!(
        splits >= 4,
        "test is only meaningful if splits happened mid-scan, saw {splits}"
    );

    let mut missing = Vec::new();
    let mut duplicated = Vec::new();
    for i in 0..preexisting {
        let key = format!("pre:{i}").into_bytes();
        match seen.get(&key) {
            None => missing.push(i),
            Some(1) => {}
            Some(n) => duplicated.push((i, *n)),
        }
    }
    assert!(
        missing.is_empty() && duplicated.is_empty(),
        "scan over {steps} steps with {splits} mid-scan splits: \
         {} pre-existing keys missing, {} duplicated (first missing {:?}, first dup {:?})",
        missing.len(),
        duplicated.len(),
        missing.first(),
        duplicated.first()
    );
}

/// The counter-example. Walking directory indices in order is what a first attempt
/// looks like; it breaks because several directory entries alias one segment whose
/// local depth is below the global depth, and because the directory doubles under
/// the cursor. This test asserts the failure so the report's "chosen cursor scheme"
/// section has evidence rather than an argument.
#[test]
fn linear_directory_cursor_breaks_under_mid_scan_splits() {
    let preexisting = 8_000;
    let mut table = str7();
    let (seen, _steps, splits) = scan_with_churn(&mut table, preexisting, 400, |t, c, out| {
        t.scan_linear(c, out)
    });
    assert!(splits >= 4, "need splits for the counter-example");

    let mut missing = 0;
    let mut duplicated = 0;
    for i in 0..preexisting {
        let key = format!("pre:{i}").into_bytes();
        match seen.get(&key) {
            None => missing += 1,
            Some(1) => {}
            Some(_) => duplicated += 1,
        }
    }
    eprintln!(
        "linear cursor over {preexisting} pre-existing keys, {splits} mid-scan splits: \
         {missing} missing, {duplicated} duplicated"
    );
    assert!(
        missing > 0 || duplicated > 0,
        "linear cursor unexpectedly held the guarantee — the counter-example is stale"
    );
}

#[test]
fn scan_without_mutation_is_exactly_once() {
    let mut table = str7();
    for i in 0..20_000 {
        let key = format!("k:{i}").into_bytes();
        assert!(table.insert(&key, Val::Int(i)));
    }
    let mut out = Vec::new();
    let mut cursor = 0;
    loop {
        cursor = table.scan(cursor, &mut out);
        if cursor == 0 {
            break;
        }
    }
    assert_eq!(out.len(), table.len(), "scan emitted a different count");
    let mut sorted = out.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), out.len(), "scan emitted a duplicate");
}

/// Directory entries that alias one under-deep segment must be visited once, not once
/// per alias. This is the case the reverse-binary advance at *local* depth handles.
#[test]
fn aliased_directory_entries_are_visited_once() {
    let mut table = str7();
    // Aliasing appears the moment one segment splits at the global depth and doubles
    // the directory: every other segment is then one depth short and owns two entries.
    // With a uniform hash the segments catch up again, so stop at the first insert that
    // leaves the directory aliased with a non-trivial number of segments.
    for i in 0..200_000 {
        let key = format!("a:{i}").into_bytes();
        table.insert(&key, Val::Int(i));
        if table.segments() >= 8 && table.directory_entries() > table.segments() {
            break;
        }
    }
    assert!(
        table.directory_entries() > table.segments(),
        "test needs an aliasing directory: {} entries, {} segments",
        table.directory_entries(),
        table.segments()
    );

    let mut out = Vec::new();
    let mut cursor = 0;
    let mut steps = 0;
    loop {
        cursor = table.scan(cursor, &mut out);
        steps += 1;
        if cursor == 0 {
            break;
        }
    }
    assert_eq!(
        steps,
        table.segments(),
        "one step per segment, not per directory entry"
    );
    assert_eq!(out.len(), table.len());
}

/// The guarantee is not a property of one slot layout: the wide/hybrid word does not
/// change the cursor, and the proof must hold there too.
#[test]
fn cursor_guarantee_holds_for_the_hybrid_layout() {
    let mut table = Hybrid::with_hasher(fixed_hasher());
    let preexisting = 6_000;
    for i in 0..preexisting {
        table.insert(format!("pre:{i}").as_bytes(), Val::Int(i as i64));
    }
    let mut seen: HashMap<Vec<u8>, u32> = HashMap::new();
    let mut cursor = 0u64;
    let mut fresh = 0;
    loop {
        let mut out = Vec::new();
        cursor = table.scan(cursor, &mut out);
        for k in out {
            *seen.entry(k).or_insert(0) += 1;
        }
        for _ in 0..400 {
            table.insert(format!("new:{fresh}").as_bytes(), Val::Int(fresh));
            fresh += 1;
        }
        if cursor == 0 {
            break;
        }
    }
    for i in 0..preexisting {
        let key = format!("pre:{i}").into_bytes();
        assert_eq!(
            seen.get(&key),
            Some(&1),
            "key pre:{i} not seen exactly once"
        );
    }
}
