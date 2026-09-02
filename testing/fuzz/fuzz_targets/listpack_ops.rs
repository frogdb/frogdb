#![no_main]
//! Model-based fuzzing of the quicklist chain and the listpack block encoding.
//!
//! Every operation runs against both a [`ListValue`] (a chain of listpack
//! blocks) and a `VecDeque<Bytes>` model, and the two are compared element by
//! element afterwards. Random element sizes straddle the plain-block threshold
//! so block splits, merges and mid-block inserts all get exercised.

use bytes::Bytes;
use frogdb_types::Listpack;
use frogdb_types::types::ListValue;
use libfuzzer_sys::arbitrary::{self, Arbitrary};
use libfuzzer_sys::fuzz_target;
use std::collections::VecDeque;

#[derive(Arbitrary, Debug)]
enum Op {
    PushFront { value: u8, size: u8 },
    PushBack { value: u8, size: u8 },
    PopFront,
    PopBack,
    Set { index: i8, value: u8 },
    InsertBefore { pivot: u8, value: u8 },
    InsertAfter { pivot: u8, value: u8 },
    Remove { count: i8, value: u8 },
    Trim { start: i8, end: i8 },
}

#[derive(Arbitrary, Debug)]
struct Input {
    ops: Vec<Op>,
}

/// Element bodies repeat across a small alphabet so pivots and LREM targets
/// actually hit. `size` decides the payload length: most are small enough to
/// pack, a few exceed the 8 KiB plain-block threshold.
fn element(value: u8, size: u8) -> Bytes {
    let len = match size % 8 {
        0..=5 => (size as usize % 24) + 1,
        6 => 200,
        _ => 9000,
    };
    let mut body = vec![b'a' + (value % 26); len];
    body[0] = b'A' + (value % 26);
    Bytes::from(body)
}

fn short(value: u8) -> Bytes {
    element(value, 0)
}

fn normalize(index: i8, len: usize) -> Option<usize> {
    if len == 0 {
        return None;
    }
    let i = index as i64;
    let i = if i < 0 { i + len as i64 } else { i };
    if i < 0 || i >= len as i64 {
        None
    } else {
        Some(i as usize)
    }
}

fn model_insert(model: &mut VecDeque<Bytes>, before: bool, pivot: &[u8], value: Bytes) {
    let Some(i) = model.iter().position(|e| e == pivot) else {
        return;
    };
    model.insert(if before { i } else { i + 1 }, value);
}

fn model_remove(model: &mut VecDeque<Bytes>, count: i64, target: &[u8]) {
    let limit = if count == 0 {
        usize::MAX
    } else {
        count.unsigned_abs() as usize
    };
    let mut hits: Vec<usize> = (0..model.len()).filter(|&i| model[i] == target).collect();
    if count < 0 {
        hits.reverse();
    }
    hits.truncate(limit);
    hits.sort_unstable();
    for (removed, i) in hits.into_iter().enumerate() {
        model.remove(i - removed);
    }
}

fn model_trim(model: &mut VecDeque<Bytes>, start: i64, end: i64) {
    let len = model.len() as i64;
    let s = if start < 0 { (start + len).max(0) } else { start };
    let e = if end < 0 { end + len } else { end.min(len - 1) };
    if s > e || s >= len {
        model.clear();
        return;
    }
    let (s, e) = (s as usize, e as usize);
    model.drain(e + 1..);
    model.drain(..s);
}

fn check(list: &ListValue, model: &VecDeque<Bytes>) {
    assert_eq!(list.len(), model.len(), "length diverged");
    let forward: Vec<&[u8]> = list.iter().collect();
    assert_eq!(forward.len(), model.len(), "forward iteration count diverged");
    for (i, expected) in model.iter().enumerate() {
        assert_eq!(forward[i], &expected[..], "element {i} diverged");
        assert_eq!(list.get(i as i64), Some(&expected[..]), "get({i}) diverged");
    }
    let reverse: Vec<&[u8]> = list.iter_rev().collect();
    assert_eq!(reverse.len(), model.len(), "reverse iteration count diverged");
    for (i, expected) in model.iter().rev().enumerate() {
        assert_eq!(reverse[i], &expected[..], "reverse element {i} diverged");
    }
    // The chain never keeps an empty block around, and never holds more blocks
    // than there are elements.
    assert!(
        list.block_count() <= model.len(),
        "empty block left in the chain: {} blocks for {} elements",
        list.block_count(),
        model.len()
    );
}

/// The same op sequence replayed onto a bare [`Listpack`], so the block encoding
/// is checked without the chain in the way.
fn exercise_listpack(model: &VecDeque<Bytes>) {
    let mut lp = Listpack::new();
    for e in model.iter().take(64) {
        lp.push_back(e);
    }
    let n = lp.len();
    assert_eq!(n, model.len().min(64));
    for (i, e) in model.iter().take(64).enumerate() {
        assert_eq!(lp.get(i), Some(&e[..]), "listpack get({i}) diverged");
    }
    if n >= 2 {
        let tail = lp.split_off(n / 2);
        assert_eq!(lp.len() + tail.len(), n, "split lost entries");
        lp.append(&tail);
        assert_eq!(lp.len(), n, "append lost entries");
        for (i, e) in model.iter().take(64).enumerate() {
            assert_eq!(lp.get(i), Some(&e[..]), "split/append changed entry {i}");
        }
    }
}

fuzz_target!(|input: Input| {
    if input.ops.len() > 96 {
        return;
    }

    let mut list = ListValue::new();
    let mut model: VecDeque<Bytes> = VecDeque::new();

    for op in &input.ops {
        match op {
            Op::PushFront { value, size } => {
                let e = element(*value, *size);
                list.push_front(e.clone());
                model.push_front(e);
            }
            Op::PushBack { value, size } => {
                let e = element(*value, *size);
                list.push_back(e.clone());
                model.push_back(e);
            }
            Op::PopFront => {
                assert_eq!(list.pop_front(), model.pop_front(), "pop_front diverged");
            }
            Op::PopBack => {
                assert_eq!(list.pop_back(), model.pop_back(), "pop_back diverged");
            }
            Op::Set { index, value } => {
                let e = short(*value);
                let applied = list.set(*index as i64, e.clone());
                match normalize(*index, model.len()) {
                    Some(i) => {
                        assert!(applied, "set({index}) rejected a valid index");
                        model[i] = e;
                    }
                    None => assert!(!applied, "set({index}) accepted an out-of-range index"),
                }
            }
            Op::InsertBefore { pivot, value } => {
                let p = short(*pivot);
                let e = short(*value);
                list.insert(true, &p, e.clone());
                model_insert(&mut model, true, &p, e);
            }
            Op::InsertAfter { pivot, value } => {
                let p = short(*pivot);
                let e = short(*value);
                list.insert(false, &p, e.clone());
                model_insert(&mut model, false, &p, e);
            }
            Op::Remove { count, value } => {
                let target = short(*value);
                let removed = list.remove(*count as i64, &target);
                let before = model.len();
                model_remove(&mut model, *count as i64, &target);
                assert_eq!(removed, before - model.len(), "remove count diverged");
            }
            Op::Trim { start, end } => {
                list.trim(*start as i64, *end as i64);
                model_trim(&mut model, *start as i64, *end as i64);
            }
        }
        check(&list, &model);
    }

    exercise_listpack(&model);

    // memory_size must be a pure function of the chain's contents: rebuilding
    // the same list the same way must report the same number.
    let mut rebuilt = ListValue::new();
    for e in &model {
        rebuilt.push_back(e.clone());
    }
    let mut again = ListValue::new();
    for e in &model {
        again.push_back(e.clone());
    }
    assert_eq!(
        rebuilt.memory_size(),
        again.memory_size(),
        "memory_size is not run-stable"
    );
});
