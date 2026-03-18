#![no_main]
use bytes::Bytes;
use frogdb_types::skiplist::SkipList;
use libfuzzer_sys::fuzz_target;
use libfuzzer_sys::arbitrary::{self, Arbitrary};
use ordered_float::OrderedFloat;

#[derive(Arbitrary, Debug)]
enum Op {
    Insert { score_bits: u64, member: u8 },
    Remove { score_bits: u64, member: u8 },
    GetByRank { rank: u8 },
}

#[derive(Arbitrary, Debug)]
struct SkipListInput {
    ops: Vec<Op>,
}

fn member_name(m: u8) -> Bytes {
    Bytes::from(format!("m{}", m & 0x0F))
}

fuzz_target!(|input: SkipListInput| {
    if input.ops.len() > 128 {
        return;
    }

    let mut sl = SkipList::new();

    for op in &input.ops {
        match op {
            Op::Insert { score_bits, member } => {
                let score = OrderedFloat(f64::from_bits(*score_bits));
                let mem = member_name(*member);
                sl.insert(score, mem);
            }
            Op::Remove { score_bits, member } => {
                let score = OrderedFloat(f64::from_bits(*score_bits));
                let mem = member_name(*member);
                sl.remove(score, &mem);
            }
            Op::GetByRank { rank } => {
                let _ = sl.get_by_rank(*rank as usize);
            }
        }
    }

    // Verify invariants after all operations
    let len = sl.len();

    // Check that iteration yields exactly `len` elements in sorted order
    let mut count = 0;
    let mut prev: Option<(OrderedFloat<f64>, Bytes)> = None;
    for (score, member) in sl.iter() {
        count += 1;
        if let Some((prev_score, ref prev_member)) = prev {
            assert!(
                (prev_score, prev_member) <= (score, member),
                "skiplist order violated: ({prev_score:?}, {prev_member:?}) > ({score:?}, {member:?})"
            );
        }
        prev = Some((score, member.clone()));
    }
    assert_eq!(count, len, "iter count {count} != len {len}");

    // Check rank consistency: get_by_rank(i) should return the i-th element
    for i in 0..len.min(64) {
        let entry = sl.get_by_rank(i);
        assert!(entry.is_some(), "get_by_rank({i}) returned None but len={len}");
    }
});
