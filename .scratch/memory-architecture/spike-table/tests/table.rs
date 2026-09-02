//! Layout and correctness tests for the R5 prototype.

use std::mem::size_of;

use memarch_spike_table::segment::{
    Bucket, Segment, SegmentHeader, Slot, BUCKETS, BUCKET_BYTES, HEADER_BYTES, META_BYTES,
    R9_RESERVED_BYTES, REGULAR_BUCKETS, STASH_BUCKETS,
};
use memarch_spike_table::table::{Val, ValueOut};
use memarch_spike_table::word::{Decoded, InlineBuf, Word, W16, W8};
use memarch_spike_table::workload::{generate, Shape};
use memarch_spike_table::{TableHybrid, TableInt, TablePtr, TableStr15, TableStr7};

#[test]
fn bucket_is_four_cache_lines_and_header_is_one() {
    assert_eq!(size_of::<SegmentHeader>(), HEADER_BYTES);

    assert_eq!(size_of::<Slot<W8, W8>>(), 16);
    assert_eq!(size_of::<Slot<W16, W16>>(), 32);
    assert_eq!(size_of::<Slot<W8, W16>>(), 24);

    assert_eq!(size_of::<Bucket<W8, W8, 14>>(), BUCKET_BYTES);
    assert_eq!(size_of::<Bucket<W16, W16, 7>>(), BUCKET_BYTES);
    // The hybrid slot does not tile 224 bytes exactly; 8 bytes go unused per bucket.
    assert_eq!(size_of::<Bucket<W8, W16, 9>>(), META_BYTES + 9 * 24);
    assert!(size_of::<Bucket<W8, W16, 9>>() <= BUCKET_BYTES);
}

#[test]
fn segment_capacity_and_r9_reservation() {
    assert_eq!(BUCKETS, REGULAR_BUCKETS + STASH_BUCKETS);
    assert_eq!(Segment::<W8, W8, 14>::CAPACITY, 60 * 14);
    assert_eq!(Segment::<W16, W16, 7>::CAPACITY, 60 * 7);

    // R9's eviction state fits inside the one-cache-line segment header, with room
    // left over — the whole point of reserving it now rather than at issue-12 time.
    assert_eq!(R9_RESERVED_BYTES, 22);
    const { assert!(R9_RESERVED_BYTES < HEADER_BYTES) };
    let header_reserved_tail = size_of::<[u8; 24]>();
    assert!(
        R9_RESERVED_BYTES + header_reserved_tail <= HEADER_BYTES,
        "R9 state plus spare headroom must still fit one cache line"
    );
}

#[test]
fn word_inline_thresholds_are_what_the_tag_scheme_allows() {
    // 8-byte word: 7 payload bytes above the tag/length byte.
    assert_eq!(W8::INLINE_STR_MAX, 7);
    assert!(W8::encode_bytes(b"1234567").is_inline());
    assert!(!W8::encode_bytes(b"12345678").is_inline());
    // 16-byte word: 15 payload bytes.
    assert_eq!(W16::INLINE_STR_MAX, 15);
    assert!(W16::encode_bytes(b"123456789012345").is_inline());
    assert!(!W16::encode_bytes(b"1234567890123456").is_inline());

    // Integers: 61 bits in the narrow word, all 64 in the wide one.
    assert!(W8::encode_int(1 << 59).is_inline());
    assert!(!W8::encode_int(i64::MAX).is_inline());
    assert!(W16::encode_int(i64::MAX).is_inline());

    // Free the words that took a heap payload.
    for mut w in [W8::encode_bytes(b"12345678"), W8::encode_int(i64::MAX)] {
        unsafe { w.free() };
    }
    let mut w = W16::encode_bytes(b"1234567890123456");
    unsafe { w.free() };
}

#[test]
fn words_round_trip_inline_and_out_of_line() {
    let mut buf: InlineBuf = [0; 16];
    let short = W8::encode_bytes(b"abc");
    assert_eq!(unsafe { short.decode(&mut buf) }, Decoded::Bytes(b"abc"));
    let mut long = W8::encode_bytes(b"a much longer key than seven bytes");
    assert_eq!(
        unsafe { long.decode(&mut buf) },
        Decoded::Bytes(b"a much longer key than seven bytes")
    );
    unsafe { long.free() };

    let n = W8::encode_int(-1234567);
    assert_eq!(unsafe { n.decode(&mut buf) }, Decoded::Int(-1234567));
    let wide = W16::encode_int(i64::MIN);
    assert_eq!(unsafe { wide.decode(&mut buf) }, Decoded::Int(i64::MIN));
}

macro_rules! roundtrip_test {
    ($name:ident, $ty:ty) => {
        #[test]
        fn $name() {
            for shape in Shape::ALL {
                let pairs = generate(shape, 5_000);
                let mut t = <$ty>::new();
                for p in &pairs {
                    let v = match p.int {
                        Some(i) => Val::Int(i),
                        None => Val::Bytes(&p.bytes),
                    };
                    assert!(t.insert(&p.key, v), "{} duplicate key", shape.name());
                }
                assert_eq!(t.len(), pairs.len(), "{}", shape.name());

                for p in &pairs {
                    let got = t.get_value(&p.key).expect("key present after insert");
                    match p.int {
                        Some(i) => assert_eq!(
                            got,
                            if <$ty>::VALUE_INLINES_INT {
                                ValueOut::Int(i)
                            } else {
                                ValueOut::Bytes(i.to_le_bytes().to_vec())
                            }
                        ),
                        None => assert_eq!(got, ValueOut::Bytes(p.bytes.clone())),
                    }
                }
                assert!(!t.contains(b"absent-key-never-inserted"));

                // Iteration sees every key exactly once.
                let mut seen = Vec::new();
                t.for_each(|k| seen.push(k.to_vec()));
                assert_eq!(seen.len(), pairs.len());
                seen.sort();
                seen.dedup();
                assert_eq!(seen.len(), pairs.len());

                // Removal empties the table and frees every payload.
                for p in &pairs {
                    assert!(t.remove(&p.key), "{} remove missed", shape.name());
                }
                assert!(t.is_empty());
            }
        }
    };
}

roundtrip_test!(roundtrip_ptr8, TablePtr);
roundtrip_test!(roundtrip_int8, TableInt);
roundtrip_test!(roundtrip_str7, TableStr7);
roundtrip_test!(roundtrip_str15w, TableStr15);
roundtrip_test!(roundtrip_hybrid, TableHybrid);

#[test]
fn overwrite_replaces_the_value_without_growing_the_table() {
    let mut t = TableStr7::new();
    assert!(t.insert(b"k", Val::Bytes(b"first")));
    assert!(!t.insert(
        b"k",
        Val::Bytes(b"a second value well past the inline width")
    ));
    assert_eq!(t.len(), 1);
    assert_eq!(
        t.get_value(b"k"),
        Some(ValueOut::Bytes(
            b"a second value well past the inline width".to_vec()
        ))
    );
    assert!(!t.insert(b"k", Val::Int(42)));
    assert_eq!(t.get_value(b"k"), Some(ValueOut::Int(42)));
}

#[test]
fn stash_buckets_absorb_spills_before_a_split() {
    let mut t = TableStr7::new();
    for i in 0..100_000 {
        t.insert(format!("s:{i}").as_bytes(), Val::Int(i));
    }
    assert!(t.stash_load() > 0.0, "stash buckets never used");
    let (mean_fill, full_share) = t.bucket_fill();
    assert!(mean_fill > 0.0 && full_share <= 1.0);
    assert!(t.stats.splits > 0);
    assert!(t.occupancy() > 0.5, "occupancy {:.3}", t.occupancy());
}

#[test]
fn directory_overhead_stays_under_one_byte_per_entry() {
    let mut t = TableStr7::new();
    for i in 0..500_000 {
        t.insert(format!("d:{i}").as_bytes(), Val::Int(i));
    }
    let per_entry = t.directory_bytes() as f64 / t.len() as f64;
    assert!(
        per_entry < 1.0,
        "directory {per_entry:.4} B/entry, {} entries for {} segments",
        t.directory_entries(),
        t.segments()
    );
}
