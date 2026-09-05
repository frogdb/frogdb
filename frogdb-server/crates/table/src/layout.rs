//! Segment geometry, derived from the allocator's size class.
//!
//! The spike sized a segment at 60 buckets (15 424 B) and paid for a 16 384 B
//! jemalloc class anyway — 960 B, 6.2 % of the whole structural cost, bought and
//! never used ([spike follow-up 5]). This module does that arithmetic the other
//! way round: pick the size class first, then fit as many buckets as it holds.
//!
//! ```text
//! class            16 384 B   jemalloc's class above 14 336
//! header               64 B   one cache line
//! buckets   (16384-64)/256 = 63.75  ->  63 buckets
//! segment      64 + 63*256 = 16 192 B   (192 B of class slack left over)
//! ```
//!
//! 63 buckets split 59 regular + 4 stash, keeping Dashtable's 14:1 ratio. The
//! numbers are `const`, and the compile-time assertions at the foot of this
//! module pin every relation a `debug_assert`-free hot path relies on.
//!
//! [spike follow-up 5]: `.scratch/memory-architecture/spike-report-table.md`

/// The allocator size class a segment is sized to fill.
///
/// jemalloc's class list around here is 8 192, 10 240, 12 288, 14 336, 16 384.
/// A segment that overshoots by one byte costs a whole 4 KB step, so the
/// geometry below is chosen to land *under* this and as close to it as a whole
/// number of buckets allows.
pub const SEGMENT_CLASS_BYTES: usize = 16_384;

/// Segment header: exactly one cache line, and not one byte more — issue 12's
/// eviction state has to fit the reserved space rather than grow the line.
pub const HEADER_BYTES: usize = 64;

/// A bucket is four cache lines: a metadata block plus the slot array.
pub const BUCKET_BYTES: usize = 256;

/// Buckets a segment holds — the largest count that keeps it inside the class.
pub const BUCKETS: usize = (SEGMENT_CLASS_BYTES - HEADER_BYTES) / BUCKET_BYTES;

/// Stash buckets, which take spill from any home bucket in the segment.
pub const STASH_BUCKETS: usize = 4;

/// Regular buckets — the ones a key hashes home to.
pub const REGULAR_BUCKETS: usize = BUCKETS - STASH_BUCKETS;

/// Slots per bucket at the production 16-byte slot.
///
/// Metadata is 48 B (see [`crate::bucket::Bucket`]) and 256 − 48 = 208 = 13 × 16.
pub const SLOTS_PER_BUCKET: usize = 13;

/// Slots a segment addresses.
pub const SEGMENT_SLOTS: usize = BUCKETS * SLOTS_PER_BUCKET;

/// Bytes a segment struct occupies.
pub const SEGMENT_BYTES: usize = HEADER_BYTES + BUCKETS * BUCKET_BYTES;

/// Directory depth beyond which a split can no longer read its routing bit out
/// of slot metadata and has to rehash the key.
///
/// A slot stores the low 16 bits of its key hash ([`crate::bucket::Bucket`]'s
/// `route` array), so bit `d` of the hash is readable for `d < 16`. At depth 16
/// a shard holds 65 536 segments — some 48 M live keys — and splits past that
/// point fall back to rehashing. Correctness is unaffected either way; only the
/// split's cost is.
pub const ROUTE_BITS: u32 = 16;

/// Every relation the geometry rests on, checked at compile time: each constant
/// above is concrete, so a bad edit to any of them fails the build rather than
/// silently costing a size class.
const _: () = {
    assert!(
        SEGMENT_BYTES <= SEGMENT_CLASS_BYTES,
        "segment overshoots its allocator size class"
    );
    assert!(
        SEGMENT_BYTES + BUCKET_BYTES > SEGMENT_CLASS_BYTES,
        "another bucket fits inside the size class — this bucket count wastes it"
    );
    assert!(
        REGULAR_BUCKETS > 1,
        "a home bucket needs a distinct neighbour"
    );
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_fills_its_size_class() {
        assert_eq!(BUCKETS, 63);
        assert_eq!(REGULAR_BUCKETS, 59);
        assert_eq!(SEGMENT_BYTES, 16_192);
        // The slack is what the class round-up costs; it must stay under one
        // bucket or the compile-time assertions would have rejected the count.
        assert_eq!(SEGMENT_CLASS_BYTES - SEGMENT_BYTES, 192);
    }

    #[test]
    fn segment_slot_count_matches_the_reported_geometry() {
        assert_eq!(SEGMENT_SLOTS, 63 * 13);
        assert_eq!(SEGMENT_SLOTS, 819);
    }
}
