//! The bucket: four cache lines of metadata plus slots.
//!
//! ```text
//! offset  size  field
//!      0    13  fp[13]          fingerprint, top 8 bits of the key hash
//!     13     1  _pad            explicit, so the SIMD load reads no padding
//!     14    26  route[13]       low 16 bits of the key hash — the split bit
//!     40     2  occupied        slot bitmap
//!     42     2  probing         set = this slot's home is the previous bucket
//!     44     4  stash_counts    spills from *this* home bucket, per stash bucket
//!     48   208  slots[13]       13 x (8-byte key word + 8-byte value word)
//!    256           total
//! ```
//!
//! Three departures from the spike, each one a follow-up it named:
//!
//! - **`route` is new** (follow-up 2). A split needs bit `local_depth` of every
//!   key's hash. The spike recomputed the hash to get it — 808 rehashes to move
//!   404 entries. Sixteen low hash bits per slot make that bit readable straight
//!   out of metadata, so a split copies slots instead of hashing keys.
//! - **`probing` is new** (follow-up 3). Dash-style displacement needs to know
//!   whether a slot's home is this bucket or the one before it; without that a
//!   relocation would have to rehash, which defeats the point.
//! - **`stash_counts` replaces the spike's single `stash_count`** (spike
//!   deviation 6). A per-stash counter means the stash bitmap is exact after
//!   deletes rather than pessimistic, so a probe never visits a stash that cannot
//!   hold the key.
//!
//! The fingerprint block is matched with one SIMD compare ([`Bucket::fp_matches`],
//! follow-up 1), which is why it sits at offset 0. The load is 16 bytes wide and
//! `fp` is only 13, so three further bytes come back with it: `_pad`, and the two
//! bytes of `route[0]`. Those lanes land on bits 13..16 of the match mask, which
//! `occupied` can never set, so they cannot affect a result. `_pad` is an
//! explicit field rather than compiler padding precisely so that all sixteen of
//! those bytes are initialised — see [`Bucket::fp_matches`] for the argument.

use std::mem::MaybeUninit;

use crate::layout::ROUTE_BITS;
use crate::word::KeyWord;

/// Stash buckets a single home bucket can spill into — the width of
/// `stash_counts`, and of the bitmap [`Bucket::stash_map`] derives from it.
pub const STASH_FANOUT: usize = 4;

/// One key word and one value word.
#[derive(Debug)]
#[repr(C)]
pub struct Slot<V> {
    pub key: KeyWord,
    pub val: V,
}

/// A bucket of `N` slots. See the module docs for the byte layout.
#[repr(C)]
pub struct Bucket<V, const N: usize> {
    fp: [u8; N],
    /// The byte the compiler would otherwise insert to align `route`, made an
    /// explicit field.
    ///
    /// The SIMD fingerprint match loads 16 bytes from offset 0, which for the
    /// production `N = 13` reaches past `fp` and over this byte. Compiler
    /// padding is not guaranteed to hold any particular value — a struct move
    /// need not copy it — so reading it would be reading uninitialised memory.
    /// As a declared field it is zeroed by the segment's `alloc_zeroed` and
    /// copied by any move, like every other field. `fp_matches` asserts at
    /// compile time that it really is the only gap before `route`.
    _pad: u8,
    route: [u16; N],
    occupied: u16,
    probing: u16,
    stash_counts: [u8; STASH_FANOUT],
    slots: [MaybeUninit<Slot<V>>; N],
}

impl<V, const N: usize> Bucket<V, N> {
    /// Live slots.
    #[inline]
    pub fn len(&self) -> u32 {
        self.occupied.count_ones()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.occupied == 0
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.len() as usize == N
    }

    /// The occupancy bitmap, for iteration.
    #[inline]
    pub fn occupied(&self) -> u16 {
        self.occupied
    }

    /// Whether slot `i` holds an item whose home is the *previous* bucket.
    #[inline]
    pub fn is_displaced(&self, i: usize) -> bool {
        self.probing & (1 << i) != 0
    }

    /// Bitmap of the stash buckets this home bucket has live spills in.
    #[inline]
    pub fn stash_map(&self) -> u8 {
        let mut map = 0u8;
        for (s, &count) in self.stash_counts.iter().enumerate() {
            if count > 0 {
                map |= 1 << s;
            }
        }
        map
    }

    /// Records that this home bucket spilled one item into stash `s`.
    #[inline]
    pub fn note_stash(&mut self, s: usize) {
        self.stash_counts[s] = self.stash_counts[s]
            .checked_add(1)
            .expect("more than 255 spills from one home bucket into one stash");
    }

    /// Records that one of this home bucket's spills left stash `s`.
    #[inline]
    pub fn forget_stash(&mut self, s: usize) {
        debug_assert!(self.stash_counts[s] > 0, "stash count underflow");
        self.stash_counts[s] -= 1;
    }

    /// The lowest free slot index, or `None` when the bucket is full.
    #[inline]
    fn free_slot(&self) -> Option<usize> {
        let mask = !self.occupied & slot_mask(N);
        (mask != 0).then(|| mask.trailing_zeros() as usize)
    }

    /// Writes a slot into the first free position.
    ///
    /// `displaced` says whether the item's home is the previous bucket, which is
    /// what lets a later relocation find its alternative without rehashing.
    /// Returns the slot index, or gives the item back when the bucket is full.
    pub fn insert(
        &mut self,
        fp: u8,
        route: u16,
        displaced: bool,
        slot: Slot<V>,
    ) -> Result<usize, Slot<V>> {
        let Some(i) = self.free_slot() else {
            return Err(slot);
        };
        self.fp[i] = fp;
        self.route[i] = route;
        self.slots[i].write(slot);
        self.occupied |= 1 << i;
        if displaced {
            self.probing |= 1 << i;
        } else {
            // Mutation note: this clear is belt and braces, and mutating its
            // shift survives. `free_slot` only ever returns a slot whose
            // occupancy bit is clear, and the only way a bit gets cleared is
            // `take`, which clears the probing bit in the same breath — so the
            // bit being cleared here is already zero on every reachable path and
            // no test can tell a working clear from a broken one. The statement
            // stays because it makes the invariant local: a slot's probing bit
            // is written by whoever writes the slot.
            self.probing &= !(1 << i);
        }
        Ok(i)
    }

    /// Finds the slot holding `key`, filtering on the fingerprint first.
    #[inline]
    pub fn find(&self, fp: u8, key: &[u8]) -> Option<usize> {
        let mut candidates = self.fp_matches(fp);
        while candidates != 0 {
            let i = candidates.trailing_zeros() as usize;
            candidates &= candidates - 1;
            if self.slot(i).key.eq_bytes(key) {
                return Some(i);
            }
        }
        None
    }

    /// Bitmap of occupied slots whose fingerprint is `fp`.
    ///
    /// One 16-byte compare on NEON and SSE2; a byte loop where neither is
    /// available. The result is masked by `occupied`, so lanes past `N` — which
    /// read `_pad` and `route` bytes, not fingerprints — can never appear in it.
    ///
    /// # Why reading past `fp` is sound
    ///
    /// The load covers bytes 0..16 of the bucket. `fp` is `N` bytes, so for the
    /// production `N = 13` the remaining three are `_pad` and `route[0]`. Every
    /// one of them is a declared field, and a declared field is initialised
    /// memory whenever the bucket is: segments come from `alloc_zeroed`
    /// ([`crate::segment::Segment::alloc`]), and a field — unlike compiler
    /// padding — is copied by any move. So there is no uninitialised byte in
    /// the load however the bucket got there.
    ///
    /// `ENOUGH_INITIALISED_BYTES` pins that at compile time for whichever `N`
    /// this is instantiated with, so an `N` that put real padding inside the
    /// first 16 bytes fails the build rather than reading it.
    #[inline]
    pub fn fp_matches(&self, fp: u8) -> u16 {
        self.fp_match_raw(fp) & self.occupied
    }

    /// The first 16 bytes of the bucket are entirely declared fields.
    ///
    /// `fp` occupies `0..N`, `_pad` the single byte at `N`, and `route` runs
    /// from `N + 1`. Both halves matter: the offset check catches an `N` for
    /// which the compiler inserts padding of its own (an even `N` would need a
    /// second byte to align `route`), and the length check catches an `N` too
    /// small for `fp` and `route` together to cover the load.
    const ENOUGH_INITIALISED_BYTES: () = {
        assert!(
            std::mem::offset_of!(Bucket<V, N>, route) == N + 1,
            "`_pad` is not the only gap before `route` — the SIMD load would \
             read compiler padding"
        );
        assert!(
            N + 1 + 2 * N >= 16,
            "`fp`, `_pad` and `route` do not cover the 16 bytes the SIMD load reads"
        );
    };

    /// Mutation note: `fp_match_raw` has three bodies behind `cfg`, and only one
    /// of them is compiled on any given host. A mutation-testing run therefore
    /// reports surviving mutants in the other two every time — the mutated code
    /// is not in the binary the tests run against, so nothing can kill it. This
    /// is a property of the run, not of the tests: the same survivors appear on
    /// x86_64 for the NEON body. `simd_and_scalar_fingerprint_match_agree` pins
    /// whichever body *is* compiled against an independent scalar reference, and
    /// CI running the suite on both architectures is what covers the pair.
    #[cfg(target_arch = "aarch64")]
    #[inline]
    fn fp_match_raw(&self, fp: u8) -> u16 {
        use std::arch::aarch64::{
            uint8x16_t, vandq_u8, vceqq_u8, vdupq_n_u8, vgetq_lane_u64, vld1q_u8, vpaddq_u8,
            vreinterpretq_u64_u8,
        };
        // 16 lanes of 1<<(i%8), so a horizontal add over each half of the
        // equality mask produces one byte of the 16-bit match bitmap.
        const LANE_BITS: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
        const { Self::ENOUGH_INITIALISED_BYTES };
        // SAFETY: NEON is baseline on aarch64, so the intrinsics are always
        // available. Both loads read 16 bytes: one from `LANE_BITS`, one from the
        // start of the bucket, whose first 16 bytes are `fp`, the explicit `_pad`
        // byte, and the two bytes of `route[0]` — all declared fields, so all
        // initialised: a segment is `alloc_zeroed`, and a field (unlike compiler
        // padding) is copied by any move of the bucket. `ENOUGH_INITIALISED_BYTES`
        // above rejects at compile time any `N` for which that is not the layout.
        // The pointer is derived from the whole bucket rather than from `fp` —
        // `fp` is only `N` bytes, so a pointer into it may not be read past its
        // end — and `assert_bucket_layout` pins the bucket at 16 bytes or more.
        // `vld1q_u8` has no alignment requirement.
        unsafe {
            let block: uint8x16_t = vld1q_u8(std::ptr::from_ref(self).cast::<u8>());
            let eq = vceqq_u8(block, vdupq_n_u8(fp));
            let bits = vandq_u8(eq, vld1q_u8(LANE_BITS.as_ptr()));
            // Two rounds of pairwise add fold 16 lanes into 4 bytes; the low two
            // hold the mask's low and high halves.
            let folded = vpaddq_u8(bits, bits);
            let folded = vpaddq_u8(folded, folded);
            let folded = vpaddq_u8(folded, folded);
            vgetq_lane_u64(vreinterpretq_u64_u8(folded), 0) as u16
        }
    }

    #[cfg(all(target_arch = "x86_64", target_feature = "sse2"))]
    #[inline]
    fn fp_match_raw(&self, fp: u8) -> u16 {
        use std::arch::x86_64::{
            _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8, _mm_set1_epi8,
        };
        const { Self::ENOUGH_INITIALISED_BYTES };
        // SAFETY: SSE2 is guaranteed by the `target_feature` gate above (and is
        // baseline on x86_64). `_mm_loadu_si128` is an unaligned 16-byte read from
        // the start of the bucket, whose first 16 bytes are `fp`, the explicit
        // `_pad` byte, and the two bytes of `route[0]` — all declared fields, so
        // all initialised: a segment is `alloc_zeroed`, and a field (unlike
        // compiler padding) is copied by any move of the bucket.
        // `ENOUGH_INITIALISED_BYTES` above rejects at compile time any `N` for
        // which that is not the layout. The pointer is derived from the whole
        // bucket rather than from `fp` — `fp` is only `N` bytes, so a pointer into
        // it may not be read past its end — and `assert_bucket_layout` pins the
        // bucket at 16 bytes or more.
        unsafe {
            let block = _mm_loadu_si128(std::ptr::from_ref(self).cast());
            _mm_movemask_epi8(_mm_cmpeq_epi8(block, _mm_set1_epi8(fp as i8))) as u16
        }
    }

    #[cfg(not(any(
        target_arch = "aarch64",
        all(target_arch = "x86_64", target_feature = "sse2")
    )))]
    #[inline]
    fn fp_match_raw(&self, fp: u8) -> u16 {
        let mut mask = 0u16;
        for (i, &b) in self.fp.iter().enumerate() {
            if b == fp {
                mask |= 1 << i;
            }
        }
        mask
    }

    /// The routing bits stored for slot `i` — the low [`ROUTE_BITS`] of its key
    /// hash.
    #[inline]
    pub fn route(&self, i: usize) -> u16 {
        self.route[i]
    }

    /// The fingerprint stored for slot `i`.
    #[inline]
    pub fn fp(&self, i: usize) -> u8 {
        self.fp[i]
    }

    /// Which half of a split at `depth` slot `i` belongs to, read out of
    /// metadata. `None` past [`ROUTE_BITS`], where the caller must rehash.
    #[inline]
    pub fn split_bit(&self, i: usize, depth: u8) -> Option<bool> {
        (u32::from(depth) < ROUTE_BITS).then(|| (self.route[i] >> depth) & 1 == 1)
    }

    /// Slot `i`. Panics in debug builds when the slot is free.
    #[inline]
    pub fn slot(&self, i: usize) -> &Slot<V> {
        debug_assert!(self.occupied & (1 << i) != 0, "slot {i} is not occupied");
        // SAFETY: the occupancy bit is set, so `insert` initialised this slot and
        // nothing has taken it since.
        unsafe { self.slots[i].assume_init_ref() }
    }

    /// Slot `i`, mutably.
    #[inline]
    pub fn slot_mut(&mut self, i: usize) -> &mut Slot<V> {
        debug_assert!(self.occupied & (1 << i) != 0, "slot {i} is not occupied");
        // SAFETY: as `slot`, and `&mut self` rules out an aliasing `&Slot`.
        unsafe { self.slots[i].assume_init_mut() }
    }

    /// Removes slot `i` and hands back what it held.
    pub fn take(&mut self, i: usize) -> Slot<V> {
        debug_assert!(self.occupied & (1 << i) != 0, "slot {i} is not occupied");
        self.occupied &= !(1 << i);
        self.probing &= !(1 << i);
        // SAFETY: the occupancy bit was set, so the slot was initialised; it is
        // now clear, so this is the only read of that value and no drop will run
        // for it here or in `Bucket::drop`.
        unsafe { self.slots[i].assume_init_read() }
    }
}

impl<V, const N: usize> Drop for Bucket<V, N> {
    fn drop(&mut self) {
        let mut bits = self.occupied;
        while bits != 0 {
            let i = bits.trailing_zeros() as usize;
            bits &= bits - 1;
            // SAFETY: the occupancy bit is set, so the slot is initialised, and
            // each index is visited once because the bit is cleared as we go.
            unsafe { self.slots[i].assume_init_drop() };
        }
    }
}

/// Bitmap with the low `n` bits set.
#[inline]
const fn slot_mask(n: usize) -> u16 {
    ((1u32 << n) - 1) as u16
}

/// Panics unless a `Bucket<V, N>` has the layout the module docs describe.
///
/// Called once per table rather than asserted as a `const`, because `size_of`
/// over a generic parameter is not usable in a `const` item on stable.
pub fn assert_bucket_layout<V, const N: usize>() {
    use std::mem::{align_of, size_of};

    assert!(
        N <= 16,
        "a {N}-slot bucket overflows the 16-bit slot bitmaps"
    );
    assert!(
        size_of::<Bucket<V, N>>() <= crate::layout::BUCKET_BYTES,
        "bucket is {} B, over the {} B budget",
        size_of::<Bucket<V, N>>(),
        crate::layout::BUCKET_BYTES
    );
    assert!(
        size_of::<Bucket<V, N>>() >= 16,
        "the SIMD fingerprint match reads 16 bytes from the start of the bucket"
    );
    assert_eq!(align_of::<Bucket<V, N>>(), 8, "bucket alignment");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::{BUCKET_BYTES, SLOTS_PER_BUCKET};
    use crate::word::ValueWord;

    type ProdBucket = Bucket<ValueWord, SLOTS_PER_BUCKET>;

    fn empty() -> Box<ProdBucket> {
        empty_of::<ValueWord>()
    }

    fn empty_of<V>() -> Box<Bucket<V, SLOTS_PER_BUCKET>> {
        // A zeroed bucket is a valid empty bucket: every bitmap and counter is 0.
        let layout = std::alloc::Layout::new::<Bucket<V, SLOTS_PER_BUCKET>>();
        // SAFETY: the layout is non-zero-sized, and all-zero is a valid
        // `Bucket` — `occupied == 0` means no `MaybeUninit` slot is ever read.
        unsafe {
            let p = std::alloc::alloc_zeroed(layout).cast::<Bucket<V, SLOTS_PER_BUCKET>>();
            assert!(!p.is_null());
            Box::from_raw(p)
        }
    }

    fn slot(key: &[u8], val: i64) -> Slot<ValueWord> {
        Slot {
            key: KeyWord::new(key),
            val: ValueWord::from_int(val),
        }
    }

    /// A value that says when it was dropped, so a leaked slot is a failed
    /// assertion rather than an invisible loss.
    struct CountsItsDrop(std::rc::Rc<std::cell::Cell<usize>>);

    impl Drop for CountsItsDrop {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }

    /// Emptiness, and the probing bit that says an entry is not in its home
    /// bucket. The bit is what `find` uses to decide whether a miss here means
    /// the key is absent or that the probe has to carry on, so it has to be set
    /// only for the slot that was actually displaced and cleared when that slot
    /// is taken — a stale mark turns an absent key into a longer probe, and a
    /// missing one turns a present key into a miss.
    #[test]
    fn the_probing_mark_follows_the_slot_that_earned_it() {
        let mut b = empty();
        assert!(b.is_empty());
        assert_eq!(b.len(), 0);

        let home = b.insert(0x01, 0, false, slot(b"a", 1)).unwrap();
        assert!(!b.is_empty(), "a bucket holding an entry is not empty");
        let displaced = b.insert(0x02, 0, true, slot(b"b", 2)).unwrap();

        assert!(!b.is_displaced(home));
        assert!(
            b.is_displaced(displaced),
            "the displaced insert must mark its own slot"
        );

        b.take(displaced);
        assert!(
            !b.is_displaced(displaced),
            "taking a displaced entry must clear its probing bit, or the slot \
             inherits a mark the next occupant never earned"
        );
        assert!(!b.is_empty());

        b.take(home);
        assert!(
            b.is_empty(),
            "a bucket with every slot taken is empty again"
        );
    }

    /// The bucket owns its live slots and nothing else. `take` hands ownership
    /// back to the caller, so the value it returns must *not* be dropped again
    /// when the bucket goes.
    #[test]
    fn dropping_a_bucket_drops_its_live_slots_exactly_once() {
        let drops = std::rc::Rc::new(std::cell::Cell::new(0usize));
        let mut b = empty_of::<CountsItsDrop>();
        for i in 0..3u8 {
            let s = Slot {
                key: KeyWord::new(b"k"),
                val: CountsItsDrop(std::rc::Rc::clone(&drops)),
            };
            assert!(b.insert(i, 0, false, s).is_ok());
        }

        let taken = b.take(2);
        assert_eq!(drops.get(), 0, "take must not drop the value it hands back");
        drop(taken);
        assert_eq!(drops.get(), 1);

        drop(b);
        assert_eq!(
            drops.get(),
            3,
            "the two slots still live must be dropped with the bucket, and the \
             taken one must not be dropped twice"
        );
    }

    /// The layout assertion is the whole reason the SIMD load is sound, so it
    /// has to actually refuse a bucket that breaks it.
    #[test]
    fn a_bucket_that_breaks_the_layout_contract_is_refused() {
        let hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let too_many_slots = std::panic::catch_unwind(assert_bucket_layout::<ValueWord, 17>);
        let too_big = std::panic::catch_unwind(assert_bucket_layout::<[u8; 512], SLOTS_PER_BUCKET>);
        std::panic::set_hook(hook);

        assert!(
            too_many_slots.is_err(),
            "17 slots overflow the 16-bit slot bitmaps and must be refused"
        );
        assert!(
            too_big.is_err(),
            "a value that blows the bucket byte budget must be refused"
        );
    }

    #[test]
    fn the_production_bucket_is_exactly_four_cache_lines() {
        assert_bucket_layout::<ValueWord, SLOTS_PER_BUCKET>();
        assert_eq!(
            std::mem::size_of::<ProdBucket>(),
            BUCKET_BYTES,
            "13 slots must fill the bucket exactly, with no tail padding"
        );
        assert_eq!(std::mem::size_of::<Slot<ValueWord>>(), 16);
    }

    #[test]
    fn fingerprint_match_finds_every_slot_with_that_fingerprint() {
        let mut b = empty();
        b.insert(0x5a, 0, false, slot(b"a", 1)).unwrap();
        b.insert(0x11, 0, false, slot(b"b", 2)).unwrap();
        b.insert(0x5a, 0, false, slot(b"c", 3)).unwrap();

        assert_eq!(b.fp_matches(0x5a), 0b101);
        assert_eq!(b.fp_matches(0x11), 0b010);
        assert_eq!(b.fp_matches(0x77), 0);
    }

    /// The SIMD load reads 16 bytes but the bucket only has 13 fingerprints; the
    /// three lanes past the end read `route` bytes, and must never match.
    #[test]
    fn fingerprint_match_ignores_lanes_past_the_slot_count() {
        let mut b = empty();
        // route = 0xFFFF puts 0xFF into the bytes the over-read lands on.
        for i in 0..SLOTS_PER_BUCKET {
            b.insert(
                0xff,
                0xffff,
                false,
                slot(format!("k{i}").as_bytes(), i as i64),
            )
            .unwrap();
        }
        assert_eq!(
            b.fp_matches(0xff).count_ones() as usize,
            SLOTS_PER_BUCKET,
            "match mask must stop at the real slots"
        );
        assert_eq!(b.fp_matches(0xff), super::slot_mask(SLOTS_PER_BUCKET));
    }

    #[test]
    fn simd_and_scalar_fingerprint_match_agree() {
        let mut b = empty();
        for i in 0..SLOTS_PER_BUCKET {
            b.insert(
                i as u8,
                i as u16,
                false,
                slot(format!("k{i}").as_bytes(), 0),
            )
            .unwrap();
        }
        for fp in 0u8..=255 {
            let mut scalar = 0u16;
            for i in 0..SLOTS_PER_BUCKET {
                if b.fp(i) == fp && b.occupied() & (1 << i) != 0 {
                    scalar |= 1 << i;
                }
            }
            assert_eq!(b.fp_matches(fp), scalar, "fingerprint {fp}");
        }
    }

    #[test]
    fn insert_fills_then_returns_the_slot_it_could_not_place() {
        let mut b = empty();
        for i in 0..SLOTS_PER_BUCKET {
            let placed = b
                .insert(1, 0, false, slot(format!("k{i}").as_bytes(), i as i64))
                .expect("bucket has room");
            assert_eq!(placed, i);
        }
        assert!(b.is_full());
        assert!(b.insert(1, 0, false, slot(b"overflow", 0)).is_err());
    }

    #[test]
    fn find_locates_by_key_not_just_fingerprint() {
        let mut b = empty();
        // Same fingerprint, different keys: the filter passes both, the key
        // comparison has to separate them.
        b.insert(9, 0, false, slot(b"alpha-key-long", 1)).unwrap();
        b.insert(9, 0, false, slot(b"beta-key-long", 2)).unwrap();
        assert_eq!(b.find(9, b"beta-key-long"), Some(1));
        assert_eq!(b.find(9, b"alpha-key-long"), Some(0));
        assert_eq!(b.find(9, b"gamma-key-long"), None);
    }

    #[test]
    fn take_frees_the_slot_and_clears_its_displacement_bit() {
        let mut b = empty();
        b.insert(1, 0, true, slot(b"x", 1)).unwrap();
        assert!(b.is_displaced(0));
        let taken = b.take(0);
        assert!(taken.key.eq_bytes(b"x"));
        assert!(b.is_empty());
        assert!(!b.is_displaced(0));
    }

    #[test]
    fn stash_counts_make_the_stash_map_exact_after_deletes() {
        let mut b = empty();
        b.note_stash(1);
        b.note_stash(1);
        b.note_stash(3);
        assert_eq!(b.stash_map(), 0b1010);
        b.forget_stash(1);
        assert_eq!(b.stash_map(), 0b1010, "one spill still lives in stash 1");
        b.forget_stash(1);
        assert_eq!(b.stash_map(), 0b1000, "stash 1 is now empty");
        b.forget_stash(3);
        assert_eq!(b.stash_map(), 0);
    }

    #[test]
    fn split_bit_reads_the_stored_route_and_gives_up_past_its_width() {
        let mut b = empty();
        b.insert(1, 0b0000_0000_0010_1101, false, slot(b"x", 1))
            .unwrap();
        assert_eq!(b.split_bit(0, 0), Some(true));
        assert_eq!(b.split_bit(0, 1), Some(false));
        assert_eq!(b.split_bit(0, 2), Some(true));
        assert_eq!(b.split_bit(0, 5), Some(true));
        assert_eq!(b.split_bit(0, 6), Some(false));
        assert_eq!(b.split_bit(0, 15), Some(false));
        assert_eq!(b.split_bit(0, 16), None, "past the stored width");
    }

    #[test]
    fn dropping_a_bucket_releases_every_live_slot() {
        // Out-of-line keys and values, so a missed drop is a leak miri catches.
        let mut b = empty();
        for i in 0..SLOTS_PER_BUCKET {
            b.insert(
                1,
                0,
                false,
                Slot {
                    key: KeyWord::new(format!("a-long-key-{i}").as_bytes()),
                    val: ValueWord::from_bytes(b"a value too long to inline"),
                },
            )
            .unwrap();
        }
        drop(b);
    }
}
