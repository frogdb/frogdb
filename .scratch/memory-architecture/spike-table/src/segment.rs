//! Segment, bucket and bucket metadata — the R5 storage unit.
//!
//! Shape is Dragonfly's Dashtable (which is the Dash paper's): a segment is 56
//! regular buckets plus 4 stash buckets; a bucket is a metadata block followed by a
//! fixed slot array; an item lives in its home bucket, the next bucket, or a stash.
//! Deviations from the reference are listed in `spike-report-table.md`.

use crate::word::Word;

/// Regular buckets per segment (Dashtable: 56).
pub const REGULAR_BUCKETS: usize = 56;
/// Stash buckets per segment (Dashtable: 4).
pub const STASH_BUCKETS: usize = 4;
/// Total buckets addressable inside a segment.
pub const BUCKETS: usize = REGULAR_BUCKETS + STASH_BUCKETS;
/// Every bucket is exactly four cache lines, whatever the slot width.
pub const BUCKET_BYTES: usize = 256;
/// Bucket metadata block: one half cache line, ahead of the slots.
pub const META_BYTES: usize = 32;
/// The segment header is exactly one cache line.
pub const HEADER_BYTES: usize = 64;

/// One key word + one value word.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct Slot<K, V> {
    pub key: K,
    pub val: V,
}

/// A bucket: a 32-byte metadata block plus `N` slots, 256 bytes in total.
///
/// `fp` holds one fingerprint byte per slot — the top bits of the key hash — so a
/// lookup rejects 255/256 of the non-matching slots without touching a key word or
/// chasing a pointer. `occupied` is the slot bitmap. `stash_map` records *which* of
/// the segment's four stash buckets received a spill from this home bucket, so a
/// lookup that misses home and neighbour probes only the stashes that can hold it.
#[repr(C)]
pub struct Bucket<K: Word, V: Word, const N: usize> {
    fp: [u8; 14],
    occupied: u16,
    stash_map: u8,
    stash_count: u8,
    _pad: [u8; 14],
    slots: [Slot<K, V>; N],
}

impl<K: Word, V: Word, const N: usize> Bucket<K, V, N> {
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

    #[inline]
    fn free_slot(&self) -> Option<usize> {
        let mask = (!self.occupied) & ((1u16 << N) - 1);
        if mask == 0 {
            None
        } else {
            Some(mask.trailing_zeros() as usize)
        }
    }

    /// Writes an already-encoded slot. Returns false if the bucket is full.
    pub fn insert(&mut self, fp: u8, key: K, val: V) -> bool {
        match self.free_slot() {
            Some(i) => {
                self.fp[i] = fp;
                self.slots[i] = Slot { key, val };
                self.occupied |= 1 << i;
                true
            }
            None => false,
        }
    }

    /// Finds the slot holding `key`, using the fingerprint as a pre-filter.
    pub fn find(&self, fp: u8, key: &[u8]) -> Option<usize> {
        let mut bits = self.occupied;
        while bits != 0 {
            let i = bits.trailing_zeros() as usize;
            bits &= bits - 1;
            if self.fp[i] == fp && unsafe { self.slots[i].key.eq_bytes(key) } {
                return Some(i);
            }
        }
        None
    }

    pub fn slot(&self, i: usize) -> &Slot<K, V> {
        &self.slots[i]
    }

    pub fn slot_mut(&mut self, i: usize) -> &mut Slot<K, V> {
        &mut self.slots[i]
    }

    pub fn clear_slot(&mut self, i: usize) {
        unsafe {
            self.slots[i].key.free();
            self.slots[i].val.free();
        }
        self.occupied &= !(1 << i);
    }

    /// Takes the slot out without freeing its payloads — used by split, which moves
    /// the words to another segment rather than dropping them.
    pub fn take_slot(&mut self, i: usize) -> Slot<K, V> {
        self.occupied &= !(1 << i);
        self.slots[i]
    }

    #[inline]
    pub fn occupied_bits(&self) -> u16 {
        self.occupied
    }

    #[inline]
    pub fn note_stash(&mut self, stash_idx: usize) {
        self.stash_map |= 1 << stash_idx;
        self.stash_count = self.stash_count.saturating_add(1);
    }

    #[inline]
    pub fn forget_stash(&mut self) {
        // Spike shortcut: the count is exact but the bitmap is only cleared when the
        // last spill from this home bucket leaves. A production build would keep a
        // per-stash counter; the effect here is a slightly pessimistic probe count
        // after deletes, which the sweep does not exercise.
        self.stash_count = self.stash_count.saturating_sub(1);
        if self.stash_count == 0 {
            self.stash_map = 0;
        }
    }

    #[inline]
    pub fn stash_map(&self) -> u8 {
        self.stash_map
    }

    #[inline]
    pub fn has_stash(&self) -> bool {
        self.stash_count > 0
    }
}

impl<K: Word, V: Word, const N: usize> Drop for Bucket<K, V, N> {
    fn drop(&mut self) {
        let mut bits = self.occupied;
        while bits != 0 {
            let i = bits.trailing_zeros() as usize;
            bits &= bits - 1;
            unsafe {
                self.slots[i].key.free();
                self.slots[i].val.free();
            }
        }
    }
}

/// Per-segment metadata, exactly one cache line.
///
/// The R9 block is reserved *space*, not behaviour: this spike proves the 2Q
/// eviction state fits in the segment header without pushing it past 64 bytes, and
/// issue 12 designs the policy against this layout. Queue links are segment
/// **indices**, not pointers, so the header stays 64 bytes on any target and a
/// segment vector can move.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct SegmentHeader {
    /// Extendible-hash local depth.
    pub local_depth: u8,
    /// R9: 2Q queue membership — 0 none, 1 A1in, 2 A1out, 3 Am. Upper bits reserved.
    pub q_state: u8,
    /// R9: rotating bucket index, so victim selection inside a segment is O(1).
    pub victim_cursor: u8,
    /// Reserved: split-in-progress / pinned / tiering flags.
    pub flags: u8,
    /// Live entries in this segment.
    pub entries: u16,
    /// Entries whose value word is inline (spike instrumentation).
    pub inline_values: u16,
    /// Low `local_depth` bits of the key hash that route here — the SCAN cursor identity.
    pub segment_key: u64,
    /// R9: intrusive 2Q queue links, segment indices, `u32::MAX` = none.
    pub q_prev: u32,
    pub q_next: u32,
    /// R9: per-segment access counters driving A1in→Am promotion.
    pub hits: u32,
    pub misses: u32,
    /// R9: coarse clock tick of last touch.
    pub last_touch: u32,
    /// R8: bytes this segment has charged to the shard budget.
    pub bytes_charged: u32,
    /// Headroom so R9's design can grow without moving the cache line boundary.
    pub reserved: [u8; 24],
}

/// Bytes of [`SegmentHeader`] reserved for R9's eviction state.
pub const R9_RESERVED_BYTES: usize = 1 /* q_state */ + 1 /* victim_cursor */
    + 4 /* q_prev */ + 4 /* q_next */ + 4 /* hits */ + 4 /* misses */ + 4 /* last_touch */;

/// A segment: one header cache line then `BUCKETS` buckets.
#[repr(C)]
pub struct Segment<K: Word, V: Word, const N: usize> {
    pub header: SegmentHeader,
    pub buckets: [Bucket<K, V, N>; BUCKETS],
}

impl<K: Word, V: Word, const N: usize> Segment<K, V, N> {
    /// Slots a segment can hold — its hard capacity, reached only if every bucket fills.
    pub const CAPACITY: usize = BUCKETS * N;

    /// Allocates a zeroed segment. All-zero is a valid state: every occupancy bitmap
    /// is empty, so no word is ever read.
    pub fn alloc_zeroed(local_depth: u8, segment_key: u64) -> Box<Self> {
        let layout = std::alloc::Layout::new::<Self>();
        unsafe {
            let p = std::alloc::alloc_zeroed(layout) as *mut Self;
            assert!(!p.is_null(), "segment allocation failed");
            let mut seg = Box::from_raw(p);
            seg.header.local_depth = local_depth;
            seg.header.segment_key = segment_key;
            seg.header.q_prev = u32::MAX;
            seg.header.q_next = u32::MAX;
            seg
        }
    }

    pub fn regular(&self) -> &[Bucket<K, V, N>] {
        &self.buckets[..REGULAR_BUCKETS]
    }
}

/// Layout checks: the header is one cache line and a bucket never exceeds four.
pub fn assert_layout<K: Word, V: Word, const N: usize>() {
    use std::mem::size_of;
    assert_eq!(size_of::<SegmentHeader>(), HEADER_BYTES, "segment header");
    assert!(
        size_of::<Bucket<K, V, N>>() <= BUCKET_BYTES,
        "bucket {} B > {} B budget",
        size_of::<Bucket<K, V, N>>(),
        BUCKET_BYTES
    );
    assert_eq!(
        size_of::<Bucket<K, V, N>>(),
        META_BYTES + size_of::<Slot<K, V>>() * N,
        "bucket must be metadata + slots with no tail padding"
    );
}
