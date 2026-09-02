//! The segmented extendible-hash table — R5's keyspace shape.
//!
//! Directory of segment indices, one segment per split event, buckets probed
//! home → neighbour → stash. Growth never rehashes the whole table: a split touches
//! exactly one segment and the directory slice that points at it.

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;

use crate::segment::{Bucket, Segment, Slot, BUCKETS, REGULAR_BUCKETS, STASH_BUCKETS};
use crate::word::{Decoded, InlineBuf, Word};

/// A value on its way into the table.
#[derive(Clone, Copy)]
pub enum Val<'a> {
    Int(i64),
    Bytes(&'a [u8]),
}

/// A value read back out.
#[derive(Debug, PartialEq, Eq)]
pub enum ValueOut {
    Int(i64),
    Bytes(Vec<u8>),
}

/// Counters the sweep reports. Probe length is buckets *touched*, not slots.
#[derive(Default, Clone, Copy, Debug)]
pub struct Stats {
    pub lookups: u64,
    pub probe_buckets: u64,
    pub probe_max: u32,
    pub stash_probes: u64,
    pub splits: u64,
    pub dir_doublings: u64,
    pub inline_keys: u64,
    pub inline_values: u64,
}

pub struct Table<K: Word, V: Word, const N: usize> {
    dir: Vec<u32>,
    segs: Vec<Box<Segment<K, V, N>>>,
    global_depth: u8,
    len: usize,
    hasher: RandomState,
    pub stats: Stats,
}

/// Hash split into its three roles: directory routing (low bits), home bucket
/// (middle bits) and fingerprint (top byte). Home bucket comes from bits the
/// directory does not use, so a split never moves an item to a different bucket
/// index — only to a different segment.
#[derive(Clone, Copy)]
struct Route {
    h: u64,
    home: usize,
    fp: u8,
}

impl<K: Word, V: Word, const N: usize> Table<K, V, N> {
    /// Longest key that fits in the key word.
    pub const KEY_INLINE_MAX: usize = K::INLINE_STR_MAX;
    /// Longest byte-string value that fits in the value word.
    pub const VALUE_INLINE_MAX: usize = V::INLINE_STR_MAX;
    /// Whether the value word inlines integers at all.
    pub const VALUE_INLINES_INT: bool = V::INLINE_INT_BITS > 0;
    /// Significant bits an inline integer keeps.
    pub const VALUE_INT_BITS: u32 = V::INLINE_INT_BITS;
    pub const SLOT_BYTES: usize = std::mem::size_of::<Slot<K, V>>();
    pub const SLOTS_PER_BUCKET: usize = N;
    pub const SEGMENT_BYTES: usize = std::mem::size_of::<Segment<K, V, N>>();
    pub const SEGMENT_CAPACITY: usize = Segment::<K, V, N>::CAPACITY;

    pub fn new() -> Self {
        crate::segment::assert_layout::<K, V, N>();
        let seg = Segment::<K, V, N>::alloc_zeroed(0, 0);
        Table {
            dir: vec![0],
            segs: vec![seg],
            global_depth: 0,
            len: 0,
            hasher: RandomState::new(),
            stats: Stats::default(),
        }
    }

    #[inline]
    fn route(&self, key: &[u8]) -> Route {
        let h = self.hasher.hash_one(key);
        Route {
            h,
            home: ((h >> 32) as usize) % REGULAR_BUCKETS,
            fp: (h >> 56) as u8,
        }
    }

    #[inline]
    fn dir_index(&self, h: u64) -> usize {
        (h & ((1u64 << self.global_depth) - 1)) as usize
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn segments(&self) -> usize {
        self.segs.len()
    }

    pub fn directory_entries(&self) -> usize {
        self.dir.len()
    }

    pub fn global_depth(&self) -> u8 {
        self.global_depth
    }

    /// Bytes of table structure: the directory plus every segment. Excludes the
    /// out-of-line key/value payloads, which jemalloc accounts for separately.
    pub fn structural_bytes(&self) -> usize {
        self.dir.len() * std::mem::size_of::<u32>()
            + self.segs.len() * std::mem::size_of::<Segment<K, V, N>>()
    }

    pub fn directory_bytes(&self) -> usize {
        self.dir.len() * std::mem::size_of::<u32>()
    }

    /// Live entries divided by addressable slots — the number bucket capacity buys.
    pub fn occupancy(&self) -> f64 {
        let slots = self.segs.len() * Segment::<K, V, N>::CAPACITY;
        if slots == 0 {
            0.0
        } else {
            self.len as f64 / slots as f64
        }
    }

    // -- read path ----------------------------------------------------------

    /// Returns whether `key` is present, counting the buckets the probe touched.
    pub fn contains(&mut self, key: &[u8]) -> bool {
        self.locate(key).is_some()
    }

    fn locate(&mut self, key: &[u8]) -> Option<(usize, usize, usize)> {
        let r = self.route(key);
        let si = self.dir[self.dir_index(r.h)] as usize;
        let nb = (r.home + 1) % REGULAR_BUCKETS;

        let mut touched = 1u32;
        let mut stash_probes = 0u64;
        let found = {
            let seg = &self.segs[si];
            if let Some(slot) = seg.buckets[r.home].find(r.fp, key) {
                Some((si, r.home, slot))
            } else {
                touched += 1;
                if let Some(slot) = seg.buckets[nb].find(r.fp, key) {
                    Some((si, nb, slot))
                } else {
                    let map = seg.buckets[r.home].stash_map();
                    let mut hit = None;
                    for s in 0..STASH_BUCKETS {
                        if map & (1 << s) == 0 {
                            continue;
                        }
                        touched += 1;
                        stash_probes += 1;
                        let b = REGULAR_BUCKETS + s;
                        if let Some(slot) = seg.buckets[b].find(r.fp, key) {
                            hit = Some((si, b, slot));
                            break;
                        }
                    }
                    hit
                }
            }
        };

        self.stats.lookups += 1;
        self.stats.probe_buckets += touched as u64;
        self.stats.stash_probes += stash_probes;
        self.stats.probe_max = self.stats.probe_max.max(touched);
        found
    }

    /// Copies the value out. Allocates for out-of-line payloads, so this is for
    /// correctness tests; the lookup bench uses [`Table::contains`].
    pub fn get_value(&mut self, key: &[u8]) -> Option<ValueOut> {
        let (si, b, slot) = self.locate(key)?;
        let mut buf: InlineBuf = [0; 16];
        // SAFETY: the slot was located through its occupancy bitmap, so it is live.
        Some(
            match unsafe { self.segs[si].buckets[b].slot(slot).val.decode(&mut buf) } {
                Decoded::Int(v) => ValueOut::Int(v),
                Decoded::Bytes(b) => ValueOut::Bytes(b.to_vec()),
            },
        )
    }

    // -- write path ---------------------------------------------------------

    /// Inserts or overwrites. Returns true when the key was not already present.
    pub fn insert(&mut self, key: &[u8], val: Val<'_>) -> bool {
        if let Some((si, b, slot)) = self.locate(key) {
            let new = encode_val::<V>(val);
            let seg = &mut self.segs[si];
            let was_inline = seg.buckets[b].slot(slot).val.is_inline();
            let old = &mut seg.buckets[b].slot_mut(slot).val;
            unsafe { old.free() };
            *old = new;
            match (was_inline, new.is_inline()) {
                (false, true) => seg.header.inline_values += 1,
                (true, false) => seg.header.inline_values -= 1,
                _ => {}
            }
            return false;
        }
        let r = self.route(key);
        let kw = K::encode_bytes(key);
        let vw = encode_val::<V>(val);
        if kw.is_inline() {
            self.stats.inline_keys += 1;
        }
        if vw.is_inline() {
            self.stats.inline_values += 1;
        }

        let mut guard = 0;
        loop {
            let si = self.dir[self.dir_index(r.h)] as usize;
            if Self::place(&mut self.segs[si], r, kw, vw) {
                self.len += 1;
                return true;
            }
            self.split(si);
            guard += 1;
            assert!(
                guard < 32,
                "segment refuses to relieve pressure after 32 splits"
            );
        }
    }

    /// Home → neighbour (balanced) → stash, exactly Dashtable's placement order.
    fn place(seg: &mut Segment<K, V, N>, r: Route, kw: K, vw: V) -> bool {
        let nb = (r.home + 1) % REGULAR_BUCKETS;
        let home_len = seg.buckets[r.home].len();
        let nb_len = seg.buckets[nb].len();
        let target = if home_len <= nb_len { r.home } else { nb };
        let other = if target == r.home { nb } else { r.home };

        for b in [target, other] {
            if seg.buckets[b].insert(r.fp, kw, vw) {
                seg.header.entries += 1;
                if vw.is_inline() {
                    seg.header.inline_values += 1;
                }
                return true;
            }
        }
        for s in 0..STASH_BUCKETS {
            if seg.buckets[REGULAR_BUCKETS + s].insert(r.fp, kw, vw) {
                seg.buckets[r.home].note_stash(s);
                seg.header.entries += 1;
                if vw.is_inline() {
                    seg.header.inline_values += 1;
                }
                return true;
            }
        }
        false
    }

    pub fn remove(&mut self, key: &[u8]) -> bool {
        let Some((si, b, slot)) = self.locate(key) else {
            return false;
        };
        let r = self.route(key);
        let seg = &mut self.segs[si];
        seg.buckets[b].clear_slot(slot);
        if b >= REGULAR_BUCKETS {
            seg.buckets[r.home].forget_stash();
        }
        seg.header.entries -= 1;
        self.len -= 1;
        true
    }

    // -- growth -------------------------------------------------------------

    /// Splits segment `si` on bit `local_depth` of the key hash, adding exactly one
    /// segment and repointing the directory slice that now belongs to the new half.
    fn split(&mut self, si: usize) {
        let depth = self.segs[si].header.local_depth;
        let old_key = self.segs[si].header.segment_key;
        if depth == self.global_depth {
            let old = self.dir.clone();
            self.dir.extend_from_slice(&old);
            self.global_depth += 1;
            self.stats.dir_doublings += 1;
        }

        let new_key = old_key | (1u64 << depth);
        let mut new_seg = Segment::<K, V, N>::alloc_zeroed(depth + 1, new_key);
        let new_idx = self.segs.len() as u32;
        let hasher = self.hasher.clone();

        // Move every item whose bit `depth` is set. The hash is recomputed from the
        // key word: fingerprints are only 8 bits, so a split has to read the keys.
        let seg = &mut self.segs[si];
        for b in 0..BUCKETS {
            let mut bits = seg.buckets[b].occupied_bits();
            while bits != 0 {
                let i = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                let r = slot_route(&hasher, &seg.buckets[b], i);
                if (r.h >> depth) & 1 == 0 {
                    continue;
                }
                let slot = seg.buckets[b].take_slot(i);
                if b >= REGULAR_BUCKETS {
                    seg.buckets[r.home].forget_stash();
                }
                seg.header.entries -= 1;
                if slot.val.is_inline() {
                    seg.header.inline_values -= 1;
                }
                assert!(
                    Self::place(&mut new_seg, r, slot.key, slot.val),
                    "split target segment overflowed"
                );
            }
        }
        seg.header.local_depth = depth + 1;
        self.segs.push(new_seg);

        let mask = (1u64 << depth) - 1;
        for (i, entry) in self.dir.iter_mut().enumerate() {
            let i = i as u64;
            if i & mask == old_key & mask && (i >> depth) & 1 == 1 {
                *entry = new_idx;
            }
        }
        self.stats.splits += 1;
    }

    // -- iteration ----------------------------------------------------------

    /// Walks every live slot in segment order. Used by the iteration bench; SCAN
    /// uses [`Table::scan`], which is cursor-resumable.
    pub fn for_each(&self, mut f: impl FnMut(&[u8])) {
        for seg in &self.segs {
            emit_segment(seg, &mut f);
        }
    }

    /// One SCAN step: emits every key in the segment the cursor names, and returns
    /// the next cursor (`0` = complete).
    ///
    /// The cursor is a **reverse-binary counter over segment keys**. `cursor & mask`
    /// is the directory index; the advance reverses the bits, increments, and
    /// reverses back at the *scanned segment's local depth*, which does two things at
    /// once: it visits directory entries in reverse-binary order (so a directory that
    /// doubles mid-scan never re-serves a visited entry), and it skips in one step
    /// every other directory entry aliasing the same segment.
    pub fn scan(&self, cursor: u64, out: &mut Vec<Vec<u8>>) -> u64 {
        let idx = (cursor & ((1u64 << self.global_depth) - 1)) as usize;
        let seg = &self.segs[self.dir[idx] as usize];
        emit_segment(seg, &mut |k: &[u8]| out.push(k.to_vec()));

        let mask = (1u64 << seg.header.local_depth) - 1;
        let mut v = cursor | !mask;
        v = v.reverse_bits();
        v = v.wrapping_add(1);
        v.reverse_bits()
    }

    /// The scheme the reverse-binary cursor replaces: walk directory indices in
    /// order. Kept so the proof test can show it failing under mid-scan splits.
    pub fn scan_linear(&self, cursor: u64, out: &mut Vec<Vec<u8>>) -> u64 {
        let idx = cursor as usize;
        if idx >= self.dir.len() {
            return 0;
        }
        let seg = &self.segs[self.dir[idx] as usize];
        emit_segment(seg, &mut |k: &[u8]| out.push(k.to_vec()));
        let next = idx as u64 + 1;
        if next as usize >= self.dir.len() {
            0
        } else {
            next
        }
    }

    /// Bucket-level fill of the regular buckets — the number the stash buckets exist
    /// to raise. Returns (mean fill, share of buckets full).
    pub fn bucket_fill(&self) -> (f64, f64) {
        let mut total = 0u64;
        let mut full = 0u64;
        let mut count = 0u64;
        for seg in &self.segs {
            for b in seg.regular() {
                total += b.len() as u64;
                if b.is_full() {
                    full += 1;
                }
                count += 1;
            }
        }
        if count == 0 {
            return (0.0, 0.0);
        }
        (total as f64 / count as f64, full as f64 / count as f64)
    }

    /// Share of segment slots held in stash buckets.
    pub fn stash_load(&self) -> f64 {
        let mut stashed = 0u64;
        for seg in &self.segs {
            for b in &seg.buckets[REGULAR_BUCKETS..] {
                stashed += b.len() as u64;
            }
        }
        if self.len == 0 {
            0.0
        } else {
            stashed as f64 / self.len as f64
        }
    }
}

/// Recomputes a slot's route from its key word, confining the key borrow to this call.
fn slot_route<K: Word, V: Word, const N: usize>(
    hasher: &RandomState,
    bucket: &Bucket<K, V, N>,
    i: usize,
) -> Route {
    let mut buf: InlineBuf = [0; 16];
    // SAFETY: caller only passes occupied slot indices.
    match unsafe { bucket.slot(i).key.decode(&mut buf) } {
        Decoded::Bytes(k) => route_with(hasher, k),
        Decoded::Int(_) => unreachable!("key words are always byte strings"),
    }
}

fn route_with(hasher: &RandomState, key: &[u8]) -> Route {
    let h = hasher.hash_one(key);
    Route {
        h,
        home: ((h >> 32) as usize) % REGULAR_BUCKETS,
        fp: (h >> 56) as u8,
    }
}

fn encode_val<V: Word>(val: Val<'_>) -> V {
    match val {
        Val::Int(v) => V::encode_int(v),
        Val::Bytes(b) => V::encode_bytes(b),
    }
}

fn emit_segment<K: Word, V: Word, const N: usize>(
    seg: &Segment<K, V, N>,
    f: &mut impl FnMut(&[u8]),
) {
    for b in &seg.buckets {
        let mut bits = b.occupied_bits();
        while bits != 0 {
            let i = bits.trailing_zeros() as usize;
            bits &= bits - 1;
            let mut buf: InlineBuf = [0; 16];
            // SAFETY: slot `i` is occupied, so its key word is live.
            match unsafe { b.slot(i).key.decode(&mut buf) } {
                Decoded::Bytes(k) => f(k),
                Decoded::Int(_) => unreachable!("key words are always byte strings"),
            }
        }
    }
}

impl<K: Word, V: Word, const N: usize> Default for Table<K, V, N> {
    fn default() -> Self {
        Self::new()
    }
}
