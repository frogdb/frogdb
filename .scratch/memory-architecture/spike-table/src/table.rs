//! The segmented extendible-hash table — R5's keyspace shape.
//!
//! Directory of segment indices, one segment per split event, buckets probed
//! home → neighbour → stash. Growth never rehashes the whole table: a split touches
//! exactly one segment and the `dir.len() >> (local_depth + 1)` directory entries
//! that alias it, reached by a strided walk rather than a full directory scan.

use std::hash::BuildHasher;

use crate::segment::{alloc_class, Bucket, Segment, Slot, BUCKETS, REGULAR_BUCKETS, STASH_BUCKETS};
use crate::word::{Decoded, InlineBuf, Word};

/// The hasher both sides of the comparison use.
///
/// This is `griddle::HashMap`'s own default (`hashbrown`'s
/// `BuildHasherDefault<ahash::AHasher>`), deliberately: the baseline cannot be given
/// a different — and several times cheaper — hash function than the prototype, or
/// every timing that contains a hash (insert, lookup, and *all* of the split cost,
/// which is nothing but rehashing) measures the hasher instead of the layout.
pub type Hasher = griddle::hash_map::DefaultHashBuilder;

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
///
/// The probe counters are **not** maintained by the timed read path: [`Table::contains`]
/// monomorphises to a version with every counter update compiled out, and the
/// probe-length table comes from [`Table::contains_counted`]. Charging four counter
/// updates (one of them a `max`) to every lookup while the baseline's `contains_key`
/// pays nothing would put the instrumentation inside the number being compared.
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
    /// Slots a split read and rehashed (every occupied slot in the segment).
    pub split_scanned: u64,
    /// Slots a split actually moved to the new segment (about half of the above).
    pub split_moved: u64,
    /// Directory entries a split rewrote.
    pub dir_writes: u64,
}

/// What one probe touched, when counting is switched on.
#[derive(Default, Clone, Copy)]
struct ProbeCount {
    buckets: u32,
    stash: u64,
}

pub struct Table<K: Word, V: Word, const N: usize, S = Hasher> {
    dir: Vec<u32>,
    segs: Vec<Box<Segment<K, V, N>>>,
    global_depth: u8,
    len: usize,
    hasher: S,
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

impl<K: Word, V: Word, const N: usize, S: BuildHasher + Default + Clone> Table<K, V, N, S> {
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
        Self::with_hasher(S::default())
    }

    /// Same table with a caller-supplied hasher — the tests use a fixed-seed one so
    /// the cursor proof is reproducible run to run.
    pub fn with_hasher(hasher: S) -> Self {
        crate::segment::assert_layout::<K, V, N>();
        let seg = Segment::<K, V, N>::alloc_zeroed(0, 0);
        Table {
            dir: vec![0],
            segs: vec![seg],
            global_depth: 0,
            len: 0,
            hasher,
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

    /// `(segment_key, local_depth)` of the segment directory entry `idx` points at.
    /// Introspection for the directory-invariant test.
    pub fn dir_segment(&self, idx: usize) -> (u64, u8) {
        let seg = &self.segs[self.dir[idx] as usize];
        (seg.header.segment_key, seg.header.local_depth)
    }

    /// Bytes of table structure: the directory plus every segment, **as the allocator
    /// serves them**. Excludes the out-of-line key/value payloads, which jemalloc
    /// accounts for separately.
    ///
    /// Segments are charged at their jemalloc size class, not `size_of`: the class
    /// round-up is memory the process really holds, and charging `size_of` understates
    /// the structure by however far the segment sits below its class boundary.
    pub fn structural_bytes(&self) -> usize {
        self.directory_bytes() + self.segs.len() * Segment::<K, V, N>::alloc_bytes()
    }

    /// Structure charged at `size_of` instead — kept only so the sweep can print the
    /// gap between the struct and the class it lands in.
    pub fn structural_bytes_unrounded(&self) -> usize {
        self.dir.len() * std::mem::size_of::<u32>()
            + self.segs.len() * std::mem::size_of::<Segment<K, V, N>>()
    }

    pub fn directory_bytes(&self) -> usize {
        alloc_class(
            self.dir.capacity() * std::mem::size_of::<u32>(),
            std::mem::align_of::<u32>(),
        )
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

    /// Returns whether `key` is present. **Uninstrumented** — this is the method the
    /// lookup bench times, so it has to cost exactly what the read path costs.
    pub fn contains(&self, key: &[u8]) -> bool {
        let mut c = ProbeCount::default();
        self.probe::<false>(key, &mut c).is_some()
    }

    /// Same lookup with the probe counters on, feeding [`Stats`]. The sweep uses this
    /// for the probe-length columns and to price the instrumentation itself.
    pub fn contains_counted(&mut self, key: &[u8]) -> bool {
        let mut c = ProbeCount::default();
        let found = self.probe::<true>(key, &mut c).is_some();
        self.stats.lookups += 1;
        self.stats.probe_buckets += c.buckets as u64;
        self.stats.stash_probes += c.stash;
        self.stats.probe_max = self.stats.probe_max.max(c.buckets);
        found
    }

    fn locate(&self, key: &[u8]) -> Option<(usize, usize, usize)> {
        let mut c = ProbeCount::default();
        self.probe::<false>(key, &mut c)
    }

    /// Home → neighbour → the stashes this home bucket actually spilled into.
    ///
    /// `COUNT` is a const generic, not a runtime flag: with `COUNT = false` the
    /// counter arithmetic is dead code before the optimiser ever sees it.
    #[inline]
    fn probe<const COUNT: bool>(
        &self,
        key: &[u8],
        c: &mut ProbeCount,
    ) -> Option<(usize, usize, usize)> {
        let r = self.route(key);
        let si = self.dir[self.dir_index(r.h)] as usize;
        let nb = (r.home + 1) % REGULAR_BUCKETS;
        if COUNT {
            c.buckets = 1;
        }

        let seg = &self.segs[si];
        if let Some(slot) = seg.buckets[r.home].find(r.fp, key) {
            return Some((si, r.home, slot));
        }
        if COUNT {
            c.buckets += 1;
        }
        if let Some(slot) = seg.buckets[nb].find(r.fp, key) {
            return Some((si, nb, slot));
        }
        let map = seg.buckets[r.home].stash_map();
        for s in 0..STASH_BUCKETS {
            if map & (1 << s) == 0 {
                continue;
            }
            if COUNT {
                c.buckets += 1;
                c.stash += 1;
            }
            let b = REGULAR_BUCKETS + s;
            if let Some(slot) = seg.buckets[b].find(r.fp, key) {
                return Some((si, b, slot));
            }
        }
        None
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
    /// segment and repointing the directory entries that now belong to the new half.
    ///
    /// Two costs worth keeping apart, because the report quotes both: the split
    /// **reads and rehashes every occupied slot in the segment** but **moves only the
    /// half whose bit `depth` is set**, and it rewrites only the
    /// `dir.len() >> (depth + 1)` directory entries that alias the old segment,
    /// reached by striding rather than by scanning the whole directory.
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
        let mut scanned = 0u64;
        let mut moved = 0u64;
        for b in 0..BUCKETS {
            let mut bits = seg.buckets[b].occupied_bits();
            while bits != 0 {
                let i = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                scanned += 1;
                let r = slot_route(&hasher, &seg.buckets[b], i);
                if (r.h >> depth) & 1 == 0 {
                    continue;
                }
                moved += 1;
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

        // The directory entries aliasing the old segment are exactly
        // `(old_key & mask) + 2^depth + k * 2^(depth+1)`; stride over them instead of
        // testing all `2^global_depth` entries (2,048 of them at 1 M entries).
        let mask = (1usize << depth) - 1;
        let stride = 1usize << (depth + 1);
        let mut i = ((old_key as usize) & mask) | (1usize << depth);
        let mut writes = 0u64;
        while i < self.dir.len() {
            self.dir[i] = new_idx;
            writes += 1;
            i += stride;
        }
        self.stats.splits += 1;
        self.stats.split_scanned += scanned;
        self.stats.split_moved += moved;
        self.stats.dir_writes += writes;
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
fn slot_route<K: Word, V: Word, const N: usize, S: BuildHasher>(
    hasher: &S,
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

fn route_with<S: BuildHasher>(hasher: &S, key: &[u8]) -> Route {
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

impl<K: Word, V: Word, const N: usize, S: BuildHasher + Default + Clone> Default
    for Table<K, V, N, S>
{
    fn default() -> Self {
        Self::new()
    }
}
