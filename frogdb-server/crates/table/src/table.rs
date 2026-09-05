//! The table: a directory of segments, and the cursor that walks it.
//!
//! Growth is extendible hashing. The directory is `2^global_depth` entries wide
//! and each entry names a segment; a segment covering more than one entry has a
//! `local_depth` below the global one. When a segment cannot take an insert, that
//! one segment splits — 16 KB of work — and the directory doubles only when the
//! segment being split was already at global depth. Nothing rehashes the whole
//! keyspace, which is the stall this structure exists to remove.
//!
//! # SCAN
//!
//! [`Table::scan`] returns whole segments and advances the cursor in
//! reverse-binary order **at the local depth of the segment it just scanned**.
//! That is Redis's rule and it is what makes the guarantee hold across a split:
//! when a segment at local depth `d` later splits into two at `d + 1`, the two
//! halves share the low `d` cursor bits, so a cursor that has already passed that
//! prefix has passed both halves, and one that has not will reach both.
//!
//! The *local* depth is doing real work there. A segment at local depth `d` is
//! reachable from `2^(global - d)` directory entries, so a cursor that walked
//! entries one at a time would return that segment once per alias and lose the
//! exactly-once property on a quiet keyspace — see
//! `stepping_one_directory_entry_at_a_time_returns_keys_over_and_over` in this
//! module's tests.
//!
//! A step returns at least one whole segment even when `count` is smaller, so a
//! reply can overshoot `count` by up to one segment. Redis overshoots for the
//! same reason: the unit that can be scanned atomically is the unit the cursor
//! addresses.

use crate::bucket::Slot;
use crate::hasher::{TableHasher, TableSeed, fingerprint, route};
use crate::layout::{SEGMENT_CLASS_BYTES, SLOTS_PER_BUCKET};
use crate::segment::{Displaced, Segment};
use crate::word::KeyWord;

/// Counters the split-stall and directory-write measurements read.
///
/// Always on: they are touched once per split, never on the lookup path, so
/// there is nothing to gate and nothing that can drift between a measured build
/// and a shipped one.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TableStats {
    /// Segment splits performed.
    pub splits: u64,
    /// Directory doublings.
    pub doublings: u64,
    /// Slots examined by splits.
    pub split_scanned: u64,
    /// Entries moved by splits.
    pub split_moved: u64,
    /// Entries a split had to hash because the directory outgrew the stored
    /// route width. Zero for any table under 2^16 segments.
    pub split_rehashed: u64,
    /// Directory entries written by splits and doublings.
    pub dir_writes: u64,
    /// Entries a split could not place in its target and had to re-insert.
    pub split_leftovers: u64,
}

/// A segmented extendible-hash table from byte-string keys to `V`.
///
/// `N` is slots per bucket. It is a parameter rather than a constant because the
/// per-entry cost of the layout depends on how wide a slot is, and the only
/// honest way to compare two slot widths is to build the table both ways and
/// measure — see `tests/layout_cost.rs`.
pub struct Table<V, const N: usize = SLOTS_PER_BUCKET> {
    /// `2^global_depth` entries, each the index of the segment serving it.
    directory: Vec<u32>,
    /// Every live segment. Segments are never freed: the table does not merge,
    /// exactly as Redis's hash table does not shrink under deletion.
    segments: Vec<Box<Segment<V, N>>>,
    global_depth: u8,
    len: usize,
    hasher: TableHasher,
    stats: TableStats,
}

impl<V, const N: usize> Table<V, N> {
    /// An empty table with a fresh random seed. The production constructor.
    pub fn new() -> Table<V, N> {
        Table::with_seed(TableSeed::from_entropy())
    }

    /// An empty table with a caller-chosen seed, so a sim or a fuzz replay puts
    /// the same key in the same bucket on every run.
    pub fn with_seed(seed: TableSeed) -> Table<V, N> {
        Table {
            directory: vec![0],
            segments: vec![Segment::alloc(0)],
            global_depth: 0,
            len: 0,
            hasher: TableHasher::new(seed),
            stats: TableStats::default(),
        }
    }

    /// The seed this table hashes with.
    pub fn seed(&self) -> TableSeed {
        self.hasher.seed()
    }

    /// Live entries.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Directory bits currently in use.
    pub fn global_depth(&self) -> u8 {
        self.global_depth
    }

    /// Live segments.
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Split and directory counters.
    pub fn stats(&self) -> TableStats {
        self.stats
    }

    /// Bytes the structure itself costs: segments at their allocator size class
    /// plus the directory.
    ///
    /// Keys and values are *not* counted. They are the caller's to charge —
    /// the store seam already tracks entry sizes and would otherwise count them
    /// twice — and [`Table::entry_heap_bytes`] is there when a caller wants them.
    pub fn structural_bytes(&self) -> usize {
        self.segments.len() * SEGMENT_CLASS_BYTES
            + self.directory.capacity() * std::mem::size_of::<u32>()
            + self.segments.capacity() * std::mem::size_of::<Box<Segment<V, N>>>()
    }

    /// Structural bytes per live entry — the figure the spike reported as 33.6
    /// for `str7`, and the one a size-class change has to move.
    pub fn structural_bytes_per_entry(&self) -> f64 {
        if self.len == 0 {
            return f64::INFINITY;
        }
        self.structural_bytes() as f64 / self.len as f64
    }

    /// Live entries as a fraction of the slots the segments address.
    pub fn occupancy(&self) -> f64 {
        let slots = self.segments.len() * crate::layout::SEGMENT_SLOTS;
        if slots == 0 {
            return 0.0;
        }
        self.len as f64 / slots as f64
    }

    #[inline]
    fn dir_index(&self, hash: u64) -> usize {
        (hash as usize) & ((1usize << self.global_depth) - 1)
    }

    /// The value stored under `key`.
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<&V> {
        let hash = self.hasher.hash(key);
        let seg = &self.segments[self.directory[self.dir_index(hash)] as usize];
        seg.get(fingerprint(hash), route(hash), key)
    }

    /// The value stored under `key`, mutably.
    #[inline]
    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut V> {
        let hash = self.hasher.hash(key);
        let si = self.directory[self.dir_index(hash)] as usize;
        self.segments[si].get_mut(fingerprint(hash), route(hash), key)
    }

    /// Whether `key` is present.
    #[inline]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    /// Inserts or replaces, returning the value that was there.
    pub fn insert(&mut self, key: &[u8], value: V) -> Option<V> {
        let hash = self.hasher.hash(key);
        let (fp, r) = (fingerprint(hash), route(hash));

        let si = self.directory[self.dir_index(hash)] as usize;
        if let Some(existing) = self.segments[si].get_mut(fp, r, key) {
            return Some(std::mem::replace(existing, value));
        }

        let mut slot = Slot {
            key: KeyWord::new(key),
            val: value,
        };
        loop {
            let di = self.dir_index(hash);
            let si = self.directory[di] as usize;
            match self.segments[si].insert(fp, r, slot) {
                Ok(()) => {
                    self.len += 1;
                    return None;
                }
                Err(given_back) => {
                    slot = given_back;
                    self.split(di);
                }
            }
        }
    }

    /// Removes `key`, returning the value it held.
    pub fn remove(&mut self, key: &[u8]) -> Option<V> {
        let hash = self.hasher.hash(key);
        let si = self.directory[self.dir_index(hash)] as usize;
        let slot = self.segments[si].remove(fingerprint(hash), route(hash), key)?;
        self.len -= 1;
        Some(slot.val)
    }

    /// Drops every entry, keeping the seed so behaviour stays reproducible.
    pub fn clear(&mut self) {
        self.directory = vec![0];
        self.segments = vec![Segment::alloc(0)];
        self.global_depth = 0;
        self.len = 0;
    }

    /// Splits the segment serving directory entry `di`.
    fn split(&mut self, di: usize) {
        let si = self.directory[di] as usize;
        let depth = self.segments[si].local_depth();
        if depth == self.global_depth {
            self.double_directory();
        }

        // The hasher is cloned rather than borrowed because the split holds the
        // segment array mutably. It is four words; the alternative is threading a
        // borrow through a path that runs once per 16 KB of growth.
        let hasher = self.hasher.clone();
        let mut high = Segment::alloc(depth + 1);
        let mut leftovers: Vec<Displaced<V>> = Vec::new();
        let stats = self.segments[si].split(
            &mut high,
            depth,
            |k| hasher.hash(k),
            |slot: &Slot<V>| {
                let mut buf = [0u8; 16];
                slot.key.bytes(&mut buf).to_vec()
            },
            &mut leftovers,
        );

        let high_index = u32::try_from(self.segments.len()).expect("more than 4 G segments");
        self.segments.push(high);

        // Every directory entry that agrees with `di` in the low `depth` bits and
        // has bit `depth` set now belongs to the new half. They are strided, not
        // contiguous: the routing bits are the *low* bits of the hash.
        let stride = 1usize << (depth + 1);
        let mut e = (di & ((1usize << depth) - 1)) | (1usize << depth);
        while e < self.directory.len() {
            self.directory[e] = high_index;
            self.stats.dir_writes += 1;
            e += stride;
        }

        self.stats.splits += 1;
        self.stats.split_scanned += u64::from(stats.scanned);
        self.stats.split_moved += u64::from(stats.moved);
        self.stats.split_rehashed += u64::from(stats.rehashed);

        // A target that filled up mid-split leaves entries with nowhere legal to
        // live. Re-inserting them goes through the ordinary path, which splits
        // again if that is what it takes.
        self.stats.split_leftovers += leftovers.len() as u64;
        for item in leftovers {
            self.place(item);
        }
    }

    /// Re-inserts an entry that already belongs to the table, splitting as needed.
    fn place(&mut self, item: Displaced<V>) {
        let Displaced {
            fp,
            route,
            mut slot,
        } = item;
        loop {
            // The directory index is the low `global_depth` bits of the hash, and
            // `route` holds the low 16 — enough while the directory is under
            // 2^16 entries, which `double_directory` refuses to exceed.
            let di = (route as usize) & ((1usize << self.global_depth) - 1);
            let si = self.directory[di] as usize;
            match self.segments[si].insert(fp, route, slot) {
                Ok(()) => return,
                Err(given_back) => {
                    slot = given_back;
                    self.split(di);
                }
            }
        }
    }

    fn double_directory(&mut self) {
        assert!(
            u32::from(self.global_depth) < crate::layout::ROUTE_BITS,
            "directory depth {} would outgrow the stored route width",
            self.global_depth + 1
        );
        let old = self.directory.len();
        self.directory.reserve_exact(old);
        for i in 0..old {
            self.directory.push(self.directory[i]);
        }
        self.global_depth += 1;
        self.stats.doublings += 1;
        self.stats.dir_writes += old as u64;
    }

    /// Every live entry, in no particular order.
    pub fn iter(&self) -> impl Iterator<Item = &Slot<V>> + '_ {
        // Over the segments, not the directory: several directory entries can
        // name the same segment, and walking the directory would return its
        // entries once per entry that points at it.
        self.segments
            .iter()
            .flat_map(|seg| seg.positions().map(move |(b, i)| seg.slot_at(b, i)))
    }

    /// Bytes the keys and values hold outside their slot words. O(n).
    pub fn entry_heap_bytes(&self) -> usize {
        self.iter().map(|s| s.key.heap_bytes()).sum()
    }

    /// One SCAN step. Feeds `visit` every entry of at least one whole segment and
    /// returns the next cursor, `0` when the walk is complete.
    ///
    /// `count` is a floor on how many entries a step tries to produce, not a cap:
    /// a step never stops mid-segment, because a partly-scanned segment is not
    /// something a cursor can name across a split.
    pub fn scan(&self, cursor: u64, count: usize, mut visit: impl FnMut(&Slot<V>)) -> u64 {
        let mut cursor = cursor;
        let mut produced = 0usize;
        loop {
            let di = (cursor as usize) & ((1usize << self.global_depth) - 1);
            let seg = &self.segments[self.directory[di] as usize];
            for (b, i) in seg.positions() {
                visit(seg.slot_at(b, i));
                produced += 1;
            }
            cursor = next_cursor(cursor, seg.local_depth());
            if cursor == 0 || produced >= count {
                return cursor;
            }
        }
    }
}

impl<V, const N: usize> Default for Table<V, N> {
    fn default() -> Table<V, N> {
        Table::new()
    }
}

impl<V, const N: usize> std::fmt::Debug for Table<V, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Table")
            .field("len", &self.len)
            .field("global_depth", &self.global_depth)
            .field("segments", &self.segments.len())
            .field("occupancy", &self.occupancy())
            .finish()
    }
}

/// Advances a SCAN cursor in reverse-binary order at `local_depth`.
///
/// Redis's algorithm, and the reason it is `local_depth` and not the global one:
/// the bits above `local_depth` are the ones a future split will start using, so
/// forcing them to 1 before the increment makes the carry propagate *out* of the
/// segment. A cursor that has visited a segment has therefore visited every
/// directory entry that segment will ever be reachable from, split or no split.
///
/// Returns `0` when the walk is complete.
#[inline]
pub fn next_cursor(cursor: u64, local_depth: u8) -> u64 {
    let mask = (1u64 << local_depth) - 1;
    let v = cursor | !mask;
    v.reverse_bits().wrapping_add(1).reverse_bits()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::word::{Decoded, ValueWord};
    use std::collections::{HashMap, HashSet};

    type T = Table<ValueWord>;

    fn table() -> T {
        Table::with_seed(TableSeed::from_u64(2024))
    }

    fn key_of(slot: &Slot<ValueWord>) -> Vec<u8> {
        let mut buf = [0u8; 16];
        slot.key.bytes(&mut buf).to_vec()
    }

    fn fill(t: &mut T, n: usize) -> Vec<String> {
        let keys: Vec<String> = (0..n).map(|i| format!("key:{i}")).collect();
        for (i, k) in keys.iter().enumerate() {
            t.insert(k.as_bytes(), ValueWord::from_int(i as i64));
        }
        keys
    }

    #[test]
    fn an_empty_table_answers_nothing() {
        let t = table();
        assert!(t.is_empty());
        assert!(t.get(b"nothing").is_none());
        assert_eq!(t.global_depth(), 0);
        assert_eq!(t.segment_count(), 1);
    }

    #[test]
    fn round_trips_a_large_keyspace_across_many_splits() {
        let mut t = table();
        let keys = fill(&mut t, 100_000);
        assert_eq!(t.len(), keys.len());
        assert!(t.stats().splits > 100, "expected many splits");

        for (i, k) in keys.iter().enumerate() {
            let mut buf = [0u8; 16];
            let got = t
                .get(k.as_bytes())
                .unwrap_or_else(|| panic!("{k} went missing"));
            assert_eq!(got.decode(&mut buf), Decoded::Int(i as i64));
        }
        assert!(t.get(b"key:100000").is_none());
    }

    #[test]
    fn insert_replaces_and_hands_back_the_old_value() {
        let mut t = table();
        assert!(t.insert(b"k", ValueWord::from_int(1)).is_none());
        let old = t.insert(b"k", ValueWord::from_int(2)).expect("replaced");
        let mut buf = [0u8; 16];
        assert_eq!(old.decode(&mut buf), Decoded::Int(1));
        assert_eq!(t.len(), 1);
        let mut buf = [0u8; 16];
        assert_eq!(t.get(b"k").unwrap().decode(&mut buf), Decoded::Int(2));
    }

    #[test]
    fn remove_takes_the_entry_and_leaves_the_rest() {
        let mut t = table();
        let keys = fill(&mut t, 20_000);
        for k in keys.iter().step_by(2) {
            assert!(t.remove(k.as_bytes()).is_some(), "{k}");
        }
        assert_eq!(t.len(), 10_000);
        for (i, k) in keys.iter().enumerate() {
            assert_eq!(t.contains_key(k.as_bytes()), i % 2 == 1, "{k}");
        }
        for k in keys.iter().step_by(2) {
            assert!(t.remove(k.as_bytes()).is_none(), "{k} removed twice");
        }
    }

    /// Splits must not rehash. This is the whole point of storing `route`.
    #[test]
    fn growth_never_rehashes_a_key() {
        let mut t = table();
        fill(&mut t, 200_000);
        let s = t.stats();
        assert_eq!(s.split_rehashed, 0, "a split hashed a key");
        assert!(s.splits > 200);
        assert_eq!(
            s.split_leftovers, 0,
            "a split target overflowed, which should be vanishingly rare"
        );
    }

    /// Occupancy across the whole table, which is what the per-entry structural
    /// cost is computed from.
    #[test]
    fn the_table_holds_its_target_occupancy_at_scale() {
        let mut t = table();
        fill(&mut t, 200_000);
        assert!(
            t.occupancy() >= 0.7,
            "table occupancy {:.3} is too low",
            t.occupancy()
        );
    }

    #[test]
    fn iter_visits_every_entry_exactly_once() {
        let mut t = table();
        let keys = fill(&mut t, 50_000);
        let mut seen = HashSet::new();
        for slot in t.iter() {
            assert!(seen.insert(key_of(slot)), "iter repeated an entry");
        }
        assert_eq!(seen.len(), keys.len());
    }

    #[test]
    fn clear_empties_the_table_but_keeps_the_seed() {
        let mut t = table();
        let seed = t.seed();
        fill(&mut t, 5_000);
        t.clear();
        assert!(t.is_empty());
        assert_eq!(t.seed(), seed);
        assert_eq!(t.global_depth(), 0);
        assert!(t.get(b"key:1").is_none());
    }

    // ----- SCAN -------------------------------------------------------------

    /// A full scan of a table nobody is touching sees everything, once.
    #[test]
    fn a_quiet_scan_returns_every_key_exactly_once() {
        let mut t = table();
        let keys = fill(&mut t, 60_000);

        let mut seen: HashMap<Vec<u8>, u32> = HashMap::new();
        let mut cursor = 0u64;
        let mut steps = 0;
        loop {
            cursor = t.scan(cursor, 100, |slot| {
                *seen.entry(key_of(slot)).or_default() += 1;
            });
            steps += 1;
            assert!(steps < 100_000, "scan did not terminate");
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(seen.len(), keys.len());
        assert!(
            seen.values().all(|&c| c == 1),
            "a quiet scan returned a key twice"
        );
    }

    /// The guarantee that matters: a key present for the whole scan is returned,
    /// however many splits happen underneath the cursor.
    #[test]
    fn a_scan_under_churn_still_returns_every_stable_key() {
        let mut t = table();
        let stable = fill(&mut t, 30_000);

        let mut seen: HashSet<Vec<u8>> = HashSet::new();
        let mut cursor = 0u64;
        let mut churn = 0i64;
        loop {
            cursor = t.scan(cursor, 200, |slot| {
                seen.insert(key_of(slot));
            });
            // Force splits mid-scan by growing the table hard between steps.
            for _ in 0..500 {
                churn += 1;
                t.insert(
                    format!("churn:{churn}").as_bytes(),
                    ValueWord::from_int(churn),
                );
            }
            if cursor == 0 {
                break;
            }
        }
        assert!(t.stats().splits > 50, "the churn did not cause splits");
        for k in &stable {
            assert!(
                seen.contains(k.as_bytes()),
                "{k} was present throughout and was never returned"
            );
        }
    }

    /// How much a churn schedule grows the table between scan steps. Growth stops
    /// after `CHURN_STEPS` so a linear walk can finish; without that bound it
    /// never would, which is itself part of the point.
    const CHURN_PER_STEP: i64 = 2_000;
    const CHURN_STEPS: usize = 30;

    fn churn(t: &mut T, step: usize, next: &mut i64) {
        if step >= CHURN_STEPS {
            return;
        }
        for _ in 0..CHURN_PER_STEP {
            *next += 1;
            let k = format!("churn:{next}");
            t.insert(k.as_bytes(), ValueWord::from_int(*next));
        }
    }

    /// The counter-example that justifies advancing at the scanned segment's
    /// *local* depth.
    ///
    /// A directory entry is not the unit of storage — a segment at local depth
    /// `d` is reachable from `2^(global - d)` entries. A cursor that steps one
    /// directory entry at a time therefore returns that segment's entries once
    /// per entry pointing at it, and Redis's "exactly once on a quiet keyspace"
    /// guarantee is gone. Advancing at the local depth strides straight over the
    /// aliases.
    ///
    /// (Under growth alone a per-entry walk over-reports rather than under-
    /// reports, so this is stated as duplication, not loss. Loss is what the same
    /// mistake costs a directory that can halve, which this table cannot yet do.)
    #[test]
    fn stepping_one_directory_entry_at_a_time_returns_keys_over_and_over() {
        let mut t = table();
        // Stop the moment the directory doubles. Right then exactly one segment
        // sits at the new global depth and every other one is a level shallower,
        // so most directory entries are aliases onto a segment that is full of
        // entries — the state a per-entry walk over-reports hardest.
        let mut keys = Vec::new();
        let mut i = 0i64;
        while t.global_depth() < 6 {
            let k = format!("key:{i}");
            i += 1;
            t.insert(k.as_bytes(), ValueWord::from_int(i));
            keys.push(k);
        }
        assert!(
            t.segment_count() < (1usize << t.global_depth()),
            "no aliased directory entries: {} segments over {} entries",
            t.segment_count(),
            1usize << t.global_depth()
        );

        let mut per_entry_emitted = 0usize;
        for e in 0..(1usize << t.global_depth()) {
            let seg = &t.segments[t.directory[e] as usize];
            per_entry_emitted += seg.positions().count();
        }

        let mut reverse_emitted = 0usize;
        let mut distinct: HashSet<Vec<u8>> = HashSet::new();
        let mut cursor = 0u64;
        loop {
            cursor = t.scan(cursor, 1, |slot| {
                reverse_emitted += 1;
                distinct.insert(key_of(slot));
            });
            if cursor == 0 {
                break;
            }
        }

        assert_eq!(
            reverse_emitted,
            keys.len(),
            "reverse-binary duplicated a key"
        );
        assert_eq!(distinct.len(), keys.len());
        assert!(
            per_entry_emitted > reverse_emitted,
            "a per-entry walk emitted {per_entry_emitted}, reverse-binary \
             {reverse_emitted}; if they are equal this table has no aliased \
             directory entries and the test proves nothing"
        );
    }

    /// Growth must not cost the scan its guarantee, and it must not cost it its
    /// exactness either — the same walk under churn returns every stable key.
    #[test]
    fn the_cursor_keeps_its_guarantee_under_a_growth_schedule() {
        let mut t = table();
        let stable = fill(&mut t, 40_000);
        let mut seen: HashSet<Vec<u8>> = HashSet::new();
        let mut next = 0i64;
        let mut cursor = 0u64;
        let mut step = 0usize;
        loop {
            cursor = t.scan(cursor, 1, |slot| {
                seen.insert(key_of(slot));
            });
            churn(&mut t, step, &mut next);
            step += 1;
            if cursor == 0 {
                break;
            }
        }
        assert!(
            step > CHURN_STEPS,
            "the churn schedule never ran to completion"
        );
        let missed = stable
            .iter()
            .filter(|k| !seen.contains(k.as_bytes()))
            .count();
        assert_eq!(
            missed, 0,
            "{missed} keys present throughout were never returned"
        );
    }

    #[test]
    fn the_cursor_enumerates_a_directory_of_every_depth_exactly_once() {
        for depth in 0u8..=10 {
            let mut seen = HashSet::new();
            let mut cursor = 0u64;
            loop {
                assert!(
                    seen.insert(cursor & ((1u64 << depth) - 1)),
                    "depth {depth} revisited cursor {cursor:#x}"
                );
                cursor = next_cursor(cursor, depth);
                if cursor == 0 {
                    break;
                }
            }
            assert_eq!(
                seen.len(),
                1usize << depth,
                "depth {depth} skipped an index"
            );
        }
    }

    /// Advancing at a shallower depth — what a segment that has not split yet
    /// does — must still land on a cursor the deeper walk would reach.
    #[test]
    fn a_shallow_advance_lands_on_a_prefix_the_deep_walk_visits() {
        let deep: HashSet<u64> = {
            let mut set = HashSet::new();
            let mut c = 0u64;
            loop {
                set.insert(c);
                c = next_cursor(c, 8);
                if c == 0 {
                    break;
                }
            }
            set
        };
        let mut c = 0u64;
        loop {
            assert!(
                deep.contains(&c),
                "shallow cursor {c:#x} is off the deep walk"
            );
            c = next_cursor(c, 4);
            if c == 0 {
                break;
            }
        }
    }

    #[test]
    fn structural_cost_is_reported_at_the_allocated_size_class() {
        let mut t = table();
        fill(&mut t, 100_000);
        let expected_segments = t.segment_count() * SEGMENT_CLASS_BYTES;
        assert!(t.structural_bytes() > expected_segments);
        assert!(
            t.structural_bytes_per_entry() < 40.0,
            "structural cost {:.1} B/entry",
            t.structural_bytes_per_entry()
        );
    }

    #[test]
    fn out_of_line_keys_and_values_are_released_with_the_table() {
        let mut t: Table<ValueWord> = table();
        for i in 0..5_000i64 {
            t.insert(
                format!("a-key-too-long-to-inline:{i}").as_bytes(),
                ValueWord::from_bytes(b"a value too long to inline"),
            );
        }
        for i in (0..5_000i64).step_by(3) {
            t.remove(format!("a-key-too-long-to-inline:{i}").as_bytes());
        }
        drop(t);
    }
}
