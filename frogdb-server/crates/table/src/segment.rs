//! The segment: one size class of buckets, and the placement rules over them.
//!
//! A segment is the unit the directory points at and the unit a split works on.
//! It is one 16 KB allocation ([`crate::layout`]): a 64-byte header followed by
//! 63 buckets, 59 of them home buckets and 4 stash.
//!
//! Placement is Dash's: a key may live in its home bucket or the one after it,
//! and when both are full an entry already there is *displaced* to its own
//! alternative to make room. That is what lifts *a segment's* occupancy towards
//! 0.9 before it is forced to split — a key with two homes and a relocation rule
//! fills far more of the array than a key with one. A whole table averages well
//! under that, because a round of splits halves it and it climbs back; see
//! `tests/layout_cost.rs` for the peak, trough and cycle mean, and for what each
//! is and is not comparable to in the spike.
//!
//! Everything placement needs is read out of slot metadata. A split moves entries
//! without hashing a single key: which half an entry belongs to comes from its
//! `route` bits and where it goes comes from [`hasher::home`], both stored. The
//! only exception is a directory deeper than [`ROUTE_BITS`], where the caller
//! supplies a hash function for the fallback.

use std::alloc::{Layout, alloc_zeroed, handle_alloc_error};

use crate::bucket::{Bucket, STASH_FANOUT, Slot};
use crate::hasher;
use crate::layout::{BUCKETS, REGULAR_BUCKETS};

/// A segment's header. Exactly one cache line, and it stays that way: issue 12's
/// eviction state has to fit the space reserved here rather than grow the line
/// and cost a bucket.
#[derive(Debug)]
#[repr(C)]
struct SegmentHeader {
    /// Directory bits this segment owns. A directory entry at global depth `g`
    /// points here iff its low `local_depth` bits match the segment's.
    local_depth: u8,
    _pad: [u8; 3],
    /// Live entries, so occupancy is O(1) rather than a walk.
    len: u32,
    /// Reserved for issue 12: the clock hand an eviction sweep resumes from.
    evict_hand: u16,
    /// Reserved for issue 12: the coarse epoch a sweep last touched.
    evict_epoch: u16,
    _reserved: [u8; 52],
}

/// One directory-addressable segment.
#[repr(C)]
pub struct Segment<V, const N: usize> {
    header: SegmentHeader,
    buckets: [Bucket<V, N>; BUCKETS],
}

/// What one split did, for the split-stall measurement.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SplitStats {
    /// Slots examined.
    pub scanned: u32,
    /// Entries that changed segment.
    pub moved: u32,
    /// Entries whose half could not be read from metadata and had to be hashed.
    /// Zero below [`ROUTE_BITS`], which is every table anyone will run.
    pub rehashed: u32,
}

/// An entry a split could not place in the target segment, handed back so the
/// caller can re-insert it through the ordinary path (which will split again).
pub struct Displaced<V> {
    pub fp: u8,
    pub route: u16,
    pub slot: Slot<V>,
}

impl<V, const N: usize> Segment<V, N> {
    /// Allocates an empty segment at `local_depth`.
    ///
    /// Zeroed rather than built and boxed: a segment is 16 KB, and
    /// `Box::new(Segment { .. })` would construct that on the stack first. All
    /// zeroes is a valid empty segment — every bitmap and counter is zero, and no
    /// uninitialised slot is readable while `occupied` is zero.
    pub fn alloc(local_depth: u8) -> Box<Segment<V, N>> {
        crate::bucket::assert_bucket_layout::<V, N>();
        let layout = Layout::new::<Segment<V, N>>();
        // SAFETY: `Segment` is far from zero-sized, which is `alloc_zeroed`'s only
        // precondition.
        let raw = unsafe { alloc_zeroed(layout) }.cast::<Segment<V, N>>();
        if raw.is_null() {
            handle_alloc_error(layout);
        }
        // SAFETY: `raw` is a fresh, correctly-sized, correctly-aligned allocation,
        // and all-zeroes is a valid `Segment` as argued above, so it is
        // initialised and `Box` may take ownership of it.
        let mut seg = unsafe { Box::from_raw(raw) };
        seg.header.local_depth = local_depth;
        seg
    }

    /// Bytes this segment costs the allocator — the size class, not the struct,
    /// because the class is what memory accounting is actually charged.
    pub const fn allocated_bytes() -> usize {
        crate::layout::SEGMENT_CLASS_BYTES
    }

    /// Directory bits this segment owns.
    #[inline]
    pub fn local_depth(&self) -> u8 {
        self.header.local_depth
    }

    /// Live entries.
    #[inline]
    pub fn len(&self) -> u32 {
        self.header.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.header.len == 0
    }

    /// Live entries as a fraction of addressable slots.
    pub fn occupancy(&self) -> f64 {
        f64::from(self.header.len) / crate::layout::SEGMENT_SLOTS as f64
    }

    /// Bucket `i`, home buckets first and the stash last.
    #[inline]
    pub fn bucket(&self, i: usize) -> &Bucket<V, N> {
        &self.buckets[i]
    }

    /// The bucket a key with this metadata calls home.
    #[inline]
    pub fn home_of(fp: u8, route: u16) -> usize {
        hasher::home(fp, route, REGULAR_BUCKETS)
    }

    #[inline]
    fn neighbour(home: usize) -> usize {
        let n = home + 1;
        if n == REGULAR_BUCKETS { 0 } else { n }
    }

    /// Finds `key`, returning the bucket and slot holding it.
    pub fn find(&self, fp: u8, route: u16, key: &[u8]) -> Option<(usize, usize)> {
        let home = Self::home_of(fp, route);
        if let Some(i) = self.buckets[home].find(fp, key) {
            return Some((home, i));
        }
        let neighbour = Self::neighbour(home);
        if let Some(i) = self.buckets[neighbour].find(fp, key) {
            return Some((neighbour, i));
        }
        // Only the stashes this home bucket actually spilled into, which is why
        // the counters are per-stash: after a delete the map narrows again
        // instead of staying pessimistic for the segment's life.
        let mut map = self.buckets[home].stash_map();
        while map != 0 {
            let s = map.trailing_zeros() as usize;
            map &= map - 1;
            let b = REGULAR_BUCKETS + s;
            if let Some(i) = self.buckets[b].find(fp, key) {
                return Some((b, i));
            }
        }
        None
    }

    /// The value stored under `key`.
    #[inline]
    pub fn get(&self, fp: u8, route: u16, key: &[u8]) -> Option<&V> {
        let (b, i) = self.find(fp, route, key)?;
        Some(&self.buckets[b].slot(i).val)
    }

    /// The value stored under `key`, mutably.
    #[inline]
    pub fn get_mut(&mut self, fp: u8, route: u16, key: &[u8]) -> Option<&mut V> {
        let (b, i) = self.find(fp, route, key)?;
        Some(&mut self.buckets[b].slot_mut(i).val)
    }

    /// Places `slot`, displacing an existing entry if that is what it takes.
    ///
    /// Gives the slot back when the segment cannot hold it, which is the caller's
    /// signal to split. That is deliberately not "when the segment is full": Dash
    /// gives up while slots remain elsewhere, and splitting then is what keeps the
    /// probe sequence short.
    pub fn insert(&mut self, fp: u8, route: u16, slot: Slot<V>) -> Result<(), Slot<V>> {
        let home = Self::home_of(fp, route);
        let neighbour = Self::neighbour(home);

        // Balanced insert: the emptier of the two candidates, so neither runs out
        // long before the other.
        if !self.buckets[home].is_full() || !self.buckets[neighbour].is_full() {
            let (target, displaced) = if self.buckets[home].len() <= self.buckets[neighbour].len() {
                (home, false)
            } else {
                (neighbour, true)
            };
            let placed = self.buckets[target].insert(fp, route, displaced, slot);
            debug_assert!(placed.is_ok(), "the emptier candidate had no room");
            return placed.map(|_| self.header.len += 1);
        }

        // Both candidates are full. Move somebody else to their alternative.
        if self.relocate_to_previous(home) {
            let placed = self.buckets[home].insert(fp, route, false, slot);
            debug_assert!(placed.is_ok(), "relocation freed no slot");
            return placed.map(|_| self.header.len += 1);
        }
        if self.relocate_to_next(neighbour) {
            let placed = self.buckets[neighbour].insert(fp, route, true, slot);
            debug_assert!(placed.is_ok(), "relocation freed no slot");
            return placed.map(|_| self.header.len += 1);
        }

        // Last resort: a stash, recorded against the home bucket so a lookup for
        // any *other* home never probes it.
        let mut slot = slot;
        for s in 0..STASH_FANOUT {
            match self.buckets[REGULAR_BUCKETS + s].insert(fp, route, false, slot) {
                Ok(_) => {
                    self.buckets[home].note_stash(s);
                    self.header.len += 1;
                    return Ok(());
                }
                Err(given_back) => slot = given_back,
            }
        }
        Err(slot)
    }

    /// Moves one entry out of bucket `b` and back to its home, `b - 1`.
    ///
    /// The entry is one `b` is holding on someone else's behalf — the `probing`
    /// bit says so, which is the whole reason that bit exists: without it,
    /// finding a relocatable entry would mean hashing keys.
    fn relocate_to_previous(&mut self, b: usize) -> bool {
        let previous = if b == 0 { REGULAR_BUCKETS - 1 } else { b - 1 };
        if self.buckets[previous].is_full() {
            return false;
        }
        let Some(i) = self.displaced_slot(b) else {
            return false;
        };
        self.move_slot(b, i, previous, false);
        true
    }

    /// Moves one entry out of bucket `b` to its alternative, `b + 1`.
    ///
    /// The entry has to be one whose home *is* `b` — an entry already displaced
    /// into `b` has no further alternative to go to.
    fn relocate_to_next(&mut self, b: usize) -> bool {
        let next = Self::neighbour(b);
        if self.buckets[next].is_full() {
            return false;
        }
        let Some(i) = self.at_home_slot(b) else {
            return false;
        };
        self.move_slot(b, i, next, true);
        true
    }

    /// The lowest slot in `b` whose home is the previous bucket.
    fn displaced_slot(&self, b: usize) -> Option<usize> {
        let bucket = &self.buckets[b];
        let mut bits = bucket.occupied();
        while bits != 0 {
            let i = bits.trailing_zeros() as usize;
            bits &= bits - 1;
            if bucket.is_displaced(i) {
                return Some(i);
            }
        }
        None
    }

    /// The lowest slot in `b` whose home is `b` itself.
    fn at_home_slot(&self, b: usize) -> Option<usize> {
        let bucket = &self.buckets[b];
        let mut bits = bucket.occupied();
        while bits != 0 {
            let i = bits.trailing_zeros() as usize;
            bits &= bits - 1;
            if !bucket.is_displaced(i) {
                return Some(i);
            }
        }
        None
    }

    /// Carries slot `i` of bucket `from` over to bucket `to`. Metadata travels
    /// with it, so nothing is recomputed and no key is hashed.
    fn move_slot(&mut self, from: usize, i: usize, to: usize, displaced: bool) {
        let (fp, route) = {
            let b = &self.buckets[from];
            (b.fp(i), b.route(i))
        };
        let slot = self.buckets[from].take(i);
        let placed = self.buckets[to].insert(fp, route, displaced, slot);
        debug_assert!(placed.is_ok(), "relocation target was checked for room");
        drop(placed);
    }

    /// Removes `key` and hands back what it held.
    pub fn remove(&mut self, fp: u8, route: u16, key: &[u8]) -> Option<Slot<V>> {
        let (b, i) = self.find(fp, route, key)?;
        Some(self.take_at(b, i, fp, route))
    }

    /// Removes the entry at a known position, keeping the stash counters exact.
    fn take_at(&mut self, b: usize, i: usize, fp: u8, route: u16) -> Slot<V> {
        let slot = self.buckets[b].take(i);
        if b >= REGULAR_BUCKETS {
            let home = Self::home_of(fp, route);
            self.buckets[home].forget_stash(b - REGULAR_BUCKETS);
        }
        self.header.len -= 1;
        slot
    }

    /// Every live entry, as `(bucket, slot)` positions in scan order.
    ///
    /// Bucket order is the order a SCAN cursor walks, so a caller can resume
    /// mid-segment; the stash buckets come last, after every home bucket.
    pub fn positions(&self) -> impl Iterator<Item = (usize, usize)> + '_ {
        (0..BUCKETS).flat_map(move |b| {
            let mut bits = self.buckets[b].occupied();
            std::iter::from_fn(move || {
                if bits == 0 {
                    return None;
                }
                let i = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                Some((b, i))
            })
        })
    }

    /// The entry at a position from [`Segment::positions`].
    #[inline]
    pub fn slot_at(&self, b: usize, i: usize) -> &Slot<V> {
        self.buckets[b].slot(i)
    }

    /// Splits this segment into itself and `into`, which must be a fresh segment.
    ///
    /// Entries whose bit `depth` is set move; the rest stay exactly where they
    /// are. Both facts come out of metadata, so the scan hashes nothing —
    /// `hash_key` is called only past [`ROUTE_BITS`], where the stored route runs
    /// out of bits, and [`SplitStats::rehashed`] counts every time it is.
    ///
    /// Entries the target could not take come back in `leftovers` for the caller
    /// to re-insert through the ordinary path; the target is the only place they
    /// can legally live, so the caller must place them before serving reads.
    pub fn split(
        &mut self,
        into: &mut Segment<V, N>,
        depth: u8,
        hash_key: impl Fn(&[u8]) -> u64,
        key_bytes: impl Fn(&Slot<V>) -> Vec<u8>,
        leftovers: &mut Vec<Displaced<V>>,
    ) -> SplitStats {
        let mut stats = SplitStats::default();
        for b in 0..BUCKETS {
            let mut bits = self.buckets[b].occupied();
            while bits != 0 {
                let i = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                stats.scanned += 1;

                let (fp, route) = {
                    let bucket = &self.buckets[b];
                    (bucket.fp(i), bucket.route(i))
                };
                let goes_high = match self.buckets[b].split_bit(i, depth) {
                    Some(bit) => bit,
                    None => {
                        // Past the stored route width. Correctness is preserved by
                        // paying for a hash; the counter makes that visible rather
                        // than silent.
                        stats.rehashed += 1;
                        let key = key_bytes(self.buckets[b].slot(i));
                        (hash_key(&key) >> depth) & 1 == 1
                    }
                };
                if !goes_high {
                    continue;
                }

                let slot = self.take_at(b, i, fp, route);
                stats.moved += 1;
                if let Err(slot) = into.insert(fp, route, slot) {
                    leftovers.push(Displaced { fp, route, slot });
                }
            }
        }
        self.header.local_depth += 1;
        into.header.local_depth = self.header.local_depth;
        stats
    }
}

impl<V, const N: usize> std::fmt::Debug for Segment<V, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Segment")
            .field("local_depth", &self.header.local_depth)
            .field("len", &self.header.len)
            .field("occupancy", &self.occupancy())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hasher::{TableHasher, TableSeed, fingerprint, route};
    use crate::layout::{ROUTE_BITS, SEGMENT_CLASS_BYTES, SEGMENT_SLOTS, SLOTS_PER_BUCKET};
    use crate::word::{KeyWord, ValueWord};

    type Seg = Segment<ValueWord, SLOTS_PER_BUCKET>;

    fn slot(key: &[u8], val: i64) -> Slot<ValueWord> {
        Slot {
            key: KeyWord::new(key),
            val: ValueWord::from_int(val),
        }
    }

    fn hasher() -> TableHasher {
        TableHasher::new(TableSeed::from_u64(11))
    }

    fn key_bytes(s: &Slot<ValueWord>) -> Vec<u8> {
        let mut buf = [0u8; 16];
        s.key.bytes(&mut buf).to_vec()
    }

    #[test]
    fn a_segment_fits_its_size_class() {
        assert_eq!(std::mem::size_of::<SegmentHeader>(), 64);
        assert_eq!(std::mem::size_of::<Seg>(), crate::layout::SEGMENT_BYTES);
        assert!(std::mem::size_of::<Seg>() <= SEGMENT_CLASS_BYTES);
        assert_eq!(Seg::allocated_bytes(), SEGMENT_CLASS_BYTES);
    }

    #[test]
    fn round_trips_entries_it_holds() {
        let h = hasher();
        let mut seg = Seg::alloc(0);
        for i in 0..200i64 {
            let key = format!("key:{i}");
            let hash = h.hash(key.as_bytes());
            seg.insert(fingerprint(hash), route(hash), slot(key.as_bytes(), i))
                .expect("segment has room for 200");
        }
        assert_eq!(seg.len(), 200);
        for i in 0..200i64 {
            let key = format!("key:{i}");
            let hash = h.hash(key.as_bytes());
            let mut buf = [0u8; 16];
            assert_eq!(
                seg.get(fingerprint(hash), route(hash), key.as_bytes())
                    .expect("inserted")
                    .decode(&mut buf),
                crate::word::Decoded::Int(i)
            );
        }
        let hash = h.hash(b"absent");
        assert!(seg.get(fingerprint(hash), route(hash), b"absent").is_none());
    }

    #[test]
    fn removing_frees_the_slot_and_narrows_the_stash_map() {
        let h = hasher();
        let mut seg = Seg::alloc(0);
        let mut keys = Vec::new();
        for i in 0..700i64 {
            let key = format!("key:{i}");
            let hash = h.hash(key.as_bytes());
            if seg
                .insert(fingerprint(hash), route(hash), slot(key.as_bytes(), i))
                .is_ok()
            {
                keys.push(key);
            }
        }
        let before = seg.len();
        for key in &keys {
            let hash = h.hash(key.as_bytes());
            assert!(
                seg.remove(fingerprint(hash), route(hash), key.as_bytes())
                    .is_some(),
                "{key} should have been removable"
            );
        }
        assert_eq!(before, keys.len() as u32);
        assert_eq!(seg.len(), 0);
        for b in 0..REGULAR_BUCKETS {
            assert_eq!(
                seg.bucket(b).stash_map(),
                0,
                "bucket {b} leaked a stash count"
            );
        }
    }

    /// Occupancy is the point of displacement. Without it a segment gives up
    /// around the spike's 0.581; with it, well past 0.85.
    #[test]
    fn displacement_fills_the_segment_past_the_target_occupancy() {
        let h = hasher();
        let mut seg = Seg::alloc(0);
        let mut placed = 0u32;
        for i in 0..100_000i64 {
            let key = format!("occupancy:{i}");
            let hash = h.hash(key.as_bytes());
            if seg
                .insert(fingerprint(hash), route(hash), slot(key.as_bytes(), i))
                .is_err()
            {
                break;
            }
            placed += 1;
        }
        assert_eq!(seg.len(), placed);
        assert!(
            seg.occupancy() >= 0.85,
            "occupancy {:.3} ({placed}/{SEGMENT_SLOTS}) is below the 0.85 target",
            seg.occupancy()
        );
    }

    /// Every entry has to remain findable after the relocations a full segment
    /// performs — a displacement that loses track of an entry is silent data loss.
    #[test]
    fn every_entry_survives_the_relocations_that_filled_the_segment() {
        let h = hasher();
        let mut seg = Seg::alloc(0);
        let mut placed = Vec::new();
        for i in 0..100_000i64 {
            let key = format!("occupancy:{i}");
            let hash = h.hash(key.as_bytes());
            if seg
                .insert(fingerprint(hash), route(hash), slot(key.as_bytes(), i))
                .is_err()
            {
                break;
            }
            placed.push((key, i));
        }
        assert!(placed.len() > 700, "not enough entries to be interesting");
        for (key, i) in &placed {
            let hash = h.hash(key.as_bytes());
            let mut buf = [0u8; 16];
            let got = seg
                .get(fingerprint(hash), route(hash), key.as_bytes())
                .unwrap_or_else(|| panic!("{key} was lost by a relocation"));
            assert_eq!(got.decode(&mut buf), crate::word::Decoded::Int(*i));
        }
    }

    /// The `probing` bitmap is a cache of a fact `home` can derive. If the two
    /// ever disagree, relocation moves entries to buckets they cannot be found in.
    #[test]
    fn the_probing_bitmap_agrees_with_the_derived_home() {
        let h = hasher();
        let mut seg = Seg::alloc(0);
        for i in 0..100_000i64 {
            let key = format!("probing:{i}");
            let hash = h.hash(key.as_bytes());
            if seg
                .insert(fingerprint(hash), route(hash), slot(key.as_bytes(), i))
                .is_err()
            {
                break;
            }
        }
        for (b, i) in seg.positions().collect::<Vec<_>>() {
            if b >= REGULAR_BUCKETS {
                continue;
            }
            let bucket = seg.bucket(b);
            let home = Seg::home_of(bucket.fp(i), bucket.route(i));
            let expected = home != b;
            assert_eq!(
                bucket.is_displaced(i),
                expected,
                "bucket {b} slot {i}: home is {home}"
            );
            assert!(
                home == b || Seg::neighbour(home) == b,
                "bucket {b} slot {i} holds an entry whose home is {home}"
            );
        }
    }

    /// The split-stall measurement: a split copies entries and hashes nothing.
    #[test]
    fn a_split_moves_entries_without_hashing_a_key() {
        let h = hasher();
        let mut seg = Seg::alloc(0);
        let mut keys = Vec::new();
        for i in 0..100_000i64 {
            let key = format!("split:{i}");
            let hash = h.hash(key.as_bytes());
            if seg
                .insert(fingerprint(hash), route(hash), slot(key.as_bytes(), i))
                .is_err()
            {
                break;
            }
            keys.push((key, i));
        }

        let mut high = Seg::alloc(0);
        let mut leftovers = Vec::new();
        let stats = seg.split(
            &mut high,
            0,
            |_| panic!("a split below the route width must not hash a key"),
            key_bytes,
            &mut leftovers,
        );

        assert_eq!(stats.rehashed, 0);
        assert_eq!(stats.scanned, keys.len() as u32);
        assert_eq!(high.len(), stats.moved);
        assert_eq!(seg.len() + high.len(), keys.len() as u32);
        assert!(
            leftovers.is_empty(),
            "a fresh target should absorb one half"
        );
        assert_eq!(seg.local_depth(), 1);
        assert_eq!(high.local_depth(), 1);

        // Every entry is still findable, in whichever half its route bit sent it.
        for (key, i) in &keys {
            let hash = h.hash(key.as_bytes());
            let (fp, r) = (fingerprint(hash), route(hash));
            let target = if hash & 1 == 1 { &high } else { &seg };
            let mut buf = [0u8; 16];
            let got = target
                .get(fp, r, key.as_bytes())
                .unwrap_or_else(|| panic!("{key} was lost by the split"));
            assert_eq!(got.decode(&mut buf), crate::word::Decoded::Int(*i));
        }
    }

    /// Past the stored route width a split has to hash. It must still be correct,
    /// and it must say so.
    #[test]
    fn a_split_past_the_route_width_falls_back_to_hashing() {
        let h = hasher();
        let mut seg = Seg::alloc(ROUTE_BITS as u8);
        let mut keys = Vec::new();
        for i in 0..300i64 {
            let key = format!("deep:{i}");
            let hash = h.hash(key.as_bytes());
            seg.insert(fingerprint(hash), route(hash), slot(key.as_bytes(), i))
                .expect("room for 300");
            keys.push(key);
        }

        let mut high = Seg::alloc(0);
        let mut leftovers = Vec::new();
        let hashes = std::cell::Cell::new(0u32);
        let stats = seg.split(
            &mut high,
            ROUTE_BITS as u8,
            |k| {
                hashes.set(hashes.get() + 1);
                h.hash(k)
            },
            key_bytes,
            &mut leftovers,
        );

        assert_eq!(stats.rehashed, keys.len() as u32);
        assert_eq!(hashes.get(), keys.len() as u32);
        for key in &keys {
            let hash = h.hash(key.as_bytes());
            let target = if (hash >> ROUTE_BITS) & 1 == 1 {
                &high
            } else {
                &seg
            };
            assert!(
                target
                    .get(fingerprint(hash), route(hash), key.as_bytes())
                    .is_some(),
                "{key} was lost by a deep split"
            );
        }
    }

    #[test]
    fn dropping_a_segment_releases_out_of_line_keys_and_values() {
        let h = hasher();
        let mut seg = Seg::alloc(0);
        for i in 0..500i64 {
            let key = format!("a-key-far-too-long-to-inline:{i}");
            let hash = h.hash(key.as_bytes());
            let placed = seg.insert(
                fingerprint(hash),
                route(hash),
                Slot {
                    key: KeyWord::new(key.as_bytes()),
                    val: ValueWord::from_bytes(b"a value far too long to inline either"),
                },
            );
            assert!(placed.is_ok());
        }
        drop(seg);
    }
}
