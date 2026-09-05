//! The segmented keyspace table (`frogdb-table`) as a store backend.
//!
//! # Why the entry is boxed
//!
//! A table slot is one 8-byte key word beside one value, and the geometry that
//! makes the structure worth having — 13 slots per 256-byte bucket, 819 slots
//! per 16 KiB segment — holds only while the value is 8 bytes. [`Entry`] is 64
//! bytes: stored inline it would drive the bucket down to 3 slots and the
//! structural cost *up* to roughly 96 B/entry, worse than griddle. So the slot
//! holds a `Box<Entry>` and the table keeps its shape.
//!
//! That box is a real per-key allocation the incumbent does not pay, and it is
//! why this backend is a smaller memory win than the crate's own numbers
//! suggest. Collapsing `Entry` into tagged words is the follow-up that turns
//! the rest of the win on; it is not this issue.
//!
//! # What is different for callers
//!
//! - **SCAN is Redis's cursor.** A step walks the directory in reverse-binary
//!   order at the scanned segment's local depth, so it costs one segment rather
//!   than sorting the whole shard. `count` bounds keys *examined* here (a step
//!   emits whole segments) where griddle bounds keys kept; both are behaviours
//!   Redis permits, and both keep the present-throughout guarantee.
//! - **Keys are copied out, not shared.** The table packs a key into a word or
//!   a heap record; it has no `Bytes` to hand back, so anything that keeps a
//!   key ([`Keyspace::scan`] results, `all_keys`) copies it.

use std::ops::ControlFlow;

use bytes::Bytes;
use frogdb_table::{InlineBuf, Table};

use super::{Entry, KeyRef, Keyspace};

pub(in crate::store) struct TableKeyspace {
    data: Table<Box<Entry>>,
    /// The last victim walk that came back empty, as the question it answered:
    /// `(volatile_only, Table::generation)`.
    ///
    /// A shard over its limit with nothing evictable refuses every write, and
    /// each refusal would otherwise re-walk every segment header — O(segments)
    /// per rejected command, ~65 k of them per GiB. Nothing but a mutation can
    /// make a key eligible, so the same question at the same generation has the
    /// same answer and is not worth asking twice.
    ///
    /// Only a walk that moved nothing itself is remembered: see
    /// [`Table::generation`], which a promotion during the walk also bumps.
    ///
    /// The saving is over *consecutive* refusals with no mutation between them.
    /// A command-path read is a mutation here — `HashMapStore::
    /// get_with_expiry_check` touches the entry's LRU clock and LFU counter
    /// through `get_mut` — so a read interleaved between two refused writes
    /// costs the second one a full walk. That is the honest bound: the flood
    /// this exists for is refused writes, and reads pay for what they change.
    fruitless_walk: Option<(bool, u64)>,
}

impl Keyspace for TableKeyspace {
    fn new() -> Self {
        TableKeyspace {
            data: Table::new(),
            fruitless_walk: None,
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    fn contains_key(&self, key: &[u8]) -> bool {
        self.data.contains_key(key)
    }

    fn get(&self, key: &[u8]) -> Option<&Entry> {
        self.data.get(key).map(|boxed| &**boxed)
    }

    fn get_mut(&mut self, key: &[u8]) -> Option<&mut Entry> {
        self.data.get_mut(key).map(|boxed| &mut **boxed)
    }

    fn insert(&mut self, key: Bytes, entry: Entry) -> Option<Entry> {
        self.data.insert(&key, Box::new(entry)).map(|boxed| *boxed)
    }

    fn remove(&mut self, key: &[u8]) -> Option<Entry> {
        self.data.remove(key).map(|boxed| *boxed)
    }

    fn clear(&mut self) {
        self.data.clear();
    }

    fn visit(&self, mut f: impl FnMut(KeyRef<'_>, &Entry) -> ControlFlow<()>) {
        for slot in self.data.iter() {
            let mut buf: InlineBuf = [0; 16];
            let key = slot.key.bytes(&mut buf);
            if f(KeyRef::Borrowed(key), &slot.val).is_break() {
                return;
            }
        }
    }

    fn scan(
        &self,
        cursor: u64,
        count: usize,
        mut visit: impl FnMut(KeyRef<'_>, &Entry) -> bool,
    ) -> u64 {
        self.data.scan(cursor, count, |slot| {
            let entry = &*slot.val;
            if entry.metadata.is_expired() {
                return;
            }
            let mut buf: InlineBuf = [0; 16];
            let key = slot.key.bytes(&mut buf);
            // The kept/skipped answer bounds griddle's step; here the step is
            // already bounded by the segment it walked, so it is discarded.
            let _ = visit(KeyRef::Borrowed(key), entry);
        })
    }

    /// The point of the segmented table for eviction: 2Q over *segments*, so
    /// the cold ordering costs 28 bytes per 16 KiB segment and nothing at all
    /// per key (PRD R9).
    ///
    /// The keys come back copied, as everything that leaves this backend does:
    /// a table slot holds a packed word, not a `Bytes`.
    ///
    /// A walk that finds nothing is remembered (see `fruitless_walk`) so that a
    /// shard refusing write after write against an unevictable keyspace pays
    /// the queue walk once, not once per refusal. `Some(vec![])` — never `None`
    /// — is the answer either way: `None` at this seam means "no cold ordering,
    /// go and sample", and a cache hit must not silently change backend.
    fn cold_candidates(
        &mut self,
        want: usize,
        epoch: u16,
        volatile_only: bool,
        accept: impl Fn(&Entry) -> bool,
    ) -> Option<Vec<Bytes>> {
        let generation = self.data.generation();
        if self.fruitless_walk == Some((volatile_only, generation)) {
            return Some(Vec::new());
        }
        let mut keys = Vec::with_capacity(want.min(self.data.len()));
        self.data.cold_candidates(
            want,
            epoch,
            // The table stores `Box<Entry>`, so the predicate it hands us takes
            // a `&Box<Entry>`; the deref coercion is what reaches `accept`.
            //
            // Expired-but-present entries are *not* filtered here, unlike in
            // `scan`: nominating one reports it as `evicted` rather than
            // `expired`, a small observability divergence, but skipping it
            // would leave keys the expiry cycle has not reached yet
            // unreclaimable while the shard is under pressure. Lazy expiry
            // frees the same bytes either way.
            |entry| accept(entry),
            |key| keys.push(Bytes::copy_from_slice(key)),
        );
        // Three walks are not worth remembering. One that produced something has
        // already changed what the next one should see, because the caller
        // deletes what it was handed. One that promoted a segment withheld that
        // segment from nomination, so repeating it can produce what it just
        // refused — the table reports that by moving its generation, and a
        // refusal is only stable when the walk left the generation alone. And
        // `want == 0` never walks at all (`Table::cold_candidates` returns
        // early), so its empty answer is evidence about nothing; caching it
        // would refuse the next real request out of hand.
        let walked = want > 0;
        let inert = self.data.generation() == generation;
        self.fruitless_walk =
            (walked && keys.is_empty() && inert).then_some((volatile_only, generation));
        Some(keys)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    fn keyspace_of(keys: usize) -> TableKeyspace {
        let mut ks = TableKeyspace::new();
        for i in 0..keys {
            ks.insert(Bytes::from(format!("k:{i}")), Entry::hot_for_test());
        }
        ks
    }

    /// A walk that returns nothing costs a queue walk; repeating the same
    /// question must not.
    // FM-MEMORY-007
    #[test]
    fn a_second_refusal_at_an_unchanged_keyspace_does_not_walk_again() {
        let mut ks = keyspace_of(4_000);
        let asked = Cell::new(0usize);
        // Nothing is acceptable, which is what a shard over its limit under
        // `volatile-*` with no TTL'd keys looks like.
        let refuse_everything = |ks: &mut TableKeyspace| {
            asked.set(0);
            let taken = ks.cold_candidates(8, 0, false, |_| {
                asked.set(asked.get() + 1);
                false
            });
            assert_eq!(taken, Some(Vec::new()), "nothing is acceptable");
            asked.get()
        };

        let first = refuse_everything(&mut ks);
        assert!(first > 0, "the first refusal has to look at the keyspace");
        assert_eq!(
            refuse_everything(&mut ks),
            0,
            "the same question at the same generation is answered from the memo"
        );

        // A mutation can make a key eligible, so the memo has to lapse.
        assert!(ks.get_mut(b"k:1").is_some());
        assert!(
            refuse_everything(&mut ks) > 0,
            "a mutation reopens the question"
        );
    }

    /// The memo answers one question, not any question: `volatile_only` picks
    /// which entries `accept` will take, so the two variants cannot share it.
    // FM-MEMORY-007
    #[test]
    fn a_refusal_under_one_confinement_does_not_answer_the_other() {
        let mut ks = keyspace_of(4_000);
        let asked = Cell::new(0usize);
        let ask = |ks: &mut TableKeyspace, volatile_only: bool| {
            asked.set(0);
            ks.cold_candidates(8, 0, volatile_only, |_| {
                asked.set(asked.get() + 1);
                false
            });
            asked.get()
        };

        assert!(ask(&mut ks, true) > 0);
        assert!(
            ask(&mut ks, false) > 0,
            "a different confinement is a different question"
        );
    }

    /// A caller asking for nothing is answered without a walk, so its empty
    /// answer is not evidence that there is nothing to take. Caching it would
    /// refuse the next real request — an `-OOM` against an evictable keyspace.
    // FM-MEMORY-007
    #[test]
    fn asking_for_no_candidates_does_not_cache_a_refusal() {
        let mut ks = keyspace_of(4_000);

        assert_eq!(ks.cold_candidates(0, 0, false, |_| true), Some(Vec::new()));

        let taken = ks
            .cold_candidates(1, 0, false, |_| true)
            .expect("the segmented backend always answers with a list");
        assert_eq!(taken.len(), 1, "the keyspace is full of evictable keys");
    }
}
