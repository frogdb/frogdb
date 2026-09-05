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
}

impl Keyspace for TableKeyspace {
    fn new() -> Self {
        TableKeyspace { data: Table::new() }
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
}
