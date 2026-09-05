//! The incumbent keyspace: `griddle::HashMap<Bytes, Entry>`.
//!
//! Behaviour here is exactly what the store did before the seam existed,
//! content-hash SCAN cursor included; the seam is a move, not a rewrite.

use std::ops::ControlFlow;

use bytes::Bytes;
use griddle::HashMap;

use super::{Entry, KeyRef, Keyspace};

/// Stable 48-bit content hash of a key, used to order the keyspace for SCAN.
///
/// SCAN's cursor is the hash of the resume point, not a table position, so the
/// ordering does not shift when griddle rehashes on insert. The result is masked
/// to 48 bits because it rides in the position field of the cross-shard SCAN
/// cursor, and remapped away from 0 (which the cross-shard driver reserves for
/// "shard exhausted").
fn scan_cursor_hash(key: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    const CURSOR_MASK: u64 = (1u64 << 48) - 1;
    let mut hasher = std::hash::DefaultHasher::new();
    key.hash(&mut hasher);
    let h = hasher.finish() & CURSOR_MASK;
    if h == 0 { 1 } else { h }
}

pub(in crate::store) struct GriddleKeyspace {
    data: HashMap<Bytes, Entry>,
}

impl Keyspace for GriddleKeyspace {
    fn new() -> Self {
        GriddleKeyspace {
            data: HashMap::new(),
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
        self.data.get(key)
    }

    fn get_mut(&mut self, key: &[u8]) -> Option<&mut Entry> {
        self.data.get_mut(key)
    }

    fn insert(&mut self, key: Bytes, entry: Entry) -> Option<Entry> {
        self.data.insert(key, entry)
    }

    fn remove(&mut self, key: &[u8]) -> Option<Entry> {
        self.data.remove(key)
    }

    fn clear(&mut self) {
        self.data.clear();
    }

    fn visit(&self, mut f: impl FnMut(KeyRef<'_>, &Entry) -> ControlFlow<()>) {
        for (key, entry) in self.data.iter() {
            if f(KeyRef::Shared(key), entry).is_break() {
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
        // Content-hash cursor: order the scannable keyspace by a stable hash of
        // each key rather than by griddle's iteration position. The position
        // order shifts whenever the table resizes (incremental rehash on
        // insert), so a positional cursor could skip keys that were present for
        // the whole scan. Hashing by key content makes the ordering independent
        // of table layout, so a key present throughout the scan is always
        // returned — the guarantee Redis provides via reverse-binary bucket
        // iteration, which a SwissTable cannot offer and the segmented table
        // can.
        //
        // The cost is the reason the segmented table exists: this sorts the
        // whole shard on *every* SCAN step, so a full walk is O(n log n) per
        // step and O(n² log n) overall.
        let mut ordered: Vec<(u64, &Bytes, &Entry)> = self
            .data
            .iter()
            .filter(|(_, entry)| !entry.metadata.is_expired())
            .map(|(key, entry)| (scan_cursor_hash(key), key, entry))
            .collect();
        ordered.sort_unstable_by_key(|(hash, _, _)| *hash);

        // Resume at the first key whose hash is >= the cursor. Cursor 0 starts
        // from the beginning; a returned cursor of 0 means the shard is done.
        let start = if cursor == 0 {
            0
        } else {
            ordered.partition_point(|(hash, _, _)| *hash < cursor)
        };

        let mut kept = 0usize;
        for (hash, key, entry) in ordered.into_iter().skip(start) {
            if kept >= count {
                // Stop before emitting this key; resume here next call.
                return hash;
            }
            if visit(KeyRef::Shared(key), entry) {
                kept += 1;
            }
        }
        0
    }

    /// Griddle holds no ordering by temperature — a `SwissTable` bucket index
    /// says nothing about when the entry was last used, and the per-key
    /// recency the store does hold (`KeyMetadata::last_access`) is what the
    /// sampling loop already reads. So this backend declines, and the store
    /// keeps the Redis-style sampled-LRU/LFU path it has always used: the
    /// default build's eviction behaviour is unchanged by this seam existing.
    fn cold_candidates(
        &mut self,
        _want: usize,
        _epoch: u16,
        _volatile_only: bool,
        _accept: impl Fn(&Entry) -> bool,
    ) -> Option<Vec<Bytes>> {
        None
    }
}
