//! Block-backed byte storage for the large hash and set forms.
//!
//! Past the listpack thresholds a hash or set used to fall into
//! `HashMap<Bytes, Bytes>` / `HashSet<Bytes>`: every field, value, and member
//! became its own refcounted heap allocation plus hashbrown table overhead —
//! the R7 fragmentation profile. A [`BlockStore`] keeps the hash-table *index*
//! for O(1) lookup but moves the *bytes* into shared append-only blocks, so
//! entries stop being individual allocations.
//!
//! This is a per-value mini-arena with compaction, not a general allocator:
//!
//! * [`append`](BlockStore::append) writes a record's bytes contiguously into
//!   the tail block (or a dedicated block when the record is larger than
//!   [`BLOCK_CAP`]) and returns a [`Handle`] carrying `(block, offset, len)`.
//!   Records have no headers — the handle is the only metadata.
//! * [`remove`](BlockStore::remove) only updates the dead-byte accounting; the
//!   bytes stay where they are until compaction.
//! * When dead bytes outweigh live bytes (and exceed a minimum, so tiny
//!   stores never bother), [`should_compact`](BlockStore::should_compact)
//!   turns true and the owner calls [`compact`](BlockStore::compact) with its
//!   handles: live records are rewritten densely into fresh blocks and every
//!   handle is patched in place. That bounds dead space at 50% of the store,
//!   amortized O(1) per removal.

/// Capacity of a regular block. A record larger than this gets a block of its
/// own, sized exactly to the record.
const BLOCK_CAP: usize = 16 * 1024;

/// Dead-byte floor below which compaction never triggers, so small stores
/// with a little churn don't rewrite themselves over nothing.
const COMPACT_MIN_DEAD: usize = 4 * 1024;

/// Location of one record inside a [`BlockStore`].
///
/// Handles are only meaningful for the store that issued them, and are
/// invalidated by [`BlockStore::compact`] — which is why `compact` takes the
/// owner's handles and patches them in place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Handle {
    block: u32,
    offset: u32,
    len: u32,
}

impl Handle {
    /// Byte length of the record this handle points at.
    #[inline]
    pub fn len(self) -> usize {
        self.len as usize
    }

    /// Whether the record is empty.
    #[inline]
    pub fn is_empty(self) -> bool {
        self.len == 0
    }
}

/// Append-only block arena. See the module docs for the design.
#[derive(Debug, Clone, Default)]
pub struct BlockStore {
    blocks: Vec<Vec<u8>>,
    /// Live payload bytes (records not yet removed).
    live_bytes: usize,
    /// Bytes belonging to removed records, reclaimed by compaction.
    dead_bytes: usize,
}

impl BlockStore {
    /// An empty store that has not allocated.
    pub const fn new() -> Self {
        Self {
            blocks: Vec::new(),
            live_bytes: 0,
            dead_bytes: 0,
        }
    }

    /// Live payload bytes across all blocks.
    #[inline]
    pub fn live_bytes(&self) -> usize {
        self.live_bytes
    }

    /// Total bytes allocated by the blocks (capacity, not just written), for
    /// memory accounting.
    pub fn allocated_bytes(&self) -> usize {
        self.blocks.iter().map(|b| b.capacity()).sum::<usize>()
            + self.blocks.capacity() * std::mem::size_of::<Vec<u8>>()
    }

    /// Append one record formed by concatenating `parts`, returning its handle.
    ///
    /// Multi-part so a hash can write `field ++ value` as one record without
    /// building a temporary buffer.
    pub fn append(&mut self, parts: &[&[u8]]) -> Handle {
        let len: usize = parts.iter().map(|p| p.len()).sum();
        debug_assert!(u32::try_from(len).is_ok(), "record exceeds u32 length");

        let need_new_block = match self.blocks.last() {
            Some(tail) => tail.len() + len > tail.capacity(),
            None => true,
        };
        if need_new_block {
            self.blocks.push(Vec::with_capacity(BLOCK_CAP.max(len)));
        }
        let block_idx = self.blocks.len() - 1;
        let block = &mut self.blocks[block_idx];
        let offset = block.len();
        for part in parts {
            block.extend_from_slice(part);
        }
        self.live_bytes += len;
        Handle {
            block: block_idx as u32,
            offset: offset as u32,
            len: len as u32,
        }
    }

    /// The record `handle` points at.
    #[inline]
    pub fn get(&self, handle: Handle) -> &[u8] {
        let start = handle.offset as usize;
        &self.blocks[handle.block as usize][start..start + handle.len as usize]
    }

    /// Mark the record dead. The bytes are reclaimed by the next
    /// [`compact`](Self::compact); the handle must not be used again.
    #[inline]
    pub fn remove(&mut self, handle: Handle) {
        let len = handle.len as usize;
        debug_assert!(self.live_bytes >= len);
        self.live_bytes -= len;
        self.dead_bytes += len;
    }

    /// Whether dead space has grown past the compaction threshold: more dead
    /// than live bytes, and enough dead bytes to be worth a rewrite.
    #[inline]
    pub fn should_compact(&self) -> bool {
        self.dead_bytes >= COMPACT_MIN_DEAD && self.dead_bytes > self.live_bytes
    }

    /// Rewrite every live record densely into fresh blocks, patching each
    /// handle in place. `handles` must yield exactly the store's live handles
    /// (each issued handle once, none removed).
    pub fn compact<'a>(&mut self, handles: impl Iterator<Item = &'a mut Handle>) {
        let mut fresh = BlockStore::new();
        for handle in handles {
            *handle = fresh.append(&[self.get(*handle)]);
        }
        debug_assert_eq!(fresh.live_bytes, self.live_bytes);
        *self = fresh;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_get_roundtrip_single_and_multipart() {
        let mut store = BlockStore::new();
        let a = store.append(&[b"hello"]);
        let b = store.append(&[b"field", b"value"]);
        let c = store.append(&[b""]);
        assert_eq!(store.get(a), b"hello");
        assert_eq!(store.get(b), b"fieldvalue");
        assert_eq!(store.get(c), b"");
        assert_eq!(c.len(), 0);
        assert!(c.is_empty());
        assert_eq!(store.live_bytes(), 15);
    }

    #[test]
    fn oversized_record_gets_its_own_block() {
        let mut store = BlockStore::new();
        let small = store.append(&[b"x"]);
        let big_payload = vec![b'q'; BLOCK_CAP * 2];
        let big = store.append(&[&big_payload]);
        let after = store.append(&[b"y"]);
        assert_eq!(store.get(small), b"x");
        assert_eq!(store.get(big), big_payload.as_slice());
        assert_eq!(store.get(after), b"y");
        assert_ne!(small.block, big.block);
    }

    #[test]
    fn records_fill_blocks_before_opening_new_ones() {
        let mut store = BlockStore::new();
        let payload = vec![b'a'; 1000];
        let handles: Vec<Handle> = (0..40).map(|_| store.append(&[&payload])).collect();
        // 16 records of 1000 bytes fit a 16 KiB block.
        assert_eq!(handles[15].block, 0);
        assert_eq!(handles[16].block, 1);
        assert!(store.allocated_bytes() >= 40 * 1000);
    }

    #[test]
    fn compaction_reclaims_dead_space_and_preserves_records() {
        let mut store = BlockStore::new();
        let payload = vec![b'z'; 512];
        let mut handles: Vec<(usize, Handle)> = (0..64)
            .map(|i| (i, store.append(&[&payload, &[i as u8]])))
            .collect();
        // Remove three quarters of the records.
        let mut removed = 0;
        handles.retain(|(i, h)| {
            if i % 4 != 0 {
                removed += 1;
                store.remove(*h);
                false
            } else {
                true
            }
        });
        assert!(store.should_compact());
        let live_before = store.live_bytes();
        store.compact(handles.iter_mut().map(|(_, h)| h));
        assert_eq!(store.live_bytes(), live_before);
        assert!(!store.should_compact());
        // Allocated space is now within one block of the live payload.
        assert!(
            store.allocated_bytes() <= live_before + BLOCK_CAP + std::mem::size_of::<Vec<u8>>()
        );
        for (i, h) in &handles {
            let record = store.get(*h);
            assert_eq!(&record[..512], payload.as_slice());
            assert_eq!(record[512], *i as u8);
        }
    }

    #[test]
    fn small_stores_never_ask_for_compaction() {
        let mut store = BlockStore::new();
        for _ in 0..100 {
            let h = store.append(&[b"tiny"]);
            store.remove(h);
        }
        // 400 dead bytes, zero live — still under the dead-byte floor.
        assert!(!store.should_compact());
    }
}
