//! Block-backed byte storage for the large hash and set forms.
//!
//! Past the listpack thresholds a hash or set used to fall into
//! `HashMap<Bytes, Bytes>` / `HashSet<Bytes>`: every field, value, and member
//! became its own refcounted heap allocation plus hashbrown table overhead —
//! the R7 fragmentation profile. A [`BlockStore`] keeps the hash-table *index*
//! for O(1) lookup but moves the *bytes* into shared append-only blocks, so
//! entries stop being individual allocations.
//!
//! This is a per-value mini-arena with per-block compaction, not a general
//! allocator:
//!
//! * [`append`](BlockStore::append) writes a record's bytes contiguously into
//!   the tail block and returns a [`Handle`] carrying `(block, offset, len)`.
//!   Records have no headers — the handle is the only metadata. Block
//!   capacities ramp from [`FIRST_BLOCK_CAP`] up to [`BLOCK_CAP`], so a value
//!   that has only just crossed the listpack thresholds does not pay a 16 KiB
//!   floor; a record larger than [`BLOCK_CAP`] gets a block sized exactly to
//!   it.
//! * [`remove`](BlockStore::remove) only updates that block's dead-byte
//!   accounting; the bytes stay where they are until compaction.
//! * A block that becomes mostly dead (past a floor, so small blocks with a
//!   little churn don't bother) — or entirely dead — becomes the compaction
//!   candidate: [`should_compact`](BlockStore::should_compact) turns true and
//!   the owner calls [`compact`](BlockStore::compact) with its handles. The
//!   victim block's live records are re-appended to the tail, their handles
//!   patched in place, and the block's allocation is released for reuse. One
//!   compaction copies at most one block's live bytes (≤ [`BLOCK_CAP`]/2 for
//!   a ratio-triggered victim), so the pause is bounded per removal; the
//!   handle scan is O(entries) but touches only the owner's dense handle vec.
//!
//! Freed block slots are recycled by later appends, so long-lived churn does
//! not grow the block table.

/// Capacity ceiling for a regular block. A record larger than this gets a
/// block of its own, sized exactly to the record.
const BLOCK_CAP: usize = 16 * 1024;

/// Capacity of the first block. Each subsequent block doubles until
/// [`BLOCK_CAP`], so a value just past the listpack thresholds does not pay
/// the full block size up front.
const FIRST_BLOCK_CAP: usize = 512;

/// Dead-byte floor below which a partially dead block is never compacted, so
/// blocks with a little churn don't rewrite themselves over nothing. A fully
/// dead block is always reclaimed — that costs no copying at all.
const COMPACT_MIN_DEAD: usize = 4 * 1024;

/// Location of one record inside a [`BlockStore`].
///
/// Handles are only meaningful for the store that issued them, and
/// [`BlockStore::compact`] invalidates handles into the victim block — which
/// is why `compact` takes the owner's handles and patches them in place.
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

#[derive(Debug, Clone, Default)]
struct Block {
    buf: Vec<u8>,
    /// Bytes in `buf` belonging to removed records.
    dead: u32,
}

/// Append-only block arena. See the module docs for the design.
#[derive(Debug, Clone)]
pub struct BlockStore {
    blocks: Vec<Block>,
    /// Block currently accepting appends.
    tail: Option<u32>,
    /// Fully reclaimed block slots awaiting reuse.
    free: Vec<u32>,
    /// Live payload bytes (records not yet removed).
    live_bytes: usize,
    /// Capacity the next new block gets (ramps up to [`BLOCK_CAP`]).
    next_block_cap: usize,
    /// Block picked for compaction by [`remove`](Self::remove), consumed by
    /// [`compact`](Self::compact).
    compact_candidate: Option<u32>,
}

impl Default for BlockStore {
    fn default() -> Self {
        Self::new()
    }
}

impl BlockStore {
    /// An empty store that has not allocated.
    pub const fn new() -> Self {
        Self {
            blocks: Vec::new(),
            tail: None,
            free: Vec::new(),
            live_bytes: 0,
            next_block_cap: FIRST_BLOCK_CAP,
            compact_candidate: None,
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
        self.blocks.iter().map(|b| b.buf.capacity()).sum::<usize>()
            + self.blocks.capacity() * std::mem::size_of::<Block>()
            + self.free.capacity() * std::mem::size_of::<u32>()
    }

    /// Append one record formed by concatenating `parts`, returning its handle.
    ///
    /// Multi-part so a hash can write `field ++ value` as one record without
    /// building a temporary buffer.
    pub fn append(&mut self, parts: &[&[u8]]) -> Handle {
        let len: usize = parts.iter().map(|p| p.len()).sum();
        // A silently truncated handle would read the wrong bytes later;
        // unreachable under the protocol's bulk-string cap, but fail loudly.
        assert!(u32::try_from(len).is_ok(), "record exceeds u32 length");

        let fits = self.tail.is_some_and(|t| {
            let buf = &self.blocks[t as usize].buf;
            buf.len() + len <= buf.capacity()
        });
        if !fits {
            let cap = if len >= BLOCK_CAP {
                len
            } else {
                self.next_block_cap.max(len)
            };
            self.next_block_cap = (self.next_block_cap * 2).min(BLOCK_CAP);
            let idx = match self.free.pop() {
                Some(i) => {
                    self.blocks[i as usize].buf = Vec::with_capacity(cap);
                    i
                }
                None => {
                    self.blocks.push(Block {
                        buf: Vec::with_capacity(cap),
                        dead: 0,
                    });
                    (self.blocks.len() - 1) as u32
                }
            };
            self.tail = Some(idx);
        }
        let block_idx = self.tail.expect("tail exists after fit check");
        let block = &mut self.blocks[block_idx as usize].buf;
        let offset = block.len();
        for part in parts {
            block.extend_from_slice(part);
        }
        self.live_bytes += len;
        Handle {
            block: block_idx,
            offset: offset as u32,
            len: len as u32,
        }
    }

    /// The record `handle` points at.
    #[inline]
    pub fn get(&self, handle: Handle) -> &[u8] {
        let start = handle.offset as usize;
        &self.blocks[handle.block as usize].buf[start..start + handle.len as usize]
    }

    /// Overwrite the record in place with same-length content, keeping the
    /// handle valid — the no-dead-bytes path for equal-size updates.
    pub fn overwrite(&mut self, handle: Handle, parts: &[&[u8]]) {
        let len: usize = parts.iter().map(|p| p.len()).sum();
        assert_eq!(
            len, handle.len as usize,
            "overwrite must keep record length"
        );
        let buf = &mut self.blocks[handle.block as usize].buf;
        let mut at = handle.offset as usize;
        for part in parts {
            buf[at..at + part.len()].copy_from_slice(part);
            at += part.len();
        }
    }

    /// Mark the record dead. The bytes are reclaimed when the block is
    /// compacted; the handle must not be used again.
    pub fn remove(&mut self, handle: Handle) {
        let len = handle.len as usize;
        debug_assert!(self.live_bytes >= len);
        self.live_bytes -= len;
        let idx = handle.block;
        let block = &mut self.blocks[idx as usize];
        block.dead += handle.len;
        let dead = block.dead as usize;
        let written = block.buf.len();
        let is_tail = self.tail == Some(idx);
        // A fully dead block is reclaimed without copying; a mostly dead one
        // (past the floor, and not the tail — the tail is still filling) has
        // its live remainder moved out.
        let fully_dead = written > 0 && dead == written;
        let ripe = !is_tail && dead >= COMPACT_MIN_DEAD && dead * 2 >= written;
        if fully_dead || ripe {
            self.compact_candidate = Some(idx);
        }
    }

    /// Whether a block is waiting to be compacted.
    #[inline]
    pub fn should_compact(&self) -> bool {
        self.compact_candidate.is_some()
    }

    /// Compact the candidate block: re-append its live records to the tail,
    /// patch their handles in place, and release the block for reuse.
    /// `handles` must yield every live handle the store has issued.
    /// No-op when nothing is ripe.
    pub fn compact<'a>(&mut self, handles: impl Iterator<Item = &'a mut Handle>) {
        let Some(victim) = self.compact_candidate.take() else {
            return;
        };
        let buf = std::mem::take(&mut self.blocks[victim as usize].buf);
        self.blocks[victim as usize].dead = 0;
        if self.tail == Some(victim) {
            // Only a fully dead tail is ever a candidate; nothing to move.
            self.tail = None;
        }
        let mut moved = 0usize;
        for handle in handles {
            if handle.block != victim {
                continue;
            }
            let start = handle.offset as usize;
            let bytes = &buf[start..start + handle.len as usize];
            moved += bytes.len();
            *handle = self.append(&[bytes]);
        }
        // The moved bytes were already counted live; append re-added them.
        self.live_bytes -= moved;
        self.free.push(victim);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Owner-style helper: every live handle lives in `handles`; compact
    /// whenever the store asks, the way BlockHash/BlockSet do.
    fn compact_if_ripe(store: &mut BlockStore, handles: &mut [Handle]) {
        if store.should_compact() {
            store.compact(handles.iter_mut());
        }
    }

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
    fn overwrite_replaces_bytes_in_place() {
        let mut store = BlockStore::new();
        let h = store.append(&[b"field", b"12345"]);
        store.overwrite(h, &[b"field", b"67890"]);
        assert_eq!(store.get(h), b"field67890");
        assert_eq!(store.live_bytes(), 10);
        assert!(!store.should_compact());
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
    fn block_capacities_ramp_up_from_small() {
        let mut store = BlockStore::new();
        store.append(&[b"tiny"]);
        // A fresh store just past the listpack thresholds holds one small
        // block, not a 16 KiB floor.
        assert!(store.allocated_bytes() < 2 * FIRST_BLOCK_CAP);
        let payload = vec![b'a'; 100];
        let handles: Vec<Handle> = (0..500).map(|_| store.append(&[&payload])).collect();
        // The ramp reaches BLOCK_CAP: some block holds many more records
        // than the first-block capacity ever could.
        let fullest = (0..store.blocks.len() as u32)
            .map(|b| handles.iter().filter(|h| h.block == b).count())
            .max()
            .unwrap();
        assert!(
            fullest > 100,
            "fullest ramped block holds {fullest} records"
        );
        // Overall allocation stays proportional to the payload.
        assert!(store.allocated_bytes() < store.live_bytes() * 2 + BLOCK_CAP);
    }

    #[test]
    fn compaction_reclaims_mostly_dead_blocks_and_preserves_records() {
        let mut store = BlockStore::new();
        let payload = vec![b'z'; 512];
        let mut slots: Vec<Option<Handle>> = (0..64u8)
            .map(|i| Some(store.append(&[&payload, &[i]])))
            .collect();
        // Remove three quarters, compacting whenever the store asks — the
        // owner protocol: compact sees every still-live handle, not just
        // the ones this loop has already decided to keep.
        for i in 0..slots.len() {
            if i % 4 == 0 {
                continue;
            }
            let h = slots[i].take().unwrap();
            store.remove(h);
            if store.should_compact() {
                store.compact(slots.iter_mut().flatten());
            }
        }
        assert!(!store.should_compact());
        // Every surviving record still reads back byte-exact.
        for (i, slot) in slots.iter().enumerate() {
            let Some(h) = slot else { continue };
            let record = store.get(*h);
            assert_eq!(&record[..512], payload.as_slice());
            assert_eq!(record[512], i as u8);
        }
        // Dead space was actually reclaimed: allocation tracks the live
        // payload, not the 64-record peak.
        let live_bytes = store.live_bytes();
        assert_eq!(live_bytes, 16 * 513);
        assert!(
            store.allocated_bytes() < live_bytes * 2 + BLOCK_CAP,
            "allocated {} for {} live",
            store.allocated_bytes(),
            live_bytes
        );
    }

    #[test]
    fn fully_dead_oversized_block_is_reclaimed_without_handles() {
        let mut store = BlockStore::new();
        let big_payload = vec![b'q'; BLOCK_CAP * 2];
        let big = store.append(&[&big_payload]);
        let keep = store.append(&[b"keep"]);
        let mut handles = [keep];
        store.remove(big);
        assert!(store.should_compact());
        store.compact(handles.iter_mut());
        assert_eq!(store.get(handles[0]), b"keep");
        assert!(store.allocated_bytes() < BLOCK_CAP);
    }

    #[test]
    fn churned_tiny_store_recycles_blocks_instead_of_growing() {
        let mut store = BlockStore::new();
        for _ in 0..10_000 {
            let h = store.append(&[&[b'x'; 64]]);
            let mut none: [Handle; 0] = [];
            store.remove(h);
            compact_if_ripe(&mut store, &mut none);
        }
        assert_eq!(store.live_bytes(), 0);
        // Freed slots are reused: the block table does not grow with churn.
        assert!(
            store.blocks.len() < 8,
            "block table grew to {}",
            store.blocks.len()
        );
        assert!(store.allocated_bytes() < 2 * BLOCK_CAP);
    }

    #[test]
    fn lightly_dead_blocks_are_left_alone() {
        let mut store = BlockStore::new();
        let payload = vec![b'a'; 100];
        let handles: Vec<Handle> = (0..100).map(|_| store.append(&[&payload])).collect();
        // Remove every tenth record — every block stays mostly live and
        // under the dead-byte floor, so nothing asks for compaction.
        for h in handles.iter().step_by(10) {
            store.remove(*h);
        }
        assert!(!store.should_compact());
    }
}
