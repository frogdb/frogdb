use bytes::Bytes;
use std::collections::VecDeque;

use super::QuicklistLimits;
use crate::listpack::Listpack;

// ============================================================================
// List Type
// ============================================================================

/// One link in the quicklist chain.
///
/// Mirrors Redis's quicklist node kinds: most elements live packed together in
/// a listpack, and an element too large for a block gets a node to itself
/// (Redis's `PLAIN` node) so one huge value does not push every neighbour into
/// its own allocation.
#[derive(Debug, Clone)]
enum Block {
    /// Many elements packed contiguously.
    Packed(Listpack),
    /// A single element too large to pack, kept as its own buffer.
    Plain(Bytes),
}

impl Block {
    #[inline]
    fn len(&self) -> usize {
        match self {
            Block::Packed(lp) => lp.len(),
            Block::Plain(_) => 1,
        }
    }

    #[inline]
    fn byte_len(&self) -> usize {
        match self {
            Block::Packed(lp) => lp.byte_len(),
            Block::Plain(b) => b.len(),
        }
    }

    #[inline]
    fn get(&self, index: usize) -> Option<&[u8]> {
        match self {
            Block::Packed(lp) => lp.get(index),
            Block::Plain(b) => (index == 0).then(|| b.as_ref()),
        }
    }
}

/// List value — a quicklist: a chain of listpack blocks.
///
/// Elements are stored packed inside block buffers, so a list of N small
/// elements costs O(N / `max_entries`) allocations rather than one refcounted
/// `Bytes` per element. Index and rank operations walk the chain by cached
/// per-block counts and then scan inside one block, which is the same
/// complexity class as Redis's quicklist.
///
/// # Deviations from Redis `quicklist.c`
///
/// * **The chain is a `VecDeque<Block>`, not an intrusive doubly-linked list.**
///   Both ends stay O(1), block lookup by index is a walk either way, and the
///   code stays in safe Rust with no node pointers to corrupt.
/// * **No node compression.** Redis can LZF-compress interior nodes
///   (`list-compress-depth`); its default is 0, i.e. off, and FrogDB does not
///   implement the tier at all. Revisit only with a workload that wants it.
/// * **No per-list config.** Block limits are the [`QuicklistLimits`]
///   constants rather than `list-max-listpack-size` at runtime.
#[derive(Debug, Clone)]
pub struct ListValue {
    blocks: VecDeque<Block>,
    /// Total element count across all blocks, kept in step with every edit.
    len: usize,
    limits: QuicklistLimits,
}

impl Default for ListValue {
    fn default() -> Self {
        Self::new()
    }
}

impl ListValue {
    /// Create a new empty list.
    pub fn new() -> Self {
        Self {
            blocks: VecDeque::new(),
            len: 0,
            limits: QuicklistLimits::DEFAULT_LIST,
        }
    }

    /// Get the number of elements.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if the list is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Number of blocks in the chain (introspection and memory-shape tests).
    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    /// Number of plain (single oversized element) blocks in the chain.
    pub fn plain_block_count(&self) -> usize {
        self.blocks
            .iter()
            .filter(|b| matches!(b, Block::Plain(_)))
            .count()
    }

    /// Whether an element is too large to share a packed block.
    #[inline]
    fn is_large(&self, value_len: usize) -> bool {
        Listpack::entry_size(value_len) > self.limits.max_bytes
    }

    /// Whether `value` still fits packed block `lp`.
    #[inline]
    fn fits(&self, lp: &Listpack, value_len: usize) -> bool {
        lp.len() < self.limits.max_entries
            && lp.byte_len() + Listpack::entry_size(value_len) <= self.limits.max_bytes
    }

    /// Whether two adjacent packed blocks can be combined into one.
    #[inline]
    fn can_merge(&self, a: &Block, b: &Block) -> bool {
        match (a, b) {
            (Block::Packed(x), Block::Packed(y)) => {
                x.len() + y.len() <= self.limits.max_entries
                    && x.byte_len() + y.byte_len() <= self.limits.max_bytes
            }
            _ => false,
        }
    }

    /// Push an element to the front (left).
    pub fn push_front(&mut self, value: Bytes) {
        if self.is_large(value.len()) {
            self.blocks.push_front(Block::Plain(value));
        } else {
            match self.blocks.front_mut() {
                Some(Block::Packed(lp)) if lp.len() < self.limits.max_entries => {
                    // Re-check the byte budget without holding the borrow.
                    if lp.byte_len() + Listpack::entry_size(value.len()) <= self.limits.max_bytes {
                        lp.push_front(&value);
                    } else {
                        let mut lp = Listpack::new();
                        lp.push_back(&value);
                        self.blocks.push_front(Block::Packed(lp));
                    }
                }
                _ => {
                    let mut lp = Listpack::new();
                    lp.push_back(&value);
                    self.blocks.push_front(Block::Packed(lp));
                }
            }
        }
        self.len += 1;
    }

    /// Push an element to the back (right).
    pub fn push_back(&mut self, value: Bytes) {
        if self.is_large(value.len()) {
            self.blocks.push_back(Block::Plain(value));
        } else {
            match self.blocks.back_mut() {
                Some(Block::Packed(lp)) if lp.len() < self.limits.max_entries => {
                    if lp.byte_len() + Listpack::entry_size(value.len()) <= self.limits.max_bytes {
                        lp.push_back(&value);
                    } else {
                        let mut lp = Listpack::new();
                        lp.push_back(&value);
                        self.blocks.push_back(Block::Packed(lp));
                    }
                }
                _ => {
                    let mut lp = Listpack::new();
                    lp.push_back(&value);
                    self.blocks.push_back(Block::Packed(lp));
                }
            }
        }
        self.len += 1;
    }

    /// Pop an element from the front (left).
    pub fn pop_front(&mut self) -> Option<Bytes> {
        let plain = matches!(self.blocks.front()?, Block::Plain(_));
        let value = if plain {
            match self.blocks.pop_front() {
                Some(Block::Plain(b)) => b,
                _ => unreachable!("front block was plain"),
            }
        } else {
            let Some(Block::Packed(lp)) = self.blocks.front_mut() else {
                unreachable!("front block was packed")
            };
            let value = Bytes::copy_from_slice(lp.first().expect("packed block is non-empty"));
            lp.remove(0);
            if lp.is_empty() {
                self.blocks.pop_front();
            }
            value
        };
        self.len -= 1;
        self.merge_around(0);
        Some(value)
    }

    /// Pop an element from the back (right).
    pub fn pop_back(&mut self) -> Option<Bytes> {
        let plain = matches!(self.blocks.back()?, Block::Plain(_));
        let value = if plain {
            match self.blocks.pop_back() {
                Some(Block::Plain(b)) => b,
                _ => unreachable!("back block was plain"),
            }
        } else {
            let Some(Block::Packed(lp)) = self.blocks.back_mut() else {
                unreachable!("back block was packed")
            };
            let value = Bytes::copy_from_slice(lp.last().expect("packed block is non-empty"));
            lp.remove(lp.len() - 1);
            if lp.is_empty() {
                self.blocks.pop_back();
            }
            value
        };
        self.len -= 1;
        self.merge_around(self.blocks.len().saturating_sub(1));
        Some(value)
    }

    /// Merge block `i` with its neighbours when the combined block still fits.
    ///
    /// Redis does the same after deletions (`_quicklistMergeNodes`): it keeps
    /// the chain from degenerating into many nearly-empty blocks.
    fn merge_around(&mut self, i: usize) {
        if self.blocks.is_empty() {
            return;
        }
        let i = i.min(self.blocks.len() - 1);
        // Merge with the following block first so a single pass can absorb both
        // neighbours into block `i`.
        if i + 1 < self.blocks.len() && self.can_merge(&self.blocks[i], &self.blocks[i + 1]) {
            let next = self.blocks.remove(i + 1).expect("index checked above");
            if let (Block::Packed(dst), Block::Packed(src)) = (&mut self.blocks[i], &next) {
                dst.append(src);
            }
        }
        if i > 0 && self.can_merge(&self.blocks[i - 1], &self.blocks[i]) {
            let cur = self.blocks.remove(i).expect("index checked above");
            if let (Block::Packed(dst), Block::Packed(src)) = (&mut self.blocks[i - 1], &cur) {
                dst.append(src);
            }
        }
    }

    /// Locate the block holding absolute `index`, as `(block, offset)`.
    ///
    /// Walks the chain from whichever end is closer using the cached per-block
    /// counts, so the cost is O(blocks) pointer-free arithmetic plus one
    /// in-block scan.
    fn locate(&self, index: usize) -> Option<(usize, usize)> {
        if index >= self.len {
            return None;
        }
        if index <= self.len / 2 {
            let mut seen = 0;
            for (b, block) in self.blocks.iter().enumerate() {
                let n = block.len();
                if index < seen + n {
                    return Some((b, index - seen));
                }
                seen += n;
            }
        } else {
            let mut seen = self.len;
            for (b, block) in self.blocks.iter().enumerate().rev() {
                let n = block.len();
                seen -= n;
                if index >= seen {
                    return Some((b, index - seen));
                }
            }
        }
        None
    }

    /// Normalize a Redis index (supports negative indices).
    fn normalize_index(&self, index: i64) -> Option<usize> {
        let len = self.len() as i64;
        if len == 0 {
            return None;
        }
        let normalized = if index < 0 { len + index } else { index };
        if normalized < 0 || normalized >= len {
            None
        } else {
            Some(normalized as usize)
        }
    }

    /// Get an element by index (supports negative indices).
    pub fn get(&self, index: i64) -> Option<&[u8]> {
        let i = self.normalize_index(index)?;
        let (b, off) = self.locate(i)?;
        self.blocks[b].get(off)
    }

    /// Set an element by index (supports negative indices).
    ///
    /// Returns true if the index was valid and the element was set.
    pub fn set(&mut self, index: i64, value: Bytes) -> bool {
        let Some(i) = self.normalize_index(index) else {
            return false;
        };
        self.remove_at(i);
        self.insert_at(i, value);
        true
    }

    /// Insert `value` so that it lands at absolute position `index`.
    fn insert_at(&mut self, index: usize, value: Bytes) {
        if index >= self.len {
            self.push_back(value);
            return;
        }
        if index == 0 {
            self.push_front(value);
            return;
        }
        let (b, off) = self.locate(index).expect("index < len");

        // The fast path: the element still fits the block it lands in.
        if !self.is_large(value.len())
            && let Block::Packed(lp) = &self.blocks[b]
            && self.fits(lp, value.len())
        {
            let Block::Packed(lp) = &mut self.blocks[b] else {
                unreachable!("block b was packed")
            };
            lp.insert(off, &value);
            self.len += 1;
            return;
        }

        // Otherwise split the block at the insertion point so `off` becomes a
        // block boundary, and drop the element in as its own block. The merge
        // pass reabsorbs it into a neighbour whenever that still fits.
        self.split_block_at(b, off);
        let at = if off == 0 { b } else { b + 1 };
        let block = if self.is_large(value.len()) {
            Block::Plain(value)
        } else {
            let mut fresh = Listpack::new();
            fresh.push_back(&value);
            Block::Packed(fresh)
        };
        self.blocks.insert(at, block);
        self.len += 1;
        self.merge_around(at);
    }

    /// Split packed block `b` so that offset `off` starts a new block.
    ///
    /// A no-op when `off` is already a block boundary or the block is plain.
    fn split_block_at(&mut self, b: usize, off: usize) {
        if off == 0 {
            return;
        }
        let tail = match &mut self.blocks[b] {
            Block::Packed(lp) => {
                if off >= lp.len() {
                    return;
                }
                lp.split_off(off)
            }
            Block::Plain(_) => return,
        };
        self.blocks.insert(b + 1, Block::Packed(tail));
    }

    /// Remove the element at absolute position `index`.
    fn remove_at(&mut self, index: usize) -> bool {
        let Some((b, off)) = self.locate(index) else {
            return false;
        };
        let now_empty = match &mut self.blocks[b] {
            Block::Packed(lp) => {
                lp.remove(off);
                lp.is_empty()
            }
            Block::Plain(_) => true,
        };
        self.len -= 1;
        if now_empty {
            self.blocks.remove(b);
            self.merge_around(b.saturating_sub(1));
        } else {
            self.merge_around(b);
        }
        true
    }

    /// Resolve start/end into (skip, take) counts. Returns (0, 0) for empty ranges.
    fn resolve_range(&self, start: i64, end: i64) -> (usize, usize) {
        let len = self.len() as i64;
        if len == 0 {
            return (0, 0);
        }

        let start = if start < 0 {
            (len + start).max(0) as usize
        } else {
            start.min(len) as usize
        };

        let end = if end < 0 {
            (len + end).max(-1)
        } else {
            end.min(len - 1)
        };

        if end < 0 || start > end as usize {
            return (0, 0);
        }

        (start, end as usize - start + 1)
    }

    /// Get a range of elements (inclusive, supports negative indices).
    pub fn range(&self, start: i64, end: i64) -> Vec<Bytes> {
        self.range_iter(start, end)
            .map(Bytes::copy_from_slice)
            .collect()
    }

    /// Iterate over a range of elements without intermediate allocation.
    pub fn range_iter(&self, start: i64, end: i64) -> impl Iterator<Item = &[u8]> {
        let (skip, take) = self.resolve_range(start, end);
        self.iter().skip(skip).take(take)
    }

    /// Trim the list to only contain elements in the specified range.
    pub fn trim(&mut self, start: i64, end: i64) {
        let len = self.len() as i64;
        if len == 0 {
            return;
        }

        // Convert negative indices
        let start = if start < 0 {
            (len + start).max(0) as usize
        } else {
            start.min(len) as usize
        };

        let end = if end < 0 {
            (len + end).max(-1)
        } else {
            end.min(len - 1)
        };

        if end < 0 || start > end as usize {
            // Empty range - clear the list
            self.blocks.clear();
            self.len = 0;
            return;
        }

        let end = end as usize;
        self.drop_back(self.len - (end + 1));
        self.drop_front(start);
    }

    /// Drop the first `n` elements, whole blocks at a time.
    fn drop_front(&mut self, n: usize) {
        let mut remaining = n.min(self.len);
        self.len -= remaining;
        while remaining > 0 {
            let Some(block) = self.blocks.front_mut() else {
                break;
            };
            let block_len = block.len();
            if block_len <= remaining {
                remaining -= block_len;
                self.blocks.pop_front();
            } else if let Block::Packed(lp) = block {
                lp.drain_front(remaining);
                remaining = 0;
            } else {
                unreachable!("plain block holds one element, covered above");
            }
        }
        self.merge_around(0);
    }

    /// Drop the last `n` elements, whole blocks at a time.
    fn drop_back(&mut self, n: usize) {
        let mut remaining = n.min(self.len);
        self.len -= remaining;
        while remaining > 0 {
            let Some(block) = self.blocks.back_mut() else {
                break;
            };
            let block_len = block.len();
            if block_len <= remaining {
                remaining -= block_len;
                self.blocks.pop_back();
            } else if let Block::Packed(lp) = block {
                lp.drain_back(remaining);
                remaining = 0;
            } else {
                unreachable!("plain block holds one element, covered above");
            }
        }
        self.merge_around(self.blocks.len().saturating_sub(1));
    }

    /// Find the position of an element.
    ///
    /// Returns the first position where element is found, or None.
    /// `rank`: how many matches to skip (0 = first, 1 = second, etc.)
    /// `count`: maximum number of positions to return
    /// `maxlen`: maximum number of elements to scan
    pub fn position(
        &self,
        element: &[u8],
        rank: i64,
        count: usize,
        maxlen: Option<usize>,
    ) -> Vec<usize> {
        let maxlen = maxlen.unwrap_or(self.len());

        if rank >= 0 {
            // Forward scan
            let rank = rank as usize;
            let mut matches = 0;
            let mut positions = Vec::new();

            for (i, item) in self.iter().enumerate().take(maxlen) {
                if item == element {
                    if matches >= rank {
                        positions.push(i);
                        if positions.len() >= count {
                            break;
                        }
                    }
                    matches += 1;
                }
            }
            positions
        } else {
            // Backward scan
            let rank = (-rank - 1) as usize;
            let mut matches = 0;
            let mut positions = Vec::new();
            let scan_start = self.len().saturating_sub(maxlen);

            for (offset, item) in self.iter_rev().enumerate() {
                let i = self.len() - 1 - offset;
                if i < scan_start {
                    break;
                }
                if item == element {
                    if matches >= rank {
                        positions.push(i);
                        if positions.len() >= count {
                            break;
                        }
                    }
                    matches += 1;
                }
            }
            positions
        }
    }

    /// Insert an element before or after a pivot element.
    ///
    /// Returns the new length of the list, -1 if pivot not found, 0 if list is empty.
    pub fn insert(&mut self, before: bool, pivot: &[u8], element: Bytes) -> i64 {
        if self.is_empty() {
            return 0;
        }

        let Some(i) = self.iter().position(|e| e == pivot) else {
            return -1;
        };
        let insert_pos = if before { i } else { i + 1 };
        self.insert_at(insert_pos, element);
        self.len() as i64
    }

    /// Remove elements equal to value.
    ///
    /// `count` determines direction and number:
    /// - count > 0: Remove first count occurrences (head to tail)
    /// - count < 0: Remove first |count| occurrences (tail to head)
    /// - count = 0: Remove all occurrences
    ///
    /// Returns the number of elements removed.
    pub fn remove(&mut self, count: i64, element: &[u8]) -> usize {
        if self.is_empty() {
            return 0;
        }

        let limit = if count == 0 {
            usize::MAX
        } else {
            count.unsigned_abs() as usize
        };

        // Record matches as (block, offset) while scanning — the scan already
        // knows both, so deletion needs no second lookup. Deleting in
        // descending order keeps every not-yet-applied coordinate valid, even
        // when a block empties out and leaves the chain.
        let mut targets: Vec<(usize, usize)> = Vec::new();
        if count < 0 {
            'outer: for (b, block) in self.blocks.iter().enumerate().rev() {
                let n = block.len();
                for (k, item) in block_iter_rev(block).enumerate() {
                    if item == element {
                        targets.push((b, n - 1 - k));
                        if targets.len() >= limit {
                            break 'outer;
                        }
                    }
                }
            }
        } else {
            'outer: for (b, block) in self.blocks.iter().enumerate() {
                for (off, item) in block_iter(block).enumerate() {
                    if item == element {
                        targets.push((b, off));
                        if targets.len() >= limit {
                            break 'outer;
                        }
                    }
                }
            }
            targets.reverse();
        }

        let removed = targets.len();
        // Both index lists stay descending, matching the deletion order.
        let mut dropped: Vec<usize> = Vec::new();
        let mut touched: Vec<usize> = Vec::new();
        for (b, off) in targets {
            let now_empty = match &mut self.blocks[b] {
                Block::Packed(lp) => {
                    lp.remove(off);
                    lp.is_empty()
                }
                Block::Plain(_) => true,
            };
            if now_empty {
                self.blocks.remove(b);
                dropped.push(b);
                touched.retain(|t| *t != b);
            } else if touched.last() != Some(&b) {
                touched.push(b);
            }
        }
        self.len -= removed;

        // Merge only around blocks that actually shrank, so an LREM matching
        // nothing — or a few entries in one block — never turns into a
        // full-chain compaction. Correct each surviving index for the blocks
        // that left the chain below it.
        for b in &mut touched {
            *b -= dropped.iter().filter(|d| **d < *b).count();
        }
        for b in touched {
            self.merge_around(b);
        }
        removed
    }

    /// Calculate approximate memory size.
    ///
    /// Counts the encoded bytes actually held by each block plus a fixed
    /// per-block overhead, never the buffers' spare capacity — so the figure
    /// depends only on the elements and how they are partitioned, and two runs
    /// of the same workload report the same number (the run-stability rule
    /// [`crate::skiplist`] documents).
    pub fn memory_size(&self) -> usize {
        const BLOCK_OVERHEAD: usize = std::mem::size_of::<Block>();
        let blocks_size: usize = self
            .blocks
            .iter()
            .map(|b| BLOCK_OVERHEAD + b.byte_len())
            .sum();
        std::mem::size_of::<Self>() + blocks_size
    }

    /// Get all elements as a vec for serialization.
    pub fn to_vec(&self) -> Vec<Bytes> {
        self.iter().map(Bytes::copy_from_slice).collect()
    }

    /// Iterate over all elements, front to back.
    pub fn iter(&self) -> impl Iterator<Item = &[u8]> + '_ {
        self.blocks.iter().flat_map(block_iter)
    }

    /// Iterate over all elements, back to front.
    pub fn iter_rev(&self) -> impl Iterator<Item = &[u8]> + '_ {
        self.blocks.iter().rev().flat_map(block_iter_rev)
    }
}

/// Elements of one block, front to back.
fn block_iter(block: &Block) -> BlockIter<'_> {
    match block {
        Block::Packed(lp) => BlockIter::Packed(lp.iter()),
        Block::Plain(b) => BlockIter::Plain(std::iter::once(b.as_ref())),
    }
}

/// Elements of one block, back to front.
fn block_iter_rev(block: &Block) -> BlockRevIter<'_> {
    match block {
        Block::Packed(lp) => BlockRevIter::Packed(lp.iter_rev()),
        Block::Plain(b) => BlockRevIter::Plain(std::iter::once(b.as_ref())),
    }
}

/// Per-block element iterator, forward.
enum BlockIter<'a> {
    Packed(crate::listpack::ListpackIter<'a>),
    Plain(std::iter::Once<&'a [u8]>),
}

impl<'a> Iterator for BlockIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        match self {
            BlockIter::Packed(it) => it.next(),
            BlockIter::Plain(it) => it.next(),
        }
    }
}

/// Per-block element iterator, backward.
enum BlockRevIter<'a> {
    Packed(crate::listpack::ListpackRevIter<'a>),
    Plain(std::iter::Once<&'a [u8]>),
}

impl<'a> Iterator for BlockRevIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        match self {
            BlockRevIter::Packed(it) => it.next(),
            BlockRevIter::Plain(it) => it.next(),
        }
    }
}

#[cfg(test)]
mod quicklist_tests {
    use super::*;
    use crate::types::QuicklistLimits;

    fn small(i: usize) -> Bytes {
        Bytes::from(format!("e{i:04}"))
    }

    /// A list of many small elements must occupy O(blocks), not O(elements),
    /// heap objects — the whole point of R7.
    #[test]
    fn many_small_elements_pack_into_few_blocks() {
        let mut list = ListValue::new();
        for i in 0..1000 {
            list.push_back(small(i));
        }
        assert_eq!(list.len(), 1000);
        let expected = 1000usize.div_ceil(QuicklistLimits::DEFAULT_LIST.max_entries);
        assert_eq!(list.block_count(), expected);
    }

    /// An element larger than the block byte cap gets its own plain block
    /// instead of forcing its neighbours apart.
    #[test]
    fn oversized_element_gets_a_plain_block() {
        let mut list = ListValue::new();
        list.push_back(small(0));
        list.push_back(Bytes::from(vec![
            b'x';
            QuicklistLimits::DEFAULT_LIST.max_bytes + 1
        ]));
        list.push_back(small(1));
        assert_eq!(list.block_count(), 3);
        assert_eq!(list.plain_block_count(), 1);
        assert_eq!(list.len(), 3);
    }

    /// Deleting down to underfull neighbours merges blocks back together.
    #[test]
    fn removal_merges_underfull_neighbours() {
        let mut list = ListValue::new();
        for i in 0..(QuicklistLimits::DEFAULT_LIST.max_entries * 2) {
            list.push_back(small(i));
        }
        assert_eq!(list.block_count(), 2);
        // Drop half of the first block; the two blocks now fit in one.
        for _ in 0..(QuicklistLimits::DEFAULT_LIST.max_entries) {
            list.pop_front();
        }
        assert_eq!(list.block_count(), 1);
    }
}
