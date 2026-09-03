//! Index-based skip list with span-based O(log n) rank queries.
//!
//! Modeled after Redis's zskiplist. Safe Rust, no raw pointers — uses `Vec<Option<Node>>`
//! with a free list for node storage.
//!
//! Nodes do not own member bytes: each node carries the caller's `u32` slot id,
//! and every operation that must compare members takes a `resolve` closure
//! mapping a slot to its bytes. The owning [`SortedSetValue`] keeps the bytes
//! in a [`BlockStore`] and resolves slots through its member table, so the
//! skip list stays a plain owned value that moves between shards while the
//! bytes live exactly once.
//!
//! [`SortedSetValue`]: crate::types::SortedSetValue
//! [`BlockStore`]: crate::blockstore::BlockStore

use ordered_float::OrderedFloat;
use rand::rngs::SmallRng;
use rand::{Rng, RngExt, SeedableRng};
use smallvec::SmallVec;
use std::cmp::Ordering;

const NIL: u32 = u32::MAX;
const MAX_LEVEL: usize = 32;
const P: f64 = 0.25;

/// Seed for every list's level generator.
///
/// Node levels are drawn from a geometric distribution to keep the list
/// balanced; nothing about the draw depends on the data, and no client can
/// observe an individual node's level. What a client *can* observe is the total
/// — `MEMORY USAGE`, `INFO memory`, and the memory-conservation checker all
/// read `memory_size()`, which counts one `Link` per level. Drawing from the
/// process-global generator therefore made a sorted set's reported size differ
/// between two otherwise identical runs, which is the one thing the
/// generated-workload harness is not allowed to do. A fixed seed keeps the
/// distribution and gives every run the same structure.
const LEVEL_SEED: u64 = 0x5C1D_71C5_7EED_0001;

/// Size of a single skip list `Node` in bytes (exposed for DEBUG STRUCTSIZE).
pub const NODE_SIZE: usize = std::mem::size_of::<Node>();

/// Index-based skip list with span-based O(log n) rank.
#[derive(Debug, Clone)]
pub struct SkipList {
    nodes: Vec<Option<Node>>,
    free: Vec<u32>,
    head: u32,
    tail: u32,
    length: usize,
    level: usize,
    /// Per-list level generator (see [`LEVEL_SEED`]).
    rng: SmallRng,
}

#[derive(Debug, Clone)]
struct Node {
    score: OrderedFloat<f64>,
    /// Caller-owned member slot; resolved to bytes via the `resolve` closures.
    slot: u32,
    levels: SmallVec<[Link; 4]>,
    backward: u32,
}

#[derive(Debug, Clone, Copy)]
struct Link {
    forward: u32,
    span: u32,
}

fn random_level(rng: &mut impl Rng) -> usize {
    let mut lvl = 1;
    while lvl < MAX_LEVEL && rng.random::<f64>() < P {
        lvl += 1;
    }
    lvl
}

/// Compare (score, member) pairs in the skip list ordering.
#[inline]
fn cmp_key(s1: OrderedFloat<f64>, m1: &[u8], s2: OrderedFloat<f64>, m2: &[u8]) -> Ordering {
    s1.cmp(&s2).then_with(|| m1.cmp(m2))
}

impl Default for SkipList {
    fn default() -> Self {
        Self::new()
    }
}

impl SkipList {
    pub fn new() -> Self {
        // Allocate sentinel head node at index 0
        let head_node = Node {
            score: OrderedFloat(0.0),
            slot: NIL,
            levels: SmallVec::from_elem(
                Link {
                    forward: NIL,
                    span: 0,
                },
                MAX_LEVEL,
            ),
            backward: NIL,
        };
        Self {
            nodes: vec![Some(head_node)],
            free: Vec::new(),
            head: 0,
            tail: NIL,
            length: 0,
            level: 1,
            rng: SmallRng::seed_from_u64(LEVEL_SEED),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.length
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    fn alloc_node(&mut self, score: OrderedFloat<f64>, slot: u32, level: usize) -> u32 {
        let node = Node {
            score,
            slot,
            levels: SmallVec::from_elem(
                Link {
                    forward: NIL,
                    span: 0,
                },
                level,
            ),
            backward: NIL,
        };
        if let Some(idx) = self.free.pop() {
            self.nodes[idx as usize] = Some(node);
            idx
        } else {
            let idx = self.nodes.len() as u32;
            self.nodes.push(Some(node));
            idx
        }
    }

    fn free_node(&mut self, idx: u32) {
        self.nodes[idx as usize] = None;
        self.free.push(idx);
    }

    #[inline]
    fn node(&self, idx: u32) -> &Node {
        self.nodes[idx as usize].as_ref().unwrap()
    }

    #[inline]
    fn node_mut(&mut self, idx: u32) -> &mut Node {
        self.nodes[idx as usize].as_mut().unwrap()
    }

    /// Insert a (score, member) pair identified by `slot`, whose bytes are
    /// `member`. Returns false if the exact pair already exists.
    #[allow(clippy::needless_range_loop)]
    pub fn insert<'m>(
        &mut self,
        score: OrderedFloat<f64>,
        slot: u32,
        member: &[u8],
        resolve: impl Fn(u32) -> &'m [u8],
    ) -> bool {
        // update[i] = last node at level i before the insertion point
        // rank[i]   = cumulative rank at that node
        let mut update = [0u32; MAX_LEVEL];
        let mut rank = [0u32; MAX_LEVEL];

        let mut x = self.head;
        for i in (0..self.level).rev() {
            rank[i] = if i + 1 < self.level { rank[i + 1] } else { 0 };
            loop {
                let fwd = self.node(x).levels[i].forward;
                if fwd == NIL {
                    break;
                }
                let fwd_node = self.node(fwd);
                match cmp_key(fwd_node.score, resolve(fwd_node.slot), score, member) {
                    Ordering::Less => {
                        rank[i] += self.node(x).levels[i].span;
                        x = fwd;
                    }
                    Ordering::Equal => return false, // exact duplicate
                    Ordering::Greater => break,
                }
            }
            update[i] = x;
        }

        let lvl = random_level(&mut self.rng);

        // Grow skip list level if needed
        if lvl > self.level {
            for i in self.level..lvl {
                rank[i] = 0;
                update[i] = self.head;
                self.node_mut(self.head).levels[i].span = self.length as u32;
            }
            self.level = lvl;
        }

        let new_idx = self.alloc_node(score, slot, lvl);

        // Splice into each level
        for i in 0..lvl {
            let old_fwd = self.node(update[i]).levels[i].forward;
            let old_span = self.node(update[i]).levels[i].span;

            self.node_mut(new_idx).levels[i].forward = old_fwd;
            self.node_mut(update[i]).levels[i].forward = new_idx;

            // span = (rank[0] + 1 is the new node's rank)
            // new node's span at level i = old_span - (rank[0] - rank[i])
            self.node_mut(new_idx).levels[i].span = old_span.saturating_sub(rank[0] - rank[i]);
            self.node_mut(update[i]).levels[i].span = (rank[0] - rank[i]) + 1;
        }

        // Increment span for untouched higher levels
        for i in lvl..self.level {
            self.node_mut(update[i]).levels[i].span += 1;
        }

        // Set backward pointer
        let bw = if update[0] == self.head {
            NIL
        } else {
            update[0]
        };
        self.node_mut(new_idx).backward = bw;

        // Update forward node's backward pointer, or set tail
        let fwd_at_0 = self.node(new_idx).levels[0].forward;
        if fwd_at_0 != NIL {
            self.node_mut(fwd_at_0).backward = new_idx;
        } else {
            self.tail = new_idx;
        }

        self.length += 1;
        true
    }

    /// Remove the (score, member) pair. Returns true if found and removed.
    #[allow(clippy::needless_range_loop)]
    pub fn remove<'m>(
        &mut self,
        score: OrderedFloat<f64>,
        member: &[u8],
        resolve: impl Fn(u32) -> &'m [u8],
    ) -> bool {
        let mut update = [0u32; MAX_LEVEL];

        let mut x = self.head;
        for i in (0..self.level).rev() {
            loop {
                let fwd = self.node(x).levels[i].forward;
                if fwd == NIL {
                    break;
                }
                let fwd_node = self.node(fwd);
                if cmp_key(fwd_node.score, resolve(fwd_node.slot), score, member) == Ordering::Less
                {
                    x = fwd;
                } else {
                    break;
                }
            }
            update[i] = x;
        }

        // Check if the element actually exists
        let target = self.node(update[0]).levels[0].forward;
        if target == NIL {
            return false;
        }
        let target_node = self.node(target);
        if target_node.score != score || resolve(target_node.slot) != member {
            return false;
        }

        self.delete_node(target, &update);
        self.free_node(target);
        true
    }

    #[allow(clippy::needless_range_loop)]
    fn delete_node(&mut self, idx: u32, update: &[u32; MAX_LEVEL]) {
        let node_level = self.node(idx).levels.len();

        for i in 0..self.level {
            if i < node_level && self.node(update[i]).levels[i].forward == idx {
                let node_span = self.node(idx).levels[i].span;
                let update_span = self.node(update[i]).levels[i].span;
                // Combined span minus 1 for the removed node
                self.node_mut(update[i]).levels[i].span =
                    (update_span + node_span).saturating_sub(1);
                self.node_mut(update[i]).levels[i].forward = self.node(idx).levels[i].forward;
            } else {
                self.node_mut(update[i]).levels[i].span =
                    self.node(update[i]).levels[i].span.saturating_sub(1);
            }
        }

        let fwd_at_0 = self.node(idx).levels[0].forward;
        if fwd_at_0 != NIL {
            self.node_mut(fwd_at_0).backward = self.node(idx).backward;
        } else {
            // Removing the tail
            let bw = self.node(idx).backward;
            self.tail = if bw == NIL { NIL } else { bw };
        }

        // Shrink level if top levels became empty
        while self.level > 1 && self.node(self.head).levels[self.level - 1].forward == NIL {
            self.level -= 1;
        }
        self.length -= 1;
    }

    /// Get the 0-based rank of the (score, member) pair. Returns None if not found.
    pub fn rank<'m>(
        &self,
        score: OrderedFloat<f64>,
        member: &[u8],
        resolve: impl Fn(u32) -> &'m [u8],
    ) -> Option<usize> {
        let mut rank = 0u32;
        let mut x = self.head;

        for i in (0..self.level).rev() {
            loop {
                let fwd = self.node(x).levels[i].forward;
                if fwd == NIL {
                    break;
                }
                let fwd_node = self.node(fwd);
                match cmp_key(fwd_node.score, resolve(fwd_node.slot), score, member) {
                    Ordering::Less => {
                        rank += self.node(x).levels[i].span;
                        x = fwd;
                    }
                    Ordering::Equal => {
                        rank += self.node(x).levels[i].span;
                        return Some((rank - 1) as usize); // 0-based
                    }
                    Ordering::Greater => break,
                }
            }
        }
        None
    }

    /// Get the element at the given 0-based rank. Returns None if out of bounds.
    pub fn get_by_rank(&self, rank: usize) -> Option<(OrderedFloat<f64>, u32)> {
        if rank >= self.length {
            return None;
        }
        let target = (rank + 1) as u32; // spans are 1-based
        let mut traversed = 0u32;
        let mut x = self.head;

        for i in (0..self.level).rev() {
            loop {
                let fwd = self.node(x).levels[i].forward;
                if fwd == NIL {
                    break;
                }
                let next_traversed = traversed + self.node(x).levels[i].span;
                if next_traversed > target {
                    break;
                }
                traversed = next_traversed;
                x = fwd;
                if traversed == target {
                    let node = self.node(x);
                    return Some((node.score, node.slot));
                }
            }
        }
        None
    }

    /// Pop the first (minimum) element.
    pub fn pop_first(&mut self) -> Option<(OrderedFloat<f64>, u32)> {
        let first = self.node(self.head).levels[0].forward;
        if first == NIL {
            return None;
        }
        let score = self.node(first).score;
        let slot = self.node(first).slot;

        // For the first element, head is the predecessor at all levels
        let update = [self.head; MAX_LEVEL];

        self.delete_node(first, &update);
        self.free_node(first);
        Some((score, slot))
    }

    /// Pop the last (maximum) element.
    pub fn pop_last<'m>(
        &mut self,
        resolve: impl Fn(u32) -> &'m [u8],
    ) -> Option<(OrderedFloat<f64>, u32)> {
        if self.tail == NIL {
            return None;
        }
        let score = self.node(self.tail).score;
        let slot = self.node(self.tail).slot;
        self.remove(score, resolve(slot), &resolve);
        Some((score, slot))
    }

    /// Iterate forward from the first element.
    pub fn iter(&self) -> SkipListIter<'_> {
        SkipListIter {
            list: self,
            current: self.node(self.head).levels[0].forward,
        }
    }

    /// Iterate backward from the last element.
    pub fn rev_iter(&self) -> SkipListRevIter<'_> {
        SkipListRevIter {
            list: self,
            current: self.tail,
        }
    }

    /// Iterate forward over elements starting at the given score bound.
    pub fn range_by_score(
        &self,
        min_score: OrderedFloat<f64>,
        min_inclusive: bool,
    ) -> SkipListIter<'_> {
        // Find first node >= min (or > min if exclusive)
        let mut x = self.head;
        for i in (0..self.level).rev() {
            loop {
                let fwd = self.node(x).levels[i].forward;
                if fwd == NIL {
                    break;
                }
                let fwd_node = self.node(fwd);
                let should_advance = if min_inclusive {
                    fwd_node.score < min_score
                } else {
                    fwd_node.score <= min_score
                };
                if should_advance {
                    x = fwd;
                } else {
                    break;
                }
            }
        }
        // x is the last node before our range; x.forward[0] is the first in range
        SkipListIter {
            list: self,
            current: self.node(x).levels[0].forward,
        }
    }

    /// Seek to the given 0-based rank and return an iterator starting there.
    pub fn range_by_rank_iter(&self, start_rank: usize) -> SkipListIter<'_> {
        if start_rank >= self.length {
            return SkipListIter {
                list: self,
                current: NIL,
            };
        }
        let target = (start_rank + 1) as u32;
        let mut traversed = 0u32;
        let mut x = self.head;
        for i in (0..self.level).rev() {
            loop {
                let fwd = self.node(x).levels[i].forward;
                if fwd == NIL {
                    break;
                }
                let next = traversed + self.node(x).levels[i].span;
                if next > target {
                    break;
                }
                traversed = next;
                x = fwd;
                if traversed == target {
                    return SkipListIter {
                        list: self,
                        current: x,
                    };
                }
            }
        }
        // Unreachable when start_rank < length: every rank has a node.
        SkipListIter {
            list: self,
            current: NIL,
        }
    }

    /// Approximate memory usage in bytes. Member bytes are owned by the
    /// caller's block store and counted there, not here.
    pub fn memory_size(&self) -> usize {
        let base = std::mem::size_of::<Self>();
        let nodes_vec = self.nodes.capacity() * std::mem::size_of::<Option<Node>>();
        let free_vec = self.free.capacity() * std::mem::size_of::<u32>();
        let spilled_links: usize = self
            .nodes
            .iter()
            .flatten()
            .map(|node| {
                if node.levels.spilled() {
                    node.levels.len() * std::mem::size_of::<Link>()
                } else {
                    0
                }
            })
            .sum();
        base + nodes_vec + free_vec + spilled_links
    }
}

/// Forward iterator over skip list elements, yielding (score, slot).
pub struct SkipListIter<'a> {
    list: &'a SkipList,
    current: u32,
}

impl<'a> Iterator for SkipListIter<'a> {
    type Item = (OrderedFloat<f64>, u32);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current == NIL {
            return None;
        }
        let node = self.list.node(self.current);
        let result = (node.score, node.slot);
        self.current = node.levels[0].forward;
        Some(result)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.list.length))
    }
}

/// Reverse iterator over skip list elements, yielding (score, slot).
pub struct SkipListRevIter<'a> {
    list: &'a SkipList,
    current: u32,
}

impl<'a> Iterator for SkipListRevIter<'a> {
    type Item = (OrderedFloat<f64>, u32);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current == NIL {
            return None;
        }
        let node = self.list.node(self.current);
        let result = (node.score, node.slot);
        self.current = node.backward;
        Some(result)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.list.length))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test-side member table: slot = index into a Vec of member bytes, the
    /// way `SortedSetValue` resolves slots through its block store.
    #[derive(Default)]
    struct Members {
        v: Vec<Vec<u8>>,
    }

    impl Members {
        fn slot(&mut self, s: &str) -> u32 {
            if let Some(i) = self.v.iter().position(|m| m == s.as_bytes()) {
                return i as u32;
            }
            self.v.push(s.as_bytes().to_vec());
            (self.v.len() - 1) as u32
        }

        fn resolve<'s>(&'s self) -> impl Fn(u32) -> &'s [u8] {
            move |slot| self.v[slot as usize].as_slice()
        }

        fn name(&self, slot: u32) -> &[u8] {
            &self.v[slot as usize]
        }
    }

    fn insert(sl: &mut SkipList, m: &mut Members, score: f64, s: &str) -> bool {
        let slot = m.slot(s);
        sl.insert(OrderedFloat(score), slot, s.as_bytes(), m.resolve())
    }

    fn remove(sl: &mut SkipList, m: &Members, score: f64, s: &str) -> bool {
        sl.remove(OrderedFloat(score), s.as_bytes(), m.resolve())
    }

    fn rank(sl: &SkipList, m: &Members, score: f64, s: &str) -> Option<usize> {
        sl.rank(OrderedFloat(score), s.as_bytes(), m.resolve())
    }

    fn collect(sl: &SkipList, m: &Members) -> Vec<(f64, Vec<u8>)> {
        sl.iter()
            .map(|(s, slot)| (s.0, m.name(slot).to_vec()))
            .collect()
    }

    #[test]
    fn test_insert_and_len() {
        let mut sl = SkipList::new();
        let mut m = Members::default();
        assert!(sl.is_empty());
        assert!(insert(&mut sl, &mut m, 1.0, "a"));
        assert!(insert(&mut sl, &mut m, 2.0, "b"));
        assert!(insert(&mut sl, &mut m, 3.0, "c"));
        assert_eq!(sl.len(), 3);

        // Duplicate insert should return false
        assert!(!insert(&mut sl, &mut m, 1.0, "a"));
        assert_eq!(sl.len(), 3);
    }

    #[test]
    fn test_ordering() {
        let mut sl = SkipList::new();
        let mut m = Members::default();
        insert(&mut sl, &mut m, 3.0, "c");
        insert(&mut sl, &mut m, 1.0, "a");
        insert(&mut sl, &mut m, 2.0, "b");

        assert_eq!(
            collect(&sl, &m),
            vec![
                (1.0, b"a".to_vec()),
                (2.0, b"b".to_vec()),
                (3.0, b"c".to_vec())
            ]
        );
    }

    #[test]
    fn test_same_score_lex_order() {
        let mut sl = SkipList::new();
        let mut m = Members::default();
        insert(&mut sl, &mut m, 1.0, "c");
        insert(&mut sl, &mut m, 1.0, "a");
        insert(&mut sl, &mut m, 1.0, "b");

        let items: Vec<Vec<u8>> = sl.iter().map(|(_, slot)| m.name(slot).to_vec()).collect();
        assert_eq!(items, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn test_rank() {
        let mut sl = SkipList::new();
        let mut m = Members::default();
        for i in 0..10 {
            insert(&mut sl, &mut m, i as f64, &format!("m{i}"));
        }

        assert_eq!(rank(&sl, &m, 0.0, "m0"), Some(0));
        assert_eq!(rank(&sl, &m, 5.0, "m5"), Some(5));
        assert_eq!(rank(&sl, &m, 9.0, "m9"), Some(9));
        assert_eq!(rank(&sl, &m, 10.0, "m10"), None);
    }

    #[test]
    fn test_get_by_rank() {
        let mut sl = SkipList::new();
        let mut m = Members::default();
        for i in 0..10 {
            insert(&mut sl, &mut m, i as f64, &format!("m{i}"));
        }

        let (score, slot) = sl.get_by_rank(0).unwrap();
        assert_eq!(score, OrderedFloat(0.0));
        assert_eq!(m.name(slot), b"m0");

        let (score, slot) = sl.get_by_rank(9).unwrap();
        assert_eq!(score, OrderedFloat(9.0));
        assert_eq!(m.name(slot), b"m9");

        assert!(sl.get_by_rank(10).is_none());
    }

    #[test]
    fn test_remove() {
        let mut sl = SkipList::new();
        let mut m = Members::default();
        insert(&mut sl, &mut m, 1.0, "a");
        insert(&mut sl, &mut m, 2.0, "b");
        insert(&mut sl, &mut m, 3.0, "c");

        assert!(remove(&mut sl, &m, 2.0, "b"));
        assert_eq!(sl.len(), 2);
        assert!(!remove(&mut sl, &m, 2.0, "b")); // already removed

        assert_eq!(
            collect(&sl, &m),
            vec![(1.0, b"a".to_vec()), (3.0, b"c".to_vec())]
        );

        // Check ranks updated
        assert_eq!(rank(&sl, &m, 1.0, "a"), Some(0));
        assert_eq!(rank(&sl, &m, 3.0, "c"), Some(1));
    }

    #[test]
    fn test_pop_first() {
        let mut sl = SkipList::new();
        let mut m = Members::default();
        insert(&mut sl, &mut m, 3.0, "c");
        insert(&mut sl, &mut m, 1.0, "a");
        insert(&mut sl, &mut m, 2.0, "b");

        let (score, slot) = sl.pop_first().unwrap();
        assert_eq!(score, OrderedFloat(1.0));
        assert_eq!(m.name(slot), b"a");
        assert_eq!(sl.len(), 2);

        let (score, slot) = sl.pop_first().unwrap();
        assert_eq!(score, OrderedFloat(2.0));
        assert_eq!(m.name(slot), b"b");
    }

    #[test]
    fn test_pop_last() {
        let mut sl = SkipList::new();
        let mut m = Members::default();
        insert(&mut sl, &mut m, 1.0, "a");
        insert(&mut sl, &mut m, 2.0, "b");
        insert(&mut sl, &mut m, 3.0, "c");

        let (score, slot) = sl.pop_last(m.resolve()).unwrap();
        assert_eq!(score, OrderedFloat(3.0));
        assert_eq!(m.name(slot), b"c");
        assert_eq!(sl.len(), 2);
    }

    #[test]
    fn test_rev_iter() {
        let mut sl = SkipList::new();
        let mut m = Members::default();
        insert(&mut sl, &mut m, 1.0, "a");
        insert(&mut sl, &mut m, 2.0, "b");
        insert(&mut sl, &mut m, 3.0, "c");

        let items: Vec<(f64, Vec<u8>)> = sl
            .rev_iter()
            .map(|(s, slot)| (s.0, m.name(slot).to_vec()))
            .collect();
        assert_eq!(
            items,
            vec![
                (3.0, b"c".to_vec()),
                (2.0, b"b".to_vec()),
                (1.0, b"a".to_vec())
            ]
        );
    }

    #[test]
    fn test_large_insert_remove() {
        let mut sl = SkipList::new();
        let mut m = Members::default();
        for i in 0..1000 {
            insert(&mut sl, &mut m, i as f64, &format!("m{i:04}"));
        }
        assert_eq!(sl.len(), 1000);

        // Check rank consistency
        for i in 0..1000 {
            assert_eq!(rank(&sl, &m, i as f64, &format!("m{i:04}")), Some(i));
        }

        // Remove every other element
        for i in (0..1000).step_by(2) {
            assert!(remove(&mut sl, &m, i as f64, &format!("m{i:04}")));
        }
        assert_eq!(sl.len(), 500);

        // Verify remaining elements have correct ranks
        for (want_rank, i) in (1..1000).step_by(2).enumerate() {
            assert_eq!(
                rank(&sl, &m, i as f64, &format!("m{i:04}")),
                Some(want_rank)
            );
        }
    }

    #[test]
    fn test_range_by_score_iter() {
        let mut sl = SkipList::new();
        let mut m = Members::default();
        for i in 0..10 {
            insert(&mut sl, &mut m, i as f64, &format!("m{i}"));
        }

        // Inclusive range [3, 7]
        let items: Vec<(f64, Vec<u8>)> = sl
            .range_by_score(OrderedFloat(3.0), true)
            .take_while(|(s, _)| *s <= OrderedFloat(7.0))
            .map(|(s, slot)| (s.0, m.name(slot).to_vec()))
            .collect();
        assert_eq!(
            items,
            vec![
                (3.0, b"m3".to_vec()),
                (4.0, b"m4".to_vec()),
                (5.0, b"m5".to_vec()),
                (6.0, b"m6".to_vec()),
                (7.0, b"m7".to_vec()),
            ]
        );

        // Exclusive range (3, 7)
        let items: Vec<(f64, Vec<u8>)> = sl
            .range_by_score(OrderedFloat(3.0), false)
            .take_while(|(s, _)| *s < OrderedFloat(7.0))
            .map(|(s, slot)| (s.0, m.name(slot).to_vec()))
            .collect();
        assert_eq!(
            items,
            vec![
                (4.0, b"m4".to_vec()),
                (5.0, b"m5".to_vec()),
                (6.0, b"m6".to_vec()),
            ]
        );
    }

    #[test]
    fn test_empty_operations() {
        let mut sl = SkipList::new();
        let m = Members::default();
        assert!(sl.pop_first().is_none());
        assert!(sl.pop_last(m.resolve()).is_none());
        assert!(sl.get_by_rank(0).is_none());
        assert!(sl.rank(OrderedFloat(1.0), b"a", m.resolve()).is_none());
        assert!(!sl.remove(OrderedFloat(1.0), b"a", m.resolve()));
        assert_eq!(sl.iter().count(), 0);
        assert_eq!(sl.rev_iter().count(), 0);
    }

    #[test]
    fn test_insert_remove_reinsert() {
        let mut sl = SkipList::new();
        let mut m = Members::default();
        insert(&mut sl, &mut m, 1.0, "a");
        remove(&mut sl, &m, 1.0, "a");
        assert!(sl.is_empty());
        insert(&mut sl, &mut m, 1.0, "a");
        assert_eq!(sl.len(), 1);
        assert_eq!(rank(&sl, &m, 1.0, "a"), Some(0));
    }
}
