//! Arena-indexed skip list with span-based O(log n) rank queries.
//!
//! Modeled after Redis's zskiplist. Safe Rust, no raw pointers — uses `Vec<Option<Node>>`
//! with a free list for arena allocation.

use bytes::Bytes;
use ordered_float::OrderedFloat;
use rand::Rng;
use smallvec::SmallVec;
use std::cmp::Ordering;

const NIL: u32 = u32::MAX;
const MAX_LEVEL: usize = 32;
const P: f64 = 0.25;

/// Size of a single skip list `Node` in bytes (exposed for DEBUG STRUCTSIZE).
pub const NODE_SIZE: usize = std::mem::size_of::<Node>();

/// Arena-indexed skip list with span-based O(log n) rank.
#[derive(Debug, Clone)]
pub struct SkipList {
    nodes: Vec<Option<Node>>,
    free: Vec<u32>,
    head: u32,
    tail: u32,
    length: usize,
    level: usize,
}

#[derive(Debug, Clone)]
struct Node {
    score: OrderedFloat<f64>,
    member: Bytes,
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
fn cmp_key(s1: OrderedFloat<f64>, m1: &Bytes, s2: OrderedFloat<f64>, m2: &Bytes) -> Ordering {
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
            member: Bytes::new(),
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

    fn alloc_node(&mut self, score: OrderedFloat<f64>, member: Bytes, level: usize) -> u32 {
        let node = Node {
            score,
            member,
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

    /// Insert a (score, member) pair. Returns false if the exact pair already exists.
    #[allow(clippy::needless_range_loop)]
    pub fn insert(&mut self, score: OrderedFloat<f64>, member: Bytes) -> bool {
        let mut rng = rand::rng();

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
                match cmp_key(fwd_node.score, &fwd_node.member, score, &member) {
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

        let lvl = random_level(&mut rng);

        // Grow skip list level if needed
        if lvl > self.level {
            for i in self.level..lvl {
                rank[i] = 0;
                update[i] = self.head;
                self.node_mut(self.head).levels[i].span = self.length as u32;
            }
            self.level = lvl;
        }

        let new_idx = self.alloc_node(score, member, lvl);

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

    /// Remove a (score, member) pair. Returns true if found and removed.
    #[allow(clippy::needless_range_loop)]
    pub fn remove(&mut self, score: OrderedFloat<f64>, member: &Bytes) -> bool {
        let mut update = [0u32; MAX_LEVEL];

        let mut x = self.head;
        for i in (0..self.level).rev() {
            loop {
                let fwd = self.node(x).levels[i].forward;
                if fwd == NIL {
                    break;
                }
                let fwd_node = self.node(fwd);
                if cmp_key(fwd_node.score, &fwd_node.member, score, member) == Ordering::Less {
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
        if target_node.score != score || target_node.member != *member {
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

    /// Get the 0-based rank of a (score, member) pair. Returns None if not found.
    pub fn rank(&self, score: OrderedFloat<f64>, member: &Bytes) -> Option<usize> {
        let mut rank = 0u32;
        let mut x = self.head;

        for i in (0..self.level).rev() {
            loop {
                let fwd = self.node(x).levels[i].forward;
                if fwd == NIL {
                    break;
                }
                let fwd_node = self.node(fwd);
                match cmp_key(fwd_node.score, &fwd_node.member, score, member) {
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
    pub fn get_by_rank(&self, rank: usize) -> Option<(OrderedFloat<f64>, &Bytes)> {
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
                    return Some((node.score, &node.member));
                }
            }
        }
        None
    }

    /// Pop the first (minimum) element.
    pub fn pop_first(&mut self) -> Option<(OrderedFloat<f64>, Bytes)> {
        let first = self.node(self.head).levels[0].forward;
        if first == NIL {
            return None;
        }
        let score = self.node(first).score;
        let member = self.node(first).member.clone();

        // For the first element, head is the predecessor at all levels
        let update = [self.head; MAX_LEVEL];

        self.delete_node(first, &update);
        self.free_node(first);
        Some((score, member))
    }

    /// Pop the last (maximum) element.
    pub fn pop_last(&mut self) -> Option<(OrderedFloat<f64>, Bytes)> {
        if self.tail == NIL {
            return None;
        }
        let score = self.node(self.tail).score;
        let member = self.node(self.tail).member.clone();
        self.remove(score, &member);
        Some((score, member))
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

    /// Iterate forward over elements in the given score range.
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

    /// Find the first node at or after the given score and seek to it,
    /// returning the rank and an iterator starting from that position.
    pub fn range_by_rank_iter(&self, start_rank: usize) -> SkipListIter<'_> {
        if start_rank >= self.length {
            return SkipListIter {
                list: self,
                current: NIL,
            };
        }
        match self.get_by_rank(start_rank) {
            Some((score, member)) => {
                // Walk to find the node index
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
                // Fallback (shouldn't reach here if get_by_rank succeeded)
                let _ = (score, member);
                SkipListIter {
                    list: self,
                    current: NIL,
                }
            }
            None => SkipListIter {
                list: self,
                current: NIL,
            },
        }
    }

    /// Approximate memory usage in bytes.
    pub fn memory_size(&self) -> usize {
        let base = std::mem::size_of::<Self>();
        let nodes_vec = self.nodes.capacity() * std::mem::size_of::<Option<Node>>();
        let free_vec = self.free.capacity() * std::mem::size_of::<u32>();
        let node_internals: usize = self
            .nodes
            .iter()
            .flatten()
            .map(|node| node.levels.len() * std::mem::size_of::<Link>() + node.member.len())
            .sum();
        base + nodes_vec + free_vec + node_internals
    }
}

/// Forward iterator over skip list elements.
pub struct SkipListIter<'a> {
    list: &'a SkipList,
    current: u32,
}

impl<'a> Iterator for SkipListIter<'a> {
    type Item = (OrderedFloat<f64>, &'a Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current == NIL {
            return None;
        }
        let node = self.list.node(self.current);
        let result = (node.score, &node.member);
        self.current = node.levels[0].forward;
        Some(result)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.list.length))
    }
}

/// Reverse iterator over skip list elements.
pub struct SkipListRevIter<'a> {
    list: &'a SkipList,
    current: u32,
}

impl<'a> Iterator for SkipListRevIter<'a> {
    type Item = (OrderedFloat<f64>, &'a Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current == NIL {
            return None;
        }
        let node = self.list.node(self.current);
        let result = (node.score, &node.member);
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

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    #[test]
    fn test_insert_and_len() {
        let mut sl = SkipList::new();
        assert!(sl.is_empty());
        assert!(sl.insert(OrderedFloat(1.0), b("a")));
        assert!(sl.insert(OrderedFloat(2.0), b("b")));
        assert!(sl.insert(OrderedFloat(3.0), b("c")));
        assert_eq!(sl.len(), 3);

        // Duplicate insert should return false
        assert!(!sl.insert(OrderedFloat(1.0), b("a")));
        assert_eq!(sl.len(), 3);
    }

    #[test]
    fn test_ordering() {
        let mut sl = SkipList::new();
        sl.insert(OrderedFloat(3.0), b("c"));
        sl.insert(OrderedFloat(1.0), b("a"));
        sl.insert(OrderedFloat(2.0), b("b"));

        let items: Vec<_> = sl.iter().map(|(s, m)| (s.0, m.as_ref())).collect();
        assert_eq!(items, vec![(1.0, b"a" as &[u8]), (2.0, b"b"), (3.0, b"c")]);
    }

    #[test]
    fn test_same_score_lex_order() {
        let mut sl = SkipList::new();
        sl.insert(OrderedFloat(1.0), b("c"));
        sl.insert(OrderedFloat(1.0), b("a"));
        sl.insert(OrderedFloat(1.0), b("b"));

        let items: Vec<_> = sl.iter().map(|(_, m)| m.as_ref()).collect();
        assert_eq!(items, vec![b"a" as &[u8], b"b", b"c"]);
    }

    #[test]
    fn test_rank() {
        let mut sl = SkipList::new();
        for i in 0..10 {
            sl.insert(OrderedFloat(i as f64), Bytes::from(format!("m{}", i)));
        }

        assert_eq!(sl.rank(OrderedFloat(0.0), &b("m0")), Some(0));
        assert_eq!(sl.rank(OrderedFloat(5.0), &b("m5")), Some(5));
        assert_eq!(sl.rank(OrderedFloat(9.0), &b("m9")), Some(9));
        assert_eq!(sl.rank(OrderedFloat(10.0), &b("m10")), None);
    }

    #[test]
    fn test_get_by_rank() {
        let mut sl = SkipList::new();
        for i in 0..10 {
            sl.insert(OrderedFloat(i as f64), Bytes::from(format!("m{}", i)));
        }

        let (score, member) = sl.get_by_rank(0).unwrap();
        assert_eq!(score, OrderedFloat(0.0));
        assert_eq!(member, &b("m0"));

        let (score, member) = sl.get_by_rank(9).unwrap();
        assert_eq!(score, OrderedFloat(9.0));
        assert_eq!(member, &b("m9"));

        assert!(sl.get_by_rank(10).is_none());
    }

    #[test]
    fn test_remove() {
        let mut sl = SkipList::new();
        sl.insert(OrderedFloat(1.0), b("a"));
        sl.insert(OrderedFloat(2.0), b("b"));
        sl.insert(OrderedFloat(3.0), b("c"));

        assert!(sl.remove(OrderedFloat(2.0), &b("b")));
        assert_eq!(sl.len(), 2);
        assert!(!sl.remove(OrderedFloat(2.0), &b("b"))); // already removed

        let items: Vec<_> = sl.iter().map(|(s, m)| (s.0, m.as_ref())).collect();
        assert_eq!(items, vec![(1.0, b"a" as &[u8]), (3.0, b"c")]);

        // Check ranks updated
        assert_eq!(sl.rank(OrderedFloat(1.0), &b("a")), Some(0));
        assert_eq!(sl.rank(OrderedFloat(3.0), &b("c")), Some(1));
    }

    #[test]
    fn test_pop_first() {
        let mut sl = SkipList::new();
        sl.insert(OrderedFloat(3.0), b("c"));
        sl.insert(OrderedFloat(1.0), b("a"));
        sl.insert(OrderedFloat(2.0), b("b"));

        let (score, member) = sl.pop_first().unwrap();
        assert_eq!(score, OrderedFloat(1.0));
        assert_eq!(member, b("a"));
        assert_eq!(sl.len(), 2);

        let (score, member) = sl.pop_first().unwrap();
        assert_eq!(score, OrderedFloat(2.0));
        assert_eq!(member, b("b"));
    }

    #[test]
    fn test_pop_last() {
        let mut sl = SkipList::new();
        sl.insert(OrderedFloat(1.0), b("a"));
        sl.insert(OrderedFloat(2.0), b("b"));
        sl.insert(OrderedFloat(3.0), b("c"));

        let (score, member) = sl.pop_last().unwrap();
        assert_eq!(score, OrderedFloat(3.0));
        assert_eq!(member, b("c"));
        assert_eq!(sl.len(), 2);
    }

    #[test]
    fn test_rev_iter() {
        let mut sl = SkipList::new();
        sl.insert(OrderedFloat(1.0), b("a"));
        sl.insert(OrderedFloat(2.0), b("b"));
        sl.insert(OrderedFloat(3.0), b("c"));

        let items: Vec<_> = sl.rev_iter().map(|(s, m)| (s.0, m.as_ref())).collect();
        assert_eq!(items, vec![(3.0, b"c" as &[u8]), (2.0, b"b"), (1.0, b"a")]);
    }

    #[test]
    fn test_large_insert_remove() {
        let mut sl = SkipList::new();
        for i in 0..1000 {
            sl.insert(OrderedFloat(i as f64), Bytes::from(format!("m{:04}", i)));
        }
        assert_eq!(sl.len(), 1000);

        // Check rank consistency
        for i in 0..1000 {
            assert_eq!(
                sl.rank(OrderedFloat(i as f64), &Bytes::from(format!("m{:04}", i))),
                Some(i)
            );
        }

        // Remove every other element
        for i in (0..1000).step_by(2) {
            assert!(sl.remove(OrderedFloat(i as f64), &Bytes::from(format!("m{:04}", i))));
        }
        assert_eq!(sl.len(), 500);

        // Verify remaining elements have correct ranks
        for (rank, i) in (1..1000).step_by(2).enumerate() {
            assert_eq!(
                sl.rank(OrderedFloat(i as f64), &Bytes::from(format!("m{:04}", i))),
                Some(rank)
            );
        }
    }

    #[test]
    fn test_range_by_score_iter() {
        let mut sl = SkipList::new();
        for i in 0..10 {
            sl.insert(OrderedFloat(i as f64), Bytes::from(format!("m{}", i)));
        }

        // Inclusive range [3, 7]
        let items: Vec<_> = sl
            .range_by_score(OrderedFloat(3.0), true)
            .take_while(|(s, _)| *s <= OrderedFloat(7.0))
            .map(|(s, m)| (s.0, m.as_ref()))
            .collect();
        assert_eq!(
            items,
            vec![
                (3.0, b"m3" as &[u8]),
                (4.0, b"m4"),
                (5.0, b"m5"),
                (6.0, b"m6"),
                (7.0, b"m7"),
            ]
        );

        // Exclusive range (3, 7)
        let items: Vec<_> = sl
            .range_by_score(OrderedFloat(3.0), false)
            .take_while(|(s, _)| *s < OrderedFloat(7.0))
            .map(|(s, m)| (s.0, m.as_ref()))
            .collect();
        assert_eq!(
            items,
            vec![(4.0, b"m4" as &[u8]), (5.0, b"m5"), (6.0, b"m6"),]
        );
    }

    #[test]
    fn test_empty_operations() {
        let mut sl = SkipList::new();
        assert!(sl.pop_first().is_none());
        assert!(sl.pop_last().is_none());
        assert!(sl.get_by_rank(0).is_none());
        assert!(sl.rank(OrderedFloat(1.0), &b("a")).is_none());
        assert!(!sl.remove(OrderedFloat(1.0), &b("a")));
        assert_eq!(sl.iter().count(), 0);
        assert_eq!(sl.rev_iter().count(), 0);
    }

    #[test]
    fn test_insert_remove_reinsert() {
        let mut sl = SkipList::new();
        sl.insert(OrderedFloat(1.0), b("a"));
        sl.remove(OrderedFloat(1.0), &b("a"));
        assert!(sl.is_empty());
        sl.insert(OrderedFloat(1.0), b("a"));
        assert_eq!(sl.len(), 1);
        assert_eq!(sl.rank(OrderedFloat(1.0), &b("a")), Some(0));
    }
}
