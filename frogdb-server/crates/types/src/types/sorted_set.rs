//! Sorted set types: ScoreBound, LexBound, SortedSetValue.
//!
//! Member bytes live once in a per-value [`BlockStore`]; the skip list nodes
//! and the member lookup table both refer to members through a `u32` slot into
//! a slot-stable entry table that owns the block-store handle. Compaction
//! therefore patches handles in exactly one place, and skip list nodes never
//! move or change when blocks compact.

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;

use bytes::Bytes;
use hashbrown::HashTable;
use ordered_float::OrderedFloat;

use crate::blockstore::{BlockStore, Handle};
use crate::skiplist::{SkipList, SkipListIter};
use crate::types::string_value::IncrementError;

// ============================================================================
// Sorted Set Types
// ============================================================================

/// Score boundary for range queries.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ScoreBound {
    /// Inclusive bound.
    Inclusive(f64),
    /// Exclusive bound.
    Exclusive(f64),
    /// Negative infinity.
    NegInf,
    /// Positive infinity.
    PosInf,
}

impl ScoreBound {
    /// Check if a score satisfies this bound as a minimum.
    pub fn satisfies_min(&self, score: f64) -> bool {
        match self {
            ScoreBound::NegInf => true,
            ScoreBound::PosInf => false,
            ScoreBound::Inclusive(bound) => score >= *bound,
            ScoreBound::Exclusive(bound) => score > *bound,
        }
    }

    /// Check if a score satisfies this bound as a maximum.
    pub fn satisfies_max(&self, score: f64) -> bool {
        match self {
            ScoreBound::NegInf => false,
            ScoreBound::PosInf => true,
            ScoreBound::Inclusive(bound) => score <= *bound,
            ScoreBound::Exclusive(bound) => score < *bound,
        }
    }
}

/// Lexicographic boundary for range queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LexBound {
    /// Inclusive bound.
    Inclusive(Bytes),
    /// Exclusive bound.
    Exclusive(Bytes),
    /// Minimum (unbounded).
    Min,
    /// Maximum (unbounded).
    Max,
}

impl LexBound {
    /// Check if a member satisfies this bound as a minimum.
    pub fn satisfies_min(&self, member: &[u8]) -> bool {
        match self {
            LexBound::Min => true,
            LexBound::Max => false,
            LexBound::Inclusive(bound) => member >= bound.as_ref(),
            LexBound::Exclusive(bound) => member > bound.as_ref(),
        }
    }

    /// Check if a member satisfies this bound as a maximum.
    pub fn satisfies_max(&self, member: &[u8]) -> bool {
        match self {
            LexBound::Min => false,
            LexBound::Max => true,
            LexBound::Inclusive(bound) => member <= bound.as_ref(),
            LexBound::Exclusive(bound) => member < bound.as_ref(),
        }
    }
}

/// Result of adding a member to a sorted set.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ZAddResult {
    /// Whether a new member was added.
    pub added: bool,
    /// Whether the score was changed (for existing members).
    pub changed: bool,
    /// The previous score (if member existed).
    pub old_score: Option<f64>,
}

// ============================================================================
// SortedSetValue
// ============================================================================

/// One member's storage: its block-store handle and its score. The score here
/// is the member→score map; the copy in the skip list node is the ordering
/// key.
#[derive(Debug, Clone, Copy)]
struct MemberEntry {
    handle: Handle,
    score: f64,
}

/// Sorted set value with O(1) score lookup and O(log n) rank/range queries.
///
/// Member bytes are stored once in `store`; `entries` is a slot-stable table
/// (freed slots are recycled, live slots never move) owning each member's
/// handle, `index` maps member bytes to a slot for O(1) lookup, and `list`
/// orders slots by (score, member-lex). Skip list comparisons resolve slots
/// to bytes through `entries` + `store` at operation time, so the whole value
/// stays a plain owned struct that moves between shards.
#[derive(Debug, Clone)]
pub struct SortedSetValue {
    store: BlockStore,
    entries: Vec<Option<MemberEntry>>,
    /// Recycled entry slots.
    free: Vec<u32>,
    /// member bytes -> slot.
    index: HashTable<u32>,
    hasher: RandomState,
    list: SkipList,
}

impl Default for SortedSetValue {
    fn default() -> Self {
        Self::new()
    }
}

/// Build a slot→bytes resolver over split borrows of the store and entry
/// table, for handing to skip list operations while `list` is borrowed
/// mutably.
fn resolver<'a>(
    store: &'a BlockStore,
    entries: &'a [Option<MemberEntry>],
) -> impl Fn(u32) -> &'a [u8] {
    move |slot| {
        let entry = entries[slot as usize].as_ref().expect("live slot");
        store.get(entry.handle)
    }
}

impl SortedSetValue {
    /// Create a new empty sorted set.
    pub fn new() -> Self {
        Self {
            store: BlockStore::new(),
            entries: Vec::new(),
            free: Vec::new(),
            index: HashTable::new(),
            hasher: RandomState::new(),
            list: SkipList::new(),
        }
    }

    /// Get the number of members.
    pub fn len(&self) -> usize {
        self.list.len()
    }

    /// Check if the set is empty.
    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    #[inline]
    fn member_at(&self, slot: u32) -> &[u8] {
        resolver(&self.store, &self.entries)(slot)
    }

    #[inline]
    fn bytes_at(&self, slot: u32) -> Bytes {
        Bytes::copy_from_slice(self.member_at(slot))
    }

    #[inline]
    fn score_at(&self, slot: u32) -> f64 {
        self.entries[slot as usize]
            .as_ref()
            .expect("live slot")
            .score
    }

    fn find_slot(&self, member: &[u8]) -> Option<u32> {
        let hash = self.hasher.hash_one(member);
        self.index
            .find(hash, |&slot| self.member_at(slot) == member)
            .copied()
    }

    /// Append the member's bytes, claim a slot, and index it. The caller
    /// inserts the slot into the skip list.
    fn alloc_member(&mut self, member: &[u8], score: f64) -> u32 {
        let handle = self.store.append(&[member]);
        let entry = Some(MemberEntry { handle, score });
        let slot = match self.free.pop() {
            Some(slot) => {
                self.entries[slot as usize] = entry;
                slot
            }
            None => {
                self.entries.push(entry);
                (self.entries.len() - 1) as u32
            }
        };
        let hash = self.hasher.hash_one(member);
        let (entries, store, hasher) = (&self.entries, &self.store, &self.hasher);
        self.index.insert_unique(hash, slot, |&s| {
            let entry = entries[s as usize].as_ref().expect("live slot");
            hasher.hash_one(store.get(entry.handle))
        });
        slot
    }

    /// Drop a member's table row, index entry, and block bytes. The caller
    /// has already removed the slot from the skip list.
    fn release_member(&mut self, slot: u32) {
        let hash = self.hasher.hash_one(self.member_at(slot));
        match self.index.find_entry(hash, |&s| s == slot) {
            Ok(occupied) => {
                occupied.remove();
            }
            Err(_) => unreachable!("live slot is indexed"),
        }
        let entry = self.entries[slot as usize].take().expect("live slot");
        self.store.remove(entry.handle);
        self.free.push(slot);
        self.maybe_compact();
    }

    fn maybe_compact(&mut self) {
        if self.store.should_compact() {
            self.store
                .compact(self.entries.iter_mut().flatten().map(|e| &mut e.handle));
        }
    }

    /// Move an existing member to a new score in the skip list.
    fn reinsert(&mut self, slot: u32, member: &[u8], old_score: f64, new_score: f64) {
        {
            let Self {
                store,
                entries,
                list,
                ..
            } = &mut *self;
            let resolve = resolver(store, entries);
            let removed = list.remove(OrderedFloat(old_score), member, &resolve);
            debug_assert!(removed, "reinserted member was in the skip list");
            let inserted = list.insert(OrderedFloat(new_score), slot, &resolve);
            debug_assert!(inserted, "reinserted member is unique");
        }
        self.entries[slot as usize]
            .as_mut()
            .expect("live slot")
            .score = new_score;
    }

    /// Remove a member by slot without materializing its bytes: skip list
    /// first (slots and handles stay valid), then the table row, index
    /// entry, and block bytes.
    fn remove_slot(&mut self, slot: u32) {
        let score = self.score_at(slot);
        {
            let Self {
                store,
                entries,
                list,
                ..
            } = &mut *self;
            let resolve = resolver(store, entries);
            let removed = list.remove(OrderedFloat(score), resolve(slot), &resolve);
            debug_assert!(removed, "live slot was in the skip list");
        }
        self.release_member(slot);
    }

    /// Add or update a member with a score.
    ///
    /// Returns information about what changed.
    pub fn add(&mut self, member: Bytes, score: f64) -> ZAddResult {
        if let Some(slot) = self.find_slot(&member) {
            let old_score = self.score_at(slot);
            if (old_score - score).abs() < f64::EPSILON
                || (old_score.is_nan() && score.is_nan())
                || (old_score == score)
            {
                // Score unchanged
                return ZAddResult {
                    added: false,
                    changed: false,
                    old_score: Some(old_score),
                };
            }
            self.reinsert(slot, &member, old_score, score);
            ZAddResult {
                added: false,
                changed: true,
                old_score: Some(old_score),
            }
        } else {
            let slot = self.alloc_member(&member, score);
            let Self {
                store,
                entries,
                list,
                ..
            } = self;
            let inserted = list.insert(OrderedFloat(score), slot, resolver(store, entries));
            debug_assert!(inserted, "freshly allocated member is unique");
            ZAddResult {
                added: true,
                changed: false,
                old_score: None,
            }
        }
    }

    /// Remove a member from the set.
    ///
    /// Returns the score if the member existed.
    pub fn remove(&mut self, member: &[u8]) -> Option<f64> {
        let slot = self.find_slot(member)?;
        let score = self.score_at(slot);
        self.remove_slot(slot);
        Some(score)
    }

    /// Get the score of a member.
    pub fn get_score(&self, member: &[u8]) -> Option<f64> {
        self.find_slot(member).map(|slot| self.score_at(slot))
    }

    /// Check if a member exists.
    pub fn contains(&self, member: &[u8]) -> bool {
        self.find_slot(member).is_some()
    }

    /// Get the 0-based rank of a member (ascending by score).
    pub fn rank(&self, member: &[u8]) -> Option<usize> {
        let slot = self.find_slot(member)?;
        let score = self.score_at(slot);
        self.list.rank(
            OrderedFloat(score),
            member,
            resolver(&self.store, &self.entries),
        )
    }

    /// Get the 0-based rank of a member (descending by score).
    pub fn rev_rank(&self, member: &[u8]) -> Option<usize> {
        let rank = self.rank(member)?;
        Some(self.len() - 1 - rank)
    }

    /// Increment the score of a member.
    ///
    /// If the member doesn't exist, it's created with the given increment as its score.
    /// Returns the new score, or `ScoreNotANumber` if the result is NaN (e.g.
    /// incrementing an existing `+inf` score by `-inf`). Unlike the string/hash
    /// increment commands, an infinite *result* is a valid sorted-set score in
    /// Redis and is not an error — only NaN is rejected, and rejection leaves
    /// the set untouched (checked before any mutation).
    pub fn incr(&mut self, member: Bytes, increment: f64) -> Result<f64, IncrementError> {
        let existing = self.find_slot(&member);
        let old_score = existing.map(|slot| self.score_at(slot)).unwrap_or(0.0);
        let new_score = old_score + increment;

        if new_score.is_nan() {
            return Err(IncrementError::ScoreNotANumber);
        }

        match existing {
            Some(slot) => self.reinsert(slot, &member, old_score, new_score),
            None => {
                let slot = self.alloc_member(&member, new_score);
                let Self {
                    store,
                    entries,
                    list,
                    ..
                } = self;
                let inserted = list.insert(OrderedFloat(new_score), slot, resolver(store, entries));
                debug_assert!(inserted, "freshly allocated member is unique");
            }
        }

        Ok(new_score)
    }

    /// Get members by rank range (inclusive).
    ///
    /// `start` and `end` are 0-based indices. Negative indices count from the end.
    pub fn range_by_rank(&self, start: i64, end: i64) -> Vec<(Bytes, f64)> {
        let Some((start, count)) = self.clamp_rank_range(start, end) else {
            return vec![];
        };
        self.list
            .range_by_rank_iter(start)
            .take(count)
            .map(|(score, slot)| (self.bytes_at(slot), score.0))
            .collect()
    }

    /// Get members by rank range in reverse order (descending by score).
    pub fn rev_range_by_rank(&self, start: i64, end: i64) -> Vec<(Bytes, f64)> {
        let Some((start, count)) = self.clamp_rank_range(start, end) else {
            return vec![];
        };
        self.list
            .rev_iter()
            .skip(start)
            .take(count)
            .map(|(score, slot)| (self.bytes_at(slot), score.0))
            .collect()
    }

    /// Clamp a Redis-style (start, end) rank pair (negative = from the end)
    /// to a (start, count) window, or None when the window is empty.
    fn clamp_rank_range(&self, start: i64, end: i64) -> Option<(usize, usize)> {
        let len = self.len() as i64;
        if len == 0 {
            return None;
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
            return None;
        }

        Some((start, end as usize - start + 1))
    }

    /// Iterate (score, slot) pairs inside a score range, or None when the
    /// bounds are degenerate (min = +inf or max = -inf).
    fn score_range_iter(&self, min: &ScoreBound, max: &ScoreBound) -> BoundedScoreIter<'_> {
        let (min_score, min_inclusive) = match min {
            ScoreBound::NegInf => (OrderedFloat(f64::NEG_INFINITY), true),
            ScoreBound::PosInf => return BoundedScoreIter::empty(),
            ScoreBound::Inclusive(v) => (OrderedFloat(*v), true),
            ScoreBound::Exclusive(v) => (OrderedFloat(*v), false),
        };
        let max_score = match max {
            ScoreBound::PosInf => None,
            ScoreBound::NegInf => return BoundedScoreIter::empty(),
            ScoreBound::Inclusive(v) => Some((OrderedFloat(*v), true)),
            ScoreBound::Exclusive(v) => Some((OrderedFloat(*v), false)),
        };
        BoundedScoreIter {
            inner: Some(self.list.range_by_score(min_score, min_inclusive)),
            max_score,
        }
    }

    /// Get members by score range.
    pub fn range_by_score(
        &self,
        min: &ScoreBound,
        max: &ScoreBound,
        offset: usize,
        count: Option<usize>,
    ) -> Vec<(Bytes, f64)> {
        let iter = self.score_range_iter(min, max).skip(offset);
        let iter = iter.map(|(score, slot)| (self.bytes_at(slot), score.0));
        match count {
            Some(count) => iter.take(count).collect(),
            None => iter.collect(),
        }
    }

    /// Get members by score range in reverse order.
    pub fn rev_range_by_score(
        &self,
        min: &ScoreBound,
        max: &ScoreBound,
        offset: usize,
        count: Option<usize>,
    ) -> Vec<(Bytes, f64)> {
        // Collect the forward range and walk it backwards; a reverse
        // score-bounded skip list iterator would avoid the collect but this
        // matches the previous implementation's complexity.
        let in_range: Vec<(OrderedFloat<f64>, u32)> = self.score_range_iter(min, max).collect();
        let iter = in_range
            .into_iter()
            .rev()
            .skip(offset)
            .map(|(score, slot)| (self.bytes_at(slot), score.0));
        match count {
            Some(count) => iter.take(count).collect(),
            None => iter.collect(),
        }
    }

    /// Get members by lexicographic range (requires all scores to be equal).
    pub fn range_by_lex(
        &self,
        min: &LexBound,
        max: &LexBound,
        offset: usize,
        count: Option<usize>,
    ) -> Vec<(Bytes, f64)> {
        // Iteration is in (score, member) order, which is lexicographic for
        // same scores. Bounds are checked on borrowed bytes; only kept
        // members materialize.
        let iter = self
            .list
            .iter()
            .filter(|&(_, slot)| {
                let member = self.member_at(slot);
                min.satisfies_min(member) && max.satisfies_max(member)
            })
            .skip(offset)
            .map(|(score, slot)| (self.bytes_at(slot), score.0));
        match count {
            Some(count) => iter.take(count).collect(),
            None => iter.collect(),
        }
    }

    /// Get members by lexicographic range in reverse order.
    pub fn rev_range_by_lex(
        &self,
        min: &LexBound,
        max: &LexBound,
        offset: usize,
        count: Option<usize>,
    ) -> Vec<(Bytes, f64)> {
        let iter = self
            .list
            .rev_iter()
            .filter(|&(_, slot)| {
                let member = self.member_at(slot);
                min.satisfies_min(member) && max.satisfies_max(member)
            })
            .skip(offset)
            .map(|(score, slot)| (self.bytes_at(slot), score.0));
        match count {
            Some(count) => iter.take(count).collect(),
            None => iter.collect(),
        }
    }

    /// Count members in score range.
    pub fn count_by_score(&self, min: &ScoreBound, max: &ScoreBound) -> usize {
        self.score_range_iter(min, max).count()
    }

    /// Count members in lex range.
    pub fn count_by_lex(&self, min: &LexBound, max: &LexBound) -> usize {
        self.list
            .iter()
            .filter(|&(_, slot)| {
                let member = self.member_at(slot);
                min.satisfies_min(member) && max.satisfies_max(member)
            })
            .count()
    }

    /// Pop members with minimum scores.
    pub fn pop_min(&mut self, count: usize) -> Vec<(Bytes, f64)> {
        let mut result = Vec::with_capacity(count.min(self.len()));
        for _ in 0..count {
            let Some((score, slot)) = self.list.pop_first() else {
                break;
            };
            let member = self.bytes_at(slot);
            self.release_member(slot);
            result.push((member, score.0));
        }
        result
    }

    /// Pop members with maximum scores.
    pub fn pop_max(&mut self, count: usize) -> Vec<(Bytes, f64)> {
        let mut result = Vec::with_capacity(count.min(self.len()));
        for _ in 0..count {
            let popped = {
                let Self {
                    store,
                    entries,
                    list,
                    ..
                } = &mut *self;
                list.pop_last(resolver(store, entries))
            };
            let Some((score, slot)) = popped else {
                break;
            };
            let member = self.bytes_at(slot);
            self.release_member(slot);
            result.push((member, score.0));
        }
        result
    }

    /// Remove members by rank range.
    ///
    /// Returns the number of members removed.
    pub fn remove_range_by_rank(&mut self, start: i64, end: i64) -> usize {
        let Some((start, count)) = self.clamp_rank_range(start, end) else {
            return 0;
        };
        // Slots stay valid across removals (compaction patches handles, not
        // slots), so collect slots instead of copying member bytes out.
        let to_remove: Vec<u32> = self
            .list
            .range_by_rank_iter(start)
            .take(count)
            .map(|(_, slot)| slot)
            .collect();
        let count = to_remove.len();
        for slot in to_remove {
            self.remove_slot(slot);
        }
        count
    }

    /// Remove members by score range.
    ///
    /// Returns the number of members removed.
    pub fn remove_range_by_score(&mut self, min: &ScoreBound, max: &ScoreBound) -> usize {
        let to_remove: Vec<u32> = self
            .score_range_iter(min, max)
            .map(|(_, slot)| slot)
            .collect();
        let count = to_remove.len();
        for slot in to_remove {
            self.remove_slot(slot);
        }
        count
    }

    /// Remove members by lex range.
    ///
    /// Returns the number of members removed.
    pub fn remove_range_by_lex(&mut self, min: &LexBound, max: &LexBound) -> usize {
        let to_remove: Vec<u32> = self
            .list
            .iter()
            .filter(|&(_, slot)| {
                let member = self.member_at(slot);
                min.satisfies_min(member) && max.satisfies_max(member)
            })
            .map(|(_, slot)| slot)
            .collect();
        let count = to_remove.len();
        for slot in to_remove {
            self.remove_slot(slot);
        }
        count
    }

    /// Get random members.
    ///
    /// If `count` is positive, returns that many unique members.
    /// If `count` is negative, returns abs(count) members with possible duplicates.
    pub fn random_members(&self, count: i64) -> Vec<(Bytes, f64)> {
        if count == 0 || self.is_empty() {
            return vec![];
        }

        use rand::RngExt;
        use rand::seq::IteratorRandom;
        let mut rng = rand::rng();

        if count > 0 {
            // Return unique members (no duplicates), up to self.len()
            let count = (count as usize).min(self.len());
            self.list
                .iter()
                .sample(&mut rng, count)
                .into_iter()
                .map(|(score, slot)| (self.bytes_at(slot), score.0))
                .collect()
        } else {
            // Allow duplicates: pick randomly with replacement
            let slots: Vec<(OrderedFloat<f64>, u32)> = self.list.iter().collect();
            let n = (-count) as usize;
            let mut result = Vec::with_capacity(n);
            for _ in 0..n {
                let (score, slot) = slots[rng.random_range(0..slots.len())];
                result.push((self.bytes_at(slot), score.0));
            }
            result
        }
    }

    /// Calculate approximate memory size.
    ///
    /// Derived only from block allocation sizes, table capacities, and the
    /// deterministic skip list structure — the same op history always reports
    /// the same size (the memory-conservation checker depends on this).
    pub fn memory_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.store.allocated_bytes()
            + self.entries.capacity() * std::mem::size_of::<Option<MemberEntry>>()
            + self.free.capacity() * std::mem::size_of::<u32>()
            // Index: one u32 slot plus ~1 control byte per capacity slot.
            + self.index.capacity() * (std::mem::size_of::<u32>() + 1)
            + self.list.memory_size()
    }

    /// Iterate over all members in score order.
    pub fn iter(&self) -> impl Iterator<Item = (Bytes, f64)> + '_ {
        self.list
            .iter()
            .map(|(score, slot)| (self.bytes_at(slot), score.0))
    }

    /// Get all members and scores as a vec for serialization.
    pub fn to_vec(&self) -> Vec<(Bytes, f64)> {
        self.iter().collect()
    }
}

/// Forward iterator over (score, slot) pairs inside a score range.
struct BoundedScoreIter<'a> {
    /// None = statically empty range (degenerate bounds).
    inner: Option<SkipListIter<'a>>,
    /// Upper bound as (score, inclusive); None = unbounded.
    max_score: Option<(OrderedFloat<f64>, bool)>,
}

impl BoundedScoreIter<'_> {
    fn empty() -> Self {
        Self {
            inner: None,
            max_score: None,
        }
    }
}

impl Iterator for BoundedScoreIter<'_> {
    type Item = (OrderedFloat<f64>, u32);

    fn next(&mut self) -> Option<Self::Item> {
        let (score, slot) = self.inner.as_mut()?.next()?;
        if let Some((max, inclusive)) = self.max_score {
            let past = if inclusive { score > max } else { score >= max };
            if past {
                self.inner = None;
                return None;
            }
        }
        Some((score, slot))
    }
}

#[cfg(test)]
mod block_form_tests {
    use super::*;
    use std::collections::BTreeMap;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    /// Model key ordered the way the zset orders: (score, member).
    type ModelKey = (OrderedFloat<f64>, Vec<u8>);

    fn model_contents(model: &BTreeMap<ModelKey, ()>) -> Vec<(Bytes, f64)> {
        model
            .keys()
            .map(|(s, m)| (Bytes::copy_from_slice(m), s.0))
            .collect()
    }

    #[test]
    fn add_update_remove_roundtrip() {
        let mut z = SortedSetValue::new();
        assert!(z.add(b("alice"), 3.0).added);
        assert!(z.add(b("bob"), 1.0).added);
        assert!(z.add(b("carol"), 2.0).added);
        assert_eq!(z.len(), 3);

        // Update moves the member, does not add.
        let res = z.add(b("alice"), 0.5);
        assert!(!res.added);
        assert!(res.changed);
        assert_eq!(res.old_score, Some(3.0));

        assert_eq!(
            z.to_vec(),
            vec![(b("alice"), 0.5), (b("bob"), 1.0), (b("carol"), 2.0)]
        );
        assert_eq!(z.rank(b"alice"), Some(0));
        assert_eq!(z.rev_rank(b"alice"), Some(2));
        assert_eq!(z.get_score(b"carol"), Some(2.0));

        assert_eq!(z.remove(b"bob"), Some(1.0));
        assert_eq!(z.remove(b"bob"), None);
        assert_eq!(z.len(), 2);
        assert!(!z.contains(b"bob"));
    }

    #[test]
    fn churn_bounds_memory_and_preserves_contents() {
        let mut z = SortedSetValue::new();
        // Interleave 64 persistent members with churned ones so live bytes
        // sit between dead bytes and compaction has to move them (patching
        // the handles in the entry table).
        for i in 0..64u32 {
            z.add(
                Bytes::from(format!("keep-{i:02}-{}", "k".repeat(80))),
                1000.0 + i as f64,
            );
            let member = Bytes::from(format!("member-{i}-{}", "x".repeat(150)));
            z.add(member.clone(), i as f64);
            z.remove(&member);
        }
        for round in 0..50usize {
            for i in 0..64u32 {
                let member = Bytes::from(format!("member-{i}-{}", "x".repeat(round * 7 % 200)));
                z.add(member.clone(), i as f64);
                z.remove(&member);
            }
        }
        assert_eq!(z.len(), 64);
        // ~450KB of churned member bytes were appended and released around a
        // ~6KB live set. A sticky footprint this small is only possible if
        // compaction reclaimed the dead bytes (copying live members forward),
        // not just tail-block recycling.
        assert!(
            z.memory_size() < 64 * 1024,
            "memory_size {} after churn",
            z.memory_size()
        );
        assert!(
            z.store.allocated_bytes() < 32 * 1024,
            "allocated {} bytes for a ~6KB live set",
            z.store.allocated_bytes()
        );
        // The persistent members survived every compaction with their exact
        // bytes and scores.
        let got = z.to_vec();
        assert_eq!(got.len(), 64);
        for (i, (member, score)) in got.iter().enumerate() {
            let want = format!("keep-{i:02}-{}", "k".repeat(80));
            assert_eq!(member.as_ref(), want.as_bytes());
            assert_eq!(*score, 1000.0 + i as f64);
        }
    }

    /// Same op history → same reported size, across separate values (each
    /// with its own RandomState). Insert-and-update histories are fully
    /// deterministic: block sizes, entry/free capacities, and skip list
    /// levels (seeded RNG) don't depend on hash values, and the index grows
    /// purely by len. Removal churn is excluded on purpose: hashbrown's
    /// tombstone reuse is hash-dependent, so the index *capacity* after
    /// interleaved remove/insert can differ by a few slots between hasher
    /// instances — the same tradeoff BlockHash and BlockSet accepted for
    /// keeping a HashDoS-resistant per-value random seed.
    #[test]
    fn memory_size_is_run_stable() {
        let run = || {
            let mut z = SortedSetValue::new();
            for i in 0..500u32 {
                z.add(Bytes::from(format!("member-{i:04}")), (i % 37) as f64);
            }
            // Score updates reorder the skip list without touching the index.
            for i in (0..500u32).step_by(3) {
                z.add(Bytes::from(format!("member-{i:04}")), -(i as f64));
            }
            for i in 500..600u32 {
                z.incr(Bytes::from(format!("member-{i:04}")), i as f64)
                    .unwrap();
            }
            z.memory_size()
        };
        assert_eq!(run(), run());
    }

    /// Model-based fuzz: the block-backed zset must agree with a
    /// BTreeMap-ordered model under random ops, across compaction.
    #[derive(Debug, Clone)]
    enum Op {
        Add(u8, i16, u8),
        Remove(u8),
        IncrBy(u8, i16),
        PopMin(u8),
        PopMax(u8),
        Probe(u8),
        RangeByScore(i16, i16),
        RangeByRank(i8, i8),
    }

    use proptest::prelude::*;

    fn op_strategy() -> impl Strategy<Value = Op> {
        prop_oneof![
            (any::<u8>(), any::<i16>(), any::<u8>()).prop_map(|(m, s, pad)| Op::Add(m, s, pad)),
            any::<u8>().prop_map(Op::Remove),
            (any::<u8>(), any::<i16>()).prop_map(|(m, d)| Op::IncrBy(m, d)),
            (0u8..4).prop_map(Op::PopMin),
            (0u8..4).prop_map(Op::PopMax),
            any::<u8>().prop_map(Op::Probe),
            (any::<i16>(), any::<i16>()).prop_map(|(a, b)| Op::RangeByScore(a, b)),
            (any::<i8>(), any::<i8>()).prop_map(|(a, b)| Op::RangeByRank(a, b)),
        ]
    }

    /// Member name for id `m`, padded so block churn crosses block
    /// boundaries and forces compaction under removal-heavy sequences.
    fn member_name(m: u8, pad: u8) -> Vec<u8> {
        let mut name = format!("member-{m:03}").into_bytes();
        name.extend(std::iter::repeat_n(b'p', pad as usize));
        name
    }

    proptest! {
        #[test]
        fn matches_btreemap_model(ops in proptest::collection::vec(op_strategy(), 1..300)) {
            let mut z = SortedSetValue::new();
            // model: (score, member) -> (), plus member -> (score, pad) lookup
            let mut model: BTreeMap<ModelKey, ()> = BTreeMap::new();
            let mut by_member: BTreeMap<u8, (f64, u8)> = BTreeMap::new();

            for op in ops {
                match op {
                    Op::Add(m, s, pad) => {
                        // A member id keeps its first pad so names stay stable.
                        let pad = by_member.get(&m).map(|&(_, p)| p).unwrap_or(pad);
                        let name = member_name(m, pad);
                        let score = s as f64;
                        let res = z.add(Bytes::from(name.clone()), score);
                        let old = by_member.insert(m, (score, pad));
                        match old {
                            None => prop_assert!(res.added),
                            Some((old_score, _)) => {
                                prop_assert!(!res.added);
                                prop_assert_eq!(res.old_score, Some(old_score));
                                model.remove(&(OrderedFloat(old_score), name.clone()));
                            }
                        }
                        model.insert((OrderedFloat(score), name), ());
                    }
                    Op::Remove(m) => {
                        let Some(&(score, pad)) = by_member.get(&m) else {
                            prop_assert_eq!(z.remove(&member_name(m, 0)), None);
                            continue;
                        };
                        let name = member_name(m, pad);
                        prop_assert_eq!(z.remove(&name), Some(score));
                        by_member.remove(&m);
                        model.remove(&(OrderedFloat(score), name));
                    }
                    Op::IncrBy(m, d) => {
                        let pad = by_member.get(&m).map(|&(_, p)| p).unwrap_or(0);
                        let name = member_name(m, pad);
                        let old = by_member.get(&m).map(|&(s, _)| s).unwrap_or(0.0);
                        let new_score = old + d as f64;
                        prop_assert_eq!(z.incr(Bytes::from(name.clone()), d as f64), Ok(new_score));
                        if by_member.contains_key(&m) {
                            model.remove(&(OrderedFloat(old), name.clone()));
                        }
                        by_member.insert(m, (new_score, pad));
                        model.insert((OrderedFloat(new_score), name), ());
                    }
                    Op::PopMin(n) => {
                        let expected: Vec<(Bytes, f64)> = model
                            .keys()
                            .take(n as usize)
                            .map(|(s, m)| (Bytes::copy_from_slice(m), s.0))
                            .collect();
                        let got = z.pop_min(n as usize);
                        prop_assert_eq!(&got, &expected);
                        for (member, score) in &got {
                            model.remove(&(OrderedFloat(*score), member.to_vec()));
                            by_member.retain(|&id, &mut (_, p)| {
                                member_name(id, p) != member.as_ref()
                            });
                        }
                    }
                    Op::PopMax(n) => {
                        let expected: Vec<(Bytes, f64)> = model
                            .keys()
                            .rev()
                            .take(n as usize)
                            .map(|(s, m)| (Bytes::copy_from_slice(m), s.0))
                            .collect();
                        let got = z.pop_max(n as usize);
                        prop_assert_eq!(&got, &expected);
                        for (member, score) in &got {
                            model.remove(&(OrderedFloat(*score), member.to_vec()));
                            by_member.retain(|&id, &mut (_, p)| {
                                member_name(id, p) != member.as_ref()
                            });
                        }
                    }
                    Op::Probe(m) => {
                        match by_member.get(&m) {
                            Some(&(score, pad)) => {
                                let name = member_name(m, pad);
                                prop_assert_eq!(z.get_score(&name), Some(score));
                                prop_assert!(z.contains(&name));
                                let expect_rank =
                                    model.range(..(OrderedFloat(score), name.clone())).count();
                                prop_assert_eq!(z.rank(&name), Some(expect_rank));
                            }
                            None => {
                                // Names embed the id, so an absent id cannot
                                // collide with any live member's name.
                                let name = member_name(m, 0);
                                prop_assert_eq!(z.get_score(&name), None);
                                prop_assert_eq!(z.rank(&name), None);
                                prop_assert!(!z.contains(&name));
                            }
                        }
                    }
                    Op::RangeByScore(a, bnd) => {
                        let (lo, hi) = (a.min(bnd) as f64, a.max(bnd) as f64);
                        let got = z.range_by_score(
                            &ScoreBound::Inclusive(lo),
                            &ScoreBound::Inclusive(hi),
                            0,
                            None,
                        );
                        let want: Vec<(Bytes, f64)> = model
                            .keys()
                            .filter(|(s, _)| s.0 >= lo && s.0 <= hi)
                            .map(|(s, m)| (Bytes::copy_from_slice(m), s.0))
                            .collect();
                        prop_assert_eq!(got, want);
                        prop_assert_eq!(
                            z.count_by_score(
                                &ScoreBound::Inclusive(lo),
                                &ScoreBound::Inclusive(hi)
                            ),
                            model.keys().filter(|(s, _)| s.0 >= lo && s.0 <= hi).count()
                        );
                    }
                    Op::RangeByRank(a, bnd) => {
                        let got = z.range_by_rank(a as i64, bnd as i64);
                        let all = model_contents(&model);
                        let len = all.len() as i64;
                        let start = if (a as i64) < 0 {
                            (len + a as i64).max(0)
                        } else {
                            (a as i64).min(len)
                        } as usize;
                        let end = if (bnd as i64) < 0 {
                            (len + bnd as i64).max(-1)
                        } else {
                            (bnd as i64).min(len - 1)
                        };
                        let want: Vec<(Bytes, f64)> = if len == 0 || end < 0 || start > end as usize
                        {
                            vec![]
                        } else {
                            all[start..=end as usize].to_vec()
                        };
                        prop_assert_eq!(got, want);
                    }
                }
                prop_assert_eq!(z.len(), model.len());
            }

            // Full-state agreement at the end of every sequence.
            prop_assert_eq!(z.to_vec(), model_contents(&model));
        }
    }
}
