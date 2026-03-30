//! Sorted set types: ScoreBound, LexBound, ScoreIndex, SortedSetValue.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU8, Ordering as AtomicOrdering};

use bytes::Bytes;
use ordered_float::OrderedFloat;

use crate::skiplist::SkipList;

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

    /// Get the value for BTreeMap range queries (minimum bound).
    pub fn start_bound_value(&self) -> Option<OrderedFloat<f64>> {
        match self {
            ScoreBound::NegInf => None,
            ScoreBound::PosInf => Some(OrderedFloat(f64::INFINITY)),
            ScoreBound::Inclusive(v) | ScoreBound::Exclusive(v) => Some(OrderedFloat(*v)),
        }
    }

    /// Convert to a BTreeMap lower bound for `(OrderedFloat<f64>, Bytes)` keys.
    fn to_btree_lower(self) -> std::ops::Bound<(OrderedFloat<f64>, Bytes)> {
        match self {
            ScoreBound::NegInf => std::ops::Bound::Unbounded,
            ScoreBound::PosInf => {
                std::ops::Bound::Excluded((OrderedFloat(f64::INFINITY), Bytes::new()))
            }
            ScoreBound::Inclusive(v) => std::ops::Bound::Included((OrderedFloat(v), Bytes::new())),
            ScoreBound::Exclusive(v) => {
                // No floats between v and next_up, so Included(next_up) excludes v
                std::ops::Bound::Included((OrderedFloat(v.next_up()), Bytes::new()))
            }
        }
    }

    /// Convert to a BTreeMap upper bound for `(OrderedFloat<f64>, Bytes)` keys.
    fn to_btree_upper(self) -> std::ops::Bound<(OrderedFloat<f64>, Bytes)> {
        match self {
            ScoreBound::PosInf => std::ops::Bound::Unbounded,
            ScoreBound::NegInf => {
                std::ops::Bound::Excluded((OrderedFloat(f64::NEG_INFINITY), Bytes::new()))
            }
            ScoreBound::Inclusive(v) => {
                let next = v.next_up();
                if next.is_infinite() && !v.is_infinite() {
                    // v is f64::MAX, next_up would be INFINITY — use Unbounded
                    std::ops::Bound::Unbounded
                } else {
                    // Excluded(next_up, empty) — empty Bytes sorts first, so all entries at score v are included
                    std::ops::Bound::Excluded((OrderedFloat(next), Bytes::new()))
                }
            }
            ScoreBound::Exclusive(v) => {
                // Excluded(v, empty) — empty Bytes sorts first, so all entries at score v are excluded
                std::ops::Bound::Excluded((OrderedFloat(v), Bytes::new()))
            }
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
// ScoreIndex: pluggable backend for sorted set score ordering
// ============================================================================

static SCORE_INDEX_BACKEND: AtomicU8 = AtomicU8::new(1); // 0=BTree, 1=SkipList (default)

/// Which index backend new sorted sets should use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScoreIndexBackend {
    BTree = 0,
    SkipList = 1,
}

/// Set the default score index backend (called once at startup).
pub fn set_default_score_index(backend: ScoreIndexBackend) {
    SCORE_INDEX_BACKEND.store(backend as u8, AtomicOrdering::Relaxed);
}

fn default_score_index_backend() -> ScoreIndexBackend {
    match SCORE_INDEX_BACKEND.load(AtomicOrdering::Relaxed) {
        0 => ScoreIndexBackend::BTree,
        _ => ScoreIndexBackend::SkipList,
    }
}

/// Score index that dispatches to either BTreeMap or SkipList.
#[derive(Debug, Clone)]
enum ScoreIndex {
    BTree(BTreeMap<(OrderedFloat<f64>, Bytes), ()>),
    SkipList(SkipList),
}

impl ScoreIndex {
    fn new(backend: ScoreIndexBackend) -> Self {
        match backend {
            ScoreIndexBackend::BTree => ScoreIndex::BTree(BTreeMap::new()),
            ScoreIndexBackend::SkipList => ScoreIndex::SkipList(SkipList::new()),
        }
    }

    fn insert(&mut self, score: OrderedFloat<f64>, member: Bytes) {
        match self {
            ScoreIndex::BTree(bt) => {
                bt.insert((score, member), ());
            }
            ScoreIndex::SkipList(sl) => {
                sl.insert(score, member);
            }
        }
    }

    fn remove(&mut self, score: OrderedFloat<f64>, member: &Bytes) {
        match self {
            ScoreIndex::BTree(bt) => {
                bt.remove(&(score, member.clone()));
            }
            ScoreIndex::SkipList(sl) => {
                sl.remove(score, member);
            }
        }
    }

    #[allow(dead_code)]
    fn len(&self) -> usize {
        match self {
            ScoreIndex::BTree(bt) => bt.len(),
            ScoreIndex::SkipList(sl) => sl.len(),
        }
    }

    fn rank(&self, score: OrderedFloat<f64>, member: &Bytes) -> Option<usize> {
        match self {
            ScoreIndex::BTree(bt) => {
                let key = (score, member.clone());
                Some(bt.range(..&key).count())
            }
            ScoreIndex::SkipList(sl) => sl.rank(score, member),
        }
    }

    fn pop_first(&mut self) -> Option<(OrderedFloat<f64>, Bytes)> {
        match self {
            ScoreIndex::BTree(bt) => bt.pop_first().map(|((s, m), _)| (s, m)),
            ScoreIndex::SkipList(sl) => sl.pop_first(),
        }
    }

    fn pop_last(&mut self) -> Option<(OrderedFloat<f64>, Bytes)> {
        match self {
            ScoreIndex::BTree(bt) => bt.pop_last().map(|((s, m), _)| (s, m)),
            ScoreIndex::SkipList(sl) => sl.pop_last(),
        }
    }

    fn iter(&self) -> ScoreIndexIter<'_> {
        match self {
            ScoreIndex::BTree(bt) => ScoreIndexIter::BTree(bt.iter()),
            ScoreIndex::SkipList(sl) => ScoreIndexIter::SkipList(sl.iter()),
        }
    }

    fn rev_iter(&self) -> ScoreIndexRevIter<'_> {
        match self {
            ScoreIndex::BTree(bt) => ScoreIndexRevIter::BTree(bt.iter().rev()),
            ScoreIndex::SkipList(sl) => ScoreIndexRevIter::SkipList(sl.rev_iter()),
        }
    }

    fn range_by_score_iter<'a>(&'a self, min: &ScoreBound, max: &ScoreBound) -> ScoreIndexIter<'a> {
        match self {
            ScoreIndex::BTree(bt) => {
                ScoreIndexIter::BTreeRange(bt.range((min.to_btree_lower(), max.to_btree_upper())))
            }
            ScoreIndex::SkipList(sl) => {
                let (min_score, min_inclusive) = match min {
                    ScoreBound::NegInf => (OrderedFloat(f64::NEG_INFINITY), true),
                    ScoreBound::PosInf => {
                        return ScoreIndexIter::Empty;
                    }
                    ScoreBound::Inclusive(v) => (OrderedFloat(*v), true),
                    ScoreBound::Exclusive(v) => (OrderedFloat(*v), false),
                };
                let max_score = match max {
                    ScoreBound::PosInf => None,
                    ScoreBound::NegInf => return ScoreIndexIter::Empty,
                    ScoreBound::Inclusive(v) => Some((OrderedFloat(*v), true)),
                    ScoreBound::Exclusive(v) => Some((OrderedFloat(*v), false)),
                };
                ScoreIndexIter::SkipListBounded {
                    inner: sl.range_by_score(min_score, min_inclusive),
                    max_score,
                }
            }
        }
    }

    fn rev_range_by_score_iter<'a>(
        &'a self,
        min: &ScoreBound,
        max: &ScoreBound,
    ) -> ScoreIndexRevIter<'a> {
        match self {
            ScoreIndex::BTree(bt) => ScoreIndexRevIter::BTreeRange(
                bt.range((min.to_btree_lower(), max.to_btree_upper())).rev(),
            ),
            ScoreIndex::SkipList(_) => {
                // For reverse score range on skip list, we use the forward range
                // then collect and reverse. This matches the BTreeMap approach.
                // A more optimal approach would be a reverse score-bounded iterator,
                // but for now this is correct and matches BTreeMap's complexity.
                ScoreIndexRevIter::Collected(
                    self.range_by_score_iter(min, max)
                        .collect::<Vec<_>>()
                        .into_iter()
                        .rev()
                        .collect::<Vec<_>>()
                        .into_iter(),
                )
            }
        }
    }

    fn range_by_rank_iter(&self, start: usize, count: usize) -> ScoreIndexIter<'_> {
        match self {
            ScoreIndex::BTree(bt) => ScoreIndexIter::BTreeSkip {
                inner: bt.iter(),
                skip: start,
                remaining: count,
            },
            ScoreIndex::SkipList(sl) => ScoreIndexIter::SkipListTake {
                inner: sl.range_by_rank_iter(start),
                remaining: count,
            },
        }
    }

    fn memory_size(&self) -> usize {
        match self {
            ScoreIndex::BTree(bt) => bt
                .iter()
                .map(|((_, member), _)| {
                    member.len() + std::mem::size_of::<OrderedFloat<f64>>() + 32
                })
                .sum(),
            ScoreIndex::SkipList(sl) => sl.memory_size(),
        }
    }
}

/// Forward iterator over ScoreIndex entries.
enum ScoreIndexIter<'a> {
    BTree(std::collections::btree_map::Iter<'a, (OrderedFloat<f64>, Bytes), ()>),
    BTreeRange(std::collections::btree_map::Range<'a, (OrderedFloat<f64>, Bytes), ()>),
    BTreeSkip {
        inner: std::collections::btree_map::Iter<'a, (OrderedFloat<f64>, Bytes), ()>,
        skip: usize,
        remaining: usize,
    },
    SkipList(crate::skiplist::SkipListIter<'a>),
    SkipListBounded {
        inner: crate::skiplist::SkipListIter<'a>,
        max_score: Option<(OrderedFloat<f64>, bool)>, // (score, inclusive)
    },
    SkipListTake {
        inner: crate::skiplist::SkipListIter<'a>,
        remaining: usize,
    },
    Empty,
}

impl<'a> Iterator for ScoreIndexIter<'a> {
    type Item = (OrderedFloat<f64>, &'a Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ScoreIndexIter::BTree(it) => it.next().map(|((s, m), _)| (*s, m)),
            ScoreIndexIter::BTreeRange(it) => it.next().map(|((s, m), _)| (*s, m)),
            ScoreIndexIter::BTreeSkip {
                inner,
                skip,
                remaining,
            } => {
                while *skip > 0 {
                    inner.next()?;
                    *skip -= 1;
                }
                if *remaining == 0 {
                    return None;
                }
                *remaining -= 1;
                inner.next().map(|((s, m), _)| (*s, m))
            }
            ScoreIndexIter::SkipList(it) => it.next(),
            ScoreIndexIter::SkipListBounded { inner, max_score } => {
                let (score, member) = inner.next()?;
                if let Some((max_s, inclusive)) = max_score {
                    if *inclusive {
                        if score > *max_s {
                            return None;
                        }
                    } else if score >= *max_s {
                        return None;
                    }
                }
                Some((score, member))
            }
            ScoreIndexIter::SkipListTake { inner, remaining } => {
                if *remaining == 0 {
                    return None;
                }
                *remaining -= 1;
                inner.next()
            }
            ScoreIndexIter::Empty => None,
        }
    }
}

/// Reverse iterator over ScoreIndex entries.
enum ScoreIndexRevIter<'a> {
    BTree(std::iter::Rev<std::collections::btree_map::Iter<'a, (OrderedFloat<f64>, Bytes), ()>>),
    BTreeRange(
        std::iter::Rev<std::collections::btree_map::Range<'a, (OrderedFloat<f64>, Bytes), ()>>,
    ),
    SkipList(crate::skiplist::SkipListRevIter<'a>),
    Collected(std::vec::IntoIter<(OrderedFloat<f64>, &'a Bytes)>),
}

impl<'a> Iterator for ScoreIndexRevIter<'a> {
    type Item = (OrderedFloat<f64>, &'a Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ScoreIndexRevIter::BTree(it) => it.next().map(|((s, m), _)| (*s, m)),
            ScoreIndexRevIter::BTreeRange(it) => it.next().map(|((s, m), _)| (*s, m)),
            ScoreIndexRevIter::SkipList(it) => it.next(),
            ScoreIndexRevIter::Collected(it) => it.next(),
        }
    }
}

/// Sorted set value with dual indexing for O(1) score lookup and O(log n) range queries.
#[derive(Debug, Clone)]
pub struct SortedSetValue {
    /// O(1) lookup: member -> score
    members: HashMap<Bytes, f64>,
    /// Ordered index for range queries (BTreeMap or SkipList).
    index: ScoreIndex,
}

impl Default for SortedSetValue {
    fn default() -> Self {
        Self::new()
    }
}

impl SortedSetValue {
    /// Create a new empty sorted set using the configured default backend.
    pub fn new() -> Self {
        Self {
            members: HashMap::new(),
            index: ScoreIndex::new(default_score_index_backend()),
        }
    }

    /// Create a new empty sorted set with a specific backend.
    pub fn with_backend(backend: ScoreIndexBackend) -> Self {
        Self {
            members: HashMap::new(),
            index: ScoreIndex::new(backend),
        }
    }

    /// Get the number of members.
    pub fn len(&self) -> usize {
        self.members.len()
    }

    /// Check if the set is empty.
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Add or update a member with a score.
    ///
    /// Returns information about what changed.
    pub fn add(&mut self, member: Bytes, score: f64) -> ZAddResult {
        if let Some(&old_score) = self.members.get(&member) {
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
            // Remove old entry from index, insert new entry
            self.index.remove(OrderedFloat(old_score), &member);
            self.index.insert(OrderedFloat(score), member.clone());
            self.members.insert(member, score);
            ZAddResult {
                added: false,
                changed: true,
                old_score: Some(old_score),
            }
        } else {
            // New member: insert into index first (needs clone), then move into members
            self.index.insert(OrderedFloat(score), member.clone());
            self.members.insert(member, score);
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
        if let Some((member_key, score)) = self.members.remove_entry(member) {
            self.index.remove(OrderedFloat(score), &member_key);
            Some(score)
        } else {
            None
        }
    }

    /// Get the score of a member.
    pub fn get_score(&self, member: &[u8]) -> Option<f64> {
        self.members.get(member).copied()
    }

    /// Check if a member exists.
    pub fn contains(&self, member: &[u8]) -> bool {
        self.members.contains_key(member)
    }

    /// Get the 0-based rank of a member (ascending by score).
    pub fn rank(&self, member: &[u8]) -> Option<usize> {
        let (member_key, &score) = self.members.get_key_value(member)?;
        self.index.rank(OrderedFloat(score), member_key)
    }

    /// Get the 0-based rank of a member (descending by score).
    pub fn rev_rank(&self, member: &[u8]) -> Option<usize> {
        let rank = self.rank(member)?;
        Some(self.len() - 1 - rank)
    }

    /// Increment the score of a member.
    ///
    /// If the member doesn't exist, it's created with the given increment as its score.
    /// Returns the new score.
    pub fn incr(&mut self, member: Bytes, increment: f64) -> f64 {
        let existing = self.members.get(&member).copied();
        let old_score = existing.unwrap_or(0.0);
        let new_score = old_score + increment;

        // Check for overflow to infinity
        if new_score.is_infinite() && !old_score.is_infinite() && !increment.is_infinite() {
            // This would be an error in Redis, but we'll handle it
            return new_score;
        }

        if existing.is_some() {
            self.index.remove(OrderedFloat(old_score), &member);
            self.index.insert(OrderedFloat(new_score), member.clone());
            self.members.insert(member, new_score);
        } else {
            self.members.insert(member.clone(), new_score);
            self.index.insert(OrderedFloat(new_score), member);
        }

        new_score
    }

    /// Get members by rank range (inclusive).
    ///
    /// `start` and `end` are 0-based indices. Negative indices count from the end.
    pub fn range_by_rank(&self, start: i64, end: i64) -> Vec<(Bytes, f64)> {
        let len = self.len() as i64;
        if len == 0 {
            return vec![];
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
            return vec![];
        }

        let end = end as usize;

        self.index
            .range_by_rank_iter(start, end - start + 1)
            .map(|(score, member)| (member.clone(), score.0))
            .collect()
    }

    /// Get members by rank range in reverse order (descending by score).
    pub fn rev_range_by_rank(&self, start: i64, end: i64) -> Vec<(Bytes, f64)> {
        let len = self.len() as i64;
        if len == 0 {
            return vec![];
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
            return vec![];
        }

        let end = end as usize;

        self.index
            .rev_iter()
            .skip(start)
            .take(end - start + 1)
            .map(|(score, member)| (member.clone(), score.0))
            .collect()
    }

    /// Get members by score range.
    pub fn range_by_score(
        &self,
        min: &ScoreBound,
        max: &ScoreBound,
        offset: usize,
        count: Option<usize>,
    ) -> Vec<(Bytes, f64)> {
        let iter = self.index.range_by_score_iter(min, max).skip(offset);

        if let Some(count) = count {
            iter.take(count)
                .map(|(score, member)| (member.clone(), score.0))
                .collect()
        } else {
            iter.map(|(score, member)| (member.clone(), score.0))
                .collect()
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
        let iter = self.index.rev_range_by_score_iter(min, max).skip(offset);

        if let Some(count) = count {
            iter.take(count)
                .map(|(score, member)| (member.clone(), score.0))
                .collect()
        } else {
            iter.map(|(score, member)| (member.clone(), score.0))
                .collect()
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
        // For lex range, we iterate in (score, member) order
        // This naturally gives us lexicographic order for same scores
        let iter = self
            .index
            .iter()
            .filter(|(_, member)| min.satisfies_min(member) && max.satisfies_max(member))
            .skip(offset);

        if let Some(count) = count {
            iter.take(count)
                .map(|(score, member)| (member.clone(), score.0))
                .collect()
        } else {
            iter.map(|(score, member)| (member.clone(), score.0))
                .collect()
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
            .index
            .rev_iter()
            .filter(|(_, member)| min.satisfies_min(member) && max.satisfies_max(member))
            .skip(offset);

        if let Some(count) = count {
            iter.take(count)
                .map(|(score, member)| (member.clone(), score.0))
                .collect()
        } else {
            iter.map(|(score, member)| (member.clone(), score.0))
                .collect()
        }
    }

    /// Count members in score range.
    pub fn count_by_score(&self, min: &ScoreBound, max: &ScoreBound) -> usize {
        self.index.range_by_score_iter(min, max).count()
    }

    /// Count members in lex range.
    pub fn count_by_lex(&self, min: &LexBound, max: &LexBound) -> usize {
        self.index
            .iter()
            .filter(|(_, member)| min.satisfies_min(member) && max.satisfies_max(member))
            .count()
    }

    /// Pop members with minimum scores.
    pub fn pop_min(&mut self, count: usize) -> Vec<(Bytes, f64)> {
        let mut result = Vec::with_capacity(count.min(self.len()));
        for _ in 0..count {
            if let Some((score, member)) = self.index.pop_first() {
                self.members.remove(&member);
                result.push((member, score.0));
            } else {
                break;
            }
        }
        result
    }

    /// Pop members with maximum scores.
    pub fn pop_max(&mut self, count: usize) -> Vec<(Bytes, f64)> {
        let mut result = Vec::with_capacity(count.min(self.len()));
        for _ in 0..count {
            if let Some((score, member)) = self.index.pop_last() {
                self.members.remove(&member);
                result.push((member, score.0));
            } else {
                break;
            }
        }
        result
    }

    /// Remove members by rank range.
    ///
    /// Returns the number of members removed.
    pub fn remove_range_by_rank(&mut self, start: i64, end: i64) -> usize {
        let len = self.len() as i64;
        if len == 0 {
            return 0;
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
            return 0;
        }

        let end = end as usize;

        let to_remove: Vec<_> = self
            .index
            .range_by_rank_iter(start, end - start + 1)
            .map(|(score, member)| (score, member.clone()))
            .collect();

        let count = to_remove.len();
        for (score, member) in to_remove {
            self.index.remove(score, &member);
            self.members.remove(&member);
        }
        count
    }

    /// Remove members by score range.
    ///
    /// Returns the number of members removed.
    pub fn remove_range_by_score(&mut self, min: &ScoreBound, max: &ScoreBound) -> usize {
        let to_remove: Vec<_> = self
            .index
            .range_by_score_iter(min, max)
            .map(|(score, member)| (score, member.clone()))
            .collect();

        let count = to_remove.len();
        for (score, member) in to_remove {
            self.index.remove(score, &member);
            self.members.remove(&member);
        }
        count
    }

    /// Remove members by lex range.
    ///
    /// Returns the number of members removed.
    pub fn remove_range_by_lex(&mut self, min: &LexBound, max: &LexBound) -> usize {
        let to_remove: Vec<_> = self
            .index
            .iter()
            .filter(|(_, member)| min.satisfies_min(member) && max.satisfies_max(member))
            .map(|(score, member)| (score, member.clone()))
            .collect();

        let count = to_remove.len();
        for (score, member) in to_remove {
            self.index.remove(score, &member);
            self.members.remove(&member);
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
            self.index
                .iter()
                .sample(&mut rng, count)
                .into_iter()
                .map(|(score, member)| (member.clone(), score.0))
                .collect()
        } else {
            // Allow duplicates: pick randomly with replacement
            let members: Vec<_> = self
                .index
                .iter()
                .map(|(score, member)| (member, score.0))
                .collect();
            let n = (-count) as usize;
            let mut result = Vec::with_capacity(n);
            for _ in 0..n {
                let idx = rng.random_range(0..members.len());
                let (member, score) = members[idx];
                result.push((member.clone(), score));
            }
            result
        }
    }

    /// Calculate approximate memory size.
    pub fn memory_size(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();

        // HashMap overhead + entries
        let members_size: usize = self
            .members
            .keys()
            .map(|k| k.len() + std::mem::size_of::<f64>() + 32) // 32 for HashMap node overhead
            .sum();

        let index_size = self.index.memory_size();

        base_size + members_size + index_size
    }

    /// Iterate over all members in score order.
    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, f64)> + '_ {
        self.index.iter().map(|(score, member)| (member, score.0))
    }

    /// Get all members and scores as a vec for serialization.
    pub fn to_vec(&self) -> Vec<(Bytes, f64)> {
        self.index
            .iter()
            .map(|(score, member)| (member.clone(), score.0))
            .collect()
    }
}
