use bytes::Bytes;
use rand::RngExt;
use rand::seq::SliceRandom;
use std::collections::HashSet;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;

use hashbrown::HashTable;

use super::{EitherIter, ListpackThresholds};
use crate::blockstore::{BlockStore, Handle};
use crate::listpack::Listpack;

// ============================================================================
// Small form — shared listpack, one entry per member
// ============================================================================

/// Index of `member` in the listpack, if present.
fn lp_find_member(lp: &Listpack, member: &[u8]) -> Option<usize> {
    lp.iter().position(|candidate| candidate == member)
}

// ============================================================================
// Large form — block-backed members indexed by a handle table
// ============================================================================

/// Large-set form: member bytes live in [`BlockStore`] blocks, a dense vec
/// carries the handles (SRANDMEMBER/SPOP-style random access by position),
/// and a [`HashTable`] of indices into that vec gives O(1) membership — the
/// hash-table *index* survives, the per-member `Bytes` allocations do not.
#[derive(Debug, Clone)]
struct BlockSet {
    store: BlockStore,
    members: Vec<Handle>,
    index: HashTable<u32>,
    hasher: RandomState,
}

impl BlockSet {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            store: BlockStore::new(),
            members: Vec::with_capacity(capacity),
            index: HashTable::with_capacity(capacity),
            hasher: RandomState::new(),
        }
    }

    #[inline]
    fn member_of(&self, idx: u32) -> &[u8] {
        self.store.get(self.members[idx as usize])
    }

    fn contains(&self, member: &[u8]) -> bool {
        let hash = self.hasher.hash_one(member);
        self.index
            .find(hash, |&idx| self.member_of(idx) == member)
            .is_some()
    }

    /// Add a member. Returns true when it was new.
    fn insert(&mut self, member: &[u8]) -> bool {
        let hash = self.hasher.hash_one(member);
        if self
            .index
            .find(hash, |&idx| self.member_of(idx) == member)
            .is_some()
        {
            return false;
        }
        let handle = self.store.append(&[member]);
        let idx = self.members.len() as u32;
        self.members.push(handle);
        let (members, store, hasher) = (&self.members, &self.store, &self.hasher);
        self.index.insert_unique(hash, idx, |&i| {
            hasher.hash_one(store.get(members[i as usize]))
        });
        true
    }

    /// Remove a member. Returns true when it existed.
    fn remove(&mut self, member: &[u8]) -> bool {
        let hash = self.hasher.hash_one(member);
        let (index, members, store) = (&mut self.index, &self.members, &self.store);
        let idx = match index.find_entry(hash, |&idx| store.get(members[idx as usize]) == member) {
            Ok(occupied) => occupied.remove().0,
            Err(_) => return false,
        };
        let idx = idx as usize;
        let handle = self.members.swap_remove(idx);
        self.store.remove(handle);
        if idx < self.members.len() {
            // The former last member moved into `idx`; repoint its index slot.
            let moved_from = self.members.len() as u32;
            let moved_hash = self.hasher.hash_one(self.member_of(idx as u32));
            let slot = self
                .index
                .find_mut(moved_hash, |&i| i == moved_from)
                .expect("moved member is indexed");
            *slot = idx as u32;
        }
        if self.store.should_compact() {
            self.store.compact(self.members.iter_mut());
        }
        true
    }

    fn iter(&self) -> impl Iterator<Item = Bytes> + '_ {
        (0..self.members.len() as u32).map(|idx| Bytes::copy_from_slice(self.member_of(idx)))
    }

    fn memory_size(&self) -> usize {
        self.store.allocated_bytes()
            + self.members.capacity() * std::mem::size_of::<Handle>()
            // Index: one u32 slot plus ~1 control byte per capacity slot.
            + self.index.capacity() * (std::mem::size_of::<u32>() + 1)
    }
}

// ============================================================================
// Set Type
// ============================================================================

/// Internal encoding for set values.
#[derive(Debug, Clone)]
enum SetEncoding {
    /// Shared [`Listpack`] with one entry per member for small sets.
    /// O(n) lookups — fast for small N due to cache locality.
    Listpack(Listpack),

    /// Block-backed form for large sets. O(1) lookups, member bytes packed
    /// into shared blocks instead of per-member allocations.
    Blocks(BlockSet),
}

impl Default for SetEncoding {
    fn default() -> Self {
        SetEncoding::Listpack(Listpack::new())
    }
}

/// Set value - an unordered collection of unique members.
#[derive(Debug, Clone)]
pub struct SetValue {
    data: SetEncoding,
}

impl Default for SetValue {
    fn default() -> Self {
        Self::new()
    }
}

impl SetValue {
    /// Create a new empty set (starts as listpack).
    pub fn new() -> Self {
        Self {
            data: SetEncoding::default(),
        }
    }

    /// Create a set from an iterator of members, choosing encoding
    /// based on thresholds.
    pub fn from_members(
        members: impl IntoIterator<Item = Bytes>,
        thresholds: ListpackThresholds,
    ) -> Self {
        let members: Vec<Bytes> = members.into_iter().collect();
        let use_listpack = members.len() <= thresholds.max_entries
            && members
                .iter()
                .all(|m| m.len() <= thresholds.max_value_bytes);

        if use_listpack {
            let mut lp = Listpack::new();
            for m in &members {
                lp.push_back(m);
            }
            Self {
                data: SetEncoding::Listpack(lp),
            }
        } else {
            Self::from_large_members(members)
        }
    }

    /// Build the block-backed large form directly, regardless of size — the
    /// shape SUNION/SINTER/SDIFF results take (matching the old behavior of
    /// always producing the hash-table form).
    fn from_large_members(members: impl IntoIterator<Item = Bytes>) -> Self {
        let members: Vec<Bytes> = members.into_iter().collect();
        let mut blocks = BlockSet::with_capacity(members.len());
        for m in &members {
            blocks.insert(m);
        }
        Self {
            data: SetEncoding::Blocks(blocks),
        }
    }

    /// Whether this set uses listpack encoding.
    pub fn is_listpack(&self) -> bool {
        matches!(self.data, SetEncoding::Listpack(_))
    }

    /// Get the number of members.
    pub fn len(&self) -> usize {
        match &self.data {
            SetEncoding::Listpack(lp) => lp.len(),
            SetEncoding::Blocks(blocks) => blocks.members.len(),
        }
    }

    /// Check if the set is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Add a member to the set. Promotes to the block-backed form if
    /// thresholds are exceeded.
    ///
    /// Returns true if the member was new, false if it already existed.
    pub fn add(&mut self, member: Bytes, thresholds: ListpackThresholds) -> bool {
        match &mut self.data {
            SetEncoding::Listpack(lp) => {
                if lp_find_member(lp, &member).is_some() {
                    return false;
                }
                let new_count = lp.len() + 1;
                if new_count > thresholds.max_entries || member.len() > thresholds.max_value_bytes {
                    // Promote to the block-backed form.
                    let mut blocks = BlockSet::with_capacity(new_count);
                    for m in lp.iter() {
                        blocks.insert(m);
                    }
                    blocks.insert(&member);
                    self.data = SetEncoding::Blocks(blocks);
                } else {
                    lp.push_back(&member);
                }
                true
            }
            SetEncoding::Blocks(blocks) => blocks.insert(&member),
        }
    }

    /// Remove a member from the set.
    ///
    /// Returns true if the member existed.
    pub fn remove(&mut self, member: &[u8]) -> bool {
        match &mut self.data {
            SetEncoding::Listpack(lp) => match lp_find_member(lp, member) {
                Some(idx) => {
                    lp.remove(idx);
                    true
                }
                None => false,
            },
            SetEncoding::Blocks(blocks) => blocks.remove(member),
        }
    }

    /// Check if a member exists.
    pub fn contains(&self, member: &[u8]) -> bool {
        match &self.data {
            SetEncoding::Listpack(lp) => lp_find_member(lp, member).is_some(),
            SetEncoding::Blocks(blocks) => blocks.contains(member),
        }
    }

    /// Get all members.
    pub fn members(&self) -> impl Iterator<Item = Bytes> + '_ {
        match &self.data {
            SetEncoding::Listpack(lp) => EitherIter::Left(lp.iter().map(Bytes::copy_from_slice)),
            SetEncoding::Blocks(blocks) => EitherIter::Right(blocks.iter()),
        }
    }

    /// Compute the union of this set with others.
    pub fn union<'a>(&'a self, others: impl Iterator<Item = &'a SetValue>) -> SetValue {
        let mut result: HashSet<Bytes> = self.members().collect();
        for other in others {
            for member in other.members() {
                result.insert(member);
            }
        }
        SetValue::from_large_members(result)
    }

    /// Compute the intersection of this set with others.
    pub fn intersection<'a>(&'a self, others: impl Iterator<Item = &'a SetValue>) -> SetValue {
        let mut result: HashSet<Bytes> = self.members().collect();
        for other in others {
            result.retain(|m| other.contains(m));
        }
        SetValue::from_large_members(result)
    }

    /// Compute the difference of this set minus others.
    pub fn difference<'a>(&'a self, others: impl Iterator<Item = &'a SetValue>) -> SetValue {
        let mut result: HashSet<Bytes> = self.members().collect();
        for other in others {
            result.retain(|m| !other.contains(m));
        }
        SetValue::from_large_members(result)
    }

    /// Pop a random member from the set.
    ///
    /// Returns None if the set is empty.
    pub fn pop(&mut self) -> Option<Bytes> {
        if self.is_empty() {
            return None;
        }
        let idx = rand::rng().random_range(0..self.len());
        let member = match &self.data {
            SetEncoding::Listpack(lp) => Bytes::copy_from_slice(lp.get(idx).expect("idx < len")),
            SetEncoding::Blocks(blocks) => Bytes::copy_from_slice(blocks.member_of(idx as u32)),
        };
        self.remove(&member);
        Some(member)
    }

    /// Pop multiple random members from the set.
    pub fn pop_many(&mut self, count: usize) -> Vec<Bytes> {
        let count = count.min(self.len());
        let mut result = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(member) = self.pop() {
                result.push(member);
            } else {
                break;
            }
        }
        result
    }

    /// Get random members without removing them.
    ///
    /// If count > 0: return up to count unique members
    /// If count < 0: return |count| members, allowing duplicates
    pub fn random_members(&self, count: i64) -> Vec<Bytes> {
        if self.is_empty() || count == 0 {
            return vec![];
        }

        let members: Vec<Bytes> = self.members().collect();
        let mut rng = rand::rng();

        if count > 0 {
            let count = (count as usize).min(members.len());
            let mut shuffled = members;
            shuffled.shuffle(&mut rng);
            shuffled.into_iter().take(count).collect()
        } else {
            let count = (-count) as usize;
            let mut result = Vec::with_capacity(count);
            for _ in 0..count {
                let idx = rng.random_range(0..members.len());
                result.push(members[idx].clone());
            }
            result
        }
    }

    /// Calculate approximate memory size.
    pub fn memory_size(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();
        match &self.data {
            SetEncoding::Listpack(lp) => base_size + lp.byte_len(),
            SetEncoding::Blocks(blocks) => base_size + blocks.memory_size(),
        }
    }

    /// Get all members as a vec for serialization.
    pub fn to_vec(&self) -> Vec<Bytes> {
        self.members().collect()
    }
}

#[cfg(test)]
mod block_form_tests {
    use super::*;
    use proptest::prelude::*;

    const TINY: ListpackThresholds = ListpackThresholds {
        max_entries: 4,
        max_value_bytes: 16,
    };

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn promotion_preserves_membership() {
        let mut set = SetValue::new();
        for i in 0..10 {
            assert!(set.add(b(&format!("m{i}")), TINY));
            assert!(!set.add(b(&format!("m{i}")), TINY));
        }
        assert!(!set.is_listpack(), "10 > 4 members must promote");
        assert_eq!(set.len(), 10);
        for i in 0..10 {
            assert!(set.contains(format!("m{i}").as_bytes()));
        }
        assert!(!set.contains(b"absent"));
    }

    #[test]
    fn swap_remove_keeps_the_index_consistent() {
        let mut set = SetValue::new();
        for i in 0..32 {
            set.add(b(&format!("member-{i}")), TINY);
        }
        for i in 0..16 {
            assert!(set.remove(format!("member-{i}").as_bytes()));
            assert!(!set.remove(format!("member-{i}").as_bytes()));
        }
        assert_eq!(set.len(), 16);
        for i in 16..32 {
            assert!(set.contains(format!("member-{i}").as_bytes()));
        }
    }

    #[test]
    fn set_operations_produce_correct_membership() {
        let a = SetValue::from_members([b("1"), b("2"), b("3")], TINY);
        let c = SetValue::from_members([b("2"), b("3"), b("4")], TINY);
        let union = a.union([&c].into_iter());
        assert_eq!(union.len(), 4);
        let inter = a.intersection([&c].into_iter());
        assert_eq!(inter.len(), 2);
        assert!(inter.contains(b"2") && inter.contains(b"3"));
        let diff = a.difference([&c].into_iter());
        assert_eq!(diff.len(), 1);
        assert!(diff.contains(b"1"));
    }

    #[test]
    fn churn_keeps_memory_bounded_by_live_payload() {
        let mut set = SetValue::new();
        let payload = "m".repeat(200);
        for round in 0..100u32 {
            for i in 0..64u32 {
                let member = b(&format!("{payload}-{i}"));
                set.remove(&member);
                set.add(b(&format!("{payload}-{i}-{round}")), TINY);
                set.remove(format!("{payload}-{i}-{round}").as_bytes());
                set.add(member, TINY);
            }
        }
        assert_eq!(set.len(), 64);
        let live_payload: usize = set.members().map(|m| m.len()).sum();
        let size = set.memory_size();
        assert!(
            size < live_payload * 4 + 64 * 1024,
            "memory_size {size} not within a constant factor of live payload {live_payload}"
        );
    }

    #[derive(Debug, Clone)]
    enum Op {
        Add(u8),
        Remove(u8),
        Contains(u8),
        Pop,
    }

    fn op_strategy() -> impl Strategy<Value = Op> {
        prop_oneof![
            any::<u8>().prop_map(Op::Add),
            any::<u8>().prop_map(Op::Remove),
            any::<u8>().prop_map(Op::Contains),
            Just(Op::Pop),
        ]
    }

    proptest! {
        #[test]
        fn block_set_matches_hashset_model(ops in proptest::collection::vec(op_strategy(), 1..400)) {
            let thresholds = ListpackThresholds { max_entries: 8, max_value_bytes: 32 };
            let mut set = SetValue::new();
            let mut model: HashSet<Vec<u8>> = HashSet::new();

            for op in ops {
                match op {
                    Op::Add(m) => {
                        let member = format!("member-{m}").into_bytes();
                        prop_assert_eq!(
                            set.add(Bytes::from(member.clone()), thresholds),
                            model.insert(member)
                        );
                    }
                    Op::Remove(m) => {
                        let member = format!("member-{m}").into_bytes();
                        prop_assert_eq!(set.remove(&member), model.remove(member.as_slice()));
                    }
                    Op::Contains(m) => {
                        let member = format!("member-{m}").into_bytes();
                        prop_assert_eq!(set.contains(&member), model.contains(member.as_slice()));
                    }
                    Op::Pop => {
                        let popped = set.pop();
                        match popped {
                            Some(member) => prop_assert!(model.remove(member.as_ref())),
                            None => prop_assert!(model.is_empty()),
                        }
                    }
                }
                prop_assert_eq!(set.len(), model.len());
            }

            let mut got: Vec<Vec<u8>> = set.members().map(|m| m.to_vec()).collect();
            let mut want: Vec<Vec<u8>> = model.into_iter().collect();
            got.sort();
            want.sort();
            prop_assert_eq!(got, want);
        }
    }
}
