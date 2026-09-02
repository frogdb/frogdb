use bytes::Bytes;
use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::time::Instant;

use hashbrown::HashTable;

use super::{EitherIter, ListpackThresholds};
use crate::blockstore::{BlockStore, Handle};
use crate::listpack::Listpack;
use crate::types::string_value::IncrementError;
use crate::types::string_value::format_float;

use rand::RngExt;
use rand::seq::SliceRandom;

// ============================================================================
// Small form — shared listpack, alternating field/value entries
// ============================================================================

/// Pair index of `field` in a listpack laid out `field0, value0, field1, ...`,
/// scanning fields only.
fn lp_find_field(lp: &Listpack, field: &[u8]) -> Option<usize> {
    lp.iter()
        .step_by(2)
        .position(|candidate| candidate == field)
}

/// Iterate `(field, value)` pairs of an alternating listpack as owned `Bytes`.
fn lp_pairs(lp: &Listpack) -> impl Iterator<Item = (Bytes, Bytes)> + '_ {
    let mut iter = lp.iter();
    std::iter::from_fn(move || {
        let field = iter.next()?;
        let value = iter.next().expect("hash listpack holds complete pairs");
        Some((Bytes::copy_from_slice(field), Bytes::copy_from_slice(value)))
    })
}

// ============================================================================
// Large form — block-backed records indexed by a handle table
// ============================================================================

/// One hash entry: a `field ++ value` record in the block store, split at
/// `field_len`.
#[derive(Debug, Clone, Copy)]
struct BlockEntry {
    handle: Handle,
    field_len: u32,
}

/// Large-hash form: entry bytes live in [`BlockStore`] blocks, a dense vec
/// carries the handles (giving HRANDFIELD-style random access by position),
/// and a [`HashTable`] of indices into that vec gives O(1) field lookup — the
/// hash-table *index* survives, the per-entry `Bytes` allocations do not.
#[derive(Debug, Clone)]
struct BlockHash {
    store: BlockStore,
    entries: Vec<BlockEntry>,
    index: HashTable<u32>,
    hasher: RandomState,
}

impl BlockHash {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            store: BlockStore::new(),
            entries: Vec::with_capacity(capacity),
            index: HashTable::with_capacity(capacity),
            hasher: RandomState::new(),
        }
    }

    #[inline]
    fn field_of(&self, idx: u32) -> &[u8] {
        let entry = self.entries[idx as usize];
        &self.store.get(entry.handle)[..entry.field_len as usize]
    }

    #[inline]
    fn value_of(&self, idx: u32) -> &[u8] {
        let entry = self.entries[idx as usize];
        &self.store.get(entry.handle)[entry.field_len as usize..]
    }

    fn find(&self, field: &[u8]) -> Option<u32> {
        let hash = self.hasher.hash_one(field);
        self.index
            .find(hash, |&idx| self.field_of(idx) == field)
            .copied()
    }

    /// Insert or update. Returns true when the field is new.
    fn insert(&mut self, field: &[u8], value: &[u8]) -> bool {
        let hash = self.hasher.hash_one(field);
        if let Some(&idx) = self.index.find(hash, |&idx| self.field_of(idx) == field) {
            let old = self.entries[idx as usize].handle;
            self.store.remove(old);
            self.entries[idx as usize] = BlockEntry {
                handle: self.store.append(&[field, value]),
                field_len: field.len() as u32,
            };
            self.maybe_compact();
            return false;
        }
        let handle = self.store.append(&[field, value]);
        let idx = self.entries.len() as u32;
        self.entries.push(BlockEntry {
            handle,
            field_len: field.len() as u32,
        });
        let (entries, store, hasher) = (&self.entries, &self.store, &self.hasher);
        self.index.insert_unique(hash, idx, |&i| {
            let entry = entries[i as usize];
            hasher.hash_one(&store.get(entry.handle)[..entry.field_len as usize])
        });
        true
    }

    /// Remove a field. Returns true when it existed.
    fn remove(&mut self, field: &[u8]) -> bool {
        let hash = self.hasher.hash_one(field);
        let (index, entries, store) = (&mut self.index, &self.entries, &self.store);
        let idx = match index.find_entry(hash, |&idx| {
            let entry = entries[idx as usize];
            &store.get(entry.handle)[..entry.field_len as usize] == field
        }) {
            Ok(occupied) => occupied.remove().0,
            Err(_) => return false,
        };
        let idx = idx as usize;
        let entry = self.entries.swap_remove(idx);
        self.store.remove(entry.handle);
        if idx < self.entries.len() {
            // The former last entry moved into `idx`; repoint its index slot.
            let moved_from = self.entries.len() as u32;
            let moved_hash = self.hasher.hash_one(self.field_of(idx as u32));
            let slot = self
                .index
                .find_mut(moved_hash, |&i| i == moved_from)
                .expect("moved entry is indexed");
            *slot = idx as u32;
        }
        self.maybe_compact();
        true
    }

    fn maybe_compact(&mut self) {
        if self.store.should_compact() {
            self.store
                .compact(self.entries.iter_mut().map(|entry| &mut entry.handle));
        }
    }

    fn iter(&self) -> impl Iterator<Item = (Bytes, Bytes)> + '_ {
        (0..self.entries.len() as u32).map(|idx| {
            (
                Bytes::copy_from_slice(self.field_of(idx)),
                Bytes::copy_from_slice(self.value_of(idx)),
            )
        })
    }

    fn memory_size(&self) -> usize {
        self.store.allocated_bytes()
            + self.entries.capacity() * std::mem::size_of::<BlockEntry>()
            // Index: one u32 slot plus ~1 control byte per capacity slot.
            + self.index.capacity() * (std::mem::size_of::<u32>() + 1)
    }
}

// ============================================================================
// Hash Type
// ============================================================================

/// Internal encoding for hash values.
#[derive(Debug, Clone)]
enum HashEncoding {
    /// Shared [`Listpack`] with alternating `field, value` entries for small
    /// hashes. O(n) lookups — fast for small N due to cache locality.
    Listpack(Listpack),

    /// Block-backed form for large hashes. O(1) lookups, entry bytes packed
    /// into shared blocks instead of per-entry allocations.
    Blocks(BlockHash),
}

impl Default for HashEncoding {
    fn default() -> Self {
        HashEncoding::Listpack(Listpack::new())
    }
}

/// Hash value - a mapping from field names to values.
#[derive(Debug, Clone)]
pub struct HashValue {
    data: HashEncoding,
    field_expiries: Option<HashMap<Bytes, Instant>>,
}

impl Default for HashValue {
    fn default() -> Self {
        Self::new()
    }
}

impl HashValue {
    /// Create a new empty hash (starts as listpack).
    pub fn new() -> Self {
        Self {
            data: HashEncoding::default(),
            field_expiries: None,
        }
    }

    /// Create a hash from an iterator of field-value pairs, choosing encoding
    /// based on thresholds.
    pub fn from_entries(
        entries: impl IntoIterator<Item = (Bytes, Bytes)>,
        thresholds: ListpackThresholds,
    ) -> Self {
        let entries: Vec<(Bytes, Bytes)> = entries.into_iter().collect();
        let use_listpack = entries.len() <= thresholds.max_entries
            && entries.iter().all(|(k, v)| {
                k.len() <= thresholds.max_value_bytes && v.len() <= thresholds.max_value_bytes
            });

        if use_listpack {
            let mut lp = Listpack::new();
            for (k, v) in &entries {
                lp.push_back(k);
                lp.push_back(v);
            }
            Self {
                data: HashEncoding::Listpack(lp),
                field_expiries: None,
            }
        } else {
            let mut blocks = BlockHash::with_capacity(entries.len());
            for (k, v) in &entries {
                blocks.insert(k, v);
            }
            Self {
                data: HashEncoding::Blocks(blocks),
                field_expiries: None,
            }
        }
    }

    /// Whether this hash uses listpack encoding.
    pub fn is_listpack(&self) -> bool {
        matches!(self.data, HashEncoding::Listpack(_))
    }

    /// Get the number of fields.
    pub fn len(&self) -> usize {
        match &self.data {
            HashEncoding::Listpack(lp) => lp.len() / 2,
            HashEncoding::Blocks(blocks) => blocks.entries.len(),
        }
    }

    /// Check if the hash is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Set a field value. Promotes to the block-backed form if thresholds are
    /// exceeded.
    ///
    /// Returns true if the field is new, false if it was updated.
    pub fn set(&mut self, field: Bytes, value: Bytes, thresholds: ListpackThresholds) -> bool {
        self.remove_field_expiry(&field);
        match &mut self.data {
            HashEncoding::Listpack(lp) => {
                let existing = lp_find_field(lp, &field);
                let new_count = if existing.is_some() {
                    lp.len() / 2
                } else {
                    lp.len() / 2 + 1
                };

                if new_count > thresholds.max_entries
                    || field.len() > thresholds.max_value_bytes
                    || value.len() > thresholds.max_value_bytes
                {
                    // Promote to the block-backed form.
                    let mut blocks = BlockHash::with_capacity(new_count);
                    let mut iter = lp.iter();
                    while let Some(f) = iter.next() {
                        let v = iter.next().expect("hash listpack holds complete pairs");
                        blocks.insert(f, v);
                    }
                    let was_new = blocks.insert(&field, &value);
                    self.data = HashEncoding::Blocks(blocks);
                    was_new
                } else if let Some(pair) = existing {
                    lp.replace(pair * 2 + 1, &value);
                    false
                } else {
                    lp.push_back(&field);
                    lp.push_back(&value);
                    true
                }
            }
            HashEncoding::Blocks(blocks) => blocks.insert(&field, &value),
        }
    }

    /// Set a field value only if it doesn't exist.
    ///
    /// Returns true if the field was set, false if it already existed.
    pub fn set_nx(&mut self, field: Bytes, value: Bytes, thresholds: ListpackThresholds) -> bool {
        if self.contains(&field) {
            return false;
        }
        self.set(field, value, thresholds)
    }

    /// Get a field value.
    pub fn get(&self, field: &[u8]) -> Option<Bytes> {
        match &self.data {
            HashEncoding::Listpack(lp) => {
                let pair = lp_find_field(lp, field)?;
                Some(Bytes::copy_from_slice(
                    lp.get(pair * 2 + 1).expect("value entry follows field"),
                ))
            }
            HashEncoding::Blocks(blocks) => blocks
                .find(field)
                .map(|idx| Bytes::copy_from_slice(blocks.value_of(idx))),
        }
    }

    /// Remove a field.
    ///
    /// Returns true if the field existed.
    pub fn remove(&mut self, field: &[u8]) -> bool {
        self.remove_field_expiry(field);
        match &mut self.data {
            HashEncoding::Listpack(lp) => match lp_find_field(lp, field) {
                Some(pair) => {
                    // Field entry first, then the value that slid into its place.
                    lp.remove(pair * 2);
                    lp.remove(pair * 2);
                    true
                }
                None => false,
            },
            HashEncoding::Blocks(blocks) => blocks.remove(field),
        }
    }

    /// Check if a field exists.
    pub fn contains(&self, field: &[u8]) -> bool {
        match &self.data {
            HashEncoding::Listpack(lp) => lp_find_field(lp, field).is_some(),
            HashEncoding::Blocks(blocks) => blocks.find(field).is_some(),
        }
    }

    /// Get all field names.
    pub fn keys(&self) -> impl Iterator<Item = Bytes> + '_ {
        match &self.data {
            HashEncoding::Listpack(lp) => {
                EitherIter::Left(lp.iter().step_by(2).map(Bytes::copy_from_slice))
            }
            HashEncoding::Blocks(blocks) => EitherIter::Right(
                (0..blocks.entries.len() as u32)
                    .map(|idx| Bytes::copy_from_slice(blocks.field_of(idx))),
            ),
        }
    }

    /// Get all values.
    pub fn values(&self) -> impl Iterator<Item = Bytes> + '_ {
        match &self.data {
            HashEncoding::Listpack(lp) => {
                EitherIter::Left(lp.iter().skip(1).step_by(2).map(Bytes::copy_from_slice))
            }
            HashEncoding::Blocks(blocks) => EitherIter::Right(
                (0..blocks.entries.len() as u32)
                    .map(|idx| Bytes::copy_from_slice(blocks.value_of(idx))),
            ),
        }
    }

    /// Iterate over all field-value pairs.
    pub fn iter(&self) -> impl Iterator<Item = (Bytes, Bytes)> + '_ {
        match &self.data {
            HashEncoding::Listpack(lp) => EitherIter::Left(lp_pairs(lp)),
            HashEncoding::Blocks(blocks) => EitherIter::Right(blocks.iter()),
        }
    }

    /// Increment an integer field by delta.
    ///
    /// If the field doesn't exist, it's created with the delta value.
    /// Returns the new value or an error if the field is not a valid integer.
    pub fn incr_by(
        &mut self,
        field: Bytes,
        delta: i64,
        thresholds: ListpackThresholds,
    ) -> Result<i64, IncrementError> {
        let current = match self.get(&field) {
            Some(val) => std::str::from_utf8(&val)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .ok_or(IncrementError::HashNotInteger)?,
            None => 0,
        };

        let new_val = current.checked_add(delta).ok_or(IncrementError::Overflow)?;
        self.set(field, Bytes::from(new_val.to_string()), thresholds);
        Ok(new_val)
    }

    /// Increment a float field by delta.
    ///
    /// If the field doesn't exist, it's created with the delta value.
    /// Returns the new value or an error if the field is not a valid float.
    pub fn incr_by_float(
        &mut self,
        field: Bytes,
        delta: f64,
        thresholds: ListpackThresholds,
    ) -> Result<f64, IncrementError> {
        let current = match self.get(&field) {
            Some(val) => std::str::from_utf8(&val)
                .ok()
                .and_then(|s| s.parse::<f64>().ok())
                .ok_or(IncrementError::HashNotFloat)?,
            None => 0.0,
        };

        // A stored "nan" field value parses successfully under Rust's f64
        // FromStr (unlike Redis's stricter string2ld), so reject it here to
        // match Redis's "hash value is not a float". An already-infinite
        // field value is left to flow into the sum below, same as
        // `StringValue::increment_float`.
        if current.is_nan() {
            return Err(IncrementError::HashNotFloat);
        }

        let new_val = current + delta;

        if new_val.is_infinite() || new_val.is_nan() {
            return Err(IncrementError::NotFinite);
        }

        self.set(field, Bytes::from(format_float(new_val)), thresholds);
        Ok(new_val)
    }

    /// Get random fields from the hash.
    ///
    /// If count > 0: return up to count unique fields
    /// If count < 0: return |count| fields, allowing duplicates
    pub fn random_fields(&self, count: i64, with_values: bool) -> Vec<(Bytes, Option<Bytes>)> {
        if self.is_empty() || count == 0 {
            return vec![];
        }

        let entries: Vec<(Bytes, Bytes)> = self.iter().collect();
        let mut rng = rand::rng();

        if count > 0 {
            let count = (count as usize).min(entries.len());
            let mut indices: Vec<usize> = (0..entries.len()).collect();
            indices.shuffle(&mut rng);
            indices
                .into_iter()
                .take(count)
                .map(|i| {
                    let (ref k, ref v) = entries[i];
                    (k.clone(), if with_values { Some(v.clone()) } else { None })
                })
                .collect()
        } else {
            let abs_count = count.unsigned_abs() as usize;
            let mut result = Vec::with_capacity(abs_count);
            for _ in 0..abs_count {
                let idx = rand::rng().random_range(0..entries.len());
                let (ref k, ref v) = entries[idx];
                result.push((k.clone(), if with_values { Some(v.clone()) } else { None }));
            }
            result
        }
    }

    /// Calculate approximate memory size.
    pub fn memory_size(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();
        let data_size = match &self.data {
            HashEncoding::Listpack(lp) => lp.byte_len(),
            HashEncoding::Blocks(blocks) => blocks.memory_size(),
        };
        let expiry_size = self
            .field_expiries
            .as_ref()
            .map(|expiries| expiries.keys().map(|k| k.len() + 16 + 32).sum::<usize>())
            .unwrap_or(0);
        base_size + data_size + expiry_size
    }

    /// Get all field-value pairs as a vec for serialization.
    pub fn to_vec(&self) -> Vec<(Bytes, Bytes)> {
        self.iter().collect()
    }

    /// Create a hash from entries with per-field expiry times (for deserialization).
    pub fn from_entries_with_expiries(
        entries: impl IntoIterator<Item = (Bytes, Bytes, Option<Instant>)>,
        thresholds: ListpackThresholds,
    ) -> Self {
        let entries: Vec<(Bytes, Bytes, Option<Instant>)> = entries.into_iter().collect();
        let mut field_expiries: HashMap<Bytes, Instant> = HashMap::new();
        let mut data_entries = Vec::with_capacity(entries.len());

        for (field, value, expiry) in entries {
            if let Some(expires_at) = expiry {
                field_expiries.insert(field.clone(), expires_at);
            }
            data_entries.push((field, value));
        }

        let mut hash = Self::from_entries(data_entries, thresholds);
        if !field_expiries.is_empty() {
            hash.field_expiries = Some(field_expiries);
        }
        hash
    }

    /// Set field expiry time.
    pub fn set_field_expiry(&mut self, field: &[u8], expires_at: Instant) {
        let expiries = self.field_expiries.get_or_insert_with(HashMap::new);
        expiries.insert(Bytes::copy_from_slice(field), expires_at);
    }

    /// Remove field expiry. Returns true if the field had an expiry.
    pub fn remove_field_expiry(&mut self, field: &[u8]) -> bool {
        if let Some(ref mut expiries) = self.field_expiries {
            let removed = expiries.remove(field).is_some();
            if expiries.is_empty() {
                self.field_expiries = None;
            }
            removed
        } else {
            false
        }
    }

    /// Get expiry time for a field.
    pub fn get_field_expiry(&self, field: &[u8]) -> Option<Instant> {
        self.field_expiries.as_ref()?.get(field).copied()
    }

    /// Check if any field has an expiry set.
    pub fn has_field_expiries(&self) -> bool {
        self.field_expiries.as_ref().is_some_and(|e| !e.is_empty())
    }

    /// Access the field expiries map.
    pub fn field_expiries(&self) -> Option<&HashMap<Bytes, Instant>> {
        self.field_expiries.as_ref()
    }

    /// Remove all expired fields from data and field_expiries.
    /// Returns the names of removed fields.
    pub fn remove_expired_fields(&mut self, now: Instant) -> Vec<Bytes> {
        let expiries = match self.field_expiries.take() {
            Some(e) => e,
            None => return vec![],
        };

        let mut removed = Vec::new();
        let mut remaining = HashMap::new();

        for (field, expires_at) in expiries {
            if expires_at <= now {
                self.remove(&field);
                removed.push(field);
            } else {
                remaining.insert(field, expires_at);
            }
        }

        if !remaining.is_empty() {
            self.field_expiries = Some(remaining);
        }

        removed
    }

    /// Get all field-value pairs with their expiry times, for serialization.
    pub fn to_vec_with_expiries(&self) -> Vec<(Bytes, Bytes, Option<Instant>)> {
        self.iter()
            .map(|(field, value)| {
                let expiry = self.get_field_expiry(&field);
                (field, value, expiry)
            })
            .collect()
    }
}

#[cfg(test)]
mod block_form_tests {
    use super::*;
    use proptest::prelude::*;

    /// Tiny thresholds so every test operates on the block-backed form after a
    /// handful of inserts.
    const TINY: ListpackThresholds = ListpackThresholds {
        max_entries: 4,
        max_value_bytes: 16,
    };

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn promotion_preserves_contents_and_updates_in_place() {
        let mut hash = HashValue::new();
        for i in 0..10 {
            assert!(hash.set(b(&format!("f{i}")), b(&format!("v{i}")), TINY));
        }
        assert!(!hash.is_listpack(), "10 > 4 entries must promote");
        assert_eq!(hash.len(), 10);
        for i in 0..10 {
            assert_eq!(
                hash.get(format!("f{i}").as_bytes()).unwrap(),
                b(&format!("v{i}"))
            );
        }
        // Update is not an insert.
        assert!(!hash.set(b("f3"), b("updated"), TINY));
        assert_eq!(hash.get(b"f3").unwrap(), b("updated"));
        assert_eq!(hash.len(), 10);
    }

    #[test]
    fn swap_remove_keeps_the_index_consistent() {
        let mut hash = HashValue::new();
        for i in 0..32 {
            hash.set(b(&format!("field-{i}")), b(&format!("value-{i}")), TINY);
        }
        // Remove from the front so swap_remove keeps relocating tail entries.
        for i in 0..16 {
            assert!(hash.remove(format!("field-{i}").as_bytes()));
            assert!(!hash.remove(format!("field-{i}").as_bytes()));
        }
        assert_eq!(hash.len(), 16);
        for i in 16..32 {
            assert_eq!(
                hash.get(format!("field-{i}").as_bytes()).unwrap(),
                b(&format!("value-{i}"))
            );
        }
    }

    #[test]
    fn churn_keeps_memory_bounded_by_live_payload() {
        let mut hash = HashValue::new();
        let value = "v".repeat(200);
        // Sustained insert/delete churn: the working set stays at 64 fields
        // while 6400 records pass through the store.
        for round in 0..100u32 {
            for i in 0..64u32 {
                hash.set(b(&format!("f{i}")), b(&format!("{value}{round}")), TINY);
            }
        }
        assert_eq!(hash.len(), 64);
        let live_payload: usize = hash.iter().map(|(k, v)| k.len() + v.len()).sum();
        let size = hash.memory_size();
        assert!(
            size < live_payload * 4 + 64 * 1024,
            "memory_size {size} not within a constant factor of live payload {live_payload}"
        );
    }

    #[test]
    fn listpack_update_and_remove_preserve_insertion_order() {
        let mut hash = HashValue::new();
        let thresholds = ListpackThresholds::DEFAULT_HASH;
        hash.set(b("a"), b("1"), thresholds);
        hash.set(b("b"), b("2"), thresholds);
        hash.set(b("c"), b("3"), thresholds);
        assert!(hash.is_listpack());
        // Updating a middle field keeps its position.
        hash.set(b("b"), b("two"), thresholds);
        let pairs: Vec<(Bytes, Bytes)> = hash.iter().collect();
        assert_eq!(
            pairs,
            vec![(b("a"), b("1")), (b("b"), b("two")), (b("c"), b("3"))]
        );
        assert!(hash.remove(b"b"));
        let pairs: Vec<(Bytes, Bytes)> = hash.iter().collect();
        assert_eq!(pairs, vec![(b("a"), b("1")), (b("c"), b("3"))]);
    }

    /// Model-based fuzz: a random op sequence on the block-backed form must
    /// agree with a plain `HashMap` model, across promotion and compaction.
    #[derive(Debug, Clone)]
    enum Op {
        Set(u8, Vec<u8>),
        Remove(u8),
        Get(u8),
    }

    fn op_strategy() -> impl Strategy<Value = Op> {
        prop_oneof![
            (any::<u8>(), proptest::collection::vec(any::<u8>(), 0..300))
                .prop_map(|(k, v)| Op::Set(k, v)),
            any::<u8>().prop_map(Op::Remove),
            any::<u8>().prop_map(Op::Get),
        ]
    }

    proptest! {
        #[test]
        fn block_hash_matches_hashmap_model(ops in proptest::collection::vec(op_strategy(), 1..400)) {
            let thresholds = ListpackThresholds { max_entries: 8, max_value_bytes: 32 };
            let mut hash = HashValue::new();
            let mut model: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

            for op in ops {
                match op {
                    Op::Set(k, v) => {
                        let key = format!("key-{k}").into_bytes();
                        let was_new = hash.set(
                            Bytes::from(key.clone()),
                            Bytes::copy_from_slice(&v),
                            thresholds,
                        );
                        let model_new = model.insert(key, v).is_none();
                        prop_assert_eq!(was_new, model_new);
                    }
                    Op::Remove(k) => {
                        let key = format!("key-{k}").into_bytes();
                        prop_assert_eq!(hash.remove(&key), model.remove(&key).is_some());
                    }
                    Op::Get(k) => {
                        let key = format!("key-{k}").into_bytes();
                        let got = hash.get(&key);
                        let want = model.get(&key);
                        prop_assert_eq!(got.as_deref(), want.map(|v| v.as_slice()));
                        prop_assert_eq!(hash.contains(&key), want.is_some());
                    }
                }
                prop_assert_eq!(hash.len(), model.len());
            }

            let mut got: Vec<(Vec<u8>, Vec<u8>)> =
                hash.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
            let mut want: Vec<(Vec<u8>, Vec<u8>)> =
                model.into_iter().collect();
            got.sort();
            want.sort();
            prop_assert_eq!(got, want);
        }
    }
}
