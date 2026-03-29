//! HashMap-based store implementation.

use bytes::Bytes;
use frogdb_persistence::{RocksStore, deserialize, serialize};
use griddle::HashMap;
use rand::seq::IteratorRandom;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

use crate::LabelIndex;
use crate::glob::glob_match;
use crate::noop::{ExpiryIndex, FieldExpiryIndex};
use crate::shard::slot_for_key;
use crate::types::{KeyMetadata, KeyType, SetCondition, SetOptions, SetResult, Value};

use super::Store;

/// Where a key's value currently resides.
#[derive(Debug)]
enum ValueLocation {
    /// Value is in RAM (normal case).
    Hot(Arc<Value>),
    /// Value has been demoted to RocksDB warm tier.
    Warm,
}

/// Entry in the store with value location and metadata.
///
/// Keys and metadata are ALWAYS in RAM. Only the value may be on disk.
///
/// Hot values are wrapped in `Arc` for copy-on-write semantics:
/// - Reads: cheap ref-count bump via `Arc::clone()`
/// - Writes with no readers: zero-copy (refcount == 1)
/// - Writes with outstanding readers: clone-on-write via `Arc::make_mut()`
#[derive(Debug)]
struct Entry {
    location: ValueLocation,
    metadata: KeyMetadata,
    /// Cached key type so TYPE/SCAN work without promotion.
    key_type: KeyType,
}

impl Entry {
    /// Returns the in-memory value, or None if demoted to warm tier.
    fn hot_value(&self) -> Option<&Arc<Value>> {
        match &self.location {
            ValueLocation::Hot(v) => Some(v),
            ValueLocation::Warm => None,
        }
    }

    /// Returns true if the value is currently in RAM.
    fn is_hot(&self) -> bool {
        matches!(self.location, ValueLocation::Hot(_))
    }

    /// Memory size of this entry for accounting purposes.
    /// Warm entries contribute zero value bytes (value is on disk).
    fn memory_size(&self, key: &[u8]) -> usize {
        let value_size = match &self.location {
            ValueLocation::Hot(v) => v.memory_size(),
            ValueLocation::Warm => 0,
        };
        key.len() + value_size + std::mem::size_of::<KeyMetadata>() + std::mem::size_of::<Entry>()
    }
}

/// Error returned by `demote_key()`.
#[derive(Debug, thiserror::Error)]
pub enum DemotionError {
    #[error("key not found")]
    KeyNotFound,
    #[error("key is already warm")]
    AlreadyWarm,
    #[error("warm tier not configured")]
    NoWarmStore,
    #[error("RocksDB error: {0}")]
    Rocks(#[from] frogdb_persistence::rocks::RocksError),
}

/// Default store implementation using griddle::HashMap.
pub struct HashMapStore {
    data: HashMap<Bytes, Entry>,
    expiry_index: ExpiryIndex,
    field_expiry_index: FieldExpiryIndex,
    label_index: LabelIndex,
    memory_used: usize,
    /// Number of changes since last save (for INFO persistence rdb_changes_since_last_save).
    dirty: u64,
    /// RocksDB store for warm tier (None when tiered storage is disabled).
    warm_store: Option<Arc<RocksStore>>,
    /// Shard ID for warm CF lookups.
    warm_shard_id: usize,
    /// Number of keys currently in the warm tier.
    warm_keys: usize,
    /// Total number of hot→warm demotions.
    total_demotions: u64,
    /// Total number of warm→hot promotions.
    total_promotions: u64,
    /// Keys that were expired during promotion attempt.
    expired_on_promote: u64,
    /// Total number of keys expired (lazy + active expiry).
    expired_keys: u64,
}

impl std::fmt::Debug for HashMapStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashMapStore")
            .field("keys", &self.data.len())
            .field("memory_used", &self.memory_used)
            .field("warm_keys", &self.warm_keys)
            .field("warm_enabled", &self.warm_store.is_some())
            .finish()
    }
}

impl HashMapStore {
    /// Create a new empty store.
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            expiry_index: ExpiryIndex::new(),
            field_expiry_index: FieldExpiryIndex::new(),
            label_index: LabelIndex::new(),
            memory_used: 0,
            dirty: 0,
            warm_store: None,
            warm_shard_id: 0,
            warm_keys: 0,
            total_demotions: 0,
            total_promotions: 0,
            expired_on_promote: 0,
            expired_keys: 0,
        }
    }

    /// Create a store with a pre-allocated expiry index.
    ///
    /// Used during recovery when we have a pre-built expiry index.
    pub fn with_expiry_index(expiry_index: ExpiryIndex) -> Self {
        Self {
            data: HashMap::new(),
            expiry_index,
            field_expiry_index: FieldExpiryIndex::new(),
            label_index: LabelIndex::new(),
            memory_used: 0,
            dirty: 0,
            warm_store: None,
            warm_shard_id: 0,
            warm_keys: 0,
            total_demotions: 0,
            total_promotions: 0,
            expired_on_promote: 0,
            expired_keys: 0,
        }
    }

    /// Restore an entry during recovery.
    ///
    /// This bypasses the normal set path to directly insert a key with
    /// its full metadata. The expiry index should be updated separately
    /// or use `with_expiry_index` to provide a pre-built index.
    pub fn restore_entry(&mut self, key: Bytes, value: Value, metadata: KeyMetadata) {
        let size = Self::hot_entry_memory_size(&key, &value);
        self.memory_used += size;

        // Update expiry index if key has expiry
        if let Some(expires_at) = metadata.expires_at {
            self.expiry_index.set(key.clone(), expires_at);
        }

        // Update label index for TimeSeries values
        if let Value::TimeSeries(ref ts) = value {
            self.label_index.add(key.clone(), ts.labels());
        }

        // Update field expiry index for Hash values with field expiries
        if let Value::Hash(ref hash) = value
            && let Some(expiries) = hash.field_expiries()
        {
            for (field, &expires_at) in expiries {
                self.field_expiry_index
                    .set(key.clone(), field.clone(), expires_at);
            }
        }

        let key_type = value.key_type();
        let entry = Entry {
            location: ValueLocation::Hot(Arc::new(value)),
            metadata,
            key_type,
        };
        self.data.insert(key, entry);
    }

    /// Restore a warm entry during recovery.
    ///
    /// Inserts a key with metadata as a `Warm` entry — no value in RAM.
    /// Does NOT insert if the key already exists (hot copy wins).
    pub fn restore_warm_entry(&mut self, key: Bytes, metadata: KeyMetadata, key_type: KeyType) {
        // Hot copy wins — don't overwrite existing entries
        if self.data.contains_key(&key) {
            return;
        }

        // Warm entries: key + metadata in RAM, value on disk
        let size = key.len() + std::mem::size_of::<KeyMetadata>() + std::mem::size_of::<Entry>();
        self.memory_used += size;

        // Update expiry index if key has expiry
        if let Some(expires_at) = metadata.expires_at {
            self.expiry_index.set(key.clone(), expires_at);
        }

        let entry = Entry {
            location: ValueLocation::Warm,
            metadata,
            key_type,
        };
        self.data.insert(key, entry);
        self.warm_keys += 1;
    }

    /// Calculate memory size for a new hot entry.
    fn hot_entry_memory_size(key: &[u8], value: &Value) -> usize {
        key.len()
            + value.memory_size()
            + std::mem::size_of::<KeyMetadata>()
            + std::mem::size_of::<Entry>()
    }

    /// Check if a key is expired and delete it if so.
    ///
    /// Returns true if the key was expired and deleted.
    fn check_and_delete_expired(&mut self, key: &[u8]) -> bool {
        if let Some(entry) = self.data.get(key)
            && entry.metadata.is_expired()
        {
            debug!(key_len = key.len(), "Key expired via lazy deletion");
            let was_warm = !entry.is_hot();
            // Remove from both data and expiry index
            if let Some(entry) = self.data.remove(key) {
                let size = entry.memory_size(key);
                self.memory_used = self.memory_used.saturating_sub(size);
            }
            // Clean up warm CF if needed
            if was_warm {
                if let Some(warm_store) = &self.warm_store {
                    let _ = warm_store.delete_warm(self.warm_shard_id, key);
                }
                self.warm_keys = self.warm_keys.saturating_sub(1);
            }
            self.expiry_index.remove(key);
            self.label_index.remove(key);
            self.expired_keys += 1;
            return true;
        }
        false
    }

    /// Total number of expired keys (lazy + active).
    pub fn expired_keys(&self) -> u64 {
        self.expired_keys
    }

    /// Add to the expired keys counter (for active expiry which bypasses `check_and_delete_expired`).
    pub fn add_expired_keys(&mut self, count: u64) {
        self.expired_keys += count;
    }

    /// Reset the expired keys counter (for CONFIG RESETSTAT).
    pub fn reset_expired_keys(&mut self) {
        self.expired_keys = 0;
    }
}

impl Default for HashMapStore {
    fn default() -> Self {
        Self::new()
    }
}

impl HashMapStore {
    /// Get a value only if it's hot (in-memory). Does not promote warm values.
    ///
    /// Use this for WAL persistence and diagnostics where promotion is unnecessary.
    pub fn get_hot(&self, key: &[u8]) -> Option<Arc<Value>> {
        self.data.get(key).and_then(|e| e.hot_value().cloned())
    }

    /// Configure the warm (RocksDB) tier for this store.
    pub fn set_warm_store(&mut self, rocks: Arc<RocksStore>, shard_id: usize) {
        self.warm_store = Some(rocks);
        self.warm_shard_id = shard_id;
    }

    /// Demote a key's value from hot (RAM) to warm (RocksDB).
    ///
    /// Returns the number of value bytes freed from RAM.
    pub fn demote_key(&mut self, key: &[u8]) -> Result<usize, DemotionError> {
        let warm_store = self.warm_store.as_ref().ok_or(DemotionError::NoWarmStore)?;

        let entry = self.data.get(key).ok_or(DemotionError::KeyNotFound)?;
        let value = entry.hot_value().ok_or(DemotionError::AlreadyWarm)?;

        // Serialize using existing persistence format
        let serialized = serialize(value, &entry.metadata);
        let value_bytes = value.memory_size();

        // Write to warm CF
        warm_store.put_warm(self.warm_shard_id, key, &serialized)?;

        // Replace location with Warm
        let entry = self.data.get_mut(key).unwrap();
        entry.location = ValueLocation::Warm;

        // Update memory accounting — value bytes freed, metadata stays in RAM
        self.memory_used = self.memory_used.saturating_sub(value_bytes);
        self.warm_keys += 1;
        self.total_demotions += 1;

        Ok(value_bytes)
    }

    /// Promote a key's value from warm (RocksDB) back to hot (RAM).
    ///
    /// Returns None if the key doesn't exist, isn't warm, or is expired.
    fn promote_key(&mut self, key: &[u8]) -> Option<Arc<Value>> {
        // Check that entry exists and is warm
        let entry = self.data.get(key)?;
        if entry.is_hot() {
            return entry.hot_value().cloned();
        }

        // Check TTL before promoting — don't waste work on expired keys
        if entry.metadata.is_expired() {
            debug!(key_len = key.len(), "Warm key expired during promotion");
            self.expired_on_promote += 1;
            // Clean up: remove from RAM, warm CF, and expiry index
            if let Some(warm_store) = &self.warm_store {
                let _ = warm_store.delete_warm(self.warm_shard_id, key);
            }
            if let Some(entry) = self.data.remove(key) {
                let size = entry.memory_size(key);
                self.memory_used = self.memory_used.saturating_sub(size);
            }
            self.expiry_index.remove(key);
            self.label_index.remove(key);
            self.warm_keys = self.warm_keys.saturating_sub(1);
            return None;
        }

        // Read from warm CF
        let warm_store = self.warm_store.as_ref()?;
        let data = match warm_store.get_warm(self.warm_shard_id, key) {
            Ok(Some(data)) => data,
            Ok(None) => {
                warn!(key_len = key.len(), "Warm key missing from RocksDB");
                return None;
            }
            Err(e) => {
                warn!(key_len = key.len(), error = %e, "Failed to read warm key");
                return None;
            }
        };

        // Deserialize
        let (value, _metadata) = match deserialize(&data) {
            Ok(v) => v,
            Err(e) => {
                warn!(key_len = key.len(), error = %e, "Failed to deserialize warm key");
                return None;
            }
        };

        // Replace location with Hot
        let value_arc = Arc::new(value);
        let value_bytes = value_arc.memory_size();

        let entry = self.data.get_mut(key).unwrap();
        entry.location = ValueLocation::Hot(value_arc.clone());

        // Update memory accounting
        self.memory_used += value_bytes;
        self.warm_keys = self.warm_keys.saturating_sub(1);
        self.total_promotions += 1;

        // Delete from warm CF
        let _ = warm_store.delete_warm(self.warm_shard_id, key);

        Some(value_arc)
    }

    /// Number of keys currently in the warm tier.
    pub fn warm_key_count(&self) -> usize {
        self.warm_keys
    }

    /// Number of hot (in-memory) keys.
    pub fn hot_key_count(&self) -> usize {
        self.data.len().saturating_sub(self.warm_keys)
    }

    /// Total number of warm→hot promotions.
    pub fn promotion_count(&self) -> u64 {
        self.total_promotions
    }

    /// Total number of hot→warm demotions.
    pub fn demotion_count(&self) -> u64 {
        self.total_demotions
    }

    /// Keys that were found expired during promotion.
    pub fn expired_on_promote_count(&self) -> u64 {
        self.expired_on_promote
    }
}

impl Store for HashMapStore {
    fn get(&mut self, key: &[u8]) -> Option<Arc<Value>> {
        // Fast path: hot value
        if let Some(entry) = self.data.get(key)
            && let Some(v) = entry.hot_value()
        {
            return Some(v.clone());
        }
        // Slow path: promote from warm tier
        self.promote_key(key)
    }

    fn set(&mut self, key: Bytes, value: Value) -> Option<Value> {
        let new_size = Self::hot_entry_memory_size(&key, &value);

        let old_value = if let Some(old_entry) = self.data.get(&key) {
            let old_size = old_entry.memory_size(&key);
            self.memory_used = self.memory_used.saturating_sub(old_size);
            // Clean up warm CF if overwriting a warm key
            if !old_entry.is_hot() {
                if let Some(warm_store) = &self.warm_store {
                    let _ = warm_store.delete_warm(self.warm_shard_id, &key);
                }
                self.warm_keys = self.warm_keys.saturating_sub(1);
            }
            old_entry
                .hot_value()
                .map(|v| Arc::try_unwrap(v.clone()).unwrap_or_else(|arc| (*arc).clone()))
        } else {
            None
        };

        // Update label index for TimeSeries values
        if let Value::TimeSeries(ref ts) = value {
            self.label_index.add(key.clone(), ts.labels());
        } else {
            // If overwriting a TS key with non-TS, remove from label index
            self.label_index.remove(&key);
        }

        self.memory_used += new_size;

        let key_type = value.key_type();
        let entry = Entry {
            location: ValueLocation::Hot(Arc::new(value)),
            metadata: KeyMetadata::new(new_size),
            key_type,
        };
        self.data.insert(key, entry);

        old_value
    }

    fn delete(&mut self, key: &[u8]) -> bool {
        if let Some(entry) = self.data.remove(key) {
            let size = entry.memory_size(key);
            if size > self.memory_used {
                warn!(
                    key_len = key.len(),
                    reported_size = size,
                    "Memory accounting underflow during delete"
                );
            }
            self.memory_used = self.memory_used.saturating_sub(size);
            // Clean up warm CF if this was a warm key
            if !entry.is_hot() {
                if let Some(warm_store) = &self.warm_store {
                    let _ = warm_store.delete_warm(self.warm_shard_id, key);
                }
                self.warm_keys = self.warm_keys.saturating_sub(1);
            }
            self.expiry_index.remove(key);
            self.label_index.remove(key);
            self.field_expiry_index.remove_key(key);
            true
        } else {
            false
        }
    }

    fn contains(&self, key: &[u8]) -> bool {
        self.data.contains_key(key)
    }

    fn key_type(&self, key: &[u8]) -> KeyType {
        self.data
            .get(key)
            .map(|e| e.key_type)
            .unwrap_or(KeyType::None)
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn memory_used(&self) -> usize {
        self.memory_used
    }

    fn scan(&self, cursor: u64, count: usize, pattern: Option<&[u8]>) -> (u64, Vec<Bytes>) {
        self.scan_filtered(cursor, count, pattern, None)
    }

    fn scan_filtered(
        &self,
        cursor: u64,
        count: usize,
        pattern: Option<&[u8]>,
        key_type: Option<KeyType>,
    ) -> (u64, Vec<Bytes>) {
        let start = cursor as usize;
        let total = self.data.len();

        if start >= total {
            return (0, vec![]);
        }

        let mut results = Vec::with_capacity(count);
        let mut current = 0;
        let mut next_pos = start;

        for (key, entry) in self.data.iter() {
            if current < start {
                current += 1;
                continue;
            }

            next_pos = current + 1;

            let pattern_matches = match pattern {
                Some(p) => glob_match(p, key),
                None => true,
            };

            let type_matches = match key_type {
                Some(filter_type) => entry.key_type == filter_type,
                None => true,
            };

            if pattern_matches && type_matches {
                results.push(key.clone());
                if results.len() >= count {
                    break;
                }
            }
            current += 1;
        }

        let next_cursor = if next_pos >= total {
            0
        } else {
            next_pos as u64
        };

        (next_cursor, results)
    }

    fn clear(&mut self) {
        // Clean up all warm entries from RocksDB
        if self.warm_keys > 0
            && let Some(warm_store) = &self.warm_store
        {
            for (key, entry) in &self.data {
                if !entry.is_hot() {
                    let _ = warm_store.delete_warm(self.warm_shard_id, key.as_ref());
                }
            }
        }
        self.data.clear();
        self.expiry_index = ExpiryIndex::new();
        self.label_index = LabelIndex::new();
        self.field_expiry_index = FieldExpiryIndex::new();
        self.memory_used = 0;
        self.warm_keys = 0;
    }

    fn all_keys(&self) -> Vec<Bytes> {
        self.data.keys().cloned().collect()
    }

    // ========================================================================
    // Expiry-aware methods
    // ========================================================================

    fn get_with_expiry_check(&mut self, key: &[u8]) -> Option<Arc<Value>> {
        // Check for expiry and delete if expired
        if self.check_and_delete_expired(key) {
            return None;
        }

        // Fast path: hot value
        if let Some(entry) = self.data.get_mut(key) {
            entry.metadata.touch();
            if let Some(v) = entry.hot_value() {
                return Some(v.clone());
            }
        }

        // Slow path: promote from warm tier
        let value = self.promote_key(key)?;
        if let Some(entry) = self.data.get_mut(key) {
            entry.metadata.touch();
        }
        Some(value)
    }

    fn set_with_options(&mut self, key: Bytes, value: Value, opts: SetOptions) -> SetResult {
        // Check condition (NX/XX)
        let key_exists = self.data.contains_key(&key) && !self.check_and_delete_expired(&key);

        match opts.condition {
            SetCondition::NX if key_exists => return SetResult::NotSet,
            SetCondition::XX if !key_exists => return SetResult::NotSet,
            _ => {}
        }

        // Get old value if needed (convert Arc<Value> to owned Value)
        let old_value: Option<Value> = if opts.return_old {
            self.data
                .get(&key)
                .and_then(|e| e.hot_value().cloned())
                .map(|arc| Arc::try_unwrap(arc).unwrap_or_else(|arc| (*arc).clone()))
        } else {
            None
        };

        // Determine the expiry for the new entry
        let new_expiry = if opts.keep_ttl && key_exists {
            // Preserve existing TTL
            self.get_expiry(&key)
        } else if let Some(expiry) = opts.expiry {
            expiry.to_instant()
        } else {
            None
        };

        // Perform the set
        let new_size = Self::hot_entry_memory_size(&key, &value);

        // Update memory accounting
        if let Some(old_entry) = self.data.get(&key) {
            let old_size = old_entry.memory_size(&key);
            self.memory_used = self.memory_used.saturating_sub(old_size);
            // Clean up warm CF if overwriting a warm key
            if !old_entry.is_hot() {
                if let Some(warm_store) = &self.warm_store {
                    let _ = warm_store.delete_warm(self.warm_shard_id, &key);
                }
                self.warm_keys = self.warm_keys.saturating_sub(1);
            }
        }

        self.memory_used += new_size;

        let mut metadata = KeyMetadata::new(new_size);
        metadata.expires_at = new_expiry;

        let key_type = value.key_type();
        let entry = Entry {
            location: ValueLocation::Hot(Arc::new(value)),
            metadata,
            key_type,
        };
        self.data.insert(key.clone(), entry);

        // Update expiry index
        if let Some(expires_at) = new_expiry {
            self.expiry_index.set(key.clone(), expires_at);
        } else {
            self.expiry_index.remove(&key);
        }

        if opts.return_old {
            SetResult::OkWithOldValue(old_value)
        } else {
            SetResult::Ok
        }
    }

    fn set_expiry(&mut self, key: &[u8], expires_at: Instant) -> bool {
        // Check and delete if expired first
        if self.check_and_delete_expired(key) {
            return false;
        }

        if let Some(entry) = self.data.get_mut(key) {
            entry.metadata.expires_at = Some(expires_at);
            self.expiry_index
                .set(Bytes::copy_from_slice(key), expires_at);
            true
        } else {
            false
        }
    }

    fn get_expiry(&self, key: &[u8]) -> Option<Instant> {
        self.data.get(key).and_then(|e| e.metadata.expires_at)
    }

    fn persist(&mut self, key: &[u8]) -> bool {
        if let Some(entry) = self.data.get_mut(key)
            && entry.metadata.expires_at.is_some()
        {
            entry.metadata.expires_at = None;
            self.expiry_index.remove(key);
            return true;
        }
        false
    }

    fn touch(&mut self, key: &[u8]) -> bool {
        // Check and delete if expired first
        if self.check_and_delete_expired(key) {
            return false;
        }

        if let Some(entry) = self.data.get_mut(key) {
            entry.metadata.touch();
            true
        } else {
            false
        }
    }

    fn get_and_delete(&mut self, key: &[u8]) -> Option<Value> {
        // Check expiry first
        if self.check_and_delete_expired(key) {
            return None;
        }

        if let Some(entry) = self.data.remove(key) {
            let size = entry.memory_size(key);
            self.memory_used = self.memory_used.saturating_sub(size);
            self.expiry_index.remove(key);
            self.label_index.remove(key);
            match entry.location {
                ValueLocation::Hot(arc) => {
                    Some(Arc::try_unwrap(arc).unwrap_or_else(|arc| (*arc).clone()))
                }
                ValueLocation::Warm => {
                    self.warm_keys = self.warm_keys.saturating_sub(1);
                    // Read from warm CF before deleting
                    if let Some(warm_store) = &self.warm_store {
                        let data = warm_store.get_warm(self.warm_shard_id, key).ok().flatten();
                        let _ = warm_store.delete_warm(self.warm_shard_id, key);
                        data.and_then(|d| deserialize(&d).ok())
                            .map(|(value, _metadata)| value)
                    } else {
                        None
                    }
                }
            }
        } else {
            None
        }
    }

    fn get_mut(&mut self, key: &[u8]) -> Option<&mut Value> {
        // Check expiry first
        if self.check_and_delete_expired(key) {
            return None;
        }

        // Promote warm values before returning mutable reference
        if let Some(entry) = self.data.get(key)
            && !entry.is_hot()
        {
            self.promote_key(key);
        }

        self.data.get_mut(key).and_then(|e| {
            e.metadata.touch();
            match &mut e.location {
                ValueLocation::Hot(arc) => Some(Arc::make_mut(arc)),
                ValueLocation::Warm => None,
            }
        })
    }

    #[allow(deprecated)]
    fn expiry_index(&self) -> Option<&ExpiryIndex> {
        Some(&self.expiry_index)
    }

    #[allow(deprecated)]
    fn expiry_index_mut(&mut self) -> Option<&mut ExpiryIndex> {
        Some(&mut self.expiry_index)
    }

    fn get_expired_keys(&self, now: std::time::Instant) -> Vec<Bytes> {
        self.expiry_index.get_expired(now)
    }

    fn keys_with_expiry_count(&self) -> usize {
        self.expiry_index.len()
    }

    fn set_field_expiry(&mut self, key: &[u8], field: &[u8], expires_at: Instant) {
        self.field_expiry_index.set(
            Bytes::copy_from_slice(key),
            Bytes::copy_from_slice(field),
            expires_at,
        );
    }

    fn remove_field_expiry(&mut self, key: &[u8], field: &[u8]) {
        self.field_expiry_index.remove(key, field);
    }

    fn remove_all_field_expiries(&mut self, key: &[u8]) {
        self.field_expiry_index.remove_key(key);
    }

    fn get_field_expiry(&self, key: &[u8], field: &[u8]) -> Option<Instant> {
        self.field_expiry_index.get(key, field)
    }

    fn get_expired_fields(&self, now: Instant) -> Vec<(Bytes, Bytes)> {
        self.field_expiry_index.get_expired(now)
    }

    fn purge_expired_hash_fields(&mut self, key: &[u8]) -> usize {
        // Get the hash value and remove expired fields
        let now = Instant::now();

        // Get mutable access to the hash and remove expired fields
        let removed_fields = {
            let value = match self.data.get_mut(key) {
                Some(entry) => match &mut entry.location {
                    ValueLocation::Hot(arc) => Arc::make_mut(arc),
                    ValueLocation::Warm => return 0,
                },
                None => return 0,
            };
            let hash = match value.as_hash_mut() {
                Some(h) => h,
                None => return 0,
            };
            hash.remove_expired_fields(now)
        };

        let count = removed_fields.len();

        // Update field expiry index
        for field in &removed_fields {
            self.field_expiry_index.remove(key, field);
        }

        // If hash is now empty, delete the key
        if count > 0
            && let Some(entry) = self.data.get(key)
            && let Some(v) = entry.hot_value()
            && let Some(hash) = v.as_hash()
            && hash.is_empty()
        {
            self.delete(key);
        }

        count
    }

    // ========================================================================
    // Eviction support methods
    // ========================================================================

    fn random_key(&self) -> Option<Bytes> {
        if self.data.is_empty() {
            return None;
        }

        let mut rng = rand::rng();
        // Skip warm entries — they're already demoted
        self.data
            .iter()
            .filter(|(_, e)| e.is_hot())
            .map(|(k, _)| k)
            .choose(&mut rng)
            .cloned()
    }

    fn sample_keys(&self, count: usize) -> Vec<Bytes> {
        if self.data.is_empty() || count == 0 {
            return vec![];
        }

        let mut rng = rand::rng();
        // Skip warm entries — they're already demoted
        self.data
            .iter()
            .filter(|(_, e)| e.is_hot())
            .map(|(k, _)| k)
            .choose_multiple(&mut rng, count)
            .into_iter()
            .cloned()
            .collect()
    }

    fn sample_volatile_keys(&self, count: usize) -> Vec<Bytes> {
        // Sample from the expiry index which only contains volatile keys
        self.expiry_index.sample(count)
    }

    fn get_metadata(&self, key: &[u8]) -> Option<KeyMetadata> {
        self.data.get(key).map(|e| e.metadata.clone())
    }

    fn idle_time(&self, key: &[u8]) -> Option<Duration> {
        self.data
            .get(key)
            .map(|e| Instant::now().duration_since(e.metadata.last_access))
    }

    fn update_lfu_counter(&mut self, key: &[u8], log_factor: u8) {
        if let Some(entry) = self.data.get_mut(key) {
            entry.metadata.lfu_counter =
                crate::eviction::lfu_log_incr(entry.metadata.lfu_counter, log_factor);
        }
    }

    fn get_lfu_value(&self, key: &[u8], decay_time: u64) -> Option<u8> {
        self.data.get(key).map(|e| {
            let minutes_since = Instant::now()
                .duration_since(e.metadata.last_access)
                .as_secs()
                / 60;
            crate::eviction::lfu_decay(e.metadata.lfu_counter, minutes_since, decay_time)
        })
    }

    // ========================================================================
    // Cluster slot support methods
    // ========================================================================

    fn keys_in_slot(&self, slot: u16, count: usize) -> Vec<Bytes> {
        self.data
            .keys()
            .filter(|k| slot_for_key(k) == slot)
            .take(count)
            .cloned()
            .collect()
    }

    fn count_keys_in_slot(&self, slot: u16) -> usize {
        self.data.keys().filter(|k| slot_for_key(k) == slot).count()
    }

    // ========================================================================
    // Dirty tracking
    // ========================================================================

    fn dirty(&self) -> u64 {
        self.dirty
    }

    fn increment_dirty(&mut self, count: u64) {
        self.dirty = self.dirty.wrapping_add(count);
    }

    fn ts_label_index(&self) -> Option<&LabelIndex> {
        Some(&self.label_index)
    }

    fn ts_label_index_mut(&mut self) -> Option<&mut LabelIndex> {
        Some(&mut self.label_index)
    }

    fn warm_key_count(&self) -> usize {
        self.warm_keys
    }

    fn hot_key_count(&self) -> usize {
        self.data.len().saturating_sub(self.warm_keys)
    }

    fn demotion_count(&self) -> u64 {
        self.total_demotions
    }

    fn promotion_count(&self) -> u64 {
        self.total_promotions
    }

    fn expired_on_promote_count(&self) -> u64 {
        self.expired_on_promote
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::glob::glob_match;

    #[test]
    fn test_store_set_get() {
        let mut store = HashMapStore::new();

        store.set(Bytes::from("key1"), Value::string("value1"));
        let value = store.get(b"key1").unwrap();

        let sv = value.as_string().expect("expected string value");
        assert_eq!(sv.as_bytes().as_ref(), b"value1");
    }

    #[test]
    fn test_store_delete() {
        let mut store = HashMapStore::new();

        store.set(Bytes::from("key1"), Value::string("value1"));
        assert!(store.contains(b"key1"));

        assert!(store.delete(b"key1"));
        assert!(!store.contains(b"key1"));
        assert!(!store.delete(b"key1"));
    }

    #[test]
    fn test_store_key_type() {
        let mut store = HashMapStore::new();

        assert_eq!(store.key_type(b"missing"), KeyType::None);

        store.set(Bytes::from("string_key"), Value::string("value"));
        assert_eq!(store.key_type(b"string_key"), KeyType::String);
    }

    #[test]
    fn test_store_memory_tracking() {
        let mut store = HashMapStore::new();

        assert_eq!(store.memory_used(), 0);

        store.set(Bytes::from("key"), Value::string("value"));
        assert!(store.memory_used() > 0);

        store.delete(b"key");
        assert_eq!(store.memory_used(), 0);
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"prefix*", b"prefix123"));
        assert!(!glob_match(b"prefix*", b"notprefix"));
        assert!(glob_match(b"*suffix", b"anysuffix"));
        assert!(!glob_match(b"*suffix", b"suffixnot"));
        assert!(glob_match(b"exact", b"exact"));
        assert!(!glob_match(b"exact", b"notexact"));
    }
}
