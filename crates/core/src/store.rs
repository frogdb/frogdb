//! Storage trait and implementations.

use bytes::Bytes;
use griddle::HashMap;
use rand::seq::IteratorRandom;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

use crate::glob::glob_match;
use crate::noop::ExpiryIndex;
use crate::shard::slot_for_key;
use crate::types::{KeyMetadata, KeyType, SetCondition, SetOptions, SetResult, Value};

/// Storage trait for key-value operations.
pub trait Store: Send {
    /// Get a value by key.
    fn get(&self, key: &[u8]) -> Option<Value>;

    /// Set a value, returns previous value if any.
    fn set(&mut self, key: Bytes, value: Value) -> Option<Value>;

    /// Delete a key, returns true if existed.
    fn delete(&mut self, key: &[u8]) -> bool;

    /// Check if key exists.
    fn contains(&self, key: &[u8]) -> bool;

    /// Get key type.
    fn key_type(&self, key: &[u8]) -> KeyType;

    /// Number of keys.
    fn len(&self) -> usize;

    /// Check if store is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Memory used by store (bytes).
    fn memory_used(&self) -> usize;

    /// Iterate keys (for SCAN).
    fn scan(&self, cursor: u64, count: usize, pattern: Option<&[u8]>) -> (u64, Vec<Bytes>);

    /// Iterate keys with type filter (for SCAN with TYPE option).
    fn scan_filtered(
        &self,
        cursor: u64,
        count: usize,
        pattern: Option<&[u8]>,
        key_type: Option<KeyType>,
    ) -> (u64, Vec<Bytes>) {
        // Default implementation ignores type filter
        let _ = key_type;
        self.scan(cursor, count, pattern)
    }

    /// Clear all keys from the store.
    fn clear(&mut self);

    /// Get all keys in the store.
    fn all_keys(&self) -> Vec<Bytes>;

    // ========================================================================
    // Expiry-aware methods (added in Phase 2)
    // ========================================================================

    /// Get a value, checking for expiry first (lazy expiry).
    ///
    /// If the key is expired, it will be deleted and None returned.
    fn get_with_expiry_check(&mut self, key: &[u8]) -> Option<Value> {
        // Default implementation just calls get()
        self.get(key)
    }

    /// Set a value with options (NX/XX, EX/PX, GET, KEEPTTL).
    fn set_with_options(&mut self, key: Bytes, value: Value, _opts: SetOptions) -> SetResult {
        // Default implementation ignores options
        self.set(key, value);
        SetResult::Ok
    }

    /// Set expiry for a key.
    ///
    /// Returns true if the key exists and expiry was set.
    fn set_expiry(&mut self, key: &[u8], expires_at: Instant) -> bool {
        // Default: no-op, return false if not implemented
        let _ = (key, expires_at);
        false
    }

    /// Get the expiry time for a key.
    fn get_expiry(&self, key: &[u8]) -> Option<Instant> {
        // Default: no expiry tracking
        let _ = key;
        None
    }

    /// Remove expiry from a key (PERSIST command).
    ///
    /// Returns true if the key exists and had an expiry that was removed.
    fn persist(&mut self, key: &[u8]) -> bool {
        // Default: no-op
        let _ = key;
        false
    }

    /// Update last access time for a key (TOUCH command).
    ///
    /// Returns true if the key exists.
    fn touch(&mut self, key: &[u8]) -> bool {
        // Default: check existence
        self.contains(key)
    }

    /// Get and delete a key atomically (GETDEL command).
    fn get_and_delete(&mut self, key: &[u8]) -> Option<Value> {
        let value = self.get(key);
        if value.is_some() {
            self.delete(key);
        }
        value
    }

    /// Get a mutable reference to a value (for in-place modifications).
    ///
    /// Returns None if key doesn't exist or is expired.
    fn get_mut(&mut self, key: &[u8]) -> Option<&mut Value>;

    /// Access the expiry index directly (for active expiry).
    ///
    /// **Deprecated**: Use `get_expired_keys()` and `keys_with_expiry_count()` instead.
    /// This method leaks internal implementation details.
    #[deprecated(since = "0.2.0", note = "use get_expired_keys() or keys_with_expiry_count() instead")]
    fn expiry_index(&self) -> Option<&ExpiryIndex> {
        None
    }

    /// Access the expiry index mutably (for active expiry cleanup).
    ///
    /// **Deprecated**: Use `get_expired_keys()` instead.
    /// This method leaks internal implementation details.
    #[deprecated(since = "0.2.0", note = "use get_expired_keys() instead")]
    fn expiry_index_mut(&mut self) -> Option<&mut ExpiryIndex> {
        None
    }

    // ========================================================================
    // Cleaner expiry abstraction (Phase 5.2)
    // ========================================================================

    /// Get all keys that have expired at or before `now`.
    ///
    /// Returns keys in expiration order (oldest first). This method provides
    /// a cleaner interface than direct expiry_index access for active expiry.
    fn get_expired_keys(&self, now: std::time::Instant) -> Vec<Bytes> {
        let _ = now;
        vec![]
    }

    /// Get the count of keys that have an expiry (TTL) set.
    ///
    /// Used for metrics reporting.
    fn keys_with_expiry_count(&self) -> usize {
        0
    }

    // ========================================================================
    // Eviction support methods (added for Phase 10.6)
    // ========================================================================

    /// Get a random key from the store.
    ///
    /// Returns None if the store is empty.
    fn random_key(&self) -> Option<Bytes> {
        None
    }

    /// Sample up to N random keys from the store.
    ///
    /// May return fewer keys if the store has fewer than N keys.
    fn sample_keys(&self, count: usize) -> Vec<Bytes> {
        let _ = count;
        vec![]
    }

    /// Sample up to N random keys that have TTL set (volatile keys).
    ///
    /// May return fewer keys if fewer volatile keys exist.
    fn sample_volatile_keys(&self, count: usize) -> Vec<Bytes> {
        let _ = count;
        vec![]
    }

    /// Get metadata for a key (for eviction decision making).
    ///
    /// Returns None if key doesn't exist.
    fn get_metadata(&self, key: &[u8]) -> Option<KeyMetadata> {
        let _ = key;
        None
    }

    /// Get the idle time (time since last access) for a key.
    ///
    /// Returns None if key doesn't exist.
    fn idle_time(&self, key: &[u8]) -> Option<Duration> {
        let _ = key;
        None
    }

    /// Update LFU counter for a key (on access).
    ///
    /// Called when a key is accessed to update its frequency counter.
    fn update_lfu_counter(&mut self, key: &[u8], log_factor: u8) {
        let _ = (key, log_factor);
    }

    /// Decay LFU counter for a key based on idle time.
    ///
    /// Called during eviction to apply time-based decay.
    fn get_lfu_value(&self, key: &[u8], decay_time: u64) -> Option<u8> {
        let _ = (key, decay_time);
        None
    }

    // ========================================================================
    // Cluster slot support methods (for slot-based routing)
    // ========================================================================

    /// Get keys in a specific slot (for CLUSTER GETKEYSINSLOT).
    ///
    /// Returns up to `count` keys that hash to the given slot.
    fn keys_in_slot(&self, slot: u16, count: usize) -> Vec<Bytes> {
        let _ = (slot, count);
        vec![]
    }

    /// Count keys in a specific slot (for CLUSTER COUNTKEYSINSLOT).
    fn count_keys_in_slot(&self, slot: u16) -> usize {
        let _ = slot;
        0
    }
}

/// Entry in the store with value and metadata.
#[derive(Debug)]
struct Entry {
    value: Value,
    metadata: KeyMetadata,
}

/// Default store implementation using griddle::HashMap.
#[derive(Debug)]
pub struct HashMapStore {
    data: HashMap<Bytes, Entry>,
    expiry_index: ExpiryIndex,
    memory_used: usize,
}

impl HashMapStore {
    /// Create a new empty store.
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            expiry_index: ExpiryIndex::new(),
            memory_used: 0,
        }
    }

    /// Create a store with a pre-allocated expiry index.
    ///
    /// Used during recovery when we have a pre-built expiry index.
    pub fn with_expiry_index(expiry_index: ExpiryIndex) -> Self {
        Self {
            data: HashMap::new(),
            expiry_index,
            memory_used: 0,
        }
    }

    /// Restore an entry during recovery.
    ///
    /// This bypasses the normal set path to directly insert a key with
    /// its full metadata. The expiry index should be updated separately
    /// or use `with_expiry_index` to provide a pre-built index.
    pub fn restore_entry(&mut self, key: Bytes, value: Value, metadata: KeyMetadata) {
        let size = Self::entry_memory_size(&key, &value);
        self.memory_used += size;

        // Update expiry index if key has expiry
        if let Some(expires_at) = metadata.expires_at {
            self.expiry_index.set(key.clone(), expires_at);
        }

        let entry = Entry { value, metadata };
        self.data.insert(key, entry);
    }

    /// Calculate memory size for an entry.
    fn entry_memory_size(key: &[u8], value: &Value) -> usize {
        key.len()
            + value.memory_size()
            + std::mem::size_of::<KeyMetadata>()
            + std::mem::size_of::<Entry>()
    }

    /// Check if a key is expired and delete it if so.
    ///
    /// Returns true if the key was expired and deleted.
    fn check_and_delete_expired(&mut self, key: &[u8]) -> bool {
        if let Some(entry) = self.data.get(key) {
            if entry.metadata.is_expired() {
                debug!(key_len = key.len(), "Key expired via lazy deletion");
                // Remove from both data and expiry index
                if let Some(entry) = self.data.remove(key) {
                    let size = Self::entry_memory_size(key, &entry.value);
                    self.memory_used = self.memory_used.saturating_sub(size);
                }
                self.expiry_index.remove(key);
                return true;
            }
        }
        false
    }
}

impl Default for HashMapStore {
    fn default() -> Self {
        Self::new()
    }
}

impl Store for HashMapStore {
    fn get(&self, key: &[u8]) -> Option<Value> {
        self.data.get(key).map(|e| e.value.clone())
    }

    fn set(&mut self, key: Bytes, value: Value) -> Option<Value> {
        let new_size = Self::entry_memory_size(&key, &value);

        let old_value = if let Some(old_entry) = self.data.get(&key) {
            let old_size = Self::entry_memory_size(&key, &old_entry.value);
            self.memory_used = self.memory_used.saturating_sub(old_size);
            Some(old_entry.value.clone())
        } else {
            None
        };

        self.memory_used += new_size;

        let entry = Entry {
            value,
            metadata: KeyMetadata::new(new_size),
        };
        self.data.insert(key, entry);

        old_value
    }

    fn delete(&mut self, key: &[u8]) -> bool {
        if let Some(entry) = self.data.remove(key) {
            let size = Self::entry_memory_size(key, &entry.value);
            if size > self.memory_used {
                warn!(
                    key_len = key.len(),
                    reported_size = size,
                    "Memory accounting underflow during delete"
                );
            }
            self.memory_used = self.memory_used.saturating_sub(size);
            self.expiry_index.remove(key);
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
            .map(|e| e.value.key_type())
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
        // Simple implementation: cursor is just an index
        let keys: Vec<_> = self.data.keys().cloned().collect();
        let start = cursor as usize;

        if start >= keys.len() {
            return (0, vec![]);
        }

        let mut results = Vec::with_capacity(count);
        let mut current = start;

        while results.len() < count && current < keys.len() {
            let key = &keys[current];

            // Pattern matching (full glob)
            let pattern_matches = match pattern {
                Some(p) => glob_match(p, key),
                None => true,
            };

            // Type filtering
            let type_matches = match key_type {
                Some(filter_type) => {
                    if let Some(entry) = self.data.get(key) {
                        entry.value.key_type() == filter_type
                    } else {
                        false
                    }
                }
                None => true,
            };

            if pattern_matches && type_matches {
                results.push(key.clone());
            }
            current += 1;
        }

        let next_cursor = if current >= keys.len() { 0 } else { current as u64 };

        (next_cursor, results)
    }

    fn clear(&mut self) {
        self.data.clear();
        self.expiry_index = ExpiryIndex::new();
        self.memory_used = 0;
    }

    fn all_keys(&self) -> Vec<Bytes> {
        self.data.keys().cloned().collect()
    }

    // ========================================================================
    // Expiry-aware methods
    // ========================================================================

    fn get_with_expiry_check(&mut self, key: &[u8]) -> Option<Value> {
        // Check for expiry and delete if expired
        if self.check_and_delete_expired(key) {
            return None;
        }

        // Update last access time and return value
        if let Some(entry) = self.data.get_mut(key) {
            entry.metadata.touch();
            Some(entry.value.clone())
        } else {
            None
        }
    }

    fn set_with_options(&mut self, key: Bytes, value: Value, opts: SetOptions) -> SetResult {
        // Check condition (NX/XX)
        let key_exists = self.data.contains_key(&key) && !self.check_and_delete_expired(&key);

        match opts.condition {
            SetCondition::NX if key_exists => return SetResult::NotSet,
            SetCondition::XX if !key_exists => return SetResult::NotSet,
            _ => {}
        }

        // Get old value if needed
        let old_value = if opts.return_old {
            self.get(&key)
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
        let new_size = Self::entry_memory_size(&key, &value);

        // Update memory accounting
        if let Some(old_entry) = self.data.get(&key) {
            let old_size = Self::entry_memory_size(&key, &old_entry.value);
            self.memory_used = self.memory_used.saturating_sub(old_size);
        }

        self.memory_used += new_size;

        let mut metadata = KeyMetadata::new(new_size);
        metadata.expires_at = new_expiry;

        let entry = Entry { value, metadata };
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
            self.expiry_index.set(Bytes::copy_from_slice(key), expires_at);
            true
        } else {
            false
        }
    }

    fn get_expiry(&self, key: &[u8]) -> Option<Instant> {
        self.data.get(key).and_then(|e| e.metadata.expires_at)
    }

    fn persist(&mut self, key: &[u8]) -> bool {
        if let Some(entry) = self.data.get_mut(key) {
            if entry.metadata.expires_at.is_some() {
                entry.metadata.expires_at = None;
                self.expiry_index.remove(key);
                return true;
            }
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
            let size = Self::entry_memory_size(key, &entry.value);
            self.memory_used = self.memory_used.saturating_sub(size);
            self.expiry_index.remove(key);
            Some(entry.value)
        } else {
            None
        }
    }

    fn get_mut(&mut self, key: &[u8]) -> Option<&mut Value> {
        // Check expiry first
        if self.check_and_delete_expired(key) {
            return None;
        }

        self.data.get_mut(key).map(|e| {
            e.metadata.touch();
            &mut e.value
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

    // ========================================================================
    // Eviction support methods
    // ========================================================================

    fn random_key(&self) -> Option<Bytes> {
        if self.data.is_empty() {
            return None;
        }

        let mut rng = rand::thread_rng();
        self.data.keys().choose(&mut rng).cloned()
    }

    fn sample_keys(&self, count: usize) -> Vec<Bytes> {
        if self.data.is_empty() || count == 0 {
            return vec![];
        }

        let mut rng = rand::thread_rng();
        self.data.keys().choose_multiple(&mut rng, count).into_iter().cloned().collect()
    }

    fn sample_volatile_keys(&self, count: usize) -> Vec<Bytes> {
        // Sample from the expiry index which only contains volatile keys
        self.expiry_index.sample(count)
    }

    fn get_metadata(&self, key: &[u8]) -> Option<KeyMetadata> {
        self.data.get(key).map(|e| e.metadata.clone())
    }

    fn idle_time(&self, key: &[u8]) -> Option<Duration> {
        self.data.get(key).map(|e| {
            Instant::now().duration_since(e.metadata.last_access)
        })
    }

    fn update_lfu_counter(&mut self, key: &[u8], log_factor: u8) {
        if let Some(entry) = self.data.get_mut(key) {
            entry.metadata.lfu_counter = crate::eviction::lfu_log_incr(
                entry.metadata.lfu_counter,
                log_factor,
            );
        }
    }

    fn get_lfu_value(&self, key: &[u8], decay_time: u64) -> Option<u8> {
        self.data.get(key).map(|e| {
            let minutes_since = Instant::now()
                .duration_since(e.metadata.last_access)
                .as_secs() / 60;
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
        self.data
            .keys()
            .filter(|k| slot_for_key(k) == slot)
            .count()
    }
}


#[cfg(test)]
mod tests {
    use super::*;

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
