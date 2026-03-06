//! Storage traits and implementations.
//!
//! This module provides the core storage abstraction for FrogDB. The main
//! trait is [`Store`], which combines several focused sub-traits:
//!
//! - [`StorageOps`] - Core CRUD operations (get, set, delete, etc.)
//! - [`ExpiryOps`] - TTL and expiration management
//! - [`ScanOps`] - Cursor-based iteration for SCAN commands
//! - [`EvictionOps`] - Memory management and eviction support
//! - [`ClusterSlotOps`] - Redis Cluster slot operations
//!
//! # Example
//!
//! ```rust,ignore
//! use frogdb_core::{HashMapStore, Store};
//!
//! let mut store = HashMapStore::new();
//! store.set(Bytes::from("key"), Value::string("value"));
//! let value = store.get(b"key");
//! ```
//!
//! # Using Narrower Bounds
//!
//! For functions that only need specific capabilities, use the sub-traits:
//!
//! ```rust,ignore
//! use frogdb_core::store::{StorageOps, ExpiryOps};
//!
//! fn process_with_expiry(store: &mut impl ExpiryOps) {
//!     if let Some(value) = store.get_with_expiry_check(b"key") {
//!         // ...
//!     }
//! }
//! ```

mod hashmap;
mod traits;

// Re-export sub-traits for narrower bounds
pub use traits::{ClusterSlotOps, EvictionOps, ExpiryOps, ScanOps, StorageOps};

// Re-export HashMapStore implementation
pub use hashmap::HashMapStore;

use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::LabelIndex;
use crate::noop::ExpiryIndex;
use crate::types::{
    HashValue, KeyMetadata, KeyType, ListValue, SetOptions, SetResult, SetValue, SortedSetValue,
    StreamValue, Value,
};

// ============================================================================
// ValueType trait - Generic type access
// ============================================================================

/// Trait for type-safe access to Redis value types.
///
/// This trait enables generic helpers like `get_or_create<T>` to work with
/// any value type (List, Set, Hash, SortedSet, Stream, etc.).
///
/// # Example
///
/// ```rust,ignore
/// let list = ctx.get_or_create::<ListValue>(key)?;
/// list.push_back(value);
/// ```
pub trait ValueType: Sized {
    /// Type name for error messages.
    fn type_name() -> &'static str;

    /// Create a new default value of this type.
    fn create_default() -> Value;

    /// Try to get a reference from a Value.
    fn from_value(value: &Value) -> Option<&Self>;

    /// Try to get a mutable reference from a Value.
    fn from_value_mut(value: &mut Value) -> Option<&mut Self>;
}

// ============================================================================
// ValueType implementations
// ============================================================================

impl ValueType for ListValue {
    fn type_name() -> &'static str {
        "list"
    }

    fn create_default() -> Value {
        Value::list()
    }

    fn from_value(value: &Value) -> Option<&Self> {
        value.as_list()
    }

    fn from_value_mut(value: &mut Value) -> Option<&mut Self> {
        value.as_list_mut()
    }
}

impl ValueType for SetValue {
    fn type_name() -> &'static str {
        "set"
    }

    fn create_default() -> Value {
        Value::set()
    }

    fn from_value(value: &Value) -> Option<&Self> {
        value.as_set()
    }

    fn from_value_mut(value: &mut Value) -> Option<&mut Self> {
        value.as_set_mut()
    }
}

impl ValueType for HashValue {
    fn type_name() -> &'static str {
        "hash"
    }

    fn create_default() -> Value {
        Value::hash()
    }

    fn from_value(value: &Value) -> Option<&Self> {
        value.as_hash()
    }

    fn from_value_mut(value: &mut Value) -> Option<&mut Self> {
        value.as_hash_mut()
    }
}

impl ValueType for SortedSetValue {
    fn type_name() -> &'static str {
        "sorted set"
    }

    fn create_default() -> Value {
        Value::sorted_set()
    }

    fn from_value(value: &Value) -> Option<&Self> {
        value.as_sorted_set()
    }

    fn from_value_mut(value: &mut Value) -> Option<&mut Self> {
        value.as_sorted_set_mut()
    }
}

impl ValueType for StreamValue {
    fn type_name() -> &'static str {
        "stream"
    }

    fn create_default() -> Value {
        Value::stream()
    }

    fn from_value(value: &Value) -> Option<&Self> {
        value.as_stream()
    }

    fn from_value_mut(value: &mut Value) -> Option<&mut Self> {
        value.as_stream_mut()
    }
}

// ============================================================================
// Store trait - Combined interface
// ============================================================================

/// Storage trait for key-value operations.
///
/// This is the main storage abstraction combining all sub-traits. It provides
/// a unified interface for all storage operations.
///
/// For new code, consider using the narrower sub-traits where applicable:
/// - [`StorageOps`] for basic CRUD
/// - [`ExpiryOps`] for TTL management
/// - [`ScanOps`] for cursor-based iteration
/// - [`EvictionOps`] for memory management
/// - [`ClusterSlotOps`] for cluster operations
pub trait Store: Send {
    // ========================================================================
    // StorageOps - Core CRUD
    // ========================================================================

    /// Get a value by key.
    ///
    /// Returns an `Arc<Value>` for zero-copy reads. The Arc ref-count bump
    /// is much cheaper than deep-cloning large values like hashes or lists.
    fn get(&self, key: &[u8]) -> Option<Arc<Value>>;

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

    /// Clear all keys from the store.
    fn clear(&mut self);

    /// Get all keys in the store.
    fn all_keys(&self) -> Vec<Bytes>;

    /// Get a mutable reference to a value (for in-place modifications).
    ///
    /// Uses copy-on-write internally: if the value is shared (Arc refcount > 1),
    /// it will be cloned before returning the mutable reference.
    fn get_mut(&mut self, key: &[u8]) -> Option<&mut Value>;

    // ========================================================================
    // ScanOps - Cursor-based iteration
    // ========================================================================

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

    // ========================================================================
    // ExpiryOps - TTL management
    // ========================================================================

    /// Get a value, checking for expiry first (lazy expiry).
    fn get_with_expiry_check(&mut self, key: &[u8]) -> Option<Arc<Value>> {
        self.get(key)
    }

    /// Set a value with options (NX/XX, EX/PX, GET, KEEPTTL).
    fn set_with_options(&mut self, key: Bytes, value: Value, _opts: SetOptions) -> SetResult {
        self.set(key, value);
        SetResult::Ok
    }

    /// Set expiry for a key.
    fn set_expiry(&mut self, key: &[u8], expires_at: Instant) -> bool {
        let _ = (key, expires_at);
        false
    }

    /// Get the expiry time for a key.
    fn get_expiry(&self, key: &[u8]) -> Option<Instant> {
        let _ = key;
        None
    }

    /// Remove expiry from a key (PERSIST command).
    fn persist(&mut self, key: &[u8]) -> bool {
        let _ = key;
        false
    }

    /// Update last access time for a key (TOUCH command).
    fn touch(&mut self, key: &[u8]) -> bool {
        self.contains(key)
    }

    /// Get and delete a key atomically (GETDEL command).
    ///
    /// Returns an owned `Value` since it's removed from the store.
    fn get_and_delete(&mut self, key: &[u8]) -> Option<Value> {
        let value = self.get(key);
        if value.is_some() {
            self.delete(key);
        }
        value.map(|arc| Arc::try_unwrap(arc).unwrap_or_else(|arc| (*arc).clone()))
    }

    /// Get all keys that have expired at or before `now`.
    fn get_expired_keys(&self, now: Instant) -> Vec<Bytes> {
        let _ = now;
        vec![]
    }

    /// Get the count of keys that have an expiry (TTL) set.
    fn keys_with_expiry_count(&self) -> usize {
        0
    }

    /// Access the expiry index directly (for active expiry).
    #[deprecated(
        since = "0.2.0",
        note = "use get_expired_keys() or keys_with_expiry_count() instead"
    )]
    fn expiry_index(&self) -> Option<&ExpiryIndex> {
        None
    }

    /// Access the expiry index mutably (for active expiry cleanup).
    #[deprecated(since = "0.2.0", note = "use get_expired_keys() instead")]
    fn expiry_index_mut(&mut self) -> Option<&mut ExpiryIndex> {
        None
    }

    // ========================================================================
    // EvictionOps - Memory management
    // ========================================================================

    /// Get a random key from the store.
    fn random_key(&self) -> Option<Bytes> {
        None
    }

    /// Sample up to N random keys from the store.
    fn sample_keys(&self, count: usize) -> Vec<Bytes> {
        let _ = count;
        vec![]
    }

    /// Sample up to N random keys that have TTL set (volatile keys).
    fn sample_volatile_keys(&self, count: usize) -> Vec<Bytes> {
        let _ = count;
        vec![]
    }

    /// Get metadata for a key (for eviction decision making).
    fn get_metadata(&self, key: &[u8]) -> Option<KeyMetadata> {
        let _ = key;
        None
    }

    /// Get the idle time (time since last access) for a key.
    fn idle_time(&self, key: &[u8]) -> Option<Duration> {
        let _ = key;
        None
    }

    /// Update LFU counter for a key (on access).
    fn update_lfu_counter(&mut self, key: &[u8], log_factor: u8) {
        let _ = (key, log_factor);
    }

    /// Decay LFU counter for a key based on idle time.
    fn get_lfu_value(&self, key: &[u8], decay_time: u64) -> Option<u8> {
        let _ = (key, decay_time);
        None
    }

    // ========================================================================
    // ClusterSlotOps - Cluster support
    // ========================================================================

    /// Get keys in a specific slot (for CLUSTER GETKEYSINSLOT).
    fn keys_in_slot(&self, slot: u16, count: usize) -> Vec<Bytes> {
        let _ = (slot, count);
        vec![]
    }

    /// Count keys in a specific slot (for CLUSTER COUNTKEYSINSLOT).
    fn count_keys_in_slot(&self, slot: u16) -> usize {
        let _ = slot;
        0
    }

    // ========================================================================
    // Dirty tracking - RDB changes since last save
    // ========================================================================

    /// Get the number of changes since last save (for INFO persistence).
    fn dirty(&self) -> u64 {
        0
    }

    /// Increment the dirty counter by the given amount.
    fn increment_dirty(&mut self, count: u64) {
        let _ = count;
    }

    // ========================================================================
    // TimeSeries label index
    // ========================================================================

    /// Access the label index for TS.QUERYINDEX / TS.MGET / TS.MRANGE lookups.
    fn ts_label_index(&self) -> Option<&LabelIndex> {
        None
    }

    /// Mutably access the label index (for TS.ALTER label updates).
    fn ts_label_index_mut(&mut self) -> Option<&mut LabelIndex> {
        None
    }
}
