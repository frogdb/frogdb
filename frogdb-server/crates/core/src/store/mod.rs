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
mod typed;

// Re-export sub-traits for narrower bounds
pub use traits::{ClusterSlotOps, EvictionOps, ExpiryOps, ScanOps, StorageOps};

// Re-export typed-access extension trait
pub use typed::{StoreTypedExt, StoreTypedFamilyExt, TypedArc, WrongTypeError};

// Re-export HashMapStore implementation
pub use hashmap::{DemotionError, HashMapStore};

use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::JsonValue;
use crate::LabelIndex;
use crate::histogram::KeysizeHistograms;
use crate::noop::ExpiryIndex;
use crate::types::{
    HashValue, KeyMetadata, KeyType, ListValue, SetOptions, SetResult, SetValue, SortedSetValue,
    StreamValue, StringValue, Value,
};
use crate::{
    BloomFilterValue, CountMinSketchValue, CuckooFilterValue, HyperLogLogValue, TDigestValue,
    TimeSeriesValue, TopKValue, VectorSetValue,
};

// ============================================================================
// ValueType trait - Generic type access
// ============================================================================

/// Type-safe projection of a [`Value`] to its inner type. Every family
/// implements this — it is the vocabulary the typed store seam
/// ([`StoreTypedExt`](typed::StoreTypedExt)) uses to enforce the `WrongType`
/// invariant in one place.
///
/// Create-if-missing is *not* part of this trait: the probabilistic/extension
/// families (bloom, cuckoo, topk, tdigest, cms, hyperloglog, timeseries,
/// vectorset) have no meaningful parameterless default — they are always
/// constructed with command-supplied parameters. Only the families that *do*
/// have a parameterless default additionally implement [`DefaultValueType`].
///
/// # Example
///
/// ```rust,ignore
/// let list: Option<&mut ListValue> = ctx.store.get_list_mut(key)?;
/// ```
pub trait ValueType: Sized {
    /// Type name for error messages.
    fn type_name() -> &'static str;

    /// Try to get a reference from a Value.
    fn from_value(value: &Value) -> Option<&Self>;

    /// Try to get a mutable reference from a Value.
    fn from_value_mut(value: &mut Value) -> Option<&mut Self>;
}

/// A [`ValueType`] with a meaningful parameterless default, enabling
/// create-if-missing via [`get_or_create_typed`](typed::StoreTypedExt::get_or_create_typed).
///
/// Implemented by the core families (list/set/hash/sorted set/stream/string)
/// plus JSON. The probabilistic/extension families deliberately do *not*
/// implement it: their creation requires command-supplied parameters and lives
/// in the command, so only the access-if-exists path is shared.
pub trait DefaultValueType: ValueType {
    /// Create a new default value of this type.
    fn create_default() -> Value;
}

// ============================================================================
// ValueType implementations
// ============================================================================

impl ValueType for ListValue {
    fn type_name() -> &'static str {
        "list"
    }

    fn from_value(value: &Value) -> Option<&Self> {
        value.as_list()
    }

    fn from_value_mut(value: &mut Value) -> Option<&mut Self> {
        value.as_list_mut()
    }
}

impl DefaultValueType for ListValue {
    fn create_default() -> Value {
        Value::list()
    }
}

impl ValueType for SetValue {
    fn type_name() -> &'static str {
        "set"
    }

    fn from_value(value: &Value) -> Option<&Self> {
        value.as_set()
    }

    fn from_value_mut(value: &mut Value) -> Option<&mut Self> {
        value.as_set_mut()
    }
}

impl DefaultValueType for SetValue {
    fn create_default() -> Value {
        Value::set()
    }
}

impl ValueType for HashValue {
    fn type_name() -> &'static str {
        "hash"
    }

    fn from_value(value: &Value) -> Option<&Self> {
        value.as_hash()
    }

    fn from_value_mut(value: &mut Value) -> Option<&mut Self> {
        value.as_hash_mut()
    }
}

impl DefaultValueType for HashValue {
    fn create_default() -> Value {
        Value::hash()
    }
}

impl ValueType for SortedSetValue {
    fn type_name() -> &'static str {
        "sorted set"
    }

    fn from_value(value: &Value) -> Option<&Self> {
        value.as_sorted_set()
    }

    fn from_value_mut(value: &mut Value) -> Option<&mut Self> {
        value.as_sorted_set_mut()
    }
}

impl DefaultValueType for SortedSetValue {
    fn create_default() -> Value {
        Value::sorted_set()
    }
}

impl ValueType for StreamValue {
    fn type_name() -> &'static str {
        "stream"
    }

    fn from_value(value: &Value) -> Option<&Self> {
        value.as_stream()
    }

    fn from_value_mut(value: &mut Value) -> Option<&mut Self> {
        value.as_stream_mut()
    }
}

impl DefaultValueType for StreamValue {
    fn create_default() -> Value {
        Value::stream()
    }
}

impl ValueType for StringValue {
    fn type_name() -> &'static str {
        "string"
    }

    fn from_value(value: &Value) -> Option<&Self> {
        value.as_string()
    }

    fn from_value_mut(value: &mut Value) -> Option<&mut Self> {
        value.as_string_mut()
    }
}

impl DefaultValueType for StringValue {
    fn create_default() -> Value {
        Value::string(Bytes::new())
    }
}

impl ValueType for JsonValue {
    fn type_name() -> &'static str {
        "json"
    }

    fn from_value(value: &Value) -> Option<&Self> {
        value.as_json()
    }

    fn from_value_mut(value: &mut Value) -> Option<&mut Self> {
        value.as_json_mut()
    }
}

impl DefaultValueType for JsonValue {
    fn create_default() -> Value {
        Value::json(serde_json::Value::Null)
    }
}

// ----------------------------------------------------------------------------
// Probabilistic / extension families.
//
// These have no meaningful parameterless default (every one is created with
// command-supplied parameters), so they implement `ValueType` only — not
// `DefaultValueType`. They ride the same typed-access seam as the core
// families; creation stays in the command.
// ----------------------------------------------------------------------------

impl ValueType for BloomFilterValue {
    fn type_name() -> &'static str {
        "bloom"
    }

    fn from_value(value: &Value) -> Option<&Self> {
        value.as_bloom_filter()
    }

    fn from_value_mut(value: &mut Value) -> Option<&mut Self> {
        value.as_bloom_filter_mut()
    }
}

impl ValueType for CuckooFilterValue {
    fn type_name() -> &'static str {
        "cuckoo"
    }

    fn from_value(value: &Value) -> Option<&Self> {
        value.as_cuckoo_filter()
    }

    fn from_value_mut(value: &mut Value) -> Option<&mut Self> {
        value.as_cuckoo_filter_mut()
    }
}

impl ValueType for TopKValue {
    fn type_name() -> &'static str {
        "topk"
    }

    fn from_value(value: &Value) -> Option<&Self> {
        value.as_topk()
    }

    fn from_value_mut(value: &mut Value) -> Option<&mut Self> {
        value.as_topk_mut()
    }
}

impl ValueType for TDigestValue {
    fn type_name() -> &'static str {
        "tdigest"
    }

    fn from_value(value: &Value) -> Option<&Self> {
        value.as_tdigest()
    }

    fn from_value_mut(value: &mut Value) -> Option<&mut Self> {
        value.as_tdigest_mut()
    }
}

impl ValueType for CountMinSketchValue {
    fn type_name() -> &'static str {
        "cms"
    }

    fn from_value(value: &Value) -> Option<&Self> {
        value.as_cms()
    }

    fn from_value_mut(value: &mut Value) -> Option<&mut Self> {
        value.as_cms_mut()
    }
}

impl ValueType for HyperLogLogValue {
    fn type_name() -> &'static str {
        "hyperloglog"
    }

    fn from_value(value: &Value) -> Option<&Self> {
        value.as_hyperloglog()
    }

    fn from_value_mut(value: &mut Value) -> Option<&mut Self> {
        value.as_hyperloglog_mut()
    }
}

impl ValueType for TimeSeriesValue {
    fn type_name() -> &'static str {
        "TSDB-TYPE"
    }

    fn from_value(value: &Value) -> Option<&Self> {
        value.as_timeseries()
    }

    fn from_value_mut(value: &mut Value) -> Option<&mut Self> {
        value.as_timeseries_mut()
    }
}

impl ValueType for VectorSetValue {
    fn type_name() -> &'static str {
        "vectorset"
    }

    fn from_value(value: &Value) -> Option<&Self> {
        value.as_vectorset()
    }

    fn from_value_mut(value: &mut Value) -> Option<&mut Self> {
        value.as_vectorset_mut()
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

    /// Get a value by key. May promote warm values to hot (requires `&mut self`).
    ///
    /// Returns an `Arc<Value>` for zero-copy reads. The Arc ref-count bump
    /// is much cheaper than deep-cloning large values like hashes or lists.
    fn get(&mut self, key: &[u8]) -> Option<Arc<Value>>;

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

    /// Lazily delete the key if it is past its expiry deadline.
    ///
    /// Returns `true` if the key was expired and removed. Callers that access
    /// the value via `get`/`get_mut`/`get_hot` (which do NOT check expiry) can
    /// use this to observe lazy expiry before reading — the blocking-satisfy
    /// paths call this so a blocker woken by a write doesn't receive a
    /// stale value from a just-expired key.
    fn purge_if_expired(&mut self, key: &[u8]) -> bool {
        let _ = key;
        false
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

    /// Get up to `limit` keys that have expired at or before `now`.
    ///
    /// Bounds the scan/allocation so active expiry cannot stall the shard
    /// scanning a huge due set before its time budget is consulted. The default
    /// truncates [`Store::get_expired_keys`]; real stores should override to
    /// bound the scan itself (stop cloning after `limit` entries).
    fn get_expired_keys_limited(&self, now: Instant, limit: usize) -> Vec<Bytes> {
        let mut keys = self.get_expired_keys(now);
        keys.truncate(limit);
        keys
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

    // Field-level expiry (hash field TTL)

    /// Set expiry for a hash field.
    fn set_field_expiry(&mut self, key: &[u8], field: &[u8], expires_at: Instant) {
        let _ = (key, field, expires_at);
    }

    /// Remove expiry for a hash field.
    fn remove_field_expiry(&mut self, key: &[u8], field: &[u8]) {
        let _ = (key, field);
    }

    /// Remove all field expiries for a key.
    fn remove_all_field_expiries(&mut self, key: &[u8]) {
        let _ = key;
    }

    /// Get expiry for a hash field.
    fn get_field_expiry(&self, key: &[u8], field: &[u8]) -> Option<Instant> {
        let _ = (key, field);
        None
    }

    /// Get all expired (key, field) pairs up to `now`.
    fn get_expired_fields(&self, now: Instant) -> Vec<(Bytes, Bytes)> {
        let _ = now;
        vec![]
    }

    /// Get up to `limit` expired (key, field) pairs up to `now`.
    ///
    /// Bounds the scan/allocation like [`Store::get_expired_keys_limited`]. The
    /// default truncates [`Store::get_expired_fields`]; real stores should
    /// override to bound the scan itself.
    fn get_expired_fields_limited(&self, now: Instant, limit: usize) -> Vec<(Bytes, Bytes)> {
        let mut fields = self.get_expired_fields(now);
        fields.truncate(limit);
        fields
    }

    /// Purge expired fields from a hash (lazy deletion).
    /// Returns count of fields removed.
    fn purge_expired_hash_fields(&mut self, key: &[u8]) -> usize {
        let _ = key;
        0
    }

    /// Suppress or unsuppress passive/lazy expiry (during CLIENT PAUSE).
    fn set_expiry_suppressed(&mut self, _suppressed: bool) {}

    /// Set whether touch() calls should be suppressed (CLIENT NO-TOUCH mode).
    ///
    /// When true, `get_with_expiry_check()` and `get_mut()` skip updating
    /// the key's last access time. The explicit `touch()` method (used by
    /// the TOUCH command) is NOT affected.
    fn set_suppress_touch(&mut self, _suppress: bool) {}

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

    // ========================================================================
    // Tiered storage stats
    // ========================================================================

    /// Number of warm (demoted) keys. Returns 0 if tiered storage is disabled.
    fn warm_key_count(&self) -> usize {
        0
    }

    /// Number of hot (in-memory) keys.
    fn hot_key_count(&self) -> usize {
        self.len()
    }

    /// Total hot→warm demotions.
    fn demotion_count(&self) -> u64 {
        0
    }

    /// Total warm→hot promotions.
    fn promotion_count(&self) -> u64 {
        0
    }

    /// Keys expired during promotion attempt.
    fn expired_on_promote_count(&self) -> u64 {
        0
    }

    // ========================================================================
    // Keysize histograms
    // ========================================================================

    /// Access the keysize histograms (for INFO keysizes).
    fn keysizes(&self) -> Option<&KeysizeHistograms> {
        None
    }

    /// Mutably access the keysize histograms (for CONFIG SET toggle).
    fn keysizes_mut(&mut self) -> Option<&mut KeysizeHistograms> {
        None
    }

    /// Flush pending keysizes refreshes from `get_mut()` calls.
    ///
    /// Commands that mutate values in-place via `get_mut()` record a
    /// before-snapshot. This method compares those snapshots with the
    /// current state and migrates histogram bins as needed.
    fn flush_keysizes_refreshes(&mut self) {}

    /// Calculate total allocated memory for keys in a given slot.
    fn allocsize_in_slot(&self, _slot: u16) -> usize {
        0
    }
}
