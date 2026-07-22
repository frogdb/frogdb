//! Storage traits and implementations.
//!
//! This module provides the core storage abstraction for FrogDB. The main
//! trait is [`Store`], a single interface over the in-memory [`HashMapStore`]
//! (the CRUD, TTL, SCAN, eviction, and cluster-slot operations a shard needs).
//! Type-safe, WrongType-checked access to the individual value families lives
//! in the [`typed`] layer ([`StoreTypedExt`]/[`StoreTypedFamilyExt`],
//! proposal 02); prefer it for command handlers that operate on one type.
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

mod hashmap;
mod timeseries_labels;
mod typed;
mod warm_tier;

// Re-export typed-access extension trait
pub use typed::{StoreTypedExt, StoreTypedFamilyExt, TypedArc, WrongTypeError};

// Re-export HashMapStore implementation
pub use hashmap::{BackdateExpiryResult, HashMapStore, SpillError};

// Re-export the cohesive subsystems extracted out of HashMapStore so the
// `Store` trait can expose them directly (`warm_tier()` / `ts_labels()`).
pub use timeseries_labels::TimeSeriesLabels;
pub use warm_tier::WarmTier;

use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::JsonValue;
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

/// One inconsistency found by [`Store::audit_expiry_index`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpiryIndexAnomaly {
    /// The offending key (lossy UTF-8 for display/transport).
    pub key: String,
    /// What is wrong.
    pub kind: ExpiryIndexAnomalyKind,
}

/// Classes of expiry-index inconsistency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpiryIndexAnomalyKind {
    /// Index references a key that no longer exists in the store.
    KeyMissing,
    /// Index references a key whose entry has no deadline (persistent).
    KeyPersistent,
    /// Index deadline disagrees with the entry's `expires_at`.
    DeadlineMismatch,
    /// Entry has a deadline but is absent from the index.
    IndexMissing,
}

/// Storage trait for key-value operations.
///
/// The single storage abstraction a shard operates through: CRUD, TTL/expiry,
/// cursor-based SCAN, eviction, and cluster-slot support. The section markers
/// below group the methods by capability. For type-checked access to a single
/// value family, use the [`typed`] extension layer rather than reaching for a
/// narrower bound here.
pub trait Store: Send {
    // ========================================================================
    // Core CRUD
    // ========================================================================

    /// Get a value by key. May unspill warm values to hot (requires `&mut self`).
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

    /// Whether the key exists and is not logically expired — the Redis
    /// `lookupKeyRead` existence test used for keyspace hit/miss accounting.
    ///
    /// Unlike [`Store::contains`], a key past its TTL reads as absent even if
    /// active/lazy expiry has not yet removed it. Non-mutating and does not
    /// touch LRU/LFU metadata, so the execution seam can probe it without
    /// perturbing the value a command's own read observes. The default
    /// implementation falls back to `contains` (no expiry tracking).
    fn exists_unexpired(&self, key: &[u8]) -> bool {
        self.contains(key)
    }

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

    /// Recompute the live memory footprint by summing every entry's current
    /// size, independent of the running [`Store::memory_used`] counter.
    ///
    /// Used by `DEBUG MEMORY-CHECK`: at quiesce (deferred size refreshes
    /// flushed) this must equal `memory_used()`; a difference is an accounting
    /// leak. The default returns `memory_used()` (trivially consistent) for
    /// stores that do not track per-entry sizes.
    fn recompute_memory_used(&self) -> usize {
        self.memory_used()
    }

    /// Cross-check the key-level expiry index against actual entry deadlines,
    /// returning every inconsistency. Used by `DEBUG EXPIRY-INDEX-CHECK`; at
    /// quiesce this must be empty. The default returns empty for stores without
    /// an expiry index.
    fn audit_expiry_index(&self) -> Vec<ExpiryIndexAnomaly> {
        Vec::new()
    }

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
    // Cursor-based iteration (SCAN)
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
    // TTL management
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

    /// Drain the buffer of keys physically removed by **lazy** expiry
    /// (`check_and_delete_expired` → `uninstall`) since the last drain.
    ///
    /// The store reports *which* keys it lazily removed; it does not act on the
    /// report — it stays version- and wait-queue-ignorant. The worker drains
    /// this after each command and applies the parity effects (shard-version
    /// bump + XREADGROUP drain), exactly as active expiry applies them from
    /// `ExpiryResult::deleted_keys`. Active expiry deletes via `delete`, not
    /// `check_and_delete_expired`, so it never populates this buffer.
    ///
    /// Default: no lazy-purge reporting (stores that do not lazily purge).
    fn take_lazily_purged(&mut self) -> Vec<Bytes> {
        Vec::new()
    }

    /// Drain the buffer of keys removed because their **last hash field**
    /// expired on a lazy read (`purge_expired_hash_fields` -> `delete`) since
    /// the last drain.
    ///
    /// A distinct seam from [`Store::take_lazily_purged`]: the whole key never
    /// had its own TTL elapse — the hash simply emptied via field TTL — so Redis
    /// emits a generic `del` (not `expired`) for it, matching active expiry's
    /// `ExpiryResult::emptied_keys`. The worker drains this after each command
    /// and fires the `del` notification + tracking/search invalidation. The
    /// active sweep shares `purge_expired_hash_fields` and so populates this
    /// buffer too, but owns its reporting via `ExpiryResult` and discards the
    /// buffer at the sweep seam, so it only ever fires for genuinely lazy reads.
    ///
    /// Default: no lazy-emptied reporting (stores without hash-field TTL).
    fn take_lazily_emptied(&mut self) -> Vec<Bytes> {
        Vec::new()
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
    // Memory management & eviction
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
    // Cluster slot support
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

    /// Access the timeseries label subsystem for TS.QUERYINDEX / TS.MGET /
    /// TS.MRANGE lookups. Returns `None` for stores without timeseries support.
    ///
    /// This is the cohesive accessor for the label index that used to be reached
    /// through the store as `ts_label_index()`; go through
    /// [`TimeSeriesLabels::index`] for the underlying `LabelIndex`.
    fn ts_labels(&self) -> Option<&TimeSeriesLabels> {
        None
    }

    /// Mutably access the timeseries label subsystem (for TS.ALTER label
    /// updates). Returns `None` for stores without timeseries support.
    fn ts_labels_mut(&mut self) -> Option<&mut TimeSeriesLabels> {
        None
    }

    // ========================================================================
    // Tiered storage stats
    // ========================================================================

    /// Access the warm (tiered) storage subsystem, or `None` when tiered
    /// storage is not part of this store.
    ///
    /// The cohesive accessor for the warm-tier counters that used to be reached
    /// through the store as `warm_key_count()` / `hot_key_count()` /
    /// `spill_count()` / `unspill_count()` / `expired_on_unspill_count()`.
    /// Hot-key count is `len() - warm_tier().map_or(0, WarmTier::warm_keys)`.
    fn warm_tier(&self) -> Option<&WarmTier> {
        None
    }

    // ========================================================================
    // Keysize histograms
    // ========================================================================

    /// Access the keysize histograms (for INFO keysizes).
    fn keysizes(&self) -> Option<&KeysizeHistograms> {
        None
    }
}
