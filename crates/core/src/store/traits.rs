//! Store sub-traits for focused functionality.
//!
//! These sub-traits decompose the monolithic `Store` trait into smaller,
//! single-responsibility interfaces. This enables:
//!
//! - **Narrower bounds**: Functions can accept `impl StorageOps` instead of full `Store`
//! - **Better documentation**: Each trait documents a specific capability
//! - **Easier testing**: Mock implementations only need to implement relevant traits

use bytes::Bytes;
use std::time::{Duration, Instant};

use crate::noop::ExpiryIndex;
use crate::types::{KeyMetadata, KeyType, SetOptions, SetResult, Value};

// ============================================================================
// StorageOps - Core CRUD operations
// ============================================================================

/// Core key-value storage operations.
///
/// This trait provides the fundamental CRUD operations that all stores must implement.
/// It represents the minimal interface for a key-value store.
pub trait StorageOps: Send {
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

    /// Number of keys in the store.
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
    /// Returns None if key doesn't exist or is expired.
    fn get_mut(&mut self, key: &[u8]) -> Option<&mut Value>;
}

// ============================================================================
// ExpiryOps - TTL and expiration management
// ============================================================================

/// Expiry and TTL management operations.
///
/// This trait provides methods for managing key expiration times.
/// Stores that don't support expiry can use the default implementations.
pub trait ExpiryOps: StorageOps {
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
        let _ = (key, expires_at);
        false
    }

    /// Get the expiry time for a key.
    fn get_expiry(&self, key: &[u8]) -> Option<Instant> {
        let _ = key;
        None
    }

    /// Remove expiry from a key (PERSIST command).
    ///
    /// Returns true if the key exists and had an expiry that was removed.
    fn persist(&mut self, key: &[u8]) -> bool {
        let _ = key;
        false
    }

    /// Update last access time for a key (TOUCH command).
    ///
    /// Returns true if the key exists.
    fn touch(&mut self, key: &[u8]) -> bool {
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

    /// Get all keys that have expired at or before `now`.
    ///
    /// Returns keys in expiration order (oldest first).
    fn get_expired_keys(&self, now: Instant) -> Vec<Bytes> {
        let _ = now;
        vec![]
    }

    /// Get the count of keys that have an expiry (TTL) set.
    fn keys_with_expiry_count(&self) -> usize {
        0
    }

    /// Access the expiry index directly (for active expiry).
    ///
    /// **Deprecated**: Use `get_expired_keys()` and `keys_with_expiry_count()` instead.
    #[deprecated(
        since = "0.2.0",
        note = "use get_expired_keys() or keys_with_expiry_count() instead"
    )]
    fn expiry_index(&self) -> Option<&ExpiryIndex> {
        None
    }

    /// Access the expiry index mutably (for active expiry cleanup).
    ///
    /// **Deprecated**: Use `get_expired_keys()` instead.
    #[deprecated(since = "0.2.0", note = "use get_expired_keys() instead")]
    fn expiry_index_mut(&mut self) -> Option<&mut ExpiryIndex> {
        None
    }
}

// ============================================================================
// ScanOps - Cursor-based iteration
// ============================================================================

/// Cursor-based iteration operations for SCAN commands.
pub trait ScanOps: StorageOps {
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
}

// ============================================================================
// EvictionOps - Memory management and eviction support
// ============================================================================

/// Eviction support operations for memory management.
///
/// This trait provides methods needed for implementing eviction policies
/// like LRU, LFU, and random eviction.
pub trait EvictionOps: StorageOps {
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
}

// ============================================================================
// ClusterSlotOps - Cluster slot-based operations
// ============================================================================

/// Cluster slot operations for Redis Cluster support.
///
/// This trait provides methods for working with slot-based key distribution.
pub trait ClusterSlotOps: StorageOps {
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
