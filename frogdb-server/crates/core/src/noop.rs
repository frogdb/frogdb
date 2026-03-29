//! Noop abstractions for future features.
//!
//! This module provides noop implementations and data structures used when
//! features like WAL, replication, and metrics are disabled.
//!
//! The trait definitions and their noop implementations have been moved to
//! the `traits` module for better organization. This module re-exports them
//! for backward compatibility.

use bytes::Bytes;
use griddle::HashMap;
use rand::seq::IteratorRandom;
use std::collections::BTreeMap;
use std::time::Instant;

// Re-export traits and noop implementations from frogdb-types
// for backward compatibility
pub use frogdb_types::traits::{
    // Metrics
    MetricsRecorder,
    NoopMetricsRecorder,
    NoopReplicationTracker,
    NoopSpan,
    NoopTracer,
    NoopWalWriter,
    // Replication
    ReplicationConfig,
    ReplicationTracker,
    Span,
    // Tracing
    Tracer,
    WalOperation,
    // WAL
    WalWriter,
};

// ============================================================================
// Expiry
// ============================================================================

/// Index for tracking key expiration times.
///
/// Uses a dual-index structure:
/// - `by_time`: BTreeMap ordered by expiration time for efficient active expiry
/// - `by_key`: HashMap for O(1) key lookup and updates
#[derive(Debug)]
pub struct ExpiryIndex {
    /// Time-ordered index for active expiry scanning.
    /// Key is (expiry_instant, key_bytes) to handle multiple keys with same expiry.
    by_time: BTreeMap<(Instant, Bytes), ()>,
    /// Fast key-to-expiry lookup for updates and removals.
    by_key: HashMap<Bytes, Instant>,
}

impl Default for ExpiryIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl ExpiryIndex {
    /// Create a new empty expiry index.
    pub fn new() -> Self {
        Self {
            by_time: BTreeMap::new(),
            by_key: HashMap::new(),
        }
    }

    /// Add or update expiry for a key.
    pub fn set(&mut self, key: Bytes, expires_at: Instant) {
        // If key already has an expiry, remove it from the time index
        if let Some(old_expiry) = self.by_key.get(&key) {
            self.by_time.remove(&(*old_expiry, key.clone()));
        }

        // Add to both indexes
        self.by_key.insert(key.clone(), expires_at);
        self.by_time.insert((expires_at, key), ());
    }

    /// Remove expiry for a key.
    pub fn remove(&mut self, key: &[u8]) {
        if let Some(expiry) = self.by_key.remove(key) {
            // Need to find and remove from by_time
            // Since we're using (Instant, Bytes) as key, we need the exact bytes
            self.by_time.remove(&(expiry, Bytes::copy_from_slice(key)));
        }
    }

    /// Get the expiry time for a key, if set.
    pub fn get(&self, key: &[u8]) -> Option<Instant> {
        self.by_key.get(key).copied()
    }

    /// Get all expired keys up to `now`.
    ///
    /// Returns keys in expiration order (oldest first).
    pub fn get_expired(&self, now: Instant) -> Vec<Bytes> {
        let mut expired = Vec::new();

        for ((expiry, key), _) in self.by_time.iter() {
            if *expiry <= now {
                expired.push(key.clone());
            } else {
                // BTreeMap is ordered, so we can stop early
                break;
            }
        }

        expired
    }

    /// Sample up to N random keys that have expiry set.
    ///
    /// Used for probabilistic active expiry (Redis-style) and volatile eviction.
    /// Returns keys in random order.
    pub fn sample(&self, n: usize) -> Vec<Bytes> {
        if self.by_key.is_empty() || n == 0 {
            return vec![];
        }

        let mut rng = rand::rng();
        self.by_key
            .keys()
            .choose_multiple(&mut rng, n)
            .into_iter()
            .cloned()
            .collect()
    }

    /// Number of keys with expiry set.
    pub fn len(&self) -> usize {
        self.by_key.len()
    }

    /// Check if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }
}

/// Index for tracking hash field expiration times.
///
/// Uses a dual-index structure:
/// - `by_time`: BTreeMap ordered by (expiration_time, key, field) for efficient active expiry
/// - `by_key`: HashMap for O(1) lookup of field expiries per key
#[derive(Debug)]
pub struct FieldExpiryIndex {
    /// Time-ordered index for active expiry scanning.
    by_time: BTreeMap<(Instant, Bytes, Bytes), ()>,
    /// Key -> { field -> expiry_time } for fast lookups.
    by_key: HashMap<Bytes, HashMap<Bytes, Instant>>,
}

impl Default for FieldExpiryIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl FieldExpiryIndex {
    /// Create a new empty field expiry index.
    pub fn new() -> Self {
        Self {
            by_time: BTreeMap::new(),
            by_key: HashMap::new(),
        }
    }

    /// Add or update field expiry.
    pub fn set(&mut self, key: Bytes, field: Bytes, expires_at: Instant) {
        // Remove old entry if exists
        if let Some(fields) = self.by_key.get(&key)
            && let Some(old_expiry) = fields.get(&field)
        {
            self.by_time
                .remove(&(*old_expiry, key.clone(), field.clone()));
        }

        // Add to both indexes
        self.by_key
            .entry(key.clone())
            .or_default()
            .insert(field.clone(), expires_at);
        self.by_time.insert((expires_at, key, field), ());
    }

    /// Remove a specific field's expiry.
    pub fn remove(&mut self, key: &[u8], field: &[u8]) {
        if let Some(fields) = self.by_key.get_mut(key) {
            if let Some(expiry) = fields.remove(field) {
                self.by_time.remove(&(
                    expiry,
                    Bytes::copy_from_slice(key),
                    Bytes::copy_from_slice(field),
                ));
            }
            if fields.is_empty() {
                self.by_key.remove(key);
            }
        }
    }

    /// Remove all field expiries for a key.
    pub fn remove_key(&mut self, key: &[u8]) {
        if let Some(fields) = self.by_key.remove(key) {
            for (field, expiry) in fields {
                self.by_time
                    .remove(&(expiry, Bytes::copy_from_slice(key), field));
            }
        }
    }

    /// Get expiry for a specific field.
    pub fn get(&self, key: &[u8], field: &[u8]) -> Option<Instant> {
        self.by_key.get(key)?.get(field).copied()
    }

    /// Get all expired (key, field) pairs up to `now`.
    ///
    /// Returns pairs in expiration order (oldest first).
    pub fn get_expired(&self, now: Instant) -> Vec<(Bytes, Bytes)> {
        let mut expired = Vec::new();
        for ((expiry, key, field), _) in self.by_time.iter() {
            if *expiry <= now {
                expired.push((key.clone(), field.clone()));
            } else {
                break;
            }
        }
        expired
    }

    /// Number of field expiries tracked.
    pub fn len(&self) -> usize {
        self.by_time.len()
    }

    /// Check if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.by_time.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expiry_index_set_get_remove() {
        let mut index = ExpiryIndex::new();
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);

        let future = Instant::now() + std::time::Duration::from_secs(10);
        index.set(Bytes::from("key1"), future);
        assert_eq!(index.len(), 1);
        assert!(!index.is_empty());
        assert_eq!(index.get(b"key1"), Some(future));

        // Update expiry
        let new_future = future + std::time::Duration::from_secs(10);
        index.set(Bytes::from("key1"), new_future);
        assert_eq!(index.len(), 1); // Still only 1 key
        assert_eq!(index.get(b"key1"), Some(new_future));

        // Remove
        index.remove(b"key1");
        assert!(index.is_empty());
        assert_eq!(index.get(b"key1"), None);
    }

    #[test]
    fn test_expiry_index_get_expired() {
        let mut index = ExpiryIndex::new();
        let now = Instant::now();
        let past = now - std::time::Duration::from_secs(1);
        let future = now + std::time::Duration::from_secs(10);

        index.set(Bytes::from("expired1"), past);
        index.set(Bytes::from("expired2"), past);
        index.set(Bytes::from("not_expired"), future);

        let expired = index.get_expired(now);
        assert_eq!(expired.len(), 2);
        assert!(expired.contains(&Bytes::from("expired1")));
        assert!(expired.contains(&Bytes::from("expired2")));
    }

    #[test]
    fn test_expiry_index_sample() {
        let mut index = ExpiryIndex::new();
        let future = Instant::now() + std::time::Duration::from_secs(10);

        index.set(Bytes::from("key1"), future);
        index.set(Bytes::from("key2"), future);
        index.set(Bytes::from("key3"), future);

        let sample = index.sample(2);
        assert_eq!(sample.len(), 2);

        let sample_all = index.sample(10);
        assert_eq!(sample_all.len(), 3);
    }

    #[test]
    fn test_field_expiry_index_set_get_remove() {
        let mut index = FieldExpiryIndex::new();
        assert!(index.is_empty());

        let future = Instant::now() + std::time::Duration::from_secs(10);
        index.set(Bytes::from("key1"), Bytes::from("field1"), future);
        assert_eq!(index.len(), 1);
        assert_eq!(index.get(b"key1", b"field1"), Some(future));

        // Update expiry
        let new_future = future + std::time::Duration::from_secs(10);
        index.set(Bytes::from("key1"), Bytes::from("field1"), new_future);
        assert_eq!(index.len(), 1);
        assert_eq!(index.get(b"key1", b"field1"), Some(new_future));

        // Remove
        index.remove(b"key1", b"field1");
        assert!(index.is_empty());
        assert_eq!(index.get(b"key1", b"field1"), None);
    }

    #[test]
    fn test_field_expiry_index_remove_key() {
        let mut index = FieldExpiryIndex::new();
        let future = Instant::now() + std::time::Duration::from_secs(10);

        index.set(Bytes::from("key1"), Bytes::from("f1"), future);
        index.set(Bytes::from("key1"), Bytes::from("f2"), future);
        index.set(Bytes::from("key2"), Bytes::from("f1"), future);
        assert_eq!(index.len(), 3);

        index.remove_key(b"key1");
        assert_eq!(index.len(), 1);
        assert_eq!(index.get(b"key1", b"f1"), None);
        assert_eq!(index.get(b"key2", b"f1"), Some(future));
    }

    #[test]
    fn test_field_expiry_index_get_expired() {
        let mut index = FieldExpiryIndex::new();
        let now = Instant::now();
        let past = now - std::time::Duration::from_secs(1);
        let future = now + std::time::Duration::from_secs(10);

        index.set(Bytes::from("key1"), Bytes::from("f1"), past);
        index.set(Bytes::from("key1"), Bytes::from("f2"), future);
        index.set(Bytes::from("key2"), Bytes::from("f1"), past);

        let expired = index.get_expired(now);
        assert_eq!(expired.len(), 2);
    }
}
