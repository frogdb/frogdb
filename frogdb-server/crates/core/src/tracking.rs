//! Client-side caching invalidation infrastructure for FrogDB.
//!
//! This module provides the server-side tracking needed for Redis-compatible
//! `CLIENT TRACKING` support:
//! - [`InvalidationRegistry`] — per-shard registry of connections with tracking enabled
//! - [`TrackingTable`] — per-shard mapping from keys to interested connections
//! - [`InvalidationMessage`] — messages sent to connections when tracked keys change

use std::collections::{HashMap, HashSet, VecDeque};

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::pubsub::ConnId;

/// Default maximum number of tracked keys per shard (1 million).
pub const DEFAULT_TRACKING_TABLE_MAX_KEYS: usize = 1_000_000;

/// Sender for delivering invalidation messages to connections.
pub type InvalidationSender = mpsc::UnboundedSender<InvalidationMessage>;

/// Messages sent to connections when tracked keys are modified.
#[derive(Debug, Clone)]
pub enum InvalidationMessage {
    /// Invalidate specific keys — the client should evict these from its cache.
    Keys(Vec<Bytes>),
    /// Flush all — the client should clear its entire cache (e.g., FLUSHDB).
    FlushAll,
}

/// Metadata for a connection registered for tracking on a shard.
#[derive(Debug)]
pub struct TrackedConnection {
    pub sender: InvalidationSender,
    pub noloop: bool,
}

/// Per-shard registry of connections that have tracking enabled.
///
/// This is analogous to `ShardSubscriptions` for pub/sub — it maps connection IDs
/// to their invalidation sender and metadata.
#[derive(Debug, Default)]
pub struct InvalidationRegistry {
    connections: HashMap<ConnId, TrackedConnection>,
}

impl InvalidationRegistry {
    /// Register a connection for tracking on this shard.
    pub fn register(&mut self, conn_id: ConnId, conn: TrackedConnection) {
        self.connections.insert(conn_id, conn);
    }

    /// Unregister a connection from tracking on this shard.
    pub fn unregister(&mut self, conn_id: ConnId) {
        self.connections.remove(&conn_id);
    }

    /// Get a tracked connection by ID.
    pub fn get(&self, conn_id: &ConnId) -> Option<&TrackedConnection> {
        self.connections.get(conn_id)
    }

    /// Check if a connection is registered for tracking.
    pub fn contains(&self, conn_id: &ConnId) -> bool {
        self.connections.contains_key(conn_id)
    }

    /// Check if no connections have tracking enabled.
    pub fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }
}

/// Per-shard tracking table: maps keys to the set of connections interested in them.
///
/// When a connection reads a key with `track_reads=true`, the key is recorded.
/// When that key is later written, all interested connections receive an invalidation.
/// LRU eviction caps the table at `max_keys` entries per shard.
#[derive(Debug)]
pub struct TrackingTable {
    /// key → set of interested connection IDs.
    key_to_clients: HashMap<Bytes, HashSet<ConnId>>,
    /// Reverse index: conn_id → set of tracked keys (for O(1) connection cleanup).
    client_to_keys: HashMap<ConnId, HashSet<Bytes>>,
    /// LRU eviction order (front = oldest).
    lru_order: VecDeque<Bytes>,
    /// Maximum number of tracked keys.
    max_keys: usize,
}

impl TrackingTable {
    /// Create a new tracking table with the given capacity.
    pub fn new(max_keys: usize) -> Self {
        Self {
            key_to_clients: HashMap::new(),
            client_to_keys: HashMap::new(),
            lru_order: VecDeque::new(),
            max_keys,
        }
    }

    /// Record that `conn_id` read this key. LRU-evict if over capacity.
    pub fn record_read(&mut self, key: &[u8], conn_id: ConnId, registry: &InvalidationRegistry) {
        // Only record if the connection is registered for tracking
        if !registry.contains(&conn_id) {
            return;
        }

        let key_bytes = Bytes::copy_from_slice(key);
        let is_new_key = !self.key_to_clients.contains_key(&key_bytes);

        // Add conn_id to the key's interest set
        self.key_to_clients
            .entry(key_bytes.clone())
            .or_default()
            .insert(conn_id);

        // Add key to the conn_id's reverse index
        self.client_to_keys
            .entry(conn_id)
            .or_default()
            .insert(key_bytes.clone());

        // Update LRU: only add if this is a genuinely new key in the table
        if is_new_key {
            self.lru_order.push_back(key_bytes);

            // Evict if over capacity
            while self.key_to_clients.len() > self.max_keys {
                self.evict_lru(registry);
            }
        }
    }

    /// Invalidate tracked keys after a write.
    ///
    /// Sends `InvalidationMessage::Keys` to all tracked clients for the given keys.
    /// If `noloop` is set for a connection and `writer_conn_id` matches, that
    /// connection is skipped (the writer doesn't invalidate its own cache).
    /// Removes the keys from the tracking table.
    pub fn invalidate_keys(
        &mut self,
        keys: &[&[u8]],
        writer_conn_id: ConnId,
        registry: &InvalidationRegistry,
    ) {
        for key in keys {
            let key_bytes = Bytes::copy_from_slice(key);
            if let Some(conn_ids) = self.key_to_clients.remove(&key_bytes) {
                for &cid in &conn_ids {
                    // NOLOOP: skip sending to the writer if their noloop flag is set
                    if cid == writer_conn_id && registry.get(&cid).is_some_and(|t| t.noloop) {
                        continue;
                    }

                    // Send invalidation (ignore send errors — connection may have dropped)
                    if let Some(tracked) = registry.get(&cid) {
                        let _ = tracked
                            .sender
                            .send(InvalidationMessage::Keys(vec![key_bytes.clone()]));
                    }
                }

                // Clean up reverse index
                for cid in &conn_ids {
                    if let Some(keys_set) = self.client_to_keys.get_mut(cid) {
                        keys_set.remove(&key_bytes);
                        if keys_set.is_empty() {
                            self.client_to_keys.remove(cid);
                        }
                    }
                }
            }
            // Note: We don't remove from lru_order here — stale entries are
            // cleaned lazily during eviction (the key won't be in key_to_clients).
        }
    }

    /// Send `FlushAll` to all registered connections and clear the table.
    pub fn flush_all(&mut self, registry: &InvalidationRegistry) {
        // Send FlushAll to every registered connection (not just those with tracked keys)
        for (_, tracked) in registry.connections.iter() {
            let _ = tracked.sender.send(InvalidationMessage::FlushAll);
        }
        self.key_to_clients.clear();
        self.client_to_keys.clear();
        self.lru_order.clear();
    }

    /// Remove all tracking entries for a disconnected connection.
    pub fn remove_connection(&mut self, conn_id: ConnId) {
        // Use the reverse index for O(1) cleanup
        if let Some(keys) = self.client_to_keys.remove(&conn_id) {
            for key in keys {
                if let Some(clients) = self.key_to_clients.get_mut(&key) {
                    clients.remove(&conn_id);
                    if clients.is_empty() {
                        self.key_to_clients.remove(&key);
                        // Stale lru_order entries cleaned lazily during eviction
                    }
                }
            }
        }
    }

    /// Evict the oldest key from the LRU, sending invalidation to interested clients.
    fn evict_lru(&mut self, registry: &InvalidationRegistry) {
        while let Some(key) = self.lru_order.pop_front() {
            // Skip stale entries (already removed by invalidate_keys or remove_connection)
            if let Some(conn_ids) = self.key_to_clients.remove(&key) {
                // Send invalidation to all interested clients
                for &cid in &conn_ids {
                    if let Some(tracked) = registry.get(&cid) {
                        let _ = tracked
                            .sender
                            .send(InvalidationMessage::Keys(vec![key.clone()]));
                    }
                }
                // Clean up reverse index
                for cid in &conn_ids {
                    if let Some(keys_set) = self.client_to_keys.get_mut(cid) {
                        keys_set.remove(&key);
                        if keys_set.is_empty() {
                            self.client_to_keys.remove(cid);
                        }
                    }
                }
                return; // Evicted one real key
            }
            // If key wasn't in key_to_clients, it was stale — continue to next
        }
    }
}

/// Per-shard broadcast tracking: prefix → set of interested connections.
/// Used for BCAST mode where all writes matching a prefix trigger invalidation,
/// without per-read tracking.
#[derive(Debug, Default)]
pub struct BroadcastTable {
    /// prefix → set of conn_ids interested in keys starting with this prefix.
    prefix_to_clients: HashMap<Bytes, HashSet<ConnId>>,
    /// Reverse index: conn_id → set of registered prefixes.
    client_to_prefixes: HashMap<ConnId, HashSet<Bytes>>,
}

impl BroadcastTable {
    /// Register a connection for broadcast tracking with the given prefixes.
    /// An empty prefixes list means "match all keys" (Redis behavior for
    /// `CLIENT TRACKING ON BCAST` without PREFIX args).
    pub fn register(&mut self, conn_id: ConnId, prefixes: &[Bytes]) {
        if prefixes.is_empty() {
            // Empty prefix means match all keys
            let empty = Bytes::new();
            self.prefix_to_clients
                .entry(empty.clone())
                .or_default()
                .insert(conn_id);
            self.client_to_prefixes
                .entry(conn_id)
                .or_default()
                .insert(empty);
        } else {
            for prefix in prefixes {
                self.prefix_to_clients
                    .entry(prefix.clone())
                    .or_default()
                    .insert(conn_id);
                self.client_to_prefixes
                    .entry(conn_id)
                    .or_default()
                    .insert(prefix.clone());
            }
        }
    }

    /// Remove all entries for a connection.
    pub fn remove_connection(&mut self, conn_id: ConnId) {
        if let Some(prefixes) = self.client_to_prefixes.remove(&conn_id) {
            for prefix in prefixes {
                if let Some(clients) = self.prefix_to_clients.get_mut(&prefix) {
                    clients.remove(&conn_id);
                    if clients.is_empty() {
                        self.prefix_to_clients.remove(&prefix);
                    }
                }
            }
        }
    }

    /// Check if no connections have broadcast tracking enabled.
    pub fn is_empty(&self) -> bool {
        self.prefix_to_clients.is_empty()
    }

    /// Send invalidation to all BCAST connections matching the written keys.
    /// Respects NOLOOP: if a connection has noloop set and is the writer, it is skipped.
    pub fn invalidate_matching(
        &self,
        keys: &[&[u8]],
        writer_conn_id: ConnId,
        registry: &InvalidationRegistry,
    ) {
        // Collect (conn_id, key) pairs to invalidate, deduplicating per connection.
        // A connection may match multiple prefixes for the same key; we send one message per key.
        let mut conn_keys: HashMap<ConnId, Vec<Bytes>> = HashMap::new();

        for key in keys {
            for (prefix, clients) in &self.prefix_to_clients {
                // Empty prefix matches all keys; otherwise check prefix match
                if prefix.is_empty() || key.starts_with(prefix) {
                    for &cid in clients {
                        // NOLOOP: skip sending to the writer if their noloop flag is set
                        if cid == writer_conn_id && registry.get(&cid).is_some_and(|t| t.noloop) {
                            continue;
                        }
                        conn_keys
                            .entry(cid)
                            .or_default()
                            .push(Bytes::copy_from_slice(key));
                    }
                }
            }
        }

        // Send collected invalidations
        for (cid, keys) in conn_keys {
            if let Some(tracked) = registry.get(&cid) {
                let _ = tracked.sender.send(InvalidationMessage::Keys(keys));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_registry_with(
        entries: Vec<(ConnId, bool)>,
    ) -> (
        InvalidationRegistry,
        Vec<mpsc::UnboundedReceiver<InvalidationMessage>>,
    ) {
        let mut registry = InvalidationRegistry::default();
        let mut receivers = Vec::new();
        for (conn_id, noloop) in entries {
            let (tx, rx) = mpsc::unbounded_channel();
            registry.register(conn_id, TrackedConnection { sender: tx, noloop });
            receivers.push(rx);
        }
        (registry, receivers)
    }

    #[test]
    fn test_record_read_and_invalidate() {
        let (registry, mut rxs) = make_registry_with(vec![(1, false)]);
        let mut table = TrackingTable::new(1000);

        // Record a read
        table.record_read(b"foo", 1, &registry);
        assert!(
            table
                .key_to_clients
                .contains_key(&Bytes::from_static(b"foo"))
        );

        // Write invalidates
        table.invalidate_keys(&[b"foo"], 2, &registry);
        assert!(
            !table
                .key_to_clients
                .contains_key(&Bytes::from_static(b"foo"))
        );

        // Connection 1 should receive the invalidation
        let msg = rxs[0].try_recv().unwrap();
        match msg {
            InvalidationMessage::Keys(keys) => {
                assert_eq!(keys, vec![Bytes::from_static(b"foo")]);
            }
            _ => panic!("Expected Keys message"),
        }
    }

    #[test]
    fn test_noloop_skips_writer() {
        let (registry, mut rxs) = make_registry_with(vec![(1, true)]);
        let mut table = TrackingTable::new(1000);

        table.record_read(b"foo", 1, &registry);

        // Writer is conn 1, which has noloop=true → should be skipped
        table.invalidate_keys(&[b"foo"], 1, &registry);

        // No message should be received
        assert!(rxs[0].try_recv().is_err());
    }

    #[test]
    fn test_noloop_includes_other_readers() {
        let (registry, mut rxs) = make_registry_with(vec![(1, true), (2, false)]);
        let mut table = TrackingTable::new(1000);

        table.record_read(b"foo", 1, &registry);
        table.record_read(b"foo", 2, &registry);

        // Writer is conn 1 (noloop=true) → conn 1 skipped, conn 2 receives
        table.invalidate_keys(&[b"foo"], 1, &registry);

        // Conn 1 should NOT receive (noloop)
        assert!(rxs[0].try_recv().is_err());

        // Conn 2 SHOULD receive
        let msg = rxs[1].try_recv().unwrap();
        assert!(matches!(msg, InvalidationMessage::Keys(_)));
    }

    #[test]
    fn test_lru_eviction() {
        let (registry, mut rxs) = make_registry_with(vec![(1, false)]);
        let mut table = TrackingTable::new(2); // Max 2 keys

        table.record_read(b"a", 1, &registry);
        table.record_read(b"b", 1, &registry);
        assert_eq!(table.key_to_clients.len(), 2);

        // Adding a third key should evict "a" (oldest)
        table.record_read(b"c", 1, &registry);
        assert_eq!(table.key_to_clients.len(), 2);
        assert!(!table.key_to_clients.contains_key(&Bytes::from_static(b"a")));
        assert!(table.key_to_clients.contains_key(&Bytes::from_static(b"b")));
        assert!(table.key_to_clients.contains_key(&Bytes::from_static(b"c")));

        // Connection should have received invalidation for evicted key "a"
        let msg = rxs[0].try_recv().unwrap();
        match msg {
            InvalidationMessage::Keys(keys) => {
                assert_eq!(keys, vec![Bytes::from_static(b"a")]);
            }
            _ => panic!("Expected Keys message for evicted key"),
        }
    }

    #[test]
    fn test_remove_connection() {
        let (registry, _rxs) = make_registry_with(vec![(1, false), (2, false)]);
        let mut table = TrackingTable::new(1000);

        table.record_read(b"foo", 1, &registry);
        table.record_read(b"foo", 2, &registry);
        table.record_read(b"bar", 1, &registry);

        table.remove_connection(1);

        // "foo" should still exist (conn 2 is interested)
        assert!(
            table
                .key_to_clients
                .contains_key(&Bytes::from_static(b"foo"))
        );
        assert_eq!(table.key_to_clients[&Bytes::from_static(b"foo")].len(), 1);

        // "bar" should be removed (only conn 1 was interested)
        assert!(
            !table
                .key_to_clients
                .contains_key(&Bytes::from_static(b"bar"))
        );

        // Reverse index for conn 1 should be gone
        assert!(!table.client_to_keys.contains_key(&1));
    }

    #[test]
    fn test_duplicate_reads_idempotent() {
        let (registry, _rxs) = make_registry_with(vec![(1, false)]);
        let mut table = TrackingTable::new(1000);

        table.record_read(b"foo", 1, &registry);
        table.record_read(b"foo", 1, &registry);

        // Should only have one entry in the interest set
        assert_eq!(table.key_to_clients[&Bytes::from_static(b"foo")].len(), 1);
    }

    #[test]
    fn test_flush_all() {
        let (registry, mut rxs) = make_registry_with(vec![(1, false), (2, false)]);
        let mut table = TrackingTable::new(1000);

        table.record_read(b"foo", 1, &registry);
        table.record_read(b"bar", 2, &registry);

        table.flush_all(&registry);

        // Table should be empty
        assert!(table.key_to_clients.is_empty());
        assert!(table.client_to_keys.is_empty());
        assert!(table.lru_order.is_empty());

        // Both connections should receive FlushAll
        assert!(matches!(
            rxs[0].try_recv().unwrap(),
            InvalidationMessage::FlushAll
        ));
        assert!(matches!(
            rxs[1].try_recv().unwrap(),
            InvalidationMessage::FlushAll
        ));
    }

    #[test]
    fn test_closed_sender_no_panic() {
        let (registry, rxs) = make_registry_with(vec![(1, false)]);
        let mut table = TrackingTable::new(1000);

        table.record_read(b"foo", 1, &registry);

        // Drop the receiver — send should fail silently
        drop(rxs);

        // Should not panic
        table.invalidate_keys(&[b"foo"], 2, &registry);
        table.flush_all(&registry);
    }

    #[test]
    fn test_unregistered_connection_not_recorded() {
        let (registry, _rxs) = make_registry_with(vec![(1, false)]);
        let mut table = TrackingTable::new(1000);

        // Conn 99 is not registered — record_read should be a no-op
        table.record_read(b"foo", 99, &registry);
        assert!(table.key_to_clients.is_empty());
    }

    #[test]
    fn test_invalidate_nonexistent_key() {
        let (registry, mut rxs) = make_registry_with(vec![(1, false)]);
        let mut table = TrackingTable::new(1000);

        // Invalidating a key that's not tracked should be a no-op
        table.invalidate_keys(&[b"nonexistent"], 2, &registry);
        assert!(rxs[0].try_recv().is_err());
    }

    #[test]
    fn test_registry_operations() {
        let mut registry = InvalidationRegistry::default();
        let (tx, _rx) = mpsc::unbounded_channel();

        assert!(registry.is_empty());
        assert!(!registry.contains(&1));

        registry.register(
            1,
            TrackedConnection {
                sender: tx,
                noloop: false,
            },
        );
        assert!(!registry.is_empty());
        assert!(registry.contains(&1));
        assert!(registry.get(&1).is_some());

        registry.unregister(1);
        assert!(registry.is_empty());
        assert!(!registry.contains(&1));
    }

    // =========================================================================
    // BroadcastTable tests
    // =========================================================================

    #[test]
    fn test_broadcast_register_and_match() {
        let (registry, mut rxs) = make_registry_with(vec![(1, false)]);
        let mut bcast = BroadcastTable::default();

        bcast.register(1, &[Bytes::from_static(b"user:")]);

        // Key matching prefix should trigger invalidation
        bcast.invalidate_matching(&[b"user:123"], 2, &registry);
        let msg = rxs[0].try_recv().unwrap();
        match msg {
            InvalidationMessage::Keys(keys) => {
                assert_eq!(keys, vec![Bytes::from_static(b"user:123")]);
            }
            _ => panic!("Expected Keys message"),
        }

        // Key NOT matching prefix should not trigger
        bcast.invalidate_matching(&[b"order:456"], 2, &registry);
        assert!(rxs[0].try_recv().is_err());
    }

    #[test]
    fn test_broadcast_empty_prefix_matches_all() {
        let (registry, mut rxs) = make_registry_with(vec![(1, false)]);
        let mut bcast = BroadcastTable::default();

        // Empty prefixes = match all keys
        bcast.register(1, &[]);

        bcast.invalidate_matching(&[b"anything"], 2, &registry);
        let msg = rxs[0].try_recv().unwrap();
        assert!(matches!(msg, InvalidationMessage::Keys(_)));

        bcast.invalidate_matching(&[b"something:else"], 2, &registry);
        let msg = rxs[0].try_recv().unwrap();
        assert!(matches!(msg, InvalidationMessage::Keys(_)));
    }

    #[test]
    fn test_broadcast_multiple_prefixes() {
        let (registry, mut rxs) = make_registry_with(vec![(1, false)]);
        let mut bcast = BroadcastTable::default();

        bcast.register(
            1,
            &[Bytes::from_static(b"user:"), Bytes::from_static(b"order:")],
        );

        // Both prefixes should match
        bcast.invalidate_matching(&[b"user:1"], 2, &registry);
        assert!(rxs[0].try_recv().is_ok());

        bcast.invalidate_matching(&[b"order:2"], 2, &registry);
        assert!(rxs[0].try_recv().is_ok());

        // Non-matching should not
        bcast.invalidate_matching(&[b"product:3"], 2, &registry);
        assert!(rxs[0].try_recv().is_err());
    }

    #[test]
    fn test_broadcast_remove_connection() {
        let (registry, mut rxs) = make_registry_with(vec![(1, false)]);
        let mut bcast = BroadcastTable::default();

        bcast.register(1, &[Bytes::from_static(b"foo:")]);
        assert!(!bcast.is_empty());

        bcast.remove_connection(1);
        assert!(bcast.is_empty());

        // Should not trigger after removal
        bcast.invalidate_matching(&[b"foo:bar"], 2, &registry);
        assert!(rxs[0].try_recv().is_err());
    }

    #[test]
    fn test_broadcast_noloop() {
        let (registry, mut rxs) = make_registry_with(vec![(1, true)]); // noloop=true
        let mut bcast = BroadcastTable::default();

        bcast.register(1, &[Bytes::from_static(b"key:")]);

        // Writer is conn 1 with noloop — should be skipped
        bcast.invalidate_matching(&[b"key:abc"], 1, &registry);
        assert!(rxs[0].try_recv().is_err());

        // Different writer — should receive
        bcast.invalidate_matching(&[b"key:abc"], 2, &registry);
        assert!(rxs[0].try_recv().is_ok());
    }

    #[test]
    fn test_broadcast_multiple_connections() {
        let (registry, mut rxs) = make_registry_with(vec![(1, false), (2, false)]);
        let mut bcast = BroadcastTable::default();

        bcast.register(1, &[Bytes::from_static(b"shared:")]);
        bcast.register(2, &[Bytes::from_static(b"shared:")]);

        bcast.invalidate_matching(&[b"shared:key"], 99, &registry);

        // Both should receive
        assert!(rxs[0].try_recv().is_ok());
        assert!(rxs[1].try_recv().is_ok());
    }
}
