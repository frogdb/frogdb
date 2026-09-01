//! Client-side caching invalidation infrastructure for FrogDB.
//!
//! This module provides the server-side tracking needed for Redis-compatible
//! `CLIENT TRACKING` support:
//! - [`InvalidationRegistry`] — per-shard registry of connections with tracking enabled
//! - [`TrackingTable`] — per-shard mapping from keys to interested connections
//! - [`InvalidationMessage`] — messages sent to connections when tracked keys change

use std::collections::{HashMap, HashSet, VecDeque};

use bytes::Bytes;
use frogdb_memory::{Budget, Charge};
use tokio::sync::mpsc;

use crate::pubsub::ConnId;

/// Default maximum number of tracked keys per shard (1 million).
///
/// This is a **ceiling**, not a budget, in the sense
/// [`specs/memory.md`](../../../../specs/memory.md) gives those words: a
/// constant bound on one quantity that exists for Redis compatibility
/// (`tracking-table-max-keys`). The live allowance that decides whether the
/// table may actually grow is the `ClientTracking` [`Budget`]; see
/// [`TrackingTable`].
pub const DEFAULT_TRACKING_TABLE_MAX_KEYS: usize = 1_000_000;

/// Rough per-entry hash-map overhead, in bytes. Shared by every charge and
/// release site so the table's charge and its recomputed footprint agree.
const ENTRY_OVERHEAD: usize = 64;

/// A `key_to_clients` entry holding no connections yet.
const fn key_entry_cost(key_len: usize) -> usize {
    key_len + ENTRY_OVERHEAD
}

/// One connection id inside a key's interest set.
const fn conn_ref_cost() -> usize {
    std::mem::size_of::<ConnId>()
}

/// A `client_to_keys` entry holding no keys yet.
const fn client_entry_cost() -> usize {
    ENTRY_OVERHEAD
}

/// One key inside a connection's reverse-index set.
const fn client_key_cost(key_len: usize) -> usize {
    key_len
}

/// One `lru_order` slot.
const fn lru_entry_cost(key_len: usize) -> usize {
    key_len + std::mem::size_of::<Bytes>()
}

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
///
/// # Bounded by a budget, capped by a ceiling
///
/// Two independent bounds apply, and they are different kinds of thing
/// ([`specs/memory.md`](../../../../specs/memory.md), "ceiling vs budget"):
///
/// * `max_keys` is the **ceiling** — Redis's `tracking-table-max-keys`, a
///   constant on one quantity, kept for compatibility. It says nothing about
///   what this node can afford.
/// * The `ClientTracking` [`Budget`] is the **budget** — the live byte
///   allowance this shard's broker issued. Every growth of this table is
///   charged against it *before* the bytes exist, which is the invariant
///   [adr/0006](../../../../adr/0006-memory-architecture-seams.md) §2 rules
///   and the reason the `lru_order` unbounded-growth class
///   (round-2 issue 66) cannot come back: `lru_order` is charged like every
///   other field, so growing it outside accounting is not something this type
///   can express.
///
/// # Disposition: shed
///
/// A refused charge sheds — the table evicts its oldest tracked key, sending
/// that key's clients an invalidation exactly as an over-capacity eviction
/// does, and retries. Shedding a tracked key is safe by construction: a client
/// that receives an invalidation drops its cached copy, so the worst outcome
/// is a cache miss. If the table is empty and the entry still does not fit —
/// a budget smaller than a single key's footprint — the read is not recorded
/// at all and [`TrackingTable::budget_declines`] counts it.
///
/// # Accounting
///
/// [`TrackingTable::memory_usage`] reads the charge, so it is O(1) and is by
/// construction the same number the broker's breakdown shows. The O(n)
/// recomputation it replaced survives as the test-only ground truth
/// `recomputed_memory_usage`, which a reconciliation test asserts the
/// incremental accounting against.
#[derive(Debug)]
pub struct TrackingTable {
    /// key → set of interested connection IDs.
    key_to_clients: HashMap<Bytes, HashSet<ConnId>>,
    /// Reverse index: conn_id → set of tracked keys (for O(1) connection cleanup).
    client_to_keys: HashMap<ConnId, HashSet<Bytes>>,
    /// LRU eviction order (front = oldest). May contain stale entries — keys
    /// already removed from `key_to_clients` by `invalidate_keys` or
    /// `remove_connection` — until the next compaction.
    lru_order: VecDeque<Bytes>,
    /// Number of `lru_order` entries known to be stale since the last compaction.
    stale_count: usize,
    /// Maximum number of tracked keys — the Redis-compatibility ceiling.
    max_keys: usize,
    /// The shard's `ClientTracking` allowance. Every field above grows only
    /// after this charge does.
    charge: Charge,
    /// Keys shed to satisfy a refused charge, since the last drain.
    budget_evictions: u64,
    /// Reads not recorded because an empty table still could not fit them,
    /// since the last drain.
    budget_declines: u64,
}

impl TrackingTable {
    /// Create a new tracking table with the given key ceiling, charging its
    /// growth against `budget`.
    pub fn new(max_keys: usize, budget: &Budget) -> Self {
        Self {
            key_to_clients: HashMap::new(),
            client_to_keys: HashMap::new(),
            lru_order: VecDeque::new(),
            stale_count: 0,
            max_keys,
            charge: budget.open_charge(),
            budget_evictions: 0,
            budget_declines: 0,
        }
    }

    /// Record that a key was removed from `key_to_clients`, leaving a stale
    /// entry behind in `lru_order`. Compacts once stale entries outnumber live
    /// ones, keeping `lru_order` bounded by the live keyspace even under a
    /// read-then-invalidate workload that never trips `evict_lru`.
    fn mark_stale(&mut self) {
        self.stale_count += 1;
        if self.stale_count > self.key_to_clients.len() {
            self.compact_lru();
        }
    }

    /// Drop every stale (no-longer-live) entry from `lru_order`.
    fn compact_lru(&mut self) {
        let key_to_clients = &self.key_to_clients;
        let mut freed = 0usize;
        self.lru_order.retain(|key| {
            if key_to_clients.contains_key(key) {
                true
            } else {
                freed += lru_entry_cost(key.len());
                false
            }
        });
        self.stale_count = 0;
        self.charge.shrink(freed as u64);
    }

    /// Number of entries currently in `lru_order`, stale entries included.
    /// Exposed so tests can assert the LRU stays bounded independent of
    /// `key_to_clients`.
    #[cfg(test)]
    pub(crate) fn lru_len(&self) -> usize {
        self.lru_order.len()
    }

    /// Heap footprint of this table, for `MEMORY STATS`/`INFO` accounting.
    ///
    /// This is the table's charge against the `ClientTracking` budget, so the
    /// operator's memory figure and the broker's breakdown cannot disagree.
    /// It is an estimate of the heap cost of the entries — a per-entry
    /// overhead constant, not allocator truth — but it is the *same* estimate
    /// the budget enforces, and it includes stale `lru_order` entries pending
    /// compaction.
    pub(crate) fn memory_usage(&self) -> usize {
        self.charge.bytes() as usize
    }

    /// The O(n) ground truth [`TrackingTable::memory_usage`]'s incremental
    /// accounting must equal. Test-only: this is what the charge is checked
    /// against, not what production reads.
    #[cfg(test)]
    pub(crate) fn recomputed_memory_usage(&self) -> usize {
        let key_to_clients: usize = self
            .key_to_clients
            .iter()
            .map(|(k, v)| key_entry_cost(k.len()) + v.len() * conn_ref_cost())
            .sum();
        let client_to_keys: usize = self
            .client_to_keys
            .values()
            .map(|keys| {
                client_entry_cost() + keys.iter().map(|k| client_key_cost(k.len())).sum::<usize>()
            })
            .sum();
        let lru_order: usize = self
            .lru_order
            .iter()
            .map(|key| lru_entry_cost(key.len()))
            .sum();
        key_to_clients + client_to_keys + lru_order
    }

    /// Keys shed because the budget refused a charge, and reads declined
    /// outright, since the last call. Draining rather than reporting an
    /// absolute lets the caller feed a Prometheus counter with `inc_by`
    /// without holding a last-emitted snapshot of its own.
    pub(crate) fn drain_budget_shed_counters(&mut self) -> (u64, u64) {
        (
            std::mem::take(&mut self.budget_evictions),
            std::mem::take(&mut self.budget_declines),
        )
    }

    /// Bytes this table would have to charge to record `conn_id`'s read of
    /// `key`. Zero when the read adds nothing (an idempotent re-read).
    fn insertion_cost(&self, key: &Bytes, conn_id: ConnId) -> usize {
        let clients = self.key_to_clients.get(key);
        let is_new_key = clients.is_none();
        let conn_new_for_key = clients.is_none_or(|set| !set.contains(&conn_id));

        let tracked = self.client_to_keys.get(&conn_id);
        let client_entry_new = tracked.is_none();
        let key_new_for_client = tracked.is_none_or(|set| !set.contains(key));

        let mut cost = 0;
        if is_new_key {
            // A new key costs its `key_to_clients` entry *and* its `lru_order`
            // slot. Charging the LRU slot here is what makes the issue-66
            // growth class unrepresentable.
            cost += key_entry_cost(key.len()) + lru_entry_cost(key.len());
        }
        if conn_new_for_key {
            cost += conn_ref_cost();
        }
        if client_entry_new {
            cost += client_entry_cost();
        }
        if key_new_for_client {
            cost += client_key_cost(key.len());
        }
        cost
    }

    /// Record that `conn_id` read this key.
    ///
    /// Charges the `ClientTracking` budget before inserting anything. On a
    /// refusal the table sheds — evicts its oldest tracked key, invalidating
    /// that key's clients — and retries, until either the charge succeeds or
    /// there is nothing left to shed.
    pub fn record_read(&mut self, key: &[u8], conn_id: ConnId, registry: &InvalidationRegistry) {
        // Only record if the connection is registered for tracking
        if !registry.contains(&conn_id) {
            return;
        }

        let key_bytes = Bytes::copy_from_slice(key);

        loop {
            // Recomputed each pass: shedding removes entries, so a charge that
            // was refused may need *more* bytes on the retry, never fewer.
            let cost = self.insertion_cost(&key_bytes, conn_id) as u64;
            if cost == 0 {
                return; // idempotent re-read: nothing grows, nothing to charge
            }
            match self.charge.grow(cost) {
                Ok(()) => break,
                Err(_refused) => {
                    // Declared disposition: shed.
                    if self.evict_lru(registry) {
                        self.budget_evictions += 1;
                    } else {
                        // Nothing left to shed and it still does not fit: the
                        // whole budget is smaller than one entry. Refuse to
                        // grow rather than charge anyway.
                        self.budget_declines += 1;
                        return;
                    }
                }
            }
        }

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

            // Evict if over the Redis-compatibility key ceiling
            while self.key_to_clients.len() > self.max_keys {
                self.evict_lru(registry);
            }
        }
    }

    /// Remove `key`'s entry from `key_to_clients` and the reverse index,
    /// releasing the charge for everything removed. Returns the connections
    /// that were interested, or `None` if the key was not live.
    ///
    /// Deliberately does **not** touch `lru_order` or `stale_count`: the two
    /// callers disagree about that half (an eviction has already popped the
    /// LRU slot; an invalidation leaves it behind as stale).
    fn take_key_entry(&mut self, key: &Bytes) -> Option<HashSet<ConnId>> {
        let conn_ids = self.key_to_clients.remove(key)?;
        let mut freed = key_entry_cost(key.len()) + conn_ids.len() * conn_ref_cost();
        for cid in &conn_ids {
            if let Some(keys_set) = self.client_to_keys.get_mut(cid) {
                if keys_set.remove(key) {
                    freed += client_key_cost(key.len());
                }
                if keys_set.is_empty() {
                    self.client_to_keys.remove(cid);
                    freed += client_entry_cost();
                }
            }
        }
        self.charge.shrink(freed as u64);
        Some(conn_ids)
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
            // `take_key_entry` releases the charge for the entry and its
            // reverse-index rows; the key's `lru_order` slot stays behind as a
            // stale entry (still charged) until `compact_lru` reclaims it.
            if let Some(conn_ids) = self.take_key_entry(&key_bytes) {
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

                self.mark_stale();
            }
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
        self.stale_count = 0;
        // Everything the table held is gone, so the whole charge goes back.
        self.charge.shrink(self.charge.bytes());
    }

    /// Remove all tracking entries for a disconnected connection.
    pub fn remove_connection(&mut self, conn_id: ConnId) {
        // Use the reverse index for O(1) cleanup
        if let Some(keys) = self.client_to_keys.remove(&conn_id) {
            let mut freed = client_entry_cost();
            for key in keys {
                freed += client_key_cost(key.len());
                if let Some(clients) = self.key_to_clients.get_mut(&key) {
                    if clients.remove(&conn_id) {
                        freed += conn_ref_cost();
                    }
                    if clients.is_empty() {
                        self.key_to_clients.remove(&key);
                        freed += key_entry_cost(key.len());
                        // `mark_stale` may compact, which shrinks the charge
                        // for the LRU slots it drops — a disjoint quantity
                        // from the `freed` running total.
                        self.mark_stale();
                    }
                }
            }
            self.charge.shrink(freed as u64);
        }
    }

    /// Evict the oldest key from the LRU, sending invalidation to interested
    /// clients. Returns whether a live key was actually evicted — `false`
    /// means the table had nothing left to shed.
    fn evict_lru(&mut self, registry: &InvalidationRegistry) -> bool {
        while let Some(key) = self.lru_order.pop_front() {
            self.charge.shrink(lru_entry_cost(key.len()) as u64);
            // Skip stale entries (already removed by invalidate_keys or remove_connection)
            if let Some(conn_ids) = self.take_key_entry(&key) {
                // Send invalidation to all interested clients
                for &cid in &conn_ids {
                    if let Some(tracked) = registry.get(&cid) {
                        let _ = tracked
                            .sender
                            .send(InvalidationMessage::Keys(vec![key.clone()]));
                    }
                }
                return true; // Evicted one real key
            }
            // Key wasn't in key_to_clients — it was already stale (invalidated
            // or its owning connection removed). Account for it and continue.
            self.stale_count = self.stale_count.saturating_sub(1);
        }
        false
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
    use frogdb_memory::{Disposition, Subsystem};

    use super::*;

    /// A table whose budget is effectively unbounded, for tests about the
    /// `max_keys` ceiling rather than the budget.
    fn test_table(max_keys: usize) -> TrackingTable {
        budgeted_table(max_keys, u64::MAX).0
    }

    /// A table plus the budget backing it, for tests that drive the budget.
    fn budgeted_table(max_keys: usize, limit_bytes: u64) -> (TrackingTable, Budget) {
        let budget = Budget::new(Subsystem::ClientTracking, Disposition::Shed, limit_bytes);
        (TrackingTable::new(max_keys, &budget), budget)
    }

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
        let mut table = test_table(1000);

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
        let mut table = test_table(1000);

        table.record_read(b"foo", 1, &registry);

        // Writer is conn 1, which has noloop=true → should be skipped
        table.invalidate_keys(&[b"foo"], 1, &registry);

        // No message should be received
        assert!(rxs[0].try_recv().is_err());
    }

    #[test]
    fn test_noloop_includes_other_readers() {
        let (registry, mut rxs) = make_registry_with(vec![(1, true), (2, false)]);
        let mut table = test_table(1000);

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
        let mut table = test_table(2); // Max 2 keys

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
    fn test_evict_lru_skips_stale_entries() {
        let (registry, mut rxs) = make_registry_with(vec![(1, false)]);
        let mut table = test_table(3); // Max 3 keys

        table.record_read(b"a", 1, &registry);
        table.record_read(b"b", 1, &registry);
        table.record_read(b"c", 1, &registry);
        assert_eq!(table.key_to_clients.len(), 3);

        // Invalidate "a" — leaves a stale lru_order entry ahead of "b" and
        // "c". Live count (2) still covers the 1 stale entry, so no
        // compaction fires and the stale entry lingers, exactly the
        // situation evict_lru must skip over.
        table.invalidate_keys(&[b"a"], 2, &registry);
        let _ = rxs[0].try_recv(); // drain "a"'s invalidation
        assert_eq!(table.key_to_clients.len(), 2);

        // Grow past capacity with two more distinct keys so evict_lru runs.
        table.record_read(b"d", 1, &registry);
        table.record_read(b"e", 1, &registry);

        // evict_lru must have popped the stale "a" entry without evicting
        // anything for it, then evicted "b" — the oldest *live* key.
        assert_eq!(table.key_to_clients.len(), 3);
        assert!(!table.key_to_clients.contains_key(&Bytes::from_static(b"a")));
        assert!(!table.key_to_clients.contains_key(&Bytes::from_static(b"b")));
        assert!(table.key_to_clients.contains_key(&Bytes::from_static(b"c")));
        assert!(table.key_to_clients.contains_key(&Bytes::from_static(b"d")));
        assert!(table.key_to_clients.contains_key(&Bytes::from_static(b"e")));

        let msg = rxs[0].try_recv().unwrap();
        match msg {
            InvalidationMessage::Keys(keys) => {
                assert_eq!(keys, vec![Bytes::from_static(b"b")]);
            }
            _ => panic!("Expected Keys message for evicted key"),
        }
    }

    #[test]
    fn test_lru_order_bounded_after_invalidate() {
        let (registry, _rxs) = make_registry_with(vec![(1, false)]);
        let mut table = test_table(DEFAULT_TRACKING_TABLE_MAX_KEYS);

        for i in 0..10_000u32 {
            let key = i.to_be_bytes();
            table.record_read(&key, 1, &registry);
            table.invalidate_keys(&[&key], 2, &registry);
        }

        // A read-then-invalidate workload never touches evict_lru (no key
        // ever accumulates past max_keys), so lru_order must be bounded by
        // compaction alone — O(live keys), not O(iterations = 10_000).
        assert!(
            table.lru_len() < 100,
            "lru_order grew unbounded: {} entries after 10_000 read+invalidate cycles",
            table.lru_len()
        );
    }

    #[test]
    fn test_lru_order_bounded_after_remove_connection() {
        let (registry, _rxs) = make_registry_with(vec![(1, false)]);
        let mut table = test_table(DEFAULT_TRACKING_TABLE_MAX_KEYS);

        for i in 0..10_000u32 {
            let key = i.to_be_bytes();
            table.record_read(&key, 1, &registry);
            table.remove_connection(1);
        }

        assert!(
            table.lru_len() < 100,
            "lru_order grew unbounded: {} entries after 10_000 read+remove_connection cycles",
            table.lru_len()
        );
    }

    #[test]
    fn test_remove_connection() {
        let (registry, _rxs) = make_registry_with(vec![(1, false), (2, false)]);
        let mut table = test_table(1000);

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
        let mut table = test_table(1000);

        table.record_read(b"foo", 1, &registry);
        table.record_read(b"foo", 1, &registry);

        // Should only have one entry in the interest set
        assert_eq!(table.key_to_clients[&Bytes::from_static(b"foo")].len(), 1);
    }

    #[test]
    fn test_flush_all() {
        let (registry, mut rxs) = make_registry_with(vec![(1, false), (2, false)]);
        let mut table = test_table(1000);

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
        let mut table = test_table(1000);

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
        let mut table = test_table(1000);

        // Conn 99 is not registered — record_read should be a no-op
        table.record_read(b"foo", 99, &registry);
        assert!(table.key_to_clients.is_empty());
    }

    #[test]
    fn test_invalidate_nonexistent_key() {
        let (registry, mut rxs) = make_registry_with(vec![(1, false)]);
        let mut table = test_table(1000);

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

    fn tracked(table: &TrackingTable, key: &[u8]) -> bool {
        table
            .key_to_clients
            .contains_key(&Bytes::copy_from_slice(key))
    }

    /// Bytes one connection's reads of `keys` charge, measured rather than
    /// derived, so the budget tests below do not re-implement the cost model
    /// they are checking.
    fn charge_for(keys: &[&[u8]]) -> u64 {
        let (registry, _rxs) = make_registry_with(vec![(1, false)]);
        let mut probe = test_table(1000);
        for key in keys {
            probe.record_read(key, 1, &registry);
        }
        probe.memory_usage() as u64
    }

    /// The declared disposition is *shed*: over budget, the oldest tracked key
    /// leaves the table and its clients are invalidated, exactly as an
    /// over-ceiling eviction does. The new read is still recorded.
    #[test]
    fn budget_refusal_sheds_the_oldest_key() {
        let (registry, mut rxs) = make_registry_with(vec![(1, false)]);
        // Room for two keys, not three.
        let (mut table, budget) = budgeted_table(1000, charge_for(&[b"k:a", b"k:b"]));

        table.record_read(b"k:a", 1, &registry);
        table.record_read(b"k:b", 1, &registry);
        assert_eq!(budget.refusals(), 0, "two keys fit without a refusal");

        table.record_read(b"k:c", 1, &registry);

        assert!(budget.refusals() > 0, "the third key must be refused first");
        assert!(!tracked(&table, b"k:a"), "oldest key shed");
        assert!(tracked(&table, b"k:b"));
        assert!(tracked(&table, b"k:c"), "the new read is still recorded");
        assert!(table.memory_usage() as u64 <= budget.limit());

        // Shedding delivers an invalidation, so the client drops its stale copy.
        let msg = rxs[0].try_recv().expect("shed key invalidates its clients");
        match msg {
            InvalidationMessage::Keys(keys) => {
                assert_eq!(keys, vec![Bytes::from_static(b"k:a")]);
            }
            other => panic!("unexpected invalidation: {other:?}"),
        }

        // The metric the shard emits: one shed, no decline.
        assert_eq!(table.drain_budget_shed_counters(), (1, 0));
        assert_eq!(
            table.drain_budget_shed_counters(),
            (0, 0),
            "drained counters are increments, not absolutes"
        );
    }

    /// A budget smaller than a single entry cannot be satisfied by shedding.
    /// The read is declined rather than charged anyway.
    #[test]
    fn budget_too_small_for_one_entry_declines_the_read() {
        let (registry, _rxs) = make_registry_with(vec![(1, false)]);
        let (mut table, _budget) = budgeted_table(1000, 8);

        table.record_read(b"k:a", 1, &registry);

        assert!(!tracked(&table, b"k:a"));
        assert_eq!(table.memory_usage(), 0, "nothing was charged");
        assert_eq!(table.drain_budget_shed_counters(), (0, 1));
    }

    /// The incremental charge must equal the O(n) recomputation after every
    /// mutation path: read, invalidate, evict, compact, disconnect, flush.
    #[test]
    fn charge_reconciles_with_recomputed_usage() {
        let (registry, _rxs) = make_registry_with(vec![(1, false), (2, false)]);
        let mut table = test_table(3);

        let check = |t: &TrackingTable, what: &str| {
            assert_eq!(
                t.memory_usage(),
                t.recomputed_memory_usage(),
                "charge drifted from the recomputed footprint after {what}"
            );
        };

        for key in [b"k:1".as_slice(), b"k:2", b"k:3"] {
            table.record_read(key, 1, &registry);
            table.record_read(key, 2, &registry);
        }
        check(&table, "reads");

        // Over the ceiling: evicts the oldest.
        table.record_read(b"k:4", 1, &registry);
        check(&table, "ceiling eviction");

        table.invalidate_keys(&[b"k:2"], 9, &registry);
        check(&table, "invalidate (stale lru entry left behind)");

        // Force compaction by staling out the rest.
        table.invalidate_keys(&[b"k:3", b"k:4"], 9, &registry);
        check(&table, "compaction");

        table.record_read(b"k:5", 1, &registry);
        table.record_read(b"k:5", 2, &registry);
        table.remove_connection(2);
        check(&table, "connection removal");

        table.flush_all(&registry);
        check(&table, "flush");
        assert_eq!(table.memory_usage(), 0, "flush releases the whole charge");
    }

    /// Growth that the budget did not authorize is the bug class R8 exists to
    /// kill: every field the table owns is charged, `lru_order` included.
    #[test]
    fn lru_entries_are_charged_like_every_other_field() {
        let (registry, _rxs) = make_registry_with(vec![(1, false)]);
        let mut table = test_table(1000);

        // Two keys, so invalidating one does not immediately trip compaction
        // (`stale_count > live keys`) and the stale slot is observable.
        table.record_read(b"k:a", 1, &registry);
        table.record_read(b"k:b", 1, &registry);
        let with_both_live = table.memory_usage();

        // Invalidation leaves a stale LRU slot behind; it is still charged.
        table.invalidate_keys(&[b"k:a"], 9, &registry);
        assert_eq!(table.lru_len(), 2, "the shed key's LRU slot is still there");
        assert!(table.memory_usage() < with_both_live);
        assert_eq!(table.memory_usage(), table.recomputed_memory_usage());

        // Compacting it away releases exactly that slot's charge.
        let before_compaction = table.memory_usage();
        table.compact_lru();
        assert_eq!(table.lru_len(), 1);
        assert_eq!(table.memory_usage(), before_compaction - lru_entry_cost(3));
        assert_eq!(table.memory_usage(), table.recomputed_memory_usage());
    }
}
