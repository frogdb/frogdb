use std::collections::{HashMap, VecDeque};
use std::time::Instant;

use bytes::Bytes;
use frogdb_protocol::Response;
use tokio::sync::oneshot;

use crate::types::BlockingOp;

/// Entry in the wait queue for blocking commands.
pub struct WaitEntry {
    /// Connection ID of the blocked client.
    pub conn_id: u64,
    /// Keys the client is waiting on.
    pub keys: Vec<Bytes>,
    /// The blocking operation type.
    pub op: BlockingOp,
    /// Channel to send the response when data is available.
    pub response_tx: oneshot::Sender<Response>,
    /// Deadline for the blocking operation (None = indefinite).
    pub deadline: Option<Instant>,
}

impl std::fmt::Debug for WaitEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WaitEntry")
            .field("conn_id", &self.conn_id)
            .field("keys", &self.keys)
            .field("op", &self.op)
            .field("deadline", &self.deadline)
            .finish()
    }
}

/// Per-shard wait queue for blocked connections.
///
/// Maintains FIFO ordering per key - when a key gets data, the oldest
/// waiter for that key is satisfied first.
#[derive(Default)]
pub struct ShardWaitQueue {
    /// Waiters indexed by key. Each key maps to a list of entry indices (FIFO).
    waiters_by_key: HashMap<Bytes, VecDeque<usize>>,
    /// All wait entries indexed for O(1) access.
    entries: Vec<Option<WaitEntry>>,
    /// Free list of entry slot indices for reuse.
    free_slots: Vec<usize>,
    /// Index from conn_id to entry indices (for cleanup on disconnect).
    conn_entries: HashMap<u64, Vec<usize>>,
    /// Current number of active waiters.
    waiter_count: usize,
    /// Maximum waiters per key (0 = unlimited).
    max_waiters_per_key: usize,
    /// Maximum total blocked connections (0 = unlimited).
    max_blocked_connections: usize,
}

impl ShardWaitQueue {
    /// Create a new wait queue with default limits.
    pub fn new() -> Self {
        Self::with_limits(10000, 50000)
    }

    /// Create a new wait queue with specific limits.
    pub fn with_limits(max_waiters_per_key: usize, max_blocked_connections: usize) -> Self {
        Self {
            waiters_by_key: HashMap::new(),
            entries: Vec::new(),
            free_slots: Vec::new(),
            conn_entries: HashMap::new(),
            waiter_count: 0,
            max_waiters_per_key,
            max_blocked_connections,
        }
    }

    /// Register a new waiter.
    ///
    /// Returns Ok(()) if registered, Err with message if limits exceeded.
    pub fn register(&mut self, entry: WaitEntry) -> Result<(), String> {
        // Check global limit
        if self.max_blocked_connections > 0 && self.waiter_count >= self.max_blocked_connections {
            return Err("ERR max blocked connections limit reached".to_string());
        }

        // Check per-key limits
        if self.max_waiters_per_key > 0 {
            for key in &entry.keys {
                if let Some(waiters) = self.waiters_by_key.get(key)
                    && waiters.len() >= self.max_waiters_per_key
                {
                    return Err("ERR max waiters per key limit reached".to_string());
                }
            }
        }

        let conn_id = entry.conn_id;
        let keys = entry.keys.clone();

        // Allocate a slot for the entry
        let slot_idx = if let Some(idx) = self.free_slots.pop() {
            self.entries[idx] = Some(entry);
            idx
        } else {
            let idx = self.entries.len();
            self.entries.push(Some(entry));
            idx
        };

        // Index by each key
        for key in &keys {
            self.waiters_by_key
                .entry(key.clone())
                .or_default()
                .push_back(slot_idx);
        }

        // Index by connection ID
        self.conn_entries.entry(conn_id).or_default().push(slot_idx);

        self.waiter_count += 1;
        Ok(())
    }

    /// Unregister all waiters for a connection (called on disconnect or timeout).
    ///
    /// Returns the entries that were removed.
    pub fn unregister(&mut self, conn_id: u64) -> Vec<WaitEntry> {
        let entry_indices = match self.conn_entries.remove(&conn_id) {
            Some(indices) => indices,
            None => return vec![],
        };

        let mut removed = Vec::new();

        for idx in entry_indices {
            if let Some(entry) = self.entries[idx].take() {
                // Remove from key index
                for key in &entry.keys {
                    if let Some(waiters) = self.waiters_by_key.get_mut(key) {
                        waiters.retain(|&i| i != idx);
                        if waiters.is_empty() {
                            self.waiters_by_key.remove(key);
                        }
                    }
                }

                self.free_slots.push(idx);
                self.waiter_count -= 1;
                removed.push(entry);
            }
        }

        removed
    }

    /// Pop the oldest waiter for a key.
    ///
    /// Returns the WaitEntry if one exists, None otherwise.
    pub fn pop_oldest_waiter(&mut self, key: &Bytes) -> Option<WaitEntry> {
        // First, find and extract the entry without holding any borrows
        let (idx, entry) = {
            let waiters = self.waiters_by_key.get_mut(key)?;

            loop {
                let idx = waiters.pop_front()?;
                if let Some(entry) = self.entries[idx].take() {
                    break (idx, entry);
                }
                // Entry was already removed, continue to next
            }
        };

        // Collect other keys to clean up (excluding the current key)
        let other_keys: Vec<Bytes> = entry.keys.iter().filter(|k| *k != key).cloned().collect();

        // Remove from all other key indices
        for k in &other_keys {
            if let Some(w) = self.waiters_by_key.get_mut(k) {
                w.retain(|&i| i != idx);
                if w.is_empty() {
                    self.waiters_by_key.remove(k);
                }
            }
        }

        // Remove from conn_entries
        if let Some(conn_entries) = self.conn_entries.get_mut(&entry.conn_id) {
            conn_entries.retain(|&i| i != idx);
            if conn_entries.is_empty() {
                self.conn_entries.remove(&entry.conn_id);
            }
        }

        self.free_slots.push(idx);
        self.waiter_count -= 1;

        // Clean up empty key entry for the primary key
        if let Some(waiters) = self.waiters_by_key.get(key)
            && waiters.is_empty()
        {
            self.waiters_by_key.remove(key);
        }

        Some(entry)
    }

    /// Collect all expired waiters (deadline has passed).
    ///
    /// Returns the expired WaitEntry objects.
    pub fn collect_expired(&mut self, now: Instant) -> Vec<WaitEntry> {
        let mut expired_indices = Vec::new();

        // Find all expired entries
        for (idx, entry) in self.entries.iter().enumerate() {
            if let Some(e) = entry
                && let Some(deadline) = e.deadline
                && deadline <= now
            {
                expired_indices.push(idx);
            }
        }

        let mut expired = Vec::new();

        // Remove expired entries
        for idx in expired_indices {
            if let Some(entry) = self.entries[idx].take() {
                // Remove from key index
                for key in &entry.keys {
                    if let Some(waiters) = self.waiters_by_key.get_mut(key) {
                        waiters.retain(|&i| i != idx);
                        if waiters.is_empty() {
                            self.waiters_by_key.remove(key);
                        }
                    }
                }

                // Remove from conn_entries
                if let Some(conn_entries) = self.conn_entries.get_mut(&entry.conn_id) {
                    conn_entries.retain(|&i| i != idx);
                    if conn_entries.is_empty() {
                        self.conn_entries.remove(&entry.conn_id);
                    }
                }

                self.free_slots.push(idx);
                self.waiter_count -= 1;
                expired.push(entry);
            }
        }

        expired
    }

    /// Drain all waiters whose keys belong to the given slot.
    ///
    /// Used when a slot migrates to another node — all blocked clients
    /// for keys in that slot must receive `-MOVED` responses.
    pub fn drain_waiters_for_slot(&mut self, slot: u16) -> Vec<WaitEntry> {
        // Collect keys that belong to this slot
        let matching_keys: Vec<Bytes> = self
            .waiters_by_key
            .keys()
            .filter(|key| super::helpers::slot_for_key(key) == slot)
            .cloned()
            .collect();

        // Collect unique entry indices to drain
        let mut indices_to_drain = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for key in &matching_keys {
            if let Some(waiters) = self.waiters_by_key.get(key) {
                for &idx in waiters {
                    if seen.insert(idx) {
                        indices_to_drain.push(idx);
                    }
                }
            }
        }

        let mut drained = Vec::new();

        for idx in indices_to_drain {
            if let Some(entry) = self.entries[idx].take() {
                // Remove from ALL key indices (entry may span multiple keys)
                for key in &entry.keys {
                    if let Some(waiters) = self.waiters_by_key.get_mut(key) {
                        waiters.retain(|&i| i != idx);
                        if waiters.is_empty() {
                            self.waiters_by_key.remove(key);
                        }
                    }
                }

                // Remove from conn_entries
                if let Some(conn_entries) = self.conn_entries.get_mut(&entry.conn_id) {
                    conn_entries.retain(|&i| i != idx);
                    if conn_entries.is_empty() {
                        self.conn_entries.remove(&entry.conn_id);
                    }
                }

                self.free_slots.push(idx);
                self.waiter_count -= 1;
                drained.push(entry);
            }
        }

        drained
    }

    /// Check if there are any waiters for a key.
    pub fn has_waiters(&self, key: &Bytes) -> bool {
        self.waiters_by_key
            .get(key)
            .map(|w| !w.is_empty())
            .unwrap_or(false)
    }

    /// Get the number of active waiters.
    pub fn waiter_count(&self) -> usize {
        self.waiter_count
    }

    /// Get the number of keys with waiters.
    pub fn blocked_keys_count(&self) -> usize {
        self.waiters_by_key.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::BlockingOp;

    fn make_entry(conn_id: u64, keys: Vec<&str>) -> WaitEntry {
        let (tx, _rx) = oneshot::channel();
        WaitEntry {
            conn_id,
            keys: keys
                .into_iter()
                .map(|k| Bytes::from(k.to_string()))
                .collect(),
            op: BlockingOp::BLPop,
            response_tx: tx,
            deadline: None,
        }
    }

    #[test]
    fn test_drain_empty_queue() {
        let mut queue = ShardWaitQueue::new();
        let drained = queue.drain_waiters_for_slot(100);
        assert!(drained.is_empty());
    }

    #[test]
    fn test_drain_matching_slot() {
        let mut queue = ShardWaitQueue::new();

        // "{slot0}" hashes to a known slot; we'll use slot_for_key to find it
        let key = b"{testslot}key1";
        let slot = super::super::helpers::slot_for_key(key);

        queue
            .register(make_entry(1, vec!["{testslot}key1"]))
            .unwrap();
        queue
            .register(make_entry(2, vec!["{testslot}key2"]))
            .unwrap();

        let drained = queue.drain_waiters_for_slot(slot);
        assert_eq!(drained.len(), 2);
        assert_eq!(queue.waiter_count(), 0);
        assert_eq!(queue.blocked_keys_count(), 0);
    }

    #[test]
    fn test_drain_non_matching_slot() {
        let mut queue = ShardWaitQueue::new();

        let key = b"{testslot}key1";
        let matching_slot = super::super::helpers::slot_for_key(key);
        // Pick a different slot
        let other_slot = (matching_slot + 1) % 16384;

        queue
            .register(make_entry(1, vec!["{testslot}key1"]))
            .unwrap();

        let drained = queue.drain_waiters_for_slot(other_slot);
        assert!(drained.is_empty());
        assert_eq!(queue.waiter_count(), 1);
        assert_eq!(queue.blocked_keys_count(), 1);
    }

    #[test]
    fn test_drain_updates_counts() {
        let mut queue = ShardWaitQueue::new();

        let key = b"{draintest}a";
        let slot = super::super::helpers::slot_for_key(key);

        // Register 3 waiters on matching slot, 1 on different slot
        queue.register(make_entry(1, vec!["{draintest}a"])).unwrap();
        queue.register(make_entry(2, vec!["{draintest}b"])).unwrap();
        queue.register(make_entry(3, vec!["{draintest}c"])).unwrap();
        queue
            .register(make_entry(4, vec!["unrelated_key"]))
            .unwrap();

        assert_eq!(queue.waiter_count(), 4);
        assert_eq!(queue.blocked_keys_count(), 4);

        let drained = queue.drain_waiters_for_slot(slot);
        assert_eq!(drained.len(), 3);
        assert_eq!(queue.waiter_count(), 1);
        assert_eq!(queue.blocked_keys_count(), 1);
    }
}
