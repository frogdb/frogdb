use std::collections::{HashMap, VecDeque};

use bytes::Bytes;
use frogdb_protocol::{ProtocolVersion, Response};
use tokio::sync::oneshot;

use tokio::time::Instant;

use crate::command::WaiterKind;
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
    ///
    /// A [`tokio::time::Instant`], i.e. the same clock the timer that will fire at this
    /// deadline runs on. It must not be a `std::time::Instant`: under a paused runtime
    /// (`tokio::time::pause`, which every simulated turmoil host uses) the two clocks run at
    /// different rates, so a std-derived deadline converted for comparison drifts by however
    /// much wall time the process has burned — and once virtual time overtakes wall time it is
    /// unconditionally in the past, timing out every blocking command instantly.
    pub deadline: Option<Instant>,
    /// Protocol version of the blocked client (for RESP3-aware score formatting).
    pub protocol_version: ProtocolVersion,
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

/// A registration the queue refused, handed back to the caller.
///
/// The entry travels with the refusal because it owns the client's
/// `response_tx`: dropping it would signal the coordinator's `Err(_)` arm,
/// which means "the shard is gone" (`specs/blocking.md` FM-BLOCKING-004). The
/// caller answers with `message` instead. See FM-BLOCKING-006.
#[derive(Debug)]
pub struct RegisterRefused {
    /// The rejected registration, still owning its response channel.
    pub entry: WaitEntry,
    /// The client-visible refusal error.
    pub message: &'static str,
}

/// Refusal text for the shard-wide blocked-connection bound.
pub const MAX_BLOCKED_CONNECTIONS_ERR: &str = "ERR max blocked connections limit reached";

/// Refusal text for the per-key waiter bound.
pub const MAX_WAITERS_PER_KEY_ERR: &str = "ERR max waiters per key limit reached";

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
    /// Monotonic registration counter; each `register` stamps the next value.
    next_seq: u64,
    /// Registration ordinal per entry slot (parallel to `entries`). Only the
    /// slots of live entries are ever read (via `dump`); reused slots are
    /// overwritten on the next `register`, so stale ordinals are never observed.
    seq_by_slot: Vec<u64>,
    /// Append-only journal of every registration, for `DEBUG WAITQUEUE-LOG`.
    /// See [`ShardWaitQueue::registration_log`].
    #[cfg(feature = "wait-queue-log")]
    registration_log: Vec<WaitRegistrationRecord>,
    /// Set once the journal hit [`MAX_REGISTRATION_LOG`] and started dropping
    /// records, so a reader can tell "no more registrations" from "I stopped
    /// recording" instead of drawing conclusions from a truncated log.
    #[cfg(feature = "wait-queue-log")]
    registration_log_truncated: bool,
}

/// Cap on the registration journal. A test run parks on the order of thousands
/// of waiters; the cap only exists so a pathological (or long-lived) process
/// running with the `wait-queue-log` feature cannot grow it without bound.
#[cfg(feature = "wait-queue-log")]
const MAX_REGISTRATION_LOG: usize = 1 << 16;

/// One recorded registration: the event-time counterpart of [`WaiterDump`],
/// which can only ever show waiters that are *still parked* when it is read.
///
/// A waiter that parks and is served between two `DEBUG WAITQUEUE` samples is
/// invisible to sampling but always present here, which is what makes exact
/// FIFO wake-order checking sound (`.scratch/concurrency-testing/issues/16`).
/// One record is emitted per key of a multi-key waiter, all sharing the
/// waiter's single `registration_seq`.
///
/// Recording is compiled in only under the `wait-queue-log` cargo feature
/// (enabled by the server's `turmoil` feature for simulation tests, never in
/// production); the type itself is always defined so the accessor has one
/// signature in both configurations.
#[derive(Debug, Clone)]
pub struct WaitRegistrationRecord {
    /// Queue-wide monotonic registration ordinal (smaller = registered earlier).
    pub registration_seq: u64,
    /// Connection id of the waiter.
    pub conn_id: u64,
    /// One of the keys the waiter parked on.
    pub key: Bytes,
    /// Blocking-command name (e.g. "BLPOP").
    pub op: &'static str,
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
            next_seq: 0,
            seq_by_slot: Vec::new(),
            #[cfg(feature = "wait-queue-log")]
            registration_log: Vec::new(),
            #[cfg(feature = "wait-queue-log")]
            registration_log_truncated: false,
        }
    }

    /// Register a new waiter.
    ///
    /// Returns `Ok(())` if registered. On refusal the entry comes back inside
    /// [`RegisterRefused`] so the caller still owns the response channel and can
    /// reply with the refusal error — dropping it would be indistinguishable
    /// from shard death (`specs/blocking.md` FM-BLOCKING-006, FM-BLOCKING-004).
    ///
    /// Both bounds are checked over every requested key *before* any insertion,
    /// so a refusal leaves the queue exactly as it found it — no partial
    /// registration under the keys checked before the offending one.
    pub fn register(&mut self, entry: WaitEntry) -> Result<(), RegisterRefused> {
        // Check global limit
        if self.max_blocked_connections > 0 && self.waiter_count >= self.max_blocked_connections {
            return Err(RegisterRefused {
                entry,
                message: MAX_BLOCKED_CONNECTIONS_ERR,
            });
        }

        // Check per-key limits
        if self.max_waiters_per_key > 0 {
            let over_limit = entry.keys.iter().any(|key| {
                self.waiters_by_key
                    .get(key)
                    .is_some_and(|waiters| waiters.len() >= self.max_waiters_per_key)
            });
            if over_limit {
                return Err(RegisterRefused {
                    entry,
                    message: MAX_WAITERS_PER_KEY_ERR,
                });
            }
        }

        let conn_id = entry.conn_id;
        let keys = entry.keys.clone();

        let seq = self.next_seq;
        self.next_seq += 1;

        // Journal the registration at the moment it happens (test builds only).
        #[cfg(feature = "wait-queue-log")]
        {
            let op = blocking_op_name(&entry.op);
            for key in &keys {
                if self.registration_log.len() >= MAX_REGISTRATION_LOG {
                    self.registration_log_truncated = true;
                    break;
                }
                self.registration_log.push(WaitRegistrationRecord {
                    registration_seq: seq,
                    conn_id,
                    key: key.clone(),
                    op,
                });
            }
        }

        // Allocate a slot for the entry, keeping `seq_by_slot` in lock-step
        // with `entries` so `seq_by_slot[slot_idx]` is always valid.
        let slot_idx = if let Some(idx) = self.free_slots.pop() {
            self.entries[idx] = Some(entry);
            self.seq_by_slot[idx] = seq;
            idx
        } else {
            let idx = self.entries.len();
            self.entries.push(Some(entry));
            self.seq_by_slot.push(seq);
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
            .filter(|key| super::partition::slot_for_key(key) == slot)
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

    /// Drain every parked waiter, leaving an empty queue.
    ///
    /// Used by the demotion release (`specs/blocking.md` FM-BLOCKING-007): once
    /// this node is a replica, no parked waiter may be served locally, so the
    /// whole queue is handed back for the caller to answer.
    pub fn drain_all(&mut self) -> Vec<WaitEntry> {
        let mut drained = Vec::with_capacity(self.waiter_count);
        for idx in 0..self.entries.len() {
            if let Some(entry) = self.entries[idx].take() {
                self.free_slots.push(idx);
                drained.push(entry);
            }
        }

        self.waiters_by_key.clear();
        self.conn_entries.clear();
        self.waiter_count = 0;

        drained
    }

    /// Check if there are any waiters for a key.
    pub fn has_waiters(&self, key: &Bytes) -> bool {
        self.waiters_by_key
            .get(key)
            .map(|w| !w.is_empty())
            .unwrap_or(false)
    }

    /// Check if there are any waiters for a key whose op matches `kind`.
    ///
    /// Used by the satisfy paths in `blocking.rs` to avoid popping waiters of
    /// the wrong type when a write of a different value kind fires on the same
    /// key (e.g. an LPUSH on a key that also has XRead waiters).
    pub fn has_waiters_for_kind(&self, key: &Bytes, kind: WaiterKind) -> bool {
        let Some(waiters) = self.waiters_by_key.get(key) else {
            return false;
        };
        waiters.iter().any(|&idx| {
            self.entries
                .get(idx)
                .and_then(|e| e.as_ref())
                .map(|e| Self::entry_matches_kind(e, kind))
                .unwrap_or(false)
        })
    }

    /// Pop the oldest waiter on `key` whose op matches `kind`.
    ///
    /// Walks the per-key FIFO in registration order and returns the first
    /// matching waiter, leaving waiters of other kinds untouched in the queue.
    /// FIFO ordering within a kind is preserved.
    pub fn pop_oldest_waiter_of_kind(
        &mut self,
        key: &Bytes,
        kind: WaiterKind,
    ) -> Option<WaitEntry> {
        // Find the position of the first matching waiter in the per-key deque.
        let found_pos = {
            let waiters = self.waiters_by_key.get(key)?;
            waiters.iter().position(|&idx| {
                self.entries
                    .get(idx)
                    .and_then(|e| e.as_ref())
                    .map(|e| Self::entry_matches_kind(e, kind))
                    .unwrap_or(false)
            })?
        };

        // Remove that index from the per-key deque.
        let idx = {
            let waiters = self.waiters_by_key.get_mut(key)?;
            waiters.remove(found_pos)?
        };

        // Take ownership of the entry.
        let entry = self.entries[idx].take()?;

        // Collect other keys to clean up (excluding the current key).
        let other_keys: Vec<Bytes> = entry.keys.iter().filter(|k| *k != key).cloned().collect();

        // Remove from all other key indices.
        for k in &other_keys {
            if let Some(w) = self.waiters_by_key.get_mut(k) {
                w.retain(|&i| i != idx);
                if w.is_empty() {
                    self.waiters_by_key.remove(k);
                }
            }
        }

        // Remove from conn_entries.
        if let Some(conn_entries) = self.conn_entries.get_mut(&entry.conn_id) {
            conn_entries.retain(|&i| i != idx);
            if conn_entries.is_empty() {
                self.conn_entries.remove(&entry.conn_id);
            }
        }

        self.free_slots.push(idx);
        self.waiter_count -= 1;

        // Clean up the primary key's deque entry if it is now empty.
        if let Some(waiters) = self.waiters_by_key.get(key)
            && waiters.is_empty()
        {
            self.waiters_by_key.remove(key);
        }

        Some(entry)
    }

    /// Returns true if `entry.op` is compatible with the given `kind`.
    fn entry_matches_kind(entry: &WaitEntry, kind: WaiterKind) -> bool {
        use BlockingOp::*;
        matches!(
            (kind, &entry.op),
            (
                WaiterKind::List,
                BLPop | BRPop | BLMove { .. } | BLMPop { .. }
            ) | (WaiterKind::SortedSet, BZPopMin | BZPopMax | BZMPop { .. })
                | (WaiterKind::Stream, XRead { .. } | XReadGroup { .. })
        )
    }

    /// Check if there are any XREADGROUP waiters for a key.
    ///
    /// Unlike `has_waiters_for_kind(Stream)` which matches both XREAD and
    /// XREADGROUP, this only matches XREADGROUP. Used by the drain-on-delete
    /// path which must send NOGROUP/WRONGTYPE to XREADGROUP clients while
    /// leaving XREAD clients blocked.
    pub fn has_xreadgroup_waiters(&self, key: &Bytes) -> bool {
        let Some(waiters) = self.waiters_by_key.get(key) else {
            return false;
        };
        waiters.iter().any(|&idx| {
            self.entries
                .get(idx)
                .and_then(|e| e.as_ref())
                .map(|e| matches!(e.op, BlockingOp::XReadGroup { .. }))
                .unwrap_or(false)
        })
    }

    /// Pop the oldest XREADGROUP waiter on `key`.
    ///
    /// Same mechanics as `pop_oldest_waiter_of_kind` but only matches
    /// `BlockingOp::XReadGroup`. XREAD waiters are left in the queue.
    pub fn pop_oldest_xreadgroup_waiter(&mut self, key: &Bytes) -> Option<WaitEntry> {
        self.pop_oldest_stream_waiter_matching(key, |op| {
            matches!(op, BlockingOp::XReadGroup { .. })
        })
    }

    /// Pop the oldest stream waiter on `key`, XREAD or XREADGROUP.
    ///
    /// The wrong-type drain's pop (`specs/blocking.md` TR-BLOCKING-022): a key
    /// that no longer holds a stream makes *every* stream wait on it
    /// unsatisfiable, so unlike [`Self::pop_oldest_xreadgroup_waiter`] this one
    /// takes plain XREAD waiters too. Same mechanics otherwise.
    pub fn pop_oldest_stream_waiter(&mut self, key: &Bytes) -> Option<WaitEntry> {
        self.pop_oldest_stream_waiter_matching(key, |op| {
            matches!(op, BlockingOp::XRead { .. } | BlockingOp::XReadGroup { .. })
        })
    }

    /// Shared body of the two stream pops: take the first entry on `key` whose
    /// op satisfies `matches_op`, unlinking it from every index it appears in.
    fn pop_oldest_stream_waiter_matching(
        &mut self,
        key: &Bytes,
        matches_op: impl Fn(&BlockingOp) -> bool,
    ) -> Option<WaitEntry> {
        let found_pos = {
            let waiters = self.waiters_by_key.get(key)?;
            waiters.iter().position(|&idx| {
                self.entries
                    .get(idx)
                    .and_then(|e| e.as_ref())
                    .map(|e| matches_op(&e.op))
                    .unwrap_or(false)
            })?
        };

        let idx = {
            let waiters = self.waiters_by_key.get_mut(key)?;
            waiters.remove(found_pos)?
        };

        let entry = self.entries[idx].take()?;

        let other_keys: Vec<Bytes> = entry.keys.iter().filter(|k| *k != key).cloned().collect();

        for k in &other_keys {
            if let Some(w) = self.waiters_by_key.get_mut(k) {
                w.retain(|&i| i != idx);
                if w.is_empty() {
                    self.waiters_by_key.remove(k);
                }
            }
        }

        if let Some(conn_entries) = self.conn_entries.get_mut(&entry.conn_id) {
            conn_entries.retain(|&i| i != idx);
            if conn_entries.is_empty() {
                self.conn_entries.remove(&entry.conn_id);
            }
        }

        self.free_slots.push(idx);
        self.waiter_count -= 1;

        if let Some(waiters) = self.waiters_by_key.get(key)
            && waiters.is_empty()
        {
            self.waiters_by_key.remove(key);
        }

        Some(entry)
    }

    /// Get the number of active waiters.
    pub fn waiter_count(&self) -> usize {
        self.waiter_count
    }

    /// Get the number of keys with waiters.
    pub fn blocked_keys_count(&self) -> usize {
        self.waiters_by_key.len()
    }

    /// Read-only per-key dump of all waiters, in registration (FIFO) order
    /// within each key. Keys are returned sorted lexicographically for a
    /// deterministic snapshot. Used by DEBUG WAITQUEUE.
    pub fn dump(&self) -> Vec<(Bytes, Vec<WaiterDump>)> {
        let mut keys: Vec<&Bytes> = self.waiters_by_key.keys().collect();
        keys.sort();
        keys.into_iter()
            .map(|key| {
                let waiters = self
                    .waiters_by_key
                    .get(key)
                    .map(|deque| {
                        deque
                            .iter()
                            .filter_map(|&idx| {
                                let entry = self.entries[idx].as_ref()?;
                                Some(WaiterDump {
                                    conn_id: entry.conn_id,
                                    op: blocking_op_name(&entry.op),
                                    registration_seq: self.seq_by_slot[idx],
                                    has_deadline: entry.deadline.is_some(),
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                (key.clone(), waiters)
            })
            .collect()
    }

    /// Append-only journal of every registration this queue has seen, in
    /// registration order. Used by `DEBUG WAITQUEUE-LOG`. Always empty without
    /// the `wait-queue-log` feature — see [`WaitRegistrationRecord`].
    pub fn registration_log(&self) -> &[WaitRegistrationRecord] {
        #[cfg(feature = "wait-queue-log")]
        {
            &self.registration_log
        }
        #[cfg(not(feature = "wait-queue-log"))]
        {
            &[]
        }
    }

    /// True once the journal stopped recording because it hit its cap. A
    /// reader must not treat a truncated journal as a complete registration
    /// history.
    pub fn registration_log_truncated(&self) -> bool {
        #[cfg(feature = "wait-queue-log")]
        {
            self.registration_log_truncated
        }
        #[cfg(not(feature = "wait-queue-log"))]
        {
            false
        }
    }
}

/// One waiter's read-only diagnostic view (DEBUG WAITQUEUE). `op` is the
/// blocking-command name; `registration_seq` is the queue-wide monotonic
/// registration ordinal (smaller = registered earlier), enabling exact FIFO
/// wake-fairness checking.
#[derive(Debug, Clone)]
pub struct WaiterDump {
    pub conn_id: u64,
    pub op: &'static str,
    pub registration_seq: u64,
    pub has_deadline: bool,
}

/// Static name for a blocking op (DEBUG WAITQUEUE display).
fn blocking_op_name(op: &BlockingOp) -> &'static str {
    match op {
        BlockingOp::BLPop => "BLPOP",
        BlockingOp::BRPop => "BRPOP",
        BlockingOp::BLMove { .. } => "BLMOVE",
        BlockingOp::BLMPop { .. } => "BLMPOP",
        BlockingOp::BZPopMin => "BZPOPMIN",
        BlockingOp::BZPopMax => "BZPOPMAX",
        BlockingOp::BZMPop { .. } => "BZMPOP",
        BlockingOp::XRead { .. } => "XREAD",
        BlockingOp::XReadGroup { .. } => "XREADGROUP",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::BlockingOp;

    fn make_entry(conn_id: u64, keys: Vec<&str>) -> WaitEntry {
        make_entry_with_op(conn_id, keys, BlockingOp::BLPop)
    }

    fn make_entry_with_op(conn_id: u64, keys: Vec<&str>, op: BlockingOp) -> WaitEntry {
        let (tx, _rx) = oneshot::channel();
        WaitEntry {
            conn_id,
            keys: keys
                .into_iter()
                .map(|k| Bytes::from(k.to_string()))
                .collect(),
            op,
            response_tx: tx,
            deadline: None,
            protocol_version: ProtocolVersion::default(),
        }
    }

    #[test]
    fn dump_reports_fifo_order_and_registration_seq() {
        let mut queue = ShardWaitQueue::new();
        // Two waiters on "k" (conn 1 then conn 2), one on "j" (conn 3).
        queue.register(make_entry(1, vec!["k"])).unwrap();
        queue.register(make_entry(2, vec!["k"])).unwrap();
        queue.register(make_entry(3, vec!["j"])).unwrap();

        let dump = queue.dump();
        // Keys sorted lexicographically: "j" before "k".
        let keys: Vec<Bytes> = dump.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec![Bytes::from("j"), Bytes::from("k")]);

        let k_waiters = &dump.iter().find(|(k, _)| k == "k").unwrap().1;
        // FIFO within the key: conn 1 registered before conn 2.
        assert_eq!(k_waiters[0].conn_id, 1);
        assert_eq!(k_waiters[1].conn_id, 2);
        assert!(k_waiters[0].registration_seq < k_waiters[1].registration_seq);
        assert_eq!(k_waiters[0].op, "BLPOP");
        assert!(!k_waiters[0].has_deadline);
    }

    #[test]
    fn dump_empty_when_no_waiters() {
        let queue = ShardWaitQueue::new();
        assert!(queue.dump().is_empty());
    }

    #[test]
    fn dump_reflects_registration_seq_across_keys() {
        let mut queue = ShardWaitQueue::new();
        queue.register(make_entry(1, vec!["k"])).unwrap();
        queue.register(make_entry(2, vec!["j"])).unwrap();
        let dump = queue.dump();
        let k_seq = dump.iter().find(|(k, _)| k == "k").unwrap().1[0].registration_seq;
        let j_seq = dump.iter().find(|(k, _)| k == "j").unwrap().1[0].registration_seq;
        // "k" was registered before "j" — its ordinal is strictly smaller even
        // though "j" sorts first in the dump output.
        assert!(k_seq < j_seq);
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
        let slot = super::super::partition::slot_for_key(key);

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
        let matching_slot = super::super::partition::slot_for_key(key);
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
        let slot = super::super::partition::slot_for_key(key);

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

    #[test]
    fn test_has_waiters_for_kind_mixed() {
        let mut queue = ShardWaitQueue::new();
        let key = Bytes::from("k");

        queue
            .register(make_entry_with_op(1, vec!["k"], BlockingOp::BLPop))
            .unwrap();
        queue
            .register(make_entry_with_op(
                2,
                vec!["k"],
                BlockingOp::XRead {
                    after_ids: vec![crate::types::StreamId::new(0, 0)],
                    count: None,
                },
            ))
            .unwrap();

        assert!(queue.has_waiters_for_kind(&key, WaiterKind::List));
        assert!(queue.has_waiters_for_kind(&key, WaiterKind::Stream));
        assert!(!queue.has_waiters_for_kind(&key, WaiterKind::SortedSet));
    }

    #[test]
    fn test_pop_of_kind_skips_wrong_kind() {
        let mut queue = ShardWaitQueue::new();
        let key = Bytes::from("k");

        // Register XRead first, then BLPop. The list satisfy path should return
        // the BLPop entry without touching the XRead entry.
        queue
            .register(make_entry_with_op(
                1,
                vec!["k"],
                BlockingOp::XRead {
                    after_ids: vec![crate::types::StreamId::new(0, 0)],
                    count: None,
                },
            ))
            .unwrap();
        queue
            .register(make_entry_with_op(2, vec!["k"], BlockingOp::BLPop))
            .unwrap();

        let popped = queue
            .pop_oldest_waiter_of_kind(&key, WaiterKind::List)
            .expect("should return BLPop waiter");
        assert_eq!(popped.conn_id, 2);
        assert_eq!(queue.waiter_count(), 1);

        // XRead waiter is still there.
        assert!(queue.has_waiters_for_kind(&key, WaiterKind::Stream));
        // No more list waiters.
        assert!(!queue.has_waiters_for_kind(&key, WaiterKind::List));
    }

    #[test]
    fn test_pop_of_kind_returns_none_when_only_wrong_kind() {
        let mut queue = ShardWaitQueue::new();
        let key = Bytes::from("k");

        queue
            .register(make_entry_with_op(
                1,
                vec!["k"],
                BlockingOp::XRead {
                    after_ids: vec![crate::types::StreamId::new(0, 0)],
                    count: None,
                },
            ))
            .unwrap();

        assert!(
            queue
                .pop_oldest_waiter_of_kind(&key, WaiterKind::List)
                .is_none()
        );
        // The XRead waiter is untouched.
        assert_eq!(queue.waiter_count(), 1);
        assert!(queue.has_waiters_for_kind(&key, WaiterKind::Stream));
    }

    #[test]
    fn test_pop_of_kind_preserves_fifo_within_kind() {
        let mut queue = ShardWaitQueue::new();
        let key = Bytes::from("k");

        // Register BLPop, XRead, BLPop in order.
        queue
            .register(make_entry_with_op(1, vec!["k"], BlockingOp::BLPop))
            .unwrap();
        queue
            .register(make_entry_with_op(
                2,
                vec!["k"],
                BlockingOp::XRead {
                    after_ids: vec![crate::types::StreamId::new(0, 0)],
                    count: None,
                },
            ))
            .unwrap();
        queue
            .register(make_entry_with_op(3, vec!["k"], BlockingOp::BLPop))
            .unwrap();

        // First list-kind pop returns conn 1.
        let first = queue
            .pop_oldest_waiter_of_kind(&key, WaiterKind::List)
            .expect("first BLPop");
        assert_eq!(first.conn_id, 1);

        // Second list-kind pop returns conn 3 (skipping XRead at conn 2).
        let second = queue
            .pop_oldest_waiter_of_kind(&key, WaiterKind::List)
            .expect("second BLPop");
        assert_eq!(second.conn_id, 3);

        // No more list waiters; XRead remains.
        assert!(!queue.has_waiters_for_kind(&key, WaiterKind::List));
        assert!(queue.has_waiters_for_kind(&key, WaiterKind::Stream));
        assert_eq!(queue.waiter_count(), 1);
    }
}
