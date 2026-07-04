//! Consolidated per-key lock table.
//!
//! One structure holds both truths about a key: which transactions *intend*
//! to access it (ordered by txid, driving SCA — Selective Contention
//! Analysis — for out-of-order execution) and which of those intents have
//! been *granted* the lock. Because a granted lock is just a flag on the
//! intent entry, the two can never fall out of lock-step, and releasing a
//! transaction — whether it committed, failed, or was aborted before ever
//! being granted — is a single transition: remove its intents.
//!
//! The table is owned exclusively by a single shard worker and mutated
//! through `&mut self`; no interior mutability or atomics are needed.

use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;

use super::types::LockMode;

/// A declared intent to access a key.
#[derive(Debug, Clone, Copy)]
struct Intent {
    /// Access mode (read or write).
    mode: LockMode,
    /// Whether the lock has been granted. Multiple Read grants may coexist
    /// on a key; a Write grant is exclusive.
    granted: bool,
}

/// Per-key lock table tracking intents and granted locks.
///
/// Transitions:
/// - [`Self::declare`] — register a transaction's intents (SCA visibility).
/// - [`Self::try_grant`] — all-or-nothing lock grant across the keys.
/// - [`Self::release`] — remove the intents, releasing any granted locks.
///   Covers both completion and abort; a never-granted transaction releases
///   nothing but disappears from SCA ordering.
#[derive(Debug, Default)]
pub struct LockTable {
    /// Intents per key, ordered by txid (BTreeMap gives SCA its ordering).
    keys: HashMap<Bytes, BTreeMap<u64, Intent>>,
}

impl LockTable {
    /// Create a new empty lock table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Declare a transaction's intent to access the given keys.
    pub fn declare(&mut self, keys: &[Bytes], txid: u64, mode: LockMode) {
        for key in keys {
            self.keys.entry(key.clone()).or_default().insert(
                txid,
                Intent {
                    mode,
                    granted: false,
                },
            );
        }
    }

    /// SCA check for one key: a transaction can proceed if no conflicting
    /// intent with a lower txid exists.
    ///
    /// Conflict rules: Read-Read is the only non-conflicting combination.
    fn can_proceed_for_key(&self, key: &Bytes, txid: u64, mode: LockMode) -> bool {
        let Some(intents) = self.keys.get(key) else {
            return true;
        };
        intents
            .range(..txid)
            .all(|(_, other)| !Self::conflicts(mode, other.mode))
    }

    /// Try to grant a declared transaction its locks on all keys —
    /// all-or-nothing.
    ///
    /// A grant requires, on every key:
    /// 1. SCA: no conflicting intent with a lower txid (pending or granted).
    /// 2. Compatibility: no conflicting *granted* intent from another
    ///    transaction, regardless of txid order (a higher-txid Read may
    ///    already hold the key when a lower-txid Write arrives).
    ///
    /// The checks run over all keys before any flag is set, so a failed
    /// grant leaves no partial state behind.
    ///
    /// Returns `false` (and grants nothing) if the transaction has no
    /// declared intent on one of the keys — callers must declare first.
    pub fn try_grant(&mut self, keys: &[Bytes], txid: u64) -> bool {
        for key in keys {
            let Some(intents) = self.keys.get(key) else {
                debug_assert!(false, "try_grant on key without declared intent");
                return false;
            };
            let Some(me) = intents.get(&txid) else {
                debug_assert!(false, "try_grant for txid without declared intent");
                return false;
            };
            let mode = me.mode;

            // SCA: conflicting lower-txid intents block, granted or not.
            if !self.can_proceed_for_key(key, txid, mode) {
                return false;
            }

            // Holders: conflicting granted intents from other transactions
            // block, regardless of txid order.
            if intents
                .iter()
                .any(|(&t, other)| t != txid && other.granted && Self::conflicts(mode, other.mode))
            {
                return false;
            }
        }

        for key in keys {
            if let Some(me) = self.keys.get_mut(key).and_then(|m| m.get_mut(&txid)) {
                me.granted = true;
            }
        }
        true
    }

    /// Remove a transaction's intents on the given keys, releasing any
    /// granted locks.
    ///
    /// This is the single exit transition: it serves completed transactions
    /// (locks granted, work done) and aborted ones (granted or still
    /// pending) alike.
    pub fn release(&mut self, keys: &[Bytes], txid: u64) {
        for key in keys {
            if let Some(intents) = self.keys.get_mut(key) {
                intents.remove(&txid);
                if intents.is_empty() {
                    self.keys.remove(key);
                }
            }
        }
    }

    /// Check if two access modes conflict.
    fn conflicts(a: LockMode, b: LockMode) -> bool {
        // Read-Read is the only non-conflicting combination.
        !(a == LockMode::Read && b == LockMode::Read)
    }

    /// Iterate over all keys with intents, yielding each key's txids in
    /// ascending order.
    pub fn iter_keys(&self) -> impl Iterator<Item = (&Bytes, Vec<u64>)> {
        self.keys
            .iter()
            .map(|(key, intents)| (key, intents.keys().copied().collect()))
    }

    /// Human-readable grant state for a key (diagnostics).
    pub fn lock_state_string(&self, key: &Bytes) -> String {
        let Some(intents) = self.keys.get(key) else {
            return "unlocked".to_string();
        };
        let mut readers = 0usize;
        for intent in intents.values().filter(|i| i.granted) {
            match intent.mode {
                LockMode::Write => return "write".to_string(),
                LockMode::Read => readers += 1,
            }
        }
        if readers > 0 {
            format!("read:{readers}")
        } else {
            "unlocked".to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(name: &'static [u8]) -> Bytes {
        Bytes::from_static(name)
    }

    #[test]
    fn declare_and_release_intent() {
        let mut table = LockTable::new();
        let keys = vec![key(b"key1")];

        table.declare(&keys, 1, LockMode::Write);
        let entries: Vec<_> = table.iter_keys().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1, vec![1]);

        table.release(&keys, 1);
        assert_eq!(table.iter_keys().count(), 0);
    }

    #[test]
    fn can_proceed_no_conflict_between_reads() {
        let mut table = LockTable::new();
        let keys = vec![key(b"key1")];

        table.declare(&keys, 1, LockMode::Read);
        table.declare(&keys, 2, LockMode::Read);

        // Transaction 2 can proceed even with 1 pending.
        assert!(table.can_proceed_for_key(&keys[0], 2, LockMode::Read));
    }

    #[test]
    fn can_proceed_blocked_by_lower_conflicting_txid() {
        let mut table = LockTable::new();
        let keys = vec![key(b"key1")];

        table.declare(&keys, 1, LockMode::Write);
        table.declare(&keys, 2, LockMode::Write);

        assert!(!table.can_proceed_for_key(&keys[0], 2, LockMode::Write));
        // But transaction 1 can proceed (no lower txid).
        assert!(table.can_proceed_for_key(&keys[0], 1, LockMode::Write));
    }

    #[test]
    fn read_write_and_write_read_conflict() {
        let mut table = LockTable::new();
        let keys = vec![key(b"key1")];

        table.declare(&keys, 1, LockMode::Read);
        table.declare(&keys, 2, LockMode::Write);
        assert!(!table.can_proceed_for_key(&keys[0], 2, LockMode::Write));

        let mut table = LockTable::new();
        table.declare(&keys, 1, LockMode::Write);
        table.declare(&keys, 2, LockMode::Read);
        assert!(!table.can_proceed_for_key(&keys[0], 2, LockMode::Read));
    }

    #[test]
    fn try_grant_all_or_nothing() {
        let mut table = LockTable::new();
        let keys = vec![key(b"key1"), key(b"key2")];

        table.declare(&keys, 1, LockMode::Write);
        assert!(table.try_grant(&keys, 1));

        // A second writer cannot be granted while 1 holds both keys.
        table.declare(&keys, 2, LockMode::Write);
        assert!(!table.try_grant(&keys, 2));
        // Failed grant left no partial state: neither key shows a second holder.
        assert_eq!(table.lock_state_string(&keys[0]), "write");
        assert_eq!(table.lock_state_string(&keys[1]), "write");

        // Release 1; 2 can now be granted.
        table.release(&keys, 1);
        assert!(table.try_grant(&keys, 2));
    }

    #[test]
    fn multiple_readers_share_then_writer_waits() {
        let mut table = LockTable::new();
        let keys = vec![key(b"key1")];

        table.declare(&keys, 1, LockMode::Read);
        table.declare(&keys, 2, LockMode::Read);
        table.declare(&keys, 3, LockMode::Read);
        assert!(table.try_grant(&keys, 1));
        assert!(table.try_grant(&keys, 2));
        assert!(table.try_grant(&keys, 3));
        assert_eq!(table.lock_state_string(&keys[0]), "read:3");

        // Writer blocked by the granted readers.
        table.declare(&keys, 4, LockMode::Write);
        assert!(!table.try_grant(&keys, 4));

        // Release all readers; writer can be granted.
        table.release(&keys, 1);
        table.release(&keys, 2);
        table.release(&keys, 3);
        assert!(table.try_grant(&keys, 4));
        assert_eq!(table.lock_state_string(&keys[0]), "write");
    }

    #[test]
    fn lower_txid_writer_blocked_by_granted_higher_reader() {
        let mut table = LockTable::new();
        let keys = vec![key(b"key1")];

        // Higher txid read arrives first and is granted.
        table.declare(&keys, 5, LockMode::Read);
        assert!(table.try_grant(&keys, 5));

        // Lower txid write passes SCA (no lower intents) but must still be
        // blocked by the granted reader.
        table.declare(&keys, 3, LockMode::Write);
        assert!(table.can_proceed_for_key(&keys[0], 3, LockMode::Write));
        assert!(!table.try_grant(&keys, 3));

        table.release(&keys, 5);
        assert!(table.try_grant(&keys, 3));
    }

    #[test]
    fn release_of_ungranted_intent_unblocks_sca() {
        let mut table = LockTable::new();
        let keys = vec![key(b"key1")];

        // 1 is pending (never granted); 2 is blocked behind it by SCA.
        table.declare(&keys, 1, LockMode::Write);
        table.declare(&keys, 2, LockMode::Write);
        assert!(!table.try_grant(&keys, 2));

        // Aborting 1 (same transition as completing) unblocks 2.
        table.release(&keys, 1);
        assert!(table.try_grant(&keys, 2));
    }

    #[test]
    fn duplicate_keys_in_slice_are_idempotent() {
        let mut table = LockTable::new();
        let keys = vec![key(b"key1"), key(b"key1")];

        table.declare(&keys, 1, LockMode::Read);
        assert!(table.try_grant(&keys, 1));
        assert_eq!(table.lock_state_string(&keys[0]), "read:1");

        table.release(&keys, 1);
        assert_eq!(table.lock_state_string(&keys[0]), "unlocked");
        assert_eq!(table.iter_keys().count(), 0);
    }

    #[test]
    fn lock_state_string_reflects_grants_not_intents() {
        let mut table = LockTable::new();
        let keys = vec![key(b"key1")];

        table.declare(&keys, 1, LockMode::Write);
        // Declared but not granted: still unlocked.
        assert_eq!(table.lock_state_string(&keys[0]), "unlocked");
        assert!(table.try_grant(&keys, 1));
        assert_eq!(table.lock_state_string(&keys[0]), "write");
        assert_eq!(table.lock_state_string(&key(b"other")), "unlocked");
    }
}
