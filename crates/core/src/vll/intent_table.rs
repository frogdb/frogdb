//! Intent table for SCA (Selective Contention Analysis).
//!
//! The intent table tracks pending operations' key access patterns.
//! This enables out-of-order execution for non-conflicting operations.

use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;

use super::{KeyLockState, LockMode};

/// An intent to access a key.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct KeyIntent {
    /// Transaction ID.
    pub txid: u64,
    /// Access mode (read or write).
    pub mode: LockMode,
}

/// Intent table for tracking pending operations' key access.
///
/// Used for SCA (Selective Contention Analysis) to determine if
/// an operation can execute out-of-order without conflicts.
#[derive(Debug, Default)]
pub struct IntentTable {
    /// Intents indexed by key. Each key maps to a list of pending intents
    /// sorted by txid (via BTreeMap inside).
    intents_by_key: HashMap<Bytes, BTreeMap<u64, LockMode>>,
    /// Per-key lock states for actual lock acquisition.
    key_locks: HashMap<Bytes, KeyLockState>,
}

impl IntentTable {
    /// Create a new empty intent table.
    pub fn new() -> Self {
        Self {
            intents_by_key: HashMap::new(),
            key_locks: HashMap::new(),
        }
    }

    /// Declare an intent to access a key.
    pub fn declare_intent(&mut self, key: Bytes, txid: u64, mode: LockMode) {
        self.intents_by_key
            .entry(key)
            .or_default()
            .insert(txid, mode);
    }

    /// Declare intents for multiple keys.
    pub fn declare_intents(&mut self, keys: &[Bytes], txid: u64, mode: LockMode) {
        for key in keys {
            self.declare_intent(key.clone(), txid, mode);
        }
    }

    /// Remove an intent.
    pub fn remove_intent(&mut self, key: &Bytes, txid: u64) {
        if let Some(intents) = self.intents_by_key.get_mut(key) {
            intents.remove(&txid);
            if intents.is_empty() {
                self.intents_by_key.remove(key);
            }
        }
    }

    /// Remove all intents for a transaction.
    pub fn remove_all_intents(&mut self, keys: &[Bytes], txid: u64) {
        for key in keys {
            self.remove_intent(key, txid);
        }
    }

    /// Check if a transaction can proceed for a specific key using SCA.
    ///
    /// A transaction can proceed if:
    /// 1. It has the lowest txid among conflicting intents for that key
    /// 2. There are no conflicting intents with lower txid
    ///
    /// Conflict rules:
    /// - Read-Read: No conflict
    /// - Read-Write: Conflict
    /// - Write-Read: Conflict
    /// - Write-Write: Conflict
    pub fn can_proceed_for_key(&self, key: &Bytes, txid: u64, mode: LockMode) -> bool {
        if let Some(intents) = self.intents_by_key.get(key) {
            // Check all intents with lower txid for conflicts
            for (&other_txid, &other_mode) in intents.iter() {
                if other_txid >= txid {
                    // We only care about operations that came before us
                    break;
                }

                // Check for conflict
                if Self::conflicts(mode, other_mode) {
                    return false;
                }
            }
        }
        true
    }

    /// Check if a transaction can proceed for all keys using SCA.
    pub fn can_proceed(&self, keys: &[Bytes], txid: u64, mode: LockMode) -> bool {
        keys.iter()
            .all(|key| self.can_proceed_for_key(key, txid, mode))
    }

    /// Check if two access modes conflict.
    fn conflicts(a: LockMode, b: LockMode) -> bool {
        // Read-Read is the only non-conflicting combination
        !(a == LockMode::Read && b == LockMode::Read)
    }

    /// Try to acquire locks for a transaction.
    ///
    /// Atomically acquires locks on all keys. If any lock fails,
    /// releases all acquired locks and returns false.
    pub fn try_acquire_locks(&mut self, keys: &[Bytes], mode: LockMode) -> bool {
        let mut acquired = Vec::new();

        for key in keys {
            // Ensure lock state exists
            let lock = self.key_locks.entry(key.clone()).or_default();

            if lock.try_lock(mode) {
                acquired.push(key.clone());
            } else {
                // Release all acquired locks
                for acq_key in acquired {
                    if let Some(lock) = self.key_locks.get(&acq_key) {
                        lock.unlock(mode);
                    }
                }
                return false;
            }
        }

        true
    }

    /// Release locks for a transaction.
    pub fn release_locks(&mut self, keys: &[Bytes], mode: LockMode) {
        for key in keys {
            if let Some(lock) = self.key_locks.get(key) {
                lock.unlock(mode);
            }
        }
    }

    /// Get the number of pending intents for a key.
    pub fn intent_count(&self, key: &Bytes) -> usize {
        self.intents_by_key.get(key).map(|m| m.len()).unwrap_or(0)
    }

    /// Get the total number of keys with pending intents.
    pub fn key_count(&self) -> usize {
        self.intents_by_key.len()
    }

    /// Check if a key has any pending intents.
    pub fn has_intents(&self, key: &Bytes) -> bool {
        self.intents_by_key
            .get(key)
            .map(|m| !m.is_empty())
            .unwrap_or(false)
    }

    /// Get all txids with intents on a key.
    pub fn get_txids_for_key(&self, key: &Bytes) -> Vec<u64> {
        self.intents_by_key
            .get(key)
            .map(|m| m.keys().copied().collect())
            .unwrap_or_default()
    }

    /// Clean up key lock state for keys with no intents.
    pub fn cleanup_unused_locks(&mut self) {
        self.key_locks.retain(|key, lock| {
            // Keep if has intents or is currently locked
            self.intents_by_key.contains_key(key) || lock.is_locked()
        });
    }

    /// Iterate over all keys with intents.
    pub fn iter_keys(&self) -> impl Iterator<Item = (&Bytes, Vec<u64>)> {
        self.intents_by_key
            .iter()
            .map(|(key, intents)| (key, intents.keys().copied().collect()))
    }

    /// Get the lock state for a key as a human-readable string.
    pub fn get_lock_state_string(&self, key: &Bytes) -> String {
        match self.key_locks.get(key) {
            Some(lock) => {
                if lock.is_write_locked() {
                    "write".to_string()
                } else if lock.is_read_locked() {
                    format!("read:{}", lock.reader_count())
                } else {
                    "unlocked".to_string()
                }
            }
            None => "unlocked".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_declare_and_remove_intent() {
        let mut table = IntentTable::new();
        let key = Bytes::from_static(b"key1");

        table.declare_intent(key.clone(), 1, LockMode::Write);
        assert_eq!(table.intent_count(&key), 1);

        table.remove_intent(&key, 1);
        assert_eq!(table.intent_count(&key), 0);
    }

    #[test]
    fn test_can_proceed_no_conflict() {
        let mut table = IntentTable::new();
        let key = Bytes::from_static(b"key1");

        // Two reads don't conflict
        table.declare_intent(key.clone(), 1, LockMode::Read);
        table.declare_intent(key.clone(), 2, LockMode::Read);

        // Transaction 2 can proceed even with 1 pending
        assert!(table.can_proceed_for_key(&key, 2, LockMode::Read));
    }

    #[test]
    fn test_can_proceed_with_conflict() {
        let mut table = IntentTable::new();
        let key = Bytes::from_static(b"key1");

        // Write followed by write conflicts
        table.declare_intent(key.clone(), 1, LockMode::Write);
        table.declare_intent(key.clone(), 2, LockMode::Write);

        // Transaction 2 cannot proceed while 1 is pending
        assert!(!table.can_proceed_for_key(&key, 2, LockMode::Write));

        // But transaction 1 can proceed (no lower txid)
        assert!(table.can_proceed_for_key(&key, 1, LockMode::Write));
    }

    #[test]
    fn test_read_write_conflict() {
        let mut table = IntentTable::new();
        let key = Bytes::from_static(b"key1");

        table.declare_intent(key.clone(), 1, LockMode::Read);
        table.declare_intent(key.clone(), 2, LockMode::Write);

        // Write cannot proceed while read is pending
        assert!(!table.can_proceed_for_key(&key, 2, LockMode::Write));
    }

    #[test]
    fn test_write_read_conflict() {
        let mut table = IntentTable::new();
        let key = Bytes::from_static(b"key1");

        table.declare_intent(key.clone(), 1, LockMode::Write);
        table.declare_intent(key.clone(), 2, LockMode::Read);

        // Read cannot proceed while write is pending
        assert!(!table.can_proceed_for_key(&key, 2, LockMode::Read));
    }

    #[test]
    fn test_try_acquire_locks() {
        let mut table = IntentTable::new();
        let key1 = Bytes::from_static(b"key1");
        let key2 = Bytes::from_static(b"key2");
        let keys = vec![key1.clone(), key2.clone()];

        // Should succeed
        assert!(table.try_acquire_locks(&keys, LockMode::Write));

        // Should fail (already locked)
        assert!(!table.try_acquire_locks(&keys, LockMode::Write));

        // Release
        table.release_locks(&keys, LockMode::Write);

        // Should succeed again
        assert!(table.try_acquire_locks(&keys, LockMode::Read));
    }

    #[test]
    fn test_multiple_readers() {
        let mut table = IntentTable::new();
        let key = Bytes::from_static(b"key1");
        let keys = vec![key.clone()];

        // Multiple readers should succeed
        assert!(table.try_acquire_locks(&keys, LockMode::Read));
        assert!(table.try_acquire_locks(&keys, LockMode::Read));
        assert!(table.try_acquire_locks(&keys, LockMode::Read));

        // But writer should fail
        assert!(!table.try_acquire_locks(&keys, LockMode::Write));

        // Release all readers
        table.release_locks(&keys, LockMode::Read);
        table.release_locks(&keys, LockMode::Read);
        table.release_locks(&keys, LockMode::Read);

        // Now writer should succeed
        assert!(table.try_acquire_locks(&keys, LockMode::Write));
    }
}
