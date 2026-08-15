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

/// What [`LockTable::try_grant`] decided about one request.
///
/// The two failure shapes are not interchangeable: one is a wait edge that
/// respects the global txid order and the other is a wait edge that inverts
/// it, and only the inverting one can close a cycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrantOutcome {
    /// Locks granted on every requested key.
    Granted,
    /// Blocked by a conflicting intent with a *lower* txid. The requester
    /// waits: the edge points from younger to older, which is the direction
    /// the deadlock-freedom argument allows.
    Blocked,
    /// Blocked only by conflicting *granted* intents belonging to **higher**
    /// txids, named here. Waiting for them would point an edge from older to
    /// younger — the one direction that lets two multi-shard transactions
    /// close a cycle when they take shards in opposite orders — so instead
    /// the requester wounds them and they give way.
    WoundYounger(Vec<u64>),
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
    ///    transaction. Rule 1 has already excluded every lower txid, so what
    ///    rule 2 can still find is exactly a *younger* holder — a higher-txid
    ///    Read that was granted the key before this lower-txid Write arrived.
    ///
    /// # Why a younger holder does not simply block
    ///
    /// Multi-shard transactions declare on each shard through independent
    /// messages and only then wait, so two of them routinely each win a
    /// different shard. If a rule-2 conflict parked the older requester, the
    /// wait-for graph would carry an older→younger edge on one shard and a
    /// younger→older edge on the other: a cycle, exited only by both sides
    /// timing out and retrying into the same interleaving. Reporting the
    /// younger holders as [`GrantOutcome::WoundYounger`] keeps every surviving
    /// wait edge pointing at a lower txid, which no cycle can do.
    ///
    /// A key that is SCA-blocked settles the request as
    /// [`GrantOutcome::Blocked`] immediately — waiting behind an older
    /// transaction is already order-respecting, so there is nothing to gain
    /// from also aborting younger holders on the other keys.
    ///
    /// The checks run over all keys before any flag is set, so a failed
    /// grant leaves no partial state behind.
    ///
    /// Returns [`GrantOutcome::Blocked`] (granting nothing) if the transaction
    /// has no declared intent on one of the keys — callers must declare first.
    pub fn try_grant(&mut self, keys: &[Bytes], txid: u64) -> GrantOutcome {
        let mut younger_holders: Vec<u64> = Vec::new();

        for key in keys {
            let Some(intents) = self.keys.get(key) else {
                debug_assert!(false, "try_grant on key without declared intent");
                return GrantOutcome::Blocked;
            };
            let Some(me) = intents.get(&txid) else {
                debug_assert!(false, "try_grant for txid without declared intent");
                return GrantOutcome::Blocked;
            };
            let mode = me.mode;

            // SCA: conflicting lower-txid intents block, granted or not.
            if !self.can_proceed_for_key(key, txid, mode) {
                return GrantOutcome::Blocked;
            }

            // Holders: only higher txids remain, and only granted ones matter.
            younger_holders.extend(
                intents
                    .range(txid..)
                    .filter(|&(&t, other)| {
                        t != txid && other.granted && Self::conflicts(mode, other.mode)
                    })
                    .map(|(&t, _)| t),
            );
        }

        if !younger_holders.is_empty() {
            younger_holders.sort_unstable();
            younger_holders.dedup();
            return GrantOutcome::WoundYounger(younger_holders);
        }

        for key in keys {
            if let Some(me) = self.keys.get_mut(key).and_then(|m| m.get_mut(&txid)) {
                me.granted = true;
            }
        }
        GrantOutcome::Granted
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
        assert_eq!(table.try_grant(&keys, 1), GrantOutcome::Granted);

        // A second writer cannot be granted while 1 holds both keys.
        table.declare(&keys, 2, LockMode::Write);
        assert_eq!(table.try_grant(&keys, 2), GrantOutcome::Blocked);
        // Failed grant left no partial state: neither key shows a second holder.
        assert_eq!(table.lock_state_string(&keys[0]), "write");
        assert_eq!(table.lock_state_string(&keys[1]), "write");

        // Release 1; 2 can now be granted.
        table.release(&keys, 1);
        assert_eq!(table.try_grant(&keys, 2), GrantOutcome::Granted);
    }

    #[test]
    fn multiple_readers_share_then_writer_waits() {
        let mut table = LockTable::new();
        let keys = vec![key(b"key1")];

        table.declare(&keys, 1, LockMode::Read);
        table.declare(&keys, 2, LockMode::Read);
        table.declare(&keys, 3, LockMode::Read);
        assert_eq!(table.try_grant(&keys, 1), GrantOutcome::Granted);
        assert_eq!(table.try_grant(&keys, 2), GrantOutcome::Granted);
        assert_eq!(table.try_grant(&keys, 3), GrantOutcome::Granted);
        assert_eq!(table.lock_state_string(&keys[0]), "read:3");

        // Writer blocked by the granted readers.
        table.declare(&keys, 4, LockMode::Write);
        assert_eq!(table.try_grant(&keys, 4), GrantOutcome::Blocked);

        // Release all readers; writer can be granted.
        table.release(&keys, 1);
        table.release(&keys, 2);
        table.release(&keys, 3);
        assert_eq!(table.try_grant(&keys, 4), GrantOutcome::Granted);
        assert_eq!(table.lock_state_string(&keys[0]), "write");
    }

    /// The edge wound-wait exists to remove. This test used to assert the
    /// older writer simply *waited* here — an older→younger wait edge, and the
    /// one shape a cross-shard cycle needs.
    // FM-VLL-010
    #[test]
    fn lower_txid_writer_wounds_a_granted_higher_reader() {
        let mut table = LockTable::new();
        let keys = vec![key(b"key1")];

        // Higher txid read arrives first and is granted.
        table.declare(&keys, 5, LockMode::Read);
        assert_eq!(table.try_grant(&keys, 5), GrantOutcome::Granted);

        // Lower txid write passes SCA (no lower intents), so what stands in
        // its way is only the younger holder: it is named for wounding, not
        // waited on.
        table.declare(&keys, 3, LockMode::Write);
        assert!(table.can_proceed_for_key(&keys[0], 3, LockMode::Write));
        assert_eq!(
            table.try_grant(&keys, 3),
            GrantOutcome::WoundYounger(vec![5])
        );

        // The wound is advisory: nothing was granted and nothing released, so
        // the reader still holds the key until its own coordinator unwinds.
        assert_eq!(table.lock_state_string(&keys[0]), "read:1");

        table.release(&keys, 5);
        assert_eq!(table.try_grant(&keys, 3), GrantOutcome::Granted);
    }

    /// Wounding is for the *inverting* edge only. A younger requester behind an
    /// older holder waits, because that edge cannot close a cycle — and an
    /// older transaction must never be aborted by a younger one, which is the
    /// whole priority rule.
    // FM-VLL-010
    #[test]
    fn higher_txid_writer_waits_for_a_granted_lower_reader_without_wounding() {
        let mut table = LockTable::new();
        let keys = vec![key(b"key1")];

        table.declare(&keys, 3, LockMode::Read);
        assert_eq!(table.try_grant(&keys, 3), GrantOutcome::Granted);

        table.declare(&keys, 5, LockMode::Write);
        assert_eq!(table.try_grant(&keys, 5), GrantOutcome::Blocked);
    }

    /// A request blocked by an older transaction on *any* key is a plain wait,
    /// even when younger holders sit on its other keys: it is already parked
    /// behind seniority, so aborting juniors elsewhere buys nothing and costs
    /// their work.
    // FM-VLL-010
    #[test]
    fn an_sca_block_on_one_key_suppresses_wounds_on_the_others() {
        let mut table = LockTable::new();
        let older = key(b"older");
        let younger = key(b"younger");

        // Key `older` is held by a lower txid; key `younger` by a higher one.
        table.declare(std::slice::from_ref(&older), 1, LockMode::Write);
        assert_eq!(
            table.try_grant(std::slice::from_ref(&older), 1),
            GrantOutcome::Granted
        );
        table.declare(std::slice::from_ref(&younger), 9, LockMode::Write);
        assert_eq!(
            table.try_grant(std::slice::from_ref(&younger), 9),
            GrantOutcome::Granted
        );

        let keys = vec![older, younger];
        table.declare(&keys, 5, LockMode::Write);
        assert_eq!(table.try_grant(&keys, 5), GrantOutcome::Blocked);
    }

    /// One wound notice per victim, however many of the requester's keys it
    /// holds — the coordinator only needs telling once.
    // FM-VLL-010
    #[test]
    fn a_victim_holding_several_keys_is_named_once() {
        let mut table = LockTable::new();
        let keys = vec![key(b"key1"), key(b"key2")];

        table.declare(&keys, 9, LockMode::Write);
        assert_eq!(table.try_grant(&keys, 9), GrantOutcome::Granted);

        table.declare(&keys, 4, LockMode::Write);
        assert_eq!(
            table.try_grant(&keys, 4),
            GrantOutcome::WoundYounger(vec![9])
        );
    }

    /// Declared-but-ungranted younger intents are not holders: nothing is in
    /// the older transaction's way, so it takes the lock and wounds no one.
    // FM-VLL-010
    #[test]
    fn a_younger_intent_that_holds_nothing_is_not_wounded() {
        let mut table = LockTable::new();
        let keys = vec![key(b"key1")];

        table.declare(&keys, 9, LockMode::Write);
        table.declare(&keys, 4, LockMode::Write);

        assert_eq!(table.try_grant(&keys, 4), GrantOutcome::Granted);
    }

    #[test]
    fn release_of_ungranted_intent_unblocks_sca() {
        let mut table = LockTable::new();
        let keys = vec![key(b"key1")];

        // 1 is pending (never granted); 2 is blocked behind it by SCA.
        table.declare(&keys, 1, LockMode::Write);
        table.declare(&keys, 2, LockMode::Write);
        assert_eq!(table.try_grant(&keys, 2), GrantOutcome::Blocked);

        // Aborting 1 (same transition as completing) unblocks 2.
        table.release(&keys, 1);
        assert_eq!(table.try_grant(&keys, 2), GrantOutcome::Granted);
    }

    #[test]
    fn duplicate_keys_in_slice_are_idempotent() {
        let mut table = LockTable::new();
        let keys = vec![key(b"key1"), key(b"key1")];

        table.declare(&keys, 1, LockMode::Read);
        assert_eq!(table.try_grant(&keys, 1), GrantOutcome::Granted);
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
        assert_eq!(table.try_grant(&keys, 1), GrantOutcome::Granted);
        assert_eq!(table.lock_state_string(&keys[0]), "write");
        assert_eq!(table.lock_state_string(&key(b"other")), "unlocked");
    }
}
