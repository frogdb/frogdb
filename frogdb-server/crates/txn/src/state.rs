//! Per-connection transaction state: the MULTI queue, the watch set, and the
//! slot/shard co-location accumulator.
//!
//! The connection owns one [`TransactionState`] and drives it through the named
//! transitions below; EXEC consumes it with [`TransactionState::take`], which
//! leaves the state clean so no exit path has to clear fields by hand.

use std::collections::HashMap;

use bytes::Bytes;
use frogdb_core::{WatchEntry, redirect, shard_for_key, slot_for_key};
use frogdb_protocol::{ParsedCommand, Response};

/// Target shard(s) for a transaction (prepared for future multi-shard support).
#[derive(Debug, Clone, Default)]
pub enum TransactionTarget {
    /// No keys yet - target undetermined.
    #[default]
    None,
    /// Single shard - execute directly.
    Single(usize),
    /// Multiple shards detected - error in Phase 7.1, VLL in future.
    Multi(Vec<usize>),
}

impl TransactionTarget {
    /// EXEC-time resolution. A `Multi` target means the transaction's keys are
    /// not co-located, so it rejects with the CROSSSLOT reply from the redirect
    /// seam (never a fresh literal); `None`/`Single` pass through for the caller
    /// to route.
    #[allow(clippy::result_large_err)]
    pub fn resolve(self) -> Result<TransactionTarget, Response> {
        match self {
            TransactionTarget::Multi(_) => Err(redirect::crossslot()),
            other => Ok(other),
        }
    }
}

/// Folds queued-command keys into a [`TransactionTarget`] during MULTI queuing,
/// promoting `None → Single → Multi`. Single owner of the transaction
/// co-location rule that once lived split across three ad-hoc spots
/// (`note_cluster_slot`, `add_transaction_shard`, and the WATCH loop). In
/// cluster mode a slot mismatch promotes to `Multi` (EXEC then returns
/// CROSSSLOT); in standalone mode a shard mismatch does.
#[derive(Debug, Default)]
struct TxnSlotAccumulator {
    /// First cluster slot seen (cluster mode only), for slot-level detection.
    /// Redis requires all keys in a MULTI/EXEC to hash to the same slot, which
    /// is stricter than shard-level detection.
    first_slot: Option<u16>,
    /// Shard(s) folded so far.
    target: TransactionTarget,
}

impl TxnSlotAccumulator {
    /// Fold one command's keys. `is_cluster` selects slot-level (cluster) vs
    /// shard-level (standalone) mismatch detection.
    fn add_keys<K: AsRef<[u8]>>(&mut self, keys: &[K], num_shards: usize, is_cluster: bool) {
        for key in keys {
            let shard = shard_for_key(key.as_ref(), num_shards);
            // In cluster mode a cross-slot key already forces `Multi`; skip the
            // normal per-shard fold for it.
            if is_cluster && self.note_slot(slot_for_key(key.as_ref()), shard) {
                continue;
            }
            self.fold_shard(shard);
        }
    }

    /// Fold a single already-resolved shard into the target (None → Single →
    /// Multi). Used for a same-shard-validated key set (WATCH).
    fn fold_shard(&mut self, shard_id: usize) {
        self.target = match &self.target {
            TransactionTarget::None => TransactionTarget::Single(shard_id),
            TransactionTarget::Single(s) if *s == shard_id => TransactionTarget::Single(shard_id),
            TransactionTarget::Single(s) => TransactionTarget::Multi(vec![*s, shard_id]),
            TransactionTarget::Multi(shards) => {
                let mut shards = shards.clone();
                if !shards.contains(&shard_id) {
                    shards.push(shard_id);
                }
                TransactionTarget::Multi(shards)
            }
        };
    }

    /// Record a key's slot during cluster-mode queuing. The first slot seen is
    /// remembered; a later key in a different slot promotes the target to
    /// `Multi`. Returns `true` when this key was cross-slot, signalling
    /// [`add_keys`](Self::add_keys) to skip the normal per-shard fold.
    fn note_slot(&mut self, slot: u16, shard_id: usize) -> bool {
        match self.first_slot {
            None => {
                self.first_slot = Some(slot);
                false
            }
            Some(s) if s != slot => {
                self.target = match &self.target {
                    TransactionTarget::None => TransactionTarget::Multi(vec![shard_id]),
                    TransactionTarget::Single(s) => TransactionTarget::Multi(vec![*s, shard_id]),
                    TransactionTarget::Multi(shards) => {
                        let mut shards = shards.clone();
                        if !shards.contains(&shard_id) {
                            shards.push(shard_id);
                        }
                        TransactionTarget::Multi(shards)
                    }
                };
                true
            }
            _ => false,
        }
    }
}

/// Error returned by a transaction lifecycle transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnError {
    /// MULTI issued while already in a transaction.
    Nested,
}

/// Snapshot of a transaction captured atomically by EXEC.
///
/// Taking the summary leaves the connection's transaction state clean, so the
/// EXEC handler never needs to clear fields by hand.
#[derive(Debug)]
pub struct TxnSummary {
    /// Queued commands, in submission order.
    pub queue: Vec<ParsedCommand>,
    /// Watched keys with their watch-time version and liveness.
    pub watches: Vec<WatchEntry>,
    /// Target shard(s) folded from the queued commands and watches.
    pub target: TransactionTarget,
    /// Whether a command failed during queuing (EXEC must abort).
    pub exec_abort: bool,
    /// The connection's ASKING flag as it stood for the whole MULTI block.
    /// Sticky across queuing and consumed at EXEC, so the batch slot
    /// re-validation can reach the importing-target routing arm.
    pub asking: bool,
    /// When MULTI was issued, for duration metrics.
    pub start_time: Option<std::time::Instant>,
}

/// Lightweight metrics captured by DISCARD.
#[derive(Debug, Clone, Copy)]
pub struct TxnMetrics {
    /// Number of commands that had been queued.
    pub queued_count: usize,
    /// When MULTI was issued, for duration metrics.
    pub start_time: Option<std::time::Instant>,
}

/// State for MULTI/EXEC transactions.
///
/// Fields are private: the connection drives the state through the named
/// transitions below, so the queue/watch/target invariants (in particular
/// "`take` leaves the state clean") live in exactly one place.
#[derive(Debug, Default)]
pub struct TransactionState {
    /// Queue of commands to execute at EXEC time (None = not in transaction).
    queue: Option<Vec<ParsedCommand>>,
    /// Watched keys: key -> (shard_id, version_at_watch_time, live_at_watch).
    /// `live_at_watch` is the `wk->expired` inverse — whether the key was
    /// present and unexpired when watched — carried per key into EXEC via
    /// [`WatchEntry`]. `shard_id` stays connection-side only (it drives the
    /// EXEC target fold, not the wire watch set).
    watches: HashMap<Bytes, (usize, u64, bool)>,
    /// Co-location accumulator: folds queued keys (and watched shards) into the
    /// target shard(s), owning the `Multi`-promotion rule.
    slots: TxnSlotAccumulator,
    /// Whether any queued command had an error (abort at EXEC).
    exec_abort: bool,
    /// Error messages for commands that had syntax errors during queuing.
    queued_errors: Vec<String>,
    /// Start time of the transaction (when MULTI was called).
    start_time: Option<std::time::Instant>,
}

impl TransactionState {
    /// Whether a transaction is open (a command queue exists).
    pub fn is_open(&self) -> bool {
        self.queue.is_some()
    }

    /// Read-only view of the queued commands, if a transaction is open
    /// (for DEBUG MEMORY accounting).
    pub fn queued_commands(&self) -> Option<&[ParsedCommand]> {
        self.queue.as_deref()
    }

    /// Read-only iterator over watched keys (for DEBUG MEMORY accounting).
    pub fn watched_key_iter(&self) -> impl Iterator<Item = &Bytes> {
        self.watches.keys()
    }

    /// Begin a transaction (MULTI). Errors with [`TxnError::Nested`] if one is
    /// already open. Existing watches are preserved (WATCH before MULTI).
    pub fn begin(&mut self) -> Result<(), TxnError> {
        if self.queue.is_some() {
            return Err(TxnError::Nested);
        }
        self.queue = Some(Vec::new());
        self.slots = TxnSlotAccumulator::default();
        self.exec_abort = false;
        self.queued_errors.clear();
        self.start_time = Some(std::time::Instant::now());
        Ok(())
    }

    /// Push a validated command onto the transaction queue (no-op outside a
    /// transaction, matching the historical guard).
    pub fn push_queued_command(&mut self, cmd: ParsedCommand) {
        if let Some(ref mut queue) = self.queue {
            queue.push(cmd);
        }
    }

    /// Mark the transaction poisoned so EXEC aborts. An accompanying error
    /// message, if any, is recorded for diagnostics.
    pub fn abort(&mut self, error: Option<String>) {
        self.exec_abort = true;
        if let Some(error) = error {
            self.queued_errors.push(error);
        }
    }

    /// Fold one queued command's keys into the transaction target. In cluster
    /// mode a slot mismatch promotes the target to `Multi` (EXEC returns
    /// CROSSSLOT); in standalone mode a shard mismatch does. The
    /// [`TxnSlotAccumulator`] owns the rule.
    pub fn fold_keys<K: AsRef<[u8]>>(&mut self, keys: &[K], num_shards: usize, is_cluster: bool) {
        self.slots.add_keys(keys, num_shards, is_cluster);
    }

    /// Fold one already-resolved shard into the transaction target (None →
    /// Single → Multi), for a key set whose home is already known.
    pub fn fold_shard(&mut self, shard_id: usize) {
        self.slots.fold_shard(shard_id);
    }

    /// Record a watched key with its watch-time version, shard, and liveness.
    pub fn watch_key(&mut self, key: Bytes, shard_id: usize, version: u64, live_at_watch: bool) {
        self.watches.insert(key, (shard_id, version, live_at_watch));
    }

    /// Forget all watched keys (UNWATCH).
    pub fn unwatch_all(&mut self) {
        self.watches.clear();
    }

    /// EXEC: take the queue and watches atomically, leaving the transaction
    /// state clean. Returns `None` for EXEC without MULTI.
    ///
    /// `asking` is the connection's MULTI-sticky ASKING flag, folded into the
    /// summary here because EXEC is its last reader; the caller clears its own
    /// copy iff this returns `Some`.
    pub fn take(&mut self, asking: bool) -> Option<TxnSummary> {
        self.queue.as_ref()?;
        // Fold every *live* watch's shard into the target now, at EXEC time.
        // Doing it here (rather than at WATCH or MULTI) keeps the fold in sync
        // with the current watch set: a cross-shard watch set promotes the
        // target to `Multi` (EXEC CROSSSLOT-rejects, so a concurrent write to a
        // watched key on another shard can't be silently missed — a
        // false-negative commit), while an UNWATCH inside MULTI that cleared the
        // watches contributes nothing, leaving no stale fold to spuriously
        // CROSSSLOT an otherwise single-shard EXEC.
        for &(shard_id, _, _) in self.watches.values() {
            self.slots.fold_shard(shard_id);
        }
        let txn = std::mem::take(self);
        Some(TxnSummary {
            queue: txn.queue.expect("queue presence checked above"),
            watches: txn
                .watches
                .into_iter()
                .map(|(key, (_, version, live_at_watch))| WatchEntry {
                    key,
                    version,
                    live_at_watch,
                })
                .collect(),
            target: txn.slots.target,
            exec_abort: txn.exec_abort,
            asking,
            start_time: txn.start_time,
        })
    }

    /// DISCARD: drop the whole transaction including watches. Returns `None` for
    /// DISCARD without MULTI; otherwise lightweight metrics for the caller.
    pub fn discard(&mut self) -> Option<TxnMetrics> {
        let queued_count = self.queue.as_ref()?.len();
        let start_time = self.start_time;
        *self = TransactionState::default();
        Some(TxnMetrics {
            queued_count,
            start_time,
        })
    }

    /// Clear the entire transaction state unconditionally (QUIT / RESET).
    pub fn clear(&mut self) {
        *self = TransactionState::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd(name: &'static [u8]) -> ParsedCommand {
        ParsedCommand {
            name: Bytes::from_static(name),
            args: vec![],
        }
    }

    #[test]
    fn begin_rejects_nesting_and_take_leaves_state_clean() {
        let mut t = TransactionState::default();
        assert!(!t.is_open());
        assert!(t.take(false).is_none(), "EXEC without MULTI");
        assert!(t.discard().is_none(), "DISCARD without MULTI");

        t.begin().expect("first MULTI succeeds");
        assert!(t.is_open());
        assert_eq!(t.begin(), Err(TxnError::Nested));

        t.push_queued_command(cmd(b"GET"));
        let summary = t.take(true).expect("in transaction");
        assert_eq!(summary.queue.len(), 1);
        assert!(summary.asking, "ASKING is carried into the summary");
        assert!(!t.is_open());
    }

    #[test]
    fn cross_shard_watch_set_folds_to_multi_at_take() {
        let mut t = TransactionState::default();
        t.watch_key(Bytes::from_static(b"a"), 0, 11, true);
        t.watch_key(Bytes::from_static(b"b"), 1, 22, true);
        t.begin().expect("MULTI after WATCH");
        t.fold_shard(1);

        let summary = t.take(false).expect("in transaction");
        assert!(matches!(summary.target, TransactionTarget::Multi(_)));
        assert!(summary.target.resolve().is_err(), "Multi → CROSSSLOT");
    }

    #[test]
    fn unwatch_drops_the_stale_cross_shard_fold() {
        let mut t = TransactionState::default();
        t.watch_key(Bytes::from_static(b"a"), 0, 11, true);
        t.begin().expect("MULTI after WATCH");
        t.unwatch_all();
        t.fold_shard(1);

        let summary = t.take(false).expect("in transaction");
        assert!(summary.watches.is_empty());
        assert!(matches!(summary.target, TransactionTarget::Single(1)));
    }

    #[test]
    fn fold_keys_promotes_on_slot_mismatch_in_cluster_mode() {
        // "a" and "b" hash to different CRC16 slots, but may share a shard;
        // cluster mode must still promote to Multi.
        let mut t = TransactionState::default();
        t.begin().expect("MULTI");
        t.fold_keys(&[b"a".as_slice(), b"b".as_slice()], 1, true);
        let summary = t.take(false).expect("in transaction");
        assert!(
            matches!(summary.target, TransactionTarget::Multi(_)),
            "cluster-mode slot mismatch must promote to Multi, got {:?}",
            summary.target
        );
    }

    // ---- TxnSlotAccumulator (transaction co-location owner) --------------

    #[test]
    fn accumulator_shard_fold_none_single_multi() {
        let mut acc = TxnSlotAccumulator::default();
        assert!(matches!(acc.target, TransactionTarget::None));
        acc.fold_shard(1);
        assert!(matches!(acc.target, TransactionTarget::Single(1)));
        // Re-folding the same shard stays Single.
        acc.fold_shard(1);
        assert!(matches!(acc.target, TransactionTarget::Single(1)));
        // A second shard promotes to Multi.
        acc.fold_shard(2);
        assert!(matches!(acc.target, TransactionTarget::Multi(_)));
    }

    #[test]
    fn accumulator_cluster_slot_mismatch_forces_multi() {
        let mut acc = TxnSlotAccumulator::default();
        // First key establishes the slot; not cross-slot.
        assert!(!acc.note_slot(100, 0));
        acc.fold_shard(0);
        // A different slot forces Multi and signals the caller to skip the fold.
        assert!(acc.note_slot(200, 1));
        assert!(matches!(acc.target, TransactionTarget::Multi(_)));
    }

    #[test]
    fn accumulator_same_slot_stays_single() {
        let mut acc = TxnSlotAccumulator::default();
        assert!(!acc.note_slot(100, 3));
        acc.fold_shard(3);
        // Same slot again: not cross-slot, stays Single.
        assert!(!acc.note_slot(100, 3));
        acc.fold_shard(3);
        assert!(matches!(acc.target, TransactionTarget::Single(3)));
    }

    #[test]
    fn transaction_target_resolve_maps_multi_to_crossslot() {
        assert!(TransactionTarget::None.resolve().is_ok());
        assert!(TransactionTarget::Single(3).resolve().is_ok());
        let err = TransactionTarget::Multi(vec![0, 1]).resolve().unwrap_err();
        // The rejection is byte-identical to the redirect seam.
        assert_eq!(format!("{err:?}"), format!("{:?}", redirect::crossslot()));
    }

    #[test]
    fn abort_is_reported_in_the_summary_and_discard_clears_watches() {
        let mut t = TransactionState::default();
        t.begin().unwrap();
        t.push_queued_command(cmd(b"GET"));
        t.abort(Some("ERR boom".to_string()));
        assert!(t.take(false).expect("in transaction").exec_abort);

        t.begin().unwrap();
        t.watch_key(Bytes::from_static(b"k"), 0, 1, true);
        let metrics = t.discard().expect("in transaction");
        assert_eq!(metrics.queued_count, 0);
        t.begin().unwrap();
        assert!(
            t.take(false).unwrap().watches.is_empty(),
            "DISCARD unwatches"
        );
    }
}
