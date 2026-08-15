//! Per-connection transaction state: the MULTI queue, the watch set, and the
//! slot/shard co-location accumulator.
//!
//! The connection owns one [`TransactionState`] and drives it through the named
//! transitions below; EXEC consumes it with [`TransactionState::take`], which
//! leaves the state clean so no exit path has to clear fields by hand.

use frogdb_core::clock;
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

/// A watched key together with the shard that owns it.
///
/// The shard stays connection-side bookkeeping — [`WatchEntry`] is what the
/// shard itself sees — but EXEC needs it to send each watch's version check to
/// the shard that can actually answer it. A watch whose shard was not folded
/// into the target (a dead watch, see [`TransactionState::take`]) is checked on
/// its own shard rather than on the batch's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchedKey {
    /// Shard owning the key, resolved at WATCH time.
    pub shard_id: usize,
    /// The entry handed to that shard at EXEC.
    pub entry: WatchEntry,
}

/// Snapshot of a transaction captured atomically by EXEC.
///
/// Taking the summary leaves the connection's transaction state clean, so the
/// EXEC handler never needs to clear fields by hand.
#[derive(Debug)]
pub struct TxnSummary {
    /// Queued commands, in submission order.
    pub queue: Vec<ParsedCommand>,
    /// Watched keys with their watch-time version, liveness, and owning shard.
    pub watches: Vec<WatchedKey>,
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
        self.start_time = Some(clock::now());
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
    ///
    /// First watch wins: re-`WATCH`ing a key that is already in the watch set
    /// keeps the *earlier* snapshot, so a write that landed between the two
    /// WATCHes still aborts the EXEC. Overwriting would re-arm the CAS against
    /// the newer version and silently forget that write — a WATCH false
    /// negative. Redis has the same rule from the other side:
    /// `watchForKey()` returns early for an already-watched key, and
    /// `CLIENT_DIRTY_CAS` is cleared only by EXEC/DISCARD/UNWATCH/RESET. The
    /// liveness flag rides along for the same reason: a key watched live and
    /// since expired must not be downgraded to an already-stale watch, which
    /// never aborts. Only the set-clearing transitions ([`Self::unwatch_all`],
    /// [`Self::take`], [`Self::discard`], [`Self::clear`]) let a later WATCH
    /// take a fresh snapshot.
    pub fn watch_key(&mut self, key: Bytes, shard_id: usize, version: u64, live_at_watch: bool) {
        self.watches
            .entry(key)
            .or_insert((shard_id, version, live_at_watch));
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
        //
        // Only *live* watches fold. A watch taken on a key that did not exist
        // (`live_at_watch = false`) has no data on its shard to be atomic with:
        // the only way to break it is to *create* the key, which bumps that
        // shard's slot version, and EXEC checks that version on the watch's own
        // shard whether or not it was folded (`exec.rs`, off-target watch
        // round-trips). Folding it anyway would `-CROSSSLOT` the canonical
        // create-if-absent CAS — `WATCH counter` (absent, shard B) plus a
        // queued write on shard A — which the spec says must commit.
        for &(shard_id, _, live_at_watch) in self.watches.values() {
            if live_at_watch {
                self.slots.fold_shard(shard_id);
            }
        }
        let txn = std::mem::take(self);
        Some(TxnSummary {
            queue: txn.queue.expect("queue presence checked above"),
            watches: txn
                .watches
                .into_iter()
                .map(|(key, (shard_id, version, live_at_watch))| WatchedKey {
                    shard_id,
                    entry: WatchEntry {
                        key,
                        version,
                        live_at_watch,
                    },
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

    // FM-TXN-001
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

    // FM-TXN-020
    #[test]
    fn cross_shard_watch_set_folds_to_multi_at_take() {
        // Both watches are *live*: they name real data on two shards, and the
        // transaction cannot be atomic with respect to both.
        let mut t = TransactionState::default();
        t.watch_key(Bytes::from_static(b"a"), 0, 11, true);
        t.watch_key(Bytes::from_static(b"b"), 1, 22, true);
        t.begin().expect("MULTI after WATCH");
        t.fold_shard(1);

        let summary = t.take(false).expect("in transaction");
        assert!(matches!(summary.target, TransactionTarget::Multi(_)));
        assert!(summary.target.resolve().is_err(), "Multi → CROSSSLOT");
    }

    // FM-TXN-020
    #[test]
    fn a_dead_watch_does_not_fold_its_shard_into_the_target() {
        // The canonical create-if-absent CAS, cross-shard: `WATCH counter` with
        // `counter` absent (shard 1), then a queued write on shard 0. A dead
        // watch has no data on its shard to be atomic with — the only way to
        // break it is to *create* the key, which bumps that shard's slot version
        // and is caught by EXEC's own round-trip to it. Folding it would
        // `-CROSSSLOT` a transaction Redis commits.
        let mut t = TransactionState::default();
        t.watch_key(Bytes::from_static(b"counter"), 1, 7, false);
        t.begin().expect("MULTI after WATCH");
        t.fold_shard(0);

        let summary = t.take(false).expect("in transaction");
        assert!(
            matches!(summary.target, TransactionTarget::Single(0)),
            "a dead watch must not promote the target, got {:?}",
            summary.target
        );
        // The watch is still carried, tagged with the shard EXEC has to check it
        // on — unfolded is not unchecked.
        assert_eq!(summary.watches.len(), 1, "the dead watch is still carried");
        assert_eq!(summary.watches[0].shard_id, 1);
        assert!(!summary.watches[0].entry.live_at_watch);
        assert!(summary.target.resolve().is_ok(), "Single → no CROSSSLOT");
    }

    // FM-TXN-020
    #[test]
    fn one_live_watch_still_folds_alongside_a_dead_one() {
        // Liveness is decided per watch, not per set: the dead watch on shard 2
        // contributes nothing, the live one on shard 1 still promotes the
        // single-shard target to `Multi`. Pins both directions of the filter.
        let mut t = TransactionState::default();
        t.watch_key(Bytes::from_static(b"live"), 1, 11, true);
        t.watch_key(Bytes::from_static(b"dead"), 2, 22, false);
        t.begin().expect("MULTI after WATCH");
        t.fold_shard(0);

        let summary = t.take(false).expect("in transaction");
        match &summary.target {
            TransactionTarget::Multi(shards) => {
                assert!(shards.contains(&0), "the queued command's shard");
                assert!(shards.contains(&1), "the live watch's shard");
                assert!(!shards.contains(&2), "the dead watch's shard must not fold");
            }
            other => panic!("expected Multi from the live watch, got {other:?}"),
        }
    }

    // FM-TXN-050
    #[test]
    fn rewatching_a_key_keeps_the_first_snapshot() {
        let mut t = TransactionState::default();
        t.watch_key(Bytes::from_static(b"k"), 0, 11, true);
        // A writer moved the version and the key died between the two WATCHes.
        // Redis' `watchForKey` no-ops on an already-watched key, so neither the
        // version nor the liveness observation may be laundered away here.
        t.watch_key(Bytes::from_static(b"k"), 0, 22, false);

        t.begin().expect("MULTI after WATCH");
        let summary = t.take(false).expect("in transaction");
        assert_eq!(summary.watches.len(), 1, "one entry per watched key");
        assert_eq!(
            summary.watches[0].entry.version, 11,
            "the first WATCH's version snapshot wins"
        );
        assert!(
            summary.watches[0].entry.live_at_watch,
            "the first WATCH's liveness observation wins"
        );

        // Clearing the watch set does re-arm: the next WATCH is a first watch.
        t.watch_key(Bytes::from_static(b"k"), 0, 33, false);
        t.unwatch_all();
        t.watch_key(Bytes::from_static(b"k"), 0, 44, true);
        t.begin().expect("MULTI after UNWATCH + WATCH");
        let summary = t.take(false).expect("in transaction");
        assert_eq!(
            summary.watches[0].entry.version, 44,
            "UNWATCH lets the next WATCH take a fresh snapshot"
        );
    }

    // FM-TXN-013
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

    // FM-TXN-019
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

    // FM-TXN-042
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

        // Re-folding an already-present shard once in `Multi` must not
        // duplicate it in the shard list.
        acc.fold_shard(1);
        match &acc.target {
            TransactionTarget::Multi(shards) => assert_eq!(
                shards,
                &vec![1, 2],
                "re-folding an existing shard must not duplicate it: {shards:?}"
            ),
            other => panic!("expected Multi, got {other:?}"),
        }

        // A genuinely new shard is appended.
        acc.fold_shard(3);
        match &acc.target {
            TransactionTarget::Multi(shards) => assert_eq!(
                shards,
                &vec![1, 2, 3],
                "a new shard must be appended: {shards:?}"
            ),
            other => panic!("expected Multi, got {other:?}"),
        }
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
    fn accumulator_note_slot_dedupes_shards_once_already_multi() {
        let mut acc = TxnSlotAccumulator::default();
        assert!(!acc.note_slot(100, 0));
        acc.fold_shard(0);
        assert!(acc.note_slot(200, 1));
        match &acc.target {
            TransactionTarget::Multi(shards) => assert_eq!(shards, &vec![0, 1]),
            other => panic!("expected Multi, got {other:?}"),
        }

        // A further cross-slot key on a shard already in the Multi list must
        // not duplicate it. `first_slot` never moves past the first slot
        // seen, so every subsequent differing slot re-enters this arm.
        assert!(acc.note_slot(300, 1));
        match &acc.target {
            TransactionTarget::Multi(shards) => assert_eq!(
                shards,
                &vec![0, 1],
                "re-noting an existing shard must not duplicate it: {shards:?}"
            ),
            other => panic!("expected Multi, got {other:?}"),
        }

        // A genuinely new shard is appended.
        assert!(acc.note_slot(400, 2));
        match &acc.target {
            TransactionTarget::Multi(shards) => assert_eq!(
                shards,
                &vec![0, 1, 2],
                "a new shard must be appended: {shards:?}"
            ),
            other => panic!("expected Multi, got {other:?}"),
        }
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

    // FM-TXN-019
    #[test]
    fn transaction_target_resolve_maps_multi_to_crossslot() {
        assert!(TransactionTarget::None.resolve().is_ok());
        assert!(TransactionTarget::Single(3).resolve().is_ok());
        let err = TransactionTarget::Multi(vec![0, 1]).resolve().unwrap_err();
        // The rejection is byte-identical to the redirect seam.
        assert_eq!(format!("{err:?}"), format!("{:?}", redirect::crossslot()));
    }

    // FM-TXN-008
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

    #[test]
    fn queued_commands_reflects_the_open_queue_and_is_none_when_closed() {
        let mut t = TransactionState::default();
        assert!(t.queued_commands().is_none(), "no transaction open -> None");

        t.begin().unwrap();
        assert!(
            t.queued_commands().is_some_and(|q| q.is_empty()),
            "MULTI with nothing queued yet is Some(&[])"
        );

        t.push_queued_command(cmd(b"GET"));
        t.push_queued_command(cmd(b"SET"));
        let queued = t.queued_commands().expect("transaction open");
        assert_eq!(queued.len(), 2, "both queued commands are visible");
        assert_eq!(queued[0].name, Bytes::from_static(b"GET"));
        assert_eq!(queued[1].name, Bytes::from_static(b"SET"));

        t.take(false);
        assert!(t.queued_commands().is_none(), "EXEC leaves the state clean");
    }

    #[test]
    fn watched_key_iter_reflects_the_watch_set() {
        let mut t = TransactionState::default();
        assert_eq!(t.watched_key_iter().count(), 0, "no watches yet");

        t.watch_key(Bytes::from_static(b"a"), 0, 1, true);
        t.watch_key(Bytes::from_static(b"b"), 1, 2, true);
        let mut keys: Vec<&Bytes> = t.watched_key_iter().collect();
        keys.sort();
        assert_eq!(
            keys,
            vec![&Bytes::from_static(b"a"), &Bytes::from_static(b"b")]
        );

        t.unwatch_all();
        assert_eq!(t.watched_key_iter().count(), 0, "UNWATCH clears it");
    }

    // FM-TXN-014
    #[test]
    fn clear_resets_everything_unconditionally() {
        let mut t = TransactionState::default();
        t.begin().unwrap();
        t.push_queued_command(cmd(b"GET"));
        t.watch_key(Bytes::from_static(b"k"), 0, 1, true);
        t.abort(Some("ERR boom".to_string()));
        assert!(t.is_open());

        t.clear();

        assert!(!t.is_open(), "QUIT/RESET must close any open transaction");
        assert!(t.queued_commands().is_none());
        assert_eq!(t.watched_key_iter().count(), 0, "watches are dropped too");

        // A clean MULTI after clear must not carry over the aborted flag.
        t.begin().unwrap();
        assert!(
            !t.take(false).unwrap().exec_abort,
            "clear must not leave exec_abort latched"
        );
    }
}
