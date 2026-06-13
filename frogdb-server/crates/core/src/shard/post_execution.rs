//! Unified post-execution write-effect pipeline.
//!
//! This module owns the **single canonical ordering** of the side effects that
//! run after a write command (or a batch of write commands) mutates the store.
//! Before this module existed, the ordering was re-stated in four near-identical
//! functions (`run_post_execution`, `run_post_execution_after_wal`,
//! `run_transaction_post_execution`, `run_transaction_post_execution_after_wal`),
//! one per `(WAL phase × scope)` combination. The duplication let the clones
//! drift; the invariant — *which effect happens before which* — lived in four
//! places maintained by hand.
//!
//! Here the ordering lives in exactly one place: [`WRITE_EFFECT_ORDER`], iterated
//! by [`ShardWorker::run_write_effects`]. The two **real** axes of variation are
//! data, not duplicated control flow:
//!
//! - [`WalPhase`] — does the pipeline persist the WAL, or did the caller already
//!   persist + confirm it (rollback mode)?
//! - [`EffectScope`] — a single command, or an atomic MULTI/EXEC batch?
//!
//! ## Divergence verdicts (intentional vs accidental)
//!
//! Re-derived from the four former clones. Intentional differences are kept and
//! expressed as [`EffectScope`] behavior; accidental drift had already been fixed
//! in the codebase before this unification (see commits `6e483280`, `23469adc`)
//! and is simply not re-introduced here.
//!
//! - **Version increment is scope-dependent (intentional).** A single command
//!   skips the increment when `dirty_delta < 0` (Redis WATCH no-op rule: a write
//!   that changed nothing must not dirty WATCH state). A transaction increments
//!   the version once, unconditionally, per `EXEC` — matching the prior batched
//!   clones and Redis's "one dirty bump framing per transaction" semantics.
//! - **Tracking invalidation, keyspace notifications, waiter satisfaction, WAL,
//!   search, broadcast all run per-write (intentional batching).** For a single
//!   command the batch is of one. Replication framing is the one place batching
//!   is observable: a command broadcasts itself; a transaction broadcasts a
//!   MULTI/EXEC-wrapped group (replicas must not observe intermediate state).
//! - **FLUSHDB/FLUSHALL tracking special case (intentional, now uniform).**
//!   These commands invalidate *everything* (keyless), so they need
//!   `flush_all_tracking()` rather than key-based invalidation. Single-command
//!   FLUSHDB never reaches this pipeline (it is `ExecutionStrategy::ServerWide`
//!   and routes through `scatter_flushdb`), so folding the special case into
//!   [`ShardWorker::invalidate_written_keys`] is a no-op for `Command` scope and
//!   correct for `Transaction` scope.
//! - **Keyspace hit/miss metrics are NOT a write effect.** They are recorded at
//!   lookup level for every command (read or write) in `execute_command_inner`
//!   via `record_keyspace_lookups`, so they are absent here by design.

use bytes::Bytes;

use crate::command::{Command, WaiterKind, WaiterWake};
use crate::store::Store;

use super::helpers::REPLICA_INTERNAL_CONN_ID;
use super::worker::ShardWorker;

/// What a (possibly batched) execution wrote — the input to the effect pipeline.
pub(crate) struct WriteSummary<'a> {
    /// Write commands in execution order: `(handler, args)`. Exactly one entry
    /// for a plain command; all write commands of the transaction for MULTI/EXEC.
    pub writes: &'a [(&'a dyn Command, &'a [Bytes])],
    /// Accumulated dirty delta. A negative value means "no data actually
    /// changed" (e.g. DEL on an already-expired key) — see the WATCH no-op rule.
    pub dirty_delta: i64,
    /// Originating connection. `REPLICA_INTERNAL_CONN_ID` suppresses broadcast
    /// (replication loop guard).
    pub conn_id: u64,
}

/// Whether the pipeline persists the WAL, or the caller already did so.
///
/// In rollback mode the caller runs `persist_and_confirm` /
/// `persist_transaction_to_wal` *before* invoking the pipeline and rolls back on
/// failure, so the pipeline must not persist again.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum WalPhase {
    /// The pipeline performs WAL persistence (default path).
    Persist,
    /// WAL already persisted and confirmed by the caller (rollback path).
    AlreadyPersisted,
}

/// Batching scope: per-command effects vs atomic transaction effects.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum EffectScope {
    /// A single write command. The summary carries exactly one write.
    Command,
    /// A MULTI/EXEC batch. Version increment and replication framing are atomic.
    Transaction,
}

/// One step in the canonical write-effect order.
///
/// The variants are dispatched by [`ShardWorker::run_write_effects`]; the order
/// they run in is [`WRITE_EFFECT_ORDER`] and nowhere else.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum WriteEffectKind {
    /// Bump the shard version for WATCH detection (scope-dependent condition).
    VersionIncrement,
    /// Invalidate client-tracking caches for written keys (or flush-all).
    TrackingInvalidation,
    /// Emit keyspace/keyevent notifications, per write in execution order.
    KeyspaceNotifications,
    /// Advance the dirty counter by the accumulated delta.
    DirtyCounter,
    /// Wake blocking waiters (BLPOP/BZPOPMIN/XREAD …); may mutate via get_mut.
    WaiterSatisfaction,
    /// Flush keysize histogram refreshes from waiter-driven mutations.
    KeysizesFlush,
    /// Persist the write to the WAL (skipped when already persisted).
    WalPersistence,
    /// Update search indexes, per write.
    SearchIndex,
    /// Broadcast to replicas (per command, or MULTI/EXEC-framed for a batch).
    ReplicationBroadcast,
}

/// THE canonical write-effect order. The single source of truth for the
/// post-execution invariant. [`ShardWorker::run_write_effects`] iterates this
/// slice; changing the order is one edit here that every caller inherits.
///
/// Ordering rationale (the dangerous part — see module docs):
/// 1. version first so WATCH/dirty state settles before any observer reads it;
/// 2. tracking invalidation before replication broadcast so RESP3 caching
///    clients are invalidated on the same ordering replicas observe the write
///    (otherwise rollback-mode vs default-mode race differently);
/// 3. keysizes flush immediately after waiter satisfaction to capture the
///    `get_mut()` mutations made while waking waiters;
/// 4. replication broadcast is terminal.
pub(crate) const WRITE_EFFECT_ORDER: [WriteEffectKind; 9] = [
    WriteEffectKind::VersionIncrement,
    WriteEffectKind::TrackingInvalidation,
    WriteEffectKind::KeyspaceNotifications,
    WriteEffectKind::DirtyCounter,
    WriteEffectKind::WaiterSatisfaction,
    WriteEffectKind::KeysizesFlush,
    WriteEffectKind::WalPersistence,
    WriteEffectKind::SearchIndex,
    WriteEffectKind::ReplicationBroadcast,
];

impl ShardWorker {
    /// Run the canonical post-execution write effects for a command or batch.
    ///
    /// This is the **only** statement of the write-effect ordering invariant.
    /// It iterates [`WRITE_EFFECT_ORDER`] and applies each effect; the two axes
    /// of variation (`wal`, `scope`) are consumed as data, replacing the four
    /// former clone functions.
    ///
    /// WAL persistence is the only suspension point: every effect before it runs
    /// synchronously on the shard task, and the per-command WAL loop must stay
    /// sequential (per-command order is part of the replay contract).
    pub(crate) async fn run_write_effects(
        &mut self,
        summary: WriteSummary<'_>,
        wal: WalPhase,
        scope: EffectScope,
    ) {
        if summary.writes.is_empty() {
            return;
        }
        debug_assert!(
            !matches!(scope, EffectScope::Command) || summary.writes.len() == 1,
            "EffectScope::Command must carry exactly one write, got {}",
            summary.writes.len()
        );

        for effect in WRITE_EFFECT_ORDER {
            match effect {
                WriteEffectKind::VersionIncrement => {
                    // Scope-dependent (intentional — see module docs):
                    //   Command:     skip when dirty_delta < 0 (WATCH no-op rule)
                    //   Transaction: unconditional, once per EXEC
                    match scope {
                        EffectScope::Command => {
                            if summary.dirty_delta >= 0 {
                                self.increment_version();
                            }
                        }
                        EffectScope::Transaction => self.increment_version(),
                    }
                }
                WriteEffectKind::TrackingInvalidation => {
                    self.invalidate_written_keys(summary.writes, summary.conn_id);
                }
                WriteEffectKind::KeyspaceNotifications => {
                    for &(handler, args) in summary.writes {
                        self.emit_keyspace_notifications_for_command(handler, args);
                    }
                }
                WriteEffectKind::DirtyCounter => {
                    self.update_dirty_counter(summary.dirty_delta);
                }
                WriteEffectKind::WaiterSatisfaction => {
                    for &(handler, args) in summary.writes {
                        self.satisfy_waiters_for_command(handler, args);
                    }
                }
                WriteEffectKind::KeysizesFlush => {
                    self.store.flush_keysizes_refreshes();
                }
                WriteEffectKind::WalPersistence => {
                    if matches!(wal, WalPhase::Persist) {
                        for &(handler, args) in summary.writes {
                            self.persist_by_strategy(handler, args).await;
                        }
                    }
                }
                WriteEffectKind::SearchIndex => {
                    if !self.search.indexes.is_empty() {
                        for &(handler, args) in summary.writes {
                            self.update_search_indexes(handler.name(), args);
                        }
                    }
                }
                WriteEffectKind::ReplicationBroadcast => {
                    if summary.conn_id != REPLICA_INTERNAL_CONN_ID
                        && self.replication_broadcaster.is_active()
                    {
                        // Scope-dependent framing (intentional): a command
                        // broadcasts itself; a transaction broadcasts a
                        // MULTI/EXEC-wrapped group so replicas never observe
                        // intermediate state.
                        match scope {
                            EffectScope::Command => {
                                let (handler, args) = summary.writes[0];
                                self.replication_broadcaster
                                    .broadcast_command(handler.name(), args);
                            }
                            EffectScope::Transaction => {
                                let commands: Vec<(&str, &[Bytes])> = summary
                                    .writes
                                    .iter()
                                    .map(|&(handler, args)| (handler.name(), args))
                                    .collect();
                                self.replication_broadcaster
                                    .broadcast_transaction(&commands);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Invalidate client-tracking caches for the keys written by `writes`.
    ///
    /// Handles both default (key → interested connections) and BCAST (prefix)
    /// tracking modes, plus the FLUSHDB/FLUSHALL special case: those commands
    /// invalidate everything, so they trigger `flush_all_tracking()` instead of
    /// key-based invalidation. The FLUSH branch only fires for `Transaction`
    /// scope in practice (single-command FLUSH routes through `scatter_flushdb`).
    pub(crate) fn invalidate_written_keys(
        &mut self,
        writes: &[(&dyn Command, &[Bytes])],
        conn_id: u64,
    ) {
        if !self.tracking.has_tracking_clients() && self.tracking.broadcast_table.is_empty() {
            return;
        }

        let has_flush = writes
            .iter()
            .any(|&(handler, _)| matches!(handler.name(), "FLUSHDB" | "FLUSHALL"));
        if has_flush && self.tracking.has_tracking_clients() {
            self.tracking.flush_all_tracking();
        } else {
            let mut all_keys: Vec<&[u8]> = Vec::new();
            for &(handler, args) in writes {
                all_keys.extend(handler.keys(args));
            }
            self.tracking.invalidate_keys_all_modes(&all_keys, conn_id);
        }
    }

    fn update_dirty_counter(&mut self, dirty_delta: i64) {
        let dirty_amount = if dirty_delta > 0 {
            dirty_delta as u64
        } else if dirty_delta < 0 {
            0
        } else {
            1 // Default: most write commands count as 1 dirty change
        };
        self.store.increment_dirty(dirty_amount);
    }

    fn satisfy_waiters_for_command(&mut self, handler: &dyn Command, args: &[Bytes]) {
        match handler.spec().wakes {
            WaiterWake::None => {}
            WaiterWake::Kind(kind) => {
                let keys = handler.keys(args);
                self.satisfy_waiters(kind, &keys);
            }
            WaiterWake::All => {
                let keys = handler.keys(args);
                for kind in [WaiterKind::List, WaiterKind::SortedSet, WaiterKind::Stream] {
                    self.satisfy_waiters(kind, &keys);
                }
            }
        }
    }

    fn satisfy_waiters(&mut self, kind: WaiterKind, keys: &[&[u8]]) {
        for key in keys {
            let key_bytes = Bytes::copy_from_slice(key);
            match kind {
                WaiterKind::List => self.try_satisfy_list_waiters(&key_bytes),
                WaiterKind::SortedSet => self.try_satisfy_zset_waiters(&key_bytes),
                WaiterKind::Stream => self.try_satisfy_stream_waiters(&key_bytes),
            }
        }
    }

    async fn persist_by_strategy(&self, handler: &dyn Command, args: &[Bytes]) {
        if self.persistence.wal_writer.is_none() {
            return;
        }

        for action in handler.wal_actions(args) {
            let _ = self
                .execute_wal_action(&action)
                .await
                .inspect_err(|e| tracing::error!(error = %e, "WAL persist failed"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn position(kind: WriteEffectKind) -> usize {
        WRITE_EFFECT_ORDER
            .iter()
            .position(|&k| k == kind)
            .expect("effect must be present in WRITE_EFFECT_ORDER")
    }

    /// Pins the exact canonical order. A reorder must update this test
    /// consciously — the invariant is no longer "a comment four functions agree
    /// on" but a failing test.
    #[test]
    fn canonical_order_is_exact() {
        use WriteEffectKind::*;
        assert_eq!(
            WRITE_EFFECT_ORDER,
            [
                VersionIncrement,
                TrackingInvalidation,
                KeyspaceNotifications,
                DirtyCounter,
                WaiterSatisfaction,
                KeysizesFlush,
                WalPersistence,
                SearchIndex,
                ReplicationBroadcast,
            ]
        );
    }

    /// Every effect kind runs exactly once: guards against a kind being dropped
    /// from (or duplicated in) the canonical order.
    #[test]
    fn every_effect_appears_exactly_once() {
        use WriteEffectKind::*;
        for kind in [
            VersionIncrement,
            TrackingInvalidation,
            KeyspaceNotifications,
            DirtyCounter,
            WaiterSatisfaction,
            KeysizesFlush,
            WalPersistence,
            SearchIndex,
            ReplicationBroadcast,
        ] {
            assert_eq!(
                WRITE_EFFECT_ORDER.iter().filter(|&&k| k == kind).count(),
                1,
                "{kind:?} must appear exactly once in WRITE_EFFECT_ORDER"
            );
        }
        assert_eq!(WRITE_EFFECT_ORDER.len(), 9);
    }

    /// The safety-relevant relative orderings the module docs justify. These
    /// would catch a reorder that compiles but breaks a correctness invariant.
    #[test]
    fn safety_ordering_invariants() {
        // Version increment is first: WATCH/dirty state settles before any
        // observer (tracking, replicas) can read it.
        assert_eq!(position(WriteEffectKind::VersionIncrement), 0);

        // Tracking invalidation precedes replication broadcast: caching clients
        // are invalidated on the same ordering replicas observe the write.
        assert!(
            position(WriteEffectKind::TrackingInvalidation)
                < position(WriteEffectKind::ReplicationBroadcast)
        );

        // Dirty counter is bumped before waiters wake.
        assert!(
            position(WriteEffectKind::DirtyCounter) < position(WriteEffectKind::WaiterSatisfaction)
        );

        // Keysizes flush immediately follows waiter satisfaction so it captures
        // get_mut() mutations made while waking waiters.
        assert_eq!(
            position(WriteEffectKind::KeysizesFlush),
            position(WriteEffectKind::WaiterSatisfaction) + 1
        );

        // Replication broadcast is the terminal effect.
        assert_eq!(
            position(WriteEffectKind::ReplicationBroadcast),
            WRITE_EFFECT_ORDER.len() - 1
        );
    }
}
