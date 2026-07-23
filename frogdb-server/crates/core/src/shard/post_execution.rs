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
//! - [`EffectScope`] — a single command, an atomic MULTI/EXEC batch, or a
//!   cross-shard scatter part ([`EffectScope::ScatterPart`]).
//!
//! The cross-shard scatter write path (`scatter_mset`, `scatter_del`,
//! `scatter_flushdb`, `scatter_copy_set` in `shard/execution.rs`) used to
//! hand-roll its *own* subset of these effects inline — each in a slightly
//! different combination, silently skipping keyspace notifications, replication
//! broadcast, waiter satisfaction, the dirty counter, and search-index upkeep.
//! It now reconstructs the per-key writes it performed as ordinary command
//! handlers (an `MSET` part as `SET`s, a `DEL`/`UNLINK` part as one `DEL`/
//! `UNLINK` over the keys actually removed, `FLUSHDB` as itself, a cross-shard
//! `COPY` destination as a `RESTORE`) and drives them through this same pipeline
//! under [`EffectScope::ScatterPart`]. There is now exactly one statement of the
//! write-effect set and order for every write path.
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
//!   `flush_all_tracking()` rather than key-based invalidation. FLUSHDB reaches
//!   this pipeline under [`EffectScope::ScatterPart`] (routed via
//!   `scatter_flushdb`) and, within a MULTI/EXEC, under `Transaction`; folding
//!   the special case into [`ShardWorker::invalidate_written_keys`] is a no-op
//!   for the keyed `Command` scope and correct for the other two.

use std::sync::Arc;

use bytes::Bytes;

use crate::command::{
    Command, CommandFlags, ReplicationOverride, ScriptWriteRecord, WaiterKind, WaiterWake,
    WriteRecord,
};
use crate::keyspace_event::KeyspaceEventFlags;
use crate::store::Store;

use super::helpers::REPLICA_INTERNAL_CONN_ID;
use super::worker::ShardWorker;

/// Resolve the replication broadcast form(s) of one write record — the single
/// home of the deterministic-propagation rules:
///
/// - a deposited [`ReplicationOverride::Rewrite`] replaces the verbatim command
///   with the synthesized command(s) naming exactly what the primary did
///   (SPOP → `SREM` of the popped members / `DEL` on a full drain);
/// - a deposited [`ReplicationOverride::Suppress`] (Redis
///   `preventCommandPropagation`) and the static
///   [`CommandFlags::NO_PROPAGATE`] flag ship nothing;
/// - everything else propagates verbatim.
///
/// A `NONDETERMINISTIC` write reaching the verbatim arm is a bug — its
/// re-execution on the replica would diverge — surfaced by debug assertion so
/// any newly flagged command fails tests until it deposits an override (or
/// declares its write a no-op).
fn replication_forms<'r>(
    record: &'r WriteRecord<'r>,
) -> smallvec::SmallVec<[(&'r str, &'r [Bytes]); 1]> {
    match record.repl_override {
        Some(ReplicationOverride::Suppress) => smallvec::SmallVec::new(),
        Some(ReplicationOverride::Rewrite(commands)) => commands
            .iter()
            .map(|cmd| (cmd.name, cmd.args.as_slice()))
            .collect(),
        None => {
            let flags = record.handler.flags();
            if flags.contains(CommandFlags::NO_PROPAGATE) {
                return smallvec::SmallVec::new();
            }
            debug_assert!(
                !flags.contains(CommandFlags::NONDETERMINISTIC),
                "nondeterministic write `{}` must deposit a ReplicationOverride \
                 (rewrite_propagation/suppress_propagation) or declare write_was_noop \
                 instead of propagating verbatim",
                record.handler.name()
            );
            smallvec::smallvec![(record.handler.name(), record.args)]
        }
    }
}

/// What a (possibly batched) execution wrote — the input to the effect pipeline.
pub(crate) struct WriteSummary<'a> {
    /// Write commands in execution order. Exactly one entry for a plain command;
    /// all write commands of the transaction for MULTI/EXEC. Each record carries
    /// the handler, its args, and an optional HLL register delta for the WAL
    /// merge-delta path.
    pub writes: &'a [WriteRecord<'a>],
    /// Accumulated dirty delta. A negative value means "no data actually
    /// changed" (e.g. DEL on an already-expired key) — see the WATCH no-op rule.
    pub dirty_delta: i64,
    /// Originating connection. `REPLICA_INTERNAL_CONN_ID` suppresses broadcast
    /// (replication loop guard).
    pub conn_id: u64,
    /// Per-write removal reason, parallel to [`WriteSummary::writes`], consumed
    /// only under [`EffectScope::InternalRemoval`]: it overrides each synthetic
    /// `DEL`'s keyspace-event class (`expired`/`del`/`evicted`). Empty for every
    /// command/transaction/scatter/script summary (their notification class comes
    /// from the command's own [`EventSpec`](crate::command_spec::EventSpec)).
    pub removal_reasons: &'a [RemovalReason],
}

/// Originating-connection sentinel for engine-initiated removals (active
/// expiry, eviction).
///
/// Must be non-zero so the replication broadcast is **not** suppressed by the
/// `REPLICA_INTERNAL_CONN_ID` (== 0) loop guard — an engine removal that opts
/// into replication (eviction) genuinely originates here and must propagate.
/// `u64::MAX` cannot collide with a real client connection id or with the
/// replica-internal sentinel, so client-tracking invalidation excludes no live
/// client (an engine removal invalidates every interested cache).
pub(crate) const ENGINE_INTERNAL_CONN_ID: u64 = u64::MAX;

/// Why an internal (engine-initiated, non-command) removal happened.
///
/// Selects the keyspace-event class the pipeline emits for a synthetic removal,
/// replacing `DelCommand`'s fixed `del`/GENERIC event when the removal was not a
/// client `DEL`. Carried per-write on [`WriteSummary::removal_reasons`] so a
/// single active-expiry cycle can coalesce whole-key-expired (`Expired`) and
/// field-emptied (`FieldEmptied`) removals into **one** effect batch — one
/// version bump — while still emitting the correct per-key event class.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum RemovalReason {
    /// Whole-key TTL elapsed (active or lazy expiry). Emits `expired` / EXPIRED.
    Expired,
    /// Key removed because its last live hash field expired. Emits `del` /
    /// GENERIC — identical to a client `DEL`, matching Redis (a hash emptied via
    /// field TTL fires a generic `del`, not `expired`).
    FieldEmptied,
    /// Key removed to reclaim memory under maxmemory. Emits `evicted` / EVICTED.
    Evicted,
}

impl RemovalReason {
    /// The keyspace-event name + class this removal reason emits.
    fn keyspace_event(self) -> (&'static str, KeyspaceEventFlags) {
        match self {
            RemovalReason::Expired => ("expired", KeyspaceEventFlags::EXPIRED),
            RemovalReason::FieldEmptied => ("del", KeyspaceEventFlags::GENERIC),
            RemovalReason::Evicted => ("evicted", KeyspaceEventFlags::EVICTED),
        }
    }
}

/// Propagation policy for an internal removal — makes the WAL/replication
/// decision explicit **data** instead of an accidental omission.
///
/// The two dimensions an engine removal needs beyond a user `DEL`:
/// - `wal`: persist the delete tombstone to the FrogDB WAL (durability).
///   `true` for eviction fixes restart-resurrection of evicted non-TTL keys;
///   for expiry it drops the stale RocksDB entry at the source.
/// - `replicate`: broadcast the synthetic `DEL` to replicas. A **policy**
///   choice — FrogDB replicas currently expire independently (role-agnostic
///   active expiry), so expiry defaults to `false` (status quo) and only makes
///   the choice explicit and testable; flipping it toward Redis-style
///   primary-drives-expiry is a deliberate ADR, out of scope here.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct RemovalPropagation {
    /// Persist the delete to the FrogDB WAL.
    pub wal: bool,
    /// Broadcast the synthetic `DEL` to replicas.
    pub replicate: bool,
}

/// Whether the pipeline persists the WAL, or the caller already did so.
///
/// In rollback mode the caller runs `persist(records, Durability::Confirm)`
/// *before* invoking the pipeline and rolls back on failure, so the pipeline
/// must not persist again.
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
    /// A cross-shard scatter part (this shard's slice of MSET/DEL/UNLINK/COPY/
    /// FLUSHDB). The summary carries the synthetic per-key writes this shard
    /// actually performed, reconstructed as ordinary command handlers so they
    /// flow through the *same* effect set + order as the single-command path.
    ///
    /// Differs from [`EffectScope::Command`] only in that it may carry more than
    /// one write, and from [`EffectScope::Transaction`] in framing: a scatter
    /// part is **not** a MULTI/EXEC unit, so its writes replicate as independent
    /// commands (no MULTI/EXEC wrap), and its version bump follows the same
    /// dirty-delta no-op rule as a single command (a scatter part that changed
    /// nothing — e.g. DEL of only-missing keys — carries no writes and short-
    /// circuits before any effect runs).
    ScatterPart,
    /// A batch of keys removed by the engine itself (active expiry or eviction),
    /// not by a client command. Reconstructed as synthetic `DEL`s — one write
    /// record per removal-reason group, over exactly the keys removed — so they
    /// flow through the *same* effect set + order as every other write path.
    ///
    /// Like [`EffectScope::ScatterPart`] it may carry more than one write and
    /// replicates each write independently (no MULTI/EXEC wrap), and its version
    /// bump follows the same dirty-delta no-op rule. It differs in two axes an
    /// engine removal owns that a user `DEL` does not: the per-key notification
    /// class comes from [`WriteSummary::removal_reasons`] (not the `DEL` spec's
    /// `del`/GENERIC), and `propagation` gates the WAL-persist and
    /// replication-broadcast effects.
    InternalRemoval { propagation: RemovalPropagation },
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
                    //   Command/ScatterPart: skip when dirty_delta < 0 (WATCH
                    //                        no-op rule)
                    //   Transaction:         unconditional, once per EXEC
                    let warranted = match scope {
                        EffectScope::Command
                        | EffectScope::ScatterPart
                        | EffectScope::InternalRemoval { .. } => summary.dirty_delta >= 0,
                        EffectScope::Transaction => true,
                    };
                    if warranted {
                        // Slot-granular (proposal 18): bump only the slots of the
                        // written keys, so a watch on a different-slot key on this
                        // shard survives. The keys come from the same
                        // `record.handler.keys(args)` extraction client-tracking
                        // uses one step later (`invalidate_written_keys`). A
                        // warranted bump that names NO key (a whole-DB `FLUSHDB`
                        // write record carries none) falls through to the
                        // shard-wide epoch inside `bump_versions_for`, invalidating
                        // every watch — matching Redis's flush-touches-all.
                        self.bump_versions_for(
                            summary
                                .writes
                                .iter()
                                .flat_map(|record| record.handler.keys(record.args)),
                        );
                    }
                }
                WriteEffectKind::TrackingInvalidation => {
                    self.invalidate_written_keys(summary.writes, summary.conn_id);
                }
                WriteEffectKind::KeyspaceNotifications => match scope {
                    // Engine removals override the synthetic `DEL`'s `del`/GENERIC
                    // event with the reason's class (`expired`/`del`/`evicted`),
                    // per write record (a record's keys share one reason).
                    EffectScope::InternalRemoval { .. } => {
                        for (record, reason) in
                            summary.writes.iter().zip(summary.removal_reasons.iter())
                        {
                            let (name, class) = reason.keyspace_event();
                            for key in &record.handler.keys(record.args) {
                                self.emit_keyspace_notification(key, name, class);
                            }
                        }
                    }
                    _ => {
                        for record in summary.writes {
                            self.emit_keyspace_notifications_for_command(record);
                        }
                    }
                },
                WriteEffectKind::DirtyCounter => {
                    self.update_dirty_counter(summary.dirty_delta);
                }
                WriteEffectKind::WaiterSatisfaction => {
                    for record in summary.writes {
                        self.satisfy_waiters_for_command(record.handler, record.args);
                    }
                }
                WriteEffectKind::KeysizesFlush => {
                    self.store.flush_keysizes_refreshes();
                }
                WriteEffectKind::WalPersistence => {
                    // Internal removals additionally gate WAL persist on their
                    // propagation policy (`wal`): eviction persists the delete
                    // tombstone (durability — fixes restart resurrection), and
                    // the choice is explicit data, not an accidental omission.
                    let persist = matches!(wal, WalPhase::Persist)
                        && match scope {
                            EffectScope::InternalRemoval { propagation } => propagation.wal,
                            _ => true,
                        };
                    if persist {
                        // Hot path: stage every write's actions and log on error;
                        // the flush pipeline owns durability asynchronously.
                        let _ = self
                            .persist(
                                summary.writes,
                                super::persistence::Durability::FireAndForget,
                            )
                            .await;
                    }
                }
                WriteEffectKind::SearchIndex => {
                    if !self.search.indexes.is_empty() {
                        for record in summary.writes {
                            self.apply_reindex(record.handler.spec().reindex, record.args);
                        }
                    }
                }
                WriteEffectKind::ReplicationBroadcast => {
                    // Internal removals gate the broadcast on their propagation
                    // policy (`replicate`): expiry defaults to `false` (replicas
                    // expire independently — documented divergence), eviction to
                    // the policy chosen at the call site.
                    let replicate = match scope {
                        EffectScope::InternalRemoval { propagation } => propagation.replicate,
                        _ => true,
                    };
                    if replicate
                        && summary.conn_id != REPLICA_INTERNAL_CONN_ID
                        && self.replication_broadcaster.is_active()
                    {
                        // Every frame is tagged with the shard the write executed
                        // on, so the replica applies it there instead of
                        // re-deriving routing from `args[0]` (wrong for keyless
                        // commands and MULTI/EXEC framing). A MULTI/EXEC group
                        // runs entirely on this one shard, so the whole group
                        // carries a single origin shard.
                        let shard_id = self.shard_id() as u16;
                        // Scope-dependent framing (intentional): a command
                        // broadcasts itself; a transaction broadcasts a
                        // MULTI/EXEC-wrapped group so replicas never observe
                        // intermediate state; a scatter part broadcasts each of
                        // its (synthetic, per-key) writes independently — it is
                        // not a transaction, so no MULTI/EXEC wrap.
                        //
                        // Every record resolves through `replication_forms`,
                        // the single home of the deterministic-propagation
                        // rules (rewrite override, dynamic suppression, the
                        // NO_PROPAGATE flag).
                        match scope {
                            EffectScope::Command
                            | EffectScope::ScatterPart
                            | EffectScope::InternalRemoval { .. } => {
                                for record in summary.writes {
                                    for (name, args) in replication_forms(record) {
                                        self.replication_broadcaster
                                            .broadcast_command_on_shard(shard_id, name, args);
                                    }
                                }
                            }
                            EffectScope::Transaction => {
                                let commands: Vec<(&str, &[Bytes])> = summary
                                    .writes
                                    .iter()
                                    .flat_map(|record| replication_forms(record))
                                    .collect();
                                // A group whose every write was rewritten away
                                // (suppressed / NO_PROPAGATE) ships nothing —
                                // not an empty MULTI/EXEC frame.
                                if !commands.is_empty() {
                                    self.replication_broadcaster
                                        .broadcast_transaction_on_shard(shard_id, &commands);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Run the canonical write-effect pipeline for a cross-shard scatter part.
    ///
    /// The cross-shard write functions (`scatter_mset`, `scatter_del`,
    /// `scatter_flushdb`, `scatter_copy_set`) mutate the store directly and then
    /// hand this method the per-key writes they performed, reconstructed as
    /// ordinary command handlers (`SET`, `DEL`/`UNLINK`, `FLUSHDB`, `RESTORE`).
    /// Routing them through [`ShardWorker::run_write_effects`] under
    /// [`EffectScope::ScatterPart`] guarantees cross-shard writes emit the exact
    /// same effect set, in the exact same [`WRITE_EFFECT_ORDER`], as the
    /// single-command path — no hand-rolled subset can drift.
    ///
    /// `dirty_delta` follows the usual convention: `>= 0` is the number of
    /// effective changes (also gating the version bump), `< 0` signals a no-op
    /// (nothing changed — used by FLUSHDB of an only-expired keyspace). Scatter
    /// always persists its own WAL here ([`WalPhase::Persist`]); the cross-shard
    /// path has no rollback mode.
    pub(crate) async fn run_scatter_effects(
        &mut self,
        writes: Vec<(Arc<dyn Command>, Vec<Bytes>)>,
        dirty_delta: i64,
        conn_id: u64,
    ) {
        if writes.is_empty() {
            return;
        }
        // Scatter parts never carry an HLL delta (no cross-shard PFADD), so each
        // record routes to a full `Put`/`Delete` via `WriteRecord::new`.
        let write_refs: Vec<WriteRecord<'_>> = writes
            .iter()
            .map(|(handler, args)| {
                WriteRecord::new(handler.as_ref() as &dyn Command, args.as_slice())
            })
            .collect();
        self.run_write_effects(
            WriteSummary {
                writes: &write_refs,
                dirty_delta,
                conn_id,
                removal_reasons: &[],
            },
            WalPhase::Persist,
            EffectScope::ScatterPart,
        )
        .await;
    }

    /// Drive a batch of engine-initiated removals (active expiry, eviction)
    /// through [`WRITE_EFFECT_ORDER`].
    ///
    /// This is the internal-removal counterpart of [`Self::run_scatter_effects`]:
    /// each `(reason, keys)` group is reconstructed as a synthetic `DEL` over
    /// exactly the keys the engine removed, and the whole batch runs the
    /// canonical effect set in one call — so a removal inherits **every** effect
    /// (version, tracking invalidation, keyspace notification, dirty counter,
    /// waiter drain, WAL persist, search-index delete, replication) in the exact
    /// same order as any other write, instead of a hand-rolled partial subset.
    ///
    /// The caller performed the store removal already (the key is gone before
    /// this runs — the synthetic `DEL`'s `DeleteIfMissing` WAL action therefore
    /// writes the tombstone, and its `DeleteKeys` reindex drops the search
    /// entry). Empty groups are skipped; an all-empty batch short-circuits before
    /// any effect. Coalescing all groups into one call means a single cycle bumps
    /// the WATCH version **once**, not once per reason group. `conn_id` is the
    /// engine connection (typically `0`); it must never be
    /// [`REPLICA_INTERNAL_CONN_ID`].
    pub(crate) async fn run_internal_removal_effects(
        &mut self,
        groups: Vec<(RemovalReason, Vec<Bytes>)>,
        propagation: RemovalPropagation,
        conn_id: u64,
    ) {
        // Drop empty reason groups so an expiry cycle that removed only whole-key
        // TTLs (or only field-emptied keys) carries a single write record.
        let groups: Vec<(RemovalReason, Vec<Bytes>)> = groups
            .into_iter()
            .filter(|(_, keys)| !keys.is_empty())
            .collect();
        if groups.is_empty() {
            return;
        }

        // Resolve the always-registered `DEL` handler once; its spec carries the
        // right WAL (`DeleteKeys`), reindex (`DeleteKeys`), waiter-wake (`All` →
        // stream NOGROUP drain), and key extraction (`KeySpec::All`). Only the
        // notification class is overridden, per reason.
        let handler = self.internal_removal_handler();
        let reasons: Vec<RemovalReason> = groups.iter().map(|(reason, _)| *reason).collect();
        let dirty_delta: i64 = groups.iter().map(|(_, keys)| keys.len() as i64).sum();
        // Own the per-group key vecs so the borrowed `WriteRecord`s outlive the
        // effect run (mirrors `run_scatter_effects`).
        let write_args: Vec<Vec<Bytes>> = groups.into_iter().map(|(_, keys)| keys).collect();
        let write_refs: Vec<WriteRecord<'_>> = write_args
            .iter()
            .map(|args| WriteRecord::new(handler.as_ref() as &dyn Command, args.as_slice()))
            .collect();

        self.run_write_effects(
            WriteSummary {
                writes: &write_refs,
                dirty_delta,
                conn_id,
                removal_reasons: &reasons,
            },
            WalPhase::Persist,
            EffectScope::InternalRemoval { propagation },
        )
        .await;
    }

    /// Resolve the `DEL` handler used to reconstruct an internal removal as a
    /// synthetic write in the canonical pipeline.
    ///
    /// `DEL` is an always-registered write command in any running server (the
    /// engine could not accept writes otherwise), so a miss is a
    /// server-construction bug surfaced loudly rather than a silently dropped
    /// removal — mirroring `scatter_write_handler`.
    fn internal_removal_handler(&self) -> Arc<dyn Command> {
        self.registry.get("DEL").unwrap_or_else(|| {
            panic!("internal removal pipeline requires the `DEL` command to be registered")
        })
    }

    /// Run the canonical write-effect pipeline for the effective writes a
    /// script (EVAL/EVALSHA/FCALL) performed via `redis.call`/`redis.pcall`.
    ///
    /// The scripting seam cannot run effects mid-script (the store is mutably
    /// borrowed by the invoker for the whole execution), so
    /// `ScriptInvoker::run_local` records each effective local write as an
    /// owned [`ScriptWriteRecord`] and the shard worker drains them here after
    /// the script completes — Redis parity: a scripted write has exactly the
    /// side effects of a direct one (keyspace notifications, WATCH bump,
    /// tracking invalidation, waiter wake, WAL persistence, replication).
    ///
    /// Scope selection mirrors Redis's effects replication: a single write
    /// propagates as itself ([`EffectScope::Command`]); multiple writes
    /// propagate as an atomic MULTI/EXEC-framed batch
    /// ([`EffectScope::Transaction`]) so replicas never observe intermediate
    /// script state. No-op sub-commands were never recorded, so an entirely
    /// no-op script short-circuits before any effect runs.
    pub(crate) async fn run_script_write_effects(
        &mut self,
        writes: Vec<ScriptWriteRecord>,
        conn_id: u64,
    ) {
        if writes.is_empty() {
            return;
        }
        let dirty_delta: i64 = writes.iter().map(|w| w.dirty_delta).sum();
        let write_refs: Vec<WriteRecord<'_>> = writes.iter().map(|w| w.as_write_record()).collect();
        let scope = if write_refs.len() == 1 {
            EffectScope::Command
        } else {
            EffectScope::Transaction
        };
        self.run_write_effects(
            WriteSummary {
                writes: &write_refs,
                dirty_delta,
                conn_id,
                removal_reasons: &[],
            },
            WalPhase::Persist,
            scope,
        )
        .await;
    }

    /// Invalidate client-tracking caches for the keys written by `writes`.
    ///
    /// Handles both default (key → interested connections) and BCAST (prefix)
    /// tracking modes, plus the FLUSHDB/FLUSHALL special case: those commands
    /// invalidate everything, so they trigger `flush_all_tracking()` instead of
    /// key-based invalidation. The FLUSH branch fires for `ScatterPart` scope
    /// (cross-shard FLUSHDB via `scatter_flushdb`) and for a FLUSHDB inside a
    /// MULTI/EXEC (`Transaction` scope).
    pub(crate) fn invalidate_written_keys(&mut self, writes: &[WriteRecord<'_>], conn_id: u64) {
        if !self.tracking.has_any_tracking_clients() {
            return;
        }

        let has_flush = writes
            .iter()
            .any(|record| matches!(record.handler.name(), "FLUSHDB" | "FLUSHALL"));
        if has_flush && self.tracking.has_tracking_clients() {
            self.tracking.flush_all_tracking();
        } else {
            let mut all_keys: Vec<&[u8]> = Vec::new();
            for record in writes {
                all_keys.extend(record.handler.keys(record.args));
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
}

#[cfg(test)]
mod tests {
    use super::*;

    use WriteEffectKind::*;

    fn position(kind: WriteEffectKind) -> usize {
        WRITE_EFFECT_ORDER
            .iter()
            .position(|&k| k == kind)
            .expect("effect must be present in WRITE_EFFECT_ORDER")
    }

    /// The must-precede relation as data: each pair `(a, b)` asserts `a` runs
    /// strictly before `b`. This is the single written home of the ordering
    /// constraints the module docs justify — validated against
    /// [`WRITE_EFFECT_ORDER`] by [`order_satisfies_all_declared_constraints`], so
    /// a reorder that violates any pair fails, and (with
    /// [`every_effect_declares_a_constraint`]) a newly added effect cannot be
    /// slotted in without declaring how it orders against the rest.
    ///
    /// `VersionIncrement`-first and `ReplicationBroadcast`-terminal are the two
    /// endpoint invariants, checked directly rather than enumerated pairwise
    /// against every other kind.
    const MUST_PRECEDE: &[(WriteEffectKind, WriteEffectKind)] = &[
        // Version settles WATCH/dirty state before any observer reads it.
        (VersionIncrement, TrackingInvalidation),
        (VersionIncrement, ReplicationBroadcast),
        // RESP3 caching clients are invalidated on the same ordering replicas
        // observe the write (otherwise rollback- vs default-mode race differ).
        (TrackingInvalidation, ReplicationBroadcast),
        // Keyspace listeners are notified before blocking waiters are woken.
        (KeyspaceNotifications, WaiterSatisfaction),
        // Dirty counter is bumped before waiters wake.
        (DirtyCounter, WaiterSatisfaction),
        // A write is durable locally (WAL) before the search index reflects the
        // persisted state and before replicas can observe it.
        (WalPersistence, SearchIndex),
        (WalPersistence, ReplicationBroadcast),
    ];

    /// Pairs `(a, b)` where `a` must be *immediately* followed by `b`.
    const MUST_BE_ADJACENT: &[(WriteEffectKind, WriteEffectKind)] =
        &[(WaiterSatisfaction, KeysizesFlush)];

    /// Validates [`WRITE_EFFECT_ORDER`] against **every** declared constraint —
    /// the endpoint invariants plus all [`MUST_PRECEDE`] / [`MUST_BE_ADJACENT`]
    /// pairs — so a reorder that compiles but breaks a correctness invariant
    /// fails here. Subsumes the former hand-picked `safety_ordering_invariants`
    /// asserts by driving them from the relation data instead.
    #[test]
    fn order_satisfies_all_declared_constraints() {
        // Version increment is first: WATCH/dirty state settles before any
        // observer (tracking, replicas) can read it.
        assert_eq!(position(VersionIncrement), 0);
        // Replication broadcast is the terminal effect.
        assert_eq!(position(ReplicationBroadcast), WRITE_EFFECT_ORDER.len() - 1);
        for &(a, b) in MUST_PRECEDE {
            assert!(
                position(a) < position(b),
                "{a:?} must run strictly before {b:?}"
            );
        }
        for &(a, b) in MUST_BE_ADJACENT {
            assert_eq!(
                position(a) + 1,
                position(b),
                "{a:?} must be immediately followed by {b:?}"
            );
        }
    }

    /// Every effect in the canonical order participates in at least one declared
    /// ordering constraint. Adding a [`WriteEffectKind`] to
    /// [`WRITE_EFFECT_ORDER`] without declaring a [`MUST_PRECEDE`] /
    /// [`MUST_BE_ADJACENT`] pair for it fails here — the ordering rule can no
    /// longer be added by comment alone.
    #[test]
    fn every_effect_declares_a_constraint() {
        for kind in WRITE_EFFECT_ORDER {
            let constrained = MUST_PRECEDE
                .iter()
                .chain(MUST_BE_ADJACENT)
                .any(|&(a, b)| a == kind || b == kind);
            assert!(constrained, "{kind:?} has no declared ordering constraint");
        }
    }

    /// Pins the exact canonical order. A reorder must update this test
    /// consciously — the invariant is no longer "a comment four functions agree
    /// on" but a failing test.
    #[test]
    fn canonical_order_is_exact() {
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

    // ------------------------------------------------------------------------
    // Scatter-path pipeline pins.
    //
    // The cross-shard scatter write path drives THIS pipeline (same
    // WRITE_EFFECT_ORDER) under EffectScope::ScatterPart. These tests pin the
    // two ways ScatterPart differs from the single-command scope — it may carry
    // more than one write, and it frames replication as independent commands
    // (no MULTI/EXEC wrap) — so the shared ordering above genuinely covers the
    // scatter path.
    // ------------------------------------------------------------------------

    use std::sync::Mutex;
    use std::sync::atomic::AtomicU64;

    use tokio::sync::mpsc;

    use crate::command::{Arity, CommandContext, CommandFlags, WalStrategy};
    use crate::command_spec::{AccessSpec, CommandSpec, EventSpec, KeySpec, LookupSpec};
    use crate::eviction::EvictionConfig;
    use crate::noop::NoopMetricsRecorder;
    use crate::registry::CommandRegistry;
    use crate::replication::{ReplicationBroadcaster, SharedBroadcaster};
    use crate::shard::message::{ShardReceiver, ShardSender};
    use frogdb_protocol::{ParsedCommand, ProtocolVersion, Response};

    #[derive(Default)]
    struct RecordingBroadcaster {
        commands: Mutex<Vec<(String, Vec<Bytes>)>>,
    }

    impl ReplicationBroadcaster for RecordingBroadcaster {
        fn broadcast_command_on_shard(
            &self,
            _shard_id: u16,
            cmd_name: &str,
            args: &[Bytes],
        ) -> u64 {
            let mut g = self.commands.lock().unwrap();
            g.push((cmd_name.to_string(), args.to_vec()));
            g.len() as u64
        }
        fn is_active(&self) -> bool {
            true
        }
        fn current_offset(&self) -> u64 {
            self.commands.lock().unwrap().len() as u64
        }
    }

    struct MockWrite;
    impl Command for MockWrite {
        fn spec(&self) -> &'static CommandSpec {
            static SPEC: CommandSpec = CommandSpec {
                name: "SET",
                arity: Arity::AtLeast(2),
                flags: CommandFlags::WRITE,
                keys: KeySpec::First,
                access: AccessSpec::Uniform,
                wal: WalStrategy::NoOp,
                wakes: WaiterWake::None,
                event: EventSpec::Suppressed,
                requires_same_slot: false,
                reindex: crate::command_spec::ReindexSpec::None,
                lookup: LookupSpec::None,
                mutation: crate::command::ConnMutation::None,
                strategy: crate::command::ExecutionStrategy::Standard,
            };
            &SPEC
        }
        fn execute(
            &self,
            _ctx: &mut CommandContext,
            _args: &[Bytes],
        ) -> Result<Response, frogdb_types::CommandError> {
            Ok(Response::ok())
        }
    }

    struct MockNoopWrite;
    impl Command for MockNoopWrite {
        fn spec(&self) -> &'static CommandSpec {
            static SPEC: CommandSpec = CommandSpec {
                name: "NOOPW",
                arity: Arity::AtLeast(1),
                flags: CommandFlags::WRITE,
                keys: KeySpec::First,
                access: AccessSpec::Uniform,
                wal: WalStrategy::NoOp,
                wakes: WaiterWake::None,
                event: EventSpec::Suppressed,
                requires_same_slot: false,
                reindex: crate::command_spec::ReindexSpec::None,
                lookup: LookupSpec::None,
                mutation: crate::command::ConnMutation::None,
                strategy: crate::command::ExecutionStrategy::Standard,
            };
            &SPEC
        }
        fn execute(
            &self,
            ctx: &mut CommandContext,
            _args: &[Bytes],
        ) -> Result<Response, frogdb_types::CommandError> {
            ctx.effects.write_was_noop = true; // verified: nothing changed
            Ok(Response::Integer(0))
        }
    }

    fn worker_with(bc: SharedBroadcaster) -> ShardWorker {
        let (msg_tx, msg_rx) = mpsc::channel(16);
        let (_conn_tx, conn_rx) = mpsc::channel(16);
        ShardWorker::with_eviction(
            0,
            1,
            ShardReceiver::new(msg_rx),
            conn_rx,
            Arc::new(vec![ShardSender::new(msg_tx)]),
            Arc::new(CommandRegistry::new()),
            EvictionConfig::default(),
            Arc::new(NoopMetricsRecorder::new()),
            Arc::new(AtomicU64::new(0)),
            bc,
        )
    }

    fn worker_with_registry(
        bc: SharedBroadcaster,
        f: impl FnOnce(&mut CommandRegistry),
    ) -> ShardWorker {
        let (msg_tx, msg_rx) = mpsc::channel(16);
        let (_conn_tx, conn_rx) = mpsc::channel(16);
        let mut reg = CommandRegistry::new();
        f(&mut reg);
        ShardWorker::with_eviction(
            0,
            1,
            ShardReceiver::new(msg_rx),
            conn_rx,
            Arc::new(vec![ShardSender::new(msg_tx)]),
            Arc::new(reg),
            EvictionConfig::default(),
            Arc::new(NoopMetricsRecorder::new()),
            Arc::new(AtomicU64::new(0)),
            bc,
        )
    }

    /// A write that declares itself a no-op skips the entire write-effect
    /// pipeline: no replication broadcast and no version bump (WATCH survives).
    #[tokio::test]
    async fn noop_write_skips_all_write_effects() {
        let bc = Arc::new(RecordingBroadcaster::default());
        let mut worker = worker_with_registry(bc.clone() as SharedBroadcaster, |r| {
            r.register(MockNoopWrite);
            r.register(MockWrite);
        });

        let noop = ParsedCommand::new(Bytes::from_static(b"NOOPW"), vec![Bytes::from_static(b"k")]);
        worker
            .execute_command(&noop, 1, ProtocolVersion::Resp2, false)
            .await;
        assert_eq!(
            worker.get_key_version(b"k"),
            0,
            "no-op write must not bump version"
        );
        assert!(
            bc.commands.lock().unwrap().is_empty(),
            "no-op write must not replicate"
        );

        // Control: an ordinary write still runs the full pipeline.
        let real = ParsedCommand::new(
            Bytes::from_static(b"SET"),
            vec![Bytes::from_static(b"k"), Bytes::from_static(b"v")],
        );
        worker
            .execute_command(&real, 1, ProtocolVersion::Resp2, false)
            .await;
        assert_eq!(worker.get_key_version(b"k"), 1);
        assert_eq!(bc.commands.lock().unwrap().len(), 1);
    }

    /// A scatter part carrying several writes bumps the version exactly once and
    /// replicates each write as an independent command — NOT wrapped in
    /// MULTI/EXEC (a scatter part is not a transaction).
    #[tokio::test]
    async fn scatter_part_broadcasts_each_write_without_multi_exec() {
        let bc = Arc::new(RecordingBroadcaster::default());
        let mut worker = worker_with(bc.clone() as SharedBroadcaster);

        let handler: Arc<dyn Command> = Arc::new(MockWrite);
        let writes = vec![
            (
                handler.clone(),
                vec![Bytes::from_static(b"a"), Bytes::from_static(b"1")],
            ),
            (
                handler.clone(),
                vec![Bytes::from_static(b"b"), Bytes::from_static(b"2")],
            ),
        ];
        worker.run_scatter_effects(writes, 2, 42).await;

        // Each written key's slot bumped once (a and b are different slots).
        assert_eq!(worker.get_key_version(b"a"), 1);
        assert_eq!(worker.get_key_version(b"b"), 1);

        // Two independent broadcasts, no MULTI/EXEC framing.
        let cmds = bc.commands.lock().unwrap();
        assert_eq!(
            cmds.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
            ["SET", "SET"]
        );
        assert!(
            !cmds.iter().any(|(n, _)| n == "MULTI" || n == "EXEC"),
            "a scatter part must not be wrapped in MULTI/EXEC"
        );
    }

    /// A scatter part whose dirty delta is negative (nothing effectively
    /// changed — e.g. FLUSHDB of an only-expired keyspace) suppresses the
    /// version bump, matching the single-command WATCH no-op rule.
    #[tokio::test]
    async fn scatter_part_negative_dirty_skips_version_bump() {
        let bc = Arc::new(RecordingBroadcaster::default());
        let mut worker = worker_with(bc.clone() as SharedBroadcaster);

        let handler: Arc<dyn Command> = Arc::new(MockWrite);
        let writes = vec![(
            handler,
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"1")],
        )];
        worker.run_scatter_effects(writes, -1, 42).await;

        assert_eq!(
            worker.get_key_version(b"a"),
            0,
            "negative dirty delta must not bump version"
        );
        // The write still replicates (the effect happened; only WATCH-dirtying
        // is suppressed).
        assert_eq!(bc.commands.lock().unwrap().len(), 1);
    }

    // ------------------------------------------------------------------------
    // Deterministic-propagation pins (`replication_forms`).
    //
    // The replication-broadcast effect resolves every record through
    // `replication_forms`: a deposited `ReplicationOverride::Rewrite` replaces
    // the verbatim command (SPOP → SREM/DEL), `Suppress` and the static
    // NO_PROPAGATE flag ship nothing, and everything else propagates verbatim.
    // These tests pin each rule, including the substitution inside MULTI/EXEC
    // framing, against the same pipeline the real write path runs.
    // ------------------------------------------------------------------------

    use crate::command::{ReplicationOverride, SynthesizedCommand};

    /// A WRITE command statically flagged NO_PROPAGATE: local effects run,
    /// replicas never see it.
    struct MockNoPropagateWrite;
    impl Command for MockNoPropagateWrite {
        fn spec(&self) -> &'static CommandSpec {
            static SPEC: CommandSpec = CommandSpec {
                name: "NOPROPW",
                arity: Arity::AtLeast(2),
                flags: CommandFlags::WRITE.union(CommandFlags::NO_PROPAGATE),
                keys: KeySpec::First,
                access: AccessSpec::Uniform,
                wal: WalStrategy::NoOp,
                wakes: WaiterWake::None,
                event: EventSpec::Suppressed,
                requires_same_slot: false,
                reindex: crate::command_spec::ReindexSpec::None,
                lookup: LookupSpec::None,
                mutation: crate::command::ConnMutation::None,
                strategy: crate::command::ExecutionStrategy::Standard,
            };
            &SPEC
        }
        fn execute(
            &self,
            _ctx: &mut CommandContext,
            _args: &[Bytes],
        ) -> Result<Response, frogdb_types::CommandError> {
            Ok(Response::ok())
        }
    }

    /// A WRITE command flagged NONDETERMINISTIC (SPOP's class): the broadcast
    /// path requires it to carry an override.
    struct MockNondeterministicWrite;
    impl Command for MockNondeterministicWrite {
        fn spec(&self) -> &'static CommandSpec {
            static SPEC: CommandSpec = CommandSpec {
                name: "RANDW",
                arity: Arity::AtLeast(1),
                flags: CommandFlags::WRITE.union(CommandFlags::NONDETERMINISTIC),
                keys: KeySpec::First,
                access: AccessSpec::Uniform,
                wal: WalStrategy::NoOp,
                wakes: WaiterWake::None,
                event: EventSpec::Suppressed,
                requires_same_slot: false,
                reindex: crate::command_spec::ReindexSpec::None,
                lookup: LookupSpec::None,
                mutation: crate::command::ConnMutation::None,
                strategy: crate::command::ExecutionStrategy::Standard,
            };
            &SPEC
        }
        fn execute(
            &self,
            _ctx: &mut CommandContext,
            _args: &[Bytes],
        ) -> Result<Response, frogdb_types::CommandError> {
            Ok(Response::ok())
        }
    }

    fn record_with_override<'a>(
        handler: &'a dyn Command,
        args: &'a [Bytes],
        repl_override: Option<&'a ReplicationOverride>,
    ) -> WriteRecord<'a> {
        WriteRecord {
            handler,
            args,
            hll_wal_delta: None,
            keyspace_events: &[],
            newly_created_keys: &[],
            repl_override,
        }
    }

    /// A deposited rewrite replaces the verbatim broadcast with the
    /// synthesized command(s), in deposit order — the SPOP → SREM/DEL shape.
    #[tokio::test]
    async fn rewrite_override_replaces_verbatim_broadcast() {
        let bc = Arc::new(RecordingBroadcaster::default());
        let mut worker = worker_with(bc.clone() as SharedBroadcaster);

        let handler = MockNondeterministicWrite;
        let args = vec![Bytes::from_static(b"k")];
        let ov = ReplicationOverride::Rewrite(smallvec::smallvec![SynthesizedCommand {
            name: "SREM",
            args: vec![Bytes::from_static(b"k"), Bytes::from_static(b"m1")],
        }]);
        let record = record_with_override(&handler, &args, Some(&ov));
        worker
            .run_write_effects(
                WriteSummary {
                    writes: std::slice::from_ref(&record),
                    dirty_delta: 1,
                    conn_id: 1,
                    removal_reasons: &[],
                },
                WalPhase::Persist,
                EffectScope::Command,
            )
            .await;

        let cmds = bc.commands.lock().unwrap();
        assert_eq!(cmds.len(), 1, "exactly the synthesized command is shipped");
        assert_eq!(cmds[0].0, "SREM");
        assert_eq!(
            cmds[0].1,
            vec![Bytes::from_static(b"k"), Bytes::from_static(b"m1")]
        );
        drop(cmds);
        // Local effects still ran from the original record.
        assert_eq!(worker.get_key_version(b"k"), 1);
    }

    /// A rewrite may synthesize several commands; all ship, in order.
    #[tokio::test]
    async fn rewrite_override_ships_all_synthesized_commands_in_order() {
        let bc = Arc::new(RecordingBroadcaster::default());
        let mut worker = worker_with(bc.clone() as SharedBroadcaster);

        let handler = MockNondeterministicWrite;
        let args = vec![Bytes::from_static(b"k")];
        let ov = ReplicationOverride::Rewrite(smallvec::smallvec![
            SynthesizedCommand {
                name: "SREM",
                args: vec![Bytes::from_static(b"k"), Bytes::from_static(b"m1")],
            },
            SynthesizedCommand {
                name: "DEL",
                args: vec![Bytes::from_static(b"k")],
            },
        ]);
        let record = record_with_override(&handler, &args, Some(&ov));
        worker
            .run_write_effects(
                WriteSummary {
                    writes: std::slice::from_ref(&record),
                    dirty_delta: 1,
                    conn_id: 1,
                    removal_reasons: &[],
                },
                WalPhase::Persist,
                EffectScope::Command,
            )
            .await;

        let cmds = bc.commands.lock().unwrap();
        assert_eq!(
            cmds.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
            ["SREM", "DEL"]
        );
    }

    /// `Suppress` (dynamic `preventCommandPropagation`) ships nothing while
    /// local effects (version bump) still run.
    #[tokio::test]
    async fn suppress_override_broadcasts_nothing() {
        let bc = Arc::new(RecordingBroadcaster::default());
        let mut worker = worker_with(bc.clone() as SharedBroadcaster);

        let handler = MockWrite;
        let args = vec![Bytes::from_static(b"k"), Bytes::from_static(b"v")];
        let ov = ReplicationOverride::Suppress;
        let record = record_with_override(&handler, &args, Some(&ov));
        worker
            .run_write_effects(
                WriteSummary {
                    writes: std::slice::from_ref(&record),
                    dirty_delta: 1,
                    conn_id: 1,
                    removal_reasons: &[],
                },
                WalPhase::Persist,
                EffectScope::Command,
            )
            .await;

        assert!(
            bc.commands.lock().unwrap().is_empty(),
            "a suppressed write must not reach replicas"
        );
        assert_eq!(worker.get_key_version(b"k"), 1, "local effects still run");
    }

    /// The static NO_PROPAGATE flag suppresses the broadcast with no override
    /// deposited — the flag is consulted, not informational.
    #[tokio::test]
    async fn no_propagate_flag_suppresses_broadcast() {
        let bc = Arc::new(RecordingBroadcaster::default());
        let mut worker = worker_with(bc.clone() as SharedBroadcaster);

        let handler = MockNoPropagateWrite;
        let args = vec![Bytes::from_static(b"k"), Bytes::from_static(b"v")];
        let record = record_with_override(&handler, &args, None);
        worker
            .run_write_effects(
                WriteSummary {
                    writes: std::slice::from_ref(&record),
                    dirty_delta: 1,
                    conn_id: 1,
                    removal_reasons: &[],
                },
                WalPhase::Persist,
                EffectScope::Command,
            )
            .await;

        assert!(
            bc.commands.lock().unwrap().is_empty(),
            "a NO_PROPAGATE write must not reach replicas"
        );
        assert_eq!(worker.get_key_version(b"k"), 1, "local effects still run");
    }

    /// Inside a MULTI/EXEC group the rewrite substitutes in place: the framing
    /// stays, the rewritten write ships its synthesized form.
    #[tokio::test]
    async fn transaction_framing_substitutes_rewritten_commands() {
        let bc = Arc::new(RecordingBroadcaster::default());
        let mut worker = worker_with(bc.clone() as SharedBroadcaster);

        let set = MockWrite;
        let set_args = vec![Bytes::from_static(b"k"), Bytes::from_static(b"v")];
        let spop_like = MockNondeterministicWrite;
        let spop_args = vec![Bytes::from_static(b"s")];
        let ov = ReplicationOverride::Rewrite(smallvec::smallvec![SynthesizedCommand {
            name: "SREM",
            args: vec![Bytes::from_static(b"s"), Bytes::from_static(b"m")],
        }]);
        let records = [
            record_with_override(&set, &set_args, None),
            record_with_override(&spop_like, &spop_args, Some(&ov)),
        ];
        worker
            .run_write_effects(
                WriteSummary {
                    writes: &records,
                    dirty_delta: 2,
                    conn_id: 1,
                    removal_reasons: &[],
                },
                WalPhase::Persist,
                EffectScope::Transaction,
            )
            .await;

        let cmds = bc.commands.lock().unwrap();
        assert_eq!(
            cmds.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
            ["MULTI", "SET", "SREM", "EXEC"],
            "the rewritten command substitutes inside the MULTI/EXEC frame"
        );
    }

    /// A transaction whose every write is suppressed ships nothing at all —
    /// not an empty MULTI/EXEC frame.
    #[tokio::test]
    async fn fully_suppressed_transaction_ships_no_frames() {
        let bc = Arc::new(RecordingBroadcaster::default());
        let mut worker = worker_with(bc.clone() as SharedBroadcaster);

        let handler = MockWrite;
        let args = vec![Bytes::from_static(b"k"), Bytes::from_static(b"v")];
        let ov = ReplicationOverride::Suppress;
        let records = [record_with_override(&handler, &args, Some(&ov))];
        worker
            .run_write_effects(
                WriteSummary {
                    writes: &records,
                    dirty_delta: 1,
                    conn_id: 1,
                    removal_reasons: &[],
                },
                WalPhase::Persist,
                EffectScope::Transaction,
            )
            .await;

        assert!(
            bc.commands.lock().unwrap().is_empty(),
            "an all-suppressed transaction must not ship MULTI/EXEC framing"
        );
    }

    /// A NONDETERMINISTIC write reaching the verbatim arm is a bug; the debug
    /// assertion surfaces it. (Debug builds only — release ships verbatim
    /// rather than crashing.)
    #[cfg(debug_assertions)]
    #[tokio::test]
    #[should_panic(expected = "must deposit a ReplicationOverride")]
    async fn nondeterministic_write_without_override_panics_in_debug() {
        let bc = Arc::new(RecordingBroadcaster::default());
        let mut worker = worker_with(bc as SharedBroadcaster);

        let handler = MockNondeterministicWrite;
        let args = vec![Bytes::from_static(b"k")];
        let record = record_with_override(&handler, &args, None);
        worker
            .run_write_effects(
                WriteSummary {
                    writes: std::slice::from_ref(&record),
                    dirty_delta: 1,
                    conn_id: 1,
                    removal_reasons: &[],
                },
                WalPhase::Persist,
                EffectScope::Command,
            )
            .await;
    }

    // ------------------------------------------------------------------------
    // Internal-removal (active expiry / eviction) pipeline pins.
    //
    // Engine removals (`run_internal_removal_effects`) reconstruct each removed
    // key as a synthetic `DEL` and drive it through THIS pipeline under
    // `EffectScope::InternalRemoval`. These tests pin the axes an engine removal
    // owns beyond a user `DEL`: the per-reason notification-class override
    // (`expired`/`del`/`evicted`), the propagation policy gating WAL + broadcast,
    // and the single coalesced version bump per cycle — the same
    // `WRITE_EFFECT_ORDER` a command `DEL` runs, so the three removal sources
    // (command DEL / active-expiry / eviction) emit one uniform effect set.
    // ------------------------------------------------------------------------

    use crate::command_spec::ReindexSpec;

    /// Minimal `DEL` stand-in the internal-removal pipeline resolves from the
    /// registry; its spec carries the removal facts (`KeySpec::All`,
    /// `WalStrategy::DeleteKeys`, `ReindexSpec::DeleteKeys`, `WaiterWake::All`).
    /// `execute` is never invoked (the store removal already happened).
    struct MockDel;
    impl Command for MockDel {
        fn spec(&self) -> &'static CommandSpec {
            static SPEC: CommandSpec = CommandSpec {
                name: "DEL",
                arity: Arity::AtLeast(1),
                flags: CommandFlags::WRITE,
                keys: KeySpec::All,
                access: AccessSpec::Uniform,
                wal: WalStrategy::DeleteKeys,
                wakes: WaiterWake::All,
                event: EventSpec::Emits {
                    class: KeyspaceEventFlags::GENERIC,
                    name: "del",
                },
                requires_same_slot: false,
                reindex: ReindexSpec::DeleteKeys,
                lookup: LookupSpec::None,
                mutation: crate::command::ConnMutation::None,
                strategy: crate::command::ExecutionStrategy::Standard,
            };
            &SPEC
        }
        fn execute(
            &self,
            _ctx: &mut CommandContext,
            _args: &[Bytes],
        ) -> Result<Response, frogdb_types::CommandError> {
            Ok(Response::ok())
        }
    }

    fn worker_with_del(bc: SharedBroadcaster) -> ShardWorker {
        worker_with_registry(bc, |r| r.register(MockDel))
    }

    /// Enable every keyspace-event class this suite observes and subscribe `rx`
    /// to the given keyevent channels.
    fn enable_notify_and_subscribe(
        worker: &mut ShardWorker,
        channels: &[&str],
    ) -> mpsc::UnboundedReceiver<crate::pubsub::PubSubMessage> {
        let flags = KeyspaceEventFlags::KEYSPACE
            | KeyspaceEventFlags::KEYEVENT
            | KeyspaceEventFlags::GENERIC
            | KeyspaceEventFlags::EXPIRED
            | KeyspaceEventFlags::EVICTED;
        worker
            .set_notify_keyspace_events(Arc::new(std::sync::atomic::AtomicU32::new(flags.bits())));
        let (tx, rx) = mpsc::unbounded_channel();
        for ch in channels {
            worker
                .subscriptions
                .subscribe(Bytes::from((*ch).to_string()), 1, tx.clone());
        }
        rx
    }

    fn keyevents(
        rx: &mut mpsc::UnboundedReceiver<crate::pubsub::PubSubMessage>,
    ) -> Vec<(String, String)> {
        let mut out = Vec::new();
        while let Ok(crate::pubsub::PubSubMessage::Message { channel, payload }) = rx.try_recv() {
            out.push((
                String::from_utf8_lossy(&channel).into_owned(),
                String::from_utf8_lossy(&payload).into_owned(),
            ));
        }
        out
    }

    /// The headline: a single `Evicted` removal runs the full canonical effect
    /// set — version bump (once), `evicted` notification, and a replicated
    /// synthetic `DEL` over exactly the removed key.
    #[tokio::test]
    async fn internal_removal_evicted_runs_full_effect_set() {
        let bc = Arc::new(RecordingBroadcaster::default());
        let mut worker = worker_with_del(bc.clone() as SharedBroadcaster);
        let mut rx = enable_notify_and_subscribe(&mut worker, &["__keyevent@0__:evicted"]);

        worker
            .run_internal_removal_effects(
                vec![(RemovalReason::Evicted, vec![Bytes::from_static(b"k")])],
                RemovalPropagation {
                    wal: true,
                    replicate: true,
                },
                ENGINE_INTERNAL_CONN_ID,
            )
            .await;

        assert_eq!(
            worker.get_key_version(b"k"),
            1,
            "one version bump for the removed key's slot"
        );
        assert_eq!(
            keyevents(&mut rx),
            vec![("__keyevent@0__:evicted".to_string(), "k".to_string())],
            "eviction emits an `evicted` keyevent, not `del`"
        );
        let cmds = bc.commands.lock().unwrap();
        assert_eq!(cmds.len(), 1, "the synthetic DEL is broadcast to replicas");
        assert_eq!(cmds[0].0, "DEL");
        assert_eq!(cmds[0].1, vec![Bytes::from_static(b"k")]);
    }

    /// The removal reason selects the keyspace-event class uniformly:
    /// whole-key expiry -> `expired`, hash-emptied -> `del`, eviction ->
    /// `evicted`. This is the one axis an engine removal overrides on the shared
    /// `DEL` handler (which would otherwise emit `del`/GENERIC).
    #[tokio::test]
    async fn internal_removal_reason_selects_notification_class() {
        for (reason, channel, expected_event) in [
            (RemovalReason::Expired, "__keyevent@0__:expired", "expired"),
            (RemovalReason::FieldEmptied, "__keyevent@0__:del", "del"),
            (RemovalReason::Evicted, "__keyevent@0__:evicted", "evicted"),
        ] {
            let bc = Arc::new(RecordingBroadcaster::default());
            let mut worker = worker_with_del(bc.clone() as SharedBroadcaster);
            let mut rx = enable_notify_and_subscribe(&mut worker, &[channel]);

            worker
                .run_internal_removal_effects(
                    vec![(reason, vec![Bytes::from_static(b"k")])],
                    RemovalPropagation {
                        wal: false,
                        replicate: false,
                    },
                    ENGINE_INTERNAL_CONN_ID,
                )
                .await;

            assert_eq!(
                keyevents(&mut rx),
                vec![(channel.to_string(), "k".to_string())],
                "reason {reason:?} must emit `{expected_event}`"
            );
        }
    }

    /// `replicate: false` (the active-expiry default — replicas expire on their
    /// own clock) suppresses the broadcast, while the local effects (version
    /// bump, notification) still run. Documents the policy as a test, not a
    /// comment.
    #[tokio::test]
    async fn internal_removal_replicate_false_suppresses_broadcast() {
        let bc = Arc::new(RecordingBroadcaster::default());
        let mut worker = worker_with_del(bc.clone() as SharedBroadcaster);
        let mut rx = enable_notify_and_subscribe(&mut worker, &["__keyevent@0__:expired"]);

        worker
            .run_internal_removal_effects(
                vec![(RemovalReason::Expired, vec![Bytes::from_static(b"k")])],
                RemovalPropagation {
                    wal: true,
                    replicate: false,
                },
                ENGINE_INTERNAL_CONN_ID,
            )
            .await;

        assert_eq!(
            worker.get_key_version(b"k"),
            1,
            "local version bump still happens"
        );
        assert_eq!(
            keyevents(&mut rx).len(),
            1,
            "local notification still fires"
        );
        assert!(
            bc.commands.lock().unwrap().is_empty(),
            "replicate=false must broadcast nothing"
        );
    }

    /// A single cycle removing BOTH a whole-key-expired key (`Expired`) and a
    /// hash-emptied key (`FieldEmptied`) bumps each removed key's slot exactly
    /// once — not more, even though the two keys arrive in separate reason
    /// groups — while still emitting the correct per-key event class and
    /// replicating both synthetic `DEL`s.
    #[tokio::test]
    async fn internal_removal_coalesces_version_bump_across_reason_groups() {
        let bc = Arc::new(RecordingBroadcaster::default());
        let mut worker = worker_with_del(bc.clone() as SharedBroadcaster);
        let mut rx = enable_notify_and_subscribe(
            &mut worker,
            &["__keyevent@0__:expired", "__keyevent@0__:del"],
        );

        worker
            .run_internal_removal_effects(
                vec![
                    (RemovalReason::Expired, vec![Bytes::from_static(b"a")]),
                    (RemovalReason::FieldEmptied, vec![Bytes::from_static(b"h")]),
                ],
                RemovalPropagation {
                    wal: false,
                    replicate: true,
                },
                ENGINE_INTERNAL_CONN_ID,
            )
            .await;

        // Each removed key's slot bumps exactly once (a and h are different
        // slots); within one cycle a given slot is never double-bumped.
        assert_eq!(worker.get_key_version(b"a"), 1);
        assert_eq!(worker.get_key_version(b"h"), 1);
        let events = keyevents(&mut rx);
        assert!(events.contains(&("__keyevent@0__:expired".into(), "a".into())));
        assert!(events.contains(&("__keyevent@0__:del".into(), "h".into())));
        // Both synthetic DELs replicate (one per reason group).
        assert_eq!(bc.commands.lock().unwrap().len(), 2);
    }

    /// An all-empty removal batch is a no-op: no version bump, no broadcast, no
    /// panic (the DEL handler is never even resolved).
    #[tokio::test]
    async fn internal_removal_empty_batch_is_noop() {
        let bc = Arc::new(RecordingBroadcaster::default());
        // Deliberately no DEL registered: an empty batch must short-circuit
        // before resolving the handler.
        let mut worker = worker_with(bc.clone() as SharedBroadcaster);

        worker
            .run_internal_removal_effects(
                vec![
                    (RemovalReason::Expired, vec![]),
                    (RemovalReason::Evicted, vec![]),
                ],
                RemovalPropagation {
                    wal: true,
                    replicate: true,
                },
                ENGINE_INTERNAL_CONN_ID,
            )
            .await;

        assert_eq!(worker.get_key_version(b"k"), 0);
        assert!(bc.commands.lock().unwrap().is_empty());
    }

    /// A removal invalidates RESP3 client-tracking caches for the removed key —
    /// the effect eviction previously skipped, leaving clients serving a stale
    /// value for a key the server no longer holds.
    #[tokio::test]
    async fn internal_removal_invalidates_tracking() {
        use crate::tracking::{InvalidationMessage, TrackedConnection};

        let bc = Arc::new(RecordingBroadcaster::default());
        let mut worker = worker_with_del(bc as SharedBroadcaster);

        // Register a tracking client that read the key (so it is cached).
        let (tx, mut inval_rx) = mpsc::unbounded_channel();
        worker.tracking.register(
            7,
            TrackedConnection {
                sender: tx,
                noloop: false,
            },
        );
        worker.tracking.record_read(b"k", 7);

        worker
            .run_internal_removal_effects(
                vec![(RemovalReason::Evicted, vec![Bytes::from_static(b"k")])],
                RemovalPropagation {
                    wal: true,
                    replicate: true,
                },
                ENGINE_INTERNAL_CONN_ID,
            )
            .await;

        match inval_rx.try_recv() {
            Ok(InvalidationMessage::Keys(keys)) => {
                assert_eq!(keys, vec![Bytes::from_static(b"k")]);
            }
            other => panic!("tracking client must be invalidated, got {other:?}"),
        }
    }
}
