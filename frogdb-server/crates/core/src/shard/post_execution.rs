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

use crate::command::{Command, ScriptWriteRecord, WaiterKind, WaiterWake, WriteRecord};
use crate::store::Store;

use super::helpers::REPLICA_INTERNAL_CONN_ID;
use super::worker::ShardWorker;

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
                    match scope {
                        EffectScope::Command | EffectScope::ScatterPart => {
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
                    for record in summary.writes {
                        self.emit_keyspace_notifications_for_command(record);
                    }
                }
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
                    if matches!(wal, WalPhase::Persist) {
                        for record in summary.writes {
                            self.persist_by_strategy(record).await;
                        }
                    }
                }
                WriteEffectKind::SearchIndex => {
                    if !self.search.indexes.is_empty() {
                        for record in summary.writes {
                            self.update_search_indexes(record.handler.name(), record.args);
                        }
                    }
                }
                WriteEffectKind::ReplicationBroadcast => {
                    if summary.conn_id != REPLICA_INTERNAL_CONN_ID
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
                        match scope {
                            EffectScope::Command => {
                                let record = &summary.writes[0];
                                self.replication_broadcaster.broadcast_command_on_shard(
                                    shard_id,
                                    record.handler.name(),
                                    record.args,
                                );
                            }
                            EffectScope::ScatterPart => {
                                for record in summary.writes {
                                    self.replication_broadcaster.broadcast_command_on_shard(
                                        shard_id,
                                        record.handler.name(),
                                        record.args,
                                    );
                                }
                            }
                            EffectScope::Transaction => {
                                let commands: Vec<(&str, &[Bytes])> = summary
                                    .writes
                                    .iter()
                                    .map(|record| (record.handler.name(), record.args))
                                    .collect();
                                self.replication_broadcaster
                                    .broadcast_transaction_on_shard(shard_id, &commands);
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
            },
            WalPhase::Persist,
            EffectScope::ScatterPart,
        )
        .await;
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

    async fn persist_by_strategy(&self, record: &WriteRecord<'_>) {
        if !self.persistence.has_wal() {
            return;
        }

        for action in record.wal_actions() {
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
        fn broadcast_command(&self, cmd_name: &str, args: &[Bytes]) -> u64 {
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
        fn extract_divergent_writes(&self, _last: u64) -> Vec<(u64, Bytes)> {
            Vec::new()
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
                lookup: LookupSpec::None,
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
                lookup: LookupSpec::None,
                strategy: crate::command::ExecutionStrategy::Standard,
            };
            &SPEC
        }
        fn execute(
            &self,
            ctx: &mut CommandContext,
            _args: &[Bytes],
        ) -> Result<Response, frogdb_types::CommandError> {
            ctx.write_was_noop = true; // verified: nothing changed
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
        assert_eq!(worker.shard_version, 0, "no-op write must not bump version");
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
        assert_eq!(worker.shard_version, 1);
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

        // Version bumped once for the whole part (not once per write).
        assert_eq!(worker.shard_version, 1);

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
            worker.shard_version, 0,
            "negative dirty delta must not bump version"
        );
        // The write still replicates (the effect happened; only WATCH-dirtying
        // is suppressed).
        assert_eq!(bc.commands.lock().unwrap().len(), 1);
    }
}
