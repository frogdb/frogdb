//! Command dispatch and pipeline logic.
//!
//! This module handles routing parsed commands to their connection-level handlers
//! and executing the main command pipeline (transaction handling, pub/sub mode,
//! cluster validation, etc.).

use std::sync::Arc;

use bytes::Bytes;
use frogdb_core::{ConnectionLevelOp, ExecutionStrategy, ServerWideOp};
use frogdb_protocol::Response;
use tracing::Instrument;

use crate::connection::ConnectionHandler;
use crate::connection::conn_command::ConnectionCommand;

/// The pre-dispatch gauntlet, in execution order. Reordering a guard means
/// editing [`PRE_DISPATCH_ORDER`] (and the pinning test notices). Mirrors
/// `WRITE_EFFECT_ORDER` (`core/src/shard/post_execution.rs`), the shard-side
/// analogue: a load-bearing ordering encoded as a `const` array instead of the
/// top-to-bottom layout of an `if`-ladder.
///
/// Two stage flavors:
/// - **Guard stages** (`PreChecks`, `Arity`, `PubSubPing`, `TransactionQueue`,
///   `ClusterSlotValidation`, `MigratingTryAgain`) are pure decisions over the
///   socketless [`PreDispatchView`](crate::connection::guards::PreDispatchView).
/// - **Dispatch stages** (`PreAuthIntercept`, `ResetIntercept`,
///   `TransactionControl`, `PauseGate`, `ConnectionStateCommand`,
///   `ConnectionCommand`, `PsyncIntercept`, `WaitIntercept`, `ServerWide`,
///   `ClusterSlotSubcommand`, `Execute`) terminate into a command executor that
///   needs the full handler; their arms are thin adapters over the unchanged
///   executors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DispatchStage {
    /// AUTH/HELLO, before the NOAUTH check.
    PreAuthIntercept,
    /// auth / replica / quorum / admin / ACL / pub-sub gate.
    PreChecks,
    /// RESET, never queued, never paused.
    ResetIntercept,
    /// `["pong", msg]` framing in pub/sub mode.
    PubSubPing,
    /// MULTI/EXEC/DISCARD/WATCH/UNWATCH.
    TransactionControl,
    /// queue if in MULTI (+ slot pre-validate).
    TransactionQueue,
    /// wrong-arg-count error, before pause.
    Arity,
    /// CLIENT PAUSE wait, after queuing.
    PauseGate,
    /// ASKING/READONLY/READWRITE (mutable seam).
    ConnectionStateCommand,
    /// CONFIG/CLIENT/INFO/… registry union.
    ConnectionCommand,
    /// PSYNC handoff signal.
    PsyncIntercept,
    /// WAIT → WaitCoordinator.
    WaitIntercept,
    /// SCAN/KEYS/FLUSHDB/MIGRATE/…
    ServerWide,
    /// CLUSTER GET/COUNTKEYSINSLOT slot routing.
    ClusterSlotSubcommand,
    /// MOVED/ASK/CROSSSLOT (consumes take_asking).
    ClusterSlotValidation,
    /// TRYAGAIN during multi-key slot migration.
    MigratingTryAgain,
    /// Terminal: bookkeeping + route_and_execute + post-execution tail.
    Execute,
}

/// THE canonical pre-dispatch order. The single source of truth for the
/// gauntlet's sequence; [`ConnectionHandler::route_and_execute_with_transaction`]
/// iterates this array. `Execute` is last and is the only stage that never
/// returns [`StageOutcome::Continue`], which is what makes the driver loop total.
pub(crate) const PRE_DISPATCH_ORDER: [DispatchStage; 17] = [
    DispatchStage::PreAuthIntercept,
    DispatchStage::PreChecks,
    DispatchStage::ResetIntercept,
    DispatchStage::PubSubPing,
    DispatchStage::TransactionControl,
    DispatchStage::TransactionQueue,
    DispatchStage::Arity,
    DispatchStage::PauseGate,
    DispatchStage::ConnectionStateCommand,
    DispatchStage::ConnectionCommand,
    DispatchStage::PsyncIntercept,
    DispatchStage::WaitIntercept,
    DispatchStage::ServerWide,
    DispatchStage::ClusterSlotSubcommand,
    DispatchStage::ClusterSlotValidation,
    DispatchStage::MigratingTryAgain,
    DispatchStage::Execute,
];

/// A pre-dispatch stage either lets the command proceed or ends dispatch with a
/// reply. `Continue` is the hot-path common case and allocates nothing; the only
/// allocation is the `ShortCircuit` `Vec`, exactly today's `Vec<Response>`
/// return.
pub(crate) enum StageOutcome {
    /// Proceed to the next stage.
    Continue,
    /// End dispatch with these responses (one-or-many, as pub/sub returns).
    ShortCircuit(Vec<Response>),
}

impl ConnectionHandler {
    /// Dispatch a command registered as [`frogdb_core::CommandImpl::Connection`]
    /// through its connection-level executor, returning `Some(responses)` if it
    /// was handled. Returns `None` for any command that is not a migrated
    /// connection command, so unmigrated groups fall through to the legacy
    /// router→handler path.
    ///
    /// The `Connection` variant holds a `&'static dyn ConnectionCommand`, so the
    /// executor reference outlives the transient registry borrow and does not
    /// conflict with borrowing `self` again to build the [`ConnCtx`].
    async fn dispatch_connection_command(
        &mut self,
        cmd_name: &str,
        args: &[Bytes],
    ) -> Option<Vec<Response>> {
        let command = self.core.registry.get_entry(cmd_name)?.as_connection()?;
        // Pub/sub (SUBSCRIBE/…/PUBSUB) emits one reply per channel and needs the
        // connection-local pub/sub machinery (subscription set + lazy channel +
        // cluster routing + scatter-gather introspection), so it dispatches
        // through the multi-response seam (`execute_multi`) over a dedicated
        // `ConnCtx::pubsub` view. `command` is `'static`, so reading its spec
        // does not conflict with re-borrowing `self` to build that view.
        if matches!(
            command.spec().strategy,
            ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::PubSub)
        ) {
            return Some(self.execute_pubsub(command, args).await);
        }
        // CLIENT mutates per-connection state (name/reply/tracking/caching) and
        // drives tracking IO, so it dispatches through the mutable builder that
        // populates `conn_state` and `tracking`. All other connection commands
        // are pure reads and use the shared `conn_ctx`. `as_connection()` yields
        // a `'static` reference, so it does not conflict with re-borrowing `self`.
        if cmd_name == "CLIENT" {
            return Some(vec![
                command.execute(&mut self.conn_ctx_clientmut(), args).await,
            ]);
        }
        // MONITOR registers the connection as a monitor: it mutates the
        // connection-local monitor receiver and reads the broadcaster, so it
        // dispatches through the dedicated builder that populates
        // `ConnCtx::monitor`. `command` is `'static`, so it does not conflict
        // with re-borrowing `self` to build that view.
        if cmd_name == "MONITOR" {
            return Some(vec![self.execute_monitor(command, args).await]);
        }
        Some(vec![command.execute(&mut self.conn_ctx(), args).await])
    }

    /// Execute a server-wide command by its typed op. The exhaustive match is
    /// the single dispatch point.
    ///
    /// Adding a new server-wide command is two steps: (1) add a [`ServerWideOp`]
    /// variant, and (2) declare `ExecutionStrategy::ServerWide(ServerWideOp::…)`
    /// on the command spec. The compiler then forces a match arm here (a missing
    /// arm is a compile error), so the name-keyed table plus drift tests this
    /// replaced are gone.
    ///
    /// `pub(crate)` because EXEC also routes here: server-wide commands queued
    /// in a MULTI are deferred past the shard transaction and dispatched
    /// through this same match (see `handlers::transaction::execute_transaction`).
    pub(crate) async fn dispatch_server_wide(
        &mut self,
        op: ServerWideOp,
        args: &[Bytes],
    ) -> Response {
        match op {
            ServerWideOp::Scan => self.handle_scan(args).await,
            ServerWideOp::Keys => self.handle_keys(args).await,
            ServerWideOp::DbSize => self.handle_dbsize().await,
            ServerWideOp::RandomKey => self.handle_randomkey().await,
            ServerWideOp::FlushDb => self.handle_flushdb(args).await,
            ServerWideOp::FlushAll => self.handle_flushall(args).await,
            ServerWideOp::Migrate => self.handle_migrate(args).await,
            ServerWideOp::Shutdown => self.handle_shutdown(args).await,
            ServerWideOp::TsQueryIndex => self.handle_ts_queryindex(args).await,
            ServerWideOp::TsMGet => self.handle_ts_mget(args).await,
            ServerWideOp::TsMRange => self.handle_ts_mrange(args, false).await,
            ServerWideOp::TsMRevRange => self.handle_ts_mrange(args, true).await,
            ServerWideOp::FtCreate => self.handle_ft_create(args).await,
            ServerWideOp::FtSearch => self.handle_ft_search(args).await,
            ServerWideOp::FtDropIndex => self.handle_ft_dropindex(args).await,
            ServerWideOp::FtInfo => self.handle_ft_info(args).await,
            ServerWideOp::FtList => self.handle_ft_list(args).await,
            ServerWideOp::FtAlter => self.handle_ft_alter(args).await,
            ServerWideOp::FtSynUpdate => self.handle_ft_synupdate(args).await,
            ServerWideOp::FtSynDump => self.handle_ft_syndump(args).await,
            ServerWideOp::FtAggregate => self.handle_ft_aggregate(args).await,
            ServerWideOp::FtHybrid => self.handle_ft_hybrid(args).await,
            ServerWideOp::FtAliasAdd => self.handle_ft_aliasadd(args).await,
            ServerWideOp::FtAliasDel => self.handle_ft_aliasdel(args).await,
            ServerWideOp::FtAliasUpdate => self.handle_ft_aliasupdate(args).await,
            ServerWideOp::FtTagVals => self.handle_ft_tagvals(args).await,
            ServerWideOp::FtDictAdd => self.handle_ft_dictadd(args).await,
            ServerWideOp::FtDictDel => self.handle_ft_dictdel(args).await,
            ServerWideOp::FtDictDump => self.handle_ft_dictdump(args).await,
            ServerWideOp::FtConfig => self.handle_ft_config(args).await,
            ServerWideOp::FtSpellCheck => self.handle_ft_spellcheck(args).await,
            ServerWideOp::FtExplain => self.handle_ft_explain(args, false).await,
            ServerWideOp::FtExplainCli => self.handle_ft_explain(args, true).await,
            ServerWideOp::FtProfile => self.handle_ft_profile(args).await,
            ServerWideOp::EsAll => self.handle_es_all(args).await,
        }
    }

    /// Handle internal action signals returned by commands.
    ///
    /// Some commands return special Response variants that signal the connection
    /// handler to perform async operations (blocking waits, Raft consensus, migration).
    async fn handle_internal_action(&mut self, response: Response) -> Response {
        match response {
            Response::BlockingNeeded { keys, timeout, op } => {
                self.handle_blocking_wait(keys, timeout, op).await
            }
            Response::RaftNeeded {
                op,
                register_node,
                unregister_node,
            } => {
                self.handle_raft_command(op, register_node, unregister_node)
                    .await
            }
            Response::MigrateNeeded { args } => self.handle_migrate_command(args).await,
            Response::SlotMigrationNeeded { kind } => self.handle_slot_migration(kind).await,
            other => other,
        }
    }

    /// Dispatch a connection-state command (ASKING/READONLY/READWRITE) migrated
    /// behind the ConnCtx seam. Unlike the read-only registry-union
    /// [`dispatch_connection_command`](Self::dispatch_connection_command), these
    /// **mutate** per-connection state, so they dispatch through the *mutable*
    /// [`conn_ctx_authmut`](Self::conn_ctx_authmut) view (`conn_state = Some`).
    ///
    /// Scoped to the `ConnectionState` strategy so it only claims these commands
    /// and leaves the read-only migrated connection commands (CONFIG, INFO, ...)
    /// to the read-only union below. RESET is *not* handled here — it needs a
    /// handler-local teardown the seam cannot reach and is intercepted earlier
    /// via [`execute_reset`](Self::execute_reset).
    async fn dispatch_connection_state_command(
        &mut self,
        cmd_name: &str,
        args: &[Bytes],
    ) -> Option<Vec<Response>> {
        let entry = self.core.registry.get_entry(cmd_name)?;
        if !matches!(
            entry.execution_strategy(),
            ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::ConnectionState)
        ) {
            return None;
        }
        let command = entry.as_connection()?;
        Some(vec![
            command.execute(&mut self.conn_ctx_authmut(), args).await,
        ])
    }

    /// Execute RESET (migrated behind the ConnCtx seam) plus the handler-local
    /// teardown the seam cannot reach.
    ///
    /// The [`RESET_CONN_COMMAND`](crate::connection::connection_state_conn_command)
    /// executor resets the connection state (via `ConnStateMut::reset`), fans out
    /// the shard `ConnectionClosed` notification, and clears the registry name.
    /// The two remaining steps — dropping the MONITOR subscription and tearing
    /// down the tracking session's local plumbing (invalidation channels +
    /// redirect forwarder) — touch `ConnectionHandler` fields absent from the
    /// `ConnCtx`, so they are done here, matching the old `handle_reset`.
    pub(crate) async fn execute_reset(&mut self, args: &[Bytes]) -> Response {
        let response = crate::connection::connection_state_conn_command::RESET_CONN_COMMAND
            .execute(&mut self.conn_ctx_authmut(), args)
            .await;
        self.tracking_session_teardown_local();
        self.monitor_rx = None;
        response
    }

    /// Route and execute a command, handling transaction and pub/sub modes.
    /// Returns a Vec of responses since pub/sub commands can return multiple messages.
    ///
    /// `cmd_name` is the precomputed uppercase command name to avoid redundant allocations.
    ///
    /// This is the driver for the pre-dispatch gauntlet: it walks
    /// [`PRE_DISPATCH_ORDER`] and runs each [`DispatchStage`] in turn, returning
    /// as soon as a stage short-circuits. The ordering — the ~15 load-bearing
    /// Redis interceptions (AUTH-before-NOAUTH, MULTI-queue-before-pause,
    /// arity-before-pause, …) — is the `const` array, not the layout of an
    /// `if`-ladder; the pinning test in this module's `tests` asserts the
    /// invariants. Guard stages run over the socketless
    /// [`PreDispatchView`](crate::connection::guards::PreDispatchView); dispatch
    /// stages are thin adapters over the unchanged executors.
    ///
    /// The loop is a `match` over a `Copy` enum with directly-inlined `.await`s —
    /// no trait objects, no boxed futures, no per-command allocation (`Continue`
    /// is payload-free; the only allocation is the `ShortCircuit` `Vec`, exactly
    /// what each `return vec![…]` built before). This is the hottest path in the
    /// server, and the design monomorphizes to the same shape as the old
    /// `if`-ladder.
    pub(crate) async fn route_and_execute_with_transaction(
        &mut self,
        cmd: &Arc<frogdb_protocol::ParsedCommand>,
        cmd_name: &str,
    ) -> Vec<Response> {
        for stage in PRE_DISPATCH_ORDER {
            match self.run_stage(stage, cmd, cmd_name).await {
                StageOutcome::Continue => {}
                StageOutcome::ShortCircuit(responses) => return responses,
            }
        }
        unreachable!("PRE_DISPATCH_ORDER ends in Execute, which never returns Continue")
    }

    /// Run a single pre-dispatch stage. A single `match` over the `Copy`
    /// [`DispatchStage`] enum whose arms are the *former* inline bodies of
    /// `route_and_execute_with_transaction`, moved verbatim. Guard arms build a
    /// [`PreDispatchView`](crate::connection::guards::PreDispatchView) (dropped
    /// before any executor re-borrows `self`); dispatch arms delegate to the
    /// unchanged executors.
    async fn run_stage(
        &mut self,
        stage: DispatchStage,
        cmd: &Arc<frogdb_protocol::ParsedCommand>,
        cmd_name: &str,
    ) -> StageOutcome {
        match stage {
            // AUTH and HELLO run *before* authentication is enforced: a
            // not-yet-authenticated client must be able to authenticate /
            // negotiate the protocol. They are migrated behind the ConnCtx seam
            // (registered as CommandImpl::Connection executors) but intercepted
            // here — before the NOAUTH pre-check and before transaction queuing —
            // and dispatched through the *mutable* `conn_ctx_authmut` view (they
            // change auth / protocol state). This preserves the historical
            // pre-auth ordering.
            DispatchStage::PreAuthIntercept => {
                if cmd_name == "AUTH" || cmd_name == "HELLO" {
                    let command = self
                        .core
                        .registry
                        .get_entry(cmd_name)
                        .and_then(|entry| entry.as_connection())
                        .expect("AUTH/HELLO are registered as connection commands");
                    return StageOutcome::ShortCircuit(vec![
                        command
                            .execute(&mut self.conn_ctx_authmut(), &cmd.args)
                            .await,
                    ]);
                }
                StageOutcome::Continue
            }

            // Pre-execution checks (auth, replica-readonly, quorum-fence,
            // admin-port, ACL, pub/sub-mode gate) over the socketless view. ACL is
            // migrated behind the ConnCtx seam: after pre-checks it dispatches
            // through the registry union (`dispatch_connection_command`) like
            // CONFIG, rather than being intercepted early here.
            DispatchStage::PreChecks => {
                let error_response = self.pre_dispatch_view().run_pre_checks(cmd_name, &cmd.args);
                if let Some(error_response) = error_response {
                    self.record_error_response(&error_response, true, cmd_name);
                    return StageOutcome::ShortCircuit(vec![error_response]);
                }
                StageOutcome::Continue
            }

            // RESET is migrated behind the ConnCtx seam. It performs a full reset
            // of the connection's server-side context and must dispatch directly —
            // never queued in MULTI, never blocked by pause — in both normal and
            // pub/sub mode. It is intercepted here (after pre-checks) and
            // dispatched through the mutable seam plus the handler-local teardown
            // the seam cannot reach.
            DispatchStage::ResetIntercept => {
                if cmd_name == "RESET" {
                    return StageOutcome::ShortCircuit(vec![self.execute_reset(&cmd.args).await]);
                }
                StageOutcome::Continue
            }

            // In pub/sub mode, PING needs bespoke framing (RESP2 array ["pong",
            // <message>]); it is registered as a Standard (shard) command so it
            // cannot use the normal shard PING path. The view predicate returns
            // `Some` only when in pub/sub mode *and* the command is PING; every
            // other command continues down the normal dispatch flow below.
            DispatchStage::PubSubPing => {
                let responses = self
                    .pre_dispatch_view()
                    .pubsub_mode_ping(cmd_name, &cmd.args);
                match responses {
                    Some(responses) => StageOutcome::ShortCircuit(responses),
                    None => StageOutcome::Continue,
                }
            }

            // Transaction control commands (MULTI/EXEC/DISCARD/WATCH/UNWATCH),
            // migrated behind the ConnCtx seam, are always dispatched directly,
            // never queued or blocked by pause. (RESET was in this set but is
            // intercepted above.) MULTI/DISCARD/WATCH/UNWATCH run their
            // `CommandImpl::Connection` executors over the mutable ConnCtx; EXEC's
            // orchestration stays in `handle_exec`.
            DispatchStage::TransactionControl => {
                match self.dispatch_transaction_command(cmd_name, &cmd.args).await {
                    Some(responses) => StageOutcome::ShortCircuit(responses),
                    None => StageOutcome::Continue,
                }
            }

            // If in transaction mode, queue the command instead of executing (the
            // view validates cluster slots before queuing and aborts on a
            // cross-slot queued command). This runs BEFORE connection-level
            // dispatch so commands like CLIENT PAUSE, EVAL, etc. are queued during
            // MULTI (not executed immediately). Blocking commands are also queued —
            // at EXEC time they run with timeout=0 (non-blocking), matching Redis.
            DispatchStage::TransactionQueue => {
                match self.pre_dispatch_view().try_queue_in_transaction(cmd) {
                    Some(responses) => StageOutcome::ShortCircuit(responses),
                    None => StageOutcome::Continue,
                }
            }

            // Validate arity BEFORE the pause check so that commands with wrong
            // argument counts return an immediate error even when paused (test:
            // syntax errors bypass pause).
            DispatchStage::Arity => {
                let err = self.pre_dispatch_view().arity_check(cmd_name, &cmd.args);
                if let Some(err) = err {
                    self.record_error_response(&err, true, cmd_name);
                    return StageOutcome::ShortCircuit(vec![err]);
                }
                StageOutcome::Continue
            }

            // Wait if the server is paused (CLIENT PAUSE). Checked AFTER
            // transaction queuing so commands inside MULTI are queued without
            // blocking. The pause *wait loop* mutates the client registry's paused
            // state and sleeps, so it stays a thin call on the handler; only its
            // predicate (`should_pause_command`) is a pure guard. Never
            // short-circuits.
            DispatchStage::PauseGate => {
                self.wait_if_paused(cmd_name, &cmd.args).await;
                StageOutcome::Continue
            }

            // Connection-state commands (ASKING/READONLY/READWRITE) migrated
            // behind the ConnCtx seam MUTATE per-connection state, so they
            // dispatch through the *mutable* `conn_ctx_authmut` view. This must
            // run before the read-only registry union below, which would otherwise
            // claim them and dispatch with `conn_state = None` (a no-op).
            DispatchStage::ConnectionStateCommand => {
                match self
                    .dispatch_connection_state_command(cmd_name, &cmd.args)
                    .await
                {
                    Some(responses) => StageOutcome::ShortCircuit(responses),
                    None => StageOutcome::Continue,
                }
            }

            // Registry-union dispatch: a command registered as
            // `CommandImpl::Connection` (CONFIG, BGSAVE/LASTSAVE, CLIENT, DEBUG,
            // MONITOR, ACL, INFO, HOTKEYS, FT.CURSOR, SLOWLOG, MEMORY, LATENCY,
            // STATUS, the pub/sub family, and the scripting family) executes
            // through its `ConnCtx` executor. This is the sole connection-command
            // dispatch path.
            DispatchStage::ConnectionCommand => {
                match self.dispatch_connection_command(cmd_name, &cmd.args).await {
                    Some(responses) => StageOutcome::ShortCircuit(responses),
                    None => StageOutcome::Continue,
                }
            }

            // Handle PSYNC command - validates args and returns handoff signal.
            // The actual handoff happens in the run() loop when it detects
            // PSYNC_HANDOFF.
            DispatchStage::PsyncIntercept => {
                if cmd_name == "PSYNC" {
                    // Check if we have a primary replication handler.
                    if self.cluster.primary_replication_handler.is_none() {
                        return StageOutcome::ShortCircuit(vec![Response::error(
                            "ERR PSYNC not supported - server is not running as primary",
                        )]);
                    }
                    // Execute PSYNC command which will return PSYNC_HANDOFF signal.
                    return StageOutcome::ShortCircuit(vec![
                        self.route_and_execute(cmd, cmd_name).await,
                    ]);
                }
                StageOutcome::Continue
            }

            // Handle WAIT at the connection level: it blocks on the replication
            // WaitCoordinator (offset snapshot, GETACK solicitation, quorum wait),
            // not on shard routing. Reached only outside MULTI — a queued WAIT
            // executes on the shard at EXEC time, where `WaitCommand::execute`
            // returns the acked count without blocking (Redis deny-blocking
            // semantics). Same dispatch shape as PSYNC above.
            DispatchStage::WaitIntercept => {
                if cmd_name == "WAIT" {
                    return StageOutcome::ShortCircuit(vec![
                        self.handle_wait_command(&cmd.args).await,
                    ]);
                }
                StageOutcome::Continue
            }

            // Server-wide commands (registry-driven: SCAN, KEYS, DBSIZE,
            // RANDOMKEY, FLUSHDB, FLUSHALL, MIGRATE, SHUTDOWN, TS.*, FT.*, ES.ALL).
            // The typed op is extracted from the registry entry's strategy before
            // re-borrowing `self` mutably: `execution_strategy()` returns an owned
            // `ExecutionStrategy`, so copying the `Copy` `ServerWideOp` out ends
            // the registry borrow before `dispatch_server_wide` takes `&mut self`.
            DispatchStage::ServerWide => {
                let server_wide_op = self.core.registry.get_entry(cmd_name).and_then(|entry| {
                    match entry.execution_strategy() {
                        ExecutionStrategy::ServerWide(op) => Some(op),
                        _ => None,
                    }
                });
                if let Some(op) = server_wide_op {
                    return StageOutcome::ShortCircuit(vec![
                        self.dispatch_server_wide(op, &cmd.args).await,
                    ]);
                }
                StageOutcome::Continue
            }

            // Route CLUSTER GETKEYSINSLOT / COUNTKEYSINSLOT to the correct shard.
            // These subcommands query a specific slot's keys, but the CLUSTER
            // command is keyless and would otherwise execute on the connection's
            // assigned shard. Since all keys for a given slot live on shard
            // (slot % num_shards), we must forward to that shard.
            DispatchStage::ClusterSlotSubcommand => {
                if cmd_name == "CLUSTER" && !cmd.args.is_empty() {
                    let sub = cmd.args[0].to_ascii_uppercase();
                    if (sub.as_slice() == b"GETKEYSINSLOT" || sub.as_slice() == b"COUNTKEYSINSLOT")
                        && cmd.args.len() >= 2
                        && let Ok(slot_str) = std::str::from_utf8(&cmd.args[1])
                        && let Ok(slot) = slot_str.parse::<u16>()
                    {
                        let target_shard = slot as usize % self.num_shards;
                        return StageOutcome::ShortCircuit(vec![
                            self.execute_on_shard(target_shard, Arc::clone(cmd)).await,
                        ]);
                    }
                }
                StageOutcome::Continue
            }

            // Validate cluster slot ownership (returns CROSSSLOT/MOVED/ASK errors).
            DispatchStage::ClusterSlotValidation => {
                match self.pre_dispatch_view().validate_cluster_slots(cmd) {
                    Some(cluster_error) => StageOutcome::ShortCircuit(vec![cluster_error]),
                    None => StageOutcome::Continue,
                }
            }

            // Check for TRYAGAIN during slot migration for multi-key commands.
            DispatchStage::MigratingTryAgain => {
                match self.pre_dispatch_view().check_migrating_multikey(cmd).await {
                    Some(tryagain) => StageOutcome::ShortCircuit(vec![tryagain]),
                    None => StageOutcome::Continue,
                }
            }

            // Terminal: per-command tracking/no-touch bookkeeping, shard routing,
            // and the post-execution ASK/internal-action/error-record tail. Always
            // short-circuits (never returns `Continue`), which is what makes the
            // driver loop total.
            DispatchStage::Execute => {
                // Client tracking: compute whether this command's reads should be
                // tracked.
                self.pending_track_reads = self.state.should_track_read();

                // NO-TOUCH: check if this connection has NO_TOUCH flag set.
                self.pending_no_touch = self
                    .admin
                    .client_registry
                    .get(self.state.id)
                    .map(|info| info.flags.contains(frogdb_core::ClientFlags::NO_TOUCH))
                    .unwrap_or(false);

                // Normal execution.
                let response = if self
                    .per_request_spans
                    .load(std::sync::atomic::Ordering::Relaxed)
                {
                    self.route_and_execute(cmd, cmd_name)
                        .instrument(tracing::info_span!("cmd_route"))
                        .await
                } else {
                    self.route_and_execute(cmd, cmd_name).await
                };

                // For MIGRATING slots: if the key doesn't exist locally (nil
                // response), convert to ASK redirect so the client retries on the
                // importing target.
                if let Some(ask) = self.migrating_ask_for_nil(cmd, &response) {
                    return StageOutcome::ShortCircuit(vec![ask]);
                }

                // Handle internal action signals (blocking, raft, migrate).
                let final_response = self.handle_internal_action(response).await;

                // Record failed call if execution returned an error.
                self.record_error_response(&final_response, false, cmd_name);

                StageOutcome::ShortCircuit(vec![final_response])
            }
        }
    }

    /// Classify and record an error response for error statistics.
    ///
    /// `is_rejected` = true means the command was rejected before execution (pre-checks, arity).
    /// `is_rejected` = false means the command failed during execution.
    fn record_error_response(&self, response: &Response, is_rejected: bool, cmd_name: &str) {
        if let Response::Error(bytes) = response {
            let prefix = frogdb_core::extract_error_prefix(bytes);
            let error_stats = &self.admin.client_registry.error_stats;
            if is_rejected {
                error_stats.record_rejected(prefix);
                self.admin.client_registry.record_command_rejected(cmd_name);
            } else {
                error_stats.record_failed(prefix);
                self.admin.client_registry.record_command_failed(cmd_name);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DispatchStage, PRE_DISPATCH_ORDER};
    use frogdb_core::{CommandRegistry, ExecutionStrategy, ServerWideOp};

    /// Index of a stage in `PRE_DISPATCH_ORDER` (panics if absent).
    fn order_index(stage: DispatchStage) -> usize {
        PRE_DISPATCH_ORDER
            .iter()
            .position(|&s| s == stage)
            .unwrap_or_else(|| panic!("{stage:?} missing from PRE_DISPATCH_ORDER"))
    }

    /// Every `DispatchStage` variant appears in `PRE_DISPATCH_ORDER` exactly
    /// once, and the array length is pinned. Guards against a stage being
    /// dropped from (or duplicated in) the canonical order. Model:
    /// `WRITE_EFFECT_ORDER`'s `every_effect_appears_exactly_once`.
    #[test]
    fn every_stage_appears_exactly_once() {
        use DispatchStage::*;
        for stage in [
            PreAuthIntercept,
            PreChecks,
            ResetIntercept,
            PubSubPing,
            TransactionControl,
            TransactionQueue,
            Arity,
            PauseGate,
            ConnectionStateCommand,
            ConnectionCommand,
            PsyncIntercept,
            WaitIntercept,
            ServerWide,
            ClusterSlotSubcommand,
            ClusterSlotValidation,
            MigratingTryAgain,
            Execute,
        ] {
            assert_eq!(
                PRE_DISPATCH_ORDER.iter().filter(|&&s| s == stage).count(),
                1,
                "{stage:?} must appear exactly once in PRE_DISPATCH_ORDER"
            );
        }
        assert_eq!(PRE_DISPATCH_ORDER.len(), 17);
    }

    /// The load-bearing relative orderings the module docs (and Redis's
    /// `processCommand`) justify. These would catch a reorder that compiles but
    /// breaks a correctness invariant.
    #[test]
    fn load_bearing_ordering_invariants() {
        use DispatchStage::*;
        // AUTH/HELLO are intercepted before the NOAUTH pre-check so a
        // not-yet-authenticated client can authenticate.
        assert!(
            order_index(PreAuthIntercept) < order_index(PreChecks),
            "PreAuthIntercept must precede PreChecks (pre-auth)"
        );
        // Arity runs before pause so syntax errors bypass CLIENT PAUSE.
        assert!(
            order_index(Arity) < order_index(PauseGate),
            "Arity must precede PauseGate (syntax errors bypass pause)"
        );
        // Queued MULTI commands must not block on pause.
        assert!(
            order_index(TransactionQueue) < order_index(PauseGate),
            "TransactionQueue must precede PauseGate (queued MULTI commands do not block)"
        );
        // Queued commands slot-validate at queue time; the standalone
        // slot-validation stage runs later on the non-transaction path.
        assert!(
            order_index(ClusterSlotValidation) > order_index(TransactionQueue),
            "ClusterSlotValidation must follow TransactionQueue"
        );
        // Execute is the non-returning terminal.
        assert_eq!(
            *PRE_DISPATCH_ORDER.last().unwrap(),
            Execute,
            "PRE_DISPATCH_ORDER must end in Execute (the driver loop is total only because of this)"
        );
    }

    /// Each `ServerWideOp` must be declared by at most one command spec.
    /// The exhaustive match in `dispatch_server_wide` already guarantees every
    /// op has a handler (adding a variant without an arm is a compile error);
    /// this catches the opposite mistake — a wrong-op copy-paste that points two
    /// different commands at the same op.
    #[test]
    fn server_wide_ops_are_unique_per_command() {
        let mut registry = CommandRegistry::new();
        crate::register_commands(&mut registry);

        let mut seen: Vec<(ServerWideOp, String)> = Vec::new();
        for (name, entry) in registry.iter() {
            if let ExecutionStrategy::ServerWide(op) = entry.execution_strategy() {
                if let Some((_, prev)) = seen.iter().find(|(seen_op, _)| *seen_op == op) {
                    panic!(
                        "ServerWideOp::{:?} declared by two commands: {} and {}",
                        op, prev, name
                    );
                }
                seen.push((op, name.to_string()));
            }
        }
    }
}
