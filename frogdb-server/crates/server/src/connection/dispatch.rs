//! Command dispatch and pipeline logic.
//!
//! This module handles routing parsed commands to their connection-level handlers
//! and executing the main command pipeline (transaction handling, pub/sub mode,
//! cluster validation, etc.).

use std::sync::Arc;

use bytes::Bytes;
use frogdb_core::{ConnMutation, ExecutionStrategy, ServerWideOp};
use frogdb_protocol::Response;
use tracing::Instrument;

use frogdb_replication::AnnouncedOption;

use crate::commands::replication::{PsyncHandoff, announcement_error};
use crate::connection::ConnectionHandler;
use crate::connection::conn_command::ConnectionCommand;

/// The pre-dispatch gauntlet, in execution order. Reordering a guard means
/// editing [`PRE_DISPATCH_ORDER`] (and the pinning test notices). Mirrors
/// `WRITE_EFFECT_ORDER` (`core/src/shard/post_execution.rs`), the shard-side
/// analogue: a load-bearing ordering encoded as a `const` array instead of the
/// top-to-bottom layout of an `if`-ladder.
///
/// Two stage flavors:
/// - **Guard stages** (`PreChecks`, `CommandLookup`, `PubSubPing`,
///   `TransactionQueue`, `ClusterSlotValidation`, `MigratingTryAgain`) are pure
///   decisions over the socketless
///   [`PreDispatchView`](crate::connection::guards::PreDispatchView).
/// - **Dispatch stages** (`PreAuthIntercept`, `ResetIntercept`,
///   `TransactionControl`, `PauseGate`, `ConnectionCommand`,
///   `ReplicationHandshake`,
///   `WaitIntercept`, `ServerWide`, `ClusterSlotSubcommand`, `Execute`)
///   terminate into a command executor that needs the full handler; their arms
///   are thin adapters over the unchanged executors.
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
    /// unknown-command / wrong-arg-count error, before pause.
    CommandLookup,
    /// CLIENT PAUSE wait, after queuing.
    PauseGate,
    /// MOVED/ASK/CROSSSLOT (consumes take_asking). Runs *before* every stage
    /// that can terminate a keyed command, connection-level scripting included.
    ClusterSlotValidation,
    /// CONFIG/CLIENT/INFO/ASKING/… registry union (capability from the spec).
    ConnectionCommand,
    /// The replica handshake: `REPLCONF` identity announcement, then the PSYNC
    /// handoff signal.
    ReplicationHandshake,
    /// WAIT → WaitCoordinator.
    WaitIntercept,
    /// SCAN/KEYS/FLUSHDB/MIGRATE/…
    ServerWide,
    /// CLUSTER GET/COUNTKEYSINSLOT slot routing.
    ClusterSlotSubcommand,
    /// TRYAGAIN during multi-key slot migration.
    MigratingTryAgain,
    /// Terminal: bookkeeping + route_and_execute + post-execution tail.
    Execute,
}

impl DispatchStage {
    /// Whether an error produced by this stage is a *rejection* (the command
    /// never began executing → `rejected_calls`) rather than a *failure* (it
    /// ran and errored → `failed_calls`). This is exactly the guard/dispatch
    /// split of the two stage flavors documented on [`DispatchStage`]: a guard
    /// decides without running anything, a dispatch stage terminates into a
    /// command executor.
    ///
    /// Redis draws the same line: `processCommand`'s pre-`call()` refusals
    /// (`rejectCommand*`, cluster redirects, unknown command, arity) count as
    /// `rejected_calls`, while an error reply raised from inside `call()`
    /// counts as `failed_calls` — including AUTH failures, `NOSCRIPT`, and
    /// `EXECABORT`, all of which are dispatch stages here.
    ///
    /// Exhaustive on purpose: a new stage must state its disposition rather
    /// than inherit a default.
    const fn rejects_pre_execution(self) -> bool {
        match self {
            // Guard stages: pure decisions over the `PreDispatchView`.
            Self::PreChecks
            | Self::CommandLookup
            | Self::PubSubPing
            | Self::TransactionQueue
            | Self::ClusterSlotValidation
            | Self::MigratingTryAgain => true,
            // Dispatch stages: they terminate into a command executor, so an
            // error reply from one is a command that ran and failed.
            Self::PreAuthIntercept
            | Self::ResetIntercept
            | Self::TransactionControl
            | Self::PauseGate
            | Self::ConnectionCommand
            | Self::ReplicationHandshake
            | Self::WaitIntercept
            | Self::ServerWide
            | Self::ClusterSlotSubcommand
            | Self::Execute => false,
        }
    }
}

/// THE canonical pre-dispatch order. The single source of truth for the
/// gauntlet's sequence; [`ConnectionHandler::route_and_execute_with_transaction`]
/// iterates this array. `Execute` is last and is the only stage that never
/// returns [`StageOutcome::Continue`], which is what makes the driver loop total.
pub(crate) const PRE_DISPATCH_ORDER: [DispatchStage; 16] = [
    DispatchStage::PreAuthIntercept,
    DispatchStage::PreChecks,
    DispatchStage::ResetIntercept,
    DispatchStage::PubSubPing,
    DispatchStage::TransactionControl,
    DispatchStage::TransactionQueue,
    DispatchStage::CommandLookup,
    DispatchStage::PauseGate,
    DispatchStage::ClusterSlotValidation,
    DispatchStage::ConnectionCommand,
    DispatchStage::ReplicationHandshake,
    DispatchStage::WaitIntercept,
    DispatchStage::ServerWide,
    DispatchStage::ClusterSlotSubcommand,
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
    /// End dispatch by handing the raw socket to the primary replication
    /// handler (PSYNC). A *control* outcome, not data: the sole producer is
    /// [`DispatchStage::ReplicationHandshake`], after its presence gate, so the
    /// takeover is a compiler-tracked value rather than a sentinel `Response`.
    Handoff(PsyncHandoff),
}

/// What a fully-dispatched frame produced: either replies to write back to the
/// client, or a connection takeover to perform. Replaces the bare
/// `Vec<Response>` return of [`ConnectionHandler::route_and_execute_with_transaction`]
/// so the PSYNC takeover is a first-class value the compiler forces every
/// caller to consider, not a magic array the caller string-matches.
pub(crate) enum Dispatched {
    /// Ordinary command replies (one-or-many, as pub/sub returns).
    Responses(Vec<Response>),
    /// PSYNC socket takeover.
    Handoff(PsyncHandoff),
}

impl ConnectionHandler {
    /// Dispatch a command registered as [`frogdb_core::CommandImpl::Connection`]
    /// through its connection-level executor, returning `Some(responses)` if it
    /// was handled. Returns `None` for any command not registered as a connection
    /// executor (shard `Standard`/`Blocking`/scatter/server-wide commands), which
    /// then continues down the gauntlet to its own dispatch stage.
    ///
    /// This is the single builder-selection site for connection commands: the
    /// `ConnCtx` capability wiring is chosen from the command's declared
    /// [`ConnMutation`](frogdb_core::ConnMutation) (its `CommandSpec.mutation`),
    /// so no command name is consulted here — the former `cmd_name == "CLIENT"` /
    /// `"MONITOR"` special-cases and the PubSub-by-strategy `matches!` are gone.
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
        // The command declares the connection-local mutable capabilities its
        // `ConnCtx` needs via its `CommandSpec` (`mutation`), so the builder is
        // selected from this datum — never from the command's string name. This
        // one `match` over `spec().mutation` replaces the former
        // `matches!(strategy, …PubSub)` plus the `cmd_name == "CLIENT"` /
        // `cmd_name == "MONITOR"` special-cases: dispatch no longer knows any
        // command's name. `command` is `'static`, so reading its spec does not
        // conflict with re-borrowing `self` to build the view.
        Some(match command.spec().mutation {
            // Pub/sub (one reply per channel) and MONITOR both own a
            // disjoint-borrow `Io` bundle that must outlive the `ConnCtx`, so
            // they are built at these call sites rather than in `conn_ctx_for`;
            // both are still selected from the declared capability, not the name.
            ConnMutation::PubSub => self.execute_pubsub(command, args).await,
            ConnMutation::Monitor => vec![self.execute_monitor(command, args).await],
            // Read-only (CONFIG/INFO/DEBUG/…), AUTH-class (ASKING/READONLY/
            // READWRITE), and CLIENT views build in place via the unified builder.
            // `execute_multi` is the uniform seam: its default wraps the single
            // response in a one-element `Vec`, so single-response commands are
            // unchanged.
            mutation @ (ConnMutation::None | ConnMutation::Auth | ConnMutation::Client) => {
                let mut ctx = self.conn_ctx_for(mutation);
                command.execute_multi(&mut ctx, args).await
            }
        })
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
    /// through this same match (see `transaction::execute_transaction`).
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
            // SHUTDOWN: signaling the main server is not wired up in this mode,
            // so we return a directive error rather than tearing down here.
            ServerWideOp::Shutdown => Response::error(
                "ERR SHUTDOWN is not supported in this mode. Use Ctrl+C to stop the server.",
            ),
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
    ///
    /// The `ConnCtx` is built from the RESET command's declared capability
    /// ([`ConnMutation::Auth`] → `conn_state = Some`), like every other mutating
    /// connection command.
    pub(crate) async fn execute_reset(&mut self, args: &[Bytes]) -> Response {
        let command = &crate::connection::connection_state_conn_command::RESET_CONN_COMMAND;
        let mutation = command.spec().mutation;
        let response = command
            .execute(&mut self.conn_ctx_for(mutation), args)
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
    ) -> Dispatched {
        for stage in PRE_DISPATCH_ORDER {
            match self.run_stage(stage, cmd, cmd_name).await {
                StageOutcome::Continue => {}
                StageOutcome::ShortCircuit(responses) => {
                    // Error accounting happens *here*, once, for whichever
                    // stage ended dispatch — so every stage is covered by
                    // construction and a new stage cannot silently skip
                    // errorstats (the bug this centralization fixed).
                    for response in &responses {
                        self.record_error_response(
                            response,
                            stage.rejects_pre_execution(),
                            cmd_name,
                        );
                    }
                    return Dispatched::Responses(responses);
                }
                StageOutcome::Handoff(handoff) => return Dispatched::Handoff(handoff),
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
            // to preserve the historical pre-auth *ordering*. The name match
            // decides only *when* to dispatch; the *how* (which ConnCtx builder)
            // still comes from the command's declared `mutation`
            // (`ConnMutation::Auth` → `conn_state = Some`), never from the name.
            DispatchStage::PreAuthIntercept => {
                if cmd_name == "AUTH" || cmd_name == "HELLO" {
                    let command = self
                        .core
                        .registry
                        .get_entry(cmd_name)
                        .and_then(|entry| entry.as_connection())
                        .expect("AUTH/HELLO are registered as connection commands");
                    let mutation = command.spec().mutation;
                    return StageOutcome::ShortCircuit(vec![
                        command
                            .execute(&mut self.conn_ctx_for(mutation), &cmd.args)
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
                let view = self.pre_dispatch_view();
                let error_response = view.run_pre_checks(cmd_name, &cmd.args);
                if let Some(error_response) = error_response {
                    // A rejection while a MULTI is open poisons the transaction,
                    // so EXEC answers `-EXECABORT` rather than a short (or
                    // empty) array. Redis does this for every pre-`call()`
                    // refusal: `rejectCommand` calls `flagTransaction`, setting
                    // `CLIENT_DIRTY_EXEC`. Without it, a NOAUTH / READONLY /
                    // CLUSTERDOWN / NOREPLICAS / NOADMIN / NOPERM / pubsub-mode
                    // rejection at queue time silently shortens the batch —
                    // the divergence recorded in testing-gap issue 33.
                    if view.state.in_transaction() {
                        let msg = match &error_response {
                            Response::Error(e) => Some(String::from_utf8_lossy(e).to_string()),
                            _ => None,
                        };
                        view.state.abort_transaction(msg);
                    }
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

            // Resolve the command in the registry and validate its arity BEFORE
            // the pause check, so unknown commands and wrong argument counts
            // return an immediate error even when paused (test: syntax errors
            // bypass pause). Both are pre-execution rejections, matching Redis's
            // `processCommand`, which rejects an unknown command ahead of
            // `call()`. Runs *after* `TransactionQueue` so an unknown command
            // inside a MULTI is still rejected by `queue_command`, which also
            // aborts the transaction.
            DispatchStage::CommandLookup => {
                let err = self
                    .pre_dispatch_view()
                    .command_lookup_check(cmd_name, &cmd.args);
                if let Some(err) = err {
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

            // Validate cluster slot ownership (returns CROSSSLOT/MOVED/ASK
            // errors). Sits ahead of *every* stage that can terminate a keyed
            // command — `ConnectionCommand` included — because the scripting
            // family (`EVAL`/`EVALSHA`/`FCALL`/…) is dispatched there and
            // declares its keys through `numkeys`. Running after it let a bare
            // script execute to completion on a node that does not own its
            // slot, acking a write the real owner never saw (FM-CLUSTER-030).
            // Everything between here and the old position is either
            // cluster-exempt by strategy (`ServerWide`, the non-scripting
            // connection families) or keyless (`REPLCONF`/`PSYNC`/`WAIT`), so
            // the hoist changes nothing but the script path.
            DispatchStage::ClusterSlotValidation => {
                match self.pre_dispatch_view().validate_cluster_slots(cmd) {
                    Some(cluster_error) => StageOutcome::ShortCircuit(vec![cluster_error]),
                    None => StageOutcome::Continue,
                }
            }

            // Registry-union dispatch: a command registered as
            // `CommandImpl::Connection` (CONFIG, BGSAVE/LASTSAVE, CLIENT, DEBUG,
            // MONITOR, ACL, INFO, HOTKEYS, FT.CURSOR, SLOWLOG, MEMORY, LATENCY,
            // STATUS, ASKING/READONLY/READWRITE, the pub/sub family, and the
            // scripting family) executes through its `ConnCtx` executor. The
            // `ConnCtx` builder is selected from each command's declared
            // `mutation` capability, so the mutating connection-state commands
            // (ASKING/READONLY/READWRITE → `conn_state = Some`) no longer need a
            // separate earlier stage — this is the sole connection-command
            // dispatch path.
            DispatchStage::ConnectionCommand => {
                match self.dispatch_connection_command(cmd_name, &cmd.args).await {
                    Some(responses) => StageOutcome::ShortCircuit(responses),
                    None => StageOutcome::Continue,
                }
            }

            // The replica handshake, both halves of it. A replica announces
            // itself with `REPLCONF listening-port` / `REPLCONF capa` and only
            // then sends `PSYNC`, and the two must be handled in the same place:
            // `PSYNC` is what *creates* the replica's session, so the
            // announcement has to be accumulated on the connection until then.
            // Handling `REPLCONF` on the shard path instead would drop it —
            // the shard executor has no connection to write it to, which was
            // exactly the `port=0` bug (FM-REPLICATION-049).
            //
            // For `PSYNC` this stage also owns the entire takeover decision: it
            // is the single site that role-gates the replication handoff *and*
            // parses the args, yielding a typed `StageOutcome::Handoff` (carried
            // out as a raw-socket takeover by the connection task) or a client
            // error. It never runs the shard `PsyncCommand::execute` — parsing
            // happens once, here.
            DispatchStage::ReplicationHandshake => {
                if cmd_name == "REPLCONF" {
                    // Only the *announcing* options are intercepted. `ACK`,
                    // `GETACK`, `ip-address` and every option this primary does
                    // not know fall through to the shard executor, which keeps
                    // answering them as before (FM-REPLICATION-018).
                    match AnnouncedOption::parse(&cmd.args) {
                        Ok(Some(option)) => {
                            self.replica_announcement.absorb(option);
                            return StageOutcome::ShortCircuit(vec![Response::ok()]);
                        }
                        Ok(None) => {}
                        Err(err) => {
                            return StageOutcome::ShortCircuit(vec![
                                announcement_error(err).to_response(),
                            ]);
                        }
                    }
                }
                if cmd_name == "PSYNC" {
                    // Role gate: only a primary can hand off a replication
                    // stream. Read live, because the handler itself is built on
                    // every role (a runtime promotion must be able to head a
                    // chain) — so the *role*, not the handler's presence, is
                    // what decides. A replica refusing PSYNC here is what keeps
                    // chained replication unsupported rather than silently
                    // half-working. This is the sole place this check lives.
                    if self.is_replica.load(std::sync::atomic::Ordering::Acquire)
                        || self.cluster.primary_replication_handler.is_none()
                    {
                        return StageOutcome::ShortCircuit(vec![Response::error(
                            "ERR PSYNC not supported - server is not running as primary",
                        )]);
                    }
                    // Parse once into the typed handoff, or surface the arg error
                    // exactly as the old `execute` did.
                    return match PsyncHandoff::from_args(&cmd.args) {
                        Ok(handoff) => StageOutcome::Handoff(handoff),
                        Err(err) => {
                            StageOutcome::ShortCircuit(vec![Response::Error(err.to_bytes())])
                        }
                    };
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
                // The error accounting for whatever this produces is the
                // driver's job (see `route_and_execute_with_transaction`).
                let final_response = self.handle_internal_action(response).await;

                StageOutcome::ShortCircuit(vec![final_response])
            }
        }
    }

    /// Classify and record an error response for error statistics.
    ///
    /// `is_rejected` = true means the command was rejected before it began
    /// executing (`rejected_calls`); false means it began executing and failed
    /// (`failed_calls`). Callers pass the *stage's* disposition
    /// ([`DispatchStage::rejects_pre_execution`]); this function still upgrades
    /// the classification for the pre-execution refusals that can only be
    /// detected downstream (see [`PRE_EXECUTION_ERROR_PREFIXES`]).
    ///
    /// Per-command (`cmdstat_*`) accounting is applied only for command names
    /// the registry knows: see [`Self::records_command_stats`].
    pub(crate) fn record_error_response(
        &self,
        response: &Response,
        is_rejected: bool,
        cmd_name: &str,
    ) {
        let Response::Error(bytes) = response else {
            return;
        };
        let prefix = frogdb_core::extract_error_prefix(bytes);
        let is_rejected = is_rejected || PRE_EXECUTION_ERROR_PREFIXES.contains(&prefix);
        let error_stats = &self.admin.client_registry.error_stats;
        let per_command = self.records_command_stats(cmd_name);
        if is_rejected {
            error_stats.record_rejected(prefix);
            if per_command {
                self.admin.client_registry.record_command_rejected(cmd_name);
            }
        } else {
            error_stats.record_failed(prefix);
            if per_command {
                self.admin.client_registry.record_command_failed(cmd_name);
            }
        }
    }

    /// Whether `cmd_name` may key a per-command statistic (`cmdstat_<name>`,
    /// latency histogram).
    ///
    /// Only registered command names qualify. An unrecognized name is raw
    /// client input, so keying a stats map by it lets any client grow those
    /// maps without bound — the cardinality guard `errorstat_*` already has in
    /// `MAX_ERROR_TYPES`. Redis reaches the same place structurally: its
    /// per-command counters live *on the command table entry*, so a command
    /// that isn't in the table simply has nowhere to count.
    pub(crate) fn records_command_stats(&self, cmd_name: &str) -> bool {
        self.core.registry.get_entry(cmd_name).is_some()
    }
}

/// Error prefixes that always denote a *rejection* — the command was refused
/// and never ran — even when the refusal is only detectable downstream of the
/// pre-dispatch gauntlet.
///
/// `OOM`: the `maxmemory` gate lives shard-side (`core/src/shard/eviction.rs`,
/// `check_memory_for_write`) because the memory accounting it reads is the
/// shard's own, so it fires inside the terminal `Execute` stage — but it fires
/// *before* the write executes and nothing is mutated. Redis performs the same
/// check in `processCommand` and counts it in `rejected_calls`; classifying it
/// here keeps FrogDB's split faithful without hoisting a shard-local memory
/// read onto the connection's hot path.
const PRE_EXECUTION_ERROR_PREFIXES: &[&str] = &["OOM"];

#[cfg(test)]
mod tests {
    use super::{DispatchStage, PRE_DISPATCH_ORDER};
    use frogdb_core::{CommandRegistry, ExecutionStrategy, ScatterGatherOp, ServerWideOp};

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
            CommandLookup,
            PauseGate,
            ClusterSlotValidation,
            ConnectionCommand,
            ReplicationHandshake,
            WaitIntercept,
            ServerWide,
            ClusterSlotSubcommand,
            MigratingTryAgain,
            Execute,
        ] {
            assert_eq!(
                PRE_DISPATCH_ORDER.iter().filter(|&&s| s == stage).count(),
                1,
                "{stage:?} must appear exactly once in PRE_DISPATCH_ORDER"
            );
        }
        assert_eq!(PRE_DISPATCH_ORDER.len(), 16);
    }

    /// The must-precede relation as data: each pair `(a, b)` asserts stage `a`
    /// runs strictly before stage `b` in [`PRE_DISPATCH_ORDER`]. This is the
    /// single written home of the load-bearing ordering constraints the module
    /// docs (and Redis's `processCommand`) justify — validated against the
    /// canonical order by [`load_bearing_ordering_invariants`], so a reorder
    /// that compiles but breaks a correctness invariant fails here. Models
    /// core's `WRITE_EFFECT_ORDER::MUST_PRECEDE` relation style (a const pair
    /// list validated in a loop rather than N hand-written asserts).
    ///
    /// `PreAuthIntercept`-first and `Execute`-terminal are the two endpoint
    /// invariants, checked directly rather than enumerated pairwise.
    const MUST_PRECEDE: &[(DispatchStage, DispatchStage)] = {
        use DispatchStage::*;
        &[
            // AUTH/HELLO are intercepted before the NOAUTH pre-check so a
            // not-yet-authenticated client can authenticate.
            (PreAuthIntercept, PreChecks),
            // RESET runs after the pre-checks gate (it is never queued/paused,
            // but pre-checks still front every dispatch/execute stage).
            (PreChecks, ResetIntercept),
            // Pre-checks (auth/replica/quorum/ACL) front every stage that
            // dispatches to or executes a command.
            (PreChecks, ConnectionCommand),
            (PreChecks, ServerWide),
            (PreChecks, Execute),
            // MULTI/EXEC/DISCARD control is recognized before the queuing stage
            // that would otherwise swallow them as ordinary queued commands.
            (TransactionControl, TransactionQueue),
            // Command lookup (unknown command + arity) runs before pause so
            // syntax errors bypass CLIENT PAUSE.
            (CommandLookup, PauseGate),
            // …and after queuing, so an unknown/wrong-arity command inside a
            // MULTI is rejected by `queue_command`, which also aborts the
            // transaction (rejecting it here instead would leave the
            // transaction live).
            (TransactionQueue, CommandLookup),
            // Redis rejects an unauthenticated client with NOAUTH before it
            // reports an unknown command name, so the pre-checks gate fronts
            // the lookup.
            (PreChecks, CommandLookup),
            // Queued MULTI commands must not block on pause.
            (TransactionQueue, PauseGate),
            // Queued commands slot-validate at queue time; the standalone
            // slot-validation stage runs later on the non-transaction path.
            // EXEC re-validates the *whole* queue against one cluster snapshot,
            // but that lives in `transaction.rs`
            // (`PreDispatchView::validate_queued_batch`), not as a gauntlet
            // stage — it is a batch decision, not a per-command pre-dispatch
            // concern, and adding a 17th stage for it would be wrong.
            (TransactionQueue, ClusterSlotValidation),
            // Unknown-command/arity errors outrank a redirect: Redis reports an
            // unknown command before it would report `MOVED`.
            (CommandLookup, ClusterSlotValidation),
            // FM-CLUSTER-030. Slot routing fronts *every* stage that can
            // terminate a keyed command, not just `Execute`. The scripting
            // family (`EVAL`/`EVALSHA`/`FCALL`/…) is
            // `ConnectionLevel(Scripting)` and declares its keys through
            // `numkeys`, so a bare script short-circuiting at
            // `ConnectionCommand` ahead of this stage ran to completion on a
            // non-owner and acked its writes there — the hole rework issue 11
            // confirmed. `is_cluster_exempt` refusing to exempt `Scripting`
            // states the intent; this pair is what enforces it.
            (ClusterSlotValidation, ConnectionCommand),
            // Slot routing (MOVED/ASK/CROSSSLOT) precedes the multi-key
            // TRYAGAIN check, which precedes the terminal execute stage.
            (ClusterSlotValidation, MigratingTryAgain),
            (MigratingTryAgain, Execute),
        ]
    };

    // FM-TXN-006
    /// Validates [`PRE_DISPATCH_ORDER`] against every declared constraint — the
    /// endpoint invariants plus all [`MUST_PRECEDE`] pairs. Would catch a
    /// reorder that compiles but breaks a correctness invariant.
    #[test]
    fn load_bearing_ordering_invariants() {
        use DispatchStage::*;
        // PreAuthIntercept is first: a not-yet-authenticated client must be able
        // to reach AUTH/HELLO before anything else can reject it.
        assert_eq!(
            *PRE_DISPATCH_ORDER.first().unwrap(),
            PreAuthIntercept,
            "PRE_DISPATCH_ORDER must begin with PreAuthIntercept (pre-auth)"
        );
        // Execute is the non-returning terminal.
        assert_eq!(
            *PRE_DISPATCH_ORDER.last().unwrap(),
            Execute,
            "PRE_DISPATCH_ORDER must end in Execute (the driver loop is total only because of this)"
        );
        for &(a, b) in MUST_PRECEDE {
            assert!(
                order_index(a) < order_index(b),
                "{a:?} must run strictly before {b:?} in PRE_DISPATCH_ORDER"
            );
        }
    }

    // FM-TXN-046
    /// Every stage's error disposition, pinned as data: `true` = an error from
    /// this stage is a `rejected_call` (nothing executed), `false` = a
    /// `failed_call` (an executor ran and errored). Guards the split the INFO
    /// `commandstats` contract exposes — e.g. flipping `ConnectionCommand` to
    /// "rejected" would silently misreport every `NOSCRIPT` as a rejection.
    #[test]
    fn stage_error_disposition_is_the_guard_dispatch_split() {
        use DispatchStage::*;
        let expected: &[(DispatchStage, bool)] = &[
            (PreAuthIntercept, false),
            (PreChecks, true),
            (ResetIntercept, false),
            (PubSubPing, true),
            (TransactionControl, false),
            (TransactionQueue, true),
            (CommandLookup, true),
            (PauseGate, false),
            (ClusterSlotValidation, true),
            (ConnectionCommand, false),
            (ReplicationHandshake, false),
            (WaitIntercept, false),
            (ServerWide, false),
            (ClusterSlotSubcommand, false),
            (MigratingTryAgain, true),
            (Execute, false),
        ];
        assert_eq!(
            expected.len(),
            PRE_DISPATCH_ORDER.len(),
            "every stage must declare an error disposition"
        );
        for &(stage, want) in expected {
            assert_eq!(
                stage.rejects_pre_execution(),
                want,
                "{stage:?} error disposition changed"
            );
        }
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

    /// Every connection command declares the [`ConnMutation`] capability the
    /// dispatcher selects its `ConnCtx` builder from — pinned here as data so a
    /// migrated command silently drifting to the wrong capability (which would
    /// wire the wrong `ConnCtx` slots — e.g. CLIENT losing `tracking`, or
    /// ASKING losing `conn_state`) fails this test rather than misbehaving at
    /// runtime. Mirrors the `*_ops_are_unique_per_command` registry-iteration
    /// style, and is the data-driven guard `dispatch_connection_command` relies
    /// on now that it consults `spec().mutation` instead of the command name.
    ///
    /// Also asserts each spec's `validate()` passes, exercising the
    /// mutation↔strategy cross-check ([`SpecError::ConnMutationStrategyMismatch`]).
    #[test]
    fn connection_commands_declare_expected_mutation() {
        use frogdb_core::ConnMutation;

        let mut registry = CommandRegistry::new();
        crate::register_commands(&mut registry);

        // (command name, expected declared capability).
        let expected: &[(&str, ConnMutation)] = &[
            // Auth-class: mutate per-connection auth/protocol/transaction state.
            ("AUTH", ConnMutation::Auth),
            ("HELLO", ConnMutation::Auth),
            ("RESET", ConnMutation::Auth),
            ("ASKING", ConnMutation::Auth),
            ("READONLY", ConnMutation::Auth),
            ("READWRITE", ConnMutation::Auth),
            ("MULTI", ConnMutation::Auth),
            ("EXEC", ConnMutation::Auth),
            ("DISCARD", ConnMutation::Auth),
            ("WATCH", ConnMutation::Auth),
            ("UNWATCH", ConnMutation::Auth),
            // CLIENT: conn_state + tracking IO.
            ("CLIENT", ConnMutation::Client),
            // MONITOR: monitor receiver.
            ("MONITOR", ConnMutation::Monitor),
            // Pub/sub family: multi-response over the pub/sub machinery.
            ("SUBSCRIBE", ConnMutation::PubSub),
            ("UNSUBSCRIBE", ConnMutation::PubSub),
            ("PSUBSCRIBE", ConnMutation::PubSub),
            ("PUNSUBSCRIBE", ConnMutation::PubSub),
            ("SSUBSCRIBE", ConnMutation::PubSub),
            ("SUNSUBSCRIBE", ConnMutation::PubSub),
            ("PUBLISH", ConnMutation::PubSub),
            ("SPUBLISH", ConnMutation::PubSub),
            ("PUBSUB", ConnMutation::PubSub),
            // Pure reads: dispatched through the read-only ConnCtx view.
            ("CONFIG", ConnMutation::None),
            ("INFO", ConnMutation::None),
            ("DEBUG", ConnMutation::None),
            ("ACL", ConnMutation::None),
            ("HOTKEYS", ConnMutation::None),
            ("BGSAVE", ConnMutation::None),
            ("LASTSAVE", ConnMutation::None),
            ("SLOWLOG", ConnMutation::None),
            ("MEMORY", ConnMutation::None),
            ("LATENCY", ConnMutation::None),
            ("EVAL", ConnMutation::None),
            ("SCRIPT", ConnMutation::None),
            ("FUNCTION", ConnMutation::None),
            ("FT.CURSOR", ConnMutation::None),
        ];

        for (name, want) in expected {
            let command = registry
                .get_entry(name)
                .unwrap_or_else(|| panic!("{name} is not registered"))
                .as_connection()
                .unwrap_or_else(|| panic!("{name} is not a connection command"));
            assert_eq!(
                command.spec().mutation,
                *want,
                "{name} declared the wrong ConnMutation"
            );
            assert!(
                command.spec().validate().is_ok(),
                "{name} spec invalid: {:?}",
                command.spec().validate()
            );
        }
    }

    /// Each `ScatterGatherOp` must be declared by at most one command spec.
    /// The exhaustive `match op` in `dispatch_scatter` (routing.rs) already
    /// guarantees every op has a handler (adding a variant without an arm is a
    /// compile error); this catches the opposite mistake — a wrong-op
    /// copy-paste that points two commands at the same scatter op, which would
    /// silently route one command through the other's merge strategy.
    #[test]
    fn scatter_gather_ops_are_unique_per_command() {
        let mut registry = CommandRegistry::new();
        crate::register_commands(&mut registry);

        let mut seen: Vec<(ScatterGatherOp, String)> = Vec::new();
        for (name, entry) in registry.iter() {
            if let ExecutionStrategy::ScatterGather(op) = entry.execution_strategy() {
                if let Some((_, prev)) = seen.iter().find(|(seen_op, _)| *seen_op == op) {
                    panic!(
                        "ScatterGatherOp::{:?} declared by two commands: {} and {}",
                        op, prev, name
                    );
                }
                seen.push((op, name.to_string()));
            }
        }
    }
}
