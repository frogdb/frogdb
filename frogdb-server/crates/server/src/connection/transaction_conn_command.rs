//! Transaction commands: MULTI, EXEC, DISCARD, WATCH, UNWATCH.
//!
//! These are the transaction state machine, migrated behind the
//! [`ConnectionCommand`] seam. Like the connection-state commands
//! (RESET/ASKING/READONLY/READWRITE) they **mutate** per-connection state, so
//! they dispatch through a `ConnCtx` whose [`ConnCtx::conn_state`] is
//! `Some(&mut dyn ConnStateMut)` (built by
//! [`ConnectionHandler::conn_ctx_authmut`](crate::connection::ConnectionHandler))
//! and drive their transitions through the transaction methods on
//! [`frogdb_core::ConnStateMut`] (`begin_multi`, `is_in_multi`, `watch_key`,
//! `fold_transaction_shard`, `unwatch`, `discard`).
//!
//! * **MULTI / DISCARD / UNWATCH** are pure state transitions. DISCARD
//!   additionally records the `discarded` transaction metric via
//!   [`ConnCtx::metrics_recorder`].
//! * **WATCH** reads a version from the owning shard (a `GetVersion` round-trip
//!   over [`ConnCtx::shard_senders`]) before recording the watched keys.
//! * **EXEC** is the finalizer's one special case: its orchestration —
//!   draining the queued commands over the shard(s) and running the deferred
//!   connection-level commands (which re-enter the `ConnCtx` dispatch machinery,
//!   the *meta-circularity*) — needs the whole [`ConnectionHandler`] and cannot
//!   be expressed against the narrow [`ConnCtx`]. It therefore stays in
//!   [`ConnectionHandler::handle_exec`](crate::connection::ConnectionHandler),
//!   to which the connection layer dispatches EXEC directly (see
//!   [`ConnectionHandler::dispatch_transaction_command`]). This executor exists
//!   only to own EXEC's single-source [`CommandSpec`] and be registered as
//!   [`frogdb_core::CommandImpl::Connection`] (which deletes the former shard
//!   stub); [`ExecConnCommand::execute`] is never reached through the seam.
//!
//! All five are intercepted *before* the transaction-queuing check in
//! [`route_and_execute_with_transaction`](crate::connection::ConnectionHandler),
//! so they are never queued inside a MULTI — matching Redis semantics.

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, BoxFuture, Command, CommandContext, CommandError, CommandFlags, CommandSpec,
    ConnCtx, ConnectionCommand, ConnectionLevelOp, EventSpec, ExecutionStrategy, KeySpec,
    LookupSpec, ShardMessage, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;
use tokio::sync::oneshot;

use crate::connection::ConnectionHandler;
use crate::slot_migration::SlotValidator;

/// Build a `CommandSpec` for a transaction command. All five share the
/// `ConnectionLevel(Transaction)` strategy, no WAL, and no keyspace event; they
/// differ only in name, arity, flags, and key spec (WATCH takes keys).
const fn transaction_spec(
    name: &'static str,
    arity: Arity,
    flags: CommandFlags,
    keys: KeySpec,
) -> CommandSpec {
    CommandSpec {
        name,
        arity,
        flags,
        keys,
        access: AccessSpec::Uniform,
        wal: WalStrategy::NoOp,
        wakes: WaiterWake::None,
        event: EventSpec::NotApplicable,
        requires_same_slot: false,
        lookup: LookupSpec::None,
        strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Transaction),
    }
}

// ---------------------------------------------------------------------------
// MULTI
// ---------------------------------------------------------------------------

/// The `CommandSpec` for MULTI (flags preserved from the former `MultiCommand`).
static MULTI_SPEC: CommandSpec = transaction_spec(
    "MULTI",
    Arity::Fixed(0),
    CommandFlags::FAST
        .union(CommandFlags::LOADING)
        .union(CommandFlags::STALE),
    KeySpec::None,
);

/// The registrable, `'static` MULTI executor.
pub(crate) static MULTI_CONN_COMMAND: MultiConnCommand = MultiConnCommand;

/// MULTI — begin a transaction block.
pub(crate) struct MultiConnCommand;

impl ConnectionCommand for MultiConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &MULTI_SPEC
    }

    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        _args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        Box::pin(async move {
            let state = ctx
                .conn_state
                .as_deref_mut()
                .expect("MULTI is dispatched with a mutable conn_state");
            if state.begin_multi() {
                Response::ok()
            } else {
                Response::error("ERR MULTI calls can not be nested")
            }
        })
    }
}

// ---------------------------------------------------------------------------
// EXEC
// ---------------------------------------------------------------------------

/// The `CommandSpec` for EXEC (flags preserved from the former `ExecCommand`).
static EXEC_SPEC: CommandSpec = transaction_spec(
    "EXEC",
    Arity::Fixed(0),
    CommandFlags::LOADING.union(CommandFlags::STALE),
    KeySpec::None,
);

/// The registrable, `'static` EXEC executor (a spec carrier — see the module
/// docs and [`ExecConnCommand::execute`]).
pub(crate) static EXEC_CONN_COMMAND: ExecConnCommand = ExecConnCommand;

/// EXEC — execute the queued transaction.
///
/// The real implementation is
/// [`ConnectionHandler::handle_exec`](crate::connection::ConnectionHandler),
/// dispatched directly by
/// [`ConnectionHandler::dispatch_transaction_command`]. This struct exists only
/// to own EXEC's single-source [`CommandSpec`] and be registered as a
/// [`frogdb_core::CommandImpl::Connection`]; its `execute` is never reached
/// through the `ConnCtx` seam.
pub(crate) struct ExecConnCommand;

impl ConnectionCommand for ExecConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &EXEC_SPEC
    }

    fn execute<'a>(
        &'a self,
        _ctx: &'a mut ConnCtx<'a>,
        _args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        // EXEC's orchestration needs the whole ConnectionHandler and is dispatched
        // via `handle_exec`, never through this executor. Reaching here is a
        // dispatch bug: surface it loudly in debug builds and return an internal
        // error (not a fabricated success) in release.
        Box::pin(async {
            debug_assert!(
                false,
                "EXEC dispatches via ConnectionHandler::handle_exec, not the ConnCtx seam"
            );
            Response::error("ERR internal: EXEC must be dispatched via handle_exec")
        })
    }
}

// ---------------------------------------------------------------------------
// DISCARD
// ---------------------------------------------------------------------------

/// The `CommandSpec` for DISCARD (flags preserved from the former `DiscardCommand`).
static DISCARD_SPEC: CommandSpec = transaction_spec(
    "DISCARD",
    Arity::Fixed(0),
    CommandFlags::FAST
        .union(CommandFlags::LOADING)
        .union(CommandFlags::STALE),
    KeySpec::None,
);

/// The registrable, `'static` DISCARD executor.
pub(crate) static DISCARD_CONN_COMMAND: DiscardConnCommand = DiscardConnCommand;

/// DISCARD — abort the transaction and clear the command queue.
pub(crate) struct DiscardConnCommand;

impl ConnectionCommand for DiscardConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &DISCARD_SPEC
    }

    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        _args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        Box::pin(async move {
            // Copy the shared metrics recorder out before taking the mutable
            // `conn_state` borrow so the two disjoint uses do not overlap.
            let recorder = ctx.metrics_recorder;
            let state = ctx
                .conn_state
                .as_deref_mut()
                .expect("DISCARD is dispatched with a mutable conn_state");
            match state.discard() {
                Some(metrics) => {
                    // Record the `discarded` transaction metric. DISCARD is the
                    // only transaction outcome recorded outside `handle_exec`'s
                    // `record_transaction_outcome` (it has no handler to call),
                    // so it emits the `discarded` label directly here.
                    frogdb_telemetry::definitions::TransactionsTotal::inc(recorder, "discarded");
                    frogdb_telemetry::definitions::TransactionsQueuedCommands::observe(
                        recorder,
                        metrics.queued_count as f64,
                        "discarded",
                    );
                    if let Some(start) = metrics.start_time {
                        frogdb_telemetry::definitions::TransactionsDuration::observe(
                            recorder,
                            start.elapsed().as_secs_f64(),
                            "discarded",
                        );
                    }
                    Response::ok()
                }
                None => Response::error("ERR DISCARD without MULTI"),
            }
        })
    }
}

// ---------------------------------------------------------------------------
// WATCH
// ---------------------------------------------------------------------------

/// The `CommandSpec` for WATCH (flags preserved from the former `WatchCommand`).
static WATCH_SPEC: CommandSpec = transaction_spec(
    "WATCH",
    Arity::AtLeast(1),
    CommandFlags::FAST
        .union(CommandFlags::LOADING)
        .union(CommandFlags::STALE),
    KeySpec::All,
);

/// The registrable, `'static` WATCH executor.
pub(crate) static WATCH_CONN_COMMAND: WatchConnCommand = WatchConnCommand;

/// WATCH — watch keys for modifications (optimistic locking).
pub(crate) struct WatchConnCommand;

impl ConnectionCommand for WatchConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &WATCH_SPEC
    }

    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        Box::pin(async move { handle_watch(ctx, args).await })
    }
}

/// Shard-local key-extraction stub for WATCH, registered into the registry's
/// `commands` map (via `register`) alongside [`WATCH_CONN_COMMAND`] in the
/// entries map (via `register_connection`). WATCH is the only transaction
/// command with keys, and `COMMAND GETKEYS` resolves through the `commands` map
/// (`CommandRegistry::get`), which holds only shard `Command`s — so without this
/// stub `COMMAND GETKEYS WATCH k` would lose its keys. This mirrors the shard
/// stubs kept for the migrated scripting commands (EVAL/EVALSHA/SCRIPT) for the
/// same reason. It is never *executed*: dispatch always routes WATCH through the
/// `Connection` entry, so `execute` returns a loud internal error rather than a
/// fabricated success.
pub(crate) struct WatchCommand;

impl Command for WatchCommand {
    fn spec(&self) -> &'static CommandSpec {
        &WATCH_SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // WATCH is a connection command dispatched via
        // `dispatch_transaction_command`; this shard stub exists only for
        // `COMMAND GETKEYS` key extraction and is never executed.
        Err(CommandError::Internal {
            message: "WATCH should be handled by the connection handler".to_string(),
        })
    }
}

/// WATCH the given keys after fetching their current version from the owning
/// shard. Mirrors the former `ConnectionHandler::handle_watch` exactly.
async fn handle_watch(ctx: &mut ConnCtx<'_>, args: &[Bytes]) -> Response {
    // Copy the shared subsystem references out before taking the mutable
    // `conn_state` borrow so the disjoint uses do not overlap.
    let shard_senders = ctx.shard_senders;
    let num_shards = ctx.num_shards;

    let state = ctx
        .conn_state
        .as_deref_mut()
        .expect("WATCH is dispatched with a mutable conn_state");

    // WATCH is not allowed inside MULTI.
    if state.is_in_multi() {
        return Response::error("ERR WATCH inside MULTI is not allowed");
    }

    // Validate arity (dispatch runs before the generic arity check, so WATCH
    // owns this).
    if args.is_empty() {
        return Response::error("ERR wrong number of arguments for 'watch' command");
    }

    // All watched keys must live on one shard. The validator owns the loop and
    // the CROSSSLOT rejection. Arity (>= 1 arg) was checked above, so `Ok(None)`
    // is unreachable.
    let shard = match SlotValidator::same_shard(args, num_shards) {
        Ok(Some(shard)) => shard,
        Ok(None) => {
            return Response::error("ERR wrong number of arguments for 'watch' command");
        }
        Err(crossslot) => return crossslot,
    };

    // Get the current version from the shard.
    let (response_tx, response_rx) = oneshot::channel();
    if shard_senders[shard]
        .send(ShardMessage::GetVersion { response_tx })
        .await
        .is_err()
    {
        return Response::error("ERR shard unavailable");
    }
    let version = match response_rx.await {
        Ok(v) => v,
        Err(_) => return Response::error("ERR shard dropped request"),
    };

    // Store watched keys with their versions, then fold the shard into the
    // transaction target.
    for key in args {
        state.watch_key(key.clone(), shard, version);
    }
    state.fold_transaction_shard(shard);

    Response::ok()
}

// ---------------------------------------------------------------------------
// UNWATCH
// ---------------------------------------------------------------------------

/// The `CommandSpec` for UNWATCH (flags preserved from the former `UnwatchCommand`).
static UNWATCH_SPEC: CommandSpec = transaction_spec(
    "UNWATCH",
    Arity::Fixed(0),
    CommandFlags::FAST
        .union(CommandFlags::LOADING)
        .union(CommandFlags::STALE),
    KeySpec::None,
);

/// The registrable, `'static` UNWATCH executor.
pub(crate) static UNWATCH_CONN_COMMAND: UnwatchConnCommand = UnwatchConnCommand;

/// UNWATCH — forget all watched keys.
pub(crate) struct UnwatchConnCommand;

impl ConnectionCommand for UnwatchConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &UNWATCH_SPEC
    }

    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        _args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        Box::pin(async move {
            let state = ctx
                .conn_state
                .as_deref_mut()
                .expect("UNWATCH is dispatched with a mutable conn_state");
            state.unwatch();
            Response::ok()
        })
    }
}

impl ConnectionHandler {
    /// Dispatch a transaction control command (MULTI/EXEC/DISCARD/WATCH/UNWATCH)
    /// migrated behind the ConnCtx seam.
    ///
    /// MULTI/DISCARD/WATCH/UNWATCH mutate per-connection transaction state and
    /// dispatch through their `CommandImpl::Connection` executor over the
    /// *mutable* [`conn_ctx_authmut`](Self::conn_ctx_authmut) view
    /// (`conn_state = Some`). EXEC's orchestration cannot be expressed against
    /// the narrow `ConnCtx` (it re-enters the dispatch machinery for the deferred
    /// connection-level commands — the meta-circularity), so it is dispatched
    /// directly to [`handle_exec`](Self::handle_exec).
    ///
    /// Returns `Some(responses)` for these five commands; `None` otherwise. The
    /// caller intercepts this *before* the transaction-queuing check, so these
    /// commands are never queued inside a MULTI.
    pub(crate) async fn dispatch_transaction_command(
        &mut self,
        cmd_name: &str,
        args: &[Bytes],
    ) -> Option<Vec<Response>> {
        match cmd_name {
            "EXEC" => Some(self.handle_exec().await),
            "MULTI" | "DISCARD" | "WATCH" | "UNWATCH" => {
                // `as_connection()` yields a `'static` reference, so it does not
                // conflict with re-borrowing `self` to build the mutable ConnCtx.
                let command = self.core.registry.get_entry(cmd_name)?.as_connection()?;
                Some(vec![
                    command.execute(&mut self.conn_ctx_authmut(), args).await,
                ])
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::ClusterDeps;
    use crate::connection::state::ConnectionState;
    use crate::cursor_store::AggregateCursorStore;
    use frogdb_core::persistence::NoopSnapshotCoordinator;
    use frogdb_core::{
        AclManager, ClientRegistry, CommandLatencyHistograms, CommandRegistry, KeyspaceStats,
        SharedHotkeySession, new_shared_hotkey_session,
    };
    use frogdb_protocol::ProtocolVersion;

    /// Build a mutable-`ConnCtx` over fixture dependencies (mirrors the
    /// connection-state fixture): the transaction commands exercise the mutable
    /// `conn_state` (a real [`ConnectionState`]) plus `metrics_recorder`
    /// (DISCARD); the rest are unused placeholders. WATCH's shard round-trip is
    /// not exercised here (no shards), so only its guard paths are tested.
    struct Fixture {
        acl_manager: std::sync::Arc<AclManager>,
        command_registry: CommandRegistry,
        client_registry: ClientRegistry,
        latency_histograms: CommandLatencyHistograms,
        keyspace_stats: KeyspaceStats,
        config_manager: crate::runtime_config::ConfigManager,
        snapshot_coordinator: NoopSnapshotCoordinator,
        hotkey_session: SharedHotkeySession,
        cluster: ClusterDeps,
        cursor_store: AggregateCursorStore,
        metrics_recorder: frogdb_core::NoopMetricsRecorder,
        memory_diag: crate::connection::observability_conn_command::MemoryDiag,
        state: ConnectionState,
    }

    impl Fixture {
        fn new() -> Self {
            let mut command_registry = CommandRegistry::new();
            crate::register_commands(&mut command_registry);
            Self {
                acl_manager: AclManager::new(Default::default()),
                command_registry,
                client_registry: ClientRegistry::new(),
                latency_histograms: CommandLatencyHistograms::new(true),
                keyspace_stats: KeyspaceStats::new(),
                config_manager: crate::runtime_config::ConfigManager::new(
                    &crate::config::Config::default(),
                ),
                snapshot_coordinator: NoopSnapshotCoordinator::new(),
                hotkey_session: new_shared_hotkey_session(),
                cluster: ClusterDeps::standalone(),
                cursor_store: AggregateCursorStore::new(),
                metrics_recorder: frogdb_core::NoopMetricsRecorder::new(),
                memory_diag: crate::connection::observability_conn_command::MemoryDiag(
                    frogdb_debug::MemoryDiagConfig::default(),
                ),
                state: ConnectionState::new(1, "127.0.0.1:0".parse().unwrap(), false),
            }
        }

        fn ctx_mut(&mut self) -> ConnCtx<'_> {
            static NOOP_INFO: frogdb_core::NoopInfoProvider = frogdb_core::NoopInfoProvider;
            ConnCtx {
                config: &self.config_manager,
                client_registry: &self.client_registry,
                latency_histograms: &self.latency_histograms,
                keyspace_stats: &self.keyspace_stats,
                shard_senders: &[],
                snapshot_coordinator: &self.snapshot_coordinator,
                hotkey_session: &self.hotkey_session,
                hotkey_cluster: &self.cluster,
                protocol_version: ProtocolVersion::default(),
                cursor_store: &self.cursor_store,
                acl_manager: self.acl_manager.as_ref(),
                command_registry: &self.command_registry,
                username: "",
                metrics_recorder: &self.metrics_recorder,
                memory_diag: &self.memory_diag,
                num_shards: 0,
                max_clients: 10000,
                info: &NOOP_INFO,
                scripting: &frogdb_core::NoopScriptingProvider,
                conn_state: Some(&mut self.state),
                tracking: None,
                pubsub: None,
                monitor: None,
            }
        }
    }

    #[tokio::test]
    async fn multi_begins_and_rejects_nested() {
        let mut fx = Fixture::new();
        let resp = MultiConnCommand.execute(&mut fx.ctx_mut(), &[]).await;
        assert_eq!(resp, Response::ok());
        assert!(fx.state.in_transaction(), "MULTI opens a transaction");

        // Nested MULTI is rejected without disturbing the open transaction.
        let resp = MultiConnCommand.execute(&mut fx.ctx_mut(), &[]).await;
        assert!(matches!(resp, Response::Error(_)), "nested MULTI errors");
        assert!(fx.state.in_transaction());
    }

    #[tokio::test]
    async fn discard_without_multi_errors_then_drops_open_transaction() {
        let mut fx = Fixture::new();
        let resp = DiscardConnCommand.execute(&mut fx.ctx_mut(), &[]).await;
        assert!(
            matches!(resp, Response::Error(_)),
            "DISCARD without MULTI errors"
        );

        assert!(fx.state.begin_transaction().is_ok());
        let resp = DiscardConnCommand.execute(&mut fx.ctx_mut(), &[]).await;
        assert_eq!(resp, Response::ok());
        assert!(!fx.state.in_transaction(), "DISCARD drops the transaction");
    }

    #[tokio::test]
    async fn watch_inside_multi_is_rejected() {
        let mut fx = Fixture::new();
        assert!(fx.state.begin_transaction().is_ok());
        let resp = WatchConnCommand
            .execute(&mut fx.ctx_mut(), &[Bytes::from_static(b"k")])
            .await;
        assert!(
            matches!(resp, Response::Error(_)),
            "WATCH inside MULTI is rejected"
        );
    }

    #[tokio::test]
    async fn watch_without_keys_errors() {
        let mut fx = Fixture::new();
        let resp = WatchConnCommand.execute(&mut fx.ctx_mut(), &[]).await;
        assert!(matches!(resp, Response::Error(_)), "WATCH needs >= 1 key");
    }

    #[tokio::test]
    async fn unwatch_is_ok_and_clears_watches() {
        let mut fx = Fixture::new();
        fx.state.watch_key(Bytes::from_static(b"k"), 0, 7);
        let resp = UnwatchConnCommand.execute(&mut fx.ctx_mut(), &[]).await;
        assert_eq!(resp, Response::ok());
        assert_eq!(
            fx.state.watched_key_iter().count(),
            0,
            "UNWATCH clears all watched keys"
        );
    }

    #[test]
    fn specs_are_transaction_and_valid() {
        for spec in [
            MULTI_CONN_COMMAND.spec(),
            EXEC_CONN_COMMAND.spec(),
            DISCARD_CONN_COMMAND.spec(),
            WATCH_CONN_COMMAND.spec(),
            UNWATCH_CONN_COMMAND.spec(),
        ] {
            assert!(spec.validate().is_ok(), "{} spec invalid", spec.name);
            assert!(matches!(
                spec.strategy,
                ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Transaction)
            ));
        }
    }
}
