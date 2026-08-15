//! Transaction commands: MULTI, EXEC, DISCARD, WATCH, UNWATCH.
//!
//! These are the transaction state machine, migrated behind the
//! [`ConnectionCommand`] seam. Like the connection-state commands
//! (RESET/ASKING/READONLY/READWRITE) they **mutate** per-connection state, so
//! they dispatch through a `ConnCtx` whose [`ConnCtx::conn_state`] is
//! `Some(&mut dyn ConnStateMut)` (built by
//! [`ConnectionHandler::conn_ctx_for`](crate::connection::ConnectionHandler))
//! and drive their transitions through the transaction methods on
//! [`frogdb_core::ConnStateMut`] (`begin_multi`, `is_in_multi`, `watch_key`,
//! `unwatch`, `discard`).
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
    AccessSpec, Arity, BoxFuture, CommandFlags, CommandSpec, ConnCtx, ConnectionCommand,
    ConnectionLevelOp, CoreMsg, EventSpec, ExecutionStrategy, KeySpec, LookupSpec, WaiterWake,
    WalStrategy,
};
use frogdb_protocol::Response;
use tokio::sync::oneshot;

use crate::connection::ConnectionHandler;

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
        reindex: frogdb_core::ReindexSpec::None,
        lookup: LookupSpec::None,
        mutation: frogdb_core::ConnMutation::Auth,
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
                    // only transaction outcome recorded outside
                    // `frogdb_txn::handle_exec` (it has no EXEC handler to run
                    // through), so it emits the `discarded` label directly —
                    // through the same metric-shape helper, so the triple stays
                    // defined in one place.
                    frogdb_txn::record_transaction_metrics(
                        recorder,
                        "discarded",
                        metrics.queued_count,
                        metrics.start_time,
                    );
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

    // A watch set is *not* co-location-constrained. Only the queued batch is
    // (FM-TXN-019): a watch names a CAS precondition, not work to run
    // atomically, so `WATCH a b` must mean exactly what `WATCH a` then
    // `WATCH b` means — semantics cannot depend on how a client packs its
    // arguments. Cluster-mode slot ownership is a separate verdict, already
    // taken in the `TransactionControl` stage (`validate_watch_slots`,
    // FM-TXN-048).
    //
    // So group the keys by owning shard and probe each shard for its own keys.
    // A shard only maintains slot versions for the slots it owns; asking one
    // shard about another's key would answer from an unrelated counter and
    // register a CAS that never fires.
    let mut groups: Vec<(usize, Vec<usize>)> = Vec::new();
    for (i, key) in args.iter().enumerate() {
        let shard = frogdb_core::shard_for_key(key, num_shards);
        match groups.iter_mut().find(|(s, _)| *s == shard) {
            Some((_, indices)) => indices.push(i),
            None => groups.push((shard, vec![i])),
        }
    }
    groups.sort_by_key(|(shard, _)| *shard);

    // Get the current version from each shard. Pass the watched keys so the
    // shard lazily purges any that are ALREADY expired (aligning physical to
    // logical state) before snapshotting the version — without bumping it. This
    // records an already-stale key as a "nonexistent" watch, so a later EXEC
    // does not treat its (already-due) removal as a modification, while a key
    // still live here that expires during the window is caught at EXEC (F3).
    //
    // Every probe completes before anything is recorded: a shard that fails
    // mid-fan-out must leave no half-built watch set behind, which would be a
    // CAS the client believes covers keys it never got a version for.
    let mut probed: Vec<(usize, usize, u64, bool)> = Vec::new();
    for (shard, indices) in groups {
        let keys: Vec<Bytes> = indices.iter().map(|&i| args[i].clone()).collect();
        let (response_tx, response_rx) = oneshot::channel();
        if shard_senders[shard]
            .send(CoreMsg::GetVersion { keys, response_tx })
            .await
            .is_err()
        {
            return Response::error("ERR shard unavailable");
        }
        let (versions, live_flags) = match response_rx.await {
            Ok(reply) => reply,
            Err(_) => return Response::error("ERR shard dropped request"),
        };

        // Both reply vectors align with the keys this shard was sent (one entry
        // per watched key, in order). `versions[i]` is key `i`'s per-slot WATCH
        // version (proposal 18 — slot-granular, so distinct-slot keys get
        // distinct versions rather than one shared shard version);
        // `live_flags[i]` reports whether the key was live (present and
        // unexpired) at watch time — the `wk->expired` inverse EXEC needs to
        // distinguish a live-then-expired watch (must abort) from an
        // already-stale one (must not). Enforce the length invariant: a shard
        // reply whose vectors do not match the watched-key count is a protocol
        // bug, not a watch we can safely record.
        if versions.len() != indices.len() || live_flags.len() != indices.len() {
            return Response::error("ERR shard returned malformed WATCH version reply");
        }
        for ((&i, version), live_at_watch) in indices.iter().zip(versions).zip(live_flags) {
            probed.push((i, shard, version, live_at_watch));
        }
    }
    // Record in argument order, so first-watch-wins below sees the keys in the
    // order the client named them rather than in shard order.
    probed.sort_by_key(|&(i, ..)| i);
    // The shard is *not* folded into the transaction target here: WATCH always
    // precedes MULTI (WATCH inside MULTI errors), and MULTI resets the
    // accumulator, so any fold recorded now would be discarded. The watch set's
    // shards are folded at EXEC time in `take_transaction`, from the live
    // (post-UNWATCH) watch set.
    // A key already in the watch set keeps its earlier snapshot: `watch_key` is
    // first-watch-wins, matching Redis' `watchForKey`, which no-ops on an
    // already-watched key. The version probe above is still taken for the
    // whole argument list — it is one round-trip per shard the batch touches
    // and it is what lazily purges already-expired keys — only the recording is
    // guarded.
    for (i, shard, version, live_at_watch) in probed {
        state.watch_key(args[i].clone(), shard, version, live_at_watch);
    }

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
    /// dispatch through their `CommandImpl::Connection` executor over the mutable
    /// view built from their declared capability
    /// ([`ConnMutation::Auth`](frogdb_core::ConnMutation::Auth) →
    /// `conn_state = Some`), via
    /// [`conn_ctx_for`](Self::conn_ctx_for). EXEC's orchestration cannot be
    /// expressed against the narrow `ConnCtx` (it re-enters the dispatch
    /// machinery for the deferred connection-level commands — the
    /// meta-circularity), so it is dispatched directly to
    /// [`handle_exec`](Self::handle_exec).
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
                // WATCH is the one keyed command in this group, and reaching
                // this arm is precisely how it escapes the `ClusterSlotValidation`
                // stage. Its own slot verdict is taken here instead — but only
                // after the two rejections that outrank it (`WATCH` inside
                // `MULTI`, and the arity error) have had their chance, which is
                // what the guards below defer to.
                if cmd_name == "WATCH"
                    && !self.state.in_transaction()
                    && !args.is_empty()
                    && let Some(refusal) = self.pre_dispatch_view().validate_watch_slots(args)
                {
                    return Some(vec![refusal]);
                }
                // `as_connection()` yields a `'static` reference, so it does not
                // conflict with re-borrowing `self` to build the mutable ConnCtx.
                let command = self.core.registry.get_entry(cmd_name)?.as_connection()?;
                let mutation = command.spec().mutation;
                Some(vec![
                    command
                        .execute(&mut self.conn_ctx_for(mutation), args)
                        .await,
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
            ConnCtx::new(
                &self.config_manager,
                &self.client_registry,
                &self.latency_histograms,
                &self.keyspace_stats,
                &[],
                &self.snapshot_coordinator,
                &self.hotkey_session,
                &self.cluster,
                &self.cursor_store,
                &self.metrics_recorder,
                &self.memory_diag,
                self.acl_manager.as_ref(),
                &self.command_registry,
                0,
                10000,
                false,
            )
            .with_conn_state(&mut self.state)
        }
    }

    // FM-TXN-001
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

    // FM-TXN-003
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

    // FM-TXN-011
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

    // FM-TXN-012
    #[tokio::test]
    async fn watch_without_keys_errors() {
        let mut fx = Fixture::new();
        let resp = WatchConnCommand.execute(&mut fx.ctx_mut(), &[]).await;
        assert!(matches!(resp, Response::Error(_)), "WATCH needs >= 1 key");
    }

    // FM-TXN-013
    #[tokio::test]
    async fn unwatch_is_ok_and_clears_watches() {
        let mut fx = Fixture::new();
        fx.state.watch_key(Bytes::from_static(b"k"), 0, 7, true);
        let resp = UnwatchConnCommand.execute(&mut fx.ctx_mut(), &[]).await;
        assert_eq!(resp, Response::ok());
        assert_eq!(
            fx.state.watched_key_iter().count(),
            0,
            "UNWATCH clears all watched keys"
        );
    }

    // FM-TXN-047
    /// Regression guard for the *loud* EXEC spec-carrier (proposal 40 item 7,
    /// as it survives the command-spec-single-source refactor).
    ///
    /// EXEC is dispatched to
    /// [`ConnectionHandler::handle_exec`](crate::connection::ConnectionHandler);
    /// [`ExecConnCommand::execute`] is only a spec carrier and is **never**
    /// reached through the `ConnCtx` seam. If a routing/dispatch bug ever does
    /// reach it, it must surface *loudly* — a `debug_assert!` panic in debug
    /// builds, an internal error response in release builds — and must **never**
    /// fabricate a `+OK`/success that would silently lie to the client (the
    /// pre-proposal-40 behavior). This asserts the non-fabrication in whichever
    /// build profile the test runs under.
    ///
    /// Note: the sibling connection-level commands (MULTI/DISCARD/WATCH/UNWATCH)
    /// are *not* covered here because, on this branch, they carry real
    /// `ConnCtx` logic and legitimately return OK/errors — only EXEC is a
    /// never-reached spec carrier, so it is the sole fabrication surface.
    #[test]
    fn exec_spec_carrier_execute_never_fabricates_success() {
        // Build the runtime and fixture *outside* `catch_unwind` so only the
        // `execute` call's panic (if any) is captured, not fixture setup.
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("build current-thread runtime");
        let mut fx = Fixture::new();

        // Silence the panic hook only around the call so the expected
        // `debug_assert!` backtrace does not clutter test output; restore it
        // immediately afterwards.
        let prev_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            rt.block_on(async {
                let mut ctx = fx.ctx_mut();
                EXEC_CONN_COMMAND.execute(&mut ctx, &[]).await
            })
        }));
        std::panic::set_hook(prev_hook);

        match outcome {
            // Debug builds: the `debug_assert!(false, ...)` fired — loud and
            // correct. A regression to a fabricated success (which drops the
            // `debug_assert!`) would land on the `Ok` arm instead and fail.
            Err(_panic) => {}
            // Release builds: must be the internal error, never a fabricated
            // success.
            Ok(resp) => {
                assert_ne!(
                    resp,
                    Response::ok(),
                    "EXEC spec carrier must never fabricate a +OK success"
                );
                assert_eq!(
                    resp,
                    Response::error("ERR internal: EXEC must be dispatched via handle_exec"),
                    "EXEC spec carrier must surface a loud internal error; got {resp:?}"
                );
            }
        }
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
