//! Connection-state commands: RESET, ASKING, READONLY, READWRITE.
//!
//! These commands **mutate** per-connection state and are migrated behind the
//! [`ConnectionCommand`] seam like AUTH/HELLO: they dispatch through a `ConnCtx`
//! whose [`ConnCtx::conn_state`] is `Some(&mut dyn ConnStateMut)` (built by
//! [`ConnectionHandler::conn_ctx_for`](crate::connection::ConnectionHandler)),
//! and drive their mutations through [`frogdb_core::ConnStateMut`].
//!
//! * **RESET** performs a full reset of the connection's server-side context —
//!   exit pub/sub, discard MULTI, disable tracking, reset the protocol to RESP2,
//!   reset the reply mode to ON, clear the client name, and revert authentication
//!   to the default user (re-evaluating the auth flag against the ACL manager, so
//!   with a password-protected default user RESET drops back to unauthenticated).
//!   The pure state reset funnels through [`ConnStateMut::reset`], the auth revert
//!   through [`ConnStateMut::revert_to_default_user`]; the shard-side
//!   `ConnectionClosed` fan-out and the client-registry name clear are done here
//!   through the `ConnCtx`. The two remaining teardown steps that the seam cannot
//!   reach (drop the MONITOR subscription, tear down the tracking session's local
//!   plumbing) are done by the handler wrapper
//!   [`ConnectionHandler::execute_reset`](crate::connection::ConnectionHandler),
//!   which invokes this executor and then performs that handler-local teardown.
//! * **ASKING / READONLY / READWRITE** set the cluster-redirect flags on the
//!   connection. Matching Redis's `askingCommand`/`readonlyCommand`/
//!   `readwriteCommand`, they reply `-ERR This instance has cluster support
//!   disabled` when cluster support is off ([`ConnCtx::cluster_enabled`] is
//!   `false`). In cluster mode, when dispatched with `conn_state: None` (the
//!   read-only registry-union view used for connection-level commands deferred
//!   from a MULTI transaction) they are a no-op returning `+OK`, matching the
//!   historical behavior where these flags did not take effect inside a
//!   transaction.

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, BoxFuture, CommandFlags, CommandSpec, ConnCtx, ConnectionCommand,
    ConnectionLevelOp, EventSpec, ExecutionStrategy, KeySpec, LookupSpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

use crate::connection::ShardMessage;

/// Build a `CommandSpec` for a connection-state command. All four share the same
/// shape (no keys, no WAL, `ConnectionLevel(ConnectionState)` strategy) and
/// differ only in name, arity, and flags.
const fn connection_state_spec(
    name: &'static str,
    arity: Arity,
    flags: CommandFlags,
) -> CommandSpec {
    CommandSpec {
        name,
        arity,
        flags,
        keys: KeySpec::None,
        access: AccessSpec::Uniform,
        wal: WalStrategy::NoOp,
        wakes: WaiterWake::None,
        event: EventSpec::NotApplicable,
        requires_same_slot: false,
        lookup: LookupSpec::None,
        mutation: frogdb_core::ConnMutation::Auth,
        strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::ConnectionState),
    }
}

// ---------------------------------------------------------------------------
// RESET
// ---------------------------------------------------------------------------

/// The `CommandSpec` for RESET (flags preserved from the former `ResetMetadata`).
static RESET_SPEC: CommandSpec = connection_state_spec(
    "RESET",
    Arity::Fixed(0),
    CommandFlags::FAST
        .union(CommandFlags::LOADING)
        .union(CommandFlags::STALE)
        .union(CommandFlags::NOSCRIPT),
);

/// The registrable, `'static` RESET executor.
pub(crate) static RESET_CONN_COMMAND: ResetConnCommand = ResetConnCommand;

/// RESET — full reset of the connection's server-side context.
pub(crate) struct ResetConnCommand;

impl ConnectionCommand for ResetConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &RESET_SPEC
    }

    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        _args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        Box::pin(async move { handle_reset(ctx).await })
    }
}

/// Reset the connection state and drive the reachable I/O half. The two
/// handler-local teardown steps (MONITOR, tracking plumbing) are performed by
/// [`ConnectionHandler::execute_reset`](crate::connection::ConnectionHandler)
/// after this returns.
async fn handle_reset(ctx: &mut ConnCtx<'_>) -> Response {
    // Copy the shared subsystem references out before taking the mutable
    // `conn_state` borrow so the disjoint uses do not overlap (mirrors AUTH).
    let shard_senders = ctx.shard_senders;
    let client_registry = ctx.client_registry;

    // Re-evaluate authentication against the default user (Redis `resetCommand`
    // sets `c->user = DefaultUser` and marks the connection authenticated only
    // when the default user is `nopass` and enabled). With a password-protected
    // default user (requirepass), RESET drops the connection to unauthenticated.
    let default_authenticated = ctx
        .acl_manager
        .get_user("default")
        .map(|user| user.nopass && user.enabled)
        .unwrap_or(false);

    let state = ctx
        .conn_state
        .as_deref_mut()
        .expect("RESET is dispatched with a mutable conn_state");
    let conn_id = state.id();

    // State half: exit pub/sub, clear tracking + transaction, reset protocol to
    // RESP2, reset reply mode to ON, clear the client name. Returns what was
    // active for the I/O half.
    let outcome = state.reset();

    // Revert to the default user and re-apply the recomputed auth flag.
    state.revert_to_default_user(default_authenticated);

    // SELECT 0 — single-DB: no-op (FrogDB has no per-connection DB index).

    // Notify shards to remove subscriptions and/or tracking state. The original
    // handle_reset sent ConnectionClosed once if either was active.
    if outcome.was_in_pubsub || outcome.tracking_was_enabled {
        for sender in shard_senders.iter() {
            let _ = sender
                .send(ShardMessage::ConnectionClosed { conn_id })
                .await;
        }
    }

    // Clear the client name in the registry (local state already cleared by
    // `reset()`). lib_name/lib_ver are intentionally NOT cleared (Redis
    // semantics: client libraries retain their identity across RESET).
    client_registry.update_name(conn_id, None);

    Response::Simple(Bytes::from_static(b"RESET"))
}

// ---------------------------------------------------------------------------
// ASKING / READONLY / READWRITE
// ---------------------------------------------------------------------------

/// The `CommandSpec` for ASKING (flags preserved from the former `AskingCommand`).
static ASKING_SPEC: CommandSpec = connection_state_spec(
    "ASKING",
    Arity::Fixed(0),
    CommandFlags::FAST.union(CommandFlags::STALE),
);

/// The registrable, `'static` ASKING executor.
pub(crate) static ASKING_CONN_COMMAND: AskingConnCommand = AskingConnCommand;

/// ASKING — set the one-shot cluster ASKING flag on the connection.
pub(crate) struct AskingConnCommand;

impl ConnectionCommand for AskingConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &ASKING_SPEC
    }

    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        _args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        Box::pin(async move {
            // Redis `askingCommand` errors when cluster support is disabled.
            if !ctx.cluster_enabled {
                return Response::error("ERR This instance has cluster support disabled");
            }
            if let Some(state) = ctx.conn_state.as_deref_mut() {
                state.set_asking();
            }
            Response::ok()
        })
    }
}

/// The `CommandSpec` for READONLY (flags preserved from the former `ReadonlyCommand`).
static READONLY_SPEC: CommandSpec = connection_state_spec(
    "READONLY",
    Arity::Fixed(0),
    CommandFlags::FAST
        .union(CommandFlags::LOADING)
        .union(CommandFlags::STALE),
);

/// The registrable, `'static` READONLY executor.
pub(crate) static READONLY_CONN_COMMAND: ReadonlyConnCommand = ReadonlyConnCommand;

/// READONLY — set the connection's READONLY replica-read flag.
pub(crate) struct ReadonlyConnCommand;

impl ConnectionCommand for ReadonlyConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &READONLY_SPEC
    }

    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        _args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        Box::pin(async move {
            // Redis `readonlyCommand` errors when cluster support is disabled.
            if !ctx.cluster_enabled {
                return Response::error("ERR This instance has cluster support disabled");
            }
            if let Some(state) = ctx.conn_state.as_deref_mut() {
                state.set_readonly(true);
            }
            Response::ok()
        })
    }
}

/// The `CommandSpec` for READWRITE (flags preserved from the former `ReadwriteCommand`).
static READWRITE_SPEC: CommandSpec = connection_state_spec(
    "READWRITE",
    Arity::Fixed(0),
    CommandFlags::FAST
        .union(CommandFlags::LOADING)
        .union(CommandFlags::STALE),
);

/// The registrable, `'static` READWRITE executor.
pub(crate) static READWRITE_CONN_COMMAND: ReadwriteConnCommand = ReadwriteConnCommand;

/// READWRITE — clear the connection's READONLY replica-read flag.
pub(crate) struct ReadwriteConnCommand;

impl ConnectionCommand for ReadwriteConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &READWRITE_SPEC
    }

    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        _args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        Box::pin(async move {
            // Redis `readwriteCommand` errors when cluster support is disabled.
            if !ctx.cluster_enabled {
                return Response::error("ERR This instance has cluster support disabled");
            }
            if let Some(state) = ctx.conn_state.as_deref_mut() {
                state.set_readonly(false);
            }
            Response::ok()
        })
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

    /// Build a mutable-`ConnCtx` over fixture dependencies (mirrors the AUTH/HELLO
    /// fixture): the connection-state commands exercise `shard_senders`,
    /// `client_registry`, and the mutable `conn_state` (a real
    /// [`ConnectionState`]); the rest are unused placeholders.
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
        /// Value wired into `ConnCtx::cluster_enabled`; toggled by tests that
        /// exercise READONLY/READWRITE/ASKING's cluster gating.
        cluster_enabled: bool,
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
                cluster_enabled: false,
            }
        }

        /// A mutable `ConnCtx` carrying `conn_state: Some(..)` (the live-dispatch
        /// shape).
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
                self.cluster_enabled,
            )
            .with_conn_state(&mut self.state)
        }
    }

    #[tokio::test]
    async fn reset_returns_reset_and_switches_to_resp2() {
        let mut fx = Fixture::new();
        fx.state.protocol_version = ProtocolVersion::Resp3;
        fx.state.name = Some(Bytes::from_static(b"worker-1"));

        let resp = ResetConnCommand.execute(&mut fx.ctx_mut(), &[]).await;
        assert_eq!(resp, Response::Simple(Bytes::from_static(b"RESET")));
        assert!(
            !fx.state.protocol_version.is_resp3(),
            "RESET reverts to RESP2"
        );
        assert!(fx.state.name.is_none(), "RESET clears the client name");
    }

    #[tokio::test]
    async fn reset_resets_reply_mode_to_on() {
        let mut fx = Fixture::new();
        // CLIENT REPLY OFF was active before RESET.
        fx.state.reply_off();

        ResetConnCommand.execute(&mut fx.ctx_mut(), &[]).await;

        // Reply mode is back ON: the next reply is delivered.
        assert!(matches!(
            fx.state.consume_reply_disposition(),
            crate::connection::state::ReplyDisposition::Send
        ));
    }

    #[tokio::test]
    async fn reset_deauthenticates_when_default_user_requires_password() {
        // Default user protected by requirepass → nopass=false.
        let mut fx = Fixture::new();
        fx.acl_manager
            .set_requirepass("s3cr3t")
            .expect("set requirepass");
        // Start authenticated (as if AUTH had succeeded).
        fx.state
            .authenticate(frogdb_core::AuthenticatedUser::default_user());
        assert!(fx.state.is_authenticated());

        ResetConnCommand.execute(&mut fx.ctx_mut(), &[]).await;

        assert!(
            !fx.state.is_authenticated(),
            "RESET drops to unauthenticated when the default user requires a password"
        );
    }

    #[tokio::test]
    async fn reset_stays_authenticated_when_default_user_is_nopass() {
        // Default user is nopass (no auth configured) → RESET keeps the
        // connection authenticated as the default user.
        let mut fx = Fixture::new();
        assert!(fx.state.is_authenticated());

        ResetConnCommand.execute(&mut fx.ctx_mut(), &[]).await;

        assert!(
            fx.state.is_authenticated(),
            "RESET stays authenticated as the default user when no auth is configured"
        );
    }

    #[tokio::test]
    async fn asking_errors_when_cluster_disabled() {
        let mut fx = Fixture::new(); // cluster_enabled = false
        let resp = AskingConnCommand.execute(&mut fx.ctx_mut(), &[]).await;
        assert_eq!(
            resp,
            Response::error("ERR This instance has cluster support disabled")
        );
        assert!(!fx.state.take_asking(), "ASKING did not set the flag");
    }

    #[tokio::test]
    async fn asking_sets_one_shot_flag_in_cluster_mode() {
        let mut fx = Fixture::new();
        fx.cluster_enabled = true;
        let resp = AskingConnCommand.execute(&mut fx.ctx_mut(), &[]).await;
        assert_eq!(resp, Response::ok());
        assert!(fx.state.take_asking(), "ASKING sets the one-shot flag");
    }

    #[tokio::test]
    async fn readonly_readwrite_error_when_cluster_disabled() {
        let mut fx = Fixture::new(); // cluster_enabled = false

        let resp = ReadonlyConnCommand.execute(&mut fx.ctx_mut(), &[]).await;
        assert_eq!(
            resp,
            Response::error("ERR This instance has cluster support disabled")
        );
        assert!(!fx.state.is_readonly(), "READONLY did not set the flag");

        let resp = ReadwriteConnCommand.execute(&mut fx.ctx_mut(), &[]).await;
        assert_eq!(
            resp,
            Response::error("ERR This instance has cluster support disabled")
        );
    }

    #[tokio::test]
    async fn readonly_then_readwrite_toggles_flag_in_cluster_mode() {
        let mut fx = Fixture::new();
        fx.cluster_enabled = true;

        let resp = ReadonlyConnCommand.execute(&mut fx.ctx_mut(), &[]).await;
        assert_eq!(resp, Response::ok());
        assert!(fx.state.is_readonly(), "READONLY sets the flag");

        let resp = ReadwriteConnCommand.execute(&mut fx.ctx_mut(), &[]).await;
        assert_eq!(resp, Response::ok());
        assert!(!fx.state.is_readonly(), "READWRITE clears the flag");
    }

    #[test]
    fn specs_are_connection_state_and_valid() {
        for spec in [
            RESET_CONN_COMMAND.spec(),
            ASKING_CONN_COMMAND.spec(),
            READONLY_CONN_COMMAND.spec(),
            READWRITE_CONN_COMMAND.spec(),
        ] {
            assert!(spec.validate().is_ok(), "{} spec invalid", spec.name);
            assert!(matches!(
                spec.strategy,
                ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::ConnectionState)
            ));
        }
    }
}
