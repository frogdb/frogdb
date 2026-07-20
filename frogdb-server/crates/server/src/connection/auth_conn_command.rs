//! AUTH and HELLO connection commands.
//!
//! These are the first connection commands migrated behind the
//! [`ConnectionCommand`] seam that **mutate** connection state — AUTH sets the
//! authenticated user; HELLO negotiates the RESP protocol version, may carry an
//! inline AUTH clause, and may set the client name. They therefore exercise the
//! mutable-`ConnCtx` capability: they are dispatched through a `ConnCtx` whose
//! [`ConnCtx::conn_state`] is `Some(&mut dyn ConnStateMut)` and read/write
//! per-connection state through it (see [`frogdb_core::ConnStateMut`] and
//! [`ConnectionHandler::conn_ctx_for`](crate::connection::ConnectionHandler)).
//!
//! # Pre-authentication
//!
//! AUTH and HELLO run *before* authentication is enforced: a not-yet-authenticated
//! client must be able to run them. Their dispatch is intercepted early in
//! [`route_and_execute_with_transaction`](crate::connection::ConnectionHandler)
//! — before the NOAUTH pre-check and before transaction queuing — so migrating
//! them to the registry union does not change that ordering. Their spec keeps the
//! [`ConnectionLevelOp::Auth`] strategy, which `is_auth_exempt` recognizes.

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, BoxFuture, ClientRegistry, CommandFlags, CommandSpec, ConnCtx, ConnStateMut,
    ConnectionCommand, ConnectionLevelOp, EventSpec, ExecutionStrategy, KeySpec, LookupSpec,
    TrackingInfoView, TrackingModeView, WaiterWake, WalStrategy,
};
use frogdb_protocol::{MapReply, ProtocolVersion, Response};
use tracing::{info, warn};

use crate::connection::state::{ConnectionState, TrackingEnableRequest, TrackingMode};

/// Bridge the server's [`ConnectionState`] to the core [`ConnStateMut`] seam so
/// AUTH/HELLO can mutate auth/protocol state without the seam naming any server
/// type. All mutation still funnels through `ConnectionState`'s own methods.
impl ConnStateMut for ConnectionState {
    fn id(&self) -> u64 {
        self.id
    }

    fn protocol_version(&self) -> ProtocolVersion {
        self.protocol_version
    }

    fn client_info(&self) -> String {
        self.client_info_string()
    }

    fn authenticate(&mut self, user: frogdb_core::AuthenticatedUser) {
        // Disambiguate to the inherent method (the trait method has the same name).
        ConnectionState::authenticate(self, user);
    }

    fn set_protocol_version(&mut self, version: ProtocolVersion) {
        self.protocol_version = version;
    }

    fn set_name(&mut self, name: Option<Bytes>) {
        self.name = name;
    }

    fn mark_hello_received(&mut self) {
        self.hello_received = true;
        self.hello_at = Some(std::time::Instant::now());
    }

    fn reset(&mut self) -> frogdb_core::ResetOutcome {
        // Disambiguate to the inherent method (the trait method has the same
        // name) and funnel the whole state reset through it.
        let effects = ConnectionState::reset(self);
        frogdb_core::ResetOutcome {
            was_in_pubsub: effects.was_in_pubsub,
            tracking_was_enabled: effects.tracking_was_enabled,
        }
    }

    fn revert_to_default_user(&mut self, authenticated: bool) {
        ConnectionState::revert_to_default_user(self, authenticated);
    }

    fn set_asking(&mut self) {
        ConnectionState::set_asking(self);
    }

    fn set_readonly(&mut self, readonly: bool) {
        ConnectionState::set_readonly(self, readonly);
    }

    // ---- CLIENT (see `client_conn_command`) ----

    fn name(&self) -> Option<Bytes> {
        self.name.clone()
    }

    fn reply_on(&mut self) {
        ConnectionState::reply_on(self);
    }

    fn reply_off(&mut self) {
        ConnectionState::reply_off(self);
    }

    fn reply_skip_next(&mut self) {
        ConnectionState::reply_skip_next(self);
    }

    fn set_caching_override(&mut self, track: bool) {
        ConnectionState::set_caching_override(self, track);
    }

    fn tracking_info(&self) -> TrackingInfoView {
        let tracking = self.tracking();
        let mode = match tracking.mode {
            TrackingMode::Default => TrackingModeView::Default,
            TrackingMode::OptIn => TrackingModeView::OptIn,
            TrackingMode::OptOut => TrackingModeView::OptOut,
            TrackingMode::Broadcast => TrackingModeView::Broadcast,
        };
        TrackingInfoView {
            enabled: tracking.enabled,
            mode,
            noloop: tracking.noloop,
            caching_override: tracking.caching_override,
            redirect: tracking.redirect,
            prefixes: tracking.prefixes.clone(),
        }
    }

    fn enable_tracking(
        &mut self,
        bcast: bool,
        optin: bool,
        optout: bool,
        noloop: bool,
        prefixes: Vec<Bytes>,
        redirect: u64,
    ) -> Result<Vec<Bytes>, String> {
        let req = TrackingEnableRequest {
            bcast,
            optin,
            optout,
            noloop,
            prefixes,
            redirect,
        };
        ConnectionState::enable_tracking(self, req).map_err(|e| e.to_string())
    }

    fn disable_tracking(&mut self) -> bool {
        ConnectionState::disable_tracking(self)
    }

    fn sync_stats_to_registry(&mut self, registry: &ClientRegistry) {
        ConnectionState::sync_stats_to_registry(self, registry);
    }

    // ---- Transactions (see `transaction_conn_command`) ----

    fn begin_multi(&mut self) -> bool {
        ConnectionState::begin_transaction(self).is_ok()
    }

    fn is_in_multi(&self) -> bool {
        ConnectionState::in_transaction(self)
    }

    fn watch_key(&mut self, key: Bytes, shard_id: usize, version: u64) {
        ConnectionState::watch_key(self, key, shard_id, version);
    }

    fn unwatch(&mut self) {
        ConnectionState::unwatch_all(self);
    }

    fn discard(&mut self) -> Option<frogdb_core::TxnDiscardOutcome> {
        ConnectionState::discard_transaction(self).map(|m| frogdb_core::TxnDiscardOutcome {
            queued_count: m.queued_count,
            start_time: m.start_time,
        })
    }
}

/// The `CommandSpec` for AUTH. Strategy is `ConnectionLevel(Auth)`: it keeps
/// AUTH auth-exempt (runnable pre-authentication) and is validated as a
/// `Connection` executor by the registry.
static AUTH_SPEC: CommandSpec = CommandSpec {
    name: "AUTH",
    arity: Arity::Range { min: 1, max: 2 },
    flags: CommandFlags::FAST,
    keys: KeySpec::None,
    access: AccessSpec::Uniform,
    wal: WalStrategy::NoOp,
    wakes: WaiterWake::None,
    event: EventSpec::NotApplicable,
    requires_same_slot: false,
    lookup: LookupSpec::None,
    mutation: frogdb_core::ConnMutation::Auth,
    strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Auth),
};

/// The registrable, `'static` AUTH executor.
pub(crate) static AUTH_CONN_COMMAND: AuthConnCommand = AuthConnCommand;

/// AUTH — authenticate the connection (mutates auth state via `conn_state`).
pub(crate) struct AuthConnCommand;

impl ConnectionCommand for AuthConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &AUTH_SPEC
    }

    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        Box::pin(async move { handle_auth(ctx, args) })
    }
}

/// Handle AUTH: `AUTH <password>` (default user) or `AUTH <username> <password>`.
fn handle_auth(ctx: &mut ConnCtx<'_>, args: &[Bytes]) -> Response {
    if args.is_empty() {
        return Response::error("ERR wrong number of arguments for 'auth' command");
    }

    // `acl_manager` is a shared field; copy the reference out before taking the
    // mutable `conn_state` borrow so the two disjoint uses do not overlap.
    let acl = ctx.acl_manager;
    let state = ctx
        .conn_state
        .as_deref_mut()
        .expect("AUTH is dispatched with a mutable conn_state");
    let client_info = state.client_info();
    let conn_id = state.id();

    let result = if args.len() == 1 {
        // AUTH <password> — authenticate as the default user.
        let password = String::from_utf8_lossy(&args[0]);
        acl.authenticate_default(&password, &client_info)
    } else {
        // AUTH <username> <password>.
        let username = String::from_utf8_lossy(&args[0]);
        let password = String::from_utf8_lossy(&args[1]);
        acl.authenticate(&username, &password, &client_info)
    };

    match result {
        Ok(user) => {
            info!(conn_id, username = %user.username, "Client authenticated");
            state.authenticate(user);
            Response::ok()
        }
        Err(e) => {
            let username = if args.len() > 1 {
                String::from_utf8_lossy(&args[0]).to_string()
            } else {
                "default".to_string()
            };
            warn!(
                conn_id,
                client = %client_info,
                username = %username,
                reason = %e,
                "Authentication failed"
            );
            Response::error(e.to_string())
        }
    }
}

/// The `CommandSpec` for HELLO. Strategy is `ConnectionLevel(Auth)` (auth-exempt,
/// carries an optional inline AUTH clause).
static HELLO_SPEC: CommandSpec = CommandSpec {
    name: "HELLO",
    arity: Arity::AtLeast(0),
    flags: CommandFlags::FAST
        .union(CommandFlags::NOSCRIPT)
        .union(CommandFlags::LOADING)
        .union(CommandFlags::STALE),
    keys: KeySpec::None,
    access: AccessSpec::Uniform,
    wal: WalStrategy::NoOp,
    wakes: WaiterWake::None,
    event: EventSpec::NotApplicable,
    requires_same_slot: false,
    lookup: LookupSpec::None,
    mutation: frogdb_core::ConnMutation::Auth,
    strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Auth),
};

/// The registrable, `'static` HELLO executor.
pub(crate) static HELLO_CONN_COMMAND: HelloConnCommand = HelloConnCommand;

/// HELLO — protocol negotiation with an optional AUTH/SETNAME clause.
pub(crate) struct HelloConnCommand;

impl ConnectionCommand for HelloConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &HELLO_SPEC
    }

    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        Box::pin(async move { handle_hello(ctx, args) })
    }
}

/// Handle HELLO: `HELLO [protover [AUTH username password] [SETNAME clientname]]`.
fn handle_hello(ctx: &mut ConnCtx<'_>, args: &[Bytes]) -> Response {
    let mut requested_version: Option<u32> = None;
    let mut auth_username: Option<&Bytes> = None;
    let mut auth_password: Option<&Bytes> = None;
    let mut setname: Option<&Bytes> = None;

    // Parse arguments.
    let mut i = 0;
    while i < args.len() {
        if i == 0 {
            // First argument is the protocol version.
            match std::str::from_utf8(&args[i])
                .ok()
                .and_then(|s| s.parse::<u32>().ok())
            {
                Some(v) => requested_version = Some(v),
                None => {
                    return Response::error(
                        "ERR Protocol version is not an integer or out of range",
                    );
                }
            }
        } else {
            // Check for AUTH or SETNAME.
            let arg = args[i].to_ascii_uppercase();
            if arg == b"AUTH".as_slice() {
                if i + 2 >= args.len() {
                    return Response::error("ERR Syntax error in HELLO option 'AUTH'");
                }
                auth_username = Some(&args[i + 1]);
                auth_password = Some(&args[i + 2]);
                i += 2;
            } else if arg == b"SETNAME".as_slice() {
                if i + 1 >= args.len() {
                    return Response::error("ERR Syntax error in HELLO option 'SETNAME'");
                }
                setname = Some(&args[i + 1]);
                i += 1;
            } else {
                return Response::error(format!(
                    "ERR Syntax error in HELLO option '{}'",
                    String::from_utf8_lossy(&args[i])
                ));
            }
        }
        i += 1;
    }

    // Handle protocol version. Validation happens before any mutation, so a
    // NOPROTO error leaves the connection's protocol untouched.
    if let Some(version) = requested_version {
        if !(2..=3).contains(&version) {
            return Response::error("NOPROTO sorry, this protocol version is not supported");
        }
        let new_version = if version == 3 {
            ProtocolVersion::Resp3
        } else {
            ProtocolVersion::Resp2
        };
        ctx.conn_state
            .as_deref_mut()
            .expect("HELLO is dispatched with a mutable conn_state")
            .set_protocol_version(new_version);
    }

    // Handle the inline AUTH clause.
    if let (Some(username), Some(password)) = (auth_username, auth_password) {
        let acl = ctx.acl_manager;
        let state = ctx
            .conn_state
            .as_deref_mut()
            .expect("HELLO is dispatched with a mutable conn_state");
        let client_info = state.client_info();
        let conn_id = state.id();
        let username_str = String::from_utf8_lossy(username);
        let password_str = String::from_utf8_lossy(password);

        match acl.authenticate(&username_str, &password_str, &client_info) {
            Ok(user) => {
                info!(conn_id, username = %user.username, "Client authenticated");
                state.authenticate(user);
            }
            Err(_) => {
                warn!(
                    conn_id,
                    client = %client_info,
                    username = %username_str,
                    reason = "invalid username-password pair or user is disabled",
                    "Authentication failed"
                );
                return Response::error(
                    "WRONGPASS invalid username-password pair or user is disabled",
                );
            }
        }
    }

    // Handle SETNAME. The name is mirrored into the client registry (as on the
    // legacy path), keyed by the connection id.
    if let Some(name) = setname {
        let client_registry = ctx.client_registry;
        let state = ctx
            .conn_state
            .as_deref_mut()
            .expect("HELLO is dispatched with a mutable conn_state");
        let conn_id = state.id();
        if name.is_empty() {
            state.set_name(None);
            client_registry.update_name(conn_id, None);
        } else {
            state.set_name(Some(name.clone()));
            client_registry.update_name(conn_id, Some(name.clone()));
        }
    }

    // Mark HELLO as received, then render the reply from the *post*-negotiation
    // state (the reply's `proto`/`id` reflect any version switch above).
    let state = ctx
        .conn_state
        .as_deref_mut()
        .expect("HELLO is dispatched with a mutable conn_state");
    state.mark_hello_received();
    build_hello_response(&*state)
}

/// Build the HELLO reply. The RESP3 Map vs RESP2 flat-Array shape is owned by
/// [`MapReply`], which picks the shape from the current protocol version.
fn build_hello_response(state: &dyn ConnStateMut) -> Response {
    let version = state.protocol_version();
    let proto = if version.is_resp3() { 3 } else { 2 };

    let mut reply = MapReply::with_capacity(version, 7);
    reply.field(b"server", Response::bulk(Bytes::from_static(b"frogdb")));
    reply.field(
        b"version",
        Response::bulk(Bytes::from(env!("CARGO_PKG_VERSION"))),
    );
    reply.field(b"proto", Response::Integer(proto));
    reply.field(b"id", Response::Integer(state.id() as i64));
    reply.field(b"mode", Response::bulk(Bytes::from_static(b"standalone")));
    reply.field(b"role", Response::bulk(Bytes::from_static(b"master")));
    reply.field(b"modules", Response::Array(vec![]));
    reply.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::ClusterDeps;
    use crate::cursor_store::AggregateCursorStore;
    use frogdb_core::persistence::NoopSnapshotCoordinator;
    use frogdb_core::{
        AclManager, ClientRegistry, CommandLatencyHistograms, CommandRegistry, KeyspaceStats,
        SharedHotkeySession, new_shared_hotkey_session,
    };

    /// Build a mutable-`ConnCtx` over fixture dependencies — no socket, no
    /// `ConnectionHandler`. AUTH/HELLO exercise `acl_manager`, `client_registry`,
    /// and the mutable `conn_state` (a real [`ConnectionState`]); the rest are
    /// unused placeholders. `ctx` takes `&mut self` because the `conn_state`
    /// handle borrows the fixture's state mutably.
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
                // `requires_auth = false` → starts as the default authenticated
                // user, matching a standalone server with no requirepass.
                state: ConnectionState::new(1, "127.0.0.1:0".parse().unwrap(), false),
            }
        }

        fn ctx(&mut self) -> ConnCtx<'_> {
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

    fn arg(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[tokio::test]
    async fn auth_empty_args_errors() {
        let mut fx = Fixture::new();
        let resp = AuthConnCommand.execute(&mut fx.ctx(), &[]).await;
        assert!(matches!(resp, Response::Error(_)));
    }

    #[tokio::test]
    async fn auth_named_user_success_and_wrong_password() {
        let mut fx = Fixture::new();
        fx.acl_manager
            .set_user("alice", &["on", ">s3cret", "~*", "+@all"])
            .unwrap();

        // Correct password authenticates and flips the connection identity.
        let resp = AuthConnCommand
            .execute(&mut fx.ctx(), &[arg("alice"), arg("s3cret")])
            .await;
        assert_eq!(resp, Response::ok());
        assert_eq!(fx.state.username(), "alice");

        // Wrong password is a WRONGPASS error and leaves identity unchanged.
        let resp = AuthConnCommand
            .execute(&mut fx.ctx(), &[arg("alice"), arg("nope")])
            .await;
        match resp {
            Response::Error(e) => {
                assert!(String::from_utf8_lossy(&e).starts_with("WRONGPASS"))
            }
            other => panic!("expected WRONGPASS error, got {other:?}"),
        }
        assert_eq!(fx.state.username(), "alice");
    }

    #[tokio::test]
    async fn hello_no_args_returns_resp2_array() {
        let mut fx = Fixture::new();
        let resp = HelloConnCommand.execute(&mut fx.ctx(), &[]).await;
        assert!(
            matches!(resp, Response::Array(_)),
            "RESP2 HELLO is an array"
        );
        assert!(fx.state.hello_received);
    }

    #[tokio::test]
    async fn hello_upgrade_to_resp3_returns_map() {
        let mut fx = Fixture::new();
        let resp = HelloConnCommand.execute(&mut fx.ctx(), &[arg("3")]).await;
        assert!(matches!(resp, Response::Map(_)), "RESP3 HELLO is a map");
        assert!(fx.state.protocol_version.is_resp3());
    }

    #[tokio::test]
    async fn hello_unsupported_protover_is_noproto() {
        let mut fx = Fixture::new();
        let resp = HelloConnCommand.execute(&mut fx.ctx(), &[arg("4")]).await;
        match resp {
            Response::Error(e) => assert!(String::from_utf8_lossy(&e).starts_with("NOPROTO")),
            other => panic!("expected NOPROTO error, got {other:?}"),
        }
        // A rejected protover leaves the protocol untouched.
        assert!(!fx.state.protocol_version.is_resp3());
    }

    #[tokio::test]
    async fn hello_setname_sets_client_name() {
        let mut fx = Fixture::new();
        let _ = HelloConnCommand
            .execute(&mut fx.ctx(), &[arg("2"), arg("SETNAME"), arg("worker-1")])
            .await;
        assert_eq!(fx.state.name.as_deref(), Some(&b"worker-1"[..]));
    }

    #[tokio::test]
    async fn hello_auth_clause_authenticates() {
        let mut fx = Fixture::new();
        fx.acl_manager
            .set_user("bob", &["on", ">pw", "~*", "+@all"])
            .unwrap();
        let resp = HelloConnCommand
            .execute(
                &mut fx.ctx(),
                &[arg("3"), arg("AUTH"), arg("bob"), arg("pw")],
            )
            .await;
        assert!(matches!(resp, Response::Map(_)));
        assert_eq!(fx.state.username(), "bob");
        assert!(fx.state.protocol_version.is_resp3());
    }

    #[tokio::test]
    async fn hello_auth_clause_wrong_password_is_wrongpass() {
        let mut fx = Fixture::new();
        fx.acl_manager
            .set_user("bob", &["on", ">pw", "~*", "+@all"])
            .unwrap();
        let resp = HelloConnCommand
            .execute(
                &mut fx.ctx(),
                &[arg("2"), arg("AUTH"), arg("bob"), arg("bad")],
            )
            .await;
        match resp {
            Response::Error(e) => assert!(String::from_utf8_lossy(&e).starts_with("WRONGPASS")),
            other => panic!("expected WRONGPASS error, got {other:?}"),
        }
    }

    #[test]
    fn auth_and_hello_specs_are_connection_level_auth() {
        for spec in [AUTH_CONN_COMMAND.spec(), HELLO_CONN_COMMAND.spec()] {
            assert!(spec.validate().is_ok());
            assert!(matches!(
                spec.strategy,
                ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Auth)
            ));
        }
    }
}
