//! The connection-command seam (server-side executors).
//!
//! The seam itself — the [`ConnectionCommand`] trait, its [`ConnCtx`] view, and
//! the [`ConfigProvider`] abstraction — is defined in `frogdb_core` so the
//! command registry can store a connection command as `CommandImpl::Connection`
//! (see [`frogdb_core::registry::CommandImpl`]). This module holds the
//! server-side executors that implement the seam and the glue that adapts
//! server state (the [`ConfigManager`], the [`ConnectionHandler`]) to it.
//!
//! CONFIG was the first command migrated behind this seam: it is registered as
//! `CommandImpl::Connection` and dispatched through the registry union. Every
//! *connection-level* command group has since followed, so the registry union
//! (see [`ConnectionHandler::dispatch_connection_command`]) is the sole dispatch
//! path for genuine connection commands, and the old `router.rs` name→op lookup
//! *table* has been deleted.
//!
//! The migration is **not** finished, and the two worlds still coexist: the
//! `handle_*` bodies under [`connection::handlers`](crate::connection::handlers)
//! are still live. They back (a) the shard-scatter *server-wide* / *scatter*
//! commands routed through the data-driven `ServerWideOp` / `ScatterGatherOp`
//! seams (`dispatch_server_wide` / `dispatch_scatter`), which is their proper
//! home, and (b) work a migrated connection command cannot reach through the
//! narrow `ConnCtx` — EXEC's transaction orchestration
//! (`transaction::execute_transaction`) and the scripting/search
//! helpers a `ScriptingProvider` / server-wide op delegates into. What is gone
//! is the name-keyed router that once *selected* a `handle_*` for every command;
//! what remains is those handler bodies, reached from the typed-op dispatch or
//! from an executor's provider impl, never from a name→handler table.

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, BoxFuture, CommandFlags, CommandSpec, ConfigProvider, ConnMutation,
    ConnectionLevelOp, CursorReadBatch, CursorStoreProvider, EventSpec, ExecutionStrategy, KeySpec,
    LookupSpec, ShardMessage, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

use crate::connection::ConnectionHandler;
use crate::connection::deps::{AdminDeps, ClusterDeps, CoreDeps, ObservabilityDeps};
use crate::connection::observability_conn_command::MemoryDiag;
use crate::cursor_store::AggregateCursorStore;
use crate::runtime_config::ConfigManager;
use crate::scatter::{DEFAULT_SCATTER_GATHER_TIMEOUT, ScatterGather};

// Re-export the core seam under this module path so existing call sites
// (`crate::connection::conn_command::{ConnCtx, ConnectionCommand}`) keep
// resolving after the trait moved to `frogdb_core`. `pub(crate) use` also brings
// both names into this module's scope for local use.
pub(crate) use frogdb_core::{ConnCtx, ConnectionCommand};

/// Adapts the server's [`ConfigManager`] to the core [`ConfigProvider`] seam so
/// the CONFIG executor can live behind a `ConnCtx` without the seam naming any
/// server type. Explicit `ConfigManager::` calls avoid resolving back into the
/// trait method (the inherent `get` shadows the trait `get`).
impl ConfigProvider for ConfigManager {
    fn get(&self, pattern: &str) -> Vec<(String, String)> {
        ConfigManager::get(self, pattern)
    }

    fn set<'a>(&'a self, name: &'a str, value: &'a str) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move { self.set_async(name, value).await.map_err(|e| e.to_string()) })
    }

    fn rewrite(&self) -> Result<(), String> {
        self.rewrite_config()
    }

    fn help(&self) -> Vec<String> {
        self.help_text()
    }
}

/// Adapts the server's [`AggregateCursorStore`] to the core [`CursorStoreProvider`]
/// seam so the FT.CURSOR executor can live behind a `ConnCtx` without the seam
/// naming any server type.
impl CursorStoreProvider for AggregateCursorStore {
    fn read_cursor(
        &self,
        id: u64,
        count: Option<usize>,
        expected_index: &str,
    ) -> Option<CursorReadBatch> {
        AggregateCursorStore::read_cursor(self, id, count, expected_index)
    }

    fn delete_cursor(&self, id: u64) -> bool {
        AggregateCursorStore::delete_cursor(self, id)
    }
}

impl ConnectionHandler {
    /// The ambient connection-command view over the handler's subsystem dep
    /// groups — the server-side wiring for [`ConnCtx::new`], which is the one
    /// place the field list itself is authored.
    ///
    /// This is an associated function taking the dep groups as explicit
    /// parameters instead of a `&self` method **on purpose**: a call site
    /// borrows exactly the handler fields that are never a capability slot
    /// (`admin`/`core`/`observability`/`cluster`/`memory_diag`), leaving
    /// `self.state`, `self.tracking_io`, the pub/sub channels, and the monitor
    /// receiver free for the `.with_*` capability layer's `&mut` borrows. Do
    /// NOT "simplify" this to take `&self`: a whole-`self` borrow destroys the
    /// disjointness and the mutable builders stop compiling.
    pub(crate) fn base_ctx<'a>(
        admin: &'a AdminDeps,
        core: &'a CoreDeps,
        observability: &'a ObservabilityDeps,
        cluster: &'a ClusterDeps,
        memory_diag: &'a MemoryDiag,
        num_shards: usize,
    ) -> ConnCtx<'a> {
        ConnCtx::new(
            admin.config_manager.as_ref(),
            admin.client_registry.as_ref(),
            observability.latency_histograms.as_ref(),
            observability.keyspace_stats.as_ref(),
            core.shard_senders.as_slice(),
            admin.snapshot_coordinator.as_ref(),
            &observability.hotkey_session,
            cluster,
            admin.cursor_store.as_ref(),
            observability.metrics_recorder.as_ref(),
            memory_diag,
            core.acl_manager.as_ref(),
            core.registry.as_ref(),
            num_shards,
            admin.config_manager.max_clients(),
            cluster.is_cluster_mode(),
        )
    }

    /// Build the [`ConnCtx`] a connection command declared it needs, selecting
    /// the capability wiring from the command's [`ConnMutation`] — declared in
    /// its [`CommandSpec`](frogdb_core::CommandSpec) — never from its string
    /// name. This is the single builder that subsumes the former
    /// `conn_ctx` / `conn_ctx_authmut` / `conn_ctx_clientmut` trio: the
    /// "which-slots-are-`Some`" protocol is now one exhaustive `match` the
    /// compiler checks, not three prose conventions across three methods.
    ///
    /// The three in-place arms:
    ///
    /// * [`ConnMutation::None`] — read-only view: the ambient deps plus the live
    ///   read providers (real INFO aggregation and scripting entry points via
    ///   `self`, the DEBUG capability, and the connection's protocol version and
    ///   authenticated username). Every capability slot but `debug` is `None`.
    /// * [`ConnMutation::Auth`] — the mutable connection-state capability
    ///   (`conn_state = Some`) for AUTH/HELLO, the RESET/ASKING/READONLY/
    ///   READWRITE family, and the MULTI/DISCARD/WATCH/UNWATCH transaction
    ///   controls. They read identity through `conn_state`, so the ambient
    ///   placeholders stand for everything else.
    /// * [`ConnMutation::Client`] — CLIENT's `conn_state = Some` *and*
    ///   `tracking = Some`. The two `&mut` borrows are of disjoint handler
    ///   fields (`self.state` and `self.tracking_io`).
    ///
    /// [`ConnMutation::Monitor`]/[`ConnMutation::PubSub`] are **not** built here:
    /// their `MonitorIo`/`PubSubIo` disjoint-borrow bundles are local temporaries
    /// that must outlive the `ConnCtx`, so they are owned at the dispatch call
    /// site (see [`execute_monitor`](Self::execute_monitor) /
    /// [`execute_pubsub`](Self::execute_pubsub)). The connection dispatcher routes
    /// those two capabilities to those methods from the same declared
    /// `ConnMutation`, so no command name is consulted anywhere.
    ///
    /// The disjointness that made the three separate builders necessary is
    /// preserved: [`base_ctx`](Self::base_ctx) borrows only the never-a-slot dep
    /// groups, leaving `self.state`/`self.tracking_io` free for the per-arm `&mut`
    /// sub-borrows.
    pub(crate) fn conn_ctx_for(&mut self, mutation: ConnMutation) -> ConnCtx<'_> {
        let base = Self::base_ctx(
            &self.admin,
            &self.core,
            &self.observability,
            &self.cluster,
            &self.memory_diag,
            self.num_shards,
        );
        match mutation {
            ConnMutation::None => base
                .with_full_reads(
                    self,
                    self,
                    // DEBUG dispatches through this read-only view; wire its
                    // handler-only capabilities (tracer, per-shard round-trips,
                    // bundle generation, subscription counts, the debug gate).
                    Some(self),
                    self.state.protocol_version,
                    self.state.username(),
                )
                // STATUS JSON renders from the shared status collector via `self`.
                .with_status(self),
            ConnMutation::Auth => base.with_conn_state(&mut self.state),
            ConnMutation::Client => base
                .with_conn_state(&mut self.state)
                .with_tracking(&mut self.tracking_io),
            ConnMutation::Monitor | ConnMutation::PubSub => unreachable!(
                "ConnMutation::{mutation:?} is dispatched via execute_monitor/execute_pubsub, \
                 which own the Io bundle the ConnCtx borrows; conn_ctx_for is never called for it"
            ),
        }
    }
}

/// The `CommandSpec` for CONFIG. Declared here alongside the executor (rather
/// than in a stub `Command` impl) so the connection command is a single
/// self-contained unit. Strategy is `ConnectionLevel(Admin)`; the registry
/// validates that this agrees with the `Connection` executor variant.
static CONFIG_SPEC: CommandSpec = CommandSpec {
    name: "CONFIG",
    arity: Arity::AtLeast(1),
    flags: CommandFlags::ADMIN
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
    mutation: frogdb_core::ConnMutation::None,
    strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin),
};

/// The registrable, `'static` CONFIG executor. Registered via
/// [`frogdb_core::CommandRegistry::register_connection`] in the server's command
/// registration (see `server/register.rs`).
pub(crate) static CONFIG_CONN_COMMAND: ConfigConnCommand = ConfigConnCommand;

/// CONFIG — configuration management (GET/SET/RESETSTAT/REWRITE/HELP).
///
/// Reads a [`ConnCtx`] instead of the whole handler, so it is unit-testable in
/// isolation (see `tests` below).
pub(crate) struct ConfigConnCommand;

impl ConnectionCommand for ConfigConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &CONFIG_SPEC
    }

    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        Box::pin(async move {
            if args.is_empty() {
                return Response::error("ERR wrong number of arguments for 'config' command");
            }

            let subcommand = args[0].to_ascii_uppercase();
            let subcommand_str = String::from_utf8_lossy(&subcommand);

            match subcommand_str.as_ref() {
                "GET" => config_get(ctx, &args[1..]),
                "SET" => config_set(ctx, &args[1..]).await,
                "RESETSTAT" => config_resetstat(ctx).await,
                "REWRITE" => config_rewrite(ctx),
                "HELP" => config_help(ctx),
                _ => Response::error(format!(
                    "ERR unknown subcommand '{}'. Try CONFIG HELP.",
                    subcommand_str
                )),
            }
        })
    }
}

/// CONFIG GET <pattern> [pattern ...] — return parameters matching patterns.
fn config_get(ctx: &ConnCtx<'_>, args: &[Bytes]) -> Response {
    if args.is_empty() {
        return Response::error("ERR wrong number of arguments for 'config|get' command");
    }

    // Collect results from all patterns, deduplicating by parameter name.
    let mut seen = std::collections::HashSet::new();
    let mut response = Vec::new();
    for arg in args {
        let pattern = String::from_utf8_lossy(arg);
        let results = ctx.config.get(&pattern);
        for (name, value) in results {
            if seen.insert(name.clone()) {
                response.push(Response::bulk(Bytes::from(name)));
                response.push(Response::bulk(Bytes::from(value)));
            }
        }
    }

    Response::Array(response)
}

/// CONFIG SET <param> <value> [param value ...] — set configuration parameters.
///
/// Async because it may propagate eviction config changes to all shard workers
/// and wait for acknowledgment.
async fn config_set(ctx: &ConnCtx<'_>, args: &[Bytes]) -> Response {
    if args.len() < 2 || !args.len().is_multiple_of(2) {
        return Response::error("ERR wrong number of arguments for 'config|set' command");
    }

    // Check for duplicate parameter names.
    {
        let mut seen = std::collections::HashSet::new();
        for pair in args.chunks(2) {
            let param = pair[0].to_ascii_lowercase();
            if !seen.insert(param) {
                return Response::error(format!(
                    "ERR Duplicate parameter '{}'",
                    String::from_utf8_lossy(&pair[0])
                ));
            }
        }
    }

    for pair in args.chunks(2) {
        let param = String::from_utf8_lossy(&pair[0]);
        let value = String::from_utf8_lossy(&pair[1]);

        if let Err(e) = ctx.config.set(&param, &value).await {
            return Response::error(e);
        }
    }

    Response::ok()
}

/// CONFIG RESETSTAT — reset server statistics.
///
/// Broadcasts a reset to all shard workers (latency monitor, slow query log,
/// peak memory), then clears the server-wide call counts, error stats, and
/// `INFO`-facing latency histograms, and rebases the operator-visible
/// keyspace hit/miss counters (Prometheus `_total` counters stay monotonic).
async fn config_resetstat(ctx: &ConnCtx<'_>) -> Response {
    // Await-and-discard: the replies are only a barrier confirming every shard
    // reset its stats. Bounded by the shared deadline (was unbounded).
    let _ = ScatterGather::new(ctx.shard_senders, DEFAULT_SCATTER_GATHER_TIMEOUT, 0)
        .gather_all(|_shard, response_tx| ShardMessage::ResetStats { response_tx })
        .await;
    ctx.client_registry.reset_command_call_counts();
    ctx.client_registry.error_stats.reset();
    ctx.latency_histograms.reset();
    ctx.keyspace_stats.reset();
    Response::ok()
}

/// CONFIG REWRITE — rewrite the config file with current runtime values.
fn config_rewrite(ctx: &ConnCtx<'_>) -> Response {
    match ctx.config.rewrite() {
        Ok(()) => Response::ok(),
        Err(e) => Response::error(e),
    }
}

/// CONFIG HELP — return help text.
fn config_help(ctx: &ConnCtx<'_>) -> Response {
    let help = ctx.config.help();
    let response: Vec<Response> = help
        .into_iter()
        .map(|s| Response::bulk(Bytes::from(s)))
        .collect();
    Response::Array(response)
}

/// The `CommandSpec` for FT.CURSOR. Declared here alongside the executor (rather
/// than in a stub `Command` impl) so the connection command is a single
/// self-contained unit. Strategy is `ConnectionLevel(Admin)`; the registry
/// validates that this agrees with the `Connection` executor variant.
static FT_CURSOR_SPEC: CommandSpec = CommandSpec {
    name: "FT.CURSOR",
    arity: Arity::AtLeast(3),
    flags: CommandFlags::READONLY,
    keys: KeySpec::None,
    access: AccessSpec::Uniform,
    wal: WalStrategy::NoOp,
    wakes: WaiterWake::None,
    event: EventSpec::NotApplicable,
    requires_same_slot: false,
    lookup: LookupSpec::None,
    mutation: frogdb_core::ConnMutation::None,
    strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin),
};

/// The registrable, `'static` FT.CURSOR executor. Registered via
/// [`frogdb_core::CommandRegistry::register_connection`] in the server's command
/// registration (see `server/register.rs`).
pub(crate) static FT_CURSOR_CONN_COMMAND: FtCursorConnCommand = FtCursorConnCommand;

/// FT.CURSOR — coordinator-only aggregate-cursor paging (READ/DEL).
///
/// Reads a [`ConnCtx`] instead of the whole handler, so it is unit-testable in
/// isolation (see `tests` below).
pub(crate) struct FtCursorConnCommand;

impl ConnectionCommand for FtCursorConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &FT_CURSOR_SPEC
    }

    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        Box::pin(async move {
            if args.len() < 3 {
                return Response::error("ERR wrong number of arguments for 'ft.cursor' command");
            }

            let subcommand = args[0].to_ascii_uppercase();
            let index_name = std::str::from_utf8(&args[1]).unwrap_or("");
            let cursor_id_str = std::str::from_utf8(&args[2]).unwrap_or("");
            let cursor_id: u64 = match cursor_id_str.parse() {
                Ok(id) => id,
                Err(_) => return Response::error("ERR invalid cursor id"),
            };

            let store = ctx.cursor_store;

            match subcommand.as_slice() {
                b"READ" => {
                    // Parse optional COUNT
                    let count_override =
                        if args.len() >= 5 && args[3].eq_ignore_ascii_case(b"COUNT") {
                            std::str::from_utf8(&args[4])
                                .ok()
                                .and_then(|s| s.parse::<usize>().ok())
                        } else {
                            None
                        };

                    match store.read_cursor(cursor_id, count_override, index_name) {
                        Some((rows, new_cursor_id)) => {
                            let mut result_items = Vec::new();
                            result_items.push(Response::Integer(rows.len() as i64));
                            for row in &rows {
                                let mut field_array = Vec::new();
                                for (k, v) in row {
                                    field_array.push(Response::bulk(Bytes::from(k.clone())));
                                    field_array.push(Response::bulk(Bytes::from(v.clone())));
                                }
                                result_items.push(Response::Array(field_array));
                            }

                            Response::Array(vec![
                                Response::Array(result_items),
                                Response::Integer(new_cursor_id as i64),
                            ])
                        }
                        None => Response::error("ERR Cursor not found"),
                    }
                }
                b"DEL" => {
                    if store.delete_cursor(cursor_id) {
                        Response::ok()
                    } else {
                        Response::error("ERR Cursor not found")
                    }
                }
                _ => Response::error(format!(
                    "ERR unknown FT.CURSOR subcommand '{}'",
                    String::from_utf8_lossy(&subcommand)
                )),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::connection::ClusterDeps;
    use crate::connection::observability_conn_command::MemoryDiag;
    use frogdb_core::{
        ClientRegistry, CommandLatencyHistograms, KeyspaceStats, NoopMetricsRecorder,
        SharedHotkeySession, new_shared_hotkey_session,
    };

    /// Build a `ConnCtx` over fixture dependencies — no socket, no
    /// `ConnectionHandler`. This is the point of the seam: a connection command
    /// is exercisable in a unit test.
    struct Fixture {
        config_manager: ConfigManager,
        client_registry: ClientRegistry,
        latency_histograms: CommandLatencyHistograms,
        keyspace_stats: KeyspaceStats,
        snapshot_coordinator: frogdb_core::persistence::NoopSnapshotCoordinator,
        hotkey_session: SharedHotkeySession,
        cluster: ClusterDeps,
        cursor_store: AggregateCursorStore,
        metrics_recorder: NoopMetricsRecorder,
        memory_diag: MemoryDiag,
        acl_manager: std::sync::Arc<frogdb_core::AclManager>,
        command_registry: frogdb_core::CommandRegistry,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                config_manager: ConfigManager::new(&Config::default()),
                client_registry: ClientRegistry::new(),
                latency_histograms: CommandLatencyHistograms::new(true),
                keyspace_stats: KeyspaceStats::new(),
                snapshot_coordinator: frogdb_core::persistence::NoopSnapshotCoordinator::new(),
                hotkey_session: new_shared_hotkey_session(),
                cluster: ClusterDeps::standalone(),
                cursor_store: AggregateCursorStore::new(),
                metrics_recorder: NoopMetricsRecorder::new(),
                memory_diag: MemoryDiag(frogdb_debug::MemoryDiagConfig::default()),
                acl_manager: frogdb_core::AclManager::new(Default::default()),
                command_registry: frogdb_core::CommandRegistry::new(),
            }
        }

        fn ctx(&self) -> ConnCtx<'_> {
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
            .with_username("default")
        }
    }

    fn arg(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[tokio::test]
    async fn config_get_returns_matching_params() {
        let fx = Fixture::new();
        let resp = ConfigConnCommand
            .execute(&mut fx.ctx(), &[arg("GET"), arg("*")])
            .await;
        match resp {
            Response::Array(items) => {
                assert!(!items.is_empty(), "CONFIG GET * should return params")
            }
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn config_unknown_subcommand_errors() {
        let fx = Fixture::new();
        let resp = ConfigConnCommand
            .execute(&mut fx.ctx(), &[arg("NOPE")])
            .await;
        assert!(matches!(resp, Response::Error(_)));
    }

    #[tokio::test]
    async fn config_empty_args_errors() {
        let fx = Fixture::new();
        let resp = ConfigConnCommand.execute(&mut fx.ctx(), &[]).await;
        assert!(matches!(resp, Response::Error(_)));
    }

    #[tokio::test]
    async fn config_resetstat_with_no_shards_is_ok() {
        let fx = Fixture::new();
        let resp = ConfigConnCommand
            .execute(&mut fx.ctx(), &[arg("RESETSTAT")])
            .await;
        assert_eq!(resp, Response::ok());
    }

    #[test]
    fn config_spec_is_connection_level_and_valid() {
        assert!(CONFIG_CONN_COMMAND.spec().validate().is_ok());
        assert!(matches!(
            CONFIG_CONN_COMMAND.spec().strategy,
            ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin)
        ));
    }

    #[tokio::test]
    async fn ft_cursor_read_pages_and_exhausts() {
        use std::time::Duration;

        let fx = Fixture::new();
        let rows = vec![
            vec![("a".to_string(), "1".to_string())],
            vec![("b".to_string(), "2".to_string())],
            vec![("c".to_string(), "3".to_string())],
        ];
        let id = fx
            .cursor_store
            .create_cursor(rows, 2, "idx".to_string(), Duration::from_secs(60));

        // First READ returns a 2-row batch and keeps the cursor open.
        let resp = FtCursorConnCommand
            .execute(
                &mut fx.ctx(),
                &[arg("READ"), arg("idx"), arg(&id.to_string())],
            )
            .await;
        match resp {
            Response::Array(ref items) => {
                assert_eq!(items.len(), 2);
                match (&items[0], &items[1]) {
                    (Response::Array(batch), Response::Integer(new_id)) => {
                        // Leading count + 2 row arrays.
                        assert_eq!(batch[0], Response::Integer(2));
                        assert_eq!(batch.len(), 3);
                        assert_eq!(*new_id, id as i64);
                    }
                    other => panic!("unexpected shape: {other:?}"),
                }
            }
            other => panic!("expected array, got {other:?}"),
        }

        // Second READ drains the remaining row and closes the cursor (id 0).
        let resp = FtCursorConnCommand
            .execute(
                &mut fx.ctx(),
                &[arg("READ"), arg("idx"), arg(&id.to_string())],
            )
            .await;
        match resp {
            Response::Array(items) => match (&items[0], &items[1]) {
                (Response::Array(batch), Response::Integer(new_id)) => {
                    assert_eq!(batch[0], Response::Integer(1));
                    assert_eq!(*new_id, 0);
                }
                other => panic!("unexpected shape: {other:?}"),
            },
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn ft_cursor_read_wrong_index_is_not_found() {
        use std::time::Duration;

        let fx = Fixture::new();
        let id = fx.cursor_store.create_cursor(
            vec![vec![("a".to_string(), "1".to_string())]],
            10,
            "idx".to_string(),
            Duration::from_secs(60),
        );
        let resp = FtCursorConnCommand
            .execute(
                &mut fx.ctx(),
                &[arg("READ"), arg("other"), arg(&id.to_string())],
            )
            .await;
        assert!(matches!(resp, Response::Error(_)));
    }

    #[tokio::test]
    async fn ft_cursor_del_removes_cursor() {
        use std::time::Duration;

        let fx = Fixture::new();
        let id = fx.cursor_store.create_cursor(
            vec![vec![("a".to_string(), "1".to_string())]],
            10,
            "idx".to_string(),
            Duration::from_secs(60),
        );
        let resp = FtCursorConnCommand
            .execute(
                &mut fx.ctx(),
                &[arg("DEL"), arg("idx"), arg(&id.to_string())],
            )
            .await;
        assert_eq!(resp, Response::ok());

        // Deleting again reports not found.
        let resp = FtCursorConnCommand
            .execute(
                &mut fx.ctx(),
                &[arg("DEL"), arg("idx"), arg(&id.to_string())],
            )
            .await;
        assert!(matches!(resp, Response::Error(_)));
    }

    #[tokio::test]
    async fn ft_cursor_invalid_id_errors() {
        let fx = Fixture::new();
        let resp = FtCursorConnCommand
            .execute(&mut fx.ctx(), &[arg("READ"), arg("idx"), arg("notanumber")])
            .await;
        assert!(matches!(resp, Response::Error(_)));
    }

    #[tokio::test]
    async fn ft_cursor_unknown_subcommand_errors() {
        let fx = Fixture::new();
        let resp = FtCursorConnCommand
            .execute(&mut fx.ctx(), &[arg("NOPE"), arg("idx"), arg("1")])
            .await;
        assert!(matches!(resp, Response::Error(_)));
    }

    #[test]
    fn ft_cursor_spec_is_connection_level_and_valid() {
        assert!(FT_CURSOR_CONN_COMMAND.spec().validate().is_ok());
        assert!(matches!(
            FT_CURSOR_CONN_COMMAND.spec().strategy,
            ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin)
        ));
    }
}
