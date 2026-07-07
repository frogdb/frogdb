//! The connection-command seam (server-side executors).
//!
//! The seam itself — the [`ConnectionCommand`] trait, its [`ConnCtx`] view, and
//! the [`ConfigProvider`] abstraction — is defined in `frogdb_core` so the
//! command registry can store a connection command as `CommandImpl::Connection`
//! (see [`frogdb_core::registry::CommandImpl`]). This module holds the
//! server-side executors that implement the seam and the glue that adapts
//! server state (the [`ConfigManager`], the [`ConnectionHandler`]) to it.
//!
//! CONFIG is the first command migrated behind this seam: it is registered as
//! `CommandImpl::Connection` and dispatched through the registry union, not the
//! legacy `router.rs`/`dispatch.rs` connection-handler path. AUTH, CLIENT, and
//! the rest of `dispatch_connection_level` follow the same shape in Phase 2.

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, BoxFuture, CommandFlags, CommandSpec, ConfigProvider, ConnectionLevelOp,
    EventSpec, ExecutionStrategy, KeySpec, LookupSpec, ShardMessage, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;
use tokio::sync::oneshot;

use crate::connection::ConnectionHandler;
use crate::runtime_config::ConfigManager;

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

impl ConnectionHandler {
    /// Build the narrow connection-command view over this handler's state.
    pub(crate) fn conn_ctx(&self) -> ConnCtx<'_> {
        ConnCtx {
            config: self.admin.config_manager.as_ref(),
            client_registry: self.admin.client_registry.as_ref(),
            latency_histograms: self.observability.latency_histograms.as_ref(),
            keyspace_stats: self.observability.keyspace_stats.as_ref(),
            shard_senders: self.core.shard_senders.as_slice(),
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

    fn execute<'a>(&'a self, ctx: &'a ConnCtx<'a>, args: &'a [Bytes]) -> BoxFuture<'a, Response> {
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
    for sender in ctx.shard_senders.iter() {
        let (response_tx, response_rx) = oneshot::channel();
        if sender
            .send(ShardMessage::ResetStats { response_tx })
            .await
            .is_ok()
        {
            let _ = response_rx.await;
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use frogdb_core::{ClientRegistry, CommandLatencyHistograms, KeyspaceStats};

    /// Build a `ConnCtx` over fixture dependencies — no socket, no
    /// `ConnectionHandler`. This is the point of the seam: a connection command
    /// is exercisable in a unit test.
    struct Fixture {
        config_manager: ConfigManager,
        client_registry: ClientRegistry,
        latency_histograms: CommandLatencyHistograms,
        keyspace_stats: KeyspaceStats,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                config_manager: ConfigManager::new(&Config::default()),
                client_registry: ClientRegistry::new(),
                latency_histograms: CommandLatencyHistograms::new(true),
                keyspace_stats: KeyspaceStats::new(),
            }
        }

        fn ctx(&self) -> ConnCtx<'_> {
            ConnCtx {
                config: &self.config_manager,
                client_registry: &self.client_registry,
                latency_histograms: &self.latency_histograms,
                keyspace_stats: &self.keyspace_stats,
                shard_senders: &[],
            }
        }
    }

    fn arg(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[tokio::test]
    async fn config_get_returns_matching_params() {
        let fx = Fixture::new();
        let resp = ConfigConnCommand
            .execute(&fx.ctx(), &[arg("GET"), arg("*")])
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
        let resp = ConfigConnCommand.execute(&fx.ctx(), &[arg("NOPE")]).await;
        assert!(matches!(resp, Response::Error(_)));
    }

    #[tokio::test]
    async fn config_empty_args_errors() {
        let fx = Fixture::new();
        let resp = ConfigConnCommand.execute(&fx.ctx(), &[]).await;
        assert!(matches!(resp, Response::Error(_)));
    }

    #[tokio::test]
    async fn config_resetstat_with_no_shards_is_ok() {
        let fx = Fixture::new();
        let resp = ConfigConnCommand
            .execute(&fx.ctx(), &[arg("RESETSTAT")])
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
}
