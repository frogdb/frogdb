//! The connection-command seam.
//!
//! A [`ConnectionCommand`] executes against a narrow [`ConnCtx`] view of the
//! connection — only the subsystems it declares — instead of taking
//! `&ConnectionHandler` (a 34-field god object). This is the target shape for
//! the connection-level command tree: it collapses the "spec stub in the
//! `commands` crate, logic as a method on `ConnectionHandler` in
//! `connection/handlers/`" split into one self-contained unit whose interface is
//! its `ConnCtx`, not the whole handler.
//!
//! CONFIG is the first command migrated behind this seam. AUTH, CLIENT, and the
//! rest of `dispatch_connection_level` follow the same shape; the entangled ones
//! (CLIENT reaches into `client_registry`/`state`/`tracking`) are exactly the
//! commands whose `ConnCtx` documents how much of the god object they touch.

use bytes::Bytes;
use frogdb_core::{
    ClientRegistry, CommandLatencyHistograms, KeyspaceStats, ShardMessage, ShardSender,
};
use frogdb_protocol::Response;
use tokio::sync::oneshot;

use crate::connection::ConnectionHandler;
use crate::runtime_config::ConfigManager;

/// A narrow, per-command view of the connection: shared borrows of only the
/// subsystems the executing [`ConnectionCommand`] needs. This is the command's
/// test surface — a command is exercised by constructing a `ConnCtx` over
/// fixture dependencies, with no socket and no `ConnectionHandler`.
pub(crate) struct ConnCtx<'a> {
    /// Runtime configuration parameters (CONFIG GET/SET/REWRITE/HELP).
    pub(crate) config_manager: &'a ConfigManager,
    /// Per-client registry: call counts, error stats (CONFIG RESETSTAT).
    pub(crate) client_registry: &'a ClientRegistry,
    /// `INFO commandstats`/`errorstats`/`latencystats` histograms (RESETSTAT).
    pub(crate) latency_histograms: &'a CommandLatencyHistograms,
    /// Operator-visible keyspace hit/miss counters (RESETSTAT rebase).
    pub(crate) keyspace_stats: &'a KeyspaceStats,
    /// Per-shard message channels, for broadcasts (RESETSTAT).
    pub(crate) shard_senders: &'a [ShardSender],
}

/// A command handled at the connection level, executed against a narrow
/// [`ConnCtx`] rather than `&mut ConnectionHandler`.
///
/// Dispatched statically today (one call site per command in `dispatch.rs`);
/// this becomes `dyn`-compatible for registry-driven dispatch once the whole
/// connection-level tree has migrated behind the seam.
#[allow(async_fn_in_trait)]
pub(crate) trait ConnectionCommand {
    async fn execute(&self, ctx: &ConnCtx<'_>, args: &[Bytes]) -> Response;
}

impl ConnectionHandler {
    /// Build the narrow connection-command view over this handler's state.
    pub(crate) fn conn_ctx(&self) -> ConnCtx<'_> {
        ConnCtx {
            config_manager: self.admin.config_manager.as_ref(),
            client_registry: self.admin.client_registry.as_ref(),
            latency_histograms: self.observability.latency_histograms.as_ref(),
            keyspace_stats: self.observability.keyspace_stats.as_ref(),
            shard_senders: self.core.shard_senders.as_slice(),
        }
    }
}

/// CONFIG — configuration management (GET/SET/RESETSTAT/REWRITE/HELP).
///
/// Migrated from `impl ConnectionHandler` in `connection/handlers/config.rs`;
/// the logic is unchanged, but it now reads a [`ConnCtx`] instead of the whole
/// handler, so it is unit-testable in isolation (see `tests` below).
pub(crate) struct ConfigConnCommand;

impl ConnectionCommand for ConfigConnCommand {
    async fn execute(&self, ctx: &ConnCtx<'_>, args: &[Bytes]) -> Response {
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
        let results = ctx.config_manager.get(&pattern);
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

        if let Err(e) = ctx.config_manager.set_async(&param, &value).await {
            return Response::error(e.to_string());
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
    match ctx.config_manager.rewrite_config() {
        Ok(()) => Response::ok(),
        Err(e) => Response::error(e),
    }
}

/// CONFIG HELP — return help text.
fn config_help(ctx: &ConnCtx<'_>) -> Response {
    let help = ctx.config_manager.help_text();
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
                config_manager: &self.config_manager,
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
            Response::Array(items) => assert!(!items.is_empty(), "CONFIG GET * should return params"),
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn config_unknown_subcommand_errors() {
        let fx = Fixture::new();
        let resp = ConfigConnCommand
            .execute(&fx.ctx(), &[arg("NOPE")])
            .await;
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
}
