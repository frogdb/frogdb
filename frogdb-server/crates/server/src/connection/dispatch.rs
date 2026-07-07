//! Command dispatch and pipeline logic.
//!
//! This module handles routing parsed commands to their connection-level handlers
//! and executing the main command pipeline (transaction handling, pub/sub mode,
//! cluster validation, etc.).

use std::sync::Arc;

use bytes::Bytes;
use frogdb_core::ExecutionStrategy;
use frogdb_protocol::Response;
use tracing::Instrument;

use crate::connection::ConnectionHandler;
use crate::connection::conn_command::ConnectionCommand;
use crate::connection::router::ConnectionLevelHandler;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::LazyLock;

/// Boxed future returned by a server-wide handler closure.
type ServerWideFuture<'a> = Pin<Box<dyn Future<Output = Response> + Send + 'a>>;

/// Function pointer for a server-wide handler. Takes a connection handler
/// and the raw arguments; returns a boxed future that resolves to the
/// command's response.
type ServerWideHandler = for<'a> fn(&'a mut ConnectionHandler, &'a [Bytes]) -> ServerWideFuture<'a>;

/// Name-keyed dispatch table for server-wide commands.
///
/// Keys are the uppercase command name (matching `CommandRegistry`'s
/// canonicalization). Adding a new server-wide command requires:
///   1. Declare `ExecutionStrategy::ServerWide` on the command struct.
///   2. Add a row here mapping the command name to its handler closure.
///
/// The `dispatch_table_covers_registry` test enforces this.
static SERVER_WIDE_HANDLERS: LazyLock<HashMap<&'static str, ServerWideHandler>> =
    LazyLock::new(|| {
        let mut t: HashMap<&'static str, ServerWideHandler> = HashMap::new();
        // Core commands
        t.insert("SCAN", |h, args| Box::pin(h.handle_scan(args)));
        t.insert("KEYS", |h, args| Box::pin(h.handle_keys(args)));
        t.insert("DBSIZE", |h, _| Box::pin(h.handle_dbsize()));
        t.insert("RANDOMKEY", |h, _| Box::pin(h.handle_randomkey()));
        t.insert("FLUSHDB", |h, args| Box::pin(h.handle_flushdb(args)));
        t.insert("FLUSHALL", |h, args| Box::pin(h.handle_flushall(args)));
        t.insert("MIGRATE", |h, args| Box::pin(h.handle_migrate(args)));
        t.insert("SHUTDOWN", |h, args| Box::pin(h.handle_shutdown(args)));
        // TimeSeries
        t.insert("TS.QUERYINDEX", |h, args| {
            Box::pin(h.handle_ts_queryindex(args))
        });
        t.insert("TS.MGET", |h, args| Box::pin(h.handle_ts_mget(args)));
        t.insert("TS.MRANGE", |h, args| {
            Box::pin(h.handle_ts_mrange(args, false))
        });
        t.insert("TS.MREVRANGE", |h, args| {
            Box::pin(h.handle_ts_mrange(args, true))
        });
        // Search (FT.*)
        t.insert("FT.CREATE", |h, args| Box::pin(h.handle_ft_create(args)));
        t.insert("FT.SEARCH", |h, args| Box::pin(h.handle_ft_search(args)));
        t.insert("FT.DROPINDEX", |h, args| {
            Box::pin(h.handle_ft_dropindex(args))
        });
        t.insert("FT.INFO", |h, args| Box::pin(h.handle_ft_info(args)));
        t.insert("FT._LIST", |h, args| Box::pin(h.handle_ft_list(args)));
        t.insert("FT.ALTER", |h, args| Box::pin(h.handle_ft_alter(args)));
        t.insert("FT.SYNUPDATE", |h, args| {
            Box::pin(h.handle_ft_synupdate(args))
        });
        t.insert("FT.SYNDUMP", |h, args| Box::pin(h.handle_ft_syndump(args)));
        t.insert("FT.AGGREGATE", |h, args| {
            Box::pin(h.handle_ft_aggregate(args))
        });
        t.insert("FT.HYBRID", |h, args| Box::pin(h.handle_ft_hybrid(args)));
        t.insert("FT.ALIASADD", |h, args| {
            Box::pin(h.handle_ft_aliasadd(args))
        });
        t.insert("FT.ALIASDEL", |h, args| {
            Box::pin(h.handle_ft_aliasdel(args))
        });
        t.insert("FT.ALIASUPDATE", |h, args| {
            Box::pin(h.handle_ft_aliasupdate(args))
        });
        t.insert("FT.TAGVALS", |h, args| Box::pin(h.handle_ft_tagvals(args)));
        t.insert("FT.DICTADD", |h, args| Box::pin(h.handle_ft_dictadd(args)));
        t.insert("FT.DICTDEL", |h, args| Box::pin(h.handle_ft_dictdel(args)));
        t.insert("FT.DICTDUMP", |h, args| {
            Box::pin(h.handle_ft_dictdump(args))
        });
        t.insert("FT.CONFIG", |h, args| Box::pin(h.handle_ft_config(args)));
        t.insert("FT.SPELLCHECK", |h, args| {
            Box::pin(h.handle_ft_spellcheck(args))
        });
        t.insert("FT.EXPLAIN", |h, args| {
            Box::pin(h.handle_ft_explain(args, false))
        });
        t.insert("FT.EXPLAINCLI", |h, args| {
            Box::pin(h.handle_ft_explain(args, true))
        });
        t.insert("FT.PROFILE", |h, args| Box::pin(h.handle_ft_profile(args)));
        // Event sourcing
        t.insert("ES.ALL", |h, args| Box::pin(h.handle_es_all(args)));
        t
    });

impl ConnectionHandler {
    /// Determine the connection-level handler for a command.
    ///
    /// Delegates to [`crate::connection::router::route_connection_level`], the
    /// single owner of the op→handler decision. Returns `Some(handler)` if the
    /// command declares a `ConnectionLevel` strategy (refined by command name,
    /// e.g. `Admin` + `CONFIG` → `Config`), or `None` for any other strategy.
    pub(crate) fn connection_level_handler_for(
        &self,
        cmd_name: &str,
    ) -> Option<ConnectionLevelHandler> {
        crate::connection::router::route_connection_level(&self.core.registry, cmd_name)
    }

    /// Dispatch a command to its connection-level handler.
    ///
    /// This method routes commands to their appropriate handlers based on the
    /// `ConnectionLevelHandler` category. Returns `Some(responses)` if the command
    /// was handled, or `None` if it should fall through to standard routing.
    pub(crate) async fn dispatch_connection_level(
        &mut self,
        handler: ConnectionLevelHandler,
        cmd_name: &str,
        args: &[Bytes],
    ) -> Option<Vec<Response>> {
        match handler {
            // Auth handlers - these are handled early in route_and_execute_with_transaction
            // so they shouldn't reach here, but we handle them for completeness
            ConnectionLevelHandler::Auth => Some(vec![self.handle_auth(args).await]),
            ConnectionLevelHandler::Hello => Some(vec![self.handle_hello(args).await]),

            // Pub/Sub handlers
            ConnectionLevelHandler::PubSub => self.dispatch_pubsub(cmd_name, args).await,

            // Sharded Pub/Sub handlers
            ConnectionLevelHandler::ShardedPubSub => {
                self.dispatch_sharded_pubsub(cmd_name, args).await
            }

            // Transaction handlers
            ConnectionLevelHandler::Transaction => self.dispatch_transaction(cmd_name, args).await,

            // Scripting handlers
            ConnectionLevelHandler::Scripting => self.dispatch_scripting(cmd_name, args).await,

            // Function handlers
            ConnectionLevelHandler::Function => self.dispatch_function(cmd_name, args).await,

            // Admin handlers
            ConnectionLevelHandler::Client => Some(vec![self.handle_client_command(args).await]),
            ConnectionLevelHandler::Config => Some(vec![
                crate::connection::conn_command::ConfigConnCommand
                    .execute(&self.conn_ctx(), args)
                    .await,
            ]),
            ConnectionLevelHandler::Info => Some(vec![self.handle_info(args).await]),
            ConnectionLevelHandler::Debug => self.dispatch_debug(args).await,
            ConnectionLevelHandler::Monitor => Some(vec![self.handle_monitor().await]),

            // Connection state handlers
            ConnectionLevelHandler::ConnectionState => {
                self.dispatch_connection_state(cmd_name, args).await
            }

            // Persistence commands (BGSAVE, LASTSAVE) are migrated behind the
            // ConnCtx seam: they dispatch through the registry union
            // (`dispatch_connection_command`) before this legacy path is
            // reached, so this arm is an unreachable fallthrough. It remains
            // only because `ConnectionLevelOp::Persistence` (required by the
            // migrated specs) keeps `handler_for` total.
            ConnectionLevelHandler::Persistence => None,

            // Replication handlers - fall through to standard routing
            // PSYNC needs the full command for route_and_execute
            ConnectionLevelHandler::Replication => None,
        }
    }

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
        &self,
        cmd_name: &str,
        args: &[Bytes],
    ) -> Option<Vec<Response>> {
        let command = self.core.registry.get_entry(cmd_name)?.as_connection()?;
        Some(vec![command.execute(&self.conn_ctx(), args).await])
    }

    /// Dispatch pub/sub commands.
    async fn dispatch_pubsub(&mut self, cmd_name: &str, args: &[Bytes]) -> Option<Vec<Response>> {
        match cmd_name {
            "SUBSCRIBE" => {
                if let Err(err) = self.validate_channel_access(args) {
                    return Some(vec![err]);
                }
                Some(self.handle_subscribe(args).await)
            }
            "UNSUBSCRIBE" => Some(self.handle_unsubscribe(args).await),
            "PSUBSCRIBE" => {
                if let Err(err) = self.validate_channel_access(args) {
                    return Some(vec![err]);
                }
                Some(self.handle_psubscribe(args).await)
            }
            "PUNSUBSCRIBE" => Some(self.handle_punsubscribe(args).await),
            "PUBLISH" => {
                if !args.is_empty()
                    && let Err(err) = self.validate_channel_access(&args[..1])
                {
                    return Some(vec![err]);
                }
                Some(vec![self.handle_publish(args).await])
            }
            "PUBSUB" => Some(vec![self.handle_pubsub_command(args).await]),
            "RESET" => Some(vec![self.handle_reset().await]),
            _ => None,
        }
    }

    /// Dispatch sharded pub/sub commands.
    async fn dispatch_sharded_pubsub(
        &mut self,
        cmd_name: &str,
        args: &[Bytes],
    ) -> Option<Vec<Response>> {
        match cmd_name {
            "SSUBSCRIBE" => {
                if let Err(err) = self.validate_channel_access(args) {
                    return Some(vec![err]);
                }
                Some(self.handle_ssubscribe(args).await)
            }
            "SUNSUBSCRIBE" => Some(self.handle_sunsubscribe(args).await),
            "SPUBLISH" => {
                if !args.is_empty()
                    && let Err(err) = self.validate_channel_access(&args[..1])
                {
                    return Some(vec![err]);
                }
                Some(vec![self.handle_spublish(args).await])
            }
            _ => None,
        }
    }

    /// Dispatch transaction commands.
    async fn dispatch_transaction(
        &mut self,
        cmd_name: &str,
        args: &[Bytes],
    ) -> Option<Vec<Response>> {
        match cmd_name {
            "MULTI" => Some(vec![self.handle_multi()]),
            "EXEC" => Some(self.handle_exec().await),
            "DISCARD" => Some(vec![self.handle_discard()]),
            "WATCH" => Some(vec![self.handle_watch(args).await]),
            "UNWATCH" => Some(vec![self.handle_unwatch()]),
            _ => None,
        }
    }

    /// Dispatch scripting commands.
    async fn dispatch_scripting(
        &mut self,
        cmd_name: &str,
        args: &[Bytes],
    ) -> Option<Vec<Response>> {
        match cmd_name {
            "EVAL" => Some(vec![self.handle_eval(args, false).await]),
            "EVAL_RO" => Some(vec![self.handle_eval(args, true).await]),
            "EVALSHA" => Some(vec![self.handle_evalsha(args, false).await]),
            "EVALSHA_RO" => Some(vec![self.handle_evalsha(args, true).await]),
            "SCRIPT" => Some(vec![self.handle_script(args).await]),
            _ => None,
        }
    }

    /// Dispatch function commands.
    async fn dispatch_function(&mut self, cmd_name: &str, args: &[Bytes]) -> Option<Vec<Response>> {
        match cmd_name {
            "FCALL" => Some(vec![self.handle_fcall(args, false).await]),
            "FCALL_RO" => Some(vec![self.handle_fcall(args, true).await]),
            "FUNCTION" => Some(vec![self.handle_function(args).await]),
            _ => None,
        }
    }

    /// Dispatch debug commands.
    pub(crate) async fn dispatch_debug(&mut self, args: &[Bytes]) -> Option<Vec<Response>> {
        if args.is_empty() {
            return None; // Fall through to standard routing
        }

        let subcommand = args[0].to_ascii_uppercase();
        match subcommand.as_slice() {
            b"SLEEP" => {
                if !self.enable_debug_command {
                    Some(vec![Response::error(
                        "ERR DEBUG SLEEP is disabled. Set server.enable-debug-command in the config to allow it.",
                    )])
                } else {
                    Some(vec![self.handle_debug_sleep(args).await])
                }
            }
            b"TRACING" => {
                if args.len() > 1 && args[1].eq_ignore_ascii_case(b"STATUS") {
                    Some(vec![self.handle_debug_tracing_status()])
                } else if args.len() > 1 && args[1].eq_ignore_ascii_case(b"RECENT") {
                    Some(vec![self.handle_debug_tracing_recent(args)])
                } else {
                    Some(vec![Response::error(
                        "ERR Unknown DEBUG TRACING subcommand. Use STATUS or RECENT [count].",
                    )])
                }
            }
            b"STRUCTSIZE" => Some(vec![self.handle_debug_structsize()]),
            b"HELP" => Some(vec![self.handle_debug_help()]),
            b"VLL" => Some(vec![self.handle_debug_vll(args).await]),
            b"PUBSUB" => {
                if args.len() > 1 && args[1].eq_ignore_ascii_case(b"LIMITS") {
                    Some(vec![self.handle_debug_pubsub_limits().await])
                } else {
                    Some(vec![Response::error(
                        "ERR Unknown DEBUG PUBSUB subcommand. Use LIMITS.",
                    )])
                }
            }
            b"BUNDLE" => {
                if args.len() > 1 && args[1].eq_ignore_ascii_case(b"GENERATE") {
                    Some(vec![self.handle_debug_bundle_generate(args).await])
                } else if args.len() > 1 && args[1].eq_ignore_ascii_case(b"LIST") {
                    Some(vec![self.handle_debug_bundle_list()])
                } else {
                    Some(vec![Response::error(
                        "ERR Unknown DEBUG BUNDLE subcommand. Use GENERATE [DURATION <seconds>] or LIST.",
                    )])
                }
            }
            b"HASHING" => Some(vec![self.handle_debug_hashing(args)]),
            b"RESP3" => Some(vec![self.handle_debug_resp3(args)]),
            b"SET-ACTIVE-EXPIRE" => Some(vec![self.handle_debug_set_active_expire(args).await]),
            b"KEYSIZES-HIST-ASSERT" => {
                Some(vec![self.handle_debug_keysizes_hist_assert(args).await])
            }
            b"ALLOCSIZE-SLOTS-ASSERT" => {
                Some(vec![self.handle_debug_allocsize_slots_assert(args).await])
            }
            // Dangerous commands — intentionally not supported
            b"SEGFAULT" | b"RELOAD" | b"CRASH-AND-RECOVER" | b"OOM" | b"PANIC" => {
                Some(vec![Response::error(format!(
                    "ERR DEBUG {} is not supported (unsafe command)",
                    String::from_utf8_lossy(&subcommand)
                ))])
            }
            _ => Some(vec![Response::error(format!(
                "ERR Unknown DEBUG subcommand '{}'",
                String::from_utf8_lossy(&subcommand)
            ))]),
        }
    }

    /// Returns the dispatch handler for a server-wide command, if any.
    fn server_wide_handler(&self, cmd_name: &str) -> Option<ServerWideHandler> {
        let entry = self.core.registry.get_entry(cmd_name)?;
        match entry.execution_strategy() {
            ExecutionStrategy::ServerWide => {
                let upper = cmd_name.to_ascii_uppercase();
                SERVER_WIDE_HANDLERS.get(upper.as_str()).copied()
            }
            _ => None,
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

    /// Dispatch connection state commands.
    async fn dispatch_connection_state(
        &mut self,
        cmd_name: &str,
        args: &[Bytes],
    ) -> Option<Vec<Response>> {
        match cmd_name {
            "RESET" => Some(vec![self.handle_reset().await]),
            "ASKING" => {
                self.state.set_asking();
                Some(vec![Response::ok()])
            }
            "READONLY" => {
                self.state.set_readonly(true);
                Some(vec![Response::ok()])
            }
            "READWRITE" => {
                self.state.set_readonly(false);
                Some(vec![Response::ok()])
            }
            // In pubsub mode, PING format depends on protocol version:
            // RESP2: array ["pong", <message>]
            // RESP3: simple string "PONG" or the message argument
            "PING" if self.state.in_pubsub_mode() => {
                if self.state.protocol_version.is_resp3() {
                    let response = if args.is_empty() {
                        Response::pong()
                    } else {
                        Response::bulk(args[0].clone())
                    };
                    Some(vec![response])
                } else {
                    let message = if args.is_empty() {
                        Bytes::from_static(b"")
                    } else {
                        args[0].clone()
                    };
                    Some(vec![Response::Array(vec![
                        Response::bulk(Bytes::from_static(b"pong")),
                        Response::bulk(message),
                    ])])
                }
            }
            // Note: SELECT, QUIT, PING, ECHO, COMMAND are handled via standard shard routing
            _ => None,
        }
    }

    /// Route and execute a command, handling transaction and pub/sub modes.
    /// Returns a Vec of responses since pub/sub commands can return multiple messages.
    ///
    /// `cmd_name` is the precomputed uppercase command name to avoid redundant allocations.
    pub(crate) async fn route_and_execute_with_transaction(
        &mut self,
        cmd: &Arc<frogdb_protocol::ParsedCommand>,
        cmd_name: &str,
    ) -> Vec<Response> {
        // Handle AUTH command (always allowed, even without authentication)
        if cmd_name == "AUTH" {
            return vec![self.handle_auth(&cmd.args).await];
        }

        // Handle HELLO command (always allowed, even without authentication)
        if cmd_name == "HELLO" {
            return vec![self.handle_hello(&cmd.args).await];
        }

        // ACL is migrated behind the ConnCtx seam: after pre-checks it dispatches
        // through the registry union (`dispatch_connection_command`) like CONFIG,
        // rather than being intercepted early here. AUTH and HELLO keep their
        // early path (they always run before authentication) until a later wave.

        // Run pre-execution checks (auth, admin port, ACL, pub/sub mode)
        if let Some(error_response) = self.run_pre_checks(cmd_name, &cmd.args) {
            self.record_error_response(&error_response, true, cmd_name);
            return vec![error_response];
        }

        // In pub/sub mode, route allowed commands through the connection state handler.
        // PING is registered as a Standard (shard) command but needs special handling
        // in pub/sub mode to return array format ["pong", <message>].
        if self.state.in_pubsub_mode()
            && let Some(responses) = self.dispatch_connection_state(cmd_name, &cmd.args).await
        {
            return responses;
        }

        // Transaction control commands and RESET are always dispatched directly,
        // never queued or blocked by pause.
        if matches!(
            cmd_name,
            "MULTI" | "EXEC" | "DISCARD" | "WATCH" | "UNWATCH" | "RESET"
        ) && let Some(handler) = self.connection_level_handler_for(cmd_name)
            && let Some(responses) = self
                .dispatch_connection_level(handler, cmd_name, &cmd.args)
                .await
        {
            return responses;
        }

        // If in transaction mode, queue the command instead of executing.
        // This must happen BEFORE connection-level dispatch so that commands like
        // CLIENT PAUSE, EVAL, etc. are queued during MULTI (not executed immediately).
        // Blocking commands (BLPOP, BRPOP, etc.) are also queued — at EXEC time they
        // execute with timeout=0 (non-blocking), matching Redis semantics.
        if self.state.in_transaction() {
            // Validate cluster slot ownership before queuing — commands that would
            // get MOVED should fail immediately rather than succeeding at EXEC time.
            if let Some(cluster_error) = self.validate_cluster_slots(cmd) {
                let error_msg = match &cluster_error {
                    Response::Error(e) => Some(String::from_utf8_lossy(e).to_string()),
                    _ => None,
                };
                self.state.abort_transaction(error_msg);
                return vec![cluster_error];
            }
            return vec![self.queue_command(cmd)];
        }

        // Validate arity BEFORE the pause check so that commands with wrong
        // argument counts return an immediate error even when paused (test:
        // syntax errors bypass pause).
        if let Some(entry) = self.core.registry.get_entry(cmd_name)
            && !entry.arity().check(cmd.args.len())
        {
            let err = Response::error(format!(
                "ERR wrong number of arguments for '{}' command",
                entry.name().to_ascii_lowercase()
            ));
            self.record_error_response(&err, true, cmd_name);
            return vec![err];
        }

        // Wait if server is paused (CLIENT PAUSE). This is checked AFTER transaction
        // queuing so commands inside MULTI are queued without blocking.
        self.wait_if_paused(cmd_name, &cmd.args).await;

        // Registry-union dispatch: a command registered as
        // `CommandImpl::Connection` (CONFIG, BGSAVE/LASTSAVE, HOTKEYS, FT.CURSOR,
        // SLOWLOG, MEMORY, LATENCY, STATUS) executes through its `ConnCtx`
        // executor, bypassing the legacy
        // router→handler path below. Every not-yet-migrated connection group
        // still routes through `connection_level_handler_for`; the two coexist
        // during the migration.
        if let Some(responses) = self.dispatch_connection_command(cmd_name, &cmd.args).await {
            return responses;
        }

        // Category-based dispatch using registry-driven handler lookup
        // This handles: pub/sub, scripting, functions, admin commands
        if let Some(handler) = self.connection_level_handler_for(cmd_name)
            && let Some(responses) = self
                .dispatch_connection_level(handler, cmd_name, &cmd.args)
                .await
        {
            return responses;
        }

        // Handle PSYNC command - validates args and returns handoff signal
        // The actual handoff happens in the run() loop when it detects PSYNC_HANDOFF
        if cmd_name == "PSYNC" {
            // Check if we have a primary replication handler (we're running as primary)
            if self.cluster.primary_replication_handler.is_none() {
                return vec![Response::error(
                    "ERR PSYNC not supported - server is not running as primary",
                )];
            }
            // Execute PSYNC command which will return PSYNC_HANDOFF signal
            return vec![self.route_and_execute(cmd, cmd_name).await];
        }

        // Handle WAIT at the connection level: it blocks on the replication
        // WaitCoordinator (offset snapshot, GETACK solicitation, quorum wait),
        // not on shard routing. Reached only outside MULTI — a queued WAIT
        // executes on the shard at EXEC time, where `WaitCommand::execute`
        // returns the acked count without blocking (Redis deny-blocking
        // semantics). Same dispatch shape as PSYNC above.
        if cmd_name == "WAIT" {
            return vec![self.handle_wait_command(&cmd.args).await];
        }

        // Server-wide commands (registry-driven: SCAN, KEYS, DBSIZE, RANDOMKEY, FLUSHDB, FLUSHALL, MIGRATE, SHUTDOWN)
        if let Some(handler) = self.server_wide_handler(cmd_name) {
            return vec![handler(self, &cmd.args).await];
        }

        // Route CLUSTER GETKEYSINSLOT / COUNTKEYSINSLOT to the correct shard.
        // These subcommands query a specific slot's keys, but the CLUSTER command
        // is keyless and would otherwise execute on the connection's assigned shard.
        // Since all keys for a given slot live on shard (slot % num_shards), we
        // must forward to that shard.
        if cmd_name == "CLUSTER" && !cmd.args.is_empty() {
            let sub = cmd.args[0].to_ascii_uppercase();
            if (sub.as_slice() == b"GETKEYSINSLOT" || sub.as_slice() == b"COUNTKEYSINSLOT")
                && cmd.args.len() >= 2
                && let Ok(slot_str) = std::str::from_utf8(&cmd.args[1])
                && let Ok(slot) = slot_str.parse::<u16>()
            {
                let target_shard = slot as usize % self.num_shards;
                return vec![self.execute_on_shard(target_shard, Arc::clone(cmd)).await];
            }
        }

        // Validate cluster slot ownership (returns CROSSSLOT/MOVED/ASK errors)
        if let Some(cluster_error) = self.validate_cluster_slots(cmd) {
            return vec![cluster_error];
        }

        // Check for TRYAGAIN during slot migration for multi-key commands
        if let Some(tryagain) = self.check_migrating_multikey(cmd).await {
            return vec![tryagain];
        }

        // Client tracking: compute whether this command's reads should be tracked
        self.pending_track_reads = self.state.should_track_read();

        // NO-TOUCH: check if this connection has NO_TOUCH flag set
        self.pending_no_touch = self
            .admin
            .client_registry
            .get(self.state.id)
            .map(|info| info.flags.contains(frogdb_core::ClientFlags::NO_TOUCH))
            .unwrap_or(false);

        // Normal execution
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

        // For MIGRATING slots: if the key doesn't exist locally (nil response),
        // convert to ASK redirect so the client retries on the importing target.
        if let Some(ask) = self.migrating_ask_for_nil(cmd, &response) {
            return vec![ask];
        }

        // Handle internal action signals (blocking, raft, migrate)
        let final_response = self.handle_internal_action(response).await;

        // Record failed call if execution returned an error
        self.record_error_response(&final_response, false, cmd_name);

        vec![final_response]
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
    use super::SERVER_WIDE_HANDLERS;
    use frogdb_core::{CommandRegistry, ExecutionStrategy};

    /// Every command registered with `ExecutionStrategy::ServerWide` must
    /// have a corresponding handler in SERVER_WIDE_HANDLERS, otherwise
    /// dispatch silently returns `None` at runtime.
    #[test]
    fn dispatch_table_covers_registry() {
        let mut registry = CommandRegistry::new();
        crate::register_commands(&mut registry);

        let missing: Vec<String> = registry
            .iter()
            .filter(|(_, entry)| {
                matches!(entry.execution_strategy(), ExecutionStrategy::ServerWide)
            })
            .map(|(name, _)| name.to_string())
            .filter(|name| !SERVER_WIDE_HANDLERS.contains_key(name.to_ascii_uppercase().as_str()))
            .collect();

        assert!(
            missing.is_empty(),
            "server-wide commands missing from SERVER_WIDE_HANDLERS: {:?}",
            missing
        );
    }

    /// Catch the inverse: a row in the table for a command that isn't
    /// registered (typo, deleted command, etc).
    #[test]
    fn dispatch_table_has_no_orphans() {
        let mut registry = CommandRegistry::new();
        crate::register_commands(&mut registry);

        let orphans: Vec<&str> = SERVER_WIDE_HANDLERS
            .keys()
            .filter(|name| {
                registry.get_entry(name).is_none_or(|entry| {
                    !matches!(entry.execution_strategy(), ExecutionStrategy::ServerWide)
                })
            })
            .copied()
            .collect();

        assert!(
            orphans.is_empty(),
            "SERVER_WIDE_HANDLERS rows that aren't registered as ServerWide: {:?}",
            orphans
        );
    }
}
