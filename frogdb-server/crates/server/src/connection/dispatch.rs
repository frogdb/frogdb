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
use crate::connection::router::ConnectionLevelHandler;

impl ConnectionHandler {
    /// Determine the connection-level handler for a command by looking up its
    /// execution strategy in the registry.
    ///
    /// Returns `Some(handler)` if the command declares a `ConnectionLevel` strategy,
    /// with the handler refined by command name (e.g., `Admin` + `CONFIG` → `Config`).
    /// Returns `None` if the command uses `Standard` or another non-connection-level strategy.
    pub(crate) fn connection_level_handler_for(
        &self,
        cmd_name: &str,
    ) -> Option<ConnectionLevelHandler> {
        let entry = self.registry.get_entry(cmd_name)?;
        match entry.execution_strategy() {
            ExecutionStrategy::ConnectionLevel(op) => Some(Self::refine_handler(&op, cmd_name)),
            _ => None,
        }
    }

    /// Refine a `ConnectionLevelOp` into a specific `ConnectionLevelHandler`
    /// based on the command name.
    fn refine_handler(op: &ConnectionLevelOp, cmd_name: &str) -> ConnectionLevelHandler {
        match op {
            ConnectionLevelOp::Admin => match cmd_name {
                "CLIENT" => ConnectionLevelHandler::Client,
                "CONFIG" => ConnectionLevelHandler::Config,
                "ACL" => ConnectionLevelHandler::Acl,
                "INFO" => ConnectionLevelHandler::Info,
                "DEBUG" => ConnectionLevelHandler::Debug,
                "SLOWLOG" => ConnectionLevelHandler::Slowlog,
                "MEMORY" => ConnectionLevelHandler::Memory,
                "LATENCY" => ConnectionLevelHandler::Latency,
                "STATUS" => ConnectionLevelHandler::Status,
                "MONITOR" => ConnectionLevelHandler::Monitor,
                _ => ConnectionLevelHandler::Client, // fallback
            },
            ConnectionLevelOp::Auth => match cmd_name {
                "HELLO" => ConnectionLevelHandler::Hello,
                _ => ConnectionLevelHandler::Auth,
            },
            ConnectionLevelOp::PubSub => match cmd_name {
                "SSUBSCRIBE" | "SUNSUBSCRIBE" | "SPUBLISH" => ConnectionLevelHandler::ShardedPubSub,
                _ => ConnectionLevelHandler::PubSub,
            },
            ConnectionLevelOp::Scripting => match cmd_name {
                "FCALL" | "FCALL_RO" | "FUNCTION" => ConnectionLevelHandler::Function,
                _ => ConnectionLevelHandler::Scripting,
            },
            ConnectionLevelOp::Transaction => ConnectionLevelHandler::Transaction,
            ConnectionLevelOp::ConnectionState => ConnectionLevelHandler::ConnectionState,
            ConnectionLevelOp::Replication => ConnectionLevelHandler::Replication,
            ConnectionLevelOp::Persistence => ConnectionLevelHandler::Persistence,
        }
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
            ConnectionLevelHandler::Acl => Some(vec![self.handle_acl_command(args).await]),

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
            ConnectionLevelHandler::Config => Some(vec![self.handle_config_command(args).await]),
            ConnectionLevelHandler::Info => Some(vec![self.handle_info(args).await]),
            ConnectionLevelHandler::Debug => self.dispatch_debug(args).await,
            ConnectionLevelHandler::Slowlog => Some(vec![self.handle_slowlog_command(args).await]),
            ConnectionLevelHandler::Memory => Some(vec![self.handle_memory_command(args).await]),
            ConnectionLevelHandler::Latency => Some(vec![self.handle_latency_command(args).await]),
            ConnectionLevelHandler::Status => Some(vec![self.handle_status_command(args).await]),
            ConnectionLevelHandler::Monitor => Some(vec![self.handle_monitor().await]),

            // Connection state handlers
            ConnectionLevelHandler::ConnectionState => {
                self.dispatch_connection_state(cmd_name, args).await
            }

            // Persistence handlers
            ConnectionLevelHandler::Persistence => self.dispatch_persistence(cmd_name, args).await,

            // Cluster handlers - fall through to standard routing
            ConnectionLevelHandler::Cluster => None,

            // Replication handlers - fall through to standard routing
            // PSYNC needs the full command for route_and_execute
            ConnectionLevelHandler::Replication => None,
        }
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
            "EXEC" => Some(vec![self.handle_exec().await]),
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
            b"SLEEP" => Some(vec![self.handle_debug_sleep(args).await]),
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
            // Dangerous commands — intentionally not supported
            b"SEGFAULT" | b"RELOAD" | b"CRASH-AND-RECOVER" | b"SET-ACTIVE-EXPIRE" | b"OOM"
            | b"PANIC" => Some(vec![Response::error(format!(
                "ERR DEBUG {} is not supported (unsafe command)",
                String::from_utf8_lossy(&subcommand)
            ))]),
            _ => Some(vec![Response::error(format!(
                "ERR Unknown DEBUG subcommand '{}'",
                String::from_utf8_lossy(&subcommand)
            ))]),
        }
    }

    /// Determine the server-wide operation for a command by looking up its
    /// execution strategy in the registry.
    fn server_wide_handler_for(&self, cmd_name: &str) -> Option<ServerWideOp> {
        let entry = self.registry.get_entry(cmd_name)?;
        match entry.execution_strategy() {
            ExecutionStrategy::ServerWide(op) => Some(op.clone()),
            _ => None,
        }
    }

    /// Dispatch a server-wide command to its handler.
    async fn dispatch_server_wide(&mut self, op: ServerWideOp, args: &[Bytes]) -> Vec<Response> {
        let response = match op {
            ServerWideOp::Scan => self.handle_scan(args).await,
            ServerWideOp::Keys => self.handle_keys(args).await,
            ServerWideOp::DbSize => self.handle_dbsize().await,
            ServerWideOp::RandomKey => self.handle_randomkey().await,
            ServerWideOp::FlushDb => self.handle_flushdb(args).await,
            ServerWideOp::FlushAll => self.handle_flushall(args).await,
            ServerWideOp::Migrate => self.handle_migrate(args).await,
            ServerWideOp::Shutdown => self.handle_shutdown(args).await,
            ServerWideOp::TsQueryIndex => self.handle_ts_queryindex(args).await,
            ServerWideOp::TsMget => self.handle_ts_mget(args).await,
            ServerWideOp::TsMrange => self.handle_ts_mrange(args, false).await,
            ServerWideOp::TsMrevrange => self.handle_ts_mrange(args, true).await,
            ServerWideOp::FtCreate => self.handle_ft_create(args).await,
            ServerWideOp::FtSearch => self.handle_ft_search(args).await,
            ServerWideOp::FtDropIndex => self.handle_ft_dropindex(args).await,
            ServerWideOp::FtInfo => self.handle_ft_info(args).await,
            ServerWideOp::FtList => self.handle_ft_list(args).await,
            ServerWideOp::FtAlter => self.handle_ft_alter(args).await,
            ServerWideOp::FtSynupdate => self.handle_ft_synupdate(args).await,
            ServerWideOp::FtSyndump => self.handle_ft_syndump(args).await,
            ServerWideOp::FtAggregate => self.handle_ft_aggregate(args).await,
        };
        vec![response]
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
            other => other,
        }
    }

    /// Dispatch persistence commands.
    async fn dispatch_persistence(
        &mut self,
        cmd_name: &str,
        args: &[Bytes],
    ) -> Option<Vec<Response>> {
        match cmd_name {
            "BGSAVE" => Some(vec![self.handle_bgsave(args)]),
            "LASTSAVE" => Some(vec![self.handle_lastsave()]),
            _ => None,
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
                self.state.asking = true;
                Some(vec![Response::ok()])
            }
            "READONLY" => {
                self.state.readonly = true;
                Some(vec![Response::ok()])
            }
            "READWRITE" => {
                self.state.readonly = false;
                Some(vec![Response::ok()])
            }
            // In pubsub mode, PING format depends on protocol version:
            // RESP2: array ["pong", <message>]
            // RESP3: simple string "PONG" or the message argument
            "PING" if self.state.pubsub.in_pubsub_mode() => {
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

        // Handle ACL command
        if cmd_name == "ACL" {
            return vec![self.handle_acl_command(&cmd.args).await];
        }

        // Run pre-execution checks (auth, admin port, ACL, pub/sub mode)
        if let Some(error_response) = self.run_pre_checks(cmd_name, &cmd.args) {
            return vec![error_response];
        }

        // In pub/sub mode, route allowed commands through the connection state handler.
        // PING is registered as a Standard (shard) command but needs special handling
        // in pub/sub mode to return array format ["pong", <message>].
        if self.state.pubsub.in_pubsub_mode()
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
        if self.state.transaction.queue.is_some() {
            // Validate cluster slot ownership before queuing — commands that would
            // get MOVED should fail immediately rather than succeeding at EXEC time.
            if let Some(cluster_error) = self.validate_cluster_slots(cmd) {
                self.state.transaction.exec_abort = true;
                if let Response::Error(ref e) = cluster_error {
                    self.state
                        .transaction
                        .queued_errors
                        .push(String::from_utf8_lossy(e).to_string());
                }
                return vec![cluster_error];
            }
            return vec![self.queue_command(cmd)];
        }

        // Wait if server is paused (CLIENT PAUSE). This is checked AFTER transaction
        // queuing so commands inside MULTI are queued without blocking.
        self.wait_if_paused(cmd_name).await;

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
            if self.primary_replication_handler.is_none() {
                return vec![Response::error(
                    "ERR PSYNC not supported - server is not running as primary",
                )];
            }
            // Execute PSYNC command which will return PSYNC_HANDOFF signal
            return vec![self.route_and_execute(cmd, cmd_name).await];
        }

        // Server-wide commands (registry-driven: SCAN, KEYS, DBSIZE, RANDOMKEY, FLUSHDB, FLUSHALL, MIGRATE, SHUTDOWN)
        if let Some(op) = self.server_wide_handler_for(cmd_name) {
            return self.dispatch_server_wide(op, &cmd.args).await;
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
        vec![self.handle_internal_action(response).await]
    }
}
