//! Command dispatch and pipeline logic.
//!
//! This module handles routing parsed commands to their connection-level handlers
//! and executing the main command pipeline (transaction handling, pub/sub mode,
//! cluster validation, etc.).

use bytes::Bytes;
use frogdb_core::{ConnectionLevelOp, ExecutionStrategy};
use frogdb_protocol::Response;

use crate::connection::router::ConnectionLevelHandler;
use crate::connection::ConnectionHandler;

impl ConnectionHandler {
    /// Determine the connection-level handler for a command by looking up its
    /// execution strategy in the registry.
    ///
    /// Returns `Some(handler)` if the command declares a `ConnectionLevel` strategy,
    /// with the handler refined by command name (e.g., `Admin` + `CONFIG` → `Config`).
    /// Returns `None` if the command uses `Standard` or another non-connection-level strategy.
    fn connection_level_handler_for(&self, cmd_name: &str) -> Option<ConnectionLevelHandler> {
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

            // Connection state handlers
            ConnectionLevelHandler::ConnectionState => {
                self.dispatch_connection_state(cmd_name, args).await
            }

            // Cluster handlers - fall through to standard routing
            ConnectionLevelHandler::Cluster => None,

            // Replication handlers - fall through to standard routing
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
                if !args.is_empty() {
                    if let Err(err) = self.validate_channel_access(&args[..1]) {
                        return Some(vec![err]);
                    }
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
                if !args.is_empty() {
                    if let Err(err) = self.validate_channel_access(&args[..1]) {
                        return Some(vec![err]);
                    }
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
            "EVAL" => Some(vec![self.handle_eval(args).await]),
            "EVALSHA" => Some(vec![self.handle_evalsha(args).await]),
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
    async fn dispatch_debug(&mut self, args: &[Bytes]) -> Option<Vec<Response>> {
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
            b"VLL" => Some(vec![self.handle_debug_vll(args).await]),
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
            _ => None, // Fall through for other DEBUG subcommands
        }
    }

    /// Dispatch connection state commands.
    async fn dispatch_connection_state(
        &mut self,
        cmd_name: &str,
        _args: &[Bytes],
    ) -> Option<Vec<Response>> {
        match cmd_name {
            "RESET" => Some(vec![self.handle_reset().await]),
            // Note: SELECT, QUIT, PING, ECHO, COMMAND are handled via standard shard routing
            _ => None,
        }
    }

    /// Route and execute a command, handling transaction and pub/sub modes.
    /// Returns a Vec of responses since pub/sub commands can return multiple messages.
    pub(crate) async fn route_and_execute_with_transaction(
        &mut self,
        cmd: &frogdb_protocol::ParsedCommand,
    ) -> Vec<Response> {
        let cmd_name = cmd.name_uppercase();
        let cmd_name_str = String::from_utf8_lossy(&cmd_name);

        // Handle AUTH command (always allowed, even without authentication)
        if cmd_name_str == "AUTH" {
            return vec![self.handle_auth(&cmd.args).await];
        }

        // Handle HELLO command (always allowed, even without authentication)
        if cmd_name_str == "HELLO" {
            return vec![self.handle_hello(&cmd.args).await];
        }

        // Handle ACL command
        if cmd_name_str == "ACL" {
            return vec![self.handle_acl_command(&cmd.args).await];
        }

        // Run pre-execution checks (auth, admin port, ACL, pub/sub mode)
        if let Some(error_response) = self.run_pre_checks(&cmd_name_str, &cmd.args) {
            return vec![error_response];
        }

        // Category-based dispatch using registry-driven handler lookup
        // This handles: pub/sub, transactions, scripting, functions, admin commands
        if let Some(handler) = self.connection_level_handler_for(&cmd_name_str) {
            if let Some(responses) = self
                .dispatch_connection_level(handler, &cmd_name_str, &cmd.args)
                .await
            {
                return responses;
            }
        }

        // Handle persistence commands (need snapshot coordinator)
        if cmd_name_str == "BGSAVE" {
            return vec![self.handle_bgsave(&cmd.args)];
        }
        if cmd_name_str == "LASTSAVE" {
            return vec![self.handle_lastsave()];
        }

        // Handle PSYNC command - validates args and returns handoff signal
        // The actual handoff happens in the run() loop when it detects PSYNC_HANDOFF
        if cmd_name_str == "PSYNC" {
            // Check if we have a primary replication handler (we're running as primary)
            if self.primary_replication_handler.is_none() {
                return vec![Response::error(
                    "ERR PSYNC not supported - server is not running as primary",
                )];
            }
            // Execute PSYNC command which will return PSYNC_HANDOFF signal
            return vec![self.route_and_execute(cmd).await];
        }

        // Handle server commands that need scatter-gather routing
        match cmd_name_str.as_ref() {
            "SCAN" => return vec![self.handle_scan(&cmd.args).await],
            "KEYS" => return vec![self.handle_keys(&cmd.args).await],
            "DBSIZE" => return vec![self.handle_dbsize().await],
            "RANDOMKEY" => return vec![self.handle_randomkey().await],
            "FLUSHDB" => return vec![self.handle_flushdb(&cmd.args).await],
            "FLUSHALL" => return vec![self.handle_flushall(&cmd.args).await],
            "MIGRATE" => return vec![self.handle_migrate(&cmd.args).await],
            "SHUTDOWN" => return vec![self.handle_shutdown(&cmd.args).await],
            _ => {}
        }

        // If in transaction mode, queue the command instead of executing
        if self.state.transaction.queue.is_some() {
            // Check if it's a blocking command - not allowed in MULTI
            // Use execution_strategy() for type-safe blocking detection
            if self.is_blocking_command(&cmd_name_str) {
                return vec![Response::error(
                    "ERR Blocking commands are not allowed inside a transaction",
                )];
            }
            return vec![self.queue_command(cmd)];
        }

        // Handle ASKING command (sets connection flag)
        if cmd_name_str == "ASKING" {
            self.state.asking = true;
            return vec![Response::ok()];
        }

        // Handle READONLY command (sets connection flag)
        if cmd_name_str == "READONLY" {
            self.state.readonly = true;
            return vec![Response::ok()];
        }

        // Handle READWRITE command (clears readonly flag)
        if cmd_name_str == "READWRITE" {
            self.state.readonly = false;
            return vec![Response::ok()];
        }

        // Validate cluster slot ownership (returns CROSSSLOT/MOVED/ASK errors)
        if let Some(cluster_error) = self.validate_cluster_slots(cmd) {
            return vec![cluster_error];
        }

        // Normal execution
        let response = self.route_and_execute(cmd).await;

        // Check if this is a blocking command that needs to wait
        if let Response::BlockingNeeded { keys, timeout, op } = response {
            return vec![self.handle_blocking_wait(keys, timeout, op).await];
        }

        // Check if this is a Raft cluster command that needs async execution
        if let Response::RaftNeeded {
            op,
            register_node,
            unregister_node,
        } = response
        {
            return vec![
                self.handle_raft_command(op, register_node, unregister_node)
                    .await,
            ];
        }

        // Check if this is a MIGRATE command that needs async execution
        if let Response::MigrateNeeded { args } = response {
            return vec![self.handle_migrate_command(args).await];
        }

        vec![response]
    }
}
