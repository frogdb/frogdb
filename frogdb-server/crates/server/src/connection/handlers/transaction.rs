//! Transaction command handlers.
//!
//! This module handles transaction commands:
//! - MULTI - Start a transaction
//! - EXEC - Execute the queued transaction
//! - DISCARD - Abort the transaction
//! - WATCH - Watch keys for modifications
//! - UNWATCH - Forget all watched keys
//!
//! These handlers are implemented as extension methods on `ConnectionHandler`.

use bytes::Bytes;
use frogdb_core::{ShardMessage, TransactionResult, shard_for_key};
use frogdb_protocol::{ParsedCommand, Response};
use std::sync::Arc;
use tokio::sync::oneshot;
use tracing::debug;

use crate::connection::router::ConnectionLevelHandler;
use crate::connection::state::TransactionTarget;
use crate::connection::{
    ConnectionHandler, TransactionState, extract_subcommand, key_access_type_for_flags,
};

impl ConnectionHandler {
    /// Handle MULTI command - start a transaction.
    pub(crate) fn handle_multi(&mut self) -> Response {
        if self.state.transaction.queue.is_some() {
            return Response::error("ERR MULTI calls can not be nested");
        }

        debug!(conn_id = self.state.id, "Transaction started");
        self.state.transaction.queue = Some(Vec::new());
        self.state.transaction.target = TransactionTarget::None;
        self.state.transaction.exec_abort = false;
        self.state.transaction.queued_errors.clear();
        self.state.transaction.start_time = Some(std::time::Instant::now());

        Response::ok()
    }

    /// Handle EXEC command - execute the queued transaction.
    pub(crate) async fn handle_exec(&mut self) -> Response {
        // Capture start time for duration metric
        let start_time = self.state.transaction.start_time;

        // Helper to record transaction metrics
        let record_transaction_metrics =
            |recorder: &Arc<dyn frogdb_core::MetricsRecorder>,
             outcome: &str,
             queued_count: usize,
             start_time: Option<std::time::Instant>| {
                recorder.increment_counter("frogdb_transactions_total", 1, &[("outcome", outcome)]);
                recorder.record_histogram(
                    "frogdb_transactions_queued_commands",
                    queued_count as f64,
                    &[("outcome", outcome)],
                );
                if let Some(start) = start_time {
                    recorder.record_histogram(
                        "frogdb_transactions_duration_seconds",
                        start.elapsed().as_secs_f64(),
                        &[("outcome", outcome)],
                    );
                }
            };

        // Check if in transaction mode
        let queue = match self.state.transaction.queue.take() {
            Some(q) => q,
            None => return Response::error("ERR EXEC without MULTI"),
        };

        let queued_count = queue.len();

        // Get watches and clear them
        let watches: Vec<(Bytes, u64)> = self
            .state
            .transaction
            .watches
            .drain()
            .map(|(key, (_, ver))| (key, ver))
            .collect();

        // Check if we should abort due to queuing errors
        if self.state.transaction.exec_abort {
            record_transaction_metrics(
                &self.metrics_recorder,
                "execabort",
                queued_count,
                start_time,
            );
            self.clear_transaction_state();
            return Response::error("EXECABORT Transaction discarded because of previous errors.");
        }

        // Handle empty transaction
        if queue.is_empty() {
            record_transaction_metrics(&self.metrics_recorder, "committed", 0, start_time);
            self.clear_transaction_state();
            return Response::Array(vec![]);
        }

        // Partition commands into shard-executable and connection-level-deferred.
        // Connection-level commands (CLIENT, CONFIG, INFO, etc.) can't execute on
        // the shard — their Command::execute() is a placeholder. We extract them
        // and run them after the shard transaction, matching Redis semantics where
        // admin commands inside MULTI take effect after EXEC.
        let mut shard_commands = Vec::new();
        // (original_index, cmd_name, args) for deferred connection-level commands
        let mut deferred: Vec<(usize, String, Vec<Bytes>)> = Vec::new();

        for (i, cmd) in queue.iter().enumerate() {
            let name = cmd.name_uppercase();
            let name_str = String::from_utf8_lossy(&name).to_string();
            if self.connection_level_handler_for(&name_str).is_some() {
                deferred.push((i, name_str, cmd.args.clone()));
            } else {
                shard_commands.push(cmd.clone());
            }
        }

        // Get target shard
        let target_shard = match &self.state.transaction.target {
            TransactionTarget::None => {
                // No keys in any command - execute on local shard
                self.shard_id
            }
            TransactionTarget::Single(shard) => *shard,
            TransactionTarget::Multi(_) => {
                record_transaction_metrics(
                    &self.metrics_recorder,
                    "crossslot",
                    queued_count,
                    start_time,
                );
                self.clear_transaction_state();
                return Response::error("CROSSSLOT Keys in request don't hash to the same slot");
            }
        };

        // Clear transaction state before executing
        self.clear_transaction_state();

        // Execute shard commands (may be empty if all commands are connection-level)
        let shard_results = if shard_commands.is_empty() {
            // Check watches even with no shard commands
            if !watches.is_empty() {
                // Need the shard to check watches
                let (response_tx, response_rx) = oneshot::channel();
                let msg = ShardMessage::ExecTransaction {
                    commands: vec![],
                    watches,
                    conn_id: self.state.id,
                    protocol_version: self.state.protocol_version,
                    response_tx,
                };
                if self.shard_senders[target_shard].send(msg).await.is_err() {
                    record_transaction_metrics(&self.metrics_recorder, "error", queued_count, start_time);
                    return Response::error("ERR shard unavailable");
                }
                match response_rx.await {
                    Ok(TransactionResult::WatchAborted) => {
                        record_transaction_metrics(&self.metrics_recorder, "watch_aborted", queued_count, start_time);
                        return Response::null();
                    }
                    Ok(TransactionResult::Error(e)) => {
                        record_transaction_metrics(&self.metrics_recorder, "error", queued_count, start_time);
                        return Response::error(e);
                    }
                    Err(_) => {
                        record_transaction_metrics(&self.metrics_recorder, "error", queued_count, start_time);
                        return Response::error("ERR shard dropped request");
                    }
                    Ok(TransactionResult::Success(_)) => {}
                }
            }
            vec![]
        } else {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ExecTransaction {
                commands: shard_commands,
                watches,
                conn_id: self.state.id,
                protocol_version: self.state.protocol_version,
                response_tx,
            };

            if self.shard_senders[target_shard].send(msg).await.is_err() {
                record_transaction_metrics(&self.metrics_recorder, "error", queued_count, start_time);
                return Response::error("ERR shard unavailable");
            }

            match response_rx.await {
                Ok(TransactionResult::Success(results)) => results,
                Ok(TransactionResult::WatchAborted) => {
                    debug!(conn_id = self.state.id, "Transaction aborted due to WATCH conflict");
                    record_transaction_metrics(&self.metrics_recorder, "watch_aborted", queued_count, start_time);
                    return Response::null();
                }
                Ok(TransactionResult::Error(e)) => {
                    record_transaction_metrics(&self.metrics_recorder, "error", queued_count, start_time);
                    return Response::error(e);
                }
                Err(_) => {
                    record_transaction_metrics(&self.metrics_recorder, "error", queued_count, start_time);
                    return Response::error("ERR shard dropped request");
                }
            }
        };

        // Merge shard results with deferred connection-level command results.
        // Execute deferred commands now (post-transaction, matching Redis semantics).
        let mut final_results = Vec::with_capacity(queued_count);
        let mut shard_idx = 0;

        for i in 0..queued_count {
            if let Some(pos) = deferred.iter().position(|(idx, ..)| *idx == i) {
                let (_, ref name, ref args) = deferred[pos];
                let response =
                    self.execute_connection_level_in_transaction(name, args).await;
                final_results.push(response);
            } else {
                final_results.push(shard_results[shard_idx].clone());
                shard_idx += 1;
            }
        }

        let duration_ms = start_time
            .map(|s| s.elapsed().as_millis() as u64)
            .unwrap_or(0);
        debug!(
            conn_id = self.state.id,
            commands_count = queued_count,
            duration_ms,
            "Transaction executed"
        );
        record_transaction_metrics(
            &self.metrics_recorder,
            "committed",
            queued_count,
            start_time,
        );
        Response::Array(final_results)
    }

    /// Handle DISCARD command - abort the transaction.
    pub(crate) fn handle_discard(&mut self) -> Response {
        if self.state.transaction.queue.is_none() {
            return Response::error("ERR DISCARD without MULTI");
        }

        // Record transaction metrics
        let queued_count = self.state.transaction.queue.as_ref().map_or(0, |q| q.len());
        self.metrics_recorder.increment_counter(
            "frogdb_transactions_total",
            1,
            &[("outcome", "discarded")],
        );
        self.metrics_recorder.record_histogram(
            "frogdb_transactions_queued_commands",
            queued_count as f64,
            &[("outcome", "discarded")],
        );
        if let Some(start) = self.state.transaction.start_time {
            self.metrics_recorder.record_histogram(
                "frogdb_transactions_duration_seconds",
                start.elapsed().as_secs_f64(),
                &[("outcome", "discarded")],
            );
        }

        // Clear all transaction state including watches (Redis behavior)
        self.state.transaction = TransactionState::default();
        Response::ok()
    }

    /// Handle WATCH command - watch keys for modifications.
    pub(crate) async fn handle_watch(&mut self, args: &[Bytes]) -> Response {
        // WATCH is not allowed inside MULTI
        if self.state.transaction.queue.is_some() {
            return Response::error("ERR WATCH inside MULTI is not allowed");
        }

        // Validate arity
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'watch' command");
        }

        // Check same-slot requirement for watched keys
        let mut target_shard: Option<usize> = None;
        for key in args {
            let shard = shard_for_key(key, self.num_shards);
            match target_shard {
                None => target_shard = Some(shard),
                Some(s) if s != shard => {
                    return Response::error(
                        "CROSSSLOT Keys in request don't hash to the same slot",
                    );
                }
                _ => {}
            }
        }

        let shard = target_shard.unwrap();

        // Get version from the shard
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::GetVersion { response_tx };

        if self.shard_senders[shard].send(msg).await.is_err() {
            return Response::error("ERR shard unavailable");
        }

        let version = match response_rx.await {
            Ok(v) => v,
            Err(_) => return Response::error("ERR shard dropped request"),
        };

        // Store watched keys with their versions
        for key in args {
            self.state
                .transaction
                .watches
                .insert(key.clone(), (shard, version));
        }

        // Update transaction target based on watched keys
        self.update_target(shard);

        Response::ok()
    }

    /// Handle UNWATCH command - forget all watched keys.
    pub(crate) fn handle_unwatch(&mut self) -> Response {
        self.state.transaction.watches.clear();
        Response::ok()
    }

    /// Queue a command during transaction mode.
    pub(crate) fn queue_command(&mut self, cmd: &ParsedCommand) -> Response {
        let cmd_name = cmd.name_uppercase();
        let cmd_name_str = String::from_utf8_lossy(&cmd_name);

        // Look up command for validation
        let handler = match self.registry.get(&cmd_name_str) {
            Some(h) => h,
            None => {
                self.state.transaction.exec_abort = true;
                self.state.transaction.queued_errors.push(format!(
                    "ERR unknown command '{}', with args beginning with:",
                    cmd_name_str
                ));
                return Response::error(format!(
                    "ERR unknown command '{}', with args beginning with:",
                    cmd_name_str
                ));
            }
        };

        // Validate arity
        if !handler.arity().check(cmd.args.len()) {
            self.state.transaction.exec_abort = true;
            self.state.transaction.queued_errors.push(format!(
                "ERR wrong number of arguments for '{}' command",
                handler.name()
            ));
            return Response::error(format!(
                "ERR wrong number of arguments for '{}' command",
                handler.name()
            ));
        }

        // Extract keys for same-slot validation
        let keys = handler.keys(&cmd.args);

        // Check key permissions with command context
        // For selectors to work correctly, we must check that BOTH the command
        // AND the key are allowed within the same permission context
        if let Some(user) = self.state.auth.user()
            && !keys.is_empty()
        {
            let access_type = key_access_type_for_flags(handler.flags());
            let cmd_name = handler.name();
            let subcommand = extract_subcommand(cmd_name, &cmd.args);
            for key in &keys {
                if !user.check_command_with_key(cmd_name, subcommand.as_deref(), key, access_type) {
                    return Response::error(
                        "NOPERM this user has no permissions to access one of the keys used as arguments",
                    );
                }
            }
        }

        // Check same-slot requirement
        for key in &keys {
            let shard = shard_for_key(key, self.num_shards);
            self.update_target(shard);

            // Check if we've crossed into multi-shard territory
            if let TransactionTarget::Multi(_) = &self.state.transaction.target {
                // Don't abort immediately, just mark for error at EXEC time
                // This allows continuing to queue commands (Redis behavior)
            }
        }

        // Queue the command
        if let Some(ref mut queue) = self.state.transaction.queue {
            queue.push(cmd.clone());
        }

        Response::Simple(Bytes::from_static(b"QUEUED"))
    }

    /// Update the transaction target shard.
    pub(crate) fn update_target(&mut self, shard_id: usize) {
        self.state.transaction.target = match &self.state.transaction.target {
            TransactionTarget::None => TransactionTarget::Single(shard_id),
            TransactionTarget::Single(s) if *s == shard_id => TransactionTarget::Single(shard_id),
            TransactionTarget::Single(s) => TransactionTarget::Multi(vec![*s, shard_id]),
            TransactionTarget::Multi(shards) => {
                let mut shards = shards.clone();
                if !shards.contains(&shard_id) {
                    shards.push(shard_id);
                }
                TransactionTarget::Multi(shards)
            }
        };
    }

    /// Clear transaction state.
    pub(crate) fn clear_transaction_state(&mut self) {
        self.state.transaction.queue = None;
        self.state.transaction.target = TransactionTarget::None;
        self.state.transaction.exec_abort = false;
        self.state.transaction.queued_errors.clear();
        // Note: watches are cleared separately, not here (they're consumed by EXEC)
    }

    /// Execute a connection-level command that was deferred from a transaction.
    ///
    /// This directly dispatches to the appropriate handler without going through
    /// `dispatch_connection_level` (which would create a recursive async cycle
    /// via dispatch_transaction → handle_exec).
    async fn execute_connection_level_in_transaction(
        &mut self,
        cmd_name: &str,
        args: &[Bytes],
    ) -> Response {
        let handler = match self.connection_level_handler_for(cmd_name) {
            Some(h) => h,
            None => return Response::ok(),
        };
        match handler {
            ConnectionLevelHandler::Client => self.handle_client_command(args).await,
            ConnectionLevelHandler::Config => self.handle_config_command(args).await,
            ConnectionLevelHandler::Info => self.handle_info(args).await,
            ConnectionLevelHandler::Slowlog => self.handle_slowlog_command(args).await,
            ConnectionLevelHandler::Memory => self.handle_memory_command(args).await,
            ConnectionLevelHandler::Latency => self.handle_latency_command(args).await,
            ConnectionLevelHandler::Status => self.handle_status_command(args).await,
            ConnectionLevelHandler::Scripting => {
                match cmd_name {
                    "EVAL" => self.handle_eval(args, false).await,
                    "EVAL_RO" => self.handle_eval(args, true).await,
                    "EVALSHA" => self.handle_evalsha(args, false).await,
                    "EVALSHA_RO" => self.handle_evalsha(args, true).await,
                    "SCRIPT" => self.handle_script(args).await,
                    _ => Response::ok(),
                }
            }
            ConnectionLevelHandler::Function => {
                match cmd_name {
                    "FCALL" => self.handle_fcall(args, false).await,
                    "FCALL_RO" => self.handle_fcall(args, true).await,
                    "FUNCTION" => self.handle_function(args).await,
                    _ => Response::ok(),
                }
            }
            ConnectionLevelHandler::Persistence => {
                match cmd_name {
                    "BGSAVE" => self.handle_bgsave(args),
                    "LASTSAVE" => self.handle_lastsave(),
                    _ => Response::ok(),
                }
            }
            ConnectionLevelHandler::PubSub => {
                match cmd_name {
                    "PUBLISH" => self.handle_publish(args).await,
                    _ => Response::error("ERR command not supported inside MULTI"),
                }
            }
            ConnectionLevelHandler::ShardedPubSub => {
                match cmd_name {
                    "SPUBLISH" => self.handle_spublish(args).await,
                    _ => Response::error("ERR command not supported inside MULTI"),
                }
            }
            ConnectionLevelHandler::Debug => {
                match self.dispatch_debug(args).await {
                    Some(responses) => {
                        responses.into_iter().next().unwrap_or_else(Response::ok)
                    }
                    None => Response::ok(),
                }
            }
            // These shouldn't appear in a transaction but handle gracefully
            _ => Response::ok(),
        }
    }
}
