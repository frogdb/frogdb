//! EXEC orchestration and transaction command queuing.
//!
//! The transaction command *state machine* (MULTI/DISCARD/WATCH/UNWATCH) is
//! migrated behind the [`ConnectionCommand`](crate::connection::conn_command::
//! ConnectionCommand) seam in
//! [`transaction_conn_command`](crate::connection::transaction_conn_command).
//! What stays here is the part that cannot be expressed against the narrow
//! `ConnCtx`:
//! - `handle_exec` — EXEC's orchestration: draining the queued commands over the
//!   owning shard(s) and running the deferred connection-level commands (which
//!   re-enter the `ConnCtx` dispatch machinery — the meta-circularity).
//! - `queue_command` — validating and queuing a command while a MULTI is open.
//!
//! These are implemented as extension methods on `ConnectionHandler`.

use bytes::Bytes;
use frogdb_core::{RateLimitExceeded, ShardMessage, TransactionResult, shard_for_key};
use frogdb_protocol::{ParsedCommand, Response};
use tokio::sync::oneshot;
use tracing::debug;

use crate::connection::conn_command::ConnectionCommand;
use crate::connection::router::ConnectionLevelHandler;
use crate::connection::state::{TransactionTarget, TxnSummary};
use crate::connection::{ConnectionHandler, key_access_type_for_flags};

/// How a transaction ended. Every exit of [`ConnectionHandler::execute_transaction`]
/// names its variant, and the single call site in `handle_exec` records the
/// metrics from the returned value — so a new early return cannot skip the
/// metric or mislabel it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionOutcome {
    /// A queuing error aborted the transaction (EXECABORT reply).
    ExecAbort,
    /// The batch exceeded the authenticated user's rate limit.
    RateLimited,
    /// Committed trivially: EXEC with an empty queue.
    CommittedEmpty,
    /// Queued keys spanned multiple slots/shards (CROSSSLOT reply).
    CrossSlot,
    /// The shard round-trip failed or the shard reported an execution error.
    Error,
    /// A watched key was modified; the transaction was not run (nil reply).
    WatchAborted,
    /// Executed; results returned to the client.
    Committed,
}

impl TransactionOutcome {
    /// The `outcome` label attached to the transaction metrics.
    ///
    /// The match is deliberately exhaustive (no wildcard arm): adding a new
    /// variant fails compilation until a label is chosen here, so every
    /// outcome always has exactly one metric string.
    fn metric_label(self) -> &'static str {
        match self {
            TransactionOutcome::ExecAbort => "execabort",
            TransactionOutcome::RateLimited => "ratelimited",
            TransactionOutcome::CrossSlot => "crossslot",
            TransactionOutcome::Error => "error",
            TransactionOutcome::WatchAborted => "watch_aborted",
            TransactionOutcome::CommittedEmpty | TransactionOutcome::Committed => "committed",
        }
    }
}

impl ConnectionHandler {
    /// Handle EXEC command - execute the queued transaction.
    pub(crate) async fn handle_exec(&mut self) -> Vec<Response> {
        // Take the queue and watches atomically, leaving the transaction state
        // clean. EXEC's exit paths therefore never need to clear fields by hand.
        let summary = match self.state.take_transaction() {
            Some(summary) => summary,
            None => return vec![Response::error("ERR EXEC without MULTI")],
        };
        let queued_count = summary.queue.len();
        let start_time = summary.start_time;

        // The single metric-recording exit: whatever path execute_transaction
        // takes, exactly one outcome comes back and is recorded here.
        let (outcome, responses) = self.execute_transaction(summary).await;
        self.record_transaction_outcome(outcome, queued_count, start_time);
        responses
    }

    /// Record the transaction metrics for one EXEC outcome. (DISCARD records its
    /// own `discarded` metric in the DISCARD connection command, which has no
    /// handler to call this.)
    fn record_transaction_outcome(
        &self,
        outcome: TransactionOutcome,
        queued_count: usize,
        start_time: Option<std::time::Instant>,
    ) {
        let label = outcome.metric_label();
        let recorder = &*self.observability.metrics_recorder;
        frogdb_telemetry::definitions::TransactionsTotal::inc(recorder, label);
        frogdb_telemetry::definitions::TransactionsQueuedCommands::observe(
            recorder,
            queued_count as f64,
            label,
        );
        if let Some(start) = start_time {
            frogdb_telemetry::definitions::TransactionsDuration::observe(
                recorder,
                start.elapsed().as_secs_f64(),
                label,
            );
        }
    }

    /// Execute a taken transaction, returning how it ended plus the wire replies.
    ///
    /// This owns everything between `take_transaction` and metric recording:
    /// abort/rate-limit gates, the connection-level/shard partition, target-shard
    /// resolution, the shard round-trip, and the deferred-command merge. Each
    /// return names its [`TransactionOutcome`]; the caller records the metric.
    async fn execute_transaction(
        &mut self,
        summary: TxnSummary,
    ) -> (TransactionOutcome, Vec<Response>) {
        let TxnSummary {
            queue,
            watches,
            target,
            exec_abort,
            start_time,
        } = summary;
        let queued_count = queue.len();

        // Check if we should abort due to queuing errors
        if exec_abort {
            return (
                TransactionOutcome::ExecAbort,
                vec![Response::error(
                    "EXECABORT Transaction discarded because of previous errors.",
                )],
            );
        }

        // Rate limit check for batch: consume N commands + total bytes
        if !self.is_admin
            && let Some(user) = self.state.authenticated_user()
            && let Some(ref rl) = user.rate_limit
        {
            let total_bytes: u64 = queue
                .iter()
                .map(|c| crate::connection::estimate_command_size(c) as u64)
                .sum();
            if let Err(exceeded) = rl.try_acquire_batch(queued_count as u64, total_bytes) {
                let msg = match exceeded {
                    RateLimitExceeded::Commands => "ERR rate limit exceeded: commands per second",
                    RateLimitExceeded::Bytes => "ERR rate limit exceeded: bytes per second",
                };
                return (TransactionOutcome::RateLimited, vec![Response::error(msg)]);
            }
        }

        // Handle empty transaction
        if queue.is_empty() {
            return (
                TransactionOutcome::CommittedEmpty,
                vec![Response::Array(vec![])],
            );
        }

        // Wait if server is paused and this transaction contains write commands.
        // CLIENT PAUSE takes effect at the end of the current transaction, so
        // EXEC blocks until the pause ends if the queued commands include writes.
        if self.transaction_has_writes(&queue) {
            self.wait_if_paused_for_transaction().await;
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

        // Get target shard. If no command keys determined a target, fall back to
        // the watched-key shard (watches are all same-slot, so at most one shard).
        let watched_shard = watches
            .first()
            .map(|(key, _)| shard_for_key(key, self.num_shards));
        // A `Multi` target is a cross-slot transaction: resolve() returns the
        // CROSSSLOT reply from the redirect seam. `None` falls back to the
        // watched-key shard (or local); `Single` routes directly.
        let target_shard = match target.resolve() {
            Ok(TransactionTarget::None) => watched_shard.unwrap_or(self.shard_id),
            Ok(TransactionTarget::Single(shard)) => shard,
            Ok(TransactionTarget::Multi(_)) => unreachable!("resolve() maps Multi to Err"),
            Err(crossslot) => return (TransactionOutcome::CrossSlot, vec![crossslot]),
        };

        // Execute shard commands (may be empty if all commands are connection-level)
        let shard_results = if shard_commands.is_empty() {
            // No shard commands, but watches still need a shard round-trip
            // (with an empty command list) to be checked and cleared.
            if !watches.is_empty()
                && let Err((outcome, reply)) = self
                    .run_shard_transaction(target_shard, vec![], watches)
                    .await
            {
                return (outcome, vec![reply]);
            }
            vec![]
        } else {
            match self
                .run_shard_transaction(target_shard, shard_commands, watches)
                .await
            {
                Ok(results) => results,
                Err((outcome, reply)) => return (outcome, vec![reply]),
            }
        };

        // Merge shard results with deferred connection-level command results.
        // Execute deferred commands now (post-transaction, matching Redis
        // semantics). Both sequences are ordered by original queue index, so a
        // single linear pass zips them back together.
        let mut final_results = Vec::with_capacity(queued_count);
        let mut deferred_pushes = Vec::new();
        let mut shard_results = shard_results.into_iter();
        let mut deferred = deferred.into_iter().peekable();

        for i in 0..queued_count {
            if deferred.peek().is_some_and(|(idx, ..)| *idx == i) {
                let (_, name, args) = deferred.next().expect("peeked entry exists");
                let (response, pushes) = self
                    .execute_connection_level_in_transaction(&name, &args)
                    .await;
                final_results.push(response);
                deferred_pushes.extend(pushes);
            } else {
                final_results.push(
                    shard_results
                        .next()
                        .expect("one shard result per non-deferred queued command"),
                );
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

        // Return EXEC array followed by any deferred push confirmations
        // (e.g., RESP3 unsubscribe confirmations from pub/sub commands in MULTI).
        let mut result = vec![Response::Array(final_results)];
        result.extend(deferred_pushes);
        (TransactionOutcome::Committed, result)
    }

    /// One shard round-trip for EXEC: send `ExecTransaction`, await the reply,
    /// and map every `TransactionResult` arm onto an `(outcome, reply)` pair.
    ///
    /// Both EXEC branches — the watch-only check (empty command list) and the
    /// real execution — call this, so the send/await/match shape exists once.
    async fn run_shard_transaction(
        &mut self,
        target_shard: usize,
        commands: Vec<ParsedCommand>,
        watches: Vec<(Bytes, u64)>,
    ) -> Result<Vec<Response>, (TransactionOutcome, Response)> {
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::ExecTransaction {
            commands,
            watches,
            conn_id: self.state.id,
            protocol_version: self.state.protocol_version,
            response_tx,
        };

        if self.core.shard_senders[target_shard]
            .send(msg)
            .await
            .is_err()
        {
            return Err((
                TransactionOutcome::Error,
                Response::error("ERR shard unavailable"),
            ));
        }

        match response_rx.await {
            Ok(TransactionResult::Success(results)) => Ok(results),
            Ok(TransactionResult::WatchAborted) => {
                debug!(
                    conn_id = self.state.id,
                    "Transaction aborted due to WATCH conflict"
                );
                Err((TransactionOutcome::WatchAborted, Response::null()))
            }
            Ok(TransactionResult::Error(e)) => Err((TransactionOutcome::Error, Response::error(e))),
            Err(_) => Err((
                TransactionOutcome::Error,
                Response::error("ERR shard dropped request"),
            )),
        }
    }

    /// Queue a command during transaction mode.
    pub(crate) fn queue_command(&mut self, cmd: &ParsedCommand) -> Response {
        let cmd_name = cmd.name_uppercase();
        let cmd_name_str = String::from_utf8_lossy(&cmd_name);

        // Look up command for validation (get_entry covers both full and metadata-only
        // commands, so connection-level commands like PUBLISH/SPUBLISH can be queued).
        let entry = match self.core.registry.get_entry(&cmd_name_str) {
            Some(e) => e,
            None => {
                let msg = format!(
                    "ERR unknown command '{}', with args beginning with:",
                    cmd_name_str
                );
                self.state.abort_transaction(Some(msg.clone()));
                return Response::error(msg);
            }
        };

        // Validate arity
        if !entry.arity().check(cmd.args.len()) {
            let msg = format!(
                "ERR wrong number of arguments for '{}' command",
                entry.name()
            );
            self.state.abort_transaction(Some(msg.clone()));
            return Response::error(msg);
        }

        // Extract keys for same-slot validation
        let keys = entry.keys(&cmd.args);

        // Check key + channel permissions through the unified enforcement seam, so
        // queue-time denials are logged to ACL LOG exactly like the live paths.
        // (The command itself is already validated upstream by run_pre_checks.)
        if let Some(guard) = self.permission_guard() {
            if !keys.is_empty() {
                let access_type = key_access_type_for_flags(entry.flags());
                if let Err(err) = guard.check_keys(&keys, access_type) {
                    return err;
                }
            }
            match cmd_name_str.as_ref() {
                // First arg is the channel.
                "PUBLISH" | "SPUBLISH" => {
                    if let Some(channel) = cmd.args.first()
                        && let Err(err) = guard.check_channels(std::slice::from_ref(channel))
                    {
                        return err;
                    }
                }
                // All args are channels.
                "SUBSCRIBE" | "PSUBSCRIBE" | "SSUBSCRIBE" => {
                    if let Err(err) = guard.check_channels(&cmd.args) {
                        return err;
                    }
                }
                _ => {}
            }
        }

        // Fold this command's keys into the transaction target. In cluster mode
        // the accumulator uses slot-level detection (Redis requires all keys in
        // one slot); in standalone mode, shard-level detection.
        let is_cluster = self.cluster.cluster_state.is_some();
        self.state
            .fold_transaction_keys(&keys, self.num_shards, is_cluster);

        // Queue the command
        self.state.push_queued_command(cmd.clone());

        Response::Simple(Bytes::from_static(b"QUEUED"))
    }

    /// Execute a connection-level command that was deferred from a transaction.
    ///
    /// This directly dispatches to the appropriate handler without going through
    /// `dispatch_connection_level` (which would create a recursive async cycle
    /// via dispatch_transaction_command -> handle_exec).
    ///
    /// Returns `(exec_slot_response, push_confirmations)`. The first element goes
    /// into the EXEC array; the second contains any out-of-band Push frames to
    /// send after the EXEC response (e.g., RESP3 subscribe/unsubscribe confirmations).
    async fn execute_connection_level_in_transaction(
        &mut self,
        cmd_name: &str,
        args: &[Bytes],
    ) -> (Response, Vec<Response>) {
        // Registry-union dispatch: commands migrated behind the ConnCtx seam
        // (CONFIG, BGSAVE/LASTSAVE, HOTKEYS, FT.CURSOR, SLOWLOG, MEMORY, LATENCY,
        // STATUS) execute through their `CommandImpl::Connection` executor,
        // exactly as on the main dispatch path (`dispatch_connection_command`).
        // This must run before the legacy `handler_for` match below: HOTKEYS,
        // FT.CURSOR, SLOWLOG, MEMORY, LATENCY, and STATUS dropped their router
        // variants during migration and now fall back to `Client` in
        // `handler_for`, so routing them through the match would misdispatch them
        // to CLIENT. `as_connection()` yields a `'static` reference, so it does
        // not conflict with re-borrowing `self` to build the `ConnCtx`.
        let migrated = self
            .core
            .registry
            .get_entry(cmd_name)
            .and_then(|entry| entry.as_connection());
        if let Some(command) = migrated {
            // Pub/sub deferred to EXEC: multi-response with bespoke MULTI framing
            // (PUBLISH/SPUBLISH single; SUBSCRIBE-family one confirmation per
            // channel; PUBSUB/SSUBSCRIBE/SUNSUBSCRIBE rejected inside MULTI). See
            // `exec_pubsub_in_transaction`.
            if matches!(
                command.spec().strategy,
                frogdb_core::ExecutionStrategy::ConnectionLevel(
                    frogdb_core::ConnectionLevelOp::PubSub
                )
            ) {
                return self
                    .exec_pubsub_in_transaction(cmd_name, command, args)
                    .await;
            }
            // CLIENT mutates per-connection state and drives tracking IO, so it
            // dispatches through the mutable builder (`conn_ctx_clientmut`),
            // exactly as on the main path (`dispatch_connection_command`). Every
            // other migrated connection command is a pure read over `conn_ctx`.
            if cmd_name == "CLIENT" {
                return (
                    command.execute(&mut self.conn_ctx_clientmut(), args).await,
                    vec![],
                );
            }
            return (command.execute(&mut self.conn_ctx(), args).await, vec![]);
        }

        let handler = match self.connection_level_handler_for(cmd_name) {
            Some(h) => h,
            None => return (Response::ok(), vec![]),
        };
        let response = match handler {
            // CLIENT is a migrated Connection command caught by the `migrated`
            // block above, so this arm is unreachable — it stays only because
            // `Client` is the `handler_for` fallback for unmatched Admin ops,
            // keeping this match total.
            ConnectionLevelHandler::Client => Response::ok(),
            ConnectionLevelHandler::Config => {
                crate::connection::conn_command::ConfigConnCommand
                    .execute(&mut self.conn_ctx(), args)
                    .await
            }
            // Scripting/Function (EVAL/EVALSHA/SCRIPT, FCALL/FUNCTION) are
            // migrated Connection commands caught by the `migrated` block above,
            // so they no longer have arms here.
            ConnectionLevelHandler::Persistence => match cmd_name {
                "BGSAVE" => {
                    crate::connection::persistence_conn_command::BGSAVE_CONN_COMMAND
                        .execute(&mut self.conn_ctx(), args)
                        .await
                }
                "LASTSAVE" => {
                    crate::connection::persistence_conn_command::LASTSAVE_CONN_COMMAND
                        .execute(&mut self.conn_ctx(), args)
                        .await
                }
                _ => Response::ok(),
            },
            // Pub/sub (including sharded) is migrated behind the ConnCtx seam and
            // caught by the `migrated` block above via
            // `exec_pubsub_in_transaction`, so it no longer has arms here.
            ConnectionLevelHandler::Debug => match self.dispatch_debug(args).await {
                Some(responses) => responses.into_iter().next().unwrap_or_else(Response::ok),
                None => Response::ok(),
            },
            // These shouldn't appear in a transaction but handle gracefully
            _ => Response::ok(),
        };
        (response, vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::TransactionOutcome;

    /// Pins the outcome → metric-label mapping. The exhaustive match in
    /// `metric_label` already guarantees at compile time that every variant
    /// has a label; this test pins the exact strings, which are a dashboard /
    /// alerting contract.
    #[test]
    fn outcome_metric_labels_are_stable() {
        let cases = [
            (TransactionOutcome::ExecAbort, "execabort"),
            (TransactionOutcome::RateLimited, "ratelimited"),
            (TransactionOutcome::CommittedEmpty, "committed"),
            (TransactionOutcome::CrossSlot, "crossslot"),
            (TransactionOutcome::Error, "error"),
            (TransactionOutcome::WatchAborted, "watch_aborted"),
            (TransactionOutcome::Committed, "committed"),
        ];
        for (outcome, label) in cases {
            assert_eq!(outcome.metric_label(), label, "label for {outcome:?}");
        }
    }
}
