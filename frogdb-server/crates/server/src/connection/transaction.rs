//! EXEC orchestration and transaction command queuing.
//!
//! The transaction command *state machine* (MULTI/DISCARD/WATCH/UNWATCH) is
//! migrated behind the [`ConnectionCommand`](crate::connection::conn_command::
//! ConnectionCommand) seam in
//! [`transaction_conn_command`](crate::connection::transaction_conn_command).
//! What stays here is the part that cannot be expressed against the narrow
//! `ConnCtx`:
//! - `handle_exec` — EXEC's orchestration: draining the queued commands over the
//!   owning shard(s) and running the deferred commands: connection-level ones
//!   (which re-enter the `ConnCtx` dispatch machinery — the meta-circularity)
//!   and server-wide ones (which fan out to all shards via
//!   `dispatch_server_wide`).
//!
//! `handle_exec` is implemented as an extension method on `ConnectionHandler`.
//! (Command *queuing* itself — `queue_command`, validating and queuing a command
//! while a MULTI is open — has moved to
//! [`PreDispatchView`](crate::connection::guards::PreDispatchView) in `guards.rs`
//! and no longer lives here.)

use bytes::Bytes;
use frogdb_core::{RateLimitExceeded, ServerWideOp, ShardMessage, TransactionResult, WatchEntry};
use frogdb_protocol::{ParsedCommand, Response};
use tokio::sync::oneshot;
use tracing::debug;

use crate::connection::ConnectionHandler;
use crate::connection::state::{TransactionTarget, TxnSummary};

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

/// A queued command that cannot execute on the shard (its shard-side
/// `Command::execute` is a placeholder) and is therefore deferred until after
/// the shard transaction completes. Deferred commands are NOT atomic with the
/// shard transaction — matching prior FrogDB convention (and Redis semantics,
/// where admin commands inside MULTI take effect at EXEC time).
enum DeferredKind {
    /// `ConnectionLevel(_)` strategy: re-enters the ConnCtx dispatch machinery
    /// via `execute_connection_level_in_transaction`.
    ConnectionLevel { name: String },
    /// `ServerWide(_)` strategy: fans out to all shards via
    /// [`ConnectionHandler::dispatch_server_wide`]. Running these on the single
    /// transaction shard would execute the placeholder (or a one-shard subset:
    /// partial KEYS, one-shard FLUSHDB, stub FT.* replies).
    ServerWide(ServerWideOp),
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
    /// abort/rate-limit gates, the deferred (connection-level + server-wide) /
    /// shard partition, target-shard resolution, the shard round-trip, and the
    /// deferred-command merge. Each
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

        // Partition commands into shard-executable and deferred. Two strategy
        // groups cannot execute on the shard — their shard-side
        // `Command::execute()` is a placeholder (see [`DeferredKind`]):
        // - Connection-level commands (CLIENT, CONFIG, INFO, etc.).
        // - Server-wide commands (SCAN, KEYS, FLUSHDB, FT.*, ...), which must
        //   fan out to all shards; a single-shard run would silently return
        //   partial results (or the stub reply).
        // Both are extracted and run after the shard transaction, matching
        // Redis semantics where admin commands inside MULTI take effect after
        // EXEC. They are NOT atomic with the shard transaction.
        let mut shard_commands = Vec::new();
        // (original_index, kind, args) for deferred commands
        let mut deferred: Vec<(usize, DeferredKind, Vec<Bytes>)> = Vec::new();

        for (i, cmd) in queue.iter().enumerate() {
            let name = cmd.name_uppercase();
            let name_str = String::from_utf8_lossy(&name).to_string();
            let kind = self.core.registry.get_entry(&name_str).and_then(|entry| {
                match entry.execution_strategy() {
                    frogdb_core::ExecutionStrategy::ConnectionLevel(_) => {
                        Some(DeferredKind::ConnectionLevel { name: name_str })
                    }
                    frogdb_core::ExecutionStrategy::ServerWide(op) => {
                        Some(DeferredKind::ServerWide(op))
                    }
                    _ => None,
                }
            });
            match kind {
                Some(kind) => deferred.push((i, kind, cmd.args.clone())),
                None => shard_commands.push(cmd.clone()),
            }
        }

        // Get target shard. `take_transaction` folds both queued-command keys and
        // every live watched shard into the transaction target at EXEC time, so
        // `None` here means there were neither keys nor watches to fold — fall
        // back to this connection's own shard. A watch set spanning shards
        // promotes the target to `Multi` and is CROSSSLOT-rejected below, so a
        // non-empty watch set never resolves to `None`.
        // A `Multi` target is a cross-slot transaction: resolve() returns the
        // CROSSSLOT reply from the redirect seam. `None` falls back to this
        // connection's shard; `Single` routes directly.
        let target_shard = match target.resolve() {
            Ok(TransactionTarget::None) => self.shard_id,
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

        // Merge shard results with deferred command results. Execute deferred
        // commands now (post-transaction, matching Redis semantics): connection
        // level commands re-enter the ConnCtx machinery, server-wide commands
        // fan out to all shards via `dispatch_server_wide`. Both sequences are
        // ordered by original queue index, so a single linear pass zips them
        // back together — every reply lands at its queued position.
        let mut final_results = Vec::with_capacity(queued_count);
        let mut deferred_pushes = Vec::new();
        let mut shard_results = shard_results.into_iter();
        let mut deferred = deferred.into_iter().peekable();

        for i in 0..queued_count {
            if deferred.peek().is_some_and(|(idx, ..)| *idx == i) {
                let (_, kind, args) = deferred.next().expect("peeked entry exists");
                match kind {
                    DeferredKind::ConnectionLevel { name } => {
                        let (response, pushes) = self
                            .execute_connection_level_in_transaction(&name, &args)
                            .await;
                        final_results.push(response);
                        deferred_pushes.extend(pushes);
                    }
                    DeferredKind::ServerWide(op) => {
                        final_results.push(self.dispatch_server_wide(op, &args).await);
                    }
                }
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
        watches: Vec<WatchEntry>,
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

    /// Execute a connection-level command that was deferred from a transaction.
    ///
    /// This dispatches directly through the command's `CommandImpl::Connection`
    /// executor (the registry union), rather than re-entering the main
    /// `route_and_execute_with_transaction` flow — which would create a recursive
    /// async cycle via `dispatch_transaction_command` -> `handle_exec`.
    ///
    /// Returns `(exec_slot_response, push_confirmations)`. The first element goes
    /// into the EXEC array; the second contains any out-of-band Push frames to
    /// send after the EXEC response (e.g., RESP3 subscribe/unsubscribe confirmations).
    async fn execute_connection_level_in_transaction(
        &mut self,
        cmd_name: &str,
        args: &[Bytes],
    ) -> (Response, Vec<Response>) {
        // Registry-union dispatch: every deferred connection-level command is a
        // migrated `CommandImpl::Connection` executor (CONFIG, BGSAVE/LASTSAVE,
        // CLIENT, DEBUG, MONITOR, ACL, INFO, HOTKEYS, FT.CURSOR, SLOWLOG, MEMORY,
        // LATENCY, STATUS, the pub/sub family, and the scripting family), so it
        // executes through its executor exactly as on the main dispatch path
        // (`dispatch_connection_command`). `as_connection()` yields a `'static`
        // reference, so it does not conflict with re-borrowing `self` to build
        // the `ConnCtx`.
        let migrated = self
            .core
            .registry
            .get_entry(cmd_name)
            .and_then(|entry| entry.as_connection());
        if let Some(command) = migrated {
            // The deferred connection command selects its dispatch shape from its
            // declared `mutation` capability, exactly as on the main path
            // (`dispatch_connection_command`) — never from its string name.
            return match command.spec().mutation {
                // Pub/sub deferred to EXEC: multi-response with bespoke MULTI
                // framing (PUBLISH/SPUBLISH single; SUBSCRIBE-family one
                // confirmation per channel; PUBSUB/SSUBSCRIBE/SUNSUBSCRIBE
                // rejected inside MULTI). This is a distinct framing path from the
                // main `execute_pubsub`. See `exec_pubsub_in_transaction`.
                frogdb_core::ConnMutation::PubSub => {
                    self.exec_pubsub_in_transaction(cmd_name, command, args)
                        .await
                }
                // MONITOR owns its `MonitorIo` at the call site; route it through
                // the dedicated builder so a deferred MONITOR wires
                // `ConnCtx::monitor` rather than hitting the read-only view (which
                // has `monitor = None`).
                frogdb_core::ConnMutation::Monitor => {
                    (self.execute_monitor(command, args).await, vec![])
                }
                // Read-only (CONFIG/…), AUTH-class, and CLIENT views build in
                // place from the same declared capability.
                mutation @ (frogdb_core::ConnMutation::None
                | frogdb_core::ConnMutation::Auth
                | frogdb_core::ConnMutation::Client) => (
                    command
                        .execute(&mut self.conn_ctx_for(mutation), args)
                        .await,
                    vec![],
                ),
            };
        }

        // The only connection-level command not registered as a
        // `CommandImpl::Connection` executor is PSYNC (`ConnectionLevel(
        // Replication)`, registered as a shard `Command`). It has no meaningful
        // behavior inside MULTI/EXEC and never had a real arm on the legacy path
        // either, so — like any other non-migrated deferred command — it replies
        // `+OK`.
        (Response::ok(), vec![])
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
