//! The EXEC algorithm.
//!
//! [`handle_exec`] is the single entry point: it runs [`execute_transaction`]
//! and records exactly one outcome metric from whatever comes back, so a new
//! early return cannot skip the metric or mislabel it.
//!
//! [`execute_transaction`] owns everything between "the connection handed us a
//! [`TxnSummary`]" and "here are the wire replies": the abort and rate-limit
//! gates, the EXEC-time slot re-validation, the pause barrier, the
//! deferred/shard partition, target resolution, the shard round-trip, and the
//! deferred-command merge. Every effect it needs goes through [`TxnHost`].

use bytes::Bytes;
use frogdb_core::{RateLimitExceeded, ServerWideOp, TransactionResult, WatchEntry};
use frogdb_protocol::{ParsedCommand, Response};
use tracing::debug;

use crate::host::{Deferral, ShardTxnReply, TxnHost};
use crate::state::{TransactionTarget, TxnSummary};

/// How a transaction ended. Every exit of [`execute_transaction`] names its
/// variant, and the single call site in [`handle_exec`] records the metrics from
/// the returned value — so a new early return cannot skip the metric or
/// mislabel it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionOutcome {
    /// A queuing error aborted the transaction (EXECABORT reply).
    ExecAbort,
    /// The batch exceeded the authenticated user's rate limit.
    RateLimited,
    /// Committed trivially: EXEC with an empty queue.
    CommittedEmpty,
    /// Queued keys spanned multiple slots/shards (CROSSSLOT reply).
    CrossSlot,
    /// EXEC-time slot re-validation refused the batch on this node: the reply
    /// is a bare `-MOVED` / `-ASK` / `-TRYAGAIN` / `-CROSSSLOT` /
    /// `-CLUSTERDOWN` and the queue was discarded.
    Redirected,
    /// The shard round-trip failed or the shard reported an execution error.
    Error,
    /// A watched key was modified; the transaction was not run (nil reply).
    WatchAborted,
    /// Executed; results returned to the client.
    Committed,
}

/// A queued command deferred until after the shard transaction completes,
/// remembered with the command name the deferred dispatch needs.
enum DeferredKind {
    /// Re-enters the connection dispatch machinery via
    /// [`TxnHost::run_connection_level`].
    ConnectionLevel { name: String },
    /// Fans out to all shards via [`TxnHost::run_server_wide`].
    ServerWide(ServerWideOp),
}

impl TransactionOutcome {
    /// The `outcome` label attached to the transaction metrics.
    ///
    /// The match is deliberately exhaustive (no wildcard arm): adding a new
    /// variant fails compilation until a label is chosen here, so every
    /// outcome always has exactly one metric string.
    pub fn metric_label(self) -> &'static str {
        match self {
            TransactionOutcome::ExecAbort => "execabort",
            TransactionOutcome::RateLimited => "ratelimited",
            TransactionOutcome::CrossSlot => "crossslot",
            TransactionOutcome::Redirected => "redirected",
            TransactionOutcome::Error => "error",
            TransactionOutcome::WatchAborted => "watch_aborted",
            TransactionOutcome::CommittedEmpty | TransactionOutcome::Committed => "committed",
        }
    }
}

/// Run a taken transaction and record its outcome metric.
///
/// This is the single metric-recording exit: whatever path
/// [`execute_transaction`] takes, exactly one outcome comes back and is
/// recorded here.
pub async fn handle_exec<H: TxnHost + ?Sized>(host: &mut H, summary: TxnSummary) -> Vec<Response> {
    let queued_count = summary.queue.len();
    let start_time = summary.start_time;

    let (outcome, responses) = execute_transaction(host, summary).await;
    record_transaction_metrics(
        host.metrics_recorder(),
        outcome.metric_label(),
        queued_count,
        start_time,
    );
    responses
}

/// Record one transaction's metric triple under `label`.
///
/// Shared by [`handle_exec`] (every EXEC outcome) and DISCARD, which has no
/// EXEC handler to run through and emits the `discarded` label directly — so
/// the metric *shape* still has exactly one definition.
pub fn record_transaction_metrics(
    recorder: &dyn frogdb_core::MetricsRecorder,
    label: &str,
    queued_count: usize,
    start_time: Option<std::time::Instant>,
) {
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
/// Each return names its [`TransactionOutcome`]; the caller records the metric.
pub async fn execute_transaction<H: TxnHost + ?Sized>(
    host: &mut H,
    summary: TxnSummary,
) -> (TransactionOutcome, Vec<Response>) {
    let TxnSummary {
        queue,
        watches,
        target,
        exec_abort,
        asking,
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
    if let Err(exceeded) = host.try_acquire_batch(&queue) {
        let msg = match exceeded {
            RateLimitExceeded::Commands => "ERR rate limit exceeded: commands per second",
            RateLimitExceeded::Bytes => "ERR rate limit exceeded: bytes per second",
        };
        return (TransactionOutcome::RateLimited, vec![Response::error(msg)]);
    }

    // Handle empty transaction
    if queue.is_empty() {
        return (
            TransactionOutcome::CommittedEmpty,
            vec![Response::Array(vec![])],
        );
    }

    // EXEC-time slot re-validation (cluster mode only).
    //
    // Placed here deliberately — after the empty-queue return and *before*
    // the pause wait and the shard round-trip:
    //
    // - Before the pause wait, so an EXEC destined for a redirect does not
    //   first block on `CLIENT PAUSE`.
    // - Before the shard round-trip, because that round-trip is
    //   all-or-nothing only with respect to *observers*, not to command
    //   failure: a command that errors mid-batch contributes no write meta
    //   and the loop continues, so the surviving subset would ship to
    //   replicas as one MULTI…EXEC frame. Any "abort mid-batch on MOVED"
    //   design would therefore replicate an orphan partial transaction.
    //   Validation has to be complete before a single command runs.
    //
    // The whole queue is validated against exactly one cluster snapshot; on
    // refusal the reply is the bare redirect (not an array, not EXECABORT)
    // and the queue is already gone, because taking the summary consumed it
    // — matching Redis's `discardTransaction` + `clusterRedirectClient`.
    //
    // If the pause below actually blocks, the verdict is re-taken after it
    // (see there): a pause is unbounded, and the topology may move while
    // EXEC sits in it.
    if let Some(redirect) = host.validate_queued_batch(&queue, asking).await {
        return (TransactionOutcome::Redirected, vec![redirect]);
    }

    // Wait if server is paused and this transaction contains write commands.
    // CLIENT PAUSE takes effect at the end of the current transaction, so
    // EXEC blocks until the pause ends if the queued commands include writes.
    let paused = if host.queue_has_writes(&queue) {
        host.wait_if_paused().await
    } else {
        false
    };

    // A pause is the one place EXEC can sit for unbounded wall-clock time,
    // and a slot can change hands while it sits — including because the
    // pause *is* a migration barrier. Re-validate against a fresh snapshot
    // before running anything. The check above is not redundant: it
    // fail-fasts a doomed EXEC instead of parking it behind the pause, and
    // it is what keeps the common (unpaused) path at exactly one snapshot.
    if paused && let Some(redirect) = host.validate_queued_batch(&queue, asking).await {
        return (TransactionOutcome::Redirected, vec![redirect]);
    }

    // Partition commands into shard-executable and deferred. Two strategy
    // groups cannot execute on the shard — their shard-side
    // `Command::execute()` is a placeholder (see [`Deferral`]):
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
        let kind = host.deferral_of(&name_str).map(|deferral| match deferral {
            Deferral::ConnectionLevel => DeferredKind::ConnectionLevel { name: name_str },
            Deferral::ServerWide(op) => DeferredKind::ServerWide(op),
        });
        match kind {
            Some(kind) => deferred.push((i, kind, cmd.args.clone())),
            None => shard_commands.push(cmd.clone()),
        }
    }

    // Get target shard. Taking the summary folds both queued-command keys and
    // every live watched shard into the transaction target at EXEC time, so
    // `None` here means there were neither keys nor watches to fold — fall
    // back to this connection's own shard. A watch set spanning shards
    // promotes the target to `Multi` and is CROSSSLOT-rejected below, so a
    // non-empty watch set never resolves to `None`.
    // A `Multi` target is a cross-slot transaction: resolve() returns the
    // CROSSSLOT reply from the redirect seam. `None` falls back to this
    // connection's shard; `Single` routes directly.
    let target_shard = match target.resolve() {
        Ok(TransactionTarget::None) => host.shard_id(),
        Ok(TransactionTarget::Single(shard)) => shard,
        Ok(TransactionTarget::Multi(_)) => unreachable!("resolve() maps Multi to Err"),
        Err(crossslot) => return (TransactionOutcome::CrossSlot, vec![crossslot]),
    };

    // Execute shard commands (may be empty if all commands are connection-level)
    let shard_results = if shard_commands.is_empty() {
        // No shard commands, but watches still need a shard round-trip
        // (with an empty command list) to be checked and cleared.
        if !watches.is_empty()
            && let Err((outcome, reply)) =
                run_shard_transaction(host, target_shard, vec![], watches).await
        {
            return (outcome, vec![reply]);
        }
        vec![]
    } else {
        match run_shard_transaction(host, target_shard, shard_commands, watches).await {
            Ok(results) => results,
            Err((outcome, reply)) => return (outcome, vec![reply]),
        }
    };

    // Merge shard results with deferred command results. Execute deferred
    // commands now (post-transaction, matching Redis semantics): connection
    // level commands re-enter the connection dispatch machinery, server-wide
    // commands fan out to all shards. Both sequences are ordered by original
    // queue index, so a single linear pass zips them back together — every
    // reply lands at its queued position.
    let mut final_results = Vec::with_capacity(queued_count);
    let mut deferred_pushes = Vec::new();
    let mut shard_results = shard_results.into_iter();
    let mut deferred = deferred.into_iter().peekable();

    for i in 0..queued_count {
        if deferred.peek().is_some_and(|(idx, ..)| *idx == i) {
            let (_, kind, args) = deferred.next().expect("peeked entry exists");
            match kind {
                DeferredKind::ConnectionLevel { name } => {
                    let (response, pushes) = host.run_connection_level(&name, &args).await;
                    final_results.push(response);
                    deferred_pushes.extend(pushes);
                }
                DeferredKind::ServerWide(op) => {
                    final_results.push(host.run_server_wide(op, &args).await);
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
        conn_id = host.conn_id(),
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

/// One shard round-trip for EXEC: hand the batch to the host and map every
/// reply arm onto an `(outcome, reply)` pair.
///
/// Both EXEC branches — the watch-only check (empty command list) and the
/// real execution — call this, so the mapping exists once.
async fn run_shard_transaction<H: TxnHost + ?Sized>(
    host: &mut H,
    target_shard: usize,
    commands: Vec<ParsedCommand>,
    watches: Vec<WatchEntry>,
) -> Result<Vec<Response>, (TransactionOutcome, Response)> {
    let conn_id = host.conn_id();
    match host
        .send_shard_transaction(target_shard, commands, watches)
        .await
    {
        ShardTxnReply::Replied(TransactionResult::Success(results)) => Ok(results),
        ShardTxnReply::Replied(TransactionResult::WatchAborted) => {
            debug!(conn_id, "Transaction aborted due to WATCH conflict");
            Err((TransactionOutcome::WatchAborted, Response::null()))
        }
        ShardTxnReply::Replied(TransactionResult::Error(e)) => {
            Err((TransactionOutcome::Error, Response::error(e)))
        }
        ShardTxnReply::Unavailable => Err((
            TransactionOutcome::Error,
            Response::error("ERR shard unavailable"),
        )),
        ShardTxnReply::Dropped => Err((
            TransactionOutcome::Error,
            Response::error("ERR shard dropped request"),
        )),
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
            (TransactionOutcome::Redirected, "redirected"),
            (TransactionOutcome::Error, "error"),
            (TransactionOutcome::WatchAborted, "watch_aborted"),
            (TransactionOutcome::Committed, "committed"),
        ];
        for (outcome, label) in cases {
            assert_eq!(outcome.metric_label(), label, "label for {outcome:?}");
        }
    }
}
