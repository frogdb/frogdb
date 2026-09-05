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
use frogdb_core::{
    RateLimitExceeded, ServerWideOp, TransactionResult, WatchEntry, WatchFence, WatchFenceRole,
};
use frogdb_protocol::{ParsedCommand, Response};
use tracing::debug;

use crate::host::{Deferral, ShardTxnReply, TxnHost};
use crate::state::{TransactionTarget, TxnSummary, WatchedKey};
use frogdb_core::clock;

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

/// How many times EXEC re-validates and re-sends a batch the shard refused
/// because the routing generation moved under it.
///
/// Bounded, not unbounded: a slot that keeps changing hands must eventually
/// answer the client rather than spin. Three is enough to absorb a single
/// handoff racing a single EXEC (the case the carry exists for) while still
/// terminating under a migration storm.
const ROUTING_ATTEMPTS: usize = 3;

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
            clock::elapsed(start).as_secs_f64(),
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
        charge,
    } = summary;
    let queued_count = queue.len();
    // The queued bytes stay charged against the home core's `TxnBuffering`
    // budget for as long as this frame holds the queue: every return below
    // drops the guard, so the release is exactly "EXEC has its reply"
    // (FM-TXN-054). Bound to a local rather than left inside `summary` so the
    // destructure above cannot silently drop it early.
    let _charge = charge;

    // Check if we should abort due to queuing errors
    if exec_abort {
        return (
            TransactionOutcome::ExecAbort,
            vec![Response::error(
                "EXECABORT Transaction discarded because of previous errors.",
            )],
        );
    }

    // Resolve the target shard *before* anything is spent on this batch.
    //
    // Taking the summary folds both queued-command keys and every *live*
    // watched shard into the transaction target at EXEC time, so `None` here
    // means there were no keys and no live watches to fold — fall back to this
    // connection's own shard. A set of live watches spanning shards promotes
    // the target to `Multi`, and `resolve()` maps that to the CROSSSLOT reply
    // from the redirect seam. `Single` routes directly.
    //
    // This is pure computation over already-parsed queued commands: no I/O, no
    // await, no host state touched. Running it ahead of the rate limiter is
    // what makes a CROSSSLOT-doomed EXEC cost nothing — it can never be served,
    // so charging tokens for it would let a client that cannot execute anything
    // exhaust the budget of clients that can. Same principle as FM-TXN-016's
    // no-charge-on-abort.
    //
    // The `CLIENT PAUSE` wait deliberately stays *below*: erroring out of a
    // pause window would change what a pause is (see TR-TXN-004), and a doomed
    // transaction waiting one out costs nothing but its own latency.
    let target_shard = match target.resolve() {
        Ok(TransactionTarget::None) => host.shard_id(),
        Ok(TransactionTarget::Single(shard)) => shard,
        Ok(TransactionTarget::Multi(_)) => unreachable!("resolve() maps Multi to Err"),
        Err(crossslot) => return (TransactionOutcome::CrossSlot, vec![crossslot]),
    };

    // Rate limit check for batch: consume N commands + total bytes
    if let Err(exceeded) = host.try_acquire_batch(&queue) {
        let msg = match exceeded {
            RateLimitExceeded::Commands => "ERR rate limit exceeded: commands per second",
            RateLimitExceeded::Bytes => "ERR rate limit exceeded: bytes per second",
        };
        return (TransactionOutcome::RateLimited, vec![Response::error(msg)]);
    }

    // Handle empty transaction — but only when it is also *unwatched*.
    //
    // An empty queue with a live watch set still has a CAS precondition to
    // check: `WATCH k`, another client writes `k`, `MULTI; EXEC` with nothing
    // queued. Returning `*0` here would commit a transaction whose watch was
    // already broken — the silent WATCH false negative FM-TXN-034 forbids.
    // Conjoining `watches.is_empty()` lets that case fall through to the
    // watch-only shard round-trip below, which version-checks and clears the
    // set exactly as an all-deferred queue's does. Redis reaches the same
    // answer from the other side: `execCommand` tests `CLIENT_DIRTY_CAS`
    // before it looks at the queue length.
    if queue.is_empty() && watches.is_empty() {
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

    // Wait if a pause covering this batch is in force and the transaction
    // contains write commands. CLIENT PAUSE takes effect at the end of the
    // current transaction, so EXEC blocks until the pause ends if the queued
    // commands include writes.
    //
    // The queue is handed to the host so a slot-scoped barrier parks only the
    // batches that can reach its slot — see [`TxnHost::wait_if_paused`]. The
    // decision itself stays on the host: which keys a command names and which
    // pauses are armed are both server facts, and pulling either in here would
    // put cluster and registry knowledge in the EXEC algorithm.
    let paused = if host.queue_has_writes(&queue) {
        host.wait_if_paused(&queue).await
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

    // The watch set is routed separately from the queue, and after it.
    //
    // Separately, because the two are not the same key set: a watch can name a
    // slot no queued command mentions (the queue may name no key at all), and
    // watch sets are not co-location-constrained the way a queue is — folding
    // them together would turn a legitimate two-slot watch into CROSSSLOT.
    //
    // After, because a batch that must run elsewhere gains nothing from being
    // told its CAS failed here first: the redirect is the more actionable
    // answer, and the client's retry will re-WATCH on the node it lands on.
    //
    // A watched slot that has left this node is not a redirect but a *failed
    // CAS*: nothing here can observe the new owner's writes to that key, so the
    // watch is by definition broken. The client's ordinary retry loop re-issues
    // WATCH, which now answers `-MOVED` and sends it to the owner.
    let watch_entries: Vec<WatchEntry> = watches.iter().map(|w| w.entry.clone()).collect();
    if !watch_entries.is_empty() && !host.watched_slots_still_local(&watch_entries, asking) {
        debug!(
            conn_id = host.conn_id(),
            "Transaction aborted: a watched key's slot is no longer served here"
        );
        return (TransactionOutcome::WatchAborted, vec![Response::null()]);
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

    // `target_shard` was resolved at the top, before the rate limiter — dead
    // watches never promote the target and may well sit on some other shard,
    // which is what the off-target round-trips below exist for.
    //
    // Route each watch's version check to the shard that can answer it.
    //
    // The target shard's watches ride with the batch, so its CAS and its
    // commands are one atomic shard step. A watch on any *other* shard is here
    // only because `take` declined to fold its shard, and `take` declines only
    // for a dead watch (`live_at_watch = false`): every live watched shard
    // folds, so a live watch elsewhere would already have CROSSSLOT-rejected at
    // `resolve()` above (FM-TXN-020).
    //
    // Those dead watches still have to be checked. A watched-nonexistent key
    // that another client *created* during the MULTI window breaks the CAS, and
    // only its own shard holds the slot version that says so — the target shard
    // never maintains a stamp for a slot it does not own, so asking it would
    // answer from an unrelated counter. Hence one extra watch-only round-trip
    // per off-target shard, taken before the batch so a broken CAS cannot leave
    // the target's commands committed behind it.
    //
    // Taking it before the batch is necessary but not sufficient: the probe and
    // the target's commit are two separate shard messages, and a write to an
    // off-target watched slot landing between them would be invisible to both.
    // So the probe does not answer a bare clean/dirty verdict — it answers with
    // a *generation handle* per watched key, and those handles ride on the
    // target's batch, which re-reads them inside its own commit step
    // (TR-TXN-028). The probe stays because it fails fast, on the shard that
    // can name the abort reason; the carried handles are what make the verdict
    // hold all the way to the commit.
    let (target_watches, off_target_watches) = partition_watches(watches, target_shard);
    let mut carried_fences = Vec::new();
    for (shard, entries) in off_target_watches {
        match run_shard_transaction(
            host,
            shard,
            vec![],
            entries,
            &queue,
            asking,
            WatchFenceRole::Mint,
        )
        .await
        {
            Ok((_, fences)) => carried_fences.extend(fences),
            Err((outcome, reply)) => return (outcome, vec![reply]),
        }
    }

    // Execute shard commands (may be empty if all commands are connection-level)
    let shard_results = if shard_commands.is_empty() {
        // No shard commands, but watches still need a shard round-trip
        // (with an empty command list) to be checked and cleared.
        if !target_watches.is_empty()
            && let Err((outcome, reply)) = run_shard_transaction(
                host,
                target_shard,
                vec![],
                target_watches,
                &queue,
                asking,
                WatchFenceRole::Verify(carried_fences),
            )
            .await
        {
            return (outcome, vec![reply]);
        }
        vec![]
    } else {
        match run_shard_transaction(
            host,
            target_shard,
            shard_commands,
            target_watches,
            &queue,
            asking,
            WatchFenceRole::Verify(carried_fences),
        )
        .await
        {
            Ok((results, _)) => results,
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
        .map(|s| clock::elapsed(s).as_millis() as u64)
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

/// Split the watch set into the target shard's entries and every other shard's,
/// one group per shard.
///
/// Groups are ordered by shard id, not by watch order: the watch set arrives
/// from a `HashMap`, so anything derived from its iteration order would make
/// the round-trip sequence vary run to run.
fn partition_watches(
    watches: Vec<WatchedKey>,
    target_shard: usize,
) -> (Vec<WatchEntry>, Vec<(usize, Vec<WatchEntry>)>) {
    let mut target = Vec::new();
    let mut off_target: Vec<(usize, Vec<WatchEntry>)> = Vec::new();
    for WatchedKey { shard_id, entry } in watches {
        if shard_id == target_shard {
            target.push(entry);
            continue;
        }
        match off_target.iter_mut().find(|(shard, _)| *shard == shard_id) {
            Some((_, entries)) => entries.push(entry),
            None => off_target.push((shard_id, vec![entry])),
        }
    }
    off_target.sort_by_key(|(shard, _)| *shard);
    (target, off_target)
}

/// What one shard round-trip settled, before the retry policy is applied.
enum ShardStep {
    /// The shard ran the batch.
    Results(Vec<Response>),
    /// A watch-only probe came back clean, with one generation handle per
    /// watched key for the target's commit to re-verify (TR-TXN-028).
    Fenced(Vec<WatchFence>),
    /// The shard refused *before running anything*: the routing generation the
    /// batch was validated against is no longer the live one.
    TopologyChanged,
    /// The round-trip is over, one way or another.
    Finished(TransactionOutcome, Response),
}

/// One shard round-trip for EXEC, retried while the routing generation keeps
/// moving under it, and mapped onto an `(outcome, reply)` pair.
///
/// Both EXEC branches — the watch-only check (empty command list) and the real
/// execution — call this, so the mapping exists once.
///
/// The retry is what makes the carried routing generation (`specs/txn.md`
/// TR-TXN-020) usable rather than merely safe. The shard refuses an apply whose
/// generation went stale, which closes the probe/apply window; without a retry
/// here every such refusal would surface as a spurious error to a client whose
/// batch is perfectly runnable one snapshot later. So: re-validate against the
/// fresh topology (which re-stamps the generation on the host), and re-send. A
/// re-validation that answers with a redirect is the real answer — the slot has
/// genuinely moved on — and ends the transaction.
///
/// `queue` and `asking` are the re-validation arguments; they are the whole
/// batch, not `commands`, because the queue's verdict is a whole-batch fact and
/// the deferred commands are part of it.
///
/// `role` decides what the trip is for (TR-TXN-028), so the success side is a
/// pair: the batch's per-command replies, and the generation handles a
/// [`WatchFenceRole::Mint`] probe answered with. Exactly one of the two is ever
/// non-empty, and neither caller has to name the other.
async fn run_shard_transaction<H: TxnHost + ?Sized>(
    host: &mut H,
    target_shard: usize,
    mut commands: Vec<ParsedCommand>,
    mut watches: Vec<WatchEntry>,
    queue: &[ParsedCommand],
    asking: bool,
    mut role: WatchFenceRole,
) -> Result<(Vec<Response>, Vec<WatchFence>), (TransactionOutcome, Response)> {
    for attempt in 1..=ROUTING_ATTEMPTS {
        // Sending consumes the batch, so keep a copy back while another attempt
        // is still allowed. The payload is `Bytes`, so the copy is a refcount
        // bump per argument.
        let held =
            (attempt < ROUTING_ATTEMPTS).then(|| (commands.clone(), watches.clone(), role.clone()));

        match shard_step(host, target_shard, commands, watches, role).await {
            ShardStep::Results(results) => return Ok((results, Vec::new())),
            ShardStep::Fenced(fences) => return Ok((Vec::new(), fences)),
            ShardStep::Finished(outcome, reply) => return Err((outcome, reply)),
            ShardStep::TopologyChanged => {
                let Some((held_commands, held_watches, held_role)) = held else {
                    break;
                };
                debug!(
                    conn_id = host.conn_id(),
                    attempt, "Shard refused the apply: routing generation moved; re-validating"
                );
                if let Some(redirect) = host.validate_queued_batch(queue, asking).await {
                    return Err((TransactionOutcome::Redirected, redirect));
                }
                commands = held_commands;
                watches = held_watches;
                role = held_role;
            }
        }
    }

    Err((
        TransactionOutcome::Redirected,
        host.routing_unsettled_reply(),
    ))
}

/// Hand the batch to the host once and classify what came back.
async fn shard_step<H: TxnHost + ?Sized>(
    host: &mut H,
    target_shard: usize,
    commands: Vec<ParsedCommand>,
    watches: Vec<WatchEntry>,
    role: WatchFenceRole,
) -> ShardStep {
    let conn_id = host.conn_id();
    match host
        .send_shard_transaction(target_shard, commands, watches, role)
        .await
    {
        ShardTxnReply::Replied(TransactionResult::Success(results)) => ShardStep::Results(results),
        ShardTxnReply::Replied(TransactionResult::WatchesFenced(fences)) => {
            ShardStep::Fenced(fences)
        }
        ShardTxnReply::Replied(TransactionResult::WatchAborted) => {
            debug!(conn_id, "Transaction aborted due to WATCH conflict");
            ShardStep::Finished(TransactionOutcome::WatchAborted, Response::null())
        }
        ShardTxnReply::Replied(TransactionResult::TopologyChanged) => ShardStep::TopologyChanged,
        ShardTxnReply::Replied(TransactionResult::Error(e)) => {
            ShardStep::Finished(TransactionOutcome::Error, Response::error(e))
        }
        ShardTxnReply::Unavailable => ShardStep::Finished(
            TransactionOutcome::Error,
            Response::error("ERR shard unavailable"),
        ),
        ShardTxnReply::Dropped => ShardStep::Finished(
            TransactionOutcome::Error,
            Response::error("ERR shard dropped request"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::TransactionOutcome;

    // FM-TXN-046
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
