# Proposal: EXEC Outcome Executor

Status: implemented
Date: 2026-07-04

## Problem

`handle_exec` (`frogdb-server/crates/server/src/connection/handlers/transaction.rs`) was a
290-line monolith that braided five concerns — metric accounting, rate-limit gating, the
connection-level/shard partition, target-shard resolution, and the deferred-command merge — into
one function with **twelve** hand-maintained metric call sites.

The metric invariant was the dangerous part. Every EXEC must record exactly one
`frogdb_transactions_total` increment (plus the queued-commands and duration histograms) with the
correct `outcome` label. The old shape enforced that by convention: a local closure
`record_transaction_metrics(recorder, outcome_str, queued_count, start_time)` had to be invoked,
with the right string, on **every** return path:

| Exit path | Label | Call sites |
|---|---|---|
| Queuing error → EXECABORT reply | `"execabort"` | 1 |
| Batch over user rate limit | `"ratelimited"` | 1 |
| Empty queue → `Array([])` | `"committed"` | 1 |
| Cross-slot keys → CROSSSLOT reply | `"crossslot"` | 1 |
| Shard send/recv failure or shard error | `"error"` | **6** |
| Watched key modified → nil reply | `"watch_aborted"` | 2 |
| Executed | `"committed"` | 1 |

Nothing tied "return" to "record". A new early return — say, a future ACL or cluster-state gate —
could silently skip the metric, or copy-paste the wrong label, and no test or type would notice.
The strings themselves were spelled at each site, so the mapping "how did this EXEC end → what do
dashboards call it" had no single owner (`handle_discard` spelled a thirteenth site inline for
`"discarded"`). In module-design terms, the outcome vocabulary was **smeared** across every exit
instead of owned by one type.

Two more defects rode along:

1. **The shard round-trip existed twice.** EXEC talks to a shard in two cases — a watch-only check
   (all queued commands were connection-level, but watches must still be verified via an empty
   `ExecTransaction`) and the real execution. Both branches restated the same
   send → `response_rx.await` → four-arm `match TransactionResult` shape (~55 lines each), each arm
   carrying its own metric call. The reply contract (nil on `WatchAborted`, error text on
   `Error`/channel failure) lived in two copies that had already drifted cosmetically (the WATCH
   debug log existed only in the real branch).
2. **O(n²) result merge.** Re-zipping shard results with deferred connection-level results ran
   `deferred.iter().position(|(idx, ..)| *idx == i)` inside a `0..queued_count` loop — a linear
   scan per queued command — and cloned each shard result besides.

The function was also untestable below the integration level: every branch needed live
`shard_senders` oneshots and a real `MetricsRecorder`, so the outcome-string mapping — pure data —
could not be unit-tested.

## Current state (before)

`handle_exec` at `transaction.rs:37-326` (old numbering):

- `:39-57` — local `record_transaction_metrics` closure (recorder + string label + count + start).
- `:61-116` — take summary, EXECABORT gate, rate-limit gate, empty-queue fast path; three metric
  calls with three different string literals.
- `:130-165` — partition into `shard_commands`/`deferred`, resolve target shard; one `"crossslot"`
  call inside the `resolve()` match.
- `:168-224` — watch-only branch: inline send/await/match, four metric calls.
- `:225-283` — real branch: the same send/await/match again, four more metric calls.
- `:287-303` — quadratic merge loop.
- `:305-325` — debug log, final `"committed"` call, reply assembly.

`handle_discard` (`:329-355`) restated the three recorder calls a final time with `"discarded"`.

## Implemented design

Three moves, all inside `transaction.rs` — the wire behavior is unchanged.

### 1. `TransactionOutcome`: the outcome vocabulary as a type

```rust
enum TransactionOutcome {
    ExecAbort,       // EXECABORT reply
    RateLimited,     // batch over the user's rate limit
    CommittedEmpty,  // EXEC with an empty queue
    CrossSlot,       // CROSSSLOT reply
    Error,           // shard round-trip failed or shard reported an error
    WatchAborted,    // nil reply
    Committed,       // results returned
    Discarded,       // DISCARD dropped the queue
}

impl TransactionOutcome {
    fn metric_label(self) -> &'static str { /* exhaustive match, no wildcard */ }
}
```

`metric_label` is the **one** place mapping outcome → metric string. The match is deliberately
exhaustive: adding a variant fails compilation until a label is chosen, so "every outcome has
exactly one metric string" is now a compile-time property, not a convention (the structural-test
style of proposals 03/24). A unit test pins the exact strings, which are a dashboard/alerting
contract.

### 2. Single-exit recording: `handle_exec` = take → execute → record

The body moved into `execute_transaction(summary) -> (TransactionOutcome, Vec<Response>)`. Every
return **names its variant** next to its reply; none of them touches the recorder. `handle_exec`
shrank to the only metric-recording exit:

```rust
let summary = match self.state.take_transaction() { ... };   // ERR EXEC without MULTI (no metric, as before)
let (outcome, responses) = self.execute_transaction(summary).await;
self.record_transaction_outcome(outcome, queued_count, start_time);
responses
```

A new early return inside `execute_transaction` now *cannot* skip the metric — the return type
demands an outcome, and the single call site records it. Mislabeling requires naming the wrong
variant beside the reply it contradicts, which is visible at review time instead of buried in a
string argument. `record_transaction_outcome` (the three recorder calls, once) is shared by
`handle_discard`, deleting the thirteenth inline copy; `Discarded` lives in the same enum so the
whole label vocabulary has one home.

### 3. `run_shard_transaction`: the round-trip owned once

```rust
async fn run_shard_transaction(
    &mut self,
    target_shard: usize,
    commands: Vec<ParsedCommand>,   // empty = watch-only check
    watches: Vec<(Bytes, u64)>,
) -> Result<Vec<Response>, (TransactionOutcome, Response)>
```

The send/await/four-arm-match shape exists once; the `TransactionResult` arms map onto
`(outcome, reply)` pairs (`WatchAborted` → nil, `Error`/channel failures → error replies). Both
EXEC branches call it and just early-return the pair on `Err`. Six duplicated metric sites and
~55 duplicated lines collapse into the helper.

The merge loop became linear: `deferred` is built in queue order, so a `peekable()` iterator over
it zips against `shard_results.into_iter()` in one pass — no per-command scan, no clones.

This is the **deep-module** shape: `handle_exec`'s interface stayed identical (same signature, same
replies), but the invariant "exactly one correctly-labeled metric per EXEC" moved from twelve
conventions into one type + one seam. It passes the deletion test — deleting `metric_label` or the
recording call breaks compilation or every outcome at once, loudly, instead of silently dropping
one path's metrics. **Locality**: "how can an EXEC end, and what does each ending look like on the
wire and in Grafana?" is now answered by reading one enum and one function. **Leverage**: a future
outcome (e.g. a cluster-down gate) is one variant + one label + one `return`, and the compiler
walks you through it.

## Migration notes

Implemented in one pass over `transaction.rs` (the function was a single unit; there was no
incremental seam worth staging):

1. `TransactionOutcome` + `metric_label` + `record_transaction_outcome`; `handle_exec` split into
   take/execute/record; `handle_discard` converged onto the shared recorder path.
2. `run_shard_transaction` extracted; both branches converted; merge loop made linear.
3. Reply-shape regression tests added where coverage was thin (see below).

Behavior deltas, all metric/log-level (wire behavior byte-identical):

- The WATCH-conflict `debug!` log now fires on the watch-only branch too (it was only in the real
  branch before) — the drift resolved toward logging.
- `CommittedEmpty` and `Committed` share the `"committed"` label, exactly as before; the enum keeps
  them distinct so a future decision to split the label is one line.

## Testing impact

- **Unit-testable mapping.** `outcome_metric_labels_are_stable` (in `transaction.rs`) pins every
  variant's label string. The exhaustive `match` provides the compile-time guarantee; the test
  pins the exact strings against dashboard queries.
- **Reply shapes pinned.** Existing integration coverage
  (`crates/server/tests/integration_transactions.rs`, `crates/redis-regression/tests/multi_regression.rs`,
  `multi_tcl.rs`) already exercised: array-of-results, empty array, nil on watch abort, EXECABORT,
  per-command error propagation, and `ERR EXEC without MULTI`.
- **New regression tests** (`integration_transactions.rs`) for the paths whose coverage was thin:
  - `test_transaction_connection_level_merge_order` — deferred connection-level commands
    (CONFIG GET) interleaved first/middle/last with shard commands land at their original queue
    positions (pins the linear merge).
  - `test_watch_with_only_connection_level_commands_success` / `..._abort` — the watch-only shard
    round-trip branch: an all-deferred queue still checks watches, returning the merged results on
    success and nil on conflict.

## Risks / open questions

- **Metric cardinality is unchanged** — same label set, same series. Only the code shape moved.
- **The `expect` in the merge.** The linear merge asserts one shard result per non-deferred
  command (`shard_results.next().expect(...)`). The old code had the same failure mode as an
  index-out-of-bounds panic; the message is now explicit. A shard returning a short result vector
  would be a shard-protocol bug, not a connection-state bug.
- **`ERR EXEC without MULTI` records no metric**, before and after. If an outcome label is ever
  wanted for it, it is one variant away — but it is arguably a protocol error, not a transaction
  outcome.
- **`execute_transaction` is still long** (~130 lines): the rate-limit gate, pause wait, and
  partition could each become helpers. They are single-copy and linear now; further splitting is
  cosmetic and can ride along with any future change to those concerns.
