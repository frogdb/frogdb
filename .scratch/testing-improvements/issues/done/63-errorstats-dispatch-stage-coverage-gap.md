# errorstats/commandstats only cover 3 of the dispatch stages; unknown-command names grow cmdstat unbounded

Status: done
Type: bug
Origin: task 44 (INFO errorstats end-to-end coverage) — found while writing the integration tests
that drive real errors through the server; see `frogdb-server/crates/redis-regression/tests/info_tcl.rs`
and `frogdb-server/crates/server/tests/integration_info.rs`
Severity: likelihood 2/3, consequence 2/3 (score 4)
Area: basic commands / introspection (area A)

## Context

`record_error_response` (the only call site that increments `errorstat_<PREFIX>`, `cmdstat_*`
`rejected_calls`/`failed_calls`, and `total_error_replies`) is wired into exactly three of the
`DispatchStage` gauntlet's stages in `server/src/connection/dispatch.rs`: `PreChecks` and `Arity`
(both call it with `is_rejected = true`), and the terminal `Execute` stage (always
`is_rejected = false`). Every other stage — `PreAuthIntercept`, `TransactionControl`,
`TransactionQueue`, and `ConnectionCommand` — returns its error straight to the client without ever
calling `record_error_response`. This was empirically probed end-to-end while porting the upstream
Redis `errorstats` TCL suite (see task 44) and produces two distinct classes of finding:

### 1. Genuine observability gap: whole error classes are invisible to `INFO`

Pinned end-to-end by `errorstats_auth_failure_is_untracked_gap`,
`errorstats_multi_exec_errors_are_untracked_gap`, and `errorstats_evalsha_noscript_is_untracked_gap`
in `info_tcl.rs`:

- **AUTH failures** (`PreAuthIntercept` stage) — no `errorstat_ERR`, no `cmdstat_auth` entry, no
  `total_error_replies` increment.
- **MULTI/EXEC errors** (`TransactionQueue`/`TransactionControl` stages) — a queue-time arity error
  and the resulting `EXECABORT` from `EXEC` are both silently untracked.
- **EVALSHA `NOSCRIPT`** (`ConnectionCommand` stage, since scripting is a `ConnectionLevel`
  execution strategy) — untracked. (EVAL's own arity error *is* tracked correctly, since `Arity` is
  a universal pre-dispatch stage that runs ahead of `ConnectionCommand` for every registered command
  name — only the script-dispatch-specific errors are the gap.)
- A deeper, related but likely **out of scope for a straightforward fix**: commands invoked from
  inside a Lua script via `redis.call`/`redis.pcall` run through a separate, lower-level executor
  (`core/src/scripting/executor.rs`) that calls `CommandRegistry` directly and never touches
  `ClientRegistry` at all — there is no cmdstat/errorstat plumbing to record through even for
  *successful* script-invoked calls, let alone failing ones. Fixing this one likely needs the
  executor to gain a path back to `ClientRegistry`, unlike the other three gaps which just need
  their stage to call `record_error_response`.

### 2. Rejected-vs-failed misclassification (divergence from Redis, not an invisibility gap)

Pinned by `errorstats_unknown_command_counts_as_err_but_not_rejected` and
`errorstats_oom_is_failed_not_rejected_call_divergence` in `info_tcl.rs` — these errors *are*
recorded (matching Redis on the count itself), but land on the wrong side of the
rejected/failed split:

- **Unknown command.** Real Redis rejects before dispatch. FrogDB's unknown-command check lives
  inside `route_and_execute`, which runs as part of the terminal `Execute` stage, so it's recorded
  `is_rejected = false` (a failed call) instead of `rejected_calls`.
- **OOM.** Real Redis checks `maxmemory` before dispatch (a rejection). FrogDB's OOM check
  (`core/src/shard/eviction.rs`) runs during shard-side write execution, downstream of `Execute`'s
  routing, so it's also recorded as a failed call rather than rejected.

### 3. Unbounded cardinality growth vector (found incidentally while writing the unknown-command test)

The unknown-command path creates a `cmdstat_<name>` entry keyed **directly off the raw,
client-supplied command name** (see `errorstats_unknown_command_counts_as_err_but_not_rejected`,
which observes `cmdstat_asdfnotacommand` appear after one bogus command). Unlike `errorstat_*`,
which is capped at `MAX_ERROR_TYPES` (128, `client_registry/mod.rs:37-88`), there is no analogous
cap on distinct `cmdstat_*` entries. A client (or a stream of clients) repeatedly sending distinct
garbage command names grows this map without bound — an unbounded-memory-growth vector driven
entirely by untrusted client input, structurally different from (and with no existing guard
comparable to) the errorstats cap that was deliberately added for the same class of problem.

## What to build

Three independent fixes, each independently shippable:

1. **Close the untracked-error gap.** Add `record_error_response` calls (with the correct
   `is_rejected` value for each stage's semantics) to `PreAuthIntercept`, `TransactionControl`, and
   `TransactionQueue`. Decide and document whether the deeper `scripting/executor.rs` gap
   (script-internal `redis.call`/`redis.pcall` accounting) is in scope here or deserves its own
   follow-up given it likely needs a `ClientRegistry` handle threaded into the executor.
2. **Fix the rejected/failed split for OOM and unknown-command.** Either move both checks earlier
   (genuinely pre-dispatch, matching Redis's rejection semantics) or, if that's architecturally
   impractical, have `record_error_response`'s caller in `Execute` distinguish "never began real
   command execution" sub-cases from "began execution, failed partway" — whichever is truer to the
   `rejected_calls` contract's intent.
3. **Cap `cmdstat_*` cardinality for unrecognized command names.** Either don't create a per-name
   `cmdstat_*` entry for unknown commands at all (fold them into a single bucket, e.g. by only
   incrementing `errorstat_ERR`/`total_error_replies` and skipping the per-command stat), or apply a
   cap/eviction policy analogous to `MAX_ERROR_TYPES`.

## Acceptance criteria

- [x] `AUTH` failure increments `errorstat_ERR`/`total_error_replies` and creates a sane
      `cmdstat_auth` entry; `errorstats_auth_failure_is_untracked_gap` in `info_tcl.rs` is flipped
      from asserting the gap to asserting the fix (keep a comment noting the prior gap for history).
- [x] A queue-time arity error inside `MULTI` and the resulting `EXECABORT` from `EXEC` are both
      recorded; `errorstats_multi_exec_errors_are_untracked_gap` flipped similarly.
- [x] `EVALSHA` `NOSCRIPT` is recorded; `errorstats_evalsha_noscript_is_untracked_gap` flipped
      similarly (script-internal `redis.call`/`redis.pcall` accounting may be explicitly carved out
      as a separate follow-up if it requires deeper `scripting/executor.rs` surgery).
- [x] Unknown-command errors record `rejected_calls`, not `failed_calls`;
      `errorstats_unknown_command_counts_as_err_but_not_rejected` flipped to assert the corrected
      split.
- [x] OOM errors record `rejected_calls`, not `failed_calls` (or the split's intent is
      re-documented and the test updated to match a deliberate design decision, if moving the check
      earlier turns out to be impractical); `errorstats_oom_is_failed_not_rejected_call_divergence`
      flipped or its divergence explicitly re-affirmed as intentional.
- [x] Distinct `cmdstat_*` entries created from unrecognized command names are bounded (capped,
      bucketed, or suppressed) — add a test that sends N distinct garbage command names and asserts
      the `cmdstat_*` entry count doesn't grow past the chosen bound.

## Blocked by

None - can start immediately. Depends conceptually on task 44 (merged first; this issue's tests were
written against 44's new test infrastructure and existing-gap assertions).

## References

- `frogdb-server/crates/server/src/connection/dispatch.rs` (`DispatchStage` gauntlet; `PreChecks`,
  `Arity`, `PreAuthIntercept`, `TransactionControl`, `TransactionQueue`, `ConnectionCommand`,
  `Execute` stages)
- `server/src/dispatch.rs:614-622`, `server/src/connection.rs:375-379` (`record_error_response`
  call sites)
- `core/src/client_registry/mod.rs:37-88` (`MAX_ERROR_TYPES` cap on `errorstat_*`, for contrast
  with the uncapped `cmdstat_*` growth described above)
- `core/src/shard/eviction.rs` (OOM check, downstream of dispatch routing)
- `core/src/scripting/executor.rs` (script-internal `redis.call`/`redis.pcall` executor, bypasses
  `ClientRegistry` entirely)
- `frogdb-server/crates/redis-regression/tests/info_tcl.rs` (`errorstats_auth_failure_is_untracked_gap`,
  `errorstats_multi_exec_errors_are_untracked_gap`, `errorstats_evalsha_noscript_is_untracked_gap`,
  `errorstats_unknown_command_counts_as_err_but_not_rejected`,
  `errorstats_oom_is_failed_not_rejected_call_divergence`)
- `.scratch/testing-improvements/issues/44` (parent task)

## Resolution

### Fix 1 — untracked-error gap: centralized recording in the stage driver

Rather than sprinkling `record_error_response` calls into `PreAuthIntercept`,
`TransactionControl`, `TransactionQueue` and `ConnectionCommand` (which would leave the same
"a new stage forgets to record" bug latent for the next stage added), recording moved **into
the `run_stage` driver loop** in `frogdb-server/crates/server/src/connection/dispatch.rs`:
every `StageOutcome::ShortCircuit` response is passed through `record_error_response` exactly
once, whichever stage produced it. The three former inline call sites (`PreChecks`, `Arity`,
`Execute`) were removed, so the count stays at one increment per error reply. Coverage is now
by construction: all 16 pre-dispatch stages are recorded, including the cluster
`MOVED`/`ASK`/`TRYAGAIN` redirect stages that were previously silent as well.

The `is_rejected` value comes from a new exhaustive `DispatchStage::rejects_pre_execution()`
classifier (no `_ =>` arm — a new stage must state its disposition). Dispositions follow
Redis's `processCommand` split: refusals that happen before `call()` are rejections
(`PreChecks` NOAUTH/NOPERM/rate limits, `CommandLookup`, `PubSubPing`, `TransactionQueue`,
`ClusterSlotValidation`, `MigratingTryAgain`), while stages that terminate *into* a command
executor are failures (`PreAuthIntercept` AUTH failure, `TransactionControl` EXECABORT,
`ConnectionCommand` NOSCRIPT, `Execute`, …). The full 16-entry table is pinned as data by
`stage_error_disposition_is_the_guard_dispatch_split` in `dispatch.rs`, so any future change
to a disposition is a deliberate test edit.

### Fix 2 — rejected-vs-failed split

- **Unknown command** is now a genuine pre-dispatch rejection: the old `Arity` stage became
  `CommandLookup` and its guard (`guards.rs::command_lookup_check`) resolves the registry entry
  first, returning `ERR unknown command '…'` when the lookup misses and the arity error
  otherwise. Both are recorded as `rejected_calls`, matching Redis. `route_and_execute` keeps
  its own unknown-command branch as defense in depth. Stage ordering: `CommandLookup` sits
  after `PreChecks` (NOAUTH must win over unknown-command, as in Redis) and after
  `TransactionQueue` (so an unknown/bad-arity command inside `MULTI` is still rejected by
  `queue_command`, which aborts the transaction).
- **OOM** could not be hoisted: the `maxmemory` gate reads shard-local memory
  (`ShardWorker::check_memory_for_write`) and hoisting it into the connection hot path would
  mean a cross-shard read per command. Instead `record_error_response` classifies by error
  prefix via `PRE_EXECUTION_ERROR_PREFIXES = ["OOM"]` — the gate fires *before* the write
  executes, so "rejected" is the truthful disposition even though the check physically lives
  downstream of `Execute`. Documented inline at the constant.

### Fix 3 — cmdstat cardinality

Real Redis never creates a `commandstats` entry for a name that is not in its command table
(`lookupCommand` fails → `rejectCommandFormat`, and `c->cmd` is NULL so `call()` never runs).
FrogDB now matches: **no per-name `cmdstat_*` entry is created for an unrecognized command**,
while `errorstat_ERR` and `total_error_replies` still increment. This required gating two
separate sources, not just `record_error_response`:

1. `record_error_response` only touches per-command counters when
   `records_command_stats(cmd_name)` (i.e. the registry knows the name).
2. `LocalClientStats::record_command` now takes `Option<&str>` — totals always bump, the
   per-command sample only for known names — and the latency-histogram map
   (`observability.latency_histograms`, another uncapped DashMap keyed by client input) is
   likewise only fed for known names.

Pinned by `commandstats_unknown_command_names_do_not_grow_cmdstat_entries` (200 distinct
garbage names → no matching `cmdstat_*` entry, bounded total entry count, `errorstat_ERR` 200).

### Tests

All five gap-pinning tests in `frogdb-server/crates/redis-regression/tests/info_tcl.rs` were
flipped from asserting the gap to asserting the fix, renamed accordingly, and keep comments
noting the prior behavior:

| before | after |
| --- | --- |
| `errorstats_auth_failure_is_untracked_gap` | `errorstats_auth_failure_is_failed_call` |
| `errorstats_multi_exec_errors_are_untracked_gap` | `errorstats_multi_exec_errors_are_recorded` |
| `errorstats_evalsha_noscript_is_untracked_gap` | `errorstats_evalsha_noscript_is_failed_call` |
| `errorstats_unknown_command_counts_as_err_but_not_rejected` | `errorstats_unknown_command_is_rejected_call` |
| `errorstats_oom_is_failed_not_rejected_call_divergence` | `errorstats_oom_is_rejected_call` |

`frogdb-server/crates/server/tests/integration_info.rs` needed no change (its three errorstats
tests cover already-recorded paths). `website/src/content/docs/architecture/execution.md` was
corrected: the validation order is Parse → Auth → ACL → Lookup → Arity → Execute, and the stale
"arity is checked before auth" note is replaced with the `CommandLookup`/`rejected_calls`
explanation.

### Carved out as follow-ups

1. **Script-internal `redis.call`/`redis.pcall` accounting** (explicitly out of scope, per the
   issue's own note). `core/src/scripting/executor.rs` drives `CommandRegistry` directly and has
   no `ClientRegistry` handle, so neither successful nor failing script-invoked calls appear in
   `commandstats`/`errorstats`. Fixing it means threading a stats sink into the executor —
   a design decision of its own, not a call-site addition.
2. **Inner-command attribution inside `EXEC`.** Errors raised by individual queued commands
   during `EXEC` are attributed to `exec`, not to the inner command name. Redis attributes them
   to the inner command. Same shape of fix as (1): the transaction executor needs the stats
   path.
