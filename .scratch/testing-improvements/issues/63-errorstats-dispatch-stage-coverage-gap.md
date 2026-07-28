# errorstats/commandstats only cover 3 of the dispatch stages; unknown-command names grow cmdstat unbounded

Status: needs-triage
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

- [ ] `AUTH` failure increments `errorstat_ERR`/`total_error_replies` and creates a sane
      `cmdstat_auth` entry; `errorstats_auth_failure_is_untracked_gap` in `info_tcl.rs` is flipped
      from asserting the gap to asserting the fix (keep a comment noting the prior gap for history).
- [ ] A queue-time arity error inside `MULTI` and the resulting `EXECABORT` from `EXEC` are both
      recorded; `errorstats_multi_exec_errors_are_untracked_gap` flipped similarly.
- [ ] `EVALSHA` `NOSCRIPT` is recorded; `errorstats_evalsha_noscript_is_untracked_gap` flipped
      similarly (script-internal `redis.call`/`redis.pcall` accounting may be explicitly carved out
      as a separate follow-up if it requires deeper `scripting/executor.rs` surgery).
- [ ] Unknown-command errors record `rejected_calls`, not `failed_calls`;
      `errorstats_unknown_command_counts_as_err_but_not_rejected` flipped to assert the corrected
      split.
- [ ] OOM errors record `rejected_calls`, not `failed_calls` (or the split's intent is
      re-documented and the test updated to match a deliberate design decision, if moving the check
      earlier turns out to be impractical); `errorstats_oom_is_failed_not_rejected_call_divergence`
      flipped or its divergence explicitly re-affirmed as intentional.
- [ ] Distinct `cmdstat_*` entries created from unrecognized command names are bounded (capped,
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
- `.scratch/testing-improvements/issues/44-errorstats-e2e.md` (parent task)
