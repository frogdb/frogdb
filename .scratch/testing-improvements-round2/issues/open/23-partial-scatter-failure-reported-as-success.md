# Partial failure is reported as total success — scatter merges discard per-shard errors

Status: needs-triage
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: MASTER.md §2 T5
Score: aggregate of 3 findings
Area: frogdb-server / scatter · frogdb-commands / timeseries

## Context

Every multi-key command that fans out across shards merges the per-shard replies with a function
that cannot represent failure. A shard that errored contributes nothing to a sum, a `nil` to an
MGET, or is ignored outright — so the client sees an undercount, a missing key, or `+OK`, and
cannot distinguish partial application from success. Every existing merge test feeds only
successful shards.

This is **one piece of work, not N command fixes**: the merges are pure functions over
`HashMap<usize, HashMap<Bytes, Response>>`, and one table test per strategy with a failing shard
in the map closes the whole family plus the error-classification mapping beside it.

**Blocked on a semantics decision.** The test cannot assert anything until the *contract* is
chosen — MASTER.md §7 lists "scatter partial-failure contract *(03/F1)*" among the decisions
requiring a semantics call before their test can assert anything. The options and the proposal's
recommendation are reproduced below; that decision is tracked in issue 30,
`.scratch/testing-improvements-round2/issues/`.

## Evidence

- **Scatter merges discard per-shard errors.** *(03/F1)* `scatter/strategies.rs:153`
  `merge_sum_integers` —
  `.filter_map(|r| if let Response::Integer(n) = r { Some(*n) } else { None }).sum()` silently drops
  every non-integer (i.e. every error) reply. `scatter/strategies.rs:68` MGET —
  `.and_then(...).cloned().unwrap_or(Response::null())`. `scatter/strategies.rs:132-138`
  `MSetStrategy::merge` **ignores `shard_results` entirely** and returns `Response::ok()`.
  `scatter/executor.rs:139` only maps a *whole-scatter* `ScatterError`; a per-key error inside a
  successful shard's `PartialResult` flows straight into `merge`. `UnlinkStrategy::merge`
  (`strategies.rs:291`) is `untested`; `MSetStrategy` is one of only two `hot-but-shallow`
  functions in the entire workspace (exec 9232, 3 tests). Span-deduped re-check:
  `strategies.rs:153 merge_sum_integers` shows **2 tests, 10/10 regions** — full region coverage,
  and both tests feed all-success shards.
- **`scatter_error_to_response` is untested — every shard-failure reply shape is unverified.**
  *(03/F8)* `scatter/executor.rs:140` is `untested` (32 regions). All six arms have never executed:
  `ShardUnavailable` → `-ERR shard unavailable`, `LockFailed(VllError::ShardBusy)` →
  `-BUSY shard busy with continuation lock; retry`, `LockChannelClosed`/`LockTimeout` →
  `-ERR VLL lock acquisition failed`, `ResultChannelClosed` → `-ERR shard dropped VLL result`,
  `ResultTimeout` → `-ERR VLL execution timeout`. The error *code* drives client retry behaviour —
  `-BUSY` is retryable, `-ERR` is not. `scatter/executor.rs` overall is 67.7%.
- **`TS.MADD` partial-failure and auto-create semantics are 0% covered.** *(07/F17)*
  `commands/src/timeseries.rs` — the arity check at `:455-459` and **every** per-element error arm
  (`:468` bad timestamp, `:475` bad value, `:486` add error, `:489` WRONGTYPE, `:500` auto-create
  add error) are untested; both MADD tests (`redis-regression/tests/timeseries_regression.rs:310-332`
  and `server/tests/timeseries.rs:308-345`) pre-create every key and pass only valid triples, so
  the auto-create branch (`:491-503`) never runs.

## Options

Reproduced from 03/F1. The contract is the real decision, and the test must pin whichever is chosen.

- **(a) Fail loudly** — any shard error propagates as the reply. Consequence: matches the closest
  Redis analogue, since Redis's multi-key commands are single-node and therefore atomic, so "error
  means nothing happened"; costs clients the partial-success information.
- **(b) Partial with a distinguishable reply** — keep the sum but return `-ERR partial` when any
  shard errored. Consequence: the caller can tell partial from complete, but must handle a new
  reply shape and cannot tell *which* keys landed.
- **(c) Status quo, documented** — best-effort merge, recorded as a deliberate divergence.
  Consequence: no code change, but `MSET` keeps replying `+OK` for a write that partly failed and
  the divergence must be written into the compatibility docs.

**Proposal's recommendation:** (a) for `MSET`, (b) for the counting commands
(`DEL`/`EXISTS`/`TOUCH`/`UNLINK`), with the unit tests below pinning it.

## What to fix

1. Settle the contract (issue 30).
2. Table test per strategy: feed shard 0 = `{k1: Integer(1)}`, shard 1 = `{k2: Error("OOM …")}`,
   and assert the chosen contract for `DEL`, `MSET`, `MGET`, `EXISTS`, `TOUCH`, `UNLINK`.
3. Table test mapping each `ScatterError` variant to its exact reply, asserting the RESP error
   *prefix* (`BUSY` vs `ERR`) separately from the message so a reword cannot silently flip
   retryability.
4. One `TS.MADD` test mixing a valid triple, a bad timestamp, a WRONGTYPE key and a not-yet-existing
   key; assert the reply array pairs successes with errors positionally and that only the valid
   samples landed.

## Acceptance criteria

- [ ] The chosen contract is recorded in the issue and in a comment on `scatter/strategies.rs`.
- [ ] `DEL` over a map containing one `Response::Error` does **not** return `Integer(1)`; `MSET`
      does **not** return `+OK`; `MGET` does **not** encode a failed shard as `nil`. Fails today.
- [ ] Each of the six `ScatterError` arms has an assertion on its exact reply, with prefix asserted
      separately.
- [ ] `TS.MADD` mixed-batch test asserts positional pairing and that the auto-create branch
      (`timeseries.rs:491-503`) executes.

## Test boundary

**Level 1.** The merges and `scatter_error_to_response` are pure functions; a socket adds nothing.
This is the anti-pattern in reverse — today the only coverage comes from full server integration
runs that never inject a failing shard. `TS.MADD` is **level 3** because it needs real dispatch and
store state to prove which samples landed.

## Depends on

Issue 30, `.scratch/testing-improvements-round2/issues/` — the scatter partial-failure contract.
Nothing else; no infrastructure item is required.

## Re-triage 2026-08-06

**Verdict: still-valid**

Nothing in the hardening campaign touched the scatter path (scatter was not one of the four locked
areas). All three findings reproduce byte-for-byte, only the crate-internal path changed:
`connection/scatter/` → `crates/server/src/scatter/`. Per-claim:

- **Scatter merges discard per-shard errors — still valid.** `scatter/strategies.rs:153-165`
  `merge_sum_integers` still `filter_map`s away every non-`Integer` reply and sums; MGET's merge at
  `:55-67` still `.cloned().unwrap_or(Response::null())`; `MSetStrategy::merge` at `:132-139` still
  ignores `_shard_results` entirely and returns `Response::ok()`. `DelStrategy::merge` (:189-195)
  and `ExistsStrategy::merge` (:223-229) both delegate to `merge_sum_integers`.
  `scatter/executor.rs:138` still calls `strategy.merge(...)` with per-key errors already folded
  into `shard_results` (:125-129); only whole-scatter errors are mapped, at :122.
- **`scatter_error_to_response` untested — still valid.** `scatter/executor.rs:141-…`; all six arms
  present and unchanged, including the retryability-critical
  `BUSY shard busy with continuation lock; retry` vs `ERR VLL lock acquisition failed` split at
  :153-159. There are no `Response::error`-carrying fixtures anywhere in `strategies.rs`.
- **`TS.MADD` partial failure — still valid.** `crates/commands/src/timeseries.rs:456` (old ref
  `:455-459`, a one-line shift); the per-element error arms and the auto-create branch are
  unchanged and still exercised only by all-valid, pre-created-key tests.

Relationship to **issue 61**: 61 (`scatter-merges-discard-per-shard-errors`) was closed on
2026-08-06 as **superseded by this issue** and moved to `issues/done/`. This issue is the strict
superset (same 03/F1 evidence + 03/F8 + 07/F17) and **must stay open**. Still blocked on the
contract decision in issue 30, which is why `Status: needs-triage` is retained.
