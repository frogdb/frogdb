# `ScriptingMsg::FunctionCall` bypasses the VLL continuation gate

Status: ready-for-agent
Type: bug (isolation)
Severity: likelihood 2/3 (any FCALL concurrent with a cross-shard script), consequence 3/3
(arbitrary Lua mutates shards a cross-shard script believes it holds exclusively) — score 6
Area: core / shard dispatch, scripting

## Problem

`ShardWorker::can_execute_during_lock` (`frogdb-server/crates/core/src/shard/worker.rs:844`)
rejects work from a connection other than the continuation-lock owner. It has five call sites:
`dispatch_core.rs:20`, `dispatch_core.rs:49`, `dispatch_scripting.rs:17`, `:42`, `:104`.

`ScriptingMsg::FunctionCall` (`frogdb-server/crates/core/src/shard/dispatch_scripting.rs:77`) is
not one of them. It runs arbitrary Lua through `functions.rs:11` with no gate, while its
`EvalScript` siblings in the same file gate at `:17` and `:42`.

Same defect shape as round-2 issue 50 (`CoreMsg::ExecTransaction`, `dispatch_core.rs:95-115`),
found in the same sweep. Fix them together: a cross-shard script holding the continuation lock
has no isolation against either an EXEC or an FCALL from another connection.

Two further arms need a documented disposition rather than a fix:

- `CoreMsg::GetVersion` (`dispatch_core.rs:56`) is **not** read-only — it calls `purge_if_expired`
  at `:76` and `apply_lazy_purge_effects_no_version_bump()` at `:86`. Whether that counts as
  store execution under a foreign lock is undecided.
- `VllMsg::VllExecute` (`dispatch_vll.rs:18`) is probably the drain path and legitimately exempt,
  but it hardcodes `conn_id = 0` (`vll.rs:49`), so the exemption is implicit.

## Fix

Preferred, per the campaign-2 W1 survey: add `ShardWorker::execute_gated(conn_id, f)` in
`worker.rs` and make `can_execute_during_lock` private to it, so an arm that reaches store
execution cannot compile without stating a disposition. Response shapes differ per arm
(`Response` / `PartialResult` / `TransactionResult`), so the wrapper needs a conversion bound —
`scatter_conflict_reply` (`dispatch_core.rs:132`) is the precedent.

The C3 lint (every arm appears in exactly one of `GATED_ARMS` / `EXEMPT_ARMS{reason}`) then
reduces to "no direct `can_execute_during_lock` outside `worker.rs`". It mirrors the
`rejects_pre_execution` table (`dispatch.rs:94`), already documented as *"exhaustive on purpose:
a new stage must state its disposition rather than inherit a default."*

## Forcing test

A cross-shard script takes the continuation lock; a second connection issues `FCALL` against a
held shard and must be rejected with the same `ERR shard busy with continuation lock` its `EVAL`
equivalent gets. Add the `ExecTransaction` twin (issue 50) in the same test module, plus a table
test asserting every `ScriptingMsg`/`CoreMsg` arm is classified.

## Comments

Found by the campaign-2 chokepoint-lint survey, 2026-08-07, while counting C3 bypasses (12 of 17
arms bypass the gate; 2 are real defects).

2026-08-07: the C3 lint landed (`scripts/continuation-lock-gate.py`, `just
lint-continuation-lock`) with this arm — and its `CoreMsg::ExecTransaction` twin, round-2 issue 50
— pinned as a **named-gap bypass**: every run warns and links here, and the moment the arm gains a
`can_execute_during_lock` call the stale gap entry hard-fails, so the fix must promote it to
`GATE` in the script. Fixing it therefore means: add the guard to the arm body (not to
`handle_function_call` — the lint requires the disposition be stated at the dispatch arm), move
`ScriptingMsg::FunctionCall` from `GATE_GAP` to `GATE`, and add the forcing test described above.
