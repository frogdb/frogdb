# MULTI/EXEC executes while another connection holds a VLL continuation lock

Status: needs-triage
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/12 F2 · MASTER.md §3
Score: severity 5 · likelihood 3 · effort 3 · priority 18
Area: frogdb-core / shard dispatch + VLL continuation locks

## Context

A cross-shard Lua script acquires continuation locks on N shards precisely to get exclusive access.
`CoreMsg::Execute` and `CoreMsg::ScatterRequest` both consult `can_execute_during_lock` and bail;
`CoreMsg::ExecTransaction` has `conn_id` in scope and never calls it, going straight to
`execute_transaction`. A concurrent `EXEC` therefore mutates the shards the script believes it
holds exclusively, and the script's cross-shard read-modify-write becomes non-atomic — a silent
consistency violation with no error surfaced to either client. It needs
`allow_cross_slot_standalone=true` plus a concurrent MULTI, which is ordinary traffic on that
config.

**This is a suspected live defect found by reading, not by test failure — the proposed test fails
against today's code.** The evidence is the auditing agent's and needs confirmation before or
during the fix.

## Evidence

- `crates/core/src/shard/dispatch_core.rs` — `CoreMsg::Execute` (line 20) and
  `CoreMsg::ScatterRequest` (line 49) both call `self.can_execute_during_lock(conn_id)` and bail;
  `CoreMsg::ExecTransaction` (line 95) has `conn_id` in scope and **never calls it**, going
  straight to `execute_transaction`.
- **Why nothing catches it**: the gate itself (`worker.rs:806 can_execute_during_lock`) is
  `well-covered` (3027 tests) — it is the missing *call site*, not the gate, that is untested.
- `dispatch_scripting.rs` applies the gate at lines 17/42/104, confirming the intended policy is
  "everything is gated".

## Options

The proposal raised an explicit `OPTIONS` block; both levels are wanted, but the decision on
whether to build one or both should be recorded before work starts.

- *3 (`shard_driver`)*: send `CoreMsg::ExecTransaction` directly while a continuation lock is held
  and assert the busy error. Cheapest, deterministic, pins the exact missing gate — but asserts on
  an internal message rather than user-visible behaviour, and would not catch the connection layer
  routing around it.
- *4 (server integration)*: as proposed. Proves the user-visible property, needs a script that
  reliably holds the lock for a window.
- **Recommendation: both** — the level-3 test as the fast regression pin (it is ~20 lines given
  `scenario_s4`), the level-4 test as the behavioural guarantee. If only one, take 4.

## Acceptance criteria

- [ ] The chosen level(s) above are recorded on this issue before implementation starts.
- [ ] Server integration test: conn A runs a cross-shard `EVAL` that sleeps/blocks while holding
      continuation locks on shards 0 and 1; conn B runs `MULTI; SET k_on_shard0 v; EXEC`. Assert B
      is rejected with the continuation-lock busy error (or is serialised after A), and that A's
      script observes a consistent snapshot. **Fails today.**
- [ ] If the level-3 pin is built, it sends `CoreMsg::ExecTransaction` while a continuation lock is
      held and asserts the busy error, mirroring the existing `scenario_s4` assertion shape.
- [ ] `dispatch_core.rs`'s `ExecTransaction` arm calls `can_execute_during_lock` after the fix, and
      a reader can see all three arms treated identically.

## Test boundary

**4** as proposed — MULTI/EXEC queueing lives in the connection layer, and the shard-level message
is only reachable through it, so level 3 alone would not catch the connection layer routing around
the gate. Level 3 is worth having as a cheap deterministic pin, not as a replacement.

## Depends on

Infrastructure I15 (cross-shard EVAL test helper) — issue 15,
`.scratch/testing-improvements-round2/issues/`. The `allow_cross_slot_standalone` knob already
exists in `test-harness/src/server.rs`; only the scripting-side helper for "run an `EVAL` whose
keys span shards" is missing (~0.5 day). Related: infrastructure I5 ("shard busy running a script"
fixture) — issue 05, same directory — supplies the long-running-script half.

## Re-triage 2026-08-06

**Verdict: still-valid**

Confirmed live; the Phase-1 txn/vll lock did not close it. `crates/core/src/shard/dispatch_core.rs`
still gates only two of the three arms: `CoreMsg::Execute` at `:20` and `CoreMsg::ScatterRequest` at
`:49` call `self.can_execute_during_lock(conn_id)`; the `CoreMsg::ExecTransaction` arm at `:95-115`
has `conn_id` in scope and goes straight to `execute_transaction` (line refs 20/49/95 unchanged).
`execute_transaction` (`crates/core/src/shard/execution.rs:533`) does not consult the gate either —
it starts at `purge_expired_watches`/`check_watches`. The gate itself is now `worker.rs:844`
(issue cited `worker.rs:806`), and `dispatch_scripting.rs` still applies it at `:17/:42/:104`.
Grepping both locked specs finds **no FM row** for this call site:
`vll-failure-modes.md` FM-VLL-001..004 cover SCA and continuation-vs-continuation contention only
(FM-VLL-002's "cross-shard Lua / MULTI" refers to a *cross-shard MULTI acquiring* a continuation
lock, not to an `ExecTransaction` running under someone else's), and `txn-failure-modes.md` contains
no occurrence of `continuation` or `can_execute_during_lock` at all. EXEC still routes as a
single-shard `CoreMsg::ExecTransaction` (`crates/txn/src/exec.rs:350-378` →
`crates/server/src/connection/transaction.rs:185-212`), so no continuation lock is taken on the way
in. The `Options` decision the issue asks for is still unrecorded.
