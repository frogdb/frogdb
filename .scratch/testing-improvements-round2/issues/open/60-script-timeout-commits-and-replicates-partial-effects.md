# A write script aborted by `lua-time-limit` has its partial effects committed *and* replicated

Status: needs-triage
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/09 F4 · MASTER.md §3 (consistency violations), §2 T6, §7 (decision required)
Score: severity 5 · likelihood 3 · effort 3 · priority 18
Area: frogdb-core / scripting

## Context

When a script exceeds `lua-time-limit`, the instruction hook aborts it mid-way — and the shard
then commits and replicates whatever the script had already written. The result is a
partially-applied, partially-replicated script. Redis never does this: its busy threshold only
starts replying `BUSY` to *other* clients, and a script that has written is explicitly
`UNKILLABLE`. FrogDB's own kill path honours that rule and returns `Unkillable` when
`has_writes`; the timeout path does not consult it at all.

**This is a suspected live defect found by reading, not by test failure — the proposed test
fails against today's code.** The file:line evidence below is the auditing agent's and needs
confirmation before or during the fix; this issue is not among the two the coordinator
verified directly. MASTER.md §7 lists the script-timeout write policy as a decision that must
be settled before the test can assert anything, hence `needs-triage`.

## Evidence

The instruction hook (`scripting/src/sandbox.rs:209-228`) raises
`mlua::Error::RuntimeError("BUSY script running for {} ms")` on elapsed >
`timeout_ms + grace_ms` with **no `has_writes` consultation** — it has no access to the
execution state at all, only the kill flag. The abort propagates out of `execute`, but
`core/src/shard/scripting.rs:114-130` has already drained `ctx.effects.script_writes` and
calls `run_script_write_effects` unconditionally — the comment at `:126-129` states this is
deliberate ("Runs even when the script itself errored mid-way"), which is right for a
*script-raised* error and wrong for a *server-imposed* abort. No test anywhere sets
`lua_time_limit_ms` low and asserts what survives. Default `lua_time_limit_ms` is 5000
(`core/src/scripting/config.rs:40`). Contrast the kill path at `lua_vm.rs:472-478`, which
returns `Unkillable` when `has_writes`.

## What to fix

1. Give the timeout path access to the execution state so it can distinguish an abort of a
   read-only script from an abort of a script that has already written.
2. Implement the chosen policy from `## Options` below — either roll the partial effects back,
   or let them survive and guarantee they replicate as one MULTI/EXEC-framed batch.
3. Whichever policy is chosen, make the propagated batch and the primary's applied set
   identical, so primary and replica agree exactly on the partial set.
4. Reconcile the deliberate comment at `core/src/shard/scripting.rs:126-129` with the chosen
   policy so the next reader is not misled.

## Options

Reproduced verbatim from proposals/09 F4:

- *(a)* `shard_driver` with a low limit and the `fake-wal` seam: assert the WAL contains a
  MULTI/EXEC-framed prefix of the writes (effort 3). Deterministic, no replica.
- *(b)* Two-node server integration: assert primary and replica converge after the abort
  (effort 4). Proves the real consequence, slower and clock-sensitive.
- **Recommendation**: (a) first as the regression net; (b) once, as the consistency proof.

The policy question the test forces: assert either "no writes survive" or "writes survive and
replicate identically" — but *pin* one.

## Acceptance criteria

- [ ] With `lua-time-limit` set to ~50 ms, `for i=1,1e9 do redis.call('SET', KEYS[1]..i, i) end`
      returns a `BUSY` error to the client. Fails today for the surviving-effects assertions.
- [ ] The number of keys actually present on the primary equals the number reported by the
      replica after `WAIT` — primary and replica agree exactly on the partial set.
- [ ] The propagated batch is MULTI/EXEC-framed (asserted at level 3 via the `fake-wal` seam).
- [ ] The chosen policy ("no writes survive" or "writes survive and replicate identically") is
      asserted explicitly, not left implicit.

## Test boundary

Level 4 (server integration) for the full property — it needs `CONFIG SET lua-time-limit` and a
replica. The primary-only half (partial effects present + framed) is worth doing first at level
3 (`shard_driver`) because it is cheap and deterministic; level 3 alone cannot observe the
replication half, which is why the level-4 case is still required.

## Depends on

issue 05 (I5 — "shard busy running a script" fixture),
`.scratch/testing-improvements-round2/issues/`
