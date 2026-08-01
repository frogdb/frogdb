# Lua scripts validate keys once, then write later — same shape as the EXEC orphan bug

Status: needs-triage
Type: AFK
Origin: exec-slot-revalidation PRD, task T10(c); same "validated once, executed later" shape as issue 33
Severity: likelihood 2/3, consequence 3/3 (score 6)
Area: Scripting / Cluster

## Context

`EVAL`/`EVALSHA`/`FCALL` validate the declared `KEYS[]` set against the cluster snapshot **once**, at
invocation, in the pre-dispatch gauntlet. The script body then runs to completion and issues an
arbitrary number of `redis.call` writes, none of which are re-checked against the slot table. This
is structurally the same defect the exec-slot-revalidation PRD fixed for `MULTI`/`EXEC`: a decision
taken at time T is acted on at time T+n, and the cluster topology may have moved in between.

It is also the same shape as issue 33 (a gate evaluated at queue time, an effect applied at execute
time). It differs from `EXEC` in one respect that makes it *harder*: an `EXEC` batch is fully known
before the first command runs, so the whole queue can be validated up front, whereas a script's
write set is only known as the script executes. The `EXEC` fix explicitly relies on "validate
everything before any shard round-trip"; a script cannot offer that.

Additionally, the self-fence and `min-replicas-to-write` gates behave differently for scripts than
for plain writes (see `test_self_fence_does_not_gate_lua_writes` in `integration_replication.rs`,
which pins the current, permissive behavior) — so the gap is not only about slot ownership.

## What to build

Decide and then implement one of:

- **(a) Re-check per `redis.call` write.** Cheapest to reason about, most expensive at runtime: every
  write inside a script consults the snapshot captured at script entry (not a fresh one, or the
  script loses its own atomicity) and errors the script if the slot has moved. Requires deciding
  what a mid-script failure returns to the client and what happens to writes already applied.
- **(b) Pause barrier at migration finalization** (see
  [02-migration-finalization-pause-barrier.md](../)) — makes
  the problem disappear for scripts *and* every other path, at an availability cost. If that issue is
  adopted, this one is subsumed.
- **(c) Accept and document.** Record the window in
  `website/src/content/docs/architecture/clustering.md` next to the MULTI/EXEC section, with the
  reasoning, and pin the current behavior with a test so a future change is deliberate.

## Acceptance criteria

- [ ] The window is reproduced by a test: a script whose slot is migrated out mid-execution, showing
      where its writes land today.
- [ ] A decision among (a)/(b)/(c) recorded in the PRD directory.
- [ ] Whichever path is chosen, the behavior is pinned by a test and described in the clustering
      docs alongside the MULTI/EXEC re-validation section.
- [ ] The interaction with self-fence / `min-replicas-to-write` for script writes is re-examined at
      the same time (`test_self_fence_does_not_gate_lua_writes`).

## Notes

Research first: check what Redis (`scripting.c` / `script.c` slot checks), Valkey, and DragonflyDB
do for a script whose slot moves mid-execution. Redis's answer is essentially (c) plus its own
gossip-epoch mechanics; Dragonfly's is essentially (b).
