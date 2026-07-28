# Pause barrier at slot-migration finalization (Dragonfly parity)

Status: needs-triage
Type: AFK
Origin: exec-slot-revalidation PRD, task T10(b) — deferred hardening
Severity: likelihood 2/3, consequence 2/3 (score 4)
Area: Cluster / Slot migration

## Context

The EXEC slot re-validation landed by the exec-slot-revalidation PRD closes the window in which a
`MULTI` queued on the old owner commits after the slot moved. It does so by re-reading the cluster
snapshot at `EXEC` entry, which leaves one residual window (risk 7 in that PRD): the interval
between the Raft entry that reassigns the slot being *committed* and it being *applied* on the node
that is about to lose the slot. During that interval the losing node's snapshot still names itself
as owner, so an `EXEC` there validates and commits. The window is bounded by Raft apply latency, not
by anything the transaction path controls.

Redis has the same window and accepts it. DragonflyDB does not: it quiesces the source before
finalizing a slot handoff, so no command — transactional or not — can be in flight across the
ownership change. That is a strictly stronger guarantee and it also covers command paths that
re-validation cannot reach, notably Lua scripts, which validate their key set once at invocation and
then execute an arbitrary number of writes afterwards.

## What to build

Research how Dragonfly implements the source-side quiesce (`cluster_family`/slot-migration
finalization) and evaluate it against FrogDB's Raft-driven control plane, where the pause has to be
coordinated with the Raft apply rather than with a gossip epoch bump. The mechanism FrogDB already
has is `CLIENT PAUSE` (`connection/transaction.rs` waits on it before the shard round-trip); the
work is deciding who arms it, on which nodes, and how the barrier is released if the finalizing node
dies mid-handoff.

Sketch:

1. Before committing the `SETSLOT NODE` entry, the control plane arms a write pause on the source
   node scoped to the migrating slot.
2. The source drains in-flight commands for that slot.
3. The reassignment is committed and applied.
4. The pause is released, and everything still queued gets the new snapshot.

## Acceptance criteria

- [ ] A written comparison of Dragonfly's approach against FrogDB's Raft control plane, with the
      failure modes of a source that dies while paused.
- [ ] A decision recorded in the PRD directory: adopt, adopt-scoped (slot-scoped pause only), or
      reject with the residual window documented as intended behavior.
- [ ] If adopted: a test that a slot handoff completing concurrently with an in-flight Lua script
      cannot leave a write on the former owner (the property `EXEC` re-validation cannot provide).

## Interaction with the EXEC re-validation already in place

`connection/transaction.rs` validates a write batch *before* the `CLIENT PAUSE` wait (so an EXEC that
is already doomed fails fast instead of occupying a pause slot) and re-validates it afterwards
**only when the wait actually blocked** — `wait_if_paused_for_transaction` returns whether it parked.
Without that second pass an EXEC held across a finalization would resume on a stale verdict and
commit on the former owner, which would silently defeat this barrier the day it is armed. Anyone
implementing the barrier must keep the post-wait re-validation, or replace it with something
strictly stronger.

There is no integration test for the re-validation arm: driving it requires a `CLIENT PAUSE WRITE`
on the source that outlives a slot handover, and the handover's own `MIGRATE` runs through a client
connection on that same paused node. Adding the barrier gives the test a natural hook — the barrier
itself becomes the thing that holds the EXEC — so the coverage should land with this issue.

## Notes

Gated behind a decision, not a straight implementation task. The pause barrier costs availability at
every migration finalization; that trade may not be worth it if the residual window is measured and
found to be sub-millisecond in practice. Measure first.
