# 22: Scatter carries one absolute deadline — per-receiver waits cannot sum

Status: ready-for-agent

## Origin

Distsys-review MAJ-20 (`.scratch/formal-spec/2026-08-13-independent-distsys-review.md`),
ruled accept-and-file by the user 2026-08-14
([rulings ledger](../../../formal-spec/2026-08-13-distsys-review-rulings.md)).

## What is wrong

`specs/vll.md:55`'s `acquisition.timeout` row documents the `participants × timeout`
accumulation for phase 4 only ("phase 4 applies it **per receiver** in a sequential
loop"). Phase 2 has the identical shape and the spec is silent about it:
`frogdb-server/crates/vll/src/coordinator.rs:247-266` runs
`for (shard_id, ready_rx) in ready_rxs { timeout(request.timeout, ready_rx) }` — a
fresh *relative* timeout per receiver, started only when the loop reaches that
receiver. `acquire_continuation` (`:394-409`) has the same loop, documented nowhere.

Consequence: with 16 shards at the 4 s default, a contended scatter can burn 64 s in
phase 2 before aborting, and another 64 s in phase 4 — a request that hangs over a
minute against a 4 s configured timeout. A reader of TR-VLL-017 takes the bound to
be one `timeout`.

gRPC and CockroachDB's DistSender compute a single *absolute* deadline once at
request entry and pass it down, so per-hop waits cannot sum. Standard fix.

## Relation to CRIT-7 (wound-wait, spec-gaps issue 14)

Complementary, not redundant. Wound-wait kills deadlock *cycles* proactively, so
timeout stops being the only exit for that class (CRIT-7's ruling notes it
"partially moots MAJ-20's timeout-only exit for this cycle class"). The deadline fix
covers what wound-wait cannot: genuine slow shards, overload, large scatters —
configured 4 s must mean observed ≤ ~4 s regardless of participant count.

`Instant` here is monotonic, request-scoped, local — timeout mechanics, not
state-bearing time. The no-wall-clock-in-state principle is not implicated.

## What to build (spec-first; txn locked, gate 0.90)

1. Spec rows first:
   - Restate the `acquisition.timeout` row (`specs/vll.md:55`) as a **total request
     bound**: one absolute deadline computed at `scatter` entry, applied to every
     receiver wait in phases 2 and 4 and in `acquire_continuation`. Remove the
     phase-4-only `participants × timeout` framing (it becomes the *pre-fix* failure
     mode, not the contract).
   - Amend TR-VLL-017 so the stated bound matches.
2. Code: compute one `Instant` deadline at `scatter` entry
   (`now + request.timeout`); replace every `timeout(request.timeout, ready_rx)` in
   phase 2 (`coordinator.rs:247-266`), phase 4, and `acquire_continuation`
   (`:394-409`) with `timeout_at(deadline, ...)`. Thread the deadline through the
   continuation package if it crosses that boundary (coordinate with
   [issue 07](07-vll-continuation-package.md)).
3. Forcing test: a scatter over N shards where earlier receivers each consume most
   of the budget → total elapsed stays ≈ one `timeout`, later receivers get the
   *remaining* budget (fails pre-fix: total ≈ N × timeout). A paused-clock/`tokio`
   time-controlled test keeps it deterministic.

## Cross-references

- [Issue 14](../done/14-sca-wound-wait-restores-acyclicity.md) (CRIT-7 wound-wait): handles
  the deadlock-cycle class; this issue handles the accumulation class. Both land
  independently.
- [Issue 07](07-vll-continuation-package.md): `acquire_continuation` is part of the
  continuation surface — if 07 restructures it, the deadline threads through the
  package.

## Acceptance criteria

- [ ] `acquisition.timeout` row + TR-VLL-017 restated as total bound; `just
      lint-spec` green
- [ ] One absolute deadline at `scatter` entry; `timeout_at` in phases 2/4 +
      `acquire_continuation`
- [ ] Forcing test fails pre-fix (accumulation), passes post-fix (total bound)
- [ ] `just mutants-diff` on frogdb-vll/frogdb-txn (locked, 0.90) triaged

## Blocked by

None — coordinate with issues 07/14 if in flight.
