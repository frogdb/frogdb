# EXEC slot re-validation: `slot_table_version` fast path

Status: needs-triage
Type: AFK
Origin: exec-slot-revalidation PRD, task T10(a) — deferred optimization
Severity: likelihood 1/3, consequence 1/3 (score 1)
Area: Cluster / Transactions

## Context

`EXEC` now re-validates the whole queued batch against a single `ClusterState::snapshot()` taken at
`EXEC` entry (`connection/transaction.rs`, `connection/guards.rs::validate_queued_batch`,
`slot_migration/routing.rs::route_queued_batch`). That is correct but unconditional: every `EXEC`
with at least one keyed command pays for a snapshot plus a slot fold, even when the cluster topology
has not moved a single slot since the commands were queued — which is the overwhelmingly common
case.

The PRD evaluated a cheaper design (Option D): keep a monotonically increasing `slot_table_version`
on `ClusterState`, capture it at `MULTI` (or at first queued keyed command), and at `EXEC` compare
the captured value with the current one. If they are equal, no slot has changed hands and the whole
re-validation can be skipped. It was deferred because it is an optimization on top of a correctness
fix, and because it needs the counter to be bumped on *every* mutation of the slot table, not only
the obvious ones.

## What to build

- Add a `slot_table_version: u64` to the replicated cluster state, bumped by every slot-table
  mutation applied through `frogdb-cluster/src/commands.rs` (`SETSLOT NODE`/`MIGRATING`/`IMPORTING`,
  `ADDSLOTS`/`DELSLOTS`, `ADDSLOTSRANGE`/`DELSLOTSRANGE`, failover-driven reassignment, node
  removal). Every one of these must bump it, or the fast path is unsound.
- Expose the version on `ClusterSnapshot` and capture it in `ConnectionState` when a transaction
  starts (or lazily on the first keyed queued command).
- In `validate_queued_batch`, take the snapshot once; if the captured version equals the snapshot's
  version, return `None` without folding keys or routing.
- The migrating/importing probe path must still run when a migration window is open, because key
  *presence* changes without the slot table changing. Simplest safe rule: the fast path applies only
  when the captured version matches **and** the batch's slots have no open migration in the
  snapshot.

## Acceptance criteria

- [ ] A unit test asserts every slot-table-mutating command bumps the version (table-driven over the
      command set, so a new mutation that forgets to bump fails the test).
- [ ] `validate_queued_batch` short-circuits when the version is unchanged and no migration touches
      the batch's slots.
- [ ] All existing `multi_exec` integration tests still pass unchanged — in particular the
      migration-boundary tests in `integration_cluster.rs`, which must *not* take the fast path.
- [ ] A benchmark or a counter shows the snapshot/fold work is skipped on the steady-state path.

## Notes

Do not land this without the "every mutation bumps" test. A missed bump turns the fast path into
exactly the orphan-write bug the PRD fixed, but only under a rare topology change — the worst
possible failure shape.
