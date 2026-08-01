# EXEC slot re-validation: `slot_table_version` fast path

Status: wontfix as specified (analysed 2026-07-31, hardening campaign phase 1c) — the design below is
unsound in the common case and aims at the wrong cost; the real cost is re-filed as issue 10
Type: AFK
Origin: exec-slot-revalidation PRD, task T10(a) — deferred optimization
Severity: likelihood 1/3, consequence 1/3 (score 1)
Area: Cluster / Transactions

## Analysis and disposition (2026-07-31)

Not a defect: EXEC re-validation is unconditional and correct today, so there is no failure mode to
spec and no failing test to write. Taken up as an optimization and rejected in the shape written
below, for three reasons — two soundness, one cost.

**1. The fast path drops verdicts queue-time validation never computed, with no topology change
involved.** The premise "version unchanged ⇒ the queue-time verdicts still hold" is false, because
`validate_queued_batch` is not a re-run of the queue-time checks. It is *whole-batch*, and two of
its verdicts have no queue-time counterpart:

- *Cross-slot.* Queuing validates each command's own keys in isolation
  (`guards.rs::try_queue_in_transaction` → `validate_cluster_slots`), and
  `TxnSlotAccumulator::take()` folds *shards*, not slots. Two single-key commands on different
  slots that share a shard (`shard = slot % num_shards`, `core/src/shard/partition.rs:32`)
  therefore reach EXEC with nothing having objected, and the only thing that rejects them is
  `route_queued_batch`'s `single_slot()` check inside `validate_queued_batch`. Skipping it when the
  version is unchanged — i.e. essentially always — serves a cross-slot transaction.
  `test_multi_exec_two_single_key_commands_different_slots_defers_crossslot_to_exec`
  (`integration_cluster.rs:10046`) pins exactly this, and would fail or flake depending on which
  slot pair the helper picks.
- *Batch-level READONLY eligibility.* Per-command eligibility is "this command is flagged
  READONLY" (`guards.rs:711`); batch eligibility is "no command anywhere in the batch is a WRITE,
  including cluster-exempt ones" (`fold_queued_batch`, `guards.rs:937-945`), matching Redis's
  `c->mstate.cmd_flags & CMD_WRITE`. A READONLY connection can queue `GET {S}k` for a slot its
  master owns (rescued per-command) alongside a keyless exempt WRITE, and today EXEC answers
  `-MOVED`. The fast path serves it on the replica.

Both are steady-state cases. The issue's own Notes anticipate a missed *bump* as the danger; the
larger danger is that the two checks are not the same check.

**2. Capture ordering is unspecified in the lazy variant.** "Capture at MULTI (or lazily on the
first keyed queued command)" is only sound if the captured version comes from *the same snapshot*
the queue-time route used. Read separately, a slot change landing between the route and the capture
is invisible forever after. Worse for the WATCH gate added the same round
(`watched_slots_still_local`, FM-TXN-049): a `WATCH` legitimately precedes `MULTI`, so a MULTI-time
capture can record a version that already reflects the slot leaving, and extending the fast path
over that gate would re-open
[issue 04](04-watch-slot-validation.md) exactly. If a fast path is ever built, it must be scoped to
`validate_queued_batch` and must never gate the watch check.

**3. It optimizes the cheap half.** As specified the snapshot is still taken ("take the snapshot
once; if the captured version equals the snapshot's version…"); only the fold and the slot routing
are skipped. Measured on this machine (throwaway harness, release build, a fully assigned
16384-slot table):

```
clone of 16384-slot snapshot: 114.501µs per call
10 slot lookups:                  132ns per call
```

`ClusterState::snapshot()` (`cluster/src/state.rs:129`) deep-clones `slot_assignment` on every
call, so the fast path would save ~0.1% of `validate_queued_batch` and leave the other 99.9%.
Acceptance criterion 4 ("a benchmark shows the snapshot/fold work is skipped") is unreachable in
this design. Skipping the snapshot too would need a lock-free `slot_table_version()` accessor *and*
a global "any migration open" flag, since the specified safe rule ("no open migration touches the
batch's slots") needs both the fold and the `migrations` map to evaluate.

Point 3 also relocates the problem: the same 114µs clone is paid **per keyed command** on the
ordinary dispatch path (`SlotMigrationCoordinator::route`, `slot_migration/routing.rs:119`, called
from `validate_cluster_slots`), and by every `CLUSTER`/`INFO`/failure-detector reader. EXEC's single
snapshot per transaction is already cheaper than the per-command path it re-checks. Fixing the
clone — publish the snapshot as an immutable `Arc` swapped on apply, so readers clone a pointer —
removes ~16k-entry copies from the whole server and makes this issue moot. Filed as
[issue 10](10-cluster-snapshot-clone-cost.md).

Nothing was implemented: the counter and its bumps live in `frogdb-cluster`'s replicated state and
command-apply path (cluster phase machinery), which this campaign phase does not touch, and the
change would alter the serialized raft snapshot body.

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
