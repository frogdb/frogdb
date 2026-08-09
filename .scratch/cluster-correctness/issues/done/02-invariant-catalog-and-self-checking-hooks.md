# 02 — Invariant catalog + self-checking state machine hooks

Status: done

## Parent

[PRD](../../PRD.md) §3 W1; catalog location ruled in §8 D5 (module in `frogdb-cluster`).

## What to build

`frogdb-server/crates/cluster/src/invariants.rs`: pure functions over
`&ClusterStateInner`, no I/O, returning `Vec<Violation>` (`id: &'static str`,
`detail: String`). Seed with the ten PRD entries (INV-REF-1..4, INV-EPOCH-1/2,
INV-HANDOFF-1/2, INV-MIG-1, INV-SLOT-1). Two tiers only: HARD (violation = defect) and
DOCUMENTED-EXCEPTION (must cite an FM row or issue; citation-less exception is a build
error). With issue 01 landed, all ten seeds land HARD.

Hooks under `#[cfg(any(test, debug_assertions))]`: assert the catalog clean at the end of
`ClusterState::apply_command` and after both restore vehicles (`from_snapshot` and the
openraft `install_snapshot` path) — the restore seam is where audit defect 2 lived. This
upgrades all ~430 existing cluster tests into invariant tests at zero authoring cost.

Each HARD invariant gets a forcing test constructing the violating state directly via
test-only mutators, asserting the catalog reports the right ID — otherwise the catalog is
dead code to the mutation gate.

## Acceptance criteria

- [x] Catalog module in `frogdb-cluster` with the ten seed invariants — eleven entries, ten
      HARD (INV-REF-3 splits; see below)
- [x] Post-apply + post-restore (both vehicles) hooks active in test/debug builds
- [x] Full suite green under the hooks (one violation cited, two filed as defects)
- [x] Every HARD invariant has a forcing test; deleting a check kills a mutant
- [x] `just mutants-diff frogdb-cluster` triaged before push

## Blocked by

- Issue 01 (`.scratch/cluster-correctness/issues/`) — lands the behavior fixes so
  INV-REF-1/2/3 can be HARD from day one.

## Resolution

Shipped 2026-08-09 on `worktree-agent-a53014068a3d72aa5`.

### What landed

`frogdb-server/crates/cluster/src/invariants.rs` — pure checks over `&ClusterStateInner`,
each returning `Vec<Violation> { id: &'static str, detail: String }`, with `check_all` (every
tier) and `check_hard` (the asserting view) differing only by a filter flag so they cannot
drift. Two tiers, no third. `Tier::DocumentedException(Citation)` carries its citation as a
variant field; `Citation::failure_mode` / `Citation::issue` are `const fn`s that `assert!` the
reference is non-empty, and `CATALOG` is a `static`, so a citation-less *or* blank-citation
exception is a compile error rather than a review comment.

The ten PRD seeds land as **eleven entries, ten of them HARD**. INV-REF-3 ("a Replica's
`primary_id` names an existing Primary") splits: the *existence* half stays HARD, and the *is
a Primary* half becomes **INV-REF-3B**, the catalog's single DOCUMENTED-EXCEPTION, citing
issue 14. Reason below.

Hooks under `#[cfg(any(test, debug_assertions))]`, at all three mutation seams:

- `ClusterState::apply_command` — the transition match moved into a private
  `ClusterState::apply_to(&mut ClusterStateInner, cmd)` so the arms' many `return Err(..)`
  early exits cannot skip the hook; `apply_command` is now lock acquisition, `apply_to`, hook,
  return. Rejected transitions are checked too.
- `ClusterState::from_snapshot` — the reader-DTO restore vehicle.
- `ClusterState::restore_from_snapshot` — the chokepoint both openraft vehicles share
  (`RaftStateMachine::install_snapshot` and the persisted snapshot reloaded by
  `attach_snapshot_store`), so neither is skipped. A test drives the real `install_snapshot`
  to demonstrate the routing rather than asserting it in a doc comment.

Tests, all in `frogdb-cluster`: a `clean_state()` fixture that sits on each check's boundary
(so `>`/`>=`, `==`/`!=` and dropped-filter mutants die), one forcing test per HARD entry, an
overlap test proving the two halves of the old INV-REF-3 do not double-report, a tier test
proving `check_hard` is the only behavioral difference between tiers, and a `#[should_panic]`
hook-forcing test for each of the three seams so a deleted hook goes red.

### Behavior triage under the hooks

Three real states the catalog rejected; none was fixed here (PRD §7 keeps individual defect
fixes out of this issue), all three are recorded:

1. **Chained replicas** — `AddNode` and `SetRole` both accept a `primary_id` that is itself a
   replica. `frogdb-cluster-runtime`'s `auto_failover_ignores_a_failed_replica` builds exactly
   that chain on purpose (node 4 replicating a replica is what makes the test non-vacuous), so
   the shape is load-bearing in the suite today. Filed as **issue 14**, and INV-REF-3B cites
   it; closing 14 retires the exception.
2. **Graceful failover ownership drift** — `Failover { force: false }` transfers the old
   primary's slots but does not prune migrations sourced at it, leaving
   `INV-MIG-1: slot 5 is migrating from 1 but is owned by 2`. No existing test reaches the
   path, so the suite is green and the defect is latent; reproduced with a throwaway test and
   filed as **issue 15** with the reproduction.
3. **Two tests needed re-pointing, not weakening**:
   `complete_migration_refuses_a_target_that_is_no_longer_a_member` (FM-CLUSTER-033) is a
   deliberate *negative* fixture — a snapshot naming a target that is not a member, which is
   the only way to reach the guard — so it now builds the `ClusterStateInner` directly and
   calls `ClusterState::apply_to`, bypassing the plumbing rather than the catalog. The
   apply-seam `#[should_panic]` test forces `AddNode` with a dangling parent, and carries a
   note to re-point it when issue 14 lands.

### Verification

- `just test frogdb-cluster` — 265 passed, 1 skipped
- `just test frogdb-cluster-runtime` — 78 passed
- `just test frogdb-server` — 1997 passed, 4 skipped (the hooks are live in every debug build,
  so the whole server suite runs under them)
- `just check` (workspace) — clean; `just fmt` on the touched crate
- `just lint-failure-modes` — OK, 278 modes / 1382 references / 1382 tags (no FM rows added:
  no behavior changed)
- `just scratch-check` — OK
- `just mutants-diff frogdb-cluster` — 71 mutants: **49 caught, 22 unviable, 0 missed**, so no
  `DOCUMENTED EQUIVALENT` markers were needed
