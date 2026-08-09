# 02 — Invariant catalog + self-checking state machine hooks

Status: needs-triage

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

- [ ] Catalog module in `frogdb-cluster` with the ten seed invariants, all HARD
- [ ] Post-apply + post-restore (both vehicles) hooks active in test/debug builds
- [ ] Full suite green under the hooks (any violation triaged: fix or cited exception)
- [ ] Every HARD invariant has a forcing test; deleting a check kills a mutant
- [ ] `just mutants-diff frogdb-cluster` triaged before push

## Blocked by

- Issue 01 (`.scratch/cluster-correctness/issues/`) — lands the behavior fixes so
  INV-REF-1/2/3 can be HARD from day one.
