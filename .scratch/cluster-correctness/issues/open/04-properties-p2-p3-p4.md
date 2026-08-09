# 04 — Properties P2 (snapshot lossless anywhere), P3 (replay determinism), P4 (event conservation)

Status: needs-triage

## Parent

[PRD](../../PRD.md) §3 W2.

## What to build

Three properties over the issue-03 generator:

- **P2 — snapshot/restore lossless at any point**: apply a prefix, round-trip through
  *both* snapshot vehicles (serialized `ClusterStateInner` openraft path and the
  `ClusterSnapshot` → `from_snapshot` DTO path — the audit showed they can disagree),
  apply the suffix, compare against the uninterrupted run. Retro-covers the whole
  audit-defect-2 class (FM-CLUSTER-100) for every field, forever.
- **P3 — replay determinism**: same sequence, two fresh states, identical results
  (closes round-2 87/F2); doubles as a purity guard against wall-clock/randomness in
  apply.
- **P4 — event conservation**: every `SlotHandoffPrepared` pairs with exactly one
  `SlotHandoffReleased` across the sequence, via the `release_events()` funnel.

## Acceptance criteria

- [ ] P2 compares both restore vehicles against the uninterrupted run at every split
      point
- [ ] P3 and P4 land beside it; all three in the default suite + nightly boosted pass
- [ ] Reverting the FM-CLUSTER-100 fix makes P2 fail (retro-validation evidence for
      issue 13)
- [ ] `just mutants-diff frogdb-cluster` triaged before push

## Blocked by

- Issue 03 (`.scratch/cluster-correctness/issues/`) — shares the generator.
