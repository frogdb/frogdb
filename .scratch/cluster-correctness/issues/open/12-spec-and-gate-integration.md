# 12 — Spec and gate integration

Status: needs-triage

## Parent

[PRD](../../PRD.md) §3 W6.

## What to build

- Cross-reference: each catalog invariant cites the FM rows it generalizes; rows whose
  invariant is now universally checked note the invariant ID.
- `lint-failure-modes` gains an optional `INV-*` vocabulary check (warn on dangling
  references) — same script, small addition.
- Mutation re-baseline: full `just mutants` + `just mutants-gate` runs for
  `frogdb-cluster` (0.80) and `frogdb-cluster-runtime` on current code — recorded scores
  predate rows 084–102 entirely. The catalog + property tests should move in-crate kill
  coverage for the 29 rows currently forced only from server-side integration tests.
- Fix the two mis-tagged rows (campaign-2 issue 09,
  `.scratch/hardening-2/issues/`) while in the file.

## Acceptance criteria

- [ ] Every catalog invariant ↔ FM row cross-reference in place, lint warns on dangling
      `INV-*`
- [ ] Fresh mutation scores recorded for both crates; gates pass or survivors documented
      at the code
- [ ] Mis-tagged rows fixed; `just lint-failure-modes` green

## Blocked by

- Issue 02 and issue 03 (`.scratch/cluster-correctness/issues/`) — the re-baseline
  measures their in-crate coverage.
