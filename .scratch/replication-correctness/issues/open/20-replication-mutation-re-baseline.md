# 20 — Mutation re-baseline for the replication crates

Status: needs-triage

## Parent

[PRD](../../PRD.md) §3 W6; exit criterion 4. Split out of issue 14, whose other two pieces (spec
cross-reference + per-area catalog lint) landed without it — see `issues/done/14-…`.

## What to build

**Mutation re-baseline on current code**, for both crates: `just mutants frogdb-replication` +
`just mutants-gate frogdb-replication 0.85`, and the same for `frogdb-replication-runtime`. Record
not just the score but the **in-crate share of forcing tests** — the number ADR 0004 says the score
is really measuring, given that the crate's headline figure was reached by moving tests down rather
than by removing the cross-crate dependency. Re-check the ADR 0004-era survivors specifically
(`apply_single`, `apply_transaction`, `apply_group`, `export_live_dataset`, `install`,
`read_snapshot`) and record whether issue 05's R6 and the runtime crate's first `[dev-dependencies]`
moved them.

Nothing in issue 14's other two pieces changed a mutable line in either crate — the catalog gained
doc comments only — so the baseline this issue produces is the first one that reflects the property
work.

## Acceptance criteria

- [ ] `just mutants-gate frogdb-replication 0.85` and `just mutants-gate
      frogdb-replication-runtime 0.85` pass on current code
- [ ] In-crate forcing-test share recorded alongside each score, with the ADR 0004 survivor list
      re-checked and its status recorded

## Blocked by

- Issues 05 and 10 — both move the mutation numbers (05 adds properties R2-R6 in-crate, 10
  restructures the session phase machine). Running the baseline before they land means running it
  twice; that is exactly why issue 14 shipped without it rather than recording a number with a
  known expiry date.
