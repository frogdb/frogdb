# 02: Bidirectional markdown↔Quint lint enforcement

Status: ready-for-agent

## What to build

Today the spec linter enforces one direction only: every `TR-`/`FM-`/`INV-` id cited in a
`.qnt` leading comment block must resolve to a real spec row / catalog entry
(`scripts/spec-lint.py:check_quint_citations`). The reverse direction does not exist — no
markdown row names a quint invariant, and the linter never reads model bodies — so nothing
stops a model and its rows from drifting apart silently.

Close the loop, same contract shape as the FM rows' forcing-tests discipline:

- **Spec side:** an opt-in `Model:` cell (or column) on TR/FM rows naming the covering
  invariant(s) as `<file>::<inv_name>`, list-valued (one invariant may cover several rows;
  several invariants may jointly cover one row). Rows without the cell are exempt — models
  cover a deliberate subset of rows, and coverage ratchets up per area over time.
- **Linter side:** extract invariant/temporal names from model bodies by regex on
  house-style definitions (`val inv_*`, `temporal *`) — stays pure-Python, no quint binary
  at commit time. Enforce both directions:
  1. every `Model:` cell entry resolves to a real definition in the named `.qnt` file;
  2. every `inv_*`/temporal definition in a model is named by at least one `Model:` cell
     (an invariant no row claims is either dead or its row forgot the cell — both are
     findings). Ghost/helper `val`s not matching the `inv_`/temporal naming rule are exempt,
     which makes the naming convention load-bearing: document it in the linter docstring.
- **Tests:** extend `scripts/tests/test_spec_lint.py` — dangling `Model:` entry is an error,
  unclaimed invariant is an error, list-valued cells parse, exempt rows stay silent, regex
  ignores commented-out definitions.
- **Backfill:** add `Model:` cells for every row the two existing cluster models' headers
  cite, so the check lands non-vacuous.

Known limitation to state in the linter docstring: linkage ≠ fidelity. The lint proves the
named invariant exists, not that it encodes the row's semantics — mutation review owns that.

## Acceptance criteria

- [ ] `Model:` cell format specified in the spec authoring doc and parsed by the linter
- [ ] Both directions enforced with tests (dangling entry, unclaimed invariant, list cells, exemption)
- [ ] Existing cluster models + their rows backfilled; `just lint-spec` green and summary line reports the new counts
- [ ] `just lint-spec` stays quint-binary-free at commit time

## Citation-accuracy findings to fold into the backfill

From the phase-2 final review (`.superpowers/sdd/2026-08-13-phase2-cluster-quint-plan/final-review.md`),
four findings about how existing citations inflate or understate coverage — relevant when this
issue's backfill counts what the two cluster models currently cite:

- [t1 m5] disclaimer-only ids (cited for context, not machine-checked) inflate the citation
  count the same as a real `Model:`-style linkage would; the backfill should not carry them
  forward as if they were coverage.
- [t2 N4] `inv_migration_endpoints_valid` cites `INV-REF-2` without it appearing in the id's own
  catalog entry — an uncited-in-the-other-direction mix worth checking once the reverse-direction
  lint (this issue's main deliverable) exists.
- [t2 N5] `INV-SLOT-1` is cited only to be rejected/disclaimed, not asserted — same
  disclaimer-vs-linkage distinction as [t1 m5], different id.
- [t2 M18] the in-module disclaimer scan window (how far a "not machine-checked" disclaimer is
  read to apply from its citation) is informal; the backfill should make explicit which citations
  in the two cluster models it does and doesn't cover for this reason.

## Blocked by

- 01 (library extraction moves/renames definitions; land layout first so backfill doesn't churn)
