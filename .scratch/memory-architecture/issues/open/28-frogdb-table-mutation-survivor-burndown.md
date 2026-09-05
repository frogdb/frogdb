# 28: burn down the frogdb-table mutation survivors the lock did not force

Status: needs-triage
Type: AFK
Origin: [issue 22](22-lock-memory-spec.md) audit, 2026-09-05 — the lock's mutation run over
`frogdb-table` left survivors the audit did not chase
Area: frogdb-table
Phase: 4 — keyspace structure

## Why

Locking [specs/memory.md](../../../../specs/memory.md) set a **0.85** mutation gate on
`frogdb-table`, and the crate clears it. Clearing a gate is not the same as having no gaps,
and the run named the gaps precisely. Two of the survivors are not equivalent mutants and
not leaks — they are real holes in what the split path is tested for:

* `Table::split`, `Segment::alloc(depth + 1)` with `+` mutated to `*`: the new half is
  allocated at its parent's local depth instead of one deeper. Every test still passes.
* `Table::split`, `let stride = 1usize << (depth + 1)` with `+` mutated to `*`: the
  directory walk strides by half what it should and rewrites entries that belong to the low
  half. Every test still passes.

Both mean the same thing: the suite checks that keys round-trip across growth, and does not
check that the *directory* is what it should be after a split. A structural assertion —
every directory entry points at the segment whose local depth and route bits it agrees
with — kills both and is worth more than either individually.

The rest are ordinary untested arithmetic in the 2Q eviction path (`cold_candidates`,
`reconcile`, `nominate_from`) and in `scan`/`next_cursor`, where the existing tests assert
the end-to-end guarantee and not the intermediate figures.

## What to do

1. Add the directory-consistency assertion after growth and after an explicit `split`, and
   confirm it kills the two `split` survivors above.
2. Force the eviction-path arithmetic: `cold_candidates`' want/stride computation,
   `reconcile`'s comparison, `nominate_from`'s modular walk. These are behaviour
   [specs/memory.md](../../../../specs/memory.md) FM-MEMORY-005 and FM-MEMORY-007 already
   contract, so the tests belong in `frogdb-table` where `cargo mutants -p frogdb-table`
   can see them.
3. Force `scan`'s masking and `next_cursor`'s reverse-bit increment directly, rather than
   only through the end-to-end cursor guarantee.
4. Anything still surviving after that is documented at the code with why it is
   unobservable, per the locked-area rule — no blanket skips.

## Acceptance criteria

- `just mutants frogdb-table` + `just mutants-gate frogdb-table 0.85` still pass, at a
  measurably higher score, with the two `split` survivors killed.
- Every remaining survivor in the crate carries a mutation note at the code.
- No behaviour change: this is a test-coverage issue against a locked spec, so any change
  to what the code *does* is a spec-first change and belongs in its own issue.

## Out of scope

Raising the gate itself. The gate number is set in `CLAUDE.md` and the ADR-0006 addendum;
moving it is a decision about the whole area, not a side effect of one crate's burndown.
