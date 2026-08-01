# `lcov.info` contains essentially no test-execution data, and nightly CI publishes a coverage number from it

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: MASTER.md §1 "Data-quality caveat" · MASTER.md §7 D3
Score: not scored — coverage-pipeline defect, measured directly by the coordinator rather than found by the finding rubric
Area: tooling / CI — `Justfile`, `.github/workflows/coverage-nightly.yml`

## Context

`target/llvm-cov/lcov.info` — the artifact `just coverage-lcov` produces and the nightly workflow
consumes — carries almost no execution counts. The only crate with nonzero function counts is
`config-derive`, a build-time proc macro, i.e. code that runs during compilation rather than during
tests. Everything else is emitted with zero hits.

The consequence is direct: `coverage-nightly.yml` sums `LH`/`LF` out of that file and posts a total
line-coverage percentage into the job summary against a recorded baseline. **That number is
meaningless.** Two agents in this audit independently distrusted their inputs and re-measured; both
confirmed it. Until this is fixed, no coverage number from this repo should be quoted anywhere.

This is filed as its own issue, independent of the audit's findings — nothing in §2–§5 depends on
it, and the strongest findings in the audit were deliberately anchored on `depth.json`'s per-file
`line_counts` or on read source rather than on this artifact.

## Evidence

Measured directly against `target/llvm-cov/lcov.info` (9 MB, generated 2026-07-28):

- **`FNDA` (function hit counts): 29 nonzero out of 34 644** — 0.1%. Every nonzero record belongs
  to `config-derive`.
- **`DA` (line hit counts): 323 nonzero out of 128 130** — 0.3%.

Production path:

- `Justfile:77-79` — `coverage-lcov:` runs
  `cargo llvm-cov nextest --all --lcov --output-path target/llvm-cov/lcov.info`.
- `.github/workflows/coverage-nightly.yml:41` — `run: just coverage-lcov`.
- `.github/workflows/coverage-nightly.yml:44-67` — the "Coverage summary" step `awk`s `LH:`/`LF:`
  out of `lcov.info`, computes `pct`, and writes
  `Total line coverage: **${pct}%** (${lh}/${lf} lines)` to `$GITHUB_STEP_SUMMARY` against
  `Baseline (2026-07-22 audit, .scratch/testing-improvements/audit/coverage-summary.md): **84.0%**`.
  The step's only failure mode is a *missing* file (`:46-51`) — an empty-of-data file passes.
- `.github/workflows/coverage-nightly.yml:69-77` — the same file is uploaded as the
  `coverage-lcov` artifact with `if-no-files-found: error`, so downstream consumers get it too.

The comparison artifact is sound: `just coverage-depth` (`Justfile:82-85`,
`scripts/coverage-depth.py`) produces per-file `line_counts` in `target/llvm-cov/depth/depth.json`
with real execution counts over the same suite, which is how the discrepancy was caught.

## What to fix

1. Root-cause why `cargo llvm-cov nextest --all --lcov` emits zero counts for the workspace while
   the depth pipeline's own `llvm-profdata merge` + `llvm-cov export` over the same suite does not.
   The depth pipeline uses a per-test `LLVM_PROFILE_FILE` set through a cargo target-runner
   (`scripts/cov-runner.sh`); the plain recipe does not, and the likely fault is profile files not
   being collected or merged for the test binaries.
2. Fix the recipe so `lcov.info` reflects the suite.
3. Add a guard to `coverage-nightly.yml` that fails the job when the report is structurally
   implausible — nonzero-`DA` ratio below a floor, or `lh == 0` — rather than only when the file is
   absent.
4. Re-baseline: the `84.0%` figure in the workflow and in
   `.scratch/testing-improvements/audit/coverage-summary.md` was computed from this pipeline and
   must be recomputed once it is fixed.

## Acceptance criteria

- [ ] After `just coverage-lcov`, the nonzero-`FNDA` count is a large fraction of 34 644, not 29,
      and the nonzero-`DA` count is a large fraction of 128 130, not 323.
- [ ] The nonzero counts are spread across workspace crates, not concentrated in `config-derive`.
- [ ] `.github/workflows/coverage-nightly.yml` fails the job (not just prints a note) when
      `lcov.info` is present but carries implausibly few nonzero counts.
- [ ] The `84.0%` baseline string in `coverage-nightly.yml:63` is replaced with a figure recomputed
      from the fixed pipeline, and the audit summary it cites is updated in the same change.
- [ ] The total from the fixed `lcov.info` agrees with the de-duplicated per-file line view the
      depth pipeline reports over the same suite.

## Test boundary

**Not a product test.** The verification is a tooling assertion: run the recipe and check the
artifact's own statistics, plus a CI guard that makes the check permanent. There is no product
behaviour to exercise, so none of the five levels applies.

## Depends on

Nothing. Issue 28, `.scratch/testing-improvements-round2/issues/`, is the sibling coverage-pipeline
defect and should be scheduled with it; issue 31,
`.scratch/testing-improvements-round2/issues/`, is the decision on when to do both relative to the
testing work.
