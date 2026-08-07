# `depth.json` `class_counts` are computed over duplicate function records — `untested` is inflated ~7×

Status: done
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: MASTER.md §1 "Data-quality caveat" · MASTER.md §7 D3
Score: not scored — coverage-pipeline defect, measured directly by the coordinator rather than found by the finding rubric
Area: tooling — `scripts/coverage-depth.py`, `docs/agents/coverage-depth.md`

## Context

`target/llvm-cov/depth/depth.json`'s `class_counts` are computed over the export's raw
`functions[]` array, which carries one record per **monomorphisation** plus `::<_>` generic
placeholder records (one copy of which is zeroed). The same function is therefore counted several
times, and because the zeroed copies land in the `untested` bucket, that class is inflated by
roughly 7× against a name-dedupe and roughly 7× again against a span-dedupe.

The class counts are what the audit brief handed to 15 parallel agents as the ranking signal
(`BRIEF.md:52` quotes `untested (14849 fns)`, `single-test (6475)`, `monoculture (4325)`,
`hot-but-shallow (13)`), and they are what the generated report and the 2026-07-28 audit summary
publish. Separately, the generated report asserts that its de-duplicated line figure "matches
`llvm-cov export --format=lcov` exactly" — **that claim is false**, because the lcov artifact it
claims to match carries essentially no test-execution data (issue 27).

The per-file `line_counts` in `depth.json` are sound. Every audit finding whose sole evidence was a
depth class was re-checked against span-deduped data before being reported, and all survived.

## Evidence

Measured directly:

- **`untested` 14 849 raw → 7 008 name-deduped → 2 163 span-deduped.**

Two of the re-checks came out sharper under span-dedupe, and are the record of why this matters:

| function | span-deduped | reading |
|---|---|---|
| `failure_detector.rs:330 trigger_auto_failover` | 0 tests, **0/104 regions** | genuinely never executed *(04/F2)* |
| `routing.rs:197 execute_cross_shard_copy` | 0 tests, **0/102 regions** | genuinely never executed *(03/F2)* |
| `strategies.rs:153 merge_sum_integers` | 2 tests, 10/10 regions | full region coverage, both tests all-success *(03/F1)* |
| `acl/categories/mod.rs:149 all_for_command` | **3551 tests, 233 560 execs, 7/7 regions** | maximally covered, and the covered path is `unwrap_or_default()` returning `[]` for 185 of 356 commands *(15/F1)* |
| `shard/types.rs:479 invalidate_keys_all_modes` | 37 tests, 16/18 regions | well tested — and the lazy-expiry callsite calls the *other* function *(02/F1)* |

Production path:

- `scripts/coverage-depth.py:499-507` — `index_functions`'s own docstring records that
  `--ignore-filename-regex` prunes `files[]` but **not** `functions[]`, so the export "still
  carries every dependency monomorphization instantiated into these binaries (24k of 26k entries
  for a single small crate)". It filters by *filename* only; it does not fold instantiations of the
  same function together before classifying.
- `scripts/coverage-depth.py:839` — the report emits: *"The de-duplicated figure is what the HTML
  gutter shows and matches `llvm-cov export --format=lcov` exactly."* That equality cannot hold
  while `lcov.info` carries 323/128 130 nonzero `DA` records (issue 27).
- `docs/agents/coverage-depth.md:63-77` — "Reading the classes" documents `untested` /
  `single-test` / `monoculture` / `hot-but-shallow` / `well-covered` / `covered` as one class per
  *function*, with no mention of monomorphisation duplicates. This is the doc that has to be
  corrected.
- `.scratch/testing-improvements/audit/coverage-depth-2026-07-28.md` and
  `.scratch/testing-improvements-round2/BRIEF.md:52` both publish the inflated raw figures.

## What to fix

1. Dedupe `functions[]` before classification — fold monomorphisations and drop `::<_>` placeholder
   records, keyed by demangled name *and* source span, so a function is classified once.
2. Recompute `class_counts` from the deduped set and publish both the raw and deduped totals so the
   difference is visible rather than silent.
3. Remove or correct the "matches `llvm-cov export --format=lcov` exactly" claim at
   `scripts/coverage-depth.py:839`. If the equality is to be kept, it must be re-established
   against a fixed `lcov.info` (issue 27) and asserted, not stated.
4. Correct `docs/agents/coverage-depth.md`: document the dedupe rule in "Reading the classes" and
   drop any claim of exact agreement with the lcov artifact.
5. Regenerate `.scratch/testing-improvements/audit/coverage-depth-2026-07-28.md` from the fixed
   pipeline, and annotate the round-2 `BRIEF.md` figures as superseded rather than editing history.

## Acceptance criteria

- [ ] `depth.json`'s `class_counts` are computed over deduped records; `untested` reports 2 163
      (span-deduped) rather than 14 849 for the 2026-07-28 suite, or the report explains in one
      line why the number differs.
- [ ] The report shows raw and deduped counts side by side.
- [ ] No claim of exact equality with `llvm-cov export --format=lcov` remains at
      `scripts/coverage-depth.py:839` unless it is backed by an assertion in the pipeline.
- [ ] `docs/agents/coverage-depth.md` documents the dedupe rule and contains no uncorrected
      equality claim.
- [ ] A regression test on the pipeline: a fixture export containing two monomorphisations plus one
      zeroed `::<_>` record for the same function yields exactly one classified entry.
- [ ] Per-file `line_counts` are unchanged by this work — they were already sound.

## Test boundary

**Not a product test.** The one mechanical check worth having is a fixture-driven unit test on
`scripts/coverage-depth.py`'s `index_functions` / classification step, asserting that duplicate
records collapse. There is no product behaviour to exercise, so none of the five levels applies.

## Depends on

Issue 27, `.scratch/testing-improvements-round2/issues/` — the equality claim in step 3 cannot be
re-established until `lcov.info` carries real data. Issue 31,
`.scratch/testing-improvements-round2/issues/`, is the decision on when to do both relative to the
testing work.

## Re-triage 2026-08-06

**Verdict: still-valid**

`scripts/coverage-depth.py` has had exactly two commits ever — `36d3f794` (the original pipeline)
and `e163ff9a` (2026-08-04, a cluster test-binary split that only touched suite naming). Nothing
addressed the dedupe. Criterion by criterion:

- `index_functions` (`scripts/coverage-depth.py:499-534`, old ref `:499-507`) still folds only by
  **mangled name** (`out: dict[str, FuncInfo]` keyed by `fn["name"]`) and still filters by filename
  only. Two monomorphisations of the same generic have different mangled names, so they are not
  folded, and the zeroed `::<_>` placeholder record survives as its own entry — which is exactly
  the span-dedupe gap the issue describes (criterion 1 unmet).
- `class_counts` is still computed over the un-span-deduped `fn_out` at
  `scripts/coverage-depth.py:764-766`, and `:800` emits only the single `class_counts` dict — no
  raw/deduped pair (criteria 1 and 2 unmet).
- The equality claim survives verbatim at `scripts/coverage-depth.py:838-839`: *"The de-duplicated
  figure is … and matches `llvm-cov export --format=lcov` exactly."* It is a bare string, backed by
  no assertion (criterion 3 unmet).
- `docs/agents/coverage-depth.md:61-72` "Reading the classes" is unchanged — still "Each function
  gets exactly one class", still no mention of monomorphisation duplicates (criterion 4 unmet).
- There is no test of any kind on the pipeline: `rg coverage-depth testing/ scripts/` matches only
  the script itself, and no `Justfile` recipe exercises it as a fixture (criterion 5 unmet).

Depends-on note: issue **27** is also still-valid, so criterion 3's equality can still not be
re-established.

The raw-count reproduction (`untested` 14 849 → 2 163 span-deduped) is
**unverified-by-execution** — the brief forbids running `just coverage-depth`, and there is no
`target/llvm-cov/depth/depth.json` on disk. The code path is unchanged, so the defect is presumed
to reproduce.

## Closing note 2026-08-07 (DONE)

Fixed and verified by executing the full `just coverage-depth` pipeline (8031 per-test profiles,
join hit-rate 100.00%). Measured, this suite:

- **`untested` 15 791 raw records → 2 414 source functions** (span-deduped). Total: **39 811
  monomorphization records → 17 115 source functions.** The deduped figure lands near the
  2 163 the audit predicted; the exact number differs because it is a fresh suite/toolchain, so
  the report publishes both `class_counts` (deduped) and `class_counts_raw` and prints the fold
  ratio, per the "explains in one line why the number differs" allowance.
- Full class table (deduped / raw): untested 2414/15791, single-test 5959/7949,
  monoculture 1679/5535, hot-but-shallow 2/13, covered 772/1351, well-covered 6289/9172.

Criterion by criterion:

- [x] **Dedupe before classification.** `dedupe_depths()` folds by
  `(strip_generics(demangled), file, line_start, line_end)`, drops `is_generic_placeholder`
  (`::<_>`) records, unions tests/suites, takes representative (max) region counts. Called from
  `build_depth` before `classify()`.
- [x] **Raw + deduped side by side.** `depth.json` carries `class_counts` (deduped),
  `class_counts_raw`, and a `dedup` block (raw_records / deduped_functions / note). The
  generated markdown renders a two-column "functions (deduped) | raw records" table.
- [x] **False equality claim removed.** The bare "matches `llvm-cov export --format=lcov`
  exactly" string is gone. The report now states the dedup uses the same per-line `DA` counting
  as the lcov, and `assert_lcov_agreement()` cross-checks the two totals when `lcov.info` is
  present (measured: lines-found 154 631 depth vs 153 502 lcov, agree within tolerance;
  lines-hit 132 959 vs 132 894, informational).
- [x] **Docs corrected.** `docs/agents/coverage-depth.md` gains a "One entry per source function
  (dedupe)" subsection under "Reading the classes"; no uncorrected equality claim remains.
- [x] **Fixture regression test.** `scripts/tests/test_coverage_depth.py` (recipe
  `just test-coverage-depth`): two monomorphizations + one zeroed `::<_>` for the same function
  → exactly one classified entry (`{"untested": 1}`), plus 5 more assertions. 6/6 pass.
- [x] **Per-file `line_counts` unchanged.** They are summed from `files[].segments`, never from
  `functions[]`; `test_line_counts_untouched_by_dedupe` locks this.

Files: `scripts/coverage-depth.py`, `scripts/tests/test_coverage_depth.py`, `Justfile`,
`docs/agents/coverage-depth.md`, regenerated
`.scratch/testing-improvements/audit/coverage-depth-2026-08-07.md` (2026-07-28 replaced with a
superseded stub), BRIEF.md / MASTER.md / proposals/09-scripting.md figures annotated.
