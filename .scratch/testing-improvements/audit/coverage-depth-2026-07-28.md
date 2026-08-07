# Coverage depth — 2026-07-28 (SUPERSEDED)

**Superseded by [`coverage-depth-2026-08-07.md`](coverage-depth-2026-08-07.md)**, regenerated
from the fixed pipeline (issues 27 + 28).

This 2026-07-28 report had two defects, both fixed:

- **Inflated function classes.** `class_counts` were computed over raw `llvm-cov export`
  records — one per monomorphization plus a zeroed `::<_>` placeholder — so a single generic
  source function was counted many times. `untested` read **14849**; the deduped figure over
  source functions is **2414** (raw records now shown alongside as `class_counts_raw`:
  15791). See issue 28.
- **False equality claim.** It stated the de-duplicated per-file line total "matches
  `llvm-cov export --format=lcov` exactly (all … files)". That was never asserted by the
  pipeline. The regenerated report states only what is true — the dedup uses the same
  per-line `DA` counting as the lcov, and `coverage-depth.py report` now cross-checks the two
  totals when `lcov.info` is present (lines-found agree within tolerance; lines-hit are
  suite-dependent and reported as informational).

The line total in the old report was itself suspect because the sibling `just coverage-lcov`
artifact it was compared against was a stale, near-empty file (issue 27); the re-baselined
line coverage is **86.6%** (`../coverage-summary.md`).
