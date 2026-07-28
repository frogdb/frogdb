# Fuzzing is manual-only with no persisted corpus — 33 parsers effectively unfuzzed

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 2/3 (score 4)
Area: Jepsen harness / fuzzing infra (area G)

## Context

`fuzz.yml` is `workflow_dispatch`-only (line 12) — it never runs on a schedule or on PRs — and
each invocation runs with `-max_total_time=10` per target (confirmed `fuzz.py:39`). Worse,
`testing/fuzz/.gitignore` excludes both `corpus/` and `artifacts/`, so every run — manual or
otherwise — starts from an empty corpus; nothing learned by a previous fuzzing session is ever
retained. With 10 seconds per target and no persisted corpus, the 33 parser fuzz targets in the
suite are effectively never meaningfully fuzzed in practice, despite the infrastructure existing.

Verdict (adversarial pass): CONFIRMED L2/C2 (`fuzz.py:39 max_total_time=10`; corpus gitignored
confirmed).

## What to build

1. A scheduled (cron) CI job running the fuzz targets with a multi-minute budget per target
   instead of the current manual 10-second `workflow_dispatch`.
2. Corpus persistence across runs — cache or artifact-based, so each scheduled run builds on
   prior findings instead of restarting from empty.
3. A PR-triggered job that replays the persisted corpus (not full fuzzing) as a fast regression
   check, catching parser regressions against previously-found inputs without paying the full
   fuzzing time budget on every PR.

## Acceptance criteria

- [ ] New/updated CI workflow runs fuzz targets on a schedule (e.g., nightly or weekly cron), not
      only via manual `workflow_dispatch`.
- [ ] Scheduled run budget is multi-minute per target (materially more than the current 10s).
- [ ] Corpus is persisted between scheduled runs (CI cache or uploaded/downloaded artifact keyed
      appropriately) rather than starting empty every time.
- [ ] A PR-triggered job replays the persisted corpus against each fuzz target as a fast
      regression gate.
- [ ] `testing/fuzz/.gitignore` reviewed — corpus persistence approach documented (cache/artifact,
      not necessarily committing corpus to git).

## Blocked by

None - can start immediately

## References

- `.github/workflows/fuzz.yml:12`
- `testing/fuzz/fuzz.py:39`
- `testing/fuzz/.gitignore`
- `.scratch/testing-improvements/audit/G-jepsen-harness.md` (`fuzzing-not-continuous`)
- `.scratch/testing-improvements/audit/verdicts-G.md`

## Resolution

Rewrote the generated `fuzz.yml` (authored in the workflow-gen DSL, not hand-edited)
from a single `workflow_dispatch`-only 10s-per-target smoke into a two-job continuous
fuzzing workflow. All 34 targets are now covered.

**Design** (`.github/workflows/workflow_gen/src/workflow_gen/workflows/fuzz.py`):

- **Triggers**: nightly `schedule` (cron `41 2 * * *`, off-hour and distinct from
  jepsen `37 5`/`37 6` and concurrency `14 3`), `pull_request` to `main`, and
  `workflow_dispatch` with a `duration` input. Two jobs are gated by
  `github.event_name` so the right one fires per trigger.
- **`fuzz-campaign`** (schedule / manual, `if: github.event_name != 'pull_request'`,
  360-min timeout): loops every `cargo fuzz list` target for `-max_total_time` seconds,
  default **180s/target** (~100 min of fuzzing across 34 targets, vs. the old fixed
  10s), overridable via the `duration` dispatch input. The loop accumulates a nonzero
  exit (mirrors the Jepsen suite loop) so one crash doesn't hide later crashes.
- **`corpus-replay`** (PR, `if: github.event_name == 'pull_request'`, 60-min timeout):
  restores the persisted corpus and replays every entry with libFuzzer `-runs=0` (run
  the corpus once, no mutation) — a fast regression gate that pays build + replay time
  only, no fuzzing budget. Targets with a cold/empty corpus are skipped, not failed.

**Corpus strategy** — GitHub Actions cache (not committed to git):

- Chosen over artifacts because caches created on the default branch are readable by PR
  branches, which is exactly what lets the PR replay job read the corpus the nightly
  campaign grew; the artifact download-latest-from-previous-run dance is more code.
- Campaign uses combined `actions/cache` with save key
  `fuzz-corpus-${{ github.run_id }}-${{ github.run_attempt }}` (always unique → the
  post-job save always writes a fresh entry, even on failure) and `restore-keys:
  fuzz-corpus-` (restores the newest prior corpus → monotonic growth across runs).
- PR replay uses the restore-only sub-action `actions/cache/restore` (new `CACHE_RESTORE`
  constant) so it never litters the cache with per-PR entries.
- The instrumented build is amortised separately by `Swatinem/rust-cache` (`shared-key:
  fuzz`), unchanged.

**Crash artifacts**: both jobs upload `testing/fuzz/artifacts/` (cargo-fuzz's
reproducer tree) `if: failure()` with `if-no-files-found: error` as the `fuzz-artifacts`
workflow artifact.

**`.gitignore`**: kept `corpus/`/`artifacts/`/`target/` ignored (corpus is not
committed); added a header comment documenting the cache-based persistence and the
artifact upload, pointing at the DSL module.

**Toolchain**: unchanged from the prior workflow — nightly Rust
(`dtolnay/rust-toolchain@nightly`) + `cargo install cargo-fuzz` + libclang; cargo-fuzz
requires nightly and is orthogonal to the mise-pinned stable toolchain, so the workflow
does not use the mise step.

**Code touched**: `constants.py` (add `CACHE_RESTORE`), `helpers.py` (add generic
`cache_step` builder), `workflows/fuzz.py` (rewrite), regenerated `.github/workflows/fuzz.yml`,
`testing/fuzz/.gitignore`.

**Validation**:

- `just workflow-gen --check` → all 9 workflows OK (fuzz.yml matches the DSL).
- `actionlint .github/workflows/fuzz.yml` → clean (includes shellcheck on the run scripts).
- `just lint-py` (ruff check) → all checks passed; `just fmt-py` clean.
- Local smoke `just fuzz glob_match 10` proved the exact CI invocation path
  (`cargo +nightly fuzz run <target> --fuzz-dir testing/fuzz -- -max_total_time=…`):
  cargo-fuzz resolved the target and built it with `libfuzzer-sys` instrumentation. Full
  workspace compile is slow on the throttled dev box; heavy fuzz runs are not needed
  locally per the task scope.
