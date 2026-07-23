# Fuzzing is manual-only with no persisted corpus — 33 parsers effectively unfuzzed

Status: needs-triage
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
