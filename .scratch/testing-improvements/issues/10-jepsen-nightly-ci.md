# Jepsen suite absent from CI — no automated distributed-regression signal

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 3/3, consequence 2/3 (score 6)
Area: jepsen

## Context

`grep -ri jepsen .github/` returns nothing — the Jepsen suite is not referenced anywhere in CI
workflows. The only automation is a manual `just` recipe (`Justfile:409-450`). This means
distributed-systems regressions (replication, cluster, raft) are only ever caught by someone
manually running the suite, which in practice means they usually aren't caught until much later, if
at all.

A working pattern already exists in this repo for exactly this kind of expensive, long-running,
nightly-cadence test job: `concurrency-nightly.yml` (the concurrency seed-sweep nightly CI job,
also referenced in `07-shuttle-multiwaiter-exactly-once-guard.md` and
`.scratch/concurrency-testing/issues/11-nightly-smoke-findings.md`). That workflow's structure
(cron trigger, Blacksmith runner, artifact upload on failure) is a directly reusable template.

## What to build

- New `jepsen-nightly.yml` (or weekly, if nightly proves too slow/expensive) GitHub Actions
  workflow, cloning the `concurrency-nightly.yml` pattern:
  - Cron-triggered (plus `workflow_dispatch` for manual runs).
  - Runs on a Blacksmith runner (per this repo's existing remote-execution convention).
  - Runs at minimum the `raft` and `replication` suites.
  - Uploads Jepsen run artifacts (history, logs, analysis) on failure.
- Do not wire this up before the checker fixes in tasks 04-08 land — running it against known-noop
  or known-blind checkers would produce false-green signal and waste the CI investment. Gate the
  initial job on those being fixed first.

## Acceptance criteria

- [ ] `jepsen-nightly.yml` (or equivalent) exists, cron-triggered, runs on Blacksmith
- [ ] Runs `raft` and `replication` suites at minimum; artifacts uploaded on failure
- [ ] Job depends on / is sequenced after the checker fixes in tasks 04-08 (slot-migration checker,
      raft-chaos checker, orphaned workloads, membership-under-fault) so it isn't gated on
      known-broken verdicts
- [ ] At least one full run completed in CI with results linked from this issue

## Blocked by

`04-jepsen-orphaned-workloads-wire-or-delete.md`, `05-jepsen-membership-under-fault.md`,
`06-jepsen-raft-chaos-blind-checker.md`, `08-jepsen-slot-migration-checker-noop.md` — wiring CI
before these checker fixes land would either run nothing meaningful or bake in false-green signal.
CI wiring itself can be drafted in parallel, but should not be turned on/merged as gating until
those land.

## References

- `.github/workflows/` — grep for `jepsen` returns nothing (confirmed absence)
- `Justfile:409-450` — manual `just` recipe, only existing automation
- `.github/workflows/concurrency-nightly.yml` — template pattern to clone
- Source: `.scratch/testing-improvements/audit/G-jepsen-harness.md` `jepsen-absent-from-ci`,
  `.scratch/testing-improvements/audit/verdicts-G.md` (same, "Fix noop/blind checkers first")
