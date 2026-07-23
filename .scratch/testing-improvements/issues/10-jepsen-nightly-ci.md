# Jepsen suite absent from CI — no automated distributed-regression signal

Status: done
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

- [x] `jepsen-nightly.yml` (or equivalent) exists, cron-triggered, runs on Blacksmith
- [x] Runs `raft` and `replication` suites at minimum; artifacts uploaded on failure
- [x] Job depends on / is sequenced after the checker fixes in tasks 04-08 (slot-migration checker,
      raft-chaos checker, orphaned workloads, membership-under-fault) so it isn't gated on
      known-broken verdicts (04-08 have landed; wired only now)
- [ ] At least one full run completed in CI with results linked from this issue (requires the
      workflow on the default branch to fire; local end-to-end smoke of the invocation path passed —
      see Resolution)

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

## Resolution

Landed after the gating checker fixes (tasks 04-08) — the slot-migration / raft-chaos /
key-routing checkers now assert data, and the orphaned cluster + failover workloads are wired —
so the nightly produces real signal rather than false-green.

### Workflow design

New generated workflow `.github/workflows/jepsen-nightly.yml`, authored as a `workflow_gen` DSL
module (`.github/workflows/workflow_gen/src/workflow_gen/workflows/jepsen_nightly.py`, registered
in `render.py`; the `.yml` is generated, never hand-edited — regenerate with `just workflow-gen`).
It clones the `concurrency-nightly.yml` pattern: single Blacksmith job
(`blacksmith-4vcpu-ubuntu-2404`, x86 to match production + the other CI jobs), `timeout-minutes:
360`, cron + `workflow_dispatch`.

Steps: checkout → `mise` (installs only `just uv java` — no host Rust; the server compiles inside
Docker) → install Leiningen (guarded bootstrap of the `stable` lein script; Jepsen's control node
runs `lein` on the host and lein has no mise plugin) → **build the debug image once** via
`just docker-build-debug` (Dockerfile.builder, in-Docker build) → run suites → `just jepsen-summary`
into `$GITHUB_STEP_SUMMARY` (always) → upload `testing/jepsen/frogdb/store/` on failure.

Suites are run with `uv run testing/jepsen/run.py run --suite <s> --no-build --teardown --no-color`,
one invocation per suite in a loop that continues past a failing suite and fails the step at the end
(full nightly picture, job still goes red). We deliberately bypass `just jepsen-suite`, which forces
`run.py --build` in its `cross`/zigbuild mode — the path issue 08 flagged as macOS-broken and, here,
redundant (the image is already built). `--no-build` guarantees run.py never falls back to that host
build path in CI.

Two cadences in one workflow, selected by `github.event.schedule` (passed via step `env`, not inline
`${{ }}`, to keep the untrusted dispatch input/schedule out of the command text):
- Nightly (`37 5 * * *`): `single crash replication raft` — covers the required `raft` +
  `replication` and equals the `all` suite (single ∪ crash ∪ replication ∪ raft).
- Weekly (`37 6 * * 0`, Sun): the above plus the heavier `raft-extended`, `replication-extended`,
  `register-fault` suites.
- A `suites` `workflow_dispatch` input overrides selection for manual runs.

### Runtime estimate

From `run.py` `TESTS` time-limits + topology bring-up: single ~15 tests, crash ~26, replication ~7,
raft ~18. Sum of per-suite time-limits ≈ core nightly ~1.5-2h wall (dominated by the raft suite and
the ~20-30m in-Docker debug build); weekly adds ~20-30m for the 4+2+3 extended/fault tests. Both sit
well under the 360m ceiling.

### Validation performed (cannot run GH Actions locally)

- `just workflow-gen` + `just workflow-gen --check`: all 9 workflows `OK` (jepsen-nightly generated,
  reproducible, and recognized by the check's known-file allowlist).
- `actionlint .github/workflows/jepsen-nightly.yml`: clean (includes shellcheck of the embedded
  run scripts).
- `ruamel` structural parse: `on` has both crons + `workflow_dispatch`; single job with the 7
  expected steps.
- Every invocation checked against source: `run.py run --help` confirms `--suite/--no-build/
  --teardown/--no-color`; `run.py list` confirms all suite names used exist
  (`single`, `crash`, `replication`, `raft`, `raft-extended`, `replication-extended`,
  `register-fault`); `just docker-build-debug` and `just jepsen-summary` exist in the Justfile.
- **End-to-end smoke** of the exact CI path against the existing `frogdb:latest` image:
  `uv run testing/jepsen/run.py run register --no-build --teardown --no-color --time-limit 5`
  → single topology up → Knossos verdict `Everything looks good!` → `register: PASS` → teardown,
  exit 0. `just jepsen-summary` then rendered the pass/fail table (the `run.py summary` seam the
  workflow's summary step uses).

### Caveats / follow-ups

- The full nightly (esp. Elle/Knossos on the raft suite) has not yet run on a Blacksmith runner;
  4vcpu memory headroom for the 5-node raft cluster + checkers is unverified — a first real run may
  need a runner-size bump. This satisfies the acceptance criteria except the final "one full run
  completed in CI with results linked" box, which requires the workflow to be on the default branch
  to fire — link the first green run here once it lands.
- Whether Leiningen is pre-baked into the Blacksmith image is unknown; the install step is guarded
  (`command -v lein`) so a pre-baked lein is a no-op and an absent one is bootstrapped.
