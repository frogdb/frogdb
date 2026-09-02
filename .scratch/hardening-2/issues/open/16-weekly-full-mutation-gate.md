# Area mutation scores were measured once, at lock — re-measure them on a schedule

Status: ready-for-agent
Type: mechanism (enforcement gap)
Severity: likelihood 2/3 (a diff run cannot see a deleted or weakened test — tests are
`exclude_globs` — and 15 rows already postdate their area's last run), consequence 2/3 (an area
drifts below its gate while still carrying the LOCKED badge) — score 4
Area: campaign mechanism / CI
Blocked by: 13

## Problem

Each area's score (`frogdb-txn` 100%, `frogdb-persistence` 99.1%, `frogdb-replication` 98.7%,
`frogdb-cluster` 99.6%, …) was measured once, on 2026-08-01..05, on a testbox, by hand. Nothing
has re-measured since. The diff ratchet (issue 15) holds the line for *new code* only:

- deleting or weakening a test produces no mutant in the diff, so survivors that test used to
  kill come back silently;
- a refactor that moves code within a crate re-tests only the moved lines;
- the campaign-2 PRD's "15 rows postdate their area's lock … never having been in a mutation
  run" is this hole, found by hand.

## Design (ruled 2026-09-02)

**`mutants-weekly.yml`** — a new workflow in `workflow_gen` (sibling of the existing nightlies),
`schedule` weekly plus `workflow_dispatch` with an optional `crate` input for on-demand runs.
Weekly, not nightly: the score drifts slowly and the diff gate covers day-to-day.

- **Matrix = crate × shard.** Crates and gates come from the manifest (issue 13's parser).
  Shard count per crate from its size: 1 for `frogdb-txn` / `frogdb-vll` / the runtimes, 4 for
  `frogdb-persistence` / `frogdb-replication` / `frogdb-cluster` (`cargo mutants --shard k/n`).
  Each leg uploads its `mutants.out/outcomes.json` as an artifact.
- **Merge + gate job.** Downloads the legs, runs `scripts/mutants-gate.py` — extended to accept
  multiple `outcomes.json` files and sum them — with the crate's header `Gate:`. A leg that timed
  out or failed to build fails the crate (an incomplete run is not a score).
- **Runner / limits.** `ubuntu-latest`, `timeout-minutes: 240` per leg, `--jobs 2`. Promote a
  crate's legs to Blacksmith arm if they exceed that.
- **Failure handling.** Red workflow + job summary listing missed mutants per crate. Issue filing
  stays manual, matching the existing nightlies (a red `regression-run` gets an issue, not an
  auto-file).
- **Local parity.** `just mutants <crate>` / `just mutants-gate <crate>` unchanged apart from the
  gate lookup (issue 13); `--iterate` runs stay a local convenience (the CI run has no prior state
  to iterate from).

## Not in scope

- Per-row provenance (which rows were in which run). Deferred; campaign-2 W3 re-runs the gates
  over the post-lock rows once by hand.
- Sub-tree gates (`frogdb-core/src/shard`) — arrives with the `crate/path` manifest form.

## Forcing test

`just workflow-gen --check` passes. A `workflow_dispatch` run with `crate: frogdb-txn` completes
and the merge job prints `score: 100.0% (gate: 90.0%)` from the header, with no threshold typed
anywhere in the workflow. `mutants-gate.py` unit case: two outcome files with 3 caught + 1 missed
each score 75%, not 75%/75% separately.
