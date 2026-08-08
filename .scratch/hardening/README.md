# hardening — foundation-hardening campaign (complete)

State: active

Working directory for the campaign that locked the four core areas — **transactions,
persistence, replication, cluster ops** — to near-zero correctness bugs with written-down
failure modes. **The campaign exited 2026-08-05** (see
[`retrospective-2026-08-05.md`](retrospective-2026-08-05.md)); the follow-on is
[`hardening-2`](../hardening-2/). The locked-area rules agents must still follow live in
`CLAUDE.md` ("Locked core areas"); this directory holds the specs those rules point at,
plus the two remaining open issues.

Each area ran the same sequence: **extract → failure-mode spec → close known bugs →
mutation-test → fill gaps → lock**.

## Layout

| path | what |
|---|---|
| `specs/` | failure-mode specs, one per area — the contract a locked area is measured against |
| `issues/open/`, `issues/done/` | campaign issues, incl. anything a nightly `regression-run` turns up |
| `metrics/loop-cost.md` | warm inner-loop cost per area, recorded by `just loop-cost <area>` before and after each extraction |

## Failure-mode specs

Every `FM-<AREA>-NNN` row names the test that forces it in its `Forced by` cell, and every
named test carries a matching `// FM-<AREA>-NNN` comment. `just lint-failure-modes` enforces
both directions and runs as part of `just lint`. A spec whose header reads
`Status: LOCKED` is a contract: behaviour changes there are spec-first — edit the row, update
the forcing test, then change the code.

| spec | status |
|---|---|
| `specs/txn-failure-modes.md` | LOCKED 2026-08-01 (phase 1, `frogdb-txn` 100% vs 90% gate) |
| `specs/vll-failure-modes.md` | LOCKED 2026-08-01 (phase 1, `frogdb-vll` 100% vs 90% gate) |
| `specs/persistence-failure-modes.md` | LOCKED 2026-08-02 (phase 2, `frogdb-persistence` 99.1% / `frogdb-recovery` 100% vs 85% gate) |
| `specs/replication-failure-modes.md` | LOCKED 2026-08-04 (phase 3, `frogdb-replication` 98.7% / `frogdb-replication-runtime` 100% of viable vs 85% gate) |
| `specs/cluster-failure-modes.md` | LOCKED 2026-08-05 (phase 4, `frogdb-cluster` 99.6% / `frogdb-cluster-runtime` 99.0% vs 80% gate) |
| `specs/blocking-failure-modes.md` | draft |

## Issues

Filed under [`issues/`](issues/) per the
[issue-tracker conventions](../../docs/agents/issue-tracker.md) — `Status:` line and
`open/`|`done/` subdirectory must agree, enforced by `just scratch-check`.

The redis-regression suite was **frozen** during the campaign (a red `regression-run` got an
issue here, not an inline fix). That freeze ended at campaign exit on 2026-08-05 — the suite
builds in `just check` and runs in `just test` again, and a red compat test is now a normal
failure to fix.
