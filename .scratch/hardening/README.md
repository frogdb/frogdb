# hardening — foundation-hardening campaign

State: active

Working directory for the campaign that locks the four core areas — **transactions,
persistence, replication, cluster ops** — to near-zero correctness bugs with written-down
failure modes. The agent-facing summary (phase table, command surface, out-of-scope list,
locked-area rules) is [`docs/agents/hardening-campaign.md`](../../docs/agents/hardening-campaign.md);
this directory holds the artefacts it refers to.

Each area runs the same sequence: **extract → failure-mode spec → close known bugs →
mutation-test → fill gaps → lock**. Areas are strictly serial, one PR per step.

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
| `specs/replication-failure-modes.md` | draft — phase 3, whole area specced (FM-REPLICATION-001..053) |
| `specs/blocking-failure-modes.md` | draft |

## Issues

Filed under [`issues/`](issues/) per the
[issue-tracker conventions](../../docs/agents/issue-tracker.md) — `Status:` line and
`open/`|`done/` subdirectory must agree, enforced by `just scratch-check`.

Note the redis-regression suite is **frozen**: a red nightly `regression-run` gets an issue
here, not an inline fix.
