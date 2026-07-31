# testing-improvements-round2 — round 2 testing-gap audit

State: active

Round 2 of the testing-gap audit (2026-07-28), targeting what
[round 1](../testing-improvements/) did not reach: per-crate command-level and unit-level
depth across scripting, server, commands, protocol, search, core, cluster, vll, persistence,
replication. 15 parallel agents, 249 findings.

**No issues have been filed yet** — this round is still at the proposal stage. Read
`MASTER.md` first.

## Layout

| path | what |
|---|---|
| `BRIEF.md` | the shared brief every agent worked from — scoring rubric, abstraction-boundary ladder, deliverable format |
| `MASTER.md` | consolidated findings: cross-cutting themes, ~40 suspected live defects, tests that cannot fail, dead code, open decisions. **Start here** |
| `INFRASTRUCTURE.md` | all ~18 requested infrastructure items, tiered A/B/C with LOE provenance. Supersedes `MASTER.md` §6 |
| `proposals/` | the 15 per-area agent proposals |

## Scoring

`Priority = 3·Severity + 2·Likelihood − Effort` — severity weighted highest, effort
negatively weighted so high-effort/low-impact work is explicitly deprioritised.

## Known blockers

Two verified coverage-pipeline defects make every coverage number in this repo untrustworthy
until fixed — see `MASTER.md` §1. `target/llvm-cov/lcov.info` carries essentially no test data
(29 of 34,644 FNDA nonzero), and `depth.json` `class_counts` are inflated ~7× by
monomorphization duplicates. The CI coverage number is currently meaningless.

Infrastructure item **I10** (fuzzing) is not a new item — it is
`../testing-improvements/issues/open/40`, reopened.
