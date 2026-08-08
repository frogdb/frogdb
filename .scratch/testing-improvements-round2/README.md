# testing-improvements-round2 — round 2 testing-gap audit

State: active

Round 2 of the testing-gap audit (2026-07-28), targeting what
[round 1](../testing-improvements/) did not reach: per-crate command-level and unit-level
depth across scripting, server, commands, protocol, search, core, cluster, vll, persistence,
replication. 15 parallel agents, 249 findings.

The 249 findings are filed as **95 issues** under `issues/open/`. The proposals stay as the
source of record — every issue cites the finding it came from, so the original wording and
evidence is always one hop away. Read `MASTER.md` first.

## Layout

| path | what |
|---|---|
| `BRIEF.md` | the shared brief every agent worked from — scoring rubric, abstraction-boundary ladder, deliverable format |
| `MASTER.md` | consolidated findings: cross-cutting themes, ~40 suspected live defects, tests that cannot fail, dead code, open decisions. **Start here** |
| `INFRASTRUCTURE.md` | all 18 requested infrastructure items, tiered A/B/C with LOE provenance. Supersedes `MASTER.md` §6 |
| `proposals/` | the 15 per-area agent proposals — the source of record for every finding |
| `re-triage-2026-08-06.md` | **re-baseline of every open issue against the post-hardening tree** — verdict per issue, the 18 confirmed live defects, and what the campaign did and did not close |
| `issues/open/` \| `issues/done/` | the 95 filed issues — 70 open; closed ones kept only where referenced, rest pruned (git history) |

## Re-triage, 2026-08-06

The issues were filed on 2026-07-28, before the foundation-hardening campaign ran. Every one of
the 82 then-open issues was re-checked against the current tree on 2026-08-06 and carries a
`## Re-triage 2026-08-06` section with its verdict and refreshed `file:line` evidence: **2 fixed,
5 superseded, 20 partially-fixed, 55 still-valid**. Read
[`re-triage-2026-08-06.md`](re-triage-2026-08-06.md) before picking anything up from this
directory — a good deal of the original evidence has moved with the crate extractions, and 18
issues are confirmed *live production defects* rather than test gaps.

## Issue number ranges

Numbers are allocated by kind, so a range tells you what a number is without opening it.

| range | count | what | source |
|---|--:|---|---|
| 01–18 | 18 | infrastructure items — issue `NN` **is** `I<NN>` | `INFRASTRUCTURE.md` |
| 19–26 | 8 | cross-cutting themes T1–T8 — each is *one* piece of work, not N | `MASTER.md` §2 |
| 27–28 | 2 | coverage-pipeline defects (see Known blockers below) | `MASTER.md` §1, §7 D3 |
| 29–32 | 4 | open decisions D1–D4 — `ready-for-human`, blocking | `MASTER.md` §7 |
| 33 | 1 | tests that cannot fail — repair-or-delete sweep | `MASTER.md` §4 |
| 34 | 1 | dead code — deletion sweep | `MASTER.md` §5 |
| 35–76 | 42 | suspected live defects, in `MASTER.md` §3 table order | `MASTER.md` §3 |
| 77–91 | 15 | per-area residual test gaps, one per proposal (area 01 → 77 … 15 → 91) | `proposals/` |
| 92–95 | 4 | items the consolidation dropped, recovered during filing (below) | — |

Prefer the themes (19–26) over the individual findings they subsume — that is where the
leverage is. Prefer tier-A infrastructure (01, 07, 16, 17) before the findings it cheapens.

### 92–95 — recovered during filing

Four items that the audit produced but the consolidation into `MASTER.md` / `INFRASTRUCTURE.md`
left unowned. Recorded here because "the consolidation dropped it" is the failure mode this
directory exists to prevent.

| # | what | why it was unowned |
|---|---|---|
| 92 | `FrogDbResp2` lives in `server/`, not `protocol` — relocate or re-scope | recorded as a structural note, never `I`-numbered |
| 93 | whole-registry `CommandSpec` validation is a `debug_assert!`, asserted by no test | same |
| 94 | no FT.* test seam below the socket (proposals/10 F15) | requested as infrastructure, dropped from the I1–I18 consolidation |
| 95 | unbounded RESP nesting depth (08/F4) | split out of issue 70 so one open decision would not block four ready fixes |

## Suggested order

1. **27–28** — until the coverage pipeline is fixed, no number from this repo means anything.
2. **29–32** — four decisions that change what gets written everywhere else. As of the
   2026-08-06 re-triage only **31** (D3, coverage-tooling ordering) is still open; 29, 30 and 32
   were superseded by the hardening campaign's own practice.
3. **Tier-A infrastructure** (01, 07, 16, 17) — ~2.5 days, drops a large fraction of the
   remaining work by one or two effort levels.
4. **35–76**, security and silent-data-loss first — these are suspected *live defects*, not
   test gaps; their proposed tests fail against today's code.
5. **19–26**, then **77–91**. Fold 92–95 in where they touch: 94 gates the search findings
   in 86, and 92 re-scores the protocol findings in 84.

## Scoring

`Priority = 3·Severity + 2·Likelihood − Effort` — severity weighted highest, effort
negatively weighted so high-effort/low-impact work is explicitly deprioritised.

## Known blockers

Two verified coverage-pipeline defects make every coverage number in this repo untrustworthy
until fixed — see `MASTER.md` §1. `target/llvm-cov/lcov.info` carries essentially no test data
(29 of 34,644 FNDA nonzero), and `depth.json` `class_counts` are inflated ~7× by
monomorphization duplicates. The CI coverage number is currently meaningless.

Infrastructure item **I10** (fuzzing), filed here as issue 10, is not a new item — the work
is tracked at issue 40 in [`../testing-improvements/issues/`](../testing-improvements/issues/),
reopened.
