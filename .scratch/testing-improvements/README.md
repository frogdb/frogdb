# testing-improvements — round 1 testing-gap audit

State: archive-of-record

**Do not delete this directory.** Ten tracked files outside `.scratch/` reference it, and two
of them read from it at runtime (see Inbound references below). Round 1's *work* is closed;
the directory is not.

## What this was

Multi-agent static testing-gap audit, 2026-07-22. Produced 60 issues plus follow-ups 61–66,
implemented and merged. Heavily replication/cluster/jepsen-focused; per-crate command-level
and unit-level depth was largely *not* covered — that gap is what
[round 2](../testing-improvements-round2/) exists to close.

## Layout

| path | what |
|---|---|
| `PRD.md` | the round-1 brief |
| `audit/` | per-area findings + adversarial verdicts. **Live write target** — `scripts/coverage-depth.py` writes its reports here on every run |
| `issues/open/` | still outstanding — see below |
| `issues/done/` | 63 closed issues, kept as the record of what was decided and why |

## Still open (4)

| # | issue | note |
|---|---|---|
| 40 | fuzzing continuous corpus | **done-then-regressed.** Landed in `81a4f910`, cron deleted by `2e2ea8bb`. Reopened 2026-07-28 with a `## Reopened` section; same work as round-2 infrastructure item I10 |
| 65 | checkpoint multi-atomicity testbox flake | |
| 66 | mutation testing | linked from `docs/agents/coverage-depth.md` |
| 67 | minimal RDB fullsync carries no dataset | renumbered from a duplicate `66` on 2026-07-28 |

## Inbound references

Load-bearing (code reads these paths — changing them breaks a run):

- `scripts/coverage-depth.py` — `AUDIT_DIR` write target (`:71`), baseline anchors (`:92`)
- `.github/workflows/coverage-nightly.yml`, `workflow_gen/…/coverage_nightly.py`

Documentation/comment references (rename-safe number+dir form):

- `workflow_gen/…/fuzz.py`, `…/jepsen_nightly.py`
- `docs/agents/coverage-depth.md`
- `frogdb-server/crates/protocol/src/response.rs`
- `frogdb-server/crates/redis-regression/tests/info_tcl.rs`
- `frogdb-server/crates/server/src/commands/cluster/mod.rs`
- `frogdb-server/crates/server/tests/resp3.rs`
