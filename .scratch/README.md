# `.scratch/` — feature workspaces

Working directories for multi-session efforts: PRDs, proposals, and the issue tracker. Every
subdirectory has a `README.md` with a `State:` line; every issue has a `Status:` line and lives
under `issues/open/` or `issues/done/`.

**This directory is git-tracked.** It is the record of what was decided and why, not a
temp dir. Several files here are load-bearing — `scripts/coverage-depth.py` writes into
`testing-improvements/audit/`, and ~20 tracked files across the repo cite issues here.

## Directories

| directory | state | open | done | what |
|---|---|--:|--:|---|
| [testing-improvements-round2](testing-improvements-round2/) | active | 95 | 0 | round-2 testing-gap audit; 249 findings → 95 issues, incl. **~40 suspected live defects** |
| [arch-deepening](arch-deepening/) | active | 15 | 5 | architecture deepening rounds; proposals backlog empty, issues backlog is not |
| [concurrency-testing](concurrency-testing/) | active | 4 | 7 | concurrency invariant testing; phases 1–5 shipped. Issue 11 Finding A was a harness defect, fixed; B and C still open |
| [replication-cluster-rework](replication-cluster-rework/) | active | 8 | 2 | four rework PRDs, all merged 2026-07-30; follow-ups outstanding |
| [hardening](hardening/) | active | 6 | 2 | foundation-hardening campaign; failure-mode specs + mutation gates, txn/vll **locked** 2026-08-01 |
| [naming-cleanup](naming-cleanup/) | active | 1 | 7 | canonical terminology; decisions now canon in `CONTEXT.md` files |
| [testing-improvements](testing-improvements/) | archive-of-record | 2 | 65 | round-1 testing-gap audit. **Do not delete** — live write target + 10 inbound refs |

Totals: **131 open, 88 done** across 219 issues.

## Conventions

Full rules live in [`docs/agents/issue-tracker.md`](../docs/agents/issue-tracker.md). The
three that bite most often:

1. **Status lives in two places and they must agree.** The `Status:` line and the
   `open/`|`done/` subdirectory. `Status: done` → `done/`; anything else → `open/`. Enforced
   by `just scratch-check`.

2. **Cite issues by number + directory, never by filename.** Write
   ``` `.scratch/testing-improvements/issues/40` ``` — not the full
   `40-fuzzing-continuous-corpus.md` path. Filenames move between `open/` and `done/` and
   get renumbered; number + directory does not. Markdown links point at the directory:
   `[issue 66](../../.scratch/testing-improvements/issues/)`.

3. **A `## Resolution` heading does not mean closed.** `concurrency-testing/issues/open/11`
   carries two of them — a phase-5 resolution that a later section marks superseded, and a
   root-cause fix for Finding A — while Findings B and C stay live. Only the `Status:` line
   is authoritative, and it takes a bare legal value: put the nuance in a section, not on
   the line.

Sub-issue numbers (`13-01`, `13-02`, `13-03` under `arch-deepening`) are distinct issues.
Cite the full number.
