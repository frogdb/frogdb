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
| [memory-architecture](memory-architecture/) | active | 11 | 10 | 2026-08-31 memory-architecture PRD (R1–R15, all ruled) + phase-1 spike (GO); phases 1–2 landed (issues 01–09); phase 3–6 issues 10, 13–22 filed 2026-09-01 (11–12 reserved for post-spike drafting); 23 filed from issue 18's review (replica feed accounting, D4) |
| [redis-feel](redis-feel/) | active | 4 | 14 | 2026-08-15 side-by-side feel test vs Redis 8.6.1; data path matched line-for-line, gaps are all introspection/metadata — rulings in `adr/0005-truthful-redis-86-surface.md` |
| [testing-improvements-round2](testing-improvements-round2/) | active | 70 | 6 | round-2 testing-gap audit; 249 findings → 95 issues. Re-triaged 2026-08-06: **18 confirmed live defects** |
| [arch-deepening](arch-deepening/) | active | 15 | 1 | architecture deepening rounds; proposals backlog empty, issues backlog is not |
| [hardening-2](hardening-2/) | active | 9 | 1 | second hardening campaign, detection-first; running |
| [concurrency-testing](concurrency-testing/) | active | 5 | 7 | concurrency invariant testing; phases 1–5 shipped. Issue 11's findings re-verified 2026-08-02 and split into issues 12–16 — one real product bug (12), the rest harness/checker defects. Issue 18 closed 2026-09-01: turmoil reds fixed, LOCKED-spec wording amended (human-approved) |
| [replication-cluster-rework](replication-cluster-rework/) | active | 1 | 10 | four rework PRDs, all merged 2026-07-30; one follow-up outstanding |
| [spec-gaps](spec-gaps/) | active | 10 | 0 | 2026-08-13 anti-pattern review follow-through: persistence + txn/vll/blocking spec gaps, all rulings settled |
| [hardening](hardening/) | active | 2 | 37 | foundation-hardening campaign, **exited 2026-08-05** — all four areas locked; specs remain the contract |
| [naming-cleanup](naming-cleanup/) | active | 1 | 4 | canonical terminology; decisions now canon in `CONTEXT.md` files |
| [testing-improvements](testing-improvements/) | archive-of-record | 1 | 18 | round-1 testing-gap audit. **Do not delete** — live write target + inbound refs |
| [roadmap](roadmap/) | active | — | — | roadmap + unfinished/follow-up items (migrated from the retired `todo/`); not an issue tracker |

Totals: **109 open, 107 done**. `done/` holds only closed issues still referenced from
tracked files; unreferenced closed issues are pruned — git history is the archive.

## Conventions

Full rules live in [`agents/issue-tracker.md`](../agents/issue-tracker.md). The
three that bite most often:

1. **Status lives in two places and they must agree.** The `Status:` line and the
   `open/`|`done/` subdirectory. `Status: done` → `done/`; anything else → `open/`. Enforced
   by `just scratch-check`.

2. **Cite issues by number + directory, never by filename.** Write
   ``` `.scratch/testing-improvements/issues/40` ``` — not the full
   `40-fuzzing-continuous-corpus.md` path. Filenames move between `open/` and `done/` and
   get renumbered; number + directory does not. Markdown links point at the directory:
   `[issue 40](../../.scratch/testing-improvements/issues/)`.

3. **A `## Resolution` heading does not mean closed.** `concurrency-testing/issues/11`
   carries two of them — a phase-5 resolution that a later section marks superseded, and a
   root-cause fix for Finding A — and stayed open for weeks afterward, until Findings B and C
   were resolved too. Only the `Status:` line is authoritative, and it takes a bare legal
   value: put the nuance in a section, not on the line.

Sub-issue numbers (`13-01`, `13-02`, `13-03` under `arch-deepening`) are distinct issues.
Cite the full number.
