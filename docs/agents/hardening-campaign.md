# Foundation-Hardening Campaign

Campaign to lock down the four core areas — **MULTI/transactions, persistence, replication,
cluster ops** — to near-zero correctness bugs with clearly defined failure modes. Full plan and
rationale: `.scratch/hardening/` (specs, issues, metrics).

## Status

| Phase | Area | State |
|---|---|---|
| 0 | Enablement (freeze, gating, recipes, frogdb-net) | in progress |
| 1 | Transactions / VLL | pending |
| 2 | Persistence / recovery | pending |
| 3 | Replication runtime | pending |
| 4 | Cluster runtime | pending |

Each area goes through: **extract → failure-mode spec → close known bugs → mutation-test →
fill gaps → lock**. Areas are strictly serial, one PR per step.

## Out of scope — do not touch during the campaign

- `frogdb-operator/**` — separate workspace, own CI.
- `frogctl/**` — excluded from the default test run (`just frogctl-test` to run explicitly).
- `website/**`.
- The **frozen** redis-regression suite (`frogdb-server/crates/redis-regression`): it does not
  build in `just check`/`just test`. If a nightly `regression-run` goes red, file an issue in
  `.scratch/hardening/issues/` — do **not** fix inline.

## Command surface

| Command | Purpose |
|---|---|
| `just core-test <area> [pattern]` | Run one area's crate tests (`txn` \| `persistence` \| `replication` \| `cluster`) |
| `just core-test-e2e <area>` | Area's end-to-end tests against the real server, core profile |
| `just regression [pattern]` | Run the frozen compat suite (on demand / nightly only) |
| `just regression-check` | Compile-only anti-rot check of the frozen suite |
| `just mutants <crate>` | Full mutation run (testbox-class; use `just tb-run` in testbox mode) |
| `just mutants-diff <crate>` | Mutate only this branch's diff (PR-viable) |
| `just mutants-gate <crate> <threshold>` | Enforce an area's mutation score from a completed run |
| `just loop-cost <area>` | Record the area's warm inner-loop cost to the metrics file |

Targeted `-p` builds (`just check <crate>`, `just test <crate>`, the `core-*` recipes) already use
the core command profile — it is the default feature set. Workspace-wide builds unify to the full
surface (docs-gen and the frozen regression suite pin `cmd-full`), which is expected. Avoid
alternating extra feature flags between commands in an iteration loop — it thrashes the build
cache.

## Conventions

- Failure-mode specs: `.scratch/hardening/specs/<area>-failure-modes.md`. Every `FM-<AREA>-NNN`
  row names the test that forces it; `just lint-failure-modes` enforces spec ↔ test agreement.
- Campaign issues: `.scratch/hardening/issues/` per [issue-tracker](issue-tracker.md) conventions.
- Metrics: `.scratch/hardening/metrics/loop-cost.md` — record a row before and after each
  extraction (`just loop-cost <area>`).
- Bug fixes follow spec-first order: failure-mode row → failing test → fix.
- Command families behind cargo features (`frogdb-commands`): default `core-profile` covers
  strings/bitmap/lists/hashes/sets/zsets/expiry/multi/blocking/scan/generic/sort. Exotic families
  (`json`, `timeseries`, `bloom`, `cuckoo`, `cms`, `topk`, `tdigest`, `geo`, `vectorset`,
  `hyperloglog`, `stream`, `event-sourcing`) are off by default; `full`/`cmd-full` enables all.
  Only `docs-gen` (and allowlisted tooling) may request `full` — a lint enforces this.
