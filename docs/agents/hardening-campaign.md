# Foundation-Hardening Campaign

Campaign to lock down the four core areas — **MULTI/transactions, persistence, replication,
cluster ops** — to near-zero correctness bugs with clearly defined failure modes. Full plan and
rationale: `.scratch/hardening/` (specs, issues, metrics).

## Status

| Phase | Area | State |
|---|---|---|
| 0 | Enablement (freeze, gating, recipes, frogdb-net) | **done** (2026-07-31) |
| 1 | Transactions / VLL | **LOCKED** (2026-08-01) — mutation gate 90%, both crates at 100% |
| 2 | Persistence / recovery | **LOCKED** (2026-08-02) — mutation gate 85%, frogdb-persistence 99.1%, frogdb-recovery 100% |
| 3 | Replication runtime | **in progress** — extracted (`docs/adr/0004-replication-runtime-seams.md`); spec at FM-REPLICATION-001..053 (044-050 written by closing a bug, 051-053 by the runtime mutation round); 13 bugs filed (issues 12-24), 12-18 fixed, 19-24 open (21-24 raised by the adversarial review of the round's own diff); baselines `frogdb-replication` 74.7%, `frogdb-replication-runtime` 50.0%, scoped re-run of the gap-filled files 95.2%, `frogdb-replication-runtime` re-run after the gap-fill 100% of viable (53 caught / 0 missed / 6 unviable) |
| 4 | Cluster runtime | pending |

Each area goes through: **extract → failure-mode spec → close known bugs → mutation-test →
fill gaps → lock**. Areas are strictly serial, one PR per step.

**Locked area rules.** Locked crates so far: **txn** — `frogdb-txn` + `frogdb-vll` (gate
`0.90`) — and **persistence** — `frogdb-persistence` + `frogdb-recovery` (gate `0.85`). The
failure-mode specs (`.scratch/hardening/specs/{txn,vll,persistence}-failure-modes.md`, header
`Status: LOCKED`) are the contract — behavior changes there are spec-first, and
`just lint-failure-modes` enforces spec↔test agreement on every commit. Before pushing changes
that touch a locked crate, run `just mutants-diff <crate>` (CI is manual-only, so this is a
push-discipline rule, not a CI gate; full runs: `just mutants <crate>` +
`just mutants-gate <crate> <the crate's gate>`). Boundary ADRs:
`docs/adr/0002-txn-orchestration-behind-txnhost-seam.md`,
`docs/adr/0003-persistence-durability-seams.md`.

A surviving mutant that no test can kill is documented *at the code*, with a comment saying why
the mutation is unobservable — never with a blanket skip. The persistence lock carries six such
equivalents (RocksDB option knobs whose two forms produce identical reads, a match arm that is
redundant with its own fallback, two `NoopSnapshotCoordinator` accessors with no state to report,
and a drain-loop disjunct that costs channel round-trips rather than batch contents).

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
  row names the test that forces it in its `Forced by` cell (backticked, comma-separated), and
  every named test carries a `// FM-<AREA>-NNN` comment on its definition (above the doc/attribute
  block). `just lint-failure-modes` enforces both directions — an unforced row, a name no test
  matches, and a tag no row names are all errors — and runs as part of `just lint`. A *tag* is a
  comment line that is nothing but ids (`// FM-TXN-004`, `// FM-TXN-009, FM-TXN-022`); prose that
  cites an id in passing (`… the complement of FM-REPLICATION-018`) is a cross-reference and is
  deliberately not linted, so production code may point at the row it implements.
- Campaign issues: `.scratch/hardening/issues/` per [issue-tracker](issue-tracker.md) conventions.
- Metrics: `.scratch/hardening/metrics/loop-cost.md` — record a row before and after each
  extraction (`just loop-cost <area>`).
- Bug fixes follow spec-first order: failure-mode row → failing test → fix.
- **A mutation score is a floor, not a measurement — put the forcing test in the mutated crate.**
  `cargo mutants -p <crate>` runs only that package's own tests against each mutant (cargo-mutants
  tests the mutated package unless `--test-package`/`--test-workspace` says otherwise). A row whose
  only `Forced by` test lives in `frogdb-server/crates/server/tests/` therefore contributes nothing
  to the score of the crate it describes: the mutant is reported as missed even though a test would
  have caught it. This is why the replication area measures lower than it behaves — many of its
  forcing tests are in `integration_replication.rs`. Widening the test scope is not the fix (the
  server suite per mutant is hours of compute); the fix is to force each invariant from a unit test
  in the crate that owns it, keeping the integration test as the end-to-end check rather than as
  the only witness.
- Command families behind cargo features (`frogdb-commands`): default `core-profile` covers
  strings/bitmap/lists/hashes/sets/zsets/expiry/multi/blocking/scan/generic/sort. Exotic families
  (`json`, `timeseries`, `bloom`, `cuckoo`, `cms`, `topk`, `tdigest`, `geo`, `vectorset`,
  `hyperloglog`, `stream`, `event-sourcing`) are off by default; `full`/`cmd-full` enables all.
  Only `docs-gen` (and allowlisted tooling) may request `full` — a lint enforces this.
