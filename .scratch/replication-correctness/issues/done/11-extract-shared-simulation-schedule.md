# 11 — Extract the topology-agnostic half of the seeded scheduler

Status: done

## Parent

[PRD](../../PRD.md) §8 D8 ruling; consumed by §3 W4.

## What to build

`frogdb-server/crates/server/tests/simulation/scheduler.rs` is ~2650 lines with Raft topology
assumptions threaded through it. D8 rules that roughly 1100 of those lines move into a shared
`frogdb-server/crates/server/tests/simulation/schedule.rs`:

- the seed→schedule derivation (`Schedule::from_seed` at `:300`, `derive_faults`,
  `prune_concurrent_crashes`), **parameterized** by a per-arm `Family` enum, budget and op
  vocabulary rather than hard-coded to the cluster arm's;
- `RunOutcome::fingerprint` and `assert_fingerprints_equal`;
- the regression/muzzle file machinery — the self-expiring `EXPECTED-FAILURE:<issue>` markers that
  fail the replay test the day the fix lands;
- `parse_check_entry` / `hard_violations`, typed over `frogdb_types::Violation` (issue 01's move);
- generic fault application.

What stays Raft-specific and does **not** move: `spawn_scheduled_hosts` (`:1059`),
`parse_cluster_nodes` (`:1492`) and `check_cross_node` (`:725`). D8 is also explicit that the
~50-line `run_seed` driver shape is **duplicated per arm, not genericized** — the cluster arm keeps
its own.

This is a pure refactor with a mechanical acceptance test, and that test is the point: the same
seed must derive the same schedule and the same fingerprint before and after the move. Record
fingerprints for a fixed seed set on the current tree first, then assert them unchanged after.

Note the scope boundary against cluster issue 23 (`.scratch/cluster-correctness/issues/`, same-seed
fingerprint diverges under host load): this issue must not *change* fingerprint behavior in either
direction — same seed, same fingerprint, before and after. Fixing the load-dependence is 23's job.

## Acceptance criteria

- [x] `simulation/schedule.rs` holds the derivation, fingerprint, regression/muzzle machinery and
      generic fault application, parameterized by per-arm `Family` / budget / op vocabulary
- [x] `spawn_scheduled_hosts`, `parse_cluster_nodes` and `check_cross_node` stay in the cluster
      arm; `run_seed` stays duplicated per arm rather than genericized
- [x] `parse_check_entry` / `hard_violations` are typed over `frogdb_types::Violation`
- [x] Fingerprints unchanged: a fixed seed set produces identical schedules and
      `RunOutcome::fingerprint` values before and after the move, with the recorded values in this
      issue
- [x] Cluster scheduler tests and `just cluster-seeds` green; `cluster-regression-seeds.txt`
      replays unchanged

## Blocked by

- Issue 01 (`.scratch/replication-correctness/issues/`) — `parse_check_entry` and
  `hard_violations` are typed over `frogdb_types::Violation`, which does not exist until 01 lands.

## Resolution (2026-08-10)

Pure refactor, no behavior change. 798-line
`frogdb-server/crates/server/tests/simulation/schedule.rs` now holds the arm-agnostic half;
`simulation/scheduler.rs` drops 2694 -> 2366 lines and becomes the cluster *arm* over it.

### The seam

`trait Arm` carries the per-arm vocabulary as associated types (`Family`, `Toggles`, `Op`) plus
`const HOSTS` and `const BUDGET`, and hooks for the arm-specific draws (`derive_toggles`,
`derive_faults`, `derive_ops`) and rendering (`family_token`, `render_toggles`, `render_op`).
`Schedule<A: Arm>::from_seed` owns the shared draw sequence; the cluster arm is the unit marker
`ClusterArm`, with `type Schedule = schedule::Schedule<ClusterArm>` so every existing call site
reads unchanged.

Budget knobs that were literals inside `from_seed` (`40..=60`, `280..=360`, `MIN_ARM_MS`,
`MAX_FAULT_MS`, the 12..60 op clamp, the 2s quiesce tail, the 300s sim duration) are now
`ClusterArm::BUDGET` fields of type `Span { lo, hi }`. `Span` is a plain struct rather than
`RangeInclusive` because an associated `const` cannot call `RangeInclusive::new`; `Span::draw`
is `rng.random_range(lo..=hi)`, the identical sampler on the identical types, which is what
keeps the draw stream byte-identical.

Also moved: `FaultKind`/`FaultEpisode`/`NodeTimers`/`distinct`/`episode`/
`prune_concurrent_crashes`/`apply_fault` (now taking `hosts: &[&'static str]` and
`base_latency_ms` instead of reaching into a cluster `Schedule`), `RunOutcome`,
`assert_fingerprints_equal`, `env_u64`, the `EXPECTED-FAILURE:` regression/muzzle machinery
(`RegressionSeed`, `regression_seeds`, `parse_regression_seed`, `muzzled_seeds` — now taking the
file text so each arm supplies its own list), and `parse_check_entry`/`hard_violations` typed
over `frogdb_types::Violation` with the catalog vocabulary passed in (`known_ids`, `unknown_id`,
`excepted`) rather than read from `frogdb_cluster::CATALOG` inside the shared module.

Stayed cluster-only, as D8 rules: `spawn_scheduled_hosts`, `check_cross_node`,
`parse_cluster_nodes`, `step_with_faults` (it drives the cluster-only `Shared`), the
`History`/`NodeView`/`Round` observation model, the owner-vote/migration helpers, and the ~50-line
`run_seed` driver, which is duplicated per arm rather than genericized. Recipes were not merged:
`just cluster-seeds` is untouched and no replication recipe was added (the replication arm does
not exist yet).

### Fingerprint invariance (the acceptance gate)

A temporary dump test rendered `Schedule::from_seed(seed).render()` for **seeds 0..=500** — which
covers seeds 0..50, every `SMOKE_SEEDS` entry (1, 2, 3, 5, 7, 14) and every seed named in
`cluster-regression-seeds.txt` (clean 2, 5; muzzled 3, 13, 17, 21, 24, 25, 39, 50, 72, 99) — on
the pre-refactor tree, then again post-refactor. Both dumps are 26670 lines and hash identically:

```
sha256(before) = 574ddad561953c5ebe334e61ebb8fc12e01967882a20a28baefc3e9c85e126e9
sha256(after)  = 574ddad561953c5ebe334e61ebb8fc12e01967882a20a28baefc3e9c85e126e9
diff before after -> empty
```

Per-seed digests (sha256 of the rendered block, first 16 hex) for the seed set that the muzzle and
regression lists reference, recorded so a future re-derivation can be checked without re-running
the pre-refactor tree:

| seed | family | sha256 (16) | lines |
|------|--------|-------------|-------|
| 0 | crash-restart | `5e841e20c8f2e714` | 51 |
| 1 | — | `0e01c8995eb38d32` | 60 |
| 2 | healthy | `ec64c7000fe9f8e6` | 24 |
| 3 | replica-partition | `54dafec1e80df643` | 60 |
| 5 | leader-isolation | `c4334a576332a496` | 70 |
| 7 | — | `877236009d27f4f5` | 70 |
| 13 | replica-partition | `2e1bb8dabe613b1a` | 52 |
| 14 | — | `2691448338c8f819` | 57 |
| 17 | replica-partition | `11e73d986d765aa7` | 67 |
| 21 | mixed | `95d4a711fec44296` | 72 |
| 24 | replica-partition | `f8c1d6ead54177b8` | 70 |
| 25 | replica-partition | `b07128604804713e` | 70 |
| 39 | replica-partition | `0faab93ace9372e5` | 68 |
| 50 | replica-partition | `d2942d5252900016` | 70 |
| 72 | replica-partition | `bcd38e3b63a65940` | 51 |
| 99 | mixed | `3ccf3f5723ed7d8d` | 71 |

The dump test was temporary scaffolding and is deleted; the standing guards are
`test_scheduler_regression_seed_file_parses` (the recorded family column must still match
`Schedule::from_seed`'s draw order), `test_cluster_scheduler_same_seed_same_run` (run-level
fingerprint equality) and the regression replay itself.

`RunOutcome::fingerprint` invariance is covered at the run level rather than by a second dump:
`test_cluster_scheduler_regression_seeds` replays all 12 committed seeds and asserts the same
clean/muzzled verdict as before, and the two clean seeds plus the six-seed smoke sweep run whole
sims to a clean catalog check. Nothing in the run path changed — only where the schedule types
are defined.

### Test evidence

- `cargo nextest run -p frogdb-server --features turmoil -E 'test(/simulation::sched/)'` —
  **30/30 pass** (24 scheduler + 6 new pure tests in `simulation::schedule`), including
  `test_cluster_scheduler_same_seed_same_run`, `test_cluster_scheduler_smoke_sweep`,
  `test_cluster_scheduler_regression_seeds` (all 10 muzzled seeds still reproduce, both clean
  seeds still clean) and `test_scheduler_regression_seed_file_parses`.
- `just cluster-seeds 12` — green.
- `cargo check -p frogdb-server --features turmoil --tests` and scoped clippy — clean.

Six pure tests moved with their subjects into `simulation::schedule`: muzzle-column parsing,
comment/blank handling, `distinct` self-edge avoidance, `prune_concurrent_crashes`,
`parse_check_entry` id recovery, `hard_violations` filtering.

No bug was found mid-refactor, so nothing was filed.
