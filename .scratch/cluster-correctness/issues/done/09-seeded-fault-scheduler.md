# 09 — Seed-driven fault scheduler for the turmoil cluster sims

Status: done

## Parent

[PRD](../../PRD.md) §3 W4; seed budget ruled in §8 D4 (500/night, locally runnable).

## What to build

Generalize the five scripted turmoil cluster sims into one seed-driven scheduler: a seed
derives the full schedule (which links partition when, which nodes SIGKILL/restart when,
message delay distributions, barrier/lease timer skew). Same seed → same run.

Invariant checking at quiesce via `DEBUG CLUSTER CHECK` on every surviving node, plus the
existing client-visible assertions, plus cross-node checks a single-state catalog cannot
express (pairwise epoch monotonicity as observed by clients, single-writer-per-slot over
the whole history).

Sweep recipe: one `just` invocation, budget in one Justfile variable (default 500),
runnable on a laptop; nightly CI calls the same recipe. A failing seed is committed to a
regression list replayed forever after.

Durability faults (lose-unsynced-writes on kill) explicitly ride campaign 2's crash
harness, not turmoil — no disk model here.

## Acceptance criteria

- [x] The five scripted sims are subsumed (each scripted scenario expressible as a seed
      or kept as a named regression seed)
- [x] Same seed reproduces the same run bit-for-bit
- [x] `just <recipe> [SEEDS=n]` works locally; nightly wired to the same recipe
- [x] Regression-seed list exists and replays in CI
- [x] Cross-node invariant checks (epoch monotonicity, single-writer-per-slot) active at
      quiesce

## Blocked by

- Issue 06 (`.scratch/cluster-correctness/issues/`) — quiesce checks consume the
  command's catalog surface (in-process equivalent acceptable under turmoil).

## Resolution

Built as `frogdb-server/crates/server/tests/simulation/scheduler.rs` (a `mod scheduler`
of the existing `tests/simulation.rs`, so it is selected by the same
`test(/simulation/)` filters and the same nextest cluster test-group).

### The scheduler

`Schedule::from_seed(u64)` is a pure function: one `StdRng::seed_from_u64` draws, in a
fixed order, the fault **family** (`healthy`, `leader-isolation`, `asymmetric-edge`,
`replica-partition`, `crash-restart`, `mixed`), each fault's kind
(`HoldEdge` / `HoldIsolate` / `SlowEdge` / `CrashRestart`), its endpoints, arm and heal
instants, the baseline link latency, the per-node Raft election-timeout skew, whether a
PSYNC replica is attached, whether auto-failover is on, and the client workload
(`SET`/`GET`/`WAIT`/slot-migration ops over a six-key pool spread across the slot space).
Family is drawn first, so a sweep covers every shape by construction rather than by luck.

Determinism details that mattered:

- Nothing in the derivation or the checkers iterates a `HashMap` — `Vec`/`BTreeMap`/
  `BTreeSet` throughout — and nothing reads the wall clock: faults are applied from the
  manual `sim.step()` loop against `sim.elapsed()`, the simulated clock.
- The one input a seed could not pin was openraft's election jitter, drawn from
  `rand::thread_rng()`. `src/server/cluster_init.rs` collapses the election-timeout window
  to a single value under `cfg(feature = "turmoil")` and the schedule skews
  `election_timeout_ms` *per node* instead — deterministic tie-breaking rather than
  unseeded jitter.
- Faults use turmoil `hold`/`release`, never `partition`/`repair`: turmoil 0.7.1 leaks an
  ephemeral port for every cancelled dial, which a sustained partition exhausts. A held
  edge still starves the peer of heartbeats past its election timeout, which is the
  property under test.

`RunOutcome::fingerprint` is the assertable form of the contract — a canonical
line-per-event rendering of the whole run (schedule, fault application, every client op
and its outcome, quiesce probes). `CLUSTER_SEED_TRACE=1` dumps one seed's fingerprint for
eyeballing.

### Quiesce checking

Three layers, all reported as `frogdb_cluster::Violation` so the shape matches the
catalog's:

1. The invariant catalog on every surviving node, via `DEBUG CLUSTER CHECK` (issue 06),
   filtered through `hard_violations` so the catalog's own DOCUMENTED-EXCEPTION tier is
   dropped — a documented exception is a ruling, not a defect. This is `check_hard` in
   effect, read through the client surface rather than in-process, which also exercises
   the command.
2. Client-visible assertions: single-owner convergence for every touched slot after every
   fault heals, and no acked-write loss. Loss is scoped by
   `History::acked_write_is_checkable`: the data plane runs without persistence in these
   sims, so a SIGKILL of the owner legitimately loses its keys, and a slot migration moves
   ownership without moving keys (FrogDB has no `MIGRATE`).
3. `check_cross_node`, the checks a single-node catalog cannot express:
   - `XNODE-EPOCH-1` / `-2` — pairwise epoch monotonicity as observed by clients: no
     client ever sees a node's epoch (or its own `cluster_current_epoch`) go backwards
     across probe rounds.
   - `XNODE-SLOT-1` — no two `cluster_state:ok` nodes claim one slot at the same time
     (contiguous slots with the same claimants collapse into one violation).
   - `XNODE-SLOT-2` — single-writer-per-slot over the *whole run history*: within one era
     (between migrations/faults of that slot) every acked write for a slot landed on one
     node.

### How the five scripted sims were subsumed

Two were deleted and re-expressed as named regression seeds, three had to stay:

| scripted sim | disposition |
| --- | --- |
| `test_cluster_moved_redirect_convergence` | deleted; **seed 2** (`healthy`) |
| `test_cluster_leader_partition_mid_migration_converges` | deleted; **seed 5** (`leader-isolation`) |
| `test_cluster_asymmetric_partition_false_failover` | kept; shape generalized as the `asymmetric-edge` family |
| `test_cluster_wait_degrades_under_partition` | kept; shape generalized as the `replica-partition` family |
| `test_cluster_wait_unblocked_across_failover` | kept; shape generalized as the `replica-partition` family |

The three kept sims are **cited by name** outside the code and deleting them would break
those citations, which is exactly the "keep it and say why" case:

- `.scratch/hardening/specs/replication-failure-modes.md:809` and `:859` name
  `test_cluster_wait_degrades_under_partition` and
  `test_cluster_wait_unblocked_across_failover` as forcing tests. That spec is **LOCKED**,
  so its rows are a contract; retargeting them at a seed is a spec change, not a test
  cleanup, and `just lint-failure-modes` enforces the pairing either way.
- `website/src/content/docs/architecture/clustering.md:440` cites
  `test_cluster_asymmetric_partition_false_failover` as the published evidence for
  FrogDB's asymmetric-partition behavior.

Their *scenarios* are subsumed regardless: each is a fault family the scheduler draws, so
the sweep explores those shapes with schedules no scripted sim wrote down. What stayed is
the named point-witness each citation depends on.

### Sweep recipe and CI

`just cluster-seeds [SEEDS=500]` — budget in one Justfile variable, per PRD §8 D4. The
nightly `cluster-nightly` workflow calls exactly that recipe (`cluster_nightly.py`,
`cluster-seeds` job); its `seeds` workflow input is optional and, when blank, defers to
the recipe's own default, so the budget is not duplicated in CI. `CLUSTER_SEEDS_JOBS`
(worker threads, default 4) and `CLUSTER_SEEDS_START` (range offset) tune a local run;
the sweep test itself is `#[ignore]`d and selected with `--run-ignored all` plus the
`cluster-seeds` nextest profile, which lifts the default hard kill (the real bound is the
CI job's `timeout-minutes`).

The default suite runs a six-seed smoke sweep (`SMOKE_SEEDS`, one per family, asserted by
`test_scheduler_smoke_seeds_cover_every_family`), the determinism double-run, and the full
regression-seed replay, so the scheduler cannot rot between nightlies.

### Regression seeds and the defect found

`frogdb-server/crates/server/tests/simulation/cluster-regression-seeds.txt`, replayed by
`test_cluster_scheduler_regression_seeds` in the default suite. Format is
`<seed> <family> [EXPECTED-FAILURE:<issue>] <why>`; the family column is re-derived and
checked by `test_scheduler_regression_seed_file_parses`, so a change to the draw order
cannot silently repoint a seed at a different scenario.

Seeds whose defect is still open carry an `EXPECTED-FAILURE:<issue>` marker, mirroring the
cluster proptest's muzzle discipline — and the muzzle is **self-expiring**: a muzzled seed
is asserted to *still fail*, so landing the fix turns the replay test red with an
instruction to delete the marker. Sweeps skip muzzled seeds (per-seed, not per-defect, so
a lookalike defect on another seed still fails).

The sweep found one real defect, filed as
[issue 17](17-force-failover-evicts-the-old-primary-from-raft-so-it-never-learns-it-lost-its-slots.md):
auto-failover proposes `Failover { force: true }`, which maps to `VoterChange::Remove`, so
the old primary is evicted from the Raft voter set and has no channel left through which
to learn it was demoted. It keeps serving its pre-failover slots after the heal; on seed 72
both halves serve the same key with *different* values. Not matched to issues 14/15/16:
14 is role-transition validation, 15 is the *graceful* branch's migration pruning, 16 is
`AssignSlots` vs. open migrations — this is the force branch's effect on Raft membership.
Issue 17 records three candidate rulings and needs one before a fix (all three change
observable failover behavior), so no locked-crate change was made here.

Two other apparent failures were **harness** modeling gaps, each fixed with a pure unit
test rather than muzzled:

- an acked write whose slot later migrated was checked for loss, though `SETSLOT ... NODE`
  moves ownership and not keys (`test_acked_writes_are_unchecked_once_their_slot_migrates`);
- an interrupted migration reads as a disagreement if `ASK` is treated as "no answer",
  though only a MIGRATING slot's *owner* emits `ASK`
  (`test_owner_votes_read_ask_as_the_source_still_owning_the_slot`). This one alone
  accounted for 16 of the first 100 seeds.

### Evidence

- `just cluster-seeds 100` (`CLUSTER_SEEDS_JOBS=6`): **clean**, 58.7s.
- `just cluster-seeds 500` (`CLUSTER_SEEDS_JOBS=6`), the ruled nightly budget: 592s, **36 of
  the 490 unmuzzled seeds fail, all issue 17** (seeds 113, 125, 126, 138, 143, 157, 159, 162,
  170, 176, 179, 183, 214, 228, 234, 265, 329, 349, 363, 364, 387, 398, 401, 406, 424, 427,
  430, 438, 450, 452, 460, 470, 478, 485, 491, 493 — every one an `XNODE-SLOT-1` with the
  "node absent from the survivors' node tables still serving its pre-failover slots"
  signature). They are recorded in issue 17 rather than muzzled individually: 46 replays of
  one defect would cost the per-PR suite minutes and add nothing over the canonical seeds 3
  and 72. Nightly therefore stays red until issue 17 lands, and issue 17's acceptance
  criteria include a clean `just cluster-seeds 500`.
- Determinism: `test_cluster_scheduler_same_seed_same_run` runs one seed twice and
  compares fingerprints line by line (reporting the first divergence, not two dumps).
  Green.
- `cargo nextest run -p frogdb-server --features turmoil -E 'test(simulation::scheduler::)'`
  — 24/24 (determinism, six-seed smoke sweep, 12-entry regression replay, 21 pure unit
  tests over the derivation and the checkers).
- `just test frogdb-cluster` 283/283, `just test frogdb-cluster-runtime` 78/78,
  `just check`, `just fmt`, `just lint-failure-modes` (278 failure modes, 1382/1382 tags),
  `just scratch-check`, `just workflow-gen --check`.
- Mutation gates (`just mutants-diff frogdb-cluster` / `frogdb-cluster-runtime`): **not
  applicable — the diff touches neither locked crate**. It is `frogdb-server`'s test tree
  (`tests/simulation*`, `tests/common/sim_helpers.rs`) plus `Justfile`,
  `.config/nextest.toml`, the workflow generator and `.scratch`. The one `src` change is in
  `frogdb-server` (not a locked crate): the `cfg(feature = "turmoil")` election-jitter
  collapse in `server/cluster_init.rs`, which leaves the production 2x window untouched.
