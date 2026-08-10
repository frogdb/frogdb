# Cluster correctness — invariant-driven validation

Status: **DRAFT** — open decisions in §8
Author: 2026-08-08
Related: [campaign 2](../hardening-2/PRD.md) (detection-first hardening, running),
[cluster failure-mode spec](../hardening/specs/cluster-failure-modes.md) (LOCKED, 97 rows at
audit time, 098–102 landing with the 2026-08-08 defect fixes),
[replication-cluster rework](../replication-cluster-rework/PRD.md) (two-phase handoff, shipped)

## 1. Why

A full audit of the cluster area on 2026-08-08 found five live defects in a locked area
carrying a 99.6% / 99.0% mutation score, 97 spec rows, and ~500 tests:

1. `save_vote` flushed the wrong column family — vote not durable, double-vote → split-brain
   precondition (campaign-2 issue 01, now fixed, FM-CLUSTER-098).
2. `handoff_seq` zeroed by `from_snapshot` — fencing-generation reuse after snapshot restore
   (fresh finding, now fixed, FM-CLUSTER-100).
3. `CLUSTER FORGET` never shrinks the Raft voter set — quorum math permanently wrong after
   membership shrink (round-2 87/F11, fix in flight, FM-CLUSTER-101).
4. `get_log_reader` served a detached, never-invalidated log cache — Raft log divergence
   (round-2 issue 53, now fixed, FM-CLUSTER-099).
5. `check_interval_ms = 0` panics the failure-detector task — unvalidated config (spec GAP 8,
   now fixed, FM-CLUSTER-102).

Every one was found by a human reading code. None was found by the machinery — and each
evades it *structurally*, not by bad luck:

- **B1 — point witnesses do not quantify.** An FM row pins one scenario; a mutation score is
  a floor on code that exists. Nothing in the current machinery states "handoff seqs are
  never reused" *for all command sequences* — so the one sequence that violates it
  (prepare → complete → snapshot → restore → prepare) was never generated. The spec's own
  GAPS section and campaign 2's B1 ("mutation testing cannot see omissions") are the same
  finding from the other side.
- **B2 — no harness explores interleavings.** The turmoil cluster sims are five scripted
  scenarios; Shuttle covers `frogdb-core` only (zero cluster coverage); the 209 integration
  tests each replay one hand-written ordering. The space of message orderings around the
  two-phase handoff and the failover composite has never been searched, randomly or
  exhaustively.
- **B3 — internal state is invisible to the black-box suites.** Jepsen checks client
  histories. A ghost slot owner, a dangling `primary_id`, or an epoch below a node epoch is
  not client-visible until it detonates later; no suite can currently ask a node "are your
  invariants intact?"

**Thesis: define cluster correctness once, as executable invariants, and check that single
definition under *generated* permutations and faults at every fidelity level — instead of
hand-writing one test per imagined scenario.** The audit's defect list is the outcome of one
manual pass; this PRD is the machine that makes the next pass continuous.

## 2. Shape of the system

One catalog, five consumers:

| layer | explores | cost/run | repro | new machinery |
|---|---|---|---|---|
| L1 post-apply self-check | every existing test's scenarios | ~free | exact | invariant module + one hook |
| L2 proptest permutations | command sequences × crash points | seconds | shrunk sequence | generator + properties |
| L3 stateright model check | all message interleavings, small scope | minutes | exact trace | protocol model |
| L4 seeded turmoil schedules | network/crash/timing schedules | minutes–hours | seed | fault scheduler |
| L5 Jepsen + live check | real processes, real faults | heavy | partial | `DEBUG CLUSTER CHECK` + checkers |

The catalog is the single source of truth; every layer imports it rather than restating it.
A violation report is identical in shape at every layer: stable invariant ID + detail string.

## 3. Workstreams

### W1 — Invariant catalog + self-checking state machine

`frogdb-server/crates/cluster/src/invariants.rs`: pure functions over `&ClusterStateInner`,
no I/O, returning `Vec<Violation>` (`id: &'static str`, `detail: String`). In-crate so the
mutation gate sees both the catalog and its tests.

Seed catalog — each entry names the defect class it would have caught, so no entry is
decorative:

| ID | claim | would have caught |
|---|---|---|
| INV-REF-1 | every `slot_assignment` owner exists in `nodes` | ghost owner via unguarded `CompleteSlotMigration` insert (FM-CLUSTER-033 blesses it today — see §8 D2) |
| INV-REF-2 | every migration's source and target exist in `nodes` | `RemoveNode` dangling migrations (round-2 issue 62) |
| INV-REF-3 | a Replica's `primary_id` names an existing Primary | orphaned replicas after removal |
| INV-REF-4 | a Primary has `primary_id == None` | role/parent desync |
| INV-EPOCH-1 | `config_epoch >= max(node config epochs)` | mint-below-node-epoch regressions |
| INV-EPOCH-2 | a nonzero node epoch is unique among Primaries | epoch-collision reconcile bugs |
| INV-HANDOFF-1 | `handoff_seq >= max(handoff.seq over migrations)` | **audit defect 2** (seq reuse after restore) |
| INV-HANDOFF-2 | a live handoff exists only inside a migration for its own slot; `drained` implies prepared | orphaned barrier state |
| INV-MIG-1 | a migrating slot's current owner is the migration's source | mid-migration ownership drift |
| INV-SLOT-1 | every slot key `< 16384` | boundary/parse bugs |

Two tiers, no third:

- **HARD** — checked always; a violation is a defect by definition.
- **DOCUMENTED-EXCEPTION** — the state is reachable today, the behavior is deliberate, and
  the exception entry cites the FM row or issue that says so (e.g. `RemoveNode` dangling
  refs, a documented non-guarantee with its own pinning test). An exception without a
  citation is a build error. The catalog thereby forces every known-dirty state into an
  explicit ruling instead of a silent shrug — see §8 D2.

The hook, at the end of `ClusterState::apply_command` under
`#[cfg(any(test, debug_assertions))]`: assert the catalog is clean after every apply. This
retroactively upgrades all 219 cluster unit tests, 209 `cluster_*` integration tests, and
the turmoil sims into invariant tests at zero authoring cost. Also hook `from_snapshot` /
`install_snapshot` (restore must land in a clean state — the exact seam audit defect 2
lived on).

Each HARD invariant lands with a forcing test that constructs the violating state directly
(via the existing test-only mutators) and asserts the catalog reports it — otherwise the
catalog itself is dead code to the mutation gate.

### W2 — Property-based permutation harness (proptest)

New dev-dependency `proptest` in `frogdb-cluster` (§8 D1). One generator, four properties:

- `arb_command_sequence(len)` — weighted strategy over all 18 `ClusterCommand` variants.
  Stateful generation (tracks live node ids / assigned slots / open migrations) biased
  ~80/20 toward commands valid in context, with garbage retained deliberately: a *rejected*
  command must also preserve every invariant, and the rejection path is exactly where
  validate-then-mutate bugs live.
- **P1 — invariants always hold**: apply the sequence via `apply_local`, assert the catalog
  clean after every step.
- **P2 — snapshot/restore is lossless at any point**: apply a prefix, round-trip through
  *both* snapshot vehicles (the serialized `ClusterStateInner` openraft path and the
  `ClusterSnapshot` → `from_snapshot` DTO path — the audit showed they can disagree), apply
  the suffix, compare against the uninterrupted run. Catches the entire audit-defect-2 class
  for every field, not just `handoff_seq`, forever.
- **P3 — replay determinism**: the same sequence applied to two fresh states yields
  identical states (closes round-2 87/F2). Doubles as a purity guard: any wall-clock or
  randomness sneaking into apply breaks it loudly.
- **P4 — event conservation**: every `SlotHandoffPrepared` is eventually paired with exactly
  one `SlotHandoffReleased` across the sequence (the `release_events()` funnel, stated as a
  property instead of per-arm tests).

Plus one non-property deliverable that fits nowhere better: **frozen encoding fixtures** for
`ClusterCommand` and `ClusterStateInner` (golden JSON checked in, round-trip asserted) —
round-2 87/F6; a silent serde rename is a rolling-upgrade wire break today.

Runs in the normal test suite at moderate case counts; a `PROPTEST_CASES`-boosted pass joins
the nightly.

### W3 — Model checking the protocols (stateright)

Exhaustive small-scope search where random search is weakest: shallow interleavings of the
two protocols whose correctness is distributed across proposers, apply, and timers.

- **Model 1 — two-phase handoff**: coordinator + source + target + Raft-as-serializer,
  3 nodes / 2 slots / bounded retries. The transition function *is* production
  `apply_command` — no hand-translated abstraction to drift (the decisive advantage over
  TLA+ here). The model layer contributes only what Raft/network contribute in production:
  ordering, loss, duplication, leader changes. Safety: no interleaving of
  Prepare/Confirm/Abort/Complete/leader-change admits writes on two nodes for one slot;
  seqs never reused. Liveness: every prepared handoff reaches Released or Completed.
- **Model 2 — failover composite**: detector verdicts + `Failover{force}` racing
  MarkNodeFailed/Recovered and concurrent proposals from two would-be leaders. Safety: at
  most one Primary per slot after quiesce; epoch strictly grows across every promotion.
- Both run as `#[test]`s with bounded depth in the nightly (§8 D1 for the per-commit
  question); a found counterexample is checked in as a regression scenario replayed against
  the real state machine.

**Model 1 shipped** (issue 10, `frogdb-cluster/src/model/`). The small-scope hypothesis
*bends* rather than breaking: breadth and depth do not compose. Two slots **with** retries
did not terminate inside 25 s / 7 GB, so the full budget is two exhaustive configs run side
by side instead of one product — `cross_slot_scope()` (2 slots, 1 attempt: 1 306 692 states,
depth 30, 8 s release) and `deep_scope()` (1 slot, 3 attempts, 2 dup acks, 2 leader changes:
12 186 542 states, depth 54, 65 s release). Per-commit smoke is 31 324 states in 0.8 s
debug. Payoff: one counterexample that is neither issue 14 nor 15 — a source whose apply of
`Complete` lags its apply of `Prepare` past `barrier_ms` re-admits writes for a slot the
target already owns (issue 17), replayed against the state machine in `model/replay.rs`.

### W4 — Seeded fault schedules (deterministic simulation)

Generalize the five scripted turmoil cluster sims into one seed-driven scheduler:

- A seed derives the full schedule: which links partition when, which nodes SIGKILL/restart
  when, message delay distributions, barrier/lease timer skew. Same seed → same run,
  turmoil's determinism guarantee.
- Invariant checking at quiesce via the L5 surface (`DEBUG CLUSTER CHECK`) on every
  surviving node, plus the existing client-visible assertions (convergence, no acked-write
  loss) — and cross-node checks a single-state catalog cannot express: pairwise epoch
  monotonicity as observed by clients, single-writer-per-slot over the whole history.
- Nightly sweep at a fixed seed budget (§8 D4); a failing seed is committed to a regression
  list and runs forever after.
- Durability faults (lose-unsynced-writes on kill) ride on campaign 2's W2 crash harness,
  not turmoil — turmoil has no disk model. The raft-log/vote rows (FM-CLUSTER-098, issue
  73) get their level-5 witnesses there; this PRD does not duplicate that machinery, it
  consumes it.

### W5 — Live invariant surface + Jepsen integration

- `DEBUG CLUSTER CHECK`: admin/debug command returning the catalog's violations as a RESP
  array (empty = clean). Always compiled — Jepsen runs release binaries (§8 D3 for gating).
- Jepsen: add an invariant checker that calls it on every node at nemesis quiesce points and
  at final; a non-empty reply fails the test with the violation IDs in the analysis.
- Close the workload gaps the audit found: port `split-brain` and `zombie` workloads to the
  raft topology (today they are replication-topology only, `run.py:264/272`); run the 11
  raft workloads with no stored results plus the 4 `raft-extended` ones; store results.

### W6 — Spec and gate integration

- Cross-reference: each catalog invariant cites the FM rows it generalizes; rows whose
  invariant is now universally checked note the invariant ID. `lint-failure-modes` gains an
  optional `INV-*` vocabulary check (warn on dangling references) — small, same script.
- Mutation: re-run `just mutants` + gates for `frogdb-cluster` and `frogdb-cluster-runtime`
  on current code. Recorded scores predate rows 084–102 entirely; the catalog + property
  tests should move in-crate kill coverage for the 29 rows currently forced only from
  server-side integration tests.
- Fix the two mis-tagged rows (campaign-2 issue 09) while in the file.

## 4. Relationship to campaign 2

Complementary, not competing. Campaign 2 asks "is the code that should exist present?"
(chokepoints) and "is the evidence real?" (witness truth) — repo-wide. This PRD asks "does
the cluster state machine satisfy its definition of correct under permutations nobody
hand-wrote?" — one area, universal quantification. Shared infrastructure flows one way:
this PRD consumes campaign 2's crash harness (W2) and seam-gate wiring (`just lint-gates`),
and produces the invariant catalog that campaign 2's W3 re-witness pass can cite as forcing
machinery for cluster rows. If the pattern pays for itself here, replication is the obvious
second area (its FM spec has the same point-witness structure) — explicitly out of scope
until this one exits.

## 5. Sequencing

1. W1 (catalog + hook + forcing tests) — unlocks everything, immediately upgrades ~430
   existing tests.
2. W2 (proptest) — highest defect-catching density per line; P2 alone retro-covers audit
   defect 2's whole class.
3. W6 mutation re-run — cheap once W1/W2 land in-crate; re-baselines the gate honestly.
4. W4 (seeded schedules) and W5 (live surface + Jepsen) — W5's command is small and worth
   doing early inside W4's first PR since W4 consumes it.
5. W3 (stateright) — highest novelty, so last; scoped to the two named models, and dropped
   without ceremony if the state space defeats the small-scope hypothesis (§8 D1 records
   the budget).

Retro-validation gate at each step: revert each of the five audit fixes in a scratch
branch; count which layers flag it. Every one of the five must be caught by at least one
layer before this PRD exits — that is the falsifiable claim that the machine now catches
what the manual audit caught.

## 6. Exit criteria

1. Catalog exists in `frogdb-cluster`; every HARD invariant has a forcing test that fails
   when its check is deleted (mutation-visible); every DOCUMENTED-EXCEPTION cites a row or
   issue.
2. Post-apply + post-restore hooks active in all test/debug builds; full suite green under
   them (or each violation triaged into a fix or a cited exception — no third bucket).
3. P1–P4 + encoding fixtures land in-crate; nightly high-case pass wired.
4. Mutation gates re-run on current code, scores recorded, 084–102 inside the corpus.
5. Seeded scheduler replaces the scripted sims; nightly sweep wired; regression-seed list
   exists and is replayed in CI.
6. `DEBUG CLUSTER CHECK` shipped; Jepsen invariant checker consumes it; raft-topology
   split-brain + zombie workloads exist; the 15 result-less workloads have stored results.
7. Stateright: both models shipped with recorded state-space size and properties, or a
   written decision records why not (with the exploration budget that was tried).
8. Retro-validation: all five 2026-08-08 audit defects mechanically caught by ≥1 layer.
   **Run 2026-08-09 — 3/5 at the time of the run** (§6.1); FM-CLUSTER-099 has since been
   covered by the layer [issue 21](issues/done/21-no-layer-sees-the-raft-log-store.md)
   built, leaving FM-CLUSTER-102 and [issue
   22](issues/open/22-no-layer-generates-runtime-config-values.md).

### 6.1 Retro-validation results (issue 13, run 2026-08-09)

Method: each of the five 2026-08-08 audit fixes was inverted one at a time in the working
tree (throwaway; never committed), the layers were run against the reverted tree, and the
tree was restored before the next defect. **The spec forcing tests are excluded from the
verdict** — they are the point witnesses the fix shipped with, so counting them would make
the gate vacuous. They are listed anyway, because "the forcing test was the *only* thing
that failed" is exactly the finding this gate exists to surface.

Verdict at the time of the run: **3 of 5 caught by a non-forcing layer** (100, 101, 098).
The two misses — 099 and 102 — were filed as
[issue 21](issues/done/21-no-layer-sees-the-raft-log-store.md) and
[issue 22](issues/open/22-no-layer-generates-runtime-config-values.md); **exit criterion 8
is not met until both close.** Issue 21 has since closed, so 099 is caught and 102 is the
one remaining.

| defect | revert | L1 catalog+hooks | L2 P1–P4 | L3 stateright | L4 seeded schedules | seam gates | L5 Jepsen | verdict |
|---|---|---|---|---|---|---|---|---|
| **098** vote durability | `MetaDurability::for_key` → always `Buffered` | miss (290/291; only the forcing test) | n/a — below the state machine | n/a — below the state machine | n/a — turmoil has no disk model (§3 W4) | **CAUGHT** `just lint-durable-ack` | n/a (issue 07 open) | **caught** (seam gate) |
| **099** log-reader cache | `get_log_reader` → `Arc::new(RwLock::new(self.log_cache.read().clone()))` | miss (289/291; only the 2 forcing tests) | n/a — below the state machine | n/a — below the state machine | **miss** (100 seeds green; 500 seeds = the 36 known issue-20 seeds, zero new) | green | n/a (issue 07 open) | **caught** — by the storage-conformance layer [issue 21](issues/done/21-no-layer-sees-the-raft-log-store.md) built in response to this miss |
| **100** handoff generation | `from_snapshot` → `handoff_seq: 0` | **CAUGHT** INV-HANDOFF-1 via the `from_snapshot` hook | **CAUGHT** `p2_a_snapshot_restore_at_any_point_is_lossless` | **CAUGHT** `handoff_model_smoke`, `stale_source_admits_writes_after_ownership_moves` | not run (state-machine defect; the three layers above are decisive) | green | n/a (issue 07 open) | **caught** (3 layers) |
| **101** voter removal | `voter_change`: `RemoveNode`/`Failover{force}` arms → `None` | miss (cluster 290/291, runtime 77/78; only the 2 forcing tests) | n/a — voter set is not on the `apply_command` path | n/a — membership deliberately unmodelled (`model/failover/mod.rs:432`) | **CAUGHT** `just cluster-seeds 100` → new seed 35 | green | n/a (issue 07 open) | **caught** (seeded schedules) |
| **102** detector clamp | drop `let config = config.clamped();` | miss (75/78; only the 3 forcing tests) | n/a — different crate, no `apply_command` surface | n/a — the model generates verdicts, never a config | n/a — the scheduler never varies `FailureDetectorConfig` | not applicable | n/a (issue 07 open) | **MISS → [22](issues/open/22-no-layer-generates-runtime-config-values.md)** |

"n/a" above always means *structurally* out of reach, never "not bothered": every one is a
layer whose input alphabet cannot express the defect, and each is named in the row.

Per-defect notes:

- **098** — the catcher is not one of this PRD's five layers but campaign 2's chokepoint
  lint. `scripts/durable-ack.py` models the metadata path as three links (`for_key`
  classifies, `write_opts` renders, `set_meta` passes) and fails with "`save_vote` has no
  `write_opt(..)` with `set_sync(true)`". Worth recording precisely: at audit time
  `save_vote` was on that lint's ALLOWLIST, so the gate existed and was muzzled — the fix
  un-muzzled it. A gate is only a gate once its exception list is empty.
- **099** — the strongest evidence of the miss is the 500-seed sweep
  (`CLUSTER_SEEDS_JOBS=6`, 346.6 s): the leadership flaps that the FM row names as the
  trigger do occur in those schedules, but nothing compares what a reader serves against
  what is on disk, so a divergent read is only visible if it detonates client-side inside
  the same run. [Issue 21](issues/done/21-no-layer-sees-the-raft-log-store.md) closed this:
  openraft's `testing::Suite` now runs against the real store twice, once through a
  long-lived log reader, and a property judges both handles against the `raft_logs` column
  family after every append/truncate/purge. Re-reverting the fix now fails that property,
  shrunk to one operation and naming the term mismatch at the index. The suite also found
  a live defect on its first run — `truncate` was exclusive where openraft contracts it
  inclusive (FM-CLUSTER-103), fixed in the same branch.
- **100** — reuses and confirms the revert experiment banked in
  [issue 04](issues/done/04-properties-p2-p3-p4.md); re-run on current `main` it is now a
  three-layer catch. P2 shrinks to a 3-command counterexample (`AddNode`,
  `BeginSlotMigration`, `PrepareSlotHandoff`), and both the property and the two model
  tests fail through the *same* `from_snapshot` hook message — `INV-HANDOFF-1: slot N
  carries handoff seq 1 above the generation counter 0` — which is the layered design
  working as advertised: one catalog, three consumers, one violation string.
- **101** — `just cluster-seeds 100` fails on **seed 35** (`XNODE-SLOT-1: round 2: slots
  10922-16383 (5462 slots) is claimed by 2 nodes at once ({0, 2}), all reporting
  cluster_state:ok`), a seed that is green and unmuzzled on a clean tree. Two secondary
  observations from the same run that are *not* catches: (a)
  `test_cluster_scheduler_regression_seeds` also fails, but with "muzzled seed 3 now
  passes" — reverting the fix makes the issue-20 seeds green, i.e. **issue 20 is a
  consequence of the FM-CLUSTER-101 fix**, which is worth knowing but is muzzle
  bookkeeping, not detection; (b) `test_cluster_scheduler_same_seed_same_run` fails under
  the revert, but it fails on a clean tree too (seed 1, `commit-pending` vs `committed`),
  so it is the pre-existing scheduler nondeterminism, not a signal.
- **102** — the whole class ("a value that arrives from `frogdb.conf` reaches a constructor
  that assumes it is sane") has no generated coverage anywhere in the cluster runtime;
  `frogdb-cluster-runtime` has no `proptest` dependency at all. See issue 22.

Two cross-cutting conclusions for the campaign, beyond the per-defect scoring:

1. **The layers' reach is exactly the state machine.** Every catch landed on a defect that
   is expressible as a `ClusterStateInner` transition or as a client-visible outcome of
   one. Both misses live *outside* that boundary — one below it (the Raft log store), one
   beside it (a runtime constructor's config admission). The PRD's §2 table implies five
   fidelity levels over one system; in practice it is five levels over one *component*, and
   the audit found defects in the other components at the same rate.
2. **L1's blast radius is the reason 100 was a triple catch.** The invariant hook is what
   turned a proptest and two model runs into three independent witnesses of one violation,
   with an identical message. That is the argument for pushing the same hook down into the
   log store and the runtime rather than writing more point tests there.

## 7. Out of scope

- Fixing individual defects (they ride the campaign-2 wave tables; four of five audit fixes
  already landed spec-first).
- Extending the pattern to replication/persistence (explicitly a follow-up decision at
  exit).
- Performance/benchmarking of the cluster path; catalog checks are test/debug-only and add
  zero release-build cost by construction.
- TLA+ specs — stateright is this PRD's bet precisely because the model can embed the
  production transition function; write TLA+ only if stateright's scope proves too small,
  as a recorded decision.

## 8. Decisions (all RULED 2026-08-08)

1. **New dev-dependencies and CI budget.** `proptest` (W2) is uncontroversial house-style.
   `stateright` (W3) is a heavier bet: exhaustive checks are minutes-scale and belong in
   the nightly, not per-commit. **RULED: adopt both; run the real budgets nightly.**
   Per-commit runs get bounded-depth smoke configs (<10 s); record the W3 exploration
   budget (states/minutes) in the model file header so "it got slow" has a number.
   *Model 1 as built (issue 10):* smoke 31 324 states / 0.8 s debug per commit; nightly
   `cluster-model-nightly` + `just model-check` run the two full configs (1.3 M and 12.2 M
   states, 8 s and 65 s release solo, ~2.5 min under parallel load), pinned one-at-a-time
   in `.config/nextest.toml` because `deep_scope()` holds ~3 GB resident. The full runs are
   `#[ignore]`d, so `just lint-failure-modes` now lists with `--run-ignored all`.
2. **The dirty-state ruling INV-REF-1/2/3 forces.** `RemoveNode` leaves dangling
   migrations/replica parents (documented non-guarantee, pinned by test), and
   FM-CLUSTER-033 blesses `CompleteSlotMigration`'s unguarded owner insert.
   **RULED: fix the behavior, do not register exceptions** — make `RemoveNode` prune
   migrations and re-parent/detach replicas exactly as `Failover{force}` already does
   (the asymmetry between the two removal paths is itself the smell), and guard the
   Complete insert; amend both FM rows spec-first.
3. **`DEBUG CLUSTER CHECK` exposure.** **RULED: always compiled**, gated behind the
   existing admin/DEBUG surface like its DEBUG siblings — Jepsen needs it in release
   builds, and a read-only self-check is not attack surface beyond what DEBUG already
   grants.
4. **Nightly seed budget for W4.** **RULED: 500 seeds/night to start, runnable locally
   on a laptop** — the recipe must work as a plain `just` invocation with the budget in
   one Justfile variable, tuned from observed run time; nightly CI calls the same recipe.
5. **Catalog location.** **RULED: module in `frogdb-cluster`** — the mutation gate must
   see it, the server crate already depends on the cluster crate for the L5 command, and
   a crate boundary here is campaign 2's §4 ceremony finding all over again.
   Generalizing the catalog pattern to other areas stays a follow-up decision at exit
   (§7); the layered architecture is not cluster-specific, but each area ports it
   separately against its own state shape rather than through a shared framework.
