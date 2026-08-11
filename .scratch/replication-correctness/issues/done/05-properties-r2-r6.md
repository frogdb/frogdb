# 05 — Properties R2–R6 and the boosted recipe

Status: done

## Parent

[PRD](../../PRD.md) §3 W2; budget numbers inherited in §8 D1.

## What to build

The five remaining W2 properties on top of issue 04's generator.

- **R2 — save/restore is lossless at any point, through both vehicles.** Apply a prefix, round-trip
  the state through `ReplicationState::save` / `load_or_create` *and* through the staged path
  (`read_staged_replication_metadata` → `apply_staged_metadata`, `state.rs:52/420`), apply the
  suffix, and compare against the uninterrupted run. The two vehicles disagreeing is exactly the
  class cluster's P2 caught as FM-CLUSTER-100, and here it is also the FM-PERSISTENCE-039 window.
- **R3 — the PSYNC decision is total.** `PartialSyncReplay::handle_partial_sync_request`
  (`primary/replay.rs:350`) never panics, yields exactly one `ReplayDecision`, and every granted
  `+CONTINUE` names a range that is contiguous and inside the ring. This states
  FM-REPLICATION-013/014/015 once for all inputs instead of three times for three scenarios.
- **R4 — determinism and purity.** The same action sequence against two fresh nodes yields
  identical views. This doubles as a wall-clock guard on the decision path — the exact class of
  cluster issue 23 (`.scratch/cluster-correctness/issues/`), caught here cheaply instead of in a
  turmoil fingerprint.
- **R5 — offset conservation.** `live` minus the seed equals the sum of bytes admitted by
  `advance`, and `landed <= applied <= live` survives arbitrary interleavings of `claim` / `land`
  / `freeze` / `admit_divergence` / `retire_replica_applies`.
- **R6 — fence admission is total.** The `frogdb-replication-runtime` half, and the cheapest way
  to move that crate's mutation score. `ReplicationQuorumChecker` (`quorum.rs:52`) is
  all-synchronous over atomics, so generate `(config, replica set, departure code)` triples and
  assert `has_quorum` and `write_fence_reason` are total, agree with each other, and never arm
  from a state no session ever streamed. The crate has **no `[dev-dependencies]` section at all**
  today and gains one. This is the direct analogue of cluster issue 22's config-admission
  properties C1/C2 — the layer that turned that campaign's second retro-validation miss into a
  catch — and the targeted fix for the ADR 0004-era 50%-score survivors (`apply_single`,
  `apply_transaction`, `apply_group`, `export_live_dataset`, `install`, `read_snapshot`).

Plus the boosted recipe: `just replication-proptest CASES='200000'`, mirroring `cluster-proptest`
(Justfile:174) including its own nextest profile so the default profile's 120 s hard kill does not
truncate the run, and called from the nightly. Default case counts stay in the normal suite.

Both crates are LOCKED — `just mutants-diff` on each before push.

## Acceptance criteria

- [x] R2 round-trips through both vehicles at an arbitrary prefix point and compares against the
      uninterrupted run
- [x] R3, R4 and R5 land in `frogdb-replication` at default case counts in the normal suite
- [x] R6 lands in `frogdb-replication-runtime`, which gains its first `[dev-dependencies]`
      section, and asserts totality plus agreement of `has_quorum` / `write_fence_reason`
- [x] `just replication-proptest CASES='200000'` exists with its own nextest profile and is wired
      into the nightly through the workflow generator (`just workflow-gen --check` green)
- [x] `just mutants-diff frogdb-replication` and `just mutants-diff frogdb-replication-runtime`
      triaged before push

## Blocked by

- Issue 04 (`.scratch/replication-correctness/issues/`) — R2, R4 and R5 drive the same
  `arb_link_sequence` alphabet and reuse its muzzle machinery.

## Resolution (2026-08-11)

All five properties landed, plus the boosted recipe. Issue 04's `LinkNode` harness was extended
rather than duplicated; R6 needed a second harness only because it lives in a different crate.

**R2–R5** — `frogdb-server/crates/replication/src/properties.rs`, on issue 04's generator and
muzzle.

- `r2_a_save_point_round_trip_is_transparent` splits the generated sequence at an arbitrary point,
  round-trips through *both* vehicles at the split (`ReplicationState::save` → `load_or_create`,
  and `read_staged_replication_metadata` → `apply_staged_metadata`), applies the suffix, and
  compares the final view against an uninterrupted control run over the same sequence.
- `r3_every_psync_request_yields_one_sound_decision` drives `handle_partial_sync_request` over generated
  `(replid, offset)` requests against a live ring: exactly one `ReplayDecision`, no panic, and
  every granted `+CONTINUE` names a range that is contiguous and inside the ring.
- `r4_the_same_actions_reach_the_same_node` replays one sequence against two fresh nodes and requires
  identical views — the wall-clock guard for the cluster-issue-23 class.
- `r5_the_offset_triple_is_conserved` ledgers the bytes `advance` admitted and requires
  `live - seed == admitted` and `landed <= applied <= live` after every step.

**R6** — new `frogdb-server/crates/replication-runtime/src/properties.rs`; the crate gained its
first `[dev-dependencies]` section (`proptest`, dev-only). Two things had to exist first:

- `ReplicationQuorumChecker::view()` (`quorum.rs`), a read-only three-load capture seam.
  `INV-FENCE-1` and `INV-SESSION-3` are claims about the arming latch held against the session
  registry; `frogdb-replication` owns the catalog but not the checker, so both entries were
  *skipped everywhere* before this seam — R6 is the first thing that evaluates them.
- Two checkers per node. `has_quorum` is the only caller of `arm_if_streaming`, so an audit that
  decides on every step destroys the unarmed-while-streaming state under test. `gate` decides only
  when the generated sequence says so and is what the catalog reads; `audited` carries the
  totality and agreement claims. They are independent because `has_quorum` mutates nothing but the
  checker's own `armed` atomic — pinned by
  `the_two_checkers_do_not_share_an_arming_latch`.

The muzzle is a *violation* filter rather than an action filter, because these shapes come from
the absence of an action. Wall-clock flake is designed out: every configured freshness window
(60 s, 1 h) sits far below the 4-hour `backdate_last_ack_for_test`, so staleness is decided by the
backdate and never by elapsed real time (`the_backdate_outruns_every_configured_window`).

**Defects.** No new defect on R2–R5; the existing muzzle (issues 16–19, 21, 22) held. R6 rejected
two states on its first run: [issue 19](../open/19-self-fence-arms-only-on-the-write-path.md) (the latch is
never installed) and a new one,
[issue 23](../open/23-demotion-keeps-the-departure-it-disarmed.md) — `RoleManager::demote` drops the
arming latch and keeps `last_streaming_departure`, leaving a demoted node permanently carrying a
streaming departure it has no witness for. They are told apart by `ledger.demoted`, checked first,
so a fix to either turns exactly one `pinned_issue_NN_*` witness red.

**Recipe.** `just replication-proptest CASES='200000'` now covers both harnesses
(`-p frogdb-replication -p frogdb-replication-runtime -E 'test(/properties/)'`), with
`.config/nextest.toml` overrides for the runtime crate in both the `default` (30 s × 4) and
`replication-proptest` (120 s × 4) profiles. Note `package()` is an *exact* name match, so
`frogdb-replication-runtime` needs its own override rather than a prefix glob that would silently
adopt future `frogdb-replication-*` crates. The nightly generator
(`workflow_gen/.../replication_nightly.py`) names both harnesses; `just workflow-gen --check` green.

**Numbers.** Default suite: `frogdb-replication` 15/15 in 1.628 s, `frogdb-replication-runtime`
9/9 in 1.799 s. Boosted pass at `PROPTEST_CASES=20000`, 24/24 in 842 s wall (all six properties
concurrently, laptop, debug): R6 52.9 s, R5 89.2 s, R1 536.8 s, R3 542.2 s, R4 802.6 s, R2 842.6 s.

That is the one number that moved a decision. Issue 04 sized the nightly's 90-minute ceiling off a
single-property 30 k measurement (~290 cases/s); R2 and R4 stand up a real node on a real temp
directory *and* round-trip it, which costs ~42 ms/case, so the 200 000-case default projects to
about 2.5 hours, not 15 minutes. The nightly's `timeout-minutes` is raised to 240 rather than the
budget cut: the job is change-gated and runs on a free runner, so wall clock is the cheaper side to
spend. The measurement is recorded at the ceiling in `replication_nightly.py`.

**Mutation.** `just mutants-diff frogdb-replication-runtime`: 1 mutant in the diff, unviable (the
`view()` return-value mutants do not build against `FenceView`'s non-`Default` fields), 0
survivors. `just mutants-diff frogdb-replication`: `No mutants to filter` — this crate's half of
the diff is `#[cfg(test)]`-only, which cargo mutants does not mutate. No survivors to document.
