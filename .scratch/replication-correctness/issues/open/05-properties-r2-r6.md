# 05 — Properties R2–R6 and the boosted recipe

Status: needs-triage

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

- [ ] R2 round-trips through both vehicles at an arbitrary prefix point and compares against the
      uninterrupted run
- [ ] R3, R4 and R5 land in `frogdb-replication` at default case counts in the normal suite
- [ ] R6 lands in `frogdb-replication-runtime`, which gains its first `[dev-dependencies]`
      section, and asserts totality plus agreement of `has_quorum` / `write_fence_reason`
- [ ] `just replication-proptest CASES='200000'` exists with its own nextest profile and is wired
      into the nightly through the workflow generator (`just workflow-gen --check` green)
- [ ] `just mutants-diff frogdb-replication` and `just mutants-diff frogdb-replication-runtime`
      triaged before push

## Blocked by

- Issue 04 (`.scratch/replication-correctness/issues/`) — R2, R4 and R5 drive the same
  `arb_link_sequence` alphabet and reuse its muzzle machinery.
