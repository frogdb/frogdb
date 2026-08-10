# 11 — Extract the topology-agnostic half of the seeded scheduler

Status: needs-triage

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

- [ ] `simulation/schedule.rs` holds the derivation, fingerprint, regression/muzzle machinery and
      generic fault application, parameterized by per-arm `Family` / budget / op vocabulary
- [ ] `spawn_scheduled_hosts`, `parse_cluster_nodes` and `check_cross_node` stay in the cluster
      arm; `run_seed` stays duplicated per arm rather than genericized
- [ ] `parse_check_entry` / `hard_violations` are typed over `frogdb_types::Violation`
- [ ] Fingerprints unchanged: a fixed seed set produces identical schedules and
      `RunOutcome::fingerprint` values before and after the move, with the recorded values in this
      issue
- [ ] Cluster scheduler tests and `just cluster-seeds` green; `cluster-regression-seeds.txt`
      replays unchanged

## Blocked by

- Issue 01 (`.scratch/replication-correctness/issues/`) — `parse_check_entry` and
  `hard_violations` are typed over `frogdb_types::Violation`, which does not exist until 01 lands.
