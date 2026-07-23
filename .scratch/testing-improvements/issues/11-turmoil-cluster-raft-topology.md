# No deterministic (turmoil) simulation coverage for cluster/raft topology

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 3/3, consequence 2/3 (score 6)
Area: cluster

## Context

`simulation.rs` (4309 lines, the turmoil-based deterministic simulation harness) has zero cluster
coverage: no Raft, no `MOVED`/`ASK` redirects, no slot migration, no multi-node topology at all. Its
only cluster-adjacent tests are internal single-shard hash-tag tests (lines 525-536), which don't
exercise multiple nodes. Today, multi-node behavior is only exercised by integration tests (real
processes, no fault injection, no linearizability checking) or by the Jepsen suite (not in CI —
see `10-jepsen-nightly-ci.md`). This leaves a gap for fast, deterministic, seed-reproducible,
per-PR coverage of timing-sensitive cluster/raft paths — exactly the class of bug (leader partition
mid-migration, redirect races) that's hardest to catch in slower, less-reproducible test tiers.

This was independently flagged by two source reports: area F as
`no-cluster-topology-in-turmoil` (F#6) and area G as the cluster/raft half of
`no-deterministic-sim-for-replication-or-raft`. The adversarial verification pass merged them,
with F's framing (cluster+raft topology sim) winning as the primary task description; G's
*replication*-topology half of the same gap is tracked separately (outside this task's scope) as
`turmoil-replication-topology`.

## What to build

- Turmoil-based `sim_harness` extension covering cluster topology: multiple simulated nodes running
  real Raft, slot ownership, and migration state machinery.
- Writes routed through `MOVED`/`ASK` redirects deterministically.
- Scenario: leader partition occurring mid-slot-migration — assert eventual single-owner
  convergence deterministically (same seed always reproduces the same outcome, enabling bisection).

## Acceptance criteria

- [ ] Turmoil simulation supports ≥2-node cluster topology with real Raft leader election
- [ ] Test: writes through the simulation correctly follow `MOVED`/`ASK` redirects to converge on
      the correct owner
- [ ] Test: leader partitioned from the raft group while a slot migration is in flight; assert
      deterministic single-owner convergence once the partition heals
- [ ] Runs fast enough (or is tiered appropriately) for per-PR CI, given its deterministic/seeded
      nature

## Blocked by

None - can start immediately. Complements the higher-fidelity, longer-running Jepsen equivalent in
`03-jepsen-failover-durability-workload.md` and `06-jepsen-raft-chaos-blind-checker.md` (different
fault classes, same underlying leader-partition/migration risk area).

## References

- `server/tests/simulation.rs` (4309 lines total; module doc line 6; cluster-relevant tests only at
  `:525-536`, internal single-shard hash-tag only)
- `server/tests/simulation.rs` `test_network_partition_client_isolated` — client isolation only, not
  multi-node
- Source: `.scratch/testing-improvements/audit/F-cluster.md` gap #6 `no-cluster-topology-in-turmoil`,
  `.scratch/testing-improvements/audit/verdicts-F.md` #6 ("MERGE with G's raft half into one 'deterministic
  multi-node cluster+raft sim' task; keep G replication-sim half separate")
- Source: `.scratch/testing-improvements/audit/G-jepsen-harness.md`
  `no-deterministic-sim-for-replication-or-raft`, `.scratch/testing-improvements/audit/verdicts-G.md` (same,
  "DEDUP: F#6 framing wins for cluster half; keep replication half here or merge into one infra
  task")
