# Bounded-duration partition primitive — failure-detector false positives and quorum loss are untestable

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: INFRASTRUCTURE.md I13
LOE: 2–4 days (estimated)
Tier: B
Area: crates/testing / partition primitive (cluster, slot migration)
Asked by: 04 (F4). **Dropped from `MASTER.md` §6.**

## Context

The failure detector's two interesting behaviours — declaring a healthy node dead after a
transient blip, and losing quorum — both require partitioning one specific node for a known
number of health-check intervals and then healing. Nothing in the suite can do that. Round 1
tried and hit an upstream turmoil bug on *indefinite* partitions; a *bounded* partition may
avoid it, which is the first thing to confirm.

## Evidence

- **Shape**: turmoil or `crates/testing/partition` scenario that partitions a *specific* node
  from the leader for a *bounded number of health-check intervals*.
- **Why**: unlocks both false-positive and quorum-loss failure-detector testing;
  `cluster_flags`' `SelfFenceGate` becomes end-to-end testable as a side effect.
- **Caveat**: round 1 hit an upstream turmoil 0.7.1 port leak that makes *indefinite*
  partitions impossible. A *bounded* partition may sidestep it — confirm before committing.

## What to build

1. First, confirm the caveat: reproduce the turmoil 0.7.1 port leak and establish whether a
   bounded partition sidesteps it. If it does not, stop and re-triage this issue rather than
   working around the leak.
2. A primitive that partitions a named node from the leader for a bounded number of
   health-check intervals, then heals.
3. Scenarios for both directions: a blip shorter than the detector threshold that must **not**
   trigger failover, and a partition long enough to lose quorum that must.
4. An end-to-end assertion on `cluster_flags`' `SelfFenceGate`, which becomes reachable as a
   side effect.

## Acceptance criteria

- [ ] The port-leak question is answered in writing on this issue before implementation
      lands.
- [ ] A scenario partitions one named node for N health-check intervals and heals it, with N
      a parameter.
- [ ] One scenario asserts no failover for a sub-threshold blip; one asserts quorum loss and
      recovery for a supra-threshold partition.
- [ ] `SelfFenceGate` is asserted end-to-end in at least one scenario.
- [ ] The suite does not leak ports across repeated runs of the new scenarios.

## Test boundary

Level 5 — partitioning a specific node from a leader and observing failure detection is
inherently multi-node with a controllable network; no lower level has a leader to be
partitioned from.

## Depends on

Nothing.

## Re-triage 2026-08-06

**Verdict: still-valid**

No reusable primitive landed. Two corrections to the body. First, `crates/testing/partition` is
not a network-partition module at all — `frogdb-server/crates/testing/src/partition.rs` is Lua
key-partitioning (`default_keys_of`, `partition_by_key`); the network-fault work lives inline in
`frogdb-server/crates/server/tests/simulation.rs`. Second, the port-leak caveat is already
answered in-tree, not on this issue: `simulation.rs:5341-5352` documents that turmoil 0.7.1 leaks
an ephemeral port per cancelled dial, so the scenarios use `sim.hold()`/`sim.release()` (which
*queues* traffic and lets the dials complete on heal) instead of `sim.partition()`, with a
bounded ~3 s window. But both hold-based scenarios predate this issue
(`run_cluster_leader_partition_migration`, and `run_cluster_asymmetric_partition_false_failover`
at `simulation.rs:5456` from `98495716`, 2026-07-23), so none of criteria 2-5 is discharged: the
window is hardcoded rather than N health-check intervals, there is no quorum-loss scenario, and
`SelfFenceGate` is still asserted only at level 2. What the cluster lock *did* add is level-2
coverage of the behaviours this primitive was wanted to unlock —
`frogdb-server/crates/cluster-runtime/src/flags.rs:112` `SelfFenceGate` is forced by
FM-CLUSTER-059 (`self_fence_gate_follows_live_flag` and three siblings), detector hysteresis by
FM-CLUSTER-052 (`test_health_table_latch_survives_flapping_recovery`, …) and quorum arithmetic by
FM-CLUSTER-055 — which lowers this issue's value without discharging it.
