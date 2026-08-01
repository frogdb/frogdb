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
