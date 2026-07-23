# No deterministic turmoil simulation for multi-node replication topology

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 3/3 (score 6)
Area: jepsen

## Context

`simulation.rs` (the turmoil-based deterministic simulation harness, 4309 lines) covers only standalone scatter-gather and client-to-server partition scenarios (module doc at line 6; `test_network_partition_client_isolated` is client isolation only). There is no multi-node replication topology under turmoil: no primary/replica failover simulation, no deterministic seed-reproducible coverage of replication-specific timing races. Today, multi-node replication is exercised either by integration tests (no fault injection, no linearizability checking) or by Jepsen (not wired into CI — see task 10 in this batch's sibling issue for that gap). This is the replication half of the broader "no deterministic sim for multi-node" gap; the cluster/raft half is tracked separately (per verdict, F#6 framing wins for that half — do not duplicate it here).

Expected: a deterministic, seed-reproducible turmoil harness exercising ≥2 FrogDB instances in a primary/replica relationship, capable of injecting partitions/timing faults and feeding resulting histories to the existing WGL (write-generalized-linearizability) checker (`crates/testing/src/checker.rs`) for automated verification — giving fast, deterministic, CI-friendly coverage of races that are currently only reachable (if at all) via slow, non-CI Jepsen runs.

## What to build

- Extend `simulation.rs` (or add a sibling module) with a turmoil topology hosting ≥2 FrogDB instances wired as primary/replica.
- Add replication-failover simulation tests: partition primary from replica, force failover/promotion, heal partition, and assert convergence.
- Feed resulting operation histories into the WGL checker (`check_linearizability`) to get automated pass/fail rather than manual inspection.
- Wire at least one such test into the per-PR or nightly test run (not just available to run by hand).

## Acceptance criteria

- [ ] Turmoil harness supports ≥2-instance primary/replica topology with deterministic, seed-reproducible scheduling.
- [ ] At least one replication-failover simulation test exists: partition, promote, heal, assert convergence.
- [ ] Test feeds its operation history to the WGL checker and asserts a checker verdict (not just "no panic").
- [ ] New test(s) run in CI (per-PR or nightly) alongside existing turmoil tests.

## Blocked by

None - can start immediately.

## References

- `frogdb-server/crates/server/tests/simulation.rs` (module doc line 6; `test_network_partition_client_isolated`)
- `frogdb-server/crates/testing/src/checker.rs` (WGL linearizability checker, `check_linearizability`)
- Related: cluster/raft-topology half of this gap tracked separately (F#6 framing) — do not duplicate.
