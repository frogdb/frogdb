# Jepsen: kill-primary -> promote -> acked-write-fate workload is missing

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 3/3, consequence 3/3 (score 9)
Area: jepsen

## Context

There is no automated kill-primary -> promote-replica -> verify-acked-write-fate workflow anywhere
in the test suite. The acked-write-loss bound on failover is documented only as a
`consistency.md:75-90` [Design intent] row and has never been measured.

In the Jepsen harness, `client.clj:497-500` defines `promote-to-primary!` (issuing `REPLICAOF NO
ONE`) but it is never called from any workload. The `replication` suite (`run.py:145-172`) only
runs nemeses `none`/`partition`/`all-replication`; `all-replication`
(`nemesis.clj:1058-1096`) kills and restarts the primary but nothing ever promotes a replica in its
place — node `n1` simply comes back as primary. So the harness's harshest replication-topology
nemesis never actually exercises failover, only crash/restart. Extended nemeses (clock skew, disk
faults, slow network, memory pressure) are wired for the `raft-extended` suite only — the
`REPLICATION` topology gets none of them.

This is a merged gap: area E flagged it as `kill-primary-promotion-ackd-write-fate-untested`
(E#10, dedupe target) and area G independently flagged the harness-side manifestation as
`failover-durability-untested`. Both verdicts confirmed at L3/C3 and were folded into one task,
with the E (replication) framing winning since it states the invariant being tested (acked-write
loss bound) rather than just the harness mechanics.

## What to build

- New Jepsen workload: `replication-failover` — track acknowledged writes, kill the primary via
  the existing REPLICATION-suite nemesis, call `promote-to-primary!` on a replica, then assert the
  loss set (acked writes that are actually missing post-promotion) is within the documented bound
  from `consistency.md:75-90` (ideally: empty, for `WAIT`-acknowledged writes at full replica
  count).
- Add clock-skew and slow-network nemeses to the `REPLICATION` topology (currently
  `raft-extended`-only), so failover is exercised under realistic fault conditions, not just clean
  kill/restart.
- Wire the new workload into `run.py`'s `replication` suite alongside `none`/`partition`/
  `all-replication`.

## Acceptance criteria

- [ ] `replication-failover` Jepsen workload exists: tracked writes, primary kill, explicit
      `promote-to-primary!` call, and a checker that asserts acked-write loss is within the
      documented bound
- [ ] Workload registered in `run.py` under the `REPLICATION` topology/suite
- [ ] Clock-skew and slow-network nemeses available (and exercised in at least one suite
      combination) on the `REPLICATION` topology, not just `raft-extended`
- [ ] Workload run at least once on a testbox/CI environment with results attached to this issue

## Blocked by

None - can start immediately. Complements the deterministic-simulation equivalent covered by
`11-turmoil-cluster-raft-topology.md` (turmoil is faster-feedback per-PR coverage; this Jepsen
workload is the higher-fidelity/longer-running counterpart) and should be included once ready in
the CI wiring done under `10-jepsen-nightly-ci.md`.

## References

- `testing/jepsen/frogdb/src/jepsen/frogdb/client.clj:497-500` — `promote-to-primary!`, never called
- `testing/jepsen/run.py:145-172` — replication suite nemesis list (none/partition/all-replication)
- `testing/jepsen/frogdb/src/jepsen/frogdb/nemesis.clj:1058-1096` — all-replication kills+restarts,
  no promotion
- `consistency.md:75-90` — [Design intent] acked-write-loss bound, unmeasured
- `server/tests/integration_replication.rs:1515-1560` — `test_secondary_replication_id_failover`
  (role-only check, not a durability workload; see also issue 34)
- Source: `.scratch/testing-improvements/audit/E-replication.md` gap #10, `.scratch/testing-improvements/audit/verdicts-E.md` #10
- Source: `.scratch/testing-improvements/audit/G-jepsen-harness.md` `failover-durability-untested`,
  `.scratch/testing-improvements/audit/verdicts-G.md` (same, "DEDUP: fold into E#10; replication framing wins")
