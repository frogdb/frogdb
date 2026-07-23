# Convert consistency.md [Design intent] rows into executable Jepsen/turmoil workloads

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 1/3, consequence 2/3 (score 2)
Area: Jepsen / Distributed testing infra

## Context

`consistency.md` documents several consistency guarantees as [Design intent] — asserted in docs but
never converted to executable tests: read-your-writes (line 27), monotonic reads (line 38),
acked-write-loss bound (line 86), replica staleness (line 89), within-connection ordering (line 138),
and pub/sub ordering (line 144).

The G-report assessment (CONFIRMED L1/C2) breaks these down by feasibility:
- **RYW** and **within-connection ordering** are cheaply testable today via a single-connection
  turmoil or Jepsen workload — no new harness capability needed.
- **Acked-write-loss bound** is the failover-durability gap already tracked as task 03
  (`jepsen-failover-durability-workload`, E#10) — do not duplicate; cross-reference only.
- **Pub/sub ordering** is partially addressable via the issue-10 capture seam, but lacks a
  cross-reconnect Jepsen workload to exercise the harder case.
- **Staleness** and **monotonic reads** (replica-side) need a dedicated replica-read client
  capability in the Jepsen/turmoil harness that doesn't exist yet — this is a harness prerequisite,
  not just a missing test.

## What to build

1. Single-connection RYW workload (turmoil and/or Jepsen): write then read on the same connection,
   assert the read observes the write.
2. Within-connection ordering workload: a sequence of operations issued on one connection, assert
   the server-observed order matches issue order.
3. Pub/sub-ordering Jepsen workload using the existing issue-10 capture seam, including the
   reconnect case where feasible.
4. New harness capability: a replica-read client for Jepsen/turmoil — a prerequisite for testing the
   monotonic-reads and staleness rows; scope explicitly as a deliverable of this task (or split into
   a tracked sub-task if large).
5. Update `consistency.md`, relabeling each row from [Design intent] to [Tested] as its
   corresponding test lands — no row should claim [Tested] without a merged test backing it.

## Acceptance criteria

- [ ] RYW single-connection workload implemented (turmoil or Jepsen) and passing.
- [ ] Within-connection ordering workload implemented and passing.
- [ ] Pub/sub-ordering Jepsen workload added, built on the issue-10 capture seam.
- [ ] Replica-read client capability added to the Jepsen/turmoil harness (prerequisite for
      monotonic-reads/staleness rows; may land as a follow-up if scoped separately).
- [ ] `consistency.md` rows relabeled [Design intent] → [Tested] individually as each corresponding
      test merges — never batch-relabeled ahead of actual test coverage.
- [ ] Acked-write-loss-bound row explicitly cross-referenced to task 03
      (`jepsen-failover-durability-workload`) rather than re-implemented here.

## Blocked by

Task 03 (`jepsen-failover-durability-workload`) covers the acked-write-loss-bound row — do not
duplicate that work in this task.

## References

- .scratch/testing-improvements/audit/G-jepsen-harness.md (`consistency-design-intent-rows-testable`)
- .scratch/testing-improvements/audit/verdicts-G.md (CONFIRMED L1/C2)
- consistency.md lines 27, 38, 86, 89, 138, 144
