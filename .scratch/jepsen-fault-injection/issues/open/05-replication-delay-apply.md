# 05 — `DEBUG REPLICATION DELAY-APPLY` + lag nemesis

Status: needs-triage

## Parent

[PRD](../../PRD.md) W2.

## What to build

A replica-side apply throttle (`DEBUG REPLICATION DELAY-APPLY <ms>`, 0 clears) that delays
WAL application without touching the network, producing real, controllable lag. Nemesis on
issue 01's plumbing; wire into the `lag` workload and any WAIT-bearing workload. This is
the deterministic forcing tool for schedules that today only luck into lag: WAIT timeout
paths, the applied gate under pressure, backlog growth, and the write-free streaming
windows behind replication-correctness issues 16 and 19.

## Acceptance criteria

- [ ] Command lands in the replication runtime path; locked-crate discipline (mutation
      gates; spec impact per D2)
- [ ] Lag nemesis produces measurable, bounded lag visible in INFO and the invariant
      sweep's offset groups; clears cleanly
- [ ] `lag` workload runs on injected delay with a clean `:valid? true` store id cited
- [ ] Demonstrated forcing: a schedule that reliably reaches an issue-16 or issue-19 shape
      (allowlisted with the issue citation, not re-filed) — proving jepsen can now hold
      those defects under test pending their rulings

## Blocked by

- Issue 01 (`.scratch/jepsen-fault-injection/issues/`) — plumbing.
- PRD rulings D1/D2/D4 (interacts with open replication-correctness rulings 16/19).
