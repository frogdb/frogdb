# 08 — Close the raft-topology workload gaps

Status: needs-triage

## Parent

[PRD](../../PRD.md) §3 W5 (audit blind spot B3 / workload inventory).

## What to build

The audit found `split-brain` and `zombie` workloads exist only for the replication
topology (`jepsen/run.py:264/272`), and 11 raft workloads plus 4 `raft-extended` ones
have no stored results. Port the two workloads to the raft topology, run all 15
result-less workloads, store results under the standard results path.

## Acceptance criteria

- [ ] `split-brain` and `zombie` runnable against the raft topology from `run.py`
- [ ] All 15 previously result-less workloads have stored, passing results (failures
      filed as issues, not buried)
- [ ] Issue-07 checker active in these runs once it lands (do not block on it)

## Blocked by

None - can start immediately.
