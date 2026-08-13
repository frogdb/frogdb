# 08 — `DEBUG CLUSTERLINK KILL` app-layer link drop

Status: needs-triage

## Parent

[PRD](../../PRD.md) W2.

## What to build

Redis-precedent `DEBUG CLUSTERLINK KILL` (Redis: `<to|from|all> <node-id>`): drop a
cluster-bus link at the application layer, per-peer and optionally asymmetric — cheaper
than iptables, no root, and composable with client traffic staying up. Research the Redis
implementation for exact semantics. Nemesis on issue 01's plumbing; an asymmetric-partition
schedule that iptables cannot express cheaply (A hears B, B does not hear A, clients reach
both) exercises the cluster failure detector and epoch machinery.

## Acceptance criteria

- [ ] Command implemented with Redis-compatible name/arity; kills the link(s), reconnection
      proceeds by normal bus recovery; deviations documented
- [ ] Locked-crate discipline (cluster crates; mutation gates; spec impact per D2)
- [ ] Asymmetric-link nemesis schedule on a cluster workload; clean `:valid? true` store id
      cited; cluster invariant sweep stays wired
- [ ] Comparison note in the workload docs: what this covers that the iptables partitioner
      does not (asymmetry, per-peer, bus-only vs full-node)

## Blocked by

- Issue 01 (`.scratch/jepsen-fault-injection/issues/`) — plumbing.
- PRD rulings D1/D2.

## Comment (2026-08-13)

PRD rulings D1-D4 settled 2026-08-13 (see PRD). Remaining blocker: predecessor issues only.
