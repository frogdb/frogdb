# 01 — Generic DEBUG-command nemesis plumbing + slow-node tracer

Status: needs-triage

## Parent

[PRD](../../PRD.md) W1.

## What to build

A reusable jepsen nemesis that issues an arbitrary DEBUG command against a chosen node
(mirroring how `invariant.clj` got a parameterized *surface*, this is the parameterized
*fault*): command + args + target selection + optional recovery command, composing with the
existing nemesis packages the way `wrap-nemesis` does. Prove it end-to-end with a slow-node
nemesis built on `DEBUG SLEEP` — softer than SIGSTOP, the node stays connected while its
event loop stalls — wired into at least one replication workload and one cluster workload.

Verify the command's exact name/arity at the server dispatch table first; the PRD's surface
list is an investigation snapshot.

## Acceptance criteria

- [ ] Generic debug-nemesis constructor takes command/args/targeter/recovery; no
      per-workload copies
- [ ] Slow-node nemesis on `DEBUG SLEEP` runs inside a partition-style schedule on one
      replication and one cluster workload
- [ ] Run evidence: one store id per workload with the nemesis ops visible in the history
      and analysis still `:valid? true` on a clean build
- [ ] Invariant sweeps (issue 13, replication-correctness) still fire at quiesce/final —
      composition, not replacement

## Blocked by

None — can start immediately (D1 does not bite: SLEEP already exists and is always
compiled).
