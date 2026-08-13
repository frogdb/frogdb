# 10 — `DEBUG REPLICATION VIEW` dump + cross-node jepsen checker

Status: ready-for-agent

## Parent

[PRD](../../PRD.md) W3.

## What to build

`DEBUG REPLICATION VIEW`: serialize the node's `ReplicationView` (the plain-data projection
from replication-correctness issue 02) as JSON. The existing CHECK commands return
violations only and are node-local by design; the raw view lets jepsen assert fleet-level
claims no single node can: replication-id agreement between primary and streaming replicas,
replica acked/applied offset ≤ primary live head, exactly one primary per replication
group, and per-node offset monotonicity across successive sweeps.

Checker extends the surface-map pattern (`invariant.clj`): collect views from all nodes at
quiesce/final, evaluate cross-node predicates, report under `:replication-topology`. Scope
of predicates per D3 ruling (strictly-cross-node vs defense-in-depth re-checks).

## Acceptance criteria

- [ ] `DEBUG REPLICATION VIEW` returns the full view as JSON; `Option` groups serialize as
      null-or-value matching the D7 semantics ("absent = not my role", never fabricated)
- [ ] View serialization is read-only over the existing projection (`try_read`, no new
      locks); no locked-crate behavior change
- [ ] Cross-node checker evaluates the D3-ruled predicate set; seeded-violation evidence
      (throwaway skew in one predicate, reverted) + clean `:valid? true` store id
- [ ] Known open defects that violate a cross-node predicate (issues 21/22's WAIT
      over-claims may) get narrow allowlist entries citing their numbers, not re-files

## Blocked by

- PRD ruling D3.
- Replication-correctness issue 03 landed the command surface pattern to mirror; no open
  blocker there.

## Comment (2026-08-13)

D3 ruled (cross-node-only). Unblocked.
