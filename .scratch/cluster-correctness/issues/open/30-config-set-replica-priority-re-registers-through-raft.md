# 30 — `CONFIG SET cluster-replica-priority` never propagates: peers score a stale priority until the node's next re-registration

Status: ready-for-agent

## Parent

`specs/cluster.md` State-space review (the `cluster-replica-priority` runtime-flag row's
`⚠ review` cell, resolved by the 2026-08-13 ruling below). The asymmetry itself was first
documented — as behavior, not as a defect — by FM-CLUSTER-058.

## What is wrong

A node's promotion priority lives in two places that can disagree indefinitely:

- `ClusterRuntimeFlags.replica_priority: AtomicU32` — node-local, updated live by
  `CONFIG SET cluster-replica-priority` (`cluster-runtime/src/flags.rs:88`).
- `NodeInfo.replica_priority: u32` — Raft-replicated, written only by `AddNode`
  registration/re-registration (`cluster/src/types.rs:74`).

`effective_priority` (`cluster-runtime/src/failure_detector.rs:763-769`) reads the live atomic
for *this* node and the replicated field for every peer. So after `CONFIG SET`, the node scores
itself with the new value while every peer keeps scoring it with the old one — not for a bounded
window, but until the node happens to re-register (`AddNode`), which in practice means its next
restart. FM-CLUSTER-058's own note names the consequence: two nodes running selection
concurrently mid-change can pick different failover targets, i.e. two competing `Failover`
proposals — exactly the nondeterminism the deterministic-tiebreak invariant exists to prevent.

Redis does not have this permanent split: `replica-priority` changes propagate to every observer
via gossip within a few PING rounds. FrogDB replaced gossip with Raft, but never gave the
priority change a Raft vehicle.

## Ruling (2026-08-13)

**The Raft-replicated `NodeInfo.replica_priority` is the single authority for cross-node
scoring, and `CONFIG SET cluster-replica-priority` must converge it: a successful `CONFIG SET`
also proposes an `AddNode` re-registration carrying the new priority (same registration shape as
boot, `reconcile_incoming_epoch` semantics unchanged).** The node-local atomic keeps exactly one
authoritative role: this node's *own* immediate view, so priority 0 still removes the node from
its own candidate set the instant the command lands (FM-CLUSTER-058's live-tunable observable),
without waiting for the Raft commit. The divergence window shrinks from "until next restart" to
one Raft round-trip, and a failed/unreachable-leader proposal leaves the flag set locally while
the replicated value converges on retry — the transient window is acceptable; the permanent
split is not.

## What to build

Spec-first (locked crates: `frogdb-cluster`, `frogdb-cluster-runtime`).

- On `CONFIG SET cluster-replica-priority`, after storing the atomic, propose `AddNode`
  re-registration for self with the updated priority (reuse the boot-time registration path).
  Fire-and-forget with retry is acceptable; the local store must not be gated on the proposal
  succeeding (a partitioned node must still be able to remove itself from its own candidate
  set).
- Amend FM-CLUSTER-058: the "Documented asymmetry" note and the "no re-published `NodeInfo`"
  observable become the ruled convergence semantics — live self-view plus one-round-trip peer
  convergence via re-registration. (Row edit tracked here, not in issue 29's sweep — this ruling
  post-dates that sweep's ledger.)
- Update `specs/cluster.md` TR-CLUSTER-027's postcondition and the State-space row's `Pending`
  marker once the code lands.

## Acceptance criteria

- [ ] FM-CLUSTER-058 amended: peers converge to a `CONFIG SET` priority after one committed
      `AddNode` re-registration; the permanent-until-restart asymmetry language removed;
      `just lint-spec` green
- [ ] Forcing test: `CONFIG SET cluster-replica-priority` on a running node, then assert a
      *peer's* selection (scored from replicated state) reflects the new priority after the
      re-registration commits — fails against today's code, passes after
- [ ] Existing FM-CLUSTER-058 tests still pass: live self-view (`priority 0` removes self
      immediately) and deterministic tiebreak unchanged
- [ ] `just mutants-diff` triaged on `frogdb-cluster` and `frogdb-cluster-runtime`

## Blocked by

None.
