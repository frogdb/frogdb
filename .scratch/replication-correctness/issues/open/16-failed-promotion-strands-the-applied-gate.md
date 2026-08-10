# 16 — A failed promotion strands the applied gate

Status: needs-triage

## Parent

Found by [issue 08](08-stateright-promotion-model.md)'s stateright promotion model
(`frogdb-server/crates/replication/src/model/promotion/`), filed under the campaign's rule that a
model counterexample is pinned and filed rather than fixed inline.
[PRD](../../PRD.md) §3 W3.

## What the model found

`sometimes("a_failed_promotion_strands_the_node")` — in the `strand` config (2 nodes, persist
failures enabled) the model reaches a state where a node is `Role::Replica` with its applied gate
still frozen. The deterministic replay of that state against real objects is
`frogdb-server/crates/replication/src/model/promotion/replay.rs`
(`a_failed_promotion_leaves_the_node_unable_to_replicate`), which runs a real
`PrimaryReplicationHandler`, `AppliedOffset` and `ReplicaOffset` with an unwritable state file:

1. `RoleManager::promote` (`frogdb-server/crates/server/src/role_manager.rs:322`) drops the
   inbound stream and clears `primary_target` — irreversibly, the socket is gone.
2. `PrimaryReplicationHandler::begin_primary_stint`
   (`frogdb-server/crates/replication/src/primary/mod.rs:397`) calls
   `settle_at_applied()` — which freezes the applied gate (`AppliedOffset::freeze`) — **before**
   the persist that can fail.
3. The persist fails. The rollback is `StintPlan`'s and only `StintPlan`'s: the `ReplicationState`
   is restored bit for bit, and nothing else is. The gate stays frozen.
4. Consequences on the surviving handles: `ReplicaApplyStint::claim` returns `Claim::Retired`, so
   the frame consumer applies nothing more; `ReplicaOffset::reset_to` returns `false`, so a full
   resync cannot install either. Only `begin_replica_stint` clears the freeze, and nothing inside
   the node calls it.

## Why this is filed rather than fixed

The end state is *documented* at `RoleManager::promote`'s `# Failure` section
(`role_manager.rs:312-321`): "the node stays read-only under its inherited identity … follows
nobody and applies nothing until an operator retries `REPLICAOF NO ONE` or re-points it". So the
question is not "is this a surprise to the author" but **is the documented choice the one we
want**, and it needs a ruling, not a unilateral patch:

- The safety argument for the freeze is sound and should not be undone: a node whose promotion was
  not persisted must not keep applying the old stream at offsets it may later re-mint over.
- The liveness gap is real: in **standalone** mode the only exit is a human. There is no
  self-healing retry, and the stranded state has no distinguishing signal — `INFO replication` shows
  a replica with a `primary_target` of `None`, indistinguishable from a node that was never
  pointed anywhere. In **cluster** mode the role reconciler re-issues the role, so the exposure is
  a window rather than a terminal state.
- A transient persist failure (full disk, a bad remount) is exactly the case where a node silently
  going inert until someone notices is the worst outcome.

## Options for the ruling

1. **Keep the behavior, make it observable.** Leave the freeze; add a distinguishable state (a
   metric / `INFO replication` field for "promotion aborted, applier frozen") so an operator and
   an alert can see it. Cheapest, no correctness risk.
2. **Undo the gate freeze on the rollback path.** Have `begin_primary_stint`'s failure branch
   re-open the applied gate along with the state, so the node is at least resyncable. Needs care:
   the stream is already gone, so the node still follows nobody — this only makes a *later*
   `REPLICAOF` cheaper (partial rather than full resync), and it re-opens a window the freeze was
   introduced to close. Wants its own failure-mode row before it is attempted.
3. **Self-heal in standalone.** Have the node re-arm a replica stint against its pre-promotion
   target on a failed promotion. Largest change; reintroduces the "who owns the role" ambiguity
   that `RoleManager` exists to remove.

## Acceptance criteria

- [ ] A ruling recorded here on options 1-3 (or a documented "keep as is" with the reasoning)
- [ ] If the behavior changes: the characterization assertions in
      `model/promotion/replay.rs` are flipped to the new expectation in the same commit (the test
      is written to fail loudly on a fix, not to be deleted)
- [ ] If the behavior changes: the model's pinned `sometimes("a_failed_promotion_strands_the_node")`
      witness is replaced by the corresponding `always`/`eventually` clause
- [ ] `FM-REPLICATION-020` in `.scratch/hardening/specs/replication-failure-modes.md` reflects the
      ruled behavior

## Evidence

- Model witness: `model::promotion::tests::a_failed_promotion_strands_the_node` (the `strand`
  config, `persist_failures: true`)
- Deterministic replay: `model::promotion::replay::a_failed_promotion_leaves_the_node_unable_to_replicate`
