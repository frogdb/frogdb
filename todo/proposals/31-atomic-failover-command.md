# Proposal: Atomic Failover ClusterCommand

Status: implemented
Date: 2026-07-04

## Problem

A failover is one topology decision — "this primary's slots, role, and epoch now belong to that
successor" — but FrogDB executed it as a **multi-write saga**: three to N+2 separate Raft entries
(`RemoveNode`/`SetRole`, one `AssignSlots` per slot range, then `IncrementEpoch`), hand-rolled
**twice**, once per trigger:

| Trigger | Where | Shape |
|---|---|---|
| Manual (`CLUSTER FAILOVER`) | `crates/server/src/connection/handlers/cluster.rs` `handle_failover_command` (deleted) | force: `RemoveNode`; graceful: `SetRole(Primary)` → per-range `AssignSlots` loop → `IncrementEpoch` |
| Automatic (failure detector) | `crates/server/src/failure_detector.rs` `trigger_auto_failover` | `RemoveNode` → per-range `AssignSlots` loop → `IncrementEpoch` |

The same defect in miniature: `mark_node_failed` (`failure_detector.rs`) wrote `MarkNodeFailed`,
then a *separate* `IncrementEpoch` entry.

The state machine (`ClusterState::apply_command`, `crates/cluster/src/commands.rs`) already owned
every primitive — `RemoveNode`, `SetRole`, `AssignSlots`, `IncrementEpoch` — but not the
**composite**. The composition rule ("a failover is removal/demotion + slot transfer + epoch bump,
together") therefore leaked upward into two async handlers, restated with drift, where Raft cannot
protect it.

### Consequences of the saga

1. **Ownerless slots on leader crash.** Each `client_write` is an independent log entry. A leader
   crash between `RemoveNode` (which drops the failed primary's slot assignments) and the
   `AssignSlots` entries leaves those slots owned by nobody. `route_with_snapshot`
   (`slot_migration/routing.rs`, `None` arm) then returns `Unassigned` → every client sees
   `CLUSTERDOWN Hash slot N not served` for data that lives, intact, on the promoted replica.
   Nothing retries the missing entries; the hole is permanent until an operator reassigns slots.
2. **Partial slot transfer was silent.** Per-range `AssignSlots` failures were only `warn!`-ed
   ("Failed to transfer slots during failover") and the loop continued; the manual path still
   returned `+OK` to the client.
3. **New topology at an old epoch.** With `IncrementEpoch` last (and its failure merely logged),
   other nodes could observe the new slot ownership at the *old* config epoch — inverting the
   epoch's whole purpose as a conflict-resolution version.
4. **Two hand-rolled copies drift.** The manual and auto paths had already diverged (see
   [correctness flags](#correctness-flags-found-during-implementation): the force path never
   promoted the successor's role; the graceful path never actually transferred slots).

**Deletion test.** There was no artifact whose deletion removed "how a failover mutates the
topology" — the rule lived as duplicated control flow in two async functions plus implicit
ordering assumptions between Raft entries. After this change, deleting the
`ClusterCommand::Failover` apply arm removes failover semantics entirely, in one place.

### What Redis and Valkey do (sanity check)

In Redis Cluster and Valkey, a promoted replica wins the failover election, bumps its
`configEpoch` to a value greater than any it knows, and then — in its node-local tables and in the
very next gossip messages — **claims the failed master's slots at that new epoch**. The slot
bitmap, the role change, and the `configEpoch` travel together in a single cluster bus message
(`clusterMsg` header carries `configEpoch` + slot bitmap + flags); receivers apply the
slot-ownership update *because* the epoch is higher (`clusterUpdateSlotsConfigWith`). There is no
protocol state where the slot claim and the epoch bump are separately visible. FrogDB's
Raft-replicated equivalent is: one log entry whose apply performs slot transfer + role change +
epoch bump as one state-machine transition. DragonflyDB (emulated cluster mode) sidesteps the
question by taking whole cluster config snapshots atomically via `DFLYCLUSTER CONFIG`. The
composite command is the direct analogue of both.

## Implemented design

### 1. `ClusterCommand::Failover` — the composite transition (`crates/cluster/src/types.rs`)

```rust
Failover {
    /// The primary losing its slots (failed, or being demoted).
    old_primary_id: NodeId,
    /// The node taking over (a replica being promoted, or an absorbing primary).
    new_primary_id: NodeId,
    /// true: old primary is presumed dead and removed from the cluster.
    /// false (graceful): old primary is demoted to a replica of the successor.
    force: bool,
}
```

Applied in `ClusterState::apply_command` (`crates/cluster/src/commands.rs`) as
**validate-then-mutate**: every check (`old != new`, successor exists, graceful requires the old
node to exist) runs before the first mutation, so the transition is all-or-nothing even within the
apply. The mutation phase, under one write lock, in one log entry:

1. every slot owned by `old_primary_id` → `new_primary_id`;
2. successor promoted (`role = Primary`, `primary_id = None`; no-op if already primary — covers
   the primary-absorbs-primary `FORCE`/`TAKEOVER` path and replayed retries);
3. old primary's fate: `force` removes it (and cancels migrations that reference the removed node,
   which could otherwise never complete and would block future migrations of those slots);
   graceful demotes it to a replica of the successor;
4. the old primary's remaining replicas are re-parented to the successor (Redis parity: orphaned
   replicas follow the new master);
5. `config_epoch += 1`, and the successor's `NodeInfo.config_epoch` claims the new value (Redis
   parity: the promoted node owns the new `configEpoch`). Returns
   `ClusterResponse::Value(epoch)`.

Follower nodes apply the identical entry, so every node sees the transition atomically — including
the demoted old primary, for which the state machine emits a `DemotionEvent` (graceful only,
post-apply only; a force removal is not a demotion, and a *failed* apply emits nothing — unlike
the pre-existing `SetRole` demotion check, which fires pre-apply).

### 2. `MarkNodeFailed` folds the epoch bump

`MarkNodeFailed`'s apply now bumps `config_epoch` in the same transition. The FAIL flag is a
topology-visibility change; other nodes must never observe it at a stale epoch. The separate
`client_write(IncrementEpoch)` follow-up in `failure_detector.rs` is deleted. (`MarkNodeRecovered`
deliberately does not bump — recovery clears a flag on an unchanged topology, and the pre-existing
behavior didn't bump either.)

### 3. Both call sites collapse to one `client_write`

- **Manual** (`connection/handlers/cluster.rs`): `handle_failover_command` (84 lines) is deleted.
  `RaftClusterOp::Failover` now maps to the composite inside `handle_raft_command` and flows
  through the *generic* write path — gaining, for free, the state-machine error check
  (`ClusterResponse::Error` → client error instead of unconditional `+OK`) and the
  ForwardToLeader cluster-bus forwarding (previously a `CLUSTER FAILOVER` issued on a non-Raft-
  leader replica just failed).
- **Auto** (`failure_detector.rs` `trigger_auto_failover`): the saga becomes one
  `client_write(Failover { force: true })`. **Error policy (deliberate):** the write is retried up
  to 3 times with 500 ms backoff — `is_marked_fail` latches, so a dropped write would otherwise
  never be re-attempted until the node recovered and failed again. A *state-machine rejection*
  (e.g. the successor vanished) is not retried (same command cannot succeed) and is logged at
  `error`. The manual path propagates errors to the client.

### 4. `ForwardedWrite` surfaces state-machine errors (`crates/cluster/src/network.rs`)

The cluster-bus `ForwardedWrite` handler previously returned `Ok(())` even when the Raft write
committed but the state machine rejected the command — a forwarded failover that failed validation
was reported as success. It now maps `ClusterResponse::Error` to `ForwardedWrite(Err(msg))` for
all forwarded commands.

### 5. Regression gate: the saga is unrepresentable-by-lint

`just lint-failover-atomicity` (in the default `lint` dependency chain, pattern follows
`lint-redirect-seam`): fails if any `crates/server/src` file issues a standalone
`client_write(ClusterCommand::IncrementEpoch)` (the saga's signature follow-up write), or if
`failure_detector.rs` / `connection/handlers/cluster.rs` mention
`ClusterCommand::{RemoveNode,AssignSlots,SetRole}` at all. `CLUSTER BUMPEPOCH` is unaffected (it
flows through `convert_raft_cluster_op`).

### Why this is the right depth

- **Locality.** The failover rule lives beside the primitives it composes, in the one module
  (`commands.rs`) whose job is topology transitions. Changing what a failover means is a
  one-arm edit, not a two-handler sweep.
- **Leverage.** One apply arm deletes two duplicated sagas, closes the ownerless-slot crash
  window, and fixes three latent bugs (below) that the duplication had already produced.
- **Interface honesty.** Raft's contract is "one entry applies atomically". The saga pretended a
  cross-entry invariant that Raft never promised; the composite states the invariant where Raft
  can actually enforce it.
- **Not a new layer.** No orchestrator, no adapter: the composite is a peer of the existing
  variants, and callers use the same `client_write` seam they already used.

## Testing impact

All in `crates/cluster/src/state.rs` (the apply is pure — no cluster needed):

- force: old node removed, successor promoted, **all** slots transferred, sibling replicas
  re-parented, epoch bumped exactly once and claimed by the successor
  (`test_failover_force_removes_old_and_transfers_everything`);
- graceful: old primary demoted to replica-of-successor, not removed
  (`test_failover_graceful_demotes_old_primary`);
- **atomicity**: a validation failure mutates *nothing* — node, role, slots, epoch all intact
  (`test_failover_validation_failure_mutates_nothing`);
- replay/retry safety: re-applying the same force failover succeeds and leaves a coherent state
  (`test_failover_force_replay_is_safe`; note each application bumps the epoch — harmless, epochs
  only need monotonicity);
- graceful requires the old node; same-node failover rejected; primary-absorbs-primary;
  force cancels migrations referencing the removed node, graceful keeps unrelated ones;
- `MarkNodeFailed` bumps the epoch atomically, and not on a rejected command;
- `DemotionEvent` fires for a graceful failover of self, not for force, not on failed apply;
- serde: `ClusterCommand::Failover` round-trips, and a post-failover `ClusterStateInner`
  round-trips through the JSON path used by Raft snapshot install
  (`test_state_snapshot_roundtrip_after_failover`).

End-to-end: the existing multi-node harness (`crates/server/tests/integration_cluster.rs` —
`test_cluster_failover_command`, `test_concurrent_failover_attempts`,
`test_failover_during_migration_preserves_data`, `test_data_survives_leader_failover`) exercises
`CLUSTER FAILOVER` and auto-failover over real Raft.

## Correctness flags (found during implementation)

Pre-existing bugs the saga's duplication had already caused, all fixed by the composite:

- **Graceful manual failover never transferred slots.** `AssignSlots` *rejects* slots owned by
  another node (`SlotAlreadyAssigned`), and during a graceful failover the slots were still owned
  by the old primary. The state-machine error came back inside `Ok(resp.data)`, which the handler
  never inspected — so every graceful `CLUSTER FAILOVER` returned `+OK`, promoted the replica, and
  left all slots (and clients) pointed at the old primary.
- **Force failover never promoted the successor.** The force path issued no `SetRole`; the
  promoted replica kept `role: Replica` (and a dangling `primary_id` to the removed node) while
  owning slots. Routing worked by accident (slot ownership only); `CLUSTER NODES`/`INFO` reported
  a slave serving as a master.
- **Graceful failover never demoted the old primary** — two primaries, one slotless, with
  replicas still parented to the demoted one.
- **Forwarded writes swallowed state-machine errors** (`network.rs` `ForwardedWrite`), reporting
  success for commands the state machine rejected.

## Risks / open questions / follow-ups

- **Raft voter membership divergence (stretch — partially implemented).** Voter changes ride as
  fire-and-forget side effects of the `AddNode` DATA command: `spawn_add_raft_voter`
  (`crates/cluster/src/network.rs`) is a detached task whose failure previously left the node in
  `ClusterState.nodes` (and routing) but absent from the Raft voter set — silently weakening fault
  tolerance. **Implemented:** the add-learner/promote sequence now retries 5 times with linear
  backoff, re-checking voter membership each attempt (add_learner on an existing voter would
  demote it), and logs at `error` with an explicit "in cluster state but NOT a Raft voter"
  message on final failure. **Deferred:** a reconciliation loop on the leader (compare
  `ClusterState.nodes` against `membership_config` voters; converge) — that is the real fix, since
  a leader crash after `AddNode` commits but before `change_membership` still loses the intent.
- **Graceful failover does not coordinate replication hand-off.** Redis pauses the master, waits
  for the replica to reach the master's offset, then swaps. FrogDB's graceful path swaps
  atomically in the cluster state but does not synchronize data replication first; the demoted
  primary learns of its demotion when it applies the entry. Data-plane hand-off is a separate
  proposal-sized concern (interacts with proposal 18's offset tracking).
- **ForwardToLeader + rejected command → REDIRECT.** With `ForwardedWrite` now surfacing
  state-machine errors, a forwarded command that the leader's state machine rejects makes the
  origin node fall through to a `REDIRECT` reply; the client retries on the leader and gets the
  real error. Correct, but one extra round trip; distinguishing "transport failed" from "command
  rejected" in the forward reply would remove it.
- **Epoch bump on retry-replay.** Re-issuing an already-applied force failover (client retry after
  a lost response) succeeds and bumps the epoch again. Harmless — epochs need monotonicity, not
  density — and Redis's `configEpoch` collision handling similarly burns epochs; noted for
  completeness.
- **`RaftClusterOp::Failover` field names** (`replica_id`/`primary_id`) predate the composite and
  read backwards next to `new_primary_id`/`old_primary_id`; renaming touches
  `crates/protocol/src/response.rs` + `commands/cluster/admin.rs` (other agents' scope at the time
  of writing) and is cosmetic — left as a trivial follow-up.
