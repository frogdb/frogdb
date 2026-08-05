# `RemoveSlots` never checks that the node it names owns the slots

Status: needs-triage
Type: bug (replicated state machine, authorization)
Severity: likelihood 1/3 (needs a wrong or stale `node_id` on an admin command), consequence 3/3
(a node's slots are unassigned out from under it — every key in them becomes `CLUSTERDOWN` until
someone re-assigns) — score 4
Area: cluster / `ClusterCommand::RemoveSlots`

## Problem

`frogdb-server/crates/cluster/src/commands.rs:142-159`:

```rust
ClusterCommand::RemoveSlots { node_id, slots } => {
    for range in slots {
        for slot in range.start..=range.end {
            if !state.slot_assignment.contains_key(&slot) {
                return Err(ClusterError::SlotNotAssigned(slot));
            }
        }
    }
    for range in slots { for slot in .. { state.slot_assignment.remove(&slot); } }
    debug!(node_id, "removed slots");
    Ok(ClusterResponse::Ok)
}
```

`node_id` is used only in the log line. The validation pass asserts that each slot has *some*
owner, never that the owner is the node in the command. So `CLUSTER DELSLOTS` proposed against
node A can strip slots owned by node B, and it succeeds — the caller gets `+OK` and node B keeps
serving a slot the replicated map says nobody owns, until its snapshot catches up and it starts
answering `CLUSTERDOWN`.

Every sibling arm is stricter. `AssignSlots` refuses a slot owned by another node
(`SlotAlreadyAssigned`); `BeginSlotMigration` refuses a source that is not the owner
(`InvalidOperation("slot N is owned by X, not Y")`); `CompleteSlotMigration` requires the
parameters to match the recorded migration. `RemoveSlots` is the only slot-mutating arm with no
ownership check at all.

## Redis comparison

Redis' `CLUSTER DELSLOTS` is a *local* unassignment with no node argument — the node can only
delete its own slots, so an ownership check is structurally unnecessary. FrogDB's is replicated
and carries an explicit `node_id`, which makes the missing check a real widening rather than
parity. Specced as FM-CLUSTER-004 so the widening is at least written down.

## Fix

Add the ownership check to the validation pass, alongside the existing `SlotNotAssigned`:

```rust
match state.slot_assignment.get(&slot) {
    None => return Err(ClusterError::SlotNotAssigned(slot)),
    Some(owner) if *owner != *node_id => return Err(ClusterError::InvalidOperation(
        format!("slot {slot} is owned by {owner}, not {node_id}"))),
    Some(_) => {}
}
```

The all-or-nothing shape (FM-CLUSTER-003) is already right and must stay: the check belongs in the
validation pass, not in the mutation loop.

## Tests that should exist

- `remove_slots_rejects_a_slot_owned_by_another_node`
- `remove_slots_rejects_the_whole_batch_when_one_slot_is_foreign`

`remove_slots_ignores_the_node_it_names` (added by FM-CLUSTER-004) pins today's behavior and must
be inverted by the fix.

## Spec impact

FM-CLUSTER-004 flips from "validates that the slots are assigned, not who owns them" to an
ownership assertion; its `NOT observable` gains the foreign-slot case.
