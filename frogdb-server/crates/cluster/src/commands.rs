//! Command handlers for cluster state mutations.

use crate::types::{
    ClusterCommand, ClusterError, ClusterEvent, ClusterResponse, NodeRole, SlotMigration,
};

use super::state::{ClusterState, EpochReconciliation};

impl ClusterState {
    /// Apply a command to the state, returning the response and any
    /// [`ClusterEvent`]s the mutation produced.
    ///
    /// Events are pushed only on the `Ok` path of the arm that performs the
    /// corresponding mutation, so a rejected command (an `Err` return) carries
    /// no events at all — emit-on-failure is structurally impossible. The
    /// events are node-agnostic; the node-local self-filter and channel routing
    /// live in [`crate::state::ClusterStateMachine`]'s `apply`.
    pub(crate) fn apply_command(
        &self,
        cmd: ClusterCommand,
    ) -> Result<(ClusterResponse, Vec<ClusterEvent>), ClusterError> {
        let mut inner = self.inner.write();

        match cmd {
            ClusterCommand::AddNode { mut node } => {
                let existed = inner.nodes.contains_key(&node.id);
                if existed {
                    tracing::info!(node_id = node.id, addr = %node.addr, "Updated node in cluster");
                } else {
                    tracing::info!(node_id = node.id, addr = %node.addr, "Adding node to cluster");
                }

                // Re-registration refreshes a node's *address*, never its role.
                // A node re-registers itself on every boot (`cluster_init`) with
                // the role it can know without the cluster's help — primary — so
                // taking the incoming role would demote-by-restart: a replica
                // that restarted would silently reappear as a slotless primary,
                // its primary would lose a replica, and its data path would keep
                // streaming from a node the topology no longer links it to.
                // Role transitions belong to `SetRole` and `Failover`; this arm
                // keeps the recorded role for the same reason it keeps the
                // recorded epoch below.
                if let Some(existing) = inner.nodes.get(&node.id) {
                    if existing.role != node.role || existing.primary_id != node.primary_id {
                        tracing::debug!(
                            node_id = node.id,
                            recorded_role = %existing.role,
                            claimed_role = %node.role,
                            "Node re-registered with a different role; kept its recorded role"
                        );
                    }
                    node.role = existing.role;
                    node.primary_id = existing.primary_id;
                }

                // Warn on version mismatch when a node joins or updates
                if !node.version.is_empty() {
                    // Check against the majority version in the cluster
                    let majority_version = inner
                        .nodes
                        .values()
                        .filter(|n| !n.version.is_empty() && n.id != node.id)
                        .map(|n| n.version.as_str())
                        .max();
                    if let Some(majority) = majority_version
                        && node.version != majority
                    {
                        tracing::warn!(
                            node_id = node.id,
                            node_version = %node.version,
                            cluster_version = %majority,
                            "Node joining with different binary version (mixed-version cluster)"
                        );
                    }
                }

                // Resolve the epoch the incoming node claims before it lands in
                // the table: `AddNode` is the one path that carries an epoch the
                // cluster-wide counter did not mint, so it is the one path that
                // can introduce a collision.
                match inner.reconcile_incoming_epoch(&mut node) {
                    EpochReconciliation::Accepted => {}
                    EpochReconciliation::Preserved(epoch) => {
                        tracing::debug!(
                            node_id = node.id,
                            epoch,
                            "Node re-registered without an epoch; kept its recorded config_epoch"
                        );
                    }
                    EpochReconciliation::Reassigned { claimed, assigned } => {
                        tracing::warn!(
                            node_id = node.id,
                            claimed_epoch = claimed,
                            assigned_epoch = assigned,
                            "config_epoch collision on join: claimed epoch is already held by \
                             another primary, assigned a fresh epoch"
                        );
                    }
                }

                inner.nodes.insert(node.id, node);
                Ok((ClusterResponse::Ok, Vec::new()))
            }

            ClusterCommand::RemoveNode { node_id } => {
                if !inner.nodes.contains_key(&node_id) {
                    return Err(ClusterError::NodeNotFound(node_id));
                }
                // Remove slot assignments for this node
                inner
                    .slot_assignment
                    .retain(|_, &mut owner| owner != node_id);
                inner.nodes.remove(&node_id);
                tracing::info!(node_id, "Removed node from cluster");
                Ok((ClusterResponse::Ok, Vec::new()))
            }

            ClusterCommand::AssignSlots { node_id, slots } => {
                if !inner.nodes.contains_key(&node_id) {
                    return Err(ClusterError::NodeNotFound(node_id));
                }

                for range in &slots {
                    for slot in range.iter() {
                        if let Some(&existing_owner) = inner.slot_assignment.get(&slot)
                            && existing_owner != node_id
                        {
                            return Err(ClusterError::SlotAlreadyAssigned(slot, existing_owner));
                        }
                    }
                }

                for range in slots {
                    for slot in range.iter() {
                        inner.slot_assignment.insert(slot, node_id);
                    }
                }
                tracing::debug!(node_id, "Assigned slots to node");
                Ok((ClusterResponse::Ok, Vec::new()))
            }

            ClusterCommand::RemoveSlots { node_id, slots } => {
                // Validate all slots are currently assigned before removing any
                for range in &slots {
                    for slot in range.iter() {
                        if !inner.slot_assignment.contains_key(&slot) {
                            return Err(ClusterError::SlotNotAssigned(slot));
                        }
                    }
                }
                // Remove validated slots
                for range in slots {
                    for slot in range.iter() {
                        inner.slot_assignment.remove(&slot);
                    }
                }
                tracing::debug!(node_id, "Removed slots from node");
                Ok((ClusterResponse::Ok, Vec::new()))
            }

            ClusterCommand::SetRole {
                node_id,
                role,
                primary_id,
            } => {
                // Validate node exists first
                if !inner.nodes.contains_key(&node_id) {
                    return Err(ClusterError::NodeNotFound(node_id));
                }

                // Validate primary_id if setting replica role
                if role == NodeRole::Replica {
                    if let Some(pid) = primary_id {
                        if !inner.nodes.contains_key(&pid) {
                            return Err(ClusterError::NodeNotFound(pid));
                        }
                    } else {
                        return Err(ClusterError::InvalidOperation(
                            "replica requires a primary_id".to_string(),
                        ));
                    }
                }

                // Now we can safely modify
                let node = inner.nodes.get_mut(&node_id).unwrap();
                let was_primary = node.role == NodeRole::Primary;
                node.role = role;
                node.primary_id = primary_id;
                tracing::info!(node_id, ?role, "Set node role");

                // Both events are node-agnostic; the state machine decides
                // whether they apply to *this* node.
                //
                // Replica: emit unconditionally. Re-issuing `SetRole { Replica }`
                // on a node that is already a replica is how a re-parent is
                // expressed, and the data path has to re-point its replication
                // stream at the new primary — so this is not a no-op.
                //
                // Primary: emit only on a real replica→primary transition. A
                // re-applied `SetRole { Primary }` on a node that is already a
                // primary changes nothing, and promoting the data path again
                // would mint a fresh replication ID and force every attached
                // replica into a full resync for no reason.
                let mut events = Vec::new();
                match role {
                    NodeRole::Replica => events.push(ClusterEvent::NodeDemoted {
                        demoted_node_id: node_id,
                        new_primary_id: primary_id,
                        epoch: inner.config_epoch,
                    }),
                    NodeRole::Primary if !was_primary => events.push(ClusterEvent::NodePromoted {
                        promoted_node_id: node_id,
                        epoch: inner.config_epoch,
                    }),
                    NodeRole::Primary => {}
                }
                Ok((ClusterResponse::Ok, events))
            }

            ClusterCommand::IncrementEpoch => {
                inner.config_epoch += 1;
                tracing::debug!(epoch = inner.config_epoch, "Incremented config epoch");
                Ok((ClusterResponse::Epoch(inner.config_epoch), Vec::new()))
            }

            ClusterCommand::Failover {
                old_primary_id,
                new_primary_id,
                force,
            } => {
                // ---- Validation phase: no mutation until every check passes,
                // so the transition is all-or-nothing.
                if old_primary_id == new_primary_id {
                    return Err(ClusterError::InvalidOperation(
                        "failover source and target are the same node".to_string(),
                    ));
                }
                if !inner.nodes.contains_key(&new_primary_id) {
                    return Err(ClusterError::NodeNotFound(new_primary_id));
                }
                let old_exists = inner.nodes.contains_key(&old_primary_id);
                if !old_exists && !force {
                    // Graceful failover demotes the old primary, so it must exist.
                    return Err(ClusterError::NodeNotFound(old_primary_id));
                }

                // ---- Mutation phase (infallible from here).

                // 1. Transfer every slot owned by the old primary to the successor.
                let mut transferred = 0usize;
                for owner in inner.slot_assignment.values_mut() {
                    if *owner == old_primary_id {
                        *owner = new_primary_id;
                        transferred += 1;
                    }
                }

                // 2. Promote the successor (no-op if it is already a primary,
                //    e.g. the absorb path or a replayed retry).
                let successor_was_replica = {
                    let new_node = inner.nodes.get_mut(&new_primary_id).unwrap();
                    let was_replica = new_node.role == NodeRole::Replica;
                    new_node.role = NodeRole::Primary;
                    new_node.primary_id = None;
                    was_replica
                };

                // 3. Apply the old primary's fate. A graceful failover demotes
                //    the old primary (a NodeDemoted event); a force failover
                //    *removes* it, which is not a demotion.
                let graceful_demotion = !force;
                if force {
                    if old_exists {
                        inner.nodes.remove(&old_primary_id);
                    }
                    // Migrations referencing a removed node can never complete
                    // and would block future migrations of those slots.
                    inner.migrations.retain(|_, m| {
                        m.source_node != old_primary_id && m.target_node != old_primary_id
                    });
                } else {
                    let old_node = inner.nodes.get_mut(&old_primary_id).unwrap();
                    old_node.role = NodeRole::Replica;
                    old_node.primary_id = Some(new_primary_id);
                }

                // 4. Re-parent the old primary's remaining replicas so they
                //    follow the successor instead of a demoted/removed node.
                for node in inner.nodes.values_mut() {
                    if node.primary_id == Some(old_primary_id) && node.id != new_primary_id {
                        node.primary_id = Some(new_primary_id);
                    }
                }

                // 5. Bump the config epoch in the same transition and let the
                //    successor claim it, so the new slot ownership can never be
                //    observed at a stale epoch (Redis parity: the promoted
                //    replica claims a new configEpoch and the slot bitmap and
                //    epoch propagate together in one cluster message).
                inner.config_epoch += 1;
                let epoch = inner.config_epoch;
                if let Some(new_node) = inner.nodes.get_mut(&new_primary_id) {
                    new_node.config_epoch = epoch;
                }

                tracing::info!(
                    old_primary = old_primary_id,
                    new_primary = new_primary_id,
                    force,
                    slots_transferred = transferred,
                    epoch,
                    "Applied atomic failover"
                );

                // Order matters on a node that is both: the demotion is the old
                // primary's, the promotion the successor's, and they can never
                // name the same node (the two ids are validated distinct above).
                let mut events = Vec::new();
                if graceful_demotion {
                    events.push(ClusterEvent::NodeDemoted {
                        demoted_node_id: old_primary_id,
                        new_primary_id: Some(new_primary_id),
                        epoch,
                    });
                }
                if successor_was_replica {
                    events.push(ClusterEvent::NodePromoted {
                        promoted_node_id: new_primary_id,
                        epoch,
                    });
                }
                Ok((ClusterResponse::Epoch(epoch), events))
            }

            ClusterCommand::MarkNodeFailed { node_id } => {
                let node = inner
                    .nodes
                    .get_mut(&node_id)
                    .ok_or(ClusterError::NodeNotFound(node_id))?;
                node.flags.fail = true;
                // Topology-visibility change: bump the epoch in the same
                // transition so other nodes never observe the FAIL flag at a
                // stale epoch (previously a separate IncrementEpoch entry that
                // could be lost on leader crash).
                inner.config_epoch += 1;
                tracing::warn!(node_id, epoch = inner.config_epoch, "Marked node as failed");
                Ok((ClusterResponse::Ok, Vec::new()))
            }

            ClusterCommand::MarkNodeRecovered { node_id } => {
                let node = inner
                    .nodes
                    .get_mut(&node_id)
                    .ok_or(ClusterError::NodeNotFound(node_id))?;
                node.flags.fail = false;
                node.flags.pfail = false;
                tracing::info!(node_id, "Marked node as recovered");
                Ok((ClusterResponse::Ok, Vec::new()))
            }

            ClusterCommand::BeginSlotMigration {
                slot,
                source_node,
                target_node,
            } => {
                // Idempotent: if the exact same migration is already in progress, succeed.
                if let Some(existing) = inner.migrations.get(&slot) {
                    if existing.source_node == source_node && existing.target_node == target_node {
                        return Ok((ClusterResponse::Ok, Vec::new()));
                    }
                    return Err(ClusterError::MigrationInProgress(slot));
                }

                if !inner.nodes.contains_key(&source_node) {
                    return Err(ClusterError::NodeNotFound(source_node));
                }
                if !inner.nodes.contains_key(&target_node) {
                    return Err(ClusterError::NodeNotFound(target_node));
                }

                // Slot assignment may be empty on follower nodes (assigned locally
                // on bootstrap, not via Raft), so only validate if present.
                if let Some(&owner) = inner.slot_assignment.get(&slot)
                    && owner != source_node
                {
                    return Err(ClusterError::InvalidOperation(format!(
                        "slot {} is owned by {}, not {}",
                        slot, owner, source_node
                    )));
                }

                inner.migrations.insert(
                    slot,
                    SlotMigration {
                        slot,
                        source_node,
                        target_node,
                    },
                );
                tracing::info!(slot, source_node, target_node, "Started slot migration");
                Ok((ClusterResponse::Ok, Vec::new()))
            }

            ClusterCommand::CompleteSlotMigration {
                slot,
                source_node,
                target_node,
            } => {
                let migration =
                    inner
                        .migrations
                        .get(&slot)
                        .ok_or(ClusterError::InvalidOperation(format!(
                            "no migration in progress for slot {}",
                            slot
                        )))?;

                if migration.source_node != source_node || migration.target_node != target_node {
                    return Err(ClusterError::InvalidOperation(
                        "migration parameters don't match".to_string(),
                    ));
                }

                // Transfer slot ownership
                inner.slot_assignment.insert(slot, target_node);
                inner.migrations.remove(&slot);
                tracing::info!(slot, source_node, target_node, "Completed slot migration");
                Ok((
                    ClusterResponse::Ok,
                    vec![ClusterEvent::SlotMigrationCompleted {
                        slot,
                        source_node,
                        target_node,
                    }],
                ))
            }

            ClusterCommand::CancelSlotMigration { slot } => {
                inner.migrations.remove(&slot);
                tracing::info!(slot, "Cancelled slot migration");
                Ok((ClusterResponse::Ok, Vec::new()))
            }

            ClusterCommand::FinalizeUpgrade { version } => {
                // Validate all nodes report a version >= target
                for (node_id, node) in &inner.nodes {
                    if node.version.is_empty() {
                        return Err(ClusterError::InvalidOperation(format!(
                            "node {} has no version info (pre-versioning binary)",
                            node_id
                        )));
                    }
                    let node_ver = semver::Version::parse(&node.version).map_err(|e| {
                        ClusterError::InvalidOperation(format!(
                            "node {} has invalid version '{}': {}",
                            node_id, node.version, e
                        ))
                    })?;
                    let target_ver = semver::Version::parse(&version).map_err(|e| {
                        ClusterError::InvalidOperation(format!(
                            "invalid target version '{}': {}",
                            version, e
                        ))
                    })?;
                    if node_ver < target_ver {
                        return Err(ClusterError::InvalidOperation(format!(
                            "node {} is at version {} but finalization requires {}",
                            node_id, node.version, version
                        )));
                    }
                }

                tracing::info!(version = %version, "Finalizing upgrade — active version advanced");
                inner.active_version = Some(version);
                Ok((ClusterResponse::Ok, Vec::new()))
            }

            ClusterCommand::ResetCluster {
                node_id,
                new_node_id,
            } => {
                // Clear all slot assignments
                inner.slot_assignment.clear();

                // Clear all migrations
                inner.migrations.clear();

                // Remove all nodes except this one, and ensure it's a primary
                if let Some(mut this_node) = inner.nodes.remove(&node_id) {
                    this_node.role = NodeRole::Primary;
                    this_node.primary_id = None;

                    inner.nodes.clear();

                    if let Some(new_id) = new_node_id {
                        // HARD: reset epoch and assign new node ID
                        inner.config_epoch = 0;
                        this_node.id = new_id;
                        this_node.config_epoch = 0;
                        inner.nodes.insert(new_id, this_node);
                        tracing::info!(
                            old_node_id = node_id,
                            new_node_id = new_id,
                            "HARD cluster reset"
                        );
                    } else {
                        // SOFT: keep same node ID and epoch
                        inner.nodes.insert(node_id, this_node);
                        tracing::info!(node_id, "SOFT cluster reset");
                    }
                } else {
                    // Node not found - clear everything anyway
                    inner.nodes.clear();
                    tracing::warn!(node_id, "Cluster reset: node not found in state");
                }

                Ok((ClusterResponse::Ok, Vec::new()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{NodeInfo, SlotRange};
    use std::net::SocketAddr;

    fn test_addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    /// Seed `count` primaries with ids `1..=count`.
    fn state_with_primaries(count: u64) -> ClusterState {
        let state = ClusterState::new();
        for id in 1..=count {
            let port = 6379 + id as u16;
            state
                .apply_local(ClusterCommand::AddNode {
                    node: NodeInfo::new_primary(id, test_addr(port), test_addr(port + 10_000)),
                })
                .expect("seeding a primary must succeed");
        }
        state
    }

    fn assign(state: &ClusterState, node_id: u64, start: u16, end: u16) {
        state
            .apply_local(ClusterCommand::AssignSlots {
                node_id,
                slots: vec![SlotRange::new(start, end)],
            })
            .expect("seeding slots must succeed");
    }

    // FM-CLUSTER-002
    #[test]
    fn remove_node_leaves_migrations_and_replicas_dangling() {
        let state = state_with_primaries(2);
        // A replica parented to the node that is about to be forgotten.
        state
            .apply_local(ClusterCommand::AddNode {
                node: NodeInfo::new_replica(3, test_addr(6382), test_addr(16382), 1),
            })
            .unwrap();
        assign(&state, 1, 0, 10);
        state
            .apply_local(ClusterCommand::BeginSlotMigration {
                slot: 5,
                source_node: 1,
                target_node: 2,
            })
            .unwrap();

        state
            .apply_command(ClusterCommand::RemoveNode { node_id: 1 })
            .expect("forgetting a member must succeed");

        // Specced: the slots are orphaned rather than transferred.
        assert_eq!(state.get_slot_owner(0), None, "slots must be orphaned");
        assert_eq!(state.get_slot_owner(10), None);
        assert!(state.get_node(1).is_none());

        // Accepted non-guarantee: neither the migration nor the child's parent
        // pointer is cleaned up. Only a *force* Failover does that.
        let migration = state
            .get_slot_migration(5)
            .expect("the migration must survive the removal of its source");
        assert_eq!(migration.source_node, 1, "and it still names the dead node");
        assert_eq!(
            state.get_node(3).unwrap().primary_id,
            Some(1),
            "the replica still points at the removed primary"
        );
    }

    // FM-CLUSTER-003
    #[test]
    fn assign_slots_rejects_the_whole_batch_on_one_conflict() {
        let state = state_with_primaries(2);
        assign(&state, 1, 5, 6);

        // Node 2 asks for a clean range *and* a conflicting one, clean range
        // first so a mutate-as-you-validate implementation would have already
        // committed it by the time it hit the conflict.
        let err = state
            .apply_command(ClusterCommand::AssignSlots {
                node_id: 2,
                slots: vec![SlotRange::new(20, 30), SlotRange::new(5, 6)],
            })
            .expect_err("a batch containing a conflict must be refused");
        assert!(
            matches!(err, ClusterError::SlotAlreadyAssigned(5, 1)),
            "expected SlotAlreadyAssigned(5, 1), got {err:?}"
        );

        assert_eq!(
            state.get_slot_owner(20),
            None,
            "the clean prefix of the batch must not have been applied"
        );
        assert_eq!(state.get_slot_owner(30), None);
        assert_eq!(state.get_slot_owner(5), Some(1), "the incumbent keeps it");
    }

    // FM-CLUSTER-004
    #[test]
    fn remove_slots_ignores_the_node_it_names() {
        let state = state_with_primaries(2);
        assign(&state, 1, 0, 10);

        // Node 2 asks to remove slots node 1 owns. Today this succeeds: the
        // arm validates only that each slot is assigned to *someone*.
        let response = state
            .apply_command(ClusterCommand::RemoveSlots {
                node_id: 2,
                slots: vec![SlotRange::new(0, 10)],
            })
            .expect("owner-blind removal is today's behavior — see hardening issue 33");
        assert!(matches!(response.0, ClusterResponse::Ok));
        assert_eq!(state.get_slot_owner(5), None);

        // The all-or-nothing shape is still enforced, on assignment alone.
        assign(&state, 1, 0, 2);
        let err = state
            .apply_command(ClusterCommand::RemoveSlots {
                node_id: 1,
                slots: vec![SlotRange::new(0, 3)],
            })
            .expect_err("an unassigned slot in the batch must refuse the batch");
        assert!(matches!(err, ClusterError::SlotNotAssigned(3)), "{err:?}");
        assert_eq!(
            state.get_slot_owner(0),
            Some(1),
            "the refused batch must not have removed its valid prefix"
        );
    }

    // FM-CLUSTER-015
    #[test]
    fn increment_epoch_returns_typed_epoch() {
        let state = ClusterState::new();
        let (response, events) = state.apply_command(ClusterCommand::IncrementEpoch).unwrap();
        assert!(
            matches!(response, ClusterResponse::Epoch(1)),
            "expected the new epoch as a typed value, got {response:?}"
        );
        assert!(events.is_empty(), "an epoch bump is not a role change");
        let (response, _) = state.apply_command(ClusterCommand::IncrementEpoch).unwrap();
        assert!(matches!(response, ClusterResponse::Epoch(2)));
        assert_eq!(state.config_epoch(), 2);
    }

    // FM-CLUSTER-032
    #[test]
    fn begin_migration_accepts_an_unassigned_slot() {
        let state = state_with_primaries(2);
        // No AssignSlots at all: a follower's slot map is legitimately empty,
        // because bootstrap seeds slots locally rather than through Raft.
        assert_eq!(state.get_slot_owner(100), None);

        state
            .apply_command(ClusterCommand::BeginSlotMigration {
                slot: 100,
                source_node: 1,
                target_node: 2,
            })
            .expect("the owner check must be skipped when the slot has no owner");

        assert!(state.is_slot_migrating(100));
        let migration = state.get_slot_migration(100).unwrap();
        assert_eq!((migration.source_node, migration.target_node), (1, 2));
    }

    // FM-CLUSTER-033
    #[test]
    fn complete_migration_transfers_ownership_over_an_existing_owner() {
        let state = state_with_primaries(2);
        assign(&state, 1, 0, 10);
        state
            .apply_local(ClusterCommand::BeginSlotMigration {
                slot: 5,
                source_node: 1,
                target_node: 2,
            })
            .unwrap();
        let epoch_before = state.config_epoch();

        // Slot 5 is still recorded as owned by node 1. Completion inserts over
        // that owner without consulting the map — the parameter match against
        // the recorded migration is what authorizes it.
        let (response, events) = state
            .apply_command(ClusterCommand::CompleteSlotMigration {
                slot: 5,
                source_node: 1,
                target_node: 2,
            })
            .expect("a matching completion must not hit SlotAlreadyAssigned");
        assert!(matches!(response, ClusterResponse::Ok));
        assert_eq!(events.len(), 1, "exactly one completion event");

        assert_eq!(state.get_slot_owner(5), Some(2), "ownership moved");
        assert_eq!(state.get_slot_owner(4), Some(1), "neighbours untouched");
        assert!(!state.is_slot_migrating(5), "the migration record is gone");
        assert_eq!(
            state.config_epoch(),
            epoch_before,
            "completion is authorized by the migration record, not a new epoch"
        );
    }
}
