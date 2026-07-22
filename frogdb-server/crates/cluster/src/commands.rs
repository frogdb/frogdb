//! Command handlers for cluster state mutations.

use crate::types::{
    ClusterCommand, ClusterError, ClusterEvent, ClusterResponse, NodeRole, SlotMigration,
};

use super::state::ClusterState;

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
            ClusterCommand::AddNode { node } => {
                let existed = inner.nodes.contains_key(&node.id);
                if existed {
                    tracing::info!(node_id = node.id, addr = %node.addr, "Updated node in cluster");
                } else {
                    tracing::info!(node_id = node.id, addr = %node.addr, "Adding node to cluster");
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
                node.role = role;
                node.primary_id = primary_id;
                tracing::info!(node_id, ?role, "Set node role");

                // Setting a node to Replica is a demotion. This is node-agnostic;
                // the state machine decides whether it applies to *this* node.
                let mut events = Vec::new();
                if role == NodeRole::Replica {
                    events.push(ClusterEvent::NodeDemoted {
                        demoted_node_id: node_id,
                        new_primary_id: primary_id,
                        epoch: inner.config_epoch,
                    });
                }
                Ok((ClusterResponse::Ok, events))
            }

            ClusterCommand::IncrementEpoch => {
                inner.config_epoch += 1;
                tracing::debug!(epoch = inner.config_epoch, "Incremented config epoch");
                Ok((
                    ClusterResponse::Value(inner.config_epoch.to_string()),
                    Vec::new(),
                ))
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
                {
                    let new_node = inner.nodes.get_mut(&new_primary_id).unwrap();
                    new_node.role = NodeRole::Primary;
                    new_node.primary_id = None;
                }

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

                let mut events = Vec::new();
                if graceful_demotion {
                    events.push(ClusterEvent::NodeDemoted {
                        demoted_node_id: old_primary_id,
                        new_primary_id: Some(new_primary_id),
                        epoch,
                    });
                }
                Ok((ClusterResponse::Value(epoch.to_string()), events))
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
