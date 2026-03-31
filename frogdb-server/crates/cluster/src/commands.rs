//! Command handlers for cluster state mutations.

use crate::types::{
    ClusterCommand, ClusterError, ClusterResponse, MigrationState, NodeRole, SlotMigration,
};

use super::state::ClusterState;

impl ClusterState {
    /// Apply a command to the state.
    pub(crate) fn apply_command(
        &self,
        cmd: ClusterCommand,
    ) -> Result<ClusterResponse, ClusterError> {
        let mut inner = self.inner.write();

        match cmd {
            ClusterCommand::AddNode { node } => {
                let existed = inner.nodes.contains_key(&node.id);
                if existed {
                    tracing::info!(node_id = node.id, addr = %node.addr, "Updated node in cluster");
                } else {
                    tracing::info!(node_id = node.id, addr = %node.addr, "Adding node to cluster");
                }
                inner.nodes.insert(node.id, node);
                Ok(ClusterResponse::Ok)
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
                Ok(ClusterResponse::Ok)
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
                Ok(ClusterResponse::Ok)
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
                Ok(ClusterResponse::Ok)
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
                Ok(ClusterResponse::Ok)
            }

            ClusterCommand::IncrementEpoch => {
                inner.config_epoch += 1;
                tracing::debug!(epoch = inner.config_epoch, "Incremented config epoch");
                Ok(ClusterResponse::Value(inner.config_epoch.to_string()))
            }

            ClusterCommand::MarkNodeFailed { node_id } => {
                let node = inner
                    .nodes
                    .get_mut(&node_id)
                    .ok_or(ClusterError::NodeNotFound(node_id))?;
                node.flags.fail = true;
                tracing::warn!(node_id, "Marked node as failed");
                Ok(ClusterResponse::Ok)
            }

            ClusterCommand::MarkNodeRecovered { node_id } => {
                let node = inner
                    .nodes
                    .get_mut(&node_id)
                    .ok_or(ClusterError::NodeNotFound(node_id))?;
                node.flags.fail = false;
                node.flags.pfail = false;
                tracing::info!(node_id, "Marked node as recovered");
                Ok(ClusterResponse::Ok)
            }

            ClusterCommand::BeginSlotMigration {
                slot,
                source_node,
                target_node,
            } => {
                // Idempotent: if the exact same migration is already in progress, succeed.
                if let Some(existing) = inner.migrations.get(&slot) {
                    if existing.source_node == source_node && existing.target_node == target_node {
                        return Ok(ClusterResponse::Ok);
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
                        state: MigrationState::Initiated,
                    },
                );
                tracing::info!(slot, source_node, target_node, "Started slot migration");
                Ok(ClusterResponse::Ok)
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
                Ok(ClusterResponse::Ok)
            }

            ClusterCommand::CancelSlotMigration { slot } => {
                inner.migrations.remove(&slot);
                tracing::info!(slot, "Cancelled slot migration");
                Ok(ClusterResponse::Ok)
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
                Ok(ClusterResponse::Ok)
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

                Ok(ClusterResponse::Ok)
            }
        }
    }
}
