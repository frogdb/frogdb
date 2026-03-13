//! Cluster and Raft command handlers.
//!
//! This module handles cluster-related commands:
//! - Raft cluster operations (MEET, FORGET, ADDSLOTS, etc.)
//! - CLUSTER FAILOVER

use frogdb_core::ClusterRaft;
use frogdb_core::cluster::{ClusterCommand, ClusterResponse, NodeRole};
use frogdb_protocol::{RaftClusterOp, Response};
use openraft::error::{ClientWriteError, RaftError};

use crate::connection::ConnectionHandler;
use crate::connection::util::convert_raft_cluster_op;

impl ConnectionHandler {
    /// Handle a Raft cluster command asynchronously.
    ///
    /// This method is called when a cluster command (MEET, FORGET, ADDSLOTS, etc.)
    /// returns `Response::RaftNeeded`. It executes the Raft operation asynchronously
    /// and updates the NetworkFactory after successful commit.
    pub(crate) async fn handle_raft_command(
        &self,
        op: RaftClusterOp,
        register_node: Option<(u64, std::net::SocketAddr)>,
        unregister_node: Option<u64>,
    ) -> Response {
        // Get the Raft instance
        let raft = match &self.raft {
            Some(r) => r,
            None => return Response::error("ERR Cluster mode not enabled"),
        };

        // Handle Failover specially - it requires multiple Raft commands
        if let RaftClusterOp::Failover {
            replica_id,
            primary_id,
            force,
        } = &op
        {
            return self
                .handle_failover_command(raft, *replica_id, *primary_id, *force)
                .await;
        }

        // Handle ResetCluster specially - needs to update self_node_id after commit
        if let RaftClusterOp::ResetCluster {
            node_id,
            new_node_id,
        } = &op
        {
            return self
                .handle_reset_command(raft, *node_id, *new_node_id)
                .await;
        }

        // Convert protocol RaftClusterOp to core ClusterCommand
        let cmd = match convert_raft_cluster_op(&op) {
            Some(cmd) => cmd,
            None => return Response::error("ERR Unsupported cluster operation"),
        };

        // Execute the Raft command
        match raft.client_write(cmd).await {
            Ok(resp) => {
                // Check if the state machine returned an error
                if let ClusterResponse::Error(msg) = &resp.data {
                    return Response::error(format!("ERR {}", msg));
                }

                // Update NetworkFactory after successful Raft commit
                if let Some((node_id, addr)) = register_node
                    && let Some(ref factory) = self.network_factory
                {
                    factory.register_node(node_id, addr);
                }
                if let Some(node_id) = unregister_node
                    && let Some(ref factory) = self.network_factory
                {
                    factory.remove_node(node_id);
                }
                Response::ok()
            }
            Err(e) => {
                // Check if this is a ForwardToLeader error
                if let RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) = &e {
                    // Try to get the leader's client address from ClusterState
                    if let Some(leader_id) = forward.leader_id
                        && let Some(ref cluster_state) = self.cluster_state
                        && let Some(leader_info) = cluster_state.get_node(leader_id)
                    {
                        // Return redirect error with leader's client address
                        return Response::error(format!(
                            "REDIRECT {} {}",
                            leader_id, leader_info.addr
                        ));
                    }
                    // Leader unknown - return error with whatever info we have
                    return Response::error(format!(
                        "CLUSTERDOWN No leader available: {:?}",
                        forward.leader_id
                    ));
                }
                // Other errors
                Response::error(format!("ERR Raft error: {}", e))
            }
        }
    }

    /// Handle a CLUSTER FAILOVER command.
    ///
    /// When `force` is true (dead primary), this:
    /// 1. Snapshots the old primary's slots
    /// 2. Removes the dead primary (clears its slots atomically)
    /// 3. Reassigns the slots to the new primary
    /// 4. Increments config epoch
    ///
    /// When `force` is false (graceful failover):
    /// 1. Promotes replica to primary via SetRole
    /// 2. Transfers slots from old primary to new primary
    /// 3. Increments config epoch
    async fn handle_failover_command(
        &self,
        raft: &ClusterRaft,
        replica_id: u64,
        primary_id: u64,
        force: bool,
    ) -> Response {
        // Get cluster state to find slots to transfer
        let cluster_state = match &self.cluster_state {
            Some(cs) => cs,
            None => return Response::error("ERR Cluster state not available"),
        };

        // 1. Snapshot slots BEFORE any mutations
        let snapshot = cluster_state.snapshot();
        let slots = snapshot.get_node_slots(primary_id);

        // 2. If force, remove the dead primary (clears node + its slot assignments)
        if force {
            if let Err(e) = raft
                .client_write(ClusterCommand::RemoveNode {
                    node_id: primary_id,
                })
                .await
            {
                return Response::error(format!(
                    "ERR Failover failed to remove dead primary: {}",
                    e
                ));
            }
        } else {
            // Graceful: promote replica to Primary
            let role_cmd = ClusterCommand::SetRole {
                node_id: replica_id,
                role: NodeRole::Primary,
                primary_id: None,
            };
            if let Err(e) = raft.client_write(role_cmd).await {
                return Response::error(format!("ERR Failover failed to promote replica: {}", e));
            }
        }

        // 3. Reassign slots from old primary to new primary
        for range in slots {
            let slot_cmd = ClusterCommand::AssignSlots {
                node_id: replica_id,
                slots: vec![range],
            };
            if let Err(e) = raft.client_write(slot_cmd).await {
                tracing::warn!(
                    error = %e,
                    slot_range = ?range,
                    "Failed to transfer slots during failover"
                );
            }
        }

        // 4. Increment config epoch to signal the cluster topology change
        if let Err(e) = raft.client_write(ClusterCommand::IncrementEpoch).await {
            tracing::warn!(error = %e, "Failed to increment epoch during failover");
        }

        tracing::info!(
            new_primary = replica_id,
            old_primary = primary_id,
            force,
            "Manual failover completed"
        );

        Response::ok()
    }

    /// Handle a CLUSTER RESET command.
    ///
    /// 1. Commits ResetCluster via Raft (clears slots, forgets nodes, promotes to primary)
    /// 2. For HARD: updates self_node_id on ClusterState so all connections see the new ID
    /// 3. Unregisters other nodes from NetworkFactory
    async fn handle_reset_command(
        &self,
        raft: &ClusterRaft,
        node_id: u64,
        new_node_id: Option<u64>,
    ) -> Response {
        let cluster_state = match &self.cluster_state {
            Some(cs) => cs,
            None => return Response::error("ERR Cluster state not available"),
        };

        // Snapshot current nodes (excluding self) to unregister from NetworkFactory
        let other_node_ids: Vec<u64> = cluster_state
            .snapshot()
            .nodes
            .keys()
            .filter(|&&id| id != node_id)
            .copied()
            .collect();

        // Commit the reset via Raft
        let cmd = ClusterCommand::ResetCluster {
            node_id,
            new_node_id,
        };
        match raft.client_write(cmd).await {
            Ok(resp) => {
                if let ClusterResponse::Error(msg) = &resp.data {
                    return Response::error(format!("ERR {}", msg));
                }
            }
            Err(e) => {
                return Response::error(format!("ERR Raft error: {}", e));
            }
        }

        // HARD: update the shared self_node_id so all connections see the new ID
        if let Some(new_id) = new_node_id {
            cluster_state.set_self_node_id(new_id);
        }

        // Unregister other nodes from NetworkFactory
        if let Some(ref factory) = self.network_factory {
            for id in other_node_ids {
                factory.remove_node(id);
            }
        }

        Response::ok()
    }
}
