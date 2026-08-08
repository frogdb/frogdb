//! Cluster and Raft command handlers.
//!
//! This module handles cluster-related commands:
//! - Raft cluster operations (MEET, FORGET, ADDSLOTS, etc.)
//! - CLUSTER FAILOVER

use frogdb_core::ClusterRaft;
use frogdb_core::cluster::{
    ClusterResponse, ClusterWriter, LeaderRedirect, ProposeError, Proposed, ResetProposed,
    spawn_voter_change, voter_change,
};
use frogdb_protocol::{RaftClusterOp, Response, SlotMigrationKind};

use crate::connection::ConnectionHandler;
use crate::connection::util::raft_op_to_command;

/// Render a typed [`LeaderRedirect`] into the RESP `REDIRECT`/`CLUSTERDOWN` wire
/// string. This is the single home for the two byte-identical format strings the
/// forwarding metadata-write sites (`handle_raft_command` and
/// `SlotMigrationCoordinator::commit`) previously maintained by hand.
pub(crate) fn redirect_to_response(r: LeaderRedirect) -> Response {
    match (r.leader_id, r.leader_client_addr) {
        (Some(id), Some(addr)) => Response::error(format!("REDIRECT {} {}", id, addr)),
        (id, _) => Response::error(format!("CLUSTERDOWN No leader available: {:?}", id)),
    }
}

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
        let raft = match &self.cluster.raft {
            Some(r) => r,
            None => return Response::error("ERR Cluster mode not enabled"),
        };

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

        // The forward/redirect saga needs the network factory (bus forward
        // target) and cluster state (client redirect address) in addition to the
        // Raft instance. In cluster mode all three are populated together (see
        // `cluster_init`), so their absence is standalone mode.
        let (network_factory, cluster_state) =
            match (&self.cluster.network_factory, &self.cluster.cluster_state) {
                (Some(nf), Some(cs)) => (nf, cs),
                _ => return Response::error("ERR Cluster mode not enabled"),
            };

        // Convert protocol RaftClusterOp to core ClusterCommand. The adapter is
        // total, so there is no "unsupported operation" fall-through: every
        // non-`ResetCluster` op (which early-returned above) yields a command.
        // Failover among them maps to the atomic composite command: role change,
        // slot transfer, and epoch bump are one replicated state-machine
        // transition (previously a multi-entry saga that could leave slots
        // ownerless if the leader crashed between entries).
        let cmd = raft_op_to_command(&op);

        // The Raft voter set follows the committed topology in both directions:
        // a node this command adds becomes a voter, a node it removes stops
        // being one. Derived here because `propose` consumes the command, and
        // from the command rather than from the connection layer's
        // register/unregister hints so that every membership-moving op is
        // covered by one rule (`voter_change`), including the forced failover
        // that removes the old primary without ever naming `unregister_node`.
        let membership_change = voter_change(&cmd);

        // The writer owns the propose → (forward | redirect) saga and its
        // retained-for-forward command copy; the connection layer keeps only its
        // register/unregister side effects, which diverge by outcome.
        let writer =
            ClusterWriter::new(raft.clone(), network_factory.clone(), cluster_state.clone());
        match writer.propose(cmd).await {
            Ok(Proposed::Committed(resp)) => {
                // Check if the state machine returned an error
                if let ClusterResponse::Error(msg) = &resp {
                    return Response::error(format!("ERR {}", msg));
                }

                // Leader-local commit: update the transport AND move the Raft
                // voter set. This is the only voter change on the pure-leader
                // path (there is no remote receiver to do it).
                if let Some((node_id, addr)) = register_node {
                    network_factory.register_node(node_id, addr);
                }
                if let Some(node_id) = unregister_node {
                    network_factory.remove_node(node_id);
                }
                if let Some(change) = membership_change {
                    spawn_voter_change((**raft).clone(), change);
                }
                Response::ok()
            }
            Ok(Proposed::Forwarded) => {
                // Forwarded to the leader: transport bookkeeping only. The voter
                // change was performed by the leader-side `ForwardedWrite`
                // receiver, so repeating it here would spawn a membership
                // proposal doomed to `ForwardToLeader`.
                if let Some((node_id, addr)) = register_node {
                    network_factory.register_node(node_id, addr);
                }
                if let Some(node_id) = unregister_node {
                    network_factory.remove_node(node_id);
                }
                Response::ok()
            }
            Err(ProposeError::Redirect(r)) => redirect_to_response(r),
            Err(ProposeError::Raft(e)) => Response::error(format!("ERR Raft error: {}", e)),
        }
    }

    /// Dispatch a slot-migration lifecycle operation to the coordinator.
    ///
    /// Called when CLUSTER SETSLOT returns `Response::SlotMigrationNeeded`.
    /// The coordinator handles Raft commit and `ForwardToLeader` redirects
    /// internally, so this is a thin delegation.
    pub(crate) async fn handle_slot_migration(&self, kind: SlotMigrationKind) -> Response {
        let coordinator = match &self.cluster.slot_migration {
            Some(c) => c,
            None => return Response::error("ERR Cluster mode not enabled"),
        };
        match kind {
            SlotMigrationKind::Begin {
                slot,
                source_node,
                target_node,
            } => coordinator.begin(slot, source_node, target_node).await,
            SlotMigrationKind::Complete {
                slot,
                source_node,
                target_node,
            } => coordinator.complete(slot, source_node, target_node).await,
            SlotMigrationKind::Cancel { slot } => coordinator.cancel(slot).await,
        }
    }

    /// Handle a CLUSTER RESET command.
    ///
    /// Proposes through [`ClusterWriter::propose_reset`] like every other admin
    /// command, so a reset that lands on a follower is forwarded or answered
    /// with `REDIRECT` instead of a `Debug`-rendered `ForwardToLeader`. The
    /// writer owns the identity update; the connection layer keeps only the
    /// transport cleanup, which is the piece it alone can perform.
    async fn handle_reset_command(
        &self,
        raft: &std::sync::Arc<ClusterRaft>,
        node_id: u64,
        new_node_id: Option<u64>,
    ) -> Response {
        let (network_factory, cluster_state) =
            match (&self.cluster.network_factory, &self.cluster.cluster_state) {
                (Some(nf), Some(cs)) => (nf, cs),
                _ => return Response::error("ERR Cluster mode not enabled"),
            };

        let writer =
            ClusterWriter::new(raft.clone(), network_factory.clone(), cluster_state.clone());
        match writer.propose_reset(node_id, new_node_id).await {
            Ok(ResetProposed::Applied { forget_nodes }) => {
                for id in forget_nodes {
                    network_factory.remove_node(id);
                }
                Response::ok()
            }
            Ok(ResetProposed::Rejected(e)) => Response::error(format!("ERR {}", e)),
            Err(ProposeError::Redirect(r)) => redirect_to_response(r),
            Err(ProposeError::Raft(e)) => Response::error(format!("ERR Raft error: {}", e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pin the exact `REDIRECT`/`CLUSTERDOWN` wire bytes so the collapse of the
    /// two former byte-identical copies into `redirect_to_response` is provably
    /// lossless.
    // FM-CLUSTER-049
    #[test]
    fn redirect_to_response_renders_wire_strings() {
        let redirect = redirect_to_response(LeaderRedirect {
            leader_id: Some(7),
            leader_client_addr: Some("127.0.0.1:6379".parse().unwrap()),
        });
        assert_eq!(
            redirect,
            Response::error("REDIRECT 7 127.0.0.1:6379".to_string())
        );

        // Leader known but its client address is not resolvable → CLUSTERDOWN
        // carrying `Some(id)`, matching the historic `{:?}` rendering.
        let clusterdown_known = redirect_to_response(LeaderRedirect {
            leader_id: Some(7),
            leader_client_addr: None,
        });
        assert_eq!(
            clusterdown_known,
            Response::error("CLUSTERDOWN No leader available: Some(7)".to_string())
        );

        // No leader at all → CLUSTERDOWN carrying `None`.
        let clusterdown_none = redirect_to_response(LeaderRedirect {
            leader_id: None,
            leader_client_addr: None,
        });
        assert_eq!(
            clusterdown_none,
            Response::error("CLUSTERDOWN No leader available: None".to_string())
        );
    }
}
