//! Slot migration coordinator.
//!
//! Owns the full slot-migration lifecycle (begin/complete/cancel), the slot
//! routing decisions used by the connection layer, and the post-completion
//! event fanout to per-shard message channels. The coordinator wraps — but
//! does not own — the Raft-replicated [`ClusterState`]; the replicated state
//! itself stays in `frogdb-cluster::state`.
//!
//! ## Lifecycle ownership
//!
//! [`begin`](Self::begin), [`complete`](Self::complete), and
//! [`cancel`](Self::cancel) commit `ClusterCommand::*SlotMigration` entries
//! through Raft. They handle `ForwardToLeader` errors by forwarding the write
//! through the cluster bus or returning a `REDIRECT` to the caller, mirroring
//! the existing pattern used by [`crate::connection::handlers::cluster::handle_raft_command`].
//!
//! ## Routing
//!
//! [`route`](Self::route) returns a [`RouteDecision`] for any slot+command
//! combination. The connection layer ([`crate::connection::guards`]) parses
//! input and formats responses; the coordinator owns the *decision*.
//!
//! ## Events
//!
//! [`run_event_dispatcher`](Self::run_event_dispatcher) is a background task
//! that consumes [`SlotMigrationCompleteEvent`]s emitted by the Raft state
//! machine and fans them out to the per-shard `ShardMessage::SlotMigrated`
//! channel used to wake blocked clients with the correct MOVED address.

mod events;
mod routing;
#[cfg(test)]
mod tests;

pub use routing::RouteDecision;

use frogdb_core::cluster::ClusterCommand;
use frogdb_core::sync::Arc;
use frogdb_core::{
    ClusterNetworkFactory, ClusterRaft, ClusterResponse, ClusterState, NodeId, ShardSender,
    SlotMigrationCompleteEvent,
};
use frogdb_protocol::Response;
use openraft::error::{ClientWriteError, RaftError};
use tokio::sync::mpsc::UnboundedReceiver;

use crate::net::spawn;

/// Coordinator for slot migration lifecycle, routing, and post-completion events.
///
/// Construct with [`SlotMigrationCoordinator::new`] in cluster mode.
pub struct SlotMigrationCoordinator {
    pub(super) cluster_state: Arc<ClusterState>,
    raft: Arc<ClusterRaft>,
    network_factory: Arc<ClusterNetworkFactory>,
}

impl SlotMigrationCoordinator {
    /// Create a new coordinator wrapping the given replicated state and Raft instance.
    pub fn new(
        cluster_state: Arc<ClusterState>,
        raft: Arc<ClusterRaft>,
        network_factory: Arc<ClusterNetworkFactory>,
    ) -> Self {
        Self {
            cluster_state,
            raft,
            network_factory,
        }
    }

    /// Spawn the background task that fans slot-migration completion events
    /// out to the appropriate per-shard `ShardMessage::SlotMigrated` channels.
    pub fn spawn_event_dispatcher(
        &self,
        migration_rx: UnboundedReceiver<SlotMigrationCompleteEvent>,
        shard_senders: Arc<Vec<ShardSender>>,
        num_shards: usize,
    ) {
        let cluster_state = self.cluster_state.clone();
        spawn(async move {
            Self::run_event_dispatcher(cluster_state, migration_rx, shard_senders, num_shards).await
        });
    }

    /// True if `slot` currently has a migration in progress.
    pub fn is_migrating(&self, slot: u16) -> bool {
        self.cluster_state.is_slot_migrating(slot)
    }

    /// The migration record for `slot`, if any.
    pub fn migration_for(&self, slot: u16) -> Option<frogdb_cluster::types::SlotMigration> {
        self.cluster_state.get_slot_migration(slot)
    }

    /// Begin a slot migration (CLUSTER SETSLOT IMPORTING / MIGRATING).
    pub async fn begin(&self, slot: u16, source_node: NodeId, target_node: NodeId) -> Response {
        self.commit(ClusterCommand::BeginSlotMigration {
            slot,
            source_node,
            target_node,
        })
        .await
    }

    /// Complete a slot migration (CLUSTER SETSLOT NODE for the migrating slot).
    pub async fn complete(&self, slot: u16, source_node: NodeId, target_node: NodeId) -> Response {
        self.commit(ClusterCommand::CompleteSlotMigration {
            slot,
            source_node,
            target_node,
        })
        .await
    }

    /// Cancel an in-flight slot migration (CLUSTER SETSLOT STABLE).
    pub async fn cancel(&self, slot: u16) -> Response {
        self.commit(ClusterCommand::CancelSlotMigration { slot })
            .await
    }

    /// Submit a slot-migration `ClusterCommand` through Raft, with the same
    /// `ForwardToLeader` handling as
    /// [`crate::connection::handlers::cluster::handle_raft_command`]. Returns
    /// `Response::ok()` on success, or a properly formatted error response.
    async fn commit(&self, cmd: ClusterCommand) -> Response {
        let cmd_clone = cmd.clone();
        match self.raft.client_write(cmd).await {
            Ok(resp) => {
                if let ClusterResponse::Error(msg) = &resp.data {
                    return Response::error(format!("ERR {}", msg));
                }
                Response::ok()
            }
            Err(e) => {
                if let RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) = &e {
                    if let Some(leader_id) = forward.leader_id
                        && let Some(leader_addr) = self.network_factory.get_node_addr(leader_id)
                    {
                        let net = self.network_factory.connect(leader_id, leader_addr);
                        if net.forward_write(cmd_clone).await.is_ok() {
                            return Response::ok();
                        }
                    }

                    if let Some(leader_id) = forward.leader_id
                        && let Some(leader_info) = self.cluster_state.get_node(leader_id)
                    {
                        return Response::error(format!(
                            "REDIRECT {} {}",
                            leader_id, leader_info.addr
                        ));
                    }

                    return Response::error(format!(
                        "CLUSTERDOWN No leader available: {:?}",
                        forward.leader_id
                    ));
                }
                Response::error(format!("ERR Raft error: {}", e))
            }
        }
    }
}
