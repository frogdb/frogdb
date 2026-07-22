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
//! through Raft via the shared [`ClusterWriter`] propose seam, which owns the
//! propose → forward → redirect saga. `commit` keeps only its own policy:
//! render a `ProposeError::Redirect` into the wire `REDIRECT`/`CLUSTERDOWN`
//! string (shared with [`crate::connection::cluster::handle_raft_command`] via
//! [`crate::connection::cluster::redirect_to_response`]).
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
//! machine and fans them out to the per-shard `ClusterMsg::SlotMigrated`
//! channel used to wake blocked clients with the correct MOVED address.

mod events;
pub(crate) mod redirect;
mod routing;
#[cfg(test)]
mod tests;
mod validator;

pub use routing::{RouteDecision, RouteOutcome};
pub(crate) use validator::SlotValidator;

use frogdb_core::cluster::{ClusterCommand, ClusterWriter, ProposeError, Proposed};
use frogdb_core::sync::Arc;
use frogdb_core::{
    ClusterNetworkFactory, ClusterRaft, ClusterResponse, ClusterState, NodeId, ShardSender,
    SlotMigrationCompleteEvent,
};
use frogdb_protocol::Response;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::connection::cluster::redirect_to_response;
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
    /// out to the appropriate per-shard `ClusterMsg::SlotMigrated` channels.
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
    /// [`crate::connection::cluster::handle_raft_command`]. Returns
    /// `Response::ok()` on success, or a properly formatted error response.
    async fn commit(&self, cmd: ClusterCommand) -> Response {
        // Slot-migration commits carry no register/voter side effects, so
        // leader-commit and forward-success collapse to the same success shape;
        // only a leader commit can surface a state-machine error.
        let writer = ClusterWriter::new(
            self.raft.clone(),
            self.network_factory.clone(),
            self.cluster_state.clone(),
        );
        match writer.propose(cmd).await {
            Ok(Proposed::Committed(resp)) => {
                if let ClusterResponse::Error(msg) = &resp {
                    return Response::error(format!("ERR {}", msg));
                }
                Response::ok()
            }
            Ok(Proposed::Forwarded) => Response::ok(),
            Err(ProposeError::Redirect(r)) => redirect_to_response(r),
            Err(ProposeError::Raft(e)) => Response::error(format!("ERR Raft error: {}", e)),
        }
    }
}
