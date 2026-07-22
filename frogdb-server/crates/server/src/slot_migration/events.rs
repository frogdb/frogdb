//! Slot migration event dispatch.
//!
//! Consumes [`SlotMigrationCompleteEvent`]s emitted by the Raft state machine
//! when a `CompleteSlotMigration` command is applied, and fans them out to the
//! per-shard [`ClusterMsg::SlotMigrated`] notification used to wake blocked
//! clients with the correct MOVED redirect.

use frogdb_core::sync::Arc;
use frogdb_core::{ClusterMsg, ClusterState, ShardSender, SlotMigrationCompleteEvent};
use tokio::sync::mpsc::UnboundedReceiver;

use super::SlotMigrationCoordinator;

impl SlotMigrationCoordinator {
    /// Run the event dispatcher loop. Each completion event is translated into
    /// a [`ClusterMsg::SlotMigrated`] sent to the shard owning the slot, so
    /// blocked clients on that shard get the MOVED redirect for the new owner.
    pub(super) async fn run_event_dispatcher(
        cluster_state: Arc<ClusterState>,
        mut migration_rx: UnboundedReceiver<SlotMigrationCompleteEvent>,
        shard_senders: Arc<Vec<ShardSender>>,
        num_shards: usize,
    ) {
        while let Some(event) = migration_rx.recv().await {
            let target_addr = match cluster_state.get_node(event.target_node) {
                Some(node_info) => node_info.addr,
                None => {
                    tracing::warn!(
                        slot = event.slot,
                        target_node = event.target_node,
                        "Migration complete but target node not found in cluster state"
                    );
                    continue;
                }
            };

            let target_shard = event.slot as usize % num_shards;
            if let Some(sender) = shard_senders.get(target_shard) {
                let _ = sender
                    .send(ClusterMsg::SlotMigrated {
                        slot: event.slot,
                        target_addr,
                    })
                    .await;
            }
        }
    }
}
