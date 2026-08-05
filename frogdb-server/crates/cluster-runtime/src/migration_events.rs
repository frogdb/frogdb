//! Slot-migration completion event dispatch.
//!
//! Consumes [`SlotMigrationCompleteEvent`]s emitted by the Raft state machine
//! when a `CompleteSlotMigration` command is applied, and fans them out to the
//! per-shard [`ClusterMsg::SlotMigrated`] notification that wakes blocked
//! clients on the slot that just moved away.
//!
//! The loop exists for exactly one purpose — waking those clients — so it must
//! not drop an event it cannot fully resolve. A client blocked on `BLPOP` with
//! a zero timeout, on a slot this node no longer owns, is blocked *forever* if
//! its notification is discarded: no local write can ever wake it. The decision
//! is therefore split into a pure [`plan_migration_notice`] (unit-testable with
//! no live cluster) and a delivery step that reports its own failures.

use std::net::SocketAddr;
use std::sync::Arc;

use frogdb_core::{ClusterMsg, ClusterState, ShardSender, SlotMigrationCompleteEvent};
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::{error, warn};

/// What one completion event asks the dispatcher to deliver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MigrationNotice {
    /// The local shard that owns the slot's blocked clients.
    pub shard: usize,
    /// The slot that moved.
    pub slot: u16,
    /// The new owner's client address, or `None` when this node cannot name it
    /// — a target added by a Raft entry this node has not applied yet. The
    /// shard then answers `CLUSTERDOWN` instead of `MOVED`, which is the same
    /// rendering routing already uses for "owner known, address unknown"
    /// (FM-CLUSTER-023).
    pub target_addr: Option<SocketAddr>,
}

/// Decide what a completion event should deliver, without sending anything.
///
/// `num_shards` is the live shard count; a zero is treated as one so a
/// misconfigured caller cannot panic a background task on the modulo. Delivery
/// then fails to find the shard and says so.
pub fn plan_migration_notice(
    event: &SlotMigrationCompleteEvent,
    cluster_state: &ClusterState,
    num_shards: usize,
) -> MigrationNotice {
    MigrationNotice {
        shard: event.slot as usize % num_shards.max(1),
        slot: event.slot,
        target_addr: cluster_state.get_node(event.target_node).map(|n| n.addr),
    }
}

/// Deliver one notice to its shard. Returns `false` if the shard could not be
/// reached, having logged why.
async fn deliver_migration_notice(notice: MigrationNotice, shard_senders: &[ShardSender]) -> bool {
    let Some(sender) = shard_senders.get(notice.shard) else {
        error!(
            slot = notice.slot,
            shard = notice.shard,
            num_shards = shard_senders.len(),
            "Slot migration notice addressed a shard that does not exist; blocked clients on \
             this slot cannot be woken"
        );
        return false;
    };
    if let Err(e) = sender
        .send(ClusterMsg::SlotMigrated {
            slot: notice.slot,
            target_addr: notice.target_addr,
        })
        .await
    {
        error!(
            slot = notice.slot,
            shard = notice.shard,
            error = %e,
            "Slot migration notice could not be delivered; blocked clients on this slot \
             cannot be woken"
        );
        return false;
    }
    true
}

/// Run the event dispatcher loop until the event channel closes.
///
/// Every event produces a notice: an unknown target downgrades the redirect
/// from `MOVED` to `CLUSTERDOWN` rather than dropping the event, because
/// leaving the clients parked is strictly worse than an imprecise wake-up.
pub async fn run_slot_migration_event_dispatcher(
    cluster_state: Arc<ClusterState>,
    mut migration_rx: UnboundedReceiver<SlotMigrationCompleteEvent>,
    shard_senders: Arc<Vec<ShardSender>>,
    num_shards: usize,
) {
    while let Some(event) = migration_rx.recv().await {
        let notice = plan_migration_notice(&event, &cluster_state, num_shards);
        if notice.target_addr.is_none() {
            warn!(
                slot = event.slot,
                target_node = event.target_node,
                "Migration complete but the target node is not in this node's cluster state; \
                 waking blocked clients with CLUSTERDOWN instead of MOVED"
            );
        }
        deliver_migration_notice(notice, &shard_senders).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_core::cluster::{ClusterCommand, NodeInfo};
    use frogdb_core::shard::{Envelope, ShardMessage};
    use tokio::sync::mpsc;

    fn test_addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    /// A cluster state holding primaries with ids `1..=count`.
    fn cluster_state(count: u64) -> Arc<ClusterState> {
        let state = ClusterState::new();
        for id in 1..=count {
            let port = 6379 + id as u16;
            state
                .apply_local(ClusterCommand::AddNode {
                    node: NodeInfo::new_primary(id, test_addr(port), test_addr(port + 10_000)),
                })
                .expect("seeding a primary must succeed");
        }
        Arc::new(state)
    }

    fn event(slot: u16, target_node: u64) -> SlotMigrationCompleteEvent {
        SlotMigrationCompleteEvent {
            slot,
            source_node: 1,
            target_node,
        }
    }

    /// `n` shard channels plus their receivers.
    fn shard_channels(n: usize) -> (Arc<Vec<ShardSender>>, Vec<mpsc::Receiver<Envelope>>) {
        let mut senders = Vec::new();
        let mut receivers = Vec::new();
        for _ in 0..n {
            let (tx, rx) = mpsc::channel(8);
            senders.push(ShardSender::new(tx));
            receivers.push(rx);
        }
        (Arc::new(senders), receivers)
    }

    /// The `SlotMigrated` payload of the next buffered message, if any.
    fn next_slot_migrated(rx: &mut mpsc::Receiver<Envelope>) -> Option<(u16, Option<SocketAddr>)> {
        match rx.try_recv().ok()?.message {
            ShardMessage::Cluster(ClusterMsg::SlotMigrated { slot, target_addr }) => {
                Some((slot, target_addr))
            }
            other => panic!("expected SlotMigrated, got {other:?}"),
        }
    }

    // FM-CLUSTER-038
    #[tokio::test]
    async fn migration_event_with_a_known_target_notifies_the_owning_shard() {
        let (senders, mut receivers) = shard_channels(4);
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(event(5, 2)).unwrap();
        drop(tx);

        run_slot_migration_event_dispatcher(cluster_state(2), rx, senders, 4).await;

        assert_eq!(
            next_slot_migrated(&mut receivers[1]),
            Some((5, Some(test_addr(6381)))),
            "slot 5 belongs to shard 1 and node 2's client address is known"
        );
    }

    /// The target is not in this node's snapshot — a legitimate state on a node
    /// lagging the entry that added it. The event must still reach the shard, or
    /// clients blocked on the departed slot are never woken.
    // FM-CLUSTER-038
    #[tokio::test]
    async fn migration_event_with_an_unknown_target_still_wakes_blocked_clients() {
        let (senders, mut receivers) = shard_channels(4);
        let (tx, rx) = mpsc::unbounded_channel();
        // Node 9 is not in the cluster state.
        tx.send(event(5, 9)).unwrap();
        drop(tx);

        run_slot_migration_event_dispatcher(cluster_state(2), rx, senders, 4).await;

        assert_eq!(
            next_slot_migrated(&mut receivers[1]),
            Some((5, None)),
            "the notice is delivered with no address rather than dropped"
        );
    }

    // FM-CLUSTER-038
    #[test]
    fn migration_event_routes_to_slot_modulo_num_shards() {
        let state = cluster_state(2);
        for (slot, num_shards, expected) in [(0u16, 4usize, 0usize), (5, 4, 1), (16383, 4, 3)] {
            assert_eq!(
                plan_migration_notice(&event(slot, 2), &state, num_shards).shard,
                expected,
                "slot {slot} over {num_shards} shards"
            );
        }
        // A zero shard count must not panic the modulo in a background task.
        assert_eq!(plan_migration_notice(&event(5, 2), &state, 0).shard, 0);
    }

    // FM-CLUSTER-038
    #[tokio::test]
    async fn migration_event_reports_a_closed_shard_channel() {
        let (senders, receivers) = shard_channels(4);
        drop(receivers);

        // A closed channel is a delivery failure, not a silent success.
        assert!(
            !deliver_migration_notice(
                MigrationNotice {
                    shard: 1,
                    slot: 5,
                    target_addr: Some(test_addr(6381)),
                },
                &senders,
            )
            .await
        );

        // So is a shard index the sender list does not have.
        assert!(
            !deliver_migration_notice(
                MigrationNotice {
                    shard: 99,
                    slot: 5,
                    target_addr: None,
                },
                &senders,
            )
            .await
        );
    }
}
