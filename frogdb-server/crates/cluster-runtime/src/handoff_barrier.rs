//! Source-side half of the two-phase slot handoff (rework issue 02).
//!
//! `frogdb-cluster` decides *that* a slot handoff is prepared; this module is
//! what the decision means on the node that owns the slot. On a prepare it arms
//! a slot-scoped write barrier and drains the owning shard, then confirms the
//! drain back through Raft. On a release it drops the barrier.
//!
//! ## Why the barrier is armed synchronously
//!
//! `Prepared` and `Released` arrive on one channel in Raft apply order, and the
//! pair is not commutative. If the arm were deferred into the spawned task, a
//! `Released` for the same slot could be handled first and the barrier would
//! then be armed with nobody left to lift it — the slot would stay fenced for
//! the whole timeout. So the arm and the release both happen inline in the recv
//! loop, and only the drain round trip (which can block on a busy shard) is
//! spawned. A wedged shard delays a confirmation; it cannot delay a release.
//!
//! ## Why the local barrier outlives the replicated window
//!
//! The state machine measures the admissible window from the *proposer's*
//! timestamp, while the source arms its pause when it applies the entry, which
//! is necessarily later. The local barrier therefore expires after the window
//! in which a `CompleteSlotMigration` would be accepted — the safe direction.
//! The reverse would leave a gap where ownership can still move but the source
//! is serving writes again, which is the bug this whole mechanism exists to
//! close.
//!
//! ## Why a drain timeout aborts instead of proceeding
//!
//! If the shard does not answer within [`HANDOFF_DRAIN_TIMEOUT_MS`] this module
//! simply never confirms. The finalizer's own budget then lapses and it aborts
//! the handoff, leaving the migration record intact for a retry. Dropping the
//! barrier and completing anyway — Dragonfly's choice — would silently
//! reintroduce exactly the exposure being fenced.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use frogdb_core::clock;
use frogdb_core::{
    ClientRegistry, ClusterMsg, ClusterState, HANDOFF_DRAIN_TIMEOUT_MS, NodeId, PauseMode,
    ShardSender, SlotHandoffEvent,
};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot;
use tracing::{debug, warn};

/// Wall-clock milliseconds since the Unix epoch, read through the clock seam.
///
/// Every handoff deadline is minted here, by the *proposer*, and carried in the
/// Raft entry as plain data. The state machine must never read a clock during
/// `apply`: two nodes applying the same entry at different instants would
/// compute different deadlines and diverge.
pub fn handoff_now_ms() -> u64 {
    clock::system_now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}

/// Wall-clock milliseconds for an arbitrary [`SystemTime`], for callers that
/// already hold one.
pub fn to_ms(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}

/// What one [`SlotHandoffEvent`] asks *this* node to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandoffAction {
    /// Arm the slot barrier and drain `shard`, then confirm `seq`.
    Arm {
        /// The slot to fence.
        slot: u16,
        /// The attempt identifier to echo back on confirmation.
        seq: u64,
        /// The shard whose mailbox round trip constitutes the drain.
        shard: usize,
        /// How long the barrier stays armed.
        barrier_ms: u64,
    },
    /// Drop the slot barrier.
    Release {
        /// The slot to unfence.
        slot: u16,
    },
    /// This node is not the source; nothing to do.
    Ignore,
}

/// Decide what an event means for `self_node_id`, without touching anything.
///
/// `num_shards` of zero is treated as one so a misconfigured caller cannot
/// panic a background task on the modulo.
pub fn plan_handoff_action(
    event: &SlotHandoffEvent,
    self_node_id: Option<NodeId>,
    num_shards: usize,
) -> HandoffAction {
    match event {
        SlotHandoffEvent::Prepared {
            slot,
            source_node,
            seq,
            barrier_ms,
            ..
        } if Some(*source_node) == self_node_id => HandoffAction::Arm {
            slot: *slot,
            seq: *seq,
            shard: *slot as usize % num_shards.max(1),
            barrier_ms: *barrier_ms,
        },
        SlotHandoffEvent::Released {
            slot, source_node, ..
        } if Some(*source_node) == self_node_id => HandoffAction::Release { slot: *slot },
        _ => HandoffAction::Ignore,
    }
}

/// Round-trip a `DrainSlot` message through `shard`'s mailbox.
///
/// Returns `true` only if the shard answered inside
/// [`HANDOFF_DRAIN_TIMEOUT_MS`]. The round trip *is* the drain: the shard runs
/// one message at a time, and scripts and transactions execute inline in that
/// loop, so an answer proves every command enqueued before the barrier was
/// armed has finished.
async fn drain_shard(slot: u16, shard: usize, shard_senders: &[ShardSender]) -> bool {
    let Some(sender) = shard_senders.get(shard) else {
        warn!(
            slot,
            shard,
            num_shards = shard_senders.len(),
            "Slot handoff drain addressed a shard that does not exist; the handoff will not be \
             confirmed and the finalizer will abort"
        );
        return false;
    };

    let (ack_tx, ack_rx) = oneshot::channel();
    if let Err(e) = sender
        .send(ClusterMsg::DrainSlot { slot, ack: ack_tx })
        .await
    {
        warn!(slot, shard, error = %e, "Slot handoff drain could not be enqueued");
        return false;
    }

    match tokio::time::timeout(Duration::from_millis(HANDOFF_DRAIN_TIMEOUT_MS), ack_rx).await {
        Ok(Ok(())) => true,
        Ok(Err(_)) => {
            warn!(
                slot,
                shard, "Shard dropped the slot handoff drain acknowledgement"
            );
            false
        }
        Err(_) => {
            warn!(
                slot,
                shard,
                timeout_ms = HANDOFF_DRAIN_TIMEOUT_MS,
                "Slot handoff drain timed out; finalization will abort with the migration intact"
            );
            false
        }
    }
}

/// Run the source-side handoff loop until the event channel closes.
///
/// `confirm` proposes `ConfirmSlotHandoffDrained` for `(slot, seq)`. It is a
/// parameter rather than a `ClusterWriter` so this loop can be tested without a
/// live Raft instance; the production wiring hands it the same writer every
/// other metadata write goes through.
pub async fn run_slot_handoff_barrier<C, F>(
    cluster_state: Arc<ClusterState>,
    client_registry: Arc<ClientRegistry>,
    mut handoff_rx: UnboundedReceiver<SlotHandoffEvent>,
    shard_senders: Arc<Vec<ShardSender>>,
    num_shards: usize,
    confirm: C,
) where
    C: Fn(u16, u64) -> F + Send + Sync + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    let confirm = Arc::new(confirm);
    while let Some(event) = handoff_rx.recv().await {
        match plan_handoff_action(&event, cluster_state.self_node_id(), num_shards) {
            HandoffAction::Arm {
                slot,
                seq,
                shard,
                barrier_ms,
            } => {
                // Writes only: the source still holds the data and still owns
                // the slot until `Complete` lands, so reads stay served. This
                // matches Redis/Valkey atomic slot migration, which pauses
                // writes (plus expiry and eviction) rather than all traffic.
                client_registry.pause_slot(slot, PauseMode::Write, barrier_ms);
                debug!(slot, seq, shard, barrier_ms, "Armed slot handoff barrier");

                let shard_senders = shard_senders.clone();
                let confirm = confirm.clone();
                tokio::spawn(async move {
                    if drain_shard(slot, shard, &shard_senders).await {
                        confirm(slot, seq).await;
                    }
                });
            }
            HandoffAction::Release { slot } => {
                client_registry.unpause_slot(slot);
                debug!(slot, "Released slot handoff barrier");
            }
            HandoffAction::Ignore => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_core::cluster::{ClusterCommand, NodeInfo};
    use frogdb_core::shard::{Envelope, ShardMessage};
    use std::net::SocketAddr;
    use std::sync::Mutex;
    use tokio::sync::mpsc;

    fn test_addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    /// Cluster state that believes it is node `self_id`.
    fn cluster_state(self_id: NodeId) -> Arc<ClusterState> {
        let state = ClusterState::new();
        for id in 1..=2u64 {
            let port = 6379 + id as u16;
            state
                .apply_local(ClusterCommand::AddNode {
                    node: NodeInfo::new_primary(id, test_addr(port), test_addr(port + 10_000)),
                })
                .expect("seeding a primary must succeed");
        }
        state.set_self_node_id(self_id);
        Arc::new(state)
    }

    fn prepared(slot: u16, source_node: NodeId, seq: u64) -> SlotHandoffEvent {
        SlotHandoffEvent::Prepared {
            slot,
            source_node,
            target_node: 2,
            seq,
            barrier_ms: 100,
        }
    }

    fn released(slot: u16, source_node: NodeId, seq: u64) -> SlotHandoffEvent {
        SlotHandoffEvent::Released {
            slot,
            source_node,
            seq,
        }
    }

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

    /// Answer the next `DrainSlot` on `rx`, returning the slot it named.
    fn answer_drain(rx: &mut mpsc::Receiver<Envelope>) -> Option<u16> {
        match rx.try_recv().ok()?.message {
            ShardMessage::Cluster(ClusterMsg::DrainSlot { slot, ack }) => {
                let _ = ack.send(());
                Some(slot)
            }
            other => panic!("expected DrainSlot, got {other:?}"),
        }
    }

    /// What [`recording_confirm`] records into: every `(slot, seq)` proposed.
    type ConfirmLog = Arc<Mutex<Vec<(u16, u64)>>>;

    /// A confirm sink recording every `(slot, seq)` it was asked to propose.
    fn recording_confirm() -> (
        impl Fn(u16, u64) -> std::future::Ready<()> + Send + Sync + 'static,
        ConfirmLog,
    ) {
        let log = Arc::new(Mutex::new(Vec::new()));
        let sink = log.clone();
        (
            move |slot, seq| {
                sink.lock().unwrap().push((slot, seq));
                std::future::ready(())
            },
            log,
        )
    }

    // FM-CLUSTER-090
    #[test]
    fn only_the_source_node_acts_on_a_handoff() {
        // This node is the source: arm, drain shard `slot % num_shards`.
        assert_eq!(
            plan_handoff_action(&prepared(5, 1, 7), Some(1), 4),
            HandoffAction::Arm {
                slot: 5,
                seq: 7,
                shard: 1,
                barrier_ms: 100,
            }
        );
        assert_eq!(
            plan_handoff_action(&released(5, 1, 7), Some(1), 4),
            HandoffAction::Release { slot: 5 }
        );

        // Some other node is the source: the event still fires here (it is
        // replicated) but means nothing locally.
        assert_eq!(
            plan_handoff_action(&prepared(5, 2, 7), Some(1), 4),
            HandoffAction::Ignore
        );
        assert_eq!(
            plan_handoff_action(&released(5, 2, 7), Some(1), 4),
            HandoffAction::Ignore
        );

        // A node that does not yet know its own id must not arm anything.
        assert_eq!(
            plan_handoff_action(&prepared(5, 1, 7), None, 4),
            HandoffAction::Ignore
        );
    }

    // FM-CLUSTER-090
    #[test]
    fn drain_targets_slot_modulo_num_shards_and_survives_zero() {
        for (slot, num_shards, expected) in [(0u16, 4usize, 0usize), (5, 4, 1), (16383, 4, 3)] {
            assert_eq!(
                plan_handoff_action(&prepared(slot, 1, 1), Some(1), num_shards),
                HandoffAction::Arm {
                    slot,
                    seq: 1,
                    shard: expected,
                    barrier_ms: 100,
                }
            );
        }
        assert!(matches!(
            plan_handoff_action(&prepared(5, 1, 1), Some(1), 0),
            HandoffAction::Arm { shard: 0, .. }
        ));
    }

    /// The happy path end to end: the barrier goes up, the shard round trip
    /// completes, and the drain is confirmed with the attempt's own `seq`.
    // FM-CLUSTER-090
    #[tokio::test]
    async fn a_prepare_arms_the_barrier_drains_the_shard_and_confirms() {
        let (senders, mut receivers) = shard_channels(4);
        let registry = Arc::new(ClientRegistry::new());
        let (tx, rx) = mpsc::unbounded_channel();
        let (confirm, log) = recording_confirm();

        tx.send(prepared(5, 1, 7)).unwrap();
        drop(tx);
        run_slot_handoff_barrier(cluster_state(1), registry.clone(), rx, senders, 4, confirm).await;

        assert!(
            registry.slot_pause(Some(5)).is_some(),
            "slot 5's writes are fenced"
        );
        assert!(
            registry.slot_pause(Some(6)).is_none(),
            "and only slot 5's are"
        );
        // Let the spawned drain task enqueue its round trip.
        tokio::task::yield_now().await;
        assert_eq!(answer_drain(&mut receivers[1]), Some(5));
        // …and let it observe the ack and confirm.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert_eq!(*log.lock().unwrap(), vec![(5, 7)]);
    }

    /// The ordering hazard: a release that follows its prepare must find the
    /// barrier already armed, so it can actually lift it.
    // FM-CLUSTER-090
    #[tokio::test]
    async fn a_release_lifts_the_barrier_its_prepare_armed() {
        let (senders, mut receivers) = shard_channels(4);
        let registry = Arc::new(ClientRegistry::new());
        let (tx, rx) = mpsc::unbounded_channel();
        let (confirm, _log) = recording_confirm();

        tx.send(prepared(5, 1, 7)).unwrap();
        tx.send(released(5, 1, 7)).unwrap();
        drop(tx);
        run_slot_handoff_barrier(cluster_state(1), registry.clone(), rx, senders, 4, confirm).await;

        assert!(
            registry.slot_pause(Some(5)).is_none(),
            "the barrier is gone without waiting for its timeout"
        );
        // The drain was still enqueued: a release racing the drain must not
        // leave a message stuck in the shard's mailbox.
        tokio::task::yield_now().await;
        assert_eq!(answer_drain(&mut receivers[1]), Some(5));
    }

    /// A shard that never answers must not produce a confirmation — the
    /// finalizer's abort is what recovers, with the migration record intact.
    // FM-CLUSTER-091
    #[tokio::test(start_paused = true)]
    async fn a_shard_that_never_answers_is_never_confirmed() {
        let (senders, mut receivers) = shard_channels(4);
        let (confirm, log) = recording_confirm();

        let drained = tokio::spawn(async move { drain_shard(5, 1, &senders).await });
        // Take the message off the mailbox but never acknowledge it.
        tokio::task::yield_now().await;
        let msg = receivers[1].try_recv().expect("drain was enqueued");
        let ack = match msg.message {
            ShardMessage::Cluster(ClusterMsg::DrainSlot { ack, .. }) => ack,
            other => panic!("expected DrainSlot, got {other:?}"),
        };
        tokio::time::advance(Duration::from_millis(HANDOFF_DRAIN_TIMEOUT_MS + 1)).await;
        assert!(!drained.await.unwrap(), "the drain timed out");
        drop(ack);
        assert!(log.lock().unwrap().is_empty(), "nothing was confirmed");
        let _ = confirm;
    }

    /// A shard index that does not exist is a drain failure, not a silent
    /// success that would let ownership move without a drain.
    // FM-CLUSTER-091
    #[tokio::test]
    async fn a_missing_shard_fails_the_drain() {
        let (senders, _receivers) = shard_channels(4);
        assert!(!drain_shard(5, 99, &senders).await);

        // So does a shard whose receiver is gone.
        let (senders, receivers) = shard_channels(4);
        drop(receivers);
        assert!(!drain_shard(5, 1, &senders).await);
    }

    // FM-CLUSTER-089
    #[test]
    fn handoff_now_ms_reads_the_clock_seam() {
        let before = to_ms(clock::system_now());
        let sampled = handoff_now_ms();
        let after = to_ms(clock::system_now());
        assert!(
            (before..=after).contains(&sampled),
            "{sampled} outside [{before}, {after}]"
        );
    }
}
