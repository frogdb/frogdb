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
//! [`spawn_event_dispatcher`](Self::spawn_event_dispatcher) spawns the loop that
//! consumes [`SlotMigrationCompleteEvent`]s emitted by the Raft state machine
//! and fans them out to the per-shard `ClusterMsg::SlotMigrated` channel used to
//! wake blocked clients. The loop itself lives in
//! [`frogdb_cluster_runtime::run_slot_migration_event_dispatcher`], next to the
//! rest of the cluster runtime; the coordinator only owns the spawn.

pub(crate) mod redirect;
mod routing;
#[cfg(test)]
mod tests;
mod validator;

pub(crate) use routing::{
    BatchKeys, BatchRoute, route_migrating_source, route_queued_batch, route_watched_keys,
    watch_slot_is_locally_served,
};
pub use routing::{RouteDecision, RouteOutcome};
pub(crate) use validator::SlotValidator;

use std::time::Duration;

use frogdb_cluster_runtime::handoff_now_ms;
use frogdb_core::cluster::{ClusterCommand, ClusterWriter, ProposeError, Proposed};
use frogdb_core::sync::Arc;
use frogdb_core::{
    ClientRegistry, ClusterNetworkFactory, ClusterRaft, ClusterResponse, ClusterState,
    HANDOFF_BARRIER_MS, HANDOFF_DRAIN_WAIT_MS, HANDOFF_LEASE_MS, HANDOFF_POLL_INTERVAL_MS, NodeId,
    ShardSender, SlotHandoffEvent, SlotMigrationCompleteEvent,
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
            frogdb_cluster_runtime::run_slot_migration_event_dispatcher(
                cluster_state,
                migration_rx,
                shard_senders,
                num_shards,
            )
            .await
        });
    }

    /// Spawn the background task that turns replicated slot-handoff decisions
    /// into a local write barrier plus a shard drain, on whichever node is the
    /// migration source.
    ///
    /// The drain confirmation goes back through Raft (`ConfirmSlotHandoffDrained`)
    /// rather than to the finalizer directly: `CLUSTER SETSLOT … NODE` may be
    /// issued to any node, so the finalizer is frequently neither the source nor
    /// the leader, and a replicated entry is the only ack every one of them can
    /// see.
    pub fn spawn_handoff_barrier(
        self: &Arc<Self>,
        handoff_rx: UnboundedReceiver<SlotHandoffEvent>,
        client_registry: Arc<ClientRegistry>,
        shard_senders: Arc<Vec<ShardSender>>,
        num_shards: usize,
    ) {
        let cluster_state = self.cluster_state.clone();
        let coordinator = self.clone();
        spawn(async move {
            frogdb_cluster_runtime::run_slot_handoff_barrier(
                cluster_state,
                client_registry,
                handoff_rx,
                shard_senders,
                num_shards,
                move |slot, seq| {
                    let coordinator = coordinator.clone();
                    async move {
                        let resp = coordinator
                            .commit(ClusterCommand::ConfirmSlotHandoffDrained { slot, seq })
                            .await;
                        if let Response::Error(msg) = resp {
                            // Losing the ack is not a correctness problem: the
                            // finalizer's budget lapses and it aborts, which
                            // lifts the barrier and keeps the migration intact.
                            tracing::warn!(
                                slot,
                                seq,
                                error = %String::from_utf8_lossy(&msg),
                                "Slot handoff drain confirmation was rejected"
                            );
                        }
                    }
                },
            )
            .await
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
    ///
    /// Two-phase (rework issue 02). A single `CompleteSlotMigration` moves
    /// ownership with no bound on the source's in-flight writes; under load that
    /// stranded acknowledged writes in 118 of 120 measured finalizations. So:
    ///
    /// 1. Propose `PrepareSlotHandoff`. Applying it tells the source to fence
    ///    the slot's writes and drain its shard.
    /// 2. Wait — by polling this node's own replicated state, which sees the
    ///    source's `ConfirmSlotHandoffDrained` entry whether or not this node is
    ///    the leader or the source — for the drain, up to
    ///    [`HANDOFF_DRAIN_WAIT_MS`].
    /// 3. Propose `CompleteSlotMigration`, which the state machine admits only
    ///    while the barrier is still up.
    ///
    /// On a drain that never arrives, propose `AbortSlotHandoff` and answer
    /// `TRYAGAIN`: the barrier comes down, the migration record survives
    /// untouched, and the operator re-issues `CLUSTER SETSLOT … NODE`. Dropping
    /// the barrier and completing anyway would reintroduce the exposure this
    /// whole path exists to close.
    pub async fn complete(&self, slot: u16, source_node: NodeId, target_node: NodeId) -> Response {
        let prepared_at_ms = handoff_now_ms();
        let prepare = self
            .commit(ClusterCommand::PrepareSlotHandoff {
                slot,
                source_node,
                target_node,
                barrier_ms: HANDOFF_BARRIER_MS,
                lease_ms: HANDOFF_LEASE_MS,
                proposed_at_ms: prepared_at_ms,
            })
            .await;
        if matches!(prepare, Response::Error(_)) {
            return prepare;
        }

        // Identify *our* attempt by the timestamp we minted: the seq is assigned
        // during apply, and a forwarded proposal never sees the response, so the
        // proposer timestamp is the only handle we hold on our own prepare.
        let Some(seq) = self.await_prepared_seq(slot, prepared_at_ms).await else {
            return Response::error(format!(
                "TRYAGAIN slot {} handoff not ready: prepare did not become visible",
                slot
            ));
        };

        if !self.await_drained(slot, seq).await {
            // Best-effort: if the abort itself fails the lease expires the
            // prepared record anyway, so the slot cannot wedge.
            let _ = self
                .commit(ClusterCommand::AbortSlotHandoff { slot, seq })
                .await;
            return Response::error(format!(
                "TRYAGAIN slot {} handoff not ready: source did not drain in {}ms",
                slot, HANDOFF_DRAIN_WAIT_MS
            ));
        }

        self.commit(ClusterCommand::CompleteSlotMigration {
            slot,
            source_node,
            target_node,
            proposed_at_ms: handoff_now_ms(),
        })
        .await
    }

    /// Poll local replicated state until our own prepare is visible, returning
    /// its attempt `seq`.
    async fn await_prepared_seq(&self, slot: u16, prepared_at_ms: u64) -> Option<u64> {
        self.poll_handoff(slot, |h| {
            (h.prepared_at_ms == prepared_at_ms).then_some(h.seq)
        })
        .await
    }

    /// Poll local replicated state until attempt `seq` reports drained.
    async fn await_drained(&self, slot: u16, seq: u64) -> bool {
        self.poll_handoff(slot, |h| (h.seq == seq && h.drained).then_some(()))
            .await
            .is_some()
    }

    /// Poll `slot`'s prepared handoff every [`HANDOFF_POLL_INTERVAL_MS`] until
    /// `want` yields a value or the drain budget runs out.
    ///
    /// Polling replicated state rather than awaiting a reply is what keeps the
    /// drain visible when the finalizer is neither the source nor the leader:
    /// `CLUSTER SETSLOT` may be issued to any node, and a Raft entry applies on
    /// all of them.
    async fn poll_handoff<T>(
        &self,
        slot: u16,
        want: impl Fn(&frogdb_cluster::types::SlotHandoff) -> Option<T>,
    ) -> Option<T> {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(HANDOFF_DRAIN_WAIT_MS);
        loop {
            if let Some(found) = self
                .cluster_state
                .get_slot_migration(slot)
                .and_then(|m| m.handoff.as_ref().and_then(&want))
            {
                return Some(found);
            }
            if tokio::time::Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(HANDOFF_POLL_INTERVAL_MS)).await;
        }
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
                    // A losing handoff attempt left the cluster exactly as it
                    // found it, so the caller may simply re-issue the command;
                    // `TRYAGAIN` is the Redis-cluster signal for that.
                    let prefix = if msg.is_retryable() {
                        "TRYAGAIN"
                    } else {
                        "ERR"
                    };
                    return Response::error(format!("{} {}", prefix, msg));
                }
                Response::ok()
            }
            Ok(Proposed::Forwarded) => Response::ok(),
            Err(ProposeError::Redirect(r)) => redirect_to_response(r),
            Err(ProposeError::Raft(e)) => Response::error(format!("ERR Raft error: {}", e)),
        }
    }
}
