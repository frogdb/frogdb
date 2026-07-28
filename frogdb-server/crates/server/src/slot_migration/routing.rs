//! Slot routing decisions during cluster mode and slot migrations.
//!
//! This module concentrates the slot-ownership decision logic — what to do
//! with a command targeting a particular slot given the current ASKING flag,
//! READONLY mode, and migration state. It is the single source of truth for
//! `MOVED` / `ASK` / `CLUSTERDOWN` routing in cluster mode.

use bytes::Bytes;
use frogdb_cluster::types::ClusterSnapshot;
use frogdb_core::{NodeId, slot_for_key};
use frogdb_protocol::Response;
use std::collections::{BTreeSet, HashSet};
use std::net::SocketAddr;

use super::SlotMigrationCoordinator;
use super::redirect;

/// The result of routing a command targeting a particular slot.
///
/// The connection layer translates this into either local execution or a
/// redirect/error response sent back to the client. `clear_asking` indicates
/// whether the caller should reset its `ASKING` flag after applying the
/// decision (true in every case except a clean local-serve with no migration).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteDecision {
    /// We own the slot and there is no migration in progress.
    /// Serve the command locally; do not touch ASKING.
    LocalServe,

    /// We own the slot and a migration is active (we are the source / MIGRATING).
    /// Serve the command locally; clear ASKING. The dispatch layer may convert
    /// nil responses into ASK redirects via [`migrating_ask_for_nil`].
    LocalServeMigrating,

    /// Another node owns the slot, but we are the importing target and the
    /// caller has set ASKING (or the command is RESTORE). Serve locally; clear
    /// ASKING.
    AcceptImporting,

    /// Another node owns the slot. Caller should clear ASKING and either
    /// return `MOVED <slot> <addr>` or — when the connection is in READONLY
    /// mode and the command is read-only — serve locally.
    ///
    /// `addr` is `None` when the owner's node info is missing from the local
    /// view; in that case the caller emits `CLUSTERDOWN` instead of `MOVED`,
    /// but the READONLY override still applies (matches existing behavior).
    Moved {
        slot: u16,
        owner: NodeId,
        addr: Option<SocketAddr>,
    },

    /// The slot has no owner. Caller should clear ASKING and return
    /// `CLUSTERDOWN`. The READONLY override does NOT apply here (no replica
    /// relationship can serve an unassigned slot).
    Unassigned { slot: u16 },
}

/// What the connection layer should do with a [`RouteDecision`] after projecting
/// it onto a client reply.
///
/// Distinguishing the two outcomes keeps "serve locally" and "no decision to
/// make" from collapsing into the same `None` the way a bare `Option<Response>`
/// would.
#[derive(Debug, Clone, PartialEq)]
pub enum RouteOutcome {
    /// Execute the command locally (we own the slot, are the importing target,
    /// or are a READONLY replica serving a read).
    ServeLocal,
    /// Send this redirect/error reply to the client instead of executing.
    Reply(Response),
}

impl RouteDecision {
    /// Project a routing decision onto a client reply.
    ///
    /// `readonly_eligible` is the connection-level policy the decision itself
    /// cannot see: `true` iff the connection is in READONLY mode AND the command
    /// is flagged `READONLY`. It only rescues the [`Moved`](RouteDecision::Moved)
    /// arm (a replica can serve a read for a slot its master owns); it never
    /// rescues [`Unassigned`](RouteDecision::Unassigned) (no replica
    /// relationship exists for an unowned slot).
    pub fn to_response(&self, readonly_eligible: bool) -> RouteOutcome {
        match self {
            RouteDecision::LocalServe
            | RouteDecision::LocalServeMigrating
            | RouteDecision::AcceptImporting => RouteOutcome::ServeLocal,

            RouteDecision::Moved { slot, addr, .. } => {
                if readonly_eligible {
                    return RouteOutcome::ServeLocal;
                }
                match addr {
                    Some(a) => RouteOutcome::Reply(redirect::moved(*slot, *a)),
                    None => RouteOutcome::Reply(redirect::clusterdown_slot(*slot)),
                }
            }
            RouteDecision::Unassigned { slot } => {
                RouteOutcome::Reply(redirect::clusterdown_slot(*slot))
            }
        }
    }
}

impl SlotMigrationCoordinator {
    /// Decide how to route a command targeting `slot`, given the connection's
    /// current ASKING flag and the local node's identity.
    ///
    /// `command_name` should be the uppercase command name; it is consulted
    /// only to honor the RESTORE special case (RESTORE is allowed on the
    /// importing target without ASKING).
    pub fn route(
        &self,
        slot: u16,
        command_name: &str,
        asking: bool,
        self_node_id: NodeId,
    ) -> RouteDecision {
        route_with_snapshot(
            &self.cluster_state.snapshot(),
            slot,
            command_name,
            asking,
            self_node_id,
        )
    }
}

/// Pure routing logic against a [`ClusterSnapshot`]. Extracted from
/// [`SlotMigrationCoordinator::route`] so it can be exercised in unit tests
/// without constructing a full coordinator (which requires a live Raft
/// instance).
pub(super) fn route_with_snapshot(
    snapshot: &ClusterSnapshot,
    slot: u16,
    command_name: &str,
    asking: bool,
    self_node_id: NodeId,
) -> RouteDecision {
    match snapshot.slot_assignment.get(&slot) {
        Some(&owner) if owner == self_node_id => {
            if snapshot.migrations.contains_key(&slot) {
                RouteDecision::LocalServeMigrating
            } else {
                RouteDecision::LocalServe
            }
        }
        Some(&owner) => {
            if let Some(migration) = snapshot.migrations.get(&slot)
                && migration.target_node == self_node_id
                && (asking || command_name == "RESTORE")
            {
                return RouteDecision::AcceptImporting;
            }
            let addr = snapshot.nodes.get(&owner).map(|n| n.addr);
            RouteDecision::Moved { slot, owner, addr }
        }
        None => {
            if let Some(migration) = snapshot.migrations.get(&slot)
                && migration.target_node == self_node_id
                && (asking || command_name == "RESTORE")
            {
                return RouteDecision::AcceptImporting;
            }
            RouteDecision::Unassigned { slot }
        }
    }
}

// ---------------------------------------------------------------------------
// Whole-batch (MULTI/EXEC) routing
// ---------------------------------------------------------------------------

/// The keyed footprint of a queued MULTI batch: every distinct slot its
/// commands touch, plus the union of their keys in queue order.
///
/// Folded once by the connection layer and routed as a unit, because a
/// per-command re-validation at EXEC would take N snapshots and could reach an
/// internally inconsistent verdict — and, worse, could only abort *mid*-batch,
/// which propagates a partial transaction to replicas.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct BatchKeys {
    slots: BTreeSet<u16>,
    keys: Vec<Bytes>,
    seen: HashSet<Bytes>,
}

impl BatchKeys {
    /// Fold one key into the footprint. Repeats are dropped: the same key
    /// queued by five commands is one key to probe, and the presence probe
    /// costs per-key work on the shard. The `seen` set keeps the fold linear in
    /// the queue length rather than quadratic.
    pub(crate) fn add_key(&mut self, key: &[u8]) {
        self.slots.insert(slot_for_key(key));
        let key = Bytes::copy_from_slice(key);
        if self.seen.insert(key.clone()) {
            self.keys.push(key);
        }
    }

    /// The union of the batch's keys, deduplicated, in first-seen queue order.
    pub(crate) fn keys(&self) -> &[Bytes] {
        &self.keys
    }

    /// The single slot the batch touches, or `None` when it touches none or
    /// more than one.
    fn single_slot(&self) -> Option<u16> {
        match self.slots.len() {
            1 => self.slots.first().copied(),
            _ => None,
        }
    }

    /// Whether the batch touches no keyed, slot-routed command at all.
    fn is_keyless(&self) -> bool {
        self.slots.is_empty()
    }
}

/// What EXEC should do with a queued batch, decided against one cluster
/// snapshot.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum BatchRoute {
    /// Run the batch on this node.
    ServeLocal,
    /// Answer EXEC with this bare redirect/error and drop the batch.
    Redirect(Response),
    /// We own the batch's slot but it is `MIGRATING` away. The caller probes
    /// key presence: all present → serve, all absent → `ASK target`, split →
    /// `TRYAGAIN`.
    ProbeMigratingSource {
        /// The batch's slot.
        slot: u16,
        /// The importing node, for the `ASK` redirect.
        target: SocketAddr,
    },
    /// Another node owns the batch's slot, we are its importing target, and
    /// `ASKING` is set. The caller probes key presence: a multi-key batch with
    /// anything still missing → `TRYAGAIN`, otherwise serve locally. Never
    /// `ASK` (that would bounce the client back to the source).
    ProbeImporting {
        /// The batch's slot.
        slot: u16,
    },
}

/// Route a whole queued MULTI batch against one [`ClusterSnapshot`].
///
/// Redis performs the identical decision at EXEC by re-running
/// `getNodeByQuery` over `c->mstate` (`cluster.c`), replying with the bare
/// redirect and discarding the queue (`server.c`). The rules, in order:
///
/// - no keyed command at all → [`BatchRoute::ServeLocal`] (a MULTI of
///   `PING`/`INFO`/… is never redirected; Redis returns `myself` when no slot
///   resolves)
/// - keys spanning more than one slot → `CROSSSLOT`
/// - otherwise the single slot goes through [`route_with_snapshot`], with the
///   two migration arms deferred to the caller's presence probe
pub(crate) fn route_queued_batch(
    snapshot: &ClusterSnapshot,
    batch: &BatchKeys,
    asking: bool,
    self_node_id: NodeId,
    readonly_eligible: bool,
) -> BatchRoute {
    if batch.is_keyless() {
        return BatchRoute::ServeLocal;
    }
    let Some(slot) = batch.single_slot() else {
        return BatchRoute::Redirect(redirect::crossslot());
    };

    // "EXEC" is the command name the routing seam sees; it only matters for the
    // RESTORE special case, which a transaction never takes.
    match route_with_snapshot(snapshot, slot, "EXEC", asking, self_node_id) {
        RouteDecision::LocalServe => BatchRoute::ServeLocal,
        RouteDecision::LocalServeMigrating => match migration_target_addr(snapshot, slot) {
            Some(target) => BatchRoute::ProbeMigratingSource { slot, target },
            // The importing node is not in our node table, so no ASK address
            // can be rendered. Serving locally is what the per-command path
            // does in the same situation (`check_migrating_multikey` bails).
            None => BatchRoute::ServeLocal,
        },
        RouteDecision::AcceptImporting => BatchRoute::ProbeImporting { slot },
        decision @ (RouteDecision::Moved { .. } | RouteDecision::Unassigned { .. }) => {
            match decision.to_response(readonly_eligible) {
                RouteOutcome::ServeLocal => BatchRoute::ServeLocal,
                RouteOutcome::Reply(reply) => BatchRoute::Redirect(reply),
            }
        }
    }
}

/// The address of the node a slot is migrating *to*, if the snapshot knows it.
fn migration_target_addr(snapshot: &ClusterSnapshot, slot: u16) -> Option<SocketAddr> {
    let migration = snapshot.migrations.get(&slot)?;
    Some(snapshot.nodes.get(&migration.target_node)?.addr)
}
