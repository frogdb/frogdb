//! Slot routing decisions during cluster mode and slot migrations.
//!
//! This module concentrates the slot-ownership decision logic — what to do
//! with a command targeting a particular slot given the current ASKING flag,
//! READONLY mode, and migration state. It is the single source of truth for
//! `MOVED` / `ASK` / `CLUSTERDOWN` routing in cluster mode.

use frogdb_cluster::types::ClusterSnapshot;
use frogdb_core::NodeId;
use frogdb_protocol::Response;
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
