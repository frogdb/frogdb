//! Unit tests for the slot migration coordinator.
//!
//! These cover the pure routing logic in [`super::routing::route_with_snapshot`]
//! exhaustively. Lifecycle methods that go through Raft (`begin`, `complete`,
//! `cancel`) and the event dispatcher are exercised by the integration suite
//! (`tests/integration_cluster.rs::test_*_slot_migration*`,
//! `cluster/src/state.rs::test_migration_complete_event_fires`).

use std::net::SocketAddr;

use frogdb_cluster::types::{ClusterSnapshot, NodeInfo, SlotMigration};

use super::routing::{RouteDecision, RouteOutcome, route_with_snapshot};

const SELF_NODE: u64 = 1;
const OTHER_NODE: u64 = 2;
const SLOT: u16 = 42;

fn test_addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{}", port).parse().unwrap()
}

/// Build a snapshot with two nodes (`SELF_NODE` and `OTHER_NODE`) registered
/// but no slot assignments or migrations.
fn empty_snapshot() -> ClusterSnapshot {
    let mut snap = ClusterSnapshot::new();
    snap.nodes.insert(
        SELF_NODE,
        NodeInfo::new_primary(SELF_NODE, test_addr(6379), test_addr(16379)),
    );
    snap.nodes.insert(
        OTHER_NODE,
        NodeInfo::new_primary(OTHER_NODE, test_addr(6380), test_addr(16380)),
    );
    snap
}

fn migration(source: u64, target: u64) -> SlotMigration {
    SlotMigration {
        slot: SLOT,
        source_node: source,
        target_node: target,
    }
}

#[test]
fn route_local_serve_when_self_owns_no_migration() {
    let mut snap = empty_snapshot();
    snap.slot_assignment.insert(SLOT, SELF_NODE);

    let decision = route_with_snapshot(&snap, SLOT, "GET", false, SELF_NODE);
    assert_eq!(decision, RouteDecision::LocalServe);
}

#[test]
fn route_local_serve_migrating_when_self_is_source() {
    let mut snap = empty_snapshot();
    snap.slot_assignment.insert(SLOT, SELF_NODE);
    snap.migrations
        .insert(SLOT, migration(SELF_NODE, OTHER_NODE));

    let decision = route_with_snapshot(&snap, SLOT, "GET", false, SELF_NODE);
    assert_eq!(decision, RouteDecision::LocalServeMigrating);
}

#[test]
fn route_moved_when_other_node_owns_no_asking() {
    let mut snap = empty_snapshot();
    snap.slot_assignment.insert(SLOT, OTHER_NODE);

    let decision = route_with_snapshot(&snap, SLOT, "GET", false, SELF_NODE);
    assert_eq!(
        decision,
        RouteDecision::Moved {
            slot: SLOT,
            owner: OTHER_NODE,
            addr: Some(test_addr(6380)),
        }
    );
}

#[test]
fn route_moved_with_no_addr_when_owner_node_unknown() {
    let mut snap = empty_snapshot();
    snap.nodes.remove(&OTHER_NODE);
    snap.slot_assignment.insert(SLOT, OTHER_NODE);

    let decision = route_with_snapshot(&snap, SLOT, "GET", false, SELF_NODE);
    assert_eq!(
        decision,
        RouteDecision::Moved {
            slot: SLOT,
            owner: OTHER_NODE,
            addr: None,
        }
    );
}

#[test]
fn route_accept_importing_when_self_is_target_and_asking() {
    let mut snap = empty_snapshot();
    snap.slot_assignment.insert(SLOT, OTHER_NODE);
    snap.migrations
        .insert(SLOT, migration(OTHER_NODE, SELF_NODE));

    let decision = route_with_snapshot(&snap, SLOT, "GET", true, SELF_NODE);
    assert_eq!(decision, RouteDecision::AcceptImporting);
}

#[test]
fn route_moved_when_self_is_target_but_no_asking() {
    let mut snap = empty_snapshot();
    snap.slot_assignment.insert(SLOT, OTHER_NODE);
    snap.migrations
        .insert(SLOT, migration(OTHER_NODE, SELF_NODE));

    // Without ASKING, target node still gets MOVED — must redirect to source.
    let decision = route_with_snapshot(&snap, SLOT, "GET", false, SELF_NODE);
    assert_eq!(
        decision,
        RouteDecision::Moved {
            slot: SLOT,
            owner: OTHER_NODE,
            addr: Some(test_addr(6380)),
        }
    );
}

#[test]
fn route_accept_importing_for_restore_without_asking() {
    let mut snap = empty_snapshot();
    snap.slot_assignment.insert(SLOT, OTHER_NODE);
    snap.migrations
        .insert(SLOT, migration(OTHER_NODE, SELF_NODE));

    // RESTORE bypasses the ASKING gate — it's the migration data-load command.
    let decision = route_with_snapshot(&snap, SLOT, "RESTORE", false, SELF_NODE);
    assert_eq!(decision, RouteDecision::AcceptImporting);
}

#[test]
fn route_unassigned_when_no_owner_no_migration() {
    let snap = empty_snapshot();

    let decision = route_with_snapshot(&snap, SLOT, "GET", false, SELF_NODE);
    assert_eq!(decision, RouteDecision::Unassigned { slot: SLOT });
}

#[test]
fn route_accept_importing_when_unassigned_self_target_with_asking() {
    let mut snap = empty_snapshot();
    snap.migrations
        .insert(SLOT, migration(OTHER_NODE, SELF_NODE));

    // No slot owner yet, but we're the importing target with ASKING set.
    let decision = route_with_snapshot(&snap, SLOT, "GET", true, SELF_NODE);
    assert_eq!(decision, RouteDecision::AcceptImporting);
}

#[test]
fn route_unassigned_when_self_is_target_but_no_asking_or_restore() {
    let mut snap = empty_snapshot();
    snap.migrations
        .insert(SLOT, migration(OTHER_NODE, SELF_NODE));

    let decision = route_with_snapshot(&snap, SLOT, "GET", false, SELF_NODE);
    assert_eq!(decision, RouteDecision::Unassigned { slot: SLOT });
}

#[test]
fn route_moved_when_self_is_source_but_other_owns() {
    // Source node during a slot move-out: the slot is still owned by us in the
    // local snapshot until the migration completes. If a *different* node
    // becomes the owner mid-flight (atypical), the routing falls through to the
    // owner, not the migration source.
    let mut snap = empty_snapshot();
    snap.slot_assignment.insert(SLOT, OTHER_NODE);
    snap.migrations
        .insert(SLOT, migration(SELF_NODE, OTHER_NODE));

    let decision = route_with_snapshot(&snap, SLOT, "GET", false, SELF_NODE);
    assert_eq!(
        decision,
        RouteDecision::Moved {
            slot: SLOT,
            owner: OTHER_NODE,
            addr: Some(test_addr(6380)),
        }
    );
}

// ---------------------------------------------------------------------------
// RouteDecision::to_response — the decision → reply projection.
// ---------------------------------------------------------------------------

fn moved_decision(addr: Option<SocketAddr>) -> RouteDecision {
    RouteDecision::Moved {
        slot: SLOT,
        owner: OTHER_NODE,
        addr,
    }
}

fn reply_text(outcome: RouteOutcome) -> String {
    match outcome {
        RouteOutcome::Reply(frogdb_protocol::Response::Error(bytes)) => {
            String::from_utf8_lossy(&bytes).into_owned()
        }
        other => panic!("expected RouteOutcome::Reply(Error(_)), got {other:?}"),
    }
}

#[test]
fn to_response_local_arms_serve_locally() {
    for decision in [
        RouteDecision::LocalServe,
        RouteDecision::LocalServeMigrating,
        RouteDecision::AcceptImporting,
    ] {
        // readonly_eligible is irrelevant for the local arms.
        assert_eq!(decision.to_response(false), RouteOutcome::ServeLocal);
        assert_eq!(decision.to_response(true), RouteOutcome::ServeLocal);
    }
}

#[test]
fn to_response_moved_with_addr_emits_moved() {
    let decision = moved_decision(Some(test_addr(6380)));
    assert_eq!(
        reply_text(decision.to_response(false)),
        format!("MOVED {} 127.0.0.1:6380", SLOT)
    );
}

#[test]
fn to_response_moved_without_addr_emits_clusterdown() {
    let decision = moved_decision(None);
    assert_eq!(
        reply_text(decision.to_response(false)),
        format!("CLUSTERDOWN Hash slot {} not served", SLOT)
    );
}

#[test]
fn to_response_moved_readonly_eligible_serves_locally() {
    // A READONLY replica serving a read for a slot its master owns.
    let decision = moved_decision(Some(test_addr(6380)));
    assert_eq!(decision.to_response(true), RouteOutcome::ServeLocal);
}

#[test]
fn to_response_unassigned_emits_clusterdown() {
    let decision = RouteDecision::Unassigned { slot: SLOT };
    assert_eq!(
        reply_text(decision.to_response(false)),
        format!("CLUSTERDOWN Hash slot {} not served", SLOT)
    );
}

#[test]
fn to_response_unassigned_ignores_readonly_override() {
    // READONLY never rescues an unassigned slot — no replica relationship exists.
    let decision = RouteDecision::Unassigned { slot: SLOT };
    assert_eq!(
        reply_text(decision.to_response(true)),
        format!("CLUSTERDOWN Hash slot {} not served", SLOT)
    );
}

#[test]
fn to_response_moved_ipv6_is_bracketed() {
    let addr: SocketAddr = "[2001:db8::1]:6379".parse().unwrap();
    let decision = moved_decision(Some(addr));
    assert_eq!(
        reply_text(decision.to_response(false)),
        format!("MOVED {} [2001:db8::1]:6379", SLOT)
    );
}
