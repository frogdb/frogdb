//! Unit tests for the slot migration coordinator.
//!
//! These cover the pure routing logic in [`super::routing::route_with_snapshot`]
//! exhaustively. Lifecycle methods that go through Raft (`begin`, `complete`,
//! `cancel`) and the event dispatcher are exercised by the integration suite
//! (`tests/cluster_migration.rs::test_*_slot_migration*`,
//! `cluster/src/state.rs::test_migration_complete_event_fires`).

use std::net::SocketAddr;

use frogdb_cluster::types::{ClusterSnapshot, NodeInfo, SlotMigration};

use super::routing::{
    BatchKeys, BatchRoute, RouteDecision, RouteOutcome, route_migrating_source, route_queued_batch,
    route_watched_keys, route_with_snapshot, watch_slot_is_locally_served,
};

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
    SlotMigration::new(SLOT, source, target)
}

// FM-CLUSTER-021
#[test]
fn route_local_serve_when_self_owns_no_migration() {
    let mut snap = empty_snapshot();
    snap.slot_assignment.insert(SLOT, SELF_NODE);

    let decision = route_with_snapshot(&snap, SLOT, "GET", false, SELF_NODE);
    assert_eq!(decision, RouteDecision::LocalServe);
}

// FM-CLUSTER-028
#[test]
fn route_local_serve_migrating_when_self_is_source() {
    let mut snap = empty_snapshot();
    snap.slot_assignment.insert(SLOT, SELF_NODE);
    snap.migrations
        .insert(SLOT, migration(SELF_NODE, OTHER_NODE));

    let decision = route_with_snapshot(&snap, SLOT, "GET", false, SELF_NODE);
    assert_eq!(decision, RouteDecision::LocalServeMigrating);
}

// FM-CLUSTER-022
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

// FM-CLUSTER-023
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

// FM-CLUSTER-026
#[test]
fn route_accept_importing_when_self_is_target_and_asking() {
    let mut snap = empty_snapshot();
    snap.slot_assignment.insert(SLOT, OTHER_NODE);
    snap.migrations
        .insert(SLOT, migration(OTHER_NODE, SELF_NODE));

    let decision = route_with_snapshot(&snap, SLOT, "GET", true, SELF_NODE);
    assert_eq!(decision, RouteDecision::AcceptImporting);
}

// FM-CLUSTER-026
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

// FM-CLUSTER-027
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

// FM-CLUSTER-024
#[test]
fn route_unassigned_when_no_owner_no_migration() {
    let snap = empty_snapshot();

    let decision = route_with_snapshot(&snap, SLOT, "GET", false, SELF_NODE);
    assert_eq!(decision, RouteDecision::Unassigned { slot: SLOT });
}

// FM-CLUSTER-026
#[test]
fn route_accept_importing_when_unassigned_self_target_with_asking() {
    let mut snap = empty_snapshot();
    snap.migrations
        .insert(SLOT, migration(OTHER_NODE, SELF_NODE));

    // No slot owner yet, but we're the importing target with ASKING set.
    let decision = route_with_snapshot(&snap, SLOT, "GET", true, SELF_NODE);
    assert_eq!(decision, RouteDecision::AcceptImporting);
}

// FM-CLUSTER-026
#[test]
fn route_unassigned_when_self_is_target_but_no_asking_or_restore() {
    let mut snap = empty_snapshot();
    snap.migrations
        .insert(SLOT, migration(OTHER_NODE, SELF_NODE));

    let decision = route_with_snapshot(&snap, SLOT, "GET", false, SELF_NODE);
    assert_eq!(decision, RouteDecision::Unassigned { slot: SLOT });
}

// FM-CLUSTER-028
/// The per-command probe instruction on the migrating source: owning the slot
/// with a migration open means "decide by key presence", not "serve it".
///
/// `ASKING` is not an input — the probe runs after `ClusterSlotValidation` has
/// consumed the one-shot flag, so the verdict must be the same either way, for
/// any command name.
#[test]
fn migrating_source_probe_names_the_importing_node() {
    let mut snap = empty_snapshot();
    snap.slot_assignment.insert(SLOT, SELF_NODE);
    snap.migrations
        .insert(SLOT, migration(SELF_NODE, OTHER_NODE));

    assert_eq!(
        route_migrating_source(&snap, SLOT, SELF_NODE),
        Some(test_addr(6380)),
        "the source must probe, with the importing node as the ASK target"
    );
}

// FM-CLUSTER-028
/// Nothing to probe when the slot is not migrating away from this node: a slot
/// we own outright, a slot another node owns, and a slot with no owner at all
/// are the ordinary `LocalServe` / `Moved` / `Unassigned` verdicts, already
/// settled by slot validation. This is the gate that keeps the probe — and its
/// keyspace round-trip — off every command outside a migration window.
#[test]
fn migrating_source_probe_is_none_without_an_open_migration() {
    let mut owned = empty_snapshot();
    owned.slot_assignment.insert(SLOT, SELF_NODE);
    assert_eq!(route_migrating_source(&owned, SLOT, SELF_NODE), None);

    let mut foreign = empty_snapshot();
    foreign.slot_assignment.insert(SLOT, OTHER_NODE);
    foreign
        .migrations
        .insert(SLOT, migration(OTHER_NODE, SELF_NODE));
    assert_eq!(
        route_migrating_source(&foreign, SLOT, SELF_NODE),
        None,
        "the importing target never ASKs back at the source"
    );

    let unassigned = empty_snapshot();
    assert_eq!(route_migrating_source(&unassigned, SLOT, SELF_NODE), None);
}

// FM-CLUSTER-028
/// An importing node missing from the local node table renders no ASK address,
/// so the source serves locally rather than emitting a redirect it cannot
/// address — the same fallback `route_queued_batch` takes.
#[test]
fn migrating_source_probe_is_none_when_the_ask_address_is_unknown() {
    let mut snap = empty_snapshot();
    snap.slot_assignment.insert(SLOT, SELF_NODE);
    snap.migrations
        .insert(SLOT, migration(SELF_NODE, OTHER_NODE));
    snap.nodes.remove(&OTHER_NODE);

    assert_eq!(route_migrating_source(&snap, SLOT, SELF_NODE), None);
}

// FM-CLUSTER-028
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

// FM-CLUSTER-021
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

// FM-CLUSTER-022
#[test]
fn to_response_moved_with_addr_emits_moved() {
    let decision = moved_decision(Some(test_addr(6380)));
    assert_eq!(
        reply_text(decision.to_response(false)),
        format!("MOVED {} 127.0.0.1:6380", SLOT)
    );
}

// FM-CLUSTER-023
#[test]
fn to_response_moved_without_addr_emits_clusterdown() {
    let decision = moved_decision(None);
    assert_eq!(
        reply_text(decision.to_response(false)),
        format!("CLUSTERDOWN Hash slot {} not served", SLOT)
    );
}

// FM-CLUSTER-025
#[test]
fn to_response_moved_readonly_eligible_serves_locally() {
    // A READONLY replica serving a read for a slot its master owns.
    let decision = moved_decision(Some(test_addr(6380)));
    assert_eq!(decision.to_response(true), RouteOutcome::ServeLocal);
}

// FM-CLUSTER-024
#[test]
fn to_response_unassigned_emits_clusterdown() {
    let decision = RouteDecision::Unassigned { slot: SLOT };
    assert_eq!(
        reply_text(decision.to_response(false)),
        format!("CLUSTERDOWN Hash slot {} not served", SLOT)
    );
}

// FM-CLUSTER-025
#[test]
fn to_response_unassigned_ignores_readonly_override() {
    // READONLY never rescues an unassigned slot — no replica relationship exists.
    let decision = RouteDecision::Unassigned { slot: SLOT };
    assert_eq!(
        reply_text(decision.to_response(true)),
        format!("CLUSTERDOWN Hash slot {} not served", SLOT)
    );
}

// FM-CLUSTER-022
#[test]
fn to_response_moved_ipv6_is_bracketed() {
    let addr: SocketAddr = "[2001:db8::1]:6379".parse().unwrap();
    let decision = moved_decision(Some(addr));
    assert_eq!(
        reply_text(decision.to_response(false)),
        format!("MOVED {} [2001:db8::1]:6379", SLOT)
    );
}

// ---------------------------------------------------------------------------
// route_queued_batch — the EXEC-time whole-transaction decision.
//
// The batch decision is what closes the MULTI/EXEC slot-migration window: a
// transaction validated at queue time is re-routed as a unit against one
// snapshot before any command runs. See
// `.scratch/replication-cluster-rework/exec-slot-revalidation.md`.
// ---------------------------------------------------------------------------

/// Two keys sharing a hash tag, so the batch lands in exactly one slot; the
/// slot is derived (not hard-coded) so the test cannot drift from the hasher.
fn tagged_batch() -> (BatchKeys, u16) {
    let mut batch = BatchKeys::default();
    batch.add_key(b"{tx}:a");
    batch.add_key(b"{tx}:b");
    let slot = frogdb_core::slot_for_key(b"{tx}:a");
    assert_eq!(frogdb_core::slot_for_key(b"{tx}:b"), slot);
    (batch, slot)
}

/// Snapshot in which `slot` is owned by `owner`, with no migration.
fn owned_by(slot: u16, owner: u64) -> ClusterSnapshot {
    let mut snap = empty_snapshot();
    snap.slot_assignment.insert(slot, owner);
    snap
}

fn batch_reply_text(route: BatchRoute) -> String {
    match route {
        BatchRoute::Redirect(frogdb_protocol::Response::Error(bytes)) => {
            String::from_utf8_lossy(&bytes).into_owned()
        }
        other => panic!("expected BatchRoute::Redirect(Error(_)), got {other:?}"),
    }
}

// FM-CLUSTER-019, FM-TXN-029
#[test]
fn batch_with_no_keyed_command_serves_locally() {
    // A MULTI of PING/INFO/… touches no slot. Redis's `getNodeByQuery` returns
    // `myself` when nothing resolves; redirecting here would be a regression.
    let batch = BatchKeys::default();
    let snap = empty_snapshot();
    assert_eq!(
        route_queued_batch(&snap, &batch, false, SELF_NODE, false),
        BatchRoute::ServeLocal
    );
}

// FM-CLUSTER-021, FM-CLUSTER-037
#[test]
fn batch_on_owned_stable_slot_serves_locally() {
    let (batch, slot) = tagged_batch();
    let snap = owned_by(slot, SELF_NODE);
    assert_eq!(
        route_queued_batch(&snap, &batch, false, SELF_NODE, false),
        BatchRoute::ServeLocal
    );
}

// FM-CLUSTER-020, FM-TXN-019
#[test]
fn batch_spanning_two_slots_is_crossslot() {
    // Individually valid commands whose slots diverged (one slot migrated)
    // must fail as CROSSSLOT, never commit partially.
    let mut batch = BatchKeys::default();
    batch.add_key(b"alpha");
    batch.add_key(b"{different}beta");
    assert_ne!(
        frogdb_core::slot_for_key(b"alpha"),
        frogdb_core::slot_for_key(b"{different}beta"),
        "test fixture must use two genuinely different slots"
    );
    let snap = empty_snapshot();
    assert_eq!(
        batch_reply_text(route_queued_batch(&snap, &batch, false, SELF_NODE, false)),
        super::redirect::CROSSSLOT_MSG
    );
}

// FM-CLUSTER-022, FM-CLUSTER-037, FM-TXN-022
#[test]
fn batch_on_foreign_slot_is_moved_to_the_owner() {
    let (batch, slot) = tagged_batch();
    let snap = owned_by(slot, OTHER_NODE);
    assert_eq!(
        batch_reply_text(route_queued_batch(&snap, &batch, false, SELF_NODE, false)),
        format!("MOVED {slot} 127.0.0.1:6380")
    );
}

// FM-CLUSTER-026
#[test]
fn batch_on_import_target_with_asking_probes_importing() {
    let (batch, slot) = tagged_batch();
    let mut snap = owned_by(slot, OTHER_NODE);
    snap.migrations
        .insert(slot, SlotMigration::new(slot, OTHER_NODE, SELF_NODE));
    assert_eq!(
        route_queued_batch(&snap, &batch, true, SELF_NODE, false),
        BatchRoute::ProbeImporting { slot }
    );
}

// FM-CLUSTER-026, FM-TXN-015
#[test]
fn batch_on_import_target_without_asking_is_moved() {
    // Sticky ASKING is what makes the arm above reachable at EXEC; without it
    // the batch must still be redirected to the slot's current owner.
    let (batch, slot) = tagged_batch();
    let mut snap = owned_by(slot, OTHER_NODE);
    snap.migrations
        .insert(slot, SlotMigration::new(slot, OTHER_NODE, SELF_NODE));
    assert_eq!(
        batch_reply_text(route_queued_batch(&snap, &batch, false, SELF_NODE, false)),
        format!("MOVED {slot} 127.0.0.1:6380")
    );
}

// FM-CLUSTER-028, FM-TXN-023
#[test]
fn batch_on_migrating_source_probes_with_the_ask_target() {
    let (batch, slot) = tagged_batch();
    let mut snap = owned_by(slot, SELF_NODE);
    snap.migrations
        .insert(slot, SlotMigration::new(slot, SELF_NODE, OTHER_NODE));
    assert_eq!(
        route_queued_batch(&snap, &batch, false, SELF_NODE, false),
        BatchRoute::ProbeMigratingSource {
            slot,
            target: test_addr(6380),
        }
    );
}

// FM-CLUSTER-028, FM-TXN-027
#[test]
fn batch_on_migrating_source_with_unknown_target_serves_locally() {
    // No renderable ASK address — the per-command path bails the same way
    // rather than inventing a redirect.
    let (batch, slot) = tagged_batch();
    let mut snap = owned_by(slot, SELF_NODE);
    snap.nodes.remove(&OTHER_NODE);
    snap.migrations
        .insert(slot, SlotMigration::new(slot, SELF_NODE, OTHER_NODE));
    assert_eq!(
        route_queued_batch(&snap, &batch, false, SELF_NODE, false),
        BatchRoute::ServeLocal
    );
}

// FM-CLUSTER-024, FM-TXN-025
#[test]
fn batch_on_unassigned_slot_is_clusterdown() {
    let (batch, slot) = tagged_batch();
    let snap = empty_snapshot();
    assert_eq!(
        batch_reply_text(route_queued_batch(&snap, &batch, false, SELF_NODE, false)),
        format!("CLUSTERDOWN Hash slot {slot} not served")
    );
}

// FM-CLUSTER-025, FM-TXN-028
#[test]
fn batch_readonly_eligible_serves_a_foreign_slot_locally() {
    // READONLY connection + an all-reads batch on a slot this replica's master
    // owns: served locally, exactly as the per-command path does.
    let (batch, slot) = tagged_batch();
    let snap = owned_by(slot, OTHER_NODE);
    assert_eq!(
        route_queued_batch(&snap, &batch, false, SELF_NODE, true),
        BatchRoute::ServeLocal
    );
}

// FM-CLUSTER-025, FM-TXN-028
#[test]
fn batch_readonly_ineligible_when_it_contains_a_write() {
    // The caller folds write-ness across the whole batch, so one queued write
    // makes `readonly_eligible` false and the batch is MOVED. This is the
    // difference between a replica-safe read batch and a lost write.
    let (batch, slot) = tagged_batch();
    let snap = owned_by(slot, OTHER_NODE);
    assert_eq!(
        batch_reply_text(route_queued_batch(&snap, &batch, false, SELF_NODE, false)),
        format!("MOVED {slot} 127.0.0.1:6380")
    );
}

// FM-CLUSTER-025, FM-TXN-025
#[test]
fn batch_readonly_never_rescues_an_unassigned_slot() {
    let (batch, slot) = tagged_batch();
    let snap = empty_snapshot();
    assert_eq!(
        batch_reply_text(route_queued_batch(&snap, &batch, false, SELF_NODE, true)),
        format!("CLUSTERDOWN Hash slot {slot} not served")
    );
}

// ---------------------------------------------------------------------------
// WATCH-time and EXEC-time watch-set slot validation.
//
// WATCH short-circuits at the `TransactionControl` dispatch stage, long before
// `ClusterSlotValidation` runs, so the stage gauntlet structurally cannot cover
// it — these two seams are where the verdict is taken instead. See
// `.scratch/replication-cluster-rework/issues/04`.
// ---------------------------------------------------------------------------

/// The refusal text a `route_watched_keys` verdict carries, or a panic when the
/// watch was accepted.
fn watch_reply_text(verdict: Option<frogdb_protocol::Response>) -> String {
    match verdict {
        Some(frogdb_protocol::Response::Error(bytes)) => {
            String::from_utf8_lossy(&bytes).into_owned()
        }
        other => panic!("expected a refusal Error(_), got {other:?}"),
    }
}

// FM-CLUSTER-029, FM-TXN-048
#[test]
fn watch_on_a_foreign_slot_is_refused_with_moved() {
    // Recording the watch here would hand the client a CAS no writer on the
    // real owner can ever dirty. Note the READONLY replica rescue deliberately
    // has no say: `route_watched_keys` takes no `readonly_eligible` argument,
    // because a CAS precondition can only be registered where the writes it
    // guards are applied.
    let (keys, slot) = tagged_batch();
    let snap = owned_by(slot, OTHER_NODE);
    assert_eq!(
        watch_reply_text(route_watched_keys(&snap, &keys, false, SELF_NODE)),
        format!("MOVED {slot} 127.0.0.1:6380")
    );
}

// FM-CLUSTER-020, FM-TXN-048
#[test]
fn watch_across_two_slots_is_crossslot() {
    let mut keys = BatchKeys::default();
    keys.add_key(b"alpha");
    keys.add_key(b"{different}beta");
    assert_ne!(
        frogdb_core::slot_for_key(b"alpha"),
        frogdb_core::slot_for_key(b"{different}beta"),
        "test fixture must use two genuinely different slots"
    );
    let snap = empty_snapshot();
    assert_eq!(
        watch_reply_text(route_watched_keys(&snap, &keys, false, SELF_NODE)),
        super::redirect::CROSSSLOT_MSG
    );
}

// FM-CLUSTER-029, FM-TXN-048
#[test]
fn watch_on_an_open_migration_is_accepted() {
    // An open migration does not make the watch unserviceable: MIGRATE's delete
    // on the source bumps the watched key's version, so the ordinary CAS check
    // still fires. Refusing here would break every WATCH for the duration of
    // any migration touching the slot.
    let (keys, slot) = tagged_batch();
    let mut snap = owned_by(slot, SELF_NODE);
    snap.migrations
        .insert(slot, SlotMigration::new(slot, SELF_NODE, OTHER_NODE));
    assert_eq!(route_watched_keys(&snap, &keys, false, SELF_NODE), None);
}

// FM-CLUSTER-024, FM-TXN-048
#[test]
fn watch_on_an_unassigned_slot_is_clusterdown() {
    let (keys, slot) = tagged_batch();
    let snap = empty_snapshot();
    assert_eq!(
        watch_reply_text(route_watched_keys(&snap, &keys, false, SELF_NODE)),
        format!("CLUSTERDOWN Hash slot {slot} not served")
    );
}

// FM-CLUSTER-029, FM-TXN-049
#[test]
fn watch_slot_locally_served_accepts_an_open_migration() {
    // Same reasoning as at WATCH time: the version check owns the open window.
    let mut snap = owned_by(SLOT, SELF_NODE);
    snap.migrations
        .insert(SLOT, migration(SELF_NODE, OTHER_NODE));
    assert!(watch_slot_is_locally_served(&snap, SLOT, false, SELF_NODE));

    // The importing side, reached with ASKING, is serviceable too.
    let mut importing = owned_by(SLOT, OTHER_NODE);
    importing
        .migrations
        .insert(SLOT, migration(OTHER_NODE, SELF_NODE));
    assert!(watch_slot_is_locally_served(
        &importing, SLOT, true, SELF_NODE
    ));
}

// FM-CLUSTER-029, FM-TXN-049
#[test]
fn watch_slot_locally_served_rejects_a_slot_owned_elsewhere() {
    // The slot changed hands: this node's copy of the watched key is stale and
    // its version can no longer observe the owner's writes.
    assert!(!watch_slot_is_locally_served(
        &owned_by(SLOT, OTHER_NODE),
        SLOT,
        false,
        SELF_NODE
    ));
    // Unassigned is just as unobservable.
    assert!(!watch_slot_is_locally_served(
        &empty_snapshot(),
        SLOT,
        false,
        SELF_NODE
    ));
}
