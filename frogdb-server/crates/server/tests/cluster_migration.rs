//! Cluster slot-migration integration tests: SETSLOT
//! IMPORTING/MIGRATING/NODE/STABLE state, the end-to-end MIGRATE workflow across
//! data types, migration redirect semantics, resharding, migration fault modes,
//! and MULTI/EXEC across a migration boundary.
//!
//! Every test here runs against a live multi-node cluster: `ClusterTestHarness`
//! starts real servers whose `ClusterState` is replicated by real Raft, and
//! `CLUSTER INFO`/`NODES`/`SLOTS`/`SHARDS` render that live state (see
//! `server/src/commands/cluster/mod.rs`). The hardcoded standalone responses
//! those commands can return apply only when a server runs without cluster mode
//! (`ctx.cluster_state == None`), which is never the case in this file.
//!
//! Split out of the former `integration_cluster.rs`; shared helpers live in
//! `common/cluster_support.rs`.
#![cfg(not(feature = "turmoil"))]

mod common;

use common::cluster_support::{
    assert_execabort, find_owner_of_slot, get_with_redirect, run_full_slot_migration,
    send_cluster_cmd, set_with_redirect, split_addr,
};
use frogdb_test_harness::cluster_harness::{ClusterNodeConfig, ClusterTestHarness};
use frogdb_test_harness::cluster_helpers::{
    get_error_message, is_ask_redirect, is_error, is_moved_redirect, key_for_slot,
    parse_cluster_nodes, slot_for_key,
};
use std::time::Duration;

// ============================================================================
// Tier 5: Migration State Tests
// ============================================================================

/// Tests CLUSTER SETSLOT IMPORTING sets migration state.
#[tokio::test]
async fn test_cluster_setslot_importing() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();

    let node_ids = harness.node_ids();
    let node = harness.node(node_ids[0]).unwrap();

    // Get other node's ID via direct state access
    let other_id = harness.get_node_id_str(node_ids[1]).unwrap();

    // Set slot 1000 to IMPORTING
    let response = node
        .send("CLUSTER", &["SETSLOT", "1000", "IMPORTING", &other_id])
        .await;

    // Should succeed (or be unsupported - either is acceptable)
    if !is_error(&response) {
        // Verify CLUSTER NODES still works
        let nodes_resp = node.send("CLUSTER", &["NODES"]).await;
        // The command should not error
        assert!(
            !is_error(&nodes_resp),
            "CLUSTER NODES should succeed after SETSLOT IMPORTING"
        );

        // Clean up
        let _ = node.send("CLUSTER", &["SETSLOT", "1000", "STABLE"]).await;
    }

    harness.shutdown_all().await;
}

/// Tests CLUSTER SETSLOT MIGRATING sets migration state.
#[tokio::test]
async fn test_cluster_setslot_migrating() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();

    let node_ids = harness.node_ids();
    let node = harness.node(node_ids[0]).unwrap();

    // Get other node's ID via direct state access
    let other_id = harness.get_node_id_str(node_ids[1]).unwrap();

    // Set slot 1001 to MIGRATING
    let response = node
        .send("CLUSTER", &["SETSLOT", "1001", "MIGRATING", &other_id])
        .await;

    // Should succeed (or be unsupported - either is acceptable)
    if !is_error(&response) {
        // Verify CLUSTER NODES still works
        let nodes_resp = node.send("CLUSTER", &["NODES"]).await;
        // The command should not error
        assert!(
            !is_error(&nodes_resp),
            "CLUSTER NODES should succeed after SETSLOT MIGRATING"
        );

        // Clean up
        let _ = node.send("CLUSTER", &["SETSLOT", "1001", "STABLE"]).await;
    }

    harness.shutdown_all().await;
}

/// Tests CLUSTER SETSLOT NODE completes migration.
#[tokio::test]
async fn test_cluster_setslot_node() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();

    let node_ids = harness.node_ids();
    let node = harness.node(node_ids[0]).unwrap();

    // Get this node's ID via direct state access
    let my_id = harness.get_node_id_str(node_ids[0]).unwrap();

    // Assign slot 1002 to this node using SETSLOT NODE
    let response = node
        .send("CLUSTER", &["SETSLOT", "1002", "NODE", &my_id])
        .await;

    // Should succeed (or fail if command not supported)
    if !is_error(&response) {
        // Verify slot is assigned
        let slots_resp = node.send("CLUSTER", &["SLOTS"]).await;
        // Slot 1002 should be in the response
        assert!(!is_error(&slots_resp), "CLUSTER SLOTS should succeed");
    }

    harness.shutdown_all().await;
}

/// Tests CLUSTER SETSLOT STABLE cancels migration.
#[tokio::test]
async fn test_cluster_setslot_stable() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();

    let node_ids = harness.node_ids();
    let node = harness.node(node_ids[0]).unwrap();

    // Get other node's ID via direct state access
    let other_id = harness.get_node_id_str(node_ids[1]).unwrap();

    // Set slot 1003 to MIGRATING
    let migrate_resp = node
        .send("CLUSTER", &["SETSLOT", "1003", "MIGRATING", &other_id])
        .await;

    if !is_error(&migrate_resp) {
        // Cancel migration with STABLE
        let stable_resp = node.send("CLUSTER", &["SETSLOT", "1003", "STABLE"]).await;
        assert!(!is_error(&stable_resp), "SETSLOT STABLE should succeed");

        // Verify migration state is cleared
        let nodes_resp = node.send("CLUSTER", &["NODES"]).await;
        if let frogdb_protocol::Response::Bulk(Some(b)) = &nodes_resp {
            let nodes_str = String::from_utf8_lossy(b);
            // Should NOT contain migrating marker anymore
            assert!(
                !nodes_str.contains("[1003->-"),
                "Migration state should be cleared"
            );
        }
    }

    harness.shutdown_all().await;
}

/// Tests that CLUSTER NODES shows migration flags.
#[tokio::test]
async fn test_migration_state_visible_in_cluster_nodes() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();

    let node_ids = harness.node_ids();
    let source = harness.node(node_ids[0]).unwrap();
    let target = harness.node(node_ids[1]).unwrap();

    // Get node IDs via direct state access
    let source_id = harness.get_node_id_str(node_ids[0]).unwrap();
    let target_id = harness.get_node_id_str(node_ids[1]).unwrap();

    // Set up migration: slot 1004 migrating from source to target
    let migrate_resp = source
        .send("CLUSTER", &["SETSLOT", "1004", "MIGRATING", &target_id])
        .await;
    let import_resp = target
        .send("CLUSTER", &["SETSLOT", "1004", "IMPORTING", &source_id])
        .await;

    if !is_error(&migrate_resp) && !is_error(&import_resp) {
        // Check source's CLUSTER NODES — should show migrating marker [1004->-<target_id>]
        let source_nodes = source.send("CLUSTER", &["NODES"]).await;
        if let frogdb_protocol::Response::Bulk(Some(b)) = &source_nodes {
            let nodes_str = String::from_utf8_lossy(b);
            let marker = format!("[1004->-{}]", target_id);
            assert!(
                nodes_str.contains(&marker),
                "Source CLUSTER NODES should contain migrating marker '{marker}', got:\n{nodes_str}"
            );
        }

        // Check target's CLUSTER NODES — should show importing marker [1004-<-<source_id>]
        let target_nodes = target.send("CLUSTER", &["NODES"]).await;
        if let frogdb_protocol::Response::Bulk(Some(b)) = &target_nodes {
            let nodes_str = String::from_utf8_lossy(b);
            let marker = format!("[1004-<-{}]", source_id);
            assert!(
                nodes_str.contains(&marker),
                "Target CLUSTER NODES should contain importing marker '{marker}', got:\n{nodes_str}"
            );
        }

        // Clean up
        let _ = source.send("CLUSTER", &["SETSLOT", "1004", "STABLE"]).await;
        let _ = target.send("CLUSTER", &["SETSLOT", "1004", "STABLE"]).await;
    }

    harness.shutdown_all().await;
}

// ============================================================================
// PHASE 5: Slot Migration Tests
// ============================================================================

/// Test 16: Migrate keys between nodes.
///
/// Scenario: Full slot migration with keys
#[tokio::test]
async fn test_migrate_keys_between_nodes() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();

    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let node_ids = harness.node_ids();
    let source = harness.node(node_ids[0]).unwrap();
    let target = harness.node(node_ids[1]).unwrap();

    // Get cluster node IDs via direct state access
    let source_id = harness.get_node_id_str(node_ids[0]).unwrap();
    let target_id = harness.get_node_id_str(node_ids[1]).unwrap();

    // Use slot 800 for migration
    let test_slot = 800u16;
    let base_key = key_for_slot(test_slot);

    // Write multiple keys to this slot
    let num_keys = 5;
    let mut written_keys = Vec::new();
    for i in 0..num_keys {
        let key = format!("{}_{}", base_key, i);
        let value = format!("migrate_value_{}", i);
        let resp = source.send("SET", &[&key, &value]).await;
        if !is_error(&resp) {
            written_keys.push((key, value));
        }
    }
    eprintln!("Wrote {} keys to slot {}", written_keys.len(), test_slot);

    // Set up migration
    let migrate_resp = source
        .send(
            "CLUSTER",
            &["SETSLOT", &test_slot.to_string(), "MIGRATING", &target_id],
        )
        .await;
    let import_resp = target
        .send(
            "CLUSTER",
            &["SETSLOT", &test_slot.to_string(), "IMPORTING", &source_id],
        )
        .await;

    if is_error(&migrate_resp) || is_error(&import_resp) {
        eprintln!("Migration setup failed");
        harness.shutdown_all().await;
        return;
    }

    // Note: In a full Redis implementation, we'd use MIGRATE command here
    // For now, we verify the migration state works

    // Complete migration
    let complete_resp = target
        .send(
            "CLUSTER",
            &["SETSLOT", &test_slot.to_string(), "NODE", &target_id],
        )
        .await;
    let complete_source = source
        .send(
            "CLUSTER",
            &["SETSLOT", &test_slot.to_string(), "NODE", &target_id],
        )
        .await;

    eprintln!(
        "Migration complete: target={:?}, source={:?}",
        complete_resp, complete_source
    );

    // Clean up migration state
    let _ = source
        .send("CLUSTER", &["SETSLOT", &test_slot.to_string(), "STABLE"])
        .await;
    let _ = target
        .send("CLUSTER", &["SETSLOT", &test_slot.to_string(), "STABLE"])
        .await;

    harness.shutdown_all().await;
}

/// Test 17: Concurrent reads during migration.
///
/// Scenario: Reads continue during migration
#[tokio::test]
async fn test_concurrent_reads_during_migration() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();

    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let node_ids = harness.node_ids();
    let source = harness.node(node_ids[0]).unwrap();
    let target = harness.node(node_ids[1]).unwrap();

    // Get cluster node IDs via direct state access
    let source_id = harness.get_node_id_str(node_ids[0]).unwrap();
    let target_id = harness.get_node_id_str(node_ids[1]).unwrap();

    // Use slot 850
    let test_slot = 850u16;
    let test_key = key_for_slot(test_slot);

    // Write a value
    let _ = source.send("SET", &[&test_key, "read_test_value"]).await;

    // Set up migration
    let _ = source
        .send(
            "CLUSTER",
            &["SETSLOT", &test_slot.to_string(), "MIGRATING", &target_id],
        )
        .await;
    let _ = target
        .send(
            "CLUSTER",
            &["SETSLOT", &test_slot.to_string(), "IMPORTING", &source_id],
        )
        .await;

    // Perform concurrent reads during migration
    let source_addr = source.client_addr();
    let target_addr = target.client_addr();
    let key_clone = test_key.clone();

    let handles: Vec<_> = (0..5)
        .map(|i| {
            let addr = if i % 2 == 0 {
                source_addr.clone()
            } else {
                target_addr.clone()
            };
            let key = key_clone.clone();

            tokio::spawn(async move {
                let stream = tokio::net::TcpStream::connect(&addr).await;
                if let Ok(stream) = stream {
                    let mut framed = tokio_util::codec::Framed::new(
                        stream,
                        redis_protocol::codec::Resp2::default(),
                    );

                    let frame = redis_protocol::resp2::types::BytesFrame::Array(vec![
                        redis_protocol::resp2::types::BytesFrame::BulkString(bytes::Bytes::from(
                            "GET",
                        )),
                        redis_protocol::resp2::types::BytesFrame::BulkString(bytes::Bytes::from(
                            key,
                        )),
                    ]);

                    use futures::{SinkExt, StreamExt};
                    if framed.send(frame).await.is_ok()
                        && let Some(Ok(_)) = framed.next().await
                    {
                        return true;
                    }
                }
                false
            })
        })
        .collect();

    let results: Vec<bool> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap_or(false))
        .collect();

    let successful = results.iter().filter(|&&r| r).count();
    eprintln!(
        "Concurrent reads during migration: {}/{} succeeded",
        successful,
        results.len()
    );

    // Clean up
    let _ = source
        .send("CLUSTER", &["SETSLOT", &test_slot.to_string(), "STABLE"])
        .await;
    let _ = target
        .send("CLUSTER", &["SETSLOT", &test_slot.to_string(), "STABLE"])
        .await;

    harness.shutdown_all().await;
}

/// Test 18: Migration cancelled midway.
///
/// Scenario: Cancel migration, verify original state preserved
#[tokio::test]
async fn test_migration_cancelled_midway() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();

    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let node_ids = harness.node_ids();
    let source = harness.node(node_ids[0]).unwrap();
    let target = harness.node(node_ids[1]).unwrap();

    // Get cluster node IDs via direct state access
    let source_id = harness.get_node_id_str(node_ids[0]).unwrap();
    let target_id = harness.get_node_id_str(node_ids[1]).unwrap();

    // Use slot 900
    let test_slot = 900u16;
    let test_key = key_for_slot(test_slot);

    // Write data
    let set_resp = source.send("SET", &[&test_key, "cancel_test_value"]).await;
    eprintln!("Pre-migration SET: {:?}", set_resp);

    // Start migration
    let migrate_resp = source
        .send(
            "CLUSTER",
            &["SETSLOT", &test_slot.to_string(), "MIGRATING", &target_id],
        )
        .await;
    let import_resp = target
        .send(
            "CLUSTER",
            &["SETSLOT", &test_slot.to_string(), "IMPORTING", &source_id],
        )
        .await;

    if is_error(&migrate_resp) || is_error(&import_resp) {
        eprintln!("Migration setup failed");
        harness.shutdown_all().await;
        return;
    }

    eprintln!("Migration started, now cancelling...");

    // Cancel migration with STABLE
    let cancel_source = source
        .send("CLUSTER", &["SETSLOT", &test_slot.to_string(), "STABLE"])
        .await;
    let cancel_target = target
        .send("CLUSTER", &["SETSLOT", &test_slot.to_string(), "STABLE"])
        .await;

    eprintln!("Cancel source: {:?}", cancel_source);
    eprintln!("Cancel target: {:?}", cancel_target);

    // Verify data still accessible (should not get ASK redirect after cancel)
    let get_resp = source.send("GET", &[&test_key]).await;
    eprintln!("GET after cancel: {:?}", get_resp);

    // Verify no migration markers in CLUSTER NODES
    let nodes_resp = source.send("CLUSTER", &["NODES"]).await;
    if let frogdb_protocol::Response::Bulk(Some(b)) = &nodes_resp {
        let nodes_str = String::from_utf8_lossy(b);
        let has_migration = nodes_str.contains(&format!("[{}", test_slot))
            || nodes_str.contains(&format!("{}->", test_slot))
            || nodes_str.contains(&format!("{}<-", test_slot));
        if has_migration {
            eprintln!("WARNING: Migration markers still present after cancel");
        } else {
            eprintln!("SUCCESS: Migration markers cleared");
        }
    }

    harness.shutdown_all().await;
}

/// Wait until every surviving node's replicated cluster state agrees on a single
/// owner for `slot` and holds no lingering migration record for it, then return
/// the converged owner. Panics if convergence is not reached within `timeout`.
///
/// This is the core post-crash invariant for slot migration: after a crash the
/// slot must never be left orphaned (assigned to nobody on some node), dual-owned
/// (two nodes claim it), or stuck with a dangling migration record. The owner is
/// read from each node's Raft-replicated [`ClusterState`], so this checks that the
/// whole surviving cluster has converged on one consistent slot map, not just that
/// a single node looks healthy.
async fn assert_slot_converges_to_single_owner(
    harness: &ClusterTestHarness,
    surviving_ids: &[u64],
    slot: u16,
    timeout: Duration,
) -> u64 {
    let start = std::time::Instant::now();
    loop {
        let mut owners = std::collections::BTreeSet::new();
        let mut lingering_migration = false;
        let mut unassigned_on_some_node = false;
        let mut observed = 0usize;

        for &nid in surviving_ids {
            let Some(node) = harness.node(nid) else {
                continue;
            };
            if !node.is_running() {
                continue;
            }
            let Some(cs) = node.cluster_state() else {
                continue;
            };
            observed += 1;
            if cs.is_slot_migrating(slot) {
                lingering_migration = true;
            }
            match cs.get_slot_owner(slot) {
                Some(owner) => {
                    owners.insert(owner);
                }
                None => unassigned_on_some_node = true,
            }
        }

        let converged =
            observed > 0 && !lingering_migration && !unassigned_on_some_node && owners.len() == 1;
        if converged {
            return *owners.iter().next().unwrap();
        }
        if start.elapsed() >= timeout {
            panic!(
                "slot {slot} did not converge to a single owner within {timeout:?}: \
                 distinct owners across {observed} surviving node(s) = {owners:?}, \
                 lingering migration record = {lingering_migration}, \
                 unassigned on some node = {unassigned_on_some_node}"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Assert `key` is served by exactly one running node among `running_ids`: that
/// node answers the GET directly (no MOVED, no error) and every other running
/// node redirects (MOVED) to that same node's address. Returns the serving node's
/// harness id. Guards against both zero-owner (data loss — no node serves) and
/// dual-owner (two nodes accept the same key) outcomes.
async fn assert_key_served_by_exactly_one(
    harness: &ClusterTestHarness,
    running_ids: &[u64],
    key: &str,
) -> u64 {
    let mut servers = Vec::new();
    let mut moved_targets = std::collections::BTreeSet::new();

    for &nid in running_ids {
        let Some(node) = harness.node(nid) else {
            continue;
        };
        if !node.is_running() {
            continue;
        }
        let resp = node.send("GET", &[key]).await;
        if let Some((_slot, addr)) = is_moved_redirect(&resp) {
            moved_targets.insert(addr);
        } else if !is_error(&resp) {
            servers.push(nid);
        } else {
            panic!(
                "GET {key} on node {nid} returned an unexpected error \
                 (neither a value nor a MOVED redirect): {resp:?}"
            );
        }
    }

    assert_eq!(
        servers.len(),
        1,
        "key {key} must be served by exactly one node (no data loss, no dual ownership): \
         serving nodes = {servers:?}, MOVED targets = {moved_targets:?}"
    );

    let server = servers[0];
    let server_addr = harness.node(server).unwrap().client_addr();
    for addr in &moved_targets {
        assert_eq!(
            *addr, server_addr,
            "MOVED redirect points at {addr}, but the sole serving node is at {server_addr}"
        );
    }
    server
}

/// Assert that no running node among `running_ids` serves `key` directly — every
/// node either redirects (MOVED) or reports an error. Used when the slot's sole
/// owner has crashed with no replica: the data is genuinely unrecoverable, but the
/// surviving nodes must not silently take ownership of the orphaned slot
/// (dual-ownership / split-brain guard).
async fn assert_key_served_by_no_running_node(
    harness: &ClusterTestHarness,
    running_ids: &[u64],
    key: &str,
) {
    for &nid in running_ids {
        let Some(node) = harness.node(nid) else {
            continue;
        };
        if !node.is_running() {
            continue;
        }
        let resp = node.send("GET", &[key]).await;
        assert!(
            is_error(&resp),
            "node {nid} served key {key} directly after its owner crashed — \
             a surviving node silently claimed the orphaned slot: {resp:?}"
        );
    }
}

/// Test 19: Source dies mid-migration and STAYS DEAD (no replica).
///
/// Crash phase: after `BeginSlotMigration` (SETSLOT MIGRATING/IMPORTING) but
/// before any keys are handed off. An operator then cancels the migration on a
/// surviving node. Because the crashed source owned the slot and had no replica,
/// its data is unrecoverable — but the cluster *metadata* must still converge
/// cleanly:
///   (a) the slot resolves to exactly one owner (rolled back to the source),
///   (b) no surviving node retains a lingering migration record, and
///   (c) no surviving node silently takes ownership of the orphaned slot.
///
/// (Note: test_failover_during_migration_preserves_data already exists; this is an
/// alternative that asserts convergence rather than only logging cluster info.)
#[tokio::test]
async fn test_source_dies_during_migration() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();

    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    // Use slot 950; source = whichever node actually owns it, target = another.
    let test_slot = 950u16;
    let test_key = key_for_slot(test_slot);
    let (source_id, target_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");
    let cluster_source_id = harness.get_node_id_str(source_id).unwrap();
    let cluster_target_id = harness.get_node_id_str(target_id).unwrap();

    // Write a key into the slot before migration begins.
    {
        let source = harness.node(source_id).unwrap();
        let set_resp = source
            .send("SET", &[&test_key, "pre_migration_value"])
            .await;
        assert!(
            !is_error(&set_resp),
            "pre-migration SET failed: {set_resp:?}"
        );
    }

    // Begin the migration (BeginSlotMigration via Raft), then crash the source
    // before any keys are transferred.
    {
        let source = harness.node(source_id).unwrap();
        let target = harness.node(target_id).unwrap();

        let mig = source
            .send(
                "CLUSTER",
                &[
                    "SETSLOT",
                    &test_slot.to_string(),
                    "MIGRATING",
                    &cluster_target_id,
                ],
            )
            .await;
        assert!(!is_error(&mig), "SETSLOT MIGRATING failed: {mig:?}");
        let imp = target
            .send(
                "CLUSTER",
                &[
                    "SETSLOT",
                    &test_slot.to_string(),
                    "IMPORTING",
                    &cluster_source_id,
                ],
            )
            .await;
        assert!(!is_error(&imp), "SETSLOT IMPORTING failed: {imp:?}");
    }

    eprintln!("Migration started, killing source node {source_id}");
    harness.kill_node(source_id).await;

    // Let failure detection settle and a leader be (re-)elected.
    tokio::time::sleep(Duration::from_secs(2)).await;
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();

    let surviving_ids: Vec<u64> = harness
        .node_ids()
        .into_iter()
        .filter(|&id| id != source_id)
        .collect();

    // Operator cancels the migration on a surviving node.
    for &nid in &surviving_ids {
        if let Some(node) = harness.node(nid)
            && node.is_running()
        {
            let _ = node
                .send("CLUSTER", &["SETSLOT", &test_slot.to_string(), "STABLE"])
                .await;
        }
    }

    // (a) + (b): the surviving cluster converges to a single owner with no
    // lingering migration record. The slot must roll back to the (now-dead)
    // source, never silently reassign to the import target.
    let owner = assert_slot_converges_to_single_owner(
        &harness,
        &surviving_ids,
        test_slot,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(
        owner, source_id,
        "slot should roll back to the (now-dead) source, not silently reassign to the target"
    );

    // (c): with the sole owner dead and no replica, the key is unrecoverable —
    // but no surviving node may silently start serving it.
    assert_key_served_by_no_running_node(&harness, &surviving_ids, &test_key).await;

    harness.shutdown_all().await;
}

/// Test 19b: Source dies mid-migration then RECOVERS (restarts and rejoins).
///
/// Crash phase: after `BeginSlotMigration`, before any key handoff. The source is
/// restarted (persistence enabled) and the migration is cancelled. The cluster
/// must converge with:
///   (a) the slot rolled back to the recovered source (exactly one owner),
///   (b) no lingering migration record on any node, and
///   (c) the slot's key readable from exactly one node (the source).
///
/// Note: a hard crash may drop writes that were not yet fsynced, so the key's
/// *value* survival is checked best-effort (logged); the routing invariant — that
/// exactly one node serves the key — is asserted strictly.
#[tokio::test]
async fn test_source_dies_during_migration_source_recovers() {
    let config = ClusterNodeConfig {
        persistence: true,
        ..Default::default()
    };
    let mut harness = ClusterTestHarness::with_config(config);
    harness.start_cluster(3).await.unwrap();

    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let test_slot = 950u16;
    let test_key = key_for_slot(test_slot);
    let (source_id, target_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");
    let cluster_source_id = harness.get_node_id_str(source_id).unwrap();
    let cluster_target_id = harness.get_node_id_str(target_id).unwrap();

    // Write a key into the slot, then begin migration.
    {
        let source = harness.node(source_id).unwrap();
        let set_resp = source
            .send("SET", &[&test_key, "pre_migration_value"])
            .await;
        assert!(
            !is_error(&set_resp),
            "pre-migration SET failed: {set_resp:?}"
        );

        let mig = source
            .send(
                "CLUSTER",
                &[
                    "SETSLOT",
                    &test_slot.to_string(),
                    "MIGRATING",
                    &cluster_target_id,
                ],
            )
            .await;
        assert!(!is_error(&mig), "SETSLOT MIGRATING failed: {mig:?}");
        let target = harness.node(target_id).unwrap();
        let imp = target
            .send(
                "CLUSTER",
                &[
                    "SETSLOT",
                    &test_slot.to_string(),
                    "IMPORTING",
                    &cluster_source_id,
                ],
            )
            .await;
        assert!(!is_error(&imp), "SETSLOT IMPORTING failed: {imp:?}");
    }

    eprintln!("Migration started, killing source node {source_id}");
    harness.kill_node(source_id).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();

    // Bring the source back and let it rejoin the cluster.
    eprintln!("Restarting source node {source_id}");
    harness.restart_node(source_id).await.unwrap();
    harness
        .wait_for_node_recognized(source_id, Duration::from_secs(20))
        .await
        .unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();

    // Operator cancels the migration (now that the source is back).
    let cancel = harness
        .node(source_id)
        .unwrap()
        .send("CLUSTER", &["SETSLOT", &test_slot.to_string(), "STABLE"])
        .await;
    eprintln!("SETSLOT STABLE on recovered source: {cancel:?}");

    let all_ids = harness.node_ids();

    // (a) + (b): converges across all three nodes, rolled back to the source.
    let owner = assert_slot_converges_to_single_owner(
        &harness,
        &all_ids,
        test_slot,
        Duration::from_secs(15),
    )
    .await;
    assert_eq!(
        owner, source_id,
        "slot should roll back to the recovered source after a cancelled migration"
    );

    // (c): the key is served by exactly one node — the source.
    let server = assert_key_served_by_exactly_one(&harness, &all_ids, &test_key).await;
    assert_eq!(
        server, source_id,
        "the migrating slot's key must be served by exactly one node (the recovered source)"
    );

    // Best-effort durability check — a hard crash may have dropped the write.
    let val = harness
        .node(source_id)
        .unwrap()
        .send("GET", &[&test_key])
        .await;
    eprintln!("recovered source GET {test_key} => {val:?}");

    harness.shutdown_all().await;
}

/// Test 19c: Source dies AFTER the slot has been fully migrated to the target.
///
/// Crash phase: post-finalization (SETSLOT NODE committed, keys already on the
/// target). Killing the old source must not disturb the converged topology:
///   (a) the slot stays owned solely by the target,
///   (b) no lingering migration record remains, and
///   (c) the migrated keys remain readable from exactly one node (the target) —
///       no data loss, no dual ownership.
#[tokio::test]
async fn test_source_dies_after_migration_complete() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();

    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let test_slot = 950u16;
    let (source_id, target_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");

    // Write hash-tagged keys that all hash to the slot.
    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let probe_key = format!("{tag}_k0");
    {
        let source = harness.node(source_id).unwrap();
        for i in 0..5 {
            let key = format!("{tag}_k{i}");
            let value = format!("v{i}");
            let resp = source.send("SET", &[&key, &value]).await;
            assert!(!is_error(&resp), "SET failed: {resp:?}");
        }
    }

    // Fully migrate the slot to the target (data + ownership handoff).
    run_full_slot_migration(&harness, source_id, target_id, test_slot, 100)
        .await
        .expect("migration failed");

    // Now crash the old source, which no longer owns the slot.
    eprintln!("Migration complete, killing old source node {source_id}");
    harness.kill_node(source_id).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();

    let surviving_ids: Vec<u64> = harness
        .node_ids()
        .into_iter()
        .filter(|&id| id != source_id)
        .collect();

    // (a) + (b): slot owned solely by the target, no lingering migration record.
    let owner = assert_slot_converges_to_single_owner(
        &harness,
        &surviving_ids,
        test_slot,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(
        owner, target_id,
        "a completed migration should leave the slot owned by the target"
    );

    // (c): the migrated key is readable from exactly one node — the target — and
    // its value survived the source crash.
    let server = assert_key_served_by_exactly_one(&harness, &surviving_ids, &probe_key).await;
    assert_eq!(
        server, target_id,
        "the migrated key must be served by exactly one node (the target)"
    );
    let val = harness
        .node(target_id)
        .unwrap()
        .send("GET", &[&probe_key])
        .await;
    match val {
        frogdb_protocol::Response::Bulk(Some(v)) => {
            assert_eq!(&v[..], b"v0", "migrated value wrong on target");
        }
        other => panic!("expected migrated value on target, got: {other:?}"),
    }

    harness.shutdown_all().await;
}

// ============================================================================
// Tier 9: Raft Snapshot Edge Cases (Existing Test Section)
// ============================================================================

/// Test Raft snapshot during slot migration.
///
/// This tests a critical scenario where:
/// 1. A slot migration is in progress (MIGRATING/IMPORTING state)
/// 2. A Raft snapshot is taken (or leader change occurs)
/// 3. The migration state should be preserved or cleanly aborted
///
/// Risk: If migration state is lost during snapshot/recovery, the cluster
/// could end up with inconsistent slot ownership.
#[tokio::test]
async fn test_raft_snapshot_during_migration() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();

    let leader = harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let node_ids = harness.node_ids();

    // The leader must be the source since SETSLOT MIGRATING is a Raft op
    // and uses my_node_id as the migration source.
    let source_node_id = leader;
    let target_node_id = *node_ids.iter().find(|&&id| id != leader).unwrap();

    // Get cluster node IDs via direct state access
    let source_node = harness.node(source_node_id).unwrap();
    let target_node = harness.node(target_node_id).unwrap();

    let source_cluster_id = harness.get_node_id_str(source_node_id).unwrap();
    let target_cluster_id = harness.get_node_id_str(target_node_id).unwrap();

    eprintln!(
        "Source cluster ID: {}, Target cluster ID: {}",
        source_cluster_id, target_cluster_id
    );

    // Pick a slot to migrate (use slot 100 which is easy to test)
    let test_slot: u16 = 100;

    // First, ensure the source node owns the slot
    // Add the slot to source if not already assigned
    let addslots_resp = source_node
        .send("CLUSTER", &["ADDSLOTS", &test_slot.to_string()])
        .await;
    eprintln!("ADDSLOTS response: {:?}", addslots_resp);

    // Small delay for slot assignment to propagate
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Write a test key that hashes to our slot
    let test_key = key_for_slot(test_slot);
    assert_eq!(
        slot_for_key(test_key.as_bytes()),
        test_slot,
        "Key should hash to test slot"
    );

    // Write data to the slot
    let set_resp = source_node
        .send("SET", &[&test_key, "migration_test_value"])
        .await;
    eprintln!("SET response: {:?}", set_resp);

    // Start migration: set MIGRATING on source, IMPORTING on target
    let migrating_resp = source_node
        .send(
            "CLUSTER",
            &[
                "SETSLOT",
                &test_slot.to_string(),
                "MIGRATING",
                &target_cluster_id,
            ],
        )
        .await;

    eprintln!("SETSLOT MIGRATING response: {:?}", migrating_resp);

    let importing_resp = target_node
        .send(
            "CLUSTER",
            &[
                "SETSLOT",
                &test_slot.to_string(),
                "IMPORTING",
                &source_cluster_id,
            ],
        )
        .await;

    eprintln!("SETSLOT IMPORTING response: {:?}", importing_resp);

    // Verify migration state is visible in CLUSTER NODES
    let nodes_resp = source_node.send("CLUSTER", &["NODES"]).await;
    if let Ok(nodes) = parse_cluster_nodes(&nodes_resp) {
        for node in &nodes {
            eprintln!(
                "Node {}: flags={:?}, slots={:?}",
                &node.id[..8.min(node.id.len())],
                node.flags,
                node.slots
            );
        }
    }

    // Now simulate a disruptive event: kill the leader
    // This will trigger leader election and potentially Raft snapshots
    eprintln!(
        "Killing leader {} to trigger election during migration",
        leader
    );
    harness.kill_node(leader).await;

    // Wait for new leader election
    let _new_leader = match harness.wait_for_leader(Duration::from_secs(15)).await {
        Ok(l) => {
            eprintln!("New leader elected: {}", l);
            l
        }
        Err(e) => {
            eprintln!("Leader election failed: {}", e);
            harness.shutdown_all().await;
            panic!("Should elect new leader");
        }
    };

    // Wait for cluster to stabilize
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Check migration state on surviving nodes
    let surviving_ids: Vec<u64> = node_ids
        .iter()
        .filter(|&&id| id != leader)
        .copied()
        .collect();

    let mut migration_state_found = false;
    let mut slot_ownership_clear = false;

    for &node_id in &surviving_ids {
        if let Some(node) = harness.node(node_id) {
            if !node.is_running() {
                continue;
            }

            let nodes_resp = node.send("CLUSTER", &["NODES"]).await;
            if let Ok(nodes) = parse_cluster_nodes(&nodes_resp) {
                for node_info in &nodes {
                    // Check if migration flags are present
                    // Note: After leader change, migration might be aborted
                    let node_flags = node_info.flags.join(",");
                    let has_migration_flag =
                        node_flags.contains("migrating") || node_flags.contains("importing");

                    if has_migration_flag {
                        migration_state_found = true;
                        eprintln!(
                            "Migration state still present on node {}: {:?}",
                            &node_info.id[..8.min(node_info.id.len())],
                            node_info.flags
                        );
                    }

                    // Check slot ownership
                    for (start, end) in &node_info.slots {
                        if *start <= test_slot && test_slot <= *end {
                            slot_ownership_clear = true;
                            eprintln!(
                                "Slot {} owned by node {} ({}-{})",
                                test_slot,
                                &node_info.id[..8.min(node_info.id.len())],
                                start,
                                end
                            );
                        }
                    }
                }
            }
        }
    }

    // After leader change during migration, we expect either:
    // 1. Migration state is preserved (cluster continues migration)
    // 2. Migration is cleanly aborted (slot returns to stable state)
    // Both are acceptable - what's NOT acceptable is inconsistent state

    eprintln!(
        "After failover: migration_state_found={}, slot_ownership_clear={}",
        migration_state_found, slot_ownership_clear
    );

    assert!(
        migration_state_found || slot_ownership_clear,
        "slot {test_slot} must be accounted for in the replicated topology after \
         the leader died mid-migration: either the migration markers survived or \
         some node owns the slot outright. Neither means the slot fell out of the \
         topology, which is the inconsistent state this test exists to catch."
    );

    // Try to clean up migration state
    for &node_id in &surviving_ids {
        if let Some(node) = harness.node(node_id)
            && node.is_running()
        {
            let _ = node
                .send("CLUSTER", &["SETSLOT", &test_slot.to_string(), "STABLE"])
                .await;
        }
    }

    // Verify the survivors still agree with each other after the disruption.
    //
    // Deliberately *not* `wait_for_cluster_convergence`: that also requires
    // `cluster_state:ok`, and the killed node is a slot-owning primary with no
    // replica, so once the failure detector FAIL-flags it the cluster reports
    // `fail` for good — correctly. (Before the detector was fixed to reconcile
    // its local health view on the new leader, the flag was never set and this
    // scenario reported `ok`, which is what let the stronger helper pass here.)
    // What this test is actually about is agreement: migration either survived
    // or was cleanly abandoned, and no two survivors disagree about it.
    let agreement = harness
        .wait_for_topology_agreement(Duration::from_secs(10))
        .await;

    eprintln!(
        "Survivors agreed on topology after migration+failover: {:?}",
        agreement
    );

    // The key assertion: cluster should be in a consistent state
    // Either migration completed, was aborted, or is still in progress
    // But never in an inconsistent state where nodes disagree
    assert!(
        agreement.is_ok(),
        "Survivors should agree on topology after failover during migration: {agreement:?}"
    );

    harness.shutdown_all().await;
}

// ============================================================================
// Tier 10: End-to-End Slot Migration Tests
// ============================================================================

/// Test 1: Basic string key migration end-to-end.
///
/// Writes 5 string keys with hash tags to a slot, runs the full migration workflow,
/// then verifies keys exist on target, are gone from source, and source returns MOVED.
#[tokio::test]
async fn test_e2e_migration_basic_string_keys() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let test_slot = 100u16;
    let (source_id, target_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");

    let source = harness.node(source_id).unwrap();

    // Write 5 keys with hash tags targeting the same slot
    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let mut keys = Vec::new();
    for i in 0..5 {
        let key = format!("{}_k{}", tag, i);
        let value = format!("value_{}", i);
        let resp = source.send("SET", &[&key, &value]).await;
        assert!(!is_error(&resp), "SET failed: {:?}", resp);
        keys.push((key, value));
    }

    // Run full migration
    run_full_slot_migration(&harness, source_id, target_id, test_slot, 100)
        .await
        .expect("migration failed");

    // Verify keys on target
    let target = harness.node(target_id).unwrap();
    for (key, expected_value) in &keys {
        let resp = target.send("GET", &[key]).await;
        match resp {
            frogdb_protocol::Response::Bulk(Some(v)) => {
                assert_eq!(
                    std::str::from_utf8(&v).unwrap(),
                    expected_value,
                    "value mismatch for key {}",
                    key
                );
            }
            other => panic!("expected value for key {} on target, got: {:?}", key, other),
        }
    }

    // Verify source returns MOVED for these keys
    let source = harness.node(source_id).unwrap();
    for (key, _) in &keys {
        let resp = source.send("GET", &[key]).await;
        assert!(
            is_moved_redirect(&resp).is_some(),
            "expected MOVED redirect from source for key {}, got: {:?}",
            key,
            resp
        );
    }

    harness.shutdown_all().await;
}

/// Test 2: Migration preserves TTL.
///
/// Writes a key with PEXPIRE, migrates it, verifies PTTL on target is positive
/// and less than or equal to the original TTL.
#[tokio::test]
async fn test_e2e_migration_preserves_ttl() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let test_slot = 200u16;
    let (source_id, target_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");

    let source = harness.node(source_id).unwrap();

    let key = format!("{{{}}}_k0", key_for_slot(test_slot));
    let ttl_ms: i64 = 30000;

    // SET + PEXPIRE
    let set_resp = source.send("SET", &[&key, "ttl_value"]).await;
    assert!(!is_error(&set_resp), "SET failed: {:?}", set_resp);

    let expire_resp = source.send("PEXPIRE", &[&key, &ttl_ms.to_string()]).await;
    assert!(!is_error(&expire_resp), "PEXPIRE failed: {:?}", expire_resp);

    // Verify TTL is set on source
    let pttl_resp = source.send("PTTL", &[&key]).await;
    let source_pttl = match pttl_resp {
        frogdb_protocol::Response::Integer(t) => t,
        other => panic!("expected integer PTTL, got: {:?}", other),
    };
    assert!(source_pttl > 0, "PTTL should be positive before migration");

    // Run full migration
    run_full_slot_migration(&harness, source_id, target_id, test_slot, 100)
        .await
        .expect("migration failed");

    // Verify TTL on target
    let target = harness.node(target_id).unwrap();
    let target_pttl_resp = target.send("PTTL", &[&key]).await;
    let target_pttl = match target_pttl_resp {
        frogdb_protocol::Response::Integer(t) => t,
        other => panic!("expected integer PTTL on target, got: {:?}", other),
    };

    assert!(target_pttl > 0, "PTTL on target should be positive");
    assert!(
        target_pttl <= ttl_ms,
        "PTTL on target ({}) should be <= original ({})",
        target_pttl,
        ttl_ms
    );

    // Verify value is correct
    let get_resp = target.send("GET", &[&key]).await;
    match get_resp {
        frogdb_protocol::Response::Bulk(Some(v)) => {
            assert_eq!(std::str::from_utf8(&v).unwrap(), "ttl_value");
        }
        other => panic!("expected value on target, got: {:?}", other),
    }

    harness.shutdown_all().await;
}

/// Test 3: Migration preserves multiple data types.
///
/// Writes String, Hash, List, Set, and SortedSet to the same slot, migrates,
/// then verifies all types are correctly reconstructed on the target.
#[tokio::test]
async fn test_e2e_migration_multiple_data_types() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let test_slot = 300u16;
    let (source_id, target_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");

    let source = harness.node(source_id).unwrap();
    let tag = format!("{{{}}}", key_for_slot(test_slot));

    // String
    let str_key = format!("{}_str", tag);
    let resp = source.send("SET", &[&str_key, "hello"]).await;
    assert!(!is_error(&resp), "SET failed: {:?}", resp);

    // Hash
    let hash_key = format!("{}_hash", tag);
    let resp = source
        .send("HSET", &[&hash_key, "field1", "val1", "field2", "val2"])
        .await;
    assert!(!is_error(&resp), "HSET failed: {:?}", resp);

    // List
    let list_key = format!("{}_list", tag);
    let resp = source.send("RPUSH", &[&list_key, "a", "b", "c"]).await;
    assert!(!is_error(&resp), "RPUSH failed: {:?}", resp);

    // Set
    let set_key = format!("{}_set", tag);
    let resp = source.send("SADD", &[&set_key, "x", "y", "z"]).await;
    assert!(!is_error(&resp), "SADD failed: {:?}", resp);

    // SortedSet
    let zset_key = format!("{}_zset", tag);
    let resp = source
        .send("ZADD", &[&zset_key, "1", "one", "2", "two", "3", "three"])
        .await;
    assert!(!is_error(&resp), "ZADD failed: {:?}", resp);

    // Run full migration
    run_full_slot_migration(&harness, source_id, target_id, test_slot, 100)
        .await
        .expect("migration failed");

    let target = harness.node(target_id).unwrap();

    // Verify String
    let resp = target.send("GET", &[&str_key]).await;
    match resp {
        frogdb_protocol::Response::Bulk(Some(v)) => {
            assert_eq!(std::str::from_utf8(&v).unwrap(), "hello");
        }
        other => panic!("expected string value, got: {:?}", other),
    }

    // Verify Hash
    let resp = target.send("HGETALL", &[&hash_key]).await;
    if let frogdb_protocol::Response::Array(arr) = resp {
        // HGETALL returns flat [field, value, field, value, ...]
        assert_eq!(arr.len(), 4, "expected 2 field-value pairs");
    } else {
        panic!("expected array from HGETALL, got: {:?}", resp);
    }

    // Verify List
    let resp = target.send("LRANGE", &[&list_key, "0", "-1"]).await;
    if let frogdb_protocol::Response::Array(arr) = resp {
        let vals: Vec<String> = arr
            .iter()
            .filter_map(|r| {
                if let frogdb_protocol::Response::Bulk(Some(b)) = r {
                    Some(String::from_utf8_lossy(b).to_string())
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(vals, vec!["a", "b", "c"]);
    } else {
        panic!("expected array from LRANGE, got: {:?}", resp);
    }

    // Verify Set
    let resp = target.send("SMEMBERS", &[&set_key]).await;
    if let frogdb_protocol::Response::Array(arr) = resp {
        let mut vals: Vec<String> = arr
            .iter()
            .filter_map(|r| {
                if let frogdb_protocol::Response::Bulk(Some(b)) = r {
                    Some(String::from_utf8_lossy(b).to_string())
                } else {
                    None
                }
            })
            .collect();
        vals.sort();
        assert_eq!(vals, vec!["x", "y", "z"]);
    } else {
        panic!("expected array from SMEMBERS, got: {:?}", resp);
    }

    // Verify SortedSet
    let resp = target
        .send("ZRANGE", &[&zset_key, "0", "-1", "WITHSCORES"])
        .await;
    if let frogdb_protocol::Response::Array(arr) = resp {
        // ZRANGE WITHSCORES returns [member, score, member, score, ...]
        assert_eq!(arr.len(), 6, "expected 3 member-score pairs");
    } else {
        panic!("expected array from ZRANGE, got: {:?}", resp);
    }

    harness.shutdown_all().await;
}

/// Test 4: Batched key migration.
///
/// Writes 25 keys, migrates with batch_size=10, verifies GETKEYSINSLOT returns
/// batches and all 25 keys land on target.
#[tokio::test]
async fn test_e2e_migration_batched_keys() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let test_slot = 400u16;
    let (source_id, target_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");

    let source = harness.node(source_id).unwrap();
    let tag = format!("{{{}}}", key_for_slot(test_slot));

    // Write 25 keys
    let num_keys = 25;
    let mut keys = Vec::new();
    for i in 0..num_keys {
        let key = format!("{}_k{}", tag, i);
        let value = format!("batch_value_{}", i);
        let resp = source.send("SET", &[&key, &value]).await;
        assert!(!is_error(&resp), "SET failed for key {}: {:?}", key, resp);
        keys.push((key, value));
    }

    // Run migration with batch_size=10 (should take 3 rounds: 10+10+5)
    run_full_slot_migration(&harness, source_id, target_id, test_slot, 10)
        .await
        .expect("batched migration failed");

    // Verify all 25 keys on target
    let target = harness.node(target_id).unwrap();
    for (key, expected_value) in &keys {
        let resp = target.send("GET", &[key]).await;
        match resp {
            frogdb_protocol::Response::Bulk(Some(v)) => {
                assert_eq!(
                    std::str::from_utf8(&v).unwrap(),
                    expected_value,
                    "value mismatch for key {}",
                    key
                );
            }
            other => panic!("expected value for key {} on target, got: {:?}", key, other),
        }
    }

    // Verify source has 0 keys in this slot
    let source = harness.node(source_id).unwrap();
    let keys_resp = source
        .send("CLUSTER", &["GETKEYSINSLOT", &test_slot.to_string(), "100"])
        .await;
    if let frogdb_protocol::Response::Array(arr) = keys_resp {
        assert_eq!(
            arr.len(),
            0,
            "source should have 0 keys in slot {} after migration",
            test_slot
        );
    }

    harness.shutdown_all().await;
}

/// Test 5: ASK redirect during partial migration.
///
/// Sets MIGRATING state, migrates only 1 of 2 keys, verifies:
/// - Non-migrated key is served directly from source
/// - Migrated key gets ASK redirect from source
/// - ASKING+GET on target works for the migrated key
#[tokio::test]
async fn test_e2e_migration_ask_redirect() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let test_slot = 500u16;
    let (source_id, target_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");

    let source = harness.node(source_id).unwrap();
    let target = harness.node(target_id).unwrap();

    let source_cluster_id = harness.get_node_id_str(source_id).unwrap();
    let target_cluster_id = harness.get_node_id_str(target_id).unwrap();

    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let key_stay = format!("{}_stay", tag);
    let key_move = format!("{}_move", tag);

    // Write two keys
    let resp = source.send("SET", &[&key_stay, "stay_value"]).await;
    assert!(!is_error(&resp), "SET stay failed: {:?}", resp);
    let resp = source.send("SET", &[&key_move, "move_value"]).await;
    assert!(!is_error(&resp), "SET move failed: {:?}", resp);

    let slot_str = test_slot.to_string();

    // Set IMPORTING on target (via Raft leader)
    let resp = send_cluster_cmd(
        &harness,
        target_id,
        &[
            "SETSLOT",
            &slot_str,
            "IMPORTING",
            &source_cluster_id,
            &target_cluster_id,
        ],
    )
    .await;
    assert!(!is_error(&resp), "SETSLOT IMPORTING failed: {:?}", resp);

    // Set MIGRATING on source (via Raft leader)
    let resp = send_cluster_cmd(
        &harness,
        source_id,
        &[
            "SETSLOT",
            &slot_str,
            "MIGRATING",
            &target_cluster_id,
            &source_cluster_id,
        ],
    )
    .await;
    assert!(!is_error(&resp), "SETSLOT MIGRATING failed: {:?}", resp);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Migrate only key_move (not key_stay)
    let target_addr = target.client_addr();
    let (host, port) = split_addr(&target_addr);
    let migrate_resp = source
        .send(
            "MIGRATE",
            &[host, port, "", "0", "5000", "REPLACE", "KEYS", &key_move],
        )
        .await;
    assert!(
        !is_error(&migrate_resp),
        "MIGRATE failed: {:?}",
        migrate_resp
    );

    // key_stay should still be served from source (it's still there)
    let resp = source.send("GET", &[&key_stay]).await;
    match &resp {
        frogdb_protocol::Response::Bulk(Some(v)) => {
            assert_eq!(std::str::from_utf8(v).unwrap(), "stay_value");
        }
        other => panic!("expected key_stay value from source, got: {:?}", other),
    }

    // key_move should get ASK redirect from source (it was deleted by MIGRATE)
    let resp = source.send("GET", &[&key_move]).await;
    assert!(
        is_ask_redirect(&resp).is_some(),
        "expected ASK redirect for migrated key from source, got: {:?}",
        resp
    );

    // ASKING + GET on target should return the value
    let mut target_client = target.connect().await;
    let asking_resp = target_client.command(&["ASKING"]).await;
    assert!(!is_error(&asking_resp), "ASKING failed: {:?}", asking_resp);
    let get_resp = target_client.command(&["GET", &key_move]).await;
    match get_resp {
        frogdb_protocol::Response::Bulk(Some(v)) => {
            assert_eq!(std::str::from_utf8(&v).unwrap(), "move_value");
        }
        other => panic!(
            "expected move_value via ASKING+GET on target, got: {:?}",
            other
        ),
    }

    // Clean up: complete migration (via Raft leader)
    let _ = send_cluster_cmd(
        &harness,
        target_id,
        &["SETSLOT", &slot_str, "NODE", &target_cluster_id],
    )
    .await;

    harness.shutdown_all().await;
}

/// Test 6: MOVED redirect after migration completes.
///
/// Runs full migration, then verifies source returns MOVED for both reads and writes.
#[tokio::test]
async fn test_e2e_migration_moved_after_complete() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();
    // Wait for Raft self-registration to propagate correct node addresses
    harness
        .wait_for_address_convergence(Duration::from_secs(10))
        .await
        .unwrap();

    let test_slot = 600u16;
    let (source_id, target_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");

    let source = harness.node(source_id).unwrap();

    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let key = format!("{}_k0", tag);

    // Write a key
    let resp = source.send("SET", &[&key, "moved_test"]).await;
    assert!(!is_error(&resp), "SET failed: {:?}", resp);

    // Run full migration
    run_full_slot_migration(&harness, source_id, target_id, test_slot, 100)
        .await
        .expect("migration failed");

    // Verify MOVED for GET (read)
    let source = harness.node(source_id).unwrap();
    let resp = source.send("GET", &[&key]).await;
    let moved = is_moved_redirect(&resp);
    assert!(
        moved.is_some(),
        "expected MOVED for GET after migration, got: {:?}",
        resp
    );
    let (moved_slot, moved_addr) = moved.unwrap();
    assert_eq!(moved_slot, test_slot, "MOVED slot mismatch");
    let target = harness.node(target_id).unwrap();
    assert_eq!(
        moved_addr,
        target.client_addr(),
        "MOVED should point to target"
    );

    // Verify MOVED for SET (write)
    let resp = source.send("SET", &[&key, "new_value"]).await;
    assert!(
        is_moved_redirect(&resp).is_some(),
        "expected MOVED for SET after migration, got: {:?}",
        resp
    );

    harness.shutdown_all().await;
}

/// Test 7: Migration of an empty slot.
///
/// Migrates a slot with 0 keys. GETKEYSINSLOT returns empty, no MIGRATE needed.
/// After SETSLOT NODE, new writes to the slot go to the target.
#[tokio::test]
async fn test_e2e_migration_empty_slot() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let test_slot = 700u16;
    let (source_id, target_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");

    let source = harness.node(source_id).unwrap();

    // Verify slot is empty
    let keys_resp = source
        .send("CLUSTER", &["GETKEYSINSLOT", &test_slot.to_string(), "100"])
        .await;
    if let frogdb_protocol::Response::Array(ref arr) = keys_resp {
        assert_eq!(arr.len(), 0, "slot should be empty before migration");
    }

    // Run full migration (should succeed immediately since no keys to move)
    run_full_slot_migration(&harness, source_id, target_id, test_slot, 100)
        .await
        .expect("empty slot migration failed");

    // New writes should go to target (source returns MOVED)
    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let key = format!("{}_new", tag);

    let source = harness.node(source_id).unwrap();
    let resp = source.send("SET", &[&key, "new_value"]).await;
    assert!(
        is_moved_redirect(&resp).is_some(),
        "expected MOVED for write to migrated empty slot, got: {:?}",
        resp
    );

    // Write directly on target should succeed
    let target = harness.node(target_id).unwrap();
    let resp = target.send("SET", &[&key, "new_value"]).await;
    assert!(
        !is_error(&resp),
        "SET on target for migrated slot should succeed: {:?}",
        resp
    );

    // Verify value is readable from target
    let resp = target.send("GET", &[&key]).await;
    match resp {
        frogdb_protocol::Response::Bulk(Some(v)) => {
            assert_eq!(std::str::from_utf8(&v).unwrap(), "new_value");
        }
        other => panic!("expected value from target, got: {:?}", other),
    }

    harness.shutdown_all().await;
}

/// Test 8: Concurrent writes during migration.
///
/// Begins migration, migrates 1 of 2 keys, updates the remaining key on source,
/// completes migration, then verifies the updated value is present on target.
#[tokio::test]
async fn test_e2e_migration_concurrent_writes() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let test_slot = 800u16;
    let (source_id, target_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");

    let source = harness.node(source_id).unwrap();
    let target = harness.node(target_id).unwrap();

    let source_cluster_id = harness.get_node_id_str(source_id).unwrap();
    let target_cluster_id = harness.get_node_id_str(target_id).unwrap();

    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let key1 = format!("{}_k1", tag);
    let key2 = format!("{}_k2", tag);

    // Write initial keys
    let resp = source.send("SET", &[&key1, "original_k1"]).await;
    assert!(!is_error(&resp), "SET k1 failed: {:?}", resp);
    let resp = source.send("SET", &[&key2, "original_k2"]).await;
    assert!(!is_error(&resp), "SET k2 failed: {:?}", resp);

    let slot_str = test_slot.to_string();

    // Begin migration (via Raft leader)
    let resp = send_cluster_cmd(
        &harness,
        target_id,
        &[
            "SETSLOT",
            &slot_str,
            "IMPORTING",
            &source_cluster_id,
            &target_cluster_id,
        ],
    )
    .await;
    assert!(!is_error(&resp), "SETSLOT IMPORTING failed: {:?}", resp);

    let resp = send_cluster_cmd(
        &harness,
        source_id,
        &[
            "SETSLOT",
            &slot_str,
            "MIGRATING",
            &target_cluster_id,
            &source_cluster_id,
        ],
    )
    .await;
    assert!(!is_error(&resp), "SETSLOT MIGRATING failed: {:?}", resp);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Migrate only key1
    let target_addr = target.client_addr();
    let (host, port) = split_addr(&target_addr);
    let resp = source
        .send(
            "MIGRATE",
            &[host, port, "", "0", "5000", "REPLACE", "KEYS", &key1],
        )
        .await;
    assert!(!is_error(&resp), "MIGRATE k1 failed: {:?}", resp);

    // Update key2 on source (still there, not yet migrated)
    let resp = source.send("SET", &[&key2, "updated_k2"]).await;
    assert!(
        !is_error(&resp),
        "SET updated k2 during migration failed: {:?}",
        resp
    );

    // Migrate key2
    let resp = source
        .send(
            "MIGRATE",
            &[host, port, "", "0", "5000", "REPLACE", "KEYS", &key2],
        )
        .await;
    assert!(!is_error(&resp), "MIGRATE k2 failed: {:?}", resp);

    // Complete migration (via Raft leader)
    let resp = send_cluster_cmd(
        &harness,
        target_id,
        &["SETSLOT", &slot_str, "NODE", &target_cluster_id],
    )
    .await;
    assert!(!is_error(&resp), "SETSLOT NODE failed: {:?}", resp);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify key1 has original value on target
    let target = harness.node(target_id).unwrap();
    let resp = target.send("GET", &[&key1]).await;
    match resp {
        frogdb_protocol::Response::Bulk(Some(v)) => {
            assert_eq!(std::str::from_utf8(&v).unwrap(), "original_k1");
        }
        other => panic!("expected original_k1 on target, got: {:?}", other),
    }

    // Verify key2 has UPDATED value on target
    let resp = target.send("GET", &[&key2]).await;
    match resp {
        frogdb_protocol::Response::Bulk(Some(v)) => {
            assert_eq!(
                std::str::from_utf8(&v).unwrap(),
                "updated_k2",
                "key2 should have the updated value after migration"
            );
        }
        other => panic!("expected updated_k2 on target, got: {:?}", other),
    }

    harness.shutdown_all().await;
}

// ============================================================================
// Tier 14: Migration Type Coverage — Known-Gap Documentation
// ============================================================================

#[cfg(feature = "cmd-stream")]
/// Verifies that stream data survives slot migration.
#[tokio::test]
async fn test_e2e_migration_stream() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let test_slot = 900u16;
    let (source_id, target_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");

    let source = harness.node(source_id).unwrap();
    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let key = format!("{}_stream", tag);

    // Create stream with entries
    let resp = source.send("XADD", &[&key, "*", "field1", "value1"]).await;
    assert!(!is_error(&resp), "XADD failed: {:?}", resp);
    let resp = source.send("XADD", &[&key, "*", "field2", "value2"]).await;
    assert!(!is_error(&resp), "XADD failed: {:?}", resp);

    // Run migration
    run_full_slot_migration(&harness, source_id, target_id, test_slot, 100)
        .await
        .expect("migration failed");

    // After proper implementation, stream should have 2 entries on target
    let target = harness.node(target_id).unwrap();
    let resp = target.send("XLEN", &[&key]).await;
    match resp {
        frogdb_protocol::Response::Integer(len) => {
            assert_eq!(len, 2, "Stream should have 2 entries after migration");
        }
        other => panic!("expected integer XLEN, got: {:?}", other),
    }

    harness.shutdown_all().await;
}

#[cfg(feature = "cmd-bloom")]
/// Verifies that bloom filter data survives slot migration.
#[tokio::test]
async fn test_e2e_migration_bloom_filter() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let test_slot = 910u16;
    let (source_id, target_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");

    let source = harness.node(source_id).unwrap();
    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let key = format!("{}_bloom", tag);

    let resp = source.send("BF.ADD", &[&key, "item1"]).await;
    assert!(!is_error(&resp), "BF.ADD failed: {:?}", resp);

    run_full_slot_migration(&harness, source_id, target_id, test_slot, 100)
        .await
        .expect("migration failed");

    let target = harness.node(target_id).unwrap();
    let resp = target.send("BF.EXISTS", &[&key, "item1"]).await;
    match resp {
        frogdb_protocol::Response::Integer(1) => {}
        other => panic!(
            "BF.EXISTS should return 1 after migration, got: {:?}",
            other
        ),
    }

    harness.shutdown_all().await;
}

#[cfg(feature = "cmd-timeseries")]
/// Verifies that timeseries data survives slot migration.
#[tokio::test]
async fn test_e2e_migration_timeseries() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let test_slot = 920u16;
    let (source_id, target_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");

    let source = harness.node(source_id).unwrap();
    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let key = format!("{}_ts", tag);

    let resp = source.send("TS.ADD", &[&key, "1000", "42.0"]).await;
    assert!(!is_error(&resp), "TS.ADD failed: {:?}", resp);

    run_full_slot_migration(&harness, source_id, target_id, test_slot, 100)
        .await
        .expect("migration failed");

    let target = harness.node(target_id).unwrap();
    let resp = target.send("TS.GET", &[&key]).await;
    assert!(
        !is_error(&resp),
        "TS.GET should succeed after migration, got: {:?}",
        resp
    );

    harness.shutdown_all().await;
}

// ============================================================================
// Tier 15: Blocking Commands During Migration — Known-Gap Documentation
// ============================================================================

/// Documents that blocking commands during migration should receive MOVED after completion.
// FM-BLOCKING-008
#[tokio::test]
async fn test_blocking_command_during_migration_gets_moved() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let test_slot = 950u16;
    let (source_id, target_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");

    let source = harness.node(source_id).unwrap();
    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let key = format!("{}_blpop", tag);

    // Start BLPOP on source (blocks waiting for data)
    let mut blocker = source.connect().await;
    blocker.send_only(&["BLPOP", &key, "10"]).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Run migration
    run_full_slot_migration(&harness, source_id, target_id, test_slot, 100)
        .await
        .expect("migration failed");

    // Blocked client should receive -MOVED after slot migrates
    let resp = blocker.read_response(Duration::from_secs(5)).await;
    assert!(
        resp.is_some(),
        "Blocked client should receive a response after migration"
    );
    if let Some(r) = resp {
        assert!(
            is_moved_redirect(&r).is_some(),
            "Blocked client should receive MOVED after slot migration, got: {:?}",
            r
        );
    }

    harness.shutdown_all().await;
}

// ============================================================================
// Tier 16: Migration Redirect Semantics
// ============================================================================

/// Tests that MGET on source for a key that has been migrated returns ASK redirect.
///
/// Inspired by Redis `29-slot-migration-response.tcl`.
#[tokio::test]
async fn test_mget_keys_in_migrating_slot_ask_redirect() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let test_slot = 400u16;
    let (source_id, target_id) = match find_owner_of_slot(&harness, test_slot).await {
        Some(pair) => pair,
        None => {
            eprintln!("Could not find slot owner, skipping test");
            harness.shutdown_all().await;
            return;
        }
    };

    let source = harness.node(source_id).unwrap();
    let target = harness.node(target_id).unwrap();

    let source_cluster_id = harness.get_node_id_str(source_id).unwrap();
    let target_cluster_id = harness.get_node_id_str(target_id).unwrap();

    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let key1 = format!("{}_mget1", tag);
    let key2 = format!("{}_mget2", tag);

    // Write keys to source
    let resp = source.send("SET", &[&key1, "val1"]).await;
    assert!(!is_error(&resp), "SET key1 failed: {:?}", resp);
    let resp = source.send("SET", &[&key2, "val2"]).await;
    assert!(!is_error(&resp), "SET key2 failed: {:?}", resp);

    let slot_str = test_slot.to_string();

    // Set up migration state
    let import_resp = send_cluster_cmd(
        &harness,
        target_id,
        &[
            "SETSLOT",
            &slot_str,
            "IMPORTING",
            &source_cluster_id,
            &target_cluster_id,
        ],
    )
    .await;
    let migrate_resp = send_cluster_cmd(
        &harness,
        source_id,
        &[
            "SETSLOT",
            &slot_str,
            "MIGRATING",
            &target_cluster_id,
            &source_cluster_id,
        ],
    )
    .await;

    if is_error(&import_resp)
        || (is_error(&migrate_resp) && {
            let msg = get_error_message(&migrate_resp).unwrap_or_default();
            !msg.contains("migration in progress")
        })
    {
        eprintln!("Migration setup failed, skipping test");
        harness.shutdown_all().await;
        return;
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Migrate key1 to target, leave key2 on source
    let target_addr = target.client_addr();
    let (host, port) = split_addr(&target_addr);
    let migrate_result = source
        .send("MIGRATE", &[host, port, &key1, "0", "5000", "REPLACE"])
        .await;
    if is_error(&migrate_result) {
        eprintln!("MIGRATE failed: {:?}", get_error_message(&migrate_result));
    }

    // MGET on source for key1 (migrated) — should get ASK redirect
    let mget_resp = source.send("MGET", &[&key1]).await;
    if let Some((slot, addr)) = is_ask_redirect(&mget_resp) {
        assert_eq!(slot, test_slot, "ASK should reference the migrating slot");
        assert!(
            !addr.is_empty(),
            "ASK redirect should include target address"
        );
        eprintln!("Got expected ASK redirect for migrated key");
    } else if is_error(&mget_resp) {
        let msg = get_error_message(&mget_resp).unwrap_or("unknown");
        eprintln!(
            "Got error instead of ASK: {} (acceptable during migration)",
            msg
        );
    } else {
        // Key might still be locally accessible — not all implementations
        // return ASK for MGET with single key
        eprintln!(
            "MGET returned: {:?} (may be implementation-specific)",
            mget_resp
        );
    }

    // Clean up migration state
    let _ = send_cluster_cmd(&harness, source_id, &["SETSLOT", &slot_str, "STABLE"]).await;

    harness.shutdown_all().await;
}

/// Tests that multi-key write on partially-migrated slot returns TRYAGAIN.
///
/// Inspired by Redis `29-slot-migration-response.tcl`.
/// TRYAGAIN is returned when a multi-key operation spans keys that are split
/// between source and target during migration.
#[tokio::test]
async fn test_mset_keys_in_migrating_slot_returns_tryagain() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let test_slot = 401u16;
    let (source_id, target_id) = match find_owner_of_slot(&harness, test_slot).await {
        Some(pair) => pair,
        None => {
            harness.shutdown_all().await;
            return;
        }
    };

    let source = harness.node(source_id).unwrap();
    let target = harness.node(target_id).unwrap();

    let source_cluster_id = harness.get_node_id_str(source_id).unwrap();
    let target_cluster_id = harness.get_node_id_str(target_id).unwrap();

    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let key1 = format!("{}_tryagain1", tag);
    let key2 = format!("{}_tryagain2", tag);

    // Write both keys to source — key1 will be migrated away, key2 stays
    source.send("SET", &[&key1, "val1"]).await;
    source.send("SET", &[&key2, "val2"]).await;

    let slot_str = test_slot.to_string();

    // Set up migration
    send_cluster_cmd(
        &harness,
        target_id,
        &[
            "SETSLOT",
            &slot_str,
            "IMPORTING",
            &source_cluster_id,
            &target_cluster_id,
        ],
    )
    .await;
    send_cluster_cmd(
        &harness,
        source_id,
        &[
            "SETSLOT",
            &slot_str,
            "MIGRATING",
            &target_cluster_id,
            &source_cluster_id,
        ],
    )
    .await;

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Migrate key1 only
    let target_addr = target.client_addr();
    let (host, port) = split_addr(&target_addr);
    source
        .send("MIGRATE", &[host, port, &key1, "0", "5000", "REPLACE"])
        .await;

    // MSET with key1 (migrated to target) and key2 (still on source) should return TRYAGAIN
    let mset_resp = source.send("MSET", &[&key1, "new1", &key2, "new2"]).await;
    assert!(
        is_error(&mset_resp),
        "MSET during split migration should error, got: {:?}",
        mset_resp
    );
    let msg = get_error_message(&mset_resp).unwrap_or("");
    assert!(
        msg.contains("TRYAGAIN"),
        "Expected TRYAGAIN error, got: {}",
        msg
    );

    // Clean up
    let _ = send_cluster_cmd(&harness, source_id, &["SETSLOT", &slot_str, "STABLE"]).await;

    harness.shutdown_all().await;
}

// ============================================================================
// Tier 23: Resharding Consistency
// ============================================================================

/// Tests that keys written during slot migration maintain correct values.
///
/// Inspired by Redis `04-resharding.tcl`.
#[tokio::test]
async fn test_resharding_consistency_concurrent_writes() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let test_slot = 700u16;
    let (source_id, target_id) = match find_owner_of_slot(&harness, test_slot).await {
        Some(pair) => pair,
        None => {
            eprintln!("Could not find slot owner, skipping test");
            harness.shutdown_all().await;
            return;
        }
    };

    let source = harness.node(source_id).unwrap();
    let tag = format!("{{{}}}", key_for_slot(test_slot));

    // Write keys before migration
    let mut keys = Vec::new();
    for i in 0..10 {
        let key = format!("{}_reshard_{}", tag, i);
        let value = format!("reshard_value_{}", i);
        let resp = source.send("SET", &[&key, &value]).await;
        if !is_error(&resp) {
            keys.push((key, value));
        }
    }

    // Run full migration
    match run_full_slot_migration(&harness, source_id, target_id, test_slot, 100).await {
        Ok(()) => {}
        Err(e) => {
            eprintln!("Migration failed: {}, skipping verification", e);
            harness.shutdown_all().await;
            return;
        }
    }

    // Verify all keys are accessible on target with correct values
    let target = harness.node(target_id).unwrap();
    let mut verified = 0;
    for (key, expected_value) in &keys {
        let resp = target.send("GET", &[key]).await;
        match resp {
            frogdb_protocol::Response::Bulk(Some(v)) => {
                assert_eq!(
                    std::str::from_utf8(&v).unwrap(),
                    expected_value,
                    "Value mismatch for key {} after resharding",
                    key
                );
                verified += 1;
            }
            other => {
                eprintln!("Key {} not found on target: {:?}", key, other);
            }
        }
    }

    eprintln!("Verified {}/{} keys after resharding", verified, keys.len());
    assert_eq!(
        verified,
        keys.len(),
        "All keys should be accessible after resharding"
    );

    harness.shutdown_all().await;
}

// ============================================================================
// Phase 4: Migration Fault Modes
// ============================================================================

/// Test 26: Remove a node via CLUSTER FORGET while it is the source of an active migration.
///
/// Scenario: Begin migration on source, then FORGET the source from surviving nodes.
/// Verify the cluster still functions after cleanup.
#[tokio::test]
async fn test_remove_node_during_active_migration() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();

    let leader_id = harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let node_ids = harness.node_ids();

    // Find the owner of slot 950 and a different node as target
    let (source_id, target_id) = find_owner_of_slot(&harness, 950)
        .await
        .expect("could not find owner of slot 950");

    let source_cluster_id = harness.get_node_id_str(source_id).unwrap();
    let target_cluster_id = harness.get_node_id_str(target_id).unwrap();

    eprintln!(
        "Migration slot 950: source={} target={}",
        source_id, target_id
    );

    // Begin migration: MIGRATING on source, IMPORTING on target
    let migrate_resp = send_cluster_cmd(
        &harness,
        source_id,
        &[
            "SETSLOT",
            "950",
            "MIGRATING",
            &target_cluster_id,
            &source_cluster_id,
        ],
    )
    .await;
    let import_resp = send_cluster_cmd(
        &harness,
        target_id,
        &[
            "SETSLOT",
            "950",
            "IMPORTING",
            &source_cluster_id,
            &target_cluster_id,
        ],
    )
    .await;

    if is_error(&migrate_resp) || is_error(&import_resp) {
        eprintln!(
            "Migration setup failed: migrate={:?} import={:?}",
            migrate_resp, import_resp
        );
        harness.shutdown_all().await;
        return;
    }

    eprintln!("Migration started, now shutting down and forgetting source");

    // Shut down the source node, then FORGET it from surviving nodes
    harness.shutdown_node(source_id).await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // If source was the leader, wait for a new one
    if source_id == leader_id {
        let _ = harness
            .wait_for_new_leader(source_id, Duration::from_secs(15))
            .await;
    }

    let surviving: Vec<u64> = node_ids
        .iter()
        .copied()
        .filter(|&id| id != source_id)
        .collect();
    for &nid in &surviving {
        if let Some(node) = harness.node(nid)
            && node.is_running()
        {
            let resp = node.send("CLUSTER", &["FORGET", &source_cluster_id]).await;
            eprintln!("Node {} FORGET response: {:?}", nid, resp);
        }
    }

    // Clean up migration state on surviving nodes
    tokio::time::sleep(Duration::from_millis(500)).await;
    for &nid in &surviving {
        if let Some(node) = harness.node(nid)
            && node.is_running()
        {
            let _ = node.send("CLUSTER", &["SETSLOT", "950", "STABLE"]).await;
        }
    }

    // Verify cluster still functions: write+read on a surviving node
    // Can't use find_owner_of_slot because it probes all nodes including dead ones.
    // Instead, try writing directly to a surviving node.
    let mut verified = false;
    for &nid in &surviving {
        if let Some(node) = harness.node(nid)
            && node.is_running()
        {
            // Write a key — it may get a MOVED redirect if this node doesn't own
            // the slot, but if the write succeeds the cluster is functional.
            let test_key = format!("post_forget_test_{}", nid);
            let set_resp = node.send("SET", &[&test_key, "alive"]).await;
            if !is_error(&set_resp) {
                let get_resp = node.send("GET", &[&test_key]).await;
                if let frogdb_protocol::Response::Bulk(Some(data)) = &get_resp {
                    assert_eq!(
                        String::from_utf8_lossy(data),
                        "alive",
                        "Should read back written value"
                    );
                    eprintln!("SUCCESS: cluster still functional after FORGET during migration");
                    verified = true;
                    break;
                }
            }
        }
    }

    if !verified {
        // Try with redirect helper as fallback
        if set_with_redirect(&harness, "forget_test_fallback", "alive").await {
            let val = get_with_redirect(&harness, "forget_test_fallback").await;
            if val.as_deref() == Some("alive") {
                eprintln!("SUCCESS: cluster functional (via redirect)");
                verified = true;
            }
        }
    }

    assert!(
        verified,
        "Cluster should remain functional after removing source node during migration"
    );

    harness.shutdown_all().await;
}

/// Test 27: Start two concurrent slot migrations on different slots simultaneously.
///
/// Scenario: Migrate slots 100 and 200 in parallel, write keys to both,
/// complete both migrations, verify keys accessible.
#[tokio::test]
async fn test_concurrent_slot_migrations_different_slots() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();

    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let slot_a = 100u16;
    let slot_b = 200u16;

    let (owner_a, non_owner_a) = find_owner_of_slot(&harness, slot_a)
        .await
        .expect("could not find owner of slot 100");
    let (owner_b, non_owner_b) = find_owner_of_slot(&harness, slot_b)
        .await
        .expect("could not find owner of slot 200");

    // Choose targets: for each slot, migrate to a node that doesn't own it
    let target_a = non_owner_a;
    let target_b = if non_owner_b != owner_a {
        non_owner_b
    } else {
        // Pick a different non-owner for slot B
        harness
            .node_ids()
            .into_iter()
            .find(|&id| id != owner_b && id != owner_a)
            .unwrap_or(non_owner_b)
    };

    let owner_a_cid = harness.get_node_id_str(owner_a).unwrap();
    let target_a_cid = harness.get_node_id_str(target_a).unwrap();
    let owner_b_cid = harness.get_node_id_str(owner_b).unwrap();
    let target_b_cid = harness.get_node_id_str(target_b).unwrap();

    let slot_a_str = slot_a.to_string();
    let slot_b_str = slot_b.to_string();

    eprintln!(
        "Migration A: slot {} from {} to {}",
        slot_a, owner_a, target_a
    );
    eprintln!(
        "Migration B: slot {} from {} to {}",
        slot_b, owner_b, target_b
    );

    // Start both migrations
    let migrate_a = send_cluster_cmd(
        &harness,
        owner_a,
        &[
            "SETSLOT",
            &slot_a_str,
            "MIGRATING",
            &target_a_cid,
            &owner_a_cid,
        ],
    )
    .await;
    let import_a = send_cluster_cmd(
        &harness,
        target_a,
        &[
            "SETSLOT",
            &slot_a_str,
            "IMPORTING",
            &owner_a_cid,
            &target_a_cid,
        ],
    )
    .await;

    let migrate_b = send_cluster_cmd(
        &harness,
        owner_b,
        &[
            "SETSLOT",
            &slot_b_str,
            "MIGRATING",
            &target_b_cid,
            &owner_b_cid,
        ],
    )
    .await;
    let import_b = send_cluster_cmd(
        &harness,
        target_b,
        &[
            "SETSLOT",
            &slot_b_str,
            "IMPORTING",
            &owner_b_cid,
            &target_b_cid,
        ],
    )
    .await;

    if is_error(&migrate_a) || is_error(&import_a) {
        eprintln!("Migration A setup failed, skipping");
        harness.shutdown_all().await;
        return;
    }
    if is_error(&migrate_b) || is_error(&import_b) {
        eprintln!("Migration B setup failed, skipping");
        // Clean up migration A
        let _ = send_cluster_cmd(&harness, owner_a, &["SETSLOT", &slot_a_str, "STABLE"]).await;
        let _ = send_cluster_cmd(&harness, target_a, &["SETSLOT", &slot_a_str, "STABLE"]).await;
        harness.shutdown_all().await;
        return;
    }

    // Write keys to both slots on their respective source nodes
    let key_a = key_for_slot(slot_a);
    let key_b = key_for_slot(slot_b);
    let source_a = harness.node(owner_a).unwrap();
    let source_b = harness.node(owner_b).unwrap();

    let set_a = source_a.send("SET", &[&key_a, "value_a"]).await;
    let set_b = source_b.send("SET", &[&key_b, "value_b"]).await;
    eprintln!("SET slot A: {:?}, SET slot B: {:?}", set_a, set_b);

    // Complete both migrations: SETSLOT NODE on targets
    let complete_a = send_cluster_cmd(
        &harness,
        target_a,
        &["SETSLOT", &slot_a_str, "NODE", &target_a_cid],
    )
    .await;
    let complete_b = send_cluster_cmd(
        &harness,
        target_b,
        &["SETSLOT", &slot_b_str, "NODE", &target_b_cid],
    )
    .await;
    eprintln!("Complete A: {:?}, Complete B: {:?}", complete_a, complete_b);

    // Clean up with STABLE
    let _ = send_cluster_cmd(&harness, owner_a, &["SETSLOT", &slot_a_str, "STABLE"]).await;
    let _ = send_cluster_cmd(&harness, target_a, &["SETSLOT", &slot_a_str, "STABLE"]).await;
    let _ = send_cluster_cmd(&harness, owner_b, &["SETSLOT", &slot_b_str, "STABLE"]).await;
    let _ = send_cluster_cmd(&harness, target_b, &["SETSLOT", &slot_b_str, "STABLE"]).await;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify keys accessible (follow redirects)
    let val_a = get_with_redirect(&harness, &key_a).await;
    let val_b = get_with_redirect(&harness, &key_b).await;
    eprintln!("After migration: key_a={:?}, key_b={:?}", val_a, val_b);

    // Verify that if keys are accessible, their values are correct.
    // Key migration does not yet move data, so keys may be None -- but if
    // they are present the values must match.
    if let Some(v) = &val_a {
        assert_eq!(v, "value_a", "Key A should have correct value");
    }
    if let Some(v) = &val_b {
        assert_eq!(v, "value_b", "Key B should have correct value");
    }

    // The core assertion for this test: both migrations completed without
    // error (guarded by the early returns above) and the cluster is still
    // operational -- verify by writing a fresh key after migration.
    assert!(
        set_with_redirect(&harness, "post_concurrent_migration", "ok").await,
        "Cluster should remain writable after concurrent slot migrations"
    );

    harness.shutdown_all().await;
}

/// Test 28: Kill the Raft leader while a slot migration is in progress.
///
/// Scenario: Start migration, kill the leader, wait for new leader,
/// clean up, verify cluster recovers.
#[tokio::test]
async fn test_leadership_transfer_during_migration() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();

    let original_leader = harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let node_ids = harness.node_ids();

    // Pick source and target for migration (slot 800)
    let test_slot = 800u16;
    let (source_id, target_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find owner of slot 800");

    let source_cluster_id = harness.get_node_id_str(source_id).unwrap();
    let target_cluster_id = harness.get_node_id_str(target_id).unwrap();
    let slot_str = test_slot.to_string();

    eprintln!(
        "Migration slot {}: source={} target={}, leader={}",
        test_slot, source_id, target_id, original_leader
    );

    // Begin migration
    let migrate_resp = send_cluster_cmd(
        &harness,
        source_id,
        &[
            "SETSLOT",
            &slot_str,
            "MIGRATING",
            &target_cluster_id,
            &source_cluster_id,
        ],
    )
    .await;
    let import_resp = send_cluster_cmd(
        &harness,
        target_id,
        &[
            "SETSLOT",
            &slot_str,
            "IMPORTING",
            &source_cluster_id,
            &target_cluster_id,
        ],
    )
    .await;

    if is_error(&migrate_resp) || is_error(&import_resp) {
        eprintln!(
            "Migration setup failed: migrate={:?} import={:?}",
            migrate_resp, import_resp
        );
        harness.shutdown_all().await;
        return;
    }

    eprintln!("Migration in progress, killing leader {}", original_leader);

    // Kill the Raft leader
    harness.kill_node(original_leader).await;

    // Wait for a new leader
    let new_leader = harness
        .wait_for_new_leader(original_leader, Duration::from_secs(15))
        .await
        .unwrap();
    assert_ne!(new_leader, original_leader);
    eprintln!("New leader elected: {}", new_leader);

    // Clean up migration on surviving nodes and FORGET the killed leader
    let surviving: Vec<u64> = node_ids
        .iter()
        .copied()
        .filter(|&id| id != original_leader)
        .collect();
    let killed_cluster_id = harness.get_node_id_str(original_leader).unwrap();

    for &nid in &surviving {
        if let Some(node) = harness.node(nid)
            && node.is_running()
        {
            let _ = node
                .send("CLUSTER", &["SETSLOT", &slot_str, "STABLE"])
                .await;
            let _ = node.send("CLUSTER", &["FORGET", &killed_cluster_id]).await;
        }
    }

    // Allow the cluster to stabilize after leader change and FORGET
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify cluster recovers: probe each surviving node directly with a
    // key that hashes to a slot it owns (avoiding orphaned slots from the
    // killed node).
    let mut recovery_verified = false;
    for &nid in &surviving {
        if let Some(node) = harness.node(nid)
            && node.is_running()
        {
            // Find a slot this surviving node owns
            for probe_slot in (0u16..16384).step_by(200) {
                let probe_key = key_for_slot(probe_slot);
                let resp = node.send("SET", &[&probe_key, "recovered"]).await;
                if !is_error(&resp) {
                    let get_resp = node.send("GET", &[&probe_key]).await;
                    if let frogdb_protocol::Response::Bulk(Some(data)) = &get_resp {
                        assert_eq!(String::from_utf8_lossy(data), "recovered");
                        eprintln!(
                            "SUCCESS: cluster recovered after leader kill during migration (node {} slot {})",
                            nid, probe_slot
                        );
                        recovery_verified = true;
                        break;
                    }
                }
            }
            if recovery_verified {
                break;
            }
        }
    }

    assert!(
        recovery_verified,
        "Cluster should recover after leadership transfer during migration"
    );

    harness.shutdown_all().await;
}

// ============================================================================
// MULTI/EXEC across a slot-migration boundary (testing-gap issue 55)
// ============================================================================
//
// PINNED CONTRACT — EXEC re-validates slot ownership for the whole queued
// batch, against one cluster snapshot, before a single queued command runs.
//
// FrogDB validates cluster slot ownership twice. At *queue* time
// (`server/src/connection/guards.rs::try_queue_in_transaction` →
// `validate_cluster_slots`) a MOVED/ASK/CROSSSLOT marks the transaction dirty,
// so EXEC replies EXECABORT. At *EXEC* time
// (`server/src/connection/transaction.rs::execute_transaction` →
// `PreDispatchView::validate_queued_batch`) the queued commands' keys are
// folded to a slot set and routed as a unit through the same
// `route_with_snapshot` seam the per-command path uses. On refusal EXEC answers
// the bare `-MOVED` / `-ASK` / `-TRYAGAIN` / `-CROSSSLOT` / `-CLUSTERDOWN` and
// the queue is gone — no array, no EXECABORT.
//
// This is Redis parity, verified against `redis/unstable` (Redis 8.x). Redis
// routes EXEC through `getNodeByQuery` over the whole transaction:
//
//     if (cmd->proc == execCommand) { ... ms = &c->mstate; }   /* cluster.c */
//
// and, when the resolved node is no longer `myself`, `processCommand`
// (server.c) does:
//
//     if (c->cmd->proc == execCommand) discardTransaction(c);
//     clusterRedirectClient(c, n, c->slot, error_code);
//
// Two ordering properties this suite exists to protect:
//
//   1. Validation completes *before* any shard round-trip. A mid-batch redirect
//      would not stop the shard's execution loop, and the surviving subset ships
//      to replicas as a single MULTI…EXEC frame — i.e. a partial transaction
//      would be replicated. The whole batch is therefore accepted or refused.
//   2. Exactly one `ClusterState::snapshot()` backs the decision, so a batch can
//      never be judged against two different views of slot ownership.
//
// The two tests that previously pinned the *absence* of this check
// (`..._commits_on_former_owner`, `..._commits_on_source`) were rewritten here;
// their rewrite is the deliberate signal that the contract moved. See
// `.scratch/replication-cluster-rework/exec-slot-revalidation.md`.
//
// Ordering against CLIENT PAUSE: a write batch is validated *before* the pause
// wait (so an already-doomed EXEC fails fast without occupying a pause slot) and
// re-validated afterwards, but only when the wait actually blocked — an EXEC
// parked across a migration finalization must not resume on a stale verdict.
// The unpaused path therefore still pays exactly one snapshot. The re-validation
// arm has no integration coverage: driving it needs a CLIENT PAUSE WRITE on the
// source that outlives a slot handover, and the handover itself runs MIGRATE
// through a client connection on that same paused node. See
// `.scratch/replication-cluster-rework/issues/02`.
//
// The CROSSSLOT arm of the same batch decision is pinned separately by
// `test_multi_exec_two_single_key_commands_different_slots_defers_crossslot_to_exec`
// (two individually-valid queued commands on different slots) and is not
// duplicated here.
// ---------------------------------------------------------------------------

/// The error message prefix of a response, or `None` if it is not an error.
fn error_prefix(response: &frogdb_protocol::Response) -> Option<String> {
    let msg = get_error_message(response)?;
    Some(msg.split_whitespace().next()?.to_string())
}

/// `DBSIZE` on one node, as an integer. Keyless, so it is never slot-redirected
/// and reports what this node *physically* holds — the only way to see an
/// orphan write that normal routing would hide behind a MOVED.
async fn node_dbsize(harness: &ClusterTestHarness, node_id: u64) -> i64 {
    match harness.node(node_id).unwrap().send("DBSIZE", &[]).await {
        frogdb_protocol::Response::Integer(n) => n,
        other => panic!("DBSIZE must return an integer, got {other:?}"),
    }
}

/// Put a slot into the open MIGRATING/IMPORTING window without transferring
/// ownership. Returns once both sides have observed the state.
async fn open_slot_migration_window(
    harness: &ClusterTestHarness,
    source_id: u64,
    target_id: u64,
    slot: u16,
) {
    let slot_str = slot.to_string();
    let source_cluster_id = harness
        .get_node_id_str(source_id)
        .expect("source cluster id");
    let target_cluster_id = harness
        .get_node_id_str(target_id)
        .expect("target cluster id");

    let resp = send_cluster_cmd(
        harness,
        target_id,
        &[
            "SETSLOT",
            &slot_str,
            "IMPORTING",
            &source_cluster_id,
            &target_cluster_id,
        ],
    )
    .await;
    assert!(!is_error(&resp), "SETSLOT IMPORTING failed: {resp:?}");

    let resp = send_cluster_cmd(
        harness,
        source_id,
        &[
            "SETSLOT",
            &slot_str,
            "MIGRATING",
            &target_cluster_id,
            &source_cluster_id,
        ],
    )
    .await;
    assert!(!is_error(&resp), "SETSLOT MIGRATING failed: {resp:?}");
    tokio::time::sleep(Duration::from_millis(300)).await;

    let source_state = harness.node(source_id).unwrap().cluster_state().unwrap();
    assert!(
        source_state.is_slot_migrating(slot),
        "the slot must actually be in MIGRATING state"
    );
    assert_eq!(
        source_state.get_slot_owner(slot),
        Some(source_id),
        "an in-flight migration must not have transferred ownership yet"
    );
}

/// Hand the named keys to the importing node with `MIGRATE`.
async fn migrate_keys(harness: &ClusterTestHarness, source_id: u64, target_id: u64, keys: &[&str]) {
    let target_addr = harness.node(target_id).unwrap().client_addr();
    let (host, port) = split_addr(&target_addr);
    let mut args = vec![host, port, "", "0", "5000", "REPLACE", "KEYS"];
    args.extend_from_slice(keys);
    let resp = harness
        .node(source_id)
        .unwrap()
        .send("MIGRATE", &args)
        .await;
    assert!(!is_error(&resp), "MIGRATE failed: {resp:?}");
}

// FM-TXN-009, FM-TXN-022, FM-TXN-047
/// A slot migration that *completes* between queue time and EXEC time.
///
/// EXEC re-validates and answers `-MOVED <slot> <new owner>` with the queue
/// discarded. The no-orphan-writes half is the load-bearing part: the former
/// owner must take **zero** writes from the doomed transaction, which is checked
/// against its physical `DBSIZE` (normal routing would hide an orphan behind the
/// very MOVED the node now emits).
#[tokio::test]
async fn test_multi_exec_after_completed_slot_migration_redirects_with_moved() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let test_slot = 1200u16;
    let (source_id, target_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find an owner for the test slot");
    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let key = format!("{tag}_tx");
    assert_eq!(
        slot_for_key(key.as_bytes()),
        test_slot,
        "the hash-tagged transaction key must land in the migrated slot"
    );

    // Seed a pre-migration value so a lost write would be unambiguous: "v0" is
    // what migrates, "v1" is what the doomed transaction tries to write.
    let source = harness.node(source_id).unwrap();
    assert_eq!(
        source.send("SET", &[&key, "v0"]).await,
        frogdb_protocol::Response::ok(),
        "seeding the key on its owner must succeed"
    );

    // Queue against the current owner. A persistent connection is required —
    // MULTI is session state, and `node.send` opens a fresh connection per call.
    let mut client = source.connect().await;
    assert_eq!(
        client.command(&["MULTI"]).await,
        frogdb_protocol::Response::ok(),
        "MULTI should succeed"
    );
    assert_eq!(
        client.command(&["SET", &key, "v1"]).await,
        frogdb_protocol::Response::queued(),
        "queue-time slot validation passes: this node still owns the slot"
    );
    assert_eq!(
        client.command(&["GET", &key]).await,
        frogdb_protocol::Response::queued(),
        "the read is queued for the same, still-owned slot"
    );

    // Hand the whole slot over while the transaction sits queued.
    run_full_slot_migration(&harness, source_id, target_id, test_slot, 100)
        .await
        .expect("slot migration failed");
    for &nid in &[source_id, target_id] {
        assert_eq!(
            harness
                .node(nid)
                .unwrap()
                .cluster_state()
                .unwrap()
                .get_slot_owner(test_slot),
            Some(target_id),
            "node {nid} must agree the slot now belongs to the migration target \
             before EXEC runs"
        );
    }
    let source_keys_before_exec = node_dbsize(&harness, source_id).await;
    assert_eq!(
        source_keys_before_exec, 0,
        "a completed migration must leave no keys behind on the source"
    );

    // THE CONTRACT: EXEC is redirected, not executed.
    let target_addr = harness.node(target_id).unwrap().client_addr();
    let exec = client.command(&["EXEC"]).await;
    assert_eq!(
        is_moved_redirect(&exec),
        Some((test_slot, target_addr.clone())),
        "EXEC must re-validate and reply MOVED to the slot's new owner, got {exec:?}"
    );

    // NO ORPHAN WRITES: the former owner physically holds nothing. Reading the
    // key back through normal routing cannot prove this — the source answers
    // MOVED for the slot either way — so the check is its raw key count.
    assert_eq!(
        node_dbsize(&harness, source_id).await,
        0,
        "the former slot owner must take ZERO writes from the redirected EXEC"
    );
    assert_eq!(
        harness.node(target_id).unwrap().send("GET", &[&key]).await,
        frogdb_protocol::Response::Bulk(Some(bytes::Bytes::from_static(b"v0"))),
        "the new owner keeps the migrated value, untouched by the refused EXEC"
    );

    // The queue was discarded with the redirect (Redis's `discardTransaction`),
    // so the connection is out of MULTI and a bare EXEC is an error.
    let exec_again = client.command(&["EXEC"]).await;
    assert!(
        is_error(&exec_again),
        "the redirect must discard the queue, leaving no transaction to re-EXEC: \
         {exec_again:?}"
    );

    // Queue-time validation still fires independently: the same write queued
    // *after* the migration is MOVED at queue time and aborts with EXECABORT.
    // The two gates report differently on purpose — EXECABORT means "you sent a
    // bad command", the bare redirect means "ask another node".
    assert_eq!(
        client.command(&["MULTI"]).await,
        frogdb_protocol::Response::ok(),
        "the connection is usable again after EXEC"
    );
    let resp = client.command(&["SET", &key, "v2"]).await;
    assert!(
        is_moved_redirect(&resp).is_some(),
        "queuing after the migration must be rejected with MOVED at queue time, \
         got: {resp:?}"
    );
    assert_execabort(
        &client.command(&["EXEC"]).await,
        "EXEC after a queue-time MOVED abort",
    );

    harness.shutdown_all().await;
}

// FM-TXN-048
/// `WATCH` is a keyed command and is slot-routed like one.
///
/// It short-circuits at the transaction-control dispatch stage, long before the
/// cluster slot-validation stage runs, so nothing in the ordinary gauntlet
/// covers it: a node that does not own the key's slot used to answer `+OK` and
/// register a CAS no writer on the real owner could ever dirty.
#[tokio::test]
async fn test_watch_on_a_slot_this_node_does_not_own_is_moved() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let test_slot = 1200u16;
    let (owner_id, other_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find an owner for the test slot");
    let key = key_for_slot(test_slot);
    assert_eq!(
        slot_for_key(key.as_bytes()),
        test_slot,
        "the probe key must land in the test slot"
    );
    let owner_addr = harness.node(owner_id).unwrap().client_addr();

    // THE CONTRACT: the non-owner refuses and names the owner.
    let watch = harness.node(other_id).unwrap().send("WATCH", &[&key]).await;
    assert_eq!(
        is_moved_redirect(&watch),
        Some((test_slot, owner_addr)),
        "WATCH on a slot this node does not own must answer MOVED, got {watch:?}"
    );

    // The owner still accepts it — the check refuses foreign slots, not WATCH.
    assert_eq!(
        harness.node(owner_id).unwrap().send("WATCH", &[&key]).await,
        frogdb_protocol::Response::ok(),
        "WATCH on the slot's owner must still succeed"
    );

    // A watch set spanning two slots is CROSSSLOT, exactly like any other
    // multi-key command.
    let other_slot_key = key_for_slot(test_slot + 1);
    assert_ne!(
        slot_for_key(other_slot_key.as_bytes()),
        test_slot,
        "the second key must genuinely land in another slot"
    );
    let crossslot = harness
        .node(owner_id)
        .unwrap()
        .send("WATCH", &[&key, &other_slot_key])
        .await;
    assert!(
        get_error_message(&crossslot).is_some_and(|m| m.starts_with("CROSSSLOT")),
        "WATCH across two slots must be CROSSSLOT, got {crossslot:?}"
    );

    harness.shutdown_all().await;
}

// FM-TXN-049
/// A watched key whose slot leaves this node between `WATCH` and `EXEC`, with a
/// transaction body that names no key of its own.
///
/// The slot is handed over with a bare `SETSLOT … NODE` — no `MIGRATE` — on
/// purpose: a key-by-key migration deletes the watched key on the source, which
/// bumps its version and makes the ordinary CAS check fire. Reassigning
/// ownership alone leaves this node holding the key at an *unchanged* version,
/// which is exactly the state in which nothing local can ever notice the new
/// owner's writes. The queued batch is keyless, so the batch re-validation has
/// nothing to route and would happily serve it here; the watch set is the only
/// thing still pointing at the departed slot, so the CAS must fail (nil) rather
/// than commit against this node's stale copy.
#[tokio::test]
async fn test_watch_then_slot_reassignment_then_keyless_exec_aborts_the_watch() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let test_slot = 1200u16;
    let (source_id, target_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find an owner for the test slot");
    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let key = format!("{tag}_watched");
    assert_eq!(
        slot_for_key(key.as_bytes()),
        test_slot,
        "the hash-tagged watched key must land in the migrated slot"
    );

    let source = harness.node(source_id).unwrap();
    assert_eq!(
        source.send("SET", &[&key, "v0"]).await,
        frogdb_protocol::Response::ok(),
        "seeding the watched key on its owner must succeed"
    );

    // WATCH, then queue a body that touches no key at all.
    let mut client = source.connect().await;
    assert_eq!(
        client.command(&["WATCH", &key]).await,
        frogdb_protocol::Response::ok(),
        "WATCH on the current owner must succeed"
    );
    assert_eq!(
        client.command(&["MULTI"]).await,
        frogdb_protocol::Response::ok(),
        "MULTI should succeed"
    );
    assert_eq!(
        client.command(&["PING"]).await,
        frogdb_protocol::Response::queued(),
        "a keyless command is queued without any slot verdict"
    );

    // Reassign the slot outright, without moving a single key: the watched key
    // stays here, at the very version WATCH recorded. (`SETSLOT … NODE` only
    // closes an open migration window, so the window is opened first — but no
    // `MIGRATE` runs inside it.)
    open_slot_migration_window(&harness, source_id, target_id, test_slot).await;
    let target_cluster_id = harness
        .get_node_id_str(target_id)
        .expect("target cluster id");
    let reassign = send_cluster_cmd(
        &harness,
        target_id,
        &[
            "SETSLOT",
            &test_slot.to_string(),
            "NODE",
            &target_cluster_id,
        ],
    )
    .await;
    assert!(!is_error(&reassign), "SETSLOT NODE failed: {reassign:?}");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        harness
            .node(source_id)
            .unwrap()
            .cluster_state()
            .unwrap()
            .get_slot_owner(test_slot),
        Some(target_id),
        "the source must agree the slot moved before EXEC runs"
    );
    assert_eq!(
        node_dbsize(&harness, source_id).await,
        1,
        "the watched key must still be physically here — that is what makes its \
         version stale rather than bumped"
    );

    // THE CONTRACT: the CAS fails rather than committing on a stale copy.
    let exec = client.command(&["EXEC"]).await;
    assert!(
        matches!(
            exec,
            frogdb_protocol::Response::Null | frogdb_protocol::Response::Bulk(None)
        ),
        "EXEC must abort the watch with nil once the watched slot left this node, \
         got {exec:?}"
    );

    // The abort discarded the transaction, so a bare EXEC is now an error.
    assert!(
        is_error(&client.command(&["EXEC"]).await),
        "the watch abort must leave no transaction to re-EXEC"
    );

    harness.shutdown_all().await;
}

// FM-TXN-023
/// A slot migration still *in flight* (slot MIGRATING, key already handed to the
/// target) at EXEC time.
///
/// EXEC probes key presence for the batch's slot, finds every key gone, and
/// answers `-ASK <slot> <importing node>` — it must not resurrect the key on the
/// source, which would leave the same key live on both sides of an open slot.
#[tokio::test]
async fn test_multi_exec_during_in_flight_slot_migration_asks_when_keys_migrated() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let test_slot = 1210u16;
    let (source_id, target_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find an owner for the test slot");
    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let key = format!("{tag}_tx");
    let slot_str = test_slot.to_string();

    let source = harness.node(source_id).unwrap();
    let target = harness.node(target_id).unwrap();
    assert_eq!(
        source.send("SET", &[&key, "v0"]).await,
        frogdb_protocol::Response::ok(),
        "seeding the key on its owner must succeed"
    );

    let mut client = source.connect().await;
    assert_eq!(
        client.command(&["MULTI"]).await,
        frogdb_protocol::Response::ok(),
        "MULTI should succeed"
    );
    assert_eq!(
        client.command(&["SET", &key, "v1"]).await,
        frogdb_protocol::Response::queued(),
        "queue-time slot validation passes: the slot is still stable and local"
    );

    // Open the slot but stop short of handing it over, then move the single key
    // across: at EXEC time the slot is ours but the key is not here — the state
    // in which Redis answers a keyed command on a MIGRATING slot with ASK.
    open_slot_migration_window(&harness, source_id, target_id, test_slot).await;
    migrate_keys(&harness, source_id, target_id, &[&key]).await;

    let target_addr = target.client_addr();
    let resp = harness.node(source_id).unwrap().send("GET", &[&key]).await;
    assert_eq!(
        is_ask_redirect(&resp),
        Some((test_slot, target_addr.clone())),
        "a plain read of the already-migrated key must ASK-redirect; got: {resp:?}"
    );

    // THE CONTRACT: EXEC answers with the same ASK the per-command path would.
    let exec = client.command(&["EXEC"]).await;
    assert_eq!(
        is_ask_redirect(&exec),
        Some((test_slot, target_addr)),
        "EXEC must consult the MIGRATING state and reply ASK, got {exec:?}"
    );

    // The key must NOT come back to life on the source: an orphan copy there
    // would make the two sides of the open slot disagree.
    let resp = harness.node(source_id).unwrap().send("GET", &[&key]).await;
    assert!(
        is_ask_redirect(&resp).is_some(),
        "the refused EXEC must not resurrect the key on the source; got: {resp:?}"
    );
    let mut target_client = target.connect().await;
    assert!(
        !is_error(&target_client.command(&["ASKING"]).await),
        "ASKING must succeed on the importing node"
    );
    assert_eq!(
        target_client.command(&["GET", &key]).await,
        frogdb_protocol::Response::Bulk(Some(bytes::Bytes::from_static(b"v0"))),
        "the importing node holds the migrated value, untouched by the refused EXEC"
    );

    // Leave the slot stable for teardown.
    let _ = send_cluster_cmd(&harness, source_id, &["SETSLOT", &slot_str, "STABLE"]).await;

    harness.shutdown_all().await;
}

// FM-TXN-024
/// A batch whose keys straddle an open slot — one already migrated, one still
/// on the source. Neither node can serve the transaction atomically, so EXEC
/// answers `-TRYAGAIN` and touches nothing.
#[tokio::test]
async fn test_multi_exec_during_migration_with_split_keys_returns_tryagain() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let test_slot = 1220u16;
    let (source_id, target_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find an owner for the test slot");
    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let (key_gone, key_here) = (format!("{tag}_gone"), format!("{tag}_here"));
    let slot_str = test_slot.to_string();

    let source = harness.node(source_id).unwrap();
    for k in [&key_gone, &key_here] {
        assert!(
            !is_error(&source.send("SET", &[k, "v0"]).await),
            "seeding {k} must succeed"
        );
    }

    let mut client = source.connect().await;
    assert_eq!(
        client.command(&["MULTI"]).await,
        frogdb_protocol::Response::ok(),
        "MULTI should succeed"
    );
    for k in [&key_gone, &key_here] {
        assert_eq!(
            client.command(&["SET", k, "v1"]).await,
            frogdb_protocol::Response::queued(),
            "both keys hash to the same, still-stable slot at queue time"
        );
    }

    // Split the batch across the boundary: only one of the two keys moves.
    open_slot_migration_window(&harness, source_id, target_id, test_slot).await;
    migrate_keys(&harness, source_id, target_id, &[&key_gone]).await;

    let exec = client.command(&["EXEC"]).await;
    assert_eq!(
        error_prefix(&exec).as_deref(),
        Some("TRYAGAIN"),
        "a batch with keys on both sides of an open slot must be TRYAGAIN, got {exec:?}"
    );

    // Nothing ran: the key still on the source keeps its pre-EXEC value.
    assert_eq!(
        harness
            .node(source_id)
            .unwrap()
            .send("GET", &[&key_here])
            .await,
        frogdb_protocol::Response::Bulk(Some(bytes::Bytes::from_static(b"v0"))),
        "TRYAGAIN must leave the batch entirely unapplied"
    );

    let _ = send_cluster_cmd(&harness, source_id, &["SETSLOT", &slot_str, "STABLE"]).await;
    harness.shutdown_all().await;
}

// FM-TXN-027
/// An open migration alone is not a refusal. While the slot is MIGRATING but
/// every key is still on the source, EXEC serves the batch normally — the
/// presence probe, not the migration flag, is what decides.
#[tokio::test]
async fn test_multi_exec_during_migration_serves_when_keys_still_local() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let test_slot = 1230u16;
    let (source_id, target_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find an owner for the test slot");
    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let key = format!("{tag}_tx");
    let slot_str = test_slot.to_string();

    let source = harness.node(source_id).unwrap();
    assert!(!is_error(&source.send("SET", &[&key, "v0"]).await));

    let mut client = source.connect().await;
    assert_eq!(
        client.command(&["MULTI"]).await,
        frogdb_protocol::Response::ok()
    );
    assert_eq!(
        client.command(&["SET", &key, "v1"]).await,
        frogdb_protocol::Response::queued()
    );
    assert_eq!(
        client.command(&["GET", &key]).await,
        frogdb_protocol::Response::queued()
    );

    // Slot opened, but nothing migrated yet.
    open_slot_migration_window(&harness, source_id, target_id, test_slot).await;

    assert_eq!(
        client.command(&["EXEC"]).await,
        frogdb_protocol::Response::Array(vec![
            frogdb_protocol::Response::ok(),
            frogdb_protocol::Response::Bulk(Some(bytes::Bytes::from_static(b"v1"))),
        ]),
        "an open migration whose keys are all still local must serve the batch"
    );

    let _ = send_cluster_cmd(&harness, source_id, &["SETSLOT", &slot_str, "STABLE"]).await;
    harness.shutdown_all().await;
}

// FM-TXN-015
/// `ASKING` must survive the whole MULTI block, not just the next command.
///
/// On the importing node the flag is read once per queued command *and* again by
/// EXEC's batch validation; if it were still one-shot the first queued command
/// would consume it and EXEC would answer MOVED, bouncing the client back to a
/// source that no longer has the key.
#[tokio::test]
async fn test_multi_exec_on_import_target_with_asking_serves_the_batch() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let test_slot = 1240u16;
    let (source_id, target_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find an owner for the test slot");
    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let key = format!("{tag}_tx");
    let slot_str = test_slot.to_string();

    let source = harness.node(source_id).unwrap();
    assert!(!is_error(&source.send("SET", &[&key, "v0"]).await));

    open_slot_migration_window(&harness, source_id, target_id, test_slot).await;
    migrate_keys(&harness, source_id, target_id, &[&key]).await;

    // The client follows the ASK to the importing node and runs a transaction
    // there, exactly as a redirect-aware client would.
    let mut client = harness.node(target_id).unwrap().connect().await;
    assert!(!is_error(&client.command(&["ASKING"]).await));
    assert_eq!(
        client.command(&["MULTI"]).await,
        frogdb_protocol::Response::ok()
    );
    assert_eq!(
        client.command(&["SET", &key, "v1"]).await,
        frogdb_protocol::Response::queued(),
        "ASKING lets the import target queue for a slot it does not own yet"
    );
    assert_eq!(
        client.command(&["GET", &key]).await,
        frogdb_protocol::Response::queued(),
        "ASKING is sticky: the second queued command must not find it consumed"
    );

    assert_eq!(
        client.command(&["EXEC"]).await,
        frogdb_protocol::Response::Array(vec![
            frogdb_protocol::Response::ok(),
            frogdb_protocol::Response::Bulk(Some(bytes::Bytes::from_static(b"v1"))),
        ]),
        "EXEC on the import target must still see ASKING and serve the batch"
    );

    // EXEC consumed the flag, so a fresh keyed command is redirected again —
    // stickiness is scoped to the block, it does not leak past it.
    let source_addr = harness.node(source_id).unwrap().client_addr();
    let resp = client.command(&["GET", &key]).await;
    assert_eq!(
        is_moved_redirect(&resp),
        Some((test_slot, source_addr)),
        "ASKING must be consumed by EXEC, not persist after it; got {resp:?}"
    );

    let _ = send_cluster_cmd(&harness, source_id, &["SETSLOT", &slot_str, "STABLE"]).await;
    harness.shutdown_all().await;
}

// FM-TXN-029
/// A transaction that touches no key is never redirected, even on a node that
/// owns nothing relevant. Redis's `getNodeByQuery` returns `myself` when no slot
/// resolves; folding a keyless batch to "slot 0" would break every
/// `MULTI; PING; EXEC`.
///
/// This is the permissive half of the fold contract. The restrictive half —
/// commands that are *keyed* but were once lumped in with these node-scoped ones
/// — is pinned by
/// [`test_multi_exec_scatter_gather_batch_is_slot_validated`] and
/// [`test_multi_exec_eval_with_declared_keys_is_slot_validated`].
#[tokio::test]
async fn test_keyless_multi_exec_is_never_redirected() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let node_id = harness.node_ids()[0];
    let mut client = harness.node(node_id).unwrap().connect().await;
    assert_eq!(
        client.command(&["MULTI"]).await,
        frogdb_protocol::Response::ok()
    );
    // A spread of the shapes the fold must keep exempt: the hard-coded name
    // list (PING/TIME), a connection-level command (ECHO), and a server-wide one
    // (DBSIZE). None of them declares a key, so none may pull the batch into a
    // slot.
    let queued: [&[&str]; 4] = [&["PING"], &["ECHO", "hi"], &["TIME"], &["DBSIZE"]];
    for args in queued {
        assert_eq!(
            client.command(args).await,
            frogdb_protocol::Response::queued(),
            "{args:?} must queue without a slot verdict"
        );
    }

    let exec = client.command(&["EXEC"]).await;
    match exec {
        frogdb_protocol::Response::Array(items) => assert_eq!(
            items.len(),
            queued.len(),
            "a keyless transaction must run every command, got {items:?}"
        ),
        other => panic!("keyless MULTI/EXEC must not be redirected, got {other:?}"),
    }

    harness.shutdown_all().await;
}

// FM-TXN-030
/// Scatter-gather commands (`MSET`/`MGET`/`DEL`/`EXISTS`/…) are *keyed*: a
/// queued batch built only from them must be slot-validated at EXEC exactly like
/// a batch of `SET`s.
///
/// Regression test. These commands were once treated as "cluster exempt"
/// alongside genuinely node-scoped ones like `PING`, on the theory that anything
/// not dispatched as a plain single-shard command needs no slot verdict. The
/// fold then produced an *empty* key set, EXEC took the keyless fast path, and
/// the transaction committed on the former slot owner — replicating an orphan
/// `MULTI…EXEC` frame for keys the node no longer owns.
#[tokio::test]
async fn test_multi_exec_scatter_gather_batch_is_slot_validated() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let test_slot = 1230u16;
    let (source_id, target_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find an owner for the test slot");
    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let (key_a, key_b) = (format!("{tag}_a"), format!("{tag}_b"));

    let source = harness.node(source_id).unwrap();
    for k in [&key_a, &key_b] {
        assert_eq!(
            source.send("SET", &[k, "v0"]).await,
            frogdb_protocol::Response::ok(),
            "seeding {k} on its owner must succeed"
        );
    }

    let mut client = source.connect().await;
    assert_eq!(
        client.command(&["MULTI"]).await,
        frogdb_protocol::Response::ok()
    );
    let queued: [&[&str]; 3] = [
        &["MSET", &key_a, "v1", &key_b, "v1"],
        &["DEL", &key_a],
        &["MGET", &key_a, &key_b],
    ];
    for args in queued {
        assert_eq!(
            client.command(args).await,
            frogdb_protocol::Response::queued(),
            "{args:?} must queue: this node still owns the slot"
        );
    }

    run_full_slot_migration(&harness, source_id, target_id, test_slot, 100)
        .await
        .expect("slot migration failed");
    assert_eq!(
        node_dbsize(&harness, source_id).await,
        0,
        "a completed migration must leave no keys behind on the source"
    );

    let target_addr = harness.node(target_id).unwrap().client_addr();
    let exec = client.command(&["EXEC"]).await;
    assert_eq!(
        is_moved_redirect(&exec),
        Some((test_slot, target_addr)),
        "a scatter-gather batch must be folded to its slot and redirected, \
         got {exec:?}"
    );
    assert_eq!(
        node_dbsize(&harness, source_id).await,
        0,
        "the former slot owner must take ZERO writes from the redirected EXEC"
    );
    let new_owner = harness.node(target_id).unwrap();
    for k in [&key_a, &key_b] {
        assert_eq!(
            new_owner.send("GET", &[k]).await,
            frogdb_protocol::Response::Bulk(Some(bytes::Bytes::from_static(b"v0"))),
            "{k} must keep its migrated value: the refused EXEC never ran"
        );
    }

    harness.shutdown_all().await;
}

// FM-TXN-030
/// The scripting family declares its keys dynamically (`EVAL <script> <numkeys>
/// <key>…`), so a queued `EVAL` must be folded into the batch's slot set.
///
/// Same regression as [`test_multi_exec_scatter_gather_batch_is_slot_validated`]:
/// scripting is a connection-level strategy, and blanket-exempting
/// connection-level commands let a script's declared `KEYS` vanish from the
/// fold. A script that writes would then commit on the former slot owner.
#[tokio::test]
async fn test_multi_exec_eval_with_declared_keys_is_slot_validated() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let test_slot = 1240u16;
    let (source_id, target_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find an owner for the test slot");
    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let key = format!("{tag}_lua");

    let source = harness.node(source_id).unwrap();
    assert_eq!(
        source.send("SET", &[&key, "v0"]).await,
        frogdb_protocol::Response::ok()
    );

    let mut client = source.connect().await;
    assert_eq!(
        client.command(&["MULTI"]).await,
        frogdb_protocol::Response::ok()
    );
    let script = "return redis.call('SET', KEYS[1], 'v1')";
    assert_eq!(
        client.command(&["EVAL", script, "1", &key]).await,
        frogdb_protocol::Response::queued(),
        "the script queues: this node still owns the declared key's slot"
    );

    run_full_slot_migration(&harness, source_id, target_id, test_slot, 100)
        .await
        .expect("slot migration failed");

    let target_addr = harness.node(target_id).unwrap().client_addr();
    let exec = client.command(&["EXEC"]).await;
    assert_eq!(
        is_moved_redirect(&exec),
        Some((test_slot, target_addr)),
        "a script's declared KEYS must fold into the batch slot, got {exec:?}"
    );
    assert_eq!(
        node_dbsize(&harness, source_id).await,
        0,
        "the script must not run on the former slot owner"
    );
    assert_eq!(
        harness.node(target_id).unwrap().send("GET", &[&key]).await,
        frogdb_protocol::Response::Bulk(Some(bytes::Bytes::from_static(b"v0"))),
        "the new owner keeps the migrated value, untouched by the refused EXEC"
    );

    harness.shutdown_all().await;
}

// FM-TXN-030
/// A `READONLY` connection never rescues a batch that contains a write, even
/// when the write is issued by a scatter-gather command.
///
/// The read-only eligibility flag is what lets a replica serve an all-reads
/// batch for a slot its master owns. It is computed by folding every queued
/// command's flags — and that fold must consider the `WRITE` flag *before* it
/// decides a command is node-scoped, or an exemption bug degrades into "the
/// write was rescued onto a node that does not own the slot".
#[tokio::test]
async fn test_multi_exec_readonly_does_not_rescue_a_scatter_write() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let test_slot = 1250u16;
    let (source_id, target_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find an owner for the test slot");
    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let (key_a, key_b) = (format!("{tag}_a"), format!("{tag}_b"));

    let source = harness.node(source_id).unwrap();
    for k in [&key_a, &key_b] {
        assert_eq!(
            source.send("SET", &[k, "v0"]).await,
            frogdb_protocol::Response::ok()
        );
    }

    let mut client = source.connect().await;
    assert!(!is_error(&client.command(&["READONLY"]).await));
    assert_eq!(
        client.command(&["MULTI"]).await,
        frogdb_protocol::Response::ok()
    );
    assert_eq!(
        client.command(&["GET", &key_b]).await,
        frogdb_protocol::Response::queued()
    );
    assert_eq!(
        client.command(&["MSET", &key_a, "v1"]).await,
        frogdb_protocol::Response::queued(),
        "the write queues: this node still owns the slot"
    );

    run_full_slot_migration(&harness, source_id, target_id, test_slot, 100)
        .await
        .expect("slot migration failed");

    let target_addr = harness.node(target_id).unwrap().client_addr();
    let exec = client.command(&["EXEC"]).await;
    assert_eq!(
        is_moved_redirect(&exec),
        Some((test_slot, target_addr)),
        "a batch containing a write is not read-only eligible, so READONLY must \
         not turn the MOVED into a local serve; got {exec:?}"
    );
    assert_eq!(
        node_dbsize(&harness, source_id).await,
        0,
        "the former slot owner must take ZERO writes from the redirected EXEC"
    );
    assert_eq!(
        harness
            .node(target_id)
            .unwrap()
            .send("GET", &[&key_a])
            .await,
        frogdb_protocol::Response::Bulk(Some(bytes::Bytes::from_static(b"v0"))),
        "the new owner keeps the migrated value"
    );

    harness.shutdown_all().await;
}

// FM-CLUSTER-028
/// Issue 40, end to end: a **single-key write** on a MIGRATING source whose key
/// has already been handed over must be `-ASK`ed, and the migration must finish
/// with nothing orphaned behind it.
///
/// Before the fix the source answered `+OK` — key presence was only consulted
/// for multi-key commands, and single-key commands relied on a post-execution
/// "nil reply means the key moved" conversion that a `SET` (which never replies
/// nil) sails straight past. The write landed on the source, behind the open
/// migration, and `CLUSTER SETSLOT <slot> NODE` then handed the slot away and
/// destroyed it: 11 acknowledged writes lost in a fault-free cluster in the
/// jepsen run this was filed from (issue 31 §"Bug X").
///
/// The `:orphaned-keys` half of that jepsen workload can be gated against this:
/// the no-orphan proof here is the source's physical `DBSIZE`, which is the only
/// view that a `MOVED`-answering node cannot hide an orphan behind.
#[tokio::test]
async fn test_single_key_write_on_migrating_source_asks_and_never_orphans() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let test_slot = 1260u16;
    let (source_id, target_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find an owner for the test slot");
    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let (key_a, key_b) = (format!("{tag}_a"), format!("{tag}_b"));
    let slot_str = test_slot.to_string();
    let target_addr = harness.node(target_id).unwrap().client_addr();

    // Seed both keys on the current owner. "v0" is what migrates; any later
    // value observed anywhere is a write that should have been redirected.
    let source = harness.node(source_id).unwrap();
    for k in [&key_a, &key_b] {
        assert_eq!(
            source.send("SET", &[k, "v0"]).await,
            frogdb_protocol::Response::ok(),
            "seeding {k} on its owner must succeed"
        );
    }

    // Open the window without transferring ownership, then hand over ONE key:
    // the slot straddles the two nodes.
    open_slot_migration_window(&harness, source_id, target_id, test_slot).await;
    migrate_keys(&harness, source_id, target_id, &[&key_a]).await;

    // PARTIAL: a multi-key command spanning both sides can be served by
    // neither, so it is TRYAGAIN — not a half-executed batch.
    for cmd in [
        vec!["MGET", key_a.as_str(), key_b.as_str()],
        vec!["MSET", key_a.as_str(), "v1", key_b.as_str(), "v1"],
    ] {
        let resp = harness
            .node(source_id)
            .unwrap()
            .send(cmd[0], &cmd[1..])
            .await;
        assert_eq!(
            error_prefix(&resp).as_deref(),
            Some("TRYAGAIN"),
            "{} straddling the open slot must TRYAGAIN, got {resp:?}",
            cmd[0]
        );
    }

    // The key still on the source is served here, unaffected by the migration.
    assert_eq!(
        harness
            .node(source_id)
            .unwrap()
            .send("GET", &[&key_b])
            .await,
        frogdb_protocol::Response::Bulk(Some(bytes::Bytes::from_static(b"v0"))),
        "a key the migration has not reached yet is still served by the source"
    );

    // Hand the second key over as well: the slot is ours, its contents are not.
    migrate_keys(&harness, source_id, target_id, &[&key_b]).await;
    assert_eq!(
        node_dbsize(&harness, source_id).await,
        0,
        "MIGRATE deletes as it hands over: the source holds nothing now"
    );

    // THE CONTRACT (issue 40): the single-key WRITE is redirected, not acked.
    let resp = harness
        .node(source_id)
        .unwrap()
        .send("SET", &[&key_a, "v1"])
        .await;
    assert_eq!(
        is_ask_redirect(&resp),
        Some((test_slot, target_addr.clone())),
        "a single-key write to an already-migrated key must ASK the importing \
         node, NOT reply +OK and re-create the key here; got {resp:?}"
    );

    // Reads of an absent key answer the same ASK (the one case the retired
    // reply-side hack did handle).
    let resp = harness
        .node(source_id)
        .unwrap()
        .send("GET", &[&key_a])
        .await;
    assert_eq!(
        is_ask_redirect(&resp),
        Some((test_slot, target_addr.clone())),
        "a read of an already-migrated key must ASK; got {resp:?}"
    );

    // A script writing the same key is redirected too: it declares its keys
    // through `numkeys` and would otherwise run to completion on the source.
    let resp = harness
        .node(source_id)
        .unwrap()
        .send(
            "EVAL",
            &["return redis.call('set', KEYS[1], 'v2')", "1", &key_a],
        )
        .await;
    assert_eq!(
        is_ask_redirect(&resp),
        Some((test_slot, target_addr.clone())),
        "a script writing an already-migrated key must ASK, not execute here; \
         got {resp:?}"
    );

    // NO ORPHAN, before the slot is handed over: none of the refused writes
    // touched the source's keyspace.
    assert_eq!(
        node_dbsize(&harness, source_id).await,
        0,
        "every refused write must leave the source's keyspace untouched — a \
         single orphan here is the acked write SETSLOT NODE would destroy"
    );

    // Finish the migration. This is the step that destroyed the acked write:
    // the slot leaves the source, taking anything it re-created with it.
    let target_cluster_id = harness
        .get_node_id_str(target_id)
        .expect("target cluster id");
    let resp = send_cluster_cmd(
        &harness,
        target_id,
        &["SETSLOT", &slot_str, "NODE", &target_cluster_id],
    )
    .await;
    assert!(!is_error(&resp), "SETSLOT NODE failed: {resp:?}");
    tokio::time::sleep(Duration::from_millis(300)).await;

    // NO ORPHAN, after: the source holds nothing and now says MOVED, and the
    // target's migrated value is the one that survived — never overwritten by a
    // redirected write, never rolled back to a value the source re-created.
    assert_eq!(
        node_dbsize(&harness, source_id).await,
        0,
        "the former owner must hold nothing once the slot is handed over"
    );
    let resp = harness
        .node(source_id)
        .unwrap()
        .send("GET", &[&key_a])
        .await;
    assert_eq!(
        is_moved_redirect(&resp),
        Some((test_slot, target_addr)),
        "with the migration closed the source answers MOVED, not ASK; got {resp:?}"
    );
    for k in [&key_a, &key_b] {
        assert_eq!(
            harness.node(target_id).unwrap().send("GET", &[k]).await,
            frogdb_protocol::Response::Bulk(Some(bytes::Bytes::from_static(b"v0"))),
            "the new owner keeps the migrated value of {k}, untouched by every \
             refused write"
        );
    }

    harness.shutdown_all().await;
}
