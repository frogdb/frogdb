//! Cluster slot-routing integration tests: slot ownership and its propagation,
//! MOVED/ASK redirects, READONLY/READWRITE session state, and the CROSSSLOT
//! rejection surface for multi-key, blocking and MULTI/EXEC commands.
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
    assert_exact_error, assert_execabort, find_owner_of_slot, run_full_slot_migration,
    send_cluster_cmd,
};
use frogdb_test_harness::cluster_harness::ClusterTestHarness;
use frogdb_test_harness::cluster_helpers::{
    get_error_message, is_ask_redirect, is_error, is_moved_redirect, key_for_slot,
    parse_cluster_nodes, slot_for_key,
};
use std::time::Duration;

// ============================================================================
// Tier 4: MOVED/ASK Redirect Tests
// ============================================================================

/// Tests that sending a command to a node that doesn't own the slot returns MOVED.
#[tokio::test]
async fn test_moved_redirect_wrong_node() {
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

    // Get slot assignments from CLUSTER NODES
    let node_ids = harness.node_ids();
    let first_node = harness.node(node_ids[0]).unwrap();
    let nodes_response = first_node.send("CLUSTER", &["NODES"]).await;
    let nodes = parse_cluster_nodes(&nodes_response).unwrap();

    // Find a node that owns some slots
    let owner_node = nodes.iter().find(|n| !n.slots.is_empty());
    if owner_node.is_none() {
        // Slots not yet assigned - skip this test
        harness.shutdown_all().await;
        return;
    }
    let owner_node = owner_node.unwrap();

    // Find a node that doesn't own those slots
    let target_slot = owner_node.slots[0].0;
    let non_owner = nodes.iter().find(|n| {
        n.id != owner_node.id
            && !n
                .slots
                .iter()
                .any(|(s, e)| target_slot >= *s && target_slot <= *e)
    });

    if non_owner.is_none() {
        // All nodes own slots containing our target - skip
        harness.shutdown_all().await;
        return;
    }

    // Generate a key that hashes to target_slot
    let key = key_for_slot(target_slot);
    assert_eq!(slot_for_key(key.as_bytes()), target_slot);

    // Find the non-owner node in our harness by matching the address
    let non_owner_addr = &non_owner.unwrap().addr;
    let mut found_node_id = None;
    for &node_id in &node_ids {
        let node = harness.node(node_id).unwrap();
        if node.client_addr() == *non_owner_addr {
            found_node_id = Some(node_id);
            break;
        }
    }

    if let Some(node_id) = found_node_id {
        let node = harness.node(node_id).unwrap();
        let response = node.send("GET", &[&key]).await;

        // Should get a MOVED redirect
        if let Some((slot, addr)) = is_moved_redirect(&response) {
            assert_eq!(slot, target_slot);
            assert!(!addr.is_empty());
        } else if is_error(&response) {
            // Might get CLUSTERDOWN or other error if slots not fully assigned
            let msg = get_error_message(&response).unwrap_or("");
            assert!(
                msg.contains("MOVED") || msg.contains("CLUSTERDOWN"),
                "Expected MOVED or CLUSTERDOWN, got: {}",
                msg
            );
        }
        // If not an error, the node might own the slot (race condition)
    }

    harness.shutdown_all().await;
}

/// Tests that ASK redirect is returned during slot migration.
#[tokio::test]
async fn test_ask_redirect_during_migration() {
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
    let source_node = harness.node(node_ids[0]).unwrap();
    let target_node = harness.node(node_ids[1]).unwrap();

    // Get node IDs via direct state access
    let source_id = harness.get_node_id_str(node_ids[0]).unwrap();
    let target_id = harness.get_node_id_str(node_ids[1]).unwrap();

    // Set slot 100 to MIGRATING state on source
    let migrate_resp = source_node
        .send("CLUSTER", &["SETSLOT", "100", "MIGRATING", &target_id])
        .await;

    // If SETSLOT succeeds, test ASK behavior
    if !is_error(&migrate_resp) {
        // Set slot 100 to IMPORTING state on target
        let import_resp = target_node
            .send("CLUSTER", &["SETSLOT", "100", "IMPORTING", &source_id])
            .await;

        if !is_error(&import_resp) {
            // Generate a key for slot 100
            let key = key_for_slot(100);

            // Try to access the key on the source (migrating) node
            let response = source_node.send("GET", &[&key]).await;

            // Should get ASK redirect if key doesn't exist locally
            if let Some((slot, addr)) = is_ask_redirect(&response) {
                assert_eq!(slot, 100);
                assert!(!addr.is_empty());
            }
            // Key might exist or we might get different error

            // Clean up migration state
            let _ = source_node
                .send("CLUSTER", &["SETSLOT", "100", "STABLE"])
                .await;
            let _ = target_node
                .send("CLUSTER", &["SETSLOT", "100", "STABLE"])
                .await;
        }
    }

    harness.shutdown_all().await;
}

/// Tests that ASKING command allows access to importing node.
#[tokio::test]
async fn test_asking_command_bypasses_migration_check() {
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

    // Determine which node actually owns slot 200 so we use the correct source.
    let first_node = harness.node(node_ids[0]).unwrap();
    let nodes_resp = first_node.send("CLUSTER", &["NODES"]).await;
    let cluster_nodes = parse_cluster_nodes(&nodes_resp).unwrap();
    let source_node_info = cluster_nodes
        .iter()
        .find(|n| n.slots.iter().any(|&(s, e)| s <= 200 && 200 <= e))
        .expect("Some node must own slot 200");
    let source_id = source_node_info.id.clone();

    // Pick a target node that is NOT the slot owner.
    let target_node_id = node_ids
        .iter()
        .find(|&&id| harness.get_node_id_str(id).unwrap() != source_id)
        .copied()
        .expect("Need a node that is not the slot owner");
    let target_node = harness.node(target_node_id).unwrap();

    // Set slot 200 to IMPORTING state on target
    let import_resp = target_node
        .send("CLUSTER", &["SETSLOT", "200", "IMPORTING", &source_id])
        .await;

    if !is_error(&import_resp) {
        let key = key_for_slot(200);

        // Wait for the IMPORTING state to be visible on the target node.
        // We probe with ASKING + GET: once IMPORTING has propagated, the
        // ASKING flag lets the GET through without a redirect.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let mut probe_client = target_node.connect().await;
            let _ = probe_client.command(&["ASKING"]).await;
            let probe = probe_client.command(&["GET", &key]).await;
            let is_redirect = if is_error(&probe) {
                let msg = get_error_message(&probe).unwrap_or("");
                msg.starts_with("MOVED") || msg.starts_with("ASK")
            } else {
                false
            };
            if !is_redirect {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("SETSLOT IMPORTING did not propagate within 5 seconds");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // First, try without ASKING - should get redirect or error
        let _response_without_asking = target_node.send("GET", &[&key]).await;

        // Now connect and use ASKING + GET in sequence
        let mut client = target_node.connect().await;

        // Send ASKING
        let asking_resp = client.command(&["ASKING"]).await;
        assert!(!is_error(&asking_resp), "ASKING should succeed");

        // Immediately send GET - should be allowed
        let get_resp = client.command(&["GET", &key]).await;

        // The GET should either succeed (key not found = nil) or work without redirect
        // It should NOT return a redirect error after ASKING
        if is_error(&get_resp) {
            let msg = get_error_message(&get_resp).unwrap_or("");
            // Should not be a MOVED/ASK redirect
            assert!(
                !msg.starts_with("MOVED") && !msg.starts_with("ASK"),
                "Got redirect after ASKING: {}",
                msg
            );
        }

        // Clean up
        let _ = target_node
            .send("CLUSTER", &["SETSLOT", "200", "STABLE"])
            .await;
    }

    harness.shutdown_all().await;
}

/// Tests that ASKING flag is cleared after a single command.
#[tokio::test]
async fn test_asking_flag_cleared_after_use() {
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
    let target_node = harness.node(node_ids[1]).unwrap();

    // Get source node ID via direct state access
    let source_id = harness.get_node_id_str(node_ids[0]).unwrap();

    // Set slot 300 to IMPORTING on target
    let import_resp = target_node
        .send("CLUSTER", &["SETSLOT", "300", "IMPORTING", &source_id])
        .await;

    if !is_error(&import_resp) {
        let key = key_for_slot(300);

        let mut client = target_node.connect().await;

        // Send ASKING
        let asking_resp = client.command(&["ASKING"]).await;
        assert!(!is_error(&asking_resp));

        // First command after ASKING should work
        let first_resp = client.command(&["GET", &key]).await;
        // This should succeed (or return nil for non-existent key)

        // Second command should NOT have ASKING flag
        let second_resp = client.command(&["GET", &key]).await;

        // The second command might get redirected since ASKING was cleared
        // (behavior depends on whether the target owns the slot or not)
        // We're mainly verifying that ASKING doesn't persist

        // If first succeeded and second got redirect, flag was cleared correctly
        if !is_error(&first_resp) && is_error(&second_resp) {
            let msg = get_error_message(&second_resp).unwrap_or("");
            // Second command should potentially get a redirect
            // (if node doesn't own slot without ASKING)
            if msg.starts_with("MOVED") || msg.starts_with("ASK") {
                // This is expected - flag was cleared
            }
        }

        // Clean up
        let _ = target_node
            .send("CLUSTER", &["SETSLOT", "300", "STABLE"])
            .await;
    }

    harness.shutdown_all().await;
}

// ============================================================================
// PHASE 4: Client Redirect Tests
// ============================================================================

/// Test 13: Client follows MOVED redirect.
///
/// Scenario: Client gets MOVED, follows to correct node
#[tokio::test]
async fn test_client_follows_moved_redirect() {
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
    let first_node = harness.node(node_ids[0]).unwrap();

    // Try different keys until we get a MOVED redirect
    let mut found_redirect = false;

    for slot in 0..100u16 {
        let key = key_for_slot(slot);
        let resp = first_node.send("GET", &[&key]).await;

        if let Some((moved_slot, target_addr)) = is_moved_redirect(&resp) {
            eprintln!("Got MOVED for slot {} to {}", moved_slot, target_addr);

            // Find the target node and follow redirect
            for &node_id in &node_ids {
                if let Some(node) = harness.node(node_id)
                    && node.client_addr() == target_addr
                {
                    // Write data via the correct node
                    let set_resp = node.send("SET", &[&key, "test_value"]).await;
                    eprintln!("SET via redirect target: {:?}", set_resp);

                    if !is_error(&set_resp) {
                        // Read back
                        let get_resp = node.send("GET", &[&key]).await;
                        eprintln!("GET via redirect target: {:?}", get_resp);

                        if let frogdb_protocol::Response::Bulk(Some(v)) = &get_resp {
                            let retrieved = String::from_utf8_lossy(v);
                            assert_eq!(retrieved, "test_value");
                            found_redirect = true;
                        }
                    }
                    break;
                }
            }
            break;
        }
    }

    if !found_redirect {
        eprintln!("No MOVED redirect encountered (all slots may be on first node)");
    }

    harness.shutdown_all().await;
}

/// Test 14: Client follows ASK redirect.
///
/// Scenario: Client gets ASK during migration, follows correctly
#[tokio::test]
async fn test_client_follows_ask_redirect() {
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

    // Set up migration for slot 700
    let test_slot = 700u16;
    let test_key = key_for_slot(test_slot);

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

    // Try to access key - may get ASK redirect
    let resp = source.send("GET", &[&test_key]).await;
    eprintln!("GET during migration: {:?}", resp);

    if let Some((ask_slot, ask_addr)) = is_ask_redirect(&resp) {
        eprintln!("Got ASK for slot {} to {}", ask_slot, ask_addr);

        // Follow ASK redirect: send ASKING, then command
        for &node_id in &node_ids {
            if let Some(node) = harness.node(node_id)
                && node.client_addr() == ask_addr
            {
                let mut client = node.connect().await;

                // Send ASKING
                let asking_resp = client.command(&["ASKING"]).await;
                assert!(!is_error(&asking_resp), "ASKING should succeed");

                // Now send the actual command
                let get_resp = client.command(&["GET", &test_key]).await;
                eprintln!("GET after ASKING: {:?}", get_resp);
                break;
            }
        }
    }

    // Clean up
    let _ = source
        .send("CLUSTER", &["SETSLOT", &test_slot.to_string(), "STABLE"])
        .await;
    let _ = target
        .send("CLUSTER", &["SETSLOT", &test_slot.to_string(), "STABLE"])
        .await;

    harness.shutdown_all().await;
}

/// Test 15: Redirect loop detection.
///
/// Scenario: Detect and handle redirect loops (max redirects)
#[tokio::test]
async fn test_redirect_loop_detection() {
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
    let max_redirects = 16;
    let test_key = "redirect_loop_test";

    let mut current_node_id = node_ids[0];
    let mut redirect_count = 0;
    let mut visited_nodes: std::collections::HashSet<String> = std::collections::HashSet::new();

    while redirect_count < max_redirects {
        if let Some(node) = harness.node(current_node_id) {
            let addr = node.client_addr();
            if visited_nodes.contains(&addr) {
                eprintln!(
                    "Detected redirect loop at {} after {} redirects",
                    addr, redirect_count
                );
                break;
            }
            visited_nodes.insert(addr.clone());

            let resp = node.send("GET", &[test_key]).await;

            if let Some((_slot, target_addr)) = is_moved_redirect(&resp) {
                redirect_count += 1;
                eprintln!("Redirect {} to {}", redirect_count, target_addr);

                // Find node with target address
                let mut found = false;
                for &nid in &node_ids {
                    if let Some(n) = harness.node(nid)
                        && n.client_addr() == target_addr
                    {
                        current_node_id = nid;
                        found = true;
                        break;
                    }
                }
                if !found {
                    eprintln!("Target node {} not found", target_addr);
                    break;
                }
            } else {
                // No redirect, we're done
                eprintln!("Command succeeded after {} redirects", redirect_count);
                break;
            }
        } else {
            break;
        }
    }

    if redirect_count >= max_redirects {
        eprintln!("WARNING: Reached max redirects ({})", max_redirects);
    }

    // Ensure we don't get stuck in infinite loops
    assert!(
        redirect_count < max_redirects,
        "Should not exceed max redirects"
    );

    harness.shutdown_all().await;
}

// ============================================================================
// Tier 13: READONLY / READWRITE State Verification
// ============================================================================

/// Tests that READONLY allows reads on a non-owner node instead of MOVED.
///
/// Currently the readonly flag is stored in connection state (dispatch.rs:335)
/// but validate_cluster_slots() in guards.rs doesn't consult it — it always
/// returns MOVED for non-owned slots regardless of the READONLY flag.
#[tokio::test]
async fn test_readonly_allows_reads_on_non_owner_node() {
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
    let _ = harness
        .wait_for_address_convergence(Duration::from_secs(5))
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let test_slot = 100u16;
    let (owner_id, non_owner_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");

    // Write a key on the owner
    let owner = harness.node(owner_id).unwrap();
    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let key = format!("{}_ro", tag);
    let resp = owner.send("SET", &[&key, "readonly_test"]).await;
    assert!(!is_error(&resp), "SET failed: {:?}", resp);

    // Without READONLY, non-owner returns MOVED
    let non_owner = harness.node(non_owner_id).unwrap();
    let resp = non_owner.send("GET", &[&key]).await;
    assert!(
        is_moved_redirect(&resp).is_some(),
        "Non-owner should return MOVED without READONLY, got: {:?}",
        resp
    );

    // Enable READONLY on a dedicated connection
    let mut client = non_owner.connect().await;
    let ro_resp = client.command(&["READONLY"]).await;
    assert!(!is_error(&ro_resp), "READONLY should succeed");

    // After READONLY, reads should NOT return MOVED
    let resp = client.command(&["GET", &key]).await;
    assert!(
        is_moved_redirect(&resp).is_none(),
        "After READONLY, non-owner should not return MOVED, got: {:?}",
        resp
    );

    harness.shutdown_all().await;
}

/// Tests that READWRITE restores MOVED redirects after READONLY.
#[tokio::test]
async fn test_readwrite_restores_moved_redirects() {
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
    let _ = harness
        .wait_for_address_convergence(Duration::from_secs(5))
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let test_slot = 100u16;
    let pair = find_owner_of_slot(&harness, test_slot).await;
    if pair.is_none() {
        eprintln!(
            "Slot {} not served yet — skipping READWRITE test",
            test_slot
        );
        harness.shutdown_all().await;
        return;
    }
    let (_owner_id, non_owner_id) = pair.unwrap();

    let non_owner = harness.node(non_owner_id).unwrap();
    let mut client = non_owner.connect().await;

    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let key = format!("{}_rw", tag);

    // Enable READONLY — should suppress MOVED
    client.command(&["READONLY"]).await;
    let resp = client.command(&["GET", &key]).await;
    assert!(
        is_moved_redirect(&resp).is_none(),
        "After READONLY, should not get MOVED"
    );

    // Disable with READWRITE
    let rw_resp = client.command(&["READWRITE"]).await;
    assert!(!is_error(&rw_resp), "READWRITE should succeed");

    // Should get MOVED again
    let resp = client.command(&["GET", &key]).await;
    assert!(
        is_moved_redirect(&resp).is_some(),
        "After READWRITE, should get MOVED again, got: {:?}",
        resp
    );

    harness.shutdown_all().await;
}

// ============================================================================
// Tier 19: MULTI-EXEC on Replica with READONLY
// ============================================================================

// FM-TXN-028
/// Tests that READONLY + MULTI + GETs + EXEC succeeds on a non-owner node.
///
/// Inspired by Redis `16-transactions-on-replica.tcl`.
#[tokio::test]
async fn test_multi_exec_reads_succeed_on_replica_with_readonly() {
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

    let test_slot = 450u16;
    let (owner_id, non_owner_id) = match find_owner_of_slot(&harness, test_slot).await {
        Some(pair) => pair,
        None => {
            harness.shutdown_all().await;
            return;
        }
    };

    // Write a key on the owner
    let key = key_for_slot(test_slot);
    let owner = harness.node(owner_id).unwrap();
    let resp = owner.send("SET", &[&key, "readonly_test"]).await;
    assert!(!is_error(&resp), "SET failed: {:?}", resp);

    // On non-owner: READONLY → MULTI → GET → EXEC
    let non_owner = harness.node(non_owner_id).unwrap();
    let mut client = non_owner.connect().await;

    let readonly_resp = client.command(&["READONLY"]).await;
    assert!(!is_error(&readonly_resp), "READONLY should succeed");

    let multi_resp = client.command(&["MULTI"]).await;
    assert!(!is_error(&multi_resp), "MULTI should succeed");

    let queued_resp = client.command(&["GET", &key]).await;
    // Should return QUEUED
    assert!(
        matches!(&queued_resp, frogdb_protocol::Response::Simple(s) if s.as_ref() == b"QUEUED"),
        "GET inside MULTI should return QUEUED, got: {:?}",
        queued_resp
    );

    let exec_resp = client.command(&["EXEC"]).await;
    // EXEC should return array with the GET result (not EXECABORT)
    assert!(
        matches!(&exec_resp, frogdb_protocol::Response::Array(_)),
        "EXEC should return array result for READONLY reads, got: {:?}",
        exec_resp
    );

    harness.shutdown_all().await;
}

// FM-TXN-009
/// Tests that a write inside MULTI on a READONLY node returns MOVED/EXECABORT.
///
/// Inspired by Redis `16-transactions-on-replica.tcl`.
#[tokio::test]
async fn test_multi_exec_write_inside_readonly_session_returns_moved() {
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

    let test_slot = 451u16;
    let (_owner_id, non_owner_id) = match find_owner_of_slot(&harness, test_slot).await {
        Some(pair) => pair,
        None => {
            harness.shutdown_all().await;
            return;
        }
    };

    let key = key_for_slot(test_slot);
    let non_owner = harness.node(non_owner_id).unwrap();
    let mut client = non_owner.connect().await;

    let readonly_resp = client.command(&["READONLY"]).await;
    assert!(!is_error(&readonly_resp));

    let multi_resp = client.command(&["MULTI"]).await;
    assert!(!is_error(&multi_resp));

    // READONLY only rescues reads. A queued write on a slot this node does not
    // own is MOVED at queue time, which flags the transaction dirty.
    let queued_resp = client.command(&["SET", &key, "should_fail"]).await;
    assert!(
        is_moved_redirect(&queued_resp).is_some(),
        "a write on a foreign slot must be MOVED even in READONLY mode, got: {:?}",
        queued_resp
    );

    let exec_resp = client.command(&["EXEC"]).await;
    assert_execabort(&exec_resp, "EXEC after a READONLY-mode write was rejected");

    harness.shutdown_all().await;
}

// FM-TXN-028
/// READONLY eligibility is folded across the *whole* batch, not per command: a
/// transaction of reads on a foreign slot serves locally, but adding a single
/// write to the same batch makes the whole thing MOVED. Anything else would let
/// a write land on a node that does not own the slot.
#[tokio::test]
async fn test_multi_exec_readonly_batch_eligibility_is_all_or_nothing() {
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

    let test_slot = 452u16;
    let (owner_id, non_owner_id) = match find_owner_of_slot(&harness, test_slot).await {
        Some(pair) => pair,
        None => {
            harness.shutdown_all().await;
            return;
        }
    };

    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let (key_a, key_b) = (format!("{tag}_a"), format!("{tag}_b"));
    let owner = harness.node(owner_id).unwrap();
    for k in [&key_a, &key_b] {
        assert!(!is_error(&owner.send("SET", &[k, "v0"]).await));
    }

    let non_owner = harness.node(non_owner_id).unwrap();
    let mut client = non_owner.connect().await;
    assert!(!is_error(&client.command(&["READONLY"]).await));

    // All-reads batch on a slot we do not own: served locally.
    assert!(!is_error(&client.command(&["MULTI"]).await));
    for k in [&key_a, &key_b] {
        assert_eq!(
            client.command(&["GET", k]).await,
            frogdb_protocol::Response::queued()
        );
    }
    let exec = client.command(&["EXEC"]).await;
    assert!(
        matches!(&exec, frogdb_protocol::Response::Array(items) if items.len() == 2),
        "a READONLY all-reads batch must be served locally, got {exec:?}"
    );

    // Same batch plus one write: no longer read-only, so the whole batch is
    // redirected. The write must never be the thing that runs locally.
    assert!(!is_error(&client.command(&["MULTI"]).await));
    assert_eq!(
        client.command(&["GET", &key_a]).await,
        frogdb_protocol::Response::queued()
    );
    let queued_write = client.command(&["SET", &key_b, "v1"]).await;
    assert!(
        is_moved_redirect(&queued_write).is_some(),
        "the queued write must be MOVED, got {queued_write:?}"
    );
    assert_execabort(
        &client.command(&["EXEC"]).await,
        "EXEC for a READONLY batch containing a write",
    );

    harness.shutdown_all().await;
}

// ============================================================================
// Category B: Slot Ownership & Gossip
// ============================================================================

/// SETSLOT NODE propagates to all nodes.
#[tokio::test]
async fn test_slot_ownership_propagates_to_all_nodes() {
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

    // Run full migration (includes SETSLOT NODE)
    run_full_slot_migration(&harness, source_id, target_id, test_slot, 100)
        .await
        .expect("migration failed");

    // Allow Raft propagation
    tokio::time::sleep(Duration::from_millis(500)).await;

    // All nodes should see the target as the new owner
    let target_cluster_id = harness.get_node_id_str(target_id).unwrap();
    for &nid in &harness.node_ids() {
        let node = harness.node(nid).unwrap();
        let nodes_resp = node.send("CLUSTER", &["NODES"]).await;
        let nodes = parse_cluster_nodes(&nodes_resp).unwrap();

        // Find which node owns the test_slot
        let owner = nodes.iter().find(|n| {
            n.slots
                .iter()
                .any(|&(start, end)| test_slot >= start && test_slot <= end)
        });

        assert!(
            owner.is_some(),
            "Node {} should see an owner for slot {}",
            nid,
            test_slot
        );
        assert_eq!(
            owner.unwrap().id,
            target_cluster_id,
            "Node {} should see target as owner of slot {}",
            nid,
            test_slot
        );
    }

    harness.shutdown_all().await;
}

/// After slot transfer, source's COUNTKEYSINSLOT drops to 0.
#[tokio::test]
async fn test_slot_ownership_transfer_clears_source_keys() {
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

    let test_slot = 450u16;
    let (source_id, target_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");

    // Write keys to the slot on the source
    let source = harness.node(source_id).unwrap();
    let tag = format!("{{{}}}", key_for_slot(test_slot));
    for i in 0..5 {
        let key = format!("{}_tc{}", tag, i);
        source.send("SET", &[&key, "v"]).await;
    }

    // Verify source has keys
    let slot_str = test_slot.to_string();
    let pre_count = source
        .send("CLUSTER", &["COUNTKEYSINSLOT", &slot_str])
        .await;
    if let frogdb_protocol::Response::Integer(n) = &pre_count {
        assert_eq!(*n, 5, "Source should have 5 keys before migration");
    }

    // Migrate
    run_full_slot_migration(&harness, source_id, target_id, test_slot, 100)
        .await
        .expect("migration failed");

    // Source's count should be 0 (keys were migrated away)
    let source = harness.node(source_id).unwrap();
    let post_count = source
        .send("CLUSTER", &["COUNTKEYSINSLOT", &slot_str])
        .await;
    if let frogdb_protocol::Response::Integer(n) = &post_count {
        assert_eq!(*n, 0, "Source should have 0 keys after migration");
    }

    harness.shutdown_all().await;
}

/// Key with TTL in migrating slot returns ASK redirect (not stale data).
#[tokio::test]
async fn test_expired_key_during_migration_returns_ask() {
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

    let test_slot = 550u16;
    let (source_id, target_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");

    let source = harness.node(source_id).unwrap();

    let source_cluster_id = harness.get_node_id_str(source_id).unwrap();
    let target_cluster_id = harness.get_node_id_str(target_id).unwrap();

    // Write a key with TTL
    let key = format!("{{{}}}_ttlkey", key_for_slot(test_slot));
    source.send("SET", &[&key, "ttlval"]).await;
    source.send("PEXPIRE", &[&key, "60000"]).await;

    let slot_str = test_slot.to_string();

    // Start migration (IMPORTING + MIGRATING) but don't finish
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
    assert!(
        !is_error(&import_resp),
        "SETSLOT IMPORTING failed: {:?}",
        import_resp
    );

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Access the key on the source during migration.
    // Source still owns the key, so it should return the value
    // OR issue an ASK redirect. Either is acceptable — the key is not lost.
    let get_resp = source.send("GET", &[&key]).await;
    let is_accessible = matches!(&get_resp, frogdb_protocol::Response::Bulk(Some(_)));
    let is_ask = is_ask_redirect(&get_resp).is_some();
    assert!(
        is_accessible || is_ask,
        "Key during migration should be accessible or ASK redirected, got: {:?}",
        get_resp
    );

    // Clean up: finish migration to avoid dangling state
    let _ = send_cluster_cmd(&harness, target_id, &["SETSLOT", &slot_str, "STABLE"]).await;

    harness.shutdown_all().await;
}

// ============================================================================
// Category C: Multi-key & Cross-slot Errors
// ============================================================================

/// MSET with keys in different slots returns CROSSSLOT error.
#[tokio::test]
async fn test_mset_cross_slot_returns_crossslot_error() {
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

    // Use keys that hash to different slots (no hash tags)
    let node = harness.node(harness.node_ids()[0]).unwrap();
    let resp = node.send("MSET", &["key_a", "val1", "key_b", "val2"]).await;

    // Should get CROSSSLOT or MOVED error
    assert!(
        is_error(&resp),
        "MSET with cross-slot keys should error, got: {:?}",
        resp
    );
    let msg = get_error_message(&resp).unwrap_or_default();
    let is_crossslot = msg.contains("CROSSSLOT");
    let is_moved = msg.starts_with("MOVED");
    assert!(
        is_crossslot || is_moved,
        "Expected CROSSSLOT or MOVED error, got: {}",
        msg
    );

    harness.shutdown_all().await;
}

/// MGET with hash-tagged keys in same slot succeeds.
#[tokio::test]
async fn test_mget_same_slot_succeeds() {
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
    let (owner_id, _) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");
    let owner = harness.node(owner_id).unwrap();

    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let k1 = format!("{}_mg1", tag);
    let k2 = format!("{}_mg2", tag);
    let k3 = format!("{}_mg3", tag);

    owner.send("SET", &[&k1, "v1"]).await;
    owner.send("SET", &[&k2, "v2"]).await;
    owner.send("SET", &[&k3, "v3"]).await;

    let resp = owner.send("MGET", &[&k1, &k2, &k3]).await;
    assert!(
        !is_error(&resp),
        "MGET with same-slot keys should succeed, got: {:?}",
        resp
    );

    if let frogdb_protocol::Response::Array(arr) = &resp {
        assert_eq!(arr.len(), 3, "MGET should return 3 values");
    } else {
        panic!("Expected array from MGET, got: {:?}", resp);
    }

    harness.shutdown_all().await;
}

/// EVAL with KEYS in different slots returns error.
#[tokio::test]
async fn test_eval_cross_slot_returns_error() {
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

    let node = harness.node(harness.node_ids()[0]).unwrap();
    // Keys without hash tags will likely land in different slots
    let resp = node
        .send("EVAL", &["return 1", "2", "cross_key_a", "cross_key_b"])
        .await;

    assert!(
        is_error(&resp),
        "EVAL with cross-slot keys should error, got: {:?}",
        resp
    );

    harness.shutdown_all().await;
}

// ---------------------------------------------------------------------------
// Cluster cross-slot MULTI/EXEC contract (testing-gap issue #49, tightening
// issue #19's cross-shard MULTI coverage to the *cluster*-mode queue-time
// path). FrogDB has two independent, differently-shaped cross-slot rejections
// for a MULTI transaction, and this section pins both exactly rather than
// accepting "an error occurs somewhere":
//
// 1. Queue-time abort (`PreDispatchView::try_queue_in_transaction` ->
//    `validate_cluster_slots`, guards.rs:454-461): fires when a SINGLE queued
//    command's OWN keys already span two slots (e.g. RENAME src dst). The
//    CROSSSLOT error is returned immediately as that command's reply (never
//    `+QUEUED`), `abort_transaction` is set, and the subsequent EXEC returns
//    `EXECABORT` without touching the shard.
// 2. Deferred EXEC-time rejection (`TxnSlotAccumulator`/`TransactionTarget`,
//    state.rs): fires when two *separate* single-key commands queue keys that
//    individually validate fine but land in different slots. Both commands
//    get `+QUEUED` (queuing never compares a command's keys against
//    previously queued commands), and the mismatch is only caught when EXEC
//    folds the queue and calls `TransactionTarget::resolve()`, which returns a
//    plain `CROSSSLOT` reply -- NOT `EXECABORT` (`exec_abort` is never set on
//    this path; nothing was aborted, EXEC itself declined to run).
//
// These are verified empirically (not inferred from reading the code): see
// the two tests below. A regression that merged these two paths, or that
// moved either detection point, will now fail a concrete assertion.
// ---------------------------------------------------------------------------

/// Find a node this harness can connect to directly, plus two distinct
/// cluster slots that node owns (`(node_id, slot_a, slot_b)`). Locating an
/// owning node up front (rather than connecting to `node_ids()[0]` and hoping
/// for the best, as the original weak test did) means both `slot_a` and
/// `slot_b` are guaranteed to be served locally: any CROSSSLOT this test
/// observes is the slot-mismatch check, never a MOVED redirect in disguise.
async fn owner_node_with_two_slots(harness: &ClusterTestHarness) -> (u64, u16, u16) {
    let node_ids = harness.node_ids();
    let first_node = harness.node(node_ids[0]).unwrap();
    let nodes_response = first_node.send("CLUSTER", &["NODES"]).await;
    let nodes = parse_cluster_nodes(&nodes_response).unwrap();
    let owner = nodes
        .iter()
        .find(|n| !n.slots.is_empty() && n.slots[0].1 > n.slots[0].0 + 1)
        .expect("expected some node to own a contiguous range of >= 2 slots");
    let (start, _end) = owner.slots[0];
    let (slot_a, slot_b) = (start, start + 1);

    for &nid in &node_ids {
        if harness.node(nid).unwrap().client_addr() == owner.addr {
            return (nid, slot_a, slot_b);
        }
    }
    panic!(
        "could not match CLUSTER NODES owner addr {} to a harness node id",
        owner.addr
    );
}

/// Assert a response is the exact `CROSSSLOT` wire error (not merely an error,
/// not a prefix match). Mirrors the `assert_crossslot` helper introduced in
/// `integration_transactions.rs` (testing-gap issue #19); duplicated rather
/// than shared because every `tests/*.rs` file compiles as its own
/// independent test binary.
fn assert_crossslot(response: &frogdb_protocol::Response, context: &str) {
    assert_exact_error(
        response,
        b"CROSSSLOT Keys in request don't hash to the same slot",
        context,
    );
}

// FM-TXN-004, FM-TXN-010
/// Queue-time CROSSSLOT + EXECABORT: a command whose own two keys (RENAME's
/// source/destination) span different slots is rejected immediately as its
/// queue reply -- it never becomes `+QUEUED` -- and the transaction is marked
/// aborted so EXEC returns `EXECABORT` without running anything. A same-slot
/// command queued beforehand still gets a plain `+QUEUED` reply, proving the
/// rejection is specifically about slot-crossing detection and not a blanket
/// refusal of the whole transaction once one is open.
#[tokio::test]
async fn test_multi_exec_cross_slot_returns_error() {
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

    let (node_id, slot_a, slot_b) = owner_node_with_two_slots(&harness).await;
    let key_a = key_for_slot(slot_a);
    let key_b = key_for_slot(slot_b);
    assert_ne!(
        slot_for_key(key_a.as_bytes()),
        slot_for_key(key_b.as_bytes()),
        "key_a/key_b must hash to different slots"
    );

    let node = harness.node(node_id).unwrap();
    let mut client = node.connect().await;

    let multi_resp = client.command(&["MULTI"]).await;
    assert_eq!(
        multi_resp,
        frogdb_protocol::Response::ok(),
        "MULTI should succeed"
    );

    // Same-slot (trivially, against itself) command queues normally.
    let set_resp = client.command(&["SET", key_a.as_str(), "v1"]).await;
    assert_eq!(
        set_resp,
        frogdb_protocol::Response::queued(),
        "the first, non-cross-slot command must be QUEUED"
    );

    // RENAME's own two keys span different slots -> rejected immediately as
    // ITS OWN queue reply, at queue time, not deferred to EXEC.
    let rename_resp = client
        .command(&["RENAME", key_a.as_str(), key_b.as_str()])
        .await;
    assert_crossslot(
        &rename_resp,
        "cross-slot RENAME must CROSSSLOT at queue time, not return QUEUED",
    );

    // The transaction is now aborted: EXEC must EXECABORT, not run a partial
    // transaction and not silently succeed.
    let exec_resp = client.command(&["EXEC"]).await;
    assert_execabort(&exec_resp, "EXEC after a queue-time CROSSSLOT abort");

    // Nothing committed: the SET that did get QUEUED never ran either.
    assert_eq!(
        client.command(&["GET", key_a.as_str()]).await,
        frogdb_protocol::Response::Bulk(None),
        "EXECABORT must not partially apply the queued SET"
    );

    // DISCARD after a queue-time-aborted transaction clears connection state
    // cleanly: a fresh MULTI immediately afterwards behaves normally.
    let multi_resp = client.command(&["MULTI"]).await;
    assert_eq!(multi_resp, frogdb_protocol::Response::ok());
    let set_resp = client.command(&["SET", key_a.as_str(), "v1"]).await;
    assert_eq!(set_resp, frogdb_protocol::Response::queued());
    let rename_resp = client
        .command(&["RENAME", key_a.as_str(), key_b.as_str()])
        .await;
    assert_crossslot(&rename_resp, "second cross-slot RENAME (pre-DISCARD)");
    let discard_resp = client.command(&["DISCARD"]).await;
    assert_eq!(
        discard_resp,
        frogdb_protocol::Response::ok(),
        "DISCARD must succeed even after a queue-time-aborted transaction"
    );

    let multi_resp = client.command(&["MULTI"]).await;
    assert_eq!(
        multi_resp,
        frogdb_protocol::Response::ok(),
        "MULTI immediately after DISCARD must succeed (no lingering abort state)"
    );
    let get_resp = client.command(&["GET", key_a.as_str()]).await;
    assert_eq!(
        get_resp,
        frogdb_protocol::Response::queued(),
        "a plain queued command in the fresh MULTI must not inherit the prior abort"
    );
    let exec_resp = client.command(&["EXEC"]).await;
    assert_eq!(
        exec_resp,
        frogdb_protocol::Response::Array(vec![frogdb_protocol::Response::Bulk(None)]),
        "the fresh transaction must commit normally post-DISCARD"
    );

    harness.shutdown_all().await;
}

// FM-TXN-019
/// Deferred EXEC-time CROSSSLOT (distinct from the queue-time path above):
/// two *separate* single-key commands, each individually valid, that target
/// different slots both get `+QUEUED` -- queuing only validates a command's
/// own keys, never compares against previously queued commands -- and the
/// mismatch is caught only when EXEC folds the queue's keys into one
/// transaction target. That EXEC-time rejection is a plain `CROSSSLOT` reply,
/// not `EXECABORT`: nothing was marked aborted, EXEC itself declined to run.
#[tokio::test]
async fn test_multi_exec_two_single_key_commands_different_slots_defers_crossslot_to_exec() {
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

    let (node_id, slot_a, slot_b) = owner_node_with_two_slots(&harness).await;
    let key_a = key_for_slot(slot_a);
    let key_b = key_for_slot(slot_b);

    let node = harness.node(node_id).unwrap();
    let mut client = node.connect().await;

    let multi_resp = client.command(&["MULTI"]).await;
    assert_eq!(multi_resp, frogdb_protocol::Response::ok());

    let set1_resp = client.command(&["SET", key_a.as_str(), "v1"]).await;
    assert_eq!(
        set1_resp,
        frogdb_protocol::Response::queued(),
        "first single-key command queues normally"
    );

    let set2_resp = client.command(&["SET", key_b.as_str(), "v2"]).await;
    assert_eq!(
        set2_resp,
        frogdb_protocol::Response::queued(),
        "second single-key command (different slot) ALSO queues normally -- \
         each command's own keys validate independently of what's already queued"
    );

    let exec_resp = client.command(&["EXEC"]).await;
    assert_crossslot(
        &exec_resp,
        "EXEC folding two different-slot queued keys must CROSSSLOT (not EXECABORT)",
    );
    assert_ne!(
        exec_resp,
        frogdb_protocol::Response::Error(bytes::Bytes::from_static(
            b"EXECABORT Transaction discarded because of previous errors."
        )),
        "this path must NOT be EXECABORT -- nothing was aborted at queue time"
    );

    // Neither write landed (rejected atomically, not partially).
    assert_eq!(
        client.command(&["GET", key_a.as_str()]).await,
        frogdb_protocol::Response::Bulk(None)
    );
    assert_eq!(
        client.command(&["GET", key_b.as_str()]).await,
        frogdb_protocol::Response::Bulk(None)
    );

    harness.shutdown_all().await;
}

// ---------------------------------------------------------------------------
// Blocking multi-key CROSSSLOT rejection (testing-gap issue #31): the only
// CROSSSLOT coverage above is for MSET/EVAL/MULTI -- nothing pinned that a
// *blocking* multi-key command (BLPOP/BLMPOP/BZPOPMIN/BZPOPMAX/BZMPOP/BLMOVE/
// BRPOPLPUSH) presented with keys in different slots is rejected immediately,
// rather than entering the blocking wait path. A regression here is worse
// than a wrong reply -- it hangs the client (or blocks against the wrong
// node) instead of failing fast.
//
// Each case below sends the command with an effectively-infinite blocking
// timeout ("0"). If CROSSSLOT rejection were ever skipped for the blocking
// family, the command would sit in the wait queue forever instead of
// replying -- so a bounded elapsed time on the reply is itself proof the
// command never blocked, on top of the plain error-shape assertion. The
// `blocked_client_count` check is the PUBSUB-style introspection asked for:
// it is the same counter `INFO`'s `blocked_clients` field reports and that
// `wait_for_blocked_clients` polls elsewhere in the suite, so it directly
// observes whether a waiter was (even partially/transiently) registered. The
// trailing PING confirms the connection itself is still in a clean state
// (no stray out-of-band wake frame desyncing the RESP2 stream).
// ---------------------------------------------------------------------------

/// Shared body for the blocking-multikey-CROSSSLOT cases: stand up a 3-node
/// cluster, pick two keys the same node owns in different slots (no
/// hash-tag trick -- that's precisely the untested path), send the command
/// built by `build_args(key_a, key_b)`, and assert it comes back as an
/// immediate CROSSSLOT with no waiter ever registered.
async fn assert_blocking_multikey_crossslot_no_block(
    build_args: impl Fn(&str, &str) -> Vec<String>,
    context: &str,
) {
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

    let (node_id, slot_a, slot_b) = owner_node_with_two_slots(&harness).await;
    let key_a = key_for_slot(slot_a);
    let key_b = key_for_slot(slot_b);
    assert_ne!(
        slot_for_key(key_a.as_bytes()),
        slot_for_key(key_b.as_bytes()),
        "{context}: key_a/key_b must hash to different slots"
    );

    let node = harness.node(node_id).unwrap();
    let mut client = node.connect().await;

    let args = build_args(&key_a, &key_b);
    let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();

    let start = std::time::Instant::now();
    client.send_only(&arg_refs).await;
    let resp = client
        .read_response(Duration::from_secs(3))
        .await
        .unwrap_or_else(|| {
            panic!(
                "{context}: no response within 3s -- looks like it entered the blocking wait \
                 path instead of rejecting cross-slot keys immediately"
            )
        });
    let elapsed = start.elapsed();

    assert_crossslot(&resp, context);
    assert!(
        elapsed < Duration::from_millis(1500),
        "{context}: CROSSSLOT reply took {elapsed:?}, expected an immediate (non-blocking) \
         rejection well under the 3s bound"
    );

    assert_eq!(
        node.blocked_client_count(),
        0,
        "{context}: a cross-slot blocking command must never register a waiter"
    );

    let ping = client.command(&["PING"]).await;
    assert_eq!(
        ping,
        frogdb_protocol::Response::pong(),
        "{context}: connection must still be clean (no stray frames) after the CROSSSLOT reply"
    );

    harness.shutdown_all().await;
}

/// `BLPOP key1{slotA} key2{slotB} 0` returns CROSSSLOT immediately, never
/// blocking.
#[tokio::test]
async fn test_blpop_cross_slot_returns_crossslot_immediately() {
    assert_blocking_multikey_crossslot_no_block(
        |a, b| {
            vec![
                "BLPOP".to_string(),
                a.to_string(),
                b.to_string(),
                "0".to_string(),
            ]
        },
        "BLPOP with cross-slot keys",
    )
    .await;
}

/// `BLMPOP 0 2 key1{slotA} key2{slotB} LEFT` returns CROSSSLOT immediately,
/// never blocking.
#[tokio::test]
async fn test_blmpop_cross_slot_returns_crossslot_immediately() {
    assert_blocking_multikey_crossslot_no_block(
        |a, b| {
            vec![
                "BLMPOP".to_string(),
                "0".to_string(),
                "2".to_string(),
                a.to_string(),
                b.to_string(),
                "LEFT".to_string(),
            ]
        },
        "BLMPOP with cross-slot keys",
    )
    .await;
}

/// `BZPOPMIN key1{slotA} key2{slotB} 0` returns CROSSSLOT immediately, never
/// blocking.
#[tokio::test]
async fn test_bzpopmin_cross_slot_returns_crossslot_immediately() {
    assert_blocking_multikey_crossslot_no_block(
        |a, b| {
            vec![
                "BZPOPMIN".to_string(),
                a.to_string(),
                b.to_string(),
                "0".to_string(),
            ]
        },
        "BZPOPMIN with cross-slot keys",
    )
    .await;
}

/// `BZPOPMAX key1{slotA} key2{slotB} 0` returns CROSSSLOT immediately, never
/// blocking.
#[tokio::test]
async fn test_bzpopmax_cross_slot_returns_crossslot_immediately() {
    assert_blocking_multikey_crossslot_no_block(
        |a, b| {
            vec![
                "BZPOPMAX".to_string(),
                a.to_string(),
                b.to_string(),
                "0".to_string(),
            ]
        },
        "BZPOPMAX with cross-slot keys",
    )
    .await;
}

/// `BZMPOP 0 2 key1{slotA} key2{slotB} MIN` returns CROSSSLOT immediately,
/// never blocking.
#[tokio::test]
async fn test_bzmpop_cross_slot_returns_crossslot_immediately() {
    assert_blocking_multikey_crossslot_no_block(
        |a, b| {
            vec![
                "BZMPOP".to_string(),
                "0".to_string(),
                "2".to_string(),
                a.to_string(),
                b.to_string(),
                "MIN".to_string(),
            ]
        },
        "BZMPOP with cross-slot keys",
    )
    .await;
}

/// `BLMOVE src{slotA} dst{slotB} LEFT RIGHT 0` (cross-slot source/dest)
/// returns CROSSSLOT immediately, never blocking.
#[tokio::test]
async fn test_blmove_cross_slot_returns_crossslot_immediately() {
    assert_blocking_multikey_crossslot_no_block(
        |a, b| {
            vec![
                "BLMOVE".to_string(),
                a.to_string(),
                b.to_string(),
                "LEFT".to_string(),
                "RIGHT".to_string(),
                "0".to_string(),
            ]
        },
        "BLMOVE with cross-slot source/destination",
    )
    .await;
}

/// `BRPOPLPUSH src{slotA} dst{slotB} 0` (cross-slot source/dest) returns
/// CROSSSLOT immediately, never blocking.
#[tokio::test]
async fn test_brpoplpush_cross_slot_returns_crossslot_immediately() {
    assert_blocking_multikey_crossslot_no_block(
        |a, b| {
            vec![
                "BRPOPLPUSH".to_string(),
                a.to_string(),
                b.to_string(),
                "0".to_string(),
            ]
        },
        "BRPOPLPUSH with cross-slot source/destination",
    )
    .await;
}

// ---------------------------------------------------------------------------
// Bare-script slot validation (FM-CLUSTER-030, rework issue 11).
//
// EVAL/EVALSHA/FCALL are
// `ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Scripting)` and are
// dispatched at the `ConnectionCommand` stage, which used to run *before*
// `ClusterSlotValidation` — so a bare script naming a foreign key executed to
// completion on the non-owner and acked its writes there. `is_cluster_exempt`
// (guards.rs) deliberately refuses to exempt Scripting, stating the intent that
// scripts be slot-routed; the fix hoisted `ClusterSlotValidation` ahead of
// `ConnectionCommand` and pinned the ordering with the `MUST_PRECEDE` pair
// `(ClusterSlotValidation, ConnectionCommand)`.
//
// The three tests below cover the whole family: `EVAL` (the original forcing
// test), the remaining five script entry points, and the keyless case that must
// stay legal on every node.
// ---------------------------------------------------------------------------

// FM-CLUSTER-030
/// A bare `EVAL` naming a key owned by another node must be answered with
/// `-MOVED`, exactly as the equivalent `GET` on the same key is.
///
/// This is the forcing test rework issue 11 asked for: before the fix the
/// non-owner answered `Integer(1)` — the script ran locally and its `SET`
/// landed on a node that does not own the slot.
#[tokio::test]
async fn test_bare_eval_on_non_owner_returns_moved() {
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

    // Slot 4483 is the slot the jepsen slot-migration workload uses; any slot
    // with a distinct owner and non-owner would do.
    let slot = 4483u16;
    let key = key_for_slot(slot);
    let (owner_id, non_owner_id) = find_owner_of_slot(&harness, slot)
        .await
        .expect("expected some node to own the probe slot");
    assert_ne!(owner_id, non_owner_id, "need a distinct non-owner");

    let non_owner = harness.node(non_owner_id).unwrap();

    // Control: the plain command for the same key *is* redirected, so any
    // difference below is about the script path and not about slot ownership.
    let get_resp = non_owner.send("GET", &[&key]).await;
    assert!(
        is_moved_redirect(&get_resp).is_some(),
        "control: GET on the non-owner should be MOVED, got: {:?}",
        get_resp
    );

    // The script writes, so a local execution is an acked write on the wrong
    // node -- not merely a stale read.
    let eval_resp = non_owner
        .send(
            "EVAL",
            &[
                "redis.call('SET', KEYS[1], 'written-on-non-owner'); return 1",
                "1",
                &key,
            ],
        )
        .await;

    assert!(
        is_moved_redirect(&eval_resp).is_some(),
        "bare EVAL naming a key owned by another node must be MOVED, got: {:?}",
        eval_resp
    );

    harness.shutdown_all().await;
}

/// The payload of a `Bulk` reply, or `None` for anything else.
fn bulk_string(resp: &frogdb_protocol::Response) -> Option<String> {
    match resp {
        frogdb_protocol::Response::Bulk(Some(bytes)) => {
            Some(String::from_utf8_lossy(bytes).into_owned())
        }
        _ => None,
    }
}

// FM-CLUSTER-030
/// The rest of the scripting family takes the identical `-MOVED`: `EVAL_RO`,
/// `EVALSHA`, `EVALSHA_RO`, `FCALL` and `FCALL_RO` all declare their keys
/// through `numkeys` and all dispatch at the same `ConnectionCommand` stage, so
/// the fix has to cover the family and not just `EVAL`.
///
/// The script and the function library are loaded *on the non-owner itself*
/// (`SCRIPT LOAD` / `FUNCTION LOAD` are keyless and legal on every node), so a
/// `NOSCRIPT` or "function not found" error cannot masquerade as the redirect
/// this test is looking for.
#[tokio::test]
async fn test_bare_script_family_on_non_owner_returns_moved() {
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

    let slot = 4483u16;
    let key = key_for_slot(slot);
    let (owner_id, non_owner_id) = find_owner_of_slot(&harness, slot)
        .await
        .expect("expected some node to own the probe slot");
    assert_ne!(owner_id, non_owner_id, "need a distinct non-owner");

    let non_owner = harness.node(non_owner_id).unwrap();

    // Control, as in the EVAL test above: the plain command is redirected.
    let get_resp = non_owner.send("GET", &[&key]).await;
    assert!(
        is_moved_redirect(&get_resp).is_some(),
        "control: GET on the non-owner should be MOVED, got: {:?}",
        get_resp
    );

    let script = "return redis.call('GET', KEYS[1])";
    let script_load = non_owner.send("SCRIPT", &["LOAD", script]).await;
    let sha = bulk_string(&script_load)
        .unwrap_or_else(|| panic!("SCRIPT LOAD should return a sha1, got: {script_load:?}"));

    let library = "#!lua name=slotlib\nredis.register_function('slotfn', function(keys, args) return redis.call('GET', keys[1]) end)\n";
    let fn_load = non_owner.send("FUNCTION", &["LOAD", library]).await;
    assert!(
        !is_error(&fn_load),
        "FUNCTION LOAD is keyless and must succeed on any node, got: {:?}",
        fn_load
    );

    let cases: [(&str, &str); 5] = [
        ("EVAL_RO", script),
        ("EVALSHA", &sha),
        ("EVALSHA_RO", &sha),
        ("FCALL", "slotfn"),
        ("FCALL_RO", "slotfn"),
    ];
    for (command, first_arg) in cases {
        let resp = non_owner.send(command, &[first_arg, "1", &key]).await;
        assert!(
            is_moved_redirect(&resp).is_some(),
            "bare {command} naming a key owned by another node must be MOVED, got: {:?}",
            resp
        );
    }

    harness.shutdown_all().await;
}

// FM-CLUSTER-030
/// `numkeys 0` is the other half of the contract: a script that declares no
/// keys is node-scoped, hashes to no slot, and stays legal on *every* node.
/// Hoisting slot validation ahead of connection-level dispatch must not
/// manufacture a slot for it — an over-eager fix that redirected keyless
/// scripts would break `EVAL "return 1" 0` everywhere, so this pins the
/// negative.
#[tokio::test]
async fn test_keyless_script_is_never_redirected() {
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

    for node_id in harness.node_ids() {
        let node = harness.node(node_id).unwrap();

        let eval = node.send("EVAL", &["return 7", "0"]).await;
        assert_eq!(
            eval,
            frogdb_protocol::Response::Integer(7),
            "keyless EVAL must run locally on node {node_id}, got: {:?}",
            eval
        );

        // `SCRIPT LOAD` is the other keyless member of the family (KeySpec::None
        // rather than an empty `numkeys`), and must likewise never redirect.
        let loaded = node.send("SCRIPT", &["LOAD", "return 7"]).await;
        assert!(
            bulk_string(&loaded).is_some(),
            "keyless SCRIPT LOAD must run locally on node {node_id}, got: {:?}",
            loaded
        );
    }

    harness.shutdown_all().await;
}
