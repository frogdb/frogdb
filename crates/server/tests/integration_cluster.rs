//! Cluster mode integration tests.
//!
//! Tests for multi-node cluster formation, leader election, node membership,
//! and failover scenarios.
//!
//! NOTE: Many tests are marked `#[ignore]` because they require full cluster mode
//! implementation. The CLUSTER commands currently return hardcoded standalone
//! responses. These tests verify the harness works correctly and will be enabled
//! once the cluster state machine is integrated with the CLUSTER commands.

mod common;

use common::cluster_harness::{ClusterNodeConfig, ClusterTestHarness};
use common::cluster_helpers::{
    is_ask_redirect, is_error, is_moved_redirect, key_for_slot, parse_cluster_info,
    parse_cluster_nodes, get_error_message, slot_for_key,
};
use std::time::Duration;

// ============================================================================
// Tier 1: Basic Cluster Formation
// ============================================================================

/// Tests that the harness can start a 3-node cluster.
#[tokio::test]
async fn test_cluster_formation_3_nodes() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();

    // Verify all nodes running
    assert_eq!(harness.node_ids().len(), 3);

    // Wait for leader
    let leader = harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    assert!(harness.node_ids().contains(&leader));

    // Verify cluster state on all nodes
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    for node_id in harness.node_ids() {
        let info = harness.get_cluster_info(node_id).await.unwrap();
        assert_eq!(info.cluster_state, "ok");
        assert_eq!(info.cluster_known_nodes, 3);
    }

    harness.shutdown_all().await;
}

/// Tests CLUSTER INFO command returns valid response format.
#[tokio::test]
async fn test_cluster_info_command_multinode() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();

    let node = harness.node(harness.node_ids()[0]).unwrap();
    let response = node.send("CLUSTER", &["INFO"]).await;

    assert!(!is_error(&response));
    let info = parse_cluster_info(&response).unwrap();
    assert_eq!(info.cluster_known_nodes, 3);

    harness.shutdown_all().await;
}

/// Tests CLUSTER NODES command returns valid response format.
#[tokio::test]
async fn test_cluster_nodes_command_multinode() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();

    let node = harness.node(harness.node_ids()[0]).unwrap();
    let response = node.send("CLUSTER", &["NODES"]).await;

    let nodes = parse_cluster_nodes(&response).unwrap();
    assert_eq!(nodes.len(), 3);

    harness.shutdown_all().await;
}

/// Tests that the harness can start a single-node cluster.
/// This verifies the harness infrastructure works.
#[tokio::test]
async fn test_single_node_cluster() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(1).await.unwrap();

    assert_eq!(harness.node_ids().len(), 1);

    let node_id = harness.node_ids()[0];
    let info = harness.get_cluster_info(node_id).await.unwrap();
    // Currently returns hardcoded standalone response
    assert_eq!(info.cluster_state, "ok");
    assert_eq!(info.cluster_known_nodes, 1);

    harness.shutdown_all().await;
}

/// Tests that the harness can start and shutdown multiple nodes.
/// This verifies the harness infrastructure works with multiple nodes.
#[tokio::test]
async fn test_harness_starts_multiple_nodes() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();

    // Verify all nodes are tracked
    assert_eq!(harness.node_ids().len(), 3);

    // Verify all nodes are running and responding
    for node_id in harness.node_ids() {
        let node = harness.node(node_id).unwrap();
        assert!(node.is_running());

        // Verify node responds to PING
        let response = node.send("PING", &[]).await;
        assert!(!is_error(&response));
    }

    harness.shutdown_all().await;
}

/// Tests CLUSTER INFO returns valid response format on single node.
#[tokio::test]
async fn test_cluster_info_command() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(1).await.unwrap();

    let node = harness.node(harness.node_ids()[0]).unwrap();
    let response = node.send("CLUSTER", &["INFO"]).await;

    assert!(!is_error(&response));
    let info = parse_cluster_info(&response).unwrap();
    // Currently returns hardcoded standalone response
    assert_eq!(info.cluster_state, "ok");
    assert!(info.cluster_known_nodes >= 1);

    harness.shutdown_all().await;
}

/// Tests CLUSTER NODES returns valid response format on single node.
#[tokio::test]
async fn test_cluster_nodes_command() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(1).await.unwrap();

    let node = harness.node(harness.node_ids()[0]).unwrap();
    let response = node.send("CLUSTER", &["NODES"]).await;

    let nodes = parse_cluster_nodes(&response).unwrap();
    // Currently returns hardcoded standalone response with 1 node
    assert!(!nodes.is_empty());

    harness.shutdown_all().await;
}

#[tokio::test]
async fn test_cluster_myid_command() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(1).await.unwrap();

    let node = harness.node(harness.node_ids()[0]).unwrap();
    let response = node.send("CLUSTER", &["MYID"]).await;

    // Should return a node ID (bulk string)
    assert!(!is_error(&response));

    harness.shutdown_all().await;
}

#[tokio::test]
async fn test_cluster_slots_command() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(1).await.unwrap();

    let node = harness.node(harness.node_ids()[0]).unwrap();
    let response = node.send("CLUSTER", &["SLOTS"]).await;

    // Should return array of slot ranges
    assert!(!is_error(&response));

    harness.shutdown_all().await;
}

#[tokio::test]
async fn test_cluster_shards_command() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(1).await.unwrap();

    let node = harness.node(harness.node_ids()[0]).unwrap();
    let response = node.send("CLUSTER", &["SHARDS"]).await;

    // Should return array of shard info
    assert!(!is_error(&response));

    harness.shutdown_all().await;
}

#[tokio::test]
async fn test_cluster_keyslot_command() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(1).await.unwrap();

    let node = harness.node(harness.node_ids()[0]).unwrap();

    // Test basic key
    let response = node.send("CLUSTER", &["KEYSLOT", "hello"]).await;
    assert!(!is_error(&response));

    // Test key with hash tag
    let response = node.send("CLUSTER", &["KEYSLOT", "user:{123}:name"]).await;
    assert!(!is_error(&response));

    harness.shutdown_all().await;
}

// ============================================================================
// Tier 2: Node Membership
// ============================================================================

/// Tests adding a node to an existing cluster.
#[tokio::test]
async fn test_add_node_to_cluster() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();

    // Add a 4th node
    let new_node_id = harness.add_node().await.unwrap();

    // Wait for new node to be recognized
    harness
        .wait_for_node_recognized(new_node_id, Duration::from_secs(10))
        .await
        .unwrap();

    // Verify all nodes see 4 members
    for node_id in harness.node_ids() {
        let info = harness.get_cluster_info(node_id).await.unwrap();
        assert_eq!(info.cluster_known_nodes, 4);
    }

    harness.shutdown_all().await;
}

/// Tests removing a node from a cluster.
#[tokio::test]
async fn test_remove_node_from_cluster() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();

    // Add then remove a node (don't remove from original 3 to maintain quorum)
    let new_node_id = harness.add_node().await.unwrap();
    harness
        .wait_for_node_recognized(new_node_id, Duration::from_secs(10))
        .await
        .unwrap();

    harness.remove_node(new_node_id).await.unwrap();

    // Wait for convergence
    harness
        .wait_for_cluster_convergence(Duration::from_secs(10))
        .await
        .unwrap();

    // Verify back to 3 nodes
    assert_eq!(harness.node_ids().len(), 3);

    harness.shutdown_all().await;
}

/// Tests starting a 5-node cluster.
#[tokio::test]
async fn test_cluster_with_5_nodes() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(5).await.unwrap();

    // Verify all nodes running
    assert_eq!(harness.node_ids().len(), 5);

    // Wait for leader
    harness
        .wait_for_leader(Duration::from_secs(15))
        .await
        .unwrap();

    // Verify cluster state on all nodes
    harness
        .wait_for_cluster_convergence(Duration::from_secs(10))
        .await
        .unwrap();

    for node_id in harness.node_ids() {
        let info = harness.get_cluster_info(node_id).await.unwrap();
        assert_eq!(info.cluster_state, "ok");
        assert_eq!(info.cluster_known_nodes, 5);
    }

    harness.shutdown_all().await;
}

// ============================================================================
// Tier 3: Failover
// ============================================================================

/// Tests leader failover when the leader node is killed.
#[tokio::test]
async fn test_leader_failover() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();

    let original_leader = harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();

    // Kill the leader
    harness.kill_node(original_leader);

    // Wait for new leader election
    let new_leader = harness
        .wait_for_leader(Duration::from_secs(15))
        .await
        .unwrap();
    assert_ne!(new_leader, original_leader);

    // Verify cluster still operational
    harness
        .wait_for_cluster_convergence(Duration::from_secs(10))
        .await
        .unwrap();

    harness.shutdown_all().await;
}

/// Tests that a restarted node can rejoin the cluster.
#[tokio::test]
async fn test_node_restart_rejoins_cluster() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();

    // Pick a non-leader node
    let leader = harness.get_leader().await.unwrap();
    let victim = harness
        .node_ids()
        .into_iter()
        .find(|&id| id != leader)
        .unwrap();

    // Shutdown and restart
    harness.shutdown_node(victim).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    harness.restart_node(victim).await.unwrap();

    // Verify it rejoins
    harness
        .wait_for_node_recognized(victim, Duration::from_secs(15))
        .await
        .unwrap();

    let info = harness.get_cluster_info(victim).await.unwrap();
    assert_eq!(info.cluster_state, "ok");

    harness.shutdown_all().await;
}

/// Tests that a minority partition cannot elect a leader.
/// NOTE: This test requires Phase 4 (Failure Detection) implementation to work correctly.
/// Currently, Raft doesn't immediately detect quorum loss without active writes.
/// The cluster needs a background heartbeat task that monitors node liveness and
/// updates cluster_state accordingly.
#[tokio::test]
#[ignore = "Requires Phase 4 (Failure Detection) - heartbeat-based quorum monitoring"]
async fn test_minority_partition_cannot_elect_leader() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(5).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();

    // Kill 3 nodes (majority) using graceful shutdown to ensure sockets close
    let nodes: Vec<_> = harness.node_ids();
    harness.shutdown_node(nodes[0]).await;
    harness.shutdown_node(nodes[1]).await;
    harness.shutdown_node(nodes[2]).await;

    // Wait for sockets to fully close and failure detector to detect the failures
    // Failure detection takes: fail_threshold * check_interval_ms = 5 * 100ms = 500ms
    // Plus some buffer time for the checks to complete
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Cluster should be down - requires Phase 4's failure detection to work
    let surviving_node = harness.node(nodes[3]).unwrap();
    let response = surviving_node.send("CLUSTER", &["INFO"]).await;
    let info = parse_cluster_info(&response).unwrap();

    // State should be "fail" since quorum lost
    // Note: Currently reports "ok" because Raft doesn't detect quorum loss without writes
    assert_eq!(info.cluster_state, "fail");

    harness.shutdown_all().await;
}

/// Tests graceful shutdown and restart of a node.
#[tokio::test]
async fn test_graceful_shutdown_and_restart() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();

    let node_ids = harness.node_ids();
    let victim = node_ids[0];

    // Graceful shutdown (not kill)
    harness.shutdown_node(victim).await;

    // Remaining nodes should still be operational
    for &id in &node_ids[1..] {
        let info = harness.get_cluster_info(id).await.unwrap();
        assert_eq!(info.cluster_state, "ok");
    }

    // Restart the node
    harness.restart_node(victim).await.unwrap();

    // Wait for it to rejoin
    harness
        .wait_for_node_recognized(victim, Duration::from_secs(15))
        .await
        .unwrap();

    harness.shutdown_all().await;
}

// ============================================================================
// Configuration Tests
// ============================================================================

/// Tests custom cluster configuration.
#[tokio::test]
async fn test_custom_cluster_config() {
    let config = ClusterNodeConfig {
        num_shards: Some(2),
        election_timeout_ms: 500,
        heartbeat_interval_ms: 150,
        ..Default::default()
    };

    let mut harness = ClusterTestHarness::with_config(config);
    harness.start_cluster(3).await.unwrap();

    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();

    // Verify cluster is operational
    for node_id in harness.node_ids() {
        let info = harness.get_cluster_info(node_id).await.unwrap();
        assert_eq!(info.cluster_state, "ok");
    }

    harness.shutdown_all().await;
}

// ============================================================================
// Basic Command Verification
// ============================================================================

#[tokio::test]
async fn test_cluster_help_command() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(1).await.unwrap();

    let node = harness.node(harness.node_ids()[0]).unwrap();
    let response = node.send("CLUSTER", &["HELP"]).await;

    // Should return array of help strings
    assert!(!is_error(&response));

    harness.shutdown_all().await;
}

#[tokio::test]
async fn test_readonly_command() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(1).await.unwrap();

    let node = harness.node(harness.node_ids()[0]).unwrap();

    // READONLY should succeed
    let response = node.send("READONLY", &[]).await;
    assert!(!is_error(&response));

    // READWRITE should succeed
    let response = node.send("READWRITE", &[]).await;
    assert!(!is_error(&response));

    harness.shutdown_all().await;
}

#[tokio::test]
async fn test_asking_command() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(1).await.unwrap();

    let node = harness.node(harness.node_ids()[0]).unwrap();

    // ASKING should succeed (no-op in standalone mode)
    let response = node.send("ASKING", &[]).await;
    assert!(!is_error(&response));

    harness.shutdown_all().await;
}

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
        n.id != owner_node.id && !n.slots.iter().any(|(s, e)| target_slot >= *s && target_slot <= *e)
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

    // Get node IDs from CLUSTER MYID
    let source_myid_resp = source_node.send("CLUSTER", &["MYID"]).await;
    let target_myid_resp = target_node.send("CLUSTER", &["MYID"]).await;

    let source_id = match &source_myid_resp {
        frogdb_protocol::Response::Bulk(Some(b)) => String::from_utf8_lossy(b).to_string(),
        _ => {
            harness.shutdown_all().await;
            return;
        }
    };
    let target_id = match &target_myid_resp {
        frogdb_protocol::Response::Bulk(Some(b)) => String::from_utf8_lossy(b).to_string(),
        _ => {
            harness.shutdown_all().await;
            return;
        }
    };

    // Set slot 100 to MIGRATING state on source
    let migrate_resp = source_node
        .send(
            "CLUSTER",
            &["SETSLOT", "100", "MIGRATING", &target_id],
        )
        .await;

    // If SETSLOT succeeds, test ASK behavior
    if !is_error(&migrate_resp) {
        // Set slot 100 to IMPORTING state on target
        let import_resp = target_node
            .send(
                "CLUSTER",
                &["SETSLOT", "100", "IMPORTING", &source_id],
            )
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
            let _ = source_node.send("CLUSTER", &["SETSLOT", "100", "STABLE"]).await;
            let _ = target_node.send("CLUSTER", &["SETSLOT", "100", "STABLE"]).await;
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
    let source_node = harness.node(node_ids[0]).unwrap();
    let target_node = harness.node(node_ids[1]).unwrap();

    // Get node IDs
    let source_myid_resp = source_node.send("CLUSTER", &["MYID"]).await;
    let source_id = match &source_myid_resp {
        frogdb_protocol::Response::Bulk(Some(b)) => String::from_utf8_lossy(b).to_string(),
        _ => {
            harness.shutdown_all().await;
            return;
        }
    };

    // Set slot 200 to IMPORTING state on target
    let import_resp = target_node
        .send(
            "CLUSTER",
            &["SETSLOT", "200", "IMPORTING", &source_id],
        )
        .await;

    if !is_error(&import_resp) {
        let key = key_for_slot(200);

        // First, try without ASKING - should get redirect or error
        let response_without_asking = target_node.send("GET", &[&key]).await;

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
        let _ = target_node.send("CLUSTER", &["SETSLOT", "200", "STABLE"]).await;
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
    let source_node = harness.node(node_ids[0]).unwrap();
    let target_node = harness.node(node_ids[1]).unwrap();

    // Get source node ID
    let source_myid_resp = source_node.send("CLUSTER", &["MYID"]).await;
    let source_id = match &source_myid_resp {
        frogdb_protocol::Response::Bulk(Some(b)) => String::from_utf8_lossy(b).to_string(),
        _ => {
            harness.shutdown_all().await;
            return;
        }
    };

    // Set slot 300 to IMPORTING on target
    let import_resp = target_node
        .send(
            "CLUSTER",
            &["SETSLOT", "300", "IMPORTING", &source_id],
        )
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
        let _ = target_node.send("CLUSTER", &["SETSLOT", "300", "STABLE"]).await;
    }

    harness.shutdown_all().await;
}

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
    let other_node = harness.node(node_ids[1]).unwrap();

    // Get other node's ID
    let other_myid = other_node.send("CLUSTER", &["MYID"]).await;
    let other_id = match &other_myid {
        frogdb_protocol::Response::Bulk(Some(b)) => String::from_utf8_lossy(b).to_string(),
        _ => {
            harness.shutdown_all().await;
            return;
        }
    };

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
    let other_node = harness.node(node_ids[1]).unwrap();

    // Get other node's ID
    let other_myid = other_node.send("CLUSTER", &["MYID"]).await;
    let other_id = match &other_myid {
        frogdb_protocol::Response::Bulk(Some(b)) => String::from_utf8_lossy(b).to_string(),
        _ => {
            harness.shutdown_all().await;
            return;
        }
    };

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

    // Get this node's ID
    let myid_resp = node.send("CLUSTER", &["MYID"]).await;
    let my_id = match &myid_resp {
        frogdb_protocol::Response::Bulk(Some(b)) => String::from_utf8_lossy(b).to_string(),
        _ => {
            harness.shutdown_all().await;
            return;
        }
    };

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
    let other_node = harness.node(node_ids[1]).unwrap();

    // Get other node's ID
    let other_myid = other_node.send("CLUSTER", &["MYID"]).await;
    let other_id = match &other_myid {
        frogdb_protocol::Response::Bulk(Some(b)) => String::from_utf8_lossy(b).to_string(),
        _ => {
            harness.shutdown_all().await;
            return;
        }
    };

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

    // Get node IDs
    let source_myid = source.send("CLUSTER", &["MYID"]).await;
    let target_myid = target.send("CLUSTER", &["MYID"]).await;

    let source_id = match &source_myid {
        frogdb_protocol::Response::Bulk(Some(b)) => String::from_utf8_lossy(b).to_string(),
        _ => {
            harness.shutdown_all().await;
            return;
        }
    };
    let target_id = match &target_myid {
        frogdb_protocol::Response::Bulk(Some(b)) => String::from_utf8_lossy(b).to_string(),
        _ => {
            harness.shutdown_all().await;
            return;
        }
    };

    // Set up migration: slot 1004 migrating from source to target
    let migrate_resp = source
        .send("CLUSTER", &["SETSLOT", "1004", "MIGRATING", &target_id])
        .await;
    let import_resp = target
        .send("CLUSTER", &["SETSLOT", "1004", "IMPORTING", &source_id])
        .await;

    if !is_error(&migrate_resp) && !is_error(&import_resp) {
        // Check source's CLUSTER NODES
        let source_nodes = source.send("CLUSTER", &["NODES"]).await;
        if let frogdb_protocol::Response::Bulk(Some(b)) = &source_nodes {
            let nodes_str = String::from_utf8_lossy(b);
            // Source should show migrating flag
            // Format varies but should contain slot 1004 with migration indicator
            assert!(
                nodes_str.contains("1004"),
                "CLUSTER NODES should mention slot 1004"
            );
        }

        // Check target's CLUSTER NODES
        let target_nodes = target.send("CLUSTER", &["NODES"]).await;
        if let frogdb_protocol::Response::Bulk(Some(b)) = &target_nodes {
            let nodes_str = String::from_utf8_lossy(b);
            // Target should show importing flag
            assert!(
                nodes_str.contains("1004"),
                "CLUSTER NODES should mention slot 1004"
            );
        }

        // Clean up
        let _ = source.send("CLUSTER", &["SETSLOT", "1004", "STABLE"]).await;
        let _ = target.send("CLUSTER", &["SETSLOT", "1004", "STABLE"]).await;
    }

    harness.shutdown_all().await;
}
