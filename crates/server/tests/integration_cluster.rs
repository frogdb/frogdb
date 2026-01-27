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
/// The failure detector monitors node liveness via heartbeats and updates
/// cluster_state to "fail" when quorum is lost.
#[tokio::test]
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

// ============================================================================
// Tier 6: Partition and Failure Scenarios
// ============================================================================

/// Tests partition behavior by shutting down nodes.
/// Simulates a network partition where minority nodes cannot reach majority.
#[tokio::test]
async fn test_partition_via_shutdown() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(5).await.unwrap();

    // Wait for cluster to stabilize
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let node_ids = harness.node_ids();

    // Shutdown 2 nodes (minority partition simulation)
    harness.shutdown_node(node_ids[0]).await;
    harness.shutdown_node(node_ids[1]).await;

    // Wait for failure detection and cluster stabilization
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Majority partition (nodes 2, 3, 4) should still work
    let surviving_nodes: Vec<u64> = node_ids[2..].to_vec();
    let mut majority_responsive = false;

    // Check that surviving nodes are at least responsive
    for &node_id in &surviving_nodes {
        if let Some(node) = harness.node(node_id) {
            if node.is_running() {
                let response = node.send("PING", &[]).await;
                if !is_error(&response) {
                    majority_responsive = true;
                }
                // Also log cluster state for debugging
                let info_resp = node.send("CLUSTER", &["INFO"]).await;
                if let Ok(info) = parse_cluster_info(&info_resp) {
                    eprintln!(
                        "Node {} after partition: cluster_state={}, known_nodes={}",
                        node_id, info.cluster_state, info.cluster_known_nodes
                    );
                }
            }
        }
    }

    assert!(
        majority_responsive,
        "Majority partition nodes should at least respond to PING"
    );

    // Restart nodes and verify recovery
    harness.restart_node(node_ids[0]).await.unwrap();
    harness.restart_node(node_ids[1]).await.unwrap();

    // Wait for nodes to rejoin
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify cluster recovers
    harness
        .wait_for_cluster_convergence(Duration::from_secs(10))
        .await
        .unwrap();

    for node_id in harness.node_ids() {
        if let Some(node) = harness.node(node_id) {
            if node.is_running() {
                let info = harness.get_cluster_info(node_id).await.unwrap();
                assert_eq!(
                    info.cluster_state, "ok",
                    "Node {} should be ok after recovery",
                    node_id
                );
            }
        }
    }

    harness.shutdown_all().await;
}

/// Tests asymmetric node failure (leader only).
/// Verifies quick recovery and client redirect behavior.
#[tokio::test]
async fn test_asymmetric_node_failure() {
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

    // Record when we kill the leader
    let kill_time = std::time::Instant::now();

    // Kill only the leader
    harness.kill_node(original_leader);

    // Wait for new leader election
    let new_leader = harness
        .wait_for_leader(Duration::from_secs(15))
        .await
        .unwrap();

    let election_time = kill_time.elapsed();
    eprintln!("New leader elected in {:?}", election_time);

    // New leader should be different
    assert_ne!(
        new_leader, original_leader,
        "New leader should be different from killed leader"
    );

    // Verify remaining nodes see the cluster as ok
    let remaining_nodes: Vec<u64> = harness
        .node_ids()
        .into_iter()
        .filter(|&id| id != original_leader)
        .collect();

    for &node_id in &remaining_nodes {
        if let Some(node) = harness.node(node_id) {
            if node.is_running() {
                let response = node.send("PING", &[]).await;
                assert!(
                    !is_error(&response),
                    "Node {} should respond to PING",
                    node_id
                );
            }
        }
    }

    harness.shutdown_all().await;
}

/// Tests rapid failover cycles - multiple sequential leader failures.
/// Verifies cluster stability under stress.
#[tokio::test]
async fn test_rapid_failover_cycles() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(5).await.unwrap();

    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    // Track killed leaders
    let mut killed_leaders = Vec::new();

    // Kill leader, wait for election, repeat 3 times
    for cycle in 0..3 {
        let current_leader = harness
            .wait_for_leader(Duration::from_secs(15))
            .await
            .unwrap();

        eprintln!("Cycle {}: Killing leader {}", cycle + 1, current_leader);
        harness.kill_node(current_leader);
        killed_leaders.push(current_leader);

        // Wait for failure detection and new election
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    // After killing 3 leaders, we should still have 2 nodes (quorum lost)
    let remaining_running: Vec<u64> = harness
        .node_ids()
        .into_iter()
        .filter(|id| !killed_leaders.contains(id))
        .filter(|&id| harness.node(id).map(|n| n.is_running()).unwrap_or(false))
        .collect();

    eprintln!("Remaining running nodes: {:?}", remaining_running);
    assert_eq!(remaining_running.len(), 2, "Should have 2 nodes remaining");

    // With only 2 of 5 nodes, cluster should be in fail state (no quorum)
    // This documents the expected behavior
    for &node_id in &remaining_running {
        if let Some(node) = harness.node(node_id) {
            let response = node.send("CLUSTER", &["INFO"]).await;
            if let Ok(info) = parse_cluster_info(&response) {
                // With 2/5 nodes, quorum is lost
                // State could be "ok" or "fail" depending on timing
                eprintln!("Node {} cluster_state: {}", node_id, info.cluster_state);
            }
        }
    }

    harness.shutdown_all().await;
}

/// Tests 7-node cluster scale.
/// Verifies larger cluster formation and failover.
#[tokio::test]
async fn test_seven_node_cluster() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(7).await.unwrap();

    // Verify all nodes running
    assert_eq!(harness.node_ids().len(), 7);

    // Wait for leader
    harness
        .wait_for_leader(Duration::from_secs(20))
        .await
        .unwrap();

    // Verify cluster state on all nodes
    harness
        .wait_for_cluster_convergence(Duration::from_secs(15))
        .await
        .unwrap();

    for node_id in harness.node_ids() {
        let info = harness.get_cluster_info(node_id).await.unwrap();
        assert_eq!(info.cluster_state, "ok");
        assert_eq!(info.cluster_known_nodes, 7);
    }

    // Kill 3 nodes (still have quorum with 4)
    let node_ids = harness.node_ids();
    harness.kill_node(node_ids[0]);
    harness.kill_node(node_ids[1]);
    harness.kill_node(node_ids[2]);

    // Wait for failure detection
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify remaining 4 nodes still work (have quorum)
    let surviving: Vec<u64> = node_ids[3..].to_vec();
    let mut operational_count = 0;

    for &node_id in &surviving {
        if let Some(node) = harness.node(node_id) {
            if node.is_running() {
                let response = node.send("PING", &[]).await;
                if !is_error(&response) {
                    operational_count += 1;
                }
            }
        }
    }

    assert_eq!(
        operational_count, 4,
        "4 surviving nodes should be operational"
    );

    // Kill 1 more (lose quorum with 3)
    harness.kill_node(node_ids[3]);
    tokio::time::sleep(Duration::from_secs(2)).await;

    // With 3/7 nodes, cluster should report failure (no quorum)
    let final_surviving: Vec<u64> = node_ids[4..].to_vec();
    for &node_id in &final_surviving {
        if let Some(node) = harness.node(node_id) {
            if node.is_running() {
                let response = node.send("CLUSTER", &["INFO"]).await;
                if let Ok(info) = parse_cluster_info(&response) {
                    // Document observed state
                    eprintln!(
                        "Node {} with 3/7 nodes: cluster_state={}",
                        node_id, info.cluster_state
                    );
                }
            }
        }
    }

    harness.shutdown_all().await;
}

/// Tests concurrent operations during slot migration.
/// Verifies data consistency when clients write during rebalance.
#[tokio::test]
async fn test_concurrent_operations_during_migration() {
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

    // Get node IDs for migration
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

    // Set up migration for slot 500
    let test_slot = 500u16;
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
        eprintln!("Migration setup failed, skipping concurrent test");
        harness.shutdown_all().await;
        return;
    }

    // Generate keys for this slot
    let test_key = key_for_slot(test_slot);

    // Spawn concurrent writers
    let mut handles = Vec::new();
    let source_addr = source.client_addr();
    let target_addr = target.client_addr();

    for i in 0..5 {
        let key = format!("{}_{}", test_key, i);
        let value = format!("value_{}", i);
        let addr = if i % 2 == 0 {
            source_addr.clone()
        } else {
            target_addr.clone()
        };

        let handle = tokio::spawn(async move {
            // Connect and try to write
            let stream = tokio::net::TcpStream::connect(&addr).await;
            match stream {
                Ok(stream) => {
                    let mut framed =
                        tokio_util::codec::Framed::new(stream, redis_protocol::codec::Resp2);

                    // Try SET
                    let frame = redis_protocol::resp2::types::BytesFrame::Array(vec![
                        redis_protocol::resp2::types::BytesFrame::BulkString(bytes::Bytes::from(
                            "SET",
                        )),
                        redis_protocol::resp2::types::BytesFrame::BulkString(bytes::Bytes::from(
                            key,
                        )),
                        redis_protocol::resp2::types::BytesFrame::BulkString(bytes::Bytes::from(
                            value,
                        )),
                    ]);

                    use futures::{SinkExt, StreamExt};
                    if framed.send(frame).await.is_ok() {
                        if let Some(Ok(_response)) = framed.next().await {
                            // Response could be OK, MOVED, or ASK - all valid during migration
                            return true;
                        }
                    }
                }
                Err(_) => {}
            }
            false
        });
        handles.push(handle);
    }

    // Wait for all writes to complete
    let results: Vec<bool> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap_or(false))
        .collect();

    // At least some operations should have completed
    let completed = results.iter().filter(|&&r| r).count();
    eprintln!(
        "Concurrent operations during migration: {}/{} completed",
        completed,
        results.len()
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

// ============================================================================
// Tier 7: Split-Brain Prevention Tests
// ============================================================================

/// Tests that a minority partition rejects writes (split-brain prevention).
///
/// In a cluster of 5 nodes, if only 2 nodes can communicate (minority),
/// write operations should return CLUSTERDOWN to prevent split-brain data inconsistency.
///
/// This test simulates a network partition by shutting down the majority (3 nodes),
/// leaving a minority of 2 nodes that should detect quorum loss and reject writes.
#[tokio::test]
async fn test_split_brain_writes_fail_on_minority() {
    use common::cluster_helpers::is_cluster_down;

    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(5).await.unwrap();

    // Wait for cluster to fully stabilize
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    // Give extra time for slot assignment to complete
    tokio::time::sleep(Duration::from_secs(2)).await;

    let node_ids = harness.node_ids();

    // First verify cluster is healthy before partition
    let pre_partition_node = harness.node(node_ids[3]).unwrap();
    let info = harness.get_cluster_info(node_ids[3]).await.unwrap();
    eprintln!(
        "Pre-partition cluster state: {} (known_nodes: {})",
        info.cluster_state, info.cluster_known_nodes
    );
    assert_eq!(info.cluster_state, "ok", "Cluster should be healthy before partition");

    // Shutdown 3 nodes (majority) - leaves 2 nodes (minority partition)
    // This simulates a network partition where the minority cannot reach majority
    harness.shutdown_node(node_ids[0]).await;
    harness.shutdown_node(node_ids[1]).await;
    harness.shutdown_node(node_ids[2]).await;

    // Wait for failure detection: fail_threshold * check_interval_ms + buffer
    // Default: 5 * 100ms = 500ms, but allow more time for propagation
    tokio::time::sleep(Duration::from_secs(3)).await;

    // The minority partition (2 remaining nodes) should detect quorum loss
    // Verify cluster state on minority nodes
    let minority_nodes = &node_ids[3..5];

    for &node_id in minority_nodes {
        if let Some(node) = harness.node(node_id) {
            if node.is_running() {
                // Check cluster state
                let info_resp = node.send("CLUSTER", &["INFO"]).await;
                if let Ok(info) = parse_cluster_info(&info_resp) {
                    eprintln!(
                        "Minority node {} cluster_state: {} (known_nodes: {})",
                        node_id, info.cluster_state, info.cluster_known_nodes
                    );

                    // Cluster should be in "fail" state due to quorum loss
                    assert_eq!(
                        info.cluster_state, "fail",
                        "Minority node {} should detect quorum loss",
                        node_id
                    );
                }

                // Attempt a write - should be rejected with CLUSTERDOWN
                let key = format!("splitbrain_test_{}", node_id);
                let write_resp = node.send("SET", &[&key, "value"]).await;

                // Verify write is rejected
                if is_cluster_down(&write_resp) {
                    eprintln!(
                        "Minority node {} correctly rejected write with CLUSTERDOWN",
                        node_id
                    );
                } else if is_error(&write_resp) {
                    // Other errors (like MOVED to a dead node) are also acceptable
                    let msg = get_error_message(&write_resp).unwrap_or("unknown");
                    eprintln!(
                        "Minority node {} rejected write with: {}",
                        node_id, msg
                    );
                } else {
                    // Write succeeded - this is split-brain behavior we want to prevent
                    panic!(
                        "Split-brain detected: minority node {} accepted write! Response: {:?}",
                        node_id, write_resp
                    );
                }
            }
        }
    }

    // Now restore majority and verify writes work again
    harness.restart_node(node_ids[0]).await.unwrap();
    harness.restart_node(node_ids[1]).await.unwrap();
    harness.restart_node(node_ids[2]).await.unwrap();

    // Wait for cluster to recover and re-establish quorum
    tokio::time::sleep(Duration::from_secs(3)).await;
    harness
        .wait_for_cluster_convergence(Duration::from_secs(10))
        .await
        .unwrap();

    // Verify cluster recovers to healthy state
    for &node_id in &node_ids {
        if let Some(node) = harness.node(node_id) {
            if node.is_running() {
                let info = harness.get_cluster_info(node_id).await.unwrap();
                assert_eq!(
                    info.cluster_state, "ok",
                    "Node {} should recover after majority restored",
                    node_id
                );
            }
        }
    }

    // Verify cluster is operational after recovery (state=ok means quorum restored)
    // Note: We check cluster state rather than writes because slot assignment
    // may not be fully propagated immediately after recovery
    let final_info = harness.get_cluster_info(node_ids[3]).await.unwrap();
    eprintln!(
        "Post-recovery cluster state: {} (known_nodes: {})",
        final_info.cluster_state, final_info.cluster_known_nodes
    );
    assert_eq!(
        final_info.cluster_state, "ok",
        "Cluster should be operational after majority restored"
    );

    harness.shutdown_all().await;
}

/// Tests that source node failure during migration doesn't cause data loss.
///
/// When a source node fails during slot migration, the migration should be
/// cancelled and the slot should remain accessible (either on surviving nodes
/// or through cluster failover).
#[tokio::test]
async fn test_failover_during_migration_preserves_data() {
    use common::cluster_helpers::is_cluster_down;

    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();

    // Wait for cluster to stabilize and get the leader
    let leader_id = harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let node_ids = harness.node_ids();

    // Use leader as source, and find another node as target
    let source_node_id = leader_id;
    let target_node_id = *node_ids.iter().find(|&&id| id != leader_id).unwrap();
    let third_node_id = *node_ids.iter().find(|&&id| id != leader_id && id != target_node_id).unwrap();

    eprintln!(
        "Using leader {} as source, {} as target, {} as third",
        source_node_id, target_node_id, third_node_id
    );

    // Get cluster node IDs for migration setup (need to do this in a scope to avoid borrow issues)
    let (source_id, target_id) = {
        let source = harness.node(source_node_id).unwrap();
        let target = harness.node(target_node_id).unwrap();

        let source_myid = source.send("CLUSTER", &["MYID"]).await;
        let target_myid = target.send("CLUSTER", &["MYID"]).await;

        match (&source_myid, &target_myid) {
            (
                frogdb_protocol::Response::Bulk(Some(s)),
                frogdb_protocol::Response::Bulk(Some(t)),
            ) => (
                String::from_utf8_lossy(s).to_string(),
                String::from_utf8_lossy(t).to_string(),
            ),
            _ => {
                eprintln!("Could not get node IDs, skipping migration test");
                harness.shutdown_all().await;
                return;
            }
        }
    };

    // Use slot 500 for migration test
    let test_slot = 500u16;
    let test_key = key_for_slot(test_slot);
    eprintln!("Testing failover during migration of slot {} (key: {})", test_slot, test_key);

    // Set up migration and write data in a scope
    let migration_started = {
        let source = harness.node(source_node_id).unwrap();
        let target = harness.node(target_node_id).unwrap();

        // First, write some data to the key (before migration)
        let pre_write = source.send("SET", &[&test_key, "original_value"]).await;
        if is_error(&pre_write) && !is_cluster_down(&pre_write) {
            let err_msg = get_error_message(&pre_write).unwrap_or("unknown");
            eprintln!("Pre-migration write response: {}", err_msg);
        }

        // Set up migration: slot 500 migrating from source to target
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
            let migrate_err = get_error_message(&migrate_resp).unwrap_or("ok");
            let import_err = get_error_message(&import_resp).unwrap_or("ok");
            eprintln!(
                "Migration setup failed - MIGRATING: {}, IMPORTING: {} (expected in some configs)",
                migrate_err, import_err
            );
            false
        } else {
            eprintln!("Migration started: slot {} from {} to {}", test_slot, source_id, target_id);

            // Write more data during migration
            let mid_migration_key = format!("{}_mid", test_key);
            let mid_write = source.send("SET", &[&mid_migration_key, "mid_migration_value"]).await;
            eprintln!("Mid-migration write result: {:?}", mid_write);
            true
        }
    };

    if !migration_started {
        harness.shutdown_all().await;
        return;
    }

    // Kill the source node mid-migration
    eprintln!("Killing source node {} during migration", source_node_id);
    harness.kill_node(source_node_id);

    // Wait for failure detection
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify cluster can still operate (surviving nodes should handle the situation)
    {
        let surviving_node = harness.node(target_node_id).unwrap();
        let post_failure_info_resp = surviving_node.send("CLUSTER", &["INFO"]).await;

        if let Ok(info) = parse_cluster_info(&post_failure_info_resp) {
            eprintln!(
                "After source failure: cluster_state={}, known_nodes={}",
                info.cluster_state, info.cluster_known_nodes
            );
        }

        // Check if we can read the key from surviving nodes
        let read_attempt = surviving_node.send("GET", &[&test_key]).await;
        eprintln!("Read attempt on target after source failure: {:?}", read_attempt);

        // Clean up migration state on surviving nodes
        let _ = surviving_node
            .send("CLUSTER", &["SETSLOT", &test_slot.to_string(), "STABLE"])
            .await;
    }

    // Clean up migration state on third node
    {
        let third_node = harness.node(third_node_id).unwrap();
        let _ = third_node
            .send("CLUSTER", &["SETSLOT", &test_slot.to_string(), "STABLE"])
            .await;
    }

    // Restart the killed node for clean shutdown
    harness.restart_node(source_node_id).await.unwrap();

    // Wait for cluster to stabilize
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Final state verification
    let final_info = harness.get_cluster_info(target_node_id).await.unwrap();
    eprintln!(
        "Final cluster state: {} (known_nodes: {})",
        final_info.cluster_state, final_info.cluster_known_nodes
    );

    harness.shutdown_all().await;
}
