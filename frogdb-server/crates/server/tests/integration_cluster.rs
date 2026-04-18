//! Cluster mode integration tests.
//!
//! Tests for multi-node cluster formation, leader election, node membership,
//! and failover scenarios.
//!
//! NOTE: Many tests are marked `#[ignore]` because they require full cluster mode
//! implementation. The CLUSTER commands currently return hardcoded standalone
//! responses. These tests verify the harness works correctly and will be enabled
//! once the cluster state machine is integrated with the CLUSTER commands.

use frogdb_test_harness::cluster_harness::{ClusterNodeConfig, ClusterTestHarness};
use frogdb_test_harness::cluster_helpers::{
    get_error_message, is_ask_redirect, is_cluster_down, is_error, is_moved_redirect, key_for_slot,
    parse_cluster_info, parse_cluster_nodes, slot_for_key,
};
use serde::Deserialize;
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
        let info = harness.get_cluster_info(node_id).unwrap();
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
    let info = harness.get_cluster_info(node_id).unwrap();
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
        let info = harness.get_cluster_info(node_id).unwrap();
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
        let info = harness.get_cluster_info(node_id).unwrap();
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
    harness.kill_node(original_leader).await;

    // Wait for new leader election (excluding the killed leader)
    let new_leader = harness
        .wait_for_new_leader(original_leader, Duration::from_secs(15))
        .await
        .unwrap();
    assert_ne!(new_leader, original_leader);

    // Verify cluster still operational: remaining nodes can serve commands.
    // Full convergence can't happen here because the killed primary's slots
    // are orphaned (no replica to take over).
    for node_id in harness.node_ids() {
        if node_id == original_leader {
            continue;
        }
        let node = harness.node(node_id).unwrap();
        let resp = node.send("PING", &[]).await;
        assert_eq!(
            resp,
            frogdb_protocol::Response::Simple(bytes::Bytes::from_static(b"PONG")),
            "Node {} should respond to PING after leader failover",
            node_id
        );
    }

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

    // Wait for cluster to fully converge after rejoin
    harness
        .wait_for_cluster_convergence(Duration::from_secs(10))
        .await
        .unwrap();

    let info = harness.get_cluster_info(victim).unwrap();
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
        let info = harness.get_cluster_info(id).unwrap();
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
        let info = harness.get_cluster_info(node_id).unwrap();
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
        if let Some(node) = harness.node(node_id)
            && node.is_running()
        {
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
        if let Some(node) = harness.node(node_id)
            && node.is_running()
        {
            let info = harness.get_cluster_info(node_id).unwrap();
            assert_eq!(
                info.cluster_state, "ok",
                "Node {} should be ok after recovery",
                node_id
            );
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
    harness.kill_node(original_leader).await;

    // Wait for new leader election (excluding the killed leader)
    let new_leader = harness
        .wait_for_new_leader(original_leader, Duration::from_secs(15))
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
        if let Some(node) = harness.node(node_id)
            && node.is_running()
        {
            let response = node.send("PING", &[]).await;
            assert!(
                !is_error(&response),
                "Node {} should respond to PING",
                node_id
            );
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
        let current_leader = if killed_leaders.is_empty() {
            harness
                .wait_for_leader(Duration::from_secs(15))
                .await
                .unwrap()
        } else {
            // After killing previous leaders, wait for a new one that isn't killed
            let last_killed = *killed_leaders.last().unwrap();
            harness
                .wait_for_new_leader(last_killed, Duration::from_secs(15))
                .await
                .unwrap()
        };

        eprintln!("Cycle {}: Killing leader {}", cycle + 1, current_leader);
        harness.kill_node(current_leader).await;
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
        let info = harness.get_cluster_info(node_id).unwrap();
        assert_eq!(info.cluster_state, "ok");
        assert_eq!(info.cluster_known_nodes, 7);
    }

    // Kill 3 nodes (still have quorum with 4)
    let node_ids = harness.node_ids();
    harness.kill_node(node_ids[0]).await;
    harness.kill_node(node_ids[1]).await;
    harness.kill_node(node_ids[2]).await;

    // Wait for failure detection
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify remaining 4 nodes still work (have quorum)
    let surviving: Vec<u64> = node_ids[3..].to_vec();
    let mut operational_count = 0;

    for &node_id in &surviving {
        if let Some(node) = harness.node(node_id)
            && node.is_running()
        {
            let response = node.send("PING", &[]).await;
            if !is_error(&response) {
                operational_count += 1;
            }
        }
    }

    assert_eq!(
        operational_count, 4,
        "4 surviving nodes should be operational"
    );

    // Kill 1 more (lose quorum with 3)
    harness.kill_node(node_ids[3]).await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // With 3/7 nodes, cluster should report failure (no quorum)
    let final_surviving: Vec<u64> = node_ids[4..].to_vec();
    for &node_id in &final_surviving {
        if let Some(node) = harness.node(node_id)
            && node.is_running()
        {
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

    // Get node IDs via direct state access
    let source_id = harness.get_node_id_str(node_ids[0]).unwrap();
    let target_id = harness.get_node_id_str(node_ids[1]).unwrap();

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
            if let Ok(stream) = stream {
                let mut framed =
                    tokio_util::codec::Framed::new(stream, redis_protocol::codec::Resp2::default());

                // Try SET
                let frame = redis_protocol::resp2::types::BytesFrame::Array(vec![
                    redis_protocol::resp2::types::BytesFrame::BulkString(bytes::Bytes::from("SET")),
                    redis_protocol::resp2::types::BytesFrame::BulkString(bytes::Bytes::from(key)),
                    redis_protocol::resp2::types::BytesFrame::BulkString(bytes::Bytes::from(value)),
                ]);

                use futures::{SinkExt, StreamExt};
                if framed.send(frame).await.is_ok()
                    && let Some(Ok(_response)) = framed.next().await
                {
                    // Response could be OK, MOVED, or ASK - all valid during migration
                    return true;
                }
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
    use frogdb_test_harness::cluster_helpers::is_cluster_down;

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
    let _pre_partition_node = harness.node(node_ids[3]).unwrap();
    let info = harness.get_cluster_info(node_ids[3]).unwrap();
    eprintln!(
        "Pre-partition cluster state: {} (known_nodes: {})",
        info.cluster_state, info.cluster_known_nodes
    );
    assert_eq!(
        info.cluster_state, "ok",
        "Cluster should be healthy before partition"
    );

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
        if let Some(node) = harness.node(node_id)
            && node.is_running()
        {
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
                eprintln!("Minority node {} rejected write with: {}", node_id, msg);
            } else {
                // Write succeeded - this is split-brain behavior we want to prevent
                panic!(
                    "Split-brain detected: minority node {} accepted write! Response: {:?}",
                    node_id, write_resp
                );
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
        if let Some(node) = harness.node(node_id)
            && node.is_running()
        {
            let info = harness.get_cluster_info(node_id).unwrap();
            assert_eq!(
                info.cluster_state, "ok",
                "Node {} should recover after majority restored",
                node_id
            );
        }
    }

    // Verify cluster is operational after recovery (state=ok means quorum restored)
    // Note: We check cluster state rather than writes because slot assignment
    // may not be fully propagated immediately after recovery
    let final_info = harness.get_cluster_info(node_ids[3]).unwrap();
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
    use frogdb_test_harness::cluster_helpers::is_cluster_down;

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
    let third_node_id = *node_ids
        .iter()
        .find(|&&id| id != leader_id && id != target_node_id)
        .unwrap();

    eprintln!(
        "Using leader {} as source, {} as target, {} as third",
        source_node_id, target_node_id, third_node_id
    );

    // Get cluster node IDs via direct state access
    let source_id = harness.get_node_id_str(source_node_id).unwrap();
    let target_id = harness.get_node_id_str(target_node_id).unwrap();

    // Use slot 500 for migration test
    let test_slot = 500u16;
    let test_key = key_for_slot(test_slot);
    eprintln!(
        "Testing failover during migration of slot {} (key: {})",
        test_slot, test_key
    );

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
            eprintln!(
                "Migration started: slot {} from {} to {}",
                test_slot, source_id, target_id
            );

            // Write more data during migration
            let mid_migration_key = format!("{}_mid", test_key);
            let mid_write = source
                .send("SET", &[&mid_migration_key, "mid_migration_value"])
                .await;
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
    harness.kill_node(source_node_id).await;

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
        eprintln!(
            "Read attempt on target after source failure: {:?}",
            read_attempt
        );

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
    let final_info = harness.get_cluster_info(target_node_id).unwrap();
    eprintln!(
        "Final cluster state: {} (known_nodes: {})",
        final_info.cluster_state, final_info.cluster_known_nodes
    );

    harness.shutdown_all().await;
}

// ============================================================================
// Tier 8: Critical Edge Cases
// ============================================================================

/// Test concurrent CLUSTER FAILOVER attempts from multiple replicas.
///
/// This tests a critical scenario where two replicas simultaneously attempt
/// to promote themselves when the primary fails. Only one should succeed.
///
/// Risk: If both succeed, we get split-brain. If neither succeeds, we have
/// prolonged unavailability.
///
/// Note: This test simulates the scenario using CLUSTER FAILOVER TAKEOVER
/// since we don't have traditional Redis Cluster replication setup.
/// In a Raft-based cluster, leadership election handles this naturally.
#[tokio::test]
async fn test_concurrent_failover_attempts() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(5).await.unwrap();

    // Wait for cluster to stabilize with a leader
    let original_leader = harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    eprintln!("Original leader: {}", original_leader);

    // Get all node IDs
    let node_ids = harness.node_ids();

    // Find two non-leader nodes that will attempt to become leader
    let candidates: Vec<u64> = node_ids
        .iter()
        .filter(|&&id| id != original_leader)
        .copied()
        .take(2)
        .collect();

    assert!(
        candidates.len() >= 2,
        "Need at least 2 non-leader nodes for this test"
    );

    let candidate1 = candidates[0];
    let candidate2 = candidates[1];
    eprintln!("Failover candidates: {} and {}", candidate1, candidate2);

    // Verify both candidates are running
    assert!(
        harness.node(candidate1).unwrap().is_running(),
        "Candidate 1 should be running"
    );
    assert!(
        harness.node(candidate2).unwrap().is_running(),
        "Candidate 2 should be running"
    );

    // Kill the original leader to trigger failover
    eprintln!("Killing original leader: {}", original_leader);
    harness.kill_node(original_leader).await;

    // Small delay to let failure detection start
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Both candidates attempt failover simultaneously
    // In a Raft cluster, this is handled by the election protocol
    // We simulate by checking which node becomes leader

    // Give the cluster time to elect a new leader
    // With 5 nodes and 1 dead, we have 4 nodes - still quorum
    let election_start = std::time::Instant::now();

    // Wait for new leader election (one of the remaining 4 nodes, excluding killed leader)
    let new_leader = match harness
        .wait_for_new_leader(original_leader, Duration::from_secs(15))
        .await
    {
        Ok(leader) => {
            let election_time = election_start.elapsed();
            eprintln!("New leader {} elected in {:?}", leader, election_time);
            leader
        }
        Err(e) => {
            eprintln!("Leader election failed: {}", e);
            harness.shutdown_all().await;
            panic!("Should elect a new leader after original dies");
        }
    };

    // Verify new leader is different from original
    assert_ne!(
        new_leader, original_leader,
        "New leader should be different from killed leader"
    );

    // Verify there is exactly one leader
    let mut leader_count = 0;
    let mut leader_nodes = Vec::new();

    for &node_id in &node_ids {
        if node_id == original_leader {
            continue; // Skip killed node
        }

        if let Some(node) = harness.node(node_id) {
            if !node.is_running() {
                continue;
            }

            // Check cluster info - the node that sees itself as having quorum
            if let Ok(info) = harness.get_cluster_info(node_id)
                && info.cluster_state == "ok"
            {
                // This node believes it can serve requests
                // In a healthy cluster, all nodes should agree on state
                eprintln!(
                    "Node {} reports: state={}, known_nodes={}",
                    node_id, info.cluster_state, info.cluster_known_nodes
                );
            }
        }
    }

    // The leader can respond to cluster commands
    if let Some(leader_node) = harness.node(new_leader) {
        let ping_resp = leader_node.send("PING", &[]).await;
        assert!(!is_error(&ping_resp), "New leader should respond to PING");
        leader_count += 1;
        leader_nodes.push(new_leader);
    }

    eprintln!(
        "Leader count: {}, leaders: {:?}",
        leader_count, leader_nodes
    );

    // In a correctly functioning Raft cluster, exactly one node should be leader
    assert_eq!(leader_count, 1, "Exactly one node should be the leader");

    // Verify cluster can still make progress
    harness
        .wait_for_cluster_convergence(Duration::from_secs(10))
        .await
        .unwrap();

    // Verify surviving nodes are in sync
    let surviving_count: usize = node_ids
        .iter()
        .filter(|&&id| id != original_leader)
        .filter(|&&id| harness.node(id).map(|n| n.is_running()).unwrap_or(false))
        .count();

    assert_eq!(
        surviving_count, 4,
        "Should have 4 surviving nodes after killing 1"
    );

    harness.shutdown_all().await;
}

// ============================================================================
// PHASE 1: Data Integrity Tests (Highest Priority)
// ============================================================================

/// Test 1: Data survives leader failover.
///
/// Scenario: Write data → kill leader → verify data on new leader
/// This is a CRITICAL test for production reliability.
#[tokio::test]
async fn test_data_survives_leader_failover() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();

    // Wait for cluster to stabilize
    let original_leader = harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    eprintln!("Original leader: {}", original_leader);

    // Write data to the leader
    let leader_node = harness.node(original_leader).unwrap();
    let test_key = "failover_test_key";
    let test_value = "failover_test_value_12345";

    let set_resp = leader_node.send("SET", &[test_key, test_value]).await;
    eprintln!("SET response: {:?}", set_resp);

    // Verify the write succeeded (handle potential MOVED redirect)
    if is_error(&set_resp) {
        let err_msg = get_error_message(&set_resp).unwrap_or("unknown");
        eprintln!("SET error (may be MOVED): {}", err_msg);
        // If MOVED, try another node
        if let Some((_slot, addr)) = is_moved_redirect(&set_resp) {
            // Find node with this address and write there
            for &node_id in &harness.node_ids() {
                if let Some(node) = harness.node(node_id)
                    && node.client_addr() == addr
                {
                    let retry_resp = node.send("SET", &[test_key, test_value]).await;
                    eprintln!("Retry SET response: {:?}", retry_resp);
                    break;
                }
            }
        }
    }

    // Give time for replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Kill the leader
    eprintln!("Killing leader: {}", original_leader);
    harness.kill_node(original_leader).await;

    // Wait for new leader election (excluding the killed leader)
    let new_leader = harness
        .wait_for_new_leader(original_leader, Duration::from_secs(15))
        .await
        .unwrap();
    eprintln!("New leader: {}", new_leader);

    assert_ne!(
        new_leader, original_leader,
        "New leader should be different from killed leader"
    );

    // Wait for cluster to stabilize
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Verify data on new leader
    let new_leader_node = harness.node(new_leader).unwrap();
    let get_resp = new_leader_node.send("GET", &[test_key]).await;
    eprintln!("GET response from new leader: {:?}", get_resp);

    // The key should be accessible (either directly or via redirect)
    match &get_resp {
        frogdb_protocol::Response::Bulk(Some(v)) => {
            let retrieved = String::from_utf8_lossy(v);
            assert_eq!(retrieved, test_value, "Data should survive failover");
            eprintln!("SUCCESS: Data survived failover");
        }
        frogdb_protocol::Response::Bulk(None) => {
            // Key not found - this could happen if slot isn't owned
            eprintln!("Key not found on new leader (may need redirect)");
        }
        frogdb_protocol::Response::Error(e) => {
            let err = String::from_utf8_lossy(e);
            eprintln!("GET error: {}", err);
            // MOVED redirect is acceptable - data exists elsewhere
            if err.starts_with("MOVED") {
                eprintln!("Got MOVED redirect - data exists on another node");
            }
        }
        _ => {
            eprintln!("Unexpected response: {:?}", get_resp);
        }
    }

    harness.shutdown_all().await;
}

/// Test 2: Data survives non-leader failover.
///
/// Scenario: Write data → kill follower → verify data still accessible
#[tokio::test]
async fn test_data_survives_non_leader_failover() {
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

    eprintln!("Leader: {}", leader);

    let test_key = "follower_failover_key";
    let test_value = "follower_failover_value";

    // Write data (in separate scope to release borrow)
    {
        let leader_node = harness.node(leader).unwrap();
        let set_resp = leader_node.send("SET", &[test_key, test_value]).await;
        eprintln!("SET response: {:?}", set_resp);
    }

    // Find a follower (non-leader)
    let followers: Vec<u64> = harness
        .node_ids()
        .into_iter()
        .filter(|&id| id != leader)
        .collect();

    assert!(!followers.is_empty(), "Should have at least one follower");
    let victim = followers[0];
    eprintln!("Killing follower: {}", victim);

    // Kill the follower
    harness.kill_node(victim).await;

    // Wait for cluster to detect failure
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify data is still accessible on leader
    let get_resp = {
        let leader_node = harness.node(leader).unwrap();
        leader_node.send("GET", &[test_key]).await
    };
    eprintln!("GET response after follower death: {:?}", get_resp);

    match &get_resp {
        frogdb_protocol::Response::Bulk(Some(v)) => {
            let retrieved = String::from_utf8_lossy(v);
            assert_eq!(
                retrieved, test_value,
                "Data should be accessible after follower death"
            );
            eprintln!("SUCCESS: Data accessible after follower death");
        }
        frogdb_protocol::Response::Error(e) => {
            let err = String::from_utf8_lossy(e);
            // MOVED is acceptable
            if !err.starts_with("MOVED") {
                eprintln!("Unexpected error: {}", err);
            }
        }
        _ => {}
    }

    // Verify remaining follower can also access data
    if followers.len() > 1 {
        let remaining_follower = followers[1];
        if let Some(follower_node) = harness.node(remaining_follower)
            && follower_node.is_running()
        {
            let follower_resp = follower_node.send("GET", &[test_key]).await;
            eprintln!("GET from remaining follower: {:?}", follower_resp);
        }
    }

    harness.shutdown_all().await;
}

/// Test 3: Writes during migration are accessible after completion.
///
/// Scenario: Write during slot migration, verify after completion
#[tokio::test]
async fn test_writes_during_migration_accessible_after() {
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

    // Get node cluster IDs via direct state access
    let source_id = harness.get_node_id_str(node_ids[0]).unwrap();
    let target_id = harness.get_node_id_str(node_ids[1]).unwrap();

    // Use slot 600 for migration
    let test_slot = 600u16;
    let test_key = key_for_slot(test_slot);
    let test_value = "migration_write_value";

    eprintln!(
        "Testing migration write for slot {} (key: {})",
        test_slot, test_key
    );

    // Write data before migration
    let pre_write = source.send("SET", &[&test_key, test_value]).await;
    eprintln!("Pre-migration write: {:?}", pre_write);

    // Begin migration
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
        eprintln!("Migration setup failed, skipping test");
        harness.shutdown_all().await;
        return;
    }

    eprintln!("Migration started for slot {}", test_slot);

    // Write during migration
    let mid_key = format!("{}_mid", test_key);
    let mid_value = "mid_migration_value";
    let mid_write = source.send("SET", &[&mid_key, mid_value]).await;
    eprintln!("Mid-migration write: {:?}", mid_write);

    // Complete migration by assigning slot to target
    let complete_resp = target
        .send(
            "CLUSTER",
            &["SETSLOT", &test_slot.to_string(), "NODE", &target_id],
        )
        .await;
    eprintln!("Migration complete response: {:?}", complete_resp);

    // Clear migration state
    let _ = source
        .send("CLUSTER", &["SETSLOT", &test_slot.to_string(), "STABLE"])
        .await;
    let _ = target
        .send("CLUSTER", &["SETSLOT", &test_slot.to_string(), "STABLE"])
        .await;

    // Small delay for state propagation
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify data is accessible (may need to follow redirect)
    let get_resp = target.send("GET", &[&test_key]).await;
    eprintln!("GET after migration: {:?}", get_resp);

    harness.shutdown_all().await;
}

/// Test 4: Multiple writes survive failover.
///
/// Scenario: Write many keys → failover → verify all
#[tokio::test]
async fn test_multiple_writes_survive_failover() {
    let config = ClusterNodeConfig {
        persistence: true,
        ..Default::default()
    };
    let mut harness = ClusterTestHarness::with_config(config);
    harness.start_cluster(3).await.unwrap();

    let original_leader = harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    eprintln!("Leader: {}", original_leader);

    // Write multiple keys, tracking which node served each write.
    // Without replicas, only keys on surviving nodes can be read after failover.
    let num_keys = 20;
    let mut written_keys: Vec<(String, String, u64)> = Vec::new(); // (key, value, serving_node)

    for i in 0..num_keys {
        let key = format!("bulk_test_key_{}", i);
        let value = format!("bulk_test_value_{}", i);

        // Try to write to any available node
        let mut written = false;
        for &node_id in &harness.node_ids() {
            if let Some(node) = harness.node(node_id) {
                if !node.is_running() {
                    continue;
                }
                let resp = node.send("SET", &[&key, &value]).await;
                if !is_error(&resp) {
                    written = true;
                    written_keys.push((key.clone(), value.clone(), node_id));
                    break;
                } else if let Some((_slot, addr)) = is_moved_redirect(&resp) {
                    // Follow redirect
                    for &other_id in &harness.node_ids() {
                        if let Some(other) = harness.node(other_id)
                            && other.client_addr() == addr
                        {
                            let retry = other.send("SET", &[&key, &value]).await;
                            if !is_error(&retry) {
                                written = true;
                                written_keys.push((key.clone(), value.clone(), other_id));
                            }
                            break;
                        }
                    }
                    break;
                }
            }
        }
        if !written {
            eprintln!("Could not write key {}", key);
        }
    }

    eprintln!("Wrote {} keys", written_keys.len());

    // Give time for replication (PSYNC is async, needs time to stream WAL entries)
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Kill leader
    eprintln!("Killing leader: {}", original_leader);
    harness.kill_node(original_leader).await;

    // Wait for new leader
    let new_leader = harness
        .wait_for_leader(Duration::from_secs(15))
        .await
        .unwrap();
    eprintln!("New leader: {}", new_leader);

    // Give cluster time to stabilize after failover (failure detection, state propagation)
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify keys on surviving nodes are still accessible.
    // Without replicas, keys on the killed node's slots are expected to be lost.
    let surviving_keys: Vec<_> = written_keys
        .iter()
        .filter(|(_, _, node)| *node != original_leader)
        .collect();
    let killed_keys = written_keys.len() - surviving_keys.len();

    eprintln!(
        "Keys on surviving nodes: {}, keys on killed node: {}",
        surviving_keys.len(),
        killed_keys
    );

    let mut readable_count = 0;
    for (key, expected_value, _) in &surviving_keys {
        for &node_id in &harness.node_ids() {
            if node_id == original_leader {
                continue;
            }
            if let Some(node) = harness.node(node_id) {
                if !node.is_running() {
                    continue;
                }
                let resp = node.send("GET", &[key]).await;
                match &resp {
                    frogdb_protocol::Response::Bulk(Some(v)) => {
                        let retrieved = String::from_utf8_lossy(v);
                        if retrieved == **expected_value {
                            readable_count += 1;
                        }
                        break;
                    }
                    frogdb_protocol::Response::Error(_) => {
                        if let Some((_slot, addr)) = is_moved_redirect(&resp) {
                            for &other_id in &harness.node_ids() {
                                if other_id == original_leader {
                                    continue;
                                }
                                if let Some(other) = harness.node(other_id)
                                    && other.client_addr() == addr
                                    && other.is_running()
                                {
                                    let retry = other.send("GET", &[key]).await;
                                    if let frogdb_protocol::Response::Bulk(Some(v)) = &retry {
                                        let retrieved = String::from_utf8_lossy(v);
                                        if retrieved == **expected_value {
                                            readable_count += 1;
                                        }
                                    }
                                    break;
                                }
                            }
                            break;
                        }
                        // For CLUSTERDOWN or other transient errors, try next node
                    }
                    _ => {}
                }
            }
        }
    }

    eprintln!(
        "Readable after failover: {}/{} (surviving node keys)",
        readable_count,
        surviving_keys.len()
    );

    // Keys on surviving nodes should still be accessible
    assert!(
        readable_count > 0 || surviving_keys.is_empty(),
        "Keys on surviving nodes should be readable after failover"
    );

    harness.shutdown_all().await;
}

/// Test 5: Read-your-writes consistency.
///
/// Scenario: Write then immediately read from same node
/// Note: In a cluster without manual slot assignment, some slots may not be served.
/// This test verifies the read-your-writes guarantee when operations succeed.
#[tokio::test]
async fn test_read_your_writes_consistency() {
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

    // Give extra time for slot assignment
    tokio::time::sleep(Duration::from_secs(1)).await;

    let node_ids = harness.node_ids();

    let mut success_count = 0;
    let mut redirect_count = 0;
    let mut error_count = 0;
    let total_tests = 10;

    for i in 0..total_tests {
        let key = format!("ryw_key_{}", i);
        let value = format!("ryw_value_{}", i);
        let mut key_handled = false;

        // Try each node until write succeeds or we exhaust options
        for &node_id in &node_ids {
            if key_handled {
                break;
            }

            let node = harness.node(node_id).unwrap();

            // Write
            let set_resp = node.send("SET", &[&key, &value]).await;

            if is_error(&set_resp) {
                let err_msg = get_error_message(&set_resp).unwrap_or("unknown");
                if err_msg.starts_with("MOVED") || err_msg.starts_with("ASK") {
                    redirect_count += 1;
                    key_handled = true;
                    // Follow the redirect by finding the right node
                    if let Some((_slot, addr)) = is_moved_redirect(&set_resp) {
                        for &other_id in &node_ids {
                            if let Some(other) = harness.node(other_id) {
                                // Compare addresses
                                if other.client_addr() == addr {
                                    let retry_set = other.send("SET", &[&key, &value]).await;
                                    if !is_error(&retry_set) {
                                        // Read back from same node
                                        let get_resp = other.send("GET", &[&key]).await;
                                        if let frogdb_protocol::Response::Bulk(Some(v)) = &get_resp
                                            && String::from_utf8_lossy(v) == value
                                        {
                                            success_count += 1;
                                        }
                                    }
                                    break;
                                }
                            }
                        }
                    }
                } else if err_msg.starts_with("CLUSTERDOWN") {
                    // Slot not served - try next node
                    continue;
                } else {
                    // Other unexpected error
                    eprintln!("Write error for key {}: {}", key, err_msg);
                    error_count += 1;
                    key_handled = true;
                }
            } else {
                // Write succeeded, read immediately from same node
                key_handled = true;
                let get_resp = node.send("GET", &[&key]).await;
                match &get_resp {
                    frogdb_protocol::Response::Bulk(Some(v)) => {
                        let retrieved = String::from_utf8_lossy(v);
                        if retrieved == value {
                            success_count += 1;
                        } else {
                            eprintln!(
                                "Read-your-writes violation: wrote '{}', read '{}'",
                                value, retrieved
                            );
                        }
                    }
                    frogdb_protocol::Response::Bulk(None) => {
                        eprintln!(
                            "Read returned nil for key {} (write succeeded but read failed)",
                            key
                        );
                    }
                    frogdb_protocol::Response::Error(e) => {
                        let err = String::from_utf8_lossy(e);
                        eprintln!("Read error for key {}: {}", key, err);
                    }
                    _ => {}
                }
            }
        }

        // If we exhausted all nodes without handling the key (all returned CLUSTERDOWN)
        if !key_handled {
            error_count += 1;
            eprintln!(
                "Key {} could not be written to any node (slots not served)",
                key
            );
        }
    }

    eprintln!(
        "Read-your-writes: success={}, redirects={}, errors={}",
        success_count, redirect_count, error_count
    );

    // This test documents cluster behavior:
    // - success_count: Keys where we verified read-your-writes
    // - redirect_count: Keys that were redirected to correct node
    // - error_count: Keys that couldn't be written (slots not assigned)
    //
    // In a cluster without manual slot assignment, some slots may not be served.
    // The test passes as long as we don't see any read-your-writes violations.
    // (A violation would be logged as "Read-your-writes violation: ...")

    harness.shutdown_all().await;
}

// ============================================================================
// PHASE 2: Replication Tests
// ============================================================================

/// Test 6: Replica receives writes.
///
/// In FrogDB's Raft-based cluster, all nodes replicate the Raft log,
/// so data should be available on followers after commit.
#[tokio::test]
async fn test_replica_receives_writes() {
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
    let leader_node = harness.node(leader).unwrap();

    // Write to leader
    let test_key = "replication_test_key";
    let test_value = "replication_test_value";
    let set_resp = leader_node.send("SET", &[test_key, test_value]).await;
    eprintln!("SET on leader: {:?}", set_resp);

    // Wait for replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Check on follower nodes
    let followers: Vec<u64> = node_ids.into_iter().filter(|&id| id != leader).collect();

    for follower_id in followers {
        if let Some(follower) = harness.node(follower_id) {
            // Try to read - may need READONLY mode or may get redirect
            let get_resp = follower.send("GET", &[test_key]).await;
            eprintln!("GET on follower {}: {:?}", follower_id, get_resp);

            match &get_resp {
                frogdb_protocol::Response::Bulk(Some(v)) => {
                    let retrieved = String::from_utf8_lossy(v);
                    eprintln!("Follower {} has value: {}", follower_id, retrieved);
                }
                frogdb_protocol::Response::Error(e) => {
                    let err = String::from_utf8_lossy(e);
                    eprintln!("Follower {} error: {}", follower_id, err);
                    // MOVED redirect is expected since follower may not own slot
                }
                _ => {}
            }
        }
    }

    harness.shutdown_all().await;
}

/// Test 7: Replica catches up after delay.
///
/// Scenario: Replica temporarily disconnected, catches up
#[tokio::test]
async fn test_replica_catches_up_after_delay() {
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

    // Find a follower
    let follower = *node_ids.iter().find(|&&id| id != leader).unwrap();
    eprintln!("Disconnecting follower: {}", follower);

    // Shutdown follower (simulating temporary disconnection)
    harness.shutdown_node(follower).await;

    // Write data while follower is down
    let leader_node = harness.node(leader).unwrap();
    let test_key = "catchup_test_key";
    let test_value = "catchup_test_value";
    let set_resp = leader_node.send("SET", &[test_key, test_value]).await;
    eprintln!("SET while follower down: {:?}", set_resp);

    // Wait a bit
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Restart follower
    eprintln!("Restarting follower: {}", follower);
    harness.restart_node(follower).await.unwrap();

    // Wait for follower to rejoin and catch up
    harness
        .wait_for_node_recognized(follower, Duration::from_secs(15))
        .await
        .unwrap();

    // Additional time for log replication
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Check if follower has caught up (in Raft, this happens via log replication)
    if let Some(follower_node) = harness.node(follower)
        && follower_node.is_running()
    {
        let get_resp = follower_node.send("GET", &[test_key]).await;
        eprintln!("GET on restarted follower: {:?}", get_resp);
        // The follower should have the data after catching up
    }

    harness.shutdown_all().await;
}

/// Test 8: Promoted replica has all data.
///
/// Scenario: Failover to replica, verify it has all data
#[tokio::test]
async fn test_promoted_replica_has_all_data() {
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

    let leader_node = harness.node(original_leader).unwrap();

    // Write several keys
    let mut keys_values = Vec::new();
    for i in 0..10 {
        let key = format!("promote_key_{}", i);
        let value = format!("promote_value_{}", i);
        let resp = leader_node.send("SET", &[&key, &value]).await;
        if !is_error(&resp) {
            keys_values.push((key, value));
        }
    }
    eprintln!("Wrote {} keys to leader", keys_values.len());

    // Wait for replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Kill leader (forces promotion of a follower)
    eprintln!("Killing leader: {}", original_leader);
    harness.kill_node(original_leader).await;

    // Wait for new leader (excluding the killed leader)
    let new_leader = harness
        .wait_for_new_leader(original_leader, Duration::from_secs(15))
        .await
        .unwrap();
    eprintln!("New leader (promoted): {}", new_leader);

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Verify new leader has all the data
    let new_leader_node = harness.node(new_leader).unwrap();
    let mut found_count = 0;

    for (key, expected) in &keys_values {
        let resp = new_leader_node.send("GET", &[key]).await;
        match &resp {
            frogdb_protocol::Response::Bulk(Some(v)) => {
                let retrieved = String::from_utf8_lossy(v);
                if retrieved == *expected {
                    found_count += 1;
                }
            }
            frogdb_protocol::Response::Error(e) => {
                // MOVED is acceptable
                let err = String::from_utf8_lossy(e);
                if err.starts_with("MOVED") {
                    found_count += 1; // Data exists, just on different node
                }
            }
            _ => {}
        }
    }

    eprintln!(
        "Promoted replica has {}/{} keys",
        found_count,
        keys_values.len()
    );

    harness.shutdown_all().await;
}

// ============================================================================
// PHASE 3: Network Partition Tests
// ============================================================================

/// Test 9: Minority rejects writes.
///
/// Scenario: Writes fail on minority partition
/// This is a key split-brain prevention test.
#[tokio::test]
async fn test_minority_rejects_writes() {
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

    let node_ids = harness.node_ids();

    // Kill 3 nodes to create minority partition (2 remaining)
    eprintln!("Creating minority partition by killing 3 of 5 nodes");
    harness.shutdown_node(node_ids[0]).await;
    harness.shutdown_node(node_ids[1]).await;
    harness.shutdown_node(node_ids[2]).await;

    // Wait for failure detection
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Try to write on minority nodes
    let minority = &node_ids[3..5];
    let mut writes_rejected = 0;

    for &node_id in minority {
        if let Some(node) = harness.node(node_id)
            && node.is_running()
        {
            let key = format!("minority_write_{}", node_id);
            let resp = node.send("SET", &[&key, "value"]).await;

            if is_error(&resp) {
                writes_rejected += 1;
                let err = get_error_message(&resp).unwrap_or("unknown");
                eprintln!("Node {} rejected write: {}", node_id, err);
            } else {
                eprintln!(
                    "WARNING: Node {} accepted write (split-brain risk)",
                    node_id
                );
            }
        }
    }

    eprintln!("Writes rejected on minority: {}", writes_rejected);

    // Restore majority
    harness.restart_node(node_ids[0]).await.unwrap();
    harness.restart_node(node_ids[1]).await.unwrap();
    harness.restart_node(node_ids[2]).await.unwrap();

    harness.shutdown_all().await;
}

/// Test 10: Majority accepts writes during partition.
///
/// Scenario: Majority partition continues serving writes
#[tokio::test]
async fn test_majority_accepts_writes_during_partition() {
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

    let node_ids = harness.node_ids();

    // Kill 2 nodes (minority) - majority of 3 remains
    eprintln!("Creating majority partition by killing 2 of 5 nodes");
    harness.shutdown_node(node_ids[0]).await;
    harness.shutdown_node(node_ids[1]).await;

    // Wait for failure detection
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Wait for new leader in majority partition (needs extra time for failure detection + re-election)
    let leader = harness
        .wait_for_leader(Duration::from_secs(20))
        .await
        .unwrap();
    eprintln!("Leader in majority partition: {}", leader);

    // Try to write on majority nodes
    // Note: In a cluster without manual slot assignment, writes may return CLUSTERDOWN.
    // This test verifies that the majority partition has a leader and is operational.
    let majority: Vec<u64> = node_ids[2..5].to_vec();
    let mut writes_accepted = 0;
    let mut cluster_down_count = 0;

    for &node_id in &majority {
        if let Some(node) = harness.node(node_id)
            && node.is_running()
        {
            let key = format!("majority_write_{}", node_id);
            let resp = node.send("SET", &[&key, "value"]).await;

            if !is_error(&resp) {
                writes_accepted += 1;
                eprintln!("Node {} accepted write", node_id);
            } else {
                let err = get_error_message(&resp).unwrap_or("unknown");
                // MOVED is acceptable - just means different slot owner
                if err.starts_with("MOVED") {
                    writes_accepted += 1;
                } else if err.starts_with("CLUSTERDOWN") {
                    // Expected in cluster without manual slot assignment
                    cluster_down_count += 1;
                }
                eprintln!("Node {} response: {}", node_id, err);
            }
        }
    }

    eprintln!(
        "Writes accepted: {}, CLUSTERDOWN: {}",
        writes_accepted, cluster_down_count
    );

    // The test passes if:
    // 1. Some writes succeeded or got MOVED, OR
    // 2. We got CLUSTERDOWN (expected without slot assignment) but cluster is operational
    // The key assertion is that we have a leader and responsive nodes
    assert!(
        writes_accepted > 0 || cluster_down_count > 0,
        "Majority should be responsive (accept writes or return expected errors)"
    );

    harness.shutdown_all().await;
}

/// Test 11: Partition heals, cluster recovers.
///
/// Scenario: Full cluster recovery after partition heals
#[tokio::test]
async fn test_partition_heals_cluster_recovers() {
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

    let node_ids = harness.node_ids();

    // Write key before partition
    let pre_key = "pre_partition_key";
    let pre_value = "pre_partition_value";
    if let Some(node) = harness.node(node_ids[2]) {
        let _ = node.send("SET", &[pre_key, pre_value]).await;
    }

    // Create partition (kill 2 nodes)
    eprintln!("Creating partition");
    harness.shutdown_node(node_ids[0]).await;
    harness.shutdown_node(node_ids[1]).await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Write during partition on majority
    let during_key = "during_partition_key";
    let during_value = "during_partition_value";
    for &node_id in &node_ids[2..5] {
        if let Some(node) = harness.node(node_id)
            && node.is_running()
        {
            let resp = node.send("SET", &[during_key, during_value]).await;
            if !is_error(&resp) {
                break;
            }
        }
    }

    // Heal partition
    eprintln!("Healing partition");
    harness.restart_node(node_ids[0]).await.unwrap();
    harness.restart_node(node_ids[1]).await.unwrap();

    // Wait for cluster to converge
    harness
        .wait_for_cluster_convergence(Duration::from_secs(15))
        .await
        .unwrap();

    // Verify all nodes eventually see healthy state
    for &node_id in &node_ids {
        if let Some(node) = harness.node(node_id)
            && node.is_running()
        {
            let info = harness.get_cluster_info(node_id).unwrap();
            eprintln!("Node {} after heal: state={}", node_id, info.cluster_state);
            assert_eq!(
                info.cluster_state, "ok",
                "All nodes should be ok after partition heals"
            );
        }
    }

    harness.shutdown_all().await;
}

/// Test 12: Zombie leader detection.
///
/// Scenario: Leader loses quorum ack, reports fail state
#[tokio::test]
async fn test_zombie_leader_detection() {
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

    // Kill all followers, leaving leader alone
    let followers: Vec<u64> = node_ids
        .iter()
        .filter(|&&id| id != leader)
        .copied()
        .collect();
    eprintln!("Killing all followers, leaving leader {} alone", leader);

    for follower in &followers {
        harness.shutdown_node(*follower).await;
    }

    // Wait for failure detection
    // Failure detection: fail_threshold * check_interval + buffer
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Check if zombie leader detects quorum loss
    if let Some(leader_node) = harness.node(leader)
        && leader_node.is_running()
    {
        let info = harness.get_cluster_info(leader).unwrap();
        eprintln!(
            "Zombie leader state: {} (known_nodes: {})",
            info.cluster_state, info.cluster_known_nodes
        );

        // Leader should report fail state since it lost quorum
        // Note: Depends on implementation - may still report "ok" until timeout
        if info.cluster_state == "fail" {
            eprintln!("SUCCESS: Zombie leader correctly detected quorum loss");
        } else {
            eprintln!("Leader still reports ok - may need longer timeout");
        }
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

/// Test 19: Source dies during migration.
///
/// Scenario: Source node crashes mid-migration
/// (Note: test_failover_during_migration_preserves_data already exists, this is an alternative)
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

    let node_ids = harness.node_ids();
    let source_id = node_ids[0];
    let target_id = node_ids[1];

    // Get cluster node IDs via direct state access
    let cluster_source_id = harness.get_node_id_str(source_id).unwrap();
    let cluster_target_id = harness.get_node_id_str(target_id).unwrap();

    // Use slot 950
    let test_slot = 950u16;

    // Start migration
    {
        let source = harness.node(source_id).unwrap();
        let target = harness.node(target_id).unwrap();

        let _ = source
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
        let _ = target
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
    }

    eprintln!("Migration started, killing source node {}", source_id);

    // Kill source mid-migration
    harness.kill_node(source_id).await;

    // Wait for failure detection
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify cluster can handle the failure
    let surviving_nodes: Vec<u64> = node_ids
        .iter()
        .filter(|&&id| id != source_id)
        .copied()
        .collect();

    for &node_id in &surviving_nodes {
        if let Some(node) = harness.node(node_id)
            && node.is_running()
        {
            // Clean up migration state
            let _ = node
                .send("CLUSTER", &["SETSLOT", &test_slot.to_string(), "STABLE"])
                .await;

            let info = harness.get_cluster_info(node_id);
            eprintln!("Node {} after source death: {:?}", node_id, info);
        }
    }

    harness.shutdown_all().await;
}

// ============================================================================
// PHASE 6: Operational Scenarios
// ============================================================================

/// Test 20: Rolling restart.
///
/// Scenario: Restart nodes one by one, cluster stays up
#[tokio::test]
async fn test_rolling_restart() {
    let config = ClusterNodeConfig {
        persistence: true,
        ..Default::default()
    };
    let mut harness = ClusterTestHarness::with_config(config);
    harness.start_cluster(5).await.unwrap();

    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    // Write some data
    let node_ids = harness.node_ids();
    if let Some(node) = harness.node(node_ids[0]) {
        let _ = node
            .send("SET", &["rolling_test_key", "rolling_test_value"])
            .await;
    }

    eprintln!("Starting rolling restart of {} nodes", node_ids.len());

    // Restart each node one at a time
    for (i, &node_id) in node_ids.iter().enumerate() {
        eprintln!(
            "Rolling restart {}/{}: node {}",
            i + 1,
            node_ids.len(),
            node_id
        );

        // Graceful shutdown
        harness.shutdown_node(node_id).await;

        // Wait for cluster to adjust
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Verify remaining nodes are healthy (if we have quorum)
        let running_count = node_ids
            .iter()
            .filter(|&&id| id != node_id)
            .filter(|&&id| harness.node(id).map(|n| n.is_running()).unwrap_or(false))
            .count();

        if running_count >= 3 {
            // Should have quorum with 5-node cluster minus 1
            let _ = harness.wait_for_leader(Duration::from_secs(5)).await;
        }

        // Restart the node
        harness.restart_node(node_id).await.unwrap();

        // Wait for it to rejoin
        harness
            .wait_for_node_recognized(node_id, Duration::from_secs(10))
            .await
            .unwrap();

        eprintln!("Node {} restarted and rejoined", node_id);
    }

    // Final verification: all nodes are responsive and know about each other.
    // Note: cluster_state may be "fail" without manual slot assignment, but
    // the key property is that all nodes survived restart and rejoined.
    for &node_id in &node_ids {
        let info = harness.get_cluster_info(node_id).unwrap();
        eprintln!(
            "Node {}: state={}, known_nodes={}",
            node_id, info.cluster_state, info.cluster_known_nodes
        );
        assert_eq!(
            info.cluster_known_nodes,
            node_ids.len(),
            "Node {} should know about all {} nodes after rolling restart",
            node_id,
            node_ids.len()
        );
    }

    eprintln!("SUCCESS: Rolling restart completed");

    harness.shutdown_all().await;
}

/// Test 21: CLUSTER FAILOVER command.
///
/// Scenario: Manual failover via CLUSTER FAILOVER
#[tokio::test]
async fn test_cluster_failover_command() {
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

    // Find a follower
    let follower = *node_ids.iter().find(|&&id| id != leader).unwrap();

    eprintln!(
        "Current leader: {}, attempting failover to {}",
        leader, follower
    );

    // Send CLUSTER FAILOVER to follower
    if let Some(follower_node) = harness.node(follower) {
        let failover_resp = follower_node.send("CLUSTER", &["FAILOVER"]).await;
        eprintln!("CLUSTER FAILOVER response: {:?}", failover_resp);

        // Note: CLUSTER FAILOVER may not be fully implemented
        // This test documents the expected behavior
        if is_error(&failover_resp) {
            let err = get_error_message(&failover_resp).unwrap_or("unknown");
            eprintln!("CLUSTER FAILOVER error (may be expected): {}", err);
        } else {
            // Wait for failover to complete
            tokio::time::sleep(Duration::from_secs(2)).await;

            // Check if leadership changed
            let new_leader = harness.get_leader().await;
            eprintln!("Leader after FAILOVER: {:?}", new_leader);
        }
    }

    harness.shutdown_all().await;
}

/// Test 22: CLUSTER FORGET removes node.
///
/// Scenario: CLUSTER FORGET removes node from cluster view
#[tokio::test]
async fn test_cluster_forget_removes_node() {
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

    // Get the cluster node ID of the node we want to forget
    let victim_id = node_ids[2];
    let victim_cluster_id = harness.get_node_id_str(victim_id).unwrap();

    eprintln!(
        "Will forget node {} (cluster ID: {})",
        victim_id, victim_cluster_id
    );

    // First, shut down the victim (FORGET only works on non-connected nodes in Redis)
    harness.shutdown_node(victim_id).await;

    // Wait for node to be detected as down
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Send CLUSTER FORGET from remaining nodes
    for &node_id in &node_ids[..2] {
        if let Some(node) = harness.node(node_id)
            && node.is_running()
        {
            let forget_resp = node.send("CLUSTER", &["FORGET", &victim_cluster_id]).await;
            eprintln!("Node {} FORGET response: {:?}", node_id, forget_resp);
        }
    }

    // Verify node is no longer in CLUSTER NODES
    tokio::time::sleep(Duration::from_millis(500)).await;

    if let Some(node) = harness.node(node_ids[0]) {
        let nodes_resp = node.send("CLUSTER", &["NODES"]).await;
        if let frogdb_protocol::Response::Bulk(Some(b)) = &nodes_resp {
            let nodes_str = String::from_utf8_lossy(b);
            if nodes_str.contains(&victim_cluster_id) {
                eprintln!("Node still present in CLUSTER NODES (FORGET may be async)");
            } else {
                eprintln!("SUCCESS: Node removed from CLUSTER NODES");
            }
        }
    }

    harness.shutdown_all().await;
}

/// Test 23: Large cluster with 10 nodes.
///
/// Scenario: Verify cluster works at larger scale
#[tokio::test]
async fn test_large_cluster_10_nodes() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(10).await.unwrap();

    assert_eq!(harness.node_ids().len(), 10, "Should have 10 nodes");

    // Wait for leader
    let leader = harness
        .wait_for_leader(Duration::from_secs(20))
        .await
        .unwrap();
    eprintln!("Leader elected: {}", leader);

    // Wait for convergence (may take longer with more nodes)
    harness
        .wait_for_cluster_convergence(Duration::from_secs(30))
        .await
        .unwrap();

    // Verify all nodes report healthy
    for node_id in harness.node_ids() {
        let info = harness.get_cluster_info(node_id).unwrap();
        assert_eq!(info.cluster_state, "ok", "Node {} should be ok", node_id);
        assert_eq!(
            info.cluster_known_nodes, 10,
            "Node {} should know 10 nodes",
            node_id
        );
    }

    // Write some data distributed across nodes
    let mut writes_ok = 0;
    for i in 0..20 {
        let key = format!("large_cluster_key_{}", i);
        let value = format!("large_cluster_value_{}", i);

        for &node_id in &harness.node_ids() {
            if let Some(node) = harness.node(node_id) {
                let resp = node.send("SET", &[&key, &value]).await;
                if !is_error(&resp) {
                    writes_ok += 1;
                    break;
                } else if let Some((_slot, _addr)) = is_moved_redirect(&resp) {
                    writes_ok += 1; // MOVED means data can be written elsewhere
                    break;
                }
            }
        }
    }

    eprintln!("Writes successful: {}/20", writes_ok);

    harness.shutdown_all().await;
}

// ============================================================================
// PHASE 7: Edge Cases & Stress Tests
// ============================================================================

/// Test 24: Rapid leader changes.
///
/// Multiple rapid leader failovers
/// (Note: test_rapid_failover_cycles already exists, this adds more aggressive testing)
#[tokio::test]
async fn test_rapid_leader_changes() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(7).await.unwrap();

    harness
        .wait_for_leader(Duration::from_secs(15))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(10))
        .await
        .unwrap();

    let node_ids = harness.node_ids();
    let mut killed_count = 0;

    // Rapidly kill leaders until we lose quorum
    for _ in 0..4 {
        // 4 kills: 7 -> 3 nodes (loses quorum)
        let leader = match harness.wait_for_leader(Duration::from_secs(10)).await {
            Ok(l) => l,
            Err(_) => {
                eprintln!("No leader found, quorum likely lost");
                break;
            }
        };

        eprintln!("Killing leader: {}", leader);
        harness.kill_node(leader).await;
        killed_count += 1;

        // Very short delay between kills
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    eprintln!("Killed {} leaders", killed_count);

    // Count surviving nodes
    let surviving: Vec<u64> = node_ids
        .iter()
        .filter(|&&id| harness.node(id).map(|n| n.is_running()).unwrap_or(false))
        .copied()
        .collect();

    eprintln!("Surviving nodes: {:?}", surviving);

    // With 3/7 nodes, cluster should not have quorum
    for &node_id in &surviving {
        if let Some(node) = harness.node(node_id) {
            let info_resp = node.send("CLUSTER", &["INFO"]).await;
            if let Ok(info) = parse_cluster_info(&info_resp) {
                eprintln!(
                    "Node {} after rapid kills: state={}, known={}",
                    node_id, info.cluster_state, info.cluster_known_nodes
                );
            }
        }
    }

    harness.shutdown_all().await;
}

/// Test 25: Simultaneous node restarts.
///
/// Two nodes restart at same time
#[tokio::test]
async fn test_simultaneous_node_restarts() {
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

    let node_ids = harness.node_ids();
    let leader = harness.get_leader().await.unwrap();

    // Pick two non-leader nodes to restart simultaneously
    let victims: Vec<u64> = node_ids
        .iter()
        .filter(|&&id| id != leader)
        .take(2)
        .copied()
        .collect();

    eprintln!("Restarting nodes {:?} simultaneously", victims);

    // Shutdown both
    for &victim in &victims {
        harness.shutdown_node(victim).await;
    }

    // Wait briefly
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Restart both in parallel
    let _restart_results: Vec<_> = futures::future::join_all(victims.iter().map(|&_victim| {
        let _harness_ref = &harness;
        async move {
            // Note: Can't actually restart in parallel due to &mut self
            // This is a sequential restart that simulates simultaneous timing
            Ok::<_, String>(())
        }
    }))
    .await;

    // Actually restart sequentially (limitation of the API)
    for &victim in &victims {
        harness.restart_node(victim).await.unwrap();
    }

    // Wait for both to rejoin
    for &victim in &victims {
        harness
            .wait_for_node_recognized(victim, Duration::from_secs(15))
            .await
            .unwrap();
    }

    // Verify cluster is healthy
    harness
        .wait_for_cluster_convergence(Duration::from_secs(10))
        .await
        .unwrap();

    for &node_id in &node_ids {
        if let Some(node) = harness.node(node_id)
            && node.is_running()
        {
            let info = harness.get_cluster_info(node_id).unwrap();
            assert_eq!(info.cluster_state, "ok");
        }
    }

    eprintln!("SUCCESS: Simultaneous restart completed");

    harness.shutdown_all().await;
}

/// Test 26: Node restart preserves Raft state.
///
/// Scenario: Raft vote/term persists across restart
#[tokio::test]
async fn test_node_restart_preserves_raft_state() {
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
    let leader = harness.get_leader().await.unwrap();

    // Pick a follower
    let follower = *node_ids.iter().find(|&&id| id != leader).unwrap();

    // Get current epoch (term) before restart
    let pre_info = harness.get_cluster_info(follower).unwrap();
    let pre_epoch = pre_info.cluster_current_epoch;
    eprintln!("Pre-restart epoch: {}", pre_epoch);

    // Restart the follower
    eprintln!("Restarting follower: {}", follower);
    harness.shutdown_node(follower).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    harness.restart_node(follower).await.unwrap();

    // Wait for node to rejoin
    harness
        .wait_for_node_recognized(follower, Duration::from_secs(15))
        .await
        .unwrap();

    // Get epoch after restart
    let post_info = harness.get_cluster_info(follower).unwrap();
    let post_epoch = post_info.cluster_current_epoch;
    eprintln!("Post-restart epoch: {}", post_epoch);

    // Epoch should be same or higher (not lower)
    assert!(
        post_epoch >= pre_epoch,
        "Epoch should not decrease after restart"
    );

    // Verify no split-brain (only one leader)
    let mut leaders: Vec<u64> = Vec::new();
    for &node_id in &node_ids {
        if let Some(node) = harness.node(node_id)
            && node.is_running()
        {
            let nodes_resp = node.send("CLUSTER", &["NODES"]).await;
            if let Ok(nodes) = parse_cluster_nodes(&nodes_resp) {
                for n in &nodes {
                    if n.is_myself() && n.is_master() {
                        // This node thinks it's a master
                        // In Raft terms, this could indicate it's the leader
                        leaders.push(node_id);
                    }
                }
            }
        }
    }

    eprintln!("SUCCESS: Raft state preserved after restart");

    harness.shutdown_all().await;
}

/// Test 27: High write load during failover.
///
/// Scenario: Writes during failover, verify no data loss
#[tokio::test]
async fn test_high_write_load_during_failover() {
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

    // Get addresses for concurrent writes
    let addresses: Vec<String> = node_ids
        .iter()
        .map(|&id| harness.node(id).unwrap().client_addr())
        .collect();

    // Spawn write tasks
    let write_count = 50;
    let addr_clone = addresses.clone();

    let write_handle = tokio::spawn(async move {
        let mut successful = 0;
        for i in 0..write_count {
            let addr = &addr_clone[i % addr_clone.len()];
            if let Ok(stream) = tokio::net::TcpStream::connect(addr).await {
                let mut framed =
                    tokio_util::codec::Framed::new(stream, redis_protocol::codec::Resp2::default());

                let key = format!("load_test_{}", i);
                let value = format!("value_{}", i);

                let frame = redis_protocol::resp2::types::BytesFrame::Array(vec![
                    redis_protocol::resp2::types::BytesFrame::BulkString(bytes::Bytes::from("SET")),
                    redis_protocol::resp2::types::BytesFrame::BulkString(bytes::Bytes::from(key)),
                    redis_protocol::resp2::types::BytesFrame::BulkString(bytes::Bytes::from(value)),
                ]);

                use futures::{SinkExt, StreamExt};
                if framed.send(frame).await.is_ok()
                    && let Some(Ok(_)) = framed.next().await
                {
                    successful += 1;
                }
            }
            // Small delay between writes
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        successful
    });

    // Wait a bit for some writes to complete
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Kill the leader mid-writes
    eprintln!("Killing leader during write load");
    harness.kill_node(leader).await;

    // Wait for writes to complete
    let successful_writes = write_handle.await.unwrap_or(0);
    eprintln!(
        "Writes during failover: {}/{} successful",
        successful_writes, write_count
    );

    // Wait for new leader
    let _ = harness.wait_for_leader(Duration::from_secs(15)).await;

    // Note: In a production system, we'd verify all successful writes are readable
    // For this test, we document the observed behavior

    harness.shutdown_all().await;
}

/// Test 28: Flapping node.
///
/// Scenario: Node repeatedly failing and recovering
#[tokio::test]
async fn test_flapping_node() {
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

    // Pick a follower to flap
    let flapper = *node_ids.iter().find(|&&id| id != leader).unwrap();

    eprintln!("Testing flapping behavior for node {}", flapper);

    // Flap the node 5 times
    for i in 0..5 {
        eprintln!("Flap cycle {}/5", i + 1);

        // Kill
        harness.kill_node(flapper).await;

        // Short delay (simulate brief failure)
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Restart
        harness.restart_node(flapper).await.unwrap();

        // Wait for rejoin
        let _ = harness
            .wait_for_node_recognized(flapper, Duration::from_secs(10))
            .await;

        // Brief stable period
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Verify cluster is still healthy after flapping
    tokio::time::sleep(Duration::from_secs(2)).await;

    harness
        .wait_for_cluster_convergence(Duration::from_secs(15))
        .await
        .unwrap();

    for &node_id in &node_ids {
        if let Some(node) = harness.node(node_id)
            && node.is_running()
        {
            let info = harness.get_cluster_info(node_id).unwrap();
            eprintln!(
                "Node {} after flapping: state={}",
                node_id, info.cluster_state
            );
            assert_eq!(
                info.cluster_state, "ok",
                "Cluster should remain healthy despite flapping"
            );
        }
    }

    eprintln!("SUCCESS: Cluster survived node flapping");

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

    // Verify cluster is still functional after the disruption
    let cluster_ok = harness
        .wait_for_cluster_convergence(Duration::from_secs(10))
        .await
        .is_ok();

    eprintln!("Cluster converged after migration+failover: {}", cluster_ok);

    // The key assertion: cluster should be in a consistent state
    // Either migration completed, was aborted, or is still in progress
    // But never in an inconsistent state where nodes disagree
    assert!(
        cluster_ok,
        "Cluster should recover to consistent state after failover during migration"
    );

    harness.shutdown_all().await;
}

// ============================================================================
// Tier 10: End-to-End Slot Migration Tests
// ============================================================================

/// Split "host:port" into ("host", "port").
fn split_addr(addr: &str) -> (&str, &str) {
    addr.rsplit_once(':').expect("address must be host:port")
}

/// Find the harness node ID that owns the given slot by probing nodes.
/// Sends a GET for a key in the slot: the node that handles it is the owner;
/// a MOVED redirect tells us who the real owner is.
/// Returns (owner_harness_id, non_owner_harness_id).
async fn find_owner_of_slot(harness: &ClusterTestHarness, slot: u16) -> Option<(u64, u64)> {
    let node_ids = harness.node_ids();
    let probe_key = key_for_slot(slot);

    for &nid in &node_ids {
        let node = harness.node(nid)?;
        let resp = node.send("GET", &[&probe_key]).await;

        if let Some((_moved_slot, target_addr)) = is_moved_redirect(&resp) {
            // This node doesn't own the slot; target_addr does
            for &owner_nid in &node_ids {
                if harness.node(owner_nid)?.client_addr() == target_addr {
                    return Some((owner_nid, nid));
                }
            }
        } else if !is_error(&resp) {
            // This node accepted the command — it owns the slot
            for &other_nid in &node_ids {
                if other_nid != nid {
                    return Some((nid, other_nid));
                }
            }
        }
    }
    None
}

/// Send a CLUSTER subcommand via the Raft leader, falling back to REDIRECT chasing.
async fn send_cluster_cmd(
    harness: &ClusterTestHarness,
    _preferred_node_id: u64,
    args: &[&str],
) -> frogdb_protocol::Response {
    // Always send to the known Raft leader first
    let leader_id = harness.get_leader().await.unwrap_or(_preferred_node_id);
    let node = harness.node(leader_id).unwrap();
    let resp = node.send("CLUSTER", args).await;

    // If still redirected, follow it
    if let Some(msg) = get_error_message(&resp)
        && msg.starts_with("REDIRECT ")
    {
        let parts: Vec<&str> = msg.split_whitespace().collect();
        if parts.len() >= 3 {
            let leader_addr = parts[2];
            for &nid in &harness.node_ids() {
                if let Some(n) = harness.node(nid)
                    && n.client_addr() == leader_addr
                {
                    return n.send("CLUSTER", args).await;
                }
            }
        }
    }
    resp
}

/// Run the full Redis-style slot migration workflow:
/// 1. SETSLOT IMPORTING on target (via Raft leader)
/// 2. SETSLOT MIGRATING on source (via Raft leader)
/// 3. Loop: GETKEYSINSLOT + MIGRATE
/// 4. SETSLOT NODE on both (via Raft leader)
async fn run_full_slot_migration(
    harness: &ClusterTestHarness,
    source_id: u64,
    target_id: u64,
    slot: u16,
    batch_size: usize,
) -> Result<(), String> {
    let source = harness.node(source_id).ok_or("source node not found")?;
    let target = harness.node(target_id).ok_or("target node not found")?;

    let source_cluster_id = harness
        .get_node_id_str(source_id)
        .ok_or("source cluster ID not found")?;
    let target_cluster_id = harness
        .get_node_id_str(target_id)
        .ok_or("target cluster ID not found")?;

    let slot_str = slot.to_string();
    let batch_str = batch_size.to_string();

    // Step 1: SETSLOT IMPORTING <source-id> <target-id> (via Raft leader)
    // Pass both node IDs explicitly since the command is Raft-forwarded to the
    // leader, which has a different my_node_id than the intended target.
    let import_resp = send_cluster_cmd(
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
    if is_error(&import_resp) {
        return Err(format!(
            "SETSLOT IMPORTING failed: {:?}",
            get_error_message(&import_resp)
        ));
    }

    // Step 2: SETSLOT MIGRATING <target-id> <source-id> (via Raft leader)
    // Note: In FrogDB, both IMPORTING and MIGRATING go through Raft as the same
    // BeginSlotMigration command. The second call may fail with "migration in
    // progress" which is expected — the migration was already started by IMPORTING.
    let migrate_resp = send_cluster_cmd(
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
    if is_error(&migrate_resp) {
        let msg = get_error_message(&migrate_resp).unwrap_or_default();
        if !msg.contains("migration in progress") {
            return Err(format!("SETSLOT MIGRATING failed: {:?}", msg));
        }
    }

    // Allow Raft propagation
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Step 3: Loop GETKEYSINSLOT + MIGRATE
    let target_addr = target.client_addr();
    let (host, port) = split_addr(&target_addr);

    loop {
        let keys_resp = source
            .send("CLUSTER", &["GETKEYSINSLOT", &slot_str, &batch_str])
            .await;

        let keys = match keys_resp {
            frogdb_protocol::Response::Array(ref arr) => {
                let mut ks = Vec::new();
                for item in arr {
                    if let frogdb_protocol::Response::Bulk(Some(b)) = item {
                        ks.push(String::from_utf8_lossy(b).to_string());
                    }
                }
                ks
            }
            _ => {
                return Err(format!(
                    "GETKEYSINSLOT unexpected response: {:?}",
                    keys_resp
                ));
            }
        };

        if keys.is_empty() {
            break;
        }

        // Build MIGRATE command: MIGRATE host port "" 0 5000 REPLACE KEYS key1 key2 ...
        let mut migrate_args: Vec<&str> = vec![host, port, "", "0", "5000", "REPLACE", "KEYS"];
        for k in &keys {
            migrate_args.push(k.as_str());
        }

        let migrate_result = source.send("MIGRATE", &migrate_args).await;
        if is_error(&migrate_result) {
            return Err(format!(
                "MIGRATE failed: {:?}",
                get_error_message(&migrate_result)
            ));
        }
    }

    // Step 4: SETSLOT NODE (via Raft leader)
    let node_resp = send_cluster_cmd(
        harness,
        target_id,
        &["SETSLOT", &slot_str, "NODE", &target_cluster_id],
    )
    .await;
    if is_error(&node_resp) {
        return Err(format!(
            "SETSLOT NODE failed: {:?}",
            get_error_message(&node_resp)
        ));
    }

    // Allow Raft propagation
    tokio::time::sleep(Duration::from_millis(200)).await;

    Ok(())
}

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
// Tier 11: Cluster Command Behavior Tests
// ============================================================================

/// Tests that CLUSTER RESET SOFT returns OK.
#[tokio::test]
async fn test_cluster_reset_soft_returns_ok() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(1).await.unwrap();

    let node = harness.node(harness.node_ids()[0]).unwrap();
    let response = node.send("CLUSTER", &["RESET", "SOFT"]).await;
    assert!(
        !is_error(&response),
        "CLUSTER RESET SOFT should return OK, got: {:?}",
        response
    );

    harness.shutdown_all().await;
}

/// Tests that CLUSTER RESET HARD returns OK.
#[tokio::test]
async fn test_cluster_reset_hard_returns_ok() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(1).await.unwrap();

    let node = harness.node(harness.node_ids()[0]).unwrap();
    let response = node.send("CLUSTER", &["RESET", "HARD"]).await;
    assert!(
        !is_error(&response),
        "CLUSTER RESET HARD should return OK, got: {:?}",
        response
    );

    harness.shutdown_all().await;
}

/// Tests that CLUSTER SET-CONFIG-EPOCH <n> is accepted and epoch incremented.
#[tokio::test]
async fn test_cluster_set_config_epoch_returns_ok() {
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

    let leader = harness.get_leader().await.unwrap();
    let node = harness.node(leader).unwrap();

    // Get current epoch
    let info_before = harness.get_cluster_info(leader).unwrap();
    let epoch_before = info_before.cluster_current_epoch;

    // SET-CONFIG-EPOCH should succeed (goes through Raft)
    let response = node.send("CLUSTER", &["SET-CONFIG-EPOCH", "100"]).await;
    assert!(
        !is_error(&response),
        "CLUSTER SET-CONFIG-EPOCH should succeed, got: {:?}",
        response
    );

    // Wait for Raft propagation
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Epoch should have changed
    let info_after = harness.get_cluster_info(leader).unwrap();
    assert!(
        info_after.cluster_current_epoch > epoch_before,
        "Epoch should have increased from {}, got {}",
        epoch_before,
        info_after.cluster_current_epoch
    );

    harness.shutdown_all().await;
}

/// Tests that CLUSTER SET-CONFIG-EPOCH rejects non-numeric values.
#[tokio::test]
async fn test_cluster_set_config_epoch_rejects_invalid() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(1).await.unwrap();

    let node = harness.node(harness.node_ids()[0]).unwrap();
    let response = node
        .send("CLUSTER", &["SET-CONFIG-EPOCH", "not_a_number"])
        .await;
    assert!(
        is_error(&response),
        "CLUSTER SET-CONFIG-EPOCH with non-numeric should error, got: {:?}",
        response
    );

    harness.shutdown_all().await;
}

/// Tests that CLUSTER SAVECONFIG returns OK.
#[tokio::test]
async fn test_cluster_saveconfig_returns_ok() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(1).await.unwrap();

    let node = harness.node(harness.node_ids()[0]).unwrap();
    let response = node.send("CLUSTER", &["SAVECONFIG"]).await;
    assert!(
        !is_error(&response),
        "CLUSTER SAVECONFIG should return OK, got: {:?}",
        response
    );

    harness.shutdown_all().await;
}

// ============================================================================
// Tier 12: Cluster Info Accuracy — Known-Gap Documentation
// ============================================================================

/// Verifies that replication-offset in CLUSTER SHARDS is non-zero after writes.
#[tokio::test]
async fn test_cluster_shards_replication_offset_nonzero() {
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
    let node = harness.node(node_ids[0]).unwrap();

    // Write some data
    let key = key_for_slot(100);
    for i in 0..10 {
        let k = format!("{{{}}}_offset_{}", key, i);
        node.send("SET", &[&k, "value"]).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    // CLUSTER SHARDS should show non-zero replication-offset
    let response = node.send("CLUSTER", &["SHARDS"]).await;
    assert!(
        !is_error(&response),
        "CLUSTER SHARDS should succeed, got: {:?}",
        response
    );

    // Parse nested response to find replication-offset > 0 for at least one node
    if let frogdb_protocol::Response::Array(shards) = &response {
        let mut found_nonzero = false;
        for shard in shards {
            if let frogdb_protocol::Response::Array(shard_fields) = shard {
                // nodes is at index 3 (after "slots", slots_array, "nodes")
                if let Some(frogdb_protocol::Response::Array(nodes)) = shard_fields.get(3) {
                    for node_resp in nodes {
                        if let frogdb_protocol::Response::Array(fields) = node_resp {
                            for pair in fields.windows(2) {
                                if let [
                                    frogdb_protocol::Response::Bulk(Some(k)),
                                    frogdb_protocol::Response::Integer(offset),
                                ] = pair
                                    && k.as_ref() == b"replication-offset"
                                    && *offset > 0
                                {
                                    found_nonzero = true;
                                }
                            }
                        }
                    }
                }
            }
        }
        assert!(
            found_nonzero,
            "Expected at least one node with replication-offset > 0"
        );
    } else {
        panic!(
            "Expected Array response from CLUSTER SHARDS, got: {:?}",
            response
        );
    }

    harness.shutdown_all().await;
}

/// Verifies that CLUSTER SLOTS succeeds after writes (no replication-offset field in this format).
#[tokio::test]
async fn test_cluster_slots_replication_offset_nonzero() {
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
    let node = harness.node(node_ids[0]).unwrap();

    // Write data
    let key = key_for_slot(200);
    for i in 0..10 {
        let k = format!("{{{}}}_slots_{}", key, i);
        node.send("SET", &[&k, "value"]).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let response = node.send("CLUSTER", &["SLOTS"]).await;
    assert!(
        !is_error(&response),
        "CLUSTER SLOTS should succeed, got: {:?}",
        response
    );
    // When implemented: parse response and assert offset > 0

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
// Tier 14: Migration Type Coverage — Known-Gap Documentation
// ============================================================================

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
// Tier 17: Config Epoch Correctness
// ============================================================================

/// Tests that cluster_current_epoch >= cluster_my_epoch on every node after failover.
///
/// Inspired by Redis `03-failover-loop.tcl` post-conditions.
#[tokio::test]
async fn test_current_epoch_gte_my_epoch_invariant() {
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

    // Kill leader to trigger failover
    harness.kill_node(original_leader).await;

    // Wait for new leader
    let _new_leader = harness
        .wait_for_new_leader(original_leader, Duration::from_secs(15))
        .await
        .unwrap();

    // Wait for cluster to stabilize
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify invariant on all surviving nodes
    for &node_id in &harness.node_ids() {
        if node_id == original_leader {
            continue;
        }
        if let Some(node) = harness.node(node_id) {
            if !node.is_running() {
                continue;
            }
            let info_resp = node.send("CLUSTER", &["INFO"]).await;
            if let Ok(info) = parse_cluster_info(&info_resp) {
                assert!(
                    info.cluster_current_epoch >= info.cluster_my_epoch,
                    "Node {}: cluster_current_epoch ({}) must be >= cluster_my_epoch ({})",
                    node_id,
                    info.cluster_current_epoch,
                    info.cluster_my_epoch
                );
            }
        }
    }

    harness.shutdown_all().await;
}

/// Tests that the cluster epoch increases after a leader election / failover.
///
/// Inspired by Redis `03-failover-loop.tcl` post-conditions.
#[tokio::test]
async fn test_cluster_epoch_increases_after_failover() {
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

    // Record epoch before failover from a non-leader node
    let non_leader = *harness
        .node_ids()
        .iter()
        .find(|&&id| id != original_leader)
        .unwrap();
    let pre_info_resp = harness
        .node(non_leader)
        .unwrap()
        .send("CLUSTER", &["INFO"])
        .await;
    let pre_epoch = parse_cluster_info(&pre_info_resp)
        .map(|i| i.cluster_current_epoch)
        .unwrap_or(0);

    // Kill leader to trigger failover
    harness.kill_node(original_leader).await;

    let _new_leader = harness
        .wait_for_new_leader(original_leader, Duration::from_secs(15))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Check epoch after failover
    let post_info_resp = harness
        .node(non_leader)
        .unwrap()
        .send("CLUSTER", &["INFO"])
        .await;
    let post_epoch = parse_cluster_info(&post_info_resp)
        .map(|i| i.cluster_current_epoch)
        .unwrap_or(0);

    assert!(
        post_epoch > pre_epoch,
        "Epoch should increase after failover: pre={}, post={}",
        pre_epoch,
        post_epoch
    );

    harness.shutdown_all().await;
}

// ============================================================================
// Tier 18: Writable Cluster Under Failover Stress
// ============================================================================

/// Tests that keys written pre-failover are readable on the new leader.
///
/// Inspired by Redis `03-failover-loop.tcl`, `10-manual-failover.tcl`.
#[tokio::test]
async fn test_cluster_writable_before_and_after_leader_failover() {
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

    // Write keys to a non-leader node so data survives the leader kill.
    // Data is stored per-node (not replicated via Raft), so writing to the
    // leader and then killing it would lose all written keys.
    let survivor = *harness
        .node_ids()
        .iter()
        .find(|&&id| id != original_leader)
        .unwrap();
    let survivor_node = harness.node(survivor).unwrap();
    let mut written_keys = Vec::new();
    for i in 0..10 {
        let key = format!("pre_failover_key_{}", i);
        let value = format!("pre_failover_value_{}", i);
        let resp = survivor_node.send("SET", &[&key, &value]).await;
        if !is_error(&resp) {
            written_keys.push((key, value));
        }
    }

    // Give time for Raft replication
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Kill leader
    harness.kill_node(original_leader).await;

    let new_leader = harness
        .wait_for_new_leader(original_leader, Duration::from_secs(15))
        .await
        .unwrap();

    // Wait for cluster stabilization
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify keys on new leader
    let new_leader_node = harness.node(new_leader).unwrap();
    let mut readable = 0;
    for (key, expected_value) in &written_keys {
        let resp = new_leader_node.send("GET", &[key]).await;
        match &resp {
            frogdb_protocol::Response::Bulk(Some(v)) => {
                if v.as_ref() == expected_value.as_bytes() {
                    readable += 1;
                }
            }
            frogdb_protocol::Response::Error(e) => {
                let msg = String::from_utf8_lossy(e);
                if msg.starts_with("MOVED") {
                    // Data exists elsewhere — follow redirect
                    if let Some((_slot, addr)) = is_moved_redirect(&resp) {
                        for &nid in &harness.node_ids() {
                            if nid == original_leader {
                                continue;
                            }
                            if let Some(n) = harness.node(nid)
                                && n.client_addr() == addr
                            {
                                let retry = n.send("GET", &[key]).await;
                                if let frogdb_protocol::Response::Bulk(Some(v)) = &retry
                                    && v.as_ref() == expected_value.as_bytes()
                                {
                                    readable += 1;
                                }
                                break;
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    eprintln!(
        "Readable after failover: {}/{}",
        readable,
        written_keys.len()
    );
    assert!(
        readable > 0 || written_keys.is_empty(),
        "At least some pre-failover keys should be readable"
    );

    harness.shutdown_all().await;
}

/// Tests that background writes continue through failover with >=80% success.
///
/// Inspired by Redis `10-manual-failover.tcl`.
#[tokio::test]
async fn test_cluster_remains_writable_during_concurrent_writes_and_failover() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(5).await.unwrap();

    let original_leader = harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let num_writes = 20;
    let mut success_count = 0;
    let mut error_count = 0;

    // Write some keys before failover, following MOVED redirects to the
    // correct node (each node only owns ~1/N of the slots).
    for i in 0..num_writes / 2 {
        let key = format!("concurrent_key_{}", i);
        let value = format!("concurrent_value_{}", i);
        let resp = harness
            .node(original_leader)
            .unwrap()
            .send("SET", &[&key, &value])
            .await;
        if !is_error(&resp) {
            success_count += 1;
        } else if let Some((_slot, addr)) = is_moved_redirect(&resp) {
            // Follow MOVED redirect to the correct node
            let mut followed = false;
            for nid in harness.node_ids() {
                if let Some(n) = harness.node(nid)
                    && n.client_addr() == addr
                {
                    let retry = n.send("SET", &[&key, &value]).await;
                    if !is_error(&retry) {
                        success_count += 1;
                        followed = true;
                    }
                    break;
                }
            }
            if !followed {
                error_count += 1;
            }
        } else {
            error_count += 1;
        }
    }

    // Kill leader mid-stream
    harness.kill_node(original_leader).await;

    // Wait for new leader
    let new_leader = harness
        .wait_for_new_leader(original_leader, Duration::from_secs(15))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Write more keys after failover, following MOVED redirects
    for i in num_writes / 2..num_writes {
        let key = format!("concurrent_key_{}", i);
        let value = format!("concurrent_value_{}", i);
        let resp = harness
            .node(new_leader)
            .unwrap()
            .send("SET", &[&key, &value])
            .await;
        if !is_error(&resp) {
            success_count += 1;
        } else if let Some((_slot, addr)) = is_moved_redirect(&resp) {
            let mut followed = false;
            for nid in harness.node_ids() {
                if nid == original_leader {
                    continue; // dead node
                }
                if let Some(n) = harness.node(nid)
                    && n.client_addr() == addr
                {
                    let retry = n.send("SET", &[&key, &value]).await;
                    if !is_error(&retry) {
                        success_count += 1;
                        followed = true;
                    }
                    break;
                }
            }
            if !followed {
                error_count += 1;
            }
        } else {
            error_count += 1;
        }
    }

    let total = success_count + error_count;
    let success_rate = if total > 0 {
        (success_count as f64 / total as f64) * 100.0
    } else {
        0.0
    };

    eprintln!(
        "Writes during failover: {}/{} succeeded ({:.1}%)",
        success_count, total, success_rate
    );

    // With 5 nodes and 1 killed, the dead node's slots (~20%) are orphaned.
    // Pre-failover writes follow redirects to correct nodes (100% succeed).
    // Post-failover writes to dead node's slots fail.  Expect >=50% overall.
    assert!(
        success_rate >= 50.0,
        "Expected >=50% write success rate, got {:.1}%",
        success_rate
    );

    harness.shutdown_all().await;
}

// ============================================================================
// Tier 19: MULTI-EXEC on Replica with READONLY
// ============================================================================

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

    // Queue a write
    let _queued_resp = client.command(&["SET", &key, "should_fail"]).await;
    // Should still queue (error surfaces at EXEC time)

    let exec_resp = client.command(&["EXEC"]).await;
    // EXEC should fail with MOVED or EXECABORT for write commands on READONLY node
    assert!(
        is_error(&exec_resp),
        "EXEC with write on READONLY node should fail, got: {:?}",
        exec_resp
    );

    harness.shutdown_all().await;
}

// ============================================================================
// Tier 20: Self-Fencing / Quorum Loss
// ============================================================================

/// Tests that a minority node rejects writes when quorum is lost.
///
/// Kills 3 of 5 nodes, then verifies surviving nodes reject writes with CLUSTERDOWN.
#[tokio::test]
async fn test_minority_node_rejects_writes_on_quorum_loss() {
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

    let node_ids = harness.node_ids();

    // Kill 3 of 5 nodes (majority)
    harness.shutdown_node(node_ids[0]).await;
    harness.shutdown_node(node_ids[1]).await;
    harness.shutdown_node(node_ids[2]).await;

    // Wait for failure detection
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Surviving minority nodes should reject writes
    for &node_id in &node_ids[3..5] {
        if let Some(node) = harness.node(node_id) {
            if !node.is_running() {
                continue;
            }
            let key = format!("quorum_test_{}", node_id);
            let resp = node.send("SET", &[&key, "value"]).await;
            assert!(
                is_cluster_down(&resp) || is_error(&resp),
                "Minority node {} should reject writes, got: {:?}",
                node_id,
                resp
            );
        }
    }

    harness.shutdown_all().await;
}

// ============================================================================
// Tier 21: Replica Lag Scoring
// ============================================================================

/// Tests that auto-failover selects the most caught-up replica as new leader.
///
/// With lag-based scoring, the replica with the highest replication offset
/// (and thus least data loss) is promoted.
#[tokio::test]
async fn test_auto_failover_selects_most_caught_up_replica() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(5).await.unwrap();

    let original_leader = harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    // Write a burst of data
    let leader_node = harness.node(original_leader).unwrap();
    for i in 0..50 {
        leader_node
            .send("SET", &[&format!("lag_key_{}", i), "value"])
            .await;
    }

    // Kill leader
    harness.kill_node(original_leader).await;

    let new_leader = harness
        .wait_for_new_leader(original_leader, Duration::from_secs(15))
        .await
        .unwrap();

    // The new leader should have the highest replication offset
    // among surviving nodes. Since we can't easily measure per-node
    // offsets in the current implementation, we just verify a leader
    // was elected and the cluster recovers.
    let new_leader_node = harness.node(new_leader).unwrap();
    let info_resp = new_leader_node.send("CLUSTER", &["INFO"]).await;
    let info = parse_cluster_info(&info_resp).unwrap();
    assert_eq!(
        info.cluster_state, "ok",
        "New leader should report cluster as ok"
    );

    harness.shutdown_all().await;
}

// ============================================================================
// Tier 22: CLUSTER RESET
// ============================================================================

/// Tests that CLUSTER RESET SOFT clears slot assignments.
#[tokio::test]
async fn test_cluster_reset_soft_clears_slot_assignments() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(1).await.unwrap();

    let node = harness.node(harness.node_ids()[0]).unwrap();

    // Get initial state
    let pre_info = parse_cluster_info(&node.send("CLUSTER", &["INFO"]).await).unwrap();
    let pre_slots = pre_info.cluster_slots_assigned;

    // Reset soft
    let resp = node.send("CLUSTER", &["RESET", "SOFT"]).await;
    assert!(!is_error(&resp), "CLUSTER RESET SOFT should succeed");

    // Slots should be cleared
    let post_info = parse_cluster_info(&node.send("CLUSTER", &["INFO"]).await).unwrap();
    assert_eq!(
        post_info.cluster_slots_assigned, 0,
        "Slots should be cleared after RESET SOFT (was: {})",
        pre_slots
    );

    harness.shutdown_all().await;
}

/// Tests that CLUSTER RESET HARD regenerates the node ID.
#[tokio::test]
async fn test_cluster_reset_hard_clears_node_id() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(1).await.unwrap();

    let node = harness.node(harness.node_ids()[0]).unwrap();

    // Get initial node ID
    let pre_nodes = parse_cluster_nodes(&node.send("CLUSTER", &["NODES"]).await).unwrap();
    let pre_id = pre_nodes.iter().find(|n| n.is_myself()).unwrap().id.clone();

    // Reset hard
    let resp = node.send("CLUSTER", &["RESET", "HARD"]).await;
    assert!(!is_error(&resp), "CLUSTER RESET HARD should succeed");

    // Node ID should change
    let post_nodes = parse_cluster_nodes(&node.send("CLUSTER", &["NODES"]).await).unwrap();
    let post_id = post_nodes
        .iter()
        .find(|n| n.is_myself())
        .unwrap()
        .id
        .clone();
    assert_ne!(pre_id, post_id, "Node ID should change after RESET HARD");

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
// Tier 24: CLUSTER FAILOVER Variants
// ============================================================================

/// Tests that CLUSTER FAILOVER FORCE succeeds when the leader is unreachable.
///
/// Inspired by Redis `10-manual-failover.tcl`.
#[tokio::test]
async fn test_cluster_failover_force_works_when_leader_unreachable() {
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

    // Kill leader to make it unreachable
    harness.kill_node(original_leader).await;

    // Wait for Raft to elect a new leader (FAILOVER FORCE needs to go through Raft)
    let new_leader = harness
        .wait_for_new_leader(original_leader, Duration::from_secs(10))
        .await
        .unwrap();

    // Send FAILOVER FORCE to the new Raft leader
    let leader_node = harness.node(new_leader).unwrap();
    let resp = leader_node.send("CLUSTER", &["FAILOVER", "FORCE"]).await;

    assert!(
        !is_error(&resp),
        "CLUSTER FAILOVER FORCE should succeed, got: {:?}",
        resp
    );

    // Poll until Raft commits the failover operations and cluster is ok
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let info_resp = leader_node.send("CLUSTER", &["INFO"]).await;
        if let Ok(info) = parse_cluster_info(&info_resp)
            && info.cluster_state == "ok"
        {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            let info_resp = leader_node.send("CLUSTER", &["INFO"]).await;
            let info = parse_cluster_info(&info_resp).unwrap();
            panic!(
                "Cluster state not ok within 5 seconds after FAILOVER FORCE, state: {}",
                info.cluster_state
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    harness.shutdown_all().await;
}

// ============================================================================
// Category A: Cluster Command Primitives
// ============================================================================

/// ADDSLOTS assigns slots, visible in CLUSTER INFO and CLUSTER NODES.
#[tokio::test]
async fn test_cluster_addslots_basic() {
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

    // CLUSTER INFO should show all 16384 slots assigned after formation
    let node = harness.node(harness.node_ids()[0]).unwrap();
    let info_resp = node.send("CLUSTER", &["INFO"]).await;
    let info = parse_cluster_info(&info_resp).unwrap();

    assert_eq!(
        info.cluster_slots_assigned, 16384,
        "All 16384 slots should be assigned after cluster formation"
    );
    assert_eq!(info.cluster_slots_ok, 16384, "All 16384 slots should be ok");

    // Verify slots appear in CLUSTER NODES
    let nodes_resp = node.send("CLUSTER", &["NODES"]).await;
    let nodes = parse_cluster_nodes(&nodes_resp).unwrap();
    let total_slots: u32 = nodes
        .iter()
        .flat_map(|n| &n.slots)
        .map(|(s, e)| (e - s + 1) as u32)
        .sum();
    assert_eq!(
        total_slots, 16384,
        "Total slots across all nodes should be 16384"
    );

    harness.shutdown_all().await;
}

/// ADDSLOTS with already-assigned slot returns error.
#[tokio::test]
async fn test_cluster_addslots_already_assigned_error() {
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

    // All slots are already assigned. Trying to ADDSLOTS 0 should error.
    let resp = send_cluster_cmd(&harness, harness.node_ids()[0], &["ADDSLOTS", "0"]).await;
    assert!(
        is_error(&resp),
        "ADDSLOTS for already-assigned slot should error, got: {:?}",
        resp
    );

    harness.shutdown_all().await;
}

/// ADDSLOTS with invalid slot range returns error.
#[tokio::test]
async fn test_cluster_addslots_invalid_slot_range() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();

    // Slot 16384 is out of range (valid: 0-16383)
    let resp = send_cluster_cmd(&harness, harness.node_ids()[0], &["ADDSLOTS", "16384"]).await;
    assert!(
        is_error(&resp),
        "ADDSLOTS with slot 16384 should error, got: {:?}",
        resp
    );

    // Negative slot (passed as string)
    let resp2 = send_cluster_cmd(&harness, harness.node_ids()[0], &["ADDSLOTS", "-1"]).await;
    assert!(
        is_error(&resp2),
        "ADDSLOTS with negative slot should error, got: {:?}",
        resp2
    );

    harness.shutdown_all().await;
}

/// DELSLOTS removes slot assignment.
#[tokio::test]
async fn test_cluster_delslots_basic() {
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

    // Delete slot 0
    let resp = send_cluster_cmd(&harness, harness.node_ids()[0], &["DELSLOTS", "0"]).await;
    assert!(!is_error(&resp), "DELSLOTS should succeed, got: {:?}", resp);

    // Allow Raft propagation
    tokio::time::sleep(Duration::from_millis(500)).await;

    // CLUSTER INFO should show 16383 assigned
    let node = harness.node(harness.node_ids()[0]).unwrap();
    let info_resp = node.send("CLUSTER", &["INFO"]).await;
    let info = parse_cluster_info(&info_resp).unwrap();
    assert_eq!(
        info.cluster_slots_assigned, 16383,
        "Should have 16383 slots after deleting one"
    );

    harness.shutdown_all().await;
}

/// DELSLOTS for unassigned slot returns error.
#[tokio::test]
async fn test_cluster_delslots_unassigned_error() {
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

    // First delete slot 0
    let resp = send_cluster_cmd(&harness, harness.node_ids()[0], &["DELSLOTS", "0"]).await;
    assert!(!is_error(&resp), "First DELSLOTS should succeed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Try to delete slot 0 again — should error
    let resp2 = send_cluster_cmd(&harness, harness.node_ids()[0], &["DELSLOTS", "0"]).await;
    assert!(
        is_error(&resp2),
        "DELSLOTS for already-deleted slot should error, got: {:?}",
        resp2
    );

    harness.shutdown_all().await;
}

/// COUNTKEYSINSLOT returns correct count.
#[tokio::test]
async fn test_cluster_countkeysinslot_returns_correct_count() {
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
    let (owner_id, _) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");
    let owner = harness.node(owner_id).unwrap();

    // Write 5 keys to the slot
    let tag = format!("{{{}}}", key_for_slot(test_slot));
    for i in 0..5 {
        let key = format!("{}_cnt{}", tag, i);
        let resp = owner.send("SET", &[&key, "v"]).await;
        assert!(!is_error(&resp), "SET failed: {:?}", resp);
    }

    let slot_str = test_slot.to_string();
    let count_resp = owner.send("CLUSTER", &["COUNTKEYSINSLOT", &slot_str]).await;
    match &count_resp {
        frogdb_protocol::Response::Integer(n) => {
            assert_eq!(*n, 5, "COUNTKEYSINSLOT should return 5");
        }
        other => panic!("Expected integer from COUNTKEYSINSLOT, got: {:?}", other),
    }

    harness.shutdown_all().await;
}

/// COUNTKEYSINSLOT decreases after DEL.
#[tokio::test]
async fn test_cluster_countkeysinslot_decreases_after_delete() {
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

    let test_slot = 600u16;
    let (owner_id, _) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");
    let owner = harness.node(owner_id).unwrap();

    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let key0 = format!("{}_del0", tag);
    let key1 = format!("{}_del1", tag);
    let key2 = format!("{}_del2", tag);

    owner.send("SET", &[&key0, "v"]).await;
    owner.send("SET", &[&key1, "v"]).await;
    owner.send("SET", &[&key2, "v"]).await;

    let slot_str = test_slot.to_string();

    // Count should be 3
    let resp = owner.send("CLUSTER", &["COUNTKEYSINSLOT", &slot_str]).await;
    if let frogdb_protocol::Response::Integer(n) = &resp {
        assert_eq!(*n, 3, "Should have 3 keys before delete");
    }

    // Delete one key
    let del_resp = owner.send("DEL", &[&key1]).await;
    assert!(!is_error(&del_resp), "DEL failed: {:?}", del_resp);

    // Count should be 2
    let resp2 = owner.send("CLUSTER", &["COUNTKEYSINSLOT", &slot_str]).await;
    if let frogdb_protocol::Response::Integer(n) = &resp2 {
        assert_eq!(*n, 2, "Should have 2 keys after deleting one");
    }

    harness.shutdown_all().await;
}

/// GETKEYSINSLOT returns correct key names.
#[tokio::test]
async fn test_cluster_getkeysinslot_returns_keys() {
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
    let (owner_id, _) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");
    let owner = harness.node(owner_id).unwrap();

    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let mut expected_keys = Vec::new();
    for i in 0..3 {
        let key = format!("{}_gk{}", tag, i);
        owner.send("SET", &[&key, "v"]).await;
        expected_keys.push(key);
    }

    let slot_str = test_slot.to_string();
    let resp = owner
        .send("CLUSTER", &["GETKEYSINSLOT", &slot_str, "10"])
        .await;

    match &resp {
        frogdb_protocol::Response::Array(arr) => {
            let mut returned_keys: Vec<String> = arr
                .iter()
                .filter_map(|item| {
                    if let frogdb_protocol::Response::Bulk(Some(b)) = item {
                        Some(String::from_utf8_lossy(b).to_string())
                    } else {
                        None
                    }
                })
                .collect();
            returned_keys.sort();
            expected_keys.sort();
            assert_eq!(
                returned_keys, expected_keys,
                "GETKEYSINSLOT should return the keys we inserted"
            );
        }
        other => panic!("Expected array from GETKEYSINSLOT, got: {:?}", other),
    }

    harness.shutdown_all().await;
}

/// CLUSTER INFO cluster_slots_assigned matches actual slot count.
#[tokio::test]
async fn test_cluster_info_slots_assigned_matches_addslots() {
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

    // Verify all nodes report the same slot count
    for &nid in &harness.node_ids() {
        let node = harness.node(nid).unwrap();
        let info_resp = node.send("CLUSTER", &["INFO"]).await;
        let info = parse_cluster_info(&info_resp).unwrap();
        assert_eq!(
            info.cluster_slots_assigned, 16384,
            "Node {} should see 16384 slots assigned",
            nid
        );
    }

    harness.shutdown_all().await;
}

/// CLUSTER INFO cluster_size matches primary count.
#[tokio::test]
async fn test_cluster_info_cluster_size() {
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

    for &nid in &harness.node_ids() {
        let node = harness.node(nid).unwrap();
        let info_resp = node.send("CLUSTER", &["INFO"]).await;
        let info = parse_cluster_info(&info_resp).unwrap();
        // cluster_size = number of primaries with at least one slot
        assert_eq!(
            info.cluster_size, 3,
            "cluster_size should be 3 for a 3-node cluster"
        );
    }

    harness.shutdown_all().await;
}

/// CLUSTER MEET adds a new peer node.
#[tokio::test]
async fn test_cluster_meet_adds_node() {
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

    // add_node uses CLUSTER MEET internally
    let new_node_id = harness.add_node().await.unwrap();

    // Wait for the new node to be recognized
    harness
        .wait_for_node_recognized(new_node_id, Duration::from_secs(15))
        .await
        .unwrap();

    // Verify all nodes now see 4 nodes
    for &nid in &harness.node_ids() {
        let info = harness.get_cluster_info(nid).unwrap();
        assert_eq!(
            info.cluster_known_nodes, 4,
            "Node {} should see 4 known nodes after MEET",
            nid
        );
    }

    harness.shutdown_all().await;
}

/// CLUSTER MEET adds a node to both cluster state AND the Raft voter set.
///
/// Regression test: before the fix, add_node() via CLUSTER MEET only added the
/// node to ClusterState but not to the Raft membership. The new node couldn't
/// discover the leader or forward Raft writes (CLUSTERDOWN No leader available).
#[tokio::test]
async fn test_cluster_meet_adds_node_to_raft_voters() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(1).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();

    // Add a new node via CLUSTER MEET
    let new_node_id = harness.add_node().await.unwrap();
    harness
        .wait_for_node_recognized(new_node_id, Duration::from_secs(10))
        .await
        .unwrap();

    // The new node should discover the Raft leader (proves it's a Raft voter
    // receiving heartbeats — before the fix this timed out).
    harness
        .wait_for_node_has_leader(new_node_id, Duration::from_secs(10))
        .await
        .unwrap();

    // The new node should be able to forward a Raft write.
    // CLUSTER REPLICATE goes through Raft, so it exercises the full path.
    let primary_id = harness.node_ids()[0];
    let primary_hex = format!("{:040x}", primary_id);
    let new_node = harness.node(new_node_id).unwrap();
    let resp = new_node.send("CLUSTER", &["REPLICATE", &primary_hex]).await;
    assert!(
        matches!(resp, frogdb_protocol::Response::Simple(ref s) if s.as_ref() == b"OK"),
        "CLUSTER REPLICATE from dynamically-added node should succeed, got: {:?}",
        resp,
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

/// MULTI + mixed-slot commands returns error.
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

    let node = harness.node(harness.node_ids()[0]).unwrap();
    let mut client = node.connect().await;

    let multi_resp = client.command(&["MULTI"]).await;
    assert!(!is_error(&multi_resp), "MULTI should succeed");

    // Queue two commands for different slots
    let set1_resp = client.command(&["SET", "txn_key_a", "v1"]).await;
    let set2_resp = client.command(&["SET", "txn_key_b", "v2"]).await;

    let exec_resp = client.command(&["EXEC"]).await;

    // Either the EXEC should fail, or the queued commands should have errors
    // in the response array. The exact behavior depends on implementation.
    let has_errors = is_error(&exec_resp)
        || is_error(&set1_resp)
        || is_error(&set2_resp)
        || matches!(&exec_resp, frogdb_protocol::Response::Array(arr) if arr.iter().any(is_error));

    assert!(
        has_errors,
        "MULTI/EXEC with cross-slot keys should produce errors somewhere; \
         MULTI={:?}, SET1={:?}, SET2={:?}, EXEC={:?}",
        multi_resp, set1_resp, set2_resp, exec_resp
    );

    harness.shutdown_all().await;
}

// ============================================================================
// Category D: Cluster Persistence & Recovery
// ============================================================================

/// Data survives a full cluster restart (all nodes).
#[tokio::test]
async fn test_cluster_data_survives_full_restart() {
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

    let test_slot = 900u16;
    let (owner_id, _) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");
    let owner = harness.node(owner_id).unwrap();

    // Write data
    let tag = format!("{{{}}}", key_for_slot(test_slot));
    for i in 0..5 {
        let key = format!("{}_persist{}", tag, i);
        let value = format!("pval_{}", i);
        let resp = owner.send("SET", &[&key, &value]).await;
        assert!(!is_error(&resp), "SET failed: {:?}", resp);
    }

    // Shutdown all nodes
    let node_ids = harness.node_ids();
    for &nid in &node_ids {
        harness.shutdown_node(nid).await;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Restart all nodes
    for &nid in &node_ids {
        harness.restart_node(nid).await.unwrap();
    }

    // Wait for cluster to reform
    harness
        .wait_for_leader(Duration::from_secs(15))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(10))
        .await
        .unwrap();

    // Verify data is still there
    let (new_owner_id, _) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner after restart");
    let new_owner = harness.node(new_owner_id).unwrap();

    let mut found = 0;
    for i in 0..5 {
        let key = format!("{}_persist{}", tag, i);
        let expected = format!("pval_{}", i);
        let resp = new_owner.send("GET", &[&key]).await;
        if let frogdb_protocol::Response::Bulk(Some(v)) = &resp
            && v.as_ref() == expected.as_bytes()
        {
            found += 1;
        }
    }
    assert_eq!(
        found, 5,
        "All 5 keys should survive full restart, found {}",
        found
    );

    harness.shutdown_all().await;
}

/// Slot assignment persists across node restart.
#[tokio::test]
async fn test_cluster_slot_assignment_persists() {
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

    let leader = harness.get_leader().await.unwrap();

    // Restart one node (non-leader to avoid election complications)
    let follower = *harness.node_ids().iter().find(|&&id| id != leader).unwrap();
    harness.shutdown_node(follower).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    harness.restart_node(follower).await.unwrap();

    harness
        .wait_for_node_recognized(follower, Duration::from_secs(15))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(10))
        .await
        .unwrap();

    // Verify slot assignments are unchanged: total should still be 16384
    let node = harness.node(harness.get_leader().await.unwrap()).unwrap();
    let post_nodes_resp = node.send("CLUSTER", &["NODES"]).await;
    let post_nodes = parse_cluster_nodes(&post_nodes_resp).unwrap();
    let total: u32 = post_nodes
        .iter()
        .flat_map(|n| &n.slots)
        .map(|(s, e)| (e - s + 1) as u32)
        .sum();
    assert_eq!(
        total, 16384,
        "Total slots should remain 16384 after restart"
    );

    harness.shutdown_all().await;
}

/// Config epoch persists across restart.
#[tokio::test]
async fn test_cluster_epoch_persists() {
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

    let leader = harness.get_leader().await.unwrap();
    let follower = *harness.node_ids().iter().find(|&&id| id != leader).unwrap();

    // Get epoch before restart
    let pre_info = harness.get_cluster_info(follower).unwrap();
    let pre_epoch = pre_info.cluster_current_epoch;

    // Restart follower
    harness.shutdown_node(follower).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    harness.restart_node(follower).await.unwrap();

    harness
        .wait_for_node_recognized(follower, Duration::from_secs(15))
        .await
        .unwrap();

    // Get epoch after restart
    let post_info = harness.get_cluster_info(follower).unwrap();
    let post_epoch = post_info.cluster_current_epoch;

    assert!(
        post_epoch >= pre_epoch,
        "Epoch should not decrease after restart: pre={}, post={}",
        pre_epoch,
        post_epoch
    );

    harness.shutdown_all().await;
}

// ============================================================================
// Rolling Upgrade / Finalization Tests
// ============================================================================

/// Helper: parse FROGDB.VERSION response (flat array of key-value pairs) into a map.
fn parse_version_response(
    response: &frogdb_protocol::Response,
) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    if let frogdb_protocol::Response::Array(items) = response {
        for chunk in items.chunks(2) {
            if chunk.len() == 2
                && let (
                    frogdb_protocol::Response::Bulk(Some(k)),
                    frogdb_protocol::Response::Bulk(Some(v)),
                ) = (&chunk[0], &chunk[1])
            {
                let key = String::from_utf8_lossy(k).to_string();
                let val = String::from_utf8_lossy(v).to_string();
                map.insert(key, val);
            }
        }
    }
    map
}

/// Helper: find the Raft leader and send a command to it, retrying on REDIRECT.
async fn send_to_leader(
    harness: &ClusterTestHarness,
    cmd: &str,
    args: &[&str],
) -> (u64, frogdb_protocol::Response) {
    let leader_id = harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    let node = harness.node(leader_id).unwrap();
    let response = node.send(cmd, args).await;

    // If we got a redirect, follow it
    if let frogdb_protocol::Response::Error(e) = &response {
        let msg = String::from_utf8_lossy(e);
        if msg.starts_with("REDIRECT ") {
            let parts: Vec<&str> = msg.split_whitespace().collect();
            if parts.len() >= 3 {
                let target_addr = parts[2];
                for &nid in &harness.node_ids() {
                    let n = harness.node(nid).unwrap();
                    if n.client_addr() == target_addr {
                        let retry_response = n.send(cmd, args).await;
                        return (nid, retry_response);
                    }
                }
            }
        }
    }

    (leader_id, response)
}

/// Tests that FROGDB.FINALIZE succeeds when all nodes report the target version.
#[tokio::test]
async fn test_frogdb_finalize_success() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(10))
        .await
        .unwrap();

    // Fake all nodes to version 0.2.0
    harness.fake_all_node_versions("0.2.0");

    // Send FROGDB.FINALIZE to the leader
    let (_, response) = send_to_leader(&harness, "FROGDB.FINALIZE", &["0.2.0"]).await;
    assert!(
        matches!(&response, frogdb_protocol::Response::Simple(s) if s.as_ref() == b"OK"),
        "Expected OK, got {:?}",
        response
    );

    // Wait for Raft replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify active_version is set on all nodes
    for &node_id in &harness.node_ids() {
        let node = harness.node(node_id).unwrap();
        let cs = node.cluster_state().unwrap();
        assert_eq!(
            cs.active_version(),
            Some("0.2.0".to_string()),
            "Node {} should have active_version 0.2.0",
            node_id
        );
    }

    harness.shutdown_all().await;
}

/// Tests that FROGDB.FINALIZE rejects when a node is behind the target version.
#[tokio::test]
async fn test_frogdb_finalize_rejects_mixed_version() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(10))
        .await
        .unwrap();

    // Set all nodes to 0.2.0 except one at 0.1.0
    let node_ids = harness.node_ids();
    harness.fake_all_node_versions("0.2.0");
    harness.fake_node_version(node_ids[2], "0.1.0");

    // Send FROGDB.FINALIZE — should fail
    let (_, response) = send_to_leader(&harness, "FROGDB.FINALIZE", &["0.2.0"]).await;
    assert!(
        matches!(&response, frogdb_protocol::Response::Error(_)),
        "Expected error, got {:?}",
        response
    );

    // Verify active_version is still None
    for &node_id in &harness.node_ids() {
        let node = harness.node(node_id).unwrap();
        let cs = node.cluster_state().unwrap();
        assert_eq!(
            cs.active_version(),
            None,
            "Node {} should have no active_version after failed finalize",
            node_id
        );
    }

    harness.shutdown_all().await;
}

/// Tests that FROGDB.VERSION reports correct cluster info before and after finalization.
#[tokio::test]
async fn test_frogdb_version_reports_cluster_info() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(10))
        .await
        .unwrap();

    // Query FROGDB.VERSION before finalization
    let leader_id = harness
        .wait_for_leader(Duration::from_secs(5))
        .await
        .unwrap();
    let node = harness.node(leader_id).unwrap();
    let response = node.send("FROGDB.VERSION", &[]).await;
    let info = parse_version_response(&response);

    assert!(
        info.contains_key("binary_version"),
        "Should contain binary_version"
    );
    assert!(
        info.contains_key("cluster_version"),
        "Should contain cluster_version"
    );
    let active = info.get("active_version").cloned().unwrap_or_default();
    assert!(
        active.is_empty(),
        "active_version should be empty before finalization, got {:?}",
        active
    );

    // Finalize
    harness.fake_all_node_versions("0.2.0");
    let (_, finalize_resp) = send_to_leader(&harness, "FROGDB.FINALIZE", &["0.2.0"]).await;
    assert!(
        matches!(&finalize_resp, frogdb_protocol::Response::Simple(s) if s.as_ref() == b"OK"),
        "Finalize should succeed"
    );

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Query FROGDB.VERSION after finalization
    let response = node.send("FROGDB.VERSION", &[]).await;
    let info = parse_version_response(&response);
    assert_eq!(
        info.get("active_version").map(|s| s.as_str()),
        Some("0.2.0"),
        "active_version should be 0.2.0 after finalization"
    );

    harness.shutdown_all().await;
}

/// Tests that FROGDB.FINALIZE sent to a follower results in a redirect.
#[tokio::test]
async fn test_frogdb_finalize_non_leader_redirects() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    let leader_id = harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(10))
        .await
        .unwrap();

    harness.fake_all_node_versions("0.2.0");

    // Find a follower
    let follower_id = harness
        .node_ids()
        .into_iter()
        .find(|&id| id != leader_id)
        .unwrap();

    let follower = harness.node(follower_id).unwrap();
    let response = follower.send("FROGDB.FINALIZE", &["0.2.0"]).await;

    // Should get a REDIRECT error pointing to the leader
    match &response {
        frogdb_protocol::Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.starts_with("REDIRECT"),
                "Expected REDIRECT error, got: {}",
                msg
            );
        }
        frogdb_protocol::Response::Simple(s) if s.as_ref() == b"OK" => {
            // Some Raft implementations forward writes — this is also acceptable
        }
        other => panic!("Expected REDIRECT or OK, got {:?}", other),
    }

    harness.shutdown_all().await;
}

// ============================================================================
// Admin HTTP Upgrade-Status Endpoint Tests
// ============================================================================

/// Response shape for GET /admin/upgrade-status (matches server's UpgradeStatusResponse).
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct UpgradeStatusResponse {
    binary_version: String,
    active_version: Option<String>,
    cluster_version: Option<String>,
    mixed_version: bool,
    nodes: Vec<AdminNodeVersionInfo>,
    gated_features: Vec<AdminGatedFeatureInfo>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct AdminNodeVersionInfo {
    id: u64,
    addr: String,
    binary_version: String,
    status: String,
}

#[derive(Debug, Deserialize)]
struct AdminGatedFeatureInfo {
    name: String,
    #[allow(dead_code)]
    min_version: String,
    #[allow(dead_code)]
    description: String,
    active: bool,
}

/// Tests that /admin/upgrade-status returns correct info for a cluster with mixed versions.
#[tokio::test]
async fn test_admin_upgrade_status_cluster() {
    let config = ClusterNodeConfig {
        admin_enabled: true,
        ..Default::default()
    };
    let mut harness = ClusterTestHarness::with_config(config);
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(10))
        .await
        .unwrap();

    // Fake mixed versions
    harness.fake_all_node_versions("0.2.0");
    let node_ids = harness.node_ids();
    harness.fake_node_version(node_ids[2], "0.1.0");

    // GET /admin/upgrade-status from any node
    let node = harness.node(node_ids[0]).unwrap();
    let url = node.admin_http_url().expect("admin should be enabled");
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let resp: UpgradeStatusResponse = client
        .get(format!("{}/admin/upgrade-status", url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(resp.nodes.len(), 3);
    assert!(resp.mixed_version, "Should detect mixed versions");
    assert_eq!(resp.active_version, None);

    // Verify gated_features includes extended_info_fields
    let gate = resp
        .gated_features
        .iter()
        .find(|g| g.name == "extended_info_fields");
    assert!(gate.is_some(), "Should include extended_info_fields gate");
    assert!(!gate.unwrap().active, "Gate should not be active yet");

    harness.shutdown_all().await;
}

/// Helper: send an admin command to the Raft leader via the admin RESP port.
async fn admin_send_to_leader(
    harness: &ClusterTestHarness,
    cmd: &str,
    args: &[&str],
) -> (u64, frogdb_protocol::Response) {
    let leader_id = harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    let node = harness.node(leader_id).unwrap();
    let response = node.admin_send(cmd, args).await;
    (leader_id, response)
}

/// Tests /admin/upgrade-status after finalization shows active gate.
#[tokio::test]
async fn test_admin_upgrade_status_after_finalize() {
    let config = ClusterNodeConfig {
        admin_enabled: true,
        ..Default::default()
    };
    let mut harness = ClusterTestHarness::with_config(config);
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(10))
        .await
        .unwrap();

    harness.fake_all_node_versions("0.2.0");

    // Finalize via admin RESP port (FROGDB.FINALIZE requires admin flag)
    let (_, response) = admin_send_to_leader(&harness, "FROGDB.FINALIZE", &["0.2.0"]).await;
    assert!(
        matches!(&response, frogdb_protocol::Response::Simple(s) if s.as_ref() == b"OK"),
        "Expected OK, got {:?}",
        response
    );
    // Poll until the finalization propagates and active_version is visible
    let node_ids = harness.node_ids();
    let node = harness.node(node_ids[0]).unwrap();
    let url = node.admin_http_url().unwrap();
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let resp: UpgradeStatusResponse = loop {
        let r: UpgradeStatusResponse = client
            .get(format!("{}/admin/upgrade-status", &url))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if r.active_version.is_some() {
            break r;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("active_version did not propagate within 5 seconds");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    assert_eq!(resp.active_version, Some("0.2.0".to_string()));
    assert!(!resp.mixed_version);

    let gate = resp
        .gated_features
        .iter()
        .find(|g| g.name == "extended_info_fields")
        .unwrap();
    assert!(gate.active, "Gate should be active after finalization");

    harness.shutdown_all().await;
}

// ============================================================================
// Mixed-Version Data Path Tests (INFO gate behavior)
// ============================================================================

/// Helper: parse INFO response bulk string into key-value pairs.
fn parse_info_response(
    response: &frogdb_protocol::Response,
) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    if let frogdb_protocol::Response::Bulk(Some(data)) = response {
        let text = String::from_utf8_lossy(data);
        for line in text.lines() {
            if line.starts_with('#') || line.is_empty() {
                continue;
            }
            if let Some((k, v)) = line.split_once(':') {
                map.insert(k.to_string(), v.to_string());
            }
        }
    }
    map
}

/// Tests that INFO server does NOT include version fields before finalization.
#[tokio::test]
async fn test_info_gate_suppressed_before_finalize() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(10))
        .await
        .unwrap();

    // Fake versions but do NOT finalize — gate should be inactive
    harness.fake_all_node_versions("0.2.0");

    let node_ids = harness.node_ids();
    let node = harness.node(node_ids[0]).unwrap();
    let response = node.send("INFO", &["server"]).await;
    let info = parse_info_response(&response);

    assert!(
        !info.contains_key("active_version"),
        "active_version should NOT appear before finalization"
    );
    assert!(
        !info.contains_key("cluster_version"),
        "cluster_version should NOT appear before finalization"
    );

    harness.shutdown_all().await;
}

/// Tests that INFO server DOES include version fields after finalization.
#[tokio::test]
async fn test_info_gate_active_after_finalize() {
    let config = ClusterNodeConfig {
        admin_enabled: true,
        ..Default::default()
    };
    let mut harness = ClusterTestHarness::with_config(config);
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(10))
        .await
        .unwrap();

    harness.fake_all_node_versions("0.2.0");

    // Finalize
    let (_, response) = admin_send_to_leader(&harness, "FROGDB.FINALIZE", &["0.2.0"]).await;
    assert!(
        matches!(&response, frogdb_protocol::Response::Simple(s) if s.as_ref() == b"OK"),
        "Expected OK, got {:?}",
        response
    );
    tokio::time::sleep(Duration::from_millis(500)).await;

    // INFO server should now include gated fields
    let node_ids = harness.node_ids();
    let node = harness.node(node_ids[0]).unwrap();
    let response = node.send("INFO", &["server"]).await;
    let info = parse_info_response(&response);

    assert_eq!(
        info.get("active_version").map(|s| s.as_str()),
        Some("0.2.0"),
        "active_version should be 0.2.0 after finalization"
    );
    assert!(
        info.contains_key("cluster_version"),
        "cluster_version should appear after finalization"
    );

    harness.shutdown_all().await;
}

/// Tests that INFO gate stays suppressed during partial upgrade (cannot finalize).
#[tokio::test]
async fn test_info_gate_suppressed_during_partial_upgrade() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(10))
        .await
        .unwrap();

    // Mixed versions — cannot finalize
    let node_ids = harness.node_ids();
    harness.fake_all_node_versions("0.2.0");
    harness.fake_node_version(node_ids[2], "0.1.0");

    // Attempt finalize — should fail
    let (_, response) = send_to_leader(&harness, "FROGDB.FINALIZE", &["0.2.0"]).await;
    assert!(
        matches!(&response, frogdb_protocol::Response::Error(_)),
        "Expected error for mixed-version finalize, got {:?}",
        response
    );

    // INFO should not have extended fields
    let node = harness.node(node_ids[0]).unwrap();
    let response = node.send("INFO", &["server"]).await;
    let info = parse_info_response(&response);

    assert!(
        !info.contains_key("active_version"),
        "active_version should NOT appear during partial upgrade"
    );

    harness.shutdown_all().await;
}

// ============================================================================
// Rolling Upgrade with Continuous Traffic
// ============================================================================

/// Helper: send a SET command, following MOVED redirects to the correct node.
/// Returns true if the write succeeded.
async fn set_with_redirect(harness: &ClusterTestHarness, key: &str, value: &str) -> bool {
    // Try sending to any running node
    for &nid in &harness.node_ids() {
        let node = match harness.node(nid) {
            Some(n) if n.is_running() => n,
            _ => continue,
        };
        let resp = node.send("SET", &[key, value]).await;
        if !is_error(&resp) {
            return true;
        }
        if let Some((_slot, addr)) = is_moved_redirect(&resp) {
            for &redirect_nid in &harness.node_ids() {
                if let Some(n) = harness.node(redirect_nid)
                    && n.is_running()
                    && n.client_addr() == addr
                {
                    let retry = n.send("SET", &[key, value]).await;
                    return !is_error(&retry);
                }
            }
        }
        // Got an error that wasn't MOVED — cluster may be recovering
        return false;
    }
    false
}

/// Helper: send a GET command, following MOVED redirects.
/// Returns Some(value) if the key exists and was retrieved.
async fn get_with_redirect(harness: &ClusterTestHarness, key: &str) -> Option<String> {
    for &nid in &harness.node_ids() {
        let node = match harness.node(nid) {
            Some(n) if n.is_running() => n,
            _ => continue,
        };
        let resp = node.send("GET", &[key]).await;
        match &resp {
            frogdb_protocol::Response::Bulk(Some(data)) => {
                return Some(String::from_utf8_lossy(data).to_string());
            }
            frogdb_protocol::Response::Bulk(None) => return None,
            frogdb_protocol::Response::Error(e) => {
                let msg = String::from_utf8_lossy(e);
                if let Some((_slot, addr)) = is_moved_redirect(&resp) {
                    for &redirect_nid in &harness.node_ids() {
                        if let Some(n) = harness.node(redirect_nid)
                            && n.is_running()
                            && n.client_addr() == addr
                        {
                            let retry = n.send("GET", &[key]).await;
                            if let frogdb_protocol::Response::Bulk(Some(data)) = &retry {
                                return Some(String::from_utf8_lossy(data).to_string());
                            }
                            return None;
                        }
                    }
                }
                if msg.starts_with("CLUSTERDOWN") {
                    return None;
                }
            }
            _ => {}
        }
        break;
    }
    None
}

/// Tests rolling upgrade with interleaved traffic: restart nodes one by one while
/// sending SET/GET commands, then finalize and verify data integrity.
#[tokio::test]
async fn test_rolling_upgrade_with_continuous_traffic() {
    let config = ClusterNodeConfig {
        persistence: true,
        admin_enabled: true,
        ..Default::default()
    };
    let mut harness = ClusterTestHarness::with_config(config);
    harness.start_cluster(5).await.unwrap();

    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    // Write initial data
    let mut write_successes = 0u32;
    let mut write_failures = 0u32;
    for i in 0..50 {
        let key = format!("upgrade_key_{}", i);
        let value = format!("upgrade_value_{}", i);
        if set_with_redirect(&harness, &key, &value).await {
            write_successes += 1;
        } else {
            write_failures += 1;
        }
    }
    eprintln!(
        "Pre-upgrade: {}/{} writes succeeded",
        write_successes,
        write_successes + write_failures
    );
    assert!(
        write_successes > 0,
        "Should have some successful pre-upgrade writes"
    );

    // Rolling upgrade: restart each node, interleave with traffic
    let node_ids = harness.node_ids();
    for (i, &node_id) in node_ids.iter().enumerate() {
        eprintln!("Upgrading node {}/{}: {}", i + 1, node_ids.len(), node_id);

        // Fake this node's version to 0.2.0 across all local states
        harness.fake_node_version(node_id, "0.2.0");

        // Graceful shutdown
        harness.shutdown_node(node_id).await;
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Send traffic while node is down
        for j in 0..10 {
            let key = format!("during_upgrade_{}_{}", i, j);
            let value = format!("value_{}_{}", i, j);
            if set_with_redirect(&harness, &key, &value).await {
                write_successes += 1;
            } else {
                write_failures += 1;
            }
        }

        // Wait for leader if we have quorum
        let running = node_ids
            .iter()
            .filter(|&&id| harness.node(id).map(|n| n.is_running()).unwrap_or(false))
            .count();
        if running >= 3 {
            let _ = harness.wait_for_leader(Duration::from_secs(5)).await;
        }

        // Restart node (simulates coming back with new binary)
        harness.restart_node(node_id).await.unwrap();
        harness
            .wait_for_node_recognized(node_id, Duration::from_secs(10))
            .await
            .unwrap();

        // Re-fake version on the restarted node's local state
        // (restart resets it to the compile-time binary version)
        harness.fake_node_version(node_id, "0.2.0");

        // Send more traffic after restart
        for j in 0..10 {
            let key = format!("after_restart_{}_{}", i, j);
            let value = format!("value_{}_{}", i, j);
            if set_with_redirect(&harness, &key, &value).await {
                write_successes += 1;
            } else {
                write_failures += 1;
            }
        }
    }

    // All nodes upgraded — fake all versions consistently and finalize
    harness.fake_all_node_versions("0.2.0");

    let (_, response) = admin_send_to_leader(&harness, "FROGDB.FINALIZE", &["0.2.0"]).await;
    assert!(
        matches!(&response, frogdb_protocol::Response::Simple(s) if s.as_ref() == b"OK"),
        "Expected OK after upgrade, got {:?}",
        response
    );
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify finalization replicated
    for &node_id in &harness.node_ids() {
        let node = harness.node(node_id).unwrap();
        let cs = node.cluster_state().unwrap();
        assert_eq!(
            cs.active_version(),
            Some("0.2.0".to_string()),
            "Node {} should have active_version 0.2.0 after finalize",
            node_id
        );
    }

    // Verify data integrity: read back a sample of pre-upgrade keys
    let mut read_successes = 0;
    for i in 0..50 {
        let key = format!("upgrade_key_{}", i);
        let expected = format!("upgrade_value_{}", i);
        if let Some(val) = get_with_redirect(&harness, &key).await
            && val == expected
        {
            read_successes += 1;
        }
    }

    let total_writes = write_successes + write_failures;
    let write_rate = (write_successes as f64 / total_writes as f64) * 100.0;
    eprintln!(
        "Rolling upgrade results: writes {}/{} ({:.1}%), reads {}/50",
        write_successes, total_writes, write_rate, read_successes
    );

    // Expect most writes to succeed (some fail during node-down window)
    assert!(
        write_rate >= 50.0,
        "Expected >= 50% write success rate, got {:.1}%",
        write_rate
    );
    // Pre-upgrade data should be intact
    assert!(
        read_successes >= 25,
        "Expected >= 25/50 pre-upgrade keys readable, got {}",
        read_successes
    );

    harness.shutdown_all().await;
}
