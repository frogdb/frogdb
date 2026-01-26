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
use common::cluster_helpers::{is_error, parse_cluster_info, parse_cluster_nodes};
use std::time::Duration;

// ============================================================================
// Tier 1: Basic Cluster Formation
// ============================================================================

/// Tests that the harness can start a 3-node cluster.
/// NOTE: Ignored until CLUSTER commands return actual cluster state.
#[tokio::test]
#[ignore = "Requires full cluster mode implementation"]
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
/// NOTE: Ignored until CLUSTER commands return actual cluster state.
#[tokio::test]
#[ignore = "Requires full cluster mode implementation"]
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
/// NOTE: Ignored until CLUSTER commands return actual cluster state.
#[tokio::test]
#[ignore = "Requires full cluster mode implementation"]
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
/// NOTE: Ignored until CLUSTER commands return actual cluster state.
#[tokio::test]
#[ignore = "Requires full cluster mode implementation"]
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
/// NOTE: Ignored until CLUSTER commands return actual cluster state.
#[tokio::test]
#[ignore = "Requires full cluster mode implementation"]
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
/// NOTE: Ignored until CLUSTER commands return actual cluster state.
#[tokio::test]
#[ignore = "Requires full cluster mode implementation"]
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
/// NOTE: Ignored until CLUSTER commands return actual cluster state.
#[tokio::test]
#[ignore = "Requires full cluster mode implementation"]
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
/// NOTE: Ignored until CLUSTER commands return actual cluster state.
#[tokio::test]
#[ignore = "Requires full cluster mode implementation"]
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

    // Kill 3 nodes (majority)
    let nodes: Vec<_> = harness.node_ids();
    harness.kill_node(nodes[0]);
    harness.kill_node(nodes[1]);
    harness.kill_node(nodes[2]);

    // Remaining 2 nodes should not be able to elect leader
    // Note: After Phase 4, surviving nodes should detect quorum loss via heartbeat failures
    tokio::time::sleep(Duration::from_secs(5)).await;

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
/// NOTE: Ignored until CLUSTER commands return actual cluster state.
#[tokio::test]
#[ignore = "Requires full cluster mode implementation"]
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
/// NOTE: Ignored until CLUSTER commands return actual cluster state.
#[tokio::test]
#[ignore = "Requires full cluster mode implementation"]
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
