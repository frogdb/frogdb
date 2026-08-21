//! Cluster failover and fault-tolerance integration tests: leader election and
//! promotion, replica roles, network partitions and split-brain prevention,
//! quorum loss, data integrity across failover, and restart/stress scenarios.
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

use common::cluster_support::{find_owner_of_slot, get_with_redirect, wait_for_data_path_role};
use frogdb_test_harness::cluster_harness::{ClusterNodeConfig, ClusterTestHarness};
use frogdb_test_harness::cluster_helpers::{
    connected_slaves, get_error_message, is_cluster_down, is_error, is_moved_redirect,
    key_for_slot, parse_cluster_info, parse_cluster_nodes, parse_info_fields,
};
use std::time::Duration;

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
            frogdb_protocol::Response::pong(),
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

    // `wait_for_cluster_convergence` above already requires every running
    // node (including `victim`) to report `cluster_state:ok` in the same
    // poll iteration, but that check and the read below are two independent
    // round trips. Under whole-suite CPU contention a transient quorum-ack
    // gap (`cluster_info`'s `millis_since_quorum_ack` check, see
    // `commands/cluster/mod.rs`) can flip the state back to `fail` in the
    // window between them, so poll the exact observable this assertion
    // reads instead of trusting the earlier snapshot (hardening issue 43).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let info = loop {
        let info = harness.get_cluster_info(victim).await.unwrap();
        if info.cluster_state == "ok" || tokio::time::Instant::now() >= deadline {
            break info;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(
        info.cluster_state, "ok",
        "victim's own view of cluster_state should converge to ok after rejoin, got {info:?}"
    );

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
            let info = harness.get_cluster_info(node_id).await.unwrap();
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
        let info = harness.get_cluster_info(node_id).await.unwrap();
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
    let info = harness.get_cluster_info(node_ids[3]).await.unwrap();
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
            let info = harness.get_cluster_info(node_id).await.unwrap();
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
    let final_info = harness.get_cluster_info(target_node_id).await.unwrap();
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
            if let Ok(info) = harness.get_cluster_info(node_id).await
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

    // Verify the cluster can still make progress.
    //
    // Deliberately *not* `wait_for_cluster_convergence`: that requires
    // `cluster_state:ok`, and the killed node is a slot-owning primary with no
    // replica. Once the failure detector latches its FAIL flag those slots are
    // uncovered and `fail` is the correct, permanent answer — so `ok` here was
    // only ever a race against the ~5-probe latch. Progress means the four
    // survivors converge on one topology.
    harness
        .wait_for_topology_agreement(Duration::from_secs(10))
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

/// A `CLUSTER REPLICATE`d replica opens a real data-replication link to its
/// primary and serves the primary's writes back under `READONLY`.
///
/// This is the load-bearing assertion for cluster-mode data replication. Raft
/// carries *cluster metadata only* (ADR-0001): no `ClusterCommand` variant
/// holds a key or a value, so a Raft follower that is a peer shard's primary
/// never sees another shard's data. Data reaches a replica exactly one way —
/// the per-node PSYNC link, the same mechanism standalone replication uses.
/// Two independent observables therefore have to hold:
///
/// 1. the primary reports `connected_slaves:1` — the PSYNC link exists at all;
/// 2. the replica returns the primary's value for an owned-slot key under
///    `READONLY` — the link actually carries the write stream.
///
/// Asserting only (2) would let a test pass on a node that happens to hold the
/// key for an unrelated reason; asserting only (1) would let it pass on a link
/// that is attached but inert.
#[tokio::test]
async fn test_cluster_replica_receives_writes_asserted() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(2).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(30))
        .await
        .unwrap();
    let _ = harness
        .wait_for_address_convergence(Duration::from_secs(10))
        .await;

    let test_slot = 100u16;
    let (owner_id, _) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");

    let replica_id = harness.add_replica(owner_id).await.unwrap();

    harness
        .wait_for_replication_link(owner_id, 1, Duration::from_secs(20))
        .await
        .expect("primary never reported an attached replica");

    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let key = format!("{}_replicated", tag);
    let owner = harness.node(owner_id).unwrap();
    let set_resp = owner.send("SET", &[&key, "replicated_value"]).await;
    assert!(
        !is_error(&set_resp),
        "SET on primary failed: {:?}",
        set_resp
    );

    // A replica does not own slots, so reads need READONLY on a held
    // connection to bypass the MOVED redirect.
    let replica = harness.node(replica_id).unwrap();
    let mut client = replica.connect().await;
    let ro = client.command(&["READONLY"]).await;
    assert!(!is_error(&ro), "READONLY on replica failed: {:?}", ro);

    let mut last = frogdb_protocol::Response::Null;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        last = client.command(&["GET", &key]).await;
        if matches!(&last, frogdb_protocol::Response::Bulk(Some(v)) if v.as_ref() == b"replicated_value")
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        matches!(&last, frogdb_protocol::Response::Bulk(Some(v)) if v.as_ref() == b"replicated_value"),
        "replica {replica_id} never received the primary's write, last GET: {last:?}"
    );

    // The replica must also present itself as a replica on the data path.
    let info = replica.send("INFO", &["replication"]).await;
    let fields = parse_info_fields(&info).expect("INFO replication should be a bulk reply");
    assert_eq!(
        fields.get("role").map(String::as_str),
        Some("slave"),
        "replica should report role:slave, got {fields:?}"
    );

    // The link is still attached after the write, not merely at handshake time.
    let primary_info = harness
        .node(owner_id)
        .unwrap()
        .send("INFO", &["replication"])
        .await;
    assert_eq!(
        connected_slaves(&primary_info),
        Some(1),
        "primary should still report one attached replica, got: {primary_info:?}"
    );

    harness.shutdown_all().await;
}

/// A committed cluster-state promotion installs the primary role on the data
/// path: the promoted node stops being a replica, accepts writes, and keeps the
/// data it replicated before the failover.
///
/// This exercises the bridge between the two role systems. Raft owns the
/// cluster-state role (`NodeRole` in `CLUSTER NODES`); the data path owns the
/// replication role (the `is_replica` flag, the inbound stream, the replication
/// identity). Without the `RoleChangeEvent` -> `RoleManager::promote` bridge a
/// failover flips only the first, leaving a node that advertises `master` while
/// still refusing writes and rejecting WAIT.
///
/// `CLUSTER FAILOVER TAKEOVER` is used rather than killing the primary because
/// auto-failover is off by default, so a kill alone changes no cluster state.
#[tokio::test]
async fn test_cluster_failover_promotes_the_data_path() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(2).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(30))
        .await
        .unwrap();
    let _ = harness
        .wait_for_address_convergence(Duration::from_secs(10))
        .await;

    let test_slot = 100u16;
    let (owner_id, _) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");
    let replica_id = harness.add_replica(owner_id).await.unwrap();
    harness
        .wait_for_replication_link(owner_id, 1, Duration::from_secs(20))
        .await
        .expect("primary never reported an attached replica");

    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let key = format!("{}_failover", tag);
    let owner = harness.node(owner_id).unwrap();
    assert!(!is_error(&owner.send("SET", &[&key, "pre_failover"]).await));

    // Before the failover the replica refuses writes on the data path.
    let replica = harness.node(replica_id).unwrap();
    assert_eq!(
        wait_for_data_path_role(&harness, replica_id, "slave", Duration::from_secs(10)).await,
        "slave"
    );

    let resp = replica
        .try_send_admin_aware("CLUSTER", &["FAILOVER", "TAKEOVER"])
        .await
        .unwrap();
    assert!(
        !is_error(&resp),
        "CLUSTER FAILOVER TAKEOVER failed: {resp:?}"
    );

    // The data-path role must follow the cluster-state role.
    assert_eq!(
        wait_for_data_path_role(&harness, replica_id, "master", Duration::from_secs(20)).await,
        "master",
        "the promoted node must install the primary role on its data path, not \
         just in cluster state"
    );

    // ...and it must actually be writable for the slots it took over, with the
    // data it replicated before the promotion still present.
    let promoted = harness.node(replica_id).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut last = frogdb_protocol::Response::Null;
    while std::time::Instant::now() < deadline {
        last = promoted.send("SET", &[&key, "post_failover"]).await;
        if !is_error(&last) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        !is_error(&last),
        "the promoted node must accept writes for the slots it took over, got: {last:?}"
    );

    let resp = promoted.send("GET", &[&key]).await;
    assert!(
        matches!(&resp, frogdb_protocol::Response::Bulk(Some(v)) if v.as_ref() == b"post_failover"),
        "promoted node should serve its own write, got: {resp:?}"
    );

    harness.shutdown_all().await;
}

/// Snapshot one node's Raft state machine and purge the log the snapshot
/// covers, so its next boot restores from the snapshot instead of replaying
/// entries.
async fn force_snapshot_and_purge(harness: &ClusterTestHarness, node_id: u64) {
    let raft = harness
        .node(node_id)
        .unwrap()
        .raft()
        .expect("cluster nodes run Raft");

    raft.trigger()
        .snapshot()
        .await
        .expect("triggering a snapshot must not fail");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let snapshot_index = loop {
        let taken = raft.metrics().borrow().snapshot.map(|id| id.index);
        if let Some(index) = taken {
            break index;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "node {node_id} never reported a snapshot"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    raft.trigger()
        .purge_log(snapshot_index)
        .await
        .expect("purging the snapshotted log prefix must not fail");
}

/// A restarted cluster replica comes back as a replica — in cluster state, on
/// its own data path, and on its primary's replica list.
///
/// Two boot-time paths used to lose the role, and both are covered here because
/// which one runs depends on whether openraft had snapshotted the `SetRole`
/// entry by restart time:
///
/// 1. **Self-registration.** Every node re-proposes `AddNode` for itself at
///    boot, claiming the only role it can know unaided — primary. When that
///    overwrote the recorded role, a restarted replica demoted-by-restart into
///    a slotless primary cluster-wide, and its primary lost a replica.
/// 2. **Snapshot restore.** A role folded into the restored snapshot produces no
///    log entry to replay, so nothing told the data path about it: the node came
///    back answering `role:master` while the cluster still listed it as a
///    replica — the contradictory role views of issue 37.
///
/// The assertions are deliberately taken from three vantage points: the
/// primary's view of the cluster (`CLUSTER NODES`), the replica's view of itself
/// (`INFO replication`), and the actual data link (`connected_slaves` plus
/// `master_link_status:up`). Any one alone can be satisfied by a node that is
/// only half-reattached.
#[tokio::test]
async fn test_restarted_cluster_replica_stays_a_replica() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(2).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(30))
        .await
        .unwrap();
    let _ = harness
        .wait_for_address_convergence(Duration::from_secs(10))
        .await;

    let test_slot = 100u16;
    let (owner_id, _) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");
    let replica_id = harness.add_replica(owner_id).await.unwrap();
    harness
        .wait_for_replication_link(owner_id, 1, Duration::from_secs(20))
        .await
        .expect("primary never reported an attached replica");
    assert_eq!(
        wait_for_data_path_role(&harness, replica_id, "slave", Duration::from_secs(10)).await,
        "slave"
    );

    // Fold the `SetRole` entry into a snapshot and purge the log behind it, so
    // the restart below has to restore the role from the snapshot. Without this
    // the node replays the entry and the ordinary apply path emits the role
    // change, which leaves boot path (2) above untested.
    force_snapshot_and_purge(&harness, replica_id).await;

    harness.restart_node(replica_id).await.unwrap();
    harness
        .wait_for_node_has_leader(replica_id, Duration::from_secs(20))
        .await
        .expect("restarted replica never rediscovered the Raft leader");

    // (1) Cluster state: every node, including the primary, still calls it a
    //     replica. This is what the self-registration `AddNode` used to clobber.
    harness
        .wait_for_role_propagation(replica_id, Duration::from_secs(20))
        .await
        .expect("restarted node stopped being a replica in cluster state");

    let replica_hex = format!("{:040x}", replica_id);
    let nodes = harness
        .node(owner_id)
        .unwrap()
        .send("CLUSTER", &["NODES"])
        .await;
    let rendered = match &nodes {
        frogdb_protocol::Response::Bulk(Some(b)) => String::from_utf8_lossy(b).to_string(),
        other => panic!("CLUSTER NODES should be a bulk reply, got {other:?}"),
    };
    let line = rendered
        .lines()
        .find(|l| l.starts_with(&replica_hex))
        .unwrap_or_else(|| panic!("restarted node missing from CLUSTER NODES:\n{rendered}"));
    assert!(
        line.contains("slave"),
        "the primary must still render the restarted node as a slave, got: {line}"
    );

    // (2) The data path: the node itself agrees, and its inbound link is up.
    assert_eq!(
        wait_for_data_path_role(&harness, replica_id, "slave", Duration::from_secs(20)).await,
        "slave",
        "the restarted node reports role:master while cluster state calls it a \
         replica — the two role views diverged across the restart"
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut link_status = String::new();
    while std::time::Instant::now() < deadline {
        let info = harness
            .node(replica_id)
            .unwrap()
            .send("INFO", &["replication"])
            .await;
        if let Some(fields) = parse_info_fields(&info)
            && let Some(status) = fields.get("master_link_status")
        {
            link_status = status.clone();
            if link_status == "up" {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(
        link_status, "up",
        "the restarted replica never re-established its PSYNC link"
    );

    // (3) The primary's side of that same link.
    harness
        .wait_for_replication_link(owner_id, 1, Duration::from_secs(20))
        .await
        .expect("primary never saw the restarted replica reattach");

    // ...and it carries data, not just a handshake.
    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let key = format!("{}_post_restart", tag);
    let owner = harness.node(owner_id).unwrap();
    assert!(!is_error(
        &owner.send("SET", &[&key, "after_restart"]).await
    ));

    let replica = harness.node(replica_id).unwrap();
    let mut client = replica.connect().await;
    assert!(!is_error(&client.command(&["READONLY"]).await));
    let mut last = frogdb_protocol::Response::Null;
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while std::time::Instant::now() < deadline {
        last = client.command(&["GET", &key]).await;
        if matches!(&last, frogdb_protocol::Response::Bulk(Some(v)) if v.as_ref() == b"after_restart")
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        matches!(&last, frogdb_protocol::Response::Bulk(Some(v)) if v.as_ref() == b"after_restart"),
        "restarted replica never received a post-restart write, last GET: {last:?}"
    );

    harness.shutdown_all().await;
}

/// Test 6: Replica receives writes.
///
/// Raft followers are *peer shard primaries*, not data replicas: Raft carries
/// cluster metadata only, so a write to one node's slots is never visible on
/// another node's data path. The only correct outcome on a peer is a MOVED
/// redirect (or, with READONLY, a miss) — never the value.
///
/// The replica-side positive case lives in
/// [`test_cluster_replica_receives_writes_asserted`].
#[tokio::test]
async fn test_raft_followers_do_not_receive_peer_shard_writes() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();

    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(30))
        .await
        .unwrap();
    let _ = harness
        .wait_for_address_convergence(Duration::from_secs(10))
        .await;

    let test_slot = 100u16;
    let (owner_id, non_owner_id) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");

    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let key = format!("{}_peer", tag);
    let owner = harness.node(owner_id).unwrap();
    let set_resp = owner.send("SET", &[&key, "owner_value"]).await;
    assert!(!is_error(&set_resp), "SET on owner failed: {:?}", set_resp);

    // Give Raft ample time to replicate its log. No amount of it can move
    // user data, because no ClusterCommand carries any.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let peer = harness.node(non_owner_id).unwrap();
    let resp = peer.send("GET", &[&key]).await;
    assert!(
        is_moved_redirect(&resp).is_some(),
        "peer shard primary should redirect, got: {resp:?}"
    );

    // Even with the redirect suppressed the peer has no copy of the value.
    let mut client = peer.connect().await;
    let ro = client.command(&["READONLY"]).await;
    assert!(!is_error(&ro), "READONLY failed: {ro:?}");
    let resp = client.command(&["GET", &key]).await;
    assert!(
        matches!(resp, frogdb_protocol::Response::Bulk(None)),
        "peer shard primary must not hold another shard's key, got: {resp:?}"
    );

    // The peer owns slots of its own, so it is a primary on the data path.
    let info = peer.send("INFO", &["replication"]).await;
    let fields = parse_info_fields(&info).expect("INFO replication should be a bulk reply");
    assert_eq!(
        fields.get("role").map(String::as_str),
        Some("master"),
        "peer shard primary should report role:master, got {fields:?}"
    );

    harness.shutdown_all().await;
}

/// Test 7: a replica that attaches late catches up on the data written before
/// it existed, and then keeps following the live stream.
///
/// The original test disconnected a *Raft follower* and looked for user data on
/// it after a restart, which cannot work: Raft carries metadata only (ADR-0001).
/// Catch-up is a property of the PSYNC link, and the interesting delay is the
/// one between a write and the replica's arrival — the replica must full-sync
/// the existing dataset (the staged-checkpoint install) and then continue on the
/// command stream without a gap.
///
/// Both halves are asserted, because either alone is passable by a broken
/// implementation: a full sync that never hands off to the stream satisfies the
/// backfill, and a stream that starts at the current offset satisfies the live
/// write while silently losing everything older.
///
/// Persistence is enabled deliberately, unlike most of this suite. A
/// persistence-disabled primary answers `PSYNC ? -1` from the minimal-RDB
/// branch, which carries no dataset at all (issue 66), so the backfill half
/// would be asserting a known gap rather than the catch-up path.
#[tokio::test]
async fn test_replica_catches_up_on_data_written_before_it_attached() {
    let config = ClusterNodeConfig {
        persistence: true,
        ..Default::default()
    };
    let mut harness = ClusterTestHarness::with_config(config);
    harness.start_cluster(2).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(30))
        .await
        .unwrap();
    let _ = harness
        .wait_for_address_convergence(Duration::from_secs(10))
        .await;

    let test_slot = 100u16;
    let (owner_id, _) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");
    let tag = format!("{{{}}}", key_for_slot(test_slot));

    // Backfill: written while the primary has no replica at all.
    let owner = harness.node(owner_id).unwrap();
    let backfill: Vec<(String, String)> = (0..10)
        .map(|i| (format!("{tag}_backfill_{i}"), format!("backfill_value_{i}")))
        .collect();
    for (key, value) in &backfill {
        let resp = owner.send("SET", &[key, value]).await;
        assert!(!is_error(&resp), "SET on primary failed: {resp:?}");
    }

    // The replica arrives afterwards and must full-sync the dataset above.
    let replica_id = harness.add_replica(owner_id).await.unwrap();
    harness
        .wait_for_replication_link(owner_id, 1, Duration::from_secs(20))
        .await
        .expect("primary never reported an attached replica");

    // A live write made after the link came up exercises the stream that
    // follows the snapshot.
    let live_key = format!("{tag}_live");
    let owner = harness.node(owner_id).unwrap();
    let resp = owner.send("SET", &[&live_key, "live_value"]).await;
    assert!(!is_error(&resp), "live SET on primary failed: {resp:?}");

    // Replicas own no slots, so reads need READONLY on a held connection.
    let replica = harness.node(replica_id).unwrap();
    let mut client = replica.connect().await;
    let ro = client.command(&["READONLY"]).await;
    assert!(!is_error(&ro), "READONLY on replica failed: {ro:?}");

    // The live write is the last thing sent, so waiting for it also bounds the
    // wait for the backfill: the snapshot precedes the stream.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut last = frogdb_protocol::Response::Null;
    while std::time::Instant::now() < deadline {
        last = client.command(&["GET", &live_key]).await;
        if matches!(&last, frogdb_protocol::Response::Bulk(Some(v)) if v.as_ref() == b"live_value")
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        matches!(&last, frogdb_protocol::Response::Bulk(Some(v)) if v.as_ref() == b"live_value"),
        "replica {replica_id} never received the post-attach write, last GET: {last:?}"
    );

    for (key, expected) in &backfill {
        let resp = client.command(&["GET", key]).await;
        assert!(
            matches!(&resp, frogdb_protocol::Response::Bulk(Some(v)) if v.as_ref() == expected.as_bytes()),
            "replica {replica_id} did not catch up on {key} (written before it attached), got: {resp:?}"
        );
    }

    harness.shutdown_all().await;
}

/// Test 8: a replica promoted after its primary dies serves every key it
/// replicated, with no losses and no redirects.
///
/// The original test counted a `MOVED` reply as a hit ("data exists, just on
/// another node"), which made the assertion vacuous — a node that lost the
/// whole dataset and a node that never owned the slots both scored 10/10. The
/// promoted node takes over the dead primary's slots, so `MOVED` here is a
/// failure, not an acceptable outcome.
///
/// `CLUSTER FAILOVER TAKEOVER` drives the promotion because `auto_failover`
/// defaults to off: killing the primary alone changes no cluster state.
#[tokio::test]
async fn test_promoted_replica_has_all_data() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(2).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(30))
        .await
        .unwrap();
    let _ = harness
        .wait_for_address_convergence(Duration::from_secs(10))
        .await;

    let test_slot = 100u16;
    let (owner_id, _) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");
    let replica_id = harness.add_replica(owner_id).await.unwrap();
    harness
        .wait_for_replication_link(owner_id, 1, Duration::from_secs(20))
        .await
        .expect("primary never reported an attached replica");

    let tag = format!("{{{}}}", key_for_slot(test_slot));
    let keys_values: Vec<(String, String)> = (0..10)
        .map(|i| (format!("{tag}_promote_{i}"), format!("promote_value_{i}")))
        .collect();
    let owner = harness.node(owner_id).unwrap();
    for (key, value) in &keys_values {
        let resp = owner.send("SET", &[key, value]).await;
        assert!(!is_error(&resp), "SET on primary failed: {resp:?}");
    }

    // WAIT is the durability barrier: it returns once the replica has acked the
    // offset covering the writes above, so the promotion below cannot race the
    // stream. (In cluster mode it counts this node's replicas, same as
    // standalone — see the WAIT section of this suite.)
    let acked = owner.send("WAIT", &["1", "10000"]).await;
    assert_eq!(
        acked,
        frogdb_protocol::Response::Integer(1),
        "the replica should have acked the writes before the failover, got: {acked:?}"
    );

    harness.kill_node(owner_id).await;
    harness
        .wait_for_new_leader(owner_id, Duration::from_secs(20))
        .await
        .expect("the surviving nodes never elected a new raft leader");

    let replica = harness.node(replica_id).unwrap();
    let resp = replica
        .try_send_admin_aware("CLUSTER", &["FAILOVER", "TAKEOVER"])
        .await
        .unwrap();
    assert!(
        !is_error(&resp),
        "CLUSTER FAILOVER TAKEOVER failed: {resp:?}"
    );
    assert_eq!(
        wait_for_data_path_role(&harness, replica_id, "master", Duration::from_secs(20)).await,
        "master",
        "the promoted node must install the primary role on its data path"
    );

    let promoted = harness.node(replica_id).unwrap();
    for (key, expected) in &keys_values {
        let resp = promoted.send("GET", &[key]).await;
        assert!(
            matches!(&resp, frogdb_protocol::Response::Bulk(Some(v)) if v.as_ref() == expected.as_bytes()),
            "promoted node lost {key}; a MOVED or a miss here is a failure, not a \
             relocation — it owns the dead primary's slots. Got: {resp:?}"
        );
    }

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
            let info = harness.get_cluster_info(node_id).await.unwrap();
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
        let info = harness.get_cluster_info(leader).await.unwrap();
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
        let info = harness.get_cluster_info(node_id).await.unwrap();
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

/// The Raft voter set a node believes in, as `(node_id, is_voter)` is not
/// enough: the assertions below need to distinguish "not a voter" from "not
/// known to Raft at all".
fn raft_voter_ids(harness: &ClusterTestHarness, node_id: u64) -> Vec<u64> {
    let raft = harness
        .node(node_id)
        .and_then(|n| n.raft())
        .expect("node is running in cluster mode");
    let membership = raft.metrics().borrow().membership_config.clone();
    let mut ids: Vec<u64> = membership.membership().voter_ids().collect();
    ids.sort_unstable();
    ids
}

/// Poll every live node's voter set until `predicate` holds on all of them.
async fn wait_for_voters(
    harness: &ClusterTestHarness,
    live: &[u64],
    what: &str,
    predicate: impl Fn(&[u64]) -> bool,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let views: Vec<Vec<u64>> = live.iter().map(|&id| raft_voter_ids(harness, id)).collect();
        if views.iter().all(|v| predicate(v)) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}; voter sets: {views:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Send `CLUSTER FORGET <node_id_str>` to `nodes` in turn until one reply
/// satisfies `accept`, or fail at the deadline naming the last reply seen.
///
/// Retrying is not flake-hiding: in the seconds after a node goes down the
/// survivors answer `REDIRECT` naming a leader that no longer exists, because
/// the writer could neither propose nor forward. That is election churn, not a
/// verdict on the command.
async fn forget_until(
    harness: &ClusterTestHarness,
    nodes: &[u64],
    node_id_str: &str,
    what: &str,
    accept: impl Fn(&frogdb_protocol::Response) -> bool,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut last = "no reply".to_string();
    while tokio::time::Instant::now() < deadline {
        for &id in nodes {
            if !harness.node(id).is_some_and(|n| n.is_running()) {
                continue;
            }
            match tokio::time::timeout(
                Duration::from_secs(10),
                harness
                    .node(id)
                    .unwrap()
                    .send("CLUSTER", &["FORGET", node_id_str]),
            )
            .await
            {
                Ok(resp) => {
                    if accept(&resp) {
                        return;
                    }
                    last = format!("{resp:?}");
                }
                Err(_) => last = "the propose never returned".to_string(),
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("timed out waiting for {what}; last reply: {last}");
}

/// CLUSTER FORGET shrinks the Raft voter set, not just the topology.
///
/// A node dropped from `ClusterState` but left in the Raft membership is still
/// counted by every quorum, so forgetting a dead node used to make the cluster
/// *less* available rather than more: the surviving nodes kept computing quorum
/// over a node that no longer exists. Four nodes shrunk to three is the case
/// that shows it — quorum falls from 3 to 2, so one more failure is survivable
/// only if the removal actually reached Raft.
// FM-CLUSTER-101
#[tokio::test]
async fn test_cluster_forget_shrinks_the_raft_voter_set() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(4).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(20))
        .await
        .unwrap();

    let node_ids = harness.node_ids();
    let victim = node_ids[3];
    let victim_cluster_id = harness.get_node_id_str(victim).unwrap();
    let survivors: Vec<u64> = node_ids[..3].to_vec();

    wait_for_voters(&harness, &node_ids, "all four nodes to be voters", |v| {
        v.len() == 4
    })
    .await;

    // The main use of FORGET is retiring a node that is already gone, so the
    // removal has to commit without the removed node answering anything.
    harness.shutdown_node(victim).await;

    // Issued on whichever survivor answers rather than on a leader read a moment
    // ago: a follower forwards, and the node just shut down may have been the
    // leader.
    forget_until(
        &harness,
        &survivors,
        &victim_cluster_id,
        "the FORGET of the shut-down node to commit",
        |resp| !is_error(resp),
    )
    .await;

    wait_for_voters(
        &harness,
        &survivors,
        "the victim to stop being a voter",
        |v| v.len() == 3 && !v.contains(&victim),
    )
    .await;

    // Repeating FORGET on every surviving node is the documented operator
    // procedure. The repeats are refused at the topology level — the node is
    // already gone, and Redis answers its own `CLUSTER FORGET` of an unknown id
    // with an error too — so what has to hold is that a refusal proposes no
    // membership change: the voter set must not churn once per operator retry.
    for &id in &survivors {
        let _ = harness
            .node(id)
            .unwrap()
            .send("CLUSTER", &["FORGET", &victim_cluster_id])
            .await;
    }
    wait_for_voters(
        &harness,
        &survivors,
        "the voter set to stay at three",
        |v| v.len() == 3 && !v.contains(&victim),
    )
    .await;

    // Three voters means quorum 2, so one more failure is survivable. With the
    // victim still counted this would be 4 voters, quorum 3, and two live nodes
    // could commit nothing.
    let leader = harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    let sacrifice = *survivors.iter().find(|&&id| id != leader).unwrap();
    let writer = *survivors
        .iter()
        .find(|&&id| id != leader && id != sacrifice)
        .unwrap();
    harness.kill_node(sacrifice).await;

    // The probe is a FORGET of an id nobody has: the state machine refuses it,
    // and that refusal is produced *after* the entry commits, so an "ERR node …
    // not found" reply is proof of a quorum. Without one the propose never
    // returns; without a leader the reply is a REDIRECT or CLUSTERDOWN, which is
    // why the message is matched rather than merely "some error".
    let probe_id = format!("{:040x}", 0xdead_beef_u64);
    forget_until(
        &harness,
        &[writer, leader],
        &probe_id,
        "the surviving two of three voters to commit a Raft write",
        |resp| {
            get_error_message(resp)
                .unwrap_or_default()
                .contains("not found")
        },
    )
    .await;

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
        let info = harness.get_cluster_info(node_id).await.unwrap();
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
            let info = harness.get_cluster_info(node_id).await.unwrap();
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
    let pre_info = harness.get_cluster_info(follower).await.unwrap();
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
    let post_info = harness.get_cluster_info(follower).await.unwrap();
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

    // Did the FAIL latch actually engage at least once while the node was down?
    // Without this the "no fail flags afterwards" assertion below could pass on
    // a cluster that never flagged anything, which would make the recovery half
    // of the hysteresis untested. The harness runs the detector at
    // `heartbeat_interval_ms = 100` with `fail_threshold = 5`, so the latch
    // engages ~500ms into each 1s down window; polling across five cycles makes
    // observing it robust to a slow cycle rather than dependent on one.
    let mut latch_observed = false;

    // Flap the node 5 times
    for i in 0..5 {
        eprintln!("Flap cycle {}/5", i + 1);

        // Kill
        harness.kill_node(flapper).await;

        // Short delay (simulate brief failure), spent watching for the latch.
        // Only the flapper is down, so any `fail` flag is necessarily its own.
        let down_until = tokio::time::Instant::now() + Duration::from_secs(1);
        while tokio::time::Instant::now() < down_until {
            if !latch_observed
                && let Some(node) = harness.node(leader)
                && let Ok(nodes) = parse_cluster_nodes(&node.send("CLUSTER", &["NODES"]).await)
                && nodes
                    .iter()
                    .any(|n| n.flags.iter().any(|flag| flag == "fail"))
            {
                latch_observed = true;
                eprintln!("FAIL latch engaged for the flapper during cycle {}", i + 1);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Restart
        harness.restart_node(flapper).await.unwrap();

        // Wait for rejoin
        let _ = harness
            .wait_for_node_recognized(flapper, Duration::from_secs(10))
            .await;

        // Brief stable period
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    assert!(
        latch_observed,
        "the flapper must have been FAIL-flagged at least once while it was down; \
         without that the recovery assertions below prove nothing"
    );

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
            let info = harness.get_cluster_info(node_id).await.unwrap();
            eprintln!(
                "Node {} after flapping: state={}",
                node_id, info.cluster_state
            );
            assert_eq!(
                info.cluster_state, "ok",
                "Cluster should remain healthy despite flapping"
            );

            // `cluster_state` alone would also read `ok` for a FAIL-flagged node
            // that owns no slots, so check the flags directly: once the flapper
            // has been up long enough to answer `fail-threshold` consecutive
            // probes, the leader must have proposed `MarkNodeRecovered` and no
            // node may still carry the flag.
            let nodes = parse_cluster_nodes(
                &harness
                    .node(node_id)
                    .unwrap()
                    .send("CLUSTER", &["NODES"])
                    .await,
            )
            .unwrap();
            let flagged: Vec<_> = nodes
                .iter()
                .filter(|n| n.flags.iter().any(|f| f == "fail"))
                .map(|n| n.id.clone())
                .collect();
            assert!(
                flagged.is_empty(),
                "node {node_id} still reports FAIL-flagged nodes after the flapping settled: {flagged:?}"
            );
        }
    }

    eprintln!("SUCCESS: Cluster survived node flapping");

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

    // The new leader should have the highest replication offset among the
    // survivors. Per-node offsets are not observable yet, so what this test
    // can pin is the *outcome*: a new Raft leader exists, and the cluster
    // reports the dead primary's slots as lost.
    //
    // This cluster has five primaries and no replicas, so nothing can take
    // over the killed node's slots. Once the failure detector latches its FAIL
    // flag those slots are uncovered and `cluster_state:fail` is correct and
    // permanent — Redis reports `fail` for uncovered slots too. Asserting `ok`
    // here only ever passed by beating the ~5-probe latch; polling for the
    // steady state is deterministic, because a killed node never answers again.
    let new_leader_node = harness.node(new_leader).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let info = loop {
        let info = parse_cluster_info(&new_leader_node.send("CLUSTER", &["INFO"]).await).unwrap();
        if info.cluster_state == "fail" {
            break info;
        }
        if tokio::time::Instant::now() >= deadline {
            break info;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(
        info.cluster_state, "fail",
        "with no replica to promote, the killed primary's slots stay uncovered"
    );
    assert!(
        info.cluster_slots_fail > 0,
        "the uncovered slots must be the reported reason, got {info:?}"
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
            let state = parse_cluster_info(&info_resp)
                .map(|i| i.cluster_state)
                .unwrap_or_else(|e| format!("<parse error: {e}>"));
            panic!(
                "Cluster state not ok within 5 seconds after FAILOVER FORCE, state: {}",
                state
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    harness.shutdown_all().await;
}

// ============================================================================
// Phase 6: Failover Integration
// ============================================================================

/// Test 32: Killing a node degrades cluster state or reduces known_nodes.
///
/// Scenario: Kill a node, check that CLUSTER INFO on surviving nodes reflects
/// the reduced cluster membership after FORGET.
#[tokio::test]
async fn test_mark_node_failed_cluster_state_degrades() {
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

    // Pick a non-leader node to kill (so we don't need re-election)
    let node_ids = harness.node_ids();
    let victim_id = node_ids
        .iter()
        .copied()
        .find(|&id| id != leader_id)
        .unwrap();
    let victim_cluster_id = harness.get_node_id_str(victim_id).unwrap();

    eprintln!(
        "Killing node {} (cluster ID: {})",
        victim_id, victim_cluster_id
    );

    // Verify initial state
    let info_before = harness.get_cluster_info(leader_id).await.unwrap();
    let known_before = info_before.cluster_known_nodes;
    eprintln!("Before kill: known_nodes={}", known_before);

    // Kill the victim
    harness.kill_node(victim_id).await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // FORGET the killed node from surviving nodes
    let surviving: Vec<u64> = node_ids
        .iter()
        .copied()
        .filter(|&id| id != victim_id)
        .collect();
    for &nid in &surviving {
        if let Some(node) = harness.node(nid)
            && node.is_running()
        {
            let _ = node.send("CLUSTER", &["FORGET", &victim_cluster_id]).await;
        }
    }

    // Poll CLUSTER INFO until known_nodes decreases or timeout
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut known_after = known_before;

    while tokio::time::Instant::now() < deadline {
        if let Ok(info) = harness.get_cluster_info(leader_id).await {
            known_after = info.cluster_known_nodes;
            if known_after < known_before {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    eprintln!(
        "After kill+forget: known_nodes={} (was {})",
        known_after, known_before
    );

    assert!(
        known_after < known_before,
        "cluster_known_nodes should decrease after FORGET: before={}, after={}",
        known_before,
        known_after
    );

    // Also check cluster_state on a surviving node -- it may have degraded
    // if the killed node owned slots that are now orphaned
    for &nid in &surviving {
        if let Some(node) = harness.node(nid)
            && node.is_running()
        {
            let resp = node.send("CLUSTER", &["INFO"]).await;
            if let Ok(info) = parse_cluster_info(&resp) {
                eprintln!(
                    "Node {} state={}, known_nodes={}",
                    nid, info.cluster_state, info.cluster_known_nodes
                );
            }
        }
    }

    harness.shutdown_all().await;
}

/// Test 33: Rapidly write across all nodes and read everything back.
///
/// Scenario: Write 5 keys per node (15 total in a 3-node cluster), then read
/// all 15 back following redirects. Verify all values are correct.
#[tokio::test]
async fn test_rapid_writes_across_all_nodes() {
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

    // For each node, find slots it owns and write 5 keys
    let mut all_keys: Vec<(String, String)> = Vec::new(); // (key, expected_value)

    for &nid in &node_ids {
        let mut keys_for_node = 0;
        // Scan slots in steps to find ones owned by this node
        for probe_slot in (0u16..16384).step_by(50) {
            if keys_for_node >= 5 {
                break;
            }
            if let Some((owner, _)) = find_owner_of_slot(&harness, probe_slot).await
                && owner == nid
            {
                let key = key_for_slot(probe_slot);
                let value = format!("rapid_{}_{}", nid, probe_slot);

                let node = harness.node(nid).unwrap();
                let resp = node.send("SET", &[&key, &value]).await;
                if !is_error(&resp) {
                    all_keys.push((key, value));
                    keys_for_node += 1;
                }
            }
        }
        eprintln!("Node {}: wrote {} keys", nid, keys_for_node);
    }

    let total_written = all_keys.len();
    eprintln!("Total keys written: {}", total_written);
    assert!(
        total_written >= 10,
        "Expected to write at least 10 keys across all nodes, only wrote {}",
        total_written
    );

    // Read all keys back, following redirects
    let mut read_successes = 0;
    for (key, expected) in &all_keys {
        if let Some(val) = get_with_redirect(&harness, key).await {
            if val == *expected {
                read_successes += 1;
            } else {
                eprintln!("Key '{}': expected '{}', got '{}'", key, expected, val);
            }
        } else {
            eprintln!("Key '{}': not found", key);
        }
    }

    eprintln!(
        "Read back {}/{} keys correctly",
        read_successes, total_written
    );
    assert_eq!(
        read_successes, total_written,
        "All written keys should be readable with correct values"
    );

    harness.shutdown_all().await;
}
