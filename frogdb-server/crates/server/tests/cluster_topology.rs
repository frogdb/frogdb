//! Cluster topology integration tests: formation, node membership, handshake
//! (MEET/FORGET), config-epoch correctness, the CLUSTER command primitives that
//! report or mutate topology (INFO/NODES/MYID/SLOTS/SHARDS/ADDSLOTS/RESET/...),
//! and cluster-state persistence across restarts.
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

use common::cluster_support::{find_owner_of_slot, send_cluster_cmd};
use frogdb_test_harness::cluster_harness::{ClusterNodeConfig, ClusterTestHarness};
use frogdb_test_harness::cluster_helpers::{
    is_error, key_for_slot, master_repl_offset, parse_cluster_info, parse_cluster_nodes,
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

    // ASKING should succeed in cluster mode.
    let response = node.send("ASKING", &[]).await;
    assert!(!is_error(&response));

    harness.shutdown_all().await;
}

/// Redis parity: READONLY / READWRITE / ASKING reject with
/// `-ERR This instance has cluster support disabled` when cluster support is off
/// (standalone mode), matching `readonlyCommand`/`readwriteCommand`/`askingCommand`.
#[tokio::test]
async fn test_readonly_readwrite_asking_error_in_standalone() {
    use crate::common::test_server::TestServer;
    use frogdb_protocol::Response;

    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let expected = "ERR This instance has cluster support disabled";
    for cmd in [["READONLY"], ["READWRITE"], ["ASKING"]] {
        let response = client.command(&cmd).await;
        match response {
            Response::Error(e) => assert_eq!(
                String::from_utf8_lossy(&e),
                expected,
                "{} error message mismatch",
                cmd[0]
            ),
            other => panic!("expected error for {} in standalone, got {other:?}", cmd[0]),
        }
    }

    server.shutdown().await;
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
    let info_before = harness.get_cluster_info(leader).await.unwrap();
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
    let info_after = harness.get_cluster_info(leader).await.unwrap();
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

/// Every `replication-offset` reported by `CLUSTER SHARDS`, keyed by node id.
fn shards_replication_offsets(response: &frogdb_protocol::Response) -> Vec<(String, i64)> {
    let frogdb_protocol::Response::Array(shards) = response else {
        panic!("expected an array from CLUSTER SHARDS, got: {response:?}");
    };
    let mut out = Vec::new();
    for shard in shards {
        let frogdb_protocol::Response::Array(shard_fields) = shard else {
            continue;
        };
        // nodes is at index 3 (after "slots", slots_array, "nodes")
        let Some(frogdb_protocol::Response::Array(nodes)) = shard_fields.get(3) else {
            continue;
        };
        for node_resp in nodes {
            let frogdb_protocol::Response::Array(fields) = node_resp else {
                continue;
            };
            let mut id = None;
            let mut offset = None;
            for pair in fields.chunks(2) {
                match pair {
                    [
                        frogdb_protocol::Response::Bulk(Some(k)),
                        frogdb_protocol::Response::Bulk(Some(v)),
                    ] if k.as_ref() == b"id" => {
                        id = Some(String::from_utf8_lossy(v).to_string());
                    }
                    [
                        frogdb_protocol::Response::Bulk(Some(k)),
                        frogdb_protocol::Response::Integer(n),
                    ] if k.as_ref() == b"replication-offset" => offset = Some(*n),
                    _ => {}
                }
            }
            if let (Some(id), Some(offset)) = (id, offset) {
                out.push((id, offset));
            }
        }
    }
    out
}

/// `CLUSTER SHARDS` reports the node's real data-replication offset — the same
/// number `INFO replication` publishes as `master_repl_offset`.
///
/// It used to report the Raft last-applied log index whenever Raft was running.
/// That was non-zero, so a "non-zero after writes" assertion passed while the
/// field described cluster-metadata entries rather than replicated bytes — it
/// would have been non-zero on a node that had never taken a single write.
/// Equality against `INFO replication` is the assertion that can tell those
/// apart.
#[tokio::test]
async fn test_cluster_shards_reports_real_replication_offset() {
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

    // Write to the node that actually owns the slot, so the offset under test
    // moves rather than the write being redirected away.
    let test_slot = 100u16;
    let (owner_id, _) = find_owner_of_slot(&harness, test_slot)
        .await
        .expect("could not find slot owner");
    let owner_name = harness.get_node_id_str(owner_id).unwrap();

    // Attach a replica: like Redis, the offset only advances once there is a
    // replication stream to advance it.
    let _replica_id = harness.add_replica(owner_id).await.unwrap();
    harness
        .wait_for_replication_link(owner_id, 1, Duration::from_secs(20))
        .await
        .expect("primary never reported an attached replica");
    let owner = harness.node(owner_id).unwrap();

    let tag = format!("{{{}}}", key_for_slot(test_slot));
    for i in 0..10 {
        let k = format!("{tag}_offset_{i}");
        assert!(!is_error(&owner.send("SET", &[&k, "value"]).await));
    }

    let live_offset = master_repl_offset(&owner.send("INFO", &["replication"]).await)
        .expect("master_repl_offset") as i64;
    assert!(
        live_offset > 0,
        "test setup: writes must have advanced the replication offset"
    );

    let offsets = shards_replication_offsets(&owner.send("CLUSTER", &["SHARDS"]).await);
    let mine = offsets
        .iter()
        .find(|(id, _)| *id == owner_name)
        .map(|(_, offset)| *offset)
        .expect("CLUSTER SHARDS must list the node it was asked on");
    assert_eq!(
        mine, live_offset,
        "CLUSTER SHARDS must report the data-replication offset, not the Raft \
         last-applied index; offsets = {offsets:?}"
    );
    assert_eq!(
        offsets.len(),
        1,
        "only the node asked knows its stream position: every peer must omit \
         `replication-offset` rather than render a 0 that reads as maximum lag; \
         offsets = {offsets:?}"
    );

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

/// Tests that the cluster epoch increases after a failover, i.e. after the
/// *topology* actually changes.
///
/// Inspired by Redis `03-failover-loop.tcl` post-conditions.
///
/// The event that moves the epoch is the failure detector committing
/// `MarkNodeFailed` for the killed node (which bumps `config_epoch` inside the
/// same state-machine transition), **not** the Raft election that precedes it.
/// Those used to be indistinguishable: `cluster_current_epoch` was reported as
/// `max(config_epoch, raft_term)`, so the election alone satisfied this
/// assertion and the test would have passed even if no topology command ever
/// committed. Since the fold was removed the test measures what its name claims,
/// which is why the fixed sleep is replaced by a poll: electing a leader is fast,
/// accumulating `fail_threshold` missed probes and replicating the resulting
/// entry is not, and the two are no longer the same event.
///
/// Rebasing the test onto the real event exposed a genuine detector bug (the
/// failure latch is one-shot, so a survivor that crossed the threshold while it
/// was still a follower never propagated the FAIL once it became leader — the
/// exact shape of every leader-death failover). `FailureDetector` now
/// reconciles its local health view into the replicated topology each check
/// interval instead of propagating the transition inline; this test is the
/// end-to-end regression test for that.
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
    let pre_info = parse_cluster_info(&pre_info_resp).unwrap();

    // Kill leader to trigger failover
    harness.kill_node(original_leader).await;

    let _new_leader = harness
        .wait_for_new_leader(original_leader, Duration::from_secs(15))
        .await
        .unwrap();

    // Poll for the replicated epoch bump. The read is taken from the same node
    // throughout: `cluster_current_epoch` is a replicated value, so any node
    // that has caught up reports it, but comparing across nodes would conflate
    // an epoch bump with replication lag.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let post_epoch = loop {
        let info = parse_cluster_info(
            &harness
                .node(non_leader)
                .unwrap()
                .send("CLUSTER", &["INFO"])
                .await,
        )
        .unwrap();
        if info.cluster_current_epoch > pre_info.cluster_current_epoch {
            break info.cluster_current_epoch;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "cluster_current_epoch never moved after the leader was killed: \
             pre={}, last seen={} (raft_term {} -> {}). A bumped term with a \
             flat epoch means the election happened but no topology command \
             committed.",
            pre_info.cluster_current_epoch,
            info.cluster_current_epoch,
            pre_info.cluster_raft_term,
            info.cluster_raft_term
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    assert!(
        post_epoch > pre_info.cluster_current_epoch,
        "Epoch should increase after failover: pre={}, post={}",
        pre_info.cluster_current_epoch,
        post_epoch
    );

    harness.shutdown_all().await;
}

/// Pins the deliberate `CLUSTER INFO` `cluster_current_epoch` vs. `CLUSTER
/// NODES` per-node `config_epoch` relationship after a pure Raft
/// re-election with **no** cluster topology change (no `CLUSTER FAILOVER`,
/// no `MarkNodeFailed`, no `IncrementEpoch` -- this test issues none of
/// them; only a leader kill, which is a Raft-internal event).
///
/// Issue 47: the original audit proposed asserting
/// `INFO epoch <= max(NODES config_epoch)` after such a re-election. **That
/// assertion is wrong and must never be reintroduced.** The counter
/// legitimately runs *above* every per-node epoch — `IncrementEpoch` and
/// `MarkNodeFailed` advance it without assigning it to anybody — so the
/// bound is upside down. Redis has the same shape of divergence:
/// `currentEpoch` may exceed every `configEpoch` after an election that
/// reassigns no slots, and `redis-cli --cluster check`-style tooling flags
/// epoch *collisions* (two nodes sharing a `configEpoch`), never epoch
/// drift/exceedance.
///
/// The relationship that DOES hold, and is pinned here instead, is the
/// reverse bound: `cluster_current_epoch` is structurally `>=` every
/// per-node `config_epoch`, because a node's own `config_epoch` is only ever
/// set to a value minted from that very counter.
///
/// The contract this test pins for a re-election is now the *sharp* one:
/// `cluster_current_epoch` reports the replicated counter directly, so a
/// pure re-election must leave it **unchanged** while `cluster_raft_term`
/// moves. Before the fold was removed the reported epoch tracked
/// `max(counter, raft_term)` and rose on any election, which made
/// `cluster_current_epoch` useless as a topology-change signal — the exact
/// use an operator reaches for.
#[tokio::test]
async fn test_cluster_info_epoch_vs_nodes_epoch_after_reelection_no_topology_change() {
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

    let non_leader = *harness
        .node_ids()
        .iter()
        .find(|&&id| id != original_leader)
        .unwrap();

    // Capture both endpoints from the same node before the re-election. No
    // topology command (CLUSTER FAILOVER / MEET / ADDSLOTS / SET-CONFIG-EPOCH
    // / ...) runs anywhere in this test, so no per-node config_epoch and no
    // cluster-wide config_epoch can change here -- only the Raft term does.
    let pre_info = parse_cluster_info(
        &harness
            .node(non_leader)
            .unwrap()
            .send("CLUSTER", &["INFO"])
            .await,
    )
    .unwrap();
    let pre_nodes = parse_cluster_nodes(
        &harness
            .node(non_leader)
            .unwrap()
            .send("CLUSTER", &["NODES"])
            .await,
    )
    .unwrap();
    let pre_max_node_epoch = pre_nodes.iter().map(|n| n.config_epoch).max().unwrap_or(0);

    assert!(
        pre_info.cluster_current_epoch >= pre_max_node_epoch,
        "deliberate invariant: INFO's cluster_current_epoch must never be \
         less than max(NODES config_epoch); pre INFO={}, pre max NODES={}",
        pre_info.cluster_current_epoch,
        pre_max_node_epoch
    );

    // Trigger a pure Raft re-election by making a follower campaign. Killing
    // the leader would *also* work as an election trigger, but it is not a
    // pure one: the failure detector then commits `MarkNodeFailed`, which is a
    // real topology change that bumps the epoch (see
    // `test_cluster_epoch_increases_after_failover`). Forcing a campaign leaves
    // every node alive and healthy, so the only thing that can move is the
    // term -- which is exactly the case this test exists to pin.
    harness
        .node(non_leader)
        .unwrap()
        .raft()
        .expect("cluster nodes run Raft")
        .trigger()
        .elect()
        .await
        .expect("triggering an election must not fail");

    // Wait for the term to actually advance on the node being measured.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let post_info = loop {
        let info = parse_cluster_info(
            &harness
                .node(non_leader)
                .unwrap()
                .send("CLUSTER", &["INFO"])
                .await,
        )
        .unwrap();
        if info.cluster_raft_term > pre_info.cluster_raft_term {
            break info;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "forced election never raised cluster_raft_term (pre={}, last={})",
            pre_info.cluster_raft_term,
            info.cluster_raft_term
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    let post_nodes = parse_cluster_nodes(
        &harness
            .node(non_leader)
            .unwrap()
            .send("CLUSTER", &["NODES"])
            .await,
    )
    .unwrap();
    let post_max_node_epoch = post_nodes.iter().map(|n| n.config_epoch).max().unwrap_or(0);

    // No topology change occurred: the raw per-node config_epoch bound must
    // be unchanged by the re-election alone.
    assert_eq!(
        post_max_node_epoch, pre_max_node_epoch,
        "a pure Raft re-election with no topology command must not change \
         any per-node config_epoch: pre max={}, post max={}",
        pre_max_node_epoch, post_max_node_epoch
    );

    // The deliberate bound still holds post-election...
    assert!(
        post_info.cluster_current_epoch >= post_max_node_epoch,
        "deliberate invariant: INFO's cluster_current_epoch must never be \
         less than max(NODES config_epoch); post INFO={}, post max NODES={}",
        post_info.cluster_current_epoch,
        post_max_node_epoch
    );

    // ...and the headline contract: the epoch does NOT move on an election.
    // `cluster_current_epoch` is the replicated counter, so it changes only
    // when a topology command commits. This assertion is the inverse of what
    // this test asserted under the fold, where the term was folded in and the
    // reported epoch rose on every election with zero topology change.
    assert_eq!(
        post_info.cluster_current_epoch,
        pre_info.cluster_current_epoch,
        "a pure Raft re-election must leave cluster_current_epoch alone \
         (term {} -> {}): pre={}, post={}",
        pre_info.cluster_raft_term,
        post_info.cluster_raft_term,
        pre_info.cluster_current_epoch,
        post_info.cluster_current_epoch
    );

    harness.shutdown_all().await;
}

/// Monotonicity of `CLUSTER INFO`'s `cluster_current_epoch` across a real
/// epoch-bumping topology event (`CLUSTER FAILOVER`), with the `>=`
/// per-node-epoch bound (see the re-election test above) holding through
/// the transition too.
///
/// Reuses the harness pattern from issue 16's `test_cluster_epoch_persists`:
/// a real primary/replica pair so the replica can run a graceful (non-FORCE)
/// `CLUSTER FAILOVER`, the only path that assigns a node its own nonzero
/// `config_epoch`.
///
/// **`cluster_current_epoch` increases strictly here**, and that is the
/// point. The reported value used to be `max(config_epoch, raft_term)`, and
/// the term dominated on a young cluster -- the first election takes
/// `raft_term` to 1 while `config_epoch` is still 0, so the first failover
/// (`config_epoch` 0 -> 1) left `cluster_current_epoch` at 1 both before and
/// after, masking the topology change entirely. Those were the exact values
/// observed here when this test was written against the fold, and they forced
/// the weaker non-decrease assertion. Now that `cluster_current_epoch` is the
/// replicated counter, a `Failover` bumps it and `CLUSTER INFO` shows it --
/// so `CLUSTER INFO` is finally usable as a topology-change detector, not
/// just `CLUSTER NODES`.
#[tokio::test]
async fn test_cluster_info_epoch_monotonic_across_failover() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(2).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let primary_id = harness.node_ids()[0];
    let replica_id = harness.add_replica(primary_id).await.unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(10))
        .await
        .unwrap();

    let replica = harness.node(replica_id).unwrap();
    let pre_info = parse_cluster_info(&replica.send("CLUSTER", &["INFO"]).await).unwrap();
    let pre_nodes = parse_cluster_nodes(&replica.send("CLUSTER", &["NODES"]).await).unwrap();
    let pre_max_node_epoch = pre_nodes.iter().map(|n| n.config_epoch).max().unwrap_or(0);

    assert!(
        pre_info.cluster_current_epoch >= pre_max_node_epoch,
        "deliberate invariant must hold before the failover too: \
         pre INFO={}, pre max NODES={}",
        pre_info.cluster_current_epoch,
        pre_max_node_epoch
    );

    // Promote the replica: this is the only path (Failover, commands.rs)
    // that sets a node's *own* config_epoch to a nonzero value.
    let failover_resp = replica.send("CLUSTER", &["FAILOVER"]).await;
    assert!(
        !is_error(&failover_resp),
        "CLUSTER FAILOVER on a healthy replica should succeed, got: {:?}",
        failover_resp
    );

    // Wait for the promotion to commit and this node's own config_epoch to
    // reflect it (mirrors test_cluster_epoch_persists's poll).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let nodes = parse_cluster_nodes(&replica.send("CLUSTER", &["NODES"]).await).unwrap();
        let myself = nodes.iter().find(|n| n.is_myself()).unwrap();
        if myself.is_master() && myself.config_epoch > 0 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!(
                "Promoted replica never reported a nonzero config_epoch \
                 (last seen: master={}, config_epoch={})",
                myself.is_master(),
                myself.config_epoch
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let post_info = parse_cluster_info(&replica.send("CLUSTER", &["INFO"]).await).unwrap();
    let post_nodes = parse_cluster_nodes(&replica.send("CLUSTER", &["NODES"]).await).unwrap();
    let post_max_node_epoch = post_nodes.iter().map(|n| n.config_epoch).max().unwrap_or(0);

    // The raw per-node epoch is the signal that actually moves: the failover
    // bumped the promoted node's own config_epoch. This is what an operator
    // (or `redis-cli --cluster check`-style tooling) must read.
    assert!(
        post_max_node_epoch > pre_max_node_epoch,
        "a graceful CLUSTER FAILOVER must bump the promoted node's raw \
         config_epoch: pre max={}, post max={}",
        pre_max_node_epoch,
        post_max_node_epoch
    );

    // The replicated counter moves with the topology event, so the reported
    // epoch increases strictly. `Failover` bumps `config_epoch` inside the
    // same state-machine transition that reassigns the slots, so there is no
    // window in which the promotion is visible but the epoch is not.
    assert!(
        post_info.cluster_current_epoch > pre_info.cluster_current_epoch,
        "INFO's cluster_current_epoch must increase across an epoch-bumping \
         failover: pre={}, post={}",
        pre_info.cluster_current_epoch,
        post_info.cluster_current_epoch
    );

    // The `>=` bound against the per-node epochs holds through the transition
    // too: the promoted node's own epoch was minted from this very counter.
    assert!(
        post_info.cluster_current_epoch >= post_max_node_epoch,
        "deliberate invariant must hold across the epoch-bumping event too: \
         post INFO={}, post max NODES={}",
        post_info.cluster_current_epoch,
        post_max_node_epoch
    );

    harness.shutdown_all().await;
}

/// Every node reports the **same** `cluster_current_epoch`.
///
/// This test is the headline benefit of reporting the replicated counter
/// instead of `max(counter, local raft_term)`, and it could not have been
/// written before: the Raft term is node-local and unreplicated, so a follower
/// that had not seen the latest election — or a node campaigning on its own —
/// reported a different `cluster_current_epoch` than the leader for identical
/// cluster state. Redis's gossip header ratchets `currentEpoch` toward
/// agreement precisely because tooling and operators compare the value across
/// nodes; FrogDB gets the same property for free from Raft, but only once the
/// reported value is the replicated one.
///
/// A forced election runs first so the nodes have genuinely had a term change
/// to disagree about, and the epoch is pushed off zero by a join carrying a
/// large epoch — an all-zero cluster would agree trivially.
///
/// The asserted property is **agreement plus a floor**, not a fixed value: the
/// joiner is a phantom at an address nobody listens on, so the failure detector
/// latches it FAIL within a few seconds and `MarkNodeFailed` bumps the counter
/// past `CLAIMED_EPOCH`. Pinning equality would make the test unsatisfiable the
/// moment the detector wins the race.
#[tokio::test]
async fn test_cluster_current_epoch_agrees_across_all_nodes() {
    const CLAIMED_EPOCH: u64 = 500;
    const JOINER_ID: u64 = 0xC0FFEE;

    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    let leader = harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(10))
        .await
        .unwrap();

    // Push the counter off zero through the ratchet path.
    harness
        .node(leader)
        .unwrap()
        .raft()
        .unwrap()
        .client_write(add_node_claiming(JOINER_ID, 7601, CLAIMED_EPOCH))
        .await
        .expect("uncontested claim must commit");

    // Make one node campaign, so the cluster has a real term change in flight
    // rather than a quiescent, trivially-identical state.
    let follower = *harness.node_ids().iter().find(|&&id| id != leader).unwrap();
    harness
        .node(follower)
        .unwrap()
        .raft()
        .unwrap()
        .trigger()
        .elect()
        .await
        .expect("triggering an election must not fail");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let mut reported = Vec::new();
        for node_id in harness.node_ids() {
            let info = parse_cluster_info(
                &harness
                    .node(node_id)
                    .unwrap()
                    .send("CLUSTER", &["INFO"])
                    .await,
            )
            .unwrap();
            reported.push((node_id, info.cluster_current_epoch, info.cluster_raft_term));
        }

        // Terms may legitimately differ across nodes; epochs may not. The
        // counter must also have ratcheted to at least the claimed epoch — it
        // may sit above it if the detector flagged the phantom joiner first.
        let first_epoch = reported[0].1;
        let agreed = reported.iter().all(|(_, epoch, _)| *epoch == first_epoch);
        if agreed && first_epoch >= CLAIMED_EPOCH {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "nodes never agreed on a cluster_current_epoch >= {CLAIMED_EPOCH}; \
             last seen (node, epoch, term): {reported:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    harness.shutdown_all().await;
}

/// `cluster_current_epoch >= cluster_my_epoch` holds after a node joins
/// carrying an epoch the cluster-wide counter never minted.
///
/// `AddNode` is the only path that introduces an epoch from outside the
/// counter, so it is the only path that could leave a node reporting a
/// `cluster_my_epoch` above `cluster_current_epoch`. The state machine ratchets
/// the counter to any larger uncontested claim (issue 64), which is what keeps
/// the bound true — and the bound now has to be *earned* there, because
/// `CLUSTER INFO` no longer folds in the Raft term, which used to paper over a
/// trailing counter on any cluster whose term happened to be larger.
#[tokio::test]
async fn test_join_with_large_epoch_keeps_current_epoch_above_my_epoch() {
    const CLAIMED_EPOCH: u64 = 900;
    const JOINER_ID: u64 = 0xDECAF;

    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    let leader = harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(10))
        .await
        .unwrap();

    harness
        .node(leader)
        .unwrap()
        .raft()
        .unwrap()
        .client_write(add_node_claiming(JOINER_ID, 7701, CLAIMED_EPOCH))
        .await
        .expect("uncontested claim must commit");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let mut all_ratcheted = true;
        for node_id in harness.node_ids() {
            let node = harness.node(node_id).unwrap();
            // `NODES` first, then `INFO`: the two reads are separate commands
            // and the `AddNode` entry can apply between them. Reading the
            // per-node epochs from the older snapshot and the counter from the
            // newer one can only understate the gap being asserted; the
            // opposite order manufactures a failure out of read skew.
            let nodes = parse_cluster_nodes(&node.send("CLUSTER", &["NODES"]).await).unwrap();
            let info = parse_cluster_info(&node.send("CLUSTER", &["INFO"]).await).unwrap();
            let max_node_epoch = nodes.iter().map(|n| n.config_epoch).max().unwrap_or(0);

            // The invariant under test holds at every instant, converged or not.
            assert!(
                info.cluster_current_epoch >= info.cluster_my_epoch,
                "node {node_id}: cluster_current_epoch ({}) must never trail \
                 cluster_my_epoch ({})",
                info.cluster_current_epoch,
                info.cluster_my_epoch
            );
            assert!(
                info.cluster_current_epoch >= max_node_epoch,
                "node {node_id}: cluster_current_epoch ({}) must never trail \
                 max(NODES config_epoch) ({max_node_epoch})",
                info.cluster_current_epoch
            );

            if info.cluster_current_epoch < CLAIMED_EPOCH {
                all_ratcheted = false;
            }
        }

        if all_ratcheted {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the cluster-wide counter never ratcheted up to the joining node's \
             claimed epoch {CLAIMED_EPOCH}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    harness.shutdown_all().await;
}

/// The test harness's `get_cluster_info` reports what the server reports.
///
/// The harness used to synthesize `ClusterInfo` from the node's in-process
/// `ClusterState` rather than issuing the command, so ~25 tests asserted
/// against a struct the server never produced — and the two disagreed on real
/// fields (`cluster_my_epoch` was the cluster-wide counter, not the node's own
/// epoch). This test pins the harness to the wire format so the shortcut cannot
/// come back.
///
/// The cluster is deliberately *not* left in the shape where every field
/// coincides: a joining replica makes `cluster_size` differ from
/// `cluster_known_nodes`, and a join claiming a large epoch pushes the
/// cluster-wide counter off both zero and every node's own epoch. Comparing
/// fields that are all equal to each other would pass against a synthesized
/// struct — the exact failure mode this test exists to catch.
#[tokio::test]
async fn test_harness_cluster_info_matches_resp_reply() {
    const CLAIMED_EPOCH: u64 = 250;
    const JOINER_ID: u64 = 0xBEEF;
    const JOINER_REPLICA_ID: u64 = 0xBEEF1;

    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    let leader = harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(10))
        .await
        .unwrap();

    let raft = harness.node(leader).unwrap().raft().unwrap().clone();
    // A primary claiming a large epoch: ratchets the counter above every node's
    // own `config_epoch`, so `cluster_my_epoch != cluster_current_epoch`.
    raft.client_write(add_node_claiming(JOINER_ID, 7801, CLAIMED_EPOCH))
        .await
        .expect("uncontested claim must commit");
    // A replica: makes `cluster_size` (primaries) differ from
    // `cluster_known_nodes` (all nodes).
    let mut replica = frogdb_core::cluster::NodeInfo::new_replica(
        JOINER_REPLICA_ID,
        "127.0.0.1:7802".parse().unwrap(),
        "127.0.0.1:17802".parse().unwrap(),
        JOINER_ID,
    );
    replica.config_epoch = CLAIMED_EPOCH;
    raft.client_write(frogdb_core::cluster::ClusterCommand::AddNode { node: replica })
        .await
        .expect("adding a replica must commit");

    // Wait for both joins to be visible everywhere before comparing.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let mut seen = Vec::new();
        for node_id in harness.node_ids() {
            let info = parse_cluster_info(
                &harness
                    .node(node_id)
                    .unwrap()
                    .send("CLUSTER", &["INFO"])
                    .await,
            )
            .unwrap();
            seen.push((node_id, info.cluster_known_nodes, info.cluster_size));
        }
        if seen
            .iter()
            .all(|(_, known, size)| *known == 5 && *size == 4)
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "joins never converged; last seen (node, known_nodes, size): {seen:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    for node_id in harness.node_ids() {
        let from_harness = harness.get_cluster_info(node_id).await.unwrap();
        let from_resp = parse_cluster_info(
            &harness
                .node(node_id)
                .unwrap()
                .send("CLUSTER", &["INFO"])
                .await,
        )
        .unwrap();

        // Non-vacuity: the fields being compared must actually be able to
        // disagree with one another on this cluster.
        assert_ne!(
            from_resp.cluster_size, from_resp.cluster_known_nodes,
            "node {node_id}: test setup failed to separate cluster_size from cluster_known_nodes"
        );
        assert_ne!(
            from_resp.cluster_my_epoch, from_resp.cluster_current_epoch,
            "node {node_id}: test setup failed to separate cluster_my_epoch from the counter"
        );

        // The epoch fields are the ones that used to differ, and the ones every
        // epoch test in this file reads.
        assert_eq!(
            from_harness.cluster_current_epoch, from_resp.cluster_current_epoch,
            "node {node_id}: harness cluster_current_epoch must equal the RESP value"
        );
        assert_eq!(
            from_harness.cluster_my_epoch, from_resp.cluster_my_epoch,
            "node {node_id}: harness cluster_my_epoch must equal the RESP value"
        );
        assert_eq!(
            from_harness.cluster_known_nodes, from_resp.cluster_known_nodes,
            "node {node_id}: harness cluster_known_nodes must equal the RESP value"
        );
        assert_eq!(
            from_harness.cluster_size, from_resp.cluster_size,
            "node {node_id}: harness cluster_size must equal the RESP value"
        );
        assert_eq!(
            from_harness.cluster_state, from_resp.cluster_state,
            "node {node_id}: harness cluster_state must equal the RESP value"
        );
        assert_eq!(
            from_harness.cluster_slots_assigned, from_resp.cluster_slots_assigned,
            "node {node_id}: harness cluster_slots_assigned must equal the RESP value"
        );
        assert_eq!(
            from_harness.cluster_slots_ok, from_resp.cluster_slots_ok,
            "node {node_id}: harness cluster_slots_ok must equal the RESP value"
        );
    }

    harness.shutdown_all().await;
}

// ============================================================================
// config_epoch collision resolution at AddNode (issue 64)
// ============================================================================

/// Build an `AddNode` command for a primary that claims `epoch`, the way a node
/// rejoining with restored on-disk state — or a node from an independently
/// bootstrapped sub-cluster being `CLUSTER MEET`-ed in — reports itself.
fn add_node_claiming(node_id: u64, port: u16, epoch: u64) -> frogdb_core::cluster::ClusterCommand {
    let mut node = frogdb_core::cluster::NodeInfo::new_primary(
        node_id,
        format!("127.0.0.1:{}", port).parse().unwrap(),
        format!("127.0.0.1:{}", port + 10000).parse().unwrap(),
    );
    node.config_epoch = epoch;
    frogdb_core::cluster::ClusterCommand::AddNode { node }
}

/// Injects a genuine `config_epoch` collision and asserts the cluster resolves
/// it instead of recording two primaries at the same epoch.
///
/// `AddNode` is the only path that carries an epoch the cluster-wide counter did
/// not mint, so it is the only path that can introduce a collision. Two
/// synthetic primaries are proposed straight through Raft, both claiming epoch
/// 42: the first is uncontested, the second collides. The expected resolution is
/// that the incumbent keeps 42 and the joiner is admitted with a freshly minted,
/// strictly higher epoch.
///
/// The resolution is computed inside the state machine, so every node derives
/// the same answer from the same log entry — asserted below on all three nodes,
/// which is the property that lets FrogDB skip Redis's gossip tie-break.
#[tokio::test]
async fn test_add_node_epoch_collision_resolved_by_state_machine() {
    const CLAIMED_EPOCH: u64 = 42;
    const INCUMBENT_ID: u64 = 0xA11CE;
    const JOINER_ID: u64 = 0xB0B;

    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    let leader = harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(10))
        .await
        .unwrap();

    let raft = harness.node(leader).unwrap().raft().unwrap().clone();
    raft.client_write(add_node_claiming(INCUMBENT_ID, 7401, CLAIMED_EPOCH))
        .await
        .expect("uncontested claim must commit");
    raft.client_write(add_node_claiming(JOINER_ID, 7402, CLAIMED_EPOCH))
        .await
        .expect("colliding claim must be admitted, not rejected");

    let leader_state = harness.node(leader).unwrap().cluster_state().unwrap();
    let incumbent = leader_state.get_node(INCUMBENT_ID).unwrap();
    let joiner = leader_state.get_node(JOINER_ID).unwrap();

    assert_eq!(
        incumbent.config_epoch, CLAIMED_EPOCH,
        "the incumbent keeps the contested epoch"
    );
    assert_ne!(
        joiner.config_epoch, CLAIMED_EPOCH,
        "the joining node must not be recorded at the colliding epoch"
    );
    assert!(
        joiner.config_epoch > CLAIMED_EPOCH,
        "the minted epoch must exceed every epoch already claimed: got {}",
        joiner.config_epoch
    );
    assert!(
        leader_state.config_epoch() >= joiner.config_epoch,
        "the cluster-wide counter must still dominate every per-node epoch: \
         counter={}, joiner={}",
        leader_state.config_epoch(),
        joiner.config_epoch
    );

    // Every node applies the same log entry through the same transition, so the
    // resolution must be identical everywhere — no node is left believing the
    // joiner still holds 42.
    let resolved = joiner.config_epoch;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    for node_id in harness.node_ids() {
        let cs = harness.node(node_id).unwrap().cluster_state().unwrap();
        loop {
            let seen = cs.get_node(JOINER_ID).map(|n| n.config_epoch);
            if seen == Some(resolved) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "node {} should converge on the resolved epoch {} (saw {:?})",
                node_id,
                resolved,
                seen
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            cs.get_node(INCUMBENT_ID).map(|n| n.config_epoch),
            Some(CLAIMED_EPOCH),
            "node {} should agree the incumbent kept the contested epoch",
            node_id
        );
    }

    // The detection surface (`CLUSTER NODES`, what `frogctl cluster check`
    // reads) now shows no two primaries at the same nonzero epoch.
    let nodes = parse_cluster_nodes(
        &harness
            .node(leader)
            .unwrap()
            .send("CLUSTER", &["NODES"])
            .await,
    )
    .unwrap();
    let mut claimed: Vec<u64> = nodes
        .iter()
        .filter(|n| n.is_master() && n.config_epoch != 0)
        .map(|n| n.config_epoch)
        .collect();
    let total = claimed.len();
    claimed.sort_unstable();
    claimed.dedup();
    assert_eq!(
        claimed.len(),
        total,
        "CLUSTER NODES must report distinct nonzero config epochs per primary, got {:?}",
        nodes
            .iter()
            .map(|n| (n.id.clone(), n.config_epoch))
            .collect::<Vec<_>>()
    );

    harness.shutdown_all().await;
}

/// A node re-registering without an epoch (`NodeInfo::new_primary` starts at 0,
/// which every self-registration and bootstrap seed uses) must not have its
/// recorded `config_epoch` reset — a reset would free the epoch for another node
/// to claim, manufacturing the collision this transition exists to prevent.
#[tokio::test]
async fn test_add_node_without_epoch_preserves_recorded_epoch() {
    const NODE_ID: u64 = 0xC0FFEE;
    const EPOCH: u64 = 11;

    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    let leader = harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(10))
        .await
        .unwrap();

    let raft = harness.node(leader).unwrap().raft().unwrap().clone();
    raft.client_write(add_node_claiming(NODE_ID, 7403, EPOCH))
        .await
        .unwrap();
    // Re-register at a new client address carrying no epoch, exactly as a
    // restarted node's self-registration does.
    raft.client_write(add_node_claiming(NODE_ID, 7404, 0))
        .await
        .unwrap();

    let node = harness
        .node(leader)
        .unwrap()
        .cluster_state()
        .unwrap()
        .get_node(NODE_ID)
        .unwrap();
    assert_eq!(
        node.config_epoch, EPOCH,
        "recorded epoch survives re-register"
    );
    assert_eq!(
        node.addr,
        "127.0.0.1:7404".parse().unwrap(),
        "the rest of the record is still updated"
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

/// CLUSTER INFO's slot-health breakdown is derived, not hardcoded (issue 36).
///
/// `cluster_slots_{ok,pfail,fail}` used to be literals (`slots_ok` was set to
/// `slots_assigned` unconditionally, `pfail`/`fail` to `0`); they are now
/// computed per slot from the owning node's FAIL/PFAIL flags
/// (`commands/cluster/mod.rs::count_slot_health`). This pins the healthy-cluster
/// end of that contract at the wire level:
///
/// - every assigned slot is bucketed exactly once, so
///   `ok + pfail + fail == slots_assigned` (the invariant that makes the
///   breakdown non-misleading);
/// - with no node flagged, all slots are `ok` and both failure buckets are 0.
///
/// The FAIL-flagged and recovery ends are covered deterministically by
/// `test_cluster_asymmetric_partition_false_failover` in `tests/simulation.rs`
/// (assertions 5 and 6), which is the only place a real `MarkNodeFailed` is
/// reachable — the leader-only failure detector is the sole producer of the
/// flag, there is no client/admin command for it. PFAIL has no producer at all
/// in FrogDB today, so it is pinned by unit tests on `count_slot_health`.
#[tokio::test]
async fn test_cluster_info_slot_health_breakdown_healthy_cluster() {
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

        assert_eq!(
            info.cluster_slots_ok + info.cluster_slots_pfail + info.cluster_slots_fail,
            info.cluster_slots_assigned,
            "node {nid}: slots_ok + slots_pfail + slots_fail must account for every \
             assigned slot exactly once"
        );
        assert_eq!(
            info.cluster_slots_ok, info.cluster_slots_assigned,
            "node {nid}: every slot is ok while no node is FAIL/PFAIL flagged"
        );
        assert_eq!(
            info.cluster_slots_fail, 0,
            "node {nid}: no node is FAIL flagged"
        );
        assert_eq!(
            info.cluster_slots_pfail, 0,
            "node {nid}: no node is PFAIL flagged"
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
        let info = harness.get_cluster_info(nid).await.unwrap();
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
///
/// Strengthened per issue 16: the previous version only asserted `post >=
/// pre` on `CLUSTER INFO`'s composite `cluster_current_epoch`. Every
/// restart triggers a fresh Raft election, which bumps `raft_term`
/// regardless of whether the persisted `config_epoch` survived — so that
/// assertion passed even if the on-disk `config_epoch` had reset to 0. This
/// version instead reads the raw per-node `config_epoch` from `CLUSTER
/// NODES` (parse_cluster_nodes) and asserts strict equality across the
/// restart. To make that assertion meaningful (a node that never leaves
/// epoch 0 can't distinguish "persisted" from "reset to 0"), the target
/// node is first promoted via a real `CLUSTER FAILOVER`, which is the only
/// path that bumps a node's own `config_epoch` to a nonzero value.
///
/// Now that `cluster_current_epoch` reports the replicated counter instead of
/// `max(counter, raft_term)`, the `CLUSTER INFO` side is worth asserting too
/// and is checked below. It is asserted as non-regression rather than
/// equality: the restart window is long enough for the surviving peer's
/// failure detector to commit `MarkNodeFailed` for the node being restarted,
/// which legitimately advances the counter. What must never happen is the
/// counter coming back *below* where it was, or below the per-node epoch that
/// demonstrably survived on disk -- either would mean the state machine lost
/// replicated state.
#[tokio::test]
async fn test_cluster_epoch_persists() {
    let config = ClusterNodeConfig {
        persistence: true,
        ..Default::default()
    };
    let mut harness = ClusterTestHarness::with_config(config);
    // Two primaries plus one replica of the first: a real primary/replica
    // pair so the replica can run a graceful (non-FORCE) `CLUSTER FAILOVER`
    // that bumps its own `config_epoch`, without removing any node from the
    // cluster (unlike the FORCE-on-a-primary path, which absorbs/evicts a
    // peer and would confound this persistence-focused test).
    harness.start_cluster(2).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let primary_id = harness.node_ids()[0];
    let replica_id = harness.add_replica(primary_id).await.unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(10))
        .await
        .unwrap();

    // Promote the replica: this is the only path (Failover, commands.rs)
    // that sets a node's *own* config_epoch to a nonzero value.
    let replica = harness.node(replica_id).unwrap();
    let failover_resp = replica.send("CLUSTER", &["FAILOVER"]).await;
    assert!(
        !is_error(&failover_resp),
        "CLUSTER FAILOVER on a healthy replica should succeed, got: {:?}",
        failover_resp
    );

    // Wait for the promotion to commit and this node's own config_epoch to
    // reflect it.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let pre_epoch = loop {
        let nodes = parse_cluster_nodes(&replica.send("CLUSTER", &["NODES"]).await).unwrap();
        let myself = nodes.iter().find(|n| n.is_myself()).unwrap();
        if myself.is_master() && myself.config_epoch > 0 {
            break myself.config_epoch;
        }
        if tokio::time::Instant::now() > deadline {
            panic!(
                "Promoted replica never reported a nonzero config_epoch \
                 (last seen: master={}, config_epoch={})",
                myself.is_master(),
                myself.config_epoch
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(
        pre_epoch > 0,
        "sanity: promotion must produce a nonzero config_epoch to make the \
         persistence check meaningful"
    );

    let pre_info = parse_cluster_info(&replica.send("CLUSTER", &["INFO"]).await).unwrap();

    // Restart the promoted node.
    harness.shutdown_node(replica_id).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    harness.restart_node(replica_id).await.unwrap();
    harness
        .wait_for_node_recognized(replica_id, Duration::from_secs(15))
        .await
        .unwrap();

    // Read the raw per-node config_epoch again, post-restart. This is
    // polled (not a single read): `wait_for_node_recognized` above only
    // confirms that *other* nodes have rediscovered this node -- it says
    // nothing about the restarted node's own Raft replay. `Raft::new()`
    // returns as soon as its background core task is spawned, and that
    // task replays the persisted log (rebuilding `config_epoch` in the
    // fresh state machine) asynchronously; the server can already be
    // answering `CLUSTER NODES` while that replay is still in flight. A
    // single immediate read can therefore observe a transient
    // `config_epoch == 0` on a node that has not finished catching up,
    // which is a test-timing race, not a persistence loss. Poll for the
    // value to converge back to `pre_epoch` (or, if it genuinely never
    // does, fall out of the loop after the deadline so the assertion
    // below still fails loudly on a real regression).
    let replica = harness.node(replica_id).unwrap();
    let post_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut post_epoch;
    loop {
        let post_nodes = parse_cluster_nodes(&replica.send("CLUSTER", &["NODES"]).await).unwrap();
        post_epoch = post_nodes
            .iter()
            .find(|n| n.is_myself())
            .expect("restarted node must still report a myself entry")
            .config_epoch;
        if post_epoch == pre_epoch || tokio::time::Instant::now() > post_deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert_eq!(
        post_epoch, pre_epoch,
        "raw per-node config_epoch must be identical (not merely >=) across \
         a restart: pre={}, post={}",
        pre_epoch, post_epoch
    );

    // The cluster-wide counter, read from the same restarted node. Under the
    // old fold this assertion was vacuous -- the restart's own election bumped
    // `raft_term`, which dominated the reported value whether or not the
    // counter survived.
    let post_info = parse_cluster_info(&replica.send("CLUSTER", &["INFO"]).await).unwrap();
    assert!(
        post_info.cluster_current_epoch >= pre_info.cluster_current_epoch,
        "cluster_current_epoch must not regress across a restart: pre={}, \
         post={} (raft_term {} -> {})",
        pre_info.cluster_current_epoch,
        post_info.cluster_current_epoch,
        pre_info.cluster_raft_term,
        post_info.cluster_raft_term
    );
    assert!(
        post_info.cluster_current_epoch >= post_epoch,
        "the restored counter must still dominate the per-node epoch that \
         survived on disk: counter={}, my epoch={}",
        post_info.cluster_current_epoch,
        post_epoch
    );

    harness.shutdown_all().await;
}
