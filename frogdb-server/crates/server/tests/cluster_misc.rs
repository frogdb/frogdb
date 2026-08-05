//! Cluster integration tests that do not belong to the topology, slot-routing,
//! migration or failover concerns: rolling upgrade / FROGDB FINALIZE, the admin
//! HTTP upgrade-status endpoint, mixed-version INFO gating, WAIT in cluster mode,
//! keyspace notifications and SCAN, and the node-local FLUSHDB/DBSIZE surface.
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
    find_owner_of_slot, get_with_redirect, set_with_redirect, wait_for_data_path_role,
};
use frogdb_test_harness::cluster_harness::{ClusterNodeConfig, ClusterTestHarness};
use frogdb_test_harness::cluster_helpers::{
    connected_slaves, get_error_message, is_error, key_for_slot, parse_cluster_nodes, slot_for_key,
};
use serde::Deserialize;
use std::time::Duration;

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
    // Fake all nodes to version 0.2.0. `fake_all_node_versions` waits for
    // self-registration to settle so the fake is the last writer.
    harness.fake_all_node_versions("0.2.0").await.unwrap();

    // Send FROGDB.FINALIZE to the leader
    let (_, response) = send_to_leader(&harness, "FROGDB.FINALIZE", &["0.2.0"]).await;
    assert!(
        matches!(&response, frogdb_protocol::Response::Simple(s) if s.as_ref() == b"OK"),
        "Expected OK, got {:?}",
        response
    );

    // FINALIZE commits on the leader's state machine before returning OK, but
    // followers apply the replicated entry asynchronously. Poll each node until
    // the finalized version lands rather than betting on a fixed sleep.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    for &node_id in &harness.node_ids() {
        let node = harness.node(node_id).unwrap();
        let cs = node.cluster_state().unwrap();
        while cs.active_version() != Some("0.2.0".to_string()) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "Node {} should have active_version 0.2.0 (got {:?})",
                node_id,
                cs.active_version()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
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
    harness.fake_all_node_versions("0.2.0").await.unwrap();
    harness
        .fake_node_version(node_ids[2], "0.1.0")
        .await
        .unwrap();

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
    harness.fake_all_node_versions("0.2.0").await.unwrap();
    let (_, finalize_resp) = send_to_leader(&harness, "FROGDB.FINALIZE", &["0.2.0"]).await;
    assert!(
        matches!(&finalize_resp, frogdb_protocol::Response::Simple(s) if s.as_ref() == b"OK"),
        "Finalize should succeed, got {:?}",
        finalize_resp
    );

    // FINALIZE may have been served by a different node than `node`, which
    // applies the replicated entry asynchronously — poll instead of sleeping.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let info = parse_version_response(&node.send("FROGDB.VERSION", &[]).await);
        if info.get("active_version").map(|s| s.as_str()) == Some("0.2.0") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "active_version should be 0.2.0 after finalization, got {:?}",
            info.get("active_version")
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

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

    harness.fake_all_node_versions("0.2.0").await.unwrap();

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
    harness.fake_all_node_versions("0.2.0").await.unwrap();
    let node_ids = harness.node_ids();
    harness
        .fake_node_version(node_ids[2], "0.1.0")
        .await
        .unwrap();

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

    harness.fake_all_node_versions("0.2.0").await.unwrap();

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
    harness.fake_all_node_versions("0.2.0").await.unwrap();

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
    harness.fake_all_node_versions("0.2.0").await.unwrap();

    // Finalize
    let (_, response) = admin_send_to_leader(&harness, "FROGDB.FINALIZE", &["0.2.0"]).await;
    assert!(
        matches!(&response, frogdb_protocol::Response::Simple(s) if s.as_ref() == b"OK"),
        "Expected OK, got {:?}",
        response
    );

    // INFO server should now include gated fields. The finalize entry applies
    // asynchronously on followers, so poll node[0]'s INFO until the gated
    // active_version appears rather than sleeping a fixed interval.
    let node_ids = harness.node_ids();
    let node = harness.node(node_ids[0]).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let info = loop {
        let info = parse_info_response(&node.send("INFO", &["server"]).await);
        if info.get("active_version").map(|s| s.as_str()) == Some("0.2.0") {
            break info;
        }
        if tokio::time::Instant::now() >= deadline {
            break info;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

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
    harness.fake_all_node_versions("0.2.0").await.unwrap();
    harness
        .fake_node_version(node_ids[2], "0.1.0")
        .await
        .unwrap();

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
        harness.fake_node_version(node_id, "0.2.0").await.unwrap();

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
        harness.fake_node_version(node_id, "0.2.0").await.unwrap();

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
    harness.fake_all_node_versions("0.2.0").await.unwrap();

    let (_, response) = admin_send_to_leader(&harness, "FROGDB.FINALIZE", &["0.2.0"]).await;
    assert!(
        matches!(&response, frogdb_protocol::Response::Simple(s) if s.as_ref() == b"OK"),
        "Expected OK after upgrade, got {:?}",
        response
    );
    // Verify finalization replicated. Followers apply the committed entry
    // asynchronously, so poll each node rather than betting on a fixed sleep.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    for &node_id in &harness.node_ids() {
        let node = harness.node(node_id).unwrap();
        let cs = node.cluster_state().unwrap();
        while cs.active_version() != Some("0.2.0".to_string()) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "Node {} should have active_version 0.2.0 after finalize (got {:?})",
                node_id,
                cs.active_version()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
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

// ============================================================================
// Phase 5: Replication Edge Cases
// ============================================================================

/// Test 29: Write a key to a slot owned by a follower node.
///
/// Scenario: In cluster mode, slot owners handle their own writes regardless
/// of Raft leadership. Verify a write to a follower-owned slot succeeds.
#[tokio::test]
async fn test_forwarded_write_through_follower() {
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

    // Find a follower node
    let follower_id = harness
        .node_ids()
        .into_iter()
        .find(|&id| id != leader_id)
        .unwrap();

    eprintln!("Leader: {}, Follower: {}", leader_id, follower_id);

    // Find a slot owned by the follower
    let mut follower_slot = None;
    for probe_slot in (0u16..16384).step_by(100) {
        if let Some((owner, _)) = find_owner_of_slot(&harness, probe_slot).await
            && owner == follower_id
        {
            follower_slot = Some(probe_slot);
            break;
        }
    }

    let slot = match follower_slot {
        Some(s) => s,
        None => {
            eprintln!("Could not find a slot owned by the follower, skipping test");
            harness.shutdown_all().await;
            return;
        }
    };

    let key = key_for_slot(slot);
    let value = "follower_owned_value";
    eprintln!(
        "Writing key '{}' to slot {} owned by follower {}",
        key, slot, follower_id
    );

    let follower = harness.node(follower_id).unwrap();
    let set_resp = follower.send("SET", &[&key, value]).await;
    eprintln!("SET response: {:?}", set_resp);

    assert!(
        !is_error(&set_resp),
        "Write to follower-owned slot should succeed, got: {:?}",
        set_resp
    );

    // Read back from the same node
    let get_resp = follower.send("GET", &[&key]).await;
    assert!(
        matches!(&get_resp, frogdb_protocol::Response::Bulk(Some(_))),
        "GET should return the written value, got: {:?}",
        get_resp
    );
    if let frogdb_protocol::Response::Bulk(Some(data)) = &get_resp {
        assert_eq!(
            String::from_utf8_lossy(data),
            value,
            "Value should match what was written"
        );
        eprintln!("SUCCESS: follower-owned slot write+read works");
    }

    harness.shutdown_all().await;
}

/// Test 30: FLUSHDB on one node only clears that node's data.
///
/// Scenario: Write keys to different nodes, FLUSHDB on one, verify others
/// are unaffected. FLUSHDB is per-node, not cluster-wide.
#[tokio::test]
async fn test_cluster_flushdb_on_single_node() {
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

    // Find owners for two different slots on different nodes
    let slot_a = 100u16;
    let (owner_a, _) = find_owner_of_slot(&harness, slot_a)
        .await
        .expect("could not find owner of slot 100");

    // Find a slot owned by a different node
    let mut other_slot = None;
    let mut other_owner = owner_a;
    for alt_slot in [1000u16, 2000, 3000, 5000, 8000, 10000, 12000] {
        if let Some((alt_owner, _)) = find_owner_of_slot(&harness, alt_slot).await
            && alt_owner != owner_a
        {
            other_slot = Some(alt_slot);
            other_owner = alt_owner;
            break;
        }
    }

    let slot_b = match other_slot {
        Some(s) => s,
        None => {
            eprintln!("All probed slots owned by the same node, skipping test");
            harness.shutdown_all().await;
            return;
        }
    };

    let key_a = key_for_slot(slot_a);
    let key_b = key_for_slot(slot_b);

    let node_a = harness.node(owner_a).unwrap();
    let node_b = harness.node(other_owner).unwrap();

    let _ = node_a.send("SET", &[&key_a, "val_a"]).await;
    let _ = node_b.send("SET", &[&key_b, "val_b"]).await;

    eprintln!(
        "Wrote key_a='{}' on node {}, key_b='{}' on node {}",
        key_a, owner_a, key_b, other_owner
    );

    // FLUSHDB on node_a only
    let flush_resp = node_a.send("FLUSHDB", &[]).await;
    eprintln!("FLUSHDB on node {}: {:?}", owner_a, flush_resp);

    // Key on flushed node should be gone
    let get_a = node_a.send("GET", &[&key_a]).await;
    eprintln!("GET key_a after flush: {:?}", get_a);
    assert!(
        matches!(&get_a, frogdb_protocol::Response::Bulk(None)) || is_error(&get_a),
        "Flushed key should be gone, got: {:?}",
        get_a
    );

    // Key on other node should still exist
    let get_b = node_b.send("GET", &[&key_b]).await;
    match &get_b {
        frogdb_protocol::Response::Bulk(Some(data)) => {
            assert_eq!(
                String::from_utf8_lossy(data),
                "val_b",
                "Key on other node should survive FLUSHDB"
            );
            eprintln!("SUCCESS: FLUSHDB is per-node, other node's data intact");
        }
        _ => {
            eprintln!("GET key_b after flush of different node: {:?}", get_b);
        }
    }

    harness.shutdown_all().await;
}

/// Test 31: DBSIZE across all nodes sums to total keys written.
///
/// Scenario: Write 10 keys distributed across nodes, verify total DBSIZE equals 10.
#[tokio::test]
async fn test_cluster_dbsize_after_writes() {
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

    // Write 10 keys to different slots to distribute across nodes
    let num_keys = 10;
    let mut written = 0;
    let slots: Vec<u16> = (0..num_keys).map(|i| (i as u16) * 1000 + 50).collect();

    for &slot in &slots {
        let key = key_for_slot(slot);
        let value = format!("dbsize_val_{}", slot);
        if set_with_redirect(&harness, &key, &value).await {
            written += 1;
        } else {
            eprintln!("Failed to write key for slot {}", slot);
        }
    }

    eprintln!("Successfully wrote {} / {} keys", written, num_keys);

    // Query DBSIZE on each node and sum
    let mut total_dbsize: i64 = 0;
    for &node_id in &harness.node_ids() {
        if let Some(node) = harness.node(node_id)
            && node.is_running()
        {
            let resp = node.send("DBSIZE", &[]).await;
            match &resp {
                frogdb_protocol::Response::Integer(n) => {
                    eprintln!("Node {} DBSIZE: {}", node_id, n);
                    total_dbsize += n;
                }
                _ => {
                    eprintln!("Node {} DBSIZE unexpected response: {:?}", node_id, resp);
                }
            }
        }
    }

    eprintln!(
        "Total DBSIZE across all nodes: {}, keys written: {}",
        total_dbsize, written
    );
    assert_eq!(
        total_dbsize, written as i64,
        "Total DBSIZE across all nodes should equal number of keys written"
    );

    harness.shutdown_all().await;
}

// ============================================================================
// WAIT in cluster mode (testing-gap issue 37)
// ============================================================================
//
// Pinned contract (see the issue's `## Resolution` and the WAIT-cluster-mode
// PRD for the full write-up):
//
// WAIT has no cluster-specific path. A cluster replica attaches over the same
// PSYNC link, into the same `ReplicationTrackerImpl`, as a standalone one —
// Raft carries cluster metadata only (ADR-0001), never key data — so the
// connection-level `WaitCoordinator`
// (`server/src/connection/blocking.rs::handle_wait_command`) counts the same
// thing in both modes: the replicas attached to *this* node.
//
// Consequences, all pinned below:
//   * WAIT counts this node's own replicas and blocks up to `timeout` for them,
//     exactly as in standalone. `numreplicas 0` returns the live count
//     immediately; an unreachable `numreplicas` blocks to the deadline and then
//     returns what it has (Redis parity, deliberately not Dragonfly's
//     early-exit).
//   * WAIT never counts another shard's replicas: a node's PSYNC replicas are
//     its own shard's replica set, which is what makes Redis's per-shard
//     contract hold here without per-shard bookkeeping.
//   * WAIT takes no key, so it never redirects — a node owning no slots still
//     answers with an integer, never MOVED.
//   * A cluster *replica* node carries the data-path `is_replica` flag, so WAIT
//     is rejected there with the standard replica error (before arg parsing).
//     A node promoted by a cluster failover installs the primary role on its
//     data path, so it *serves* WAIT from that point on.
//   * A slot-migration target is not a replica (migration moves keys with
//     `MIGRATE`, not PSYNC), so it never counts toward the quorum.
//
// The one divergence from Redis is inherited from standalone and unrelated to
// clustering: the coordinator waits on the node-global replication offset
// rather than the connection's own `c->woff`
// (`replication/src/wait_coordinator.rs`), which Dragonfly does too and which
// can only over-wait, never under-wait.
//
// Cluster-*wide* durability is a client concern: fan the WAIT out to every
// primary and take the minimum. FrogDB does not fan out server-side.

/// Extract the integer from a `WAIT` reply, or panic with the actual response.
fn wait_reply_count(resp: &frogdb_protocol::Response) -> i64 {
    match resp {
        frogdb_protocol::Response::Integer(n) => *n,
        other => panic!("expected WAIT to reply with an integer, got {other:?}"),
    }
}

// FM-REPLICATION-039
/// WAIT on a cluster primary counts the replicas attached to that primary's
/// shard, blocking until they ACK the write under test.
///
/// Acceptance criteria (issue 37):
///   * the ack count matches the actual replica set of the contacted node;
///   * `WAIT 0` returns the live count immediately, without blocking;
///   * a satisfiable `numreplicas` resolves well inside its timeout.
#[tokio::test]
async fn test_wait_in_cluster_counts_shard_replicas() {
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

    // Attach a real cluster replica to the first primary's shard, and wait for
    // the PSYNC link itself rather than for cluster-state convergence: the link
    // is what WAIT counts.
    let primary_id = harness.node_ids()[0];
    let _replica_id = harness.add_replica(primary_id).await.unwrap();
    harness
        .wait_for_replication_link(primary_id, 1, Duration::from_secs(20))
        .await
        .expect("primary never reported an attached replica");

    // Write a key into a slot this primary owns (no MOVED).
    let primary = harness.node(primary_id).unwrap();
    let nodes = parse_cluster_nodes(&primary.send("CLUSTER", &["NODES"]).await).unwrap();
    let myself = nodes.iter().find(|n| n.is_myself()).unwrap();
    assert!(
        myself.is_master() && !myself.slots.is_empty(),
        "test setup: node_ids()[0] must be a slot-owning primary, got {:?}",
        myself.flags
    );
    let key = key_for_slot(myself.slots[0].0);
    assert!(
        !is_error(&primary.send("SET", &[&key, "wait-cluster"]).await),
        "SET on the owning primary should succeed"
    );

    // The replica must ACK the write, and do so well inside the timeout.
    let start = std::time::Instant::now();
    let resp = primary.send("WAIT", &["1", "5000"]).await;
    let elapsed = start.elapsed();
    assert_eq!(
        wait_reply_count(&resp),
        1,
        "cluster WAIT must count the shard's attached replica, got {resp:?}"
    );
    assert!(
        elapsed < Duration::from_millis(2500),
        "a satisfiable cluster WAIT must resolve on the ACK, not on the \
         deadline; took {elapsed:?}"
    );

    // `numreplicas 0` reports the live count without blocking at all.
    let start = std::time::Instant::now();
    let resp = primary.send("WAIT", &["0", "1000"]).await;
    let elapsed = start.elapsed();
    assert_eq!(
        wait_reply_count(&resp),
        1,
        "WAIT 0 must report the live replica count, got {resp:?}"
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "WAIT 0 must not block; took {elapsed:?}"
    );

    harness.shutdown_all().await;
}

// FM-REPLICATION-040
/// WAIT on a cluster *replica* node is rejected with the replica error, before
/// argument parsing — the data-path `is_replica` flag gates it exactly as in
/// standalone replica mode.
#[tokio::test]
async fn test_wait_rejected_on_cluster_replica() {
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
    // Even with obviously-invalid args, the replica rejection wins first.
    let resp = replica.send("WAIT", &["0", "100"]).await;
    assert!(
        is_error(&resp),
        "WAIT on a cluster replica must be rejected, got {resp:?}"
    );
    let msg = get_error_message(&resp).unwrap_or("");
    assert!(
        msg.contains("replica"),
        "WAIT-on-replica error should mention 'replica', got: {msg}"
    );

    harness.shutdown_all().await;
}

// FM-REPLICATION-040
/// A promoted node *serves* WAIT: the failover moves the WAIT surface along
/// with the primary role, rather than leaving a node that advertises `master`
/// while still rejecting WAIT as a replica.
///
/// This is the tripwire the original issue-37 test was built to be. It pinned
/// the opposite — a promoted node kept its data-path replica flag and kept
/// rejecting WAIT — with an explicit note to flip it once promotions installed
/// the data-path role. They do now (see
/// `test_cluster_failover_promotes_the_data_path`), so this asserts the served
/// contract, plus the safety property the old test guarded: WAIT resolves
/// within its timeout on both the promoted node and the surviving sibling.
///
/// `CLUSTER FAILOVER TAKEOVER` drives the promotion because auto-failover is
/// off by default, so killing the primary alone changes no cluster state.
#[tokio::test]
async fn test_wait_is_served_by_a_promoted_cluster_primary() {
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
        .wait_for_replication_link(primary_id, 1, Duration::from_secs(20))
        .await
        .expect("primary never reported an attached replica");

    // Write into a slot the outgoing primary owns.
    let primary = harness.node(primary_id).unwrap();
    let nodes = parse_cluster_nodes(&primary.send("CLUSTER", &["NODES"]).await).unwrap();
    let myself = nodes.iter().find(|n| n.is_myself()).unwrap();
    let key = key_for_slot(myself.slots[0].0);
    let _ = primary.send("SET", &[&key, "pre-failover"]).await;

    // Before the promotion, WAIT on the replica is rejected.
    let replica = harness.node(replica_id).unwrap();
    assert!(
        is_error(&replica.send("WAIT", &["0", "100"]).await),
        "a cluster replica must reject WAIT before the promotion"
    );

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

    // WAIT is now served, not rejected. The promoted node has no replicas of
    // its own, so `WAIT 0` reports 0 — an honest count, not a stub.
    let promoted = harness.node(replica_id).unwrap();
    let start = std::time::Instant::now();
    let resp = promoted.send("WAIT", &["0", "1500"]).await;
    let elapsed = start.elapsed();
    assert_eq!(
        wait_reply_count(&resp),
        0,
        "a promoted primary with no replicas must report 0, served not rejected; got {resp:?}"
    );
    assert!(
        elapsed < Duration::from_millis(750),
        "WAIT 0 on the promoted node must not block; took {elapsed:?}"
    );

    // And an unsatisfiable WAIT still resolves at its own deadline rather than
    // hanging — the failover-safety property the original test guarded.
    let start = std::time::Instant::now();
    let resp = promoted.send("WAIT", &["1", "800"]).await;
    let elapsed = start.elapsed();
    assert_eq!(
        wait_reply_count(&resp),
        0,
        "an unreachable quorum returns the count it has, got {resp:?}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "WAIT must never outlive its timeout by more than scheduling slack; took {elapsed:?}"
    );

    harness.shutdown_all().await;
}

// FM-REPLICATION-037
/// A `numreplicas` larger than the attached replica set blocks to the deadline
/// and then returns the count it actually has (Redis parity).
///
/// Deliberately *not* Dragonfly's early exit: a replica may be mid-attach when
/// WAIT starts, so returning early on "every currently-tracked replica has
/// acked" would answer a question the client did not ask. The lower bound on
/// elapsed time is the assertion that matters.
#[tokio::test]
async fn test_wait_numreplicas_exceeds_actual_blocks_to_timeout() {
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
    let _replica_id = harness.add_replica(primary_id).await.unwrap();
    harness
        .wait_for_replication_link(primary_id, 1, Duration::from_secs(20))
        .await
        .expect("primary never reported an attached replica");

    let primary = harness.node(primary_id).unwrap();
    let nodes = parse_cluster_nodes(&primary.send("CLUSTER", &["NODES"]).await).unwrap();
    let myself = nodes.iter().find(|n| n.is_myself()).unwrap();
    let key = key_for_slot(myself.slots[0].0);
    assert!(!is_error(
        &primary.send("SET", &[&key, "unreachable"]).await
    ));

    let start = std::time::Instant::now();
    let resp = primary.send("WAIT", &["3", "500"]).await;
    let elapsed = start.elapsed();
    assert_eq!(
        wait_reply_count(&resp),
        1,
        "an unreachable quorum returns the replicas that did ack, got {resp:?}"
    );
    assert!(
        elapsed >= Duration::from_millis(450),
        "WAIT must block toward its deadline rather than early-exiting; took {elapsed:?}"
    );

    harness.shutdown_all().await;
}

// FM-REPLICATION-039
/// WAIT counts *this* node's replicas, never the whole cluster's.
///
/// Two shards, one replica each: WAIT on shard A's primary must answer 1, not
/// 2. This is the per-shard property Redis documents; FrogDB gets it for free
/// because a node's PSYNC replicas are exactly its own shard's replica set.
#[tokio::test]
async fn test_wait_does_not_count_other_shards_replicas() {
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

    let primary_ids = harness.node_ids();
    let (shard_a, shard_b) = (primary_ids[0], primary_ids[1]);
    let _ = harness.add_replica(shard_a).await.unwrap();
    let _ = harness.add_replica(shard_b).await.unwrap();
    harness
        .wait_for_replication_link(shard_a, 1, Duration::from_secs(20))
        .await
        .expect("shard A never reported an attached replica");
    harness
        .wait_for_replication_link(shard_b, 1, Duration::from_secs(20))
        .await
        .expect("shard B never reported an attached replica");

    for id in [shard_a, shard_b] {
        let node = harness.node(id).unwrap();
        let nodes = parse_cluster_nodes(&node.send("CLUSTER", &["NODES"]).await).unwrap();
        let myself = nodes.iter().find(|n| n.is_myself()).unwrap();
        let key = key_for_slot(myself.slots[0].0);
        assert!(!is_error(&node.send("SET", &[&key, "per-shard"]).await));

        let resp = node.send("WAIT", &["1", "5000"]).await;
        assert_eq!(
            wait_reply_count(&resp),
            1,
            "node {id} must count its own single replica, not the cluster's two; got {resp:?}"
        );
    }

    harness.shutdown_all().await;
}

// FM-REPLICATION-037
/// WAIT takes no key, so it never redirects.
///
/// The control is the same connection redirecting a *keyed* command in the same
/// breath: that proves the node's redirect path is live and that WAIT's integer
/// reply is an exemption rather than an accident of topology.
#[tokio::test]
async fn test_wait_never_redirects_in_cluster() {
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

    // Find a slot this node does *not* own, so a keyed command against it must
    // be redirected.
    let node_id = harness.node_ids()[0];
    let node = harness.node(node_id).unwrap();
    let nodes = parse_cluster_nodes(&node.send("CLUSTER", &["NODES"]).await).unwrap();
    let myself = nodes.iter().find(|n| n.is_myself()).unwrap();
    let foreign_slot = (0..16384u16)
        .find(|slot| !myself.slots.iter().any(|(lo, hi)| slot >= lo && slot <= hi))
        .expect("a 2-node cluster must leave some slot to another owner");

    let redirected = node.send("GET", &[&key_for_slot(foreign_slot)]).await;
    assert!(
        get_error_message(&redirected)
            .unwrap_or_default()
            .starts_with("MOVED"),
        "control: a keyed command for a foreign slot must be redirected, got {redirected:?}"
    );

    // Same connection, same topology: WAIT answers with an integer.
    let resp = node.send("WAIT", &["0", "500"]).await;
    assert_eq!(
        wait_reply_count(&resp),
        0,
        "WAIT is keyless and must answer with an integer, never MOVED, got {resp:?}"
    );

    harness.shutdown_all().await;
}

// FM-REPLICATION-039
/// A slot-migration target does not count as a replica.
///
/// Migration moves keys with `MIGRATE` (a client-level command), not PSYNC, so
/// the importing node never enters the source's streaming-replica set. Pinning
/// this keeps a future migration implementation from quietly inflating WAIT's
/// answer with a node that holds only part of the keyspace.
#[tokio::test]
async fn test_wait_ignores_slot_migration_target() {
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

    let ids = harness.node_ids();
    let (source_id, target_id) = (ids[0], ids[1]);
    let source = harness.node(source_id).unwrap();
    let nodes = parse_cluster_nodes(&source.send("CLUSTER", &["NODES"]).await).unwrap();
    let myself = nodes.iter().find(|n| n.is_myself()).unwrap();
    let slot = myself.slots[0].0;

    let target_name = harness.get_node_id_str(target_id).unwrap();
    let source_name = harness.get_node_id_str(source_id).unwrap();
    let resp = source
        .send(
            "CLUSTER",
            &["SETSLOT", &slot.to_string(), "MIGRATING", &target_name],
        )
        .await;
    assert!(!is_error(&resp), "SETSLOT MIGRATING failed: {resp:?}");
    let resp = harness
        .node(target_id)
        .unwrap()
        .send(
            "CLUSTER",
            &["SETSLOT", &slot.to_string(), "IMPORTING", &source_name],
        )
        .await;
    assert!(!is_error(&resp), "SETSLOT IMPORTING failed: {resp:?}");

    let source = harness.node(source_id).unwrap();
    let resp = source.send("WAIT", &["0", "500"]).await;
    assert_eq!(
        wait_reply_count(&resp),
        0,
        "a migration target is not a replica and must not count toward WAIT, got {resp:?}"
    );

    let _ = source
        .send("CLUSTER", &["SETSLOT", &slot.to_string(), "STABLE"])
        .await;
    let _ = harness
        .node(target_id)
        .unwrap()
        .send("CLUSTER", &["SETSLOT", &slot.to_string(), "STABLE"])
        .await;

    harness.shutdown_all().await;
}

// FM-REPLICATION-040
/// A WAIT parked on a cluster primary is released with `-UNBLOCKED` when the
/// cluster demotes that node.
///
/// The cluster path reaches the same seam as a standalone `REPLICAOF`: the
/// committed `SetRole { Replica }` becomes a `RoleChangeEvent::Demoted`, which
/// drives `RoleManager::demote` -> `end_primary_stint`, which releases the
/// parked waits alongside the downstream disconnect. Pinned separately from the
/// standalone case because the trigger, not the mechanism, is what differs.
#[tokio::test]
async fn test_wait_unblocked_on_cluster_demotion() {
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

    let ids = harness.node_ids();
    let (demoted_id, survivor_id) = (ids[0], ids[1]);
    let survivor_name = harness.get_node_id_str(survivor_id).unwrap();

    let node = harness.node(demoted_id).unwrap();
    let mut blocked = node.connect().await;
    blocked.send_only(&["WAIT", "5", "0"]).await;
    assert!(
        blocked
            .read_response(Duration::from_millis(500))
            .await
            .is_none(),
        "WAIT 5 0 on a replica-less cluster primary must park"
    );

    let resp = node
        .try_send_admin_aware("CLUSTER", &["REPLICATE", &survivor_name])
        .await
        .unwrap();
    assert!(!is_error(&resp), "CLUSTER REPLICATE failed: {resp:?}");
    assert_eq!(
        wait_for_data_path_role(&harness, demoted_id, "slave", Duration::from_secs(20)).await,
        "slave",
        "the node must actually be demoted on the data path"
    );

    let resp = blocked
        .read_response(Duration::from_secs(5))
        .await
        .expect("a cluster demotion must release the parked WAIT");
    let msg = get_error_message(&resp).unwrap_or_default();
    assert!(
        msg.starts_with("UNBLOCKED") && msg.contains("master -> replica"),
        "a WAIT released by a cluster demotion must be the -UNBLOCKED error, got: {resp:?}"
    );

    harness.shutdown_all().await;
}

// FM-REPLICATION-041
/// A cluster primary with zero replicas still accepts writes.
///
/// Guard for the risk that building the primary-side replication seams on every
/// cluster node silently turns on the self-fence / `min-replicas-to-write`
/// machinery. Those stay gated on their own config flags, which default off, so
/// a replica-less cluster primary must behave exactly as before.
#[tokio::test]
async fn test_cluster_primary_writes_without_replicas() {
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
    let primary = harness.node(primary_id).unwrap();
    let nodes = parse_cluster_nodes(&primary.send("CLUSTER", &["NODES"]).await).unwrap();
    let myself = nodes.iter().find(|n| n.is_myself()).unwrap();
    let key = key_for_slot(myself.slots[0].0);

    assert_eq!(
        connected_slaves(&primary.send("INFO", &["replication"]).await),
        Some(0),
        "test setup: this primary must have no replicas"
    );

    for i in 0..5 {
        let resp = primary.send("SET", &[&key, &format!("v{i}")]).await;
        assert!(
            !is_error(&resp),
            "a replica-less cluster primary must accept writes, got {resp:?}"
        );
    }
    let resp = primary.send("GET", &[&key]).await;
    assert!(
        matches!(&resp, frogdb_protocol::Response::Bulk(Some(v)) if v.as_ref() == b"v4"),
        "the write must be readable back, got {resp:?}"
    );

    harness.shutdown_all().await;
}

// ============================================================================
// Issue 58: keyspace notifications + SCAN in cluster mode
// ============================================================================
//
// Pinned contract (also documented in the architecture "Node-Local Surfaces"
// section of website/src/content/docs/architecture/clustering.md):
//
//   * **Keyspace notifications are node-local.** A write is executed by the
//     slot-owning primary, and that node's shard worker publishes the
//     `__keyspace@0__:<key>` / `__keyevent@0__:<event>` messages straight into
//     its own subscription table
//     (`core/src/shard/keyspace_notify.rs::emit_keyspace_notification` ->
//     `KeyspaceNotificationCoordinator::publish`). That path never touches
//     `ClusterPubSubForwarder`, so the event does not cross the cluster bus:
//     only clients subscribed *on the owning primary* observe it. This matches
//     Redis, where `notifyKeyspaceEvent` calls `pubsubPublishMessage` directly
//     while only the `PUBLISH` command calls `clusterPropagatePublish`.
//     Consequence for clients: a cluster-wide keyspace-notification consumer
//     must hold one subscription per primary.
//
//   * **SCAN is per-node.** `SCAN` is a server-wide scatter over the node's own
//     internal shards (`server/src/connection/scatter.rs::handle_scan`) with no
//     cluster fan-out and no slot filtering — a node simply has no keys outside
//     the slots it owns. A full-keyspace scan therefore requires client-side
//     fan-out across every primary plus a union of the results. The union is
//     duplicate-free because slot ownership is exclusive, which also means no
//     key ever "bleeds" into a non-owning node's scan.

/// Slot ranges each harness node owns, per `CLUSTER NODES`.
///
/// Returned in `harness.node_ids()` order; nodes owning no slots are omitted.
///
/// Polls until the reported ranges cover the whole ring. `wait_for_cluster_convergence`
/// only establishes that the nodes agree on membership — the bootstrap's slot
/// assignment can still be mid-propagation, so a single read of `CLUSTER NODES`
/// can show a partial ring (e.g. `[0,5460]` and `[10922,16383]` with the middle
/// third missing). Callers resolve arbitrary slots through `owner_of_slot`, so a
/// partial ring makes them fail on whichever slot lands in the hole.
async fn slot_ownership(harness: &ClusterTestHarness) -> Vec<(u64, Vec<(u16, u16)>)> {
    let node_ids = harness.node_ids();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

    loop {
        let first = harness.node(node_ids[0]).expect("node 0 must exist");
        let nodes = parse_cluster_nodes(&first.send("CLUSTER", &["NODES"]).await)
            .expect("CLUSTER NODES should parse");
        let mut ownership = Vec::new();
        for &id in &node_ids {
            let addr = harness.node(id).expect("node must exist").client_addr();
            if let Some(info) = nodes.iter().find(|n| n.addr == addr)
                && !info.slots.is_empty()
            {
                ownership.push((id, info.slots.clone()));
            }
        }

        let covered: u32 = ownership
            .iter()
            .flat_map(|(_, ranges)| ranges.iter())
            .map(|(s, e)| u32::from(*e) - u32::from(*s) + 1)
            .sum();
        if covered == 16384 {
            return ownership;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "slot assignment never covered the full ring; got {covered}/16384 in {ownership:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The harness node id owning `slot`, if any.
fn owner_of_slot(ownership: &[(u64, Vec<(u16, u16)>)], slot: u16) -> Option<u64> {
    ownership
        .iter()
        .find(|(_, ranges)| ranges.iter().any(|(s, e)| slot >= *s && slot <= *e))
        .map(|(id, _)| *id)
}

/// Drive `SCAN` to completion on one connection.
///
/// Returns every key yielded, in yield order and with duplicates preserved, so
/// callers can assert on duplicates rather than have them silently folded away.
async fn scan_to_completion(client: &mut frogdb_test_harness::server::TestClient) -> Vec<String> {
    let mut cursor = "0".to_string();
    let mut keys = Vec::new();
    // Bounded so a cursor that never returns to 0 fails loudly rather than
    // hanging the test. 48 keys at COUNT 64 needs a handful of round trips.
    for _ in 0..1000 {
        let resp = client.command(&["SCAN", &cursor, "COUNT", "64"]).await;
        let frogdb_protocol::Response::Array(parts) = &resp else {
            panic!("SCAN should reply with a 2-element array, got {resp:?}");
        };
        assert_eq!(parts.len(), 2, "SCAN reply shape: {resp:?}");
        let frogdb_protocol::Response::Bulk(Some(next)) = &parts[0] else {
            panic!("SCAN cursor should be a bulk string, got {:?}", parts[0]);
        };
        let frogdb_protocol::Response::Array(batch) = &parts[1] else {
            panic!("SCAN keys should be an array, got {:?}", parts[1]);
        };
        for entry in batch {
            match entry {
                frogdb_protocol::Response::Bulk(Some(k)) => {
                    keys.push(String::from_utf8_lossy(k).to_string())
                }
                other => panic!("SCAN key should be a bulk string, got {other:?}"),
            }
        }
        cursor = String::from_utf8_lossy(next).to_string();
        if cursor == "0" {
            return keys;
        }
    }
    panic!("SCAN never returned to cursor 0");
}

/// Read one pub/sub `message` frame as `(channel, payload)`.
fn expect_pubsub_message(msg: Option<frogdb_protocol::Response>) -> (String, String) {
    let Some(frogdb_protocol::Response::Array(arr)) = msg else {
        panic!("expected a pub/sub message array, got {msg:?}");
    };
    assert_eq!(arr.len(), 3, "pub/sub message shape: {arr:?}");
    assert_eq!(
        arr[0],
        frogdb_protocol::Response::Bulk(Some(bytes::Bytes::from_static(b"message"))),
        "expected a `message` push, got {arr:?}"
    );
    let field = |r: &frogdb_protocol::Response| match r {
        frogdb_protocol::Response::Bulk(Some(b)) => String::from_utf8_lossy(b).to_string(),
        other => panic!("expected a bulk string, got {other:?}"),
    };
    (field(&arr[1]), field(&arr[2]))
}

/// A keyspace notification for a key write fires only on the slot-owning
/// primary: subscribers attached to the other primaries never see it, even
/// though ordinary `PUBLISH` on the same channel does reach them.
///
/// Acceptance criteria (issue 58): the event is observed exactly once, only via
/// the owning primary's subscription, and not via any other primary's.
///
/// The trailing `PUBLISH` is the control that makes the negative assertion
/// meaningful — it proves each non-owner subscription was live and that
/// cross-node broadcast delivery works on this cluster, so the silence above it
/// is genuinely "keyspace notifications do not cross the bus" and not a
/// mis-registered subscriber.
#[tokio::test]
async fn test_cluster_keyspace_notification_fires_only_on_owning_primary() {
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

    let ownership = slot_ownership(&harness).await;
    assert!(
        ownership.len() >= 2,
        "test needs at least two slot-owning primaries, got {ownership:?}"
    );

    // A key whose slot is owned by a known node; every other node is a
    // non-owner for it by construction (slot ownership is exclusive).
    let owner_id = ownership[0].0;
    let slot = ownership[0].1[0].0;
    let key = key_for_slot(slot);
    assert_eq!(slot_for_key(key.as_bytes()), slot);

    // Notifications are configured per node (there is no cluster-wide CONFIG
    // propagation), so enable them everywhere: a node that stayed silent purely
    // because its own flags were unset would not prove anything.
    for &id in &harness.node_ids() {
        let resp = harness
            .node(id)
            .unwrap()
            .send("CONFIG", &["SET", "notify-keyspace-events", "KEA"])
            .await;
        assert!(
            !is_error(&resp),
            "CONFIG SET notify-keyspace-events on node {id}: {resp:?}"
        );
    }

    // One long-lived subscriber per node, on both the keyspace and the keyevent
    // channel for this write.
    let keyspace_channel = format!("__keyspace@0__:{key}");
    let keyevent_channel = "__keyevent@0__:set".to_string();
    let mut subscribers = Vec::new();
    for &id in &harness.node_ids() {
        let mut client = harness.node(id).unwrap().connect().await;
        for channel in [keyspace_channel.as_str(), keyevent_channel.as_str()] {
            let resp = client.command(&["SUBSCRIBE", channel]).await;
            assert!(
                matches!(&resp, frogdb_protocol::Response::Array(a) if a.len() == 3),
                "SUBSCRIBE {channel} on node {id}: {resp:?}"
            );
        }
        subscribers.push((id, client));
    }
    // Subscriptions register on the broadcast coordinator shard asynchronously.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Write on the owning primary — no MOVED, this node owns the slot.
    let resp = harness
        .node(owner_id)
        .unwrap()
        .send("SET", &[&key, "notify-me"])
        .await;
    assert!(
        !is_error(&resp),
        "SET {key} on its owning primary {owner_id} should succeed, got {resp:?}"
    );

    // The owner's subscriber sees exactly the two events for this write.
    let owner_index = subscribers
        .iter()
        .position(|(id, _)| *id == owner_id)
        .expect("owner must have a subscriber");
    let mut observed = Vec::new();
    for _ in 0..2 {
        let msg = subscribers[owner_index]
            .1
            .read_message(Duration::from_secs(2))
            .await;
        observed.push(expect_pubsub_message(msg));
    }
    observed.sort();
    let mut want = vec![
        (keyspace_channel.clone(), "set".to_string()),
        (keyevent_channel.clone(), key.clone()),
    ];
    want.sort();
    assert_eq!(
        observed, want,
        "the owning primary must emit exactly the keyspace + keyevent pair for its own write"
    );
    // Exactly once: nothing further arrives for this single SET.
    let extra = subscribers[owner_index]
        .1
        .read_message(Duration::from_millis(400))
        .await;
    assert!(
        extra.is_none(),
        "one SET must produce one keyspace + one keyevent message, got extra {extra:?}"
    );

    // No other primary observes the event: emission never crosses the bus.
    for (id, client) in subscribers.iter_mut() {
        if *id == owner_id {
            continue;
        }
        let msg = client.read_message(Duration::from_millis(600)).await;
        assert!(
            msg.is_none(),
            "node {id} does not own slot {slot}; it must not deliver a keyspace \
             notification for {key}, got {msg:?}"
        );
    }

    // Control: ordinary PUBLISH on the same channel *does* reach every node,
    // proving the subscriptions above were live.
    let publisher = harness.node_ids()[0];
    let resp = harness
        .node(publisher)
        .unwrap()
        .send("PUBLISH", &[&keyevent_channel, "control"])
        .await;
    assert!(!is_error(&resp), "PUBLISH control message: {resp:?}");
    for (id, client) in subscribers.iter_mut() {
        let msg = client.read_message(Duration::from_secs(2)).await;
        let (channel, payload) = expect_pubsub_message(msg);
        assert_eq!(
            (channel.as_str(), payload.as_str()),
            (keyevent_channel.as_str(), "control"),
            "node {id} must receive the cross-node PUBLISH control message"
        );
    }

    harness.shutdown_all().await;
}

/// `SCAN` on a cluster node iterates only that node's own keys; the union
/// across every primary reconstructs the full keyspace exactly once.
///
/// Acceptance criteria (issue 58): scan each primary to completion, then assert
/// the union equals everything written, with no duplicates (within a node or
/// across nodes) and no cross-node bleed (every key a node yields hashes into a
/// slot that node owns).
#[tokio::test]
async fn test_cluster_scan_is_per_node_and_unions_to_full_keyspace() {
    use std::collections::{BTreeMap, BTreeSet};

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

    let ownership = slot_ownership(&harness).await;
    assert!(
        ownership.len() >= 2,
        "test needs at least two slot-owning primaries, got {ownership:?}"
    );

    // Spread one key per slot across the whole ring so every primary receives
    // some. Each key is written *directly* to its owning primary (never through
    // a redirect) so the test pins ownership instead of tolerating it.
    let mut expected: BTreeMap<String, u64> = BTreeMap::new();
    for i in 0..48u16 {
        let slot = i * 341;
        let key = key_for_slot(slot);
        assert_eq!(slot_for_key(key.as_bytes()), slot);
        let owner = owner_of_slot(&ownership, slot)
            .unwrap_or_else(|| panic!("slot {slot} has no owner in {ownership:?}"));
        let resp = harness
            .node(owner)
            .unwrap()
            .send("SET", &[&key, "scan-value"])
            .await;
        assert!(
            !is_error(&resp),
            "SET {key} (slot {slot}) on owning primary {owner}: {resp:?}"
        );
        assert!(
            expected.insert(key.clone(), owner).is_none(),
            "test bug: {key} written twice"
        );
    }

    // Fan out SCAN across every node, to completion.
    let mut per_node: Vec<(u64, Vec<String>)> = Vec::new();
    for &id in &harness.node_ids() {
        let mut client = harness.node(id).unwrap().connect().await;
        per_node.push((id, scan_to_completion(&mut client).await));
    }

    let mut union: BTreeSet<String> = BTreeSet::new();
    for (id, keys) in &per_node {
        // No cross-node bleed: every key a node yields hashes into a slot that
        // node owns.
        for key in keys {
            let slot = slot_for_key(key.as_bytes());
            let owner = owner_of_slot(&ownership, slot);
            assert_eq!(
                owner,
                Some(*id),
                "SCAN on node {id} returned {key} (slot {slot}), which is owned by {owner:?}"
            );
        }
        // No duplicates within a node: the keyspace is static for this scan.
        let distinct: BTreeSet<&String> = keys.iter().collect();
        assert_eq!(
            distinct.len(),
            keys.len(),
            "SCAN on node {id} returned duplicate keys: {keys:?}"
        );
        // No duplicates across nodes: slot ownership is exclusive.
        for key in keys {
            assert!(
                union.insert(key.clone()),
                "{key} was returned by more than one node's SCAN"
            );
        }
        // Per-node, not cluster-wide: a single node can never see the whole
        // keyspace of a multi-primary cluster.
        assert!(
            keys.len() < expected.len(),
            "SCAN on node {id} returned {} of {} keys — a node must iterate only \
             its own slots, never the whole cluster",
            keys.len(),
            expected.len()
        );
    }

    // Every slot-owning primary contributed something (otherwise "union equals
    // the keyspace" could hold with a single node doing all the work).
    for (id, _) in &ownership {
        let scanned = &per_node
            .iter()
            .find(|(n, _)| n == id)
            .expect("every node was scanned")
            .1;
        assert!(
            !scanned.is_empty(),
            "slot-owning primary {id} returned no keys; the write spread should \
             have reached every primary"
        );
    }

    // The union is the full keyspace.
    let want: BTreeSet<String> = expected.keys().cloned().collect();
    assert_eq!(
        union, want,
        "the union of per-node SCANs must equal the full written keyspace"
    );

    harness.shutdown_all().await;
}
