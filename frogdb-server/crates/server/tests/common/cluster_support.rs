//! Helpers shared by the `cluster_*` integration test binaries.
//!
//! Extracted verbatim from the former `integration_cluster.rs` when it was
//! split per concern; each helper is used by more than one `cluster_*` binary.

#![allow(dead_code)]

use frogdb_test_harness::cluster_harness::ClusterTestHarness;
use frogdb_test_harness::cluster_helpers::{
    get_error_message, is_error, is_moved_redirect, key_for_slot, parse_info_fields,
};
use std::time::Duration;

/// Poll `INFO replication` on a node until `role:` matches, returning the
/// last value seen.
pub async fn wait_for_data_path_role(
    harness: &ClusterTestHarness,
    node_id: u64,
    want: &str,
    timeout: Duration,
) -> String {
    let deadline = std::time::Instant::now() + timeout;
    let mut last = String::new();
    while std::time::Instant::now() < deadline {
        if let Some(node) = harness.node(node_id)
            && let Ok(resp) = node.try_send("INFO", &["replication"]).await
            && let Some(fields) = parse_info_fields(&resp)
            && let Some(role) = fields.get("role")
        {
            last = role.clone();
            if last == want {
                return last;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    last
}

/// Split "host:port" into ("host", "port").
pub fn split_addr(addr: &str) -> (&str, &str) {
    addr.rsplit_once(':').expect("address must be host:port")
}

/// Find the harness node ID that owns the given slot by probing nodes.
/// Sends a GET for a key in the slot: the node that handles it is the owner;
/// a MOVED redirect tells us who the real owner is.
/// Returns (owner_harness_id, non_owner_harness_id).
pub async fn find_owner_of_slot(harness: &ClusterTestHarness, slot: u16) -> Option<(u64, u64)> {
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
pub async fn send_cluster_cmd(
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
pub async fn run_full_slot_migration(
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

/// Assert a response is the exact `EXECABORT` wire error.
pub fn assert_execabort(response: &frogdb_protocol::Response, context: &str) {
    assert_exact_error(
        response,
        b"EXECABORT Transaction discarded because of previous errors.",
        context,
    );
}

pub fn assert_exact_error(response: &frogdb_protocol::Response, expected: &[u8], context: &str) {
    match response {
        frogdb_protocol::Response::Error(e) => assert_eq!(
            e.as_ref(),
            expected,
            "{context}: expected exact error {:?}, got {:?}",
            String::from_utf8_lossy(expected),
            String::from_utf8_lossy(e)
        ),
        other => panic!(
            "{context}: expected error {:?}, got {other:?}",
            String::from_utf8_lossy(expected)
        ),
    }
}

/// Helper: send a SET command, following MOVED redirects to the correct node.
/// Returns true if the write succeeded.
pub async fn set_with_redirect(harness: &ClusterTestHarness, key: &str, value: &str) -> bool {
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
pub async fn get_with_redirect(harness: &ClusterTestHarness, key: &str) -> Option<String> {
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
