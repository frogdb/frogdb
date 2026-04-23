//! Rust port tracker for Redis 8.6.0 `unit/hotkeys.tcl`.
//!
//! FrogDB implements the `HOTKEYS START / STOP / RESET / GET` command family
//! for hot-key detection via probabilistic sampling.
//!
//! ## Intentional exclusions
//!
//! Cluster-mode tests that require a multi-node cluster with specific slot
//! distributions. FrogDB standalone mode cannot replicate these scenarios:
//! - `HOTKEYS START - with SLOTS parameter in cluster mode` — intentional-incompatibility:observability — requires multi-node cluster
//! - `HOTKEYS GET - selected-slots returns node's slot ranges when no SLOTS specified in cluster mode` — intentional-incompatibility:observability — requires multi-node cluster
//! - `HOTKEYS START - Error: slot not handled by this node` — intentional-incompatibility:observability — requires multi-node cluster
//! - `HOTKEYS GET - selected-slots returns each node's slot ranges in multi-node cluster` — intentional-incompatibility:observability — requires multi-node cluster
//! - `HOTKEYS - tracks only keys in selected slots` — intentional-incompatibility:observability — requires cluster mode SLOTS parameter
//! - `HOTKEYS - multiple selected slots` — intentional-incompatibility:observability — requires cluster mode SLOTS parameter

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ===========================================================================
// HOTKEYS START - parameter parsing and validation
// ===========================================================================

#[tokio::test]
async fn tcl_hotkeys_start_metrics_required() {
    // HOTKEYS START without METRICS should error
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["HOTKEYS", "START"]).await;
    assert_error_prefix(&resp, "ERR METRICS option is required");
}

#[tokio::test]
async fn tcl_hotkeys_start_metrics_with_cpu_only() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["HOTKEYS", "START", "METRICS", "1", "CPU"])
        .await;
    assert_ok(&resp);

    // Cleanup
    client.command(&["HOTKEYS", "STOP"]).await;
    client.command(&["HOTKEYS", "RESET"]).await;
}

#[tokio::test]
async fn tcl_hotkeys_start_metrics_with_net_only() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["HOTKEYS", "START", "METRICS", "1", "NET"])
        .await;
    assert_ok(&resp);

    client.command(&["HOTKEYS", "STOP"]).await;
    client.command(&["HOTKEYS", "RESET"]).await;
}

#[tokio::test]
async fn tcl_hotkeys_start_metrics_with_both_cpu_and_net() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["HOTKEYS", "START", "METRICS", "2", "CPU", "NET"])
        .await;
    assert_ok(&resp);

    client.command(&["HOTKEYS", "STOP"]).await;
    client.command(&["HOTKEYS", "RESET"]).await;
}

#[tokio::test]
async fn tcl_hotkeys_start_error_session_already_started() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["HOTKEYS", "START", "METRICS", "1", "CPU"])
        .await;
    assert_ok(&resp);

    // Starting again should error
    let resp = client
        .command(&["HOTKEYS", "START", "METRICS", "1", "CPU"])
        .await;
    assert_error_prefix(&resp, "ERR hotkeys session already started");

    client.command(&["HOTKEYS", "STOP"]).await;
    client.command(&["HOTKEYS", "RESET"]).await;
}

#[tokio::test]
async fn tcl_hotkeys_start_error_invalid_metrics_count() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Count of 0 is invalid
    let resp = client.command(&["HOTKEYS", "START", "METRICS", "0"]).await;
    assert_error_prefix(&resp, "ERR METRICS count must be between 1 and 2");

    // Count > 2 is invalid
    let resp = client.command(&["HOTKEYS", "START", "METRICS", "3"]).await;
    assert_error_prefix(&resp, "ERR METRICS count must be between 1 and 2");
}

#[tokio::test]
async fn tcl_hotkeys_start_error_metrics_count_mismatch() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Says 2 metrics but only provides 1
    let resp = client
        .command(&["HOTKEYS", "START", "METRICS", "2", "CPU"])
        .await;
    assert_error_prefix(&resp, "ERR METRICS count does not match");
}

#[tokio::test]
async fn tcl_hotkeys_start_error_metrics_invalid_metrics() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["HOTKEYS", "START", "METRICS", "1", "INVALID"])
        .await;
    assert_error_prefix(&resp, "ERR METRICS invalid metric");
}

#[tokio::test]
async fn tcl_hotkeys_start_error_metrics_same_parameter() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["HOTKEYS", "START", "METRICS", "2", "CPU", "CPU"])
        .await;
    assert_error_prefix(&resp, "ERR METRICS duplicate metric");
}

#[tokio::test]
async fn tcl_hotkeys_start_with_count_parameter() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["HOTKEYS", "START", "METRICS", "1", "CPU", "COUNT", "10"])
        .await;
    assert_ok(&resp);

    // Verify COUNT is reflected in GET
    let resp = client.command(&["HOTKEYS", "GET"]).await;
    let items = unwrap_array(resp);
    // Find count field
    let count_idx = items
        .iter()
        .position(
            |r| matches!(r, frogdb_protocol::Response::Bulk(Some(b)) if b.as_ref() == b"count"),
        )
        .expect("should have count field");
    assert_integer_eq(&items[count_idx + 1], 10);

    client.command(&["HOTKEYS", "STOP"]).await;
    client.command(&["HOTKEYS", "RESET"]).await;
}

#[tokio::test]
async fn tcl_hotkeys_start_error_count_out_of_range() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // COUNT 0 is out of range
    let resp = client
        .command(&["HOTKEYS", "START", "METRICS", "1", "CPU", "COUNT", "0"])
        .await;
    assert_error_prefix(&resp, "ERR COUNT must be between 1 and 100");

    // COUNT 101 is out of range
    let resp = client
        .command(&["HOTKEYS", "START", "METRICS", "1", "CPU", "COUNT", "101"])
        .await;
    assert_error_prefix(&resp, "ERR COUNT must be between 1 and 100");
}

#[tokio::test]
async fn tcl_hotkeys_start_with_duration_parameter() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "HOTKEYS", "START", "METRICS", "1", "CPU", "DURATION", "5000",
        ])
        .await;
    assert_ok(&resp);

    client.command(&["HOTKEYS", "STOP"]).await;
    client.command(&["HOTKEYS", "RESET"]).await;
}

#[tokio::test]
async fn tcl_hotkeys_start_with_sample_parameter() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["HOTKEYS", "START", "METRICS", "1", "CPU", "SAMPLE", "100"])
        .await;
    assert_ok(&resp);

    // Verify sample-ratio is reflected in GET
    let resp = client.command(&["HOTKEYS", "GET"]).await;
    let items = unwrap_array(resp);
    let sr_idx = items
        .iter()
        .position(|r| matches!(r, frogdb_protocol::Response::Bulk(Some(b)) if b.as_ref() == b"sample-ratio"))
        .expect("should have sample-ratio field");
    assert_integer_eq(&items[sr_idx + 1], 100);

    client.command(&["HOTKEYS", "STOP"]).await;
    client.command(&["HOTKEYS", "RESET"]).await;
}

#[tokio::test]
async fn tcl_hotkeys_start_error_sample_ratio_invalid() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // SAMPLE 0 is invalid
    let resp = client
        .command(&["HOTKEYS", "START", "METRICS", "1", "CPU", "SAMPLE", "0"])
        .await;
    assert_error_prefix(&resp, "ERR SAMPLE ratio must be >= 1");
}

#[tokio::test]
async fn tcl_hotkeys_start_error_slots_not_allowed_in_non_cluster_mode() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["HOTKEYS", "START", "METRICS", "1", "CPU", "SLOTS", "1", "0"])
        .await;
    assert_error_prefix(&resp, "ERR SLOTS option is not allowed in non-cluster mode");
}

// ===========================================================================
// HOTKEYS STOP / RESET
// ===========================================================================

#[tokio::test]
async fn tcl_hotkeys_stop_basic_functionality() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Start a session
    let resp = client
        .command(&["HOTKEYS", "START", "METRICS", "1", "CPU"])
        .await;
    assert_ok(&resp);

    // Stop it
    let resp = client.command(&["HOTKEYS", "STOP"]).await;
    assert_ok(&resp);

    // GET should still return data (not nil)
    let resp = client.command(&["HOTKEYS", "GET"]).await;
    assert!(
        !matches!(resp, frogdb_protocol::Response::Bulk(None)),
        "GET after STOP should not return nil"
    );

    client.command(&["HOTKEYS", "RESET"]).await;
}

#[tokio::test]
async fn tcl_hotkeys_reset_basic_functionality() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Start and stop
    client
        .command(&["HOTKEYS", "START", "METRICS", "1", "CPU"])
        .await;
    client.command(&["HOTKEYS", "STOP"]).await;

    // Reset
    let resp = client.command(&["HOTKEYS", "RESET"]).await;
    assert_ok(&resp);

    // GET should return nil
    let resp = client.command(&["HOTKEYS", "GET"]).await;
    assert_nil(&resp);
}

#[tokio::test]
async fn tcl_hotkeys_reset_error_session_in_progress() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Start a session
    client
        .command(&["HOTKEYS", "START", "METRICS", "1", "CPU"])
        .await;

    // Resetting an active session should error
    let resp = client.command(&["HOTKEYS", "RESET"]).await;
    assert_error_prefix(
        &resp,
        "ERR cannot reset an active hotkeys session. Stop it first",
    );

    client.command(&["HOTKEYS", "STOP"]).await;
    client.command(&["HOTKEYS", "RESET"]).await;
}

// ===========================================================================
// HOTKEYS GET
// ===========================================================================

#[tokio::test]
async fn tcl_hotkeys_get_returns_nil_when_not_started() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["HOTKEYS", "GET"]).await;
    assert_nil(&resp);
}

#[tokio::test]
async fn tcl_hotkeys_get_sample_ratio_field() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Start with sample ratio = 5
    client
        .command(&["HOTKEYS", "START", "METRICS", "1", "CPU", "SAMPLE", "5"])
        .await;

    let resp = client.command(&["HOTKEYS", "GET"]).await;
    let items = unwrap_array(resp);

    // Find sample-ratio field
    let sr_idx = items
        .iter()
        .position(|r| matches!(r, frogdb_protocol::Response::Bulk(Some(b)) if b.as_ref() == b"sample-ratio"))
        .expect("should have sample-ratio field");
    assert_integer_eq(&items[sr_idx + 1], 5);

    client.command(&["HOTKEYS", "STOP"]).await;
    client.command(&["HOTKEYS", "RESET"]).await;
}

#[tokio::test]
async fn tcl_hotkeys_get_no_conditional_fields_without_selected_slots() {
    // With sample_ratio=1 and no explicit slots, conditional fields
    // (sample-ratio, selected-slots) should NOT be present in GET response.
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Start with default sample ratio (1) and no SLOTS
    client
        .command(&["HOTKEYS", "START", "METRICS", "1", "CPU"])
        .await;

    let resp = client.command(&["HOTKEYS", "GET"]).await;
    let items = unwrap_array(resp);

    // sample-ratio should NOT be present (it's 1)
    let has_sr = items.iter().any(
        |r| matches!(r, frogdb_protocol::Response::Bulk(Some(b)) if b.as_ref() == b"sample-ratio"),
    );
    assert!(
        !has_sr,
        "sample-ratio should not be in response when ratio is 1"
    );

    // selected-slots should NOT be present (no explicit slots, sample=1)
    let has_ss = items.iter().any(
        |r| matches!(r, frogdb_protocol::Response::Bulk(Some(b)) if b.as_ref() == b"selected-slots"),
    );
    assert!(
        !has_ss,
        "selected-slots should not be in response with sample_ratio=1 and no explicit slots"
    );

    client.command(&["HOTKEYS", "STOP"]).await;
    client.command(&["HOTKEYS", "RESET"]).await;
}

#[tokio::test]
async fn tcl_hotkeys_get_no_conditional_fields_with_sample_ratio_1() {
    // With sample_ratio = 1, no conditional fields
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HOTKEYS", "START", "METRICS", "1", "CPU", "SAMPLE", "1"])
        .await;

    let resp = client.command(&["HOTKEYS", "GET"]).await;
    let items = unwrap_array(resp);

    // sample-ratio should NOT be present
    let has_sr = items.iter().any(
        |r| matches!(r, frogdb_protocol::Response::Bulk(Some(b)) if b.as_ref() == b"sample-ratio"),
    );
    assert!(!has_sr, "sample-ratio should not appear when ratio is 1");

    client.command(&["HOTKEYS", "STOP"]).await;
    client.command(&["HOTKEYS", "RESET"]).await;
}

#[tokio::test]
async fn tcl_hotkeys_get_conditional_fields_with_sample_ratio_gt_1_and_selected_slots() {
    // With sample_ratio > 1, both sample-ratio and selected-slots should appear
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HOTKEYS", "START", "METRICS", "1", "CPU", "SAMPLE", "10"])
        .await;

    let resp = client.command(&["HOTKEYS", "GET"]).await;
    let items = unwrap_array(resp);

    // sample-ratio should be present
    let has_sr = items.iter().any(
        |r| matches!(r, frogdb_protocol::Response::Bulk(Some(b)) if b.as_ref() == b"sample-ratio"),
    );
    assert!(has_sr, "sample-ratio should appear when ratio > 1");

    // selected-slots should be present (full range 0-16383)
    let has_ss = items.iter().any(
        |r| matches!(r, frogdb_protocol::Response::Bulk(Some(b)) if b.as_ref() == b"selected-slots"),
    );
    assert!(has_ss, "selected-slots should appear when sample_ratio > 1");

    client.command(&["HOTKEYS", "STOP"]).await;
    client.command(&["HOTKEYS", "RESET"]).await;
}

#[tokio::test]
async fn tcl_hotkeys_get_selected_slots_returns_full_range_in_non_cluster_mode() {
    // In non-cluster mode with sample_ratio > 1, selected-slots should contain all 16384 slots
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HOTKEYS", "START", "METRICS", "1", "CPU", "SAMPLE", "2"])
        .await;

    let resp = client.command(&["HOTKEYS", "GET"]).await;
    let items = unwrap_array(resp);

    // Find selected-slots
    let ss_idx = items
        .iter()
        .position(|r| matches!(r, frogdb_protocol::Response::Bulk(Some(b)) if b.as_ref() == b"selected-slots"))
        .expect("should have selected-slots field");

    // The value after selected-slots should be an array of 16384 integers
    assert_array_len(&items[ss_idx + 1], 16384);

    client.command(&["HOTKEYS", "STOP"]).await;
    client.command(&["HOTKEYS", "RESET"]).await;
}

// ===========================================================================
// HOTKEYS - key access tracking
// ===========================================================================

#[tokio::test]
async fn tcl_hotkeys_nested_commands() {
    // Nested commands (e.g., SET/GET) should have keys tracked
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HOTKEYS", "START", "METRICS", "1", "CPU"])
        .await;

    // Execute several SET/GET commands
    client.command(&["SET", "key1", "value1"]).await;
    client.command(&["SET", "key1", "value2"]).await;
    client.command(&["GET", "key1"]).await;
    client.command(&["SET", "key2", "value3"]).await;

    client.command(&["HOTKEYS", "STOP"]).await;

    let resp = client.command(&["HOTKEYS", "GET"]).await;
    let items = unwrap_array(resp);

    // Find hotkeys field
    let hk_idx = items
        .iter()
        .position(
            |r| matches!(r, frogdb_protocol::Response::Bulk(Some(b)) if b.as_ref() == b"hotkeys"),
        )
        .expect("should have hotkeys field");

    let hotkeys = unwrap_array(items[hk_idx + 1].clone());
    // Should have entries for key1 and key2
    assert!(
        !hotkeys.is_empty(),
        "hotkeys should have at least one entry"
    );

    // key1 should have more accesses than key2
    let mut key1_count = 0i64;
    let mut key2_count = 0i64;
    for entry in &hotkeys {
        let entry_arr = unwrap_array(entry.clone());
        let key_idx = entry_arr
            .iter()
            .position(
                |r| matches!(r, frogdb_protocol::Response::Bulk(Some(b)) if b.as_ref() == b"key"),
            )
            .unwrap();
        let key = unwrap_bulk(&entry_arr[key_idx + 1]);
        let ac_idx = entry_arr
            .iter()
            .position(|r| matches!(r, frogdb_protocol::Response::Bulk(Some(b)) if b.as_ref() == b"access-count"))
            .unwrap();
        let count = unwrap_integer(&entry_arr[ac_idx + 1]);
        if key == b"key1" {
            key1_count = count;
        } else if key == b"key2" {
            key2_count = count;
        }
    }
    assert!(
        key1_count > key2_count,
        "key1 (count={key1_count}) should have more accesses than key2 (count={key2_count})"
    );

    client.command(&["HOTKEYS", "RESET"]).await;
}

#[tokio::test]
async fn tcl_hotkeys_commands_inside_multi_exec() {
    // Commands inside MULTI/EXEC should also be tracked
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HOTKEYS", "START", "METRICS", "1", "CPU"])
        .await;

    client.command(&["MULTI"]).await;
    client.command(&["SET", "txkey", "val1"]).await;
    client.command(&["GET", "txkey"]).await;
    client.command(&["EXEC"]).await;

    client.command(&["HOTKEYS", "STOP"]).await;

    let resp = client.command(&["HOTKEYS", "GET"]).await;
    let items = unwrap_array(resp);

    let hk_idx = items
        .iter()
        .position(
            |r| matches!(r, frogdb_protocol::Response::Bulk(Some(b)) if b.as_ref() == b"hotkeys"),
        )
        .expect("should have hotkeys field");

    let hotkeys = unwrap_array(items[hk_idx + 1].clone());
    // Should have entry for txkey
    let has_txkey = hotkeys.iter().any(|entry| {
        let arr = unwrap_array(entry.clone());
        arr.iter().any(
            |r| matches!(r, frogdb_protocol::Response::Bulk(Some(b)) if b.as_ref() == b"txkey"),
        )
    });
    assert!(has_txkey, "txkey should appear in hotkeys after MULTI/EXEC");

    client.command(&["HOTKEYS", "RESET"]).await;
}

#[tokio::test]
async fn tcl_hotkeys_eval_inside_multi_exec_with_nested_calls() {
    // EVAL inside MULTI/EXEC with nested calls
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HOTKEYS", "START", "METRICS", "1", "CPU"])
        .await;

    // EVAL that sets a key
    client
        .command(&[
            "EVAL",
            "redis.call('SET', KEYS[1], 'eval_value')",
            "1",
            "evalkey",
        ])
        .await;

    client.command(&["HOTKEYS", "STOP"]).await;

    let resp = client.command(&["HOTKEYS", "GET"]).await;
    let items = unwrap_array(resp);

    let hk_idx = items
        .iter()
        .position(
            |r| matches!(r, frogdb_protocol::Response::Bulk(Some(b)) if b.as_ref() == b"hotkeys"),
        )
        .expect("should have hotkeys field");

    let hotkeys = unwrap_array(items[hk_idx + 1].clone());
    // EVAL has the key in its keys() extraction, so evalkey should appear
    let has_evalkey = hotkeys.iter().any(|entry| {
        let arr = unwrap_array(entry.clone());
        arr.iter().any(
            |r| matches!(r, frogdb_protocol::Response::Bulk(Some(b)) if b.as_ref() == b"evalkey"),
        )
    });
    assert!(has_evalkey, "evalkey should appear in hotkeys after EVAL");

    client.command(&["HOTKEYS", "RESET"]).await;
}

// ===========================================================================
// HOTKEYS detection with biased key access
// ===========================================================================

#[tokio::test]
async fn tcl_hotkeys_detection_with_biased_key_access_sample_ratio_1() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "HOTKEYS", "START", "METRICS", "1", "CPU", "SAMPLE", "1", "COUNT", "5",
        ])
        .await;

    // Access "hotkey" much more than others
    for _ in 0..50 {
        client.command(&["SET", "hotkey", "value"]).await;
    }
    for i in 0..10 {
        let key = format!("coldkey:{}", i);
        client.command(&["SET", &key, "value"]).await;
    }

    client.command(&["HOTKEYS", "STOP"]).await;

    let resp = client.command(&["HOTKEYS", "GET"]).await;
    let items = unwrap_array(resp);

    let hk_idx = items
        .iter()
        .position(
            |r| matches!(r, frogdb_protocol::Response::Bulk(Some(b)) if b.as_ref() == b"hotkeys"),
        )
        .expect("should have hotkeys field");

    let hotkeys = unwrap_array(items[hk_idx + 1].clone());
    assert!(!hotkeys.is_empty(), "should have at least one hotkey");

    // The first hotkey should be "hotkey" (highest access count)
    let first_entry = unwrap_array(hotkeys[0].clone());
    let key_idx = first_entry
        .iter()
        .position(|r| matches!(r, frogdb_protocol::Response::Bulk(Some(b)) if b.as_ref() == b"key"))
        .unwrap();
    assert_bulk_eq(&first_entry[key_idx + 1], b"hotkey");

    client.command(&["HOTKEYS", "RESET"]).await;
}

#[tokio::test]
async fn tcl_hotkeys_detection_with_biased_key_access_sample_ratio_10() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "HOTKEYS", "START", "METRICS", "1", "CPU", "SAMPLE", "10", "COUNT", "5",
        ])
        .await;

    // Access "hotkey" much more than others
    for _ in 0..200 {
        client.command(&["SET", "hotkey", "value"]).await;
    }
    for i in 0..10 {
        let key = format!("coldkey:{}", i);
        for _ in 0..5 {
            client.command(&["SET", &key, "value"]).await;
        }
    }

    client.command(&["HOTKEYS", "STOP"]).await;

    let resp = client.command(&["HOTKEYS", "GET"]).await;
    let items = unwrap_array(resp);

    let hk_idx = items
        .iter()
        .position(
            |r| matches!(r, frogdb_protocol::Response::Bulk(Some(b)) if b.as_ref() == b"hotkeys"),
        )
        .expect("should have hotkeys field");

    let hotkeys = unwrap_array(items[hk_idx + 1].clone());
    assert!(!hotkeys.is_empty(), "should have at least one hotkey");

    // The first hotkey should be "hotkey" (highest access count even with sampling)
    let first_entry = unwrap_array(hotkeys[0].clone());
    let key_idx = first_entry
        .iter()
        .position(|r| matches!(r, frogdb_protocol::Response::Bulk(Some(b)) if b.as_ref() == b"key"))
        .unwrap();
    assert_bulk_eq(&first_entry[key_idx + 1], b"hotkey");

    client.command(&["HOTKEYS", "RESET"]).await;
}

// ===========================================================================
// HOTKEYS GET - RESP3 format
// ===========================================================================

#[tokio::test]
async fn tcl_hotkeys_get_resp3_returns_map_with_flat_array_values_for_hotkeys() {
    use redis_protocol::resp3::types::BytesFrame as Resp3Frame;

    let server = TestServer::start_standalone().await;
    let mut client = server.connect_resp3().await;

    // Issue HELLO 3 to switch to RESP3
    client.command(&["HELLO", "3"]).await;

    client
        .command(&["HOTKEYS", "START", "METRICS", "1", "CPU"])
        .await;

    client.command(&["SET", "r3key", "value"]).await;

    client.command(&["HOTKEYS", "STOP"]).await;

    let resp = client.command(&["HOTKEYS", "GET"]).await;

    // Verify it's a Map response
    assert!(
        matches!(&resp, Resp3Frame::Map { .. }),
        "RESP3 GET should return a Map, got {:?}",
        resp
    );

    client.command(&["HOTKEYS", "RESET"]).await;
}

// ===========================================================================
// HOTKEYS START - SLOTS parameter errors (standalone mode)
// ===========================================================================

#[tokio::test]
async fn tcl_hotkeys_start_error_slots_count_mismatch() {
    // This requires cluster mode for SLOTS, but the count-mismatch is still
    // tested. In standalone mode SLOTS is rejected before count validation.
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["HOTKEYS", "START", "METRICS", "1", "CPU", "SLOTS", "2", "0"])
        .await;
    // In standalone mode, SLOTS is rejected entirely
    assert_error_prefix(&resp, "ERR SLOTS option is not allowed in non-cluster mode");
}

#[tokio::test]
async fn tcl_hotkeys_start_error_duplicate_slots() {
    // In standalone mode, SLOTS is rejected entirely
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "HOTKEYS", "START", "METRICS", "1", "CPU", "SLOTS", "2", "0", "0",
        ])
        .await;
    assert_error_prefix(&resp, "ERR SLOTS option is not allowed in non-cluster mode");
}

#[tokio::test]
async fn tcl_hotkeys_start_error_slots_already_specified() {
    // In standalone mode, SLOTS is rejected entirely
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "HOTKEYS", "START", "METRICS", "1", "CPU", "SLOTS", "1", "0", "SLOTS", "1", "1",
        ])
        .await;
    assert_error_prefix(&resp, "ERR SLOTS option is not allowed in non-cluster mode");
}

#[tokio::test]
async fn tcl_hotkeys_start_error_invalid_slot_negative_value() {
    // In standalone mode, SLOTS is rejected entirely
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "HOTKEYS", "START", "METRICS", "1", "CPU", "SLOTS", "1", "-1",
        ])
        .await;
    assert_error_prefix(&resp, "ERR SLOTS option is not allowed in non-cluster mode");
}

#[tokio::test]
async fn tcl_hotkeys_start_error_invalid_slot_out_of_range() {
    // In standalone mode, SLOTS is rejected entirely
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "HOTKEYS", "START", "METRICS", "1", "CPU", "SLOTS", "1", "16384",
        ])
        .await;
    assert_error_prefix(&resp, "ERR SLOTS option is not allowed in non-cluster mode");
}

#[tokio::test]
async fn tcl_hotkeys_start_error_invalid_slot_non_integer() {
    // In standalone mode, SLOTS is rejected entirely
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "HOTKEYS", "START", "METRICS", "1", "CPU", "SLOTS", "1", "abc",
        ])
        .await;
    assert_error_prefix(&resp, "ERR SLOTS option is not allowed in non-cluster mode");
}

#[tokio::test]
async fn tcl_hotkeys_get_selected_slots_field_with_individual_slots() {
    // In non-cluster mode, SLOTS is not allowed. This test verifies that
    // with sample_ratio > 1, selected-slots contains the full range.
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HOTKEYS", "START", "METRICS", "1", "CPU", "SAMPLE", "5"])
        .await;

    let resp = client.command(&["HOTKEYS", "GET"]).await;
    let items = unwrap_array(resp);

    let ss_idx = items
        .iter()
        .position(|r| matches!(r, frogdb_protocol::Response::Bulk(Some(b)) if b.as_ref() == b"selected-slots"))
        .expect("should have selected-slots when sample_ratio > 1");

    // Should contain all 16384 slots in standalone mode
    assert_array_len(&items[ss_idx + 1], 16384);

    client.command(&["HOTKEYS", "STOP"]).await;
    client.command(&["HOTKEYS", "RESET"]).await;
}

#[tokio::test]
async fn tcl_hotkeys_get_selected_slots_with_unordered_input_slots_are_sorted() {
    // In standalone mode, selected-slots is always the full ordered range.
    // This test verifies the full range is sorted (0..16383).
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HOTKEYS", "START", "METRICS", "1", "CPU", "SAMPLE", "2"])
        .await;

    let resp = client.command(&["HOTKEYS", "GET"]).await;
    let items = unwrap_array(resp);

    let ss_idx = items
        .iter()
        .position(|r| matches!(r, frogdb_protocol::Response::Bulk(Some(b)) if b.as_ref() == b"selected-slots"))
        .expect("should have selected-slots");

    let slots = unwrap_array(items[ss_idx + 1].clone());
    // Verify sorted
    for (i, slot) in slots.iter().enumerate() {
        assert_integer_eq(slot, i as i64);
    }

    client.command(&["HOTKEYS", "STOP"]).await;
    client.command(&["HOTKEYS", "RESET"]).await;
}
