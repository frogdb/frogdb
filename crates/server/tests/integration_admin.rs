//! Integration tests for admin commands (SLOWLOG, BGSAVE, LASTSAVE, MEMORY, LATENCY, CONFIG).

mod common;

use common::response_helpers::{assert_ok, unwrap_array, unwrap_bulk, unwrap_integer};
use common::test_server::TestServer;
use frogdb_protocol::Response;
use std::time::Duration;

// ============================================================================
// SLOWLOG tests
// ============================================================================

#[tokio::test]
async fn test_slowlog_get_empty() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Initially the slowlog should be empty
    let response = client.command(&["SLOWLOG", "GET"]).await;
    let entries = unwrap_array(response);
    assert!(entries.is_empty(), "Slowlog should be empty initially");

    server.shutdown().await;
}

#[tokio::test]
async fn test_slowlog_len_empty() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Initially the slowlog length should be 0
    let response = client.command(&["SLOWLOG", "LEN"]).await;
    assert_eq!(response, Response::Integer(0));

    server.shutdown().await;
}

#[tokio::test]
async fn test_slowlog_reset() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Reset should always succeed
    let response = client.command(&["SLOWLOG", "RESET"]).await;
    assert_ok(&response);

    // After reset, length should be 0
    let response = client.command(&["SLOWLOG", "LEN"]).await;
    assert_eq!(response, Response::Integer(0));

    server.shutdown().await;
}

#[tokio::test]
async fn test_slowlog_help() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["SLOWLOG", "HELP"]).await;
    let items = unwrap_array(response);
    // Should return help text
    assert!(!items.is_empty(), "Help should not be empty");
    // First item should mention SLOWLOG
    if let Some(Response::Bulk(Some(first))) = items.first() {
        let text = String::from_utf8_lossy(first);
        assert!(
            text.to_uppercase().contains("SLOWLOG"),
            "Help should mention SLOWLOG"
        );
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_slowlog_threshold_disabled() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Disable slowlog
    let response = client
        .command(&["CONFIG", "SET", "slowlog-log-slower-than", "-1"])
        .await;
    assert_ok(&response);

    // Run a command
    let _ = client.command(&["SET", "key1", "value1"]).await;

    // Slowlog should still be empty because logging is disabled
    let response = client.command(&["SLOWLOG", "LEN"]).await;
    assert_eq!(response, Response::Integer(0));

    server.shutdown().await;
}

#[tokio::test]
async fn test_slowlog_threshold_log_all() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set threshold to 0 to log all commands
    let response = client
        .command(&["CONFIG", "SET", "slowlog-log-slower-than", "0"])
        .await;
    assert_ok(&response);

    // Reset slowlog first
    let _ = client.command(&["SLOWLOG", "RESET"]).await;

    // Run a few commands
    let _ = client.command(&["SET", "key1", "value1"]).await;
    let _ = client.command(&["GET", "key1"]).await;
    let _ = client.command(&["DEL", "key1"]).await;

    // Slowlog should have entries now
    let response = client.command(&["SLOWLOG", "LEN"]).await;
    let len = unwrap_integer(&response);
    assert!(len >= 3, "Expected at least 3 entries, got {}", len);

    // Get entries and verify structure
    let response = client.command(&["SLOWLOG", "GET", "10"]).await;
    let entries = unwrap_array(response);
    assert!(
        entries.len() >= 3,
        "Expected at least 3 entries, got {}",
        entries.len()
    );

    // Check structure of first entry
    if let Response::Array(ref entry) = entries[0] {
        assert_eq!(entry.len(), 6, "Entry should have 6 fields");

        // Field 0: ID (integer)
        assert!(matches!(entry[0], Response::Integer(_)));

        // Field 1: timestamp (integer)
        assert!(matches!(entry[1], Response::Integer(_)));

        // Field 2: duration_us (integer)
        assert!(matches!(entry[2], Response::Integer(_)));

        // Field 3: command args (array)
        assert!(matches!(entry[3], Response::Array(_)));

        // Field 4: client_addr (bulk string)
        assert!(matches!(entry[4], Response::Bulk(Some(_))));

        // Field 5: client_name (bulk string)
        assert!(matches!(entry[5], Response::Bulk(_)));
    } else {
        panic!("Expected array entry, got {:?}", entries[0]);
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_slowlog_get_count_limit() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set threshold to 0 to log all commands
    let _ = client
        .command(&["CONFIG", "SET", "slowlog-log-slower-than", "0"])
        .await;

    // Reset slowlog first
    let _ = client.command(&["SLOWLOG", "RESET"]).await;

    // Run many commands
    for i in 0..20 {
        let _ = client
            .command(&["SET", &format!("key{}", i), &format!("value{}", i)])
            .await;
    }

    // Get only 5 entries
    let response = client.command(&["SLOWLOG", "GET", "5"]).await;
    let entries = unwrap_array(response);
    assert_eq!(
        entries.len(),
        5,
        "Expected exactly 5 entries when count=5"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_slowlog_skip_slowlog_command() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set threshold to 0 to log all commands
    let _ = client
        .command(&["CONFIG", "SET", "slowlog-log-slower-than", "0"])
        .await;

    // Reset slowlog
    let _ = client.command(&["SLOWLOG", "RESET"]).await;

    // Run SLOWLOG commands
    let _ = client.command(&["SLOWLOG", "GET"]).await;
    let _ = client.command(&["SLOWLOG", "LEN"]).await;
    let _ = client.command(&["SLOWLOG", "HELP"]).await;

    // SLOWLOG commands should not be logged (SKIP_SLOWLOG flag)
    let response = client.command(&["SLOWLOG", "LEN"]).await;
    assert_eq!(response, Response::Integer(0));

    server.shutdown().await;
}

#[tokio::test]
async fn test_slowlog_config_get() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Get slowlog configuration
    let response = client.command(&["CONFIG", "GET", "slowlog-*"]).await;
    let items = unwrap_array(response);
    // Should have at least 3 params (log-slower-than, max-len, max-arg-len)
    // Each param is name, value pair so 6 items minimum
    assert!(
        items.len() >= 6,
        "Expected at least 6 items for slowlog config, got {}",
        items.len()
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_slowlog_unknown_subcommand() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["SLOWLOG", "INVALID"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("unknown subcommand"),
                "Error should mention unknown subcommand"
            );
        }
        _ => panic!("Expected error response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_slowlog_get_default_count() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set threshold to 0 to log all commands
    let _ = client
        .command(&["CONFIG", "SET", "slowlog-log-slower-than", "0"])
        .await;

    // Reset slowlog
    let _ = client.command(&["SLOWLOG", "RESET"]).await;

    // Run many commands
    for i in 0..15 {
        let _ = client
            .command(&["SET", &format!("key{}", i), &format!("value{}", i)])
            .await;
    }

    // GET without count should return default (10)
    let response = client.command(&["SLOWLOG", "GET"]).await;
    let entries = unwrap_array(response);
    assert_eq!(
        entries.len(),
        10,
        "Default count should be 10, got {}",
        entries.len()
    );

    server.shutdown().await;
}

// ============================================================================
// BGSAVE / LASTSAVE Tests
// ============================================================================

#[tokio::test]
async fn test_bgsave_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // BGSAVE should return success message
    let response = client.command(&["BGSAVE"]).await;
    match response {
        Response::Simple(msg) => {
            let msg_str = String::from_utf8_lossy(&msg);
            assert!(
                msg_str.contains("Background saving started")
                    || msg_str.contains("already in progress"),
                "Unexpected BGSAVE response: {}",
                msg_str
            );
        }
        _ => panic!("Expected simple string response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_lastsave_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // LASTSAVE should return an integer (Unix timestamp)
    let response = client.command(&["LASTSAVE"]).await;
    let timestamp = unwrap_integer(&response);
    // Timestamp should be a reasonable Unix timestamp
    // Either 0 (no save yet) or a recent timestamp
    assert!(
        timestamp >= 0,
        "LASTSAVE should return non-negative timestamp, got {}",
        timestamp
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_bgsave_then_lastsave() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // First, write some data
    client.command(&["SET", "snapshot_test_key", "test_value"]).await;

    // Trigger BGSAVE
    let response = client.command(&["BGSAVE"]).await;
    match response {
        Response::Simple(msg) => {
            let msg_str = String::from_utf8_lossy(&msg);
            assert!(
                msg_str.contains("Background saving started")
                    || msg_str.contains("already in progress"),
                "BGSAVE should start or already be in progress"
            );
        }
        _ => panic!("Expected simple string response for BGSAVE"),
    }

    // Give the background task time to complete
    tokio::time::sleep(Duration::from_millis(500)).await;

    // LASTSAVE should now return a recent timestamp
    let response = client.command(&["LASTSAVE"]).await;
    let timestamp = unwrap_integer(&response);
    // After BGSAVE, timestamp should be positive (or 0 if save is still in progress)
    assert!(
        timestamp >= 0,
        "LASTSAVE should return valid timestamp after BGSAVE"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_bgsave_concurrent_returns_already_in_progress() {
    let server = TestServer::start_standalone().await;
    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    // Write some data to make the snapshot take longer
    for i in 0..100 {
        client1
            .command(&["SET", &format!("key_{}", i), &format!("value_{}", i)])
            .await;
    }

    // First BGSAVE should start
    let response1 = client1.command(&["BGSAVE"]).await;

    // Second BGSAVE should either start (if first completed) or report already in progress
    let response2 = client2.command(&["BGSAVE"]).await;

    match response1 {
        Response::Simple(msg) => {
            let msg_str = String::from_utf8_lossy(&msg);
            assert!(
                msg_str.contains("saving"),
                "First BGSAVE should indicate saving: {}",
                msg_str
            );
        }
        _ => panic!("Expected simple string response for first BGSAVE"),
    }

    match response2 {
        Response::Simple(msg) => {
            let msg_str = String::from_utf8_lossy(&msg);
            // Either started (first completed quickly) or in progress
            assert!(
                msg_str.contains("saving") || msg_str.contains("progress"),
                "Second BGSAVE should indicate saving or in progress: {}",
                msg_str
            );
        }
        _ => panic!("Expected simple string response for second BGSAVE"),
    }

    server.shutdown().await;
}

// ============================================================================
// MEMORY tests
// ============================================================================

#[tokio::test]
async fn test_memory_help() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["MEMORY", "HELP"]).await;
    let items = unwrap_array(response);
    assert!(!items.is_empty(), "Help should not be empty");
    // First item should mention MEMORY
    if let Some(Response::Bulk(Some(first))) = items.first() {
        let text = String::from_utf8_lossy(first);
        assert!(
            text.to_uppercase().contains("MEMORY"),
            "Help should mention MEMORY"
        );
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_memory_usage_missing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // MEMORY USAGE for non-existent key should return null
    let response = client.command(&["MEMORY", "USAGE", "nonexistent"]).await;
    match response {
        Response::Null => {}
        Response::Bulk(None) => {}
        _ => panic!("Expected null response for missing key, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_memory_usage_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set a key
    let _ = client.command(&["SET", "testkey", "testvalue"]).await;

    // MEMORY USAGE should return a positive integer
    let response = client.command(&["MEMORY", "USAGE", "testkey"]).await;
    let size = unwrap_integer(&response);
    assert!(size > 0, "Memory usage should be positive, got {}", size);

    server.shutdown().await;
}

#[tokio::test]
async fn test_memory_usage_with_samples() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set a key
    let _ = client.command(&["SET", "testkey", "testvalue"]).await;

    // MEMORY USAGE with SAMPLES option
    let response = client
        .command(&["MEMORY", "USAGE", "testkey", "SAMPLES", "5"])
        .await;
    let size = unwrap_integer(&response);
    assert!(size > 0, "Memory usage should be positive, got {}", size);

    server.shutdown().await;
}

#[tokio::test]
async fn test_memory_stats() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // MEMORY STATS should return an array of key-value pairs
    let response = client.command(&["MEMORY", "STATS"]).await;
    let items = unwrap_array(response);
    assert!(!items.is_empty(), "MEMORY STATS should not be empty");
    // Should have pairs (key, value)
    assert!(items.len() % 2 == 0, "Should have even number of items");

    server.shutdown().await;
}

#[tokio::test]
async fn test_memory_doctor() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // MEMORY DOCTOR should return a bulk string report
    let response = client.command(&["MEMORY", "DOCTOR"]).await;
    let report = unwrap_bulk(&response);
    let text = String::from_utf8_lossy(report);
    assert!(!text.is_empty(), "Doctor report should not be empty");

    server.shutdown().await;
}

#[tokio::test]
async fn test_memory_doctor_enhanced_format() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Add some data first
    client.command(&["SET", "key1", "value1"]).await;
    client.command(&["SET", "key2", "value2"]).await;

    // MEMORY DOCTOR should return enhanced report with summary section
    let response = client.command(&["MEMORY", "DOCTOR"]).await;
    let report = unwrap_bulk(&response);
    let text = String::from_utf8_lossy(report);

    // Check for required sections in enhanced format
    assert!(
        text.contains("=== Summary ==="),
        "Report should contain Summary section"
    );
    assert!(
        text.contains("Total keys:"),
        "Report should show total keys"
    );
    assert!(
        text.contains("Total data memory:"),
        "Report should show total data memory"
    );
    assert!(
        text.contains("Number of shards:"),
        "Report should show number of shards"
    );
    assert!(
        text.contains("=== Issues Detected ==="),
        "Report should contain Issues Detected section"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_memory_doctor_big_keys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create a big key (>1MB) - using a 2MB value
    let big_value = "x".repeat(2_000_000);
    client.command(&["SET", "bigkey1", &big_value]).await;

    // MEMORY DOCTOR should detect the big key
    let response = client.command(&["MEMORY", "DOCTOR"]).await;
    let report = unwrap_bulk(&response);
    let text = String::from_utf8_lossy(report);

    // Should detect big keys
    assert!(
        text.contains("=== Big Keys"),
        "Report should contain Big Keys section when big keys exist"
    );
    assert!(
        text.contains("bigkey1"),
        "Report should list the big key"
    );
    assert!(
        text.contains("big key(s) found"),
        "Report should indicate big keys were found in issues section"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_memory_doctor_no_big_keys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // With just a small amount of data, there should be no big keys
    client.command(&["SET", "small_key", "small_value"]).await;

    let response = client.command(&["MEMORY", "DOCTOR"]).await;
    let report = unwrap_bulk(&response);
    let text = String::from_utf8_lossy(report);

    // Should not contain Big Keys section when there are no big keys
    assert!(
        !text.contains("=== Big Keys"),
        "Report should not contain Big Keys section when there are no big keys"
    );
    // The report should still contain the Issues Detected section
    assert!(
        text.contains("=== Issues Detected ==="),
        "Report should contain Issues Detected section"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_memory_malloc_size() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // MEMORY MALLOC-SIZE should return the input (stub behavior)
    let response = client.command(&["MEMORY", "MALLOC-SIZE", "1024"]).await;
    let size = unwrap_integer(&response);
    assert_eq!(size, 1024, "Should return the input size");

    server.shutdown().await;
}

#[tokio::test]
async fn test_memory_purge() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // MEMORY PURGE should return OK (stub behavior)
    let response = client.command(&["MEMORY", "PURGE"]).await;
    assert_ok(&response);

    server.shutdown().await;
}

#[tokio::test]
async fn test_memory_unknown_subcommand() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["MEMORY", "INVALID"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("unknown subcommand"),
                "Error should mention unknown subcommand"
            );
        }
        _ => panic!("Expected error response, got {:?}", response),
    }

    server.shutdown().await;
}

// ============================================================================
// LATENCY tests
// ============================================================================

#[tokio::test]
async fn test_latency_help() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["LATENCY", "HELP"]).await;
    let items = unwrap_array(response);
    assert!(!items.is_empty(), "Help should not be empty");
    // First item should mention LATENCY
    if let Some(Response::Bulk(Some(first))) = items.first() {
        let text = String::from_utf8_lossy(first);
        assert!(
            text.to_uppercase().contains("LATENCY"),
            "Help should mention LATENCY"
        );
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_latency_latest_empty() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Initially LATENCY LATEST should return empty or minimal data
    let response = client.command(&["LATENCY", "LATEST"]).await;
    // Either empty or with entries - both are valid
    unwrap_array(response);

    server.shutdown().await;
}

#[tokio::test]
async fn test_latency_reset() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // LATENCY RESET should return OK
    let response = client.command(&["LATENCY", "RESET"]).await;
    assert_ok(&response);

    server.shutdown().await;
}

#[tokio::test]
async fn test_latency_reset_specific_event() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // LATENCY RESET with specific event
    let response = client.command(&["LATENCY", "RESET", "command"]).await;
    assert_ok(&response);

    server.shutdown().await;
}

#[tokio::test]
async fn test_latency_history_unknown_event() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // LATENCY HISTORY with invalid event should return error
    let response = client.command(&["LATENCY", "HISTORY", "invalid"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("Unknown event type"),
                "Error should mention unknown event type, got: {}",
                err_str
            );
        }
        _ => panic!("Expected error response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_latency_history_valid_event() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // LATENCY HISTORY with valid event should return array
    let response = client.command(&["LATENCY", "HISTORY", "command"]).await;
    // Empty or with entries - both are valid
    unwrap_array(response);

    server.shutdown().await;
}

#[tokio::test]
async fn test_latency_graph_unknown_event() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // LATENCY GRAPH with invalid event should return error
    let response = client.command(&["LATENCY", "GRAPH", "invalid"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("Unknown event type"),
                "Error should mention unknown event type, got: {}",
                err_str
            );
        }
        _ => panic!("Expected error response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_latency_graph_valid_event() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // LATENCY GRAPH with valid event should return bulk string
    let response = client.command(&["LATENCY", "GRAPH", "command"]).await;
    let graph = unwrap_bulk(&response);
    let text = String::from_utf8_lossy(graph);
    // Should contain the event name
    assert!(
        text.contains("command"),
        "Graph should mention the event name"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_latency_doctor() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // LATENCY DOCTOR should return a bulk string report
    let response = client.command(&["LATENCY", "DOCTOR"]).await;
    let report = unwrap_bulk(&response);
    let text = String::from_utf8_lossy(report);
    assert!(!text.is_empty(), "Doctor report should not be empty");

    server.shutdown().await;
}

#[tokio::test]
async fn test_latency_histogram() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // LATENCY HISTOGRAM should return an array (may be empty)
    let response = client.command(&["LATENCY", "HISTOGRAM"]).await;
    // Empty or with data - both are valid
    unwrap_array(response);

    server.shutdown().await;
}

#[tokio::test]
async fn test_latency_unknown_subcommand() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["LATENCY", "INVALID"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("unknown subcommand"),
                "Error should mention unknown subcommand"
            );
        }
        _ => panic!("Expected error response, got {:?}", response),
    }

    server.shutdown().await;
}

// =============================================================================
// CONFIG SET eviction parameter tests
// =============================================================================

#[tokio::test]
async fn test_config_set_maxmemory() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Get initial maxmemory
    let response = client.command(&["CONFIG", "GET", "maxmemory"]).await;
    let initial_value = match &response {
        Response::Array(items) if items.len() >= 2 => match &items[1] {
            Response::Bulk(Some(b)) => String::from_utf8_lossy(b).to_string(),
            _ => panic!("Expected bulk string value"),
        },
        _ => panic!("Expected array response"),
    };

    // Set new maxmemory value
    let response = client
        .command(&["CONFIG", "SET", "maxmemory", "104857600"])
        .await;
    assert_ok(&response);

    // Verify the new value
    let response = client.command(&["CONFIG", "GET", "maxmemory"]).await;
    let items = unwrap_array(response);
    assert_eq!(items.len(), 2, "Should return key-value pair");
    match &items[1] {
        Response::Bulk(Some(b)) => {
            let value = String::from_utf8_lossy(b);
            assert_eq!(value, "104857600", "maxmemory should be updated");
        }
        _ => panic!("Expected bulk string value"),
    }

    // Restore original value
    let _ = client
        .command(&["CONFIG", "SET", "maxmemory", &initial_value])
        .await;

    server.shutdown().await;
}

#[tokio::test]
async fn test_config_set_maxmemory_policy() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set maxmemory-policy to allkeys-lru
    let response = client
        .command(&["CONFIG", "SET", "maxmemory-policy", "allkeys-lru"])
        .await;
    assert_ok(&response);

    // Verify the new value
    let response = client.command(&["CONFIG", "GET", "maxmemory-policy"]).await;
    let items = unwrap_array(response);
    assert_eq!(items.len(), 2, "Should return key-value pair");
    match &items[1] {
        Response::Bulk(Some(b)) => {
            let value = String::from_utf8_lossy(b);
            assert_eq!(value, "allkeys-lru", "maxmemory-policy should be updated");
        }
        _ => panic!("Expected bulk string value"),
    }

    // Test invalid policy
    let response = client
        .command(&["CONFIG", "SET", "maxmemory-policy", "invalid-policy"])
        .await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("Invalid value"),
                "Should reject invalid policy"
            );
        }
        _ => panic!("Expected error response for invalid policy"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_config_set_eviction_params() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Test maxmemory-samples
    let response = client
        .command(&["CONFIG", "SET", "maxmemory-samples", "10"])
        .await;
    assert_ok(&response);

    // Verify the value
    let response = client.command(&["CONFIG", "GET", "maxmemory-samples"]).await;
    let items = unwrap_array(response);
    match &items[1] {
        Response::Bulk(Some(b)) => {
            let value = String::from_utf8_lossy(b);
            assert_eq!(value, "10", "maxmemory-samples should be updated");
        }
        _ => panic!("Expected bulk string value"),
    }

    // Test lfu-log-factor
    let response = client
        .command(&["CONFIG", "SET", "lfu-log-factor", "5"])
        .await;
    assert_ok(&response);

    // Test lfu-decay-time
    let response = client
        .command(&["CONFIG", "SET", "lfu-decay-time", "2"])
        .await;
    assert_ok(&response);

    server.shutdown().await;
}

#[tokio::test]
async fn test_config_set_immutable_param() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Try to set an immutable parameter
    let response = client
        .command(&["CONFIG", "SET", "bind", "0.0.0.0"])
        .await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("not mutable"),
                "Should reject setting immutable param"
            );
        }
        _ => panic!("Expected error response for immutable param"),
    }

    // Try to set num-shards (also immutable)
    let response = client
        .command(&["CONFIG", "SET", "num-shards", "8"])
        .await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("not mutable"),
                "Should reject setting num-shards"
            );
        }
        _ => panic!("Expected error response for immutable param"),
    }

    server.shutdown().await;
}

// ============================================================================
// DEBUG HASHING tests
// ============================================================================

#[tokio::test]
async fn test_debug_hashing_single_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // DEBUG HASHING with a single key should return a simple string
    let response = client.command(&["DEBUG", "HASHING", "user:123"]).await;
    match response {
        Response::Simple(s) => {
            let text = String::from_utf8_lossy(&s);
            // Verify the output contains expected fields
            assert!(text.contains("key:user:123"), "Should contain key name");
            assert!(text.contains("hash_tag:(none)"), "Should show no hash tag");
            assert!(text.contains("hash:0x"), "Should contain hash value");
            assert!(text.contains("slot:"), "Should contain slot");
            assert!(text.contains("shard:"), "Should contain shard");
            assert!(text.contains("num_shards:"), "Should contain num_shards");
        }
        _ => panic!("Expected simple string response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_hashing_multiple_keys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // DEBUG HASHING with multiple keys should return an array
    let response = client
        .command(&["DEBUG", "HASHING", "key1", "key2", "key3"])
        .await;
    let items = unwrap_array(response);
    assert_eq!(items.len(), 3, "Should return 3 items for 3 keys");

    // Check first item
    if let Response::Bulk(Some(first)) = &items[0] {
        let text = String::from_utf8_lossy(first);
        assert!(text.contains("key:key1"), "First item should be key1");
    } else {
        panic!("Expected bulk string in array");
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_hashing_with_hash_tag() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Keys with hash tags should show the extracted tag
    let response = client.command(&["DEBUG", "HASHING", "{user}:profile"]).await;
    match response {
        Response::Simple(s) => {
            let text = String::from_utf8_lossy(&s);
            assert!(
                text.contains("hash_tag:user"),
                "Should show extracted hash tag 'user', got: {}",
                text
            );
        }
        _ => panic!("Expected simple string response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_hashing_same_hash_tag_same_slot() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Keys with the same hash tag should have the same slot and shard
    let response = client
        .command(&["DEBUG", "HASHING", "{user}:profile", "{user}:settings"])
        .await;
    let items = unwrap_array(response);
    assert_eq!(items.len(), 2, "Should return 2 items");

    let first = match &items[0] {
        Response::Bulk(Some(b)) => String::from_utf8_lossy(b).to_string(),
        _ => panic!("Expected bulk string"),
    };
    let second = match &items[1] {
        Response::Bulk(Some(b)) => String::from_utf8_lossy(b).to_string(),
        _ => panic!("Expected bulk string"),
    };

    // Extract slot values from both
    let slot1: u16 = first
        .split_whitespace()
        .find(|s| s.starts_with("slot:"))
        .unwrap()
        .strip_prefix("slot:")
        .unwrap()
        .parse()
        .unwrap();
    let slot2: u16 = second
        .split_whitespace()
        .find(|s| s.starts_with("slot:"))
        .unwrap()
        .strip_prefix("slot:")
        .unwrap()
        .parse()
        .unwrap();

    assert_eq!(slot1, slot2, "Keys with same hash tag should have same slot");

    // Both should show the same hash tag
    assert!(first.contains("hash_tag:user"), "First key should have hash tag 'user'");
    assert!(second.contains("hash_tag:user"), "Second key should have hash tag 'user'");

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_hashing_empty_braces() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Empty braces {} should not be treated as a hash tag
    let response = client.command(&["DEBUG", "HASHING", "{}:key"]).await;
    match response {
        Response::Simple(s) => {
            let text = String::from_utf8_lossy(&s);
            assert!(
                text.contains("hash_tag:(none)"),
                "Empty braces should not extract a hash tag, got: {}",
                text
            );
        }
        _ => panic!("Expected simple string response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_hashing_no_args_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // DEBUG HASHING without keys should return an error
    let response = client.command(&["DEBUG", "HASHING"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("wrong number of arguments"),
                "Should report wrong arity, got: {}",
                err_str
            );
        }
        _ => panic!("Expected error response, got {:?}", response),
    }

    server.shutdown().await;
}

// ============================================================================
// DEBUG TRACING tests
// ============================================================================

#[tokio::test]
async fn test_debug_tracing_status() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // DEBUG TRACING STATUS should return a bulk string with configuration
    let response = client.command(&["DEBUG", "TRACING", "STATUS"]).await;
    let data = unwrap_bulk(&response);
    let text = String::from_utf8_lossy(data);
    // Should contain expected fields
    assert!(text.contains("enabled:"), "Should contain enabled field");
    assert!(
        text.contains("sampling_rate:") || text.contains("reason:"),
        "Should contain sampling_rate or reason field"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_tracing_status_disabled_tracer() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // By default, tracing is disabled. Status should indicate this.
    let response = client.command(&["DEBUG", "TRACING", "STATUS"]).await;
    let data = unwrap_bulk(&response);
    let text = String::from_utf8_lossy(data);
    // Should indicate tracing is not enabled or tracer not configured
    assert!(
        text.contains("enabled:no") || text.contains("reason:tracer not configured"),
        "Should indicate tracing is disabled, got: {}",
        text
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_tracing_recent_empty() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // DEBUG TRACING RECENT should return an array (may be empty when tracing is disabled)
    let response = client.command(&["DEBUG", "TRACING", "RECENT"]).await;
    let entries = unwrap_array(response);
    // With tracing disabled, should be empty
    assert!(
        entries.is_empty(),
        "With tracing disabled, RECENT should return empty array"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_tracing_recent_with_count() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // DEBUG TRACING RECENT with count argument
    let response = client.command(&["DEBUG", "TRACING", "RECENT", "5"]).await;
    let entries = unwrap_array(response);
    // Should return at most 5 entries (0 when tracing is disabled)
    assert!(
        entries.len() <= 5,
        "Should return at most 5 entries, got {}",
        entries.len()
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_tracing_unknown_subcommand() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["DEBUG", "TRACING", "INVALID"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("Unknown DEBUG TRACING subcommand"),
                "Error should mention unknown subcommand, got: {}",
                err_str
            );
        }
        _ => panic!("Expected error response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_tracing_no_subcommand() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // DEBUG TRACING without a subcommand should return an error
    let response = client.command(&["DEBUG", "TRACING"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("Unknown DEBUG TRACING subcommand"),
                "Error should mention unknown subcommand, got: {}",
                err_str
            );
        }
        _ => panic!("Expected error response, got {:?}", response),
    }

    server.shutdown().await;
}

// =========================================================================
// DEBUG VLL tests
// =========================================================================

#[tokio::test]
async fn test_debug_vll_empty() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // DEBUG VLL should work and show empty state
    let response = client.command(&["DEBUG", "VLL"]).await;
    let data = unwrap_bulk(&response);
    let content = String::from_utf8_lossy(data);
    assert!(
        content.contains("VLL queues are empty"),
        "Expected empty VLL message, got: {}",
        content
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_vll_specific_shard() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // DEBUG VLL 0 should work and show shard 0
    let response = client.command(&["DEBUG", "VLL", "0"]).await;
    let data = unwrap_bulk(&response);
    let content = String::from_utf8_lossy(data);
    // Either empty message or shard:0 should be present
    assert!(
        content.contains("VLL queues are empty") || content.contains("shard:0"),
        "Expected shard 0 info or empty message, got: {}",
        content
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_vll_invalid_shard() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // DEBUG VLL 999 should return an error (assuming less than 1000 shards)
    let response = client.command(&["DEBUG", "VLL", "999"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("invalid shard_id"),
                "Error should mention invalid shard_id, got: {}",
                err_str
            );
        }
        _ => panic!("Expected error response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_vll_invalid_shard_format() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // DEBUG VLL abc should return an error
    let response = client.command(&["DEBUG", "VLL", "abc"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("invalid shard_id"),
                "Error should mention invalid shard_id, got: {}",
                err_str
            );
        }
        _ => panic!("Expected error response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_vll_with_pending_ops() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Run some operations to ensure the command doesn't crash during activity
    for i in 0..10 {
        let key = format!("test_key_{}", i);
        let _: Response = client.command(&["SET", &key, "value"]).await;
    }

    // DEBUG VLL should not crash
    let response = client.command(&["DEBUG", "VLL"]).await;
    match response {
        Response::Bulk(Some(_)) => {
            // Success - command completed without error
        }
        Response::Error(e) => {
            panic!(
                "DEBUG VLL should not error during normal operation: {}",
                String::from_utf8_lossy(&e)
            );
        }
        _ => panic!("Expected bulk response, got {:?}", response),
    }

    server.shutdown().await;
}
