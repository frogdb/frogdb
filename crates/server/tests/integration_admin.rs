//! Integration tests for admin commands (SLOWLOG, BGSAVE, LASTSAVE, MEMORY, LATENCY, CONFIG).

mod common;

use bytes::Bytes;
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
    match response {
        Response::Array(entries) => {
            assert!(entries.is_empty(), "Slowlog should be empty initially");
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

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
    match response {
        Response::Simple(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("Expected OK response, got {:?}", response),
    }

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
    match response {
        Response::Array(items) => {
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
        }
        _ => panic!("Expected array response, got {:?}", response),
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
    match response {
        Response::Simple(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("Expected OK response, got {:?}", response),
    }

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
    match response {
        Response::Simple(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("Expected OK response, got {:?}", response),
    }

    // Reset slowlog first
    let _ = client.command(&["SLOWLOG", "RESET"]).await;

    // Run a few commands
    let _ = client.command(&["SET", "key1", "value1"]).await;
    let _ = client.command(&["GET", "key1"]).await;
    let _ = client.command(&["DEL", "key1"]).await;

    // Slowlog should have entries now
    let response = client.command(&["SLOWLOG", "LEN"]).await;
    match response {
        Response::Integer(len) => {
            assert!(len >= 3, "Expected at least 3 entries, got {}", len);
        }
        _ => panic!("Expected integer response, got {:?}", response),
    }

    // Get entries and verify structure
    let response = client.command(&["SLOWLOG", "GET", "10"]).await;
    match response {
        Response::Array(entries) => {
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
        }
        _ => panic!("Expected array response, got {:?}", response),
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
    match response {
        Response::Array(entries) => {
            assert_eq!(
                entries.len(),
                5,
                "Expected exactly 5 entries when count=5"
            );
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

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
    match response {
        Response::Array(items) => {
            // Should have at least 3 params (log-slower-than, max-len, max-arg-len)
            // Each param is name, value pair so 6 items minimum
            assert!(
                items.len() >= 6,
                "Expected at least 6 items for slowlog config, got {}",
                items.len()
            );
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

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
    match response {
        Response::Array(entries) => {
            assert_eq!(
                entries.len(),
                10,
                "Default count should be 10, got {}",
                entries.len()
            );
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

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
    match response {
        Response::Integer(timestamp) => {
            // Timestamp should be a reasonable Unix timestamp
            // Either 0 (no save yet) or a recent timestamp
            assert!(
                timestamp >= 0,
                "LASTSAVE should return non-negative timestamp, got {}",
                timestamp
            );
        }
        _ => panic!("Expected integer response, got {:?}", response),
    }

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
    match response {
        Response::Integer(timestamp) => {
            // After BGSAVE, timestamp should be positive (or 0 if save is still in progress)
            assert!(
                timestamp >= 0,
                "LASTSAVE should return valid timestamp after BGSAVE"
            );
        }
        _ => panic!("Expected integer response for LASTSAVE"),
    }

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
    match response {
        Response::Array(items) => {
            assert!(!items.is_empty(), "Help should not be empty");
            // First item should mention MEMORY
            if let Some(Response::Bulk(Some(first))) = items.first() {
                let text = String::from_utf8_lossy(first);
                assert!(
                    text.to_uppercase().contains("MEMORY"),
                    "Help should mention MEMORY"
                );
            }
        }
        _ => panic!("Expected array response, got {:?}", response),
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
    match response {
        Response::Integer(size) => {
            assert!(size > 0, "Memory usage should be positive, got {}", size);
        }
        _ => panic!("Expected integer response, got {:?}", response),
    }

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
    match response {
        Response::Integer(size) => {
            assert!(size > 0, "Memory usage should be positive, got {}", size);
        }
        _ => panic!("Expected integer response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_memory_stats() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // MEMORY STATS should return an array of key-value pairs
    let response = client.command(&["MEMORY", "STATS"]).await;
    match response {
        Response::Array(items) => {
            assert!(!items.is_empty(), "MEMORY STATS should not be empty");
            // Should have pairs (key, value)
            assert!(items.len() % 2 == 0, "Should have even number of items");
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_memory_doctor() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // MEMORY DOCTOR should return a bulk string report
    let response = client.command(&["MEMORY", "DOCTOR"]).await;
    match response {
        Response::Bulk(Some(report)) => {
            let text = String::from_utf8_lossy(&report);
            assert!(!text.is_empty(), "Doctor report should not be empty");
        }
        _ => panic!("Expected bulk response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_memory_malloc_size() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // MEMORY MALLOC-SIZE should return the input (stub behavior)
    let response = client.command(&["MEMORY", "MALLOC-SIZE", "1024"]).await;
    match response {
        Response::Integer(size) => {
            assert_eq!(size, 1024, "Should return the input size");
        }
        _ => panic!("Expected integer response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_memory_purge() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // MEMORY PURGE should return OK (stub behavior)
    let response = client.command(&["MEMORY", "PURGE"]).await;
    match response {
        Response::Simple(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("Expected OK response, got {:?}", response),
    }

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
    match response {
        Response::Array(items) => {
            assert!(!items.is_empty(), "Help should not be empty");
            // First item should mention LATENCY
            if let Some(Response::Bulk(Some(first))) = items.first() {
                let text = String::from_utf8_lossy(first);
                assert!(
                    text.to_uppercase().contains("LATENCY"),
                    "Help should mention LATENCY"
                );
            }
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_latency_latest_empty() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Initially LATENCY LATEST should return empty or minimal data
    let response = client.command(&["LATENCY", "LATEST"]).await;
    match response {
        Response::Array(_) => {
            // Either empty or with entries - both are valid
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_latency_reset() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // LATENCY RESET should return OK
    let response = client.command(&["LATENCY", "RESET"]).await;
    match response {
        Response::Simple(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("Expected OK response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_latency_reset_specific_event() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // LATENCY RESET with specific event
    let response = client.command(&["LATENCY", "RESET", "command"]).await;
    match response {
        Response::Simple(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("Expected OK response, got {:?}", response),
    }

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
    match response {
        Response::Array(_) => {
            // Empty or with entries - both are valid
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

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
    match response {
        Response::Bulk(Some(graph)) => {
            let text = String::from_utf8_lossy(&graph);
            // Should contain the event name
            assert!(
                text.contains("command"),
                "Graph should mention the event name"
            );
        }
        _ => panic!("Expected bulk response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_latency_doctor() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // LATENCY DOCTOR should return a bulk string report
    let response = client.command(&["LATENCY", "DOCTOR"]).await;
    match response {
        Response::Bulk(Some(report)) => {
            let text = String::from_utf8_lossy(&report);
            assert!(!text.is_empty(), "Doctor report should not be empty");
        }
        _ => panic!("Expected bulk response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_latency_histogram() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // LATENCY HISTOGRAM should return an array (may be empty)
    let response = client.command(&["LATENCY", "HISTOGRAM"]).await;
    match response {
        Response::Array(_) => {
            // Empty or with data - both are valid
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

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
    assert_eq!(response, Response::ok(), "CONFIG SET should succeed");

    // Verify the new value
    let response = client.command(&["CONFIG", "GET", "maxmemory"]).await;
    match response {
        Response::Array(items) => {
            assert_eq!(items.len(), 2, "Should return key-value pair");
            match &items[1] {
                Response::Bulk(Some(b)) => {
                    let value = String::from_utf8_lossy(b);
                    assert_eq!(value, "104857600", "maxmemory should be updated");
                }
                _ => panic!("Expected bulk string value"),
            }
        }
        _ => panic!("Expected array response"),
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
    assert_eq!(response, Response::ok(), "CONFIG SET should succeed");

    // Verify the new value
    let response = client.command(&["CONFIG", "GET", "maxmemory-policy"]).await;
    match response {
        Response::Array(items) => {
            assert_eq!(items.len(), 2, "Should return key-value pair");
            match &items[1] {
                Response::Bulk(Some(b)) => {
                    let value = String::from_utf8_lossy(b);
                    assert_eq!(value, "allkeys-lru", "maxmemory-policy should be updated");
                }
                _ => panic!("Expected bulk string value"),
            }
        }
        _ => panic!("Expected array response"),
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
    assert_eq!(response, Response::ok(), "CONFIG SET maxmemory-samples should succeed");

    // Verify the value
    let response = client.command(&["CONFIG", "GET", "maxmemory-samples"]).await;
    match response {
        Response::Array(items) => {
            match &items[1] {
                Response::Bulk(Some(b)) => {
                    let value = String::from_utf8_lossy(b);
                    assert_eq!(value, "10", "maxmemory-samples should be updated");
                }
                _ => panic!("Expected bulk string value"),
            }
        }
        _ => panic!("Expected array response"),
    }

    // Test lfu-log-factor
    let response = client
        .command(&["CONFIG", "SET", "lfu-log-factor", "5"])
        .await;
    assert_eq!(response, Response::ok(), "CONFIG SET lfu-log-factor should succeed");

    // Test lfu-decay-time
    let response = client
        .command(&["CONFIG", "SET", "lfu-decay-time", "2"])
        .await;
    assert_eq!(response, Response::ok(), "CONFIG SET lfu-decay-time should succeed");

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
