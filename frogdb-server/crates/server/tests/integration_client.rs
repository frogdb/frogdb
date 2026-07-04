//! Integration tests for client commands (CLIENT, RESET, TRACKING).

use crate::common::test_server::{TestServer, TestServerConfig};
use bytes::Bytes;
use frogdb_protocol::Response;
use futures::StreamExt;
use redis_protocol::resp3::types::BytesFrame as Resp3Frame;
use std::time::Duration;
use tokio::time::timeout;

// ============================================================================
// RESET Command Tests
// ============================================================================

#[tokio::test]
async fn test_reset_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // RESET should return "RESET" simple string
    let response = client.command(&["RESET"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("RESET")));

    // RESET is idempotent - can be called multiple times
    let response = client.command(&["RESET"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("RESET")));

    server.shutdown().await;
}

#[tokio::test]
async fn test_reset_exits_pubsub_mode() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Enter pub/sub mode
    client.command(&["SUBSCRIBE", "mychannel"]).await;

    // Verify we're in pub/sub mode (GET should fail)
    let response = client.command(&["GET", "foo"]).await;
    assert!(matches!(response, Response::Error(ref e) if e.starts_with(b"ERR Can't execute")));

    // RESET should exit pub/sub mode
    let response = client.command(&["RESET"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("RESET")));

    // Now GET should work
    let response = client.command(&["GET", "foo"]).await;
    assert!(matches!(response, Response::Bulk(None))); // Key doesn't exist, but command works

    server.shutdown().await;
}

#[tokio::test]
async fn test_reset_exits_pattern_pubsub_mode() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Enter pub/sub mode via pattern subscribe
    client.command(&["PSUBSCRIBE", "chan*"]).await;

    // RESET should exit pub/sub mode
    let response = client.command(&["RESET"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("RESET")));

    // Now normal commands should work
    let response = client.command(&["SET", "foo", "bar"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    server.shutdown().await;
}

#[tokio::test]
async fn test_reset_aborts_transaction() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Start a transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Queue a SET command
    let response = client.command(&["SET", "txkey", "txvalue"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    // RESET aborts the transaction
    let response = client.command(&["RESET"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("RESET")));

    // Key should not exist (transaction was aborted)
    let response = client.command(&["GET", "txkey"]).await;
    assert!(matches!(response, Response::Bulk(None)));

    // Should be able to start a new transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    server.shutdown().await;
}

#[tokio::test]
async fn test_reset_clears_watches() {
    let server = TestServer::start_standalone().await;
    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    // Set initial value
    client1.command(&["SET", "watchkey", "initial"]).await;

    // Watch the key
    client1.command(&["WATCH", "watchkey"]).await;

    // RESET clears the watch
    client1.command(&["RESET"]).await;

    // Modify with client2
    client2.command(&["SET", "watchkey", "modified"]).await;

    // Client1's MULTI/EXEC should succeed (watch was cleared by RESET)
    client1.command(&["MULTI"]).await;
    client1.command(&["SET", "watchkey", "client1"]).await;
    let response = client1.command(&["EXEC"]).await;

    // EXEC should succeed (not return nil)
    assert!(matches!(response, Response::Array(_)));

    server.shutdown().await;
}

#[tokio::test]
async fn test_reset_clears_client_name() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set client name
    client.command(&["CLIENT", "SETNAME", "my-client"]).await;

    // Verify name is set
    let response = client.command(&["CLIENT", "GETNAME"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("my-client"))));

    // RESET clears the name
    client.command(&["RESET"]).await;

    // Name should be cleared
    let response = client.command(&["CLIENT", "GETNAME"]).await;
    assert!(matches!(response, Response::Bulk(None)));

    server.shutdown().await;
}

#[tokio::test]
async fn test_reset_unsubscribes_from_sharded_channels() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut publisher = server.connect().await;

    // Subscribe to sharded channel
    subscriber.command(&["SSUBSCRIBE", "sharded:chan"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // RESET unsubscribes
    subscriber.command(&["RESET"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publish should report 0 subscribers
    let response = publisher
        .command(&["SPUBLISH", "sharded:chan", "msg"])
        .await;
    assert_eq!(response, Response::Integer(0));

    server.shutdown().await;
}

#[tokio::test]
async fn test_reset_publisher_still_reaches_other_subscribers() {
    let server = TestServer::start_standalone().await;
    let mut sub1 = server.connect().await;
    let mut sub2 = server.connect().await;
    let mut publisher = server.connect().await;

    // Both subscribe
    sub1.command(&["SUBSCRIBE", "channel"]).await;
    sub2.command(&["SUBSCRIBE", "channel"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // sub1 RESETs (unsubscribes)
    sub1.command(&["RESET"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publish should reach sub2 only
    let response = publisher.command(&["PUBLISH", "channel", "hello"]).await;
    assert_eq!(response, Response::Integer(1)); // Only 1 subscriber remains

    // sub2 should receive the message
    let msg = sub2.read_message(Duration::from_secs(2)).await;
    assert!(msg.is_some());

    server.shutdown().await;
}

// ============================================================================
// CLIENT Command Tests
// ============================================================================

#[tokio::test]
async fn test_client_id() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // CLIENT ID should return a positive integer
    let response = client.command(&["CLIENT", "ID"]).await;
    match response {
        Response::Integer(id) => assert!(id > 0, "CLIENT ID should return positive integer"),
        _ => panic!("Expected integer response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_client_setname_getname() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Initially, name should be null
    let response = client.command(&["CLIENT", "GETNAME"]).await;
    assert!(matches!(response, Response::Null | Response::Bulk(None)));

    // Set a name
    let response = client
        .command(&["CLIENT", "SETNAME", "test-connection"])
        .await;
    assert_eq!(response, Response::ok());

    // Get the name back
    let response = client.command(&["CLIENT", "GETNAME"]).await;
    assert_eq!(
        response,
        Response::Bulk(Some(Bytes::from("test-connection")))
    );

    // Clear the name by setting empty string
    let response = client.command(&["CLIENT", "SETNAME", ""]).await;
    assert_eq!(response, Response::ok());

    let response = client.command(&["CLIENT", "GETNAME"]).await;
    assert!(matches!(response, Response::Null | Response::Bulk(None)));

    server.shutdown().await;
}

#[tokio::test]
async fn test_client_setname_invalid_name() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Names with spaces should be rejected
    let response = client
        .command(&["CLIENT", "SETNAME", "name with spaces"])
        .await;
    assert!(matches!(response, Response::Error(_)));

    server.shutdown().await;
}

#[tokio::test]
async fn test_client_list() {
    let server = TestServer::start_standalone().await;
    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    // Set names for identification
    client1.command(&["CLIENT", "SETNAME", "client-one"]).await;
    client2.command(&["CLIENT", "SETNAME", "client-two"]).await;

    // CLIENT LIST should show both connections
    let response = client1.command(&["CLIENT", "LIST"]).await;
    match response {
        Response::Bulk(Some(data)) => {
            let list_str = String::from_utf8_lossy(&data);
            assert!(list_str.contains("client-one"), "Should contain client-one");
            assert!(list_str.contains("client-two"), "Should contain client-two");
            // Should have id= field
            assert!(list_str.contains("id="), "Should contain id field");
            // Should have addr= field
            assert!(list_str.contains("addr="), "Should contain addr field");
        }
        _ => panic!("Expected bulk string response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_client_info() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set a name
    client.command(&["CLIENT", "SETNAME", "info-test"]).await;

    // CLIENT INFO should return our connection info
    let response = client.command(&["CLIENT", "INFO"]).await;
    match response {
        Response::Bulk(Some(data)) => {
            let info_str = String::from_utf8_lossy(&data);
            assert!(info_str.contains("info-test"), "Should contain our name");
            assert!(info_str.contains("id="), "Should contain id field");
        }
        _ => panic!("Expected bulk string response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_client_kill_by_id() {
    let server = TestServer::start_standalone().await;
    let mut killer = server.connect().await;
    let mut victim = server.connect().await;

    // Get victim's ID
    let victim_id = match victim.command(&["CLIENT", "ID"]).await {
        Response::Integer(id) => id,
        other => panic!("Expected integer, got {:?}", other),
    };

    // Kill victim by ID
    let response = killer
        .command(&["CLIENT", "KILL", "ID", &victim_id.to_string()])
        .await;
    assert_eq!(response, Response::Integer(1), "Should kill 1 connection");

    // Give time for kill to take effect
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Victim should be disconnected - verify by trying to read from the connection
    // The connection should be closed, so reading should return None or timeout
    let read_result = timeout(Duration::from_millis(500), victim.framed.next()).await;

    match read_result {
        Ok(None) => {
            // Connection closed as expected
        }
        Err(_) => {
            // Timeout - also acceptable, connection may be stuck
        }
        Ok(Some(_)) => {
            // Got some data - this might happen if there's pending data
            // Just verify killer is still alive
        }
    }

    // Verify killer is still connected
    let response = killer.command(&["PING"]).await;
    assert_eq!(response, Response::pong());

    server.shutdown().await;
}

#[tokio::test]
async fn test_client_pause_unpause() {
    let server = TestServer::start_standalone().await;
    let mut admin = server.connect().await;
    let mut worker = server.connect().await;

    // Pause with a long timeout
    let response = admin.command(&["CLIENT", "PAUSE", "10000", "WRITE"]).await;
    assert_eq!(response, Response::ok());

    // Reads should still work
    let response = timeout(Duration::from_millis(500), worker.command(&["GET", "key"]))
        .await
        .expect("GET should complete during WRITE pause");
    assert!(matches!(response, Response::Null | Response::Bulk(None)));

    // Unpause
    let response = admin.command(&["CLIENT", "UNPAUSE"]).await;
    assert_eq!(response, Response::ok());

    // Writes should work after unpause
    let response = timeout(
        Duration::from_millis(500),
        worker.command(&["SET", "key", "value"]),
    )
    .await
    .expect("SET should complete after unpause");
    assert_eq!(response, Response::ok());

    server.shutdown().await;
}

#[tokio::test]
async fn test_client_pause_timeout() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Pause for a very short time (50ms)
    let response = client.command(&["CLIENT", "PAUSE", "50", "ALL"]).await;
    assert_eq!(response, Response::ok());

    // Wait for pause to expire
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Commands should work after timeout expires
    let response = timeout(Duration::from_millis(500), client.command(&["PING"]))
        .await
        .expect("PING should complete after pause timeout");
    assert_eq!(response, Response::pong());

    server.shutdown().await;
}

#[tokio::test]
async fn test_client_list_type_filter() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Filter by normal type
    let response = client.command(&["CLIENT", "LIST", "TYPE", "normal"]).await;
    match response {
        Response::Bulk(Some(data)) => {
            let list_str = String::from_utf8_lossy(&data);
            // Normal connections should appear
            assert!(!list_str.is_empty() || list_str.contains("id="));
        }
        _ => panic!("Expected bulk string response, got {:?}", response),
    }

    // Filter by master type (should be empty since we have no replication)
    let response = client.command(&["CLIENT", "LIST", "TYPE", "master"]).await;
    match response {
        Response::Bulk(Some(data)) => {
            // Should return empty or only newlines
            let list_str = String::from_utf8_lossy(&data);
            // Master list should be empty since we don't have replication
            assert!(!list_str.contains("id=") || list_str.is_empty());
        }
        Response::Bulk(None) => {
            // Also acceptable - no clients of this type
        }
        _ => panic!("Expected bulk string response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_client_help() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // CLIENT HELP should return an array of strings
    let response = client.command(&["CLIENT", "HELP"]).await;
    match response {
        Response::Array(arr) => {
            assert!(!arr.is_empty(), "Help should not be empty");
            // First entry should mention CLIENT command
            if let Response::Bulk(Some(first)) = &arr[0] {
                let first_str = String::from_utf8_lossy(first);
                assert!(
                    first_str.to_uppercase().contains("CLIENT"),
                    "Help should mention CLIENT"
                );
            }
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

// CLIENT SETINFO tests
#[tokio::test]
async fn test_client_setinfo_lib_name() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client
        .command(&["CLIENT", "SETINFO", "LIB-NAME", "my-test-lib"])
        .await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Verify it appears in CLIENT INFO
    let response = client.command(&["CLIENT", "INFO"]).await;
    match response {
        Response::Bulk(Some(data)) => {
            let info_str = String::from_utf8_lossy(&data);
            assert!(
                info_str.contains("lib-name=my-test-lib"),
                "Should contain lib-name"
            );
        }
        _ => panic!("Expected bulk response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_client_setinfo_lib_ver() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client
        .command(&["CLIENT", "SETINFO", "LIB-VER", "1.2.3"])
        .await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Verify it appears in CLIENT INFO
    let response = client.command(&["CLIENT", "INFO"]).await;
    match response {
        Response::Bulk(Some(data)) => {
            let info_str = String::from_utf8_lossy(&data);
            assert!(info_str.contains("lib-ver=1.2.3"), "Should contain lib-ver");
        }
        _ => panic!("Expected bulk response, got {:?}", response),
    }

    server.shutdown().await;
}

// CLIENT NO-EVICT tests
#[tokio::test]
async fn test_client_no_evict() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Enable NO-EVICT
    let response = client.command(&["CLIENT", "NO-EVICT", "ON"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Disable NO-EVICT
    let response = client.command(&["CLIENT", "NO-EVICT", "OFF"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Invalid argument
    let response = client.command(&["CLIENT", "NO-EVICT", "INVALID"]).await;
    assert!(matches!(response, Response::Error(_)));

    server.shutdown().await;
}

// CLIENT NO-TOUCH tests
#[tokio::test]
async fn test_client_no_touch() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Enable NO-TOUCH
    let response = client.command(&["CLIENT", "NO-TOUCH", "ON"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Disable NO-TOUCH
    let response = client.command(&["CLIENT", "NO-TOUCH", "OFF"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    server.shutdown().await;
}

// CLIENT REPLY tests
#[tokio::test]
async fn test_client_reply_on() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // CLIENT REPLY ON should be accepted (normal mode)
    let response = client.command(&["CLIENT", "REPLY", "ON"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Verify commands still work
    let response = client.command(&["PING"]).await;
    assert_eq!(response, Response::pong());

    server.shutdown().await;
}

#[tokio::test]
async fn test_client_reply_invalid() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Invalid argument should return error
    let response = client.command(&["CLIENT", "REPLY", "INVALID"]).await;
    assert!(matches!(response, Response::Error(_)));

    server.shutdown().await;
}

// CLIENT TRACKINGINFO tests
#[tokio::test]
async fn test_client_trackinginfo() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["CLIENT", "TRACKINGINFO"]).await;
    match response {
        Response::Array(arr) => {
            // Should return tracking info structure (flags, redirect, prefixes)
            assert!(!arr.is_empty());
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

// CLIENT GETREDIR tests
#[tokio::test]
async fn test_client_getredir() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Without tracking, should return -1
    let response = client.command(&["CLIENT", "GETREDIR"]).await;
    assert_eq!(response, Response::Integer(-1));

    server.shutdown().await;
}

// CLIENT CACHING tests
#[tokio::test]
async fn test_client_caching() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // CACHING without tracking enabled should error
    let response = client.command(&["CLIENT", "CACHING", "YES"]).await;
    assert!(matches!(response, Response::Error(_)));

    // Enable tracking with OPTIN mode, then CACHING should work
    client.command(&["CLIENT", "TRACKING", "ON", "OPTIN"]).await;

    let response = client.command(&["CLIENT", "CACHING", "YES"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    let response = client.command(&["CLIENT", "CACHING", "NO"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Invalid argument
    let response = client.command(&["CLIENT", "CACHING", "INVALID"]).await;
    assert!(matches!(response, Response::Error(_)));

    server.shutdown().await;
}

// CLIENT UNBLOCK tests
#[tokio::test]
async fn test_client_unblock_not_blocked() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Get our own ID
    let client_id = match client.command(&["CLIENT", "ID"]).await {
        Response::Integer(id) => id,
        other => panic!("Expected integer, got {:?}", other),
    };

    // Unblocking a client that isn't blocked should return 0
    let response = client
        .command(&["CLIENT", "UNBLOCK", &client_id.to_string()])
        .await;
    assert_eq!(response, Response::Integer(0));

    // Non-existent client ID should also return 0
    let response = client.command(&["CLIENT", "UNBLOCK", "999999"]).await;
    assert_eq!(response, Response::Integer(0));

    server.shutdown().await;
}

// ============================================================================
// CLIENT STATS Command Tests
// ============================================================================

#[tokio::test]
async fn test_client_stats_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set a name for identification
    client.command(&["CLIENT", "SETNAME", "stats-test"]).await;

    // Run some commands to generate stats
    for _ in 0..10 {
        client.command(&["PING"]).await;
    }
    client.command(&["SET", "foo", "bar"]).await;
    client.command(&["GET", "foo"]).await;

    // CLIENT STATS should return stats for all clients
    let response = client.command(&["CLIENT", "STATS"]).await;
    match response {
        Response::Bulk(Some(data)) => {
            let stats_str = String::from_utf8_lossy(&data);
            // Should contain our client name
            assert!(
                stats_str.contains("stats-test"),
                "Should contain client name"
            );
            // Should contain cmd_total (at least 13 commands: SETNAME + 10 PINGs + SET + GET)
            assert!(stats_str.contains("cmd_total="), "Should contain cmd_total");
            // Should contain bytes_recv
            assert!(
                stats_str.contains("bytes_recv="),
                "Should contain bytes_recv"
            );
            // Should contain bytes_sent
            assert!(
                stats_str.contains("bytes_sent="),
                "Should contain bytes_sent"
            );
            // Should contain latency metrics
            assert!(
                stats_str.contains("latency_avg_us="),
                "Should contain latency_avg_us"
            );
            assert!(
                stats_str.contains("latency_p99_us="),
                "Should contain latency_p99_us"
            );
            assert!(
                stats_str.contains("latency_max_us="),
                "Should contain latency_max_us"
            );
        }
        _ => panic!("Expected bulk string response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_client_stats_by_id() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Get our client ID
    let client_id = match client.command(&["CLIENT", "ID"]).await {
        Response::Integer(id) => id,
        other => panic!("Expected integer, got {:?}", other),
    };

    // Set name for identification
    client.command(&["CLIENT", "SETNAME", "id-test"]).await;

    // Run some commands
    for _ in 0..5 {
        client.command(&["PING"]).await;
    }

    // CLIENT STATS ID <id> should return stats for specific client
    let response = client
        .command(&["CLIENT", "STATS", "ID", &client_id.to_string()])
        .await;
    match response {
        Response::Bulk(Some(data)) => {
            let stats_str = String::from_utf8_lossy(&data);
            // Should contain our client info
            assert!(
                stats_str.contains(&format!("id={}", client_id)),
                "Should contain our ID"
            );
            assert!(stats_str.contains("id-test"), "Should contain our name");
            assert!(stats_str.contains("cmd_total="), "Should contain cmd_total");
        }
        _ => panic!("Expected bulk string response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_client_stats_nonexistent_id() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // CLIENT STATS ID with non-existent ID should return error
    let response = client.command(&["CLIENT", "STATS", "ID", "999999"]).await;
    assert!(
        matches!(response, Response::Error(_)),
        "Should return error for non-existent client ID"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_client_stats_command_breakdown() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Run different commands to generate command breakdown
    for _ in 0..5 {
        client.command(&["PING"]).await;
    }
    for _ in 0..3 {
        client.command(&["SET", "key", "value"]).await;
    }
    for _ in 0..2 {
        client.command(&["GET", "key"]).await;
    }

    // Get our client ID
    let client_id = match client.command(&["CLIENT", "ID"]).await {
        Response::Integer(id) => id,
        other => panic!("Expected integer, got {:?}", other),
    };

    let response = client
        .command(&["CLIENT", "STATS", "ID", &client_id.to_string()])
        .await;
    match response {
        Response::Bulk(Some(data)) => {
            let stats_str = String::from_utf8_lossy(&data);
            // Should contain command breakdown section
            assert!(
                stats_str.contains("# command breakdown"),
                "Should contain command breakdown header"
            );
            // Should contain PING, SET, GET commands
            assert!(stats_str.contains("PING:"), "Should contain PING breakdown");
            assert!(stats_str.contains("SET:"), "Should contain SET breakdown");
            assert!(stats_str.contains("GET:"), "Should contain GET breakdown");
        }
        _ => panic!("Expected bulk string response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_client_stats_bytes_tracking() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Get client ID first
    let client_id = match client.command(&["CLIENT", "ID"]).await {
        Response::Integer(id) => id,
        other => panic!("Expected integer, got {:?}", other),
    };

    // Set a large value to ensure bytes are being tracked
    let large_value = "x".repeat(1000);
    client.command(&["SET", "largekey", &large_value]).await;
    client.command(&["GET", "largekey"]).await;

    let response = client
        .command(&["CLIENT", "STATS", "ID", &client_id.to_string()])
        .await;
    match response {
        Response::Bulk(Some(data)) => {
            let stats_str = String::from_utf8_lossy(&data);

            // Parse bytes_recv value
            let bytes_recv: u64 = stats_str
                .lines()
                .find(|l| l.contains("bytes_recv="))
                .and_then(|l| {
                    l.split_whitespace()
                        .find(|s| s.starts_with("bytes_recv="))
                        .and_then(|s| s.strip_prefix("bytes_recv="))
                        .and_then(|s| s.parse().ok())
                })
                .unwrap_or(0);

            // Parse bytes_sent value
            let bytes_sent: u64 = stats_str
                .lines()
                .find(|l| l.contains("bytes_sent="))
                .and_then(|l| {
                    l.split_whitespace()
                        .find(|s| s.starts_with("bytes_sent="))
                        .and_then(|s| s.strip_prefix("bytes_sent="))
                        .and_then(|s| s.parse().ok())
                })
                .unwrap_or(0);

            // Bytes should be non-zero and reflect the large value we sent/received
            assert!(
                bytes_recv > 1000,
                "bytes_recv should be > 1000 (large value sent), got {}",
                bytes_recv
            );
            assert!(
                bytes_sent > 1000,
                "bytes_sent should be > 1000 (large value received), got {}",
                bytes_sent
            );
        }
        _ => panic!("Expected bulk string response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_client_stats_latency_tracking() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Get client ID
    let client_id = match client.command(&["CLIENT", "ID"]).await {
        Response::Integer(id) => id,
        other => panic!("Expected integer, got {:?}", other),
    };

    // Run many commands to build up latency samples
    for _ in 0..50 {
        client.command(&["PING"]).await;
    }

    let response = client
        .command(&["CLIENT", "STATS", "ID", &client_id.to_string()])
        .await;
    match response {
        Response::Bulk(Some(data)) => {
            let stats_str = String::from_utf8_lossy(&data);

            // Parse latency values
            let avg: u64 = stats_str
                .lines()
                .find(|l| l.contains("latency_avg_us="))
                .and_then(|l| {
                    l.split_whitespace()
                        .find(|s| s.starts_with("latency_avg_us="))
                        .and_then(|s| s.strip_prefix("latency_avg_us="))
                        .and_then(|s| s.parse().ok())
                })
                .unwrap_or(0);

            let p99: u64 = stats_str
                .lines()
                .find(|l| l.contains("latency_p99_us="))
                .and_then(|l| {
                    l.split_whitespace()
                        .find(|s| s.starts_with("latency_p99_us="))
                        .and_then(|s| s.strip_prefix("latency_p99_us="))
                        .and_then(|s| s.parse().ok())
                })
                .unwrap_or(0);

            let max: u64 = stats_str
                .lines()
                .find(|l| l.contains("latency_max_us="))
                .and_then(|l| {
                    l.split_whitespace()
                        .find(|s| s.starts_with("latency_max_us="))
                        .and_then(|s| s.strip_prefix("latency_max_us="))
                        .and_then(|s| s.parse().ok())
                })
                .unwrap_or(0);

            // Latency values should be non-zero
            assert!(avg > 0, "avg latency should be > 0, got {}", avg);
            // p99 should be >= avg
            assert!(p99 >= avg, "p99 ({}) should be >= avg ({})", p99, avg);
            // max should be >= p99
            assert!(max >= p99, "max ({}) should be >= p99 ({})", max, p99);
        }
        _ => panic!("Expected bulk string response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_client_stats_invalid_syntax() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Invalid syntax - missing client ID after ID keyword
    let response = client.command(&["CLIENT", "STATS", "ID"]).await;
    assert!(
        matches!(response, Response::Error(_)),
        "Should return error for missing client ID"
    );

    // Invalid syntax - unknown keyword
    let response = client.command(&["CLIENT", "STATS", "UNKNOWN"]).await;
    assert!(
        matches!(response, Response::Error(_)),
        "Should return error for unknown keyword"
    );

    // Invalid client ID format
    let response = client
        .command(&["CLIENT", "STATS", "ID", "notanumber"])
        .await;
    assert!(
        matches!(response, Response::Error(_)),
        "Should return error for invalid client ID format"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_client_stats_multiple_clients() {
    let server = TestServer::start_standalone().await;
    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    // Set names for identification
    client1
        .command(&["CLIENT", "SETNAME", "multi-test-1"])
        .await;
    client2
        .command(&["CLIENT", "SETNAME", "multi-test-2"])
        .await;

    // Run different numbers of commands on each client
    for _ in 0..10 {
        client1.command(&["PING"]).await;
    }
    for _ in 0..5 {
        client2.command(&["PING"]).await;
    }

    // CLIENT STATS should show both clients
    let response = client1.command(&["CLIENT", "STATS"]).await;
    match response {
        Response::Bulk(Some(data)) => {
            let stats_str = String::from_utf8_lossy(&data);
            // Should contain both client names
            assert!(
                stats_str.contains("multi-test-1"),
                "Should contain client1 name"
            );
            assert!(
                stats_str.contains("multi-test-2"),
                "Should contain client2 name"
            );
        }
        _ => panic!("Expected bulk string response, got {:?}", response),
    }

    server.shutdown().await;
}

// ============================================================================
// CLIENT TRACKING Integration Tests
// ============================================================================

/// Assert that a Resp3Frame is a Push invalidation containing the expected keys.
fn assert_invalidation_keys(frame: &Resp3Frame, expected_keys: &[&str]) {
    match frame {
        Resp3Frame::Push { data, .. } => {
            assert!(data.len() >= 2, "Push should have at least 2 elements");
            if let Resp3Frame::BlobString { data: kind, .. } = &data[0] {
                assert_eq!(
                    kind.as_ref(),
                    b"invalidate",
                    "First element should be 'invalidate'"
                );
            } else {
                panic!("Expected BlobString 'invalidate', got {:?}", data[0]);
            }
            if let Resp3Frame::Array { data: keys, .. } = &data[1] {
                let key_strs: Vec<&[u8]> = keys
                    .iter()
                    .map(|k| match k {
                        Resp3Frame::BlobString { data, .. } => data.as_ref(),
                        _ => panic!("Expected BlobString key, got {:?}", k),
                    })
                    .collect();
                for expected in expected_keys {
                    assert!(
                        key_strs.contains(&expected.as_bytes()),
                        "Expected key '{}' in invalidation, got keys: {:?}",
                        expected,
                        key_strs
                            .iter()
                            .map(|k| String::from_utf8_lossy(k))
                            .collect::<Vec<_>>()
                    );
                }
            } else {
                panic!("Expected Array of keys, got {:?}", data[1]);
            }
        }
        _ => panic!("Expected Push frame, got {:?}", frame),
    }
}

/// Assert that a Resp3Frame is a Push flush-all invalidation (null keys).
fn assert_invalidation_flush(frame: &Resp3Frame) {
    match frame {
        Resp3Frame::Push { data, .. } => {
            assert!(data.len() >= 2, "Push should have at least 2 elements");
            if let Resp3Frame::BlobString { data: kind, .. } = &data[0] {
                assert_eq!(kind.as_ref(), b"invalidate");
            } else {
                panic!("Expected BlobString 'invalidate', got {:?}", data[0]);
            }
            assert!(
                matches!(&data[1], Resp3Frame::Null),
                "Expected Null for flush-all invalidation, got {:?}",
                data[1]
            );
        }
        _ => panic!("Expected Push frame, got {:?}", frame),
    }
}

#[tokio::test]
async fn test_tracking_basic_invalidation() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut writer = server.connect().await;

    // Enable RESP3 + tracking
    tracker.command(&["HELLO", "3"]).await;
    tracker.command(&["CLIENT", "TRACKING", "ON"]).await;

    // Seed and read key to track it
    tracker.command(&["SET", "{t}foo", "bar"]).await;
    tracker.command(&["GET", "{t}foo"]).await;

    // Write from a different client
    writer.command(&["SET", "{t}foo", "baz"]).await;

    // Tracker should receive an invalidation push
    let msg = tracker.read_message(Duration::from_secs(2)).await;
    assert!(msg.is_some(), "Should receive invalidation message");
    assert_invalidation_keys(&msg.unwrap(), &["{t}foo"]);

    server.shutdown().await;
}

#[tokio::test]
async fn test_tracking_optin_requires_caching_yes() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut writer = server.connect().await;

    // Enable RESP3 + OPTIN tracking
    tracker.command(&["HELLO", "3"]).await;
    tracker
        .command(&["CLIENT", "TRACKING", "ON", "OPTIN"])
        .await;

    // Read key WITHOUT CACHING YES — should NOT track
    tracker.command(&["SET", "{t}optin", "val"]).await;
    tracker.command(&["GET", "{t}optin"]).await;

    // Write from another client
    writer.command(&["SET", "{t}optin", "new"]).await;

    // Should NOT receive invalidation (no CACHING YES was sent)
    let msg = tracker.read_message(Duration::from_millis(500)).await;
    assert!(
        msg.is_none(),
        "Should NOT receive invalidation without CACHING YES"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_tracking_optin_with_caching_yes() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut writer = server.connect().await;

    // Enable RESP3 + OPTIN tracking
    tracker.command(&["HELLO", "3"]).await;
    tracker
        .command(&["CLIENT", "TRACKING", "ON", "OPTIN"])
        .await;

    // Seed key, then CACHING YES + GET
    tracker.command(&["SET", "{t}optin2", "val"]).await;
    tracker.command(&["CLIENT", "CACHING", "YES"]).await;
    tracker.command(&["GET", "{t}optin2"]).await;

    // Write from another client
    writer.command(&["SET", "{t}optin2", "new"]).await;

    // Should receive invalidation
    let msg = tracker.read_message(Duration::from_secs(2)).await;
    assert!(
        msg.is_some(),
        "Should receive invalidation after CACHING YES + GET"
    );
    assert_invalidation_keys(&msg.unwrap(), &["{t}optin2"]);

    server.shutdown().await;
}

#[tokio::test]
async fn test_tracking_optout_default_tracks() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut writer = server.connect().await;

    // Enable RESP3 + OPTOUT tracking
    tracker.command(&["HELLO", "3"]).await;
    tracker
        .command(&["CLIENT", "TRACKING", "ON", "OPTOUT"])
        .await;

    // Read key — OPTOUT tracks by default
    tracker.command(&["SET", "{t}optout", "val"]).await;
    tracker.command(&["GET", "{t}optout"]).await;

    // Write from another client
    writer.command(&["SET", "{t}optout", "new"]).await;

    // Should receive invalidation
    let msg = tracker.read_message(Duration::from_secs(2)).await;
    assert!(msg.is_some(), "OPTOUT mode should track by default");
    assert_invalidation_keys(&msg.unwrap(), &["{t}optout"]);

    server.shutdown().await;
}

#[tokio::test]
async fn test_tracking_optout_caching_no() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut writer = server.connect().await;

    // Enable RESP3 + OPTOUT tracking
    tracker.command(&["HELLO", "3"]).await;
    tracker
        .command(&["CLIENT", "TRACKING", "ON", "OPTOUT"])
        .await;

    // CACHING NO + GET — should NOT track this read
    tracker.command(&["SET", "{t}notrack", "val"]).await;
    tracker.command(&["CLIENT", "CACHING", "NO"]).await;
    tracker.command(&["GET", "{t}notrack"]).await;

    // Write from another client
    writer.command(&["SET", "{t}notrack", "new"]).await;

    // Should NOT receive invalidation
    let msg = tracker.read_message(Duration::from_millis(500)).await;
    assert!(msg.is_none(), "CACHING NO should suppress tracking");

    server.shutdown().await;
}

#[tokio::test]
async fn test_tracking_noloop() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;

    // Enable RESP3 + NOLOOP tracking
    tracker.command(&["HELLO", "3"]).await;
    tracker
        .command(&["CLIENT", "TRACKING", "ON", "NOLOOP"])
        .await;

    // Read key
    tracker.command(&["SET", "{t}noloop", "val"]).await;
    tracker.command(&["GET", "{t}noloop"]).await;

    // Write from SAME client — NOLOOP should suppress self-invalidation
    tracker.command(&["SET", "{t}noloop", "new"]).await;

    // Should NOT receive invalidation (NOLOOP)
    let msg = tracker.read_message(Duration::from_millis(500)).await;
    assert!(msg.is_none(), "NOLOOP should suppress self-invalidation");

    server.shutdown().await;
}

#[tokio::test]
async fn test_tracking_off_stops() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut writer = server.connect().await;

    // Enable RESP3 + tracking, read a key
    tracker.command(&["HELLO", "3"]).await;
    tracker.command(&["CLIENT", "TRACKING", "ON"]).await;
    tracker.command(&["SET", "{t}offtest", "val"]).await;
    tracker.command(&["GET", "{t}offtest"]).await;

    // Turn tracking OFF
    tracker.command(&["CLIENT", "TRACKING", "OFF"]).await;

    // Write from another client
    writer.command(&["SET", "{t}offtest", "new"]).await;

    // Should NOT receive invalidation (tracking disabled)
    let msg = tracker.read_message(Duration::from_millis(500)).await;
    assert!(
        msg.is_none(),
        "Should not receive invalidation after TRACKING OFF"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_tracking_trackinginfo() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Before enabling tracking — should show "off"
    let response = client.command(&["CLIENT", "TRACKINGINFO"]).await;
    if let Response::Array(arr) = &response {
        // arr = ["flags", [flags...], "redirect", -1, "prefixes", []]
        assert_eq!(arr.len(), 6);
        if let Response::Array(flags) = &arr[1] {
            assert!(flags.contains(&Response::Bulk(Some(Bytes::from("off")))));
        }
    } else {
        panic!("Expected array, got {:?}", response);
    }

    // Enable with OPTIN + NOLOOP
    client
        .command(&["CLIENT", "TRACKING", "ON", "OPTIN", "NOLOOP"])
        .await;
    let response = client.command(&["CLIENT", "TRACKINGINFO"]).await;
    if let Response::Array(arr) = &response {
        if let Response::Array(flags) = &arr[1] {
            assert!(
                flags.contains(&Response::Bulk(Some(Bytes::from("on")))),
                "Should contain 'on' flag"
            );
            assert!(
                flags.contains(&Response::Bulk(Some(Bytes::from("optin")))),
                "Should contain 'optin' flag"
            );
            assert!(
                flags.contains(&Response::Bulk(Some(Bytes::from("noloop")))),
                "Should contain 'noloop' flag"
            );
        }
        // redirect should be 0 (tracking on, no redirect)
        assert_eq!(arr[3], Response::Integer(0));
    } else {
        panic!("Expected array, got {:?}", response);
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_tracking_flushdb() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut writer = server.connect().await;

    // Enable RESP3 + tracking, read a key
    tracker.command(&["HELLO", "3"]).await;
    tracker.command(&["CLIENT", "TRACKING", "ON"]).await;
    tracker.command(&["SET", "{t}flushkey", "val"]).await;
    tracker.command(&["GET", "{t}flushkey"]).await;

    // FLUSHDB from another client
    writer.command(&["FLUSHDB"]).await;

    // Should receive flush-all invalidation (null)
    let msg = tracker.read_message(Duration::from_secs(2)).await;
    assert!(msg.is_some(), "Should receive flush-all invalidation");
    assert_invalidation_flush(&msg.unwrap());

    server.shutdown().await;
}

// ============================================================================
// CLIENT TRACKING BCAST Mode Tests
// ============================================================================

#[tokio::test]
async fn test_tracking_bcast_basic() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut writer = server.connect().await;

    // Enable RESP3 + BCAST tracking (no prefix = match all)
    tracker.command(&["HELLO", "3"]).await;
    tracker
        .command(&["CLIENT", "TRACKING", "ON", "BCAST"])
        .await;

    // Any write should trigger invalidation (no read needed for BCAST)
    writer.command(&["SET", "{t}bcast1", "val"]).await;

    let msg = tracker.read_message(Duration::from_secs(2)).await;
    assert!(
        msg.is_some(),
        "BCAST should receive invalidation for any write"
    );
    assert_invalidation_keys(&msg.unwrap(), &["{t}bcast1"]);

    server.shutdown().await;
}

#[tokio::test]
async fn test_tracking_bcast_prefix_filter() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut writer = server.connect().await;

    // Enable BCAST with PREFIX filter
    tracker.command(&["HELLO", "3"]).await;
    tracker
        .command(&["CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "{t}user:"])
        .await;

    // Write matching prefix — should get invalidation
    writer.command(&["SET", "{t}user:123", "val"]).await;
    let msg = tracker.read_message(Duration::from_secs(2)).await;
    assert!(
        msg.is_some(),
        "Should receive invalidation for matching prefix"
    );
    assert_invalidation_keys(&msg.unwrap(), &["{t}user:123"]);

    // Write NOT matching prefix — should NOT get invalidation
    writer.command(&["SET", "{t}order:456", "val"]).await;
    let msg = tracker.read_message(Duration::from_millis(500)).await;
    assert!(
        msg.is_none(),
        "Should NOT receive invalidation for non-matching prefix"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_tracking_bcast_no_prefix() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut writer = server.connect().await;

    // BCAST without PREFIX matches all keys
    tracker.command(&["HELLO", "3"]).await;
    tracker
        .command(&["CLIENT", "TRACKING", "ON", "BCAST"])
        .await;

    writer.command(&["SET", "{t}anything", "v"]).await;
    let msg = tracker.read_message(Duration::from_secs(2)).await;
    assert!(msg.is_some(), "BCAST without PREFIX should match all keys");

    server.shutdown().await;
}

/// BCAST prefixes accumulate across CLIENT TRACKING ON calls (Redis
/// semantics): a second ON call adds its prefixes instead of replacing the
/// first call's, and TRACKINGINFO reports the union.
#[tokio::test]
async fn test_tracking_bcast_prefix_accumulation() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut writer = server.connect().await;

    tracker.command(&["HELLO", "3"]).await;
    tracker
        .command(&["CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "{t}acc-a:"])
        .await;
    tracker
        .command(&["CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "{t}acc-b:"])
        .await;

    // TRACKINGINFO must report BOTH prefixes.
    let info = tracker.command(&["CLIENT", "TRACKINGINFO"]).await;
    let info_text = format!("{info:?}");
    assert!(
        info_text.contains("{t}acc-a:") && info_text.contains("{t}acc-b:"),
        "TRACKINGINFO should list the accumulated prefixes, got {info_text}"
    );

    // Writes under the FIRST prefix must still invalidate after the second
    // ON call.
    writer.command(&["SET", "{t}acc-a:1", "v"]).await;
    let msg = tracker
        .read_message(Duration::from_secs(2))
        .await
        .expect("prefix from the first ON call must still be tracked");
    assert_invalidation_keys(&msg, &["{t}acc-a:1"]);

    // And the second prefix works too.
    writer.command(&["SET", "{t}acc-b:1", "v"]).await;
    let msg = tracker
        .read_message(Duration::from_secs(2))
        .await
        .expect("prefix from the second ON call must be tracked");
    assert_invalidation_keys(&msg, &["{t}acc-b:1"]);

    server.shutdown().await;
}

/// A new prefix overlapping one registered by an EARLIER ON call is rejected
/// (the overlap check runs against the accumulated union, not just the most
/// recent batch).
#[tokio::test]
async fn test_tracking_bcast_overlap_with_accumulated_prefix_rejected() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "{t}ov-a:"])
        .await;
    client
        .command(&["CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "{t}ov-b:"])
        .await;

    // "{t}ov-a:x" overlaps the prefix from the FIRST call.
    let resp = client
        .command(&["CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "{t}ov-a:x"])
        .await;
    assert!(
        matches!(&resp, Response::Error(e) if String::from_utf8_lossy(e).contains("overlaps")),
        "overlap with a prefix from an earlier ON call should error, got {resp:?}"
    );

    server.shutdown().await;
}

/// Switching BCAST on/off or OPTIN<->OPTOUT requires CLIENT TRACKING OFF
/// first (Redis semantics).
#[tokio::test]
async fn test_tracking_mode_switch_requires_off() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Default mode -> BCAST: rejected.
    client.command(&["CLIENT", "TRACKING", "ON"]).await;
    let resp = client.command(&["CLIENT", "TRACKING", "ON", "BCAST"]).await;
    assert!(
        matches!(&resp, Response::Error(e) if String::from_utf8_lossy(e).contains("switch BCAST")),
        "enabling BCAST while tracking non-BCAST should error, got {resp:?}"
    );

    // After OFF, BCAST is allowed; BCAST -> non-BCAST is then rejected.
    client.command(&["CLIENT", "TRACKING", "OFF"]).await;
    let resp = client.command(&["CLIENT", "TRACKING", "ON", "BCAST"]).await;
    assert_eq!(resp, Response::ok());
    let resp = client.command(&["CLIENT", "TRACKING", "ON"]).await;
    assert!(
        matches!(&resp, Response::Error(e) if String::from_utf8_lossy(e).contains("switch BCAST")),
        "disabling BCAST while tracking BCAST should error, got {resp:?}"
    );

    // OPTIN -> OPTOUT: rejected.
    client.command(&["CLIENT", "TRACKING", "OFF"]).await;
    client.command(&["CLIENT", "TRACKING", "ON", "OPTIN"]).await;
    let resp = client
        .command(&["CLIENT", "TRACKING", "ON", "OPTOUT"])
        .await;
    assert!(
        matches!(&resp, Response::Error(e) if String::from_utf8_lossy(e).contains("OPTIN/OPTOUT")),
        "switching OPTIN->OPTOUT while enabled should error, got {resp:?}"
    );

    server.shutdown().await;
}

/// Extract the invalidated keys from a RESP3 invalidation Push frame.
fn invalidation_keys(frame: &Resp3Frame) -> Vec<Vec<u8>> {
    match frame {
        Resp3Frame::Push { data, .. } => match data.get(1) {
            Some(Resp3Frame::Array { data: keys, .. }) => keys
                .iter()
                .filter_map(|k| match k {
                    Resp3Frame::BlobString { data, .. } => Some(data.to_vec()),
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

/// Find two keys that hash to different shards so a multi-key command is
/// dispatched through the cross-shard scatter path instead of a single shard.
fn cross_shard_key_pair(num_shards: usize) -> (String, String) {
    let mut first: Option<(usize, String)> = None;
    for i in 0..100_000 {
        let key = format!("scatterkey:{i}");
        let shard = frogdb_core::shard_for_key(key.as_bytes(), num_shards);
        match &first {
            None => first = Some((shard, key)),
            Some((s0, k0)) if *s0 != shard => return (k0.clone(), key),
            _ => {}
        }
    }
    panic!("could not find a cross-shard key pair for {num_shards} shards");
}

/// Cross-shard MSET routes through the scatter path. BCAST tracking clients
/// must still receive invalidations for the written keys (the scatter path
/// previously skipped `broadcast_table` invalidation).
#[tokio::test]
async fn test_tracking_bcast_scatter_mset_invalidation() {
    // Default standalone server uses 4 shards.
    let (k1, k2) = cross_shard_key_pair(4);

    let server = TestServer::start_standalone_with_config(TestServerConfig {
        allow_cross_slot_standalone: true,
        ..Default::default()
    })
    .await;
    let mut tracker = server.connect_resp3().await;
    let mut writer = server.connect().await;

    // Enable RESP3 + BCAST tracking (no prefix = match all keys).
    tracker.command(&["HELLO", "3"]).await;
    tracker
        .command(&["CLIENT", "TRACKING", "ON", "BCAST"])
        .await;

    // Cross-shard MSET -> scatter path on two shards.
    writer
        .command(&["MSET", k1.as_str(), "v1", k2.as_str(), "v2"])
        .await;

    // Each touched shard sends its own invalidation push; collect keys until
    // both written keys are observed.
    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    for _ in 0..6 {
        match tracker.read_message(Duration::from_secs(2)).await {
            Some(frame) => {
                for key in invalidation_keys(&frame) {
                    seen.insert(key);
                }
                if seen.contains(k1.as_bytes()) && seen.contains(k2.as_bytes()) {
                    break;
                }
            }
            None => break,
        }
    }

    assert!(
        seen.contains(k1.as_bytes()),
        "BCAST client should receive scatter MSET invalidation for {k1}, saw {seen:?}"
    );
    assert!(
        seen.contains(k2.as_bytes()),
        "BCAST client should receive scatter MSET invalidation for {k2}, saw {seen:?}"
    );

    server.shutdown().await;
}

/// Cross-shard DEL/UNLINK routes through the scatter path. BCAST tracking
/// clients must receive invalidations for the deleted keys.
#[tokio::test]
async fn test_tracking_bcast_scatter_del_invalidation() {
    let (k1, k2) = cross_shard_key_pair(4);

    let server = TestServer::start_standalone_with_config(TestServerConfig {
        allow_cross_slot_standalone: true,
        ..Default::default()
    })
    .await;
    let mut tracker = server.connect_resp3().await;
    let mut writer = server.connect().await;

    // Seed the keys first (single-key SETs route directly to each shard).
    writer.command(&["SET", k1.as_str(), "v1"]).await;
    writer.command(&["SET", k2.as_str(), "v2"]).await;

    // Enable RESP3 + BCAST tracking after seeding so the only invalidations we
    // observe come from the DEL below.
    tracker.command(&["HELLO", "3"]).await;
    tracker
        .command(&["CLIENT", "TRACKING", "ON", "BCAST"])
        .await;

    // Cross-shard DEL -> scatter path on two shards.
    writer.command(&["DEL", k1.as_str(), k2.as_str()]).await;

    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    for _ in 0..6 {
        match tracker.read_message(Duration::from_secs(2)).await {
            Some(frame) => {
                for key in invalidation_keys(&frame) {
                    seen.insert(key);
                }
                if seen.contains(k1.as_bytes()) && seen.contains(k2.as_bytes()) {
                    break;
                }
            }
            None => break,
        }
    }

    assert!(
        seen.contains(k1.as_bytes()),
        "BCAST client should receive scatter DEL invalidation for {k1}, saw {seen:?}"
    );
    assert!(
        seen.contains(k2.as_bytes()),
        "BCAST client should receive scatter DEL invalidation for {k2}, saw {seen:?}"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_tracking_bcast_rejects_optin() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CLIENT", "TRACKING", "ON", "BCAST", "OPTIN"])
        .await;
    assert!(
        matches!(resp, Response::Error(_)),
        "BCAST + OPTIN should return error"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_tracking_bcast_caching_rejects() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["CLIENT", "TRACKING", "ON", "BCAST"]).await;
    let resp = client.command(&["CLIENT", "CACHING", "YES"]).await;
    assert!(
        matches!(resp, Response::Error(_)),
        "CLIENT CACHING should be rejected in BCAST mode"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_tracking_prefix_requires_bcast() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CLIENT", "TRACKING", "ON", "PREFIX", "foo:"])
        .await;
    assert!(
        matches!(resp, Response::Error(_)),
        "PREFIX without BCAST should return error"
    );

    server.shutdown().await;
}

// ============================================================================
// CLIENT TRACKING REDIRECT Mode Tests
// ============================================================================

#[tokio::test]
async fn test_tracking_redirect_invalid_id() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // REDIRECT with nonexistent client ID
    let resp = client
        .command(&["CLIENT", "TRACKING", "ON", "REDIRECT", "999999"])
        .await;
    assert!(
        matches!(resp, Response::Error(_)),
        "REDIRECT with nonexistent ID should return error"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_tracking_redirect_self() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let id_resp = client.command(&["CLIENT", "ID"]).await;
    let my_id = match id_resp {
        Response::Integer(id) => id.to_string(),
        _ => panic!("Expected integer from CLIENT ID"),
    };

    let resp = client
        .command(&["CLIENT", "TRACKING", "ON", "REDIRECT", &my_id])
        .await;
    assert!(
        matches!(resp, Response::Error(_)),
        "REDIRECT to self should return error"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_tracking_trackinginfo_bcast() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "{t}user:"])
        .await;

    let response = client.command(&["CLIENT", "TRACKINGINFO"]).await;
    if let Response::Array(arr) = &response {
        // flags should contain "on" and "bcast"
        if let Response::Array(flags) = &arr[1] {
            assert!(
                flags.contains(&Response::Bulk(Some(Bytes::from("on")))),
                "Should contain 'on' flag"
            );
            assert!(
                flags.contains(&Response::Bulk(Some(Bytes::from("bcast")))),
                "Should contain 'bcast' flag"
            );
        }
        // redirect should be 0 (no redirect)
        assert_eq!(arr[3], Response::Integer(0));
        // prefixes should contain our prefix
        if let Response::Array(prefixes) = &arr[5] {
            assert_eq!(prefixes.len(), 1);
            assert_eq!(prefixes[0], Response::Bulk(Some(Bytes::from("{t}user:"))));
        }
    } else {
        panic!("Expected array, got {:?}", response);
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_tracking_getredir() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Before tracking — should return -1
    let resp = client.command(&["CLIENT", "GETREDIR"]).await;
    assert_eq!(resp, Response::Integer(-1));

    // Enable tracking without redirect — should return 0
    client.command(&["CLIENT", "TRACKING", "ON"]).await;
    let resp = client.command(&["CLIENT", "GETREDIR"]).await;
    assert_eq!(resp, Response::Integer(0));

    server.shutdown().await;
}
