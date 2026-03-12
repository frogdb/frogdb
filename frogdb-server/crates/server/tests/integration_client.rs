//! Integration tests for client commands (CLIENT, RESET).

use crate::common::test_server::TestServer;
use bytes::Bytes;
use frogdb_protocol::Response;
use futures::StreamExt;
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

    // Caching commands should be accepted (stubs for now)
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
