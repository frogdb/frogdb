//! Integration tests for stream commands (XREAD, XREADGROUP with blocking).

mod common;

use common::test_server::TestServer;
use frogdb_protocol::Response;
use std::time::Duration;
use tokio::time::timeout;

// ============================================================================
// Stream Blocking Tests (XREAD BLOCK, XREADGROUP BLOCK)
// ============================================================================

#[tokio::test]
async fn test_xread_non_blocking() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Add some entries to a stream
    let response = client
        .command(&["XADD", "mystream", "*", "field1", "value1"])
        .await;
    match response {
        Response::Bulk(Some(_)) => {} // ID returned
        _ => panic!("Expected bulk string for XADD, got {:?}", response),
    }

    // Non-blocking XREAD should return immediately
    let response = client
        .command(&["XREAD", "STREAMS", "mystream", "0"])
        .await;
    match response {
        Response::Array(streams) => {
            assert_eq!(streams.len(), 1, "Should have one stream result");
        }
        _ => panic!("Expected array for XREAD, got {:?}", response),
    }

    // Non-blocking XREAD with no data should return null
    let response = client
        .command(&["XREAD", "STREAMS", "mystream", "$"])
        .await;
    match response {
        Response::Bulk(None) => {} // null response expected
        _ => panic!("Expected null for XREAD with $, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_xread_block_timeout() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create stream first
    let _ = client
        .command(&["XADD", "mystream", "*", "field1", "value1"])
        .await;

    // Blocking XREAD with short timeout should timeout
    let start = std::time::Instant::now();
    let response = client
        .command(&["XREAD", "BLOCK", "100", "STREAMS", "mystream", "$"])
        .await;
    let elapsed = start.elapsed();

    // Should have waited at least 100ms (minus some tolerance)
    assert!(
        elapsed.as_millis() >= 80,
        "Should have waited at least 80ms, but waited {}ms",
        elapsed.as_millis()
    );

    // Should return null on timeout
    match response {
        Response::Bulk(None) => {}
        _ => panic!("Expected null on timeout, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_xread_block_satisfied_by_xadd() {
    let server = TestServer::start_standalone().await;
    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    // Create stream first
    let _ = client1
        .command(&["XADD", "mystream", "*", "field1", "value1"])
        .await;

    // Start blocking read in background
    let handle = tokio::spawn(async move {
        client1
            .command(&["XREAD", "BLOCK", "5000", "STREAMS", "mystream", "$"])
            .await
    });

    // Give time for the blocking command to be processed
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Add new entry from another client
    let _ = client2
        .command(&["XADD", "mystream", "*", "field2", "value2"])
        .await;

    // The blocking read should complete with the new entry
    let response = timeout(Duration::from_secs(2), handle)
        .await
        .expect("timeout waiting for task")
        .expect("task panicked");

    match response {
        Response::Array(streams) => {
            assert_eq!(streams.len(), 1, "Should have one stream result");
            // Check inner structure
            if let Response::Array(ref stream_data) = streams[0] {
                assert_eq!(stream_data.len(), 2, "Stream result should have key and entries");
            }
        }
        _ => panic!(
            "Expected array for satisfied XREAD, got {:?}",
            response
        ),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_xread_block_dollar_resolution() {
    let server = TestServer::start_standalone().await;
    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    // Add initial entry
    let _ = client1
        .command(&["XADD", "mystream", "*", "field1", "initial"])
        .await;

    // Start blocking read with $ (current last ID)
    let handle = tokio::spawn(async move {
        client1
            .command(&["XREAD", "BLOCK", "5000", "STREAMS", "mystream", "$"])
            .await
    });

    // Give time for the blocking command to be processed
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Add new entry
    let _ = client2
        .command(&["XADD", "mystream", "*", "field2", "new"])
        .await;

    // Should receive only the new entry, not the initial one
    let response = timeout(Duration::from_secs(2), handle)
        .await
        .expect("timeout waiting for task")
        .expect("task panicked");

    match response {
        Response::Array(streams) => {
            assert_eq!(streams.len(), 1);
            if let Response::Array(ref stream_data) = streams[0] {
                if let Response::Array(ref entries) = stream_data[1] {
                    assert_eq!(entries.len(), 1, "Should have exactly one new entry");
                }
            }
        }
        _ => panic!("Expected array, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_xreadgroup_non_blocking() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create stream and consumer group
    let _ = client
        .command(&["XADD", "mystream", "*", "field1", "value1"])
        .await;
    let response = client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "0"])
        .await;
    match response {
        Response::Simple(_) => {}
        _ => panic!("Expected OK for XGROUP CREATE, got {:?}", response),
    }

    // Non-blocking XREADGROUP should return the entry
    let response = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    match response {
        Response::Array(streams) => {
            assert_eq!(streams.len(), 1);
        }
        _ => panic!("Expected array for XREADGROUP, got {:?}", response),
    }

    // Another read should return null (no more new messages)
    let response = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    match response {
        Response::Bulk(None) => {}
        _ => panic!("Expected null for second XREADGROUP, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_xreadgroup_block_timeout() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create stream and group
    let _ = client
        .command(&["XADD", "mystream", "*", "field1", "value1"])
        .await;
    let _ = client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$"])
        .await;

    // Blocking XREADGROUP should timeout
    let start = std::time::Instant::now();
    let response = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer1",
            "BLOCK",
            "100",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    let elapsed = start.elapsed();

    assert!(
        elapsed.as_millis() >= 80,
        "Should have waited at least 80ms"
    );

    match response {
        Response::Bulk(None) => {}
        _ => panic!("Expected null on timeout, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_xreadgroup_block_satisfied_by_xadd() {
    let server = TestServer::start_standalone().await;
    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    // Create stream and group
    let _ = client1
        .command(&["XADD", "mystream", "*", "field1", "value1"])
        .await;
    let _ = client1
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$"])
        .await;

    // Start blocking read in background
    let handle = tokio::spawn(async move {
        client1
            .command(&[
                "XREADGROUP",
                "GROUP",
                "mygroup",
                "consumer1",
                "BLOCK",
                "5000",
                "STREAMS",
                "mystream",
                ">",
            ])
            .await
    });

    // Give time for blocking command to be processed
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Add new entry
    let _ = client2
        .command(&["XADD", "mystream", "*", "field2", "value2"])
        .await;

    // Should receive the new entry
    let response = timeout(Duration::from_secs(2), handle)
        .await
        .expect("timeout waiting for task")
        .expect("task panicked");

    match response {
        Response::Array(streams) => {
            assert_eq!(streams.len(), 1);
        }
        _ => panic!("Expected array, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_xreadgroup_block_with_noack() {
    let server = TestServer::start_standalone().await;
    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    // Create stream and group
    let _ = client1
        .command(&["XADD", "mystream", "*", "field1", "value1"])
        .await;
    let _ = client1
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$"])
        .await;

    // Start blocking read with NOACK
    let handle = tokio::spawn(async move {
        client1
            .command(&[
                "XREADGROUP",
                "GROUP",
                "mygroup",
                "consumer1",
                "NOACK",
                "BLOCK",
                "5000",
                "STREAMS",
                "mystream",
                ">",
            ])
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Add new entry
    let _ = client2
        .command(&["XADD", "mystream", "*", "field2", "value2"])
        .await;

    let response = timeout(Duration::from_secs(2), handle)
        .await
        .expect("timeout")
        .expect("task panicked");

    match response {
        Response::Array(_) => {}
        _ => panic!("Expected array, got {:?}", response),
    }

    // With NOACK, the entry should not be in the pending list
    let pending = client2
        .command(&["XPENDING", "mystream", "mygroup"])
        .await;
    match pending {
        Response::Array(ref items) => {
            if let Response::Integer(count) = &items[0] {
                assert_eq!(*count, 0, "Should have no pending entries with NOACK");
            }
        }
        _ => panic!("Expected array for XPENDING, got {:?}", pending),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_xreadgroup_specific_id_no_block() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create stream and group
    let _ = client
        .command(&["XADD", "mystream", "*", "field1", "value1"])
        .await;
    let _ = client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "0"])
        .await;

    // Read and acknowledge
    let response = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    match response {
        Response::Array(_) => {}
        _ => panic!("Expected array, got {:?}", response),
    }

    // Read with specific ID (re-read from PEL) - should NOT block even with BLOCK specified
    // because specific ID means re-reading pending entries, not waiting for new ones
    let start = std::time::Instant::now();
    let response = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer1",
            "BLOCK",
            "1000",
            "STREAMS",
            "mystream",
            "0",
        ])
        .await;
    let elapsed = start.elapsed();

    // Should return almost immediately (not wait 1000ms)
    assert!(
        elapsed.as_millis() < 500,
        "Should not block when reading PEL with specific ID, took {}ms",
        elapsed.as_millis()
    );

    // Should have the pending entry
    match response {
        Response::Array(streams) => {
            assert_eq!(streams.len(), 1);
        }
        Response::Bulk(None) => {} // OK if no pending entries
        _ => panic!("Expected array or null, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_xread_immediate_data() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Add entry to stream
    let _ = client
        .command(&["XADD", "mystream", "*", "field1", "value1"])
        .await;

    // XREAD BLOCK with data available should return immediately
    let start = std::time::Instant::now();
    let response = client
        .command(&["XREAD", "BLOCK", "5000", "STREAMS", "mystream", "0"])
        .await;
    let elapsed = start.elapsed();

    // Should return immediately, not wait 5 seconds
    assert!(
        elapsed.as_millis() < 500,
        "Should return immediately when data exists, took {}ms",
        elapsed.as_millis()
    );

    match response {
        Response::Array(streams) => {
            assert_eq!(streams.len(), 1);
        }
        _ => panic!("Expected array, got {:?}", response),
    }

    server.shutdown().await;
}
