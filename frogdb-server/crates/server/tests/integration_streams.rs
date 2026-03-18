//! Integration tests for stream commands (XREAD, XREADGROUP with blocking).

use crate::common::test_server::TestServer;
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
    let response = client.command(&["XREAD", "STREAMS", "mystream", "0"]).await;
    match response {
        Response::Array(streams) => {
            assert_eq!(streams.len(), 1, "Should have one stream result");
        }
        _ => panic!("Expected array for XREAD, got {:?}", response),
    }

    // Non-blocking XREAD with no data should return null
    let response = client.command(&["XREAD", "STREAMS", "mystream", "$"]).await;
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
                assert_eq!(
                    stream_data.len(),
                    2,
                    "Stream result should have key and entries"
                );
            }
        }
        _ => panic!("Expected array for satisfied XREAD, got {:?}", response),
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
            if let Response::Array(ref stream_data) = streams[0]
                && let Response::Array(ref entries) = stream_data[1]
            {
                assert_eq!(entries.len(), 1, "Should have exactly one new entry");
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
    let pending = client2.command(&["XPENDING", "mystream", "mygroup"]).await;
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

// ============================================================================
// XDELEX Tests
// ============================================================================

#[tokio::test]
async fn test_xdelex_basic_keepref() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Add entries
    client.command(&["XADD", "s", "1-0", "f", "v1"]).await;
    client.command(&["XADD", "s", "2-0", "f", "v2"]).await;
    client.command(&["XADD", "s", "3-0", "f", "v3"]).await;

    // Create group and deliver entries so PEL is populated
    client.command(&["XGROUP", "CREATE", "s", "g1", "0"]).await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "g1",
            "c1",
            "COUNT",
            "3",
            "STREAMS",
            "s",
            ">",
        ])
        .await;

    // XDELEX with default KEEPREF
    let response = client
        .command(&["XDELEX", "s", "IDS", "2", "1-0", "2-0"])
        .await;
    match &response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], Response::Integer(1)); // deleted
            assert_eq!(arr[1], Response::Integer(1)); // deleted
        }
        _ => panic!("Expected array, got {:?}", response),
    }

    // Verify entries deleted
    let response = client.command(&["XLEN", "s"]).await;
    assert_eq!(response, Response::Integer(1)); // only 3-0 remains

    // PEL should still have refs (KEEPREF)
    let response = client
        .command(&["XPENDING", "s", "g1", "-", "+", "10"])
        .await;
    match &response {
        Response::Array(arr) => {
            // All 3 entries should still be in PEL (KEEPREF preserves refs)
            assert_eq!(arr.len(), 3);
        }
        _ => panic!("Expected array, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_xdelex_delref() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["XADD", "s", "1-0", "f", "v1"]).await;
    client.command(&["XADD", "s", "2-0", "f", "v2"]).await;

    // Create two groups, deliver to both
    client.command(&["XGROUP", "CREATE", "s", "g1", "0"]).await;
    client.command(&["XGROUP", "CREATE", "s", "g2", "0"]).await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "g1",
            "c1",
            "COUNT",
            "2",
            "STREAMS",
            "s",
            ">",
        ])
        .await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "g2",
            "c1",
            "COUNT",
            "2",
            "STREAMS",
            "s",
            ">",
        ])
        .await;

    // XDELEX with DELREF
    let response = client
        .command(&["XDELEX", "s", "DELREF", "IDS", "1", "1-0"])
        .await;
    match &response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], Response::Integer(1));
        }
        _ => panic!("Expected array, got {:?}", response),
    }

    // PEL in g1 should only have 2-0 (1-0 ref removed)
    let response = client
        .command(&["XPENDING", "s", "g1", "-", "+", "10"])
        .await;
    match &response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 1); // only 2-0
        }
        _ => panic!("Expected array, got {:?}", response),
    }

    // PEL in g2 should also only have 2-0
    let response = client
        .command(&["XPENDING", "s", "g2", "-", "+", "10"])
        .await;
    match &response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 1);
        }
        _ => panic!("Expected array, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_xdelex_acked() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["XADD", "s", "1-0", "f", "v1"]).await;
    client.command(&["XADD", "s", "2-0", "f", "v2"]).await;

    // Two groups
    client.command(&["XGROUP", "CREATE", "s", "g1", "0"]).await;
    client.command(&["XGROUP", "CREATE", "s", "g2", "0"]).await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "g1",
            "c1",
            "COUNT",
            "2",
            "STREAMS",
            "s",
            ">",
        ])
        .await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "g2",
            "c1",
            "COUNT",
            "2",
            "STREAMS",
            "s",
            ">",
        ])
        .await;

    // Ack 1-0 in g1 only
    client.command(&["XACK", "s", "g1", "1-0"]).await;

    // ACKED: 1-0 not fully acked (g2 still pending), 2-0 not acked at all
    let response = client
        .command(&["XDELEX", "s", "ACKED", "IDS", "2", "1-0", "2-0"])
        .await;
    match &response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], Response::Integer(2)); // not deleted, g2 still pending
            assert_eq!(arr[1], Response::Integer(2)); // not deleted
        }
        _ => panic!("Expected array, got {:?}", response),
    }

    // Ack 1-0 in g2 too → now fully acked
    client.command(&["XACK", "s", "g2", "1-0"]).await;

    let response = client
        .command(&["XDELEX", "s", "ACKED", "IDS", "1", "1-0"])
        .await;
    match &response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], Response::Integer(1)); // deleted
        }
        _ => panic!("Expected array, got {:?}", response),
    }

    // Verify 1-0 is gone
    assert_eq!(client.command(&["XLEN", "s"]).await, Response::Integer(1));

    server.shutdown().await;
}

#[tokio::test]
async fn test_xdelex_not_found() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["XADD", "s", "1-0", "f", "v"]).await;

    // Non-existent IDs
    let response = client
        .command(&["XDELEX", "s", "IDS", "2", "99-0", "100-0"])
        .await;
    match &response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], Response::Integer(-1));
            assert_eq!(arr[1], Response::Integer(-1));
        }
        _ => panic!("Expected array, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_xdelex_nonexistent_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client
        .command(&["XDELEX", "nosuchkey", "IDS", "1", "1-0"])
        .await;
    match &response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], Response::Integer(-1));
        }
        _ => panic!("Expected array, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_xdelex_wrongtype() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "k", "v"]).await;

    let response = client.command(&["XDELEX", "k", "IDS", "1", "1-0"]).await;
    assert!(matches!(response, Response::Error(_)));

    server.shutdown().await;
}

#[tokio::test]
async fn test_xdelex_ids_mismatch() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["XADD", "s", "1-0", "f", "v"]).await;

    // numids says 3 but only 1 ID provided
    let response = client.command(&["XDELEX", "s", "IDS", "3", "1-0"]).await;
    assert!(matches!(response, Response::Error(_)));

    server.shutdown().await;
}

#[tokio::test]
async fn test_xdelex_mixed_results() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["XADD", "s", "1-0", "f", "v1"]).await;
    client.command(&["XADD", "s", "2-0", "f", "v2"]).await;
    client.command(&["XADD", "s", "3-0", "f", "v3"]).await;

    // Mix of found and not found
    let response = client
        .command(&["XDELEX", "s", "IDS", "3", "1-0", "99-0", "3-0"])
        .await;
    match &response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], Response::Integer(1)); // deleted
            assert_eq!(arr[1], Response::Integer(-1)); // not found
            assert_eq!(arr[2], Response::Integer(1)); // deleted
        }
        _ => panic!("Expected array, got {:?}", response),
    }

    server.shutdown().await;
}

// ============================================================================
// XACKDEL Tests
// ============================================================================

#[tokio::test]
async fn test_xackdel_basic_keepref() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["XADD", "s", "1-0", "f", "v1"]).await;
    client.command(&["XADD", "s", "2-0", "f", "v2"]).await;

    client.command(&["XGROUP", "CREATE", "s", "g1", "0"]).await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "g1",
            "c1",
            "COUNT",
            "2",
            "STREAMS",
            "s",
            ">",
        ])
        .await;

    // XACKDEL: ack in g1 and delete (KEEPREF default)
    let response = client
        .command(&["XACKDEL", "s", "g1", "IDS", "1", "1-0"])
        .await;
    match &response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], Response::Integer(1)); // acked + deleted
        }
        _ => panic!("Expected array, got {:?}", response),
    }

    // Entry should be gone
    assert_eq!(client.command(&["XLEN", "s"]).await, Response::Integer(1));

    // 1-0 should be acked (removed from PEL) in g1
    let response = client
        .command(&["XPENDING", "s", "g1", "-", "+", "10"])
        .await;
    match &response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 1); // only 2-0 pending
        }
        _ => panic!("Expected array, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_xackdel_delref() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["XADD", "s", "1-0", "f", "v1"]).await;

    client.command(&["XGROUP", "CREATE", "s", "g1", "0"]).await;
    client.command(&["XGROUP", "CREATE", "s", "g2", "0"]).await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "g1",
            "c1",
            "COUNT",
            "1",
            "STREAMS",
            "s",
            ">",
        ])
        .await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "g2",
            "c1",
            "COUNT",
            "1",
            "STREAMS",
            "s",
            ">",
        ])
        .await;

    // XACKDEL with DELREF: ack in g1, delete entry, remove all PEL refs
    let response = client
        .command(&["XACKDEL", "s", "g1", "DELREF", "IDS", "1", "1-0"])
        .await;
    match &response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], Response::Integer(1));
        }
        _ => panic!("Expected array, got {:?}", response),
    }

    // g2 PEL should also be cleaned
    let response = client
        .command(&["XPENDING", "s", "g2", "-", "+", "10"])
        .await;
    match &response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 0);
        }
        _ => panic!("Expected array, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_xackdel_acked_multi_group() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["XADD", "s", "1-0", "f", "v1"]).await;

    client.command(&["XGROUP", "CREATE", "s", "g1", "0"]).await;
    client.command(&["XGROUP", "CREATE", "s", "g2", "0"]).await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "g1",
            "c1",
            "COUNT",
            "1",
            "STREAMS",
            "s",
            ">",
        ])
        .await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "g2",
            "c1",
            "COUNT",
            "1",
            "STREAMS",
            "s",
            ">",
        ])
        .await;

    // XACKDEL ACKED in g1: acks in g1, but g2 still pending → not deleted
    let response = client
        .command(&["XACKDEL", "s", "g1", "ACKED", "IDS", "1", "1-0"])
        .await;
    match &response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], Response::Integer(2)); // acked but not deleted
        }
        _ => panic!("Expected array, got {:?}", response),
    }

    // Entry should still exist
    assert_eq!(client.command(&["XLEN", "s"]).await, Response::Integer(1));

    // Now ack in g2 via XACKDEL ACKED
    let response = client
        .command(&["XACKDEL", "s", "g2", "ACKED", "IDS", "1", "1-0"])
        .await;
    match &response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], Response::Integer(1)); // now fully acked → deleted
        }
        _ => panic!("Expected array, got {:?}", response),
    }

    assert_eq!(client.command(&["XLEN", "s"]).await, Response::Integer(0));

    server.shutdown().await;
}

#[tokio::test]
async fn test_xackdel_not_found() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["XADD", "s", "1-0", "f", "v"]).await;
    client.command(&["XGROUP", "CREATE", "s", "g1", "0"]).await;

    let response = client
        .command(&["XACKDEL", "s", "g1", "IDS", "1", "99-0"])
        .await;
    match &response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], Response::Integer(-1));
        }
        _ => panic!("Expected array, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_xackdel_nonexistent_group() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["XADD", "s", "1-0", "f", "v"]).await;

    let response = client
        .command(&["XACKDEL", "s", "nogroup", "IDS", "1", "1-0"])
        .await;
    assert!(matches!(response, Response::Error(_)));

    server.shutdown().await;
}

#[tokio::test]
async fn test_xackdel_wrongtype() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "k", "v"]).await;

    let response = client
        .command(&["XACKDEL", "k", "g1", "IDS", "1", "1-0"])
        .await;
    assert!(matches!(response, Response::Error(_)));

    server.shutdown().await;
}
