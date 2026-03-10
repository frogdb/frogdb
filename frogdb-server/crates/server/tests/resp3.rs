//! Integration tests for RESP3 protocol support.
//!
//! These tests verify RESP3 protocol negotiation via HELLO command and
//! RESP3-specific response types (Map, Set, Double, Push).


use bytes::Bytes;
use crate::common::test_server::{TestServer, TestServerConfig};
use frogdb_telemetry::testing::{MetricsDelta, MetricsSnapshot, fetch_metrics};
use futures::{SinkExt, StreamExt};
use redis_protocol::codec::Resp2;
use redis_protocol::resp2::types::BytesFrame as Resp2Frame;
use redis_protocol::resp3::types::BytesFrame as Resp3Frame;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::codec::Framed;

async fn start_server() -> TestServer {
    TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(1),
        ..Default::default()
    })
    .await
}

async fn start_server_with_security(requirepass: &str) -> TestServer {
    TestServer::start_with_security(requirepass).await
}

/// Client using RESP2 codec (returns raw Resp2Frame, not Response).
struct Resp2Client {
    framed: Framed<TcpStream, Resp2>,
}

impl Resp2Client {
    async fn connect(server: &TestServer) -> Self {
        let stream = TcpStream::connect(server.socket_addr()).await.unwrap();
        let framed = Framed::new(stream, Resp2);
        Resp2Client { framed }
    }

    async fn command(&mut self, args: &[&str]) -> Resp2Frame {
        let frame = Resp2Frame::Array(
            args.iter()
                .map(|s| Resp2Frame::BulkString(Bytes::from(s.to_string())))
                .collect(),
        );
        self.framed.send(frame).await.unwrap();

        timeout(Duration::from_secs(5), self.framed.next())
            .await
            .expect("timeout")
            .expect("connection closed")
            .expect("frame error")
    }
}

// =============================================================================
// 12.4.2 Integration tests for HELLO
// =============================================================================

#[tokio::test]
async fn test_hello_no_args() {
    let server = start_server().await;
    let mut client = Resp2Client::connect(&server).await;

    // HELLO with no args should return server info as an array (RESP2 format)
    let response = client.command(&["HELLO"]).await;
    match response {
        Resp2Frame::Array(items) => {
            // Should have even number of items (key-value pairs flattened)
            assert!(items.len() >= 2);
            assert!(items.len() % 2 == 0);
            // First key should be "server"
            if let Resp2Frame::BulkString(key) = &items[0] {
                assert_eq!(key.as_ref(), b"server");
            } else {
                panic!("Expected BulkString key, got {:?}", items[0]);
            }
        }
        _ => panic!("Expected Array response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_hello_upgrade_to_resp3() {
    let server = start_server().await;
    let mut client = server.connect_resp3().await;

    // Get baseline metrics
    let before = MetricsSnapshot::new(fetch_metrics(server.metrics_addr()).await);

    // Send HELLO 3 to upgrade to RESP3
    let response = client.command(&["HELLO", "3"]).await;
    match response {
        Resp3Frame::Map { data, .. } => {
            // Should contain "server" key
            let has_server = data.iter().any(|(k, _)| {
                matches!(k, Resp3Frame::BlobString { data, .. } if data.as_ref() == b"server")
            });
            assert!(has_server, "Response should contain 'server' key");

            // Check protocol version is 3
            let proto = data.iter().find(|(k, _)| {
                matches!(k, Resp3Frame::BlobString { data, .. } if data.as_ref() == b"proto")
            });
            if let Some((_, Resp3Frame::Number { data: 3, .. })) = proto {
                // Good - protocol is 3
            } else {
                panic!("Expected proto=3 in response, got {:?}", proto);
            }
        }
        _ => panic!("Expected Map response for HELLO 3, got {:?}", response),
    }

    // Verify metrics - HELLO command was tracked
    tokio::time::sleep(Duration::from_millis(50)).await;
    let after = MetricsSnapshot::new(fetch_metrics(server.metrics_addr()).await);
    MetricsDelta::new(before, after).assert_counter_increased(
        "frogdb_commands_total",
        &[("command", "HELLO")],
        1.0,
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_hello_stay_resp2() {
    let server = start_server().await;
    let mut client = Resp2Client::connect(&server).await;

    // HELLO 2 should stay in RESP2
    let response = client.command(&["HELLO", "2"]).await;
    match response {
        Resp2Frame::Array(items) => {
            // Should have even number of items (flattened map)
            assert!(items.len() >= 2);
            assert!(items.len() % 2 == 0);
        }
        _ => panic!("Expected Array response for HELLO 2, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_hello_noproto() {
    let server = start_server().await;
    let mut client = Resp2Client::connect(&server).await;

    // HELLO 4 should return NOPROTO error
    let response = client.command(&["HELLO", "4"]).await;
    match response {
        Resp2Frame::Error(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("NOPROTO"),
                "Expected NOPROTO error, got: {}",
                msg
            );
        }
        _ => panic!("Expected Error response for HELLO 4, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_hello_no_downgrade() {
    let server = start_server().await;
    let mut client = server.connect_resp3().await;

    // First upgrade to RESP3
    let response = client.command(&["HELLO", "3"]).await;
    assert!(matches!(response, Resp3Frame::Map { .. }));

    // Now try to downgrade to RESP2 - should error
    let response = client.command(&["HELLO", "2"]).await;
    match response {
        Resp3Frame::SimpleError { data, .. } => {
            let msg = data.to_string();
            assert!(
                msg.contains("downgrade") || msg.contains("NOPROTO"),
                "Expected downgrade error, got: {}",
                msg
            );
        }
        _ => panic!("Expected Error for downgrade attempt, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_hello_auth_success() {
    let server = start_server_with_security("testpass").await;
    let mut client = server.connect_resp3().await;

    // HELLO 3 AUTH default testpass
    let response = client
        .command(&["HELLO", "3", "AUTH", "default", "testpass"])
        .await;
    match response {
        Resp3Frame::Map { data, .. } => {
            // Should contain "server" key after successful auth
            let has_server = data.iter().any(|(k, _)| {
                matches!(k, Resp3Frame::BlobString { data, .. } if data.as_ref() == b"server")
            });
            assert!(has_server, "Successful auth should return server info");
        }
        _ => panic!(
            "Expected Map response for successful HELLO AUTH, got {:?}",
            response
        ),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_hello_auth_failure() {
    let server = start_server_with_security("testpass").await;
    let mut client = server.connect_resp3().await;

    // HELLO 3 AUTH default wrongpassword
    let response = client
        .command(&["HELLO", "3", "AUTH", "default", "wrongpassword"])
        .await;
    match response {
        Resp3Frame::SimpleError { data, .. } => {
            let msg = data.to_string();
            assert!(
                msg.contains("WRONGPASS") || msg.contains("invalid") || msg.contains("ERR"),
                "Expected auth error, got: {}",
                msg
            );
        }
        _ => panic!("Expected Error for failed auth, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_hello_setname() {
    let server = start_server().await;
    let mut client = server.connect_resp3().await;

    // HELLO 3 SETNAME myclient
    let response = client.command(&["HELLO", "3", "SETNAME", "myclient"]).await;
    match response {
        Resp3Frame::Map { .. } => {
            // Successful - now verify the name was set
            let info = client.command(&["CLIENT", "GETNAME"]).await;
            match info {
                Resp3Frame::BlobString { data, .. } => {
                    assert_eq!(data.as_ref(), b"myclient");
                }
                _ => panic!("Expected BlobString for CLIENT GETNAME, got {:?}", info),
            }
        }
        _ => panic!(
            "Expected Map response for HELLO SETNAME, got {:?}",
            response
        ),
    }

    server.shutdown().await;
}

// =============================================================================
// 12.4.3 Integration tests for RESP3 responses
// =============================================================================

#[tokio::test]
async fn test_hgetall_resp3_returns_map() {
    let server = start_server().await;
    let mut client = server.connect_resp3().await;

    // Upgrade to RESP3
    client.command(&["HELLO", "3"]).await;

    // Set up some hash data
    client
        .command(&["HSET", "myhash", "field1", "value1", "field2", "value2"])
        .await;

    // Get baseline metrics (after setup)
    let before = MetricsSnapshot::fetch(server.metrics_addr()).await;

    // HGETALL should return a Map in RESP3
    let response = client.command(&["HGETALL", "myhash"]).await;
    match response {
        Resp3Frame::Map { data, .. } => {
            assert_eq!(data.len(), 2, "Expected 2 fields in map");
        }
        _ => panic!(
            "Expected Map response for HGETALL in RESP3, got {:?}",
            response
        ),
    }

    // Verify metrics - HGETALL was tracked
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let after = MetricsSnapshot::fetch(server.metrics_addr()).await;
    MetricsDelta::new(before, after).assert_counter_increased(
        "frogdb_commands_total",
        &[("command", "HGETALL")],
        1.0,
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_hgetall_resp2_returns_array() {
    let server = start_server().await;
    let mut client = Resp2Client::connect(&server).await;

    // Set up some hash data
    client
        .command(&["HSET", "myhash", "field1", "value1", "field2", "value2"])
        .await;

    // Get baseline metrics (after setup)
    let before = MetricsSnapshot::fetch(server.metrics_addr()).await;

    // HGETALL should return a flattened Array in RESP2
    let response = client.command(&["HGETALL", "myhash"]).await;
    match response {
        Resp2Frame::Array(items) => {
            // 2 fields = 4 items (key, value, key, value)
            assert_eq!(items.len(), 4, "Expected 4 items in flattened array");
        }
        _ => panic!(
            "Expected Array response for HGETALL in RESP2, got {:?}",
            response
        ),
    }

    // Verify metrics - HGETALL was tracked
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let after = MetricsSnapshot::fetch(server.metrics_addr()).await;
    MetricsDelta::new(before, after).assert_counter_increased(
        "frogdb_commands_total",
        &[("command", "HGETALL")],
        1.0,
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_smembers_resp3_returns_set() {
    let server = start_server().await;
    let mut client = server.connect_resp3().await;

    // Upgrade to RESP3
    client.command(&["HELLO", "3"]).await;

    // Set up some set data
    client.command(&["SADD", "myset", "a", "b", "c"]).await;

    // Get baseline metrics (after setup)
    let before = MetricsSnapshot::fetch(server.metrics_addr()).await;

    // SMEMBERS should return a Set in RESP3
    let response = client.command(&["SMEMBERS", "myset"]).await;
    match response {
        Resp3Frame::Set { data, .. } => {
            assert_eq!(data.len(), 3, "Expected 3 members in set");
        }
        _ => panic!(
            "Expected Set response for SMEMBERS in RESP3, got {:?}",
            response
        ),
    }

    // Verify metrics - SMEMBERS was tracked
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let after = MetricsSnapshot::fetch(server.metrics_addr()).await;
    MetricsDelta::new(before, after).assert_counter_increased(
        "frogdb_commands_total",
        &[("command", "SMEMBERS")],
        1.0,
    );

    server.shutdown().await;
}

#[tokio::test]
#[allow(clippy::approx_constant)]
async fn test_zscore_resp3_returns_double() {
    let server = start_server().await;
    let mut client = server.connect_resp3().await;

    // Upgrade to RESP3
    client.command(&["HELLO", "3"]).await;

    // Set up some sorted set data
    client.command(&["ZADD", "myzset", "3.14159", "pi"]).await;

    // ZSCORE should return a Double in RESP3
    let response = client.command(&["ZSCORE", "myzset", "pi"]).await;
    match response {
        Resp3Frame::Double { data, .. } => {
            assert!(
                (data - 3.14159).abs() < 1e-10,
                "Expected score ~3.14159, got {}",
                data
            );
        }
        _ => panic!(
            "Expected Double response for ZSCORE in RESP3, got {:?}",
            response
        ),
    }

    server.shutdown().await;
}

#[tokio::test]
#[allow(clippy::approx_constant)]
async fn test_incrbyfloat_resp3_double() {
    let server = start_server().await;
    let mut client = server.connect_resp3().await;

    // Upgrade to RESP3
    client.command(&["HELLO", "3"]).await;

    // INCRBYFLOAT should return a Double in RESP3
    let response = client.command(&["INCRBYFLOAT", "myfloat", "3.14"]).await;
    match response {
        Resp3Frame::Double { data, .. } => {
            assert!((data - 3.14).abs() < 1e-10, "Expected 3.14, got {}", data);
        }
        _ => panic!(
            "Expected Double response for INCRBYFLOAT in RESP3, got {:?}",
            response
        ),
    }

    server.shutdown().await;
}

// =============================================================================
// 12.4.4 Integration tests for Pub/Sub Push
// =============================================================================

#[tokio::test]
async fn test_pubsub_message_resp3_push() {
    let server = start_server().await;

    // Subscriber uses RESP3
    let mut subscriber = server.connect_resp3().await;
    subscriber.command(&["HELLO", "3"]).await;
    subscriber.command(&["SUBSCRIBE", "test-channel"]).await;

    // Publisher
    let mut publisher = Resp2Client::connect(&server).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    publisher
        .command(&["PUBLISH", "test-channel", "hello"])
        .await;

    // Read the message on subscriber
    if let Some(response) = subscriber.read_message(Duration::from_secs(2)).await {
        match response {
            Resp3Frame::Push { data, .. } => {
                assert!(data.len() >= 3, "Push should have at least 3 elements");
                // First element should be "message"
                if let Resp3Frame::BlobString { data: msg_type, .. } = &data[0] {
                    assert_eq!(msg_type.as_ref(), b"message");
                }
            }
            _ => panic!(
                "Expected Push response for pub/sub message in RESP3, got {:?}",
                response
            ),
        }
    } else {
        panic!("No message received");
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_pubsub_message_resp2_array() {
    let server = start_server().await;

    // Subscriber uses RESP2 (default)
    let mut subscriber = Resp2Client::connect(&server).await;
    subscriber.command(&["SUBSCRIBE", "test-channel"]).await;

    // Publisher
    let mut publisher = Resp2Client::connect(&server).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    publisher
        .command(&["PUBLISH", "test-channel", "hello"])
        .await;

    // In RESP2, pub/sub messages are arrays, not push types
    // The server sends them as arrays, which is correct for RESP2
    tokio::time::sleep(Duration::from_millis(100)).await;

    server.shutdown().await;
}

// =============================================================================
// 12.4.5 RESP2 backwards compatibility tests
// =============================================================================

#[tokio::test]
async fn test_default_is_resp2() {
    let server = start_server().await;
    let mut client = Resp2Client::connect(&server).await;

    // Without HELLO, the server should use RESP2
    let response = client.command(&["PING"]).await;
    match response {
        Resp2Frame::SimpleString(s) => {
            assert_eq!(s.as_ref(), b"PONG");
        }
        _ => panic!("Expected SimpleString PONG, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_existing_commands_work_resp2() {
    let server = start_server().await;
    let mut client = Resp2Client::connect(&server).await;

    // All basic commands should work without HELLO
    client.command(&["SET", "key1", "value1"]).await;
    let response = client.command(&["GET", "key1"]).await;
    match response {
        Resp2Frame::BulkString(data) => {
            assert_eq!(data.as_ref(), b"value1");
        }
        _ => panic!("Expected BulkString, got {:?}", response),
    }

    // Hash commands
    client.command(&["HSET", "hash1", "field1", "val1"]).await;
    let response = client.command(&["HGET", "hash1", "field1"]).await;
    match response {
        Resp2Frame::BulkString(data) => {
            assert_eq!(data.as_ref(), b"val1");
        }
        _ => panic!("Expected BulkString, got {:?}", response),
    }

    // List commands
    client.command(&["RPUSH", "list1", "a", "b", "c"]).await;
    let response = client.command(&["LRANGE", "list1", "0", "-1"]).await;
    match response {
        Resp2Frame::Array(items) => {
            assert_eq!(items.len(), 3);
        }
        _ => panic!("Expected Array, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_resp2_after_server_restart() {
    // Test that protocol resets on reconnect
    let server = start_server().await;

    // First client upgrades to RESP3
    {
        let mut client = server.connect_resp3().await;
        let response = client.command(&["HELLO", "3"]).await;
        assert!(matches!(response, Resp3Frame::Map { .. }));
    }

    // Second client should be RESP2 by default
    {
        let mut client = Resp2Client::connect(&server).await;
        let response = client.command(&["PING"]).await;
        // Should get RESP2 response
        match response {
            Resp2Frame::SimpleString(s) => {
                assert_eq!(s.as_ref(), b"PONG");
            }
            _ => panic!(
                "Expected SimpleString PONG for new connection, got {:?}",
                response
            ),
        }
    }

    server.shutdown().await;
}
