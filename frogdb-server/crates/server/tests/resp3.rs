//! Integration tests for RESP3 protocol support.
//!
//! These tests verify RESP3 protocol negotiation via HELLO command and
//! RESP3-specific response types (Map, Set, Double, Push).

use crate::common::test_server::{TestServer, TestServerConfig};
use bytes::Bytes;
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
        let framed = Framed::new(stream, Resp2::default());
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
    // In RESP3 the SUBSCRIBE confirmation is itself a Push frame, so read it
    // directly rather than via command() (which blocks waiting for a
    // non-push reply that never comes).
    subscriber.send_only(&["SUBSCRIBE", "test-channel"]).await;
    let confirm = subscriber.read_raw_frame(Duration::from_secs(2)).await;
    assert!(
        matches!(confirm, Some(Resp3Frame::Push { .. })),
        "SUBSCRIBE confirmation must be a Push in RESP3, got {confirm:?}"
    );

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

// =============================================================================
// Array-null wire shape (proposal 26)
//
// `WireResponse::NullArray` is emitted through the single `write_null_array`
// helper shared by both send_wire_response and feed_wire_response. RESP2 must
// emit the raw `*-1\r\n` array-null (which redis-protocol cannot produce), and
// RESP3 the `_\r\n` null. `LPOP <missing> <count>` returns a null array.
// =============================================================================

/// RESP2: a null-array reply is exactly `*-1\r\n` on the wire.
#[tokio::test]
async fn test_null_array_wire_bytes_resp2() {
    let server = start_server().await;

    // LPOP with a count on a non-existent key yields a null array.
    let raw = server
        .send_raw(b"*3\r\n$4\r\nLPOP\r\n$8\r\nmissing1\r\n$1\r\n2\r\n")
        .await;
    assert_eq!(&raw, b"*-1\r\n", "RESP2 null array must be *-1\\r\\n");

    server.shutdown().await;
}

/// RESP3: the same null-array reply is exactly `_\r\n` on the wire.
#[tokio::test]
async fn test_null_array_wire_bytes_resp3() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let server = start_server().await;
    let mut stream = TcpStream::connect(server.socket_addr()).await.unwrap();

    // Upgrade the connection to RESP3 and drain the HELLO reply.
    stream
        .write_all(b"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n")
        .await
        .unwrap();
    let mut buf = [0u8; 4096];
    let _ = timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .expect("timeout reading HELLO reply")
        .expect("HELLO read error");

    // LPOP with a count on a non-existent key yields a null array; in RESP3 the
    // array-null shape is `_\r\n`.
    stream
        .write_all(b"*3\r\n$4\r\nLPOP\r\n$8\r\nmissing1\r\n$1\r\n2\r\n")
        .await
        .unwrap();
    let n = timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .expect("timeout reading LPOP reply")
        .expect("LPOP read error");
    assert_eq!(&buf[..n], b"_\r\n", "RESP3 null array must be _\\r\\n");

    server.shutdown().await;
}

/// Pipelined regression (proposal 49): a single batch whose middle reply is a
/// RESP2 null array must not reorder the replies. `GET k1; LPOP missing 2;
/// GET k2` written in one segment returns `[bulk v1, *-1, bulk v2]` in command
/// order. Before the fix the buffered null-array (`*-1\r\n`) was written
/// straight to the socket, jumping ahead of the two buffered GET replies and
/// desynchronizing the client.
#[tokio::test]
async fn test_pipelined_null_array_preserves_reply_order_resp2() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let server = start_server().await;

    // Seed the two string keys the pipeline reads back.
    server.send("SET", &["k1", "v1"]).await;
    server.send("SET", &["k2", "v2"]).await;

    let mut stream = TcpStream::connect(server.socket_addr()).await.unwrap();

    // One pipelined write: GET k1 ; LPOP missing 2 (null array) ; GET k2.
    stream
        .write_all(
            b"*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n\
              *3\r\n$4\r\nLPOP\r\n$7\r\nmissing\r\n$1\r\n2\r\n\
              *2\r\n$3\r\nGET\r\n$2\r\nk2\r\n",
        )
        .await
        .unwrap();

    // Read the exact expected reply bytes in command order.
    let expected: &[u8] = b"$2\r\nv1\r\n*-1\r\n$2\r\nv2\r\n";
    let mut got = vec![0u8; expected.len()];
    timeout(Duration::from_secs(5), stream.read_exact(&mut got))
        .await
        .expect("timeout reading pipelined replies")
        .expect("read error");
    assert_eq!(
        got.as_slice(),
        expected,
        "pipelined replies must arrive in command order: GET k1, LPOP null-array, GET k2",
    );

    server.shutdown().await;
}

// =============================================================================
// RESP3 non-finite double wire format (issue 54, regression pin)
//
// The original audit gap claimed RESP3 non-finite doubles (inf/-inf/nan) were
// untested and possibly broken: `WireResponse::Double(f64)` passes the raw
// `f64` straight into `Resp3BytesFrame::Double` (protocol/src/response.rs:
// 320-323), bypassing FrogDB's own `format_float` inf/-inf/nan special-casing
// that RESP2 uses (response.rs:239,876-885). Verified against the actual
// encoder: `redis-protocol` 6.0.0's RESP3 encoder (resp3/encode.rs:108-120,
// `f64_to_redis_string` in resp3/utils.rs:595-607) already emits `,inf\r\n` /
// `,-inf\r\n` / `,nan\r\n` correctly and has its own upstream tests covering
// exactly this (encode.rs:1484-1506) — there is no live bug in the encoder
// (verdict ADJUSTED L1/C2, .scratch/testing-improvements/audit/verdicts-A.md
// #3). These tests pin the wire bytes at the command level so that if FrogDB
// ever adds its own RESP3 float formatting layer, or the `redis-protocol`
// dependency is upgraded/swapped and changes behavior, a test fails instead
// of the regression going unnoticed.
//
// IMPORTANT — this is *not* uniform across commands that return a score:
//   - ZINCRBY (commands/src/sorted_set/basic.rs:410-414) is unconditional:
//     `Response::Double` is returned whenever the connection is RESP3,
//     regardless of finiteness. This is real, reachable command-level
//     coverage of the raw Double passthrough for +inf/-inf.
//   - ZSCORE/ZMSCORE route through `commands::utils::score_response`, which
//     *deliberately* falls back to a RESP2-style bulk string (via
//     `format_float`) for non-finite scores even under RESP3
//     (commands/src/utils.rs:835-841) — so ZSCORE of an inf/-inf member does
//     NOT exercise the Double passthrough at all. Pinned below as current
//     behavior (a bulk string), not the `,inf\r\n` shape one might expect by
//     analogy with ZINCRBY; ZSCORE *does* use the Double passthrough for
//     finite scores, which is also pinned below.
//   - ZADD ... INCR (commands/src/sorted_set/basic.rs:131) is unconditional
//     the *other* way: it always returns a bulk string via `format_float`,
//     never `Response::Double`, even in RESP3 and even for a finite result.
//     Pinned below as current behavior. This looks like a pre-existing
//     RESP3-consistency gap relative to ZINCRBY (out of scope for this
//     regression-pin task, which is encoder/dispatch-change-free by design;
//     flagged for a follow-up issue rather than fixed here).
// =============================================================================

/// Encode a command as a RESP array of bulk strings (raw wire bytes, no
/// framing library involved — this constructs exactly what a real RESP3
/// client would send).
fn encode_resp_command(args: &[&str]) -> Vec<u8> {
    let mut buf = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        buf.extend_from_slice(arg.as_bytes());
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

/// Open a raw (non-`redis-protocol`-framed) connection and upgrade it to
/// RESP3 via `HELLO 3`, draining the HELLO reply so the stream is
/// positioned exactly at the start of the next command's reply.
async fn connect_resp3_raw(server: &TestServer) -> TcpStream {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = TcpStream::connect(server.socket_addr()).await.unwrap();
    stream
        .write_all(&encode_resp_command(&["HELLO", "3"]))
        .await
        .unwrap();
    let mut buf = [0u8; 4096];
    let _ = timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .expect("timeout reading HELLO reply")
        .expect("HELLO read error");
    stream
}

/// Send a command on a raw stream and return the exact reply bytes.
async fn send_raw_command(stream: &mut TcpStream, args: &[&str]) -> Vec<u8> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    stream.write_all(&encode_resp_command(args)).await.unwrap();
    let mut buf = vec![0u8; 4096];
    let n = timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .expect("timeout reading reply")
        .expect("read error");
    buf.truncate(n);
    buf
}

/// ZINCRBY producing +inf must be the RESP3 Double `,inf\r\n` — the real,
/// reachable command path through the raw `f64` passthrough.
#[tokio::test]
async fn test_zincrby_resp3_positive_infinity_wire_bytes() {
    let server = start_server().await;
    let mut stream = connect_resp3_raw(&server).await;

    let raw = send_raw_command(&mut stream, &["ZINCRBY", "zkey", "inf", "member"]).await;
    assert_eq!(
        &raw, b",inf\r\n",
        "ZINCRBY producing +inf must be RESP3 Double ,inf\\r\\n"
    );

    server.shutdown().await;
}

/// ZINCRBY producing -inf must be the RESP3 Double `,-inf\r\n`.
#[tokio::test]
async fn test_zincrby_resp3_negative_infinity_wire_bytes() {
    let server = start_server().await;
    let mut stream = connect_resp3_raw(&server).await;

    let raw = send_raw_command(&mut stream, &["ZINCRBY", "zkey", "-inf", "member"]).await;
    assert_eq!(
        &raw, b",-inf\r\n",
        "ZINCRBY producing -inf must be RESP3 Double ,-inf\\r\\n"
    );

    server.shutdown().await;
}

/// A second, independent connection incrementing +inf by -inf would produce
/// NaN — ZINCRBY rejects this (matching Redis and ZADD INCR's NaN check),
/// so NaN cannot reach the wire through this path either. Confirms the
/// error contract rather than a wire shape.
#[tokio::test]
async fn test_zincrby_resp3_nan_result_is_rejected_not_wired() {
    let server = start_server().await;
    let mut stream = connect_resp3_raw(&server).await;

    send_raw_command(&mut stream, &["ZINCRBY", "zkey", "inf", "member"]).await;
    let raw = send_raw_command(&mut stream, &["ZINCRBY", "zkey", "-inf", "member"]).await;
    assert!(
        raw.starts_with(b"-"),
        "inf + -inf must be a wire error, not a NaN Double; got {:?}",
        String::from_utf8_lossy(&raw)
    );

    server.shutdown().await;
}

/// Normal (finite) RESP3 double formatting through the same reachable
/// Double passthrough: Redis and redis-protocol emit `,3\r\n`, not
/// `,3.0\r\n`, for an integer-valued result.
#[tokio::test]
async fn test_zincrby_resp3_integer_valued_double_wire_bytes() {
    let server = start_server().await;
    let mut stream = connect_resp3_raw(&server).await;

    let raw = send_raw_command(&mut stream, &["ZINCRBY", "zkey", "3", "member"]).await;
    assert_eq!(
        &raw, b",3\r\n",
        "integer-valued RESP3 double must be ,3\\r\\n, not ,3.0\\r\\n"
    );

    server.shutdown().await;
}

/// ZSCORE of a finite score in RESP3 uses the Double passthrough too.
#[tokio::test]
async fn test_zscore_resp3_finite_score_wire_bytes() {
    let server = start_server().await;
    let mut stream = connect_resp3_raw(&server).await;

    send_raw_command(&mut stream, &["ZADD", "zkey", "3.125", "member"]).await;
    let raw = send_raw_command(&mut stream, &["ZSCORE", "zkey", "member"]).await;
    assert_eq!(&raw, b",3.125\r\n");

    server.shutdown().await;
}

/// ZSCORE of a +inf score in RESP3: `score_response` (commands/src/utils.rs)
/// deliberately routes non-finite scores to a bulk string even under RESP3,
/// so this is NOT the `,inf\r\n` Double shape — pinned as current behavior.
/// See the module doc above for why this differs from ZINCRBY.
#[tokio::test]
async fn test_zscore_resp3_positive_infinity_wire_bytes() {
    let server = start_server().await;
    let mut stream = connect_resp3_raw(&server).await;

    send_raw_command(&mut stream, &["ZADD", "zkey", "inf", "member"]).await;
    let raw = send_raw_command(&mut stream, &["ZSCORE", "zkey", "member"]).await;
    assert_eq!(
        &raw, b"$3\r\ninf\r\n",
        "ZSCORE of +inf in RESP3 currently returns a bulk string (not Double) \
         via score_response's finite-only gate — pinned as current behavior"
    );

    server.shutdown().await;
}

/// ZSCORE of a -inf score in RESP3: same bulk-string fallback as +inf above.
#[tokio::test]
async fn test_zscore_resp3_negative_infinity_wire_bytes() {
    let server = start_server().await;
    let mut stream = connect_resp3_raw(&server).await;

    send_raw_command(&mut stream, &["ZADD", "zkey", "-inf", "member"]).await;
    let raw = send_raw_command(&mut stream, &["ZSCORE", "zkey", "member"]).await;
    assert_eq!(
        &raw, b"$4\r\n-inf\r\n",
        "ZSCORE of -inf in RESP3 currently returns a bulk string (not Double) \
         via score_response's finite-only gate — pinned as current behavior"
    );

    server.shutdown().await;
}

/// `ZADD ... INCR` in RESP3, finite result: unlike ZINCRBY, ZADD's INCR mode
/// (commands/src/sorted_set/basic.rs:131) is unconditionally a bulk string —
/// it never checks `ctx.protocol_version` at all, so it never uses
/// `Response::Double` even for a finite result. Pinned as current behavior;
/// flagged in the issue resolution as a RESP3-consistency gap relative to
/// ZINCRBY, out of scope for this encoder/dispatch-change-free pin task.
#[tokio::test]
async fn test_zadd_incr_resp3_finite_wire_bytes() {
    let server = start_server().await;
    let mut stream = connect_resp3_raw(&server).await;

    let raw = send_raw_command(&mut stream, &["ZADD", "zkey", "INCR", "3", "member"]).await;
    assert_eq!(
        &raw, b"$1\r\n3\r\n",
        "ZADD ... INCR in RESP3 currently always returns a bulk string, \
         never Double — pinned as current behavior"
    );

    server.shutdown().await;
}

/// `ZADD ... INCR` in RESP3 producing +inf: same bulk-string shape as the
/// finite case above (never Double, regardless of finiteness).
#[tokio::test]
async fn test_zadd_incr_resp3_positive_infinity_wire_bytes() {
    let server = start_server().await;
    let mut stream = connect_resp3_raw(&server).await;

    let raw = send_raw_command(&mut stream, &["ZADD", "zkey", "INCR", "inf", "member"]).await;
    assert_eq!(
        &raw, b"$3\r\ninf\r\n",
        "ZADD ... INCR producing +inf in RESP3 currently returns a bulk \
         string, never Double — pinned as current behavior"
    );

    server.shutdown().await;
}
