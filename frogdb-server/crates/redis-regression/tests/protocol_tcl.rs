//! Rust port of Redis 8.6.0 `unit/protocol.tcl` test suite.
//!
//! Excluded tests:
//! - Protocol desync regression tests (complex multi-step desync simulation)
//! - RESP3 attribute tests (require `needs:debug` and CLIENT TRACKING)
//! - RESP3 bool/verbatim parsing tests (`needs:debug resp3`)
//! - Big number parsing test (`needs:debug resp3`)
//! - Inline command tests (require special inline parsing)
//! - Argument rewriting / INCRBYFLOAT test (valgrind-specific, `needs:debug`)
//! - Regression for crash with blocking ops and pipelining (`needs:repl`)
//!
//! ## Intentional exclusions
//!
//! Protocol desync / regression simulators (require multi-step injected
//! desync; covered separately by frogdb-protocol's own fuzz/property tests):
//! - `Protocol desync regression test #$c` — redis-specific — Redis-internal desync simulation
//! - `Regression for a crash with blocking ops and pipelining` — intentional-incompatibility:replication — needs:repl
//! - `Regression for a crash with cron release of client arguments` — redis-specific — Redis-internal cron path
//!
//! RESP3 attribute / readraw / bool / verbatim tests:
//! - `RESP3 attributes` — intentional-incompatibility:protocol — RESP3-only
//! - `RESP3 attributes readraw` — intentional-incompatibility:protocol — RESP3-only
//! - `RESP3 attributes on RESP2` — intentional-incompatibility:protocol — RESP3-only
//! - `test big number parsing` — intentional-incompatibility:protocol — RESP3-only + needs:debug
//! - `test bool parsing` — intentional-incompatibility:protocol — RESP3-only + needs:debug
//! - `test verbatim str parsing` — intentional-incompatibility:protocol — RESP3-only + needs:debug
//!
//! INCRBYFLOAT argument-rewriting (valgrind-specific):
//! - `test argument rewriting - issue 9598` — intentional-incompatibility:debug — needs:debug
//! - Regression for crash with cron release of client arguments (timing-dependent)

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// ---------------------------------------------------------------------------
// Empty query / basic protocol handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_handle_an_empty_query() {
    let server = TestServer::start_standalone().await;

    // Send bare \r\n then PING — server should ignore the empty line and respond to PING.
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", server.port()))
        .await
        .unwrap();
    // Empty query followed by a proper PING command
    stream.write_all(b"\r\n*1\r\n$4\r\nPING\r\n").await.unwrap();
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(
        response.contains("PONG"),
        "expected PONG after empty query, got: {response}"
    );
}

// ---------------------------------------------------------------------------
// Negative multibulk length
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_negative_multibulk_length() {
    let server = TestServer::start_standalone().await;

    // A negative multibulk count should be ignored; a subsequent PING should work.
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", server.port()))
        .await
        .unwrap();
    stream
        .write_all(b"*-10\r\n*1\r\n$4\r\nPING\r\n")
        .await
        .unwrap();
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(
        response.contains("PONG"),
        "expected PONG after negative multibulk length, got: {response}"
    );
}

// ---------------------------------------------------------------------------
// Out of range multibulk length
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_out_of_range_multibulk_length() {
    let server = TestServer::start_standalone().await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", server.port()))
        .await
        .unwrap();
    stream.write_all(b"*3000000000\r\n").await.unwrap();
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(
        response.contains("-ERR"),
        "expected error for out-of-range multibulk length, got: {response}"
    );
}

// ---------------------------------------------------------------------------
// Wrong multibulk payload header
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_wrong_multibulk_payload_header() {
    let server = TestServer::start_standalone().await;

    // Third argument uses "fooz\r\n" instead of "$N\r\n..."
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", server.port()))
        .await
        .unwrap();
    stream
        .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nx\r\nfooz\r\n")
        .await
        .unwrap();
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(
        response.contains("-ERR"),
        "expected protocol error for wrong payload header, got: {response}"
    );
}

// ---------------------------------------------------------------------------
// Negative multibulk payload length
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_negative_multibulk_payload_length() {
    let server = TestServer::start_standalone().await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", server.port()))
        .await
        .unwrap();
    stream
        .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$-10\r\n")
        .await
        .unwrap();
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(
        response.contains("-ERR"),
        "expected error for negative bulk length, got: {response}"
    );
}

// ---------------------------------------------------------------------------
// Out of range multibulk payload length
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_out_of_range_multibulk_payload_length() {
    let server = TestServer::start_standalone().await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", server.port()))
        .await
        .unwrap();
    stream
        .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$2000000000\r\n")
        .await
        .unwrap();
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(
        response.contains("-ERR"),
        "expected error for out-of-range bulk length, got: {response}"
    );
}

// ---------------------------------------------------------------------------
// Non-number multibulk payload length
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_non_number_multibulk_payload_length() {
    let server = TestServer::start_standalone().await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", server.port()))
        .await
        .unwrap();
    stream
        .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$blabla\r\n")
        .await
        .unwrap();
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(
        response.contains("-ERR"),
        "expected error for non-number bulk length, got: {response}"
    );
}

// ---------------------------------------------------------------------------
// Multi bulk request not followed by bulk arguments
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_multi_bulk_request_not_followed_by_bulk_arguments() {
    let server = TestServer::start_standalone().await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", server.port()))
        .await
        .unwrap();
    stream.write_all(b"*1\r\nfoo\r\n").await.unwrap();
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(
        response.contains("-ERR"),
        "expected protocol error when bulk arg missing '$', got: {response}"
    );
}

// ---------------------------------------------------------------------------
// Generic wrong number of args
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_generic_wrong_number_of_args() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["PING", "x", "y", "z"]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// Unbalanced number of quotes (inline command)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_unbalanced_number_of_quotes() {
    let server = TestServer::start_standalone().await;

    // Send an inline command with unbalanced quotes
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", server.port()))
        .await
        .unwrap();
    stream
        .write_all(b"set \"\"\"test-key\"\"\" test-value\r\n")
        .await
        .unwrap();
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(
        response.contains("-ERR"),
        "expected error for unbalanced quotes, got: {response}"
    );
}

// ---------------------------------------------------------------------------
// Raw protocol response — SRANDMEMBER on nonexisting key returns nil
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_raw_protocol_response_nil() {
    let server = TestServer::start_standalone().await;

    // SRANDMEMBER on a nonexistent key should return a nil bulk reply ($-1\r\n)
    let raw = server
        .send_raw(b"*2\r\n$11\r\nSRANDMEMBER\r\n$15\r\nnonexisting_key\r\n")
        .await;
    let response = String::from_utf8_lossy(&raw);
    assert!(
        response.starts_with("$-1\r\n"),
        "expected nil bulk reply ($-1), got: {response}"
    );
}

// ---------------------------------------------------------------------------
// Raw protocol response — multiline (SADD + SRANDMEMBER with COUNT)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_raw_protocol_response_multiline() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Seed a set with one member
    assert_integer_eq(&client.command(&["SADD", "ss", "a"]).await, 1);

    // SRANDMEMBER ss 100 — should return an array with one element "a"
    let raw = server
        .send_raw(b"*3\r\n$11\r\nSRANDMEMBER\r\n$2\r\nss\r\n$3\r\n100\r\n")
        .await;
    let response = String::from_utf8_lossy(&raw);
    // Expect: *1\r\n$1\r\na\r\n
    assert!(
        response.starts_with("*1\r\n"),
        "expected array of length 1, got: {response}"
    );
    assert!(
        response.contains("$1\r\na\r\n"),
        "expected bulk string 'a', got: {response}"
    );
}

// ---------------------------------------------------------------------------
// Bulk reply protocol — various encodings
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_bulk_reply_protocol_int_encoding() {
    let server = TestServer::start_standalone().await;

    // value=2 (small int)
    let raw = server
        .send_raw(b"*3\r\n$3\r\nSET\r\n$4\r\ncrlf\r\n$1\r\n2\r\n")
        .await;
    let response = String::from_utf8_lossy(&raw);
    assert!(
        response.contains("+OK"),
        "expected +OK for SET, got: {response}"
    );

    let raw = server.send_raw(b"*2\r\n$3\r\nGET\r\n$4\r\ncrlf\r\n").await;
    let response = String::from_utf8_lossy(&raw);
    assert_eq!(response, "$1\r\n2\r\n", "expected bulk reply for value 2");
}

#[tokio::test]
async fn tcl_bulk_reply_protocol_int32_max() {
    let server = TestServer::start_standalone().await;

    // value=2147483647 (int32 max)
    server
        .send_raw(b"*3\r\n$3\r\nSET\r\n$4\r\ncrlf\r\n$10\r\n2147483647\r\n")
        .await;

    let raw = server.send_raw(b"*2\r\n$3\r\nGET\r\n$4\r\ncrlf\r\n").await;
    let response = String::from_utf8_lossy(&raw);
    assert_eq!(
        response, "$10\r\n2147483647\r\n",
        "expected bulk reply for int32 max"
    );
}

#[tokio::test]
async fn tcl_bulk_reply_protocol_int32_min() {
    let server = TestServer::start_standalone().await;

    // value=-2147483648 (int32 min)
    server
        .send_raw(b"*3\r\n$3\r\nSET\r\n$4\r\ncrlf\r\n$11\r\n-2147483648\r\n")
        .await;

    let raw = server.send_raw(b"*2\r\n$3\r\nGET\r\n$4\r\ncrlf\r\n").await;
    let response = String::from_utf8_lossy(&raw);
    assert_eq!(
        response, "$11\r\n-2147483648\r\n",
        "expected bulk reply for int32 min"
    );
}

#[tokio::test]
async fn tcl_bulk_reply_protocol_beyond_i64_negative() {
    let server = TestServer::start_standalone().await;

    // value=-9223372036854775809 (beyond i64 range, embstr encoding)
    server
        .send_raw(b"*3\r\n$3\r\nSET\r\n$4\r\ncrlf\r\n$20\r\n-9223372036854775809\r\n")
        .await;

    let raw = server.send_raw(b"*2\r\n$3\r\nGET\r\n$4\r\ncrlf\r\n").await;
    let response = String::from_utf8_lossy(&raw);
    assert_eq!(
        response, "$20\r\n-9223372036854775809\r\n",
        "expected bulk reply for value beyond i64 negative"
    );
}

#[tokio::test]
async fn tcl_bulk_reply_protocol_beyond_i64_positive() {
    let server = TestServer::start_standalone().await;

    // value=9223372036854775808 (beyond i64 range, embstr encoding)
    server
        .send_raw(b"*3\r\n$3\r\nSET\r\n$4\r\ncrlf\r\n$19\r\n9223372036854775808\r\n")
        .await;

    let raw = server.send_raw(b"*2\r\n$3\r\nGET\r\n$4\r\ncrlf\r\n").await;
    let response = String::from_utf8_lossy(&raw);
    assert_eq!(
        response, "$19\r\n9223372036854775808\r\n",
        "expected bulk reply for value beyond i64 positive"
    );
}

#[tokio::test]
async fn tcl_bulk_reply_protocol_embstr() {
    let server = TestServer::start_standalone().await;

    // value=aaaaaaaaaaaaaaaa (16 chars, embstr encoding)
    server
        .send_raw(b"*3\r\n$3\r\nSET\r\n$4\r\ncrlf\r\n$16\r\naaaaaaaaaaaaaaaa\r\n")
        .await;

    let raw = server.send_raw(b"*2\r\n$3\r\nGET\r\n$4\r\ncrlf\r\n").await;
    let response = String::from_utf8_lossy(&raw);
    assert_eq!(
        response, "$16\r\naaaaaaaaaaaaaaaa\r\n",
        "expected bulk reply for 16-char embstr"
    );
}

#[tokio::test]
async fn tcl_bulk_reply_protocol_raw_string_45_chars() {
    let server = TestServer::start_standalone().await;

    // 45 'a' chars (raw string encoding)
    let value = "a".repeat(45);
    let set_cmd = format!("*3\r\n$3\r\nSET\r\n$4\r\ncrlf\r\n$45\r\n{value}\r\n");
    server.send_raw(set_cmd.as_bytes()).await;

    let raw = server.send_raw(b"*2\r\n$3\r\nGET\r\n$4\r\ncrlf\r\n").await;
    let response = String::from_utf8_lossy(&raw);
    let expected = format!("$45\r\n{value}\r\n");
    assert_eq!(
        response, expected,
        "expected bulk reply for 45-char raw string"
    );
}

// ---------------------------------------------------------------------------
// Large number of args (MSET with 10000 key-value pairs)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_test_large_number_of_args() {
    // Use 1 shard to avoid CROSSSLOT errors — the 10001 keys span multiple hash
    // slots and FrogDB multi-shard standalone mode requires allow_cross_slot.
    // Redis standalone has no slot concept, so this test only validates large arg count handling.
    use frogdb_test_harness::server::TestServerConfig;
    let server = TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(1),
        ..Default::default()
    })
    .await;
    let mut client = server.connect().await;

    client.command(&["FLUSHDB"]).await;

    // Build MSET with 10000 key-value pairs plus one extra tagged key
    let mut args: Vec<String> = Vec::with_capacity(20003);
    args.push("MSET".to_string());
    for i in 0..10000 {
        args.push(format!("k{i}"));
        args.push(format!("v{i}"));
    }
    args.push("{k}2".to_string());
    args.push("v2".to_string());

    let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    let resp = client.command(&arg_refs).await;
    assert_ok(&resp);

    let resp = client.command(&["GET", "{k}2"]).await;
    assert_bulk_eq(&resp, b"v2");
}
