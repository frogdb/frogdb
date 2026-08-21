//! Integration tests for basic Redis commands (PING, ECHO, SET, GET, DEL, EXISTS).

use crate::common::test_server::TestServer;
use bytes::Bytes;
use frogdb_protocol::{Response, SafeStatus};
use frogdb_telemetry::testing::get_counter;
use std::time::Duration;

#[tokio::test]
async fn test_ping_pong() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["PING"]).await;
    assert_eq!(response, Response::pong());

    server.shutdown().await;
}

#[tokio::test]
async fn test_ping_with_message() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["PING", "hello"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("hello"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_echo() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["ECHO", "hello world"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("hello world"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_set_get() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Get baseline metrics
    let before = server.fetch_metrics().await;
    let set_before = get_counter(&before, "frogdb_commands_total", &[("command", "SET")]);
    let get_before = get_counter(&before, "frogdb_commands_total", &[("command", "GET")]);

    // SET
    let response = client.command(&["SET", "foo", "bar"]).await;
    assert_eq!(response, Response::ok());

    // GET
    let response = client.command(&["GET", "foo"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("bar"))));

    // Verify metrics recorded
    tokio::time::sleep(Duration::from_millis(50)).await;
    let after = server.fetch_metrics().await;
    let set_after = get_counter(&after, "frogdb_commands_total", &[("command", "SET")]);
    let get_after = get_counter(&after, "frogdb_commands_total", &[("command", "GET")]);

    assert_eq!(set_after, set_before + 1.0, "SET command should be counted");
    assert_eq!(get_after, get_before + 1.0, "GET command should be counted");

    server.shutdown().await;
}

#[tokio::test]
async fn test_get_nonexistent() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["GET", "nonexistent"]).await;
    assert_eq!(response, Response::Bulk(None));

    server.shutdown().await;
}

#[tokio::test]
async fn test_del() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // SET
    client.command(&["SET", "mykey", "myvalue"]).await;

    // DEL existing
    let response = client.command(&["DEL", "mykey"]).await;
    assert_eq!(response, Response::Integer(1));

    // DEL nonexistent
    let response = client.command(&["DEL", "mykey"]).await;
    assert_eq!(response, Response::Integer(0));

    // Verify deleted
    let response = client.command(&["GET", "mykey"]).await;
    assert_eq!(response, Response::Bulk(None));

    server.shutdown().await;
}

#[tokio::test]
async fn test_exists() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // EXISTS nonexistent
    let response = client.command(&["EXISTS", "mykey"]).await;
    assert_eq!(response, Response::Integer(0));

    // SET
    client.command(&["SET", "mykey", "myvalue"]).await;

    // EXISTS existing
    let response = client.command(&["EXISTS", "mykey"]).await;
    assert_eq!(response, Response::Integer(1));

    server.shutdown().await;
}

#[tokio::test]
async fn test_unknown_command() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["FOOBAR"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"ERR unknown command")));

    server.shutdown().await;
}

#[tokio::test]
async fn test_wrong_arity() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // GET requires exactly 1 argument
    let response = client.command(&["GET"]).await;
    assert!(
        matches!(response, Response::Error(e) if e.starts_with(b"ERR wrong number of arguments"))
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_set_overwrites() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "key", "value1"]).await;
    let response = client.command(&["GET", "key"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("value1"))));

    client.command(&["SET", "key", "value2"]).await;
    let response = client.command(&["GET", "key"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("value2"))));

    server.shutdown().await;
}

/// Bare `COMMAND` (no subcommand) returns the real, non-empty registry —
/// one 10-element `COMMAND INFO`-shaped entry per registered command,
/// matching `COMMAND COUNT` — not the old empty-array placeholder.
#[tokio::test]
async fn test_command_returns_registry_info() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let count = client.command(&["COMMAND", "COUNT"]).await;
    let Response::Integer(count) = count else {
        panic!("expected COMMAND COUNT to return an Integer, got {count:?}");
    };
    assert!(count > 0, "registry should be non-empty");

    let response = client.command(&["COMMAND"]).await;
    match response {
        Response::Array(entries) => {
            assert_eq!(
                entries.len(),
                count as usize,
                "bare COMMAND should list the same commands COMMAND COUNT counts"
            );
            for entry in &entries {
                match entry {
                    Response::Array(fields) => assert_eq!(
                        fields.len(),
                        10,
                        "each COMMAND entry is the 10-element COMMAND INFO shape, got {entry:?}"
                    ),
                    other => panic!("expected each entry to be an Array, got {other:?}"),
                }
            }
        }
        other => panic!("expected bare COMMAND to return an Array, got {other:?}"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_multiple_clients() {
    let server = TestServer::start_standalone().await;

    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    // Client 1 sets a value
    client1.command(&["SET", "shared", "value"]).await;

    // Client 2 reads it
    let response = client2.command(&["GET", "shared"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("value"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_type_command_string() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "value"]).await;

    let response = client.command(&["TYPE", "mykey"]).await;
    assert_eq!(
        response,
        Response::Simple(SafeStatus::from_static("string"))
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_type_nonexistent() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["TYPE", "nonexistent"]).await;
    assert_eq!(response, Response::Simple(SafeStatus::from_static("none")));

    server.shutdown().await;
}
