//! Integration tests for hash commands (HSET, HGET, HDEL, etc.)

mod common;

use bytes::Bytes;
use common::test_server::TestServer;
use frogdb_protocol::Response;

#[tokio::test]
async fn test_hset_hget() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // HSET single field
    let response = client
        .command(&["HSET", "myhash", "field1", "value1"])
        .await;
    assert_eq!(response, Response::Integer(1)); // 1 new field added

    // HGET existing field
    let response = client.command(&["HGET", "myhash", "field1"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("value1"))));

    // HGET nonexistent field
    let response = client.command(&["HGET", "myhash", "nonexistent"]).await;
    assert_eq!(response, Response::Bulk(None));

    // HSET multiple fields
    let response = client
        .command(&["HSET", "myhash", "field2", "value2", "field3", "value3"])
        .await;
    assert_eq!(response, Response::Integer(2)); // 2 new fields added

    // Update existing field (returns 0)
    let response = client
        .command(&["HSET", "myhash", "field1", "updated"])
        .await;
    assert_eq!(response, Response::Integer(0)); // 0 new fields, 1 updated

    server.shutdown().await;
}

#[tokio::test]
async fn test_hsetnx() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // HSETNX new field
    let response = client
        .command(&["HSETNX", "myhash", "field1", "value1"])
        .await;
    assert_eq!(response, Response::Integer(1));

    // HSETNX existing field
    let response = client
        .command(&["HSETNX", "myhash", "field1", "value2"])
        .await;
    assert_eq!(response, Response::Integer(0));

    // Verify original value unchanged
    let response = client.command(&["HGET", "myhash", "field1"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("value1"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_hdel() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "myhash", "f1", "v1", "f2", "v2", "f3", "v3"])
        .await;

    // HDEL existing fields
    let response = client.command(&["HDEL", "myhash", "f1", "f2"]).await;
    assert_eq!(response, Response::Integer(2));

    // HDEL nonexistent
    let response = client.command(&["HDEL", "myhash", "f1"]).await;
    assert_eq!(response, Response::Integer(0));

    // Verify f3 still exists
    let response = client.command(&["HGET", "myhash", "f3"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("v3"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_hmget() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "myhash", "f1", "v1", "f2", "v2"])
        .await;

    let response = client
        .command(&["HMGET", "myhash", "f1", "nonexistent", "f2"])
        .await;
    assert_eq!(
        response,
        Response::Array(vec![
            Response::Bulk(Some(Bytes::from("v1"))),
            Response::Bulk(None),
            Response::Bulk(Some(Bytes::from("v2"))),
        ])
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_hgetall() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "myhash", "f1", "v1"]).await;

    let response = client.command(&["HGETALL", "myhash"]).await;
    // Returns flat array: [field1, value1, field2, value2, ...]
    match response {
        Response::Array(items) => {
            assert_eq!(items.len(), 2);
        }
        _ => panic!("Expected array response"),
    }

    // HGETALL nonexistent key
    let response = client.command(&["HGETALL", "nonexistent"]).await;
    assert_eq!(response, Response::Array(vec![]));

    server.shutdown().await;
}

#[tokio::test]
async fn test_hlen_hexists() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // HLEN nonexistent
    let response = client.command(&["HLEN", "myhash"]).await;
    assert_eq!(response, Response::Integer(0));

    client
        .command(&["HSET", "myhash", "f1", "v1", "f2", "v2"])
        .await;

    // HLEN
    let response = client.command(&["HLEN", "myhash"]).await;
    assert_eq!(response, Response::Integer(2));

    // HEXISTS
    let response = client.command(&["HEXISTS", "myhash", "f1"]).await;
    assert_eq!(response, Response::Integer(1));

    let response = client.command(&["HEXISTS", "myhash", "nonexistent"]).await;
    assert_eq!(response, Response::Integer(0));

    server.shutdown().await;
}

#[tokio::test]
async fn test_hincrby() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // HINCRBY on nonexistent field (creates it)
    let response = client.command(&["HINCRBY", "myhash", "counter", "5"]).await;
    assert_eq!(response, Response::Integer(5));

    // HINCRBY on existing field
    let response = client.command(&["HINCRBY", "myhash", "counter", "3"]).await;
    assert_eq!(response, Response::Integer(8));

    // HINCRBY negative
    let response = client
        .command(&["HINCRBY", "myhash", "counter", "-2"])
        .await;
    assert_eq!(response, Response::Integer(6));

    server.shutdown().await;
}

#[tokio::test]
async fn test_hincrbyfloat() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client
        .command(&["HINCRBYFLOAT", "myhash", "price", "10.5"])
        .await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("10.5"))));

    let response = client
        .command(&["HINCRBYFLOAT", "myhash", "price", "0.5"])
        .await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("11"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_type_command_hash() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "myhash", "f1", "v1"]).await;

    let response = client.command(&["TYPE", "myhash"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("hash")));

    server.shutdown().await;
}
