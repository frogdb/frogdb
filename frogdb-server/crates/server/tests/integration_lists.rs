//! Integration tests for list commands (LPUSH, RPUSH, LPOP, RPOP, etc.)

mod common;

use bytes::Bytes;
use common::test_server::TestServer;
use frogdb_protocol::Response;

#[tokio::test]
async fn test_lpush_rpush() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // LPUSH
    let response = client.command(&["LPUSH", "mylist", "a"]).await;
    assert_eq!(response, Response::Integer(1));

    let response = client.command(&["LPUSH", "mylist", "b", "c"]).await;
    assert_eq!(response, Response::Integer(3)); // c, b, a

    // RPUSH
    let response = client.command(&["RPUSH", "mylist", "d"]).await;
    assert_eq!(response, Response::Integer(4)); // c, b, a, d

    server.shutdown().await;
}

#[tokio::test]
async fn test_lpop_rpop() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["RPUSH", "mylist", "a", "b", "c", "d"])
        .await;

    // LPOP
    let response = client.command(&["LPOP", "mylist"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("a"))));

    // RPOP
    let response = client.command(&["RPOP", "mylist"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("d"))));

    // LPOP with count
    let response = client.command(&["LPOP", "mylist", "2"]).await;
    match response {
        Response::Array(items) => {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], Response::Bulk(Some(Bytes::from("b"))));
            assert_eq!(items[1], Response::Bulk(Some(Bytes::from("c"))));
        }
        _ => panic!("Expected array response"),
    }

    // LPOP empty list
    let response = client.command(&["LPOP", "mylist"]).await;
    assert_eq!(response, Response::Bulk(None));

    server.shutdown().await;
}

#[tokio::test]
async fn test_llen() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // LLEN nonexistent
    let response = client.command(&["LLEN", "mylist"]).await;
    assert_eq!(response, Response::Integer(0));

    client.command(&["RPUSH", "mylist", "a", "b", "c"]).await;

    let response = client.command(&["LLEN", "mylist"]).await;
    assert_eq!(response, Response::Integer(3));

    server.shutdown().await;
}

#[tokio::test]
async fn test_lrange() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["RPUSH", "mylist", "a", "b", "c", "d", "e"])
        .await;

    // Full range
    let response = client.command(&["LRANGE", "mylist", "0", "-1"]).await;
    match response {
        Response::Array(items) => {
            assert_eq!(items.len(), 5);
            assert_eq!(items[0], Response::Bulk(Some(Bytes::from("a"))));
            assert_eq!(items[4], Response::Bulk(Some(Bytes::from("e"))));
        }
        _ => panic!("Expected array response"),
    }

    // Partial range
    let response = client.command(&["LRANGE", "mylist", "1", "3"]).await;
    match response {
        Response::Array(items) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], Response::Bulk(Some(Bytes::from("b"))));
            assert_eq!(items[2], Response::Bulk(Some(Bytes::from("d"))));
        }
        _ => panic!("Expected array response"),
    }

    // Negative indices
    let response = client.command(&["LRANGE", "mylist", "-3", "-1"]).await;
    match response {
        Response::Array(items) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], Response::Bulk(Some(Bytes::from("c"))));
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_lindex_lset() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "mylist", "a", "b", "c"]).await;

    // LINDEX
    let response = client.command(&["LINDEX", "mylist", "1"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("b"))));

    // LINDEX negative
    let response = client.command(&["LINDEX", "mylist", "-1"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("c"))));

    // LINDEX out of range
    let response = client.command(&["LINDEX", "mylist", "100"]).await;
    assert_eq!(response, Response::Bulk(None));

    // LSET
    let response = client.command(&["LSET", "mylist", "1", "updated"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    let response = client.command(&["LINDEX", "mylist", "1"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("updated"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_ltrim() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["RPUSH", "mylist", "a", "b", "c", "d", "e"])
        .await;

    let response = client.command(&["LTRIM", "mylist", "1", "3"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    let response = client.command(&["LRANGE", "mylist", "0", "-1"]).await;
    match response {
        Response::Array(items) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], Response::Bulk(Some(Bytes::from("b"))));
            assert_eq!(items[2], Response::Bulk(Some(Bytes::from("d"))));
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_linsert() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "mylist", "a", "c"]).await;

    // LINSERT BEFORE
    let response = client
        .command(&["LINSERT", "mylist", "BEFORE", "c", "b"])
        .await;
    assert_eq!(response, Response::Integer(3));

    let response = client.command(&["LRANGE", "mylist", "0", "-1"]).await;
    match response {
        Response::Array(items) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[1], Response::Bulk(Some(Bytes::from("b"))));
        }
        _ => panic!("Expected array response"),
    }

    // LINSERT AFTER
    let response = client
        .command(&["LINSERT", "mylist", "AFTER", "c", "d"])
        .await;
    assert_eq!(response, Response::Integer(4));

    server.shutdown().await;
}

#[tokio::test]
async fn test_lrem() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["RPUSH", "mylist", "a", "b", "a", "c", "a"])
        .await;

    // LREM count > 0 (remove from head)
    let response = client.command(&["LREM", "mylist", "2", "a"]).await;
    assert_eq!(response, Response::Integer(2));

    let response = client.command(&["LLEN", "mylist"]).await;
    assert_eq!(response, Response::Integer(3)); // b, c, a

    server.shutdown().await;
}

#[tokio::test]
async fn test_type_command_list() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "mylist", "a"]).await;

    let response = client.command(&["TYPE", "mylist"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("list")));

    server.shutdown().await;
}
