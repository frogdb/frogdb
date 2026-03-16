//! Integration tests for set commands (SADD, SMEMBERS, SINTER, SUNION, etc.)

use crate::common::test_server::TestServer;
use bytes::Bytes;
use frogdb_protocol::Response;

#[tokio::test]
async fn test_sadd_smembers() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // SADD
    let response = client.command(&["SADD", "myset", "a", "b", "c"]).await;
    assert_eq!(response, Response::Integer(3));

    // SADD with duplicates
    let response = client.command(&["SADD", "myset", "a", "d"]).await;
    assert_eq!(response, Response::Integer(1)); // Only d was added

    // SMEMBERS
    let response = client.command(&["SMEMBERS", "myset"]).await;
    match response {
        Response::Array(items) => {
            assert_eq!(items.len(), 4);
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_srem() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SADD", "myset", "a", "b", "c"]).await;

    let response = client.command(&["SREM", "myset", "a", "d"]).await;
    assert_eq!(response, Response::Integer(1)); // Only a was removed

    let response = client.command(&["SCARD", "myset"]).await;
    assert_eq!(response, Response::Integer(2));

    server.shutdown().await;
}

#[tokio::test]
async fn test_sismember_smismember() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SADD", "myset", "a", "b"]).await;

    // SISMEMBER
    let response = client.command(&["SISMEMBER", "myset", "a"]).await;
    assert_eq!(response, Response::Integer(1));

    let response = client.command(&["SISMEMBER", "myset", "c"]).await;
    assert_eq!(response, Response::Integer(0));

    // SMISMEMBER
    let response = client
        .command(&["SMISMEMBER", "myset", "a", "c", "b"])
        .await;
    assert_eq!(
        response,
        Response::Array(vec![
            Response::Integer(1),
            Response::Integer(0),
            Response::Integer(1),
        ])
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_scard() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // SCARD nonexistent
    let response = client.command(&["SCARD", "myset"]).await;
    assert_eq!(response, Response::Integer(0));

    client.command(&["SADD", "myset", "a", "b", "c"]).await;

    let response = client.command(&["SCARD", "myset"]).await;
    assert_eq!(response, Response::Integer(3));

    server.shutdown().await;
}

#[tokio::test]
async fn test_sunion() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SADD", "{set}1", "a", "b"]).await;
    client.command(&["SADD", "{set}2", "b", "c"]).await;

    let response = client.command(&["SUNION", "{set}1", "{set}2"]).await;
    match response {
        Response::Array(items) => {
            assert_eq!(items.len(), 3); // a, b, c
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_sinter() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SADD", "{set}1", "a", "b", "c"]).await;
    client.command(&["SADD", "{set}2", "b", "c", "d"]).await;

    let response = client.command(&["SINTER", "{set}1", "{set}2"]).await;
    match response {
        Response::Array(items) => {
            assert_eq!(items.len(), 2); // b, c
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_sdiff() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SADD", "{set}1", "a", "b", "c"]).await;
    client.command(&["SADD", "{set}2", "b", "c", "d"]).await;

    let response = client.command(&["SDIFF", "{set}1", "{set}2"]).await;
    match response {
        Response::Array(items) => {
            assert_eq!(items.len(), 1); // a
            assert_eq!(items[0], Response::Bulk(Some(Bytes::from("a"))));
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_sunionstore() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SADD", "{set}1", "a", "b"]).await;
    client.command(&["SADD", "{set}2", "b", "c"]).await;

    let response = client
        .command(&["SUNIONSTORE", "{set}dest", "{set}1", "{set}2"])
        .await;
    assert_eq!(response, Response::Integer(3));

    let response = client.command(&["SCARD", "{set}dest"]).await;
    assert_eq!(response, Response::Integer(3));

    server.shutdown().await;
}

#[tokio::test]
async fn test_sinterstore() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SADD", "{set}1", "a", "b", "c"]).await;
    client.command(&["SADD", "{set}2", "b", "c", "d"]).await;

    let response = client
        .command(&["SINTERSTORE", "{set}dest", "{set}1", "{set}2"])
        .await;
    assert_eq!(response, Response::Integer(2));

    server.shutdown().await;
}

#[tokio::test]
async fn test_spop() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SADD", "myset", "a", "b", "c"]).await;

    // SPOP single
    let response = client.command(&["SPOP", "myset"]).await;
    match response {
        Response::Bulk(Some(_)) => {}
        _ => panic!("Expected bulk response"),
    }

    let response = client.command(&["SCARD", "myset"]).await;
    assert_eq!(response, Response::Integer(2));

    // SPOP with count
    let response = client.command(&["SPOP", "myset", "2"]).await;
    match response {
        Response::Array(items) => {
            assert_eq!(items.len(), 2);
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_type_command_set() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SADD", "myset", "a"]).await;

    let response = client.command(&["TYPE", "myset"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("set")));

    server.shutdown().await;
}

#[tokio::test]
async fn test_object_encoding_set_listpack_to_hashtable() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Small set should be listpack
    client.command(&["SADD", "myset", "a"]).await;
    let enc = client.command(&["OBJECT", "ENCODING", "myset"]).await;
    assert_eq!(enc, Response::bulk(Bytes::from("listpack")));

    // Add many members to force promotion to hashtable
    for i in 0..200 {
        client
            .command(&["SADD", "myset", &format!("member{i}")])
            .await;
    }
    let enc = client.command(&["OBJECT", "ENCODING", "myset"]).await;
    assert_eq!(enc, Response::bulk(Bytes::from("hashtable")));

    server.shutdown().await;
}

#[tokio::test]
async fn test_wrongtype_hash_list_set() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create a hash
    client.command(&["HSET", "myhash", "f1", "v1"]).await;

    // Try list command on hash
    let response = client.command(&["LPUSH", "myhash", "a"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"WRONGTYPE")));

    // Try set command on hash
    let response = client.command(&["SADD", "myhash", "a"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"WRONGTYPE")));

    // Create a list
    client.command(&["RPUSH", "mylist", "a"]).await;

    // Try hash command on list
    let response = client.command(&["HGET", "mylist", "f1"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"WRONGTYPE")));

    // Create a set
    client.command(&["SADD", "myset", "a"]).await;

    // Try hash command on set
    let response = client.command(&["HGET", "myset", "f1"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"WRONGTYPE")));

    server.shutdown().await;
}
