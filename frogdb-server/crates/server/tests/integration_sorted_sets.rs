//! Integration tests for sorted set commands (ZADD, ZSCORE, ZRANGE, etc.)


use bytes::Bytes;
use crate::common::test_server::{TestServer, TestServerConfig};
use frogdb_protocol::Response;
use frogdb_server::config::server::SortedSetIndexConfig;
use rstest::rstest;

async fn zset_server(backend: SortedSetIndexConfig) -> TestServer {
    TestServer::start_standalone_with_config(TestServerConfig {
        sorted_set_index: Some(backend),
        ..Default::default()
    })
    .await
}

// ============================================================================
// Sorted Set Tests
// ============================================================================

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn test_zadd_basic(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect().await;

    // ZADD returns number of elements added
    let response = client.command(&["ZADD", "myzset", "1", "one"]).await;
    assert_eq!(response, Response::Integer(1));

    // Add another member
    let response = client
        .command(&["ZADD", "myzset", "2", "two", "3", "three"])
        .await;
    assert_eq!(response, Response::Integer(2));

    // Update existing member (returns 0 - no new members added)
    let response = client.command(&["ZADD", "myzset", "1.5", "one"]).await;
    assert_eq!(response, Response::Integer(0));

    // ZCARD
    let response = client.command(&["ZCARD", "myzset"]).await;
    assert_eq!(response, Response::Integer(3));

    server.shutdown().await;
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn test_zadd_options(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect().await;

    // Add initial member
    client.command(&["ZADD", "myzset", "1", "one"]).await;

    // NX - only add new elements
    let response = client.command(&["ZADD", "myzset", "NX", "2", "one"]).await;
    assert_eq!(response, Response::Integer(0));

    // NX with new element
    let response = client.command(&["ZADD", "myzset", "NX", "2", "two"]).await;
    assert_eq!(response, Response::Integer(1));

    // XX - only update existing elements
    let response = client
        .command(&["ZADD", "myzset", "XX", "3", "three"])
        .await;
    assert_eq!(response, Response::Integer(0));

    // XX with existing element
    let response = client.command(&["ZADD", "myzset", "XX", "5", "one"]).await;
    assert_eq!(response, Response::Integer(0));

    // Verify score was updated
    let response = client.command(&["ZSCORE", "myzset", "one"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("5"))));

    // CH - return changed count
    let response = client.command(&["ZADD", "myzset", "CH", "10", "one"]).await;
    assert_eq!(response, Response::Integer(1));

    server.shutdown().await;
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn test_zadd_incr(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect().await;

    // INCR mode returns the new score
    let response = client
        .command(&["ZADD", "myzset", "INCR", "5", "member"])
        .await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("5"))));

    // Increment again
    let response = client
        .command(&["ZADD", "myzset", "INCR", "3", "member"])
        .await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("8"))));

    server.shutdown().await;
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn test_zscore(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect().await;

    client
        .command(&["ZADD", "myzset", "1.5", "one", "2.5", "two"])
        .await;

    let response = client.command(&["ZSCORE", "myzset", "one"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("1.5"))));

    // Non-existent member
    let response = client.command(&["ZSCORE", "myzset", "three"]).await;
    assert_eq!(response, Response::Bulk(None));

    // Non-existent key
    let response = client.command(&["ZSCORE", "nonexistent", "one"]).await;
    assert_eq!(response, Response::Bulk(None));

    server.shutdown().await;
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn test_zmscore(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect().await;

    client
        .command(&["ZADD", "myzset", "1", "one", "2", "two"])
        .await;

    let response = client
        .command(&["ZMSCORE", "myzset", "one", "two", "three"])
        .await;
    assert_eq!(
        response,
        Response::Array(vec![
            Response::Bulk(Some(Bytes::from("1"))),
            Response::Bulk(Some(Bytes::from("2"))),
            Response::Bulk(None),
        ])
    );

    server.shutdown().await;
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn test_zrem(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect().await;

    client
        .command(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
        .await;

    let response = client.command(&["ZREM", "myzset", "one", "four"]).await;
    assert_eq!(response, Response::Integer(1)); // Only "one" was removed

    let response = client.command(&["ZCARD", "myzset"]).await;
    assert_eq!(response, Response::Integer(2));

    server.shutdown().await;
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn test_zincrby(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect().await;

    // Increment non-existent key
    let response = client.command(&["ZINCRBY", "myzset", "5", "member"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("5"))));

    // Increment existing member
    let response = client.command(&["ZINCRBY", "myzset", "3", "member"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("8"))));

    // Negative increment
    let response = client.command(&["ZINCRBY", "myzset", "-2", "member"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("6"))));

    server.shutdown().await;
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn test_zrank(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect().await;

    client
        .command(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
        .await;

    let response = client.command(&["ZRANK", "myzset", "one"]).await;
    assert_eq!(response, Response::Integer(0));

    let response = client.command(&["ZRANK", "myzset", "two"]).await;
    assert_eq!(response, Response::Integer(1));

    let response = client.command(&["ZRANK", "myzset", "three"]).await;
    assert_eq!(response, Response::Integer(2));

    // Non-existent member
    let response = client.command(&["ZRANK", "myzset", "four"]).await;
    assert_eq!(response, Response::Bulk(None));

    server.shutdown().await;
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn test_zrevrank(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect().await;

    client
        .command(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
        .await;

    let response = client.command(&["ZREVRANK", "myzset", "one"]).await;
    assert_eq!(response, Response::Integer(2));

    let response = client.command(&["ZREVRANK", "myzset", "three"]).await;
    assert_eq!(response, Response::Integer(0));

    server.shutdown().await;
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn test_zrange_by_rank(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect().await;

    client
        .command(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
        .await;

    // Basic range
    let response = client.command(&["ZRANGE", "myzset", "0", "-1"]).await;
    assert_eq!(
        response,
        Response::Array(vec![
            Response::Bulk(Some(Bytes::from("one"))),
            Response::Bulk(Some(Bytes::from("two"))),
            Response::Bulk(Some(Bytes::from("three"))),
        ])
    );

    // With WITHSCORES
    let response = client
        .command(&["ZRANGE", "myzset", "0", "-1", "WITHSCORES"])
        .await;
    assert_eq!(
        response,
        Response::Array(vec![
            Response::Bulk(Some(Bytes::from("one"))),
            Response::Bulk(Some(Bytes::from("1"))),
            Response::Bulk(Some(Bytes::from("two"))),
            Response::Bulk(Some(Bytes::from("2"))),
            Response::Bulk(Some(Bytes::from("three"))),
            Response::Bulk(Some(Bytes::from("3"))),
        ])
    );

    // Reverse
    let response = client
        .command(&["ZRANGE", "myzset", "0", "-1", "REV"])
        .await;
    assert_eq!(
        response,
        Response::Array(vec![
            Response::Bulk(Some(Bytes::from("three"))),
            Response::Bulk(Some(Bytes::from("two"))),
            Response::Bulk(Some(Bytes::from("one"))),
        ])
    );

    server.shutdown().await;
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn test_zrange_by_score(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect().await;

    client
        .command(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
        .await;

    // Range by score
    let response = client
        .command(&["ZRANGE", "myzset", "1", "2", "BYSCORE"])
        .await;
    assert_eq!(
        response,
        Response::Array(vec![
            Response::Bulk(Some(Bytes::from("one"))),
            Response::Bulk(Some(Bytes::from("two"))),
        ])
    );

    // Exclusive bounds
    let response = client
        .command(&["ZRANGE", "myzset", "(1", "(3", "BYSCORE"])
        .await;
    assert_eq!(
        response,
        Response::Array(vec![Response::Bulk(Some(Bytes::from("two"))),])
    );

    // Infinity bounds
    let response = client
        .command(&["ZRANGE", "myzset", "-inf", "+inf", "BYSCORE"])
        .await;
    assert_eq!(
        response,
        Response::Array(vec![
            Response::Bulk(Some(Bytes::from("one"))),
            Response::Bulk(Some(Bytes::from("two"))),
            Response::Bulk(Some(Bytes::from("three"))),
        ])
    );

    server.shutdown().await;
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn test_zrangebyscore_legacy(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect().await;

    client
        .command(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
        .await;

    let response = client
        .command(&["ZRANGEBYSCORE", "myzset", "-inf", "+inf"])
        .await;
    assert_eq!(
        response,
        Response::Array(vec![
            Response::Bulk(Some(Bytes::from("one"))),
            Response::Bulk(Some(Bytes::from("two"))),
            Response::Bulk(Some(Bytes::from("three"))),
        ])
    );

    // With LIMIT
    let response = client
        .command(&["ZRANGEBYSCORE", "myzset", "-inf", "+inf", "LIMIT", "1", "1"])
        .await;
    assert_eq!(
        response,
        Response::Array(vec![Response::Bulk(Some(Bytes::from("two"))),])
    );

    server.shutdown().await;
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn test_zcount(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect().await;

    client
        .command(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
        .await;

    let response = client.command(&["ZCOUNT", "myzset", "-inf", "+inf"]).await;
    assert_eq!(response, Response::Integer(3));

    let response = client.command(&["ZCOUNT", "myzset", "1", "2"]).await;
    assert_eq!(response, Response::Integer(2));

    let response = client.command(&["ZCOUNT", "myzset", "(1", "3"]).await;
    assert_eq!(response, Response::Integer(2));

    server.shutdown().await;
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn test_zpopmin_zpopmax(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect().await;

    client
        .command(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
        .await;

    // Pop minimum
    let response = client.command(&["ZPOPMIN", "myzset"]).await;
    assert_eq!(
        response,
        Response::Array(vec![
            Response::Bulk(Some(Bytes::from("one"))),
            Response::Bulk(Some(Bytes::from("1"))),
        ])
    );

    // Pop maximum
    let response = client.command(&["ZPOPMAX", "myzset"]).await;
    assert_eq!(
        response,
        Response::Array(vec![
            Response::Bulk(Some(Bytes::from("three"))),
            Response::Bulk(Some(Bytes::from("3"))),
        ])
    );

    // Only "two" should remain
    let response = client.command(&["ZCARD", "myzset"]).await;
    assert_eq!(response, Response::Integer(1));

    server.shutdown().await;
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn test_zunionstore(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect().await;

    client
        .command(&["ZADD", "{z}zset1", "1", "a", "2", "b"])
        .await;
    client
        .command(&["ZADD", "{z}zset2", "3", "b", "4", "c"])
        .await;

    let response = client
        .command(&["ZUNIONSTORE", "{z}result", "2", "{z}zset1", "{z}zset2"])
        .await;
    assert_eq!(response, Response::Integer(3)); // a, b, c

    // Check scores (default SUM aggregate)
    let response = client.command(&["ZSCORE", "{z}result", "a"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("1"))));

    let response = client.command(&["ZSCORE", "{z}result", "b"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("5")))); // 2 + 3

    let response = client.command(&["ZSCORE", "{z}result", "c"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("4"))));

    server.shutdown().await;
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn test_zinterstore(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect().await;

    client
        .command(&["ZADD", "{z}zset1", "1", "a", "2", "b"])
        .await;
    client
        .command(&["ZADD", "{z}zset2", "3", "b", "4", "c"])
        .await;

    let response = client
        .command(&["ZINTERSTORE", "{z}result", "2", "{z}zset1", "{z}zset2"])
        .await;
    assert_eq!(response, Response::Integer(1)); // Only b is in both

    let response = client.command(&["ZSCORE", "{z}result", "b"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("5")))); // 2 + 3

    server.shutdown().await;
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn test_zdiffstore(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect().await;

    client
        .command(&["ZADD", "{z}zset1", "1", "a", "2", "b"])
        .await;
    client
        .command(&["ZADD", "{z}zset2", "3", "b", "4", "c"])
        .await;

    let response = client
        .command(&["ZDIFFSTORE", "{z}result", "2", "{z}zset1", "{z}zset2"])
        .await;
    assert_eq!(response, Response::Integer(1)); // Only a is in zset1 but not zset2

    let response = client.command(&["ZSCORE", "{z}result", "a"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("1"))));

    server.shutdown().await;
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn test_zremrangebyrank(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect().await;

    client
        .command(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
        .await;

    let response = client
        .command(&["ZREMRANGEBYRANK", "myzset", "0", "1"])
        .await;
    assert_eq!(response, Response::Integer(2)); // Removed "one" and "two"

    let response = client.command(&["ZCARD", "myzset"]).await;
    assert_eq!(response, Response::Integer(1));

    server.shutdown().await;
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn test_zremrangebyscore(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect().await;

    client
        .command(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
        .await;

    let response = client
        .command(&["ZREMRANGEBYSCORE", "myzset", "-inf", "(3"])
        .await;
    assert_eq!(response, Response::Integer(2)); // Removed "one" and "two"

    let response = client.command(&["ZCARD", "myzset"]).await;
    assert_eq!(response, Response::Integer(1));

    server.shutdown().await;
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn test_wrongtype_error(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect().await;

    // Create a string key
    client.command(&["SET", "mykey", "hello"]).await;

    // Try to use sorted set commands on it
    let response = client.command(&["ZADD", "mykey", "1", "member"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"WRONGTYPE")));

    let response = client.command(&["ZSCORE", "mykey", "member"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"WRONGTYPE")));

    server.shutdown().await;
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn test_type_command_zset(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect().await;

    client.command(&["ZADD", "myzset", "1", "one"]).await;

    let response = client.command(&["TYPE", "myzset"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("zset")));

    server.shutdown().await;
}
