use bytes::Bytes;
use frogdb_server::config::server::SortedSetIndexConfig;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::{TestServer, TestServerConfig};
use redis_protocol::resp3::types::BytesFrame as Resp3Frame;
use rstest::rstest;

async fn zset_server(backend: SortedSetIndexConfig) -> TestServer {
    TestServer::start_standalone_with_config(TestServerConfig {
        sorted_set_index: Some(backend),
        ..Default::default()
    })
    .await
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn zpopmin_resp3_without_count_returns_flat_pair(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect_resp3().await;

    let _hello = client.command(&["HELLO", "3"]).await;
    client.command(&["ZADD", "myzset", "1", "a"]).await;
    client.command(&["ZADD", "myzset", "2", "b"]).await;

    // Without count: should return flat [member, score]
    let resp = client.command(&["ZPOPMIN", "myzset"]).await;
    match &resp {
        Resp3Frame::Array { data, .. } => {
            assert_eq!(
                data.len(),
                2,
                "ZPOPMIN without count should return 2 elements"
            );
            // First is member
            match &data[0] {
                Resp3Frame::BlobString { data, .. } => assert_eq!(data, &Bytes::from("a")),
                other => panic!("expected BlobString for member, got {other:?}"),
            }
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn zpopmin_resp3_with_count_returns_nested_pairs(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect_resp3().await;

    let _hello = client.command(&["HELLO", "3"]).await;
    client.command(&["ZADD", "myzset", "1", "a"]).await;
    client.command(&["ZADD", "myzset", "2", "b"]).await;

    // With count: should return array of [member, score] pairs
    let resp = client.command(&["ZPOPMIN", "myzset", "2"]).await;
    match &resp {
        Resp3Frame::Array { data, .. } => {
            assert_eq!(data.len(), 2, "ZPOPMIN with count 2 should return 2 pairs");
            // Each element should be a nested array [member, score]
            for pair in data {
                match pair {
                    Resp3Frame::Array { data: inner, .. } => {
                        assert_eq!(inner.len(), 2, "each pair should have 2 elements");
                    }
                    other => panic!("expected nested Array pair, got {other:?}"),
                }
            }
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn zpopmax_resp3_without_count_returns_flat_pair(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect_resp3().await;

    let _hello = client.command(&["HELLO", "3"]).await;
    client.command(&["ZADD", "myzset", "1", "a"]).await;
    client.command(&["ZADD", "myzset", "2", "b"]).await;

    let resp = client.command(&["ZPOPMAX", "myzset"]).await;
    match &resp {
        Resp3Frame::Array { data, .. } => {
            assert_eq!(
                data.len(),
                2,
                "ZPOPMAX without count should return 2 elements"
            );
            match &data[0] {
                Resp3Frame::BlobString { data, .. } => assert_eq!(data, &Bytes::from("b")),
                other => panic!("expected BlobString for member, got {other:?}"),
            }
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn zpopmax_resp3_with_count_returns_nested_pairs(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect_resp3().await;

    let _hello = client.command(&["HELLO", "3"]).await;
    client.command(&["ZADD", "myzset", "1", "a"]).await;
    client.command(&["ZADD", "myzset", "2", "b"]).await;

    let resp = client.command(&["ZPOPMAX", "myzset", "2"]).await;
    match &resp {
        Resp3Frame::Array { data, .. } => {
            assert_eq!(data.len(), 2, "ZPOPMAX with count 2 should return 2 pairs");
            for pair in data {
                match pair {
                    Resp3Frame::Array { data: inner, .. } => {
                        assert_eq!(inner.len(), 2);
                    }
                    other => panic!("expected nested Array pair, got {other:?}"),
                }
            }
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn zrandmember_negative_count_allows_duplicates(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect().await;

    // Only 3 members, but request -10 (allows duplicates)
    client.command(&["ZADD", "myzset", "1", "a"]).await;
    client.command(&["ZADD", "myzset", "2", "b"]).await;
    client.command(&["ZADD", "myzset", "3", "c"]).await;

    let resp = client.command(&["ZRANDMEMBER", "myzset", "-10"]).await;
    let items = extract_bulk_strings(&resp);
    // Should return exactly 10 items (with duplicates)
    assert_eq!(items.len(), 10);
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn zrandmember_count_exceeding_cardinality(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect().await;

    client.command(&["ZADD", "myzset", "1", "a"]).await;
    client.command(&["ZADD", "myzset", "2", "b"]).await;
    client.command(&["ZADD", "myzset", "3", "c"]).await;

    // Request more than exists - should return all unique members
    let resp = client.command(&["ZRANDMEMBER", "myzset", "100"]).await;
    let mut items = extract_bulk_strings(&resp);
    items.sort();
    items.dedup();
    assert_eq!(items.len(), 3);
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn zrange_byscore_rev_with_limit(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect().await;

    for i in 1..=10 {
        client
            .command(&["ZADD", "myzset", &i.to_string(), &format!("m{i}")])
            .await;
    }

    // ZRANGE key max min BYSCORE REV LIMIT offset count
    let resp = client
        .command(&[
            "ZRANGE", "myzset", "10", "1", "BYSCORE", "REV", "LIMIT", "2", "3",
        ])
        .await;
    let items = extract_bulk_strings(&resp);
    // Reversed: 10,9,8,7,6,5,4,3,2,1 → skip 2, take 3 → [8, 7, 6]
    assert_eq!(items, vec!["m8", "m7", "m6"]);
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn zrangestore_byscore_rev_with_limit(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect().await;

    for i in 1..=10 {
        client
            .command(&["ZADD", "{k}src", &i.to_string(), &format!("m{i}")])
            .await;
    }

    let resp = client
        .command(&[
            "ZRANGESTORE",
            "{k}dst",
            "{k}src",
            "10",
            "1",
            "BYSCORE",
            "REV",
            "LIMIT",
            "2",
            "3",
        ])
        .await;
    let stored = unwrap_integer(&resp);
    assert_eq!(stored, 3);

    // Verify stored elements
    let resp = client.command(&["ZRANGE", "{k}dst", "0", "-1"]).await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items.len(), 3);
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn score_formatting_extreme_floats(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect().await;

    // Test extreme positive float
    client
        .command(&["ZADD", "myzset", "1.7976931348623157e+308", "pos"])
        .await;
    let resp = client.command(&["ZSCORE", "myzset", "pos"]).await;
    let score_str = String::from_utf8(unwrap_bulk(&resp).to_vec()).unwrap();
    assert!(
        score_str.contains("e+308") || score_str.contains("E+308"),
        "expected scientific notation with e+308, got {score_str}"
    );

    // Test extreme negative float
    client
        .command(&["ZADD", "myzset", "-1.7976931348623157e+308", "neg"])
        .await;
    let resp = client.command(&["ZSCORE", "myzset", "neg"]).await;
    let score_str = String::from_utf8(unwrap_bulk(&resp).to_vec()).unwrap();
    assert!(
        score_str.starts_with('-'),
        "negative extreme float should start with '-', got {score_str}"
    );
}

#[rstest]
#[case::skiplist(SortedSetIndexConfig::Skiplist)]
#[case::btree(SortedSetIndexConfig::Btreemap)]
#[tokio::test]
async fn zintercard_error_messages(#[case] backend: SortedSetIndexConfig) {
    let server = zset_server(backend).await;
    let mut client = server.connect().await;

    // ZINTERCARD with 0 keys
    let resp = client.command(&["ZINTERCARD", "0"]).await;
    assert_error_prefix(&resp, "ERR");

    // ZINTERCARD with negative LIMIT
    client.command(&["ZADD", "myzset", "1", "a"]).await;
    let resp = client
        .command(&["ZINTERCARD", "1", "myzset", "LIMIT", "-1"])
        .await;
    assert_error_prefix(&resp, "ERR");
}
