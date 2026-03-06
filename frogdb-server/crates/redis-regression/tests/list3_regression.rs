use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// list-3.tcl focuses on ziplist/listpack encoding with various integer sizes.
// We test observable semantics: ordering, LRANGE negative indices, edge-case
// integer values, and LPUSH/RPUSH interleaving.

#[tokio::test]
async fn lrange_negative_indices_backward_traversal() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["RPUSH", "mylist", "a", "b", "c", "d", "e"])
        .await;

    // Last 2 elements
    let resp = client.command(&["LRANGE", "mylist", "-2", "-1"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 2);
    assert_bulk_eq(&items[0], b"d");
    assert_bulk_eq(&items[1], b"e");

    // Single negative index
    let resp = client.command(&["LRANGE", "mylist", "-1", "-1"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 1);
    assert_bulk_eq(&items[0], b"e");
}

#[tokio::test]
async fn lpush_rpush_interleaving_preserves_order() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Interleave LPUSH and RPUSH
    client.command(&["RPUSH", "mylist", "mid"]).await;
    client.command(&["LPUSH", "mylist", "left1"]).await;
    client.command(&["RPUSH", "mylist", "right1"]).await;
    client.command(&["LPUSH", "mylist", "left2"]).await;
    client.command(&["RPUSH", "mylist", "right2"]).await;

    let resp = client.command(&["LRANGE", "mylist", "0", "-1"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 5);
    assert_bulk_eq(&items[0], b"left2");
    assert_bulk_eq(&items[1], b"left1");
    assert_bulk_eq(&items[2], b"mid");
    assert_bulk_eq(&items[3], b"right1");
    assert_bulk_eq(&items[4], b"right2");
}

#[tokio::test]
async fn integer_values_various_sizes() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let values = [
        "0",
        "127",                  // i8 max
        "-128",                 // i8 min
        "32767",                // i16 max
        "-32768",               // i16 min
        "2147483647",           // i32 max
        "-2147483648",          // i32 min
        "9223372036854775807",  // i64 max
        "-9223372036854775808", // i64 min
    ];

    for v in &values {
        client.command(&["RPUSH", "mylist", v]).await;
    }

    let resp = client.command(&["LRANGE", "mylist", "0", "-1"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), values.len());

    for (i, v) in values.iter().enumerate() {
        assert_bulk_eq(&items[i], v.as_bytes());
    }
}

#[tokio::test]
async fn edge_case_integer_values_roundtrip() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // INT64_MIN and INT64_MAX survive a SET/GET round-trip
    let int64_max = "9223372036854775807";
    let int64_min = "-9223372036854775808";

    client
        .command(&["RPUSH", "{l}list", int64_max, int64_min])
        .await;

    assert_bulk_eq(
        &client.command(&["LINDEX", "{l}list", "0"]).await,
        int64_max.as_bytes(),
    );
    assert_bulk_eq(
        &client.command(&["LINDEX", "{l}list", "1"]).await,
        int64_min.as_bytes(),
    );
}
