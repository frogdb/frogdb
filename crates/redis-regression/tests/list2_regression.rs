use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// list-2.tcl focuses on compression and plain node behavior with large values.
// We test the observable semantics (ordering, correctness) without relying on
// internal quicklist encoding details or DEBUG commands.

#[tokio::test]
async fn lpush_rpush_with_large_values() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let big = "x".repeat(500);
    client.command(&["LPUSH", "mylist", &big]).await;
    client.command(&["LPUSH", "mylist", &"y".repeat(500)]).await;
    client.command(&["RPUSH", "mylist", &"z".repeat(500)]).await;

    assert_eq!(unwrap_integer(&client.command(&["LLEN", "mylist"]).await), 3);
}

#[tokio::test]
async fn lindex_on_large_values() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let a = "a".repeat(500);
    let b = "b".repeat(500);
    let c = "c".repeat(500);
    client.command(&["RPUSH", "mylist", &a, &b, &c]).await;

    assert_bulk_eq(&client.command(&["LINDEX", "mylist", "0"]).await, a.as_bytes());
    assert_bulk_eq(&client.command(&["LINDEX", "mylist", "2"]).await, c.as_bytes());
    assert_bulk_eq(&client.command(&["LINDEX", "mylist", "-1"]).await, c.as_bytes());
}

#[tokio::test]
async fn linsert_in_large_list_maintains_ordering() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let a = "a".repeat(500);
    let c = "c".repeat(500);
    let b = "b".repeat(500);

    client.command(&["RPUSH", "mylist", &a, &c]).await;
    client.command(&["LINSERT", "mylist", "BEFORE", &c, &b]).await;

    let resp = client.command(&["LRANGE", "mylist", "0", "-1"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 3);
    assert_bulk_eq(&items[0], a.as_bytes());
    assert_bulk_eq(&items[1], b.as_bytes());
    assert_bulk_eq(&items[2], c.as_bytes());
}

#[tokio::test]
async fn ltrim_stress_with_various_ranges() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Build a 10-element list with large values
    for i in 0..10u8 {
        let val = format!("{}", char::from(b'a' + i)).repeat(500);
        client.command(&["RPUSH", "mylist", &val]).await;
    }
    assert_eq!(unwrap_integer(&client.command(&["LLEN", "mylist"]).await), 10);

    // Trim to [2, 7]
    assert_ok(&client.command(&["LTRIM", "mylist", "2", "7"]).await);
    assert_eq!(unwrap_integer(&client.command(&["LLEN", "mylist"]).await), 6);

    // Trim beyond end
    assert_ok(&client.command(&["LTRIM", "mylist", "0", "100"]).await);
    assert_eq!(unwrap_integer(&client.command(&["LLEN", "mylist"]).await), 6);
}

#[tokio::test]
async fn lset_on_large_nodes() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let orig = "a".repeat(500);
    let new = "b".repeat(500);

    client.command(&["RPUSH", "mylist", &orig]).await;
    assert_ok(&client.command(&["LSET", "mylist", "0", &new]).await);
    assert_bulk_eq(&client.command(&["LINDEX", "mylist", "0"]).await, new.as_bytes());
}
