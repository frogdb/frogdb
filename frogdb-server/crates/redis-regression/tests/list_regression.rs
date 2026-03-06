use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn lpush_rpush_lpop_rpop_basics() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // RPUSH returns list length
    assert_eq!(
        unwrap_integer(&client.command(&["RPUSH", "mylist", "a", "b", "c"]).await),
        3
    );
    // LPUSH prepends
    assert_eq!(
        unwrap_integer(&client.command(&["LPUSH", "mylist", "z"]).await),
        4
    );

    // LPOP removes from head
    assert_bulk_eq(&client.command(&["LPOP", "mylist"]).await, b"z");
    // RPOP removes from tail
    assert_bulk_eq(&client.command(&["RPOP", "mylist"]).await, b"c");

    // LLEN
    assert_eq!(
        unwrap_integer(&client.command(&["LLEN", "mylist"]).await),
        2
    );
}

#[tokio::test]
async fn lpop_rpop_with_count() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["RPUSH", "mylist", "a", "b", "c", "d"])
        .await;

    let resp = client.command(&["LPOP", "mylist", "2"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 2);
    assert_bulk_eq(&items[0], b"a");
    assert_bulk_eq(&items[1], b"b");

    let resp = client.command(&["RPOP", "mylist", "2"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 2);
    assert_bulk_eq(&items[0], b"d");
    assert_bulk_eq(&items[1], b"c");
}

#[tokio::test]
async fn lindex_and_lrange() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "mylist", "a", "b", "c"]).await;

    assert_bulk_eq(&client.command(&["LINDEX", "mylist", "0"]).await, b"a");
    assert_bulk_eq(&client.command(&["LINDEX", "mylist", "2"]).await, b"c");
    // Negative index
    assert_bulk_eq(&client.command(&["LINDEX", "mylist", "-1"]).await, b"c");
    // Out of range → nil
    assert!(matches!(
        client.command(&["LINDEX", "mylist", "100"]).await,
        frogdb_protocol::Response::Bulk(None)
    ));

    // LRANGE
    let resp = client.command(&["LRANGE", "mylist", "0", "-1"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 3);
    assert_bulk_eq(&items[0], b"a");
    assert_bulk_eq(&items[2], b"c");
}

#[tokio::test]
async fn lpos_finds_element_position() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["RPUSH", "mylist", "a", "b", "a", "c"])
        .await;

    // First occurrence
    assert_eq!(
        unwrap_integer(&client.command(&["LPOS", "mylist", "a"]).await),
        0
    );
    // RANK 2 → second occurrence
    assert_eq!(
        unwrap_integer(&client.command(&["LPOS", "mylist", "a", "RANK", "2"]).await),
        2
    );
    // COUNT 0 → all occurrences
    let resp = client.command(&["LPOS", "mylist", "a", "COUNT", "0"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 2);
}

#[tokio::test]
async fn linsert_before_and_after() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "mylist", "a", "c"]).await;

    let resp = client
        .command(&["LINSERT", "mylist", "BEFORE", "c", "b"])
        .await;
    assert_eq!(unwrap_integer(&resp), 3);

    let resp = client.command(&["LRANGE", "mylist", "0", "-1"]).await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"a");
    assert_bulk_eq(&items[1], b"b");
    assert_bulk_eq(&items[2], b"c");

    // AFTER
    client
        .command(&["LINSERT", "mylist", "AFTER", "b", "b2"])
        .await;
    let resp = client.command(&["LRANGE", "mylist", "0", "-1"]).await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[2], b"b2");
}

#[tokio::test]
async fn lset_at_index() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "mylist", "a", "b", "c"]).await;
    assert_ok(&client.command(&["LSET", "mylist", "1", "B"]).await);
    assert_bulk_eq(&client.command(&["LINDEX", "mylist", "1"]).await, b"B");

    // Out of range
    let resp = client.command(&["LSET", "mylist", "100", "x"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn ltrim_range() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["RPUSH", "mylist", "a", "b", "c", "d", "e"])
        .await;
    assert_ok(&client.command(&["LTRIM", "mylist", "1", "3"]).await);

    let resp = client.command(&["LRANGE", "mylist", "0", "-1"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 3);
    assert_bulk_eq(&items[0], b"b");
    assert_bulk_eq(&items[2], b"d");
}

#[tokio::test]
async fn lrem_by_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["RPUSH", "mylist", "a", "b", "a", "c", "a"])
        .await;

    // Remove 2 occurrences from head
    let resp = client.command(&["LREM", "mylist", "2", "a"]).await;
    assert_eq!(unwrap_integer(&resp), 2);
    assert_eq!(
        unwrap_integer(&client.command(&["LLEN", "mylist"]).await),
        3
    );

    // One "a" remains
    assert_eq!(
        unwrap_integer(&client.command(&["LPOS", "mylist", "a"]).await),
        2
    );
}

#[tokio::test]
async fn lmove_between_lists() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "{l}src", "a", "b", "c"]).await;

    // Move left→left
    let resp = client
        .command(&["LMOVE", "{l}src", "{l}dst", "LEFT", "LEFT"])
        .await;
    assert_bulk_eq(&resp, b"a");
    assert_bulk_eq(&client.command(&["LINDEX", "{l}dst", "0"]).await, b"a");

    // Move right→right
    let resp = client
        .command(&["LMOVE", "{l}src", "{l}dst", "RIGHT", "RIGHT"])
        .await;
    assert_bulk_eq(&resp, b"c");
}

#[tokio::test]
async fn lmpop_from_multiple_lists() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "{l}a", "1", "2"]).await;
    client.command(&["RPUSH", "{l}b", "3", "4"]).await;

    let resp = client
        .command(&["LMPOP", "2", "{l}a", "{l}b", "LEFT"])
        .await;
    let parts = unwrap_array(resp);
    // Returns [key, [elements]]
    assert_eq!(parts.len(), 2);
    assert_bulk_eq(&parts[0], b"{l}a");
    let elems = unwrap_array(parts[1].clone());
    assert_bulk_eq(&elems[0], b"1");
}

#[tokio::test]
async fn list_commands_against_wrong_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "notalist", "val"]).await;

    let resp = client.command(&["RPUSH", "notalist", "x"]).await;
    assert_error_prefix(&resp, "WRONGTYPE");

    let resp = client.command(&["LRANGE", "notalist", "0", "-1"]).await;
    assert_error_prefix(&resp, "WRONGTYPE");
}
