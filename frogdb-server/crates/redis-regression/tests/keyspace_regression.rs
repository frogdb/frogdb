use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn copy_with_invalid_destination_db_returns_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "{k}src", "hello"]).await);

    // Invalid DB argument (not a number)
    let resp = client
        .command(&["COPY", "{k}src", "{k}dst", "DB", "notanumber"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn keys_with_backtracking_glob_matches_correctly() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create keys that test glob backtracking
    assert_ok(&client.command(&["SET", "hellofoobarbaz", "1"]).await);
    assert_ok(&client.command(&["SET", "foobarbaz", "1"]).await);
    assert_ok(&client.command(&["SET", "foobar", "1"]).await);
    assert_ok(&client.command(&["SET", "nope", "1"]).await);

    // Pattern with multiple wildcards requiring backtracking
    let resp = client.command(&["KEYS", "*foo*bar*"]).await;
    let mut keys = extract_bulk_strings(&resp);
    keys.sort();
    assert_eq!(keys, vec!["foobar", "foobarbaz", "hellofoobarbaz"]);
}

// ===========================================================================
// COPY — copy a key to another key (same slot)
// ===========================================================================

#[tokio::test]
async fn copy_basic_same_slot() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "{k}src", "hello"]).await);
    let resp = client.command(&["COPY", "{k}src", "{k}dst"]).await;
    assert_eq!(unwrap_integer(&resp), 1);
    assert_bulk_eq(&client.command(&["GET", "{k}dst"]).await, b"hello");
}

#[tokio::test]
async fn copy_returns_zero_when_dest_exists_without_replace() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "{k}src", "hello"]).await);
    assert_ok(&client.command(&["SET", "{k}dst", "existing"]).await);

    let resp = client.command(&["COPY", "{k}src", "{k}dst"]).await;
    assert_eq!(unwrap_integer(&resp), 0);
    // Destination should be unchanged
    assert_bulk_eq(&client.command(&["GET", "{k}dst"]).await, b"existing");
}

#[tokio::test]
async fn copy_replace_overwrites_destination() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "{k}src", "new_value"]).await);
    assert_ok(&client.command(&["SET", "{k}dst", "old_value"]).await);

    let resp = client
        .command(&["COPY", "{k}src", "{k}dst", "REPLACE"])
        .await;
    assert_eq!(unwrap_integer(&resp), 1);
    assert_bulk_eq(&client.command(&["GET", "{k}dst"]).await, b"new_value");
}

#[tokio::test]
async fn copy_nonexistent_source_returns_zero() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["COPY", "{k}nosrc", "{k}dst"]).await;
    assert_eq!(unwrap_integer(&resp), 0);
}

#[tokio::test]
async fn copy_preserves_ttl_on_source() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "{k}src", "hello"]).await);
    assert_eq!(
        unwrap_integer(&client.command(&["EXPIRE", "{k}src", "100"]).await),
        1
    );

    let resp = client.command(&["COPY", "{k}src", "{k}dst"]).await;
    assert_eq!(unwrap_integer(&resp), 1);

    // Source TTL should still be set
    let src_ttl = unwrap_integer(&client.command(&["TTL", "{k}src"]).await);
    assert!(src_ttl > 0 && src_ttl <= 100);

    // Destination should also have the TTL (COPY copies expiry)
    let dst_ttl = unwrap_integer(&client.command(&["TTL", "{k}dst"]).await);
    assert!(dst_ttl > 0 && dst_ttl <= 100);
}

#[tokio::test]
async fn copy_hash_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "{k}src", "f1", "v1", "f2", "v2"])
        .await;

    let resp = client.command(&["COPY", "{k}src", "{k}dst"]).await;
    assert_eq!(unwrap_integer(&resp), 1);

    assert_bulk_eq(&client.command(&["HGET", "{k}dst", "f1"]).await, b"v1");
    assert_bulk_eq(&client.command(&["HGET", "{k}dst", "f2"]).await, b"v2");
}

#[tokio::test]
async fn copy_list_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "{k}src", "a", "b", "c"]).await;

    let resp = client.command(&["COPY", "{k}src", "{k}dst"]).await;
    assert_eq!(unwrap_integer(&resp), 1);

    assert_eq!(
        unwrap_integer(&client.command(&["LLEN", "{k}dst"]).await),
        3
    );
    assert_bulk_eq(&client.command(&["LINDEX", "{k}dst", "0"]).await, b"a");
}

#[tokio::test]
async fn copy_json_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "{k}src", "$", r#"{"a":1,"b":"hello"}"#])
            .await,
    );

    let resp = client.command(&["COPY", "{k}src", "{k}dst"]).await;
    assert_eq!(unwrap_integer(&resp), 1);

    let dst_val = client.command(&["JSON.GET", "{k}dst", "$"]).await;
    assert!(
        !matches!(dst_val, frogdb_protocol::Response::Error(_)),
        "JSON.GET on copied key should not error: {dst_val:?}"
    );
}
