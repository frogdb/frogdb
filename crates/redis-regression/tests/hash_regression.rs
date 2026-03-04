use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn hset_hget_hdel_basic_roundtrip() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // HSET returns number of new fields added
    let resp = client.command(&["HSET", "myhash", "f1", "v1"]).await;
    assert_eq!(unwrap_integer(&resp), 1);

    // HGET returns the value
    let resp = client.command(&["HGET", "myhash", "f1"]).await;
    assert_bulk_eq(&resp, b"v1");

    // HSET update returns 0 (field already exists)
    let resp = client.command(&["HSET", "myhash", "f1", "v2"]).await;
    assert_eq!(unwrap_integer(&resp), 0);

    let resp = client.command(&["HGET", "myhash", "f1"]).await;
    assert_bulk_eq(&resp, b"v2");

    // HDEL returns 1 for existing field
    let resp = client.command(&["HDEL", "myhash", "f1"]).await;
    assert_eq!(unwrap_integer(&resp), 1);

    // HDEL returns 0 for non-existing field
    let resp = client.command(&["HDEL", "myhash", "f1"]).await;
    assert_eq!(unwrap_integer(&resp), 0);

    // HGET returns nil after deletion
    let resp = client.command(&["HGET", "myhash", "f1"]).await;
    assert!(matches!(resp, frogdb_protocol::Response::Bulk(None)));
}

#[tokio::test]
async fn hsetnx_only_sets_when_field_absent() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // HSETNX on missing field returns 1 and sets value
    let resp = client.command(&["HSETNX", "myhash", "f1", "first"]).await;
    assert_eq!(unwrap_integer(&resp), 1);
    assert_bulk_eq(&client.command(&["HGET", "myhash", "f1"]).await, b"first");

    // HSETNX on existing field returns 0 and does not overwrite
    let resp = client.command(&["HSETNX", "myhash", "f1", "second"]).await;
    assert_eq!(unwrap_integer(&resp), 0);
    assert_bulk_eq(&client.command(&["HGET", "myhash", "f1"]).await, b"first");
}

#[tokio::test]
async fn hmset_hmget_multiple_fields() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["HMSET", "myhash", "a", "1", "b", "2", "c", "3"])
            .await,
    );

    let resp = client
        .command(&["HMGET", "myhash", "a", "b", "c", "missing"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 4);
    assert_bulk_eq(&items[0], b"1");
    assert_bulk_eq(&items[1], b"2");
    assert_bulk_eq(&items[2], b"3");
    assert!(matches!(items[3], frogdb_protocol::Response::Bulk(None)));
}

#[tokio::test]
async fn hgetall_returns_all_field_value_pairs() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "myhash", "f1", "v1", "f2", "v2"])
        .await;
    let resp = client.command(&["HGETALL", "myhash"]).await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items.len(), 4);
    // Items come as field, value pairs; sort for determinism
    let mut sorted = items.clone();
    sorted.sort();
    assert!(sorted.contains(&"f1".to_string()));
    assert!(sorted.contains(&"f2".to_string()));
    assert!(sorted.contains(&"v1".to_string()));
    assert!(sorted.contains(&"v2".to_string()));
}

#[tokio::test]
async fn hlen_tracks_field_count() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_eq!(
        unwrap_integer(&client.command(&["HLEN", "myhash"]).await),
        0
    );
    client
        .command(&["HSET", "myhash", "a", "1", "b", "2"])
        .await;
    assert_eq!(
        unwrap_integer(&client.command(&["HLEN", "myhash"]).await),
        2
    );
}

#[tokio::test]
async fn hkeys_and_hvals() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "myhash", "x", "10", "y", "20"])
        .await;

    let keys = extract_bulk_strings(&client.command(&["HKEYS", "myhash"]).await);
    let mut keys_sorted = keys.clone();
    keys_sorted.sort();
    assert_eq!(keys_sorted, vec!["x", "y"]);

    let vals = extract_bulk_strings(&client.command(&["HVALS", "myhash"]).await);
    let mut vals_sorted = vals.clone();
    vals_sorted.sort();
    assert_eq!(vals_sorted, vec!["10", "20"]);
}

#[tokio::test]
async fn hexists_reports_field_presence() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "myhash", "f1", "v1"]).await;
    assert_eq!(
        unwrap_integer(&client.command(&["HEXISTS", "myhash", "f1"]).await),
        1
    );
    assert_eq!(
        unwrap_integer(&client.command(&["HEXISTS", "myhash", "missing"]).await),
        0
    );
}

#[tokio::test]
async fn hincrby_and_hincrbyfloat() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["HINCRBY", "myhash", "counter", "5"]).await;
    assert_eq!(unwrap_integer(&resp), 5);

    let resp = client.command(&["HINCRBY", "myhash", "counter", "3"]).await;
    assert_eq!(unwrap_integer(&resp), 8);

    let resp = client
        .command(&["HINCRBYFLOAT", "myhash", "score", "1.5"])
        .await;
    assert_bulk_eq(&resp, b"1.5");

    let resp = client
        .command(&["HINCRBYFLOAT", "myhash", "score", "0.5"])
        .await;
    assert_bulk_eq(&resp, b"2");
}

#[tokio::test]
async fn hrandfield_negative_count_allows_duplicates() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "myhash", "a", "1", "b", "2"])
        .await;

    // Negative count allows duplicates and always returns exactly |count| items
    let resp = client.command(&["HRANDFIELD", "myhash", "-5"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 5);
}

#[tokio::test]
async fn hrandfield_positive_count_no_duplicates() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "myhash", "a", "1", "b", "2", "c", "3"])
        .await;

    // Positive count returns unique fields, capped at hash size
    let resp = client.command(&["HRANDFIELD", "myhash", "10"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 3);
}

#[tokio::test]
async fn hstrlen_returns_correct_length() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "myhash", "f", "hello"]).await;
    let resp = client.command(&["HSTRLEN", "myhash", "f"]).await;
    assert_eq!(unwrap_integer(&resp), 5);

    // Missing field → 0
    let resp = client.command(&["HSTRLEN", "myhash", "missing"]).await;
    assert_eq!(unwrap_integer(&resp), 0);
}

#[tokio::test]
async fn hash_commands_against_wrong_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "wrongtype", "somevalue"]).await;

    let resp = client.command(&["HGET", "wrongtype", "f"]).await;
    assert_error_prefix(&resp, "WRONGTYPE");

    let resp = client.command(&["HSET", "wrongtype", "f", "v"]).await;
    assert_error_prefix(&resp, "WRONGTYPE");

    let resp = client.command(&["HGETALL", "wrongtype"]).await;
    assert_error_prefix(&resp, "WRONGTYPE");
}
