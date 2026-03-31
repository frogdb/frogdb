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

// ===========================================================================
// HGETEX — get fields and atomically set/remove field expiry (Redis 8.0)
// ===========================================================================

#[tokio::test]
async fn hgetex_basic_returns_values() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "h", "f1", "v1", "f2", "v2", "f3", "v3"])
        .await;

    let resp = client
        .command(&["HGETEX", "h", "FIELDS", "3", "f1", "f2", "f3"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 3);
    assert_bulk_eq(&items[0], b"v1");
    assert_bulk_eq(&items[1], b"v2");
    assert_bulk_eq(&items[2], b"v3");
}

#[tokio::test]
async fn hgetex_with_ex_sets_field_expiry() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "h", "f1", "v1"]).await;

    let resp = client
        .command(&["HGETEX", "h", "EX", "100", "FIELDS", "1", "f1"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 1);
    assert_bulk_eq(&items[0], b"v1");

    // Field should now have a TTL
    let ttl = client.command(&["HTTL", "h", "FIELDS", "1", "f1"]).await;
    let ttl_arr = unwrap_array(ttl);
    let ttl_val = unwrap_integer(&ttl_arr[0]);
    assert!(ttl_val > 0 && ttl_val <= 100, "expected TTL in (0,100], got {ttl_val}");
}

#[tokio::test]
async fn hgetex_with_px_sets_field_expiry_millis() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "h", "f1", "v1"]).await;

    let resp = client
        .command(&["HGETEX", "h", "PX", "50000", "FIELDS", "1", "f1"])
        .await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"v1");

    let pttl = client.command(&["HPTTL", "h", "FIELDS", "1", "f1"]).await;
    let pttl_arr = unwrap_array(pttl);
    let pttl_val = unwrap_integer(&pttl_arr[0]);
    assert!(pttl_val > 0 && pttl_val <= 50000, "expected PTTL in (0,50000], got {pttl_val}");
}

#[tokio::test]
async fn hgetex_persist_removes_field_expiry() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "h", "f1", "v1"]).await;
    // Set an expiry first
    client
        .command(&["HEXPIRE", "h", "100", "FIELDS", "1", "f1"])
        .await;

    // PERSIST should remove it
    let resp = client
        .command(&["HGETEX", "h", "PERSIST", "FIELDS", "1", "f1"])
        .await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"v1");

    // Verify no TTL (-1 = no expiry)
    let ttl = client.command(&["HTTL", "h", "FIELDS", "1", "f1"]).await;
    let ttl_arr = unwrap_array(ttl);
    assert_eq!(unwrap_integer(&ttl_arr[0]), -1);
}

#[tokio::test]
async fn hgetex_nonexistent_key_returns_nils() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["HGETEX", "nokey", "FIELDS", "2", "f1", "f2"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 2);
    assert_nil(&items[0]);
    assert_nil(&items[1]);
}

#[tokio::test]
async fn hgetex_missing_fields_return_nil() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "h", "f1", "v1"]).await;

    let resp = client
        .command(&["HGETEX", "h", "FIELDS", "2", "f1", "missing"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 2);
    assert_bulk_eq(&items[0], b"v1");
    assert_nil(&items[1]);
}

#[tokio::test]
async fn hgetex_wrongtype_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "str", "hello"]).await;

    let resp = client
        .command(&["HGETEX", "str", "FIELDS", "1", "f1"])
        .await;
    assert_error_prefix(&resp, "WRONGTYPE");
}

// ===========================================================================
// HSETEX — set fields with conditions and expiry (Redis 8.0)
// ===========================================================================

#[tokio::test]
async fn hsetex_basic_sets_fields_with_expiry() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["HSETEX", "h", "EX", "100", "FIELDS", "2", "f1", "v1", "f2", "v2"])
        .await;
    // Returns 1 on success
    assert_eq!(unwrap_integer(&resp), 1);

    assert_bulk_eq(&client.command(&["HGET", "h", "f1"]).await, b"v1");
    assert_bulk_eq(&client.command(&["HGET", "h", "f2"]).await, b"v2");

    // Both fields should have TTL
    let ttl = client.command(&["HTTL", "h", "FIELDS", "1", "f1"]).await;
    let ttl_arr = unwrap_array(ttl);
    let ttl_val = unwrap_integer(&ttl_arr[0]);
    assert!(ttl_val > 0 && ttl_val <= 100);
}

#[tokio::test]
async fn hsetex_fnx_only_sets_when_no_field_exists() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // First call: no fields exist, should succeed
    let resp = client
        .command(&["HSETEX", "h", "FNX", "EX", "100", "FIELDS", "1", "f1", "v1"])
        .await;
    assert_eq!(unwrap_integer(&resp), 1);

    // Second call: f1 already exists, should return 0 and not modify
    let resp = client
        .command(&["HSETEX", "h", "FNX", "EX", "100", "FIELDS", "1", "f1", "v2"])
        .await;
    assert_eq!(unwrap_integer(&resp), 0);

    // Value should still be v1
    assert_bulk_eq(&client.command(&["HGET", "h", "f1"]).await, b"v1");
}

#[tokio::test]
async fn hsetex_fxx_only_sets_when_all_fields_exist() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // FXX on non-existent field: returns 0
    let resp = client
        .command(&["HSETEX", "h", "FXX", "EX", "100", "FIELDS", "1", "f1", "v1"])
        .await;
    assert_eq!(unwrap_integer(&resp), 0);

    // Field should not exist
    assert_nil(&client.command(&["HGET", "h", "f1"]).await);

    // Now create the field, then update with FXX
    client.command(&["HSET", "h", "f1", "v1"]).await;
    let resp = client
        .command(&["HSETEX", "h", "FXX", "EX", "100", "FIELDS", "1", "f1", "v2"])
        .await;
    assert_eq!(unwrap_integer(&resp), 1); // 1 = success (field existed)

    assert_bulk_eq(&client.command(&["HGET", "h", "f1"]).await, b"v2");
}

#[tokio::test]
async fn hsetex_keepttl_preserves_existing_field_ttl() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set field with TTL
    client
        .command(&["HSETEX", "h", "EX", "200", "FIELDS", "1", "f1", "v1"])
        .await;

    let ttl_before = client.command(&["HTTL", "h", "FIELDS", "1", "f1"]).await;
    let ttl_before_val = unwrap_integer(&unwrap_array(ttl_before)[0]);
    assert!(ttl_before_val > 0);

    // Overwrite with KEEPTTL — value changes but TTL is preserved
    let resp = client
        .command(&["HSETEX", "h", "KEEPTTL", "FIELDS", "1", "f1", "v2"])
        .await;
    // Should succeed (field existed, no condition)
    unwrap_integer(&resp);

    assert_bulk_eq(&client.command(&["HGET", "h", "f1"]).await, b"v2");

    let ttl_after = client.command(&["HTTL", "h", "FIELDS", "1", "f1"]).await;
    let ttl_after_val = unwrap_integer(&unwrap_array(ttl_after)[0]);
    assert!(ttl_after_val > 0 && ttl_after_val <= ttl_before_val);
}

#[tokio::test]
async fn hsetex_wrongtype_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "str", "hello"]).await;

    let resp = client
        .command(&["HSETEX", "str", "EX", "100", "FIELDS", "1", "f1", "v1"])
        .await;
    assert_error_prefix(&resp, "WRONGTYPE");
}

#[tokio::test]
async fn hsetex_creates_key_if_not_exists() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["HSETEX", "newkey", "EX", "100", "FIELDS", "1", "f1", "v1"])
        .await;
    assert_eq!(unwrap_integer(&resp), 1);

    assert_bulk_eq(&client.command(&["HGET", "newkey", "f1"]).await, b"v1");
    let resp = client.command(&["TYPE", "newkey"]).await;
    // TYPE returns a status reply
    if let frogdb_protocol::Response::Simple(s) = &resp {
        assert_eq!(s, "hash");
    } else if let frogdb_protocol::Response::Bulk(Some(b)) = &resp {
        assert_eq!(b.as_ref(), b"hash");
    } else {
        panic!("expected hash type, got {resp:?}");
    }
}
