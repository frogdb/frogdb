use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn dump_restore_string_roundtrip() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    let dumped = client.command(&["DUMP", "foo"]).await;
    // DUMP returns serialized bytes (non-nil bulk)
    assert!(!matches!(dumped, frogdb_protocol::Response::Bulk(None)));
    let serialized = unwrap_bulk(&dumped).to_vec();

    client.command(&["DEL", "foo"]).await;
    assert_eq!(unwrap_integer(&client.command(&["EXISTS", "foo"]).await), 0);

    // RESTORE recreates the key
    // We need to send raw bytes for the serialized value
    use bytes::Bytes;
    let resp = client
        .command_raw(&[
            &Bytes::from("RESTORE"),
            &Bytes::from("foo"),
            &Bytes::from("0"),
            &Bytes::from(serialized),
        ])
        .await;
    assert_ok(&resp);

    assert_bulk_eq(&client.command(&["GET", "foo"]).await, b"bar");
    // No expiry: TTL should be -1
    assert_eq!(unwrap_integer(&client.command(&["TTL", "foo"]).await), -1);
}

#[tokio::test]
async fn dump_restore_with_ttl() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    let dumped = client.command(&["DUMP", "foo"]).await;
    let serialized = unwrap_bulk(&dumped).to_vec();

    client.command(&["DEL", "foo"]).await;

    use bytes::Bytes;
    let resp = client
        .command_raw(&[
            &Bytes::from("RESTORE"),
            &Bytes::from("foo"),
            &Bytes::from("5000"),
            &Bytes::from(serialized),
        ])
        .await;
    assert_ok(&resp);

    let pttl = unwrap_integer(&client.command(&["PTTL", "foo"]).await);
    assert!(pttl > 3000 && pttl <= 5000, "PTTL {pttl} out of expected range");
    assert_bulk_eq(&client.command(&["GET", "foo"]).await, b"bar");
}

#[tokio::test]
async fn dump_restore_replace_overwrites_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "original"]).await;
    let dumped = client.command(&["DUMP", "foo"]).await;
    let serialized = unwrap_bulk(&dumped).to_vec();

    // Key still exists — RESTORE without REPLACE should error
    use bytes::Bytes;
    let resp = client
        .command_raw(&[
            &Bytes::from("RESTORE"),
            &Bytes::from("foo"),
            &Bytes::from("0"),
            &Bytes::from(serialized.clone()),
        ])
        .await;
    assert_error_prefix(&resp, "BUSYKEY");

    // With REPLACE it should succeed
    let resp = client
        .command_raw(&[
            &Bytes::from("RESTORE"),
            &Bytes::from("foo"),
            &Bytes::from("0"),
            &Bytes::from(serialized),
            &Bytes::from("REPLACE"),
        ])
        .await;
    assert_ok(&resp);
    assert_bulk_eq(&client.command(&["GET", "foo"]).await, b"original");
}

#[tokio::test]
async fn dump_on_nonexistent_key_returns_nil() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["DUMP", "no_such_key"]).await;
    assert!(matches!(resp, frogdb_protocol::Response::Bulk(None)));
}

#[tokio::test]
async fn dump_restore_hash_roundtrip() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "myhash", "f1", "v1", "f2", "v2"]).await;
    let dumped = client.command(&["DUMP", "myhash"]).await;
    let serialized = unwrap_bulk(&dumped).to_vec();

    client.command(&["DEL", "myhash"]).await;

    use bytes::Bytes;
    let resp = client
        .command_raw(&[
            &Bytes::from("RESTORE"),
            &Bytes::from("myhash"),
            &Bytes::from("0"),
            &Bytes::from(serialized),
        ])
        .await;
    assert_ok(&resp);

    assert_bulk_eq(&client.command(&["HGET", "myhash", "f1"]).await, b"v1");
    assert_bulk_eq(&client.command(&["HGET", "myhash", "f2"]).await, b"v2");
}
