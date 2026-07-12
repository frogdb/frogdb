//! Integration tests for persistence: write → shutdown → restart → read cycles.

use crate::common::response_helpers::{assert_ok, unwrap_array, unwrap_bulk, unwrap_integer};
use crate::common::test_server::{TestServer, TestServerConfig};
use bytes::Bytes;
use frogdb_protocol::Response;
use std::time::Duration;

/// Helper: build a persistence-enabled config pointing at the given directory.
fn persistence_config(data_dir: &std::path::Path) -> TestServerConfig {
    TestServerConfig {
        persistence: true,
        data_dir: Some(data_dir.to_path_buf()),
        num_shards: Some(1),
        ..Default::default()
    }
}

/// Helper: persistence config with an explicit shard count.
fn persistence_config_shards(data_dir: &std::path::Path, num_shards: usize) -> TestServerConfig {
    TestServerConfig {
        persistence: true,
        data_dir: Some(data_dir.to_path_buf()),
        num_shards: Some(num_shards),
        ..Default::default()
    }
}

// ============================================================================
// Test 1: Data survives a restart
// ============================================================================

#[tokio::test]
async fn test_data_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();

    // --- First boot: write data ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "key1", "value1"]).await);
    assert_eq!(
        client.command(&["LPUSH", "list1", "a", "b", "c"]).await,
        Response::Integer(3)
    );
    assert_eq!(
        client.command(&["HSET", "hash1", "f1", "v1"]).await,
        Response::Integer(1)
    );
    assert_eq!(
        client.command(&["SADD", "set1", "m1", "m2"]).await,
        Response::Integer(2)
    );

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Second boot: verify data ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    // String
    assert_eq!(
        client.command(&["GET", "key1"]).await,
        Response::Bulk(Some(Bytes::from("value1")))
    );

    // List (LPUSH a b c → stored as [c, b, a])
    let list = unwrap_array(client.command(&["LRANGE", "list1", "0", "-1"]).await);
    let vals: Vec<&[u8]> = list.iter().map(unwrap_bulk).collect();
    assert_eq!(vals, vec![b"c", b"b", b"a"]);

    // Hash
    assert_eq!(
        client.command(&["HGET", "hash1", "f1"]).await,
        Response::Bulk(Some(Bytes::from("v1")))
    );

    // Set
    let members = unwrap_array(client.command(&["SMEMBERS", "set1"]).await);
    let mut strs: Vec<String> = members
        .iter()
        .map(|r| String::from_utf8(unwrap_bulk(r).to_vec()).unwrap())
        .collect();
    strs.sort();
    assert_eq!(strs, vec!["m1", "m2"]);

    server.shutdown().await;
}

// ============================================================================
// Test 2: Expiry persists across restart
// ============================================================================

#[tokio::test]
async fn test_expiry_persists_across_restart() {
    let tmp = tempfile::tempdir().unwrap();

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["SET", "key1", "value1", "EX", "3600"])
            .await,
    );
    let ttl = unwrap_integer(&client.command(&["TTL", "key1"]).await);
    assert!(ttl > 3500, "TTL should be ~3600, got {ttl}");

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_eq!(
        client.command(&["GET", "key1"]).await,
        Response::Bulk(Some(Bytes::from("value1")))
    );
    let ttl = unwrap_integer(&client.command(&["TTL", "key1"]).await);
    assert!(
        ttl > 0,
        "TTL should still be positive after restart, got {ttl}"
    );
    assert!(ttl <= 3600, "TTL should not exceed original, got {ttl}");

    server.shutdown().await;
}

// ============================================================================
// Test 3: Deleted keys stay deleted after restart
// ============================================================================

#[tokio::test]
async fn test_deleted_keys_stay_deleted_after_restart() {
    let tmp = tempfile::tempdir().unwrap();

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "key1", "val1"]).await);
    assert_ok(&client.command(&["SET", "key2", "val2"]).await);
    assert_ok(&client.command(&["SET", "key3", "val3"]).await);
    assert_eq!(client.command(&["DEL", "key2"]).await, Response::Integer(1));

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_eq!(
        client.command(&["GET", "key1"]).await,
        Response::Bulk(Some(Bytes::from("val1")))
    );
    assert_eq!(
        client.command(&["GET", "key2"]).await,
        Response::Bulk(None) // deleted
    );
    assert_eq!(
        client.command(&["GET", "key3"]).await,
        Response::Bulk(Some(Bytes::from("val3")))
    );

    server.shutdown().await;
}

// ============================================================================
// Test 4: BGSAVE snapshot + WAL replay both survive restart
// ============================================================================

#[tokio::test]
async fn test_bgsave_snapshot_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    // Write data and snapshot it
    assert_ok(&client.command(&["SET", "snap_key", "snap_val"]).await);
    let bgsave_resp = client.command(&["BGSAVE"]).await;
    match &bgsave_resp {
        Response::Simple(msg) => {
            let msg_str = String::from_utf8_lossy(msg);
            assert!(
                msg_str.contains("Background saving started")
                    || msg_str.contains("already in progress"),
                "Unexpected BGSAVE response: {msg_str}"
            );
        }
        _ => panic!("Expected simple string response for BGSAVE, got: {bgsave_resp:?}"),
    }

    // Wait for snapshot to complete
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Write more data AFTER the snapshot — this should be in the WAL only
    assert_ok(&client.command(&["SET", "wal_key", "wal_val"]).await);

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    // Snapshot data
    assert_eq!(
        client.command(&["GET", "snap_key"]).await,
        Response::Bulk(Some(Bytes::from("snap_val")))
    );
    // WAL-replayed data
    assert_eq!(
        client.command(&["GET", "wal_key"]).await,
        Response::Bulk(Some(Bytes::from("wal_val")))
    );

    server.shutdown().await;
}

// ============================================================================
// Test 5: Empty restart — no data, server healthy
// ============================================================================

#[tokio::test]
async fn test_empty_restart_no_data() {
    let tmp = tempfile::tempdir().unwrap();

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_eq!(client.command(&["DBSIZE"]).await, Response::Integer(0));
    assert_eq!(
        client.command(&["PING"]).await,
        Response::Simple(Bytes::from_static(b"PONG"))
    );

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_eq!(client.command(&["DBSIZE"]).await, Response::Integer(0));
    assert_eq!(
        client.command(&["PING"]).await,
        Response::Simple(Bytes::from_static(b"PONG"))
    );

    server.shutdown().await;
}

// ============================================================================
// Test 6: Multiple restarts accumulate data
// ============================================================================

#[tokio::test]
async fn test_multiple_restarts_accumulate_data() {
    let tmp = tempfile::tempdir().unwrap();

    // --- Cycle 1: write key1 ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;
    assert_ok(&client.command(&["SET", "key1", "val1"]).await);
    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Cycle 2: write key2, verify key1 ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;
    assert_eq!(
        client.command(&["GET", "key1"]).await,
        Response::Bulk(Some(Bytes::from("val1")))
    );
    assert_ok(&client.command(&["SET", "key2", "val2"]).await);
    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Cycle 3: write key3, verify key1 + key2 ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;
    assert_eq!(
        client.command(&["GET", "key1"]).await,
        Response::Bulk(Some(Bytes::from("val1")))
    );
    assert_eq!(
        client.command(&["GET", "key2"]).await,
        Response::Bulk(Some(Bytes::from("val2")))
    );
    assert_ok(&client.command(&["SET", "key3", "val3"]).await);
    assert_eq!(client.command(&["DBSIZE"]).await, Response::Integer(3));

    server.shutdown().await;
}

// ============================================================================
// Test 7: Restarting with a different shard count fails loudly (no data loss)
// ============================================================================

#[tokio::test]
async fn test_shard_count_mismatch_fails_startup() {
    let tmp = tempfile::tempdir().unwrap();

    // --- First boot: write data with 4 shards ---
    let server =
        TestServer::start_standalone_with_config(persistence_config_shards(tmp.path(), 4)).await;
    let mut client = server.connect().await;
    // Use several keys so they spread across shards.
    for i in 0..16 {
        assert_ok(
            &client
                .command(&["SET", &format!("k{i}"), &format!("v{i}")])
                .await,
        );
    }
    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Second boot: a DIFFERENT shard count must be rejected, not silently
    //     started with empty / misrouted shards. ---
    let err = match TestServer::try_start_standalone_with_config(persistence_config_shards(
        tmp.path(),
        8,
    ))
    .await
    {
        Ok(_) => panic!("startup must fail on shard-count mismatch instead of dropping data"),
        Err(e) => e,
    };
    assert!(
        err.contains("shard count mismatch") && err.contains('4') && err.contains('8'),
        "error should name both shard counts, got: {err}"
    );

    // Shrinking is rejected too.
    let err = match TestServer::try_start_standalone_with_config(persistence_config_shards(
        tmp.path(),
        2,
    ))
    .await
    {
        Ok(_) => panic!("startup must fail when shrinking shard count"),
        Err(e) => e,
    };
    assert!(
        err.contains("shard count mismatch"),
        "error should describe the mismatch, got: {err}"
    );

    // --- Third boot: matching shard count succeeds and data is intact. ---
    let server =
        TestServer::start_standalone_with_config(persistence_config_shards(tmp.path(), 4)).await;
    let mut client = server.connect().await;
    assert_eq!(client.command(&["DBSIZE"]).await, Response::Integer(16));
    for i in 0..16 {
        assert_eq!(
            client.command(&["GET", &format!("k{i}")]).await,
            Response::Bulk(Some(Bytes::from(format!("v{i}"))))
        );
    }
    server.shutdown().await;
}

// ============================================================================
// Dynamic STORE-destination WAL persistence (CommandSpec WAL-gap fix)
// ============================================================================

/// GEORADIUS ... STORE writes its destination at a *dynamic* argument position.
/// Before the declarative-spec migration the index-based WAL strategy never
/// captured it, so the stored zset was lost on restart. WalStrategy::Dynamic
/// now persists exactly the write-access keys from the command's own key
/// extraction. This verifies the destination survives a restart.
#[tokio::test]
async fn test_georadius_store_destination_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();

    // --- First boot: populate a geo set and STORE a radius query ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_eq!(
        client
            .command(&["GEOADD", "geo", "13.361389", "38.115556", "Palermo"])
            .await,
        Response::Integer(1)
    );
    assert_eq!(
        client
            .command(&["GEOADD", "geo", "15.087269", "37.502669", "Catania"])
            .await,
        Response::Integer(1)
    );
    // Both points fall within 200km of (15, 37); STORE the result set.
    let stored = unwrap_integer(
        &client
            .command(&[
                "GEORADIUS",
                "geo",
                "15",
                "37",
                "200",
                "km",
                "STORE",
                "geo:store",
            ])
            .await,
    );
    assert_eq!(stored, 2);
    assert_eq!(
        client.command(&["ZCARD", "geo:store"]).await,
        Response::Integer(2)
    );

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Second boot: the stored zset must still be present ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_eq!(
        client.command(&["ZCARD", "geo:store"]).await,
        Response::Integer(2),
        "GEORADIUS STORE destination must survive a restart"
    );
    let members = unwrap_array(client.command(&["ZRANGE", "geo:store", "0", "-1"]).await);
    let mut strs: Vec<String> = members
        .iter()
        .map(|r| String::from_utf8(unwrap_bulk(r).to_vec()).unwrap())
        .collect();
    strs.sort();
    assert_eq!(strs, vec!["Catania", "Palermo"]);

    server.shutdown().await;
}

/// SORT ... STORE has the same dynamic-destination WAL gap as GEORADIUS STORE.
#[tokio::test]
async fn test_sort_store_destination_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_eq!(
        client.command(&["RPUSH", "nums", "3", "1", "2"]).await,
        Response::Integer(3)
    );
    let stored = unwrap_integer(
        &client
            .command(&["SORT", "nums", "STORE", "nums:sorted"])
            .await,
    );
    assert_eq!(stored, 3);

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    let list = unwrap_array(client.command(&["LRANGE", "nums:sorted", "0", "-1"]).await);
    let vals: Vec<&[u8]> = list.iter().map(unwrap_bulk).collect();
    assert_eq!(
        vals,
        vec![b"1", b"2", b"3"],
        "SORT STORE destination must survive a restart"
    );

    server.shutdown().await;
}

// ============================================================================
// Proposal-01-era WAL argument regressions
// ----------------------------------------------------------------------------
// MSETEX, BITOP, XGROUP CREATE and XREADGROUP each once had a WAL declaration
// that persisted the *wrong* argument (a non-key argument) or nothing at all
// (a WAL-NoOp). The spec-level guard `every_write_command_declares_wal` catches
// only the NoOp case; a strategy that persists the wrong arg still declares a
// WAL action and passes that guard silently. These restart-survival tests close
// that gap by exercising the real write → shutdown → restart → read cycle.
// ============================================================================

/// MSETEX must persist *every* key (not the leading `numkeys` argument) with
/// its value and TTL. The historical defect persisted a non-key argument, so
/// the actual keys were lost on restart.
#[tokio::test]
async fn test_msetex_keys_survive_restart() {
    let tmp = tempfile::tempdir().unwrap();

    // --- First boot: MSETEX two keys with a shared TTL ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    // MSETEX replies Integer(1) when all keys were set.
    assert_eq!(
        client
            .command(&["MSETEX", "2", "mk1", "mv1", "mk2", "mv2", "EX", "3600"])
            .await,
        Response::Integer(1)
    );
    // Sanity: both keys present with the expiry before restart.
    assert_eq!(
        client.command(&["GET", "mk1"]).await,
        Response::Bulk(Some(Bytes::from("mv1")))
    );
    assert!(unwrap_integer(&client.command(&["TTL", "mk1"]).await) > 3500);

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Second boot: every key + value + TTL must survive ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_eq!(
        client.command(&["GET", "mk1"]).await,
        Response::Bulk(Some(Bytes::from("mv1"))),
        "MSETEX key mk1 must survive a restart"
    );
    assert_eq!(
        client.command(&["GET", "mk2"]).await,
        Response::Bulk(Some(Bytes::from("mv2"))),
        "MSETEX key mk2 must survive a restart"
    );
    let ttl1 = unwrap_integer(&client.command(&["TTL", "mk1"]).await);
    assert!(ttl1 > 3500, "MSETEX TTL must survive a restart, got {ttl1}");
    let ttl2 = unwrap_integer(&client.command(&["TTL", "mk2"]).await);
    assert!(ttl2 > 3500, "MSETEX TTL must survive a restart, got {ttl2}");

    server.shutdown().await;
}

/// BITOP must persist its destination key (args[1]), not a source key or the
/// operation name. The historical defect persisted the wrong argument, dropping
/// the computed destination on restart.
#[tokio::test]
async fn test_bitop_destination_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();

    // --- First boot: XOR two sources into a destination ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "bsrc1", "abc"]).await);
    assert_ok(&client.command(&["SET", "bsrc2", "abd"]).await);
    // "abc" XOR "abd" = [0x00, 0x00, 0x07]  (only the last byte differs).
    let len = unwrap_integer(
        &client
            .command(&["BITOP", "XOR", "bdest", "bsrc1", "bsrc2"])
            .await,
    );
    assert_eq!(len, 3);
    let expected = Bytes::from_static(&[0x00, 0x00, 0x07]);
    assert_eq!(
        client.command(&["GET", "bdest"]).await,
        Response::Bulk(Some(expected.clone()))
    );

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Second boot: the destination bytes must survive ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_eq!(
        client.command(&["GET", "bdest"]).await,
        Response::Bulk(Some(expected)),
        "BITOP destination bytes must survive a restart"
    );

    server.shutdown().await;
}

/// XGROUP CREATE must persist the target stream key (args[1] after the
/// subcommand) so the consumer group exists after a restart. The historical
/// defect persisted the wrong argument (e.g. the CREATE subcommand token).
///
/// Currently ignored: stream serialization deliberately omits consumer groups
/// (`persistence/src/serialization/stream.rs`), so no WAL declaration can make
/// a group survive a restart yet. Unignore once group state is serialized.
#[ignore = "consumer groups are not serialized (see persistence serialization/stream.rs); \
            groups cannot survive a restart regardless of the WAL declaration"]
#[tokio::test]
async fn test_xgroup_create_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();

    // --- First boot: create a stream and a consumer group ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    let _ = client
        .command(&["XADD", "xg:stream", "1-1", "field1", "value1"])
        .await;
    assert_ok(
        &client
            .command(&["XGROUP", "CREATE", "xg:stream", "xg:group", "0"])
            .await,
    );

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Second boot: the group must still be registered on the stream ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    let groups = unwrap_array(client.command(&["XINFO", "GROUPS", "xg:stream"]).await);
    assert!(
        groups.iter().any(|g| {
            let fields = match g {
                Response::Array(arr) => arr,
                _ => return false,
            };
            // Each group is a flat [name, <name>, ...] field list.
            fields.windows(2).any(|w| {
                w[0] == Response::bulk(Bytes::from_static(b"name"))
                    && w[1] == Response::bulk(Bytes::from_static(b"xg:group"))
            })
        }),
        "XGROUP CREATE consumer group must survive a restart, got: {groups:?}"
    );

    server.shutdown().await;
}

/// XREADGROUP is a write: reading with `>` advances the group's last-delivered
/// id and adds the delivered entry to the consumer's PEL. Those effects must
/// survive a restart. The historical defect persisted the wrong argument, so
/// the pending entry was lost.
///
/// Currently ignored for the same reason as `test_xgroup_create_survives_restart`:
/// consumer-group state (including the PEL) is not serialized, so the pending
/// entry cannot survive a restart regardless of the WAL declaration.
#[ignore = "consumer groups (incl. PEL) are not serialized (see persistence serialization/stream.rs); \
            pending entries cannot survive a restart regardless of the WAL declaration"]
#[tokio::test]
async fn test_xreadgroup_pending_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();

    // --- First boot: deliver an entry to a consumer (no NOACK ⇒ it pends) ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    let _ = client
        .command(&["XADD", "xr:stream", "1-1", "field1", "value1"])
        .await;
    assert_ok(
        &client
            .command(&["XGROUP", "CREATE", "xr:stream", "xr:group", "0"])
            .await,
    );
    let delivered = unwrap_array(
        client
            .command(&[
                "XREADGROUP",
                "GROUP",
                "xr:group",
                "xr:consumer",
                "STREAMS",
                "xr:stream",
                ">",
            ])
            .await,
    );
    assert!(!delivered.is_empty(), "XREADGROUP should deliver one entry");
    // The delivered entry is now pending for xr:consumer.
    let pending = client.command(&["XPENDING", "xr:stream", "xr:group"]).await;
    assert_eq!(
        pending_count(&pending),
        1,
        "one entry should be pending before restart"
    );

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Second boot: the PEL / last-delivered effect must survive ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    let pending = client.command(&["XPENDING", "xr:stream", "xr:group"]).await;
    assert_eq!(
        pending_count(&pending),
        1,
        "XREADGROUP pending entry must survive a restart, got: {pending:?}"
    );

    server.shutdown().await;
}

/// Extract the summary count (first element) from an XPENDING reply.
fn pending_count(response: &Response) -> i64 {
    match response {
        Response::Array(items) => unwrap_integer(&items[0]),
        _ => panic!("expected array from XPENDING, got {response:?}"),
    }
}
