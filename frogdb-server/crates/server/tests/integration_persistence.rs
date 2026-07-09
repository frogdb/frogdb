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
// HyperLogLog dense-delta WAL persistence (Tier 2 merge path)
//
// A PFADD on a *dense* existing HLL persists only the raised registers as a WAL
// `Merge` operand, not a full `Put`. This exercises the whole delta path end to
// end: the merge operand is emitted (asserted via the `WalMergeOperands`
// counter), the RocksDB merge operator folds it onto the base value, and a
// restart must recover the exact merged cardinality.
// ============================================================================

/// Parse the value of a no-label Prometheus counter/gauge from the metrics text.
/// Returns 0.0 if the metric is absent (a counter is only exported once it has
/// been incremented at least once).
fn metric_value(metrics: &str, name: &str) -> f64 {
    metrics
        .lines()
        .filter(|l| !l.starts_with('#'))
        .find_map(|l| {
            let rest = l.trim().strip_prefix(name)?;
            // Exact-name match: the sample line is `name value`, so the char
            // after the name must be whitespace (reject `<name>_created`, etc.).
            if !rest.starts_with(char::is_whitespace) {
                return None;
            }
            rest.split_whitespace().last()?.parse::<f64>().ok()
        })
        .unwrap_or(0.0)
}

#[tokio::test]
async fn test_hll_delta_persistence_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();

    // --- First boot: build a dense HLL, then add via the delta path ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    // PFADD 4000 distinct elements in batches to force promotion to dense.
    for batch in 0..8 {
        let mut owned: Vec<String> = vec!["PFADD".to_string(), "hll".to_string()];
        for i in 0..500 {
            owned.push(format!("e:{}", batch * 500 + i));
        }
        let args: Vec<&str> = owned.iter().map(String::as_str).collect();
        client.command(&args).await;
    }

    // Precondition: the value is dense, so PFADD takes the delta-merge path.
    assert_eq!(
        client.command(&["PFDEBUG", "ENCODING", "hll"]).await,
        Response::Bulk(Some(Bytes::from("dense"))),
        "4000 distinct elements must promote the HLL to dense encoding"
    );

    let n1 = unwrap_integer(&client.command(&["PFCOUNT", "hll"]).await);

    // Delta path: dense existing key gains three new distinct registers.
    assert_eq!(
        client.command(&["PFADD", "hll", "x", "y", "z"]).await,
        Response::Integer(1)
    );
    let n2 = unwrap_integer(&client.command(&["PFCOUNT", "hll"]).await);
    assert!(n2 >= n1, "adding new elements must not shrink cardinality");

    // The dense PFADD(s) must have persisted as WAL merge operands.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let metrics = server.fetch_metrics().await;
    let merges = metric_value(&metrics, "frogdb_wal_merge_operands_total");
    assert!(
        merges >= 1.0,
        "a dense PFADD must persist as a WAL merge operand \
         (frogdb_wal_merge_operands_total >= 1), got {merges}\n{metrics}"
    );

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Second boot: recovery must fold the merge operand onto the base ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    let after_restart = unwrap_integer(&client.command(&["PFCOUNT", "hll"]).await);
    assert_eq!(
        after_restart, n2,
        "PFCOUNT after restart must equal the pre-restart value exactly"
    );

    // Duplicate PFADD is a no-op (Tier 1 suppression): nothing new persists.
    assert_eq!(
        client.command(&["PFADD", "hll", "x", "y", "z"]).await,
        Response::Integer(0)
    );
    let after_dup = unwrap_integer(&client.command(&["PFCOUNT", "hll"]).await);
    assert_eq!(after_dup, n2, "no-op PFADD must not change cardinality");

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Third boot: the no-op left the persisted value unchanged ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;
    let after_second_restart = unwrap_integer(&client.command(&["PFCOUNT", "hll"]).await);
    assert_eq!(
        after_second_restart, n2,
        "a suppressed no-op PFADD must not alter the persisted cardinality"
    );

    server.shutdown().await;
}
