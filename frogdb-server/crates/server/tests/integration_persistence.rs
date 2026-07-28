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

/// SMOVE mutates two keys (SREM on the source, SADD on the destination) but
/// its spec previously declared `WalStrategy::PersistFirstKey`, which only
/// persisted the source. The destination add never reached the WAL and was
/// lost on restart. This pins the fix (`WalStrategy::MoveKeys`, the same
/// strategy RPOPLPUSH uses for its identical `[source, dest, ...]` shape):
/// the destination must survive, and — since the move empties the
/// single-member source — the source key must be gone too.
#[tokio::test]
async fn test_smove_destination_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_eq!(
        client.command(&["SADD", "src", "a"]).await,
        Response::Integer(1)
    );
    assert_eq!(
        client.command(&["SMOVE", "src", "dest", "a"]).await,
        Response::Integer(1)
    );

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_eq!(
        client.command(&["SISMEMBER", "dest", "a"]).await,
        Response::Integer(1),
        "SMOVE destination must survive a restart"
    );
    assert_eq!(
        client.command(&["SISMEMBER", "src", "a"]).await,
        Response::Integer(0)
    );
    assert_eq!(
        client.command(&["EXISTS", "src"]).await,
        Response::Integer(0),
        "source emptied by SMOVE must not be recreated on recovery"
    );

    server.shutdown().await;
}

/// Same fix, but with a pre-existing destination set: recovery must not clobber
/// the destination's other members when replaying the moved element.
#[tokio::test]
async fn test_smove_destination_survives_restart_with_existing_members() {
    let tmp = tempfile::tempdir().unwrap();

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_eq!(
        client.command(&["SADD", "dest", "b"]).await,
        Response::Integer(1)
    );
    assert_eq!(
        client.command(&["SADD", "src", "a"]).await,
        Response::Integer(1)
    );
    assert_eq!(
        client.command(&["SMOVE", "src", "dest", "a"]).await,
        Response::Integer(1)
    );

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_eq!(
        client.command(&["SISMEMBER", "dest", "a"]).await,
        Response::Integer(1),
        "SMOVE destination must survive a restart"
    );
    assert_eq!(
        client.command(&["SISMEMBER", "dest", "b"]).await,
        Response::Integer(1),
        "pre-existing destination member must be unaffected"
    );
    assert_eq!(
        client.command(&["EXISTS", "src"]).await,
        Response::Integer(0)
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

/// PFADD `count` distinct elements to `key` (prefix-namespaced) in batches of
/// 500, forcing the HLL toward dense encoding via the client.
async fn pfadd_distinct(
    client: &mut crate::common::test_server::TestClient,
    key: &str,
    prefix: &str,
    count: usize,
) {
    let mut i = 0;
    while i < count {
        let end = (i + 500).min(count);
        let mut owned: Vec<String> = vec!["PFADD".to_string(), key.to_string()];
        for j in i..end {
            owned.push(format!("{prefix}:{j}"));
        }
        let args: Vec<&str> = owned.iter().map(String::as_str).collect();
        client.command(&args).await;
        i = end;
    }
}

// ============================================================================
// Lifecycle: DEL clears a delta-persisted HLL, then a re-add re-bases it.
//
// A dense HLL accumulates a chain of merge operands on disk. `DEL` must write a
// tombstone that clears that whole chain; a subsequent PFADD recreates the key
// as a fresh full value. After a restart the recovered cardinality must reflect
// ONLY the post-delete add (== 1), never the stale operand chain.
// ============================================================================

#[tokio::test]
async fn test_hll_delete_then_readd_clears_delta_chain() {
    let tmp = tempfile::tempdir().unwrap();

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    // Build a dense HLL, then a dense PFADD to lay down a merge operand.
    pfadd_distinct(&mut client, "hll", "e", 4000).await;
    assert_eq!(
        client.command(&["PFDEBUG", "ENCODING", "hll"]).await,
        Response::Bulk(Some(Bytes::from("dense"))),
        "4000 distinct elements must promote the HLL to dense encoding"
    );
    assert_eq!(
        client.command(&["PFADD", "hll", "x", "y", "z"]).await,
        Response::Integer(1),
        "dense PFADD persists as a merge operand (delta chain)"
    );

    // Delete: the tombstone must clear the operand chain on disk.
    assert_eq!(client.command(&["DEL", "hll"]).await, Response::Integer(1));

    // Re-add a single fresh element: recreates the key as a full Put.
    assert_eq!(
        client.command(&["PFADD", "hll", "a"]).await,
        Response::Integer(1)
    );
    assert_eq!(
        unwrap_integer(&client.command(&["PFCOUNT", "hll"]).await),
        1
    );

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Restart: the recovered value must be the re-based single element only. A
    // leaked operand chain (missing DEL tombstone) would recover ~4000 here.
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;
    assert_eq!(
        unwrap_integer(&client.command(&["PFCOUNT", "hll"]).await),
        1,
        "DEL must clear the delta chain; only the post-delete add survives"
    );

    server.shutdown().await;
}

// ============================================================================
// Lifecycle: PFMERGE over a delta-persisted source survives a restart.
//
// A dense source `src` carries pending merge operands (a dense PFADD delta).
// `PFMERGE dst src` must fold the source's full cardinality — deltas included —
// into `dst`, which then persists and recovers within the 5% tolerance the tcl
// suite uses for HLL estimates.
// ============================================================================

#[tokio::test]
async fn test_hll_pfmerge_delta_source_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    // Dense `src` with 4000 distinct elements ...
    pfadd_distinct(&mut client, "src", "e", 4000).await;
    assert_eq!(
        client.command(&["PFDEBUG", "ENCODING", "src"]).await,
        Response::Bulk(Some(Bytes::from("dense"))),
        "src must be dense so the next PFADD takes the delta path"
    );
    // ... plus 20 more via the dense delta path (pending merge operands).
    let mut owned: Vec<String> = vec!["PFADD".to_string(), "src".to_string()];
    for j in 0..20 {
        owned.push(format!("d:{j}"));
    }
    let args: Vec<&str> = owned.iter().map(String::as_str).collect();
    client.command(&args).await;

    // True distinct cardinality inserted into `src`.
    let expected = 4020.0_f64;

    // Fold the delta-persisted source into a fresh destination.
    client.command(&["PFMERGE", "dst", "src"]).await;
    let before = unwrap_integer(&client.command(&["PFCOUNT", "dst"]).await);

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;
    let after = unwrap_integer(&client.command(&["PFCOUNT", "dst"]).await);

    // Recovery is lossless: the estimate is unchanged across the restart ...
    assert_eq!(
        after, before,
        "PFCOUNT dst must be identical before and after the restart"
    );
    // ... and stays within the tcl 5% tolerance of the true cardinality, which
    // proves PFMERGE captured the source's pending delta, not just its base.
    let err = (after as f64 - expected).abs() / expected;
    assert!(
        err < 0.05,
        "recovered PFCOUNT dst ({after}) must be within 5% of {expected}, got {:.3}",
        err
    );

    server.shutdown().await;
}

// ============================================================================
// Lifecycle: a sparse->dense promotion survives a restart exactly.
//
// PFADD batches are added until the HLL crosses the 3000-register promotion
// threshold (encoding flips to `dense`). Stopping right after the crossing and
// restarting must recover the identical cardinality — promotion is persisted as
// a full value, so recovery is exact, not merely within tolerance.
// ============================================================================

#[tokio::test]
async fn test_hll_sparse_to_dense_promotion_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    // Add batches until the encoding flips to dense (crossing 3000 registers),
    // then stop on the batch that crossed it.
    let mut added = 0usize;
    loop {
        pfadd_distinct(&mut client, "promo", &format!("b{added}"), 500).await;
        added += 500;
        let encoding = client.command(&["PFDEBUG", "ENCODING", "promo"]).await;
        if encoding == Response::Bulk(Some(Bytes::from("dense"))) {
            break;
        }
        assert!(
            added < 20_000,
            "HLL should have promoted to dense well before {added} elements"
        );
    }

    let before = unwrap_integer(&client.command(&["PFCOUNT", "promo"]).await);

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    // A freshly-promoted dense value is persisted as a full Put, so the recovered
    // cardinality must match the pre-restart value exactly.
    assert_eq!(
        unwrap_integer(&client.command(&["PFCOUNT", "promo"]).await),
        before,
        "sparse->dense promotion must recover the exact pre-restart cardinality"
    );
    // And it must still be dense after recovery.
    assert_eq!(
        client.command(&["PFDEBUG", "ENCODING", "promo"]).await,
        Response::Bulk(Some(Bytes::from("dense"))),
        "the recovered HLL must remain dense"
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

/// BITOP with an empty result deletes the destination (Redis parity,
/// bitops.c bitopCommand: `dbDelete` on `maxlen == 0`). That deletion must
/// reach the WAL — a persist-if-exists (`WalStrategy::PersistDestination`)
/// strategy would leave the stale prior value authoritative on disk,
/// resurrecting the destination on restart. BITOP's `WalStrategy::Dynamic`
/// (persist-or-delete over its declared write-access destination) writes the
/// removal so the destination stays absent after a reboot.
#[tokio::test]
async fn test_bitop_empty_result_deletion_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();

    // --- First boot: seed a destination, then empty it via BITOP on absent
    // sources so the computed result is zero-length. ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "edest", "preexisting"]).await);
    // Both sources are missing -> AND yields an empty result -> dest deleted.
    let len = unwrap_integer(
        &client
            .command(&["BITOP", "AND", "edest", "emiss1", "emiss2"])
            .await,
    );
    assert_eq!(len, 0);
    assert_eq!(
        client.command(&["EXISTS", "edest"]).await,
        Response::Integer(0),
        "empty-result BITOP must delete the pre-existing destination"
    );

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Second boot: the destination must NOT be resurrected. ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_eq!(
        client.command(&["EXISTS", "edest"]).await,
        Response::Integer(0),
        "BITOP empty-result deletion must survive a restart"
    );

    server.shutdown().await;
}

/// XGROUP CREATE must persist the target stream key (args[1] after the
/// subcommand) so the consumer group exists after a restart. The historical
/// defect persisted the wrong argument (e.g. the CREATE subcommand token).
///
/// Stream serialization persists consumer groups (proposal 45), so the group
/// survives a restart.
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
/// Consumer-group state (including the PEL) is serialized (proposal 45), so the
/// pending entry survives a restart.
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

/// Extract `(id, consumer, idle_ms, delivery_count)` rows from a detailed
/// (`start end count`) XPENDING reply.
fn pending_details(response: &Response) -> Vec<(Bytes, Bytes, i64, i64)> {
    let rows = match response {
        Response::Array(items) => items,
        _ => panic!("expected array from detailed XPENDING, got {response:?}"),
    };
    rows.iter()
        .map(|row| {
            let fields = match row {
                Response::Array(f) => f,
                _ => panic!("expected per-entry array, got {row:?}"),
            };
            (
                Bytes::copy_from_slice(unwrap_bulk(&fields[0])),
                Bytes::copy_from_slice(unwrap_bulk(&fields[1])),
                unwrap_integer(&fields[2]),
                unwrap_integer(&fields[3]),
            )
        })
        .collect()
}

/// Full consumer-group durability flow (proposal 45): XADD two entries,
/// XREADGROUP pends both, XACK one, restart. After the restart XPENDING must
/// show exactly the one unacked entry with its delivery count intact, XCLAIM
/// must still work on it, and XINFO GROUPS must report the pre-restart
/// last-delivered-id.
#[tokio::test]
async fn test_consumer_group_full_state_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();

    // --- First boot: build group state ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    let _ = client
        .command(&["XADD", "xf:stream", "1-1", "f", "a"])
        .await;
    let _ = client
        .command(&["XADD", "xf:stream", "2-1", "f", "b"])
        .await;
    assert_ok(
        &client
            .command(&["XGROUP", "CREATE", "xf:stream", "xf:group", "0"])
            .await,
    );
    let delivered = unwrap_array(
        client
            .command(&[
                "XREADGROUP",
                "GROUP",
                "xf:group",
                "xf:consumer",
                "STREAMS",
                "xf:stream",
                ">",
            ])
            .await,
    );
    assert!(!delivered.is_empty(), "XREADGROUP should deliver entries");
    // Ack the first entry; 2-1 stays pending.
    assert_eq!(
        client
            .command(&["XACK", "xf:stream", "xf:group", "1-1"])
            .await,
        Response::Integer(1)
    );

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Second boot: PEL, delivery count, XCLAIM, and last-delivered-id ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    let details = pending_details(
        &client
            .command(&["XPENDING", "xf:stream", "xf:group", "-", "+", "10"])
            .await,
    );
    assert_eq!(
        details.len(),
        1,
        "exactly the unacked entry must survive, got: {details:?}"
    );
    let (id, consumer, _idle, delivery_count) = &details[0];
    assert_eq!(id.as_ref(), b"2-1");
    assert_eq!(consumer.as_ref(), b"xf:consumer");
    assert_eq!(
        *delivery_count, 1,
        "delivery count must survive the restart"
    );

    // XCLAIM still works on the restored PEL entry.
    let claimed = unwrap_array(
        client
            .command(&["XCLAIM", "xf:stream", "xf:group", "xf:other", "0", "2-1"])
            .await,
    );
    assert_eq!(claimed.len(), 1, "XCLAIM must reassign the restored entry");

    // XINFO GROUPS reports the pre-restart last-delivered-id.
    let groups = unwrap_array(client.command(&["XINFO", "GROUPS", "xf:stream"]).await);
    assert!(
        groups.iter().any(|g| {
            let fields = match g {
                Response::Array(arr) => arr,
                _ => return false,
            };
            fields.windows(2).any(|w| {
                w[0] == Response::bulk(Bytes::from_static(b"last-delivered-id"))
                    && w[1] == Response::bulk(Bytes::from_static(b"2-1"))
            })
        }),
        "last-delivered-id must survive a restart, got: {groups:?}"
    );

    server.shutdown().await;
}

/// Idle-time continuity (proposal 45): a pending entry's idle time must resume
/// from its persisted wall-clock delivery time after a restart — never reset to
/// zero. Uses the ClaimClock unix-ms mapping in the stream codec.
#[tokio::test]
async fn test_pending_idle_time_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();

    // --- First boot: pend one entry, let it idle ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    let _ = client
        .command(&["XADD", "xi:stream", "1-1", "f", "v"])
        .await;
    assert_ok(
        &client
            .command(&["XGROUP", "CREATE", "xi:stream", "xi:group", "0"])
            .await,
    );
    let _ = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "xi:group",
            "xi:consumer",
            "STREAMS",
            "xi:stream",
            ">",
        ])
        .await;

    // Accumulate observable idle before the snapshot.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let details = pending_details(
        &client
            .command(&["XPENDING", "xi:stream", "xi:group", "-", "+", "10"])
            .await,
    );
    let idle_before = details[0].2;
    assert!(
        idle_before >= 300,
        "test setup: idle_before = {idle_before}"
    );

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Second boot: idle continues from the persisted delivery time ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    let details = pending_details(
        &client
            .command(&["XPENDING", "xi:stream", "xi:group", "-", "+", "10"])
            .await,
    );
    assert_eq!(details.len(), 1, "entry must still be pending: {details:?}");
    let idle_after = details[0].2;
    assert!(
        idle_after >= idle_before,
        "idle must resume across a restart (never reset): before={idle_before}ms after={idle_after}ms"
    );

    server.shutdown().await;
}

// ============================================================================
// FLUSHDB / FLUSHALL persistence clearing (proposal 43)
//
// FLUSHDB/FLUSHALL must clear the *persisted* keyspace, not merely the
// in-memory store. Before proposal 43 the flush emitted zero WAL actions, so a
// flush followed by a restart silently resurrected the entire pre-flush
// keyspace from RocksDB.
// ============================================================================

/// Test 1 — Resurrection regression (the bug): a persisted String + dense HLL
/// must be gone after FLUSHDB + restart.
#[tokio::test]
async fn test_flushdb_clears_persisted_data_across_restart() {
    let tmp = tempfile::tempdir().unwrap();

    // --- First boot: persist a String (Put) and a dense HLL (Put + Merge) ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "str", "value"]).await);
    // Build a dense HLL and add via the delta-merge path so both a Put and a
    // Merge operand are on disk for the same key.
    pfadd_distinct(&mut client, "hll", "e", 4000).await;
    assert_eq!(
        client.command(&["PFDEBUG", "ENCODING", "hll"]).await,
        Response::Bulk(Some(Bytes::from("dense"))),
        "4000 distinct elements must promote the HLL to dense encoding"
    );
    assert_eq!(
        client.command(&["PFADD", "hll", "x", "y", "z"]).await,
        Response::Integer(1),
        "dense PFADD persists as a merge operand"
    );
    assert!(unwrap_integer(&client.command(&["DBSIZE"]).await) >= 2);

    // FLUSHDB clears everything.
    assert_ok(&client.command(&["FLUSHDB"]).await);
    assert_eq!(unwrap_integer(&client.command(&["DBSIZE"]).await), 0);

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Second boot: nothing may resurrect ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_eq!(
        unwrap_integer(&client.command(&["DBSIZE"]).await),
        0,
        "FLUSHDB must clear persisted keys; none may resurrect after restart"
    );
    assert_eq!(
        client.command(&["GET", "str"]).await,
        Response::Bulk(None),
        "the flushed String key must not resurrect"
    );
    assert_eq!(
        unwrap_integer(&client.command(&["PFCOUNT", "hll"]).await),
        0,
        "the flushed HLL (base + merge operands) must not resurrect"
    );

    server.shutdown().await;
}

/// Test 2 — Post-flush writes survive: the range tombstone is seq-ordered with
/// surrounding writes, so a SET after FLUSHDB outlives it.
#[tokio::test]
async fn test_flushdb_preserves_post_flush_writes_across_restart() {
    let tmp = tempfile::tempdir().unwrap();

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "before", "old"]).await);
    assert_ok(&client.command(&["FLUSHDB"]).await);
    assert_ok(&client.command(&["SET", "after", "new"]).await);

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_eq!(
        client.command(&["GET", "before"]).await,
        Response::Bulk(None),
        "the pre-flush key must stay cleared"
    );
    assert_eq!(
        client.command(&["GET", "after"]).await,
        Response::Bulk(Some(Bytes::from("new"))),
        "a write accepted after FLUSHDB must survive the restart"
    );
    assert_eq!(unwrap_integer(&client.command(&["DBSIZE"]).await), 1);

    server.shutdown().await;
}

/// Test 4 — FLUSHALL parity: FLUSHALL clears persisted data exactly like
/// FLUSHDB (single logical DB; both take the ClearShard path).
#[tokio::test]
async fn test_flushall_clears_persisted_data_across_restart() {
    let tmp = tempfile::tempdir().unwrap();

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "str", "value"]).await);
    pfadd_distinct(&mut client, "hll", "e", 4000).await;
    assert_eq!(
        client.command(&["PFADD", "hll", "x", "y", "z"]).await,
        Response::Integer(1)
    );

    assert_ok(&client.command(&["FLUSHALL"]).await);
    assert_eq!(unwrap_integer(&client.command(&["DBSIZE"]).await), 0);

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_eq!(
        unwrap_integer(&client.command(&["DBSIZE"]).await),
        0,
        "FLUSHALL must clear persisted keys across a restart"
    );
    assert_eq!(client.command(&["GET", "str"]).await, Response::Bulk(None));
    assert_eq!(
        unwrap_integer(&client.command(&["PFCOUNT", "hll"]).await),
        0
    );

    server.shutdown().await;
}

/// Test 5 — Merge-operand interaction across a clear: an outstanding dense-PFADD
/// delta chain must not fold into a same-key PFADD issued after FLUSHDB. The
/// range tombstone clears the base and every operand, and recovery must reflect
/// only the post-flush add. Guards proposal 42's delta-base invariant across a
/// clear.
#[tokio::test]
async fn test_flushdb_clears_hll_delta_chain_before_readd() {
    let tmp = tempfile::tempdir().unwrap();

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    // Dense HLL (~4000) plus a dense PFADD → a merge operand chain on disk.
    pfadd_distinct(&mut client, "hll", "e", 4000).await;
    assert_eq!(
        client.command(&["PFDEBUG", "ENCODING", "hll"]).await,
        Response::Bulk(Some(Bytes::from("dense"))),
        "4000 distinct elements must promote the HLL to dense encoding"
    );
    assert_eq!(
        client.command(&["PFADD", "hll", "x", "y", "z"]).await,
        Response::Integer(1),
        "dense PFADD lays down a merge operand"
    );

    // Clear the whole keyspace: base + operands must go.
    assert_ok(&client.command(&["FLUSHDB"]).await);

    // Re-add a single element to the SAME key: recreates it as a fresh full Put.
    assert_eq!(
        client.command(&["PFADD", "hll", "solo"]).await,
        Response::Integer(1)
    );
    assert_eq!(
        unwrap_integer(&client.command(&["PFCOUNT", "hll"]).await),
        1
    );

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Restart: a leaked pre-flush base or operand chain would recover ~4000.
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;
    assert_eq!(
        unwrap_integer(&client.command(&["PFCOUNT", "hll"]).await),
        1,
        "FLUSHDB must clear the delta chain; only the post-flush add survives"
    );

    server.shutdown().await;
}

// ============================================================================
// BGSAVE snapshot artifact -> fresh-directory restore (end-to-end)
//
// Unlike `test_bgsave_snapshot_survives_restart` (which reopens the *same live
// directory* and so only exercises ordinary WAL replay), these tests recover a
// server from the *actual `snapshot_NNNNN` artifact* a `BGSAVE` produces, using
// the documented operator restore procedure: stage the snapshot's `checkpoint/`
// contents as a `checkpoint_ready` sibling of a fresh data dir and let normal
// startup recovery (`RocksStore::load_staged_checkpoint`) install it before the
// DB is opened. See `website/src/content/docs/operations/backup-restore.md`
// (`## Restore`).
// ============================================================================

/// Copy a directory tree (used to stage a snapshot's `checkpoint/` contents into
/// a `checkpoint_ready` directory, mirroring the doc's `cp -a checkpoint/. dst/`).
fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let target = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_recursive(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), &target)?;
        }
    }
    Ok(())
}

/// Poll `snapshot_dir` until a fully-promoted `snapshot_NNNNN` artifact appears,
/// returning the path to its `checkpoint/` subdirectory (the RocksDB database).
///
/// "Fully promoted" = a `snapshot_NNNNN` directory (not the `.snapshot_*.tmp`
/// staging dir) carrying both `metadata.json` and `checkpoint/CURRENT`. Picks the
/// highest epoch, matching the `latest` symlink the stager repoints. Panics after
/// the timeout so a broken/absent BGSAVE fails the test loudly.
async fn wait_for_snapshot_checkpoint(snapshot_dir: &std::path::Path) -> std::path::PathBuf {
    for _ in 0..100 {
        let mut best: Option<(u64, std::path::PathBuf)> = None;
        if let Ok(entries) = std::fs::read_dir(snapshot_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                let Some(epoch) = name
                    .strip_prefix("snapshot_")
                    .and_then(|s| s.parse::<u64>().ok())
                else {
                    continue; // skip `.snapshot_*.tmp`, `latest`, etc.
                };
                let checkpoint = entry.path().join("checkpoint");
                let complete = entry.path().join("metadata.json").is_file()
                    && checkpoint.join("CURRENT").is_file();
                if complete && best.as_ref().is_none_or(|(e, _)| epoch > *e) {
                    best = Some((epoch, checkpoint));
                }
            }
        }
        if let Some((_, checkpoint)) = best {
            return checkpoint;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!(
        "BGSAVE snapshot artifact never appeared under {}",
        snapshot_dir.display()
    );
}

/// End-to-end: populate a server, `BGSAVE`, then restore the produced snapshot
/// artifact into a *fresh* data directory via the documented operator procedure
/// and assert every key/type survives — while a key written *after* the snapshot
/// (living only in the original dir's WAL) is correctly absent from the
/// point-in-time image.
#[tokio::test]
async fn test_bgsave_snapshot_restores_into_fresh_data_dir() {
    // Owned dirs: the snapshot dir must outlive the source server so the artifact
    // is still on disk to copy from (the harness's auto snapshot dir is dropped
    // when the server drops).
    let src_root = tempfile::tempdir().unwrap();
    let snapshot_root = tempfile::tempdir().unwrap();
    let src_data = src_root.path().join("data");

    let src_config = TestServerConfig {
        persistence: true,
        data_dir: Some(src_data.clone()),
        snapshot_dir: Some(snapshot_root.path().to_path_buf()),
        num_shards: Some(1),
        // Disable the periodic snapshot task so the only artifact is the manual
        // BGSAVE below. Otherwise its fire-immediately first tick writes an empty
        // startup snapshot that races (and can be mistaken for) ours.
        snapshot_interval_secs: Some(0),
        ..Default::default()
    };

    // --- Source server: write a spread of types, then snapshot ---
    let server = TestServer::start_standalone_with_config(src_config).await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "str_key", "str_val"]).await);
    assert_ok(
        &client
            .command(&["SET", "ttl_key", "ttl_val", "EX", "3600"])
            .await,
    );
    assert_eq!(
        client.command(&["RPUSH", "list_key", "a", "b", "c"]).await,
        Response::Integer(3)
    );
    assert_eq!(
        client
            .command(&["HSET", "hash_key", "f1", "v1", "f2", "v2"])
            .await,
        Response::Integer(2)
    );
    assert_eq!(
        client.command(&["SADD", "set_key", "m1", "m2", "m3"]).await,
        Response::Integer(3)
    );
    assert_eq!(
        client
            .command(&["ZADD", "zset_key", "1", "one", "2", "two"])
            .await,
        Response::Integer(2)
    );

    let bgsave_resp = client.command(&["BGSAVE"]).await;
    assert!(
        matches!(&bgsave_resp, Response::Simple(msg)
            if String::from_utf8_lossy(msg).contains("Background saving started")),
        "Unexpected BGSAVE response: {bgsave_resp:?}"
    );

    // Locate the produced artifact (proves the snapshot was actually written).
    let checkpoint = wait_for_snapshot_checkpoint(snapshot_root.path()).await;

    // Write a key AFTER the snapshot cut. It lives only in the source dir's WAL,
    // never in the point-in-time checkpoint, so a fresh-dir restore must not see
    // it — this is what makes the test exercise the artifact, not the live dir.
    assert_ok(
        &client
            .command(&["SET", "post_snapshot_key", "post_val"])
            .await,
    );

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Restore into a FRESH data dir via the documented procedure ---
    // Stage the checkpoint's contents as `checkpoint_ready`, a sibling of the new
    // (not-yet-existing) data dir; startup recovery installs it before opening.
    let restore_root = tempfile::tempdir().unwrap();
    let restore_data = restore_root.path().join("data");
    let checkpoint_ready = restore_root.path().join("checkpoint_ready");
    copy_dir_recursive(&checkpoint, &checkpoint_ready).unwrap();
    assert!(
        !restore_data.exists(),
        "restore target must start as a fresh, empty data directory"
    );

    let restore_config = TestServerConfig {
        persistence: true,
        data_dir: Some(restore_data.clone()),
        num_shards: Some(1),
        ..Default::default()
    };
    let server = TestServer::start_standalone_with_config(restore_config).await;
    let mut client = server.connect().await;

    // The restore installed the checkpoint (consuming `checkpoint_ready`) rather
    // than booting an empty fresh dir.
    assert!(
        !checkpoint_ready.exists(),
        "startup recovery must consume checkpoint_ready by installing it"
    );

    // Every type from the snapshot must be intact.
    assert_eq!(
        client.command(&["GET", "str_key"]).await,
        Response::Bulk(Some(Bytes::from("str_val")))
    );
    let ttl = unwrap_integer(&client.command(&["TTL", "ttl_key"]).await);
    assert!(
        ttl > 0 && ttl <= 3600,
        "TTL should survive restore, got {ttl}"
    );
    let list = unwrap_array(client.command(&["LRANGE", "list_key", "0", "-1"]).await);
    let vals: Vec<&[u8]> = list.iter().map(unwrap_bulk).collect();
    assert_eq!(vals, vec![b"a", b"b", b"c"]);
    assert_eq!(
        client.command(&["HGET", "hash_key", "f2"]).await,
        Response::Bulk(Some(Bytes::from("v2")))
    );
    let mut members: Vec<String> = unwrap_array(client.command(&["SMEMBERS", "set_key"]).await)
        .iter()
        .map(|r| String::from_utf8(unwrap_bulk(r).to_vec()).unwrap())
        .collect();
    members.sort();
    assert_eq!(members, vec!["m1", "m2", "m3"]);
    assert_eq!(
        client.command(&["ZSCORE", "zset_key", "two"]).await,
        Response::Bulk(Some(Bytes::from("2")))
    );

    // The post-snapshot key lived only in the source WAL — a point-in-time
    // restore must not resurrect it.
    assert_eq!(
        client.command(&["GET", "post_snapshot_key"]).await,
        Response::Bulk(None),
        "a key written after the snapshot cut must not appear in a fresh-dir restore"
    );

    server.shutdown().await;
}

/// A server restored from a BGSAVE artifact must behave like any live server:
/// subsequent writes are WAL-durable and replay on a later restart, layered on
/// top of the installed checkpoint.
#[tokio::test]
async fn test_restored_snapshot_accepts_and_replays_new_writes() {
    let src_root = tempfile::tempdir().unwrap();
    let snapshot_root = tempfile::tempdir().unwrap();
    let src_data = src_root.path().join("data");

    let src_config = TestServerConfig {
        persistence: true,
        data_dir: Some(src_data.clone()),
        snapshot_dir: Some(snapshot_root.path().to_path_buf()),
        num_shards: Some(1),
        // See the sibling test: disable periodic snapshots for a deterministic
        // single BGSAVE artifact.
        snapshot_interval_secs: Some(0),
        ..Default::default()
    };

    let server = TestServer::start_standalone_with_config(src_config).await;
    let mut client = server.connect().await;
    assert_ok(&client.command(&["SET", "base_key", "base_val"]).await);
    let bgsave_resp = client.command(&["BGSAVE"]).await;
    assert!(
        matches!(&bgsave_resp, Response::Simple(msg)
            if String::from_utf8_lossy(msg).contains("Background saving started")),
        "Unexpected BGSAVE response: {bgsave_resp:?}"
    );
    let checkpoint = wait_for_snapshot_checkpoint(snapshot_root.path()).await;
    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Install the artifact into a fresh dir.
    let restore_root = tempfile::tempdir().unwrap();
    let restore_data = restore_root.path().join("data");
    let checkpoint_ready = restore_root.path().join("checkpoint_ready");
    copy_dir_recursive(&checkpoint, &checkpoint_ready).unwrap();

    let restore_config = |data: &std::path::Path| TestServerConfig {
        persistence: true,
        data_dir: Some(data.to_path_buf()),
        num_shards: Some(1),
        ..Default::default()
    };

    // Boot from the restored checkpoint, then write a NEW key.
    let server = TestServer::start_standalone_with_config(restore_config(&restore_data)).await;
    let mut client = server.connect().await;
    assert_eq!(
        client.command(&["GET", "base_key"]).await,
        Response::Bulk(Some(Bytes::from("base_val")))
    );
    assert_ok(
        &client
            .command(&["SET", "after_restore_key", "after_val"])
            .await,
    );
    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Plain reopen of the restored dir: the checkpoint's data AND the write made
    // against the restored server must both replay from the WAL.
    let server = TestServer::start_standalone_with_config(restore_config(&restore_data)).await;
    let mut client = server.connect().await;
    assert_eq!(
        client.command(&["GET", "base_key"]).await,
        Response::Bulk(Some(Bytes::from("base_val"))),
        "checkpoint data must survive a restart of the restored server"
    );
    assert_eq!(
        client.command(&["GET", "after_restore_key"]).await,
        Response::Bulk(Some(Bytes::from("after_val"))),
        "writes to a restored server must be WAL-durable and replay on restart"
    );

    server.shutdown().await;
}

// ============================================================================
// BGSAVE checkpoint cross-shard consistent-cut contract (issue 43)
//
// A BGSAVE checkpoint is a single RocksDB `Checkpoint` over one shared DB, so it
// captures an atomic point-in-time image at one RocksDB sequence number across
// every shard's column family. The pre-snapshot hook first drains every shard's
// WAL flush engine into RocksDB (issue 13's `FlushWal` fan-out), so the cut
// captures a prefix of each shard's committed history rather than a lossy
// mid-flush state.
//
// The concern (audit D#5): per-shard writes go through *separate* per-shard
// `WriteBatch`es and separate flush engines, so the cut could catch one shard's
// half of a cross-shard write and not another's — a torn cross-shard cut.
//
// The intended contract, re-verified against current `main`:
//
//   * SINGLE-SHARD atomicity IS preserved in the cut (strong guarantee). A
//     single-shard `MULTI`/`EXEC` is one shard event-loop message that enqueues
//     all of its WAL writes before any later message (including the pre-snapshot
//     `FlushWal`) is processed, and RocksDB commits in sequence order, so the
//     cut captures either all of a transaction's writes or none — never a torn
//     prefix. `test_checkpoint_preserves_single_shard_multi_atomicity_*` pins
//     this. A regression that tore a single-shard transaction across the cut
//     would be a real bug.
//
//   * CROSS-SHARD atomicity is NOT preserved in the cut (accepted limitation).
//     A cross-shard `MSET`/scatter dispatches independent per-shard writes, and
//     the pre-snapshot drain is likewise per-shard, so the cut can capture
//     shard A's half of a cross-shard write and not shard B's. This matches
//     FrogDB's documented cross-shard model — execution atomicity via locking,
//     but not failure/durability atomicity (see
//     `.scratch/concurrency-testing/issues/05-06`; the abort-on-recovery framing
//     that would close it is deferred issue 06, not built). What IS preserved,
//     and asserted, is *per-shard* atomicity: the subset of a cross-shard write
//     that lands on any one shard is applied as one shard event-loop message, so
//     that shard's subset is never itself torn.
// ============================================================================

/// Spawn `n` writer tasks that each own an independent client and loop until
/// `stop` is set, invoking `body` with a fresh monotonic generation each
/// iteration. Returns their join handles.
async fn spawn_writers<F, Fut>(
    server: &TestServer,
    n: usize,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    generation: std::sync::Arc<std::sync::atomic::AtomicU64>,
    body: F,
) -> Vec<tokio::task::JoinHandle<()>>
where
    F: Fn(crate::common::test_server::TestClient, u64) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = crate::common::test_server::TestClient> + Send,
{
    use std::sync::atomic::Ordering;
    let mut handles = Vec::with_capacity(n);
    for _ in 0..n {
        let mut client = server.connect().await;
        let stop = stop.clone();
        let generation = generation.clone();
        let body = body.clone();
        handles.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let g = generation.fetch_add(1, Ordering::SeqCst);
                client = body(client, g).await;
            }
        }));
    }
    handles
}

/// Pick `per_shard` distinct keys for each of `num_shards` internal shards, so
/// an `MSET` over the returned keys is a genuine cross-shard write.
fn keys_spanning_shards(num_shards: usize, per_shard: usize) -> Vec<String> {
    let mut by_shard: Vec<Vec<String>> = vec![Vec::new(); num_shards];
    let mut i = 0u64;
    while by_shard.iter().any(|v| v.len() < per_shard) {
        let key = format!("cs:{i}");
        let s = frogdb_core::shard_for_key(key.as_bytes(), num_shards);
        if by_shard[s].len() < per_shard {
            by_shard[s].push(key);
        }
        i += 1;
    }
    by_shard.into_iter().flatten().collect()
}

/// Fire `count` `BGSAVE`s spaced by `gap`, asserting every reply is one of the
/// accepted simple-string replies (never an error or a hang).
async fn hammer_bgsave(
    client: &mut crate::common::test_server::TestClient,
    count: usize,
    gap: Duration,
) {
    for _ in 0..count {
        let resp = client.command(&["BGSAVE"]).await;
        match &resp {
            Response::Simple(msg) => {
                let m = String::from_utf8_lossy(msg);
                assert!(
                    m.contains("Background saving started")
                        || m.contains("Background saving scheduled")
                        || m.contains("Background save already in progress"),
                    "unexpected BGSAVE simple reply: {m}"
                );
            }
            other => panic!("BGSAVE must reply with a simple string, got {other:?}"),
        }
        tokio::time::sleep(gap).await;
    }
}

/// Concurrent single-shard `MULTI`/`EXEC` transactions while `BGSAVE` fires
/// repeatedly. Each transaction rewrites every key of one hash-tag group to a
/// single generation value; the restored checkpoint must show, for every group,
/// that its keys are either all absent or all present and equal — a single-shard
/// transaction is never torn across the cut.
#[tokio::test]
async fn test_checkpoint_preserves_single_shard_multi_atomicity_under_concurrent_bgsave() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    const NUM_SHARDS: usize = 4;
    const NUM_TAGS: u64 = 12;
    const KEYS_PER_TXN: usize = 5;
    const NUM_WRITERS: usize = 4;

    let src_root = tempfile::tempdir().unwrap();
    let snapshot_root = tempfile::tempdir().unwrap();
    let src_data = src_root.path().join("data");

    let config = TestServerConfig {
        persistence: true,
        data_dir: Some(src_data.clone()),
        snapshot_dir: Some(snapshot_root.path().to_path_buf()),
        num_shards: Some(NUM_SHARDS),
        snapshot_interval_secs: Some(0),
        ..Default::default()
    };
    let server = TestServer::start_standalone_with_config(config).await;

    let stop = Arc::new(AtomicBool::new(false));
    let generation = Arc::new(AtomicU64::new(1));
    let writers = spawn_writers(
        &server,
        NUM_WRITERS,
        stop.clone(),
        generation.clone(),
        |mut client, g| async move {
            // All keys share one hash tag → one slot → one internal shard, so the
            // whole transaction is single-shard and must be atomic in any cut.
            let tag = format!("g{}", g % NUM_TAGS);
            let gv = g.to_string();
            let _ = client.command(&["MULTI"]).await;
            for i in 0..KEYS_PER_TXN {
                let key = format!("{{{tag}}}:k{i}");
                let _ = client.command(&["SET", &key, &gv]).await;
            }
            let _ = client.command(&["EXEC"]).await;
            client
        },
    )
    .await;

    // Fire BGSAVEs while writers stream transactions. wait_for_snapshot_checkpoint
    // afterward confirms at least one artifact was promoted during active writes.
    let mut driver = server.connect().await;
    hammer_bgsave(&mut driver, 20, Duration::from_millis(25)).await;
    let _ = wait_for_snapshot_checkpoint(snapshot_root.path()).await;
    hammer_bgsave(&mut driver, 10, Duration::from_millis(25)).await;

    stop.store(true, Ordering::Relaxed);
    for h in writers {
        h.await.unwrap();
    }
    drop(driver);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Restore the latest promoted checkpoint into a fresh dir.
    let checkpoint = wait_for_snapshot_checkpoint(snapshot_root.path()).await;
    let restore_root = tempfile::tempdir().unwrap();
    let restore_data = restore_root.path().join("data");
    let checkpoint_ready = restore_root.path().join("checkpoint_ready");
    copy_dir_recursive(&checkpoint, &checkpoint_ready).unwrap();

    let restore_config = TestServerConfig {
        persistence: true,
        data_dir: Some(restore_data.clone()),
        num_shards: Some(NUM_SHARDS),
        ..Default::default()
    };
    let server = TestServer::start_standalone_with_config(restore_config).await;
    let mut client = server.connect().await;

    // Every hash-tag group must be all-absent or all-present-with-one-value.
    let mut groups_present = 0;
    for tag_idx in 0..NUM_TAGS {
        let tag = format!("g{tag_idx}");
        let mut values: Vec<Option<String>> = Vec::with_capacity(KEYS_PER_TXN);
        for i in 0..KEYS_PER_TXN {
            let key = format!("{{{tag}}}:k{i}");
            let v = match client.command(&["GET", &key]).await {
                Response::Bulk(Some(b)) => Some(String::from_utf8(b.to_vec()).unwrap()),
                Response::Bulk(None) => None,
                other => panic!("unexpected GET reply for {key}: {other:?}"),
            };
            values.push(v);
        }
        let present = values.iter().filter(|v| v.is_some()).count();
        assert!(
            present == 0 || present == KEYS_PER_TXN,
            "single-shard transaction torn across BGSAVE cut for tag {tag}: {values:?}"
        );
        if present == KEYS_PER_TXN {
            groups_present += 1;
            let first = values[0].clone();
            assert!(
                values.iter().all(|v| *v == first),
                "single-shard transaction cut captured mixed generations for tag {tag}: {values:?}"
            );
        }
    }
    assert!(
        groups_present > 0,
        "checkpoint captured no transactions; test would be vacuous"
    );

    server.shutdown().await;
}

/// Concurrent cross-shard `MSET` while `BGSAVE` fires repeatedly. Each `MSET`
/// rewrites the whole cross-shard key set to a single generation. Asserts the
/// intended contract: per-shard atomicity holds (all present keys on any one
/// shard share a generation), while cross-shard atomicity is an accepted
/// limitation (different shards may show different generations — a torn cut).
#[tokio::test]
async fn test_checkpoint_cross_shard_mset_contract_under_concurrent_bgsave() {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    const NUM_SHARDS: usize = 4;
    const PER_SHARD: usize = 3;
    const NUM_WRITERS: usize = 4;

    let keys = keys_spanning_shards(NUM_SHARDS, PER_SHARD);
    // Sanity: the key set genuinely spans every shard.
    let mut spanned = std::collections::BTreeSet::new();
    for k in &keys {
        spanned.insert(frogdb_core::shard_for_key(k.as_bytes(), NUM_SHARDS));
    }
    assert_eq!(spanned.len(), NUM_SHARDS, "key set must span all shards");

    let src_root = tempfile::tempdir().unwrap();
    let snapshot_root = tempfile::tempdir().unwrap();
    let src_data = src_root.path().join("data");

    let config = TestServerConfig {
        persistence: true,
        data_dir: Some(src_data.clone()),
        snapshot_dir: Some(snapshot_root.path().to_path_buf()),
        num_shards: Some(NUM_SHARDS),
        snapshot_interval_secs: Some(0),
        allow_cross_slot_standalone: true,
        ..Default::default()
    };
    let server = TestServer::start_standalone_with_config(config).await;

    let stop = Arc::new(AtomicBool::new(false));
    let generation = Arc::new(AtomicU64::new(1));
    let keys_arc = Arc::new(keys.clone());
    let writers = {
        let keys_arc = keys_arc.clone();
        spawn_writers(
            &server,
            NUM_WRITERS,
            stop.clone(),
            generation.clone(),
            move |mut client, g| {
                let keys_arc = keys_arc.clone();
                async move {
                    let gv = g.to_string();
                    let mut args: Vec<&str> = Vec::with_capacity(1 + keys_arc.len() * 2);
                    args.push("MSET");
                    for k in keys_arc.iter() {
                        args.push(k);
                        args.push(&gv);
                    }
                    let _ = client.command(&args).await;
                    client
                }
            },
        )
        .await
    };

    let mut driver = server.connect().await;
    hammer_bgsave(&mut driver, 20, Duration::from_millis(25)).await;
    let _ = wait_for_snapshot_checkpoint(snapshot_root.path()).await;
    hammer_bgsave(&mut driver, 10, Duration::from_millis(25)).await;

    stop.store(true, Ordering::Relaxed);
    for h in writers {
        h.await.unwrap();
    }
    let max_issued = generation.load(Ordering::SeqCst);
    drop(driver);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let checkpoint = wait_for_snapshot_checkpoint(snapshot_root.path()).await;
    let restore_root = tempfile::tempdir().unwrap();
    let restore_data = restore_root.path().join("data");
    let checkpoint_ready = restore_root.path().join("checkpoint_ready");
    copy_dir_recursive(&checkpoint, &checkpoint_ready).unwrap();

    let restore_config = TestServerConfig {
        persistence: true,
        data_dir: Some(restore_data.clone()),
        num_shards: Some(NUM_SHARDS),
        allow_cross_slot_standalone: true,
        ..Default::default()
    };
    let server = TestServer::start_standalone_with_config(restore_config).await;
    let mut client = server.connect().await;

    // Collect the restored generation of each key, grouped by shard.
    let mut per_shard_gens: BTreeMap<usize, Vec<Option<u64>>> = BTreeMap::new();
    for k in &keys {
        let shard = frogdb_core::shard_for_key(k.as_bytes(), NUM_SHARDS);
        let v = match client.command(&["GET", k]).await {
            Response::Bulk(Some(b)) => {
                let s = String::from_utf8(b.to_vec()).unwrap();
                let g: u64 = s
                    .parse()
                    .unwrap_or_else(|_| panic!("torn/garbage value for {k}: {s:?}"));
                assert!(
                    g >= 1 && g < max_issued.max(2),
                    "restored value for {k} is not a generation actually issued: {g} (max {max_issued})"
                );
                Some(g)
            }
            Response::Bulk(None) => None,
            other => panic!("unexpected GET reply for {k}: {other:?}"),
        };
        per_shard_gens.entry(shard).or_default().push(v);
    }

    // Per-shard atomicity (asserted): every present key on a shard shares one generation.
    let mut observed_gens = std::collections::BTreeSet::new();
    let mut shards_with_data = 0;
    for (shard, gens) in &per_shard_gens {
        let present: Vec<u64> = gens.iter().filter_map(|g| *g).collect();
        if !present.is_empty() {
            shards_with_data += 1;
            let first = present[0];
            assert!(
                present.iter().all(|g| *g == first),
                "per-shard atomicity violated on shard {shard}: cross-shard MSET subset torn, gens {present:?}"
            );
            observed_gens.insert(first);
        }
    }
    assert!(
        shards_with_data > 0,
        "checkpoint captured no cross-shard writes; test would be vacuous"
    );

    // Cross-shard atomicity is NOT asserted: `observed_gens.len() > 1` means the
    // checkpoint took a torn cross-shard cut, which is an accepted limitation
    // (issues 05/06). Surface it for the record rather than failing on it.
    eprintln!(
        "cross-shard checkpoint cut observed {} distinct generation(s) across {} shard(s): {:?} \
         (>1 == accepted torn cross-shard cut)",
        observed_gens.len(),
        shards_with_data,
        observed_gens
    );

    server.shutdown().await;
}

/// Concurrent-BGSAVE stress: several clients spam `BGSAVE` while writers stream
/// updates. Every reply must be an accepted simple string (no error, no hang),
/// the server must stay responsive, and a baseline dataset written before the
/// storm must restore intact from the resulting checkpoint.
#[tokio::test]
async fn test_concurrent_bgsave_stress_restores_cleanly() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    const NUM_SHARDS: usize = 4;
    const NUM_BASELINE: usize = 50;
    const NUM_SAVERS: usize = 4;
    const NUM_WRITERS: usize = 3;

    let src_root = tempfile::tempdir().unwrap();
    let snapshot_root = tempfile::tempdir().unwrap();
    let src_data = src_root.path().join("data");

    let config = TestServerConfig {
        persistence: true,
        data_dir: Some(src_data.clone()),
        snapshot_dir: Some(snapshot_root.path().to_path_buf()),
        num_shards: Some(NUM_SHARDS),
        snapshot_interval_secs: Some(0),
        ..Default::default()
    };
    let server = TestServer::start_standalone_with_config(config).await;

    // Baseline dataset (present in every subsequent checkpoint).
    let mut setup = server.connect().await;
    for i in 0..NUM_BASELINE {
        assert_ok(
            &setup
                .command(&["SET", &format!("baseline:{i}"), &format!("v{i}")])
                .await,
        );
    }

    let stop = Arc::new(AtomicBool::new(false));
    let generation = Arc::new(AtomicU64::new(1));
    // Writers churn unrelated keys during the BGSAVE storm.
    let writers = spawn_writers(
        &server,
        NUM_WRITERS,
        stop.clone(),
        generation.clone(),
        |mut client, g| async move {
            let key = format!("churn:{}", g % 64);
            let _ = client.command(&["SET", &key, &g.to_string()]).await;
            client
        },
    )
    .await;

    // Overlapping BGSAVE spammers exercise the coordinator's already-running
    // coalescing under real concurrency.
    let mut savers = Vec::with_capacity(NUM_SAVERS);
    for _ in 0..NUM_SAVERS {
        let mut client = server.connect().await;
        savers.push(tokio::spawn(async move {
            hammer_bgsave(&mut client, 40, Duration::from_millis(5)).await;
        }));
    }
    for s in savers {
        s.await.unwrap();
    }

    stop.store(true, Ordering::Relaxed);
    for h in writers {
        h.await.unwrap();
    }

    // Server is still responsive after the storm.
    assert_eq!(
        setup.command(&["PING"]).await,
        Response::Simple(Bytes::from("PONG"))
    );
    // A final quiescent BGSAVE to guarantee a fresh, complete artifact.
    let _ = setup.command(&["BGSAVE"]).await;
    let _ = wait_for_snapshot_checkpoint(snapshot_root.path()).await;
    drop(setup);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The baseline dataset must restore intact from the checkpoint.
    let checkpoint = wait_for_snapshot_checkpoint(snapshot_root.path()).await;
    let restore_root = tempfile::tempdir().unwrap();
    let restore_data = restore_root.path().join("data");
    let checkpoint_ready = restore_root.path().join("checkpoint_ready");
    copy_dir_recursive(&checkpoint, &checkpoint_ready).unwrap();

    let restore_config = TestServerConfig {
        persistence: true,
        data_dir: Some(restore_data.clone()),
        num_shards: Some(NUM_SHARDS),
        ..Default::default()
    };
    let server = TestServer::start_standalone_with_config(restore_config).await;
    let mut client = server.connect().await;
    for i in 0..NUM_BASELINE {
        assert_eq!(
            client.command(&["GET", &format!("baseline:{i}")]).await,
            Response::Bulk(Some(Bytes::from(format!("v{i}")))),
            "baseline key {i} lost across concurrent-BGSAVE storm + restore"
        );
    }

    server.shutdown().await;
}

// ============================================================================
// Tiered storage: real spill (hot→warm under memory pressure) survives restart
// ============================================================================
//
// Gap closed (testing-gap issue 42, audit D#4): `recover_warm_shard_into` is
// unit-tested against a hand-built warm CF, but nothing drove a *genuine* spill
// — the hot tier overflowing to the RocksDB warm CF under real memory pressure —
// through a full server restart. These tests fill enough data to trip the
// `tiered-lru` spill trigger (asserting via `INFO tiered` that a real spill
// actually happened, not a synthetic fixture), restart the server, and assert
// every spilled key comes back with the correct value, type, and TTL.

/// Persistence + two-tier storage with a small `maxmemory` and the `tiered-lru`
/// policy, so cold values spill to the RocksDB warm tier under memory pressure.
///
/// `maxmemory` is generous relative to per-key metadata (which stays resident
/// even for spilled keys) but far below the total value volume the tests write,
/// so the store spills the bulk of values while never OOM-rejecting a write.
fn tiered_config(data_dir: &std::path::Path) -> TestServerConfig {
    TestServerConfig {
        persistence: true,
        data_dir: Some(data_dir.to_path_buf()),
        num_shards: Some(1),
        tiered_storage_enabled: true,
        maxmemory: Some(256 * 1024),
        maxmemory_policy: Some("tiered-lru".to_string()),
        ..Default::default()
    }
}

/// Value of an `INFO <section>` field parsed as `u64` (panics if absent).
fn info_field_u64(info: &str, name: &str) -> u64 {
    let prefix = format!("{name}:");
    info.lines()
        .map(|l| l.trim_end())
        .find_map(|l| l.strip_prefix(prefix.as_str()))
        .unwrap_or_else(|| panic!("field {name} missing from INFO:\n{info}"))
        .parse()
        .unwrap_or_else(|e| panic!("field {name} not numeric: {e:?}"))
}

/// A ~4 KiB value payload, distinct per index, big enough that a few hundred of
/// them dwarf the `maxmemory` limit and force the store to spill.
fn big_value(seed: &str, i: usize) -> String {
    format!("{seed}:{i}:{}", "x".repeat(4096))
}

/// Real spill of many keys (multiple types) → restart → every key recovers with
/// the correct value and type. Asserts `INFO tiered` proves a genuine spill
/// happened before the restart (`tiered_spills > 0`, `tiered_warm_keys > 0`), so
/// the warm CF holds real spilled artifacts — not a hand-constructed fixture.
#[tokio::test]
async fn test_tiered_spill_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    const NUM_STR: usize = 200;

    // --- First boot: write enough to force real spills ---
    let server = TestServer::start_standalone_with_config(tiered_config(tmp.path())).await;
    let mut client = server.connect().await;

    // Typed keys (written first, so LRU treats them as the coldest and spills
    // them ahead of the later filler). Each carries a distinct, verifiable shape.
    assert_ok(
        &client
            .command(&["SET", "str:typed", &big_value("typed", 0)])
            .await,
    );
    assert_eq!(
        client
            .command(&["RPUSH", "list:typed", "a", "b", "c", &big_value("l", 0)])
            .await,
        Response::Integer(4)
    );
    assert_eq!(
        client
            .command(&["HSET", "hash:typed", "f1", "v1", "f2", &big_value("h", 0)])
            .await,
        Response::Integer(2)
    );
    assert_eq!(
        client
            .command(&["SADD", "set:typed", "m1", "m2", &big_value("s", 0)])
            .await,
        Response::Integer(3)
    );
    assert_eq!(
        client
            .command(&["ZADD", "zset:typed", "1", "one", "2", &big_value("z", 0)])
            .await,
        Response::Integer(2)
    );

    // A typed key with a (long) TTL — its expiry must survive the spill+restart.
    assert_ok(
        &client
            .command(&["SET", "str:ttl", &big_value("ttl", 0)])
            .await,
    );
    assert_eq!(
        client.command(&["PEXPIRE", "str:ttl", "100000000"]).await,
        Response::Integer(1)
    );

    // Bulk string filler to drive the store well past `maxmemory`.
    for i in 0..NUM_STR {
        assert_ok(
            &client
                .command(&["SET", &format!("str:{i}"), &big_value("v", i)])
                .await,
        );
    }

    // Prove a real spill happened: the warm tier is non-empty and spills fired.
    let info = client.command(&["INFO", "tiered"]).await;
    let info = String::from_utf8(unwrap_bulk(&info).to_vec()).unwrap();
    let spills = info_field_u64(&info, "tiered_spills");
    let warm_keys = info_field_u64(&info, "tiered_warm_keys");
    assert!(
        spills > 0,
        "expected a genuine hot→warm spill under memory pressure, got tiered_spills=0:\n{info}"
    );
    assert!(
        warm_keys > 0,
        "expected keys resident in the warm tier, got tiered_warm_keys=0:\n{info}"
    );

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Second boot: every spilled key must recover intact ---
    let server = TestServer::start_standalone_with_config(tiered_config(tmp.path())).await;
    let mut client = server.connect().await;

    // Typed values + types survive (GET/reads unspill from the warm tier).
    assert_eq!(
        client.command(&["GET", "str:typed"]).await,
        Response::Bulk(Some(Bytes::from(big_value("typed", 0))))
    );
    assert_eq!(
        client.command(&["TYPE", "str:typed"]).await,
        Response::Simple("string".into())
    );

    let list = unwrap_array(client.command(&["LRANGE", "list:typed", "0", "-1"]).await);
    let vals: Vec<&[u8]> = list.iter().map(unwrap_bulk).collect();
    assert_eq!(
        vals,
        vec![b"a".as_ref(), b"b", b"c", big_value("l", 0).as_bytes()]
    );
    assert_eq!(
        client.command(&["TYPE", "list:typed"]).await,
        Response::Simple("list".into())
    );

    assert_eq!(
        client.command(&["HGET", "hash:typed", "f2"]).await,
        Response::Bulk(Some(Bytes::from(big_value("h", 0))))
    );
    assert_eq!(
        client.command(&["TYPE", "hash:typed"]).await,
        Response::Simple("hash".into())
    );

    assert_eq!(
        client
            .command(&["SISMEMBER", "set:typed", &big_value("s", 0)])
            .await,
        Response::Integer(1)
    );
    assert_eq!(
        client.command(&["TYPE", "set:typed"]).await,
        Response::Simple("set".into())
    );

    assert_eq!(
        client
            .command(&["ZSCORE", "zset:typed", &big_value("z", 0)])
            .await,
        Response::Bulk(Some(Bytes::from("2")))
    );
    assert_eq!(
        client.command(&["TYPE", "zset:typed"]).await,
        Response::Simple("zset".into())
    );

    // TTL survives the spill+restart: still present and still counting down.
    assert_eq!(
        client.command(&["GET", "str:ttl"]).await,
        Response::Bulk(Some(Bytes::from(big_value("ttl", 0))))
    );
    let pttl = unwrap_integer(&client.command(&["PTTL", "str:ttl"]).await);
    assert!(
        pttl > 0 && pttl <= 100_000_000,
        "TTL of a spilled key must survive restart, got PTTL={pttl}"
    );

    // Every filler string recovers with its exact value.
    for i in 0..NUM_STR {
        assert_eq!(
            client.command(&["GET", &format!("str:{i}")]).await,
            Response::Bulk(Some(Bytes::from(big_value("v", i)))),
            "spilled filler key str:{i} lost or corrupted across restart"
        );
    }

    // DBSIZE accounts for every key written (typed + ttl + filler), none lost.
    assert_eq!(
        unwrap_integer(&client.command(&["DBSIZE"]).await),
        (NUM_STR + 6) as i64
    );

    server.shutdown().await;
}

/// A spilled key whose TTL has already elapsed by restart time must NOT
/// resurrect: recovery filters it from both the warm CF and the hot CF. A
/// non-expiring survivor written alongside it must still come back.
#[tokio::test]
async fn test_tiered_spilled_key_past_ttl_does_not_resurrect() {
    let tmp = tempfile::tempdir().unwrap();
    const NUM_FILLER: usize = 200;

    let server = TestServer::start_standalone_with_config(tiered_config(tmp.path())).await;
    let mut client = server.connect().await;

    // A survivor with no TTL and a victim with a short TTL, both written early so
    // the later filler makes them the coldest candidates and spills them.
    assert_ok(
        &client
            .command(&["SET", "survivor", &big_value("keep", 0)])
            .await,
    );
    assert_ok(
        &client
            .command(&["SET", "victim", &big_value("doomed", 0)])
            .await,
    );
    assert_eq!(
        client.command(&["PEXPIRE", "victim", "2000"]).await,
        Response::Integer(1)
    );

    // Filler to force spills (victim + survivor land in the warm tier).
    for i in 0..NUM_FILLER {
        assert_ok(
            &client
                .command(&["SET", &format!("f:{i}"), &big_value("f", i)])
                .await,
        );
    }

    let info = client.command(&["INFO", "tiered"]).await;
    let info = String::from_utf8(unwrap_bulk(&info).to_vec()).unwrap();
    assert!(
        info_field_u64(&info, "tiered_warm_keys") > 0,
        "expected a genuine spill into the warm tier:\n{info}"
    );

    drop(client);
    server.shutdown().await;

    // Let the victim's TTL elapse while the server is down.
    tokio::time::sleep(Duration::from_millis(2200)).await;

    // --- Restart: the expired spilled key must be gone, survivor intact ---
    let server = TestServer::start_standalone_with_config(tiered_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_eq!(
        client.command(&["EXISTS", "victim"]).await,
        Response::Integer(0),
        "a spilled key past its TTL must not resurrect on restart"
    );
    assert_eq!(
        client.command(&["GET", "victim"]).await,
        Response::Bulk(None),
        "expired spilled key must read as nil after restart"
    );
    assert_eq!(
        client.command(&["GET", "survivor"]).await,
        Response::Bulk(Some(Bytes::from(big_value("keep", 0)))),
        "non-expiring spilled key must survive the restart"
    );

    server.shutdown().await;
}
