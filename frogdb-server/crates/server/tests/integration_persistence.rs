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
