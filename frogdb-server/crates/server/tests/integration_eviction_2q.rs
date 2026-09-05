//! End-to-end eviction against the segmented keyspace backend (2Q over
//! segments), compiled only under `--features table-keyspace`.
//!
//! The rest of the maxmemory suite runs against whichever backend is compiled
//! in and is the real parity net. What this file adds is the two promises that
//! are *about* the swap rather than about a command.
//!
//! The first test is an end-to-end smoke on the feature build: it asserts the
//! behaviour any backend owes — pressure evicts, and the eviction is observable
//! as an `evicted` keyevent plus the counters — and would pass on griddle too.
//! Its value is that it exercises the whole segmented-table server, from the
//! socket to the victim walk, in one run.
//!
//! The second is the backend-specific one: `volatile-lru` against a keyspace of
//! persistent keys is exactly the case where `Table::cold_candidates` must walk
//! its queues, find nothing it may take, and say so — refusing writes with
//! `-OOM` without spinning and without going quiet on reads.

use crate::common::test_server::{TestClient, TestServer, TestServerConfig};
use bytes::Bytes;
use frogdb_protocol::Response;
use frogdb_telemetry::testing::get_counter;
use std::time::Duration;

/// Read `used_memory` from `INFO memory` over the RESP connection.
async fn used_memory(client: &mut TestClient) -> u64 {
    let body = match client.command(&["INFO", "memory"]).await {
        Response::Bulk(Some(data)) => String::from_utf8_lossy(&data).into_owned(),
        other => panic!("expected bulk reply for INFO memory, got: {other:?}"),
    };
    for line in body.lines() {
        if let Some(value) = line.strip_prefix("used_memory:") {
            return value.trim().parse().expect("used_memory must be a number");
        }
    }
    panic!("INFO memory carried no used_memory field:\n{body}");
}

/// A single-shard server: the whole `maxmemory` budget belongs to shard 0, so
/// the limit the test sets is exactly the limit the shard enforces.
async fn single_shard_server() -> TestServer {
    TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(1),
        ..Default::default()
    })
    .await
}

/// Under pressure the segmented backend nominates a victim from its own cold
/// ordering, and that eviction is observable exactly as a sampled one is: an
/// `evicted` keyevent naming the key, and the eviction counters.
#[tokio::test]
async fn pressure_evicts_from_the_segmented_keyspace_and_reports_it() {
    let server = single_shard_server().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHALL"]).await;
    assert_eq!(
        client
            .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
            .await,
        Response::ok()
    );
    assert!(matches!(
        subscriber.command(&["SUBSCRIBE", "__keyevent@0__:evicted"]).await,
        Response::Array(ref arr) if arr.len() == 3
    ));
    tokio::time::sleep(Duration::from_millis(50)).await;

    // A tight budget over current usage, and a policy that ranks by recency —
    // the one that asks the keyspace for its cold ordering.
    let limit = used_memory(&mut client).await + 100 * 1024;
    assert_eq!(
        client
            .command(&["CONFIG", "SET", "maxmemory", &limit.to_string()])
            .await,
        Response::ok()
    );
    assert_eq!(
        client
            .command(&["CONFIG", "SET", "maxmemory-policy", "allkeys-lru"])
            .await,
        Response::ok()
    );

    // Fill to the limit, then keep writing so the shard has to free space to
    // serve each new write.
    let payload = "x".repeat(512);
    let mut written = 0u32;
    loop {
        client
            .command(&["SET", &format!("ev:{written}"), &payload])
            .await;
        written += 1;
        if used_memory(&mut client).await + 4096 > limit {
            assert!(written > 10, "should fit >10 keys before nearing the limit");
            break;
        }
        assert!(written < 100_000, "never reached the memory limit");
    }
    for j in 0..written {
        client
            .command(&["SET", &format!("ev:more:{j}"), &payload])
            .await;
    }

    // The eviction is observable as an event...
    let msg = subscriber.read_message(Duration::from_secs(2)).await;
    match msg {
        Some(Response::Array(arr)) => {
            assert_eq!(arr.len(), 3);
            assert_eq!(
                arr[1],
                Response::Bulk(Some(Bytes::from("__keyevent@0__:evicted")))
            );
        }
        other => panic!("expected an `evicted` keyevent, got {other:?}"),
    }

    // ...and as the eviction counters, carrying the active policy.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let metrics = server.fetch_metrics().await;
    let keys = get_counter(
        &metrics,
        "frogdb_eviction_keys_total",
        &[("policy", "allkeys-lru")],
    );
    assert!(
        keys > 0.0,
        "expected frogdb_eviction_keys_total with policy=\"allkeys-lru\" to be nonzero, \
         got {keys}. /metrics:\n{metrics}"
    );
    let bytes = get_counter(&metrics, "frogdb_eviction_bytes_total", &[]);
    assert!(
        bytes > 0.0,
        "an eviction must report the bytes it freed, got {bytes}. /metrics:\n{metrics}"
    );

    server.shutdown().await;
}

/// A keyspace the policy may not take from: `volatile-lru` against persistent
/// keys. The shard must refuse writes with `-OOM` rather than reaching outside
/// the candidate set or spinning, and must go on answering reads.
#[tokio::test]
async fn an_unevictable_segmented_keyspace_refuses_writes_and_still_reads() {
    let server = single_shard_server().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHALL"]).await;

    // Fill with persistent (no-TTL) keys first, so the candidate set of a
    // volatile policy is empty by construction.
    let payload = "x".repeat(512);
    for i in 0..200 {
        client
            .command(&["SET", &format!("keep:{i}"), &payload])
            .await;
    }

    // Now pin the limit *below* what the shard already holds.
    let used = used_memory(&mut client).await;
    assert_eq!(
        client
            .command(&["CONFIG", "SET", "maxmemory", &(used / 2).to_string()])
            .await,
        Response::ok()
    );
    assert_eq!(
        client
            .command(&["CONFIG", "SET", "maxmemory-policy", "volatile-lru"])
            .await,
        Response::ok()
    );

    // Every write is refused, in bounded time, however many times we ask.
    for attempt in 0..5 {
        match client
            .command(&["SET", &format!("new:{attempt}"), &payload])
            .await
        {
            Response::Error(e) => {
                let text = String::from_utf8_lossy(&e).to_string();
                assert!(
                    text.contains("OOM"),
                    "expected an -OOM refusal, got: {text}"
                );
            }
            other => panic!("expected the write to be refused, got {other:?}"),
        }
    }

    // ...and reads are still served from the keys the policy could not take.
    assert_eq!(
        client.command(&["GET", "keep:0"]).await,
        Response::Bulk(Some(Bytes::from(payload.clone()))),
        "an unevictable shard must keep serving reads"
    );
    assert_eq!(
        client.command(&["DBSIZE"]).await,
        Response::Integer(200),
        "a persistent key is outside volatile-lru's candidate set"
    );

    server.shutdown().await;
}
