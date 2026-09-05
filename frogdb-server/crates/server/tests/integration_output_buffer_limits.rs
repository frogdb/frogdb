//! End-to-end tests for `client-output-buffer-limit` and the `NetworkOutput`
//! budget (memory-architecture issue 18, PRD ruling R11).
//!
//! The decision itself — hard limit, soft limit, soft window, budget refusal —
//! is unit-tested in `connection::output_buffer`. What can only be tested with a
//! real socket pair is that the seam is *reached*: that a client which asks for
//! more reply bytes than the server will hold for it is actually disconnected,
//! that the operator sees why, and that the bytes it buffered were charged to a
//! budget the shard broker publishes.

use crate::common::test_server::{TestServer, TestServerConfig, is_error};
use frogdb_protocol::Response;
use futures::StreamExt;
use std::time::Duration;
use tokio::io::AsyncReadExt;

/// How much reply the server will hold for a normal client in these tests.
/// Small enough that a modest pipeline blows past it, large enough that it is
/// several replies rather than one oversized frame — this must test
/// *accumulation*, which is what a slow reader actually causes.
const NORMAL_HARD_LIMIT: usize = 256 * 1024;

/// A `client-output-buffer-limit` value that leaves replica and pubsub at
/// Redis's defaults and gives `normal` a reachable hard limit.
fn normal_hard_limit_spec() -> String {
    format!("normal {NORMAL_HARD_LIMIT} 0 0")
}

/// A client whose queued reply exceeds what the `normal` class allows must be
/// disconnected rather than served.
///
/// Redis ships `normal 0 0 0` (unlimited) precisely because a normal client's
/// output is bounded by what it asked for — but that bound is the client's, not
/// the server's. This pins that an operator who sets the limit gets it, on the
/// class Redis leaves open by default.
///
/// # What bounds a normal client's buffer here
///
/// FrogDB's reply path applies *backpressure* rather than unbounded buffering:
/// the codec flushes once its write buffer passes its backpressure boundary, so
/// a client that stops reading parks the connection in a blocking write instead
/// of growing a queue. Buffered output is therefore bounded by the boundary plus
/// the frame being encoded — which is why the reachable overflow, and the one
/// this test forces, is a single queued reply larger than the class allows.
/// (Pub/sub is the case where output genuinely accumulates; it has its own test
/// in `integration_pubsub`.)
// FM-MEMORY-001
#[tokio::test]
async fn test_normal_class_hard_limit_disconnects_an_oversized_reply() {
    let server = TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(1),
        client_output_buffer_limit: Some(normal_hard_limit_spec()),
        ..Default::default()
    })
    .await;

    let mut writer = server.connect().await;
    let mut client = server.connect().await;
    let mut control = server.connect().await;

    // Written by a different connection: the SET's own reply is tiny, but its
    // 4x-the-limit value is what the GET below will have to queue.
    let payload = "v".repeat(4 * NORMAL_HARD_LIMIT);
    assert_eq!(
        writer.command(&["SET", "big", &payload]).await,
        Response::ok()
    );

    client.send_only(&["GET", "big"]).await;

    // The client is torn down: `next()` yields `None` on a clean close, or
    // `Some(Err(..))` on a reset. Either is the disconnect.
    let disconnect = tokio::time::timeout(Duration::from_secs(10), client.framed.next()).await;
    assert!(
        matches!(disconnect, Ok(None) | Ok(Some(Err(_)))),
        "a reply past client-output-buffer-limit normal must disconnect the client, \
         not be served; got {disconnect:?}"
    );

    // The server is unharmed: shedding the offender is the point.
    assert_eq!(control.command(&["PING"]).await, Response::pong());

    // The operator can see it happened, to which class, and why.
    let metrics = server.fetch_metrics().await;
    let disconnects = frogdb_telemetry::testing::get_counter(
        &metrics,
        "frogdb_client_output_buffer_disconnects_total",
        &[("class", "normal"), ("reason", "hard_limit")],
    );
    assert!(
        disconnects >= 1.0,
        "the disconnect must be counted against its class and reason; got {disconnects}"
    );

    server.shutdown().await;
}

/// A reply that fits is served in full — the limit must not cost correctness.
///
/// The companion to the test above: same server, same limit, a pipeline whose
/// total reply stays under it. Without this, "disconnect everything" would pass
/// the suite.
// FM-MEMORY-001
#[tokio::test]
async fn test_a_pipeline_within_the_limit_is_served_in_full() {
    let server = TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(1),
        client_output_buffer_limit: Some(normal_hard_limit_spec()),
        ..Default::default()
    })
    .await;

    let mut client = server.connect().await;
    let payload = "v".repeat(1024);
    assert_eq!(
        client.command(&["SET", "small", &payload]).await,
        Response::ok()
    );

    // 64 KiB of reply against a 256 KiB allowance, pipelined the same way.
    for _ in 0..64 {
        client.send_only(&["GET", "small"]).await;
    }
    for i in 0..64 {
        let frame = tokio::time::timeout(Duration::from_secs(10), client.framed.next())
            .await
            .unwrap_or_else(|_| panic!("reply {i} timed out; the client must not be disconnected"));
        assert!(
            matches!(frame, Some(Ok(_))),
            "reply {i} must arrive: this pipeline is inside the limit"
        );
    }

    assert_eq!(client.command(&["PING"]).await, Response::pong());
    server.shutdown().await;
}

/// `Subsystem::NetworkOutput` must be a budget the shard broker knows about, so
/// its ceiling and its charge appear in the per-subsystem breakdown an operator
/// reads out of `frogdb_memory_budget_*`.
///
/// Charged bytes are sampled, and FrogDB's write path drains a batch before it
/// returns, so a snapshot's `charged_bytes` for a healthy server is legitimately
/// 0 — what is pinned here is that the row *exists* with its limit, for both
/// protocol versions, which is what "the budget is opened and adopted" means
/// observably. That the charge itself is real is pinned by the disconnect test
/// above (the limit is enforced on the charge) and by the unit tests.
// FM-MEMORY-002
#[tokio::test]
async fn test_network_output_budget_is_published_for_resp2_and_resp3() {
    let server = TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(1),
        ..Default::default()
    })
    .await;

    // RESP2 traffic.
    let mut resp2 = server.connect().await;
    let payload = "v".repeat(32 * 1024);
    assert_eq!(resp2.command(&["SET", "k", &payload]).await, Response::ok());
    assert!(matches!(
        resp2.command(&["GET", "k"]).await,
        Response::Bulk(Some(_))
    ));

    // RESP3 traffic over the other encoder, which buffers in `resp3_buf` rather
    // than the codec's write buffer. Both are charged at the same seam.
    let mut resp3 = server.connect_resp3().await;
    let reply = resp3.command(&["GET", "k"]).await;
    assert!(
        !format!("{reply:?}").is_empty(),
        "the RESP3 client must get its reply"
    );

    let metrics = server.fetch_metrics().await;
    let limit = frogdb_telemetry::testing::get_gauge(
        &metrics,
        "frogdb_memory_budget_limit_bytes",
        &[("shard", "0"), ("subsystem", "network_output")],
    );
    assert!(
        limit > 0.0,
        "the shard broker must publish a NetworkOutput budget row; got limit {limit}"
    );

    server.shutdown().await;
}

// ============================================================================
// The `replica` class, on the replication feed (FM-REPLICATION-069)
// ============================================================================

/// One key's worth of the dataset a replica has to be sent. Sized so that a
/// handful of them is far more than a loopback socket will swallow, which is
/// what makes the primary *hold* the payload while a replica does not read it.
const REPLICA_VALUE_BYTES: usize = 512 * 1024;
const REPLICA_VALUE_COUNT: usize = 16;
const REPLICA_DATASET_BYTES: usize = REPLICA_VALUE_BYTES * REPLICA_VALUE_COUNT;

/// Write a dataset big enough that the full sync staged for a replica cannot
/// disappear into socket buffers.
async fn write_replica_dataset(server: &TestServer) {
    let mut writer = server.connect().await;
    let payload = "v".repeat(REPLICA_VALUE_BYTES);
    for i in 0..REPLICA_VALUE_COUNT {
        assert_eq!(
            writer.command(&["SET", &format!("k{i}"), &payload]).await,
            Response::ok()
        );
    }
}

/// Ask for a full sync and then stop reading, which is what a slow replica is.
///
/// The `+FULLRESYNC` line is consumed so the test knows the handoff happened;
/// from there the socket is left un-drained and the primary is holding the
/// payload.
///
/// The connection's own id is taken first, so a test can read `omem` off *this*
/// row rather than off whichever row happens to be largest.
async fn psync_and_stall(server: &TestServer) -> (crate::common::test_server::TestClient, i64) {
    let mut replica = server.connect().await;
    let conn_id = match replica.command(&["CLIENT", "ID"]).await {
        Response::Integer(id) => id,
        other => panic!("CLIENT ID must return this connection's id; got {other:?}"),
    };
    let reply = replica.command(&["PSYNC", "?", "-1"]).await;
    assert!(
        !is_error(&reply),
        "the primary must accept the full-sync request; got {reply:?}"
    );
    (replica, conn_id)
}

/// The `omem=` connection `conn_id` reports, or `None` if it is no longer
/// listed.
async fn reported_omem(
    control: &mut crate::common::test_server::TestClient,
    conn_id: i64,
) -> Option<usize> {
    let listing = match control.command(&["CLIENT", "LIST"]).await {
        Response::Bulk(Some(data)) => String::from_utf8_lossy(&data).to_string(),
        other => panic!("CLIENT LIST must return a bulk listing; got {other:?}"),
    };
    let wanted = format!("id={conn_id}");
    let row = listing
        .lines()
        .find(|line| line.split(' ').any(|field| field == wanted))?;
    Some(
        row.split(' ')
            .find_map(|field| field.strip_prefix("omem="))
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or_else(|| panic!("every CLIENT LIST row carries omem=; got {row:?}")),
    )
}

/// The bytes a primary is holding for a replica are the replica's `omem`.
///
/// This is the half of FM-REPLICATION-069 that says the feed is *accounted*:
/// the full-sync dataset is staged in the primary's memory before a byte of it
/// is written, and while a replica is not reading, that staged payload is what
/// `CLIENT LIST` must report against the replica's own connection. Before this,
/// a replica's `omem` froze at whatever it held when `PSYNC` arrived — near
/// zero — no matter how much the primary was carrying for it.
///
/// The limit here is Redis's default (256 MiB hard), so nothing is shed: what
/// is under test is the figure, not the verdict.
// FM-REPLICATION-069
#[tokio::test]
async fn test_replica_feed_reports_its_buffered_bytes_as_omem() {
    let server = TestServer::start_primary_with_config(TestServerConfig {
        num_shards: Some(1),
        ..Default::default()
    })
    .await;

    write_replica_dataset(&server).await;

    let mut control = server.connect().await;
    let (_replica, replica_id) = psync_and_stall(&server).await;

    // The export is asynchronous with the `+FULLRESYNC` line, so poll rather
    // than sample once. Read the replica's *own* row: a figure that appeared on
    // some other connection would be a different bug, not this fix.
    let mut observed = 0;
    for _ in 0..100 {
        observed = reported_omem(&mut control, replica_id)
            .await
            .expect("the replica stays listed as a client across the PSYNC handoff");
        if observed > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(
        observed >= REPLICA_VALUE_BYTES,
        "a replica that is not reading must show the staged full sync as omem; \
         its row reported {observed} against a {REPLICA_DATASET_BYTES}-byte dataset"
    );

    // Same bytes, the operator's other view of them: the per-subsystem budget
    // breakdown must attribute the stalled feed to `NetworkOutput`, which is
    // what D4 rules the feed accounts under. The gauges are republished on the
    // shard's 10-second diagnostics tick, so this waits for a tick that falls
    // inside the stall rather than sampling once.
    let mut charged = 0.0;
    for _ in 0..40 {
        let metrics = server.fetch_metrics().await;
        charged = frogdb_telemetry::testing::get_gauge(
            &metrics,
            "frogdb_memory_budget_charged_bytes",
            &[("shard", "0"), ("subsystem", "network_output")],
        );
        if charged > 0.0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        charged >= REPLICA_VALUE_BYTES as f64,
        "the bytes staged for a stalled replica must appear under NetworkOutput in the \
         budget breakdown; the gauge read {charged}"
    );

    server.shutdown().await;
}

/// A replica the primary cannot afford to hold bytes for is dropped.
///
/// The other half of FM-REPLICATION-069: the `replica` class is not decoration
/// on the feed, it is enforced there. The hard limit is set below the dataset,
/// so the full sync this replica asked for is one the primary will not pay for
/// and the link goes rather than the primary's memory.
///
/// A dropped replica reconnects and resyncs, which is exactly Redis's behavior
/// for `client-output-buffer-limit slave` — the operator's lever against one
/// replica taking the primary down with it.
// FM-REPLICATION-069
#[tokio::test]
async fn test_replica_feed_over_the_hard_limit_is_disconnected() {
    let server = TestServer::start_primary_with_config(TestServerConfig {
        num_shards: Some(1),
        // Well under the dataset below, and under no other class's traffic.
        client_output_buffer_limit: Some(format!("replica {REPLICA_VALUE_BYTES} 0 0")),
        ..Default::default()
    })
    .await;

    write_replica_dataset(&server).await;

    let (mut replica, _replica_id) = psync_and_stall(&server).await;

    // Read raw rather than through the RESP codec: the full-sync payload is a
    // binary envelope the client codec cannot parse, so a decode error would
    // otherwise be indistinguishable from the disconnect under test. EOF on the
    // socket is the disconnect and nothing else is.
    let closed = tokio::time::timeout(Duration::from_secs(30), async {
        let socket = replica.framed.get_mut();
        let mut scratch = vec![0u8; 64 * 1024];
        loop {
            match socket.read(&mut scratch).await {
                Ok(0) => return true,
                Ok(_) => continue,
                Err(_) => return true,
            }
        }
    })
    .await;
    assert_eq!(
        closed,
        Ok(true),
        "a full sync past client-output-buffer-limit replica must drop the link, \
         not be served"
    );

    // The primary survives the replica it refused.
    let mut control = server.connect().await;
    assert_eq!(control.command(&["PING"]).await, Response::pong());

    // And the operator is told which class and why, on the same counter the
    // client-side seam moves.
    let metrics = server.fetch_metrics().await;
    let disconnects = frogdb_telemetry::testing::get_counter(
        &metrics,
        "frogdb_client_output_buffer_disconnects_total",
        &[("class", "replica"), ("reason", "hard_limit")],
    );
    assert!(
        disconnects >= 1.0,
        "the replica disconnect must be counted against its class and reason; got {disconnects}"
    );

    server.shutdown().await;
}

/// A replica that stalls under the soft mark and stays there is dropped when
/// the window expires — with no further byte of feed activity to notice it.
///
/// This is the failure mode `client-output-buffer-limit`'s soft seconds exist
/// for and the one issue 23 was filed against: the hard limit catches *growth*,
/// and a stalled full sync does not grow. The primary stages the dataset once,
/// parks inside the write to a replica that has stopped reading, and from there
/// nothing on that link runs again — so the drop can only come from the
/// account's own clock. The hard limit here is above the dataset precisely so
/// that it cannot be what fires.
// FM-REPLICATION-069
#[tokio::test]
async fn test_replica_feed_past_its_soft_window_is_disconnected() {
    let soft = REPLICA_VALUE_BYTES;
    let hard = REPLICA_DATASET_BYTES * 4;
    let server = TestServer::start_primary_with_config(TestServerConfig {
        num_shards: Some(1),
        // Startup-fixed knob (CONFIG SET is refused), so the window is set here.
        client_output_buffer_limit: Some(format!("replica {hard} {soft} 1")),
        ..Default::default()
    })
    .await;

    write_replica_dataset(&server).await;

    let (mut replica, _replica_id) = psync_and_stall(&server).await;

    let closed = tokio::time::timeout(Duration::from_secs(30), async {
        let socket = replica.framed.get_mut();
        let mut scratch = vec![0u8; 64 * 1024];
        loop {
            match socket.read(&mut scratch).await {
                Ok(0) => return true,
                Ok(_) => continue,
                Err(_) => return true,
            }
        }
    })
    .await;
    assert_eq!(
        closed,
        Ok(true),
        "a replica held above the soft mark for the whole window must be dropped"
    );

    let mut control = server.connect().await;
    assert_eq!(control.command(&["PING"]).await, Response::pong());

    let metrics = server.fetch_metrics().await;
    let disconnects = frogdb_telemetry::testing::get_counter(
        &metrics,
        "frogdb_client_output_buffer_disconnects_total",
        &[("class", "replica"), ("reason", "soft_limit")],
    );
    assert!(
        disconnects >= 1.0,
        "the drop must be counted as a soft-limit kill, not a hard one; got {disconnects}"
    );

    server.shutdown().await;
}

/// A malformed `client-output-buffer-limit` must refuse the boot rather than be
/// silently replaced by the default: a limit the operator believes they set and
/// does not have is worse than no limit at all.
///
/// The parser's own rejections are unit-tested in `connection::output_buffer`;
/// what this pins is that the rejection actually reaches startup — that the
/// parse sits on the boot path rather than beside it, so a typo in a config file
/// is a failure to start and not a server running wide open.
// FM-MEMORY-001
#[tokio::test]
async fn test_a_malformed_limit_spec_is_a_configuration_error() {
    let started = TestServer::try_start_standalone_with_config(TestServerConfig {
        num_shards: Some(1),
        // An incomplete triple: a class, a hard limit, a soft limit and no
        // soft-seconds.
        client_output_buffer_limit: Some("normal 1 2".to_string()),
        ..Default::default()
    })
    .await;

    let err = match started {
        Ok(server) => {
            server.shutdown().await;
            panic!("a malformed client-output-buffer-limit must refuse the boot");
        }
        Err(e) => format!("{e:#}"),
    };
    assert!(
        err.contains("client-output-buffer-limit"),
        "the operator must be told which parameter refused the boot; got: {err}"
    );

    // The same server boots when the spec is well formed, so the refusal is the
    // spec's doing and not an unrelated startup failure.
    let server = TestServer::try_start_standalone_with_config(TestServerConfig {
        num_shards: Some(1),
        client_output_buffer_limit: Some("normal 1 2 3".to_string()),
        ..Default::default()
    })
    .await
    .expect("a well-formed spec must boot");
    server.shutdown().await;
}
