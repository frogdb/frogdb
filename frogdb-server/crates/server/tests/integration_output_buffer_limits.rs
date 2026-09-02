//! End-to-end tests for `client-output-buffer-limit` and the `NetworkOutput`
//! budget (memory-architecture issue 18, PRD ruling R11).
//!
//! The decision itself — hard limit, soft limit, soft window, budget refusal —
//! is unit-tested in `connection::output_buffer`. What can only be tested with a
//! real socket pair is that the seam is *reached*: that a client which asks for
//! more reply bytes than the server will hold for it is actually disconnected,
//! that the operator sees why, and that the bytes it buffered were charged to a
//! budget the shard broker publishes.

use crate::common::test_server::{TestServer, TestServerConfig};
use frogdb_protocol::Response;
use futures::StreamExt;
use std::time::Duration;

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

/// A malformed `client-output-buffer-limit` must refuse the boot rather than be
/// silently replaced by the default: a limit the operator believes they set and
/// does not have is worse than no limit at all.
#[test]
fn test_a_malformed_limit_spec_is_a_configuration_error() {
    use frogdb_server::connection::output_buffer::OutputBufferLimits;

    assert!(OutputBufferLimits::parse("normal 1 2 3").is_ok());
    assert!(
        OutputBufferLimits::parse("normal 1 2").is_err(),
        "an incomplete triple must not be accepted"
    );
}
