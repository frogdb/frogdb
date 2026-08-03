//! Level-3 regression net for issue 55: `INCRBYFLOAT`/`HINCRBYFLOAT` must store
//! exactly the bytes they reply.
//!
//! The bug was two float renderers on one path — the reply built by
//! `commands::utils::format_float` (shortest round-trip) and the store built by
//! a `{:.17}`-and-trim copy in `frogdb-types`. `SET k 0; INCRBYFLOAT k 0.1`
//! replied `0.1` and stored `0.10000000000000001`, so a later `GET` disagreed
//! with the command that wrote the key — and the divergent string, not the
//! reply, is what the WAL persists and what crosses the replication link.
//!
//! Why level 3 rather than level 1: the equality being asserted is between a
//! *reply* and a *stored value*, which needs real command dispatch against a
//! real store. It does not need a socket, so a level-4 server test would add
//! RESP framing without adding signal. The level-1 half (the rendering table
//! itself) lives in `frogdb-types` as
//! `increment_float_stores_exactly_what_the_reply_renders`.

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use frogdb_core::noop::NoopMetricsRecorder;
use frogdb_core::{
    CommandRegistry, CoreMsg, ShardReceiver, ShardSender, ShardWorker, ShardWorkerBuilder,
};
use frogdb_protocol::{ParsedCommand, ProtocolVersion, Response};

/// A 1-shard worker holding the real production command set.
fn real_worker() -> (ShardWorker, mpsc::Sender<frogdb_core::shard::NewConnection>) {
    let (msg_tx, msg_rx) = mpsc::channel(16);
    let (conn_tx, conn_rx) = mpsc::channel(16);
    let mut registry = CommandRegistry::new();
    frogdb_commands::register_all(&mut registry);
    let worker = ShardWorkerBuilder::new(0, 1)
        .with_message_rx(ShardReceiver::new(msg_rx))
        .with_new_conn_rx(conn_rx)
        .with_shard_senders(Arc::new(vec![ShardSender::new(msg_tx)]))
        .with_registry(Arc::new(registry))
        .with_metrics(Arc::new(NoopMetricsRecorder::new()))
        .build();
    (worker, conn_tx)
}

async fn run(
    worker: &mut ShardWorker,
    protocol_version: ProtocolVersion,
    name: &'static [u8],
    args: &[&str],
) -> Response {
    let (tx, rx) = oneshot::channel();
    let command = Arc::new(ParsedCommand::new(
        Bytes::from_static(name),
        args.iter()
            .map(|a| Bytes::copy_from_slice(a.as_bytes()))
            .collect(),
    ));
    let msg = CoreMsg::Execute {
        command,
        conn_id: 1,
        txid: None,
        protocol_version,
        track_reads: false,
        no_touch: false,
        response_tx: tx,
    };
    worker.drive(msg).await;
    rx.await.expect("execute response")
}

fn bulk_bytes(response: Response, what: &str) -> Bytes {
    match response {
        Response::Bulk(Some(b)) => b,
        other => panic!("expected a bulk reply from {what}, got {other:?}"),
    }
}

/// The deltas issue 55 names. Every one of them is a value whose shortest
/// round-trip rendering differs from seventeen decimal places of it.
const DELTAS: [&str; 5] = ["0.1", "3.14", "1e-7", "0.30000000000000004", "1e-320"];

/// Issue 55, acceptance criterion 2: the `INCRBYFLOAT` reply bytes equal the
/// `GET` bytes. **Failed before the collapse** — the reply was `0.1` and the
/// `GET` was `0.10000000000000001`.
///
/// Note the bug was order-dependent, which is why the key is seeded with `SET k
/// 0` first: `INCRBYFLOAT` on a *missing* key stored `format_float(delta)`
/// through the canonical renderer and was always correct.
#[tokio::test]
async fn incrbyfloat_reply_bytes_equal_the_stored_bytes() {
    for delta in DELTAS {
        let (mut worker, _conn_tx) = real_worker();
        run(&mut worker, ProtocolVersion::Resp2, b"SET", &["k", "0"]).await;

        let reply = bulk_bytes(
            run(
                &mut worker,
                ProtocolVersion::Resp2,
                b"INCRBYFLOAT",
                &["k", delta],
            )
            .await,
            "INCRBYFLOAT",
        );
        let stored = bulk_bytes(
            run(&mut worker, ProtocolVersion::Resp2, b"GET", &["k"]).await,
            "GET",
        );

        assert_eq!(
            reply, stored,
            "INCRBYFLOAT k {delta} replied {reply:?} but stored {stored:?}",
        );
    }
}

/// The same property for `HINCRBYFLOAT`, which reached the deleted renderer
/// through `HashValue::incr_by_float`. It replies a bulk string on every
/// protocol version, so this one diverged for RESP3 clients too.
#[tokio::test]
async fn hincrbyfloat_reply_bytes_equal_the_stored_bytes() {
    for delta in DELTAS {
        let (mut worker, _conn_tx) = real_worker();
        run(
            &mut worker,
            ProtocolVersion::Resp2,
            b"HSET",
            &["h", "f", "0"],
        )
        .await;

        let reply = bulk_bytes(
            run(
                &mut worker,
                ProtocolVersion::Resp2,
                b"HINCRBYFLOAT",
                &["h", "f", delta],
            )
            .await,
            "HINCRBYFLOAT",
        );
        let stored = bulk_bytes(
            run(&mut worker, ProtocolVersion::Resp2, b"HGET", &["h", "f"]).await,
            "HGET",
        );

        assert_eq!(
            reply, stored,
            "HINCRBYFLOAT h f {delta} replied {reply:?} but stored {stored:?}",
        );
    }
}

/// Repeated increments are the shape that compounds: each step re-parses the
/// string the previous step stored, so a store renderer that spells out
/// representation error feeds that error back in.
#[tokio::test]
async fn repeated_incrbyfloat_never_drifts_from_the_reply() {
    let (mut worker, _conn_tx) = real_worker();
    run(&mut worker, ProtocolVersion::Resp2, b"SET", &["k", "0"]).await;

    for _ in 0..20 {
        let reply = bulk_bytes(
            run(
                &mut worker,
                ProtocolVersion::Resp2,
                b"INCRBYFLOAT",
                &["k", "0.1"],
            )
            .await,
            "INCRBYFLOAT",
        );
        let stored = bulk_bytes(
            run(&mut worker, ProtocolVersion::Resp2, b"GET", &["k"]).await,
            "GET",
        );
        assert_eq!(reply, stored, "reply and store drifted apart mid-sequence");
    }
}

/// Issue 55, acceptance criterion 3: the RESP3 `Response::Double` path and the
/// RESP2 bulk path describe the same number.
///
/// **This is asserted as a round-trip, not as byte equality, and that is a
/// deliberate divergence from the issue's wording.** RESP3's `,<double>\r\n` is
/// rendered by the `redis-protocol` crate from the raw `f64` (Rust's `Display`),
/// not by FrogDB's `format_float`, so the two spellings legitimately differ for
/// extreme magnitudes — `1e300` is `1e+300` on RESP2 and 301 literal digits on
/// RESP3. Both parse back to the same `f64`, which is the property a client can
/// actually rely on; pinning byte equality would mean FrogDB overriding the
/// RESP3 encoder's spec-conformant rendering. See the note in the issue's
/// Resolution.
#[tokio::test]
async fn the_resp3_double_and_the_resp2_bulk_describe_the_same_number() {
    for delta in DELTAS {
        let (mut worker, _conn_tx) = real_worker();
        run(&mut worker, ProtocolVersion::Resp2, b"SET", &["k", "0"]).await;
        let resp2 = bulk_bytes(
            run(
                &mut worker,
                ProtocolVersion::Resp2,
                b"INCRBYFLOAT",
                &["k", delta],
            )
            .await,
            "INCRBYFLOAT",
        );

        let (mut worker, _conn_tx) = real_worker();
        run(&mut worker, ProtocolVersion::Resp2, b"SET", &["k", "0"]).await;
        let resp3 = match run(
            &mut worker,
            ProtocolVersion::Resp3,
            b"INCRBYFLOAT",
            &["k", delta],
        )
        .await
        {
            Response::Double(d) => d,
            other => panic!("expected a RESP3 double, got {other:?}"),
        };

        let resp2_parsed: f64 = std::str::from_utf8(&resp2).unwrap().parse().unwrap();
        assert_eq!(
            resp2_parsed, resp3,
            "RESP2 bulk {resp2:?} and RESP3 double {resp3} disagree for delta {delta}",
        );

        // And the RESP3 client's view of the *stored* value is the RESP2 bytes,
        // because GET is a bulk reply on both protocols.
        let stored = bulk_bytes(
            run(&mut worker, ProtocolVersion::Resp3, b"GET", &["k"]).await,
            "GET",
        );
        assert_eq!(
            stored, resp2,
            "the stored rendering must be the canonical one regardless of the writer's protocol",
        );
    }
}
