//! Integration tests for HOTKEYS GET reply shape across RESP2 and RESP3.
//!
//! HOTKEYS GET is built once through `MapReply`, so the RESP2 flat-array and
//! RESP3 map replies are guaranteed to carry the same fields in the same order.
//! These tests pin both protocol shapes, including the conditional
//! `sample-ratio` / `selected-slots` fields.

use crate::common::test_server::TestServer;
use frogdb_protocol::Response;
use redis_protocol::resp3::types::BytesFrame as Resp3Frame;

/// Extract a RESP3 map's key names (as bytes), sorted.
///
/// The `redis-protocol` decoder materializes a RESP3 map into a `HashMap`, so
/// decode order is not observable here — we compare key *sets*. The field
/// *order* is pinned on the RESP2 side (a flat array/`Vec`), and because
/// `MapReply` emits both protocols from the same pair `Vec`, matching RESP2
/// order guarantees the RESP3 wire order too.
fn resp3_map_keys_sorted(frame: &Resp3Frame) -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = match frame {
        Resp3Frame::Map { data, .. } => data
            .keys()
            .map(|k| match k {
                Resp3Frame::BlobString { data, .. } => data.to_vec(),
                Resp3Frame::SimpleString { data, .. } => data.to_vec(),
                other => panic!("expected string key, got {other:?}"),
            })
            .collect(),
        other => panic!("expected Map, got {other:?}"),
    };
    keys.sort();
    keys
}

fn sorted(mut keys: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    keys.sort();
    keys
}

/// Extract a RESP2 flat array's key names (even indices) in order.
fn resp2_flat_keys(resp: &Response) -> Vec<Vec<u8>> {
    match resp {
        Response::Array(items) => items
            .iter()
            .step_by(2)
            .map(|k| match k {
                Response::Bulk(Some(b)) => b.to_vec(),
                other => panic!("expected bulk key, got {other:?}"),
            })
            .collect(),
        other => panic!("expected Array, got {other:?}"),
    }
}

/// Without SAMPLE, HOTKEYS GET has exactly the four unconditional fields, in
/// the same order in both protocols. RESP2 flattens the map to an even-length
/// array; RESP3 returns a map with half as many entries.
#[tokio::test]
async fn test_hotkeys_get_shape_minimal() {
    let server = TestServer::start_standalone().await;
    let mut c2 = server.connect().await;

    let started = c2
        .command(&["HOTKEYS", "START", "METRICS", "1", "cpu"])
        .await;
    assert_eq!(started, Response::ok());

    // RESP2: flat array of alternating key/value pairs.
    let r2 = c2.command(&["HOTKEYS", "GET"]).await;
    let r2_keys = resp2_flat_keys(&r2);
    assert_eq!(
        r2_keys,
        vec![
            b"metrics".to_vec(),
            b"count".to_vec(),
            b"duration".to_vec(),
            b"hotkeys".to_vec(),
        ],
    );
    if let Response::Array(items) = &r2 {
        assert_eq!(items.len(), 8, "four flattened key/value pairs");
    }

    // RESP3: map carrying the same keys (compared as a set; see helper doc).
    let mut c3 = server.connect_resp3().await;
    c3.command(&["HELLO", "3"]).await;
    let r3 = c3.command(&["HOTKEYS", "GET"]).await;
    assert!(matches!(r3, Resp3Frame::Map { .. }), "RESP3 must be a Map");
    if let Resp3Frame::Map { data, .. } = &r3 {
        assert_eq!(data.len(), 4);
    }
    assert_eq!(
        resp3_map_keys_sorted(&r3),
        sorted(r2_keys),
        "RESP3 map keys match RESP2"
    );

    server.shutdown().await;
}

/// With SAMPLE > 1, the conditional `sample-ratio` and `selected-slots` fields
/// appear — once, in both protocols, in the same position.
#[tokio::test]
async fn test_hotkeys_get_shape_with_conditional_fields() {
    let server = TestServer::start_standalone().await;
    let mut c2 = server.connect().await;

    let started = c2
        .command(&["HOTKEYS", "START", "METRICS", "1", "cpu", "SAMPLE", "5"])
        .await;
    assert_eq!(started, Response::ok());

    let expected_keys = vec![
        b"metrics".to_vec(),
        b"count".to_vec(),
        b"duration".to_vec(),
        b"sample-ratio".to_vec(),
        b"selected-slots".to_vec(),
        b"hotkeys".to_vec(),
    ];

    // RESP2: flat array, six flattened key/value pairs.
    let r2 = c2.command(&["HOTKEYS", "GET"]).await;
    assert_eq!(resp2_flat_keys(&r2), expected_keys);
    if let Response::Array(items) = &r2 {
        assert_eq!(items.len(), 12);
    }

    // RESP3: map with six pairs carrying identical keys (as a set).
    let mut c3 = server.connect_resp3().await;
    c3.command(&["HELLO", "3"]).await;
    let r3 = c3.command(&["HOTKEYS", "GET"]).await;
    assert_eq!(resp3_map_keys_sorted(&r3), sorted(expected_keys.clone()));
    if let Resp3Frame::Map { data, .. } = &r3 {
        assert_eq!(data.len(), 6);
    }

    // The sample-ratio value round-trips as the integer we set.
    if let Response::Array(items) = &r2 {
        assert_eq!(items[7], Response::Integer(5));
    }

    server.shutdown().await;
}
