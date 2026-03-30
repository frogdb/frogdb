//! Regression tests for Event Sourcing commands (ES.APPEND, ES.READ, ES.REPLAY,
//! ES.INFO, ES.SNAPSHOT, ES.ALL).

use frogdb_protocol::Response;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract (version, stream_id_option) from an ES.APPEND response: [Integer, Bulk|Null].
fn unwrap_version_and_id(resp: &Response) -> (i64, Option<String>) {
    let arr = unwrap_array(resp.clone());
    assert!(arr.len() >= 2, "expected [version, id], got {arr:?}");
    let version = unwrap_integer(&arr[0]);
    let id = match &arr[1] {
        Response::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).to_string()),
        _ => None,
    };
    (version, id)
}

/// Find a field value in a flat key-value ES.INFO array by label name.
fn info_field<'a>(items: &'a [Response], label: &str) -> &'a Response {
    for i in (0..items.len()).step_by(2) {
        if let Response::Bulk(Some(b)) = &items[i]
            && std::str::from_utf8(b).unwrap() == label
        {
            return &items[i + 1];
        }
        if let Response::Simple(s) = &items[i]
            && s == label
        {
            return &items[i + 1];
        }
    }
    panic!("field {label:?} not found in ES.INFO response");
}

// ---------------------------------------------------------------------------
// ES.APPEND tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn es_append_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "ES.APPEND",
            "es1",
            "0",
            "UserCreated",
            r#"{"name":"alice"}"#,
        ])
        .await;
    let (version, id) = unwrap_version_and_id(&resp);
    assert_eq!(version, 1);
    assert!(id.is_some(), "expected stream ID, got nil");
}

#[tokio::test]
async fn es_append_sequential() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["ES.APPEND", "es_seq", "0", "evt1", "data1"])
        .await;
    let (v1, _) = unwrap_version_and_id(&resp);
    assert_eq!(v1, 1);

    let resp = client
        .command(&["ES.APPEND", "es_seq", "1", "evt2", "data2"])
        .await;
    let (v2, _) = unwrap_version_and_id(&resp);
    assert_eq!(v2, 2);

    let resp = client
        .command(&["ES.APPEND", "es_seq", "2", "evt3", "data3"])
        .await;
    let (v3, _) = unwrap_version_and_id(&resp);
    assert_eq!(v3, 3);
}

#[tokio::test]
async fn es_append_version_mismatch() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // First append succeeds
    client
        .command(&["ES.APPEND", "es_vm", "0", "evt1", "data1"])
        .await;

    // Wrong version: expect 0 but actual is 1
    let resp = client
        .command(&["ES.APPEND", "es_vm", "0", "evt2", "data2"])
        .await;
    assert_error_prefix(&resp, "VERSIONMISMATCH");
}

#[tokio::test]
async fn es_append_extra_fields() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "ES.APPEND",
            "es_ef",
            "0",
            "OrderPlaced",
            r#"{"id":1}"#,
            "source",
            "web",
            "region",
            "us-east",
        ])
        .await;
    let (version, _) = unwrap_version_and_id(&resp);
    assert_eq!(version, 1);

    // Read and verify extra fields are present
    let resp = client.command(&["ES.READ", "es_ef", "1"]).await;
    let events = unwrap_array(resp);
    assert_eq!(events.len(), 1);
    let event = unwrap_array(events[0].clone());
    // event[2] is the fields array
    let fields = unwrap_array(event[2].clone());
    let field_strs: Vec<String> = fields
        .iter()
        .map(|f| std::str::from_utf8(unwrap_bulk(f)).unwrap().to_string())
        .collect();
    assert!(field_strs.contains(&"source".to_string()));
    assert!(field_strs.contains(&"web".to_string()));
}

#[tokio::test]
async fn es_append_idempotent() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // First append with idempotency key
    let resp = client
        .command(&[
            "ES.APPEND",
            "es_idem",
            "0",
            "evt1",
            "data1",
            "IF_NOT_EXISTS",
            "dedup-key-1",
        ])
        .await;
    let (v1, id1) = unwrap_version_and_id(&resp);
    assert_eq!(v1, 1);
    assert!(id1.is_some());

    // Repeat with same idempotency key — should return version + null id
    let resp = client
        .command(&[
            "ES.APPEND",
            "es_idem",
            "1",
            "evt1",
            "data1",
            "IF_NOT_EXISTS",
            "dedup-key-1",
        ])
        .await;
    let (v2, id2) = unwrap_version_and_id(&resp);
    assert!(v2 >= 1, "expected version >= 1");
    assert!(id2.is_none(), "expected nil stream ID for duplicate");
}

#[tokio::test]
async fn es_append_idempotent_different_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "ES.APPEND",
            "es_idem2",
            "0",
            "evt1",
            "data1",
            "IF_NOT_EXISTS",
            "key-a",
        ])
        .await;
    let (v1, _) = unwrap_version_and_id(&resp);
    assert_eq!(v1, 1);

    // Different idempotency key — should succeed
    let resp = client
        .command(&[
            "ES.APPEND",
            "es_idem2",
            "1",
            "evt2",
            "data2",
            "IF_NOT_EXISTS",
            "key-b",
        ])
        .await;
    let (v2, id2) = unwrap_version_and_id(&resp);
    assert_eq!(v2, 2);
    assert!(id2.is_some());
}

#[tokio::test]
async fn es_append_wrongtype() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "str_key", "hello"]).await;

    let resp = client
        .command(&["ES.APPEND", "str_key", "0", "evt1", "data1"])
        .await;
    assert_error_prefix(&resp, "WRONGTYPE");
}

#[tokio::test]
async fn es_append_creates_stream() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Key doesn't exist yet
    let resp = client.command(&["EXISTS", "es_new"]).await;
    assert_integer_eq(&resp, 0);

    // Append creates it
    let resp = client
        .command(&["ES.APPEND", "es_new", "0", "evt1", "data1"])
        .await;
    let (v, _) = unwrap_version_and_id(&resp);
    assert_eq!(v, 1);

    let resp = client.command(&["EXISTS", "es_new"]).await;
    assert_integer_eq(&resp, 1);
}

// ---------------------------------------------------------------------------
// ES.READ tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn es_read_all() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..5 {
        client
            .command(&[
                "ES.APPEND",
                "es_rd",
                &i.to_string(),
                &format!("evt{}", i + 1),
                &format!("data{}", i + 1),
            ])
            .await;
    }

    let resp = client.command(&["ES.READ", "es_rd", "1"]).await;
    let events = unwrap_array(resp);
    assert_eq!(events.len(), 5);
}

#[tokio::test]
async fn es_read_range() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..5 {
        client
            .command(&[
                "ES.APPEND",
                "es_rdr",
                &i.to_string(),
                &format!("evt{}", i + 1),
                &format!("data{}", i + 1),
            ])
            .await;
    }

    // Read versions 2 through 4
    let resp = client.command(&["ES.READ", "es_rdr", "2", "4"]).await;
    let events = unwrap_array(resp);
    assert_eq!(events.len(), 3);

    // Verify first event is version 2
    let first = unwrap_array(events[0].clone());
    assert_eq!(unwrap_integer(&first[0]), 2);
}

#[tokio::test]
async fn es_read_count() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..5 {
        client
            .command(&[
                "ES.APPEND",
                "es_rdc",
                &i.to_string(),
                &format!("evt{}", i + 1),
                &format!("data{}", i + 1),
            ])
            .await;
    }

    let resp = client
        .command(&["ES.READ", "es_rdc", "1", "COUNT", "2"])
        .await;
    let events = unwrap_array(resp);
    assert_eq!(events.len(), 2);
}

#[tokio::test]
async fn es_read_nonexistent() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["ES.READ", "nope", "1"]).await;
    let events = unwrap_array(resp);
    assert!(events.is_empty());
}

#[tokio::test]
async fn es_read_event_format() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ES.APPEND", "es_fmt", "0", "UserCreated", "alice"])
        .await;

    let resp = client.command(&["ES.READ", "es_fmt", "1"]).await;
    let events = unwrap_array(resp);
    assert_eq!(events.len(), 1);

    // Each event: [version, stream_id, [field, value, ...]]
    let event = unwrap_array(events[0].clone());
    assert_eq!(event.len(), 3);
    assert_eq!(unwrap_integer(&event[0]), 1); // version
    // event[1] is stream ID (bulk string)
    let _id = std::str::from_utf8(unwrap_bulk(&event[1])).unwrap();
    // event[2] is fields array
    let fields = unwrap_array(event[2].clone());
    assert!(
        fields.len() >= 4,
        "expected at least event_type + data fields"
    );
    // Fields: [event_type, UserCreated, data, alice]
    assert_bulk_eq(&fields[0], b"event_type");
    assert_bulk_eq(&fields[1], b"UserCreated");
    assert_bulk_eq(&fields[2], b"data");
    assert_bulk_eq(&fields[3], b"alice");
}

// ---------------------------------------------------------------------------
// ES.REPLAY tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn es_replay_no_snapshot() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..3 {
        client
            .command(&[
                "ES.APPEND",
                "es_rep",
                &i.to_string(),
                &format!("evt{}", i + 1),
                &format!("data{}", i + 1),
            ])
            .await;
    }

    let resp = client.command(&["ES.REPLAY", "es_rep"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 2); // [snapshot_or_null, events]
    assert_nil(&arr[0]); // No snapshot
    let events = unwrap_array(arr[1].clone());
    assert_eq!(events.len(), 3);
}

#[tokio::test]
async fn es_replay_with_snapshot() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Use hash tags so stream and snapshot land on the same slot
    for i in 0..5 {
        client
            .command(&[
                "ES.APPEND",
                "{rep2}stream",
                &i.to_string(),
                &format!("evt{}", i + 1),
                &format!("data{}", i + 1),
            ])
            .await;
    }

    // Store a snapshot at version 3
    let resp = client
        .command(&["ES.SNAPSHOT", "{rep2}snap", "3", r#"{"balance":300}"#])
        .await;
    assert_ok(&resp);

    // Replay with snapshot — should return snapshot state + events after version 3
    let resp = client
        .command(&["ES.REPLAY", "{rep2}stream", "SNAPSHOT", "{rep2}snap"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 2);

    // Snapshot state
    let snap_state = std::str::from_utf8(unwrap_bulk(&arr[0])).unwrap();
    assert!(
        snap_state.contains("300"),
        "expected snapshot state, got: {snap_state}"
    );

    // Events after version 3 (should be versions 4 and 5)
    let events = unwrap_array(arr[1].clone());
    assert_eq!(events.len(), 2);
}

#[tokio::test]
async fn es_replay_snapshot_at_latest() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Use hash tags so stream and snapshot land on the same slot
    for i in 0..3 {
        client
            .command(&[
                "ES.APPEND",
                "{rep3}stream",
                &i.to_string(),
                &format!("evt{}", i + 1),
                &format!("data{}", i + 1),
            ])
            .await;
    }

    // Snapshot at version 3 (the latest)
    let resp = client
        .command(&["ES.SNAPSHOT", "{rep3}snap", "3", r#"{"state":"final"}"#])
        .await;
    assert_ok(&resp);

    let resp = client
        .command(&["ES.REPLAY", "{rep3}stream", "SNAPSHOT", "{rep3}snap"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 2);
    // Should have empty events list
    let events = unwrap_array(arr[1].clone());
    assert!(
        events.is_empty(),
        "expected no events after latest snapshot"
    );
}

#[tokio::test]
async fn es_replay_missing_stream() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["ES.REPLAY", "nope"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 2);
    assert_nil(&arr[0]);
    let events = unwrap_array(arr[1].clone());
    assert!(events.is_empty());
}

// ---------------------------------------------------------------------------
// ES.INFO tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn es_info_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..3 {
        client
            .command(&[
                "ES.APPEND",
                "es_info",
                &i.to_string(),
                &format!("evt{}", i + 1),
                &format!("data{}", i + 1),
            ])
            .await;
    }

    let resp = client.command(&["ES.INFO", "es_info"]).await;
    let items = unwrap_array(resp);

    assert_integer_eq(info_field(&items, "version"), 3);
    assert_integer_eq(info_field(&items, "entries"), 3);
    // first-id and last-id should be non-null
    let _ = unwrap_bulk(info_field(&items, "first-id"));
    let _ = unwrap_bulk(info_field(&items, "last-id"));
    assert_integer_eq(info_field(&items, "idempotency-keys"), 0);
}

#[tokio::test]
async fn es_info_after_idempotent() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ES.APPEND",
            "es_info2",
            "0",
            "evt1",
            "data1",
            "IF_NOT_EXISTS",
            "key-1",
        ])
        .await;

    let resp = client.command(&["ES.INFO", "es_info2"]).await;
    let items = unwrap_array(resp);
    assert_integer_eq(info_field(&items, "idempotency-keys"), 1);
}

#[tokio::test]
async fn es_info_nonexistent() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["ES.INFO", "nope"]).await;
    assert_nil(&resp);
}

// ---------------------------------------------------------------------------
// ES.SNAPSHOT tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn es_snapshot_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["ES.SNAPSHOT", "snap1", "5", r#"{"count":42}"#])
        .await;
    assert_ok(&resp);

    // Stored as string "5:{\"count\":42}"
    let resp = client.command(&["GET", "snap1"]).await;
    let s = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    assert!(s.starts_with("5:"), "expected '5:...' format, got: {s}");
    assert!(s.contains("42"), "expected state data, got: {s}");
}

// ---------------------------------------------------------------------------
// ES.ALL tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn es_all_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Append to two different streams
    client
        .command(&["ES.APPEND", "es_all_a", "0", "evt1", "data1"])
        .await;
    client
        .command(&["ES.APPEND", "es_all_b", "0", "evt2", "data2"])
        .await;

    let resp = client.command(&["ES.ALL", "COUNT", "10"]).await;
    let events = unwrap_array(resp);
    assert!(
        events.len() >= 2,
        "expected at least 2 events from ES.ALL, got {}",
        events.len()
    );
}

#[tokio::test]
async fn es_all_count_limit() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..5 {
        client
            .command(&[
                "ES.APPEND",
                &format!("es_all_c{i}"),
                "0",
                &format!("evt{i}"),
                &format!("data{i}"),
            ])
            .await;
    }

    let resp = client.command(&["ES.ALL", "COUNT", "2"]).await;
    let events = unwrap_array(resp);
    assert_eq!(events.len(), 2);
}

#[tokio::test]
async fn es_all_after() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Append several events and capture the stream ID of the first
    let resp = client
        .command(&["ES.APPEND", "es_all_af", "0", "evt1", "data1"])
        .await;
    let (_, first_id) = unwrap_version_and_id(&resp);
    let first_id = first_id.unwrap();

    client
        .command(&["ES.APPEND", "es_all_af", "1", "evt2", "data2"])
        .await;
    client
        .command(&["ES.APPEND", "es_all_af", "2", "evt3", "data3"])
        .await;

    // ES.ALL AFTER first_id — should exclude the first event
    let resp = client
        .command(&["ES.ALL", "AFTER", &first_id, "COUNT", "10"])
        .await;
    let events = unwrap_array(resp);
    // Should have at least the 2nd and 3rd events
    assert!(
        events.len() >= 2,
        "expected at least 2 events after first, got {}",
        events.len()
    );
}
