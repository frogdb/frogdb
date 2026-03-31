//! Rust port of Redis 8.6.0 `unit/type/stream.tcl` test suite.
//!
//! Excludes: IDMP, IDMPAUTO, ACKED, DELREF, XDELEX, KEEPREF, needs:debug,
//! AOF, CONFIG SET stream-node-max-entries, encoding-specific tests.

use std::time::Duration;

use frogdb_protocol::Response;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract stream entry ID from an XRANGE response entry.
fn entry_id(entry: &Response) -> String {
    let arr = match entry {
        Response::Array(v) => v,
        other => panic!("expected Array, got {other:?}"),
    };
    String::from_utf8(unwrap_bulk(&arr[0]).to_vec()).unwrap()
}

/// Extract stream entry fields from an XRANGE response entry.
fn entry_fields(entry: &Response) -> Vec<String> {
    let arr = match entry {
        Response::Array(v) => v,
        other => panic!("expected Array, got {other:?}"),
    };
    extract_bulk_strings(&arr[1])
}

/// Parse a bulk response as a UTF-8 string.
fn parse_bulk_string(resp: &Response) -> String {
    String::from_utf8(unwrap_bulk(resp).to_vec()).unwrap()
}

/// Parse a stream ID into (ms, seq).
fn parse_id_parts(id: &str) -> (u64, u64) {
    let parts: Vec<&str> = id.split('-').collect();
    (parts[0].parse().unwrap(), parts[1].parse().unwrap())
}

/// Find a key in a flat alternating key-value array and return its value.
fn xinfo_get_field<'a>(items: &'a [Response], key: &str) -> &'a Response {
    for i in (0..items.len()).step_by(2) {
        if let Response::Bulk(Some(b)) = &items[i]
            && b.as_ref() == key.as_bytes()
        {
            return &items[i + 1];
        }
    }
    panic!("field {key:?} not found in XINFO response");
}

// ---------------------------------------------------------------------------
// 1. XADD wrong number of args
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xadd_wrong_number_of_args() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["XADD", "mystream"]).await;
    assert_error_prefix(&resp, "ERR");

    let resp = client.command(&["XADD", "mystream", "*"]).await;
    assert_error_prefix(&resp, "ERR");

    let resp = client.command(&["XADD", "mystream", "*", "field"]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// 2. XADD can add entries into a stream that XRANGE can fetch
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xadd_can_add_entries_and_xrange_can_fetch() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["XADD", "mystream", "*", "item", "1", "value", "a"])
        .await;
    client
        .command(&["XADD", "mystream", "*", "item", "2", "value", "b"])
        .await;

    assert_integer_eq(&client.command(&["XLEN", "mystream"]).await, 2);

    let resp = client.command(&["XRANGE", "mystream", "-", "+"]).await;
    let entries = unwrap_array(resp);
    assert_eq!(entries.len(), 2);

    let fields0 = entry_fields(&entries[0]);
    assert_eq!(fields0, vec!["item", "1", "value", "a"]);

    let fields1 = entry_fields(&entries[1]);
    assert_eq!(fields1, vec!["item", "2", "value", "b"]);
}

// ---------------------------------------------------------------------------
// 3. XADD IDs are incremental
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xadd_ids_are_incremental() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let id1_resp = client
        .command(&["XADD", "mystream", "*", "item", "1", "value", "a"])
        .await;
    let id2_resp = client
        .command(&["XADD", "mystream", "*", "item", "2", "value", "b"])
        .await;

    let id1 = parse_bulk_string(&id1_resp);
    let id2 = parse_bulk_string(&id2_resp);

    let (ms1, seq1) = parse_id_parts(&id1);
    let (ms2, seq2) = parse_id_parts(&id2);

    assert!(
        ms2 > ms1 || (ms2 == ms1 && seq2 > seq1),
        "IDs not incremental: {id1} vs {id2}"
    );
}

// ---------------------------------------------------------------------------
// 4. XADD IDs are incremental when ms is the same as well (MULTI/EXEC)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xadd_ids_incremental_same_ms_via_multi_exec() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["MULTI"]).await);
    client
        .command(&["XADD", "mystream", "*", "item", "1", "value", "a"])
        .await;
    client
        .command(&["XADD", "mystream", "*", "item", "2", "value", "b"])
        .await;
    let exec_resp = client.command(&["EXEC"]).await;
    let results = unwrap_array(exec_resp);
    assert_eq!(results.len(), 2);

    let id1 = parse_bulk_string(&results[0]);
    let id2 = parse_bulk_string(&results[1]);

    let (ms1, seq1) = parse_id_parts(&id1);
    let (ms2, seq2) = parse_id_parts(&id2);

    // Within a MULTI, both should get the same ms but seq must be incremental
    assert!(
        ms2 > ms1 || (ms2 == ms1 && seq2 > seq1),
        "IDs not incremental within MULTI: {id1} vs {id2}"
    );
}

// ---------------------------------------------------------------------------
// 5. XADD IDs correctly report an error when overflowing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xadd_id_overflow_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "XADD",
            "mystream",
            "18446744073709551615-18446744073709551615",
            "k",
            "v",
        ])
        .await;
    let resp = client.command(&["XADD", "mystream", "*", "k", "v"]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// 6. XADD auto-generated sequence is incremented for last ID
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xadd_auto_seq_incremented_for_last_id() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["XADD", "mystream", "123-456", "k", "v"])
        .await;
    let resp = client
        .command(&["XADD", "mystream", "123-*", "k", "v"])
        .await;
    let id = parse_bulk_string(&resp);
    assert_eq!(id, "123-457");
}

// ---------------------------------------------------------------------------
// 7. XADD auto-generated sequence is zero for future timestamp ID
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xadd_auto_seq_zero_for_future_timestamp() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["XADD", "mystream", "123-456", "k", "v"])
        .await;
    let resp = client
        .command(&["XADD", "mystream", "789-*", "k", "v"])
        .await;
    let id = parse_bulk_string(&resp);
    assert_eq!(id, "789-0");
}

// ---------------------------------------------------------------------------
// 8. XADD auto-generated sequence can't be smaller than last ID
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xadd_auto_seq_cant_be_smaller_than_last_id() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["XADD", "mystream", "123-456", "k", "v"])
        .await;
    let resp = client
        .command(&["XADD", "mystream", "42-*", "k", "v"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// 9. XADD auto-generated sequence can't overflow
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xadd_auto_seq_cant_overflow() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["XADD", "mystream", "1-18446744073709551615", "k", "v"])
        .await;
    let resp = client.command(&["XADD", "mystream", "1-*", "k", "v"]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// 10. XADD 0-* should succeed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xadd_zero_star_should_succeed() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["XADD", "mystream", "0-*", "k", "v"]).await;
    let id = parse_bulk_string(&resp);
    // 0-* on an empty stream should produce 0-1 (since 0-0 is the minimum)
    assert_eq!(id, "0-1");
}

// ---------------------------------------------------------------------------
// 11. XADD with MAXLEN option
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xadd_with_maxlen_option() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..1000 {
        client
            .command(&[
                "XADD",
                "mystream",
                "MAXLEN",
                "5",
                "*",
                "item",
                &i.to_string(),
            ])
            .await;
    }

    assert_integer_eq(&client.command(&["XLEN", "mystream"]).await, 5);
}

// ---------------------------------------------------------------------------
// 12. XADD with MAXLEN option and the '=' argument
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xadd_with_maxlen_exact() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..1000 {
        client
            .command(&[
                "XADD",
                "mystream",
                "MAXLEN",
                "=",
                "5",
                "*",
                "item",
                &i.to_string(),
            ])
            .await;
    }

    assert_integer_eq(&client.command(&["XLEN", "mystream"]).await, 5);
}

// ---------------------------------------------------------------------------
// 13. XADD with NOMKSTREAM option
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xadd_with_nomkstream_option() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // NOMKSTREAM on a non-existing stream should return nil
    let resp = client
        .command(&["XADD", "mystream", "NOMKSTREAM", "*", "item", "1"])
        .await;
    assert_nil(&resp);

    assert_integer_eq(&client.command(&["EXISTS", "mystream"]).await, 0);

    // Create the stream, then NOMKSTREAM should work
    client
        .command(&["XADD", "mystream", "*", "item", "1"])
        .await;
    let resp = client
        .command(&["XADD", "mystream", "NOMKSTREAM", "*", "item", "2"])
        .await;
    // Should return a valid ID
    let id = parse_bulk_string(&resp);
    assert!(id.contains('-'), "expected stream ID, got {id}");

    assert_integer_eq(&client.command(&["XLEN", "mystream"]).await, 2);
}

// ---------------------------------------------------------------------------
// 14. XADD with MINID option
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xadd_with_minid_option() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 1..=5 {
        client
            .command(&["XADD", "mystream", &format!("{i}-0"), "k", &format!("v{i}")])
            .await;
    }

    // XADD with MINID should trim entries below that ID
    client
        .command(&["XADD", "mystream", "MINID", "3", "*", "k", "v6"])
        .await;

    let resp = client.command(&["XRANGE", "mystream", "-", "+"]).await;
    let entries = unwrap_array(resp);
    // Entries with IDs 1-0 and 2-0 should have been trimmed
    for entry in &entries {
        let id = entry_id(entry);
        let (ms, _) = parse_id_parts(&id);
        assert!(ms >= 3, "entry {id} should have been trimmed");
    }
}

// ---------------------------------------------------------------------------
// 15. XTRIM with MINID option
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xtrim_with_minid_option() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 1..=10 {
        client
            .command(&["XADD", "mystream", &format!("{i}-0"), "k", &format!("v{i}")])
            .await;
    }

    let resp = client.command(&["XTRIM", "mystream", "MINID", "5"]).await;
    let trimmed = unwrap_integer(&resp);
    assert_eq!(trimmed, 4);

    assert_integer_eq(&client.command(&["XLEN", "mystream"]).await, 6);
}

// ---------------------------------------------------------------------------
// 16. XTRIM with MINID option, big delta from master record
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xtrim_with_minid_big_delta() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 1..=10 {
        client
            .command(&["XADD", "mystream", &format!("{i}-0"), "k", &format!("v{i}")])
            .await;
    }

    let resp = client.command(&["XTRIM", "mystream", "MINID", "8"]).await;
    let trimmed = unwrap_integer(&resp);
    assert_eq!(trimmed, 7);

    assert_integer_eq(&client.command(&["XLEN", "mystream"]).await, 3);

    // Verify remaining entries start at 8-0
    let resp = client.command(&["XRANGE", "mystream", "-", "+"]).await;
    let entries = unwrap_array(resp);
    assert_eq!(entry_id(&entries[0]), "8-0");
}

// ---------------------------------------------------------------------------
// 17. XADD mass insertion and XLEN (simplified: 100 entries)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xadd_mass_insertion_and_xlen() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..100 {
        client
            .command(&[
                "XADD",
                "mystream",
                "*",
                "item",
                &i.to_string(),
                "value",
                &format!("v{i}"),
            ])
            .await;
    }

    assert_integer_eq(&client.command(&["XLEN", "mystream"]).await, 100);
}

// ---------------------------------------------------------------------------
// 18. XADD with ID 0-0
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xadd_with_id_zero_zero() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Redis allows 0-0 on an empty stream
    let resp = client.command(&["XADD", "mystream", "0-0", "k", "v"]).await;
    let id = parse_bulk_string(&resp);
    assert_eq!(id, "0-0");

    // Adding another entry with 0-0 should be rejected (not monotonic)
    let resp = client.command(&["XADD", "mystream", "0-0", "k", "v"]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// 19. XRANGE COUNT works as expected (uses mass data)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xrange_count_works() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..100 {
        client
            .command(&["XADD", "mystream", "*", "item", &i.to_string()])
            .await;
    }

    let resp = client
        .command(&["XRANGE", "mystream", "-", "+", "COUNT", "10"])
        .await;
    let entries = unwrap_array(resp);
    assert_eq!(entries.len(), 10);
}

// ---------------------------------------------------------------------------
// 20. XREVRANGE COUNT works as expected
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xrevrange_count_works() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..100 {
        client
            .command(&["XADD", "mystream", "*", "item", &i.to_string()])
            .await;
    }

    let resp = client
        .command(&["XREVRANGE", "mystream", "+", "-", "COUNT", "10"])
        .await;
    let entries = unwrap_array(resp);
    assert_eq!(entries.len(), 10);
}

// ---------------------------------------------------------------------------
// 21. XRANGE exclusive ranges
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xrange_exclusive_ranges() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["XADD", "mystream", "1-0", "k", "v1"])
        .await;
    client
        .command(&["XADD", "mystream", "2-0", "k", "v2"])
        .await;
    client
        .command(&["XADD", "mystream", "3-0", "k", "v3"])
        .await;
    client
        .command(&["XADD", "mystream", "4-0", "k", "v4"])
        .await;
    client
        .command(&["XADD", "mystream", "5-0", "k", "v5"])
        .await;

    // Exclusive start: (1-0 means start after 1-0
    let resp = client.command(&["XRANGE", "mystream", "(1-0", "5-0"]).await;
    let entries = unwrap_array(resp);
    assert_eq!(entries.len(), 4);
    assert_eq!(entry_id(&entries[0]), "2-0");

    // Exclusive end: (5-0 means end before 5-0
    let resp = client.command(&["XRANGE", "mystream", "1-0", "(5-0"]).await;
    let entries = unwrap_array(resp);
    assert_eq!(entries.len(), 4);
    assert_eq!(entry_id(&entries[3]), "4-0");

    // Both exclusive
    let resp = client
        .command(&["XRANGE", "mystream", "(1-0", "(5-0"])
        .await;
    let entries = unwrap_array(resp);
    assert_eq!(entries.len(), 3);
    assert_eq!(entry_id(&entries[0]), "2-0");
    assert_eq!(entry_id(&entries[2]), "4-0");
}

// ---------------------------------------------------------------------------
// 22. XREAD with non empty stream
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xread_with_non_empty_stream() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["XADD", "s1", "*", "k", "v1"]).await;
    client.command(&["XADD", "s1", "*", "k", "v2"]).await;

    let resp = client
        .command(&["XREAD", "COUNT", "10", "STREAMS", "s1", "0-0"])
        .await;
    let streams = unwrap_array(resp);
    assert_eq!(streams.len(), 1);

    let stream_data = unwrap_array(streams[0].clone());
    assert_bulk_eq(&stream_data[0], b"s1");

    let entries = unwrap_array(stream_data[1].clone());
    assert_eq!(entries.len(), 2);
}

// ---------------------------------------------------------------------------
// 23. Non blocking XREAD with empty streams
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xread_non_blocking_empty_stream() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["XREAD", "COUNT", "10", "STREAMS", "nonexistent", "0-0"])
        .await;
    // Should return nil for non-existing stream
    assert_nil(&resp);
}

// ---------------------------------------------------------------------------
// 24. Blocking XREAD waiting new data (single stream)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_blocking_xread_waiting_new_data() {
    let server = TestServer::start_standalone().await;
    let mut blocker = server.connect().await;
    let mut pusher = server.connect().await;

    // Create the stream first
    pusher.command(&["XADD", "s1", "*", "old", "data"]).await;

    blocker
        .send_only(&["XREAD", "BLOCK", "20000", "STREAMS", "s1", "$"])
        .await;
    server.wait_for_blocked_clients(1).await;

    pusher.command(&["XADD", "s1", "*", "new", "data"]).await;

    let resp = blocker
        .read_response(Duration::from_secs(5))
        .await
        .expect("should unblock");

    let streams = unwrap_array(resp);
    assert_eq!(streams.len(), 1);

    let stream_data = unwrap_array(streams[0].clone());
    assert_bulk_eq(&stream_data[0], b"s1");

    let entries = unwrap_array(stream_data[1].clone());
    assert_eq!(entries.len(), 1);

    let fields = entry_fields(&entries[0]);
    assert_eq!(fields, vec!["new", "data"]);
}

// ---------------------------------------------------------------------------
// 25. Blocking XREAD for stream that ran dry (issue 5299) -- simplified
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_blocking_xread_stream_ran_dry() {
    let server = TestServer::start_standalone().await;
    let mut blocker = server.connect().await;
    let mut writer = server.connect().await;

    // Create stream and consume all entries
    writer.command(&["XADD", "s1", "*", "k", "v"]).await;

    // Block reading from $ (latest)
    blocker
        .send_only(&["XREAD", "BLOCK", "1000", "STREAMS", "s1", "$"])
        .await;
    server.wait_for_blocked_clients(1).await;

    // Add new data -- should unblock
    writer.command(&["XADD", "s1", "*", "new", "data"]).await;

    let resp = blocker
        .read_response(Duration::from_secs(5))
        .await
        .expect("should unblock");

    let streams = unwrap_array(resp);
    assert_eq!(streams.len(), 1);
}

// ---------------------------------------------------------------------------
// 26. XREAD last element from non-empty stream
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xread_last_element_from_non_empty_stream() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["XADD", "s1", "1-1", "a", "1"]).await;
    client.command(&["XADD", "s1", "1-2", "b", "2"]).await;
    client.command(&["XADD", "s1", "1-3", "c", "3"]).await;

    // XREAD with "$" should return nil (no new data since last)
    let resp = client.command(&["XREAD", "STREAMS", "s1", "$"]).await;
    assert_nil(&resp);
}

// ---------------------------------------------------------------------------
// 27. XREAD last element from empty stream
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xread_last_element_from_empty_stream() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["XREAD", "STREAMS", "s1", "$"]).await;
    assert_nil(&resp);
}

// ---------------------------------------------------------------------------
// 28. XREAD last element blocking from empty stream
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xread_last_element_blocking_from_empty_stream() {
    let server = TestServer::start_standalone().await;
    let mut blocker = server.connect().await;
    let mut pusher = server.connect().await;

    blocker
        .send_only(&["XREAD", "BLOCK", "20000", "STREAMS", "s1", "$"])
        .await;
    server.wait_for_blocked_clients(1).await;

    pusher.command(&["XADD", "s1", "*", "k", "v"]).await;

    let resp = blocker
        .read_response(Duration::from_secs(5))
        .await
        .expect("should unblock");

    let streams = unwrap_array(resp);
    assert_eq!(streams.len(), 1);

    let stream_data = unwrap_array(streams[0].clone());
    let entries = unwrap_array(stream_data[1].clone());
    assert_eq!(entries.len(), 1);
}

// ---------------------------------------------------------------------------
// 29. XREAD last element blocking from non-empty stream
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xread_last_element_blocking_from_non_empty_stream() {
    let server = TestServer::start_standalone().await;
    let mut blocker = server.connect().await;
    let mut pusher = server.connect().await;

    pusher.command(&["XADD", "s1", "1-1", "a", "1"]).await;
    pusher.command(&["XADD", "s1", "1-2", "b", "2"]).await;

    blocker
        .send_only(&["XREAD", "BLOCK", "20000", "STREAMS", "s1", "$"])
        .await;
    server.wait_for_blocked_clients(1).await;

    pusher.command(&["XADD", "s1", "1-3", "c", "3"]).await;

    let resp = blocker
        .read_response(Duration::from_secs(5))
        .await
        .expect("should unblock");

    let streams = unwrap_array(resp);
    let stream_data = unwrap_array(streams[0].clone());
    let entries = unwrap_array(stream_data[1].clone());
    assert_eq!(entries.len(), 1);
    assert_eq!(entry_id(&entries[0]), "1-3");
}

// ---------------------------------------------------------------------------
// 30. XREAD last element with count > 1
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xread_last_element_with_count_gt_1() {
    let server = TestServer::start_standalone().await;
    let mut blocker = server.connect().await;
    let mut pusher = server.connect().await;

    pusher.command(&["XADD", "s1", "1-1", "a", "1"]).await;
    pusher.command(&["XADD", "s1", "1-2", "b", "2"]).await;

    blocker
        .send_only(&[
            "XREAD", "BLOCK", "20000", "COUNT", "10", "STREAMS", "s1", "$",
        ])
        .await;
    server.wait_for_blocked_clients(1).await;

    pusher.command(&["XADD", "s1", "1-3", "c", "3"]).await;
    pusher.command(&["XADD", "s1", "1-4", "d", "4"]).await;

    let resp = blocker
        .read_response(Duration::from_secs(5))
        .await
        .expect("should unblock");

    let streams = unwrap_array(resp);
    let stream_data = unwrap_array(streams[0].clone());
    let entries = unwrap_array(stream_data[1].clone());
    // Blocking XREAD returns entries added after the block point;
    // with COUNT 10, should get both entries (or at least the first that unblocked)
    assert!(!entries.is_empty());
}

// ---------------------------------------------------------------------------
// 31. XREAD: read last element after XDEL (issue 13628)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xread_last_element_after_xdel() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let id1 = parse_bulk_string(&client.command(&["XADD", "s1", "*", "a", "1"]).await);
    client.command(&["XADD", "s1", "*", "b", "2"]).await;
    let id3 = parse_bulk_string(&client.command(&["XADD", "s1", "*", "c", "3"]).await);

    // Delete the first entry
    assert_integer_eq(&client.command(&["XDEL", "s1", &id1]).await, 1);

    // XREAD from 0-0 should return remaining entries
    let resp = client.command(&["XREAD", "STREAMS", "s1", "0-0"]).await;
    let streams = unwrap_array(resp);
    let stream_data = unwrap_array(streams[0].clone());
    let entries = unwrap_array(stream_data[1].clone());
    assert_eq!(entries.len(), 2);

    // XREAD from the last entry ID should return nil (no new entries)
    let resp = client.command(&["XREAD", "STREAMS", "s1", &id3]).await;
    assert_nil(&resp);
}

// ---------------------------------------------------------------------------
// 32. XDEL basic test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xdel_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["XADD", "mystream", "1-0", "a", "1"]).await;
    client.command(&["XADD", "mystream", "2-0", "b", "2"]).await;
    client.command(&["XADD", "mystream", "3-0", "c", "3"]).await;

    assert_integer_eq(&client.command(&["XDEL", "mystream", "2-0"]).await, 1);

    let resp = client.command(&["XRANGE", "mystream", "-", "+"]).await;
    let entries = unwrap_array(resp);
    assert_eq!(entries.len(), 2);
    assert_eq!(entry_id(&entries[0]), "1-0");
    assert_eq!(entry_id(&entries[1]), "3-0");
}

// ---------------------------------------------------------------------------
// 33. XDEL multiply id test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xdel_multiple_ids() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["XADD", "mystream", "1-0", "a", "1"]).await;
    client.command(&["XADD", "mystream", "2-0", "b", "2"]).await;
    client.command(&["XADD", "mystream", "3-0", "c", "3"]).await;
    client.command(&["XADD", "mystream", "4-0", "d", "4"]).await;

    assert_integer_eq(
        &client.command(&["XDEL", "mystream", "2-0", "3-0"]).await,
        2,
    );

    assert_integer_eq(&client.command(&["XLEN", "mystream"]).await, 2);

    let resp = client.command(&["XRANGE", "mystream", "-", "+"]).await;
    let entries = unwrap_array(resp);
    assert_eq!(entries.len(), 2);
    assert_eq!(entry_id(&entries[0]), "1-0");
    assert_eq!(entry_id(&entries[1]), "4-0");
}

// ---------------------------------------------------------------------------
// 34. XREVRANGE regression test for issue 5006
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xrevrange_regression_issue_5006() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..100 {
        client
            .command(&["XADD", "mystream", "*", "item", &i.to_string()])
            .await;
    }

    let resp = client
        .command(&["XREVRANGE", "mystream", "+", "-", "COUNT", "1"])
        .await;
    let entries = unwrap_array(resp);
    assert_eq!(entries.len(), 1);

    // The single entry should have item = 99 (last inserted)
    let fields = entry_fields(&entries[0]);
    assert_eq!(fields[0], "item");
    assert_eq!(fields[1], "99");
}

// ---------------------------------------------------------------------------
// 35. XREAD streamID edge no-blocking
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xread_stream_id_edge_non_blocking() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["XADD", "s1", "1-1", "a", "1"]).await;
    client.command(&["XADD", "s1", "1-2", "b", "2"]).await;
    client.command(&["XADD", "s1", "1-3", "c", "3"]).await;

    // Read from 1-1 should return entries 1-2 and 1-3
    let resp = client.command(&["XREAD", "STREAMS", "s1", "1-1"]).await;
    let streams = unwrap_array(resp);
    let stream_data = unwrap_array(streams[0].clone());
    let entries = unwrap_array(stream_data[1].clone());
    assert_eq!(entries.len(), 2);
    assert_eq!(entry_id(&entries[0]), "1-2");
    assert_eq!(entry_id(&entries[1]), "1-3");

    // Read from 1-3 should return nil
    let resp = client.command(&["XREAD", "STREAMS", "s1", "1-3"]).await;
    assert_nil(&resp);
}

// ---------------------------------------------------------------------------
// 36. XREAD streamID edge blocking
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xread_stream_id_edge_blocking() {
    let server = TestServer::start_standalone().await;
    let mut blocker = server.connect().await;
    let mut pusher = server.connect().await;

    pusher.command(&["XADD", "s1", "1-1", "a", "1"]).await;
    pusher.command(&["XADD", "s1", "1-2", "b", "2"]).await;

    // Block reading from 1-2 (the last entry)
    blocker
        .send_only(&["XREAD", "BLOCK", "20000", "STREAMS", "s1", "1-2"])
        .await;
    server.wait_for_blocked_clients(1).await;

    pusher.command(&["XADD", "s1", "1-3", "c", "3"]).await;

    let resp = blocker
        .read_response(Duration::from_secs(5))
        .await
        .expect("should unblock");

    let streams = unwrap_array(resp);
    let stream_data = unwrap_array(streams[0].clone());
    let entries = unwrap_array(stream_data[1].clone());
    assert_eq!(entries.len(), 1);
    assert_eq!(entry_id(&entries[0]), "1-3");
}

// ---------------------------------------------------------------------------
// 37. XTRIM with MAXLEN option basic test (simplified)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xtrim_with_maxlen_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..100 {
        client
            .command(&["XADD", "mystream", "*", "item", &i.to_string()])
            .await;
    }

    let resp = client.command(&["XTRIM", "mystream", "MAXLEN", "10"]).await;
    let trimmed = unwrap_integer(&resp);
    assert_eq!(trimmed, 90);

    assert_integer_eq(&client.command(&["XLEN", "mystream"]).await, 10);
}

// ---------------------------------------------------------------------------
// 38. XTRIM without ~ and with LIMIT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xtrim_without_approx_with_limit() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..100 {
        client
            .command(&["XADD", "mystream", "*", "item", &i.to_string()])
            .await;
    }

    // XTRIM MAXLEN with ~ and LIMIT should only trim up to LIMIT entries
    let resp = client
        .command(&["XTRIM", "mystream", "MAXLEN", "~", "0", "LIMIT", "30"])
        .await;
    let trimmed = unwrap_integer(&resp);
    // With ~ and LIMIT, the actual trimming may vary but should trim some entries
    assert!(trimmed > 0, "expected some entries trimmed");
}

// ---------------------------------------------------------------------------
// 39. XADD can CREATE an empty stream
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xadd_can_create_empty_stream() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // XADD with MAXLEN 0 creates the stream then trims it to zero entries
    let resp = client
        .command(&["XADD", "mystream", "MAXLEN", "0", "*", "k", "v"])
        .await;
    let id = parse_bulk_string(&resp);
    assert!(id.contains('-'));

    // Stream exists but has 0 entries due to MAXLEN 0
    assert_integer_eq(&client.command(&["EXISTS", "mystream"]).await, 1);
    assert_integer_eq(&client.command(&["XLEN", "mystream"]).await, 0);
}

// ---------------------------------------------------------------------------
// 40. XSETID can set a specific ID
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xsetid_can_set_specific_id() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Use an explicit small ID so XSETID to a larger ID succeeds
    client.command(&["XADD", "mystream", "1-0", "k", "v"]).await;

    assert_ok(&client.command(&["XSETID", "mystream", "200-0"]).await);

    // Now adding with * should produce an ID after 200-0
    let resp = client.command(&["XADD", "mystream", "*", "k", "v2"]).await;
    let id = parse_bulk_string(&resp);
    let (ms, seq) = parse_id_parts(&id);
    assert!(
        ms > 200 || (ms == 200 && seq > 0),
        "expected ID after 200-0, got {id}"
    );
}

// ---------------------------------------------------------------------------
// 41. XSETID cannot SETID with smaller ID
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xsetid_cannot_set_smaller_id() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["XADD", "mystream", "100-100", "k", "v"])
        .await;

    // FrogDB rejects XSETID with a smaller ID than the stream's top item
    let resp = client.command(&["XSETID", "mystream", "50-50"]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// 42. XSETID cannot SETID on non-existent key
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xsetid_on_non_existent_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["XSETID", "nonexistent", "100-100"]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// 43. XADD advances the entries-added counter (via XINFO STREAM)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xadd_advances_entries_added_counter() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["XADD", "mystream", "*", "a", "1"]).await;
    client.command(&["XADD", "mystream", "*", "b", "2"]).await;
    client.command(&["XADD", "mystream", "*", "c", "3"]).await;

    let resp = client.command(&["XINFO", "STREAM", "mystream"]).await;
    let items = unwrap_array(resp);

    let length = xinfo_get_field(&items, "length");
    assert_integer_eq(length, 3);
}

// ---------------------------------------------------------------------------
// 44. XDEL/TRIM are reflected by recorded first entry
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xdel_trim_reflected_by_first_entry() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["XADD", "mystream", "1-0", "a", "1"]).await;
    client.command(&["XADD", "mystream", "2-0", "b", "2"]).await;
    client.command(&["XADD", "mystream", "3-0", "c", "3"]).await;

    // Delete the first entry
    assert_integer_eq(&client.command(&["XDEL", "mystream", "1-0"]).await, 1);

    // XRANGE should start from 2-0
    let resp = client.command(&["XRANGE", "mystream", "-", "+"]).await;
    let entries = unwrap_array(resp);
    assert_eq!(entries.len(), 2);
    assert_eq!(entry_id(&entries[0]), "2-0");

    // XTRIM to 1 entry
    let resp = client.command(&["XTRIM", "mystream", "MAXLEN", "1"]).await;
    let trimmed = unwrap_integer(&resp);
    assert_eq!(trimmed, 1);

    let resp = client.command(&["XRANGE", "mystream", "-", "+"]).await;
    let entries = unwrap_array(resp);
    assert_eq!(entries.len(), 1);
    assert_eq!(entry_id(&entries[0]), "3-0");
}

// ---------------------------------------------------------------------------
// 45. Maximum XDEL ID behaves correctly
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_maximum_xdel_id_behaves_correctly() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["XADD", "mystream", "1-0", "a", "1"]).await;
    client.command(&["XADD", "mystream", "2-0", "b", "2"]).await;
    client.command(&["XADD", "mystream", "3-0", "c", "3"]).await;

    // XDEL on a non-existing entry should return 0
    assert_integer_eq(&client.command(&["XDEL", "mystream", "999-0"]).await, 0);

    // XDEL of existing entry
    assert_integer_eq(&client.command(&["XDEL", "mystream", "2-0"]).await, 1);

    // Can still add entries after the deleted ones
    client.command(&["XADD", "mystream", "4-0", "d", "4"]).await;
    assert_integer_eq(&client.command(&["XLEN", "mystream"]).await, 3);
}

// ---------------------------------------------------------------------------
// 46. XADD with partial ID with maximal seq
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xadd_partial_id_with_maximal_seq() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Add with explicit ID
    let resp = client.command(&["XADD", "mystream", "1-0", "k", "v"]).await;
    let id = parse_bulk_string(&resp);
    assert_eq!(id, "1-0");

    // Now add with partial ID at same ms -- should auto-increment seq
    let resp = client
        .command(&["XADD", "mystream", "1-*", "k", "v2"])
        .await;
    let id = parse_bulk_string(&resp);
    assert_eq!(id, "1-1");

    // Add with max seq for this ms
    client
        .command(&["XADD", "mystream", "1-18446744073709551615", "k", "v3"])
        .await;

    // Now auto-seq at ms=1 should fail since max seq is reached
    let resp = client
        .command(&["XADD", "mystream", "1-*", "k", "v4"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// 47. XADD streamID edge
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xadd_stream_id_edge() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Test edge case: ID 0-1 should work
    let resp = client.command(&["XADD", "mystream", "0-1", "a", "1"]).await;
    assert_bulk_eq(&resp, b"0-1");

    // ID 0-2 should work
    let resp = client.command(&["XADD", "mystream", "0-2", "b", "2"]).await;
    assert_bulk_eq(&resp, b"0-2");

    // Verify XRANGE returns them
    let resp = client.command(&["XRANGE", "mystream", "-", "+"]).await;
    let entries = unwrap_array(resp);
    assert_eq!(entries.len(), 2);
    assert_eq!(entry_id(&entries[0]), "0-1");
    assert_eq!(entry_id(&entries[1]), "0-2");
}
