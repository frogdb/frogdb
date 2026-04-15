//! Rust port of Redis 8.6.0 `unit/type/stream-cgroups.tcl` test suite.
//!
//! Excludes: needs:debug, needs:repl, external:skip, SELECT/SWAPDB, FLUSHDB,
//! CONFIG SET, consumer group lag tests (lines 1319-1519), legacy persistence
//! loading (lines 1520+), replication (lines 1569+), AOF-related (lines 1272-1318),
//! XREADGROUP dirty tests (require WAIT/DEBUG), multi-stream blocking XREADGROUP
//! (FrogDB only supports single stream).
//!
//! ## Intentional exclusions
//!
//! XREADGROUP CLAIM (Redis 8.x extended consumer-claim feature, not implemented):
//! - `XREADGROUP CLAIM field types are correct` — redis-specific — Redis-internal feature
//! - `XREADGROUP CLAIM respects min-idle-time threshold` — redis-specific — Redis-internal feature
//! - `XREADGROUP CLAIM without messages` — redis-specific — Redis-internal feature
//! - `XREADGROUP CLAIM without pending messages` — redis-specific — Redis-internal feature
//! - `XREADGROUP CLAIM message response format` — redis-specific — Redis-internal feature
//! - `XREADGROUP CLAIM idle time` — redis-specific — Redis-internal feature
//! - `XREADGROUP CLAIM with NOACK` — redis-specific — Redis-internal feature
//! - `XREADGROUP CLAIM with NOACK and pending messages` — redis-specific — Redis-internal feature
//! - `XREADGROUP CLAIM with min-idle-time equal to zero` — redis-specific — Redis-internal feature
//! - `XREADGROUP CLAIM with large min-idle-time` — redis-specific — Redis-internal feature
//! - `XREADGROUP CLAIM with not integer for min-idle-time` — redis-specific — Redis-internal feature
//! - `XREADGROUP CLAIM with negative integer for min-idle-time` — redis-specific — Redis-internal feature
//! - `XREADGROUP CLAIM with different position` — redis-specific — Redis-internal feature
//! - `XREADGROUP CLAIM verify forced entries are claimable` — redis-specific — Redis-internal feature
//! - `XREADGROUP CLAIM with two blocked clients` — redis-specific — Redis-internal feature
//! - `XREADGROUP CLAIM claims all pending immediately` — redis-specific — Redis-internal feature
//! - `XREADGROUP CLAIM verify claiming order` — redis-specific — Redis-internal feature
//! - `XREADGROUP CLAIM when pending messages get trimmed` — redis-specific — Redis-internal feature
//! - `XREADGROUP CLAIM state persists across RDB save/load` — redis-specific — Redis-internal feature
//! - `XREADGROUP CLAIM idle time resets after RDB reload` — redis-specific — Redis-internal feature
//! - `XREADGROUP CLAIM multiple groups persist correctly` — redis-specific — Redis-internal feature
//! - `XREADGROUP CLAIM NOACK state not persisted` — redis-specific — Redis-internal feature
//! - `XREADGROUP CLAIM high delivery counts persist in RDB` — redis-specific — Redis-internal feature
//! - `XREADGROUP CLAIM usage stability with repeated claims` — redis-specific — Redis-internal feature
//! - `XREADGROUP CLAIM with large number of PEL messages` — redis-specific — Redis-internal feature
//! - `XREADGROUP CLAIM within MULTI/EXEC transaction` — redis-specific — Redis-internal feature
//! - `XREAD with CLAIM option` — redis-specific — Redis-internal feature
//!
//! XACKDEL (Redis 8.x ack-and-delete command, not implemented):
//! - `XACKDEL wrong number of args` — redis-specific — Redis-internal feature
//! - `XACKDEL with DELREF option acknowledges will remove entry from all PELs` — redis-specific — Redis-internal feature
//! - `XACKDEL with ACKED option only deletes messages acknowledged by all groups` — redis-specific — Redis-internal feature
//! - `XACKDEL with KEEPREF` — redis-specific — Redis-internal feature
//! - `XACKDEL with IDs exceeding STREAMID_STATIC_VECTOR_LEN for heap allocation` — redis-specific — Redis-internal feature
//!
//! Other Redis 8.x stream features and Redis-internal mechanics:
//! - `XGROUP SETID with ENTRIESREAD larger than stream entries should cap the value` — redis-specific — Redis-internal feature
//! - `XREADGROUP of multiple entries changes dirty by one` — redis-specific — Redis-internal stat (dirty counter)
//! - `XREADGROUP from PEL does not change dirty` — redis-specific — Redis-internal stat (dirty counter)
//!
//! Legacy persistence-format tests (FrogDB uses RocksDB, not RDB):
//! - `Loading from legacy (Redis <= v6.2.x, rdb_ver < 10) persistence` — intentional-incompatibility:encoding — internal-encoding (RDB)
//! - `Loading from legacy (Redis <= v7.0.x, rdb_ver < 11) persistence` — intentional-incompatibility:encoding — internal-encoding (RDB)

use std::time::Duration;

use frogdb_protocol::Response;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_bulk_string(resp: &Response) -> String {
    String::from_utf8(unwrap_bulk(resp).to_vec()).unwrap()
}

/// Find a group by name in the XINFO STREAM FULL groups array.
fn find_group_by_name<'a>(groups: &'a [Response], name: &str) -> &'a [Response] {
    for g in groups {
        if let Response::Array(arr) = g {
            let gname = parse_bulk_string(xinfo_get_field(arr, "name"));
            if gname == name {
                return arr;
            }
        }
    }
    panic!("group {name:?} not found in XINFO STREAM FULL groups");
}

/// Extract entries from an XREADGROUP response for a single stream.
fn xreadgroup_entries(resp: Response) -> Vec<Response> {
    let streams = unwrap_array(resp);
    let stream_data = unwrap_array(streams.into_iter().next().unwrap());
    unwrap_array(stream_data.into_iter().nth(1).unwrap())
}

/// Extract entry ID from a stream entry [id, [fields...]].
fn entry_id(entry: &Response) -> String {
    match entry {
        Response::Array(arr) => parse_bulk_string(&arr[0]),
        _ => panic!("expected array entry"),
    }
}

/// Extract entry fields as strings from a stream entry [id, [fields...]].
fn entry_fields(entry: &Response) -> Vec<String> {
    match entry {
        Response::Array(arr) => extract_bulk_strings(&arr[1]),
        _ => panic!("expected array entry"),
    }
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
// 1. XGROUP CREATE: creation and duplicate group name detection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xgroup_create_and_duplicate_group() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    client
        .command(&["XADD", "mystream", "*", "foo", "bar"])
        .await;
    assert_ok(
        &client
            .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$"])
            .await,
    );

    // Duplicate group name should error
    let resp = client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$"])
        .await;
    assert_error_prefix(&resp, "BUSYGROUP");
}

// ---------------------------------------------------------------------------
// 2. XGROUP CREATE: automatic stream creation fails without MKSTREAM
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xgroup_create_fails_without_mkstream() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    let resp = client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// 3. XGROUP CREATE: automatic stream creation works with MKSTREAM
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xgroup_create_works_with_mkstream() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    assert_ok(
        &client
            .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$", "MKSTREAM"])
            .await,
    );
}

// ---------------------------------------------------------------------------
// 4. XREADGROUP basic argument count validation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xreadgroup_basic_argument_count_validation() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["XREADGROUP"]).await;
    assert_error_prefix(&resp, "ERR");

    let resp = client.command(&["XREADGROUP", "GROUP"]).await;
    assert_error_prefix(&resp, "ERR");

    let resp = client.command(&["XREADGROUP", "GROUP", "mygroup"]).await;
    assert_error_prefix(&resp, "ERR");

    let resp = client
        .command(&["XREADGROUP", "GROUP", "mygroup", "consumer"])
        .await;
    assert_error_prefix(&resp, "ERR");

    let resp = client
        .command(&["XREADGROUP", "GROUP", "mygroup", "consumer", "STREAMS"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// 5. XREADGROUP GROUP keyword validation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xreadgroup_group_keyword_validation() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    client
        .command(&["XADD", "mystream", "*", "field", "value"])
        .await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$"])
        .await;

    // Wrong keyword instead of GROUP
    let resp = client
        .command(&[
            "XREADGROUP",
            "GROUPS",
            "mygroup",
            "consumer",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// 6. XREADGROUP empty group name handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xreadgroup_empty_group_name() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    client
        .command(&["XADD", "mystream", "*", "field", "value"])
        .await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$"])
        .await;

    let resp = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "",
            "consumer",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    assert_error_prefix(&resp, "NOGROUP");
}

// ---------------------------------------------------------------------------
// 7. XREADGROUP STREAMS keyword validation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xreadgroup_streams_keyword_validation() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    client
        .command(&["XADD", "mystream", "*", "field", "value"])
        .await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$"])
        .await;

    // Wrong keyword
    let resp = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer",
            "STREAM",
            "mystream",
            ">",
        ])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// 8. XREADGROUP stream and ID pairing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xreadgroup_stream_and_id_pairing() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    client
        .command(&["XADD", "mystream", "*", "field", "value"])
        .await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$"])
        .await;

    // Missing stream ID
    let resp = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer",
            "STREAMS",
            "mystream",
        ])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// 9. XREADGROUP COUNT parameter validation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xreadgroup_count_parameter_validation() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    client
        .command(&["XADD", "mystream", "*", "field", "value"])
        .await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$"])
        .await;

    // Non-numeric count
    let resp = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer",
            "COUNT",
            "abc",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    assert_error_prefix(&resp, "ERR");

    let resp = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer",
            "COUNT",
            "1.5",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// 10. XREADGROUP BLOCK parameter validation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xreadgroup_block_parameter_validation() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    client
        .command(&["XADD", "mystream", "*", "field", "value"])
        .await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$"])
        .await;

    // Non-numeric block timeout
    let resp = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer",
            "BLOCK",
            "abc",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    assert_error_prefix(&resp, "ERR");

    let resp = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer",
            "BLOCK",
            "1.5",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    assert_error_prefix(&resp, "ERR");

    // Negative timeout
    let resp = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer",
            "BLOCK",
            "-1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// 11. XREADGROUP stream ID format validation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xreadgroup_stream_id_format_validation() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    client
        .command(&["XADD", "mystream", "*", "field", "value"])
        .await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$"])
        .await;

    // Invalid ID formats
    for invalid_id in &["invalid-id", "abc-def", "123-abc"] {
        let resp = client
            .command(&[
                "XREADGROUP",
                "GROUP",
                "mygroup",
                "consumer",
                "STREAMS",
                "mystream",
                invalid_id,
            ])
            .await;
        assert_error_prefix(&resp, "ERR");
    }
}

// ---------------------------------------------------------------------------
// 12. XREADGROUP nonexistent group
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xreadgroup_nonexistent_group() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    client
        .command(&["XADD", "mystream", "*", "field", "value"])
        .await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$"])
        .await;

    let resp = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "nonexistent",
            "consumer",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    assert_error_prefix(&resp, "NOGROUP");
}

// ---------------------------------------------------------------------------
// 13. XREADGROUP wrong key type
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xreadgroup_wrong_key_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "wrongtype", "not a stream"]).await;
    let resp = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer",
            "STREAMS",
            "wrongtype",
            ">",
        ])
        .await;
    assert_error_prefix(&resp, "WRONGTYPE");
}

// ---------------------------------------------------------------------------
// 14. XREADGROUP will return only new elements
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xreadgroup_returns_only_new_elements() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    client
        .command(&["XADD", "mystream", "*", "foo", "bar"])
        .await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$"])
        .await;

    // Add new elements after group creation
    client.command(&["XADD", "mystream", "*", "a", "1"]).await;
    client.command(&["XADD", "mystream", "*", "b", "2"]).await;

    // XREADGROUP should return only the new elements
    let resp = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer-1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    let entries = xreadgroup_entries(resp);
    assert_eq!(entries.len(), 2);

    // First entry should have fields "a" "1"
    let fields = entry_fields(&entries[0]);
    assert_eq!(fields, vec!["a", "1"]);
}

// ---------------------------------------------------------------------------
// 15. XREADGROUP can read the history of the elements we own
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xreadgroup_can_read_history() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    client
        .command(&["XADD", "mystream", "*", "foo", "bar"])
        .await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$"])
        .await;

    // Add entries and read with consumer-1
    client.command(&["XADD", "mystream", "*", "a", "1"]).await;
    client.command(&["XADD", "mystream", "*", "b", "2"]).await;

    let resp = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer-1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    let entries = xreadgroup_entries(resp);
    assert_eq!(entries.len(), 2);

    // Add more entries and read with consumer-2
    client.command(&["XADD", "mystream", "*", "c", "3"]).await;
    client.command(&["XADD", "mystream", "*", "d", "4"]).await;

    let resp = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer-2",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    let entries = xreadgroup_entries(resp);
    assert_eq!(entries.len(), 2);
    let fields = entry_fields(&entries[0]);
    assert_eq!(fields, vec!["c", "3"]);

    // Read history for consumer-1 (should see a,b)
    let r1 = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer-1",
            "COUNT",
            "10",
            "STREAMS",
            "mystream",
            "0",
        ])
        .await;
    let entries1 = xreadgroup_entries(r1);
    let fields = entry_fields(&entries1[0]);
    assert_eq!(fields, vec!["a", "1"]);

    // Read history for consumer-2 (should see c,d)
    let r2 = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer-2",
            "COUNT",
            "10",
            "STREAMS",
            "mystream",
            "0",
        ])
        .await;
    let entries2 = xreadgroup_entries(r2);
    let fields = entry_fields(&entries2[0]);
    assert_eq!(fields, vec!["c", "3"]);
}

// ---------------------------------------------------------------------------
// 16. XPENDING is able to return pending items
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xpending_returns_pending_items() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    client
        .command(&["XADD", "mystream", "*", "foo", "bar"])
        .await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$"])
        .await;

    // Add entries and read with two consumers
    client.command(&["XADD", "mystream", "*", "a", "1"]).await;
    client.command(&["XADD", "mystream", "*", "b", "2"]).await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer-1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;

    client.command(&["XADD", "mystream", "*", "c", "3"]).await;
    client.command(&["XADD", "mystream", "*", "d", "4"]).await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer-2",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;

    let resp = client
        .command(&["XPENDING", "mystream", "mygroup", "-", "+", "10"])
        .await;
    let pending = unwrap_array(resp);
    assert_eq!(pending.len(), 4);

    // First two should be consumer-1, last two consumer-2
    for (j, entry) in pending.iter().enumerate().take(4) {
        let item = unwrap_array(entry.clone());
        let owner = parse_bulk_string(&item[1]);
        if j < 2 {
            assert_eq!(owner, "consumer-1");
        } else {
            assert_eq!(owner, "consumer-2");
        }
    }
}

// ---------------------------------------------------------------------------
// 17. XPENDING can return single consumer items
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xpending_single_consumer() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    client
        .command(&["XADD", "mystream", "*", "foo", "bar"])
        .await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$"])
        .await;

    client.command(&["XADD", "mystream", "*", "a", "1"]).await;
    client.command(&["XADD", "mystream", "*", "b", "2"]).await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer-1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;

    client.command(&["XADD", "mystream", "*", "c", "3"]).await;
    client.command(&["XADD", "mystream", "*", "d", "4"]).await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer-2",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;

    let resp = client
        .command(&[
            "XPENDING",
            "mystream",
            "mygroup",
            "-",
            "+",
            "10",
            "consumer-1",
        ])
        .await;
    let pending = unwrap_array(resp);
    assert_eq!(pending.len(), 2);
}

// ---------------------------------------------------------------------------
// 18. XPENDING only group (summary form)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xpending_only_group() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    client
        .command(&["XADD", "mystream", "*", "foo", "bar"])
        .await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$"])
        .await;

    client.command(&["XADD", "mystream", "*", "a", "1"]).await;
    client.command(&["XADD", "mystream", "*", "b", "2"]).await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer-1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;

    client.command(&["XADD", "mystream", "*", "c", "3"]).await;
    client.command(&["XADD", "mystream", "*", "d", "4"]).await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer-2",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;

    // Summary form: [count, min_id, max_id, [[consumer, count]...]]
    let resp = client.command(&["XPENDING", "mystream", "mygroup"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 4);

    // pending count should be 4
    assert_integer_eq(&arr[0], 4);

    // arr[3] should be array of consumer entries
    let consumers = unwrap_array(arr[3].clone());
    assert_eq!(consumers.len(), 2);
}

// ---------------------------------------------------------------------------
// 19. XPENDING with exclusive range intervals
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xpending_exclusive_range() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    client
        .command(&["XADD", "mystream", "*", "foo", "bar"])
        .await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$"])
        .await;

    client.command(&["XADD", "mystream", "*", "a", "1"]).await;
    client.command(&["XADD", "mystream", "*", "b", "2"]).await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer-1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;

    client.command(&["XADD", "mystream", "*", "c", "3"]).await;
    client.command(&["XADD", "mystream", "*", "d", "4"]).await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer-2",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;

    // Get all pending entries
    let resp = client
        .command(&["XPENDING", "mystream", "mygroup", "-", "+", "10"])
        .await;
    let pending = unwrap_array(resp);
    assert_eq!(pending.len(), 4);

    let startid = {
        let item = unwrap_array(pending[0].clone());
        parse_bulk_string(&item[0])
    };
    let endid = {
        let item = unwrap_array(pending[3].clone());
        parse_bulk_string(&item[0])
    };

    // Use exclusive range
    let start_exclusive = format!("({startid}");
    let end_exclusive = format!("({endid}");
    let resp = client
        .command(&[
            "XPENDING",
            "mystream",
            "mygroup",
            &start_exclusive,
            &end_exclusive,
            "10",
        ])
        .await;
    let expending = unwrap_array(resp);
    assert_eq!(expending.len(), 2);

    for entry in expending.iter().take(2) {
        let item = unwrap_array(entry.clone());
        let itemid = parse_bulk_string(&item[0]);
        assert_ne!(itemid, startid);
        assert_ne!(itemid, endid);
    }
}

// ---------------------------------------------------------------------------
// 20. XACK is able to remove items from the consumer/group PEL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xack_removes_from_pel() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    client
        .command(&["XADD", "mystream", "*", "foo", "bar"])
        .await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$"])
        .await;

    client.command(&["XADD", "mystream", "*", "a", "1"]).await;
    client.command(&["XADD", "mystream", "*", "b", "2"]).await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer-1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;

    client.command(&["XADD", "mystream", "*", "c", "3"]).await;
    client.command(&["XADD", "mystream", "*", "d", "4"]).await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer-2",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;

    // Get consumer-1's pending entries
    let resp = client
        .command(&[
            "XPENDING",
            "mystream",
            "mygroup",
            "-",
            "+",
            "10",
            "consumer-1",
        ])
        .await;
    let pending = unwrap_array(resp);
    let id1 = {
        let item = unwrap_array(pending[0].clone());
        parse_bulk_string(&item[0])
    };
    let id2 = {
        let item = unwrap_array(pending[1].clone());
        parse_bulk_string(&item[0])
    };

    // ACK id1
    assert_integer_eq(
        &client.command(&["XACK", "mystream", "mygroup", &id1]).await,
        1,
    );

    // consumer-1 should have 1 pending now
    let resp = client
        .command(&[
            "XPENDING",
            "mystream",
            "mygroup",
            "-",
            "+",
            "10",
            "consumer-1",
        ])
        .await;
    let pending = unwrap_array(resp);
    assert_eq!(pending.len(), 1);
    let remaining_id = {
        let item = unwrap_array(pending[0].clone());
        parse_bulk_string(&item[0])
    };
    assert_eq!(remaining_id, id2);

    // Global PEL should have 3
    let resp = client
        .command(&["XPENDING", "mystream", "mygroup", "-", "+", "10"])
        .await;
    let global_pel = unwrap_array(resp);
    assert_eq!(global_pel.len(), 3);
}

// ---------------------------------------------------------------------------
// 21. XACK can't remove the same item multiple times
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xack_no_double_remove() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    client.command(&["XADD", "mystream", "*", "a", "1"]).await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "0"])
        .await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer-1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;

    // Get the pending entry ID
    let resp = client
        .command(&[
            "XPENDING",
            "mystream",
            "mygroup",
            "-",
            "+",
            "10",
            "consumer-1",
        ])
        .await;
    let pending = unwrap_array(resp);
    let id1 = {
        let item = unwrap_array(pending[0].clone());
        parse_bulk_string(&item[0])
    };

    // First XACK should succeed
    assert_integer_eq(
        &client.command(&["XACK", "mystream", "mygroup", &id1]).await,
        1,
    );

    // Second XACK should return 0
    assert_integer_eq(
        &client.command(&["XACK", "mystream", "mygroup", &id1]).await,
        0,
    );
}

// ---------------------------------------------------------------------------
// 22. XACK is able to accept multiple arguments
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xack_multiple_arguments() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    client.command(&["XADD", "mystream", "*", "a", "1"]).await;
    client.command(&["XADD", "mystream", "*", "b", "2"]).await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "0"])
        .await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer-1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;

    // Get pending entry IDs
    let resp = client
        .command(&[
            "XPENDING",
            "mystream",
            "mygroup",
            "-",
            "+",
            "10",
            "consumer-1",
        ])
        .await;
    let pending = unwrap_array(resp);
    let id1 = {
        let item = unwrap_array(pending[0].clone());
        parse_bulk_string(&item[0])
    };
    let id2 = {
        let item = unwrap_array(pending[1].clone());
        parse_bulk_string(&item[0])
    };

    // ACK id1 first
    assert_integer_eq(
        &client.command(&["XACK", "mystream", "mygroup", &id1]).await,
        1,
    );

    // ACK both (id1 already acked, so should return 1 for id2 only)
    assert_integer_eq(
        &client
            .command(&["XACK", "mystream", "mygroup", &id1, &id2])
            .await,
        1,
    );
}

// ---------------------------------------------------------------------------
// 23. XACK should fail if got at least one invalid ID
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xack_fails_on_invalid_id() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "s"]).await;
    client
        .command(&["XGROUP", "CREATE", "s", "g", "$", "MKSTREAM"])
        .await;
    client.command(&["XADD", "s", "*", "f1", "v1"]).await;
    let resp = client
        .command(&["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"])
        .await;
    let entries = xreadgroup_entries(resp);
    assert_eq!(entries.len(), 1);

    let id1 = entry_id(&entries[0]);

    // XACK with an invalid ID should error
    let resp = client
        .command(&["XACK", "s", "g", &id1, "invalid-id"])
        .await;
    assert_error_prefix(&resp, "ERR");

    // But the valid ID should still be ackable
    assert_integer_eq(&client.command(&["XACK", "s", "g", &id1]).await, 1);
}

// ---------------------------------------------------------------------------
// 24. PEL NACK reassignment after XGROUP SETID event
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_pel_nack_reassignment_after_setid() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "events"]).await;
    client.command(&["XADD", "events", "*", "f1", "v1"]).await;
    client.command(&["XADD", "events", "*", "f1", "v1"]).await;
    client.command(&["XADD", "events", "*", "f1", "v1"]).await;
    client.command(&["XADD", "events", "*", "f1", "v1"]).await;
    client
        .command(&["XGROUP", "CREATE", "events", "g1", "$"])
        .await;

    client.command(&["XADD", "events", "*", "f1", "v1"]).await;

    let resp = client
        .command(&["XREADGROUP", "GROUP", "g1", "c1", "STREAMS", "events", ">"])
        .await;
    let entries = xreadgroup_entries(resp);
    assert_eq!(entries.len(), 1);

    // Reset the group's last delivered ID to the beginning
    client
        .command(&["XGROUP", "SETID", "events", "g1", "-"])
        .await;

    // Now reading with a new consumer should get all 5 entries
    let resp = client
        .command(&["XREADGROUP", "GROUP", "g1", "c2", "STREAMS", "events", ">"])
        .await;
    let entries = xreadgroup_entries(resp);
    assert_eq!(entries.len(), 5);
}

// ---------------------------------------------------------------------------
// 25. XREADGROUP will not report data on empty history (Bug #5577)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xreadgroup_empty_history_bug_5577() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "events"]).await;
    client.command(&["XADD", "events", "*", "a", "1"]).await;
    client.command(&["XADD", "events", "*", "b", "2"]).await;
    client.command(&["XADD", "events", "*", "c", "3"]).await;
    client
        .command(&["XGROUP", "CREATE", "events", "mygroup", "0"])
        .await;

    // PEL should be empty
    let resp = client
        .command(&["XPENDING", "events", "mygroup", "-", "+", "10"])
        .await;
    let pending = unwrap_array(resp);
    assert_eq!(pending.len(), 0);

    // XREADGROUP with history ID "0" should return empty entries
    let resp = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "myconsumer",
            "COUNT",
            "3",
            "STREAMS",
            "events",
            "0",
        ])
        .await;
    let streams = unwrap_array(resp);
    let stream_data = unwrap_array(streams.into_iter().next().unwrap());
    let entries = unwrap_array(stream_data.into_iter().nth(1).unwrap());
    assert_eq!(entries.len(), 0);

    // Fetch all with ">"
    let resp = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "myconsumer",
            "COUNT",
            "3",
            "STREAMS",
            "events",
            ">",
        ])
        .await;
    let entries = xreadgroup_entries(resp);
    assert_eq!(entries.len(), 3);

    // Now history should have 3 entries
    let resp = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "myconsumer",
            "COUNT",
            "3",
            "STREAMS",
            "events",
            "0",
        ])
        .await;
    let entries = xreadgroup_entries(resp);
    assert_eq!(entries.len(), 3);
}

// ---------------------------------------------------------------------------
// 26. XREADGROUP history reporting of deleted entries (Bug #5570)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xreadgroup_deleted_entries_bug_5570() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$", "MKSTREAM"])
        .await;
    client
        .command(&["XADD", "mystream", "1", "field1", "A"])
        .await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "myconsumer",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    client
        .command(&["XADD", "mystream", "MAXLEN", "1", "2", "field1", "B"])
        .await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "myconsumer",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;

    // Now read history starting from 0-1
    let resp = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "myconsumer",
            "STREAMS",
            "mystream",
            "0-1",
        ])
        .await;
    let entries = xreadgroup_entries(resp);
    assert_eq!(entries.len(), 2);

    // First entry (1-0) should have been deleted, so empty fields
    let id0 = entry_id(&entries[0]);
    assert_eq!(id0, "1-0");
    let fields0 = entry_fields(&entries[0]);
    assert_eq!(fields0.len(), 0); // deleted entry has empty fields

    // Second entry (2-0) should have field1=B
    let id1 = entry_id(&entries[1]);
    assert_eq!(id1, "2-0");
    let fields1 = entry_fields(&entries[1]);
    assert_eq!(fields1, vec!["field1", "B"]);
}

// ---------------------------------------------------------------------------
// 27. Blocking XREADGROUP will not reply with an empty array
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_blocking_xreadgroup_no_empty_array() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$", "MKSTREAM"])
        .await;
    client.command(&["XADD", "mystream", "666", "f", "v"]).await;

    // Non-blocking read should work and return the entry
    let resp = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "Alice",
            "BLOCK",
            "10",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    let entries = xreadgroup_entries(resp);
    assert_eq!(entries.len(), 1);
    let id = entry_id(&entries[0]);
    assert_eq!(id, "666-0");

    // Add and delete an entry, then block — should not get an empty array
    client
        .command(&["XADD", "mystream", "667", "f2", "v2"])
        .await;
    client.command(&["XDEL", "mystream", "667"]).await;

    let mut blocker = server.connect().await;
    blocker
        .send_only(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "Alice",
            "BLOCK",
            "100",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;

    // Should timeout with nil, not return an empty array
    let resp = blocker.read_response(Duration::from_millis(300)).await;
    match resp {
        None => {}                       // read_response itself timed out — acceptable
        Some(Response::Bulk(None)) => {} // nil response from BLOCK timeout — correct
        Some(other) => panic!("expected nil/timeout, got non-nil response: {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// 28. Blocking XREADGROUP: key deleted
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_blocking_xreadgroup_key_deleted() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    client.command(&["XADD", "mystream", "666", "f", "v"]).await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$"])
        .await;

    let mut blocker = server.connect().await;
    blocker
        .send_only(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "Alice",
            "BLOCK",
            "0",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    server.wait_for_blocked_clients(1).await;

    // Delete the key
    client.command(&["DEL", "mystream"]).await;

    let resp = blocker
        .read_response(Duration::from_secs(5))
        .await
        .expect("should unblock with error");
    assert_error_prefix(&resp, "NOGROUP");
}

// ---------------------------------------------------------------------------
// 29. Blocking XREADGROUP: key type changed with SET
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_blocking_xreadgroup_key_type_changed() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    client.command(&["XADD", "mystream", "666", "f", "v"]).await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$"])
        .await;

    let mut blocker = server.connect().await;
    blocker
        .send_only(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "Alice",
            "BLOCK",
            "0",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    server.wait_for_blocked_clients(1).await;

    // Overwrite key with a string
    client.command(&["SET", "mystream", "val1"]).await;

    let resp = blocker
        .read_response(Duration::from_secs(5))
        .await
        .expect("should unblock with error");
    assert_error_prefix(&resp, "WRONGTYPE");
}

// ---------------------------------------------------------------------------
// 30. XGROUP DESTROY should unblock XREADGROUP with -NOGROUP
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xgroup_destroy_unblocks_xreadgroup() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$", "MKSTREAM"])
        .await;

    let mut blocker = server.connect().await;
    blocker
        .send_only(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "Alice",
            "BLOCK",
            "0",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    server.wait_for_blocked_clients(1).await;

    client
        .command(&["XGROUP", "DESTROY", "mystream", "mygroup"])
        .await;

    let resp = blocker
        .read_response(Duration::from_secs(5))
        .await
        .expect("should unblock with NOGROUP error");
    assert_error_prefix(&resp, "NOGROUP");
}

// ---------------------------------------------------------------------------
// 30a. RENAME can unblock XREADGROUP with data
// ---------------------------------------------------------------------------

/// Upstream: `RENAME can unblock XREADGROUP with data`
/// Tests that RENAME'ing a stream (whose destination already has a matching
/// consumer group) over a key with a blocked XREADGROUP wakes the blocker
/// with the new entries from the renamed stream.
#[tokio::test]
async fn tcl_rename_can_unblock_xreadgroup_with_data() {
    let server = TestServer::start_standalone().await;
    let mut blocker = server.connect().await;
    let mut writer = server.connect().await;

    writer.command(&["DEL", "mystream{t}"]).await;
    writer
        .command(&[
            "XGROUP",
            "CREATE",
            "mystream{t}",
            "mygroup",
            "$",
            "MKSTREAM",
        ])
        .await;

    blocker
        .send_only(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "Alice",
            "BLOCK",
            "0",
            "STREAMS",
            "mystream{t}",
            ">",
        ])
        .await;
    server.wait_for_blocked_clients(1).await;

    // Create mystream2{t} with a matching group, XADD an entry, then RENAME
    // mystream2{t} over mystream{t}. Because mystream2{t} had mygroup before
    // the RENAME, the blocker should wake with the XADD'd entry.
    writer
        .command(&[
            "XGROUP",
            "CREATE",
            "mystream2{t}",
            "mygroup",
            "$",
            "MKSTREAM",
        ])
        .await;
    writer
        .command(&["XADD", "mystream2{t}", "100", "f1", "v1"])
        .await;
    writer
        .command(&["RENAME", "mystream2{t}", "mystream{t}"])
        .await;

    let resp = blocker
        .read_response(Duration::from_secs(2))
        .await
        .expect("should unblock after RENAME with data");

    // Expect [[stream_name, [[100-0, [f1, v1]]]]]
    let streams = unwrap_array(resp);
    assert_eq!(streams.len(), 1);
    let stream_data = unwrap_array(streams.into_iter().next().unwrap());
    assert_eq!(stream_data.len(), 2);
    let mut stream_iter = stream_data.into_iter();
    let stream_name = parse_bulk_string(&stream_iter.next().unwrap());
    assert_eq!(stream_name, "mystream{t}");
    let entries = unwrap_array(stream_iter.next().unwrap());
    assert_eq!(entries.len(), 1);
    assert_eq!(entry_id(&entries[0]), "100-0");
    assert_eq!(entry_fields(&entries[0]), vec!["f1", "v1"]);
}

// ---------------------------------------------------------------------------
// 30b. RENAME can unblock XREADGROUP with -NOGROUP
// ---------------------------------------------------------------------------

/// Upstream: `RENAME can unblock XREADGROUP with -NOGROUP`
/// Tests that RENAME'ing a stream (whose destination does NOT have a matching
/// consumer group) over a key with a blocked XREADGROUP wakes the blocker
/// with a NOGROUP error.
#[tokio::test]
async fn tcl_rename_can_unblock_xreadgroup_with_nogroup() {
    let server = TestServer::start_standalone().await;
    let mut blocker = server.connect().await;
    let mut writer = server.connect().await;

    writer.command(&["DEL", "mystream{t}"]).await;
    writer
        .command(&[
            "XGROUP",
            "CREATE",
            "mystream{t}",
            "mygroup",
            "$",
            "MKSTREAM",
        ])
        .await;

    blocker
        .send_only(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "Alice",
            "BLOCK",
            "0",
            "STREAMS",
            "mystream{t}",
            ">",
        ])
        .await;
    server.wait_for_blocked_clients(1).await;

    // XADD into mystream2{t} (no mygroup), then RENAME mystream2{t} over
    // mystream{t}. Because mystream2{t} didn't have mygroup before the
    // RENAME, the blocker should wake with a NOGROUP error.
    writer
        .command(&["XADD", "mystream2{t}", "100", "f1", "v1"])
        .await;
    writer
        .command(&["RENAME", "mystream2{t}", "mystream{t}"])
        .await;

    let resp = blocker
        .read_response(Duration::from_secs(2))
        .await
        .expect("should unblock after RENAME with NOGROUP");
    assert_error_prefix(&resp, "NOGROUP");
}

// ---------------------------------------------------------------------------
// 31. XCLAIM can claim PEL items from another consumer
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xclaim_claim_from_another_consumer() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    let id1 = parse_bulk_string(&client.command(&["XADD", "mystream", "*", "a", "1"]).await);
    let id2 = parse_bulk_string(&client.command(&["XADD", "mystream", "*", "b", "2"]).await);
    let id3 = parse_bulk_string(&client.command(&["XADD", "mystream", "*", "c", "3"]).await);
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "0"])
        .await;

    // Consumer1 reads item 1
    let resp = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer1",
            "COUNT",
            "1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    let entries = xreadgroup_entries(resp);
    assert_eq!(entries.len(), 1);
    let fields = entry_fields(&entries[0]);
    assert_eq!(fields, vec!["a", "1"]);

    // Verify PEL state
    let resp = client
        .command(&["XPENDING", "mystream", "mygroup", "-", "+", "10"])
        .await;
    assert_array_len(&resp, 1);

    let resp = client
        .command(&[
            "XPENDING",
            "mystream",
            "mygroup",
            "-",
            "+",
            "10",
            "consumer1",
        ])
        .await;
    assert_array_len(&resp, 1);

    let resp = client
        .command(&[
            "XPENDING",
            "mystream",
            "mygroup",
            "-",
            "+",
            "10",
            "consumer2",
        ])
        .await;
    assert_array_len(&resp, 0);

    // Small delay for min-idle-time
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Claim from consumer1 to consumer2
    let resp = client
        .command(&["XCLAIM", "mystream", "mygroup", "consumer2", "10", &id1])
        .await;
    let claimed = unwrap_array(resp);
    assert_eq!(claimed.len(), 1);
    let fields = entry_fields(&claimed[0]);
    assert_eq!(fields, vec!["a", "1"]);

    // Verify PEL moved
    let resp = client
        .command(&[
            "XPENDING",
            "mystream",
            "mygroup",
            "-",
            "+",
            "10",
            "consumer1",
        ])
        .await;
    assert_array_len(&resp, 0);

    let resp = client
        .command(&[
            "XPENDING",
            "mystream",
            "mygroup",
            "-",
            "+",
            "10",
            "consumer2",
        ])
        .await;
    assert_array_len(&resp, 1);

    // Consumer1 reads 2 more items
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer1",
            "COUNT",
            "2",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Delete item 2, then try to claim it — should be NOP
    client.command(&["XDEL", "mystream", &id2]).await;
    let resp = client
        .command(&["XCLAIM", "mystream", "mygroup", "consumer2", "10", &id2])
        .await;
    let claimed = unwrap_array(resp);
    assert_eq!(claimed.len(), 0);

    // Delete item 3, then try to claim it — should be NOP
    tokio::time::sleep(Duration::from_millis(50)).await;
    client.command(&["XDEL", "mystream", &id3]).await;
    let resp = client
        .command(&["XCLAIM", "mystream", "mygroup", "consumer2", "10", &id3])
        .await;
    let claimed = unwrap_array(resp);
    assert_eq!(claimed.len(), 0);
}

// ---------------------------------------------------------------------------
// 32. XCLAIM without JUSTID increments delivery count
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xclaim_increments_delivery_count() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    let id1 = parse_bulk_string(&client.command(&["XADD", "mystream", "*", "a", "1"]).await);
    client.command(&["XADD", "mystream", "*", "b", "2"]).await;
    client.command(&["XADD", "mystream", "*", "c", "3"]).await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "0"])
        .await;

    // Consumer1 reads item 1
    let resp = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer1",
            "COUNT",
            "1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    let entries = xreadgroup_entries(resp);
    let fields = entry_fields(&entries[0]);
    assert_eq!(fields, vec!["a", "1"]);

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Consumer2 claims id1 (without JUSTID — increments delivery count)
    let resp = client
        .command(&["XCLAIM", "mystream", "mygroup", "consumer2", "10", &id1])
        .await;
    let claimed = unwrap_array(resp);
    let fields = entry_fields(&claimed[0]);
    assert_eq!(fields, vec!["a", "1"]);

    // Check delivery count is 2
    let resp = client
        .command(&["XPENDING", "mystream", "mygroup", "-", "+", "10"])
        .await;
    let pending = unwrap_array(resp);
    let item = unwrap_array(pending[0].clone());
    assert_eq!(item.len(), 4);
    assert_integer_eq(&item[3], 2);

    // Consumer3 claims with JUSTID — should NOT increment delivery count
    tokio::time::sleep(Duration::from_millis(50)).await;
    let resp = client
        .command(&[
            "XCLAIM",
            "mystream",
            "mygroup",
            "consumer3",
            "10",
            &id1,
            "JUSTID",
        ])
        .await;
    let claimed = unwrap_array(resp);
    assert_eq!(claimed.len(), 1);
    assert_eq!(parse_bulk_string(&claimed[0]), id1);

    // Delivery count should still be 2
    let resp = client
        .command(&["XPENDING", "mystream", "mygroup", "-", "+", "10"])
        .await;
    let pending = unwrap_array(resp);
    let item = unwrap_array(pending[0].clone());
    assert_integer_eq(&item[3], 2);
}

// ---------------------------------------------------------------------------
// 33. XCLAIM same consumer
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xclaim_same_consumer() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    let id1 = parse_bulk_string(&client.command(&["XADD", "mystream", "*", "a", "1"]).await);
    client.command(&["XADD", "mystream", "*", "b", "2"]).await;
    client.command(&["XADD", "mystream", "*", "c", "3"]).await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "0"])
        .await;

    let resp = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer1",
            "COUNT",
            "1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    let entries = xreadgroup_entries(resp);
    let fields = entry_fields(&entries[0]);
    assert_eq!(fields, vec!["a", "1"]);

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Re-claim with the same consumer
    let resp = client
        .command(&["XCLAIM", "mystream", "mygroup", "consumer1", "10", &id1])
        .await;
    let claimed = unwrap_array(resp);
    assert_eq!(claimed.len(), 1);

    // Still in PEL for consumer1
    let resp = client
        .command(&["XPENDING", "mystream", "mygroup", "-", "+", "10"])
        .await;
    let pending = unwrap_array(resp);
    assert_eq!(pending.len(), 1);
    let item = unwrap_array(pending[0].clone());
    assert_eq!(parse_bulk_string(&item[1]), "consumer1");
}

// ---------------------------------------------------------------------------
// 34. XAUTOCLAIM can claim PEL items from another consumer
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xautoclaim_claim_from_another() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    let _id1 = parse_bulk_string(&client.command(&["XADD", "mystream", "*", "a", "1"]).await);
    let id2 = parse_bulk_string(&client.command(&["XADD", "mystream", "*", "b", "2"]).await);
    let _id3 = parse_bulk_string(&client.command(&["XADD", "mystream", "*", "c", "3"]).await);
    let id4 = parse_bulk_string(&client.command(&["XADD", "mystream", "*", "d", "4"]).await);
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "0"])
        .await;

    // Consumer1 reads item 1
    let resp = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer1",
            "COUNT",
            "1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    let entries = xreadgroup_entries(resp);
    assert_eq!(entries.len(), 1);
    let fields = entry_fields(&entries[0]);
    assert_eq!(fields, vec!["a", "1"]);

    tokio::time::sleep(Duration::from_millis(50)).await;

    // XAUTOCLAIM with COUNT 1
    let resp = client
        .command(&[
            "XAUTOCLAIM",
            "mystream",
            "mygroup",
            "consumer2",
            "10",
            "-",
            "COUNT",
            "1",
        ])
        .await;
    let result = unwrap_array(resp);
    assert_eq!(result.len(), 3);

    // Cursor should be 0-0 (finished)
    assert_eq!(parse_bulk_string(&result[0]), "0-0");

    // Should have claimed 1 entry
    let claimed = unwrap_array(result[1].clone());
    assert_eq!(claimed.len(), 1);
    let fields = entry_fields(&claimed[0]);
    assert_eq!(fields, vec!["a", "1"]);

    // Consumer1 reads remaining 3 items
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer1",
            "COUNT",
            "3",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Delete item 2
    client.command(&["XDEL", "mystream", &id2]).await;

    // XAUTOCLAIM with COUNT 3 — id2 is deleted, should be skipped
    let resp = client
        .command(&[
            "XAUTOCLAIM",
            "mystream",
            "mygroup",
            "consumer2",
            "10",
            "-",
            "COUNT",
            "3",
        ])
        .await;
    let result = unwrap_array(resp);
    assert_eq!(result.len(), 3);

    // Cursor should point to id4
    assert_eq!(parse_bulk_string(&result[0]), id4);

    // Should have claimed id1 and id3 (id2 deleted)
    let claimed = unwrap_array(result[1].clone());
    assert_eq!(claimed.len(), 2);
    let fields0 = entry_fields(&claimed[0]);
    assert_eq!(fields0, vec!["a", "1"]);
    let fields1 = entry_fields(&claimed[1]);
    assert_eq!(fields1, vec!["c", "3"]);

    // Deleted IDs array should contain id2
    let deleted = unwrap_array(result[2].clone());
    assert_eq!(deleted.len(), 1);
}

// ---------------------------------------------------------------------------
// 35. XAUTOCLAIM as an iterator
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xautoclaim_as_iterator() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    client.command(&["XADD", "mystream", "1-0", "a", "1"]).await;
    client.command(&["XADD", "mystream", "2-0", "b", "2"]).await;
    client.command(&["XADD", "mystream", "3-0", "c", "3"]).await;
    client.command(&["XADD", "mystream", "4-0", "d", "4"]).await;
    client.command(&["XADD", "mystream", "5-0", "e", "5"]).await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "0"])
        .await;

    // Read all 5 into consumer1
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer1",
            "COUNT",
            "90",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Claim 2 entries
    let resp = client
        .command(&[
            "XAUTOCLAIM",
            "mystream",
            "mygroup",
            "consumer2",
            "10",
            "-",
            "COUNT",
            "2",
        ])
        .await;
    let result = unwrap_array(resp);
    assert_eq!(result.len(), 3);
    let cursor = parse_bulk_string(&result[0]);
    assert_eq!(cursor, "2-1");
    let claimed = unwrap_array(result[1].clone());
    assert_eq!(claimed.len(), 2);
    let fields = entry_fields(&claimed[0]);
    assert_eq!(fields, vec!["a", "1"]);

    // Claim 2 more using cursor
    let resp = client
        .command(&[
            "XAUTOCLAIM",
            "mystream",
            "mygroup",
            "consumer2",
            "10",
            &cursor,
            "COUNT",
            "2",
        ])
        .await;
    let result = unwrap_array(resp);
    let cursor = parse_bulk_string(&result[0]);
    assert_eq!(cursor, "4-1");
    let claimed = unwrap_array(result[1].clone());
    assert_eq!(claimed.len(), 2);
    let fields = entry_fields(&claimed[0]);
    assert_eq!(fields, vec!["c", "3"]);

    // Claim last entry
    let resp = client
        .command(&[
            "XAUTOCLAIM",
            "mystream",
            "mygroup",
            "consumer2",
            "10",
            &cursor,
            "COUNT",
            "1",
        ])
        .await;
    let result = unwrap_array(resp);
    let cursor = parse_bulk_string(&result[0]);
    assert_eq!(cursor, "0-0");
    let claimed = unwrap_array(result[1].clone());
    assert_eq!(claimed.len(), 1);
    let fields = entry_fields(&claimed[0]);
    assert_eq!(fields, vec!["e", "5"]);
}

// ---------------------------------------------------------------------------
// 36. XAUTOCLAIM COUNT must be > 0
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xautoclaim_count_must_be_positive() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "XAUTOCLAIM",
            "key",
            "group",
            "consumer",
            "1",
            "1",
            "COUNT",
            "0",
        ])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// 37. XCLAIM with XDEL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xclaim_with_xdel() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "x"]).await;
    client.command(&["XADD", "x", "1-0", "f", "v"]).await;
    client.command(&["XADD", "x", "2-0", "f", "v"]).await;
    client.command(&["XADD", "x", "3-0", "f", "v"]).await;
    client.command(&["XGROUP", "CREATE", "x", "grp", "0"]).await;

    // Alice reads all 3
    let resp = client
        .command(&["XREADGROUP", "GROUP", "grp", "Alice", "STREAMS", "x", ">"])
        .await;
    let entries = xreadgroup_entries(resp);
    assert_eq!(entries.len(), 3);

    // Delete entry 2-0
    client.command(&["XDEL", "x", "2-0"]).await;

    // Bob claims all 3 — should only get 1-0 and 3-0 (2-0 deleted)
    let resp = client
        .command(&["XCLAIM", "x", "grp", "Bob", "0", "1-0", "2-0", "3-0"])
        .await;
    let claimed = unwrap_array(resp);
    assert_eq!(claimed.len(), 2);
    assert_eq!(entry_id(&claimed[0]), "1-0");
    assert_eq!(entry_id(&claimed[1]), "3-0");

    // Alice should have no pending entries
    let resp = client
        .command(&["XPENDING", "x", "grp", "-", "+", "10", "Alice"])
        .await;
    let pending = unwrap_array(resp);
    assert_eq!(pending.len(), 0);
}

// ---------------------------------------------------------------------------
// 38. XCLAIM with trimming
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xclaim_with_trimming() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "x"]).await;
    client.command(&["XADD", "x", "1-0", "f", "v"]).await;
    client.command(&["XADD", "x", "2-0", "f", "v"]).await;
    client.command(&["XADD", "x", "3-0", "f", "v"]).await;
    client.command(&["XGROUP", "CREATE", "x", "grp", "0"]).await;

    // Alice reads all 3
    let resp = client
        .command(&["XREADGROUP", "GROUP", "grp", "Alice", "STREAMS", "x", ">"])
        .await;
    let entries = xreadgroup_entries(resp);
    assert_eq!(entries.len(), 3);

    // Trim to only 1 entry
    client.command(&["XTRIM", "x", "MAXLEN", "1"]).await;

    // Bob claims all 3 — should only get 3-0 (1-0, 2-0 trimmed)
    let resp = client
        .command(&["XCLAIM", "x", "grp", "Bob", "0", "1-0", "2-0", "3-0"])
        .await;
    let claimed = unwrap_array(resp);
    assert_eq!(claimed.len(), 1);
    assert_eq!(entry_id(&claimed[0]), "3-0");

    // Alice should have no pending entries
    let resp = client
        .command(&["XPENDING", "x", "grp", "-", "+", "10", "Alice"])
        .await;
    let pending = unwrap_array(resp);
    assert_eq!(pending.len(), 0);
}

// ---------------------------------------------------------------------------
// 39. XAUTOCLAIM with XDEL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xautoclaim_with_xdel() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "x"]).await;
    client.command(&["XADD", "x", "1-0", "f", "v"]).await;
    client.command(&["XADD", "x", "2-0", "f", "v"]).await;
    client.command(&["XADD", "x", "3-0", "f", "v"]).await;
    client.command(&["XGROUP", "CREATE", "x", "grp", "0"]).await;

    // Alice reads all 3
    let resp = client
        .command(&["XREADGROUP", "GROUP", "grp", "Alice", "STREAMS", "x", ">"])
        .await;
    let entries = xreadgroup_entries(resp);
    assert_eq!(entries.len(), 3);

    // Delete entry 2-0
    client.command(&["XDEL", "x", "2-0"]).await;

    // XAUTOCLAIM — should claim 1-0, 3-0, report 2-0 deleted
    let resp = client
        .command(&["XAUTOCLAIM", "x", "grp", "Bob", "0", "0-0"])
        .await;
    let result = unwrap_array(resp);
    assert_eq!(result.len(), 3);

    // Cursor should be 0-0 (finished)
    assert_eq!(parse_bulk_string(&result[0]), "0-0");

    // Claimed entries
    let claimed = unwrap_array(result[1].clone());
    assert_eq!(claimed.len(), 2);
    assert_eq!(entry_id(&claimed[0]), "1-0");
    assert_eq!(entry_id(&claimed[1]), "3-0");

    // Deleted IDs
    let deleted = unwrap_array(result[2].clone());
    assert_eq!(deleted.len(), 1);
    assert_eq!(parse_bulk_string(&deleted[0]), "2-0");

    // Alice should have no pending entries
    let resp = client
        .command(&["XPENDING", "x", "grp", "-", "+", "10", "Alice"])
        .await;
    let pending = unwrap_array(resp);
    assert_eq!(pending.len(), 0);
}

// ---------------------------------------------------------------------------
// 40. XINFO FULL output
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xinfo_full_output() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "x"]).await;
    client.command(&["XADD", "x", "100", "a", "1"]).await;
    client.command(&["XADD", "x", "101", "b", "1"]).await;
    client.command(&["XADD", "x", "102", "c", "1"]).await;
    client.command(&["XADD", "x", "103", "e", "1"]).await;
    client.command(&["XADD", "x", "104", "f", "1"]).await;
    client.command(&["XGROUP", "CREATE", "x", "g1", "0"]).await;
    client.command(&["XGROUP", "CREATE", "x", "g2", "0"]).await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "g1",
            "Alice",
            "COUNT",
            "1",
            "STREAMS",
            "x",
            ">",
        ])
        .await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "g1",
            "Bob",
            "COUNT",
            "1",
            "STREAMS",
            "x",
            ">",
        ])
        .await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "g1",
            "Bob",
            "NOACK",
            "COUNT",
            "1",
            "STREAMS",
            "x",
            ">",
        ])
        .await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "g2",
            "Charlie",
            "COUNT",
            "4",
            "STREAMS",
            "x",
            ">",
        ])
        .await;
    client.command(&["XDEL", "x", "103"]).await;

    let resp = client.command(&["XINFO", "STREAM", "x", "FULL"]).await;
    let items = match &resp {
        Response::Array(arr) => arr,
        _ => panic!("expected array"),
    };

    // Check length field
    let length = xinfo_get_field(items, "length");
    assert_integer_eq(length, 4);

    // Check entries field exists and has entries
    let entries_resp = xinfo_get_field(items, "entries");
    let entries = unwrap_array(entries_resp.clone());
    assert!(!entries.is_empty());
    // First entry should be 100-0
    assert_eq!(entry_id(&entries[0]), "100-0");

    // Check groups field
    let groups_resp = xinfo_get_field(items, "groups");
    let groups = unwrap_array(groups_resp.clone());
    assert_eq!(groups.len(), 2);

    // First group (g1)
    let g1 = match &groups[0] {
        Response::Array(arr) => arr,
        _ => panic!("expected array"),
    };
    let g1_name = xinfo_get_field(g1, "name");
    assert_bulk_eq(g1_name, b"g1");

    // Second group (g2)
    let g2 = match &groups[1] {
        Response::Array(arr) => arr,
        _ => panic!("expected array"),
    };
    let g2_name = xinfo_get_field(g2, "name");
    assert_bulk_eq(g2_name, b"g2");

    // Test FULL with COUNT 1
    let resp = client
        .command(&["XINFO", "STREAM", "x", "FULL", "COUNT", "1"])
        .await;
    let items = match &resp {
        Response::Array(arr) => arr,
        _ => panic!("expected array"),
    };
    let length = xinfo_get_field(items, "length");
    assert_integer_eq(length, 4);

    // With COUNT 1, entries should have only 1 entry
    let entries_resp = xinfo_get_field(items, "entries");
    let entries = unwrap_array(entries_resp.clone());
    assert_eq!(entries.len(), 1);
    assert_eq!(entry_id(&entries[0]), "100-0");
}

// ---------------------------------------------------------------------------
// 41. XGROUP CREATECONSUMER: create consumer if does not exist
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xgroup_createconsumer() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$", "MKSTREAM"])
        .await;
    client.command(&["XADD", "mystream", "*", "f", "v"]).await;

    // No consumers yet
    let resp = client.command(&["XINFO", "GROUPS", "mystream"]).await;
    let groups = unwrap_array(resp);
    let g = match &groups[0] {
        Response::Array(arr) => arr,
        _ => panic!("expected array"),
    };
    let consumers = xinfo_get_field(g, "consumers");
    assert_integer_eq(consumers, 0);

    // Create consumer via XREADGROUP
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "Alice",
            "COUNT",
            "1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;

    let resp = client.command(&["XINFO", "GROUPS", "mystream"]).await;
    let groups = unwrap_array(resp);
    let g = match &groups[0] {
        Response::Array(arr) => arr,
        _ => panic!("expected array"),
    };
    let consumers = xinfo_get_field(g, "consumers");
    assert_integer_eq(consumers, 1);

    // CREATECONSUMER for existing consumer (Alice) — should return 0
    assert_integer_eq(
        &client
            .command(&["XGROUP", "CREATECONSUMER", "mystream", "mygroup", "Alice"])
            .await,
        0,
    );

    // CREATECONSUMER for new consumer (Bob) — should return 1
    assert_integer_eq(
        &client
            .command(&["XGROUP", "CREATECONSUMER", "mystream", "mygroup", "Bob"])
            .await,
        1,
    );

    // Now 2 consumers
    let resp = client.command(&["XINFO", "GROUPS", "mystream"]).await;
    let groups = unwrap_array(resp);
    let g = match &groups[0] {
        Response::Array(arr) => arr,
        _ => panic!("expected array"),
    };
    let consumers = xinfo_get_field(g, "consumers");
    assert_integer_eq(consumers, 2);

    // Verify consumer names via XINFO CONSUMERS
    let resp = client
        .command(&["XINFO", "CONSUMERS", "mystream", "mygroup"])
        .await;
    let consumer_list = unwrap_array(resp);
    assert_eq!(consumer_list.len(), 2);

    // Collect consumer names (order may vary between implementations)
    let mut names: Vec<String> = consumer_list
        .iter()
        .map(|c| {
            let arr = match c {
                Response::Array(arr) => arr,
                _ => panic!("expected array"),
            };
            parse_bulk_string(xinfo_get_field(arr, "name"))
        })
        .collect();
    names.sort();
    assert_eq!(names, vec!["Alice", "Bob"]);
}

// ---------------------------------------------------------------------------
// 42. XGROUP CREATECONSUMER: group must exist
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xgroup_createconsumer_group_must_exist() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    client.command(&["XADD", "mystream", "*", "f", "v"]).await;

    let resp = client
        .command(&[
            "XGROUP",
            "CREATECONSUMER",
            "mystream",
            "mygroup",
            "consumer",
        ])
        .await;
    assert_error_prefix(&resp, "NOGROUP");
}

// ---------------------------------------------------------------------------
// 43. XPENDING with IDLE filter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_xpending_with_idle() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    client
        .command(&["XADD", "mystream", "*", "foo", "bar"])
        .await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$"])
        .await;

    // Add entries and read with two consumers (same setup as XPENDING tests)
    client.command(&["XADD", "mystream", "*", "a", "1"]).await;
    client.command(&["XADD", "mystream", "*", "b", "2"]).await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer-1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;

    client.command(&["XADD", "mystream", "*", "c", "3"]).await;
    client.command(&["XADD", "mystream", "*", "d", "4"]).await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer-2",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;

    // Small delay so idle time accumulates
    tokio::time::sleep(Duration::from_millis(20)).await;

    // IDLE with a very high threshold — nothing should be idle enough
    let resp = client
        .command(&[
            "XPENDING",
            "mystream",
            "mygroup",
            "IDLE",
            "99999999",
            "-",
            "+",
            "10",
            "consumer-1",
        ])
        .await;
    let pending = unwrap_array(resp);
    assert_eq!(pending.len(), 0);

    // IDLE with a low threshold — consumer-1's 2 entries should match
    let resp = client
        .command(&[
            "XPENDING",
            "mystream",
            "mygroup",
            "IDLE",
            "1",
            "-",
            "+",
            "10",
            "consumer-1",
        ])
        .await;
    let pending = unwrap_array(resp);
    assert_eq!(pending.len(), 2);

    // IDLE filter without consumer name — high threshold
    let resp = client
        .command(&[
            "XPENDING", "mystream", "mygroup", "IDLE", "99999999", "-", "+", "10",
        ])
        .await;
    let pending = unwrap_array(resp);
    assert_eq!(pending.len(), 0);

    // IDLE filter without consumer name — low threshold
    let resp = client
        .command(&[
            "XPENDING", "mystream", "mygroup", "IDLE", "1", "-", "+", "10",
        ])
        .await;
    let pending = unwrap_array(resp);
    assert_eq!(pending.len(), 4);
}

// ---------------------------------------------------------------------------
// 44. Consumer seen-time and active-time via XINFO CONSUMERS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_consumer_seen_time_and_active_time() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mystream"]).await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$", "MKSTREAM"])
        .await;

    // Reading from empty stream creates consumer but no active-time
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "Alice",
            "COUNT",
            "1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    let resp = client
        .command(&["XINFO", "CONSUMERS", "mystream", "mygroup"])
        .await;
    let consumers = unwrap_array(resp);
    let c = match &consumers[0] {
        Response::Array(arr) => arr,
        _ => panic!("expected array"),
    };
    let idle = unwrap_integer(xinfo_get_field(c, "idle"));
    assert!(idle >= 100); // seen-time idle
    let inactive = unwrap_integer(xinfo_get_field(c, "inactive"));
    assert_eq!(inactive, -1); // never actively consumed

    // Add and read an entry — both idle and inactive should be small
    client.command(&["XADD", "mystream", "*", "f", "v"]).await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "Alice",
            "COUNT",
            "1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;

    let resp = client
        .command(&["XINFO", "CONSUMERS", "mystream", "mygroup"])
        .await;
    let consumers = unwrap_array(resp);
    let c = match &consumers[0] {
        Response::Array(arr) => arr,
        _ => panic!("expected array"),
    };
    assert!(unwrap_integer(xinfo_get_field(c, "idle")) < 80);
    assert!(unwrap_integer(xinfo_get_field(c, "inactive")) < 80);

    // After a delay, read from empty stream — idle should be small but inactive
    // should be >= 100ms since last real read
    tokio::time::sleep(Duration::from_millis(100)).await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "Alice",
            "COUNT",
            "1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;

    let resp = client
        .command(&["XINFO", "CONSUMERS", "mystream", "mygroup"])
        .await;
    let consumers = unwrap_array(resp);
    let c = match &consumers[0] {
        Response::Array(arr) => arr,
        _ => panic!("expected array"),
    };
    assert!(unwrap_integer(xinfo_get_field(c, "idle")) < 80);
    assert!(unwrap_integer(xinfo_get_field(c, "inactive")) >= 100);
}

// ---------------------------------------------------------------------------
// 45. Consumer group read counter and lag sanity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_consumer_group_read_counter_and_lag_sanity() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "x"]).await;
    client.command(&["XADD", "x", "1-0", "data", "a"]).await;
    client.command(&["XADD", "x", "2-0", "data", "b"]).await;
    client.command(&["XADD", "x", "3-0", "data", "c"]).await;
    client.command(&["XADD", "x", "4-0", "data", "d"]).await;
    client.command(&["XADD", "x", "5-0", "data", "e"]).await;
    client.command(&["XGROUP", "CREATE", "x", "g1", "0"]).await;

    // Before any reads: entries-read=nil, lag=5
    let resp = client.command(&["XINFO", "STREAM", "x", "FULL"]).await;
    let items = unwrap_array(resp);
    let groups = unwrap_array(xinfo_get_field(&items, "groups").clone());
    let g = match &groups[0] {
        Response::Array(arr) => arr,
        _ => panic!("expected array"),
    };
    assert!(matches!(
        xinfo_get_field(g, "entries-read"),
        Response::Bulk(None)
    ));
    assert_integer_eq(xinfo_get_field(g, "lag"), 5);

    // After reading 1: entries-read=1, lag=4
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "g1",
            "c11",
            "COUNT",
            "1",
            "STREAMS",
            "x",
            ">",
        ])
        .await;
    let resp = client.command(&["XINFO", "STREAM", "x", "FULL"]).await;
    let items = unwrap_array(resp);
    let groups = unwrap_array(xinfo_get_field(&items, "groups").clone());
    let g = match &groups[0] {
        Response::Array(arr) => arr,
        _ => panic!("expected array"),
    };
    assert_integer_eq(xinfo_get_field(g, "entries-read"), 1);
    assert_integer_eq(xinfo_get_field(g, "lag"), 4);

    // After reading all remaining: entries-read=5, lag=0
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "g1",
            "c12",
            "COUNT",
            "10",
            "STREAMS",
            "x",
            ">",
        ])
        .await;
    let resp = client.command(&["XINFO", "STREAM", "x", "FULL"]).await;
    let items = unwrap_array(resp);
    let groups = unwrap_array(xinfo_get_field(&items, "groups").clone());
    let g = match &groups[0] {
        Response::Array(arr) => arr,
        _ => panic!("expected array"),
    };
    assert_integer_eq(xinfo_get_field(g, "entries-read"), 5);
    assert_integer_eq(xinfo_get_field(g, "lag"), 0);

    // After adding one more: entries-read=5, lag=1
    client.command(&["XADD", "x", "6-0", "data", "f"]).await;
    let resp = client.command(&["XINFO", "STREAM", "x", "FULL"]).await;
    let items = unwrap_array(resp);
    let groups = unwrap_array(xinfo_get_field(&items, "groups").clone());
    let g = match &groups[0] {
        Response::Array(arr) => arr,
        _ => panic!("expected array"),
    };
    assert_integer_eq(xinfo_get_field(g, "entries-read"), 5);
    assert_integer_eq(xinfo_get_field(g, "lag"), 1);
}

// ---------------------------------------------------------------------------
// 46. Consumer group lag with XDELs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_consumer_group_lag_with_xdels() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "x"]).await;
    client.command(&["XADD", "x", "1-0", "data", "a"]).await;
    client.command(&["XADD", "x", "2-0", "data", "b"]).await;
    client.command(&["XADD", "x", "3-0", "data", "c"]).await;
    client.command(&["XADD", "x", "4-0", "data", "d"]).await;
    client.command(&["XADD", "x", "5-0", "data", "e"]).await;
    client.command(&["XDEL", "x", "3-0"]).await;
    client.command(&["XGROUP", "CREATE", "x", "g1", "0"]).await;
    client.command(&["XGROUP", "CREATE", "x", "g2", "0"]).await;

    // With deletions, entries-read=nil, lag=nil (can't compute)
    let resp = client.command(&["XINFO", "STREAM", "x", "FULL"]).await;
    let items = unwrap_array(resp);
    let groups = unwrap_array(xinfo_get_field(&items, "groups").clone());
    let g1 = find_group_by_name(&groups, "g1");
    assert!(matches!(
        xinfo_get_field(g1, "entries-read"),
        Response::Bulk(None)
    ));
    assert!(matches!(xinfo_get_field(g1, "lag"), Response::Bulk(None)));

    // Read entries one by one — until all consumed, entries-read/lag stay nil
    for _ in 0..3 {
        client
            .command(&[
                "XREADGROUP",
                "GROUP",
                "g1",
                "c11",
                "COUNT",
                "1",
                "STREAMS",
                "x",
                ">",
            ])
            .await;
        let resp = client.command(&["XINFO", "STREAM", "x", "FULL"]).await;
        let items = unwrap_array(resp);
        let groups = unwrap_array(xinfo_get_field(&items, "groups").clone());
        let g = find_group_by_name(&groups, "g1");
        assert!(matches!(
            xinfo_get_field(g, "entries-read"),
            Response::Bulk(None)
        ));
        assert!(matches!(xinfo_get_field(g, "lag"), Response::Bulk(None)));
    }

    // 4th read — reads last entry, now entries-read=5, lag=0
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "g1",
            "c11",
            "COUNT",
            "1",
            "STREAMS",
            "x",
            ">",
        ])
        .await;
    let resp = client.command(&["XINFO", "STREAM", "x", "FULL"]).await;
    let items = unwrap_array(resp);
    let groups = unwrap_array(xinfo_get_field(&items, "groups").clone());
    let g = find_group_by_name(&groups, "g1");
    assert_integer_eq(xinfo_get_field(g, "entries-read"), 5);
    assert_integer_eq(xinfo_get_field(g, "lag"), 0);

    // Add entry 6-0: g1 lag=1
    client.command(&["XADD", "x", "6-0", "data", "f"]).await;
    let resp = client.command(&["XINFO", "STREAM", "x", "FULL"]).await;
    let items = unwrap_array(resp);
    let groups = unwrap_array(xinfo_get_field(&items, "groups").clone());
    let g = find_group_by_name(&groups, "g1");
    assert_integer_eq(xinfo_get_field(g, "entries-read"), 5);
    assert_integer_eq(xinfo_get_field(g, "lag"), 1);

    // XTRIM MINID=3-0 — trims entries 1-0 and 2-0, g2 lag resolves
    client.command(&["XTRIM", "x", "MINID", "=", "3-0"]).await;
    let resp = client.command(&["XINFO", "STREAM", "x", "FULL"]).await;
    let items = unwrap_array(resp);
    let groups = unwrap_array(xinfo_get_field(&items, "groups").clone());
    let g1 = find_group_by_name(&groups, "g1");
    assert_integer_eq(xinfo_get_field(g1, "entries-read"), 5);
    assert_integer_eq(xinfo_get_field(g1, "lag"), 1);
    let g2 = find_group_by_name(&groups, "g2");
    assert!(matches!(
        xinfo_get_field(g2, "entries-read"),
        Response::Bulk(None)
    ));
    assert_integer_eq(xinfo_get_field(g2, "lag"), 3);

    // XTRIM MINID=5-0 — trims more, g2 lag adjusts
    client.command(&["XTRIM", "x", "MINID", "=", "5-0"]).await;
    let resp = client.command(&["XINFO", "STREAM", "x", "FULL"]).await;
    let items = unwrap_array(resp);
    let groups = unwrap_array(xinfo_get_field(&items, "groups").clone());
    let g1 = find_group_by_name(&groups, "g1");
    assert_integer_eq(xinfo_get_field(g1, "entries-read"), 5);
    assert_integer_eq(xinfo_get_field(g1, "lag"), 1);
    let g2 = find_group_by_name(&groups, "g2");
    assert!(matches!(
        xinfo_get_field(g2, "entries-read"),
        Response::Bulk(None)
    ));
    assert_integer_eq(xinfo_get_field(g2, "lag"), 2);
}

// ---------------------------------------------------------------------------
// 47. Consumer group lag with XDELs and tombstone after last_id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_consumer_group_lag_with_xdels_and_tombstone() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "x"]).await;
    client
        .command(&["XGROUP", "CREATE", "x", "g1", "$", "MKSTREAM"])
        .await;
    client.command(&["XADD", "x", "1-0", "data", "a"]).await;

    // Read entry 1-0
    client
        .command(&["XREADGROUP", "GROUP", "g1", "alice", "STREAMS", "x", ">"])
        .await;

    client.command(&["XADD", "x", "2-0", "data", "c"]).await;
    client.command(&["XADD", "x", "3-0", "data", "d"]).await;
    client.command(&["XDEL", "x", "2-0"]).await;

    // Tombstone 2-0 is after last_id 1-0 — lag should be nil
    let resp = client.command(&["XINFO", "STREAM", "x", "FULL"]).await;
    let items = unwrap_array(resp);
    let groups = unwrap_array(xinfo_get_field(&items, "groups").clone());
    let g = match &groups[0] {
        Response::Array(arr) => arr,
        _ => panic!("expected array"),
    };
    assert_integer_eq(xinfo_get_field(g, "entries-read"), 1);
    assert!(matches!(xinfo_get_field(g, "lag"), Response::Bulk(None)));

    // Delete entry 1-0 too — now all tombstones are behind first entry
    client.command(&["XDEL", "x", "1-0"]).await;
    let resp = client.command(&["XINFO", "STREAM", "x", "FULL"]).await;
    let items = unwrap_array(resp);
    let groups = unwrap_array(xinfo_get_field(&items, "groups").clone());
    let g = match &groups[0] {
        Response::Array(arr) => arr,
        _ => panic!("expected array"),
    };
    assert_integer_eq(xinfo_get_field(g, "entries-read"), 1);
    assert_integer_eq(xinfo_get_field(g, "lag"), 1);

    // Read remaining entry 3-0 — counter becomes 3 (due to tombstone gap)
    client
        .command(&["XREADGROUP", "GROUP", "g1", "alice", "STREAMS", "x", ">"])
        .await;
    let resp = client.command(&["XINFO", "STREAM", "x", "FULL"]).await;
    let items = unwrap_array(resp);
    let groups = unwrap_array(xinfo_get_field(&items, "groups").clone());
    let g = match &groups[0] {
        Response::Array(arr) => arr,
        _ => panic!("expected array"),
    };
    assert_integer_eq(xinfo_get_field(g, "entries-read"), 3);
    assert_integer_eq(xinfo_get_field(g, "lag"), 0);
}

// ---------------------------------------------------------------------------
// 48. Consumer group lag with XTRIM
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_consumer_group_lag_with_xtrim() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "x"]).await;
    client
        .command(&["XGROUP", "CREATE", "x", "mygroup", "$", "MKSTREAM"])
        .await;
    client.command(&["XADD", "x", "1-0", "data", "a"]).await;
    client.command(&["XADD", "x", "2-0", "data", "b"]).await;
    client.command(&["XADD", "x", "3-0", "data", "c"]).await;
    client.command(&["XADD", "x", "4-0", "data", "d"]).await;
    client.command(&["XADD", "x", "5-0", "data", "e"]).await;

    // Read 1 entry
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "alice",
            "COUNT",
            "1",
            "STREAMS",
            "x",
            ">",
        ])
        .await;

    let resp = client.command(&["XINFO", "STREAM", "x", "FULL"]).await;
    let items = unwrap_array(resp);
    let groups = unwrap_array(xinfo_get_field(&items, "groups").clone());
    let g = match &groups[0] {
        Response::Array(arr) => arr,
        _ => panic!("expected array"),
    };
    assert_integer_eq(xinfo_get_field(g, "entries-read"), 1);
    assert_integer_eq(xinfo_get_field(g, "lag"), 4);

    // XTRIM MAXLEN 1 — keeps only last entry (5-0), lag should be 1
    client.command(&["XTRIM", "x", "MAXLEN", "1"]).await;
    let resp = client.command(&["XINFO", "STREAM", "x", "FULL"]).await;
    let items = unwrap_array(resp);
    assert_bulk_eq(xinfo_get_field(&items, "max-deleted-entry-id"), b"0-0");
    let groups = unwrap_array(xinfo_get_field(&items, "groups").clone());
    let g = match &groups[0] {
        Response::Array(arr) => arr,
        _ => panic!("expected array"),
    };
    assert_integer_eq(xinfo_get_field(g, "entries-read"), 1);
    assert_integer_eq(xinfo_get_field(g, "lag"), 1);

    // Read remaining entry — lag=0
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "alice",
            "STREAMS",
            "x",
            ">",
        ])
        .await;
    let resp = client.command(&["XINFO", "STREAM", "x", "FULL"]).await;
    let items = unwrap_array(resp);
    let groups = unwrap_array(xinfo_get_field(&items, "groups").clone());
    let g = match &groups[0] {
        Response::Array(arr) => arr,
        _ => panic!("expected array"),
    };
    assert_integer_eq(xinfo_get_field(g, "entries-read"), 5);
    assert_integer_eq(xinfo_get_field(g, "lag"), 0);

    // Add entry 6-0 — lag=1
    client.command(&["XADD", "x", "6-0", "data", "f"]).await;
    let resp = client.command(&["XINFO", "STREAM", "x", "FULL"]).await;
    let items = unwrap_array(resp);
    let groups = unwrap_array(xinfo_get_field(&items, "groups").clone());
    let g = match &groups[0] {
        Response::Array(arr) => arr,
        _ => panic!("expected array"),
    };
    assert_integer_eq(xinfo_get_field(g, "entries-read"), 5);
    assert_integer_eq(xinfo_get_field(g, "lag"), 1);

    // XTRIM MAXLEN 0 — empty stream, lag=0
    client.command(&["XTRIM", "x", "MAXLEN", "0"]).await;
    let resp = client.command(&["XINFO", "STREAM", "x", "FULL"]).await;
    let items = unwrap_array(resp);
    let groups = unwrap_array(xinfo_get_field(&items, "groups").clone());
    let g = match &groups[0] {
        Response::Array(arr) => arr,
        _ => panic!("expected array"),
    };
    assert_integer_eq(xinfo_get_field(g, "lag"), 0);
}
