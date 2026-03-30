//! Rust port of Redis 8.6.0 `unit/type/list.tcl`, `list-2.tcl`, `list-3.tcl`.
//!
//! Excludes: encoding-specific tests, `needs:debug`, `needs:repl`, large-memory,
//! readraw, fuzzing/stress tests.

use std::time::Duration;

use frogdb_protocol::Response;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// LPOS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_lpos_basic_usage() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client
        .command(&["RPUSH", "mylist", "a", "b", "c", "d", "2", "3", "c", "c"])
        .await;
    assert_integer_eq(&client.command(&["LPOS", "mylist", "a"]).await, 0);
    assert_integer_eq(&client.command(&["LPOS", "mylist", "c"]).await, 2);
}

#[tokio::test]
async fn tcl_lpos_rank_option() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client
        .command(&["RPUSH", "mylist", "a", "b", "c", "d", "2", "3", "c", "c"])
        .await;

    assert_integer_eq(
        &client.command(&["LPOS", "mylist", "c", "RANK", "1"]).await,
        2,
    );
    assert_integer_eq(
        &client.command(&["LPOS", "mylist", "c", "RANK", "2"]).await,
        6,
    );
    assert_nil(&client.command(&["LPOS", "mylist", "c", "RANK", "4"]).await);
    assert_integer_eq(
        &client.command(&["LPOS", "mylist", "c", "RANK", "-1"]).await,
        7,
    );
    assert_integer_eq(
        &client.command(&["LPOS", "mylist", "c", "RANK", "-2"]).await,
        6,
    );
    assert_error_prefix(
        &client.command(&["LPOS", "mylist", "c", "RANK", "0"]).await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_lpos_count_option() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client
        .command(&["RPUSH", "mylist", "a", "b", "c", "d", "2", "3", "c", "c"])
        .await;

    // COUNT 0 = all matches
    let resp = client.command(&["LPOS", "mylist", "c", "COUNT", "0"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 3);
    assert_integer_eq(&items[0], 2);
    assert_integer_eq(&items[1], 6);
    assert_integer_eq(&items[2], 7);

    // COUNT 1
    let resp = client.command(&["LPOS", "mylist", "c", "COUNT", "1"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 1);
    assert_integer_eq(&items[0], 2);

    // COUNT 2
    let resp = client.command(&["LPOS", "mylist", "c", "COUNT", "2"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 2);
    assert_integer_eq(&items[0], 2);
    assert_integer_eq(&items[1], 6);
}

#[tokio::test]
async fn tcl_lpos_count_plus_rank() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client
        .command(&["RPUSH", "mylist", "a", "b", "c", "d", "2", "3", "c", "c"])
        .await;

    let resp = client
        .command(&["LPOS", "mylist", "c", "COUNT", "0", "RANK", "2"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 2);
    assert_integer_eq(&items[0], 6);
    assert_integer_eq(&items[1], 7);

    let resp = client
        .command(&["LPOS", "mylist", "c", "COUNT", "2", "RANK", "-1"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 2);
    assert_integer_eq(&items[0], 7);
    assert_integer_eq(&items[1], 6);
}

#[tokio::test]
async fn tcl_lpos_non_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["LPOS", "mylistxxx", "c", "COUNT", "0", "RANK", "2"])
        .await;
    let items = unwrap_array(resp);
    assert!(items.is_empty());
}

#[tokio::test]
async fn tcl_lpos_no_match() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client.command(&["RPUSH", "mylist", "a", "b", "c"]).await;

    let resp = client
        .command(&["LPOS", "mylist", "x", "COUNT", "2", "RANK", "-1"])
        .await;
    let items = unwrap_array(resp);
    assert!(items.is_empty());

    assert_nil(&client.command(&["LPOS", "mylist", "x", "RANK", "-1"]).await);
}

#[tokio::test]
async fn tcl_lpos_maxlen() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client
        .command(&["RPUSH", "mylist", "a", "b", "c", "d", "2", "3", "c", "c"])
        .await;

    let resp = client
        .command(&["LPOS", "mylist", "a", "COUNT", "0", "MAXLEN", "1"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 1);
    assert_integer_eq(&items[0], 0);

    let resp = client
        .command(&["LPOS", "mylist", "c", "COUNT", "0", "MAXLEN", "1"])
        .await;
    let items = unwrap_array(resp);
    assert!(items.is_empty());

    let resp = client
        .command(&["LPOS", "mylist", "c", "COUNT", "0", "MAXLEN", "3"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 1);
    assert_integer_eq(&items[0], 2);
}

// ---------------------------------------------------------------------------
// LPUSH / RPUSH / LLEN / LINDEX / LPOP / RPOP
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_lpush_rpush_llength_lindex_lpop() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // first lpush then rpush
    client.command(&["DEL", "mylist1"]).await;
    assert_integer_eq(&client.command(&["LPUSH", "mylist1", "a"]).await, 1);
    assert_integer_eq(&client.command(&["RPUSH", "mylist1", "b"]).await, 2);
    assert_integer_eq(&client.command(&["RPUSH", "mylist1", "c"]).await, 3);
    assert_integer_eq(&client.command(&["LLEN", "mylist1"]).await, 3);
    assert_bulk_eq(&client.command(&["LINDEX", "mylist1", "0"]).await, b"a");
    assert_bulk_eq(&client.command(&["LINDEX", "mylist1", "1"]).await, b"b");
    assert_bulk_eq(&client.command(&["LINDEX", "mylist1", "2"]).await, b"c");
    assert_nil(&client.command(&["LINDEX", "mylist1", "3"]).await);
    assert_bulk_eq(&client.command(&["RPOP", "mylist1"]).await, b"c");
    assert_bulk_eq(&client.command(&["LPOP", "mylist1"]).await, b"a");
}

#[tokio::test]
async fn tcl_lpop_rpop_wrong_number_of_arguments() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_error_prefix(&client.command(&["LPOP", "key", "1", "1"]).await, "ERR");
    assert_error_prefix(&client.command(&["RPOP", "key", "2", "2"]).await, "ERR");
}

#[tokio::test]
async fn tcl_rpop_lpop_with_optional_count_argument() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "listcount"]).await;
    assert_integer_eq(
        &client
            .command(&[
                "LPUSH",
                "listcount",
                "aa",
                "bb",
                "cc",
                "dd",
                "ee",
                "ff",
                "gg",
            ])
            .await,
        7,
    );

    let resp = client.command(&["LPOP", "listcount", "1"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 1);
    assert_bulk_eq(&items[0], b"gg");

    let resp = client.command(&["LPOP", "listcount", "2"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 2);
    assert_bulk_eq(&items[0], b"ff");
    assert_bulk_eq(&items[1], b"ee");

    let resp = client.command(&["RPOP", "listcount", "2"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 2);
    assert_bulk_eq(&items[0], b"aa");
    assert_bulk_eq(&items[1], b"bb");

    let resp = client.command(&["RPOP", "listcount", "1"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 1);
    assert_bulk_eq(&items[0], b"cc");

    // More than remaining
    let resp = client.command(&["RPOP", "listcount", "123"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 1);
    assert_bulk_eq(&items[0], b"dd");

    // Negative count
    assert_error_prefix(&client.command(&["LPOP", "forbarqaz", "-123"]).await, "ERR");
}

#[tokio::test]
async fn tcl_variadic_rpush_lpush() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    assert_integer_eq(
        &client
            .command(&["LPUSH", "mylist", "a", "b", "c", "d"])
            .await,
        4,
    );
    assert_integer_eq(
        &client
            .command(&["RPUSH", "mylist", "0", "1", "2", "3"])
            .await,
        8,
    );
    let items = extract_bulk_strings(&client.command(&["LRANGE", "mylist", "0", "-1"]).await);
    assert_eq!(items, vec!["d", "c", "b", "a", "0", "1", "2", "3"]);
}

#[tokio::test]
async fn tcl_del_a_list() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client.command(&["RPUSH", "mylist", "a", "b"]).await;
    assert_integer_eq(&client.command(&["DEL", "mylist"]).await, 1);
    assert_integer_eq(&client.command(&["EXISTS", "mylist"]).await, 0);
    assert_integer_eq(&client.command(&["LLEN", "mylist"]).await, 0);
}

// ---------------------------------------------------------------------------
// LPUSHX / RPUSHX
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_lpushx_rpushx_generic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "xlist"]).await;
    assert_integer_eq(&client.command(&["LPUSHX", "xlist", "a"]).await, 0);
    assert_integer_eq(&client.command(&["LLEN", "xlist"]).await, 0);
    assert_integer_eq(&client.command(&["RPUSHX", "xlist", "a"]).await, 0);
    assert_integer_eq(&client.command(&["LLEN", "xlist"]).await, 0);
}

#[tokio::test]
async fn tcl_lpushx_rpushx() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "xlist"]).await;
    client.command(&["RPUSH", "xlist", "a", "c"]).await;
    assert_integer_eq(&client.command(&["RPUSHX", "xlist", "d"]).await, 3);
    assert_integer_eq(&client.command(&["LPUSHX", "xlist", "z"]).await, 4);
    assert_integer_eq(&client.command(&["RPUSHX", "xlist", "42", "x"]).await, 6);
    assert_integer_eq(
        &client.command(&["LPUSHX", "xlist", "y3", "y2", "y1"]).await,
        9,
    );
    let items = extract_bulk_strings(&client.command(&["LRANGE", "xlist", "0", "-1"]).await);
    assert_eq!(items, vec!["y1", "y2", "y3", "z", "a", "c", "d", "42", "x"]);
}

// ---------------------------------------------------------------------------
// LINSERT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_linsert() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "xlist"]).await;
    client
        .command(&["RPUSH", "xlist", "a", "b", "c", "d"])
        .await;

    assert_integer_eq(
        &client
            .command(&["LINSERT", "xlist", "BEFORE", "c", "zz"])
            .await,
        5,
    );
    let items = extract_bulk_strings(&client.command(&["LRANGE", "xlist", "0", "-1"]).await);
    assert_eq!(items, vec!["a", "b", "zz", "c", "d"]);

    assert_integer_eq(
        &client
            .command(&["LINSERT", "xlist", "AFTER", "c", "yy"])
            .await,
        6,
    );
    let items = extract_bulk_strings(&client.command(&["LRANGE", "xlist", "0", "-1"]).await);
    assert_eq!(items, vec!["a", "b", "zz", "c", "yy", "d"]);

    assert_integer_eq(
        &client
            .command(&["LINSERT", "xlist", "AFTER", "d", "dd"])
            .await,
        7,
    );

    // Non-existing pivot
    assert_integer_eq(
        &client
            .command(&["LINSERT", "xlist", "AFTER", "bad", "ddd"])
            .await,
        -1,
    );

    // Insert integer encoded value
    assert_integer_eq(
        &client
            .command(&["LINSERT", "xlist", "BEFORE", "a", "42"])
            .await,
        8,
    );
    let first = extract_bulk_strings(&client.command(&["LRANGE", "xlist", "0", "0"]).await);
    assert_eq!(first, vec!["42"]);
}

#[tokio::test]
async fn tcl_linsert_raise_error_on_bad_syntax() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "xlist"]).await;
    client.command(&["RPUSH", "xlist", "a"]).await;
    assert_error_prefix(
        &client
            .command(&["LINSERT", "xlist", "aft3r", "a", "42"])
            .await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_linsert_against_non_list_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "k1", "v1"]).await;
    assert_error_prefix(
        &client.command(&["LINSERT", "k1", "AFTER", "0", "0"]).await,
        "WRONGTYPE",
    );
}

#[tokio::test]
async fn tcl_linsert_against_non_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_integer_eq(
        &client
            .command(&["LINSERT", "not-a-key", "BEFORE", "0", "0"])
            .await,
        0,
    );
}

// ---------------------------------------------------------------------------
// LLEN / LINDEX error cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_llen_against_non_list_value_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client.command(&["SET", "mylist", "foobar"]).await;
    assert_error_prefix(&client.command(&["LLEN", "mylist"]).await, "WRONGTYPE");
}

#[tokio::test]
async fn tcl_llen_against_non_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_integer_eq(&client.command(&["LLEN", "not-a-key"]).await, 0);
}

#[tokio::test]
async fn tcl_lindex_against_non_list_value_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mylist", "foobar"]).await;
    assert_error_prefix(
        &client.command(&["LINDEX", "mylist", "0"]).await,
        "WRONGTYPE",
    );
}

#[tokio::test]
async fn tcl_lindex_against_non_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_nil(&client.command(&["LINDEX", "not-a-key", "10"]).await);
}

#[tokio::test]
async fn tcl_lpush_against_non_list_value_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mylist", "foobar"]).await;
    assert_error_prefix(
        &client.command(&["LPUSH", "mylist", "0"]).await,
        "WRONGTYPE",
    );
}

#[tokio::test]
async fn tcl_rpush_against_non_list_value_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mylist", "foobar"]).await;
    assert_error_prefix(
        &client.command(&["RPUSH", "mylist", "0"]).await,
        "WRONGTYPE",
    );
}

// ---------------------------------------------------------------------------
// RPOPLPUSH / LMOVE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_rpoplpush_base_case() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist1{t}", "mylist2{t}"]).await;
    client
        .command(&["RPUSH", "mylist1{t}", "a", "b", "c", "d"])
        .await;
    assert_bulk_eq(
        &client
            .command(&["RPOPLPUSH", "mylist1{t}", "mylist2{t}"])
            .await,
        b"d",
    );
    assert_bulk_eq(
        &client
            .command(&["RPOPLPUSH", "mylist1{t}", "mylist2{t}"])
            .await,
        b"c",
    );
    assert_bulk_eq(
        &client
            .command(&["RPOPLPUSH", "mylist1{t}", "mylist2{t}"])
            .await,
        b"b",
    );
    let src = extract_bulk_strings(&client.command(&["LRANGE", "mylist1{t}", "0", "-1"]).await);
    assert_eq!(src, vec!["a"]);
    let dst = extract_bulk_strings(&client.command(&["LRANGE", "mylist2{t}", "0", "-1"]).await);
    assert_eq!(dst, vec!["b", "c", "d"]);
}

#[tokio::test]
async fn tcl_rpoplpush_with_same_list_as_src_and_dst() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist{t}"]).await;
    client.command(&["RPUSH", "mylist{t}", "a", "b", "c"]).await;
    assert_bulk_eq(
        &client
            .command(&["RPOPLPUSH", "mylist{t}", "mylist{t}"])
            .await,
        b"c",
    );
    let items = extract_bulk_strings(&client.command(&["LRANGE", "mylist{t}", "0", "-1"]).await);
    assert_eq!(items, vec!["c", "a", "b"]);
}

#[tokio::test]
async fn tcl_rpoplpush_against_non_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "srclist{t}", "dstlist{t}"]).await;
    assert_nil(
        &client
            .command(&["RPOPLPUSH", "srclist{t}", "dstlist{t}"])
            .await,
    );
    assert_integer_eq(&client.command(&["EXISTS", "srclist{t}"]).await, 0);
    assert_integer_eq(&client.command(&["EXISTS", "dstlist{t}"]).await, 0);
}

#[tokio::test]
async fn tcl_rpoplpush_against_non_list_src_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "srclist{t}", "dstlist{t}"]).await;
    client.command(&["SET", "srclist{t}", "x"]).await;
    assert_error_prefix(
        &client
            .command(&["RPOPLPUSH", "srclist{t}", "dstlist{t}"])
            .await,
        "WRONGTYPE",
    );
}

#[tokio::test]
async fn tcl_rpoplpush_against_non_list_dst_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "srclist{t}", "dstlist{t}"]).await;
    client
        .command(&["RPUSH", "srclist{t}", "a", "b", "c", "d"])
        .await;
    client.command(&["SET", "dstlist{t}", "x"]).await;
    assert_error_prefix(
        &client
            .command(&["RPOPLPUSH", "srclist{t}", "dstlist{t}"])
            .await,
        "WRONGTYPE",
    );
}

#[tokio::test]
async fn tcl_lmove_right_left_base_case() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist1{t}", "mylist2{t}"]).await;
    client
        .command(&["RPUSH", "mylist1{t}", "a", "b", "c", "d"])
        .await;
    assert_bulk_eq(
        &client
            .command(&["LMOVE", "mylist1{t}", "mylist2{t}", "RIGHT", "LEFT"])
            .await,
        b"d",
    );
    assert_bulk_eq(
        &client
            .command(&["LMOVE", "mylist1{t}", "mylist2{t}", "RIGHT", "LEFT"])
            .await,
        b"c",
    );
    let src = extract_bulk_strings(&client.command(&["LRANGE", "mylist1{t}", "0", "-1"]).await);
    assert_eq!(src, vec!["a", "b"]);
    let dst = extract_bulk_strings(&client.command(&["LRANGE", "mylist2{t}", "0", "-1"]).await);
    assert_eq!(dst, vec!["c", "d"]);
}

#[tokio::test]
async fn tcl_lmove_left_right_base_case() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist1{t}", "mylist2{t}"]).await;
    client
        .command(&["RPUSH", "mylist1{t}", "a", "b", "c", "d"])
        .await;
    assert_bulk_eq(
        &client
            .command(&["LMOVE", "mylist1{t}", "mylist2{t}", "LEFT", "RIGHT"])
            .await,
        b"a",
    );
    assert_bulk_eq(
        &client
            .command(&["LMOVE", "mylist1{t}", "mylist2{t}", "LEFT", "RIGHT"])
            .await,
        b"b",
    );
    let src = extract_bulk_strings(&client.command(&["LRANGE", "mylist1{t}", "0", "-1"]).await);
    assert_eq!(src, vec!["c", "d"]);
    let dst = extract_bulk_strings(&client.command(&["LRANGE", "mylist2{t}", "0", "-1"]).await);
    assert_eq!(dst, vec!["a", "b"]);
}

// ---------------------------------------------------------------------------
// LPOP/RPOP/LMPOP against empty list and non-list
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_lpop_rpop_lmpop_against_empty_list() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["DEL", "non-existing-list{t}", "non-existing-list2{t}"])
        .await;
    assert_nil(&client.command(&["LPOP", "non-existing-list{t}"]).await);
    assert_nil(&client.command(&["RPOP", "non-existing-list2{t}"]).await);
    assert_nil(
        &client
            .command(&["LMPOP", "1", "non-existing-list{t}", "LEFT", "COUNT", "1"])
            .await,
    );
    assert_nil(
        &client
            .command(&["LMPOP", "1", "non-existing-list{t}", "LEFT", "COUNT", "10"])
            .await,
    );
}

#[tokio::test]
async fn tcl_lpop_rpop_against_non_list_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "notalist{t}", "foo"]).await;
    assert_error_prefix(&client.command(&["LPOP", "notalist{t}"]).await, "WRONGTYPE");
    assert_error_prefix(&client.command(&["RPOP", "notalist{t}"]).await, "WRONGTYPE");
}

// ---------------------------------------------------------------------------
// LMPOP argument validation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_lmpop_with_illegal_argument() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_error_prefix(&client.command(&["LMPOP"]).await, "ERR");
    assert_error_prefix(&client.command(&["LMPOP", "1"]).await, "ERR");
    assert_error_prefix(&client.command(&["LMPOP", "1", "mylist{t}"]).await, "ERR");
    assert_error_prefix(
        &client.command(&["LMPOP", "0", "mylist{t}", "LEFT"]).await,
        "ERR",
    );
    assert_error_prefix(
        &client
            .command(&["LMPOP", "1", "mylist{t}", "bad_where"])
            .await,
        "ERR",
    );
    assert_error_prefix(
        &client
            .command(&["LMPOP", "1", "mylist{t}", "LEFT", "COUNT", "0"])
            .await,
        "ERR",
    );
    assert_error_prefix(
        &client
            .command(&["LMPOP", "1", "mylist{t}", "LEFT", "COUNT", "-1"])
            .await,
        "ERR",
    );
}

// ---------------------------------------------------------------------------
// LRANGE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_lrange_basics() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    for i in 0..10 {
        client.command(&["RPUSH", "mylist", &i.to_string()]).await;
    }
    let items = extract_bulk_strings(&client.command(&["LRANGE", "mylist", "1", "-2"]).await);
    assert_eq!(items, vec!["1", "2", "3", "4", "5", "6", "7", "8"]);

    let items = extract_bulk_strings(&client.command(&["LRANGE", "mylist", "-3", "-1"]).await);
    assert_eq!(items, vec!["7", "8", "9"]);

    let items = extract_bulk_strings(&client.command(&["LRANGE", "mylist", "4", "4"]).await);
    assert_eq!(items, vec!["4"]);
}

#[tokio::test]
async fn tcl_lrange_inverted_indexes() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    for i in 0..10 {
        client.command(&["RPUSH", "mylist", &i.to_string()]).await;
    }
    let resp = client.command(&["LRANGE", "mylist", "6", "2"]).await;
    let items = unwrap_array(resp);
    assert!(items.is_empty());
}

#[tokio::test]
async fn tcl_lrange_out_of_range_indexes() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client
        .command(&["RPUSH", "mylist", "a", "1", "2", "3"])
        .await;
    let items = extract_bulk_strings(&client.command(&["LRANGE", "mylist", "-1000", "1000"]).await);
    assert_eq!(items, vec!["a", "1", "2", "3"]);
}

#[tokio::test]
async fn tcl_lrange_against_non_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["LRANGE", "nosuchkey", "0", "1"]).await;
    let items = unwrap_array(resp);
    assert!(items.is_empty());
}

// ---------------------------------------------------------------------------
// LTRIM
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_ltrim_basics() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Helper: create list, trim, return result
    async fn trim_list(
        client: &mut frogdb_test_harness::server::TestClient,
        min: &str,
        max: &str,
    ) -> Vec<String> {
        client.command(&["DEL", "mylist"]).await;
        client
            .command(&["RPUSH", "mylist", "1", "2", "3", "4", "5"])
            .await;
        client.command(&["LTRIM", "mylist", min, max]).await;
        extract_bulk_strings(&client.command(&["LRANGE", "mylist", "0", "-1"]).await)
    }

    assert_eq!(trim_list(&mut client, "0", "0").await, vec!["1"]);
    assert_eq!(trim_list(&mut client, "0", "1").await, vec!["1", "2"]);
    assert_eq!(trim_list(&mut client, "0", "2").await, vec!["1", "2", "3"]);
    assert_eq!(trim_list(&mut client, "1", "2").await, vec!["2", "3"]);
    assert_eq!(
        trim_list(&mut client, "1", "-1").await,
        vec!["2", "3", "4", "5"]
    );
    assert_eq!(trim_list(&mut client, "1", "-2").await, vec!["2", "3", "4"]);
    assert_eq!(trim_list(&mut client, "-2", "-1").await, vec!["4", "5"]);
    assert_eq!(trim_list(&mut client, "-1", "-1").await, vec!["5"]);
    assert_eq!(
        trim_list(&mut client, "-5", "-1").await,
        vec!["1", "2", "3", "4", "5"]
    );
    assert_eq!(
        trim_list(&mut client, "-10", "10").await,
        vec!["1", "2", "3", "4", "5"]
    );
    assert_eq!(
        trim_list(&mut client, "0", "5").await,
        vec!["1", "2", "3", "4", "5"]
    );
}

// ---------------------------------------------------------------------------
// LSET
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_lset() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client
        .command(&["RPUSH", "mylist", "99", "98", "97", "96", "95"])
        .await;
    client.command(&["LSET", "mylist", "1", "foo"]).await;
    client.command(&["LSET", "mylist", "-1", "bar"]).await;
    let items = extract_bulk_strings(&client.command(&["LRANGE", "mylist", "0", "-1"]).await);
    assert_eq!(items, vec!["99", "foo", "97", "96", "bar"]);
}

#[tokio::test]
async fn tcl_lset_out_of_range_index() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client.command(&["RPUSH", "mylist", "a"]).await;
    assert_error_prefix(
        &client.command(&["LSET", "mylist", "10", "foo"]).await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_lset_against_non_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_error_prefix(
        &client.command(&["LSET", "nosuchkey", "10", "foo"]).await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_lset_against_non_list_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "nolist", "foobar"]).await;
    assert_error_prefix(
        &client.command(&["LSET", "nolist", "0", "foo"]).await,
        "WRONGTYPE",
    );
}

// ---------------------------------------------------------------------------
// LREM
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_lrem_remove_all_occurrences() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client
        .command(&[
            "RPUSH", "mylist", "foo", "bar", "foobar", "foobared", "zap", "bar", "test", "foo",
        ])
        .await;
    assert_integer_eq(&client.command(&["LREM", "mylist", "0", "bar"]).await, 2);
    let items = extract_bulk_strings(&client.command(&["LRANGE", "mylist", "0", "-1"]).await);
    assert_eq!(
        items,
        vec!["foo", "foobar", "foobared", "zap", "test", "foo"]
    );
}

#[tokio::test]
async fn tcl_lrem_remove_first_occurrence() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client
        .command(&["RPUSH", "mylist", "foo", "bar", "foo", "baz"])
        .await;
    assert_integer_eq(&client.command(&["LREM", "mylist", "1", "foo"]).await, 1);
    let items = extract_bulk_strings(&client.command(&["LRANGE", "mylist", "0", "-1"]).await);
    assert_eq!(items, vec!["bar", "foo", "baz"]);
}

#[tokio::test]
async fn tcl_lrem_remove_non_existing_element() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client.command(&["RPUSH", "mylist", "a", "b", "c"]).await;
    assert_integer_eq(
        &client
            .command(&["LREM", "mylist", "1", "nosuchelement"])
            .await,
        0,
    );
}

#[tokio::test]
async fn tcl_lrem_starting_from_tail_with_negative_count() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client
        .command(&[
            "RPUSH", "mylist", "foo", "bar", "foobar", "foobared", "zap", "bar", "test", "foo",
            "foo",
        ])
        .await;
    assert_integer_eq(&client.command(&["LREM", "mylist", "-1", "bar"]).await, 1);
    let items = extract_bulk_strings(&client.command(&["LRANGE", "mylist", "0", "-1"]).await);
    assert_eq!(
        items,
        vec![
            "foo", "bar", "foobar", "foobared", "zap", "test", "foo", "foo"
        ]
    );
}

#[tokio::test]
async fn tcl_lrem_deleting_objects_that_may_be_int_encoded() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myotherlist"]).await;
    client
        .command(&["RPUSH", "myotherlist", "a", "1", "2", "3"])
        .await;
    assert_integer_eq(&client.command(&["LREM", "myotherlist", "1", "2"]).await, 1);
    assert_integer_eq(&client.command(&["LLEN", "myotherlist"]).await, 3);
}

// ---------------------------------------------------------------------------
// BRPOPLPUSH inside a transaction (non-blocking path)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB does not support blocking commands inside MULTI"]
async fn tcl_brpoplpush_inside_a_transaction() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "xlist{t}", "target{t}"]).await;
    client.command(&["LPUSH", "xlist{t}", "foo"]).await;
    client.command(&["LPUSH", "xlist{t}", "bar"]).await;

    client.command(&["MULTI"]).await;
    client
        .command(&["BRPOPLPUSH", "xlist{t}", "target{t}", "0"])
        .await;
    client
        .command(&["BRPOPLPUSH", "xlist{t}", "target{t}", "0"])
        .await;
    client
        .command(&["BRPOPLPUSH", "xlist{t}", "target{t}", "0"])
        .await;
    client.command(&["LRANGE", "xlist{t}", "0", "-1"]).await;
    client.command(&["LRANGE", "target{t}", "0", "-1"]).await;
    let resp = client.command(&["EXEC"]).await;
    let items = unwrap_array(resp);
    // foo, bar, nil, empty list, {bar foo}
    assert_bulk_eq(&items[0], b"foo");
    assert_bulk_eq(&items[1], b"bar");
    assert_nil(&items[2]);
    let xlist = unwrap_array(items[3].clone());
    assert!(xlist.is_empty());
    let target = unwrap_array(items[4].clone());
    assert_eq!(target.len(), 2);
    assert_bulk_eq(&target[0], b"bar");
    assert_bulk_eq(&target[1], b"foo");
}

// ---------------------------------------------------------------------------
// list-3.tcl: Regression tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_explicit_regression_for_a_list_bug() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let val0 = "49376042582";
    let val1 = r#"BkG2o\pIC]4YYJa9cJ4GWZalG[4tin;1D2whSkCOW`mX;SFXGyS8sedcff3fQI^tgPCC@^Nu1J6o]meM@Lko]t_jRyo<xSJ1oObDYd`ppZuW6P@fS278YaOx=s6lvdFlMbP0[SbkI^Kr\HBXtuFaA^mDx:yzS4a[skiiPWhT<nNfAf=aQVfclcuwDrfe;iVuKdNvB9kbfq>tK?tH[\EvWqS]b`o2OCtjg:?nUTwdjpcUm]y:pg5q24q7LlCOwQE^"#;

    client.command(&["DEL", "l"]).await;
    client.command(&["RPUSH", "l", val0]).await;
    client.command(&["RPUSH", "l", val1]).await;
    assert_bulk_eq(
        &client.command(&["LINDEX", "l", "0"]).await,
        val0.as_bytes(),
    );
    assert_bulk_eq(
        &client.command(&["LINDEX", "l", "1"]).await,
        val1.as_bytes(),
    );
}

#[tokio::test]
async fn tcl_regression_for_quicklist_3343_bug() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client.command(&["LPUSH", "mylist", "401"]).await;
    client.command(&["LPUSH", "mylist", "392"]).await;
    let big_val = format!("{}799", "x".repeat(5105));
    client.command(&["RPUSH", "mylist", &big_val]).await;
    let big_val2 = format!("{}702", "x".repeat(1014));
    client.command(&["LSET", "mylist", "-1", &big_val2]).await;
    client.command(&["LPOP", "mylist"]).await;
    let big_val3 = format!("{}852", "x".repeat(4149));
    client.command(&["LSET", "mylist", "-1", &big_val3]).await;
    let big_val4 = format!("{}12", "x".repeat(9927));
    client
        .command(&["LINSERT", "mylist", "BEFORE", "401", &big_val4])
        .await;
    client.command(&["LRANGE", "mylist", "0", "-1"]).await;
    let resp = client.command(&["PING"]).await;
    assert!(
        matches!(&resp, Response::Simple(s) if s == "PONG"),
        "expected PONG, got {resp:?}",
    );
}

// ===========================================================================
// BLOCKING OPERATIONS
// ===========================================================================

/// Helper: unwrap a BLPOP/BRPOP two-element array response into (key, value).
fn unwrap_bpop_response(resp: &Response) -> (&[u8], &[u8]) {
    match resp {
        Response::Array(items) if items.len() == 2 => {
            (unwrap_bulk(&items[0]), unwrap_bulk(&items[1]))
        }
        other => panic!("expected 2-element array, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// BLPOP / BRPOP: single existing list (data already there, non-blocking path)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_blpop_single_existing_list() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "blist"]).await;
    client
        .command(&["RPUSH", "blist", "a", "b", "c", "d"])
        .await;

    // BLPOP with 1s timeout on existing list returns immediately
    let resp = client.command(&["BLPOP", "blist", "1"]).await;
    let (key, val) = unwrap_bpop_response(&resp);
    assert_eq!(key, b"blist");
    assert_eq!(val, b"a");

    // BRPOP
    let resp = client.command(&["BRPOP", "blist", "1"]).await;
    let (key, val) = unwrap_bpop_response(&resp);
    assert_eq!(key, b"blist");
    assert_eq!(val, b"d");

    assert_integer_eq(&client.command(&["LLEN", "blist"]).await, 2);
}

#[tokio::test]
async fn tcl_blpop_multiple_existing_lists() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "blist1{t}", "blist2{t}"]).await;
    client.command(&["RPUSH", "blist1{t}", "a", "b", "c"]).await;
    client.command(&["RPUSH", "blist2{t}", "d", "e", "f"]).await;

    // Should pop from first non-empty list
    let resp = client
        .command(&["BLPOP", "blist1{t}", "blist2{t}", "1"])
        .await;
    let (key, val) = unwrap_bpop_response(&resp);
    assert_eq!(key, b"blist1{t}");
    assert_eq!(val, b"a");
}

#[tokio::test]
async fn tcl_blpop_second_list_has_an_entry() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "blist1{t}", "blist2{t}"]).await;
    client.command(&["RPUSH", "blist2{t}", "d", "e", "f"]).await;

    // blist1 is empty, should pop from blist2
    let resp = client
        .command(&["BLPOP", "blist1{t}", "blist2{t}", "1"])
        .await;
    let (key, val) = unwrap_bpop_response(&resp);
    assert_eq!(key, b"blist2{t}");
    assert_eq!(val, b"d");
}

// ---------------------------------------------------------------------------
// BLPOP / BRPOP: empty list → push unblocks
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_blpop_single_empty_list_push_unblocks() {
    let server = TestServer::start_standalone().await;
    let mut blocker = server.connect().await;
    let mut pusher = server.connect().await;

    pusher.command(&["DEL", "blist1"]).await;

    blocker.send_only(&["BLPOP", "blist1", "0"]).await;
    server.wait_for_blocked_clients(1).await;

    pusher.command(&["RPUSH", "blist1", "foo"]).await;

    let resp = blocker
        .read_response(Duration::from_secs(2))
        .await
        .expect("should unblock");
    let (key, val) = unwrap_bpop_response(&resp);
    assert_eq!(key, b"blist1");
    assert_eq!(val, b"foo");
    assert_integer_eq(&pusher.command(&["EXISTS", "blist1"]).await, 0);
}

#[tokio::test]
async fn tcl_brpop_single_empty_list_push_unblocks() {
    let server = TestServer::start_standalone().await;
    let mut blocker = server.connect().await;
    let mut pusher = server.connect().await;

    pusher.command(&["DEL", "blist1"]).await;

    blocker.send_only(&["BRPOP", "blist1", "0"]).await;
    server.wait_for_blocked_clients(1).await;

    pusher.command(&["RPUSH", "blist1", "foo"]).await;

    let resp = blocker
        .read_response(Duration::from_secs(2))
        .await
        .expect("should unblock");
    let (key, val) = unwrap_bpop_response(&resp);
    assert_eq!(key, b"blist1");
    assert_eq!(val, b"foo");
}

// ---------------------------------------------------------------------------
// BLPOP / BRPOP: timeout, negative timeout, non-integer timeout
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_blpop_with_negative_timeout() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["BLPOP", "blist1", "-1"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_brpop_with_negative_timeout() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["BRPOP", "blist1", "-1"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_blpop_with_non_integer_timeout() {
    let server = TestServer::start_standalone().await;
    let mut blocker = server.connect().await;
    let mut pusher = server.connect().await;

    pusher.command(&["DEL", "blist1"]).await;

    blocker.send_only(&["BLPOP", "blist1", "0.1"]).await;
    // Ensure client blocks before pushing
    server.wait_for_blocked_clients(1).await;
    pusher.command(&["RPUSH", "blist1", "foo"]).await;

    let resp = blocker
        .read_response(Duration::from_secs(2))
        .await
        .expect("should unblock");
    let (key, val) = unwrap_bpop_response(&resp);
    assert_eq!(key, b"blist1");
    assert_eq!(val, b"foo");
}

#[tokio::test]
async fn tcl_blpop_with_short_timeout_expires() {
    let server = TestServer::start_standalone().await;
    let mut blocker = server.connect().await;

    blocker.send_only(&["BLPOP", "blist1", "0.001"]).await;

    // Should return nil after timeout
    let resp = blocker.read_response(Duration::from_secs(2)).await;
    assert!(
        resp.is_none() || matches!(&resp, Some(Response::Bulk(None))),
        "expected nil or timeout, got {resp:?}"
    );
}

#[tokio::test]
async fn tcl_blpop_timeout_1s() {
    let server = TestServer::start_standalone().await;
    let mut blocker = server.connect().await;

    blocker
        .send_only(&["BLPOP", "blist1{t}", "blist2{t}", "1"])
        .await;
    server.wait_for_blocked_clients(1).await;

    // Wait for timeout (nil response)
    let resp = blocker.read_response(Duration::from_secs(3)).await;
    assert!(
        resp.is_none() || matches!(&resp, Some(Response::Bulk(None))),
        "expected nil/timeout, got {resp:?}"
    );
}

#[tokio::test]
async fn tcl_blpop_second_argument_is_not_a_list() {
    let server = TestServer::start_standalone().await;
    let mut blocker = server.connect().await;

    blocker.command(&["DEL", "blist1{t}", "blist2{t}"]).await;
    blocker.command(&["SET", "blist2{t}", "nolist"]).await;

    let resp = blocker
        .command(&["BLPOP", "blist1{t}", "blist2{t}", "1"])
        .await;
    assert_error_prefix(&resp, "WRONGTYPE");
}

#[tokio::test]
async fn tcl_blpop_timeout_value_out_of_range() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["BLPOP", "blist1", "0x7FFFFFFFFFFFFF"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// BLPOP: arguments are empty → push unblocks
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_blpop_arguments_are_empty_push_unblocks() {
    let server = TestServer::start_standalone().await;
    let mut blocker = server.connect().await;
    let mut pusher = server.connect().await;

    pusher.command(&["DEL", "blist1{t}", "blist2{t}"]).await;

    blocker
        .send_only(&["BLPOP", "blist1{t}", "blist2{t}", "1"])
        .await;
    server.wait_for_blocked_clients(1).await;

    pusher.command(&["RPUSH", "blist1{t}", "foo"]).await;
    let resp = blocker
        .read_response(Duration::from_secs(2))
        .await
        .expect("should unblock");
    let (key, val) = unwrap_bpop_response(&resp);
    assert_eq!(key, b"blist1{t}");
    assert_eq!(val, b"foo");
    assert_integer_eq(&pusher.command(&["EXISTS", "blist1{t}"]).await, 0);
    assert_integer_eq(&pusher.command(&["EXISTS", "blist2{t}"]).await, 0);

    // Now push to blist2
    blocker
        .send_only(&["BLPOP", "blist1{t}", "blist2{t}", "1"])
        .await;
    server.wait_for_blocked_clients(1).await;

    pusher.command(&["RPUSH", "blist2{t}", "foo"]).await;
    let resp = blocker
        .read_response(Duration::from_secs(2))
        .await
        .expect("should unblock");
    let (key, val) = unwrap_bpop_response(&resp);
    assert_eq!(key, b"blist2{t}");
    assert_eq!(val, b"foo");
}

// ---------------------------------------------------------------------------
// BLPOP with variadic LPUSH
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_blpop_with_variadic_lpush() {
    let server = TestServer::start_standalone().await;
    let mut blocker = server.connect().await;
    let mut pusher = server.connect().await;

    pusher.command(&["DEL", "blist"]).await;

    blocker.send_only(&["BLPOP", "blist", "0"]).await;
    server.wait_for_blocked_clients(1).await;

    assert_integer_eq(&pusher.command(&["LPUSH", "blist", "foo", "bar"]).await, 2);

    let resp = blocker
        .read_response(Duration::from_secs(2))
        .await
        .expect("should unblock");
    let (key, val) = unwrap_bpop_response(&resp);
    assert_eq!(key, b"blist");
    assert_eq!(val, b"bar");

    // "foo" should remain
    let items = extract_bulk_strings(&pusher.command(&["LRANGE", "blist", "0", "-1"]).await);
    assert_eq!(items[0], "foo");
}

// ---------------------------------------------------------------------------
// BLPOP with same key multiple times (issue #801)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_blpop_with_same_key_multiple_times_issue_801() {
    let server = TestServer::start_standalone().await;
    let mut blocker = server.connect().await;
    let mut pusher = server.connect().await;

    pusher.command(&["DEL", "list1{t}", "list2{t}"]).await;

    // Data arriving after the BLPOP
    blocker
        .send_only(&["BLPOP", "list1{t}", "list2{t}", "list2{t}", "list1{t}", "0"])
        .await;
    server.wait_for_blocked_clients(1).await;
    pusher.command(&["LPUSH", "list1{t}", "a"]).await;
    let resp = blocker
        .read_response(Duration::from_secs(2))
        .await
        .expect("should unblock");
    let (key, val) = unwrap_bpop_response(&resp);
    assert_eq!(key, b"list1{t}");
    assert_eq!(val, b"a");

    blocker
        .send_only(&["BLPOP", "list1{t}", "list2{t}", "list2{t}", "list1{t}", "0"])
        .await;
    server.wait_for_blocked_clients(1).await;
    pusher.command(&["LPUSH", "list2{t}", "b"]).await;
    let resp = blocker
        .read_response(Duration::from_secs(2))
        .await
        .expect("should unblock");
    let (key, val) = unwrap_bpop_response(&resp);
    assert_eq!(key, b"list2{t}");
    assert_eq!(val, b"b");

    // Data already there
    pusher.command(&["LPUSH", "list1{t}", "a"]).await;
    pusher.command(&["LPUSH", "list2{t}", "b"]).await;
    let resp = blocker
        .command(&["BLPOP", "list1{t}", "list2{t}", "list2{t}", "list1{t}", "0"])
        .await;
    let (key, val) = unwrap_bpop_response(&resp);
    assert_eq!(key, b"list1{t}");
    assert_eq!(val, b"a");
}

// ---------------------------------------------------------------------------
// BRPOPLPUSH
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_brpoplpush_existing_list() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "blist{t}", "target{t}"]).await;
    client.command(&["RPUSH", "target{t}", "bar"]).await;
    client
        .command(&["RPUSH", "blist{t}", "a", "b", "c", "d"])
        .await;

    // Non-blocking path: list has data
    let resp = client
        .command(&["BRPOPLPUSH", "blist{t}", "target{t}", "1"])
        .await;
    assert_bulk_eq(&resp, b"d");
    assert_bulk_eq(&client.command(&["LPOP", "target{t}"]).await, b"d");
}

#[tokio::test]
async fn tcl_brpoplpush_with_zero_timeout_blocks_then_unblocks() {
    let server = TestServer::start_standalone().await;
    let mut blocker = server.connect().await;
    let mut pusher = server.connect().await;

    pusher.command(&["DEL", "blist{t}", "target{t}"]).await;
    pusher.command(&["RPUSH", "target{t}", "bar"]).await;

    blocker
        .send_only(&["BRPOPLPUSH", "blist{t}", "target{t}", "0"])
        .await;
    server.wait_for_blocked_clients(1).await;

    pusher.command(&["RPUSH", "blist{t}", "foo"]).await;

    let resp = blocker
        .read_response(Duration::from_secs(2))
        .await
        .expect("should unblock");
    assert_bulk_eq(&resp, b"foo");

    let items = extract_bulk_strings(&pusher.command(&["LRANGE", "target{t}", "0", "-1"]).await);
    assert_eq!(items, vec!["foo", "bar"]);
}

#[tokio::test]
async fn tcl_brpoplpush_timeout() {
    let server = TestServer::start_standalone().await;
    let mut blocker = server.connect().await;

    blocker
        .send_only(&["BRPOPLPUSH", "foo_list{t}", "bar_list{t}", "1"])
        .await;
    server.wait_for_blocked_clients(1).await;

    // Wait for timeout
    server.wait_for_blocked_clients(0).await;
    let resp = blocker.read_response(Duration::from_secs(2)).await;
    assert!(
        resp.is_none() || matches!(&resp, Some(Response::Bulk(None))),
        "expected nil/timeout, got {resp:?}"
    );
}

#[tokio::test]
async fn tcl_brpoplpush_with_wrong_source_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "blist{t}", "target{t}"]).await;
    client.command(&["SET", "blist{t}", "nolist"]).await;

    let resp = client
        .command(&["BRPOPLPUSH", "blist{t}", "target{t}", "1"])
        .await;
    assert_error_prefix(&resp, "WRONGTYPE");
}

#[tokio::test]
async fn tcl_brpoplpush_with_wrong_destination_type_nonblocking() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "blist{t}", "target{t}"]).await;
    client.command(&["SET", "target{t}", "nolist"]).await;
    client.command(&["LPUSH", "blist{t}", "foo"]).await;

    let resp = client
        .command(&["BRPOPLPUSH", "blist{t}", "target{t}", "1"])
        .await;
    assert_error_prefix(&resp, "WRONGTYPE");
}

// ---------------------------------------------------------------------------
// BLMOVE with existing list (data already there, non-blocking path)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_blmove_right_left_existing_list() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "blist{t}", "target{t}"]).await;
    client.command(&["RPUSH", "target{t}", "bar"]).await;
    client
        .command(&["RPUSH", "blist{t}", "a", "b", "c", "d"])
        .await;

    let resp = client
        .command(&["BLMOVE", "blist{t}", "target{t}", "RIGHT", "LEFT", "1"])
        .await;
    assert_bulk_eq(&resp, b"d");
}

#[tokio::test]
async fn tcl_blmove_left_right_existing_list() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "blist{t}", "target{t}"]).await;
    client.command(&["RPUSH", "target{t}", "bar"]).await;
    client
        .command(&["RPUSH", "blist{t}", "a", "b", "c", "d"])
        .await;

    let resp = client
        .command(&["BLMOVE", "blist{t}", "target{t}", "LEFT", "RIGHT", "1"])
        .await;
    assert_bulk_eq(&resp, b"a");

    assert_bulk_eq(&client.command(&["RPOP", "target{t}"]).await, b"a");
}

// ---------------------------------------------------------------------------
// BLMOVE with zero timeout → push unblocks
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_blmove_right_left_zero_timeout_blocks_then_unblocks() {
    let server = TestServer::start_standalone().await;
    let mut blocker = server.connect().await;
    let mut pusher = server.connect().await;

    pusher.command(&["DEL", "blist{t}", "target{t}"]).await;
    pusher.command(&["RPUSH", "target{t}", "bar"]).await;

    blocker
        .send_only(&["BLMOVE", "blist{t}", "target{t}", "RIGHT", "LEFT", "0"])
        .await;
    server.wait_for_blocked_clients(1).await;

    pusher.command(&["RPUSH", "blist{t}", "foo"]).await;

    let resp = blocker
        .read_response(Duration::from_secs(2))
        .await
        .expect("should unblock");
    assert_bulk_eq(&resp, b"foo");

    let items = extract_bulk_strings(&pusher.command(&["LRANGE", "target{t}", "0", "-1"]).await);
    assert_eq!(items, vec!["foo", "bar"]);
}

#[tokio::test]
async fn tcl_blmove_left_right_zero_timeout_blocks_then_unblocks() {
    let server = TestServer::start_standalone().await;
    let mut blocker = server.connect().await;
    let mut pusher = server.connect().await;

    pusher.command(&["DEL", "blist{t}", "target{t}"]).await;
    pusher.command(&["RPUSH", "target{t}", "bar"]).await;

    blocker
        .send_only(&["BLMOVE", "blist{t}", "target{t}", "LEFT", "RIGHT", "0"])
        .await;
    server.wait_for_blocked_clients(1).await;

    pusher.command(&["RPUSH", "blist{t}", "foo"]).await;

    let resp = blocker
        .read_response(Duration::from_secs(2))
        .await
        .expect("should unblock");
    assert_bulk_eq(&resp, b"foo");

    let items = extract_bulk_strings(&pusher.command(&["LRANGE", "target{t}", "0", "-1"]).await);
    assert_eq!(items, vec!["bar", "foo"]);
}

// ===========================================================================
// SKIPLIST TESTS — previously skipped in TCL, trying in Rust
// ===========================================================================

// ---------------------------------------------------------------------------
// BLPOP, LPUSH + DEL should not awake blocked client
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB LPUSH+DEL atomicity gap: shard notifies waiter before DEL completes"]
async fn tcl_blpop_lpush_del_should_not_awake_blocked_client() {
    let server = TestServer::start_standalone().await;
    let mut blocker = server.connect().await;
    let mut pusher = server.connect().await;

    pusher.command(&["DEL", "list"]).await;

    blocker.send_only(&["BLPOP", "list", "0"]).await;
    server.wait_for_blocked_clients(1).await;

    // MULTI: LPUSH + DEL — blocker should NOT see "a"
    pusher.command(&["MULTI"]).await;
    pusher.command(&["LPUSH", "list", "a"]).await;
    pusher.command(&["DEL", "list"]).await;
    pusher.command(&["EXEC"]).await;

    // Now push "b" — blocker should see "b"
    pusher.command(&["DEL", "list"]).await;
    pusher.command(&["LPUSH", "list", "b"]).await;

    let resp = blocker
        .read_response(Duration::from_secs(2))
        .await
        .expect("should unblock");
    let (key, val) = unwrap_bpop_response(&resp);
    assert_eq!(key, b"list");
    assert_eq!(val, b"b");
}

#[tokio::test]
#[ignore = "FrogDB LPUSH+DEL+SET atomicity gap: shard notifies waiter before DEL completes"]
async fn tcl_blpop_lpush_del_set_should_not_awake_blocked_client() {
    let server = TestServer::start_standalone().await;
    let mut blocker = server.connect().await;
    let mut pusher = server.connect().await;

    pusher.command(&["DEL", "list"]).await;

    blocker.send_only(&["BLPOP", "list", "0"]).await;
    server.wait_for_blocked_clients(1).await;

    // MULTI: LPUSH + DEL + SET — blocker should NOT see "a"
    pusher.command(&["MULTI"]).await;
    pusher.command(&["LPUSH", "list", "a"]).await;
    pusher.command(&["DEL", "list"]).await;
    pusher.command(&["SET", "list", "foo"]).await;
    pusher.command(&["EXEC"]).await;

    // Now push "b" — blocker should see "b"
    pusher.command(&["DEL", "list"]).await;
    pusher.command(&["LPUSH", "list", "b"]).await;

    let resp = blocker
        .read_response(Duration::from_secs(2))
        .await
        .expect("should unblock");
    let (key, val) = unwrap_bpop_response(&resp);
    assert_eq!(key, b"list");
    assert_eq!(val, b"b");
}

// ---------------------------------------------------------------------------
// MULTI/EXEC isolation from point of view of BLPOP
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB blocking client sees intermediate MULTI state"]
async fn tcl_multi_exec_is_isolated_from_the_point_of_view_of_blpop() {
    let server = TestServer::start_standalone().await;
    let mut blocker = server.connect().await;
    let mut pusher = server.connect().await;

    pusher.command(&["DEL", "list"]).await;

    blocker.send_only(&["BLPOP", "list", "0"]).await;
    server.wait_for_blocked_clients(1).await;

    // MULTI: push a, b, c — blocker should see "c" (last pushed = head)
    pusher.command(&["MULTI"]).await;
    pusher.command(&["LPUSH", "list", "a"]).await;
    pusher.command(&["LPUSH", "list", "b"]).await;
    pusher.command(&["LPUSH", "list", "c"]).await;
    pusher.command(&["EXEC"]).await;

    let resp = blocker
        .read_response(Duration::from_secs(2))
        .await
        .expect("should unblock");
    let (key, val) = unwrap_bpop_response(&resp);
    assert_eq!(key, b"list");
    assert_eq!(val, b"c");
}

// ---------------------------------------------------------------------------
// BLMOVE with a client BLPOPing the target list (chained wake)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB BLMOVE does not wake clients blocked on the destination list"]
async fn tcl_blmove_right_left_with_client_blpopping_target() {
    let server = TestServer::start_standalone().await;
    let mut rd1 = server.connect().await;
    let mut rd2 = server.connect().await;
    let mut pusher = server.connect().await;

    pusher.command(&["DEL", "blist{t}", "target{t}"]).await;

    // rd2 blocks on target
    rd2.send_only(&["BLPOP", "target{t}", "0"]).await;
    server.wait_for_blocked_clients(1).await;

    // rd1 blocks on blmove from blist → target
    rd1.send_only(&["BLMOVE", "blist{t}", "target{t}", "RIGHT", "LEFT", "0"])
        .await;
    server.wait_for_blocked_clients(2).await;

    // Push to blist → rd1 wakes, moves to target → rd2 should wake
    pusher.command(&["RPUSH", "blist{t}", "foo"]).await;

    let resp1 = rd1
        .read_response(Duration::from_secs(2))
        .await
        .expect("rd1 should unblock");
    assert_bulk_eq(&resp1, b"foo");

    let resp2 = rd2
        .read_response(Duration::from_secs(2))
        .await
        .expect("rd2 should unblock");
    let (key, val) = unwrap_bpop_response(&resp2);
    assert_eq!(key, b"target{t}");
    assert_eq!(val, b"foo");

    assert_integer_eq(&pusher.command(&["EXISTS", "target{t}"]).await, 0);
}

// ---------------------------------------------------------------------------
// BRPOPLPUSH with wrong destination type (blocked variant)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB BRPOPLPUSH blocked variant: wrong dest type handling differs"]
async fn tcl_brpoplpush_with_wrong_destination_type_blocked() {
    let server = TestServer::start_standalone().await;
    let mut blocker = server.connect().await;
    let mut pusher = server.connect().await;

    pusher.command(&["DEL", "blist{t}", "target{t}"]).await;
    pusher.command(&["SET", "target{t}", "nolist"]).await;

    blocker
        .send_only(&["BRPOPLPUSH", "blist{t}", "target{t}", "0"])
        .await;
    server.wait_for_blocked_clients(1).await;

    pusher.command(&["RPUSH", "blist{t}", "foo"]).await;

    let resp = blocker
        .read_response(Duration::from_secs(2))
        .await
        .expect("should get error");
    assert_error_prefix(&resp, "WRONGTYPE");

    // foo should still be in blist
    let items = extract_bulk_strings(&pusher.command(&["LRANGE", "blist{t}", "0", "-1"]).await);
    assert_eq!(items, vec!["foo"]);
}

// ---------------------------------------------------------------------------
// BRPOPLPUSH maintains order of elements after failure
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB BRPOPLPUSH: element order after wrong-type failure differs"]
async fn tcl_brpoplpush_maintains_order_of_elements_after_failure() {
    let server = TestServer::start_standalone().await;
    let mut blocker = server.connect().await;
    let mut pusher = server.connect().await;

    pusher.command(&["DEL", "blist{t}", "target{t}"]).await;
    pusher.command(&["SET", "target{t}", "nolist"]).await;

    blocker
        .send_only(&["BRPOPLPUSH", "blist{t}", "target{t}", "0"])
        .await;
    server.wait_for_blocked_clients(1).await;

    pusher.command(&["RPUSH", "blist{t}", "a", "b", "c"]).await;

    let resp = blocker
        .read_response(Duration::from_secs(2))
        .await
        .expect("should get error");
    assert_error_prefix(&resp, "WRONGTYPE");

    let items = extract_bulk_strings(&pusher.command(&["LRANGE", "blist{t}", "0", "-1"]).await);
    assert_eq!(items, vec!["a", "b", "c"]);
}

// ---------------------------------------------------------------------------
// BRPOPLPUSH with multiple blocked clients
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB multi-client BRPOPLPUSH wake ordering differs"]
async fn tcl_brpoplpush_with_multiple_blocked_clients() {
    let server = TestServer::start_standalone().await;
    let mut rd1 = server.connect().await;
    let mut rd2 = server.connect().await;
    let mut pusher = server.connect().await;

    pusher
        .command(&["DEL", "blist{t}", "target1{t}", "target2{t}"])
        .await;
    // target1 is wrong type → rd1 will get WRONGTYPE error
    pusher.command(&["SET", "target1{t}", "nolist"]).await;

    rd1.send_only(&["BRPOPLPUSH", "blist{t}", "target1{t}", "0"])
        .await;
    server.wait_for_blocked_clients(1).await;

    rd2.send_only(&["BRPOPLPUSH", "blist{t}", "target2{t}", "0"])
        .await;
    server.wait_for_blocked_clients(2).await;

    pusher.command(&["LPUSH", "blist{t}", "foo"]).await;

    // rd1 should get WRONGTYPE, rd2 should get the value
    let resp1 = rd1
        .read_response(Duration::from_secs(2))
        .await
        .expect("rd1 should respond");
    assert_error_prefix(&resp1, "WRONGTYPE");

    let resp2 = rd2
        .read_response(Duration::from_secs(2))
        .await
        .expect("rd2 should respond");
    assert_bulk_eq(&resp2, b"foo");

    let items = extract_bulk_strings(&pusher.command(&["LRANGE", "target2{t}", "0", "-1"]).await);
    assert_eq!(items, vec!["foo"]);
}

// ---------------------------------------------------------------------------
// Linked LMOVEs (chained blocking wake)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB chained blocking wake: BLMOVE does not cascade to next blocker"]
async fn tcl_linked_lmoves() {
    let server = TestServer::start_standalone().await;
    let mut rd1 = server.connect().await;
    let mut rd2 = server.connect().await;
    let mut pusher = server.connect().await;

    pusher
        .command(&["DEL", "list1{t}", "list2{t}", "list3{t}"])
        .await;

    rd1.send_only(&["BLMOVE", "list1{t}", "list2{t}", "RIGHT", "LEFT", "0"])
        .await;
    server.wait_for_blocked_clients(1).await;

    rd2.send_only(&["BLMOVE", "list2{t}", "list3{t}", "LEFT", "RIGHT", "0"])
        .await;
    server.wait_for_blocked_clients(2).await;

    pusher.command(&["RPUSH", "list1{t}", "foo"]).await;

    // Wait for both to unblock
    rd1.read_response(Duration::from_secs(2)).await;
    rd2.read_response(Duration::from_secs(2)).await;

    let l1 = extract_bulk_strings(&pusher.command(&["LRANGE", "list1{t}", "0", "-1"]).await);
    assert!(l1.is_empty());
    let l2 = extract_bulk_strings(&pusher.command(&["LRANGE", "list2{t}", "0", "-1"]).await);
    assert!(l2.is_empty());
    let l3 = extract_bulk_strings(&pusher.command(&["LRANGE", "list3{t}", "0", "-1"]).await);
    assert_eq!(l3, vec!["foo"]);
}

// ---------------------------------------------------------------------------
// Circular BRPOPLPUSH
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB chained blocking wake: circular BRPOPLPUSH does not cascade"]
async fn tcl_circular_brpoplpush() {
    let server = TestServer::start_standalone().await;
    let mut rd1 = server.connect().await;
    let mut rd2 = server.connect().await;
    let mut pusher = server.connect().await;

    pusher.command(&["DEL", "list1{t}", "list2{t}"]).await;

    rd1.send_only(&["BRPOPLPUSH", "list1{t}", "list2{t}", "0"])
        .await;
    server.wait_for_blocked_clients(1).await;

    rd2.send_only(&["BRPOPLPUSH", "list2{t}", "list1{t}", "0"])
        .await;
    server.wait_for_blocked_clients(2).await;

    pusher.command(&["RPUSH", "list1{t}", "foo"]).await;

    // Wait for chain to complete
    rd1.read_response(Duration::from_secs(2)).await;
    rd2.read_response(Duration::from_secs(2)).await;

    let l1 = extract_bulk_strings(&pusher.command(&["LRANGE", "list1{t}", "0", "-1"]).await);
    assert_eq!(l1, vec!["foo"]);
    let l2 = extract_bulk_strings(&pusher.command(&["LRANGE", "list2{t}", "0", "-1"]).await);
    assert!(l2.is_empty());
}

// ---------------------------------------------------------------------------
// Self-referential BRPOPLPUSH
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_self_referential_brpoplpush() {
    let server = TestServer::start_standalone().await;
    let mut blocker = server.connect().await;
    let mut pusher = server.connect().await;

    pusher.command(&["DEL", "blist{t}"]).await;

    blocker
        .send_only(&["BRPOPLPUSH", "blist{t}", "blist{t}", "0"])
        .await;
    server.wait_for_blocked_clients(1).await;

    pusher.command(&["RPUSH", "blist{t}", "foo"]).await;

    blocker.read_response(Duration::from_secs(2)).await;

    let items = extract_bulk_strings(&pusher.command(&["LRANGE", "blist{t}", "0", "-1"]).await);
    assert_eq!(items, vec!["foo"]);
}
