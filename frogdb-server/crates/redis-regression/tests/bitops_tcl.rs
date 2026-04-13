//! Rust port of Redis 8.6.0 `unit/bitops.tcl` test suite.
//!
//! Excluded:
//! - Fuzzing / stress tests (BITCOUNT fuzzing, BITOP fuzzing, BITPOS fuzzy testing)
//! - `BITPOS/BITCOUNT fuzzy testing using SETBIT` (complex Tcl helper + randomized)
//! - `SETBIT/BITFIELD only increase dirty when the value changed` (needs server INFO introspection)
//! - `BIT pos larger than UINT_MAX` (large-memory tag)
//! - `SETBIT values larger than UINT32_MAX` (large-memory + needs:debug)
//! - Encoding-loop variants (converted once with default encoding)
//! - `BITOP ONE` / `BITOP DIFF` / `BITOP DIFF1` / `BITOP ANDOR` (Redis 8.x extensions, not standard)
//!
//! ## Intentional exclusions
//!
//! Fuzz / stress tests (require Tcl helpers for random data generation):
//! - `BITOP $op fuzzing` — tested-elsewhere — fuzzing/stress
//! - `BITOP NOT fuzzing` — tested-elsewhere — fuzzing/stress
//! - `BITPOS bit=1 fuzzy testing using SETBIT` — tested-elsewhere — fuzzing/stress
//! - `BITPOS bit=0 fuzzy testing using SETBIT` — tested-elsewhere — fuzzing/stress
//! - `BITPOS/BITCOUNT fuzzy testing using SETBIT` — tested-elsewhere — fuzzing/stress
//!
//! Internal `dirty` counter introspection (needs INFO server stats access):
//! - `SETBIT/BITFIELD only increase dirty when the value changed` — redis-specific — Redis-internal stat (dirty counter)
//!
//! Large-memory tests (>UINT32_MAX bit positions):
//! - `BIT pos larger than UINT_MAX` — tested-elsewhere — large-memory
//! - `SETBIT values larger than UINT32_MAX and lzf_compress/lzf_decompress correctly` — tested-elsewhere — large-memory + needs:debug

use bytes::Bytes;
use frogdb_protocol::Response;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// BITCOUNT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_bitcount_against_wrong_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client.command(&["LPUSH", "mylist", "a", "b", "c"]).await;

    assert_error_prefix(&client.command(&["BITCOUNT", "mylist"]).await, "WRONGTYPE");
    assert_error_prefix(
        &client.command(&["BITCOUNT", "mylist", "0", "100"]).await,
        "WRONGTYPE",
    );
    // with negative indexes where start > end
    assert_error_prefix(
        &client.command(&["BITCOUNT", "mylist", "-6", "-7"]).await,
        "WRONGTYPE",
    );
    assert_error_prefix(
        &client
            .command(&["BITCOUNT", "mylist", "-6", "-15", "bit"])
            .await,
        "WRONGTYPE",
    );
}

#[tokio::test]
async fn tcl_bitcount_returns_0_against_non_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "no-key"]).await;
    assert_integer_eq(&client.command(&["BITCOUNT", "no-key"]).await, 0);
    assert_integer_eq(
        &client
            .command(&["BITCOUNT", "no-key", "0", "1000", "bit"])
            .await,
        0,
    );
}

#[tokio::test]
async fn tcl_bitcount_returns_0_with_out_of_range_indexes() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "str", "xxxx"]).await;
    assert_integer_eq(&client.command(&["BITCOUNT", "str", "4", "10"]).await, 0);
    assert_integer_eq(
        &client
            .command(&["BITCOUNT", "str", "32", "87", "bit"])
            .await,
        0,
    );
}

#[tokio::test]
async fn tcl_bitcount_returns_0_with_negative_indexes_where_start_gt_end() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "str", "xxxx"]).await;
    assert_integer_eq(&client.command(&["BITCOUNT", "str", "-6", "-7"]).await, 0);
    assert_integer_eq(
        &client
            .command(&["BITCOUNT", "str", "-6", "-15", "bit"])
            .await,
        0,
    );

    // against non existing key
    client.command(&["DEL", "str"]).await;
    assert_integer_eq(&client.command(&["BITCOUNT", "str", "-6", "-7"]).await, 0);
    assert_integer_eq(
        &client
            .command(&["BITCOUNT", "str", "-6", "-15", "bit"])
            .await,
        0,
    );
}

#[tokio::test]
async fn tcl_bitcount_against_test_vector_empty() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command_raw(&[
            &Bytes::from_static(b"SET"),
            &Bytes::from_static(b"str"),
            &Bytes::from_static(b""),
        ])
        .await;
    assert_integer_eq(&client.command(&["BITCOUNT", "str"]).await, 0);
    assert_integer_eq(
        &client.command(&["BITCOUNT", "str", "0", "-1", "bit"]).await,
        0,
    );
}

#[tokio::test]
async fn tcl_bitcount_against_test_vector_0xaa() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command_raw(&[
            &Bytes::from_static(b"SET"),
            &Bytes::from_static(b"str"),
            &Bytes::from_static(b"\xaa"),
        ])
        .await;
    assert_integer_eq(&client.command(&["BITCOUNT", "str"]).await, 4);
    assert_integer_eq(
        &client.command(&["BITCOUNT", "str", "0", "-1", "bit"]).await,
        4,
    );
}

#[tokio::test]
async fn tcl_bitcount_against_test_vector_0x0000ff() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command_raw(&[
            &Bytes::from_static(b"SET"),
            &Bytes::from_static(b"str"),
            &Bytes::from_static(b"\x00\x00\xff"),
        ])
        .await;
    assert_integer_eq(&client.command(&["BITCOUNT", "str"]).await, 8);
    assert_integer_eq(
        &client.command(&["BITCOUNT", "str", "0", "-1", "bit"]).await,
        8,
    );
}

#[tokio::test]
async fn tcl_bitcount_against_test_vector_foobar() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "str", "foobar"]).await;
    assert_integer_eq(&client.command(&["BITCOUNT", "str"]).await, 26);
    assert_integer_eq(
        &client.command(&["BITCOUNT", "str", "0", "-1", "bit"]).await,
        26,
    );
}

#[tokio::test]
async fn tcl_bitcount_against_test_vector_123() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "str", "123"]).await;
    assert_integer_eq(&client.command(&["BITCOUNT", "str"]).await, 10);
    assert_integer_eq(
        &client.command(&["BITCOUNT", "str", "0", "-1", "bit"]).await,
        10,
    );
}

#[tokio::test]
async fn tcl_bitcount_with_start_end() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "s", "foobar"]).await;

    // Byte ranges
    assert_integer_eq(&client.command(&["BITCOUNT", "s", "0", "-1"]).await, 26);
    assert_integer_eq(&client.command(&["BITCOUNT", "s", "1", "-2"]).await, 18);
    assert_integer_eq(&client.command(&["BITCOUNT", "s", "-2", "1"]).await, 0);
    assert_integer_eq(&client.command(&["BITCOUNT", "s", "0", "1000"]).await, 26);

    // Bit ranges
    assert_integer_eq(
        &client.command(&["BITCOUNT", "s", "0", "-1", "bit"]).await,
        26,
    );
    assert_integer_eq(
        &client.command(&["BITCOUNT", "s", "10", "14", "bit"]).await,
        4,
    );
    assert_integer_eq(
        &client.command(&["BITCOUNT", "s", "3", "14", "bit"]).await,
        7,
    );
    assert_integer_eq(
        &client.command(&["BITCOUNT", "s", "3", "29", "bit"]).await,
        16,
    );
    assert_integer_eq(
        &client.command(&["BITCOUNT", "s", "10", "-34", "bit"]).await,
        4,
    );
    assert_integer_eq(
        &client.command(&["BITCOUNT", "s", "3", "-34", "bit"]).await,
        7,
    );
    assert_integer_eq(
        &client.command(&["BITCOUNT", "s", "3", "-19", "bit"]).await,
        16,
    );
    assert_integer_eq(
        &client.command(&["BITCOUNT", "s", "-2", "1", "bit"]).await,
        0,
    );
    assert_integer_eq(
        &client.command(&["BITCOUNT", "s", "0", "1000", "bit"]).await,
        26,
    );
}

#[tokio::test]
async fn tcl_bitcount_with_illegal_arguments() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Against non-existing key
    client.command(&["DEL", "s"]).await;
    assert_error_prefix(&client.command(&["BITCOUNT", "s", "0"]).await, "ERR");
    assert_error_prefix(
        &client.command(&["BITCOUNT", "s", "0", "1", "hello"]).await,
        "ERR",
    );
    assert_error_prefix(
        &client
            .command(&["BITCOUNT", "s", "0", "1", "hello", "hello2"])
            .await,
        "ERR",
    );

    // Against existing key
    client.command(&["SET", "s", "1"]).await;
    assert_error_prefix(&client.command(&["BITCOUNT", "s", "0"]).await, "ERR");
    assert_error_prefix(
        &client.command(&["BITCOUNT", "s", "0", "1", "hello"]).await,
        "ERR",
    );
    assert_error_prefix(
        &client
            .command(&["BITCOUNT", "s", "0", "1", "hello", "hello2"])
            .await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_bitcount_against_non_integer_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // against existing key
    client.command(&["SET", "s", "1"]).await;
    assert_error_prefix(&client.command(&["BITCOUNT", "s", "a", "b"]).await, "ERR");

    // against non existing key
    client.command(&["DEL", "s"]).await;
    assert_error_prefix(&client.command(&["BITCOUNT", "s", "a", "b"]).await, "ERR");

    // against wrong type
    client.command(&["LPUSH", "s", "a", "b", "c"]).await;
    assert_error_prefix(&client.command(&["BITCOUNT", "s", "a", "b"]).await, "ERR");
}

#[tokio::test]
async fn tcl_bitcount_regression_github_issue_582() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "foo"]).await;
    client.command(&["SETBIT", "foo", "0", "1"]).await;
    // Either returns 1 (bitcount of single set bit) or an out-of-range error
    let resp = client
        .command(&["BITCOUNT", "foo", "0", "4294967296"])
        .await;
    match &resp {
        Response::Integer(n) => assert_eq!(*n, 1),
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("out of range"),
                "expected out-of-range error, got: {msg}"
            );
        }
        other => panic!("unexpected response: {other:?}"),
    }
}

#[tokio::test]
async fn tcl_bitcount_misaligned_prefix() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "str"]).await;
    client.command(&["SET", "str", "ab"]).await;
    assert_integer_eq(&client.command(&["BITCOUNT", "str", "1", "-1"]).await, 3);
}

#[tokio::test]
async fn tcl_bitcount_misaligned_prefix_full_words_remainder() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "str"]).await;
    client
        .command(&["SET", "str", "__PPxxxxxxxxxxxxxxxxRR__"])
        .await;
    assert_integer_eq(&client.command(&["BITCOUNT", "str", "2", "-3"]).await, 74);
}

// ---------------------------------------------------------------------------
// BITOP
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_bitop_not_empty_string() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "{t}s", ""]).await;
    client.command(&["BITOP", "NOT", "{t}dest", "{t}s"]).await;
    assert_bulk_eq(&client.command(&["GET", "{t}dest"]).await, b"");
}

#[tokio::test]
async fn tcl_bitop_not_known_string() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command_raw(&[
            &Bytes::from_static(b"SET"),
            &Bytes::from_static(b"{t}s"),
            &Bytes::from_static(b"\xaa\x00\xff\x55"),
        ])
        .await;
    client.command(&["BITOP", "NOT", "{t}dest", "{t}s"]).await;
    assert_bulk_eq(
        &client.command(&["GET", "{t}dest"]).await,
        b"\x55\xff\x00\xaa",
    );
}

#[tokio::test]
async fn tcl_bitop_not_with_multiple_source_keys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command_raw(&[
            &Bytes::from_static(b"SET"),
            &Bytes::from_static(b"{t}s"),
            &Bytes::from_static(b"\xaa\x00\xff\x55"),
        ])
        .await;
    assert_error_prefix(
        &client
            .command(&["BITOP", "NOT", "{t}dest", "{t}s", "{t}s"])
            .await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_bitop_where_dest_and_target_are_same_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command_raw(&[
            &Bytes::from_static(b"SET"),
            &Bytes::from_static(b"s"),
            &Bytes::from_static(b"\xaa\x00\xff\x55"),
        ])
        .await;
    client.command(&["BITOP", "NOT", "s", "s"]).await;
    assert_bulk_eq(&client.command(&["GET", "s"]).await, b"\x55\xff\x00\xaa");
}

#[tokio::test]
async fn tcl_bitop_single_input_key_unchanged() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command_raw(&[
            &Bytes::from_static(b"SET"),
            &Bytes::from_static(b"{t}a"),
            &Bytes::from_static(b"\x01\x02\xff"),
        ])
        .await;

    client.command(&["BITOP", "AND", "{t}res1", "{t}a"]).await;
    client.command(&["BITOP", "OR", "{t}res2", "{t}a"]).await;
    client.command(&["BITOP", "XOR", "{t}res3", "{t}a"]).await;

    assert_bulk_eq(&client.command(&["GET", "{t}res1"]).await, b"\x01\x02\xff");
    assert_bulk_eq(&client.command(&["GET", "{t}res2"]).await, b"\x01\x02\xff");
    assert_bulk_eq(&client.command(&["GET", "{t}res3"]).await, b"\x01\x02\xff");
}

#[tokio::test]
async fn tcl_bitop_missing_key_is_stream_of_zero() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command_raw(&[
            &Bytes::from_static(b"SET"),
            &Bytes::from_static(b"{t}a"),
            &Bytes::from_static(b"\x01\x02\xff"),
        ])
        .await;

    client
        .command(&["BITOP", "AND", "{t}res1", "{t}no-such-key", "{t}a"])
        .await;
    client
        .command(&[
            "BITOP",
            "OR",
            "{t}res2",
            "{t}no-such-key",
            "{t}a",
            "{t}no-such-key",
        ])
        .await;
    client
        .command(&["BITOP", "XOR", "{t}res3", "{t}no-such-key", "{t}a"])
        .await;

    assert_bulk_eq(&client.command(&["GET", "{t}res1"]).await, b"\x00\x00\x00");
    assert_bulk_eq(&client.command(&["GET", "{t}res2"]).await, b"\x01\x02\xff");
    assert_bulk_eq(&client.command(&["GET", "{t}res3"]).await, b"\x01\x02\xff");
}

#[tokio::test]
async fn tcl_bitop_shorter_keys_are_zero_padded() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command_raw(&[
            &Bytes::from_static(b"SET"),
            &Bytes::from_static(b"{t}a"),
            &Bytes::from_static(b"\x01\x02\xff\xff"),
        ])
        .await;
    client
        .command_raw(&[
            &Bytes::from_static(b"SET"),
            &Bytes::from_static(b"{t}b"),
            &Bytes::from_static(b"\x01\x02\xff"),
        ])
        .await;

    client
        .command(&["BITOP", "AND", "{t}res1", "{t}a", "{t}b"])
        .await;
    client
        .command(&["BITOP", "OR", "{t}res2", "{t}a", "{t}b"])
        .await;
    client
        .command(&["BITOP", "XOR", "{t}res3", "{t}a", "{t}b"])
        .await;

    assert_bulk_eq(
        &client.command(&["GET", "{t}res1"]).await,
        b"\x01\x02\xff\x00",
    );
    assert_bulk_eq(
        &client.command(&["GET", "{t}res2"]).await,
        b"\x01\x02\xff\xff",
    );
    assert_bulk_eq(
        &client.command(&["GET", "{t}res3"]).await,
        b"\x00\x00\x00\xff",
    );
}

#[tokio::test]
async fn tcl_bitop_with_integer_encoded_source_objects() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "{t}a", "1"]).await;
    client.command(&["SET", "{t}b", "2"]).await;
    client
        .command(&["BITOP", "XOR", "{t}dest", "{t}a", "{t}b", "{t}a"])
        .await;
    assert_bulk_eq(&client.command(&["GET", "{t}dest"]).await, b"2");
}

#[tokio::test]
async fn tcl_bitop_with_non_string_source_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "{t}c"]).await;
    client.command(&["SET", "{t}a", "1"]).await;
    client.command(&["SET", "{t}b", "2"]).await;
    client.command(&["LPUSH", "{t}c", "foo"]).await;

    assert_error_prefix(
        &client
            .command(&["BITOP", "XOR", "{t}dest", "{t}a", "{t}b", "{t}c", "{t}d"])
            .await,
        "WRONGTYPE",
    );
}

#[tokio::test]
async fn tcl_bitop_with_empty_string_after_non_empty_string_issue_529() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHDB"]).await;
    let zeros = vec![0u8; 32];
    client
        .command_raw(&[
            &Bytes::from_static(b"SET"),
            &Bytes::from_static(b"{t}a"),
            &Bytes::from(zeros),
        ])
        .await;
    // b is missing (treated as zeros); result length should be 32
    assert_integer_eq(
        &client
            .command(&["BITOP", "OR", "{t}x", "{t}a", "{t}b"])
            .await,
        32,
    );
}

// ---------------------------------------------------------------------------
// BITPOS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_bitpos_against_wrong_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client.command(&["LPUSH", "mylist", "a", "b", "c"]).await;

    assert_error_prefix(
        &client.command(&["BITPOS", "mylist", "0"]).await,
        "WRONGTYPE",
    );
    assert_error_prefix(
        &client
            .command(&["BITPOS", "mylist", "1", "10", "100"])
            .await,
        "WRONGTYPE",
    );
}

#[tokio::test]
async fn tcl_bitpos_with_illegal_arguments() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Against non-existing key
    client.command(&["DEL", "s"]).await;
    assert_error_prefix(
        &client
            .command(&["BITPOS", "s", "0", "1", "hello", "hello2"])
            .await,
        "ERR",
    );
    assert_error_prefix(
        &client
            .command(&["BITPOS", "s", "0", "0", "1", "hello"])
            .await,
        "ERR",
    );

    // Against existing key
    client.command(&["SET", "s", "1"]).await;
    assert_error_prefix(
        &client
            .command(&["BITPOS", "s", "0", "1", "hello", "hello2"])
            .await,
        "ERR",
    );
    assert_error_prefix(
        &client
            .command(&["BITPOS", "s", "0", "0", "1", "hello"])
            .await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_bitpos_against_non_integer_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // against existing key
    client.command(&["SET", "s", "1"]).await;
    assert_error_prefix(&client.command(&["BITPOS", "s", "a"]).await, "ERR");
    assert_error_prefix(
        &client.command(&["BITPOS", "s", "0", "a", "b"]).await,
        "ERR",
    );

    // against non existing key
    client.command(&["DEL", "s"]).await;
    assert_error_prefix(&client.command(&["BITPOS", "s", "b"]).await, "ERR");
    assert_error_prefix(
        &client.command(&["BITPOS", "s", "0", "a", "b"]).await,
        "ERR",
    );

    // against wrong type
    client.command(&["LPUSH", "s", "a", "b", "c"]).await;
    assert_error_prefix(&client.command(&["BITPOS", "s", "a"]).await, "ERR");
    assert_error_prefix(
        &client.command(&["BITPOS", "s", "1", "a", "b"]).await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_bitpos_bit0_with_empty_key_returns_0() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "str"]).await;
    assert_integer_eq(&client.command(&["BITPOS", "str", "0"]).await, 0);
    assert_integer_eq(
        &client
            .command(&["BITPOS", "str", "0", "0", "-1", "bit"])
            .await,
        0,
    );
}

#[tokio::test]
async fn tcl_bitpos_bit1_with_empty_key_returns_neg1() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "str"]).await;
    assert_integer_eq(&client.command(&["BITPOS", "str", "1"]).await, -1);
    assert_integer_eq(
        &client.command(&["BITPOS", "str", "1", "0", "-1"]).await,
        -1,
    );
}

#[tokio::test]
async fn tcl_bitpos_bit0_with_string_less_than_1_word() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command_raw(&[
            &Bytes::from_static(b"SET"),
            &Bytes::from_static(b"str"),
            &Bytes::from_static(b"\xff\xf0\x00"),
        ])
        .await;
    assert_integer_eq(&client.command(&["BITPOS", "str", "0"]).await, 12);
    assert_integer_eq(
        &client
            .command(&["BITPOS", "str", "0", "0", "-1", "bit"])
            .await,
        12,
    );
}

#[tokio::test]
async fn tcl_bitpos_bit1_with_string_less_than_1_word() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command_raw(&[
            &Bytes::from_static(b"SET"),
            &Bytes::from_static(b"str"),
            &Bytes::from_static(b"\x00\x0f\x00"),
        ])
        .await;
    assert_integer_eq(&client.command(&["BITPOS", "str", "1"]).await, 12);
    assert_integer_eq(
        &client
            .command(&["BITPOS", "str", "1", "0", "-1", "bit"])
            .await,
        12,
    );
}

#[tokio::test]
async fn tcl_bitpos_bit0_starting_at_unaligned_address() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command_raw(&[
            &Bytes::from_static(b"SET"),
            &Bytes::from_static(b"str"),
            &Bytes::from_static(b"\xff\xf0\x00"),
        ])
        .await;
    assert_integer_eq(&client.command(&["BITPOS", "str", "0", "1"]).await, 12);
    assert_integer_eq(
        &client
            .command(&["BITPOS", "str", "0", "1", "-1", "bit"])
            .await,
        12,
    );
}

#[tokio::test]
async fn tcl_bitpos_bit1_starting_at_unaligned_address() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command_raw(&[
            &Bytes::from_static(b"SET"),
            &Bytes::from_static(b"str"),
            &Bytes::from_static(b"\x00\x0f\xff"),
        ])
        .await;
    assert_integer_eq(&client.command(&["BITPOS", "str", "1", "1"]).await, 12);
    assert_integer_eq(
        &client
            .command(&["BITPOS", "str", "1", "1", "-1", "bit"])
            .await,
        12,
    );
}

#[tokio::test]
async fn tcl_bitpos_bit0_unaligned_full_word_remainder() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "str"]).await;
    // Build: 3 bytes 0xff prefix + 24 bytes 0xff + 1 byte 0x0f = 28 bytes total.
    // First zero bit is at position 216 (byte 27, bit 0 of 0x0f = 00001111).
    client
        .command_raw(&[
            &Bytes::from_static(b"SET"),
            &Bytes::from_static(b"str"),
            &Bytes::from_static(b"\xff\xff\xff"),
        ])
        .await;
    client
        .command_raw(&[
            &Bytes::from_static(b"APPEND"),
            &Bytes::from_static(b"str"),
            &Bytes::from_static(b"\xff\xff\xff\xff\xff\xff\xff\xff"),
        ])
        .await;
    client
        .command_raw(&[
            &Bytes::from_static(b"APPEND"),
            &Bytes::from_static(b"str"),
            &Bytes::from_static(b"\xff\xff\xff\xff\xff\xff\xff\xff"),
        ])
        .await;
    client
        .command_raw(&[
            &Bytes::from_static(b"APPEND"),
            &Bytes::from_static(b"str"),
            &Bytes::from_static(b"\xff\xff\xff\xff\xff\xff\xff\xff"),
        ])
        .await;
    client
        .command_raw(&[
            &Bytes::from_static(b"APPEND"),
            &Bytes::from_static(b"str"),
            &Bytes::from_static(b"\x0f"),
        ])
        .await;

    // Byte-index start variants
    for start in 0..=8 {
        assert_integer_eq(
            &client
                .command(&["BITPOS", "str", "0", &start.to_string()])
                .await,
            216,
        );
    }

    // Bit-index start variants
    for start in [1, 9, 17, 25, 33, 41, 49, 57, 65] {
        assert_integer_eq(
            &client
                .command(&["BITPOS", "str", "0", &start.to_string(), "-1", "bit"])
                .await,
            216,
        );
    }
}

#[tokio::test]
async fn tcl_bitpos_bit1_unaligned_full_word_remainder() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "str"]).await;
    // Build: 3 bytes 0x00 prefix + 24 bytes 0x00 + 1 byte 0xf0 = 28 bytes total.
    // First one bit at position 216.
    client
        .command_raw(&[
            &Bytes::from_static(b"SET"),
            &Bytes::from_static(b"str"),
            &Bytes::from_static(b"\x00\x00\x00"),
        ])
        .await;
    client
        .command_raw(&[
            &Bytes::from_static(b"APPEND"),
            &Bytes::from_static(b"str"),
            &Bytes::from_static(b"\x00\x00\x00\x00\x00\x00\x00\x00"),
        ])
        .await;
    client
        .command_raw(&[
            &Bytes::from_static(b"APPEND"),
            &Bytes::from_static(b"str"),
            &Bytes::from_static(b"\x00\x00\x00\x00\x00\x00\x00\x00"),
        ])
        .await;
    client
        .command_raw(&[
            &Bytes::from_static(b"APPEND"),
            &Bytes::from_static(b"str"),
            &Bytes::from_static(b"\x00\x00\x00\x00\x00\x00\x00\x00"),
        ])
        .await;
    client
        .command_raw(&[
            &Bytes::from_static(b"APPEND"),
            &Bytes::from_static(b"str"),
            &Bytes::from_static(b"\xf0"),
        ])
        .await;

    // Byte-index start variants
    for start in 0..=8 {
        assert_integer_eq(
            &client
                .command(&["BITPOS", "str", "1", &start.to_string()])
                .await,
            216,
        );
    }

    // Bit-index start variants
    for start in [1, 9, 17, 25, 33, 41, 49, 57, 65] {
        assert_integer_eq(
            &client
                .command(&["BITPOS", "str", "1", &start.to_string(), "-1", "bit"])
                .await,
            216,
        );
    }
}

#[tokio::test]
async fn tcl_bitpos_bit1_returns_neg1_if_string_is_all_0_bits() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command_raw(&[
            &Bytes::from_static(b"SET"),
            &Bytes::from_static(b"str"),
            &Bytes::from_static(b""),
        ])
        .await;
    for _ in 0..20 {
        assert_integer_eq(&client.command(&["BITPOS", "str", "1"]).await, -1);
        assert_integer_eq(
            &client
                .command(&["BITPOS", "str", "1", "0", "-1", "bit"])
                .await,
            -1,
        );
        client
            .command_raw(&[
                &Bytes::from_static(b"APPEND"),
                &Bytes::from_static(b"str"),
                &Bytes::from_static(b"\x00"),
            ])
            .await;
    }
}

#[tokio::test]
async fn tcl_bitpos_bit0_works_with_intervals() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command_raw(&[
            &Bytes::from_static(b"SET"),
            &Bytes::from_static(b"str"),
            &Bytes::from_static(b"\x00\xff\x00"),
        ])
        .await;

    // Byte ranges
    assert_integer_eq(&client.command(&["BITPOS", "str", "0", "0", "-1"]).await, 0);
    assert_integer_eq(
        &client.command(&["BITPOS", "str", "0", "1", "-1"]).await,
        16,
    );
    assert_integer_eq(
        &client.command(&["BITPOS", "str", "0", "2", "-1"]).await,
        16,
    );
    assert_integer_eq(
        &client.command(&["BITPOS", "str", "0", "2", "200"]).await,
        16,
    );
    assert_integer_eq(&client.command(&["BITPOS", "str", "0", "1", "1"]).await, -1);

    // Bit ranges
    assert_integer_eq(
        &client
            .command(&["BITPOS", "str", "0", "0", "-1", "bit"])
            .await,
        0,
    );
    assert_integer_eq(
        &client
            .command(&["BITPOS", "str", "0", "8", "-1", "bit"])
            .await,
        16,
    );
    assert_integer_eq(
        &client
            .command(&["BITPOS", "str", "0", "16", "-1", "bit"])
            .await,
        16,
    );
    assert_integer_eq(
        &client
            .command(&["BITPOS", "str", "0", "16", "200", "bit"])
            .await,
        16,
    );
    assert_integer_eq(
        &client
            .command(&["BITPOS", "str", "0", "8", "8", "bit"])
            .await,
        -1,
    );
}

#[tokio::test]
async fn tcl_bitpos_bit1_works_with_intervals() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command_raw(&[
            &Bytes::from_static(b"SET"),
            &Bytes::from_static(b"str"),
            &Bytes::from_static(b"\x00\xff\x00"),
        ])
        .await;

    // Byte ranges
    assert_integer_eq(&client.command(&["BITPOS", "str", "1", "0", "-1"]).await, 8);
    assert_integer_eq(&client.command(&["BITPOS", "str", "1", "1", "-1"]).await, 8);
    assert_integer_eq(
        &client.command(&["BITPOS", "str", "1", "2", "-1"]).await,
        -1,
    );
    assert_integer_eq(
        &client.command(&["BITPOS", "str", "1", "2", "200"]).await,
        -1,
    );
    assert_integer_eq(&client.command(&["BITPOS", "str", "1", "1", "1"]).await, 8);

    // Bit ranges
    assert_integer_eq(
        &client
            .command(&["BITPOS", "str", "1", "0", "-1", "bit"])
            .await,
        8,
    );
    assert_integer_eq(
        &client
            .command(&["BITPOS", "str", "1", "8", "-1", "bit"])
            .await,
        8,
    );
    assert_integer_eq(
        &client
            .command(&["BITPOS", "str", "1", "16", "-1", "bit"])
            .await,
        -1,
    );
    assert_integer_eq(
        &client
            .command(&["BITPOS", "str", "1", "16", "200", "bit"])
            .await,
        -1,
    );
    assert_integer_eq(
        &client
            .command(&["BITPOS", "str", "1", "8", "8", "bit"])
            .await,
        8,
    );
}

#[tokio::test]
async fn tcl_bitpos_bit0_changes_behavior_if_end_is_given() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command_raw(&[
            &Bytes::from_static(b"SET"),
            &Bytes::from_static(b"str"),
            &Bytes::from_static(b"\xff\xff\xff"),
        ])
        .await;

    assert_integer_eq(&client.command(&["BITPOS", "str", "0"]).await, 24);
    assert_integer_eq(&client.command(&["BITPOS", "str", "0", "0"]).await, 24);
    assert_integer_eq(
        &client.command(&["BITPOS", "str", "0", "0", "-1"]).await,
        -1,
    );
    assert_integer_eq(
        &client
            .command(&["BITPOS", "str", "0", "0", "-1", "bit"])
            .await,
        -1,
    );
}
