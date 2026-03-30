//! Rust port of Redis 8.6.0 `unit/bitfield.tcl` test suite.
//!
//! Excludes: `external:skip` / `needs:repl` tests (replication block),
//! fuzzing tests (overflow detection fuzzing, overflow wrap fuzzing — require
//! `randomInt` helper and are stochastic).

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// BITFIELD signed SET and GET basics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_bitfield_signed_set_and_get_basics() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "bits"]).await;
    let resp = client
        .command(&["BITFIELD", "bits", "SET", "i8", "0", "-100"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[0]), 0);

    let resp = client
        .command(&["BITFIELD", "bits", "SET", "i8", "0", "101"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[0]), -100);

    let resp = client
        .command(&["BITFIELD", "bits", "GET", "i8", "0"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[0]), 101);
}

// ---------------------------------------------------------------------------
// BITFIELD unsigned SET and GET basics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_bitfield_unsigned_set_and_get_basics() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "bits"]).await;
    let resp = client
        .command(&["BITFIELD", "bits", "SET", "u8", "0", "255"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[0]), 0);

    let resp = client
        .command(&["BITFIELD", "bits", "SET", "u8", "0", "100"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[0]), 255);

    let resp = client
        .command(&["BITFIELD", "bits", "GET", "u8", "0"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[0]), 100);
}

// ---------------------------------------------------------------------------
// BITFIELD signed SET and GET together
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_bitfield_signed_set_and_get_together() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "bits"]).await;
    let resp = client
        .command(&[
            "BITFIELD", "bits", "SET", "i8", "0", "255", "SET", "i8", "0", "100", "GET", "i8", "0",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[0]), 0);
    assert_eq!(unwrap_integer(&items[1]), -1);
    assert_eq!(unwrap_integer(&items[2]), 100);
}

// ---------------------------------------------------------------------------
// BITFIELD unsigned with SET, GET and INCRBY arguments
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_bitfield_unsigned_set_get_incrby() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "bits"]).await;
    let resp = client
        .command(&[
            "BITFIELD", "bits", "SET", "u8", "0", "255", "INCRBY", "u8", "0", "100", "GET", "u8",
            "0",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[0]), 0);
    assert_eq!(unwrap_integer(&items[1]), 99);
    assert_eq!(unwrap_integer(&items[2]), 99);
}

// ---------------------------------------------------------------------------
// BITFIELD with only key as argument
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB BITFIELD requires subcommands"]
async fn tcl_bitfield_with_only_key_as_argument() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "bits"]).await;
    let resp = client.command(&["BITFIELD", "bits"]).await;
    assert_array_len(&resp, 0);
}

// ---------------------------------------------------------------------------
// BITFIELD #<idx> form
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_bitfield_hash_idx_form() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "bits"]).await;
    client
        .command(&["BITFIELD", "bits", "SET", "u8", "#0", "65"])
        .await;
    client
        .command(&["BITFIELD", "bits", "SET", "u8", "#1", "66"])
        .await;
    client
        .command(&["BITFIELD", "bits", "SET", "u8", "#2", "67"])
        .await;
    assert_bulk_eq(&client.command(&["GET", "bits"]).await, b"ABC");
}

// ---------------------------------------------------------------------------
// BITFIELD basic INCRBY form
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_bitfield_basic_incrby_form() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "bits"]).await;
    client
        .command(&["BITFIELD", "bits", "SET", "u8", "#0", "10"])
        .await;

    let resp = client
        .command(&["BITFIELD", "bits", "INCRBY", "u8", "#0", "100"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[0]), 110);

    let resp = client
        .command(&["BITFIELD", "bits", "INCRBY", "u8", "#0", "100"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[0]), 210);
}

// ---------------------------------------------------------------------------
// BITFIELD chaining of multiple commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_bitfield_chaining_of_multiple_commands() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "bits"]).await;
    client
        .command(&["BITFIELD", "bits", "SET", "u8", "#0", "10"])
        .await;

    let resp = client
        .command(&[
            "BITFIELD", "bits", "INCRBY", "u8", "#0", "100", "INCRBY", "u8", "#0", "100",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[0]), 110);
    assert_eq!(unwrap_integer(&items[1]), 210);
}

// ---------------------------------------------------------------------------
// BITFIELD unsigned overflow wrap
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_bitfield_unsigned_overflow_wrap() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "bits"]).await;
    client
        .command(&["BITFIELD", "bits", "SET", "u8", "#0", "100"])
        .await;

    // OVERFLOW WRAP INCRBY u8 #0 257 => (100+257) % 256 = 101
    let resp = client
        .command(&[
            "BITFIELD", "bits", "OVERFLOW", "WRAP", "INCRBY", "u8", "#0", "257",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[0]), 101);

    let resp = client
        .command(&["BITFIELD", "bits", "GET", "u8", "#0"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[0]), 101);

    // OVERFLOW WRAP INCRBY u8 #0 255 => (101+255) % 256 = 100
    let resp = client
        .command(&[
            "BITFIELD", "bits", "OVERFLOW", "WRAP", "INCRBY", "u8", "#0", "255",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[0]), 100);

    let resp = client
        .command(&["BITFIELD", "bits", "GET", "u8", "#0"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[0]), 100);
}

// ---------------------------------------------------------------------------
// BITFIELD unsigned overflow sat
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB BITFIELD OVERFLOW SAT behavior differs"]
async fn tcl_bitfield_unsigned_overflow_sat() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "bits"]).await;
    client
        .command(&["BITFIELD", "bits", "SET", "u8", "#0", "100"])
        .await;

    // SAT: 100 + 257 saturates at 255
    let resp = client
        .command(&[
            "BITFIELD", "bits", "OVERFLOW", "SAT", "INCRBY", "u8", "#0", "257",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[0]), 255);

    let resp = client
        .command(&["BITFIELD", "bits", "GET", "u8", "#0"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[0]), 255);

    // SAT: 255 + (-255) saturates at 0
    let resp = client
        .command(&[
            "BITFIELD", "bits", "OVERFLOW", "SAT", "INCRBY", "u8", "#0", "-255",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[0]), 0);

    let resp = client
        .command(&["BITFIELD", "bits", "GET", "u8", "#0"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[0]), 0);
}

// ---------------------------------------------------------------------------
// BITFIELD signed overflow wrap
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_bitfield_signed_overflow_wrap() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "bits"]).await;
    client
        .command(&["BITFIELD", "bits", "SET", "i8", "#0", "100"])
        .await;

    // WRAP: 100 + 257 wraps in signed i8 range => 101
    let resp = client
        .command(&[
            "BITFIELD", "bits", "OVERFLOW", "WRAP", "INCRBY", "i8", "#0", "257",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[0]), 101);

    let resp = client
        .command(&["BITFIELD", "bits", "GET", "i8", "#0"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[0]), 101);

    // WRAP: 101 + 255 wraps => 100
    let resp = client
        .command(&[
            "BITFIELD", "bits", "OVERFLOW", "WRAP", "INCRBY", "i8", "#0", "255",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[0]), 100);

    let resp = client
        .command(&["BITFIELD", "bits", "GET", "i8", "#0"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[0]), 100);
}

// ---------------------------------------------------------------------------
// BITFIELD signed overflow sat
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_bitfield_signed_overflow_sat() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Note: TCL test sets u8 #0 100, then uses i8 for INCRBY — this is intentional.
    client.command(&["DEL", "bits"]).await;
    client
        .command(&["BITFIELD", "bits", "SET", "u8", "#0", "100"])
        .await;

    // SAT: i8 view of u8(100) is 100; 100 + 257 saturates at 127
    let resp = client
        .command(&[
            "BITFIELD", "bits", "OVERFLOW", "SAT", "INCRBY", "i8", "#0", "257",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[0]), 127);

    let resp = client
        .command(&["BITFIELD", "bits", "GET", "i8", "#0"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[0]), 127);

    // SAT: 127 + (-255) saturates at -128
    let resp = client
        .command(&[
            "BITFIELD", "bits", "OVERFLOW", "SAT", "INCRBY", "i8", "#0", "-255",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[0]), -128);

    let resp = client
        .command(&["BITFIELD", "bits", "GET", "i8", "#0"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[0]), -128);
}

// ---------------------------------------------------------------------------
// BITFIELD regression for #3221
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_bitfield_regression_3221() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "bits", "1"]).await;
    let resp = client
        .command(&["BITFIELD", "bits", "GET", "u1", "0"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[0]), 0);
}

// ---------------------------------------------------------------------------
// BITFIELD regression for #3564
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_bitfield_regression_3564() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for _ in 0..10 {
        client.command(&["DEL", "mystring"]).await;
        let resp = client
            .command(&[
                "BITFIELD", "mystring", "SET", "i8", "0", "10", "SET", "i8", "64", "10", "INCRBY",
                "i8", "10", "99900",
            ])
            .await;
        let items = unwrap_array(resp);
        assert_eq!(unwrap_integer(&items[0]), 0);
        assert_eq!(unwrap_integer(&items[1]), 0);
        assert_eq!(unwrap_integer(&items[2]), 60);
    }
    client.command(&["DEL", "mystring"]).await;
}

// ---------------------------------------------------------------------------
// BITFIELD_RO with only key as argument
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB BITFIELD_RO requires subcommands"]
async fn tcl_bitfield_ro_with_only_key_as_argument() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["BITFIELD_RO", "bits"]).await;
    assert_array_len(&resp, 0);
}

// ---------------------------------------------------------------------------
// BITFIELD_RO fails when write option is used
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB BITFIELD_RO error message differs"]
async fn tcl_bitfield_ro_fails_when_write_option_is_used() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "BITFIELD_RO",
            "bits",
            "SET",
            "u8",
            "0",
            "100",
            "GET",
            "u8",
            "0",
        ])
        .await;
    assert_error_prefix(&resp, "ERR BITFIELD_RO only supports the GET subcommand");
}
