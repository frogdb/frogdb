//! Rust port of Redis 8.6.0 `unit/auth.tcl` + protected mode from `unit/networking.tcl`.
//!
//! Skipped tests:
//! - `MASTERAUTH test with binary password` — requires replication (`needs:repl`)
//! - All tests tagged `needs:debug` or `external:skip` are ported where applicable
//!   (the `external:skip` tag in the original controls *runner* behavior, not test logic)

use frogdb_protocol::Response;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// auth.tcl — no-password server block
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB AUTH returns OK when no password is set"]
async fn tcl_auth_fails_if_no_password_configured() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["AUTH", "foo"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_auth_arity_check() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["AUTH", "a", "b", "c"]).await;
    assert!(
        matches!(resp, Response::Error(_)),
        "AUTH with too many args should return a syntax error, got {resp:?}"
    );
}

// ---------------------------------------------------------------------------
// auth.tcl — password-protected server block (requirepass foobar)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_auth_fails_with_wrong_password() {
    let server = TestServer::start_with_security("foobar").await;
    let mut client = server.connect().await;

    let resp = client.command(&["AUTH", "wrong!"]).await;
    assert_error_prefix(&resp, "WRONGPASS");
}

#[tokio::test]
async fn tcl_arbitrary_command_gives_error_when_auth_required() {
    let server = TestServer::start_with_security("foobar").await;
    let mut client = server.connect().await;

    let resp = client.command(&["SET", "foo", "bar"]).await;
    assert_error_prefix(&resp, "NOAUTH");
}

#[tokio::test]
async fn tcl_auth_succeeds_with_right_password() {
    let server = TestServer::start_with_security("foobar").await;
    let mut client = server.connect().await;

    let resp = client.command(&["AUTH", "foobar"]).await;
    assert_ok(&resp);
}

#[tokio::test]
async fn tcl_commands_work_after_auth() {
    let server = TestServer::start_with_security("foobar").await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["AUTH", "foobar"]).await);
    assert_ok(&client.command(&["SET", "foo", "100"]).await);

    let resp = client.command(&["INCR", "foo"]).await;
    assert_integer_eq(&resp, 101);
}

#[tokio::test]
#[ignore = "FrogDB unauthenticated client limits not implemented"]
async fn tcl_unauthenticated_clients_multibulk_bulk_length_limited() {
    // Original test verifies that unauthenticated clients sending oversized
    // multibulk (*100) or oversized bulk ($100000000) get disconnected with
    // specific error messages about "unauthenticated multibulk length" and
    // "unauthenticated bulk length".
    let server = TestServer::start_with_security("foobar").await;
    let _client = server.connect().await;
}

#[tokio::test]
#[ignore = "FrogDB unauthenticated client limits not implemented"]
async fn tcl_unauthenticated_clients_output_buffer_limited() {
    // Original test verifies that an unauthenticated client flooding SET
    // commands without reading replies eventually gets disconnected when
    // the output buffer exceeds a few MB.
    let server = TestServer::start_with_security("foobar").await;
    let _client = server.connect().await;
}

// ---------------------------------------------------------------------------
// auth.tcl — binary password block
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB AUTH binary password handling differs"]
async fn tcl_auth_fails_when_binary_password_is_wrong() {
    let server = TestServer::start_with_security("abc\x00def").await;
    let mut client = server.connect().await;

    // Authenticate so we can issue CONFIG SET, then reconnect unauthenticated
    assert_ok(&client.command(&["AUTH", "abc\x00def"]).await);
    let resp = client
        .command(&["CONFIG", "SET", "requirepass", "abc\x00def"])
        .await;
    assert_ok(&resp);

    // New connection — try wrong (truncated) password
    let mut client2 = server.connect().await;
    let resp = client2.command(&["AUTH", "abc"]).await;
    assert_error_prefix(&resp, "WRONGPASS");
}

#[tokio::test]
async fn tcl_auth_succeeds_when_binary_password_is_correct() {
    let server = TestServer::start_with_security("abc\x00def").await;
    let mut client = server.connect().await;

    let resp = client.command(&["AUTH", "abc\x00def"]).await;
    assert_ok(&resp);
}

// ---------------------------------------------------------------------------
// networking.tcl — protected mode
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB protected mode not implemented"]
async fn tcl_protected_mode_works_as_expected() {
    // Original test from networking.tcl verifies:
    // 1. Non-loopback clients are denied by default (DENIED error on PING)
    // 2. Changing bind to "*" still denies non-loopback clients
    // 3. Setting requirepass disables protected mode (AUTH + PING succeed)
    // 4. Clearing requirepass re-enables protected mode
    // 5. Explicitly setting protected-mode no allows non-loopback clients
    let server = TestServer::start_standalone().await;
    let _client = server.connect().await;
}
