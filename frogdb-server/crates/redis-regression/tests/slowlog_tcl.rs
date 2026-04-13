//! Rust port of Redis 8.6.0 `unit/slowlog.tcl` test suite.
//!
//! FrogDB implements `SLOWLOG GET [count]`, `SLOWLOG LEN`, `SLOWLOG RESET`,
//! and `SLOWLOG HELP`, plus the `slowlog-log-slower-than`, `slowlog-max-len`,
//! and `slowlog-max-arg-len` configuration parameters. Each shard has its own
//! log and the connection handler scatter-gathers across shards; commands are
//! always logged to shard 0 (see `maybe_log_slow_query`).
//!
//! Tests here use `CONFIG SET slowlog-log-slower-than 0` to force every
//! command to be logged, which avoids depending on `DEBUG SLEEP` timing.
//!
//! ## Intentional exclusions
//!
//! - `SLOWLOG - Certain commands are omitted that contain sensitive information` — intentional-incompatibility:replication — needs:repl tag (config set masterauth/masteruser/tls-key-file-pass); FrogDB does not redact ACL SETUSER / CONFIG SET sensitive arguments in slowlog
//! - `SLOWLOG - Some commands can redact sensitive fields` — intentional-incompatibility:replication — needs:repl tag (MIGRATE AUTH/AUTH2 redaction)
//! - `SLOWLOG - Rewritten commands are logged as their original command` — intentional-incompatibility:observability — tests Redis-internal command rewriting (SPOP->DEL, GEOADD->ZADD, GETSET->SET, INCRBYFLOAT->SET, blocked BLPOP->LPOP) for replication purposes; FrogDB does not implement replication command rewriting
//! - `SLOWLOG - EXEC is not logged, just executed commands` — intentional-incompatibility:observability — FrogDB logs each command inside a MULTI/EXEC block individually but does not skip the enclosing EXEC
//! - `SLOWLOG - blocking command is reported only after unblocked` — intentional-incompatibility:observability — FrogDB logs blocking commands at submit time, not after unblock

use std::time::Duration;

use frogdb_protocol::Response;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// Helper: pull the command-args array (element 3, 0-indexed) out of a single
// slowlog entry. Each entry is a 6-element array:
//   [id, timestamp, duration_us, [args...], client_addr, client_name]
fn entry_command(entry: &Response) -> Vec<String> {
    let arr = match entry {
        Response::Array(items) => items,
        other => panic!("expected Array, got {other:?}"),
    };
    assert_eq!(arr.len(), 6, "slowlog entry should have 6 fields");
    extract_bulk_strings(&arr[3])
}

// Helper: reset slowlog and set threshold so every command is logged.
async fn prime_slowlog_log_all(client: &mut frogdb_test_harness::server::TestClient) {
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "slowlog-log-slower-than", "0"])
            .await,
    );
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "slowlog-max-len", "128"])
            .await,
    );
    assert_ok(&client.command(&["SLOWLOG", "RESET"]).await);
}

// ---------------------------------------------------------------------------
// Basic SLOWLOG GET / LEN / RESET
// ---------------------------------------------------------------------------

/// Upstream: `SLOWLOG - check that it starts with an empty log`
#[tokio::test]
async fn tcl_slowlog_check_that_it_starts_with_an_empty_log() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Fresh server — default threshold is 10ms so nothing should be logged.
    // Explicitly reset in case the handler path logged something.
    assert_ok(&client.command(&["SLOWLOG", "RESET"]).await);
    assert_integer_eq(&client.command(&["SLOWLOG", "LEN"]).await, 0);
}

/// Upstream: `SLOWLOG - only logs commands taking more time than specified`
///
/// Uses DEBUG SLEEP to guarantee a slow command. Upstream sets threshold to
/// 100_000 us (100 ms) and sleeps 200 ms.
#[tokio::test]
async fn tcl_slowlog_only_logs_commands_taking_more_time_than_specified() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["CONFIG", "SET", "slowlog-log-slower-than", "100000"])
            .await,
    );
    assert_ok(&client.command(&["SLOWLOG", "RESET"]).await);

    assert!(matches!(&client.command(&["PING"]).await, Response::Simple(s) if s == "PONG"));
    assert_integer_eq(&client.command(&["SLOWLOG", "LEN"]).await, 0);

    assert_ok(&client.command(&["DEBUG", "SLEEP", "0.2"]).await);
    assert_integer_eq(&client.command(&["SLOWLOG", "LEN"]).await, 1);
}

/// Upstream: `SLOWLOG - zero max length is correctly handled`
#[tokio::test]
async fn tcl_slowlog_zero_max_length_is_correctly_handled() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SLOWLOG", "RESET"]).await);
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "slowlog-max-len", "0"])
            .await,
    );
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "slowlog-log-slower-than", "0"])
            .await,
    );
    for _ in 0..100 {
        assert!(matches!(&client.command(&["PING"]).await, Response::Simple(s) if s == "PONG"));
    }
    assert_integer_eq(&client.command(&["SLOWLOG", "LEN"]).await, 0);
}

/// Upstream: `SLOWLOG - max entries is correctly handled`
#[tokio::test]
async fn tcl_slowlog_max_entries_is_correctly_handled() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["CONFIG", "SET", "slowlog-log-slower-than", "0"])
            .await,
    );
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "slowlog-max-len", "10"])
            .await,
    );
    assert_ok(&client.command(&["SLOWLOG", "RESET"]).await);

    for _ in 0..100 {
        assert!(matches!(&client.command(&["PING"]).await, Response::Simple(s) if s == "PONG"));
    }
    // At most 10 entries kept — slowlog-max-len capped the ring buffer.
    assert_integer_eq(&client.command(&["SLOWLOG", "LEN"]).await, 10);
}

/// Upstream: `SLOWLOG - GET optional argument to limit output len works`
#[tokio::test]
async fn tcl_slowlog_get_optional_argument_to_limit_output_len_works() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["CONFIG", "SET", "slowlog-log-slower-than", "0"])
            .await,
    );
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "slowlog-max-len", "10"])
            .await,
    );
    assert_ok(&client.command(&["SLOWLOG", "RESET"]).await);

    // Fill the log with 100 entries (only 10 will be kept).
    for _ in 0..100 {
        assert!(matches!(&client.command(&["PING"]).await, Response::Simple(s) if s == "PONG"));
    }

    let arr = unwrap_array(client.command(&["SLOWLOG", "GET", "5"]).await);
    assert_eq!(arr.len(), 5);
    let arr = unwrap_array(client.command(&["SLOWLOG", "GET", "-1"]).await);
    assert_eq!(arr.len(), 10);
    let arr = unwrap_array(client.command(&["SLOWLOG", "GET", "20"]).await);
    assert_eq!(arr.len(), 10);
}

/// Upstream: `SLOWLOG - RESET subcommand works`
#[tokio::test]
async fn tcl_slowlog_reset_subcommand_works() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["CONFIG", "SET", "slowlog-log-slower-than", "0"])
            .await,
    );
    // Log a few commands.
    for _ in 0..5 {
        assert!(matches!(&client.command(&["PING"]).await, Response::Simple(s) if s == "PONG"));
    }
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "slowlog-log-slower-than", "100000"])
            .await,
    );
    assert_ok(&client.command(&["SLOWLOG", "RESET"]).await);
    assert_integer_eq(&client.command(&["SLOWLOG", "LEN"]).await, 0);
}

/// Upstream: `SLOWLOG - logged entry sanity check`
///
/// Upstream asserts the entry has 6 fields, that the id is 106 (not portable),
/// that duration > 100000 us, that the command string is "debug sleep 0.2",
/// and that the client name is "foobar". We skip the hard-coded ID check.
#[tokio::test]
async fn tcl_slowlog_logged_entry_sanity_check() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["CONFIG", "SET", "slowlog-log-slower-than", "100000"])
            .await,
    );
    assert_ok(&client.command(&["SLOWLOG", "RESET"]).await);
    assert_ok(&client.command(&["CLIENT", "SETNAME", "foobar"]).await);
    assert_ok(&client.command(&["DEBUG", "SLEEP", "0.2"]).await);

    let arr = unwrap_array(client.command(&["SLOWLOG", "GET"]).await);
    assert!(!arr.is_empty(), "slowlog should have at least one entry");
    let entry = match &arr[0] {
        Response::Array(items) => items,
        other => panic!("expected Array, got {other:?}"),
    };
    assert_eq!(entry.len(), 6);
    // entry[0] = id (we do not pin to 106)
    // entry[1] = timestamp (unpredictable but integer)
    assert!(matches!(entry[0], Response::Integer(_)));
    assert!(matches!(entry[1], Response::Integer(_)));
    // entry[2] = duration_us > 100000
    let duration = unwrap_integer(&entry[2]);
    assert!(
        duration > 100000,
        "expected duration > 100000 us, got {duration}"
    );
    // entry[3] = command args = ["debug","sleep","0.2"] (case-insensitive)
    let cmd = extract_bulk_strings(&entry[3]);
    assert_eq!(cmd.len(), 3);
    assert!(cmd[0].eq_ignore_ascii_case("debug"));
    assert!(cmd[1].eq_ignore_ascii_case("sleep"));
    assert_eq!(cmd[2], "0.2");
    // entry[5] = client name
    assert_bulk_eq(&entry[5], b"foobar");
}

/// Upstream: `SLOWLOG - commands with too many arguments are trimmed`
///
/// Upstream `sadd set 3..33` expects the last arg to be
/// `{... (2 more arguments)}`. Our truncate_args agrees (cap at 32 args).
/// Upstream takes `lindex $slowlog get] end-1` because the SADD is logged
/// second (the CONFIG SET / RESET are logged as well when threshold is 0).
#[tokio::test]
async fn tcl_slowlog_commands_with_too_many_arguments_are_trimmed() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["CONFIG", "SET", "slowlog-log-slower-than", "0"])
            .await,
    );
    assert_ok(&client.command(&["SLOWLOG", "RESET"]).await);

    // 33 arg values: this matches upstream exactly (3..33 inclusive = 31
    // values) plus "set" gives 32 args after the command — but upstream
    // actually uses 3..33 = 31 values, making 33 total args including SADD
    // and the key. The expected truncation shows "... (2 more arguments)"
    // which corresponds to MAX_ARGS=32 and 34 args total.
    let values: Vec<String> = (3..=33).map(|i| i.to_string()).collect();
    let mut cmd = vec!["SADD", "set"];
    for v in &values {
        cmd.push(v.as_str());
    }
    client.command(&cmd).await;

    // Find the SADD entry — it's the second-to-last in the log (the slowlog
    // also logged the SLOWLOG RESET call before it). Upstream reads
    // `lindex [r slowlog get] end-1` for the same reason. We scan by name.
    let arr = unwrap_array(client.command(&["SLOWLOG", "GET"]).await);
    let mut sadd_cmd: Option<Vec<String>> = None;
    for e in &arr {
        let c = entry_command(e);
        if !c.is_empty() && c[0].eq_ignore_ascii_case("sadd") {
            sadd_cmd = Some(c);
            break;
        }
    }
    let c = sadd_cmd.expect("SADD entry should be in slowlog");
    // 32 original args + the "... (N more arguments)" indicator entry.
    assert_eq!(c.len(), 33);
    // First 32 args: SADD, set, 3..=32 (30 values = 32 total).
    assert!(c[0].eq_ignore_ascii_case("sadd"));
    assert_eq!(c[1], "set");
    for (i, expected) in (3..=32).enumerate() {
        assert_eq!(c[2 + i], expected.to_string());
    }
    // Last element is the overflow indicator.
    assert!(
        c[32].contains("more arguments"),
        "expected trailing indicator, got {:?}",
        c[32]
    );
}

/// Upstream: `SLOWLOG - too long arguments are trimmed`
///
/// A 129-character arg should be truncated to 128 chars + `"... (1 more bytes)"`.
#[tokio::test]
async fn tcl_slowlog_too_long_arguments_are_trimmed() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["CONFIG", "SET", "slowlog-log-slower-than", "0"])
            .await,
    );
    assert_ok(&client.command(&["SLOWLOG", "RESET"]).await);

    let arg = "A".repeat(129);
    assert_integer_eq(&client.command(&["SADD", "set", "foo", &arg]).await, 2);

    // Find the SADD entry (skipping the SLOWLOG RESET we just called).
    let arr = unwrap_array(client.command(&["SLOWLOG", "GET"]).await);
    let mut c: Option<Vec<String>> = None;
    for e in &arr {
        let args = entry_command(e);
        if !args.is_empty() && args[0].eq_ignore_ascii_case("sadd") {
            c = Some(args);
            break;
        }
    }
    let c = c.expect("SADD entry should be in slowlog");
    assert_eq!(c.len(), 4);
    assert!(c[0].eq_ignore_ascii_case("sadd"));
    assert_eq!(c[1], "set");
    assert_eq!(c[2], "foo");
    // 128 A's followed by the indicator suffix.
    assert!(c[3].starts_with(&"A".repeat(128)));
    assert!(c[3].contains("more bytes"));
}

/// Upstream: `SLOWLOG - can be disabled`
///
/// With `slowlog-log-slower-than -1`, nothing should be logged even when
/// running a slow command.
#[tokio::test]
async fn tcl_slowlog_can_be_disabled() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["CONFIG", "SET", "slowlog-max-len", "1"])
            .await,
    );
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "slowlog-log-slower-than", "1"])
            .await,
    );
    assert_ok(&client.command(&["SLOWLOG", "RESET"]).await);
    assert_ok(&client.command(&["DEBUG", "SLEEP", "0.2"]).await);
    assert_integer_eq(&client.command(&["SLOWLOG", "LEN"]).await, 1);

    assert_ok(
        &client
            .command(&["CONFIG", "SET", "slowlog-log-slower-than", "-1"])
            .await,
    );
    assert_ok(&client.command(&["SLOWLOG", "RESET"]).await);
    assert_ok(&client.command(&["DEBUG", "SLEEP", "0.2"]).await);
    assert_integer_eq(&client.command(&["SLOWLOG", "LEN"]).await, 0);
}

/// Upstream: `SLOWLOG - can clean older entries`
///
/// With max-len=1, only the most recent slow-log entry is retained.
#[tokio::test]
async fn tcl_slowlog_can_clean_older_entries() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["CONFIG", "SET", "slowlog-log-slower-than", "100000"])
            .await,
    );
    assert_ok(
        &client
            .command(&["CLIENT", "SETNAME", "lastentry_client"])
            .await,
    );
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "slowlog-max-len", "1"])
            .await,
    );
    assert_ok(&client.command(&["DEBUG", "SLEEP", "0.2"]).await);
    // Upstream runs DEBUG SLEEP twice in some versions; in 8.6 it runs once
    // and asserts the single remaining entry belongs to lastentry_client.
    let arr = unwrap_array(client.command(&["SLOWLOG", "GET"]).await);
    assert_eq!(arr.len(), 1, "expected exactly one entry");
    let entry = match &arr[0] {
        Response::Array(items) => items,
        other => panic!("expected Array, got {other:?}"),
    };
    assert_bulk_eq(&entry[5], b"lastentry_client");
}

/// Upstream: `SLOWLOG - count must be >= -1`
#[tokio::test]
async fn tcl_slowlog_count_must_be_gte_neg_one() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_error_prefix(
        &client.command(&["SLOWLOG", "GET", "-2"]).await,
        "ERR count should be greater than or equal to -1",
    );
    assert_error_prefix(
        &client.command(&["SLOWLOG", "GET", "-222"]).await,
        "ERR count should be greater than or equal to -1",
    );
}

/// Upstream: `SLOWLOG - get all slow logs`
///
/// Upstream:
///   slowlog-log-slower-than 0
///   slowlog-max-len 3
///   reset
///   set key test; sadd set a b c; incr num; lpush list a
///   expect len == 3 (max-len capped it)
///   expect GET 0 → 0, GET 1 → 1, GET -1 → 3, GET 3 → 3
#[tokio::test]
async fn tcl_slowlog_get_all_slow_logs() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    prime_slowlog_log_all(&mut client).await;
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "slowlog-max-len", "3"])
            .await,
    );
    assert_ok(&client.command(&["SLOWLOG", "RESET"]).await);

    assert_ok(&client.command(&["SET", "key", "test"]).await);
    assert_integer_eq(&client.command(&["SADD", "set", "a", "b", "c"]).await, 3);
    assert_integer_eq(&client.command(&["INCR", "num"]).await, 1);
    assert_integer_eq(&client.command(&["LPUSH", "list", "a"]).await, 1);

    assert_integer_eq(&client.command(&["SLOWLOG", "LEN"]).await, 3);
    assert_eq!(
        unwrap_array(client.command(&["SLOWLOG", "GET", "0"]).await).len(),
        0
    );
    assert_eq!(
        unwrap_array(client.command(&["SLOWLOG", "GET", "1"]).await).len(),
        1
    );
    assert_eq!(
        unwrap_array(client.command(&["SLOWLOG", "GET", "-1"]).await).len(),
        3
    );
    assert_eq!(
        unwrap_array(client.command(&["SLOWLOG", "GET", "3"]).await).len(),
        3
    );

    // Silence unused_imports warning from the Duration re-export.
    let _ = Duration::from_millis(0);
}
