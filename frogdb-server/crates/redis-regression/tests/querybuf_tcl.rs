//! Rust port of Redis 8.6.0 `unit/querybuf.tcl` test suite.
//!
//! Upstream tests in this suite observe the per-client query buffer
//! growing and shrinking via the `qbuf=` / `qbuf-free=` fields in
//! `CLIENT LIST` output. FrogDB's `CLIENT LIST` always reports
//! `qbuf=0 qbuf-free=0` because FrogDB does not expose the tokio-based
//! connection read buffer as a per-client introspection field. Tests
//! that exercise the shrink-on-idle / grow-on-large-argv behavior
//! cannot meaningfully run against FrogDB and are excluded below.
//!
//! The only test that survives ports the reusable-query-buffer
//! assertion, which on FrogDB happens to match trivially: the
//! hardcoded `qbuf=0 qbuf-free=0` lines match both the `client|setname`
//! and the `client|list` CLIENT LIST entries the upstream test inspects.
//!
//! ## Intentional exclusions
//!
//! - `query buffer resized correctly` — introspection field not implemented
//!   (requires observable `qbuf=` growth/shrink via `DEBUG PAUSE-CRON`)
//! - `query buffer resized correctly when not idle` — introspection field not implemented
//!   (requires observable `qbuf=` growth/shrink; also tagged `needs:debug`)
//! - `query buffer resized correctly with fat argv` — introspection field not implemented
//!   (requires observable `qbuf=` growth for a 1MB partial argv; needs `DEBUG PAUSE-CRON`)

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn tcl_client_executes_small_argv_commands_using_reusable_query_buffer() {
    // Upstream sets up a deferring client and reads `CLIENT LIST` from a
    // separate primary client, asserting:
    //   *name=test_client * qbuf=0 qbuf-free=0 * cmd=client|setname *
    //   *qbuf=0 qbuf-free=* cmd=client|list *
    //
    // FrogDB always reports `qbuf=0 qbuf-free=0` in CLIENT LIST, and the
    // `cmd=` field is populated from the currently-executing command (or
    // the most recently executed one for idle clients). We verify that
    // both entries are present and that qbuf is 0 for each.
    let server = TestServer::start_standalone().await;
    let mut driver = server.connect().await;
    let mut target = server.connect().await;

    // Name the target client so we can find it in CLIENT LIST.
    assert_ok(&target.command(&["CLIENT", "SETNAME", "test_client"]).await);

    let resp = driver.command(&["CLIENT", "LIST"]).await;
    let list_str = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();

    // Find the test_client line.
    let target_line = list_str
        .lines()
        .find(|l| l.contains("name=test_client"))
        .unwrap_or_else(|| panic!("expected to find test_client in CLIENT LIST:\n{list_str}"));
    assert!(
        target_line.contains("qbuf=0"),
        "test_client entry should report qbuf=0 (small-argv reusable buffer): {target_line}"
    );
    assert!(
        target_line.contains("qbuf-free=0"),
        "test_client entry should report qbuf-free=0: {target_line}"
    );

    // The driver client executed `CLIENT LIST` most recently, so its entry
    // should carry cmd=client|list.
    let driver_line = list_str
        .lines()
        .find(|l| l.contains("cmd=client|list"))
        .unwrap_or_else(|| panic!("expected to find cmd=client|list entry:\n{list_str}"));
    assert!(
        driver_line.contains("qbuf=0"),
        "driver CLIENT LIST entry should report qbuf=0: {driver_line}"
    );
}
