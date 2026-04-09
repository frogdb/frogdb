//! Rust port of Redis 8.6.0 `unit/info-command.tcl` test suite.
//!
//! **Note**: this file tests the `INFO [section ...]` command's section
//! filtering behavior — it is NOT about `COMMAND INFO` (which is covered
//! by `info_command_regression.rs`). The upstream filename `info-command.tcl`
//! is an unfortunate overlap.
//!
//! FrogDB's INFO output does not implement Sentinel-related fields (e.g.
//! `sentinel_tilt`) so the upstream "should NOT contain sentinel_tilt"
//! assertions are vacuously true on FrogDB. The tests still exercise the
//! core substring-based section filtering contract.

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

/// Fetch an INFO response as a UTF-8 string. `args` are appended after
/// the `INFO` verb — pass `&[]` for a no-arg INFO.
async fn info_as_string(
    client: &mut frogdb_test_harness::server::TestClient,
    args: &[&str],
) -> String {
    let mut full = vec!["INFO"];
    full.extend_from_slice(args);
    let resp = client.command(&full).await;
    std::str::from_utf8(unwrap_bulk(&resp))
        .expect("INFO response must be valid UTF-8")
        .to_string()
}

// ---------------------------------------------------------------------------
// info command with at most one sub command
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_info_command_with_at_most_one_sub_command() {
    // Upstream iterates over {"" "all" "default" "everything"} and asserts
    // that basic fields are present in each, plus that `rejected_calls`
    // (a commandstats field) is present only for "all"/"everything" and
    // absent for "" and "default".
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for arg in ["", "all", "default", "everything"] {
        let info = if arg.is_empty() {
            info_as_string(&mut client, &[]).await
        } else {
            info_as_string(&mut client, &[arg]).await
        };

        assert!(
            info.contains("redis_version"),
            "INFO {arg} should contain redis_version"
        );
        assert!(
            info.contains("used_cpu_user"),
            "INFO {arg} should contain used_cpu_user"
        );
        assert!(
            !info.contains("sentinel_tilt"),
            "INFO {arg} should not contain sentinel_tilt (FrogDB has no Sentinel)"
        );
        assert!(
            info.contains("used_memory"),
            "INFO {arg} should contain used_memory"
        );

        if arg.is_empty() || arg == "default" {
            assert!(
                !info.contains("rejected_calls"),
                "INFO {arg} should not contain rejected_calls (commandstats is not in default)"
            );
        } else {
            assert!(
                info.contains("rejected_calls"),
                "INFO {arg} should contain rejected_calls (commandstats is included in all/everything)"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// info command with one sub-section
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_info_command_with_one_sub_section() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // INFO cpu: cpu-only section; no memory, no sentinel.
    let info = info_as_string(&mut client, &["cpu"]).await;
    assert!(
        info.contains("used_cpu_user"),
        "INFO cpu should contain used_cpu_user"
    );
    assert!(
        !info.contains("sentinel_tilt"),
        "INFO cpu should not contain sentinel_tilt"
    );
    assert!(
        !info.contains("used_memory"),
        "INFO cpu should not contain used_memory"
    );

    // INFO sentinel: FrogDB treats unknown sections as empty; upstream
    // also asserts it lacks sentinel_tilt and used_memory.
    let info = info_as_string(&mut client, &["sentinel"]).await;
    assert!(
        !info.contains("sentinel_tilt"),
        "INFO sentinel should not contain sentinel_tilt on FrogDB"
    );
    assert!(
        !info.contains("used_memory"),
        "INFO sentinel should not contain used_memory"
    );

    // INFO commandSTATS: case-insensitive section matching; should expose
    // rejected_calls (from commandstats) without leaking memory fields.
    let info = info_as_string(&mut client, &["commandSTATS"]).await;
    assert!(
        !info.contains("used_memory"),
        "INFO commandSTATS should not contain used_memory"
    );
    assert!(
        info.contains("rejected_calls"),
        "INFO commandSTATS should contain rejected_calls"
    );
}

// ---------------------------------------------------------------------------
// info command with multiple sub-sections
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_info_command_with_multiple_sub_sections() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // INFO cpu sentinel: cpu only; sentinel is unknown (empty on FrogDB).
    let info = info_as_string(&mut client, &["cpu", "sentinel"]).await;
    assert!(
        info.contains("used_cpu_user"),
        "INFO cpu sentinel should contain used_cpu_user"
    );
    assert!(
        !info.contains("sentinel_tilt"),
        "INFO cpu sentinel should not contain sentinel_tilt"
    );
    assert!(
        !info.contains("master_repl_offset"),
        "INFO cpu sentinel should not contain master_repl_offset (replication not requested)"
    );

    // INFO cpu all: cpu first, then all remaining sections (including
    // commandstats and replication and memory).
    let info = info_as_string(&mut client, &["cpu", "all"]).await;
    assert!(
        info.contains("used_cpu_user"),
        "INFO cpu all should contain used_cpu_user"
    );
    assert!(
        !info.contains("sentinel_tilt"),
        "INFO cpu all should not contain sentinel_tilt"
    );
    assert!(
        info.contains("used_memory"),
        "INFO cpu all should contain used_memory"
    );
    assert!(
        info.contains("master_repl_offset"),
        "INFO cpu all should contain master_repl_offset"
    );
    assert!(
        info.contains("rejected_calls"),
        "INFO cpu all should contain rejected_calls"
    );
    // Check that we didn't get the cpu section twice.
    assert!(
        info.matches("used_cpu_user_children").count() <= 1,
        "INFO cpu all should not include the cpu section twice"
    );

    // INFO cpu default: cpu first, then default sections (no commandstats).
    let info = info_as_string(&mut client, &["cpu", "default"]).await;
    assert!(
        info.contains("used_cpu_user"),
        "INFO cpu default should contain used_cpu_user"
    );
    assert!(
        !info.contains("sentinel_tilt"),
        "INFO cpu default should not contain sentinel_tilt"
    );
    assert!(
        info.contains("used_memory"),
        "INFO cpu default should contain used_memory"
    );
    assert!(
        info.contains("master_repl_offset"),
        "INFO cpu default should contain master_repl_offset"
    );
    assert!(
        !info.contains("rejected_calls"),
        "INFO cpu default should not contain rejected_calls"
    );
    assert!(
        info.matches("used_cpu_user_children").count() <= 1,
        "INFO cpu default should not include the cpu section twice"
    );
}
