//! Integration tests for the INFO section builder.
//!
//! Pins the connection-level render path end to end: section structure and
//! order, fleet-wide aggregation (all shards, not just the local one), real
//! keyspace hit/miss counters, and the Persistence section's real WAL fields
//! (present and live when persistence is enabled, honestly absent when not).

use crate::common::response_helpers::{assert_ok, unwrap_bulk};
use crate::common::test_server::{TestServer, TestServerConfig};
use frogdb_protocol::Response;

/// Run INFO with the given args and return the response text.
async fn info_text(client: &mut frogdb_test_harness::server::TestClient, args: &[&str]) -> String {
    let mut cmd = vec!["INFO"];
    cmd.extend_from_slice(args);
    let resp = client.command(&cmd).await;
    String::from_utf8(unwrap_bulk(&resp).to_vec()).expect("INFO is utf8")
}

/// Section headers (`# Name`) in order of appearance.
fn section_headers(info: &str) -> Vec<&str> {
    info.lines()
        .filter(|l| l.starts_with("# "))
        .map(|l| l.trim_end())
        .collect()
}

/// The value of `field:` as a string, if present.
fn field<'a>(info: &'a str, name: &str) -> Option<&'a str> {
    let prefix = format!("{name}:");
    info.lines()
        .map(|l| l.trim_end())
        .find_map(|l| l.strip_prefix(prefix.as_str()))
}

/// The value of `field:` parsed as u64, panicking if absent.
fn field_u64(info: &str, name: &str) -> u64 {
    field(info, name)
        .unwrap_or_else(|| panic!("field {name} missing from INFO:\n{info}"))
        .parse()
        .unwrap_or_else(|v| panic!("field {name} not numeric: {v:?}"))
}

// ============================================================================
// Structure
// ============================================================================

#[tokio::test]
async fn info_default_renders_sections_in_canonical_order() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let info = info_text(&mut client, &[]).await;
    let headers = section_headers(&info);
    // Ratelimit renders nothing while inactive, so the default set is:
    assert_eq!(
        headers,
        vec![
            "# Server",
            "# Clients",
            "# Memory",
            "# Persistence",
            "# Stats",
            "# Replication",
            "# CPU",
            "# Keyspace",
        ],
        "full INFO:\n{info}"
    );
    // Every section is CRLF-terminated with a blank line.
    assert!(info.ends_with("\r\n\r\n"), "trailing blank line:\n{info:?}");
}

#[tokio::test]
async fn info_all_includes_extra_sections() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    assert_ok(&client.command(&["SET", "k", "v"]).await);

    let info = info_text(&mut client, &["all"]).await;
    for header in [
        "# Commandstats",
        "# Errorstats",
        "# Latencystats",
        "# Latency_Baseline",
        "# Tiered",
        "# Keysizes",
    ] {
        assert!(info.contains(header), "{header} missing:\n{info}");
    }
    // The connection's own SET appears via the local-stats merge.
    assert!(info.contains("cmdstat_set:calls="), "{info}");
}

#[tokio::test]
async fn info_section_filter_returns_only_requested_in_arg_order() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let info = info_text(&mut client, &["cpu", "server"]).await;
    assert_eq!(
        section_headers(&info),
        vec!["# CPU", "# Server"],
        "arg order preserved:\n{info}"
    );

    let unknown = info_text(&mut client, &["bogus_section"]).await;
    assert_eq!(unknown, "", "unknown sections render nothing");
}

// ============================================================================
// Fleet-wide aggregation (flag 3 companion: the one scatter sees every shard)
// ============================================================================

#[tokio::test]
async fn info_keyspace_counts_keys_across_all_shards() {
    // Default harness config runs 4 shards; keys spread across them.
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..20 {
        assert_ok(&client.command(&["SET", &format!("agg:key:{i}"), "v"]).await);
    }

    let info = info_text(&mut client, &["keyspace"]).await;
    assert!(
        info.contains("db0:keys=20,"),
        "db0 must count every shard's keys, not just the local shard:\n{info}"
    );

    // INFO and DBSIZE agree.
    let dbsize = client.command(&["DBSIZE"]).await;
    assert!(matches!(dbsize, Response::Integer(20)), "{dbsize:?}");
}

// ============================================================================
// Keyspace hits/misses come from the real metrics counters (flag 1)
// ============================================================================

#[tokio::test]
async fn info_keyspace_hits_and_misses_reflect_real_counters() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "hitkey", "v"]).await);
    // One hit, one miss.
    let _ = client.command(&["GET", "hitkey"]).await;
    let _ = client.command(&["GET", "nosuchkey"]).await;

    // The counters are incremented by shard workers via the metrics
    // recorder; poll briefly for the increments to land.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let info = info_text(&mut client, &["stats"]).await;
        let hits = field(&info, "keyspace_hits").and_then(|v| v.parse::<u64>().ok());
        let misses = field(&info, "keyspace_misses").and_then(|v| v.parse::<u64>().ok());
        if hits.is_some_and(|h| h >= 1) && misses.is_some_and(|m| m >= 1) {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("keyspace_hits/misses never reflected real counters:\n{info}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

// ============================================================================
// Persistence WAL fields (flag 2)
// ============================================================================

#[tokio::test]
async fn info_persistence_disabled_reports_honest_absence() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let info = info_text(&mut client, &["persistence"]).await;
    assert_eq!(field(&info, "persistence_enabled"), Some("0"), "{info}");
    // No placeholder WAL zeros: the fields are absent entirely.
    for name in [
        "wal_pending_ops",
        "wal_pending_bytes",
        "wal_durability_lag_ms",
        "wal_last_flush_status",
        "wal_flush_failures",
        "wal_lost_ops",
        "wal_last_flush_time",
        "wal_writes_total",
        "wal_bytes_total",
    ] {
        assert!(
            field(&info, name).is_none(),
            "{name} must be absent when persistence is disabled:\n{info}"
        );
    }
}

#[tokio::test]
async fn info_persistence_enabled_reports_live_wal_values() {
    let tmp = tempfile::tempdir().unwrap();
    let server = TestServer::start_standalone_with_config(TestServerConfig {
        persistence: true,
        data_dir: Some(tmp.path().to_path_buf()),
        num_shards: Some(2),
        ..Default::default()
    })
    .await;
    let mut client = server.connect().await;

    for i in 0..5 {
        assert_ok(&client.command(&["SET", &format!("wal:key:{i}"), "v"]).await);
    }

    let info = info_text(&mut client, &["persistence"]).await;
    assert_eq!(field(&info, "persistence_enabled"), Some("1"), "{info}");
    // Real aggregated values, not permanent placeholders: the lag fields
    // exist and parse, and the last-flush wall clock is a real timestamp
    // (the old stub reported a hardcoded 0 forever).
    let _ = field_u64(&info, "wal_pending_ops");
    let _ = field_u64(&info, "wal_pending_bytes");
    let _ = field_u64(&info, "wal_durability_lag_ms");
    // Durability outcomes: a healthy fresh server reports ok and no losses.
    assert_eq!(field(&info, "wal_last_flush_status"), Some("ok"), "{info}");
    assert_eq!(field_u64(&info, "wal_flush_failures"), 0, "{info}");
    assert_eq!(field_u64(&info, "wal_lost_ops"), 0, "{info}");
    assert!(
        field_u64(&info, "wal_last_flush_time") > 0,
        "wal_last_flush_time must be a live unix timestamp:\n{info}"
    );
    // Writes have been recorded through the same counters Prometheus scrapes.
    assert!(
        field_u64(&info, "wal_writes_total") >= 5,
        "wal_writes_total must count the writes:\n{info}"
    );
    // The dirty counter aggregates across both shards.
    assert!(
        field_u64(&info, "rdb_changes_since_last_save") >= 5,
        "{info}"
    );
}

// ============================================================================
// Memory aggregation
// ============================================================================

#[tokio::test]
async fn info_memory_reports_fleet_wide_usage() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Spread ~40KB of values across the shards.
    let big = "x".repeat(2048);
    for i in 0..20 {
        assert_ok(
            &client
                .command(&["SET", &format!("mem:key:{i}"), &big])
                .await,
        );
    }

    let info = info_text(&mut client, &["memory"]).await;
    let used = field_u64(&info, "used_memory");
    assert!(
        used >= 20 * 2048,
        "used_memory must cover every shard's data ({used} bytes):\n{info}"
    );
}
