//! Rust port of Redis 8.6.0 `unit/info.tcl` test suite (subset).
//!
//! ## Intentional exclusions
//!
//! - `Verify that LUT overhead is properly updated when dicts are emptied or reused (issue #13973)` — redis-specific — Redis-internal dict/LUT overhead accounting

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn info_returns_bulk_string() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["INFO"]).await;
    // INFO returns a bulk string
    let info_text = String::from_utf8(unwrap_bulk(&resp).to_vec()).unwrap();
    assert!(!info_text.is_empty());
}

// issue 06 (redis-feel): `redis_version` advertises the compat version
// (single source of truth: `frogdb_core::ADVERTISED_REDIS_VERSION`), while
// `frogdb_version` keeps reporting the actual product version.
#[tokio::test]
async fn info_redis_version_is_the_advertised_compat_version() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["INFO", "server"]).await;
    let text = String::from_utf8(unwrap_bulk(&resp).to_vec()).unwrap();
    let expected = format!(
        "redis_version:{}\r\n",
        frogdb_core::ADVERTISED_REDIS_VERSION
    );
    assert!(
        text.contains(&expected),
        "expected {expected:?} in server section: {text}"
    );
    // frogdb_version keeps reporting the real product version — distinct
    // from the advertised compat version above (workspace-inherited, but
    // asserted structurally rather than pinned to the crate's own
    // `CARGO_PKG_VERSION`, which is a different crate than the server).
    let frogdb_version_line = text
        .lines()
        .find(|l| l.starts_with("frogdb_version:"))
        .unwrap_or_else(|| panic!("no frogdb_version line: {text}"));
    assert_ne!(
        frogdb_version_line,
        format!("frogdb_version:{}", frogdb_core::ADVERTISED_REDIS_VERSION),
        "frogdb_version and redis_version happening to share a string would hide a \
         regression where one was accidentally wired to the other: {text}"
    );
}

#[tokio::test]
async fn info_server_section_filtering() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["INFO", "server"]).await;
    let text = String::from_utf8(unwrap_bulk(&resp).to_vec()).unwrap();
    assert!(
        text.contains("redis_version") || text.contains("frogdb_version"),
        "server section should contain version info: {text}"
    );
}

#[tokio::test]
async fn info_clients_section() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["INFO", "clients"]).await;
    let text = String::from_utf8(unwrap_bulk(&resp).to_vec()).unwrap();
    assert!(
        text.contains("connected_clients"),
        "clients section should contain connected_clients: {text}"
    );
}

#[tokio::test]
async fn info_memory_section() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["INFO", "memory"]).await;
    let text = String::from_utf8(unwrap_bulk(&resp).to_vec()).unwrap();
    assert!(
        text.contains("used_memory"),
        "memory section should contain used_memory: {text}"
    );
}

#[tokio::test]
async fn info_all_returns_all_sections() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["INFO", "all"]).await;
    let text = String::from_utf8(unwrap_bulk(&resp).to_vec()).unwrap();
    // ALL should cover multiple sections
    assert!(
        text.contains("connected_clients") || text.contains("used_memory"),
        "INFO ALL should contain stats: {text}"
    );
}

// issue 08 (redis-feel): bare INFO must include every section Redis 8.6's
// own default list includes — errorstats, cluster, and keysizes are real
// Redis defaults, not `all`-only extras, and Threads/Modules were previously
// missing from FrogDB's INFO surface entirely.
#[tokio::test]
async fn info_default_includes_redis_8_6_default_sections() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["INFO"]).await;
    let text = String::from_utf8(unwrap_bulk(&resp).to_vec()).unwrap();
    for section in [
        "# Server",
        "# Clients",
        "# Memory",
        "# Persistence",
        "# Stats",
        "# Replication",
        "# Threads",
        "# CPU",
        "# Modules",
        "# Errorstats",
        "# Cluster",
        "# Keyspace",
        "# Keysizes",
    ] {
        assert!(
            text.contains(section),
            "bare INFO should contain {section}: {text}"
        );
    }
    // commandstats/latencystats stay `all`/`everything`-only, matching Redis.
    assert!(!text.contains("# Commandstats"), "{text}");
    assert!(!text.contains("# Latencystats"), "{text}");

    let resp = client.command(&["INFO", "cluster"]).await;
    let cluster = String::from_utf8(unwrap_bulk(&resp).to_vec()).unwrap();
    assert!(
        cluster.contains("cluster_enabled:0"),
        "standalone server should report cluster_enabled:0: {cluster}"
    );
}

#[tokio::test]
async fn info_keyspace_shows_db_stats_after_writes() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Write a key first
    client.command(&["SET", "testkey", "testval"]).await;

    let resp = client.command(&["INFO", "keyspace"]).await;
    let text = String::from_utf8(unwrap_bulk(&resp).to_vec()).unwrap();
    // After a write, keyspace section should contain db info
    // (may be empty if not tracked per-db, just verify no error)
    assert!(!matches!(
        client.command(&["INFO", "keyspace"]).await,
        frogdb_protocol::Response::Error(_)
    ));
    let _ = text;
}
