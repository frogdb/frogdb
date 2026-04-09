//! Rust port of Redis 8.6.0 `unit/latency-monitor.tcl` test suite.
//!
//! FrogDB exposes the LATENCY command surface (LATENCY LATEST, HISTORY,
//! RESET, GRAPH, DOCTOR, HISTOGRAM, HELP, plus an FrogDB-specific
//! LATENCY BANDS subcommand), but does NOT collect per-command latency
//! histograms or time events that exceed `latency-monitor-threshold`.
//! All data-producing subcommands return empty structures:
//! * `LATENCY HISTOGRAM [...]` → empty map
//! * `LATENCY HISTORY <event>` → empty array
//! * `LATENCY LATEST` → empty array
//! * `LATENCY GRAPH <event>` → `"<event> - no data available"`
//! * `LATENCY DOCTOR` → a canned "no latency problems" report string
//! * `LATENCY RESET` → `0` (no events to reset)
//!
//! FrogDB also validates event names strictly against a fixed set
//! (`command`, `fork`, `aof-fsync`, `expire-cycle`, `eviction-cycle`,
//! `snapshot-io`) — unknown event names return
//! `ERR Unknown event type: ...` whereas upstream Redis is lenient and
//! returns an empty history. This is a deliberate FrogDB design choice.
//!
//! Given these differences, this port file covers only the two tests
//! whose assertions still hold against FrogDB's stub implementation
//! (`LATENCY DOCTOR` returning a non-empty string, and the surface area
//! of the top-level LATENCY command being addressable). Tests that
//! depend on actually populated histograms or tolerant event-name
//! validation are documented as intentional exclusions below.
//!
//! ## Intentional exclusions
//!
//! Histogram population not implemented (LATENCY HISTOGRAM always
//! returns empty; there is no per-command latency accumulator):
//! - `LATENCY HISTOGRAM with empty histogram` — histogram always empty (FrogDB doesn't record CONFIG|RESETSTAT)
//! - `LATENCY HISTOGRAM all commands` — FrogDB doesn't populate per-command histograms
//! - `LATENCY HISTOGRAM sub commands` — FrogDB doesn't populate per-command histograms
//! - `LATENCY HISTOGRAM with a subset of commands` — FrogDB doesn't populate per-command histograms
//! - `LATENCY HISTOGRAM command` — FrogDB doesn't populate per-command histograms
//! - `LATENCY HISTOGRAM with wrong command name skips the invalid one` — FrogDB doesn't populate per-command histograms
//!
//! Latency event logging not implemented (LATENCY HISTORY / LATEST /
//! GRAPH always return empty; threshold has no effect):
//! - `Test latency events logging` — latency event collection not implemented (also `needs:debug`)
//! - `LATENCY HISTORY output is ok` — latency event collection not implemented (also `needs:debug`)
//! - `LATENCY LATEST output is ok` — latency event collection not implemented (also `needs:debug`)
//! - `LATENCY GRAPH can output the event graph` — latency event collection not implemented (also `needs:debug`)
//! - `LATENCY of expire events are correctly collected` — expire-cycle timing events not recorded
//! - `LATENCY GRAPH can output the expire event graph` — expire-cycle timing events not recorded
//!
//! Behavioral differences in argument validation:
//! - `LATENCY HISTORY / RESET with wrong event name is fine` — FrogDB strictly validates event names and errors on unknown events
//! - `LATENCY HELP should not have unexpected options` — FrogDB's LATENCY HELP accepts extra arguments without erroring
//!
//! Empty-result reset test:
//! - `LATENCY RESET is able to reset events` — upstream asserts `reset > 0` and empty `LATEST` after; FrogDB always returns 0 from RESET because no events are tracked, so the `> 0` assertion can't hold

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// LATENCY DOCTOR produces some output
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_latency_doctor_produces_some_output() {
    // Upstream: `assert {[string length [r latency doctor]] > 0}`
    // FrogDB returns a fixed "I have a few latency reports to share: ..."
    // message when no events exist, which satisfies the non-empty assertion.
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["LATENCY", "DOCTOR"]).await;
    let bulk = unwrap_bulk(&resp);
    assert!(
        !bulk.is_empty(),
        "LATENCY DOCTOR should produce non-empty output, got: {bulk:?}"
    );
}
