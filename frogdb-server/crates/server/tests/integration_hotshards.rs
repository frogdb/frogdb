//! Integration tests for `FROGDB.HOTSHARDS`.
//!
//! These are the end-to-end proof that the collector is *wired*: the shard
//! event loop must actually record every dispatched command into its op-rate
//! ring buffer, the collector must be installed on the node, and the command
//! must render live numbers. A report of all zeros passes no test here.

use crate::common::test_server::{TestServer, get_error_message};
use frogdb_protocol::Response;

/// Flatten a field/value array reply into `(name, value)` pairs.
fn pairs(response: &Response) -> Vec<(String, Response)> {
    let Response::Array(items) = response else {
        panic!("expected an array reply, got {response:?}");
    };
    items
        .chunks(2)
        .map(|chunk| match &chunk[0] {
            Response::Bulk(Some(name)) => {
                (String::from_utf8_lossy(name).into_owned(), chunk[1].clone())
            }
            other => panic!("expected a bulk field name, got {other:?}"),
        })
        .collect()
}

fn field(response: &Response, name: &str) -> Response {
    pairs(response)
        .into_iter()
        .find(|(field, _)| field == name)
        .unwrap_or_else(|| panic!("missing field {name} in {response:?}"))
        .1
}

fn int_field(response: &Response, name: &str) -> i64 {
    match field(response, name) {
        Response::Integer(v) => v,
        other => panic!("{name} should be an integer, got {other:?}"),
    }
}

/// Rates travel as fixed-precision bulk strings (RESP2 has no float type).
fn rate_field(response: &Response, name: &str) -> f64 {
    match field(response, name) {
        Response::Bulk(Some(b)) => String::from_utf8_lossy(&b)
            .parse()
            .unwrap_or_else(|e| panic!("{name} should parse as a rate: {e}")),
        other => panic!("{name} should be a bulk rate, got {other:?}"),
    }
}

fn string_field(response: &Response, name: &str) -> String {
    match field(response, name) {
        Response::Bulk(Some(b)) => String::from_utf8_lossy(&b).into_owned(),
        other => panic!("{name} should be a bulk string, got {other:?}"),
    }
}

fn shards(response: &Response) -> Vec<Response> {
    match field(response, "shards") {
        Response::Array(items) => items,
        other => panic!("shards should be an array, got {other:?}"),
    }
}

/// A freshly started server has every shard idle: the report must be present,
/// cover every shard, and classify nothing as hot. Zeros are correct *here* —
/// and this pins the balanced baseline the skew test is measured against.
#[tokio::test]
async fn test_hotshards_idle_fleet_is_balanced() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let report = client.command(&["FROGDB.HOTSHARDS"]).await;

    assert_eq!(
        int_field(&report, "num_shards"),
        4,
        "the report must cover every shard of the default 4-shard test server"
    );
    assert_eq!(int_field(&report, "period_secs"), 10, "configured default");
    assert_eq!(int_field(&report, "hot_count"), 0);
    assert_eq!(int_field(&report, "warm_count"), 0);
    assert_eq!(shards(&report).len(), 4);
    for shard in shards(&report) {
        assert_eq!(
            string_field(&shard, "status"),
            "OK",
            "an idle shard is never hot"
        );
    }
}

/// The heart of the feature: hammer a single key so every command lands on one
/// shard, then assert the report shows that skew with live numbers. This fails
/// if the shard event loop stops feeding its op counters, if the collector is
/// not installed, or if the reply is rendered from stale/zero data.
#[tokio::test]
async fn test_hotshards_reports_non_uniform_load() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // All writes to one key => one shard. 200 ops over the 10s default window
    // is ~20 ops/sec on that shard and 0 everywhere else.
    for i in 0..200 {
        let value = i.to_string();
        client.command(&["SET", "hot:key", &value]).await;
    }
    // A handful of reads on the same shard, so the read/write split is
    // observable rather than write-only.
    for _ in 0..20 {
        client.command(&["GET", "hot:key"]).await;
    }

    let report = client.command(&["FROGDB.HOTSHARDS"]).await;

    assert!(
        rate_field(&report, "total_ops_per_sec") > 0.0,
        "the fleet just served 220 commands; a zero total means the op \
         counters are not being fed: {report:?}"
    );
    assert_eq!(
        int_field(&report, "hot_count"),
        1,
        "exactly one shard took all the traffic: {report:?}"
    );
    assert!(
        rate_field(&report, "imbalance_ratio") > 2.0,
        "one shard out of four carrying everything is a 4x imbalance: {report:?}"
    );

    // Shards are sorted hottest-first.
    let shards = shards(&report);
    let hottest = &shards[0];
    assert_eq!(string_field(hottest, "status"), "HOT");
    assert!(
        rate_field(hottest, "percentage") > 90.0,
        "the single loaded shard carries nearly all traffic: {hottest:?}"
    );
    assert!(
        rate_field(hottest, "writes_per_sec") > 0.0,
        "200 SETs must register as writes: {hottest:?}"
    );
    assert!(
        rate_field(hottest, "reads_per_sec") > 0.0,
        "20 GETs must register as reads: {hottest:?}"
    );
    assert!(
        rate_field(hottest, "ops_per_sec")
            >= rate_field(hottest, "reads_per_sec") + rate_field(hottest, "writes_per_sec") - 0.01,
        "ops must account for both reads and writes: {hottest:?}"
    );

    // The idle shards stay at zero — the collector reports per-shard numbers,
    // not a fleet average smeared across shards.
    let idle = shards
        .iter()
        .filter(|s| rate_field(s, "ops_per_sec") == 0.0)
        .count();
    assert_eq!(idle, 3, "only one shard was targeted: {report:?}");

    assert!(
        !matches!(field(&report, "recommendations"), Response::Array(ref r) if r.is_empty()),
        "a 4x imbalance must produce at least one recommendation: {report:?}"
    );
}

/// `PERIOD` selects the window the shards answer over, and the rate is divided
/// by the seconds of data that window actually covers — not by the number the
/// operator asked for. A burst measured seconds after startup is the same rate
/// through a 5s and a 30s window, because neither has 30 seconds of history;
/// dividing by the request would report a fraction of real load exactly when
/// someone is hunting a hot shard.
#[tokio::test]
async fn test_hotshards_period_rates_are_divided_by_observed_seconds() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..200 {
        let value = i.to_string();
        client.command(&["SET", "period:key", &value]).await;
    }

    let short = client.command(&["FROGDB.HOTSHARDS", "PERIOD", "5"]).await;
    let long = client.command(&["FROGDB.HOTSHARDS", "PERIOD", "30"]).await;

    // The requested window still reaches the shards and is reported back.
    assert_eq!(int_field(&short, "period_secs"), 5);
    assert_eq!(int_field(&long, "period_secs"), 30);

    let short_rate = rate_field(&short, "total_ops_per_sec");
    let long_rate = rate_field(&long, "total_ops_per_sec");
    assert!(
        short_rate > 0.0 && long_rate > 0.0,
        "both windows see the ops"
    );
    assert!(
        long_rate >= short_rate * 0.5,
        "a wider window must not deflate a rate the node has no history for \
         ({long_rate} vs {short_rate}); dividing 200 ops by 30 would report ~1/6 \
         of real load"
    );
}

/// The window slides, and `PERIOD` chooses how far back it reaches: once a
/// burst is older than the requested window it drops out of the rate, while a
/// wider window still covers it. This is what proves the argument reaches the
/// shards rather than being parsed and dropped.
#[tokio::test]
async fn test_hotshards_period_argument_selects_how_far_back_the_window_reaches() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..200 {
        let value = i.to_string();
        client.command(&["SET", "period:key", &value]).await;
    }

    // Let the burst age out of a 1-second window while staying inside a wide one.
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;

    let narrow = client.command(&["FROGDB.HOTSHARDS", "PERIOD", "1"]).await;
    let wide = client.command(&["FROGDB.HOTSHARDS", "PERIOD", "30"]).await;

    let narrow_rate = rate_field(&narrow, "total_ops_per_sec");
    let wide_rate = rate_field(&wide, "total_ops_per_sec");
    assert!(
        narrow_rate < 1.0,
        "a 1s window must have slid past a burst that stopped 2.5s ago, got {narrow_rate}"
    );
    assert!(
        wide_rate > 10.0,
        "a 30s window must still cover that burst, got {wide_rate}"
    );
}

/// The averaging window is bounded by the per-shard ring (60x1s buckets), and
/// the *clamped* value is what the reply reports: a reply claiming a 1-hour
/// window over 60 seconds of data would misdescribe what was measured.
#[tokio::test]
async fn test_hotshards_period_is_clamped_and_reported_honestly() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let report = client
        .command(&["FROGDB.HOTSHARDS", "PERIOD", "3600"])
        .await;
    assert_eq!(
        int_field(&report, "period_secs"),
        60,
        "an over-long window must be reported as the window actually used"
    );
}

/// The `/status` JSON renders the *same* collector as `FROGDB.HOTSHARDS`, so
/// the section must be present and carry the same live numbers. `ops_per_sec`
/// is now a real windowed rate sourced from that snapshot.
#[tokio::test]
async fn test_status_json_carries_the_hot_shard_section() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..200 {
        let value = i.to_string();
        client.command(&["SET", "status:key", &value]).await;
    }

    let reply = client.command(&["STATUS", "JSON"]).await;
    let Response::Bulk(Some(body)) = reply else {
        panic!("STATUS JSON must reply with a bulk string, got {reply:?}");
    };
    let status: serde_json::Value = serde_json::from_slice(&body).expect("valid JSON");

    let hot = &status["hot_shards"];
    assert!(!hot.is_null(), "hot_shards section missing from {status:#}");
    assert_eq!(hot["shards"].as_array().expect("shard rows").len(), 4);
    assert!(
        hot["total_ops_per_sec"].as_f64().unwrap_or(0.0) > 0.0,
        "the section must carry live numbers, not zeros: {hot:#}"
    );
    assert_eq!(hot["hot_count"].as_u64(), Some(1));

    // The node-wide rate is the collector's total, never a second estimate.
    assert_eq!(
        status["commands"]["ops_per_sec"].as_f64(),
        hot["total_ops_per_sec"].as_f64(),
    );

    // Classes serialize lowercase for JSON consumers.
    let classes: Vec<&str> = hot["shards"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["class"].as_str().expect("class"))
        .collect();
    assert!(classes.contains(&"hot"), "got {classes:?}");
}

/// The debug web UI panel renders the same installed collector, so the HTML
/// must show the live skew rather than the "no collector" placeholder.
#[tokio::test]
async fn test_debug_ui_hot_shard_panel_renders_live_load() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..200 {
        let value = i.to_string();
        client.command(&["SET", "ui:key", &value]).await;
    }

    let http = reqwest::Client::builder().no_proxy().build().unwrap();

    let html = http
        .get(server.metrics_url("/debug/partials/hot-shards"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(html.contains("Shard Load"), "panel header: {html}");
    assert!(
        !html.contains("No hot-shard collector installed"),
        "the collector must be installed on a real server: {html}"
    );
    assert!(html.contains("HOT"), "the loaded shard is flagged: {html}");

    // The same panel is part of the combined performance view.
    let performance = http
        .get(server.metrics_url("/debug/partials/performance"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(performance.contains("Shard Load"), "got {performance}");

    let json: serde_json::Value = http
        .get(server.metrics_url("/debug/api/hot-shards"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(json["hot_count"].as_u64(), Some(1), "got {json:#}");
    assert!(json["total_ops_per_sec"].as_f64().unwrap_or(0.0) > 0.0);

    server.shutdown().await;
}

#[tokio::test]
async fn test_hotshards_rejects_bad_arguments() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let bad_syntax = client.command(&["FROGDB.HOTSHARDS", "NOPE"]).await;
    assert!(
        get_error_message(&bad_syntax)
            .unwrap_or_default()
            .contains("syntax error"),
        "got {bad_syntax:?}"
    );

    let zero = client.command(&["FROGDB.HOTSHARDS", "PERIOD", "0"]).await;
    assert!(
        get_error_message(&zero)
            .unwrap_or_default()
            .contains("greater than 0"),
        "got {zero:?}"
    );

    let not_a_number = client.command(&["FROGDB.HOTSHARDS", "PERIOD", "wat"]).await;
    assert!(
        get_error_message(&not_a_number).is_some(),
        "got {not_a_number:?}"
    );
}
