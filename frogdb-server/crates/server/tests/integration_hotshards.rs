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

/// `PERIOD` selects the averaging window: the same op count over a longer
/// window is a lower rate. This proves the argument reaches the shards rather
/// than being parsed and dropped.
#[tokio::test]
async fn test_hotshards_period_argument_widens_the_window() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..200 {
        let value = i.to_string();
        client.command(&["SET", "period:key", &value]).await;
    }

    let short = client.command(&["FROGDB.HOTSHARDS", "PERIOD", "5"]).await;
    let long = client.command(&["FROGDB.HOTSHARDS", "PERIOD", "30"]).await;

    assert_eq!(int_field(&short, "period_secs"), 5);
    assert_eq!(int_field(&long, "period_secs"), 30);

    let short_rate = rate_field(&short, "total_ops_per_sec");
    let long_rate = rate_field(&long, "total_ops_per_sec");
    assert!(
        short_rate > 0.0 && long_rate > 0.0,
        "both windows see the ops"
    );
    assert!(
        long_rate < short_rate,
        "the same burst averaged over 30s must be a lower rate than over 5s \
         ({long_rate} vs {short_rate})"
    );
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
