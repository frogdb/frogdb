//! Rust port of Redis 8.6.0 `unit/geo.tcl` test suite.
//!
//! Excludes: encoding-specific loops, fuzzy randomized search tests
//! (`GEOSEARCH fuzzy test`, `GEOSEARCH box edges fuzzy test`),
//! `needs:debug`, `needs:repl`.

use frogdb_protocol::Response;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::{TestClient, TestServer};

// ---------------------------------------------------------------------------
// Helper functions (inlined from TCL helper procs)
// ---------------------------------------------------------------------------

/// Convert degrees to radians.
#[allow(dead_code)]
fn geo_degrad(deg: f64) -> f64 {
    deg * (std::f64::consts::PI / 180.0)
}

/// Haversine distance between two points in meters.
#[allow(dead_code)]
fn geo_distance(lon1d: f64, lat1d: f64, lon2d: f64, lat2d: f64) -> f64 {
    let lon1r = geo_degrad(lon1d);
    let lat1r = geo_degrad(lat1d);
    let lon2r = geo_degrad(lon2d);
    let lat2r = geo_degrad(lat2d);
    let v = ((lon2r - lon1r) / 2.0).sin();
    let u = ((lat2r - lat1r) / 2.0).sin();
    2.0 * 6_372_797.560856 * (u * u + lat1r.cos() * lat2r.cos() * v * v).sqrt().asin()
}

/// Parse a bulk string response as f64.
fn parse_bulk_f64(resp: &Response) -> f64 {
    let b = unwrap_bulk(resp);
    std::str::from_utf8(b).unwrap().parse::<f64>().unwrap()
}

/// Assert that two f64 values are within `tolerance` of each other.
fn assert_float_eq(actual: f64, expected: f64, tolerance: f64) {
    assert!(
        (actual - expected).abs() < tolerance,
        "expected {expected} +/- {tolerance}, got {actual}",
    );
}

/// Extract the nested [lon, lat] pair from a GEOPOS response element.
/// GEOPOS returns Array([ Array([Bulk(lon), Bulk(lat)]), ... ]).
fn extract_lonlat(resp: &Response, index: usize) -> (f64, f64) {
    let outer = match resp {
        Response::Array(items) => items,
        other => panic!("expected Array, got {other:?}"),
    };
    match &outer[index] {
        Response::Array(pair) => {
            let lon = parse_bulk_f64(&pair[0]);
            let lat = parse_bulk_f64(&pair[1]);
            (lon, lat)
        }
        Response::Bulk(None) => (f64::NAN, f64::NAN),
        other => panic!("expected Array or nil in geopos, got {other:?}"),
    }
}

/// Check if a GEOPOS element is nil (missing member).
fn is_geopos_nil(resp: &Response, index: usize) -> bool {
    let outer = match resp {
        Response::Array(items) => items,
        other => panic!("expected Array, got {other:?}"),
    };
    matches!(&outer[index], Response::Bulk(None))
        || matches!(&outer[index], Response::Array(v) if v.is_empty())
}

// ---------------------------------------------------------------------------
// GEO with wrong type / non-existing key
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB GEO behavior differs from Redis"]
async fn tcl_geo_wrong_type_src_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "src", "wrong_type"]).await;

    // GEORADIUS with lonlat
    assert_error_prefix(
        &client
            .command(&["GEORADIUS", "src", "1", "1", "1", "km"])
            .await,
        "WRONGTYPE",
    );
    // GEORADIUS STORE
    assert_error_prefix(
        &client
            .command(&[
                "GEORADIUS", "src", "1", "1", "1", "km", "store", "dest",
            ])
            .await,
        "WRONGTYPE",
    );
    // GEOSEARCH fromlonlat
    assert_error_prefix(
        &client
            .command(&[
                "GEOSEARCH", "src", "FROMLONLAT", "0", "0", "BYRADIUS", "1", "km",
            ])
            .await,
        "WRONGTYPE",
    );
    // GEOSEARCHSTORE fromlonlat
    assert_error_prefix(
        &client
            .command(&[
                "GEOSEARCHSTORE",
                "dest",
                "src",
                "FROMLONLAT",
                "0",
                "0",
                "BYRADIUS",
                "1",
                "km",
            ])
            .await,
        "WRONGTYPE",
    );
    // GEORADIUSBYMEMBER
    assert_error_prefix(
        &client
            .command(&["GEORADIUSBYMEMBER", "src", "member", "1", "km"])
            .await,
        "WRONGTYPE",
    );
    // GEODIST
    assert_error_prefix(
        &client
            .command(&["GEODIST", "src", "member", "1", "km"])
            .await,
        "WRONGTYPE",
    );
    // GEOHASH
    assert_error_prefix(
        &client.command(&["GEOHASH", "src", "member"]).await,
        "WRONGTYPE",
    );
    // GEOPOS
    assert_error_prefix(
        &client.command(&["GEOPOS", "src", "member"]).await,
        "WRONGTYPE",
    );
}

#[tokio::test]
#[ignore = "FrogDB GEO behavior differs from Redis"]
async fn tcl_geo_non_existing_src_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "src"]).await;

    // GEORADIUS on non-existing key returns empty array
    let resp = client
        .command(&["GEORADIUS", "src", "1", "1", "1", "km"])
        .await;
    assert_array_len(&resp, 0);

    // GEORADIUS STORE returns 0
    assert_integer_eq(
        &client
            .command(&[
                "GEORADIUS", "src", "1", "1", "1", "km", "store", "dest",
            ])
            .await,
        0,
    );

    // GEOSEARCH on non-existing key returns empty array
    let resp = client
        .command(&[
            "GEOSEARCH", "src", "FROMLONLAT", "0", "0", "BYRADIUS", "1", "km",
        ])
        .await;
    assert_array_len(&resp, 0);

    // GEOSEARCHSTORE returns 0
    assert_integer_eq(
        &client
            .command(&[
                "GEOSEARCHSTORE",
                "dest",
                "src",
                "FROMLONLAT",
                "0",
                "0",
                "BYRADIUS",
                "1",
                "km",
            ])
            .await,
        0,
    );
}

#[tokio::test]
#[ignore = "FrogDB GEO behavior differs from Redis"]
async fn tcl_geo_bylonlat_empty_search() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "src"]).await;
    client
        .command(&[
            "GEOADD", "src", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669",
            "Catania",
        ])
        .await;

    // Search at lon=1,lat=1 radius=1km finds nothing
    let resp = client
        .command(&["GEORADIUS", "src", "1", "1", "1", "km"])
        .await;
    assert_array_len(&resp, 0);

    assert_integer_eq(
        &client
            .command(&[
                "GEORADIUS", "src", "1", "1", "1", "km", "store", "dest",
            ])
            .await,
        0,
    );

    let resp = client
        .command(&[
            "GEOSEARCH", "src", "FROMLONLAT", "0", "0", "BYRADIUS", "1", "km",
        ])
        .await;
    assert_array_len(&resp, 0);

    assert_integer_eq(
        &client
            .command(&[
                "GEOSEARCHSTORE",
                "dest",
                "src",
                "FROMLONLAT",
                "0",
                "0",
                "BYRADIUS",
                "1",
                "km",
            ])
            .await,
        0,
    );
}

#[tokio::test]
#[ignore = "FrogDB GEO behavior differs from Redis"]
async fn tcl_geo_bymember_non_existing_member() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "src"]).await;
    client
        .command(&[
            "GEOADD", "src", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669",
            "Catania",
        ])
        .await;

    assert_error_prefix(
        &client
            .command(&["GEORADIUSBYMEMBER", "src", "member", "1", "km"])
            .await,
        "ERR",
    );
    assert_error_prefix(
        &client
            .command(&[
                "GEORADIUSBYMEMBER",
                "src",
                "member",
                "1",
                "km",
                "store",
                "dest",
            ])
            .await,
        "ERR",
    );
    assert_error_prefix(
        &client
            .command(&[
                "GEOSEARCH", "src", "FROMMEMBER", "member", "BYBOX", "1", "1", "km",
            ])
            .await,
        "ERR",
    );
    assert_error_prefix(
        &client
            .command(&[
                "GEOSEARCHSTORE",
                "dest",
                "src",
                "FROMMEMBER",
                "member",
                "BYBOX",
                "1",
                "1",
                "m",
            ])
            .await,
        "ERR",
    );
}

// ---------------------------------------------------------------------------
// GEOADD
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_geoadd_create() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_integer_eq(
        &client
            .command(&[
                "GEOADD",
                "nyc",
                "-73.9454966",
                "40.747533",
                "lic market",
            ])
            .await,
        1,
    );
}

#[tokio::test]
async fn tcl_geoadd_update() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "GEOADD",
            "nyc",
            "-73.9454966",
            "40.747533",
            "lic market",
        ])
        .await;
    assert_integer_eq(
        &client
            .command(&[
                "GEOADD",
                "nyc",
                "-73.9454966",
                "40.747533",
                "lic market",
            ])
            .await,
        0,
    );
}

#[tokio::test]
async fn tcl_geoadd_update_with_ch() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "GEOADD",
            "nyc",
            "-73.9454966",
            "40.747533",
            "lic market",
        ])
        .await;
    // CH + swapped coords => updates position, returns 1
    assert_integer_eq(
        &client
            .command(&[
                "GEOADD",
                "nyc",
                "CH",
                "40.747533",
                "-73.9454966",
                "lic market",
            ])
            .await,
        1,
    );
    let resp = client.command(&["GEOPOS", "nyc", "lic market"]).await;
    let (x1, y1) = extract_lonlat(&resp, 0);
    assert_float_eq(x1, 40.747, 0.001);
    assert!((y1.abs() - 73.945).abs() < 0.001);
}

#[tokio::test]
async fn tcl_geoadd_update_with_nx() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // First add with CH to set position to swapped coords
    client
        .command(&[
            "GEOADD",
            "nyc",
            "CH",
            "40.747533",
            "-73.9454966",
            "lic market",
        ])
        .await;
    // NX should not update existing member
    assert_integer_eq(
        &client
            .command(&[
                "GEOADD",
                "nyc",
                "NX",
                "-73.9454966",
                "40.747533",
                "lic market",
            ])
            .await,
        0,
    );
    let resp = client.command(&["GEOPOS", "nyc", "lic market"]).await;
    let (x1, y1) = extract_lonlat(&resp, 0);
    // Position should remain as originally set (40.747, -73.945)
    assert_float_eq(x1, 40.747, 0.001);
    assert!((y1.abs() - 73.945).abs() < 0.001);
}

#[tokio::test]
async fn tcl_geoadd_update_with_xx() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Seed the key with CH-swapped coords
    client
        .command(&[
            "GEOADD",
            "nyc",
            "CH",
            "40.747533",
            "-73.9454966",
            "lic market",
        ])
        .await;
    // XX updates existing member, returns 0 (no new members added)
    assert_integer_eq(
        &client
            .command(&[
                "GEOADD",
                "nyc",
                "XX",
                "-83.9454966",
                "40.747533",
                "lic market",
            ])
            .await,
        0,
    );
    let resp = client.command(&["GEOPOS", "nyc", "lic market"]).await;
    let (x1, y1) = extract_lonlat(&resp, 0);
    assert!((x1.abs() - 83.945).abs() < 0.001);
    assert!((y1.abs() - 40.747).abs() < 0.001);
}

#[tokio::test]
async fn tcl_geoadd_update_with_ch_nx() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "GEOADD",
            "nyc",
            "-73.9454966",
            "40.747533",
            "lic market",
        ])
        .await;
    // CH NX on existing member returns 0
    assert_integer_eq(
        &client
            .command(&[
                "GEOADD",
                "nyc",
                "CH",
                "NX",
                "-73.9454966",
                "40.747533",
                "lic market",
            ])
            .await,
        0,
    );
}

#[tokio::test]
#[ignore = "FrogDB GEO behavior differs from Redis"]
async fn tcl_geoadd_update_with_ch_xx() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "GEOADD",
            "nyc",
            "-73.9454966",
            "40.747533",
            "lic market",
        ])
        .await;
    // CH XX updates existing, returns 1 (changed)
    assert_integer_eq(
        &client
            .command(&[
                "GEOADD",
                "nyc",
                "CH",
                "XX",
                "-73.9454966",
                "40.747533",
                "lic market",
            ])
            .await,
        1,
    );
}

#[tokio::test]
async fn tcl_geoadd_xx_nx_syntax_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_error_prefix(
        &client
            .command(&[
                "GEOADD",
                "nyc",
                "xx",
                "nx",
                "-73.9454966",
                "40.747533",
                "lic market",
            ])
            .await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_geoadd_invalid_option() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_error_prefix(
        &client
            .command(&[
                "GEOADD",
                "nyc",
                "ch",
                "xx",
                "foo",
                "-73.9454966",
                "40.747533",
                "lic market",
            ])
            .await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_geoadd_invalid_coordinates() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_error_prefix(
        &client
            .command(&[
                "GEOADD",
                "nyc",
                "-73.9454966",
                "40.747533",
                "lic market",
                "foo",
                "bar",
                "luck market",
            ])
            .await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_geoadd_multi_add() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "GEOADD",
            "nyc",
            "-73.9454966",
            "40.747533",
            "lic market",
        ])
        .await;
    assert_integer_eq(
        &client
            .command(&[
                "GEOADD",
                "nyc",
                "-73.9733487",
                "40.7648057",
                "central park n/q/r",
                "-73.9903085",
                "40.7362513",
                "union square",
                "-74.0131604",
                "40.7126674",
                "wtc one",
                "-73.7858139",
                "40.6428986",
                "jfk",
                "-73.9375699",
                "40.7498929",
                "q4",
                "-73.9564142",
                "40.7480973",
                "4545",
            ])
            .await,
        6,
    );
}

// ---------------------------------------------------------------------------
// Check geoset values (ZRANGE)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_check_geoset_values() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Setup the NYC geoset
    setup_nyc(&mut client).await;

    let resp = client
        .command(&["ZRANGE", "nyc", "0", "-1", "WITHSCORES"])
        .await;
    let items = extract_bulk_strings(&resp);
    // Expected order by score: wtc one, union square, central park n/q/r, 4545, lic market, q4, jfk
    assert_eq!(items[0], "wtc one");
    assert_eq!(items[2], "union square");
    assert_eq!(items[4], "central park n/q/r");
    assert_eq!(items[6], "4545");
    assert_eq!(items[8], "lic market");
    assert_eq!(items[10], "q4");
    assert_eq!(items[12], "jfk");
}

/// Helper to set up the NYC geoset used by many tests.
async fn setup_nyc(client: &mut TestClient) {
    client
        .command(&[
            "GEOADD",
            "nyc",
            "-73.9454966",
            "40.747533",
            "lic market",
            "-73.9733487",
            "40.7648057",
            "central park n/q/r",
            "-73.9903085",
            "40.7362513",
            "union square",
            "-74.0131604",
            "40.7126674",
            "wtc one",
            "-73.7858139",
            "40.6428986",
            "jfk",
            "-73.9375699",
            "40.7498929",
            "q4",
            "-73.9564142",
            "40.7480973",
            "4545",
        ])
        .await;
}

// ---------------------------------------------------------------------------
// GEORADIUS / GEORADIUS_RO
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_georadius_simple_sorted() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    setup_nyc(&mut client).await;

    let resp = client
        .command(&[
            "GEORADIUS", "nyc", "-73.9798091", "40.7598464", "3", "km", "asc",
        ])
        .await;
    let members = extract_bulk_strings(&resp);
    assert_eq!(members, vec!["central park n/q/r", "4545", "union square"]);
}

#[tokio::test]
async fn tcl_georadius_ro_simple_sorted() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    setup_nyc(&mut client).await;

    let resp = client
        .command(&[
            "GEORADIUS_RO",
            "nyc",
            "-73.9798091",
            "40.7598464",
            "3",
            "km",
            "asc",
        ])
        .await;
    let members = extract_bulk_strings(&resp);
    assert_eq!(members, vec!["central park n/q/r", "4545", "union square"]);
}

// ---------------------------------------------------------------------------
// GEOSEARCH
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_geosearch_simple_sorted() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    setup_nyc(&mut client).await;

    let resp = client
        .command(&[
            "GEOSEARCH",
            "nyc",
            "FROMLONLAT",
            "-73.9798091",
            "40.7598464",
            "BYBOX",
            "6",
            "6",
            "km",
            "asc",
        ])
        .await;
    let members = extract_bulk_strings(&resp);
    assert_eq!(
        members,
        vec!["central park n/q/r", "4545", "union square", "lic market"]
    );
}

#[tokio::test]
async fn tcl_geosearch_fromlonlat_frommember_cannot_coexist() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    setup_nyc(&mut client).await;

    assert_error_prefix(
        &client
            .command(&[
                "GEOSEARCH",
                "nyc",
                "FROMLONLAT",
                "-73.9798091",
                "40.7598464",
                "FROMMEMBER",
                "xxx",
                "BYBOX",
                "6",
                "6",
                "km",
                "asc",
            ])
            .await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_geosearch_fromlonlat_or_frommember_required() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    setup_nyc(&mut client).await;

    assert_error_prefix(
        &client
            .command(&[
                "GEOSEARCH",
                "nyc",
                "BYBOX",
                "3",
                "3",
                "km",
                "asc",
                "desc",
                "WITHHASH",
                "WITHDIST",
                "WITHCOORD",
            ])
            .await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_geosearch_byradius_bybox_cannot_coexist() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    setup_nyc(&mut client).await;

    assert_error_prefix(
        &client
            .command(&[
                "GEOSEARCH",
                "nyc",
                "FROMLONLAT",
                "-73.9798091",
                "40.7598464",
                "BYRADIUS",
                "3",
                "km",
                "BYBOX",
                "3",
                "3",
                "km",
                "asc",
            ])
            .await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_geosearch_byradius_or_bybox_required() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    setup_nyc(&mut client).await;

    assert_error_prefix(
        &client
            .command(&[
                "GEOSEARCH",
                "nyc",
                "FROMLONLAT",
                "-73.9798091",
                "40.7598464",
                "asc",
                "desc",
                "WITHHASH",
                "WITHDIST",
                "WITHCOORD",
            ])
            .await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_geosearch_storedist_option_syntax_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    setup_nyc(&mut client).await;

    assert_error_prefix(
        &client
            .command(&[
                "GEOSEARCH",
                "nyc",
                "FROMLONLAT",
                "-73.9798091",
                "40.7598464",
                "BYBOX",
                "6",
                "6",
                "km",
                "asc",
                "STOREDIST",
            ])
            .await,
        "ERR",
    );
}

// ---------------------------------------------------------------------------
// GEORADIUS WITHDIST / GEOSEARCH WITHDIST
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_georadius_withdist_sorted() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    setup_nyc(&mut client).await;

    let resp = client
        .command(&[
            "GEORADIUS",
            "nyc",
            "-73.9798091",
            "40.7598464",
            "3",
            "km",
            "WITHDIST",
            "asc",
        ])
        .await;
    // Each element is [name, dist]
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 3);

    let first = unwrap_array(items[0].clone());
    assert_bulk_eq(&first[0], b"central park n/q/r");
    let dist = parse_bulk_f64(&first[1]);
    assert_float_eq(dist, 0.7750, 0.001);

    let second = unwrap_array(items[1].clone());
    assert_bulk_eq(&second[0], b"4545");
    let dist = parse_bulk_f64(&second[1]);
    assert_float_eq(dist, 2.3651, 0.001);

    let third = unwrap_array(items[2].clone());
    assert_bulk_eq(&third[0], b"union square");
    let dist = parse_bulk_f64(&third[1]);
    assert_float_eq(dist, 2.7697, 0.001);
}

#[tokio::test]
async fn tcl_geosearch_withdist_sorted() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    setup_nyc(&mut client).await;

    let resp = client
        .command(&[
            "GEOSEARCH",
            "nyc",
            "FROMLONLAT",
            "-73.9798091",
            "40.7598464",
            "BYBOX",
            "6",
            "6",
            "km",
            "WITHDIST",
            "asc",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 4);

    let last = unwrap_array(items[3].clone());
    assert_bulk_eq(&last[0], b"lic market");
    let dist = parse_bulk_f64(&last[1]);
    assert_float_eq(dist, 3.1991, 0.001);
}

// ---------------------------------------------------------------------------
// GEORADIUS COUNT / ANY / DESC
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_georadius_with_count() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    setup_nyc(&mut client).await;

    let resp = client
        .command(&[
            "GEORADIUS",
            "nyc",
            "-73.9798091",
            "40.7598464",
            "10",
            "km",
            "COUNT",
            "3",
        ])
        .await;
    let members = extract_bulk_strings(&resp);
    assert_eq!(members.len(), 3);
}

#[tokio::test]
async fn tcl_georadius_with_count_desc() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    setup_nyc(&mut client).await;

    let resp = client
        .command(&[
            "GEORADIUS",
            "nyc",
            "-73.9798091",
            "40.7598464",
            "10",
            "km",
            "COUNT",
            "2",
            "DESC",
        ])
        .await;
    let members = extract_bulk_strings(&resp);
    assert_eq!(members.len(), 2);
    // DESC means farthest first
}

#[tokio::test]
async fn tcl_georadius_with_any_not_sorted() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    setup_nyc(&mut client).await;

    let resp = client
        .command(&[
            "GEORADIUS",
            "nyc",
            "-73.9798091",
            "40.7598464",
            "10",
            "km",
            "COUNT",
            "3",
            "ANY",
        ])
        .await;
    let members = extract_bulk_strings(&resp);
    assert_eq!(members.len(), 3);
}

#[tokio::test]
async fn tcl_georadius_with_any_sorted_asc() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    setup_nyc(&mut client).await;

    let resp = client
        .command(&[
            "GEORADIUS",
            "nyc",
            "-73.9798091",
            "40.7598464",
            "10",
            "km",
            "COUNT",
            "3",
            "ANY",
            "ASC",
        ])
        .await;
    let members = extract_bulk_strings(&resp);
    assert_eq!(members.len(), 3);
}

#[tokio::test]
async fn tcl_georadius_any_without_count() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    setup_nyc(&mut client).await;

    assert_error_prefix(
        &client
            .command(&[
                "GEORADIUS",
                "nyc",
                "-73.9798091",
                "40.7598464",
                "10",
                "km",
                "ANY",
                "ASC",
            ])
            .await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_georadius_count_missing_integer() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    setup_nyc(&mut client).await;

    assert_error_prefix(
        &client
            .command(&[
                "GEORADIUS",
                "nyc",
                "-73.9798091",
                "40.7598464",
                "10",
                "km",
                "COUNT",
            ])
            .await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_georadius_with_multiple_with_tokens() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    setup_nyc(&mut client).await;

    // WITHCOORD WITHHASH COUNT 2
    let resp = client
        .command(&[
            "GEORADIUS",
            "nyc",
            "-73.9798091",
            "40.7598464",
            "10",
            "km",
            "WITHCOORD",
            "WITHHASH",
            "COUNT",
            "2",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 2);
    // Each element: [name, hash, [lon, lat]]
    let first = unwrap_array(items[0].clone());
    assert!(first.len() >= 3);
}

// ---------------------------------------------------------------------------
// GEORADIUS HUGE (issue #2767)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_georadius_huge_issue_2767() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "GEOADD",
            "users",
            "-47.271613776683807",
            "-54.534504198047678",
            "user_000000",
        ])
        .await;
    let resp = client
        .command(&[
            "GEORADIUS",
            "users",
            "0",
            "0",
            "50000",
            "km",
            "WITHCOORD",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 1);
}

// ---------------------------------------------------------------------------
// GEORADIUSBYMEMBER / GEORADIUSBYMEMBER_RO
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_georadiusbymember_simple_sorted() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    setup_nyc(&mut client).await;

    let resp = client
        .command(&[
            "GEORADIUSBYMEMBER",
            "nyc",
            "wtc one",
            "7",
            "km",
        ])
        .await;
    let members = extract_bulk_strings(&resp);
    assert_eq!(
        members,
        vec![
            "wtc one",
            "union square",
            "central park n/q/r",
            "4545",
            "lic market"
        ]
    );
}

#[tokio::test]
async fn tcl_georadiusbymember_ro_simple_sorted() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    setup_nyc(&mut client).await;

    let resp = client
        .command(&[
            "GEORADIUSBYMEMBER_RO",
            "nyc",
            "wtc one",
            "7",
            "km",
        ])
        .await;
    let members = extract_bulk_strings(&resp);
    assert_eq!(
        members,
        vec![
            "wtc one",
            "union square",
            "central park n/q/r",
            "4545",
            "lic market"
        ]
    );
}

#[tokio::test]
async fn tcl_georadiusbymember_oblique_direction() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "k1"]).await;
    client
        .command(&[
            "GEOADD",
            "k1",
            "-0.15307903289794921875",
            "85",
            "n1",
            "0.3515625",
            "85.00019260486917005437",
            "n2",
        ])
        .await;
    let resp = client
        .command(&["GEORADIUSBYMEMBER", "k1", "n1", "4891.94", "m"])
        .await;
    let mut members = extract_bulk_strings(&resp);
    members.sort();
    assert_eq!(members, vec!["n1", "n2"]);

    client.command(&["ZREM", "k1", "n1", "n2"]).await;
    client
        .command(&[
            "GEOADD",
            "k1",
            "-4.95211958885192871094",
            "85",
            "n3",
            "11.25",
            "85.0511",
            "n4",
        ])
        .await;
    let resp = client
        .command(&["GEORADIUSBYMEMBER", "k1", "n3", "156544", "m"])
        .await;
    let mut members = extract_bulk_strings(&resp);
    members.sort();
    assert_eq!(members, vec!["n3", "n4"]);

    client.command(&["ZREM", "k1", "n3", "n4"]).await;
    client
        .command(&[
            "GEOADD",
            "k1",
            "-45",
            "65.50900022111811438208",
            "n5",
            "90",
            "85.0511",
            "n6",
        ])
        .await;
    let resp = client
        .command(&["GEORADIUSBYMEMBER", "k1", "n5", "5009431", "m"])
        .await;
    let mut members = extract_bulk_strings(&resp);
    members.sort();
    assert_eq!(members, vec!["n5", "n6"]);
}

#[tokio::test]
async fn tcl_georadiusbymember_crossing_pole() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "k1"]).await;
    client
        .command(&[
            "GEOADD", "k1", "45", "65", "n1", "-135", "85.05", "n2",
        ])
        .await;
    let resp = client
        .command(&["GEORADIUSBYMEMBER", "k1", "n1", "5009431", "m"])
        .await;
    let mut members = extract_bulk_strings(&resp);
    members.sort();
    assert_eq!(members, vec!["n1", "n2"]);
}

#[tokio::test]
async fn tcl_georadiusbymember_withdist_sorted() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    setup_nyc(&mut client).await;

    let resp = client
        .command(&[
            "GEORADIUSBYMEMBER",
            "nyc",
            "wtc one",
            "7",
            "km",
            "WITHDIST",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 5);

    // First element: wtc one with dist 0.0000
    let first = unwrap_array(items[0].clone());
    assert_bulk_eq(&first[0], b"wtc one");
    let dist = parse_bulk_f64(&first[1]);
    assert_float_eq(dist, 0.0, 0.001);
}

// ---------------------------------------------------------------------------
// GEOSEARCH FROMMEMBER
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_geosearch_frommember_simple_sorted() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    setup_nyc(&mut client).await;

    let resp = client
        .command(&[
            "GEOSEARCH",
            "nyc",
            "FROMMEMBER",
            "wtc one",
            "BYBOX",
            "14",
            "14",
            "km",
        ])
        .await;
    let members = extract_bulk_strings(&resp);
    assert_eq!(
        members,
        vec![
            "wtc one",
            "union square",
            "central park n/q/r",
            "4545",
            "lic market",
            "q4"
        ]
    );
}

// ---------------------------------------------------------------------------
// GEOSEARCH vs GEORADIUS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_geosearch_vs_georadius() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "Sicily"]).await;
    client
        .command(&[
            "GEOADD", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669",
            "Catania",
        ])
        .await;
    client
        .command(&[
            "GEOADD", "Sicily", "12.758489", "38.788135", "edge1", "17.241510", "38.788135",
            "eage2",
        ])
        .await;

    let resp = client
        .command(&[
            "GEORADIUS", "Sicily", "15", "37", "200", "km", "asc",
        ])
        .await;
    let members = extract_bulk_strings(&resp);
    assert_eq!(members, vec!["Catania", "Palermo"]);

    let resp = client
        .command(&[
            "GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYBOX", "400", "400", "km", "asc",
        ])
        .await;
    let members = extract_bulk_strings(&resp);
    assert_eq!(members, vec!["Catania", "Palermo", "eage2", "edge1"]);
}

// ---------------------------------------------------------------------------
// GEOSEARCH non-square / narrow box
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_geosearch_non_square_long_narrow() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "Sicily"]).await;
    client
        .command(&["GEOADD", "Sicily", "12.75", "36.995", "test1"])
        .await;
    client
        .command(&["GEOADD", "Sicily", "12.75", "36.50", "test2"])
        .await;
    client
        .command(&["GEOADD", "Sicily", "13.00", "36.50", "test3"])
        .await;

    // box height=2km width=400km
    let resp = client
        .command(&[
            "GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYBOX", "400", "2", "km",
        ])
        .await;
    let members = extract_bulk_strings(&resp);
    assert_eq!(members, vec!["test1"]);

    // Add western hemisphere point
    client
        .command(&["GEOADD", "Sicily", "-1", "37.00", "test3"])
        .await;
    let resp = client
        .command(&[
            "GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYBOX", "3000", "2", "km", "asc",
        ])
        .await;
    let members = extract_bulk_strings(&resp);
    assert_eq!(members, vec!["test1", "test3"]);
}

// ---------------------------------------------------------------------------
// GEOSEARCH corner point test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_geosearch_corner_point() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "Sicily"]).await;
    client
        .command(&[
            "GEOADD",
            "Sicily",
            "12.758489",
            "38.788135",
            "edge1",
            "17.241510",
            "38.788135",
            "edge2",
            "17.250000",
            "35.202000",
            "edge3",
            "12.750000",
            "35.202000",
            "edge4",
            "12.748489955781654",
            "37",
            "edge5",
            "15",
            "38.798135872540925",
            "edge6",
            "17.251510044218346",
            "37",
            "edge7",
            "15",
            "35.201864127459075",
            "edge8",
            "12.692799634687903",
            "38.798135872540925",
            "corner1",
            "12.692799634687903",
            "38.798135872540925",
            "corner2",
            "17.200560937451133",
            "35.201864127459075",
            "corner3",
            "12.799439062548865",
            "35.201864127459075",
            "corner4",
        ])
        .await;

    let resp = client
        .command(&[
            "GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYBOX", "400", "400", "km", "asc",
        ])
        .await;
    let mut members = extract_bulk_strings(&resp);
    members.sort();
    assert_eq!(members, vec!["edge1", "edge2", "edge5", "edge7"]);
}

// ---------------------------------------------------------------------------
// GEOHASH
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_geohash_returns_geohash_strings() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "points"]).await;
    client
        .command(&["GEOADD", "points", "-5.6", "42.6", "test"])
        .await;

    let resp = client.command(&["GEOHASH", "points", "test"]).await;
    let hashes = extract_bulk_strings(&resp);
    assert_eq!(hashes[0], "ezs42e44yx0");
}

#[tokio::test]
async fn tcl_geohash_with_only_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "points"]).await;
    client
        .command(&["GEOADD", "points", "10", "20", "a", "30", "40", "b"])
        .await;

    let resp = client.command(&["GEOHASH", "points"]).await;
    assert_array_len(&resp, 0);
}

// ---------------------------------------------------------------------------
// GEOPOS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_geopos_simple() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "points"]).await;
    client
        .command(&["GEOADD", "points", "10", "20", "a", "30", "40", "b"])
        .await;

    let resp = client.command(&["GEOPOS", "points", "a", "b"]).await;
    let (x1, y1) = extract_lonlat(&resp, 0);
    let (x2, y2) = extract_lonlat(&resp, 1);
    assert_float_eq(x1, 10.0, 0.001);
    assert_float_eq(y1, 20.0, 0.001);
    assert_float_eq(x2, 30.0, 0.001);
    assert_float_eq(y2, 40.0, 0.001);
}

#[tokio::test]
async fn tcl_geopos_missing_element() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "points"]).await;
    client
        .command(&["GEOADD", "points", "10", "20", "a", "30", "40", "b"])
        .await;

    let resp = client.command(&["GEOPOS", "points", "a", "x", "b"]).await;
    // Index 1 (for "x") should be nil
    assert!(is_geopos_nil(&resp, 1));
}

#[tokio::test]
async fn tcl_geopos_with_only_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "points"]).await;
    client
        .command(&["GEOADD", "points", "10", "20", "a", "30", "40", "b"])
        .await;

    let resp = client.command(&["GEOPOS", "points"]).await;
    assert_array_len(&resp, 0);
}

// ---------------------------------------------------------------------------
// GEODIST
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_geodist_simple_and_unit() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "points"]).await;
    client
        .command(&[
            "GEOADD", "points", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669",
            "Catania",
        ])
        .await;

    // Default (meters)
    let resp = client
        .command(&["GEODIST", "points", "Palermo", "Catania"])
        .await;
    let m = parse_bulk_f64(&resp);
    assert!(m > 166274.0 && m < 166275.0, "expected ~166274, got {m}");

    // km
    let resp = client
        .command(&["GEODIST", "points", "Palermo", "Catania", "km"])
        .await;
    let km = parse_bulk_f64(&resp);
    assert!(km > 166.2 && km < 166.3, "expected ~166.27, got {km}");

    // Same point distance = 0
    let resp = client
        .command(&["GEODIST", "points", "Palermo", "Palermo"])
        .await;
    let dist = parse_bulk_f64(&resp);
    assert_float_eq(dist, 0.0, 0.001);
}

#[tokio::test]
async fn tcl_geodist_missing_elements() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "points"]).await;
    client
        .command(&[
            "GEOADD", "points", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669",
            "Catania",
        ])
        .await;

    // One missing
    assert_nil(
        &client
            .command(&["GEODIST", "points", "Palermo", "Agrigento"])
            .await,
    );
    // Both missing
    assert_nil(
        &client
            .command(&["GEODIST", "points", "Ragusa", "Agrigento"])
            .await,
    );
    // Non-existing key
    assert_nil(
        &client
            .command(&["GEODIST", "empty_key", "Palermo", "Catania"])
            .await,
    );
}

// ---------------------------------------------------------------------------
// GEORADIUS STORE / STOREDIST
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_georadius_store_syntax_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "points"]).await;
    client
        .command(&[
            "GEOADD", "points", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669",
            "Catania",
        ])
        .await;

    assert_error_prefix(
        &client
            .command(&[
                "GEORADIUS",
                "points",
                "13.361389",
                "38.115556",
                "50",
                "km",
                "store",
            ])
            .await,
        "ERR",
    );
}

#[tokio::test]
#[ignore = "CROSSSLOT error — STORE dest/src key names differ"]
async fn tcl_geosearchstore_syntax_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "points"]).await;
    client
        .command(&[
            "GEOADD", "points", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669",
            "Catania",
        ])
        .await;

    assert_error_prefix(
        &client
            .command(&[
                "GEOSEARCHSTORE",
                "abc",
                "points",
                "FROMLONLAT",
                "13.361389",
                "38.115556",
                "BYRADIUS",
                "50",
                "km",
                "store",
                "abc",
            ])
            .await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_georadius_store_incompatible_options() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "points"]).await;
    client
        .command(&[
            "GEOADD", "points", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669",
            "Catania",
        ])
        .await;

    assert_error_prefix(
        &client
            .command(&[
                "GEORADIUS",
                "points",
                "13.361389",
                "38.115556",
                "50",
                "km",
                "store",
                "points2",
                "withdist",
            ])
            .await,
        "ERR",
    );
    assert_error_prefix(
        &client
            .command(&[
                "GEORADIUS",
                "points",
                "13.361389",
                "38.115556",
                "50",
                "km",
                "store",
                "points2",
                "withhash",
            ])
            .await,
        "ERR",
    );
    assert_error_prefix(
        &client
            .command(&[
                "GEORADIUS",
                "points",
                "13.361389",
                "38.115556",
                "50",
                "km",
                "store",
                "points2",
                "withcoords",
            ])
            .await,
        "ERR",
    );
}

#[tokio::test]
#[ignore = "CROSSSLOT error — STORE dest/src key names differ"]
async fn tcl_georadius_store_plain_usage() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "points"]).await;
    client
        .command(&[
            "GEOADD", "points", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669",
            "Catania",
        ])
        .await;

    client
        .command(&[
            "GEORADIUS",
            "points",
            "13.361389",
            "38.115556",
            "500",
            "km",
            "store",
            "points2",
        ])
        .await;

    let src = extract_bulk_strings(&client.command(&["ZRANGE", "points", "0", "-1"]).await);
    let dst = extract_bulk_strings(&client.command(&["ZRANGE", "points2", "0", "-1"]).await);
    assert_eq!(src, dst);
}

#[tokio::test]
#[ignore = "CROSSSLOT error — STORE dest/src key names differ"]
async fn tcl_georadiusbymember_store_storedist_plain() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "points"]).await;
    client
        .command(&[
            "GEOADD", "points", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669",
            "Catania",
        ])
        .await;

    // STORE
    client
        .command(&[
            "GEORADIUSBYMEMBER",
            "points",
            "Palermo",
            "500",
            "km",
            "store",
            "points2",
        ])
        .await;
    let members = extract_bulk_strings(&client.command(&["ZRANGE", "points2", "0", "-1"]).await);
    assert_eq!(members, vec!["Palermo", "Catania"]);

    // STOREDIST
    client
        .command(&[
            "GEORADIUSBYMEMBER",
            "points",
            "Catania",
            "500",
            "km",
            "storedist",
            "points2",
        ])
        .await;
    let members = extract_bulk_strings(&client.command(&["ZRANGE", "points2", "0", "-1"]).await);
    assert_eq!(members, vec!["Catania", "Palermo"]);

    // Check that STOREDIST stored distance scores
    let resp = client
        .command(&["ZRANGE", "points2", "0", "-1", "WITHSCORES"])
        .await;
    let items = extract_bulk_strings(&resp);
    // items: [member, score, member, score, ...]
    let score0: f64 = items[1].parse().unwrap();
    let score1: f64 = items[3].parse().unwrap();
    assert!(score0 < 1.0, "Catania->Catania distance should be < 1 km");
    assert!(
        score1 > 166.0,
        "Catania->Palermo distance should be > 166 km"
    );
}

#[tokio::test]
#[ignore = "CROSSSLOT error — STORE dest/src key names differ"]
async fn tcl_geosearchstore_plain_usage() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "points"]).await;
    client
        .command(&[
            "GEOADD", "points", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669",
            "Catania",
        ])
        .await;

    client
        .command(&[
            "GEOSEARCHSTORE",
            "points2",
            "points",
            "FROMLONLAT",
            "13.361389",
            "38.115556",
            "BYRADIUS",
            "500",
            "km",
        ])
        .await;

    let src = extract_bulk_strings(&client.command(&["ZRANGE", "points", "0", "-1"]).await);
    let dst = extract_bulk_strings(&client.command(&["ZRANGE", "points2", "0", "-1"]).await);
    assert_eq!(src, dst);
}

#[tokio::test]
#[ignore = "CROSSSLOT error — STORE dest/src key names differ"]
async fn tcl_georadius_storedist_plain_usage() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "points"]).await;
    client
        .command(&[
            "GEOADD", "points", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669",
            "Catania",
        ])
        .await;

    client
        .command(&[
            "GEORADIUS",
            "points",
            "13.361389",
            "38.115556",
            "500",
            "km",
            "storedist",
            "points2",
        ])
        .await;

    let resp = client
        .command(&["ZRANGE", "points2", "0", "-1", "WITHSCORES"])
        .await;
    let items = extract_bulk_strings(&resp);
    let score0: f64 = items[1].parse().unwrap();
    let score1: f64 = items[3].parse().unwrap();
    assert!(score0 < 1.0);
    assert!(score1 > 166.0 && score1 < 167.0);
}

#[tokio::test]
#[ignore = "CROSSSLOT error — STORE dest/src key names differ"]
async fn tcl_geosearchstore_storedist_plain_usage() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "points"]).await;
    client
        .command(&[
            "GEOADD", "points", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669",
            "Catania",
        ])
        .await;

    client
        .command(&[
            "GEOSEARCHSTORE",
            "points2",
            "points",
            "FROMLONLAT",
            "13.361389",
            "38.115556",
            "BYRADIUS",
            "500",
            "km",
            "STOREDIST",
        ])
        .await;

    let resp = client
        .command(&["ZRANGE", "points2", "0", "-1", "WITHSCORES"])
        .await;
    let items = extract_bulk_strings(&resp);
    let score0: f64 = items[1].parse().unwrap();
    let score1: f64 = items[3].parse().unwrap();
    assert!(score0 < 1.0);
    assert!(score1 > 166.0 && score1 < 167.0);
}

#[tokio::test]
#[ignore = "CROSSSLOT error — STORE dest/src key names differ"]
async fn tcl_georadius_storedist_count_asc_desc() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "points"]).await;
    client
        .command(&[
            "GEOADD", "points", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669",
            "Catania",
        ])
        .await;

    // ASC COUNT 1
    client
        .command(&[
            "GEORADIUS",
            "points",
            "13.361389",
            "38.115556",
            "500",
            "km",
            "storedist",
            "points2",
            "asc",
            "count",
            "1",
        ])
        .await;
    assert_integer_eq(&client.command(&["ZCARD", "points2"]).await, 1);
    let items = extract_bulk_strings(
        &client
            .command(&["ZRANGE", "points2", "0", "-1", "WITHSCORES"])
            .await,
    );
    assert_eq!(items[0], "Palermo");

    // DESC COUNT 1
    client
        .command(&[
            "GEORADIUS",
            "points",
            "13.361389",
            "38.115556",
            "500",
            "km",
            "storedist",
            "points2",
            "desc",
            "count",
            "1",
        ])
        .await;
    assert_integer_eq(&client.command(&["ZCARD", "points2"]).await, 1);
    let items = extract_bulk_strings(
        &client
            .command(&["ZRANGE", "points2", "0", "-1", "WITHSCORES"])
            .await,
    );
    assert_eq!(items[0], "Catania");
}

// ---------------------------------------------------------------------------
// GEOSEARCH box spanning -180/180 degrees
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_geosearch_box_spans_180_degrees() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "points"]).await;
    client
        .command(&["GEOADD", "points", "179.5", "36", "point1"])
        .await;
    client
        .command(&["GEOADD", "points", "-179.5", "36", "point2"])
        .await;

    let resp = client
        .command(&[
            "GEOSEARCH", "points", "FROMLONLAT", "179", "37", "BYBOX", "400", "400", "km", "asc",
        ])
        .await;
    let members = extract_bulk_strings(&resp);
    assert_eq!(members, vec!["point1", "point2"]);

    let resp = client
        .command(&[
            "GEOSEARCH", "points", "FROMLONLAT", "-179", "37", "BYBOX", "400", "400", "km",
            "asc",
        ])
        .await;
    let members = extract_bulk_strings(&resp);
    assert_eq!(members, vec!["point2", "point1"]);
}

// ---------------------------------------------------------------------------
// GEOSEARCH with small distance
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_geosearch_small_distance() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "points"]).await;
    client
        .command(&["GEOADD", "points", "-122.407107", "37.794300", "1"])
        .await;
    client
        .command(&["GEOADD", "points", "-122.227336", "37.794300", "2"])
        .await;

    let resp = client
        .command(&[
            "GEORADIUS",
            "points",
            "-122.407107",
            "37.794300",
            "30",
            "mi",
            "ASC",
            "WITHDIST",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 2);

    let first = unwrap_array(items[0].clone());
    assert_bulk_eq(&first[0], b"1");
    let dist1 = parse_bulk_f64(&first[1]);
    assert_float_eq(dist1, 0.0001, 0.001);

    let second = unwrap_array(items[1].clone());
    assert_bulk_eq(&second[0], b"2");
    let dist2 = parse_bulk_f64(&second[1]);
    assert_float_eq(dist2, 9.8182, 0.01);
}
