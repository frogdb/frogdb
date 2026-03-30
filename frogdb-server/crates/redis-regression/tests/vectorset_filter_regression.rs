//! Regression tests for advanced Vector Set features: VSIM FILTER expressions,
//! TRUTH brute-force mode, REDUCE dimensionality projection, and VLINKS.

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn vadd_values(key: &str, element: &str, values: &[f32]) -> Vec<String> {
    let mut args = vec![
        "VADD".into(),
        key.into(),
        "VALUES".into(),
        values.len().to_string(),
    ];
    args.extend(values.iter().map(|v| v.to_string()));
    args.push(element.into());
    args
}

fn refs(args: &[String]) -> Vec<&str> {
    args.iter().map(|s| s.as_str()).collect()
}

/// Add an element with attributes in a single VADD call.
fn vadd_with_attr(key: &str, element: &str, values: &[f32], attr_json: &str) -> Vec<String> {
    let mut args = vadd_values(key, element, values);
    args.push("SETATTR".into());
    args.push(attr_json.into());
    args
}

// ---------------------------------------------------------------------------
// VSIM FILTER tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn vsim_filter_numeric_eq() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let cmd = vadd_with_attr("vf1", "a", &[1.0, 0.0, 0.0], r#"{"age":30}"#);
    client.command(&refs(&cmd)).await;
    let cmd = vadd_with_attr("vf1", "b", &[0.9, 0.1, 0.0], r#"{"age":25}"#);
    client.command(&refs(&cmd)).await;
    let cmd = vadd_with_attr("vf1", "c", &[0.8, 0.2, 0.0], r#"{"age":30}"#);
    client.command(&refs(&cmd)).await;

    let resp = client
        .command(&[
            "VSIM",
            "vf1",
            "VALUES",
            "3",
            "1",
            "0",
            "0",
            "FILTER",
            "age == 30",
            "COUNT",
            "10",
        ])
        .await;
    let arr = unwrap_array(resp);
    // Only "a" and "c" have age==30
    assert_eq!(arr.len(), 2);
    let names: Vec<&str> = arr
        .iter()
        .map(|r| std::str::from_utf8(unwrap_bulk(r)).unwrap())
        .collect();
    assert!(names.contains(&"a"));
    assert!(names.contains(&"c"));
}

#[tokio::test]
async fn vsim_filter_numeric_gt() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let cmd = vadd_with_attr("vf2", "young", &[1.0, 0.0, 0.0], r#"{"age":20}"#);
    client.command(&refs(&cmd)).await;
    let cmd = vadd_with_attr("vf2", "old", &[0.9, 0.1, 0.0], r#"{"age":40}"#);
    client.command(&refs(&cmd)).await;

    let resp = client
        .command(&[
            "VSIM", "vf2", "VALUES", "3", "1", "0", "0", "FILTER", "age > 25", "COUNT", "10",
        ])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 1);
    assert_bulk_eq(&arr[0], b"old");
}

#[tokio::test]
async fn vsim_filter_string_eq() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let cmd = vadd_with_attr("vf3", "a", &[1.0, 0.0, 0.0], r#"{"status":"active"}"#);
    client.command(&refs(&cmd)).await;
    let cmd = vadd_with_attr("vf3", "b", &[0.9, 0.1, 0.0], r#"{"status":"inactive"}"#);
    client.command(&refs(&cmd)).await;

    let resp = client
        .command(&[
            "VSIM",
            "vf3",
            "VALUES",
            "3",
            "1",
            "0",
            "0",
            "FILTER",
            "status == 'active'",
            "COUNT",
            "10",
        ])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 1);
    assert_bulk_eq(&arr[0], b"a");
}

#[tokio::test]
async fn vsim_filter_and() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let cmd = vadd_with_attr(
        "vf4",
        "a",
        &[1.0, 0.0, 0.0],
        r#"{"age":30,"status":"active"}"#,
    );
    client.command(&refs(&cmd)).await;
    let cmd = vadd_with_attr(
        "vf4",
        "b",
        &[0.9, 0.1, 0.0],
        r#"{"age":30,"status":"inactive"}"#,
    );
    client.command(&refs(&cmd)).await;
    let cmd = vadd_with_attr(
        "vf4",
        "c",
        &[0.8, 0.2, 0.0],
        r#"{"age":20,"status":"active"}"#,
    );
    client.command(&refs(&cmd)).await;

    let resp = client
        .command(&[
            "VSIM",
            "vf4",
            "VALUES",
            "3",
            "1",
            "0",
            "0",
            "FILTER",
            "age > 25 && status == 'active'",
            "COUNT",
            "10",
        ])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 1);
    assert_bulk_eq(&arr[0], b"a");
}

#[tokio::test]
async fn vsim_filter_or() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let cmd = vadd_with_attr("vf5", "a", &[1.0, 0.0, 0.0], r#"{"x":15,"y":5}"#);
    client.command(&refs(&cmd)).await;
    let cmd = vadd_with_attr("vf5", "b", &[0.9, 0.1, 0.0], r#"{"x":5,"y":15}"#);
    client.command(&refs(&cmd)).await;
    let cmd = vadd_with_attr("vf5", "c", &[0.8, 0.2, 0.0], r#"{"x":5,"y":5}"#);
    client.command(&refs(&cmd)).await;

    let resp = client
        .command(&[
            "VSIM",
            "vf5",
            "VALUES",
            "3",
            "1",
            "0",
            "0",
            "FILTER",
            "x > 10 || y > 10",
            "COUNT",
            "10",
        ])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 2);
    let names: Vec<&str> = arr
        .iter()
        .map(|r| std::str::from_utf8(unwrap_bulk(r)).unwrap())
        .collect();
    assert!(names.contains(&"a"));
    assert!(names.contains(&"b"));
}

#[tokio::test]
async fn vsim_filter_not() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let cmd = vadd_with_attr("vf6", "a", &[1.0, 0.0, 0.0], r#"{"archived":false}"#);
    client.command(&refs(&cmd)).await;
    let cmd = vadd_with_attr("vf6", "b", &[0.9, 0.1, 0.0], r#"{"archived":true}"#);
    client.command(&refs(&cmd)).await;

    let resp = client
        .command(&[
            "VSIM",
            "vf6",
            "VALUES",
            "3",
            "1",
            "0",
            "0",
            "FILTER",
            "!(archived == true)",
            "COUNT",
            "10",
        ])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 1);
    assert_bulk_eq(&arr[0], b"a");
}

#[tokio::test]
async fn vsim_filter_nested_field() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let cmd = vadd_with_attr("vf7", "a", &[1.0, 0.0, 0.0], r#"{"meta":{"score":0.9}}"#);
    client.command(&refs(&cmd)).await;
    let cmd = vadd_with_attr("vf7", "b", &[0.9, 0.1, 0.0], r#"{"meta":{"score":0.3}}"#);
    client.command(&refs(&cmd)).await;

    let resp = client
        .command(&[
            "VSIM",
            "vf7",
            "VALUES",
            "3",
            "1",
            "0",
            "0",
            "FILTER",
            "meta.score > 0.5",
            "COUNT",
            "10",
        ])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 1);
    assert_bulk_eq(&arr[0], b"a");
}

#[tokio::test]
async fn vsim_filter_no_matches() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let cmd = vadd_with_attr("vf8", "a", &[1.0, 0.0, 0.0], r#"{"x":1}"#);
    client.command(&refs(&cmd)).await;

    let resp = client
        .command(&[
            "VSIM", "vf8", "VALUES", "3", "1", "0", "0", "FILTER", "x > 100", "COUNT", "10",
        ])
        .await;
    let arr = unwrap_array(resp);
    assert!(arr.is_empty());
}

// ---------------------------------------------------------------------------
// VSIM TRUTH (brute-force) tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn vsim_truth_mode() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Add vectors in known directions
    let cmd = vadd_values("vt1", "exact", &[1.0, 0.0, 0.0]);
    client.command(&refs(&cmd)).await;
    let cmd = vadd_values("vt1", "close", &[0.9, 0.1, 0.0]);
    client.command(&refs(&cmd)).await;
    let cmd = vadd_values("vt1", "far", &[0.0, 0.0, 1.0]);
    client.command(&refs(&cmd)).await;

    let resp = client
        .command(&[
            "VSIM", "vt1", "VALUES", "3", "1", "0", "0", "TRUTH", "COUNT", "3",
        ])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 3);
    // Brute-force should return exact nearest neighbor first
    assert_bulk_eq(&arr[0], b"exact");
    assert_bulk_eq(&arr[1], b"close");
    assert_bulk_eq(&arr[2], b"far");
}

#[tokio::test]
async fn vsim_truth_vs_hnsw() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Add enough vectors to make HNSW non-trivial
    for i in 0..20 {
        let angle = (i as f32) * std::f32::consts::PI / 10.0;
        let cmd = vadd_values("vt2", &format!("e{i}"), &[angle.cos(), angle.sin(), 0.0]);
        client.command(&refs(&cmd)).await;
    }

    // Truth result
    let resp_truth = client
        .command(&[
            "VSIM", "vt2", "VALUES", "3", "1", "0", "0", "TRUTH", "COUNT", "3",
        ])
        .await;
    let truth_arr = unwrap_array(resp_truth);

    // HNSW result
    let resp_hnsw = client
        .command(&["VSIM", "vt2", "VALUES", "3", "1", "0", "0", "COUNT", "3"])
        .await;
    let hnsw_arr = unwrap_array(resp_hnsw);

    // Both should return the same top-1 result (e0, the [1,0,0] vector)
    assert_eq!(truth_arr.len(), 3);
    assert_eq!(hnsw_arr.len(), 3);
    // The exact nearest neighbor should match
    assert_bulk_eq(&truth_arr[0], unwrap_bulk(&hnsw_arr[0]));
}

// ---------------------------------------------------------------------------
// VADD REDUCE tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn vadd_reduce_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Add a 6-dim vector, reduce to 3 stored dims
    let resp = client
        .command(&[
            "VADD", "vs_red", "REDUCE", "3", "VALUES", "6", "1", "0", "0", "0", "0", "0", "a",
        ])
        .await;
    assert_integer_eq(&resp, 1);

    // Stored dimension should be 3 (the REDUCE target)
    let resp = client.command(&["VINFO", "vs_red"]).await;
    let items = unwrap_array(resp);
    assert_integer_eq(info_field(&items, "vector-dim"), 3);
}

#[tokio::test]
async fn vadd_reduce_vdim_reports_original() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "VADD", "vs_red2", "REDUCE", "3", "VALUES", "6", "1", "0", "0", "0", "0", "0", "a",
        ])
        .await;
    assert_integer_eq(&resp, 1);

    // VDIM should return the original (input) dimension
    let resp = client.command(&["VDIM", "vs_red2"]).await;
    assert_integer_eq(&resp, 6);
}

#[tokio::test]
async fn vsim_reduce_query() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Add vectors with REDUCE
    let resp = client
        .command(&[
            "VADD", "vs_red3", "REDUCE", "3", "VALUES", "6", "1", "0", "0", "0", "0", "0", "a",
        ])
        .await;
    assert_integer_eq(&resp, 1);

    let resp = client
        .command(&[
            "VADD", "vs_red3", "REDUCE", "3", "VALUES", "6", "0", "1", "0", "0", "0", "0", "b",
        ])
        .await;
    assert_integer_eq(&resp, 1);

    // Query in the original 6-dim space
    let resp = client
        .command(&[
            "VSIM", "vs_red3", "VALUES", "6", "1", "0", "0", "0", "0", "0", "COUNT", "2",
        ])
        .await;
    let arr = unwrap_array(resp);
    assert!(!arr.is_empty());
    // "a" should be the closest match
    assert_bulk_eq(&arr[0], b"a");
}

// ---------------------------------------------------------------------------
// VLINKS tests
// ---------------------------------------------------------------------------

fn info_field<'a>(
    items: &'a [frogdb_protocol::Response],
    label: &str,
) -> &'a frogdb_protocol::Response {
    for i in (0..items.len()).step_by(2) {
        if let frogdb_protocol::Response::Bulk(Some(b)) = &items[i]
            && std::str::from_utf8(b).unwrap() == label
        {
            return &items[i + 1];
        }
        if let frogdb_protocol::Response::Simple(s) = &items[i]
            && s == label
        {
            return &items[i + 1];
        }
    }
    panic!("field {label:?} not found in INFO response");
}

#[tokio::test]
async fn vlinks_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Add several elements so HNSW has neighbors to return
    for i in 0..5 {
        let v = vec![(i as f32) * 0.2, 1.0 - (i as f32) * 0.2, 0.0];
        let cmd = vadd_values("vs_links", &format!("e{i}"), &v);
        client.command(&refs(&cmd)).await;
    }

    let resp = client.command(&["VLINKS", "vs_links", "e0"]).await;
    let arr = unwrap_array(resp);
    // Should have at least one neighbor
    assert!(!arr.is_empty(), "expected at least one link");
}

#[tokio::test]
async fn vlinks_nonexistent() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let cmd = vadd_values("vs_links2", "a", &[1.0, 0.0]);
    client.command(&refs(&cmd)).await;

    let resp = client.command(&["VLINKS", "vs_links2", "no_such"]).await;
    assert_nil(&resp);
}
