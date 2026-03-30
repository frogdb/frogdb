//! Regression tests for Vector Set commands (VADD, VSIM, VCARD, VDIM, VEMB,
//! VREM, VGETATTR, VSETATTR, VINFO, VRANDMEMBER, VRANGE).

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a VADD command using the VALUES format.
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

/// Convert `&[String]` to `Vec<&str>` for passing to `client.command()`.
fn refs(args: &[String]) -> Vec<&str> {
    args.iter().map(|s| s.as_str()).collect()
}

/// Find a field value in a flat key-value VINFO array by label name.
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
    panic!("field {label:?} not found in VINFO response");
}

// ---------------------------------------------------------------------------
// VADD tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn vadd_values_format() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let cmd = vadd_values("vs1", "a", &[1.0, 0.0, 0.0]);
    let resp = client.command(&refs(&cmd)).await;
    assert_integer_eq(&resp, 1);
}

#[tokio::test]
async fn vadd_fp32_format() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let values: [f32; 3] = [1.0, 0.0, 0.0];
    let blob: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

    let cmd_str = b"VADD";
    let key = b"vs_fp32";
    let fp32 = b"FP32";
    let elem = b"a";

    let cmd_bytes = bytes::Bytes::from_static(cmd_str);
    let key_bytes = bytes::Bytes::from_static(key);
    let fp32_bytes = bytes::Bytes::from_static(fp32);
    let blob_bytes = bytes::Bytes::from(blob);
    let elem_bytes = bytes::Bytes::from_static(elem);

    let resp = client
        .command_raw(&[
            &cmd_bytes,
            &key_bytes,
            &fp32_bytes,
            &blob_bytes,
            &elem_bytes,
        ])
        .await;
    assert_integer_eq(&resp, 1);
}

#[tokio::test]
async fn vadd_update_existing() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let cmd = vadd_values("vs_upd", "a", &[1.0, 0.0, 0.0]);
    let resp = client.command(&refs(&cmd)).await;
    assert_integer_eq(&resp, 1); // new

    let cmd2 = vadd_values("vs_upd", "a", &[0.0, 1.0, 0.0]);
    let resp2 = client.command(&refs(&cmd2)).await;
    assert_integer_eq(&resp2, 0); // update
}

#[tokio::test]
async fn vadd_with_setattr() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "VADD",
            "vs_attr",
            "VALUES",
            "3",
            "1",
            "0",
            "0",
            "a",
            "SETATTR",
            r#"{"color":"red"}"#,
        ])
        .await;
    assert_integer_eq(&resp, 1);

    let resp = client.command(&["VGETATTR", "vs_attr", "a"]).await;
    let s = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    assert!(
        s.contains("red"),
        "expected attribute to contain 'red', got: {s}"
    );
}

#[tokio::test]
async fn vadd_dimension_mismatch() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create a 3-dim set
    let cmd = vadd_values("vs_dim", "a", &[1.0, 0.0, 0.0]);
    client.command(&refs(&cmd)).await;

    // Try adding a 2-dim vector
    let cmd2 = vadd_values("vs_dim", "b", &[1.0, 0.0]);
    let resp = client.command(&refs(&cmd2)).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn vadd_wrongtype() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "str_key", "hello"]).await;

    let cmd = vadd_values("str_key", "a", &[1.0, 0.0, 0.0]);
    let resp = client.command(&refs(&cmd)).await;
    assert_error_prefix(&resp, "WRONGTYPE");
}

#[tokio::test]
async fn vadd_quantization_q8() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["VADD", "vs_q8", "VALUES", "3", "1", "0", "0", "a", "Q8"])
        .await;
    assert_integer_eq(&resp, 1);

    let resp = client.command(&["VINFO", "vs_q8"]).await;
    let items = unwrap_array(resp);
    assert_bulk_eq(info_field(&items, "quant-type"), b"Q8");
}

#[tokio::test]
async fn vadd_with_m_and_ef() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "VADD", "vs_mef", "VALUES", "3", "1", "0", "0", "a", "M", "32", "EF", "100",
        ])
        .await;
    assert_integer_eq(&resp, 1);

    let resp = client.command(&["VINFO", "vs_mef"]).await;
    let items = unwrap_array(resp);
    assert_integer_eq(info_field(&items, "hnsw-m"), 32);
    assert_integer_eq(info_field(&items, "hnsw-ef-construction"), 100);
}

// ---------------------------------------------------------------------------
// VSIM tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn vsim_basic_search() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Add 3 vectors in different directions
    for (name, v) in [
        ("a", [1.0, 0.0, 0.0]),
        ("b", [0.0, 1.0, 0.0]),
        ("c", [0.9, 0.1, 0.0]),
    ] {
        let cmd = vadd_values("vs_sim", name, &v);
        client.command(&refs(&cmd)).await;
    }

    // Search near [1, 0, 0] — "a" and "c" should be closest
    let resp = client
        .command(&["VSIM", "vs_sim", "VALUES", "3", "1", "0", "0", "COUNT", "2"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 2);
    // First result should be "a" (exact match)
    assert_bulk_eq(&arr[0], b"a");
}

#[tokio::test]
async fn vsim_ele_query() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let cmd = vadd_values("vs_ele", "a", &[1.0, 0.0, 0.0]);
    client.command(&refs(&cmd)).await;
    let cmd = vadd_values("vs_ele", "b", &[0.9, 0.1, 0.0]);
    client.command(&refs(&cmd)).await;

    let resp = client
        .command(&["VSIM", "vs_ele", "ELE", "a", "COUNT", "2"])
        .await;
    let arr = unwrap_array(resp);
    assert!(!arr.is_empty());
    // "a" itself should be returned as the closest match
    assert_bulk_eq(&arr[0], b"a");
}

#[tokio::test]
async fn vsim_count_limit() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..10 {
        let v = vec![i as f32, 0.0, 0.0];
        let cmd = vadd_values("vs_cnt", &format!("e{i}"), &v);
        client.command(&refs(&cmd)).await;
    }

    let resp = client
        .command(&["VSIM", "vs_cnt", "VALUES", "3", "1", "0", "0", "COUNT", "3"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 3);
}

#[tokio::test]
async fn vsim_withscores() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let cmd = vadd_values("vs_ws", "a", &[1.0, 0.0, 0.0]);
    client.command(&refs(&cmd)).await;
    let cmd = vadd_values("vs_ws", "b", &[0.0, 1.0, 0.0]);
    client.command(&refs(&cmd)).await;

    let resp = client
        .command(&[
            "VSIM",
            "vs_ws",
            "VALUES",
            "3",
            "1",
            "0",
            "0",
            "WITHSCORES",
            "COUNT",
            "2",
        ])
        .await;
    let arr = unwrap_array(resp);
    // With scores: [name1, score1, name2, score2]
    assert_eq!(arr.len(), 4);
    assert_bulk_eq(&arr[0], b"a");
    // Score for exact match should be high (close to 1.0)
    let score: f64 = std::str::from_utf8(unwrap_bulk(&arr[1]))
        .unwrap()
        .parse()
        .unwrap();
    assert!(score > 0.9, "expected score > 0.9, got {score}");
}

#[tokio::test]
async fn vsim_withattribs() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "VADD",
            "vs_wa",
            "VALUES",
            "3",
            "1",
            "0",
            "0",
            "a",
            "SETATTR",
            r#"{"tag":"hello"}"#,
        ])
        .await;
    assert_integer_eq(&resp, 1);

    let resp = client
        .command(&[
            "VSIM",
            "vs_wa",
            "VALUES",
            "3",
            "1",
            "0",
            "0",
            "WITHATTRIBS",
            "COUNT",
            "1",
        ])
        .await;
    let arr = unwrap_array(resp);
    // With attribs: [name, attrib]
    assert_eq!(arr.len(), 2);
    assert_bulk_eq(&arr[0], b"a");
    let attrib = std::str::from_utf8(unwrap_bulk(&arr[1])).unwrap();
    assert!(
        attrib.contains("hello"),
        "expected attrib to contain 'hello', got: {attrib}"
    );
}

#[tokio::test]
async fn vsim_empty_set() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["VSIM", "nonexistent", "VALUES", "3", "1", "0", "0"])
        .await;
    let arr = unwrap_array(resp);
    assert!(arr.is_empty());
}

// ---------------------------------------------------------------------------
// VCARD / VDIM tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn vcard_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for (name, v) in [("a", [1.0, 0.0]), ("b", [0.0, 1.0]), ("c", [1.0, 1.0])] {
        let cmd = vadd_values("vs_card", name, &v);
        client.command(&refs(&cmd)).await;
    }

    let resp = client.command(&["VCARD", "vs_card"]).await;
    assert_integer_eq(&resp, 3);
}

#[tokio::test]
async fn vcard_nonexistent() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["VCARD", "nope"]).await;
    assert_integer_eq(&resp, 0);
}

#[tokio::test]
async fn vdim_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let cmd = vadd_values("vs_vdim", "a", &[1.0, 2.0, 3.0, 4.0]);
    client.command(&refs(&cmd)).await;

    let resp = client.command(&["VDIM", "vs_vdim"]).await;
    assert_integer_eq(&resp, 4);
}

#[tokio::test]
async fn vdim_nonexistent() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["VDIM", "nope"]).await;
    assert_nil(&resp);
}

// ---------------------------------------------------------------------------
// VEMB tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn vemb_values_format() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let cmd = vadd_values("vs_emb", "a", &[1.0, 2.0, 3.0]);
    client.command(&refs(&cmd)).await;

    let resp = client.command(&["VEMB", "vs_emb", "a"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 3);
    // Each element should be a float string
    let v0: f64 = std::str::from_utf8(unwrap_bulk(&arr[0]))
        .unwrap()
        .parse()
        .unwrap();
    assert!((v0 - 1.0).abs() < 0.01, "expected ~1.0, got {v0}");
}

#[tokio::test]
async fn vemb_raw_format() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let cmd = vadd_values("vs_emb_raw", "a", &[1.0, 2.0, 3.0]);
    client.command(&refs(&cmd)).await;

    let resp = client.command(&["VEMB", "vs_emb_raw", "a", "RAW"]).await;
    let raw = unwrap_bulk(&resp);
    // 3 floats × 4 bytes each = 12 bytes
    assert_eq!(raw.len(), 12);
}

#[tokio::test]
async fn vemb_nonexistent() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let cmd = vadd_values("vs_emb_ne", "a", &[1.0, 2.0]);
    client.command(&refs(&cmd)).await;

    let resp = client.command(&["VEMB", "vs_emb_ne", "no_such_elem"]).await;
    assert_nil(&resp);
}

// ---------------------------------------------------------------------------
// VREM tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn vrem_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let cmd = vadd_values("vs_rem", "a", &[1.0, 0.0]);
    client.command(&refs(&cmd)).await;
    let cmd = vadd_values("vs_rem", "b", &[0.0, 1.0]);
    client.command(&refs(&cmd)).await;

    let resp = client.command(&["VREM", "vs_rem", "a"]).await;
    assert_integer_eq(&resp, 1);

    let resp = client.command(&["VCARD", "vs_rem"]).await;
    assert_integer_eq(&resp, 1);
}

#[tokio::test]
async fn vrem_nonexistent() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["VREM", "nope", "a"]).await;
    assert_integer_eq(&resp, 0);
}

#[tokio::test]
async fn vrem_last_element_deletes_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let cmd = vadd_values("vs_rem_last", "only", &[1.0, 0.0]);
    client.command(&refs(&cmd)).await;

    let resp = client.command(&["VREM", "vs_rem_last", "only"]).await;
    assert_integer_eq(&resp, 1);

    let resp = client.command(&["EXISTS", "vs_rem_last"]).await;
    assert_integer_eq(&resp, 0);
}

// ---------------------------------------------------------------------------
// VGETATTR / VSETATTR tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn vsetattr_and_getattr() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let cmd = vadd_values("vs_ga", "a", &[1.0, 0.0]);
    client.command(&refs(&cmd)).await;

    let resp = client
        .command(&["VSETATTR", "vs_ga", "a", r#"{"age":30}"#])
        .await;
    assert_integer_eq(&resp, 1);

    let resp = client.command(&["VGETATTR", "vs_ga", "a"]).await;
    let s = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    assert!(
        s.contains("30"),
        "expected attribute to contain '30', got: {s}"
    );
}

#[tokio::test]
async fn vsetattr_update() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let cmd = vadd_values("vs_ga2", "a", &[1.0, 0.0]);
    client.command(&refs(&cmd)).await;

    client
        .command(&["VSETATTR", "vs_ga2", "a", r#"{"v":1}"#])
        .await;
    client
        .command(&["VSETATTR", "vs_ga2", "a", r#"{"v":2}"#])
        .await;

    let resp = client.command(&["VGETATTR", "vs_ga2", "a"]).await;
    let s = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    assert!(s.contains("2"), "expected updated attribute, got: {s}");
}

#[tokio::test]
async fn vsetattr_nonexistent_element() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let cmd = vadd_values("vs_ga3", "a", &[1.0, 0.0]);
    client.command(&refs(&cmd)).await;

    let resp = client
        .command(&["VSETATTR", "vs_ga3", "no_such", r#"{"x":1}"#])
        .await;
    assert_integer_eq(&resp, 0);
}

#[tokio::test]
async fn vgetattr_no_attribute() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let cmd = vadd_values("vs_ga4", "a", &[1.0, 0.0]);
    client.command(&refs(&cmd)).await;

    let resp = client.command(&["VGETATTR", "vs_ga4", "a"]).await;
    assert_nil(&resp);
}

#[tokio::test]
async fn vsetattr_invalid_json() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let cmd = vadd_values("vs_ga5", "a", &[1.0, 0.0]);
    client.command(&refs(&cmd)).await;

    let resp = client
        .command(&["VSETATTR", "vs_ga5", "a", "not json {{{"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// VINFO tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn vinfo_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["VADD", "vs_info", "VALUES", "3", "1", "0", "0", "a"])
        .await;
    assert_integer_eq(&resp, 1);

    let resp = client.command(&["VINFO", "vs_info"]).await;
    let items = unwrap_array(resp);

    assert_bulk_eq(info_field(&items, "quant-type"), b"NOQUANT");
    assert_integer_eq(info_field(&items, "vector-dim"), 3);
    assert_integer_eq(info_field(&items, "size"), 1);
    assert_integer_eq(info_field(&items, "hnsw-m"), 16); // default
    assert_integer_eq(info_field(&items, "hnsw-ef-construction"), 200); // default
}

#[tokio::test]
async fn vinfo_nonexistent() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["VINFO", "nope"]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// VRANDMEMBER tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn vrandmember_single() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for name in ["a", "b", "c"] {
        let cmd = vadd_values("vs_rand", name, &[1.0, 0.0]);
        client.command(&refs(&cmd)).await;
    }

    let resp = client.command(&["VRANDMEMBER", "vs_rand"]).await;
    // Should return a single bulk string (one of "a", "b", "c")
    let name = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    assert!(
        ["a", "b", "c"].contains(&name),
        "unexpected random member: {name}"
    );
}

#[tokio::test]
async fn vrandmember_count_positive() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for name in ["a", "b", "c", "d", "e"] {
        let cmd = vadd_values("vs_rand2", name, &[1.0, 0.0]);
        client.command(&refs(&cmd)).await;
    }

    let resp = client.command(&["VRANDMEMBER", "vs_rand2", "3"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 3);
    // All should be unique (positive count)
    let names: Vec<String> = arr
        .iter()
        .map(|r| std::str::from_utf8(unwrap_bulk(r)).unwrap().to_string())
        .collect();
    let mut dedup = names.clone();
    dedup.sort();
    dedup.dedup();
    assert_eq!(names.len(), dedup.len(), "expected unique members");
}

#[tokio::test]
async fn vrandmember_count_negative() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let cmd = vadd_values("vs_rand3", "only", &[1.0, 0.0]);
    client.command(&refs(&cmd)).await;

    // Negative count: returns |count| elements, allows duplicates
    let resp = client.command(&["VRANDMEMBER", "vs_rand3", "-5"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 5);
    // All should be "only" since that's the only element
    for item in &arr {
        assert_bulk_eq(item, b"only");
    }
}

#[tokio::test]
async fn vrandmember_empty() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["VRANDMEMBER", "nope"]).await;
    assert_nil(&resp);
}

// ---------------------------------------------------------------------------
// VRANGE tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn vrange_full_scan() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Add elements with known names for lex ordering
    for name in ["alpha", "beta", "gamma"] {
        let cmd = vadd_values("vs_range", name, &[1.0, 0.0]);
        client.command(&refs(&cmd)).await;
    }

    let resp = client
        .command(&["VRANGE", "vs_range", "0", "COUNT", "10"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 3);
    // Lexicographic order
    assert_bulk_eq(&arr[0], b"alpha");
    assert_bulk_eq(&arr[1], b"beta");
    assert_bulk_eq(&arr[2], b"gamma");
}

#[tokio::test]
async fn vrange_with_cursor() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for name in ["a", "b", "c", "d"] {
        let cmd = vadd_values("vs_range2", name, &[1.0, 0.0]);
        client.command(&refs(&cmd)).await;
    }

    // Start after "b" — should get "c" and "d"
    let resp = client
        .command(&["VRANGE", "vs_range2", "b", "COUNT", "10"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 2);
    assert_bulk_eq(&arr[0], b"c");
    assert_bulk_eq(&arr[1], b"d");
}

#[tokio::test]
async fn vrange_count_limit() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for name in ["a", "b", "c", "d"] {
        let cmd = vadd_values("vs_range3", name, &[1.0, 0.0]);
        client.command(&refs(&cmd)).await;
    }

    let resp = client
        .command(&["VRANGE", "vs_range3", "0", "COUNT", "2"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 2);
    assert_bulk_eq(&arr[0], b"a");
    assert_bulk_eq(&arr[1], b"b");
}
