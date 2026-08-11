use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn sscan_full_iteration_returns_all_members() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Add members to a set
    for i in 0..20 {
        client
            .command(&["SADD", "myset", &format!("member{i}")])
            .await;
    }

    // Iterate through full scan
    let mut cursor = "0".to_string();
    let mut all_members = Vec::new();
    loop {
        let resp = client.command(&["SSCAN", "myset", &cursor]).await;
        let arr = unwrap_array(resp);
        assert_eq!(arr.len(), 2);

        cursor = String::from_utf8(unwrap_bulk(&arr[0]).to_vec()).unwrap();
        let members = extract_bulk_strings(&arr[1]);
        all_members.extend(members);

        if cursor == "0" {
            break;
        }
    }

    all_members.sort();
    all_members.dedup();
    assert_eq!(all_members.len(), 20);
}

#[tokio::test]
async fn hscan_full_iteration_returns_all_fields() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..20 {
        client
            .command(&["HSET", "myhash", &format!("field{i}"), &format!("val{i}")])
            .await;
    }

    let mut cursor = "0".to_string();
    let mut all_fields = Vec::new();
    loop {
        let resp = client.command(&["HSCAN", "myhash", &cursor]).await;
        let arr = unwrap_array(resp);
        assert_eq!(arr.len(), 2);

        cursor = String::from_utf8(unwrap_bulk(&arr[0]).to_vec()).unwrap();
        let field_vals = extract_bulk_strings(&arr[1]);
        // HSCAN returns field,value pairs — collect fields (even indices)
        for (i, s) in field_vals.iter().enumerate() {
            if i % 2 == 0 {
                all_fields.push(s.clone());
            }
        }

        if cursor == "0" {
            break;
        }
    }

    all_fields.sort();
    all_fields.dedup();
    assert_eq!(all_fields.len(), 20);
}

#[tokio::test]
async fn zscan_full_iteration_returns_all_members() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..20 {
        client
            .command(&["ZADD", "myzset", &format!("{i}"), &format!("member{i}")])
            .await;
    }

    let mut cursor = "0".to_string();
    let mut all_members = Vec::new();
    loop {
        let resp = client.command(&["ZSCAN", "myzset", &cursor]).await;
        let arr = unwrap_array(resp);
        assert_eq!(arr.len(), 2);

        cursor = String::from_utf8(unwrap_bulk(&arr[0]).to_vec()).unwrap();
        let member_scores = extract_bulk_strings(&arr[1]);
        // ZSCAN returns member,score pairs — collect members (even indices)
        for (i, s) in member_scores.iter().enumerate() {
            if i % 2 == 0 {
                all_members.push(s.clone());
            }
        }

        if cursor == "0" {
            break;
        }
    }

    all_members.sort();
    all_members.dedup();
    assert_eq!(all_members.len(), 20);
}

// ---------------------------------------------------------------------------
// MATCH pattern semantics for the per-key SCAN variants.
//
// Regression: HSCAN/SSCAN/ZSCAN used to filter through a bespoke matcher that
// understood only `*` and `?`. That diverged from SCAN/KEYS (which have always
// used the canonical Redis glob) and, having no cap on `*` groups, handed
// clients a cheap way to burn server CPU. All four now share
// `frogdb_core::glob_match`.
// ---------------------------------------------------------------------------

/// Drive an HSCAN to completion with `MATCH pattern`, returning the fields.
async fn hscan_fields(
    client: &mut frogdb_test_harness::server::TestClient,
    key: &str,
    pattern: &str,
) -> Vec<String> {
    let mut cursor = "0".to_string();
    let mut fields = Vec::new();
    loop {
        let resp = client
            .command(&["HSCAN", key, &cursor, "MATCH", pattern, "COUNT", "100"])
            .await;
        let arr = unwrap_array(resp);
        assert_eq!(arr.len(), 2);
        cursor = String::from_utf8(unwrap_bulk(&arr[0]).to_vec()).unwrap();
        let field_vals = extract_bulk_strings(&arr[1]);
        // HSCAN returns field,value pairs — collect fields (even indices)
        for (i, s) in field_vals.iter().enumerate() {
            if i % 2 == 0 {
                fields.push(s.clone());
            }
        }
        if cursor == "0" {
            break;
        }
    }
    fields.sort();
    fields
}

#[tokio::test]
async fn hscan_match_supports_bracket_classes() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for f in ["f1", "f2", "f3", "fx"] {
        client.command(&["HSET", "myhash", f, "v"]).await;
    }

    assert_eq!(
        hscan_fields(&mut client, "myhash", "f[12]").await,
        vec!["f1".to_string(), "f2".to_string()]
    );
    assert_eq!(
        hscan_fields(&mut client, "myhash", "f[^123]").await,
        vec!["fx".to_string()]
    );
    assert_eq!(
        hscan_fields(&mut client, "myhash", "f[1-3]").await,
        vec!["f1".to_string(), "f2".to_string(), "f3".to_string()]
    );
}

#[tokio::test]
async fn hscan_match_supports_backslash_escape() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for f in ["f*", "fx", "fy"] {
        client.command(&["HSET", "myhash", f, "v"]).await;
    }

    // `\*` is a literal star, so only the field actually named `f*` matches.
    assert_eq!(
        hscan_fields(&mut client, "myhash", r"f\*").await,
        vec!["f*".to_string()]
    );
    // An unescaped `*` still matches everything.
    assert_eq!(
        hscan_fields(&mut client, "myhash", "f*").await,
        vec!["f*".to_string(), "fx".to_string(), "fy".to_string()]
    );
}

#[tokio::test]
async fn hscan_match_star_and_question_unchanged() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for f in ["field1", "field2", "other"] {
        client.command(&["HSET", "myhash", f, "v"]).await;
    }

    assert_eq!(
        hscan_fields(&mut client, "myhash", "field*").await,
        vec!["field1".to_string(), "field2".to_string()]
    );
    assert_eq!(
        hscan_fields(&mut client, "myhash", "field?").await,
        vec!["field1".to_string(), "field2".to_string()]
    );
    assert_eq!(
        hscan_fields(&mut client, "myhash", "*").await,
        vec![
            "field1".to_string(),
            "field2".to_string(),
            "other".to_string()
        ]
    );
    assert!(
        hscan_fields(&mut client, "myhash", "nope*")
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn hscan_match_pathological_pattern_is_bounded() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Long field so the matcher's `*`-group cap (100) is genuinely reached.
    let field = "a".repeat(200);
    client.command(&["HSET", "myhash", &field, "v"]).await;

    // Exponential-backtracking bait for a naive recursive matcher.
    let evil = format!("{}b", "a*".repeat(50));
    assert!(hscan_fields(&mut client, "myhash", &evil).await.is_empty());

    // Far beyond MAX_STAR_COUNT: the matcher bails instead of grinding. What
    // this asserts is that the call returns at all.
    let many_stars = "*?".repeat(500);
    assert!(
        hscan_fields(&mut client, "myhash", &many_stars)
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn sscan_and_zscan_match_support_bracket_classes() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for m in ["m1", "m2", "m3"] {
        client.command(&["SADD", "myset", m]).await;
        client.command(&["ZADD", "myzset", "1", m]).await;
    }

    let resp = client
        .command(&["SSCAN", "myset", "0", "MATCH", "m[12]", "COUNT", "100"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(
        String::from_utf8(unwrap_bulk(&arr[0]).to_vec()).unwrap(),
        "0"
    );
    let mut members = extract_bulk_strings(&arr[1]);
    members.sort();
    assert_eq!(members, vec!["m1".to_string(), "m2".to_string()]);

    let resp = client
        .command(&["ZSCAN", "myzset", "0", "MATCH", "m[12]", "COUNT", "100"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(
        String::from_utf8(unwrap_bulk(&arr[0]).to_vec()).unwrap(),
        "0"
    );
    let member_scores = extract_bulk_strings(&arr[1]);
    // ZSCAN returns member,score pairs — collect members (even indices)
    let mut members: Vec<String> = member_scores
        .iter()
        .enumerate()
        .filter(|(i, _)| i % 2 == 0)
        .map(|(_, s)| s.clone())
        .collect();
    members.sort();
    assert_eq!(members, vec!["m1".to_string(), "m2".to_string()]);
}
