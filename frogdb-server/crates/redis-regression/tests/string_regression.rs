use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn set_with_get_returns_previous_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "mykey", "oldval"]).await);
    let resp = client.command(&["SET", "mykey", "newval", "GET"]).await;
    assert_bulk_eq(&resp, b"oldval");
}

#[tokio::test]
async fn set_with_get_on_nonexistent_returns_nil() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["SET", "mykey", "val", "GET"]).await;
    assert!(matches!(resp, frogdb_protocol::Response::Bulk(None)));
}

#[tokio::test]
async fn set_with_get_on_wrong_type_returns_wrongtype() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create a list key
    client.command(&["RPUSH", "{k}mykey", "a"]).await;
    let resp = client.command(&["SET", "{k}mykey", "val", "GET"]).await;
    assert_error_prefix(&resp, "WRONGTYPE");
}

#[tokio::test]
async fn substr_alias_behaves_like_getrange() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "mykey", "hello world"]).await);
    let substr_resp = client.command(&["SUBSTR", "mykey", "0", "4"]).await;
    let getrange_resp = client.command(&["GETRANGE", "mykey", "0", "4"]).await;
    assert_bulk_eq(&substr_resp, b"hello");
    assert_bulk_eq(&getrange_resp, b"hello");
}

#[tokio::test]
async fn strlen_on_integer_encoded_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "mykey", "12345"]).await);
    let resp = client.command(&["STRLEN", "mykey"]).await;
    assert_eq!(unwrap_integer(&resp), 5);

    // Negative integer
    assert_ok(&client.command(&["SET", "mykey", "-42"]).await);
    let resp = client.command(&["STRLEN", "mykey"]).await;
    assert_eq!(unwrap_integer(&resp), 3);
}

// ===========================================================================
// LCS — Longest Common Subsequence (IDX, MINMATCHLEN, WITHMATCHLEN modes)
// ===========================================================================

#[tokio::test]
async fn lcs_idx_returns_match_positions() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "{t}a", "ohmytext"]).await);
    assert_ok(&client.command(&["SET", "{t}b", "mynewtext"]).await);

    let resp = client.command(&["LCS", "{t}a", "{t}b", "IDX"]).await;
    // Should return a structured response with "matches" and "len" fields
    assert!(
        !matches!(resp, frogdb_protocol::Response::Error(_)),
        "LCS IDX should not error: {resp:?}"
    );

    let items = unwrap_array(resp);
    // Expected format: ["matches", [[...], [...]], "len", N]
    assert!(items.len() >= 4, "LCS IDX response too short: {items:?}");
}

#[tokio::test]
async fn lcs_idx_withmatchlen() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "{t}a", "ohmytext"]).await);
    assert_ok(&client.command(&["SET", "{t}b", "mynewtext"]).await);

    let resp = client
        .command(&["LCS", "{t}a", "{t}b", "IDX", "WITHMATCHLEN"])
        .await;
    assert!(
        !matches!(resp, frogdb_protocol::Response::Error(_)),
        "LCS IDX WITHMATCHLEN should not error: {resp:?}"
    );
}

#[tokio::test]
async fn lcs_idx_minmatchlen_filters_short_matches() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "{t}a", "ohmytext"]).await);
    assert_ok(&client.command(&["SET", "{t}b", "mynewtext"]).await);

    let resp = client
        .command(&["LCS", "{t}a", "{t}b", "IDX", "MINMATCHLEN", "4"])
        .await;
    assert!(
        !matches!(resp, frogdb_protocol::Response::Error(_)),
        "LCS IDX MINMATCHLEN should not error: {resp:?}"
    );

    let items = unwrap_array(resp);
    // Find "matches" array
    let mut matches_idx = None;
    for (i, item) in items.iter().enumerate() {
        if let frogdb_protocol::Response::Bulk(Some(b)) = item
            && b.as_ref() == b"matches"
        {
            matches_idx = Some(i + 1);
            break;
        }
    }
    if let Some(idx) = matches_idx {
        let matches = unwrap_array(items[idx].clone());
        // With MINMATCHLEN 4, "mytext" (len 4) might still match but shorter ones filtered
        // All returned matches should have length >= 4
        for m in &matches {
            let m_arr = match m {
                frogdb_protocol::Response::Array(a) => a,
                _ => continue,
            };
            // Each match is [[a_start, a_end], [b_start, b_end]] or with WITHMATCHLEN: [[a_start, a_end], [b_start, b_end], len]
            if m_arr.len() >= 2
                && let frogdb_protocol::Response::Array(range) = &m_arr[0]
            {
                let start = unwrap_integer(&range[0]);
                let end = unwrap_integer(&range[1]);
                let match_len = end - start + 1;
                assert!(match_len >= 4, "match length {match_len} < MINMATCHLEN 4");
            }
        }
    }
}

#[tokio::test]
async fn lcs_nonexistent_keys_returns_empty() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["LCS", "{t}nokey1", "{t}nokey2"]).await;
    // Should return empty string
    assert!(
        !matches!(resp, frogdb_protocol::Response::Error(_)),
        "LCS on nonexistent keys should not error: {resp:?}"
    );
    assert_bulk_eq(&resp, b"");
}
