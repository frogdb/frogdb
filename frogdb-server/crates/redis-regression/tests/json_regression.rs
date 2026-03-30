//! Regression tests for RedisJSON core commands.

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// JSON.SET — root-level types
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_set_object_at_root() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["JSON.SET", "doc", "$", r#"{"name":"alice","age":30}"#])
        .await;
    assert_ok(&resp);

    let resp = client.command(&["JSON.GET", "doc", "$"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    let arr = v.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["name"], "alice");
    assert_eq!(arr[0]["age"], 30);
}

#[tokio::test]
async fn json_set_array_at_root() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "arr", "$", r#"[1,2,3]"#])
            .await,
    );

    let resp = client.command(&["JSON.GET", "arr", "$"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(v, serde_json::json!([[1, 2, 3]]));
}

#[tokio::test]
async fn json_set_string_at_root() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "str", "$", r#""hello""#])
            .await,
    );

    let resp = client.command(&["JSON.GET", "str", "$"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(v, serde_json::json!(["hello"]));
}

#[tokio::test]
async fn json_set_number_at_root() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["JSON.SET", "num", "$", "42"]).await);

    let resp = client.command(&["JSON.GET", "num", "$"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(v, serde_json::json!([42]));
}

#[tokio::test]
async fn json_set_bool_at_root() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["JSON.SET", "b", "$", "true"]).await);

    let resp = client.command(&["JSON.GET", "b", "$"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(v, serde_json::json!([true]));
}

#[tokio::test]
async fn json_set_null_at_root() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["JSON.SET", "n", "$", "null"]).await);

    let resp = client.command(&["JSON.GET", "n", "$"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(v, serde_json::json!([null]));
}

#[tokio::test]
async fn json_set_nested_path() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"nested":{"key":"old"}}"#])
            .await,
    );
    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$.nested.key", r#""new""#])
            .await,
    );

    let resp = client.command(&["JSON.GET", "doc", "$.nested.key"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(v, serde_json::json!(["new"]));
}

// ---------------------------------------------------------------------------
// JSON.SET — NX / XX
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_set_nx_creates_only_when_missing() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // NX on non-existent path at root — should succeed
    let resp = client
        .command(&["JSON.SET", "doc", "$", r#"{"a":1}"#, "NX"])
        .await;
    assert_ok(&resp);

    // NX again on existing key — should return nil
    let resp = client
        .command(&["JSON.SET", "doc", "$", r#"{"a":2}"#, "NX"])
        .await;
    assert_nil(&resp);

    // Original value should remain
    let resp = client.command(&["JSON.GET", "doc", "$.a"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(v, serde_json::json!([1]));
}

#[tokio::test]
async fn json_set_xx_updates_only_when_exists() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // XX on non-existent key — should fail
    let resp = client
        .command(&["JSON.SET", "doc", "$", r#"{"a":1}"#, "XX"])
        .await;
    assert_nil(&resp);

    // Create the key
    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"a":1}"#])
            .await,
    );

    // XX now should succeed
    let resp = client
        .command(&["JSON.SET", "doc", "$", r#"{"a":2}"#, "XX"])
        .await;
    assert_ok(&resp);

    let resp = client.command(&["JSON.GET", "doc", "$.a"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(v, serde_json::json!([2]));
}

#[tokio::test]
async fn json_set_nx_xx_mutually_exclusive() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["JSON.SET", "doc", "$", r#"{"a":1}"#, "NX", "XX"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// JSON.GET
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_get_root() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"x":10}"#])
            .await,
    );

    // GET with no path defaults to $, which wraps in array
    let resp = client.command(&["JSON.GET", "doc"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    let arr = v.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["x"], 10);
}

#[tokio::test]
async fn json_get_single_path() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&[
                "JSON.SET",
                "doc",
                "$",
                r#"{"name":"bob","age":25}"#,
            ])
            .await,
    );

    let resp = client.command(&["JSON.GET", "doc", "$.name"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(v, serde_json::json!(["bob"]));
}

#[tokio::test]
async fn json_get_multiple_paths() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&[
                "JSON.SET",
                "doc",
                "$",
                r#"{"name":"carol","age":40}"#,
            ])
            .await,
    );

    let resp = client
        .command(&["JSON.GET", "doc", "$.name", "$.age"])
        .await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    // Multiple paths return an object keyed by path
    assert_eq!(v["$.name"], serde_json::json!(["carol"]));
    assert_eq!(v["$.age"], serde_json::json!([40]));
}

#[tokio::test]
async fn json_get_nonexistent_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["JSON.GET", "nosuchkey", "$"]).await;
    assert_nil(&resp);
}

#[tokio::test]
async fn json_get_formatting_options() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"a":1,"b":2}"#])
            .await,
    );

    let resp = client
        .command(&[
            "JSON.GET", "doc", "INDENT", "  ", "NEWLINE", "\n", "SPACE", " ", "$",
        ])
        .await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    // The formatted output should contain newlines and indentation
    assert!(body.contains('\n'));
    assert!(body.contains("  "));
}

// ---------------------------------------------------------------------------
// JSON.DEL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_del_root_removes_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"a":1}"#])
            .await,
    );

    let resp = client.command(&["JSON.DEL", "doc", "$"]).await;
    assert_integer_eq(&resp, 1);

    // Key should no longer exist
    let resp = client.command(&["EXISTS", "doc"]).await;
    assert_integer_eq(&resp, 0);
}

#[tokio::test]
async fn json_del_nested_path() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"a":1,"b":2}"#])
            .await,
    );

    let resp = client.command(&["JSON.DEL", "doc", "$.a"]).await;
    assert_integer_eq(&resp, 1);

    // "a" should be gone but "b" should remain
    let resp = client.command(&["JSON.GET", "doc", "$"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    let obj = &v[0];
    assert!(obj.get("a").is_none());
    assert_eq!(obj["b"], 2);
}

#[tokio::test]
async fn json_del_nonexistent_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["JSON.DEL", "nosuchkey", "$"]).await;
    assert_integer_eq(&resp, 0);
}

// ---------------------------------------------------------------------------
// JSON.MGET
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_mget_multiple_keys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Use hash tags so all keys hash to the same slot
    assert_ok(
        &client
            .command(&["JSON.SET", "{mg}k1", "$", r#"{"v":1}"#])
            .await,
    );
    assert_ok(
        &client
            .command(&["JSON.SET", "{mg}k2", "$", r#"{"v":2}"#])
            .await,
    );
    // {mg}k3 intentionally not set

    let resp = client
        .command(&["JSON.MGET", "{mg}k1", "{mg}k2", "{mg}k3", "$.v"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 3);

    // k1 and k2 should have results, k3 should be nil
    let v1: serde_json::Value =
        serde_json::from_str(std::str::from_utf8(unwrap_bulk(&items[0])).unwrap()).unwrap();
    assert_eq!(v1, serde_json::json!([1]));

    let v2: serde_json::Value =
        serde_json::from_str(std::str::from_utf8(unwrap_bulk(&items[1])).unwrap()).unwrap();
    assert_eq!(v2, serde_json::json!([2]));

    assert_nil(&items[2]);
}

// ---------------------------------------------------------------------------
// JSON.TYPE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_type_various_types() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&[
                "JSON.SET",
                "doc",
                "$",
                r#"{"obj":{},"arr":[],"str":"hi","int":42,"num":3.14,"bool":true,"nil":null}"#,
            ])
            .await,
    );

    let cases = [
        ("$.obj", "object"),
        ("$.arr", "array"),
        ("$.str", "string"),
        ("$.int", "integer"),
        ("$.num", "number"),
        ("$.bool", "boolean"),
        ("$.nil", "null"),
    ];

    for (path, expected_type) in &cases {
        let resp = client.command(&["JSON.TYPE", "doc", path]).await;
        // single_or_multi returns Bulk directly for single match
        assert_bulk_eq(&resp, expected_type.as_bytes());
    }
}

// ---------------------------------------------------------------------------
// JSON.CLEAR
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_clear_object() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"a":1,"b":2}"#])
            .await,
    );

    let resp = client.command(&["JSON.CLEAR", "doc", "$"]).await;
    assert_integer_eq(&resp, 1);

    let resp = client.command(&["JSON.GET", "doc", "$"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(v, serde_json::json!([{}]));
}

#[tokio::test]
async fn json_clear_array() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"[1,2,3]"#])
            .await,
    );

    let resp = client.command(&["JSON.CLEAR", "doc", "$"]).await;
    assert_integer_eq(&resp, 1);

    let resp = client.command(&["JSON.GET", "doc", "$"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(v, serde_json::json!([[]]));
}

#[tokio::test]
async fn json_clear_nonexistent_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["JSON.CLEAR", "nosuchkey", "$"]).await;
    assert_integer_eq(&resp, 0);
}

// ---------------------------------------------------------------------------
// JSON.TOGGLE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_toggle_boolean() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"flag":true}"#])
            .await,
    );

    // Toggle true -> false (single_or_multi returns Integer directly)
    let resp = client.command(&["JSON.TOGGLE", "doc", "$.flag"]).await;
    assert_integer_eq(&resp, 0);

    // Verify the value is now false
    let resp = client.command(&["JSON.GET", "doc", "$.flag"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(v, serde_json::json!([false]));

    // Toggle false -> true
    let resp = client.command(&["JSON.TOGGLE", "doc", "$.flag"]).await;
    assert_integer_eq(&resp, 1);

    // Verify the value is now true
    let resp = client.command(&["JSON.GET", "doc", "$.flag"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(v, serde_json::json!([true]));
}

// ---------------------------------------------------------------------------
// JSON.MERGE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_merge_partial_update() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&[
                "JSON.SET",
                "doc",
                "$",
                r#"{"name":"dan","age":20}"#,
            ])
            .await,
    );

    // Merge: update age, add new field "email"
    let resp = client
        .command(&[
            "JSON.MERGE",
            "doc",
            "$",
            r#"{"age":21,"email":"dan@example.com"}"#,
        ])
        .await;
    assert_ok(&resp);

    let resp = client.command(&["JSON.GET", "doc", "$"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    let obj = &v[0];
    assert_eq!(obj["name"], "dan");
    assert_eq!(obj["age"], 21);
    assert_eq!(obj["email"], "dan@example.com");
}

// ---------------------------------------------------------------------------
// JSON.DEBUG MEMORY
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_debug_memory() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "doc", "$", r#"{"a":1,"b":"hello"}"#])
            .await,
    );

    let resp = client
        .command(&["JSON.DEBUG", "MEMORY", "doc", "$"])
        .await;
    // single_or_multi returns Integer directly for single match
    let mem = unwrap_integer(&resp);
    assert!(mem > 0, "memory usage should be positive, got {mem}");
}

// ---------------------------------------------------------------------------
// Error cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_set_wrong_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create a regular string key
    assert_ok(&client.command(&["SET", "strkey", "value"]).await);

    // Attempt JSON.SET on a non-JSON key
    let resp = client
        .command(&["JSON.SET", "strkey", "$", r#"{"a":1}"#])
        .await;
    assert_error_prefix(&resp, "WRONGTYPE");
}

#[tokio::test]
async fn json_set_non_root_new_doc() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Setting a non-root path on a key that doesn't exist should error
    let resp = client
        .command(&["JSON.SET", "newdoc", "$.field", r#""value""#])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// Wildcard path tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_get_wildcard_object() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "wc1", "$", r#"{"a":1,"b":2,"c":3}"#])
            .await,
    );

    // $.*  should return all property values
    let resp = client.command(&["JSON.GET", "wc1", "$.*"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    let arr = v.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    // Should contain 1, 2, 3 in some order
    let mut nums: Vec<i64> = arr.iter().map(|x| x.as_i64().unwrap()).collect();
    nums.sort();
    assert_eq!(nums, vec![1, 2, 3]);
}

#[tokio::test]
async fn json_get_wildcard_array() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["JSON.SET", "wc2", "$", r#"{"arr":[10,20,30]}"#])
            .await,
    );

    let resp = client.command(&["JSON.GET", "wc2", "$.arr[*]"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    let arr = v.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0], 10);
    assert_eq!(arr[1], 20);
    assert_eq!(arr[2], 30);
}

#[tokio::test]
async fn json_get_wildcard_nested() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&[
                "JSON.SET",
                "wc3",
                "$",
                r#"{"items":[{"name":"alice"},{"name":"bob"},{"name":"carol"}]}"#,
            ])
            .await,
    );

    // $.items[*].name should return all names
    let resp = client.command(&["JSON.GET", "wc3", "$.items[*].name"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    let arr = v.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0], "alice");
    assert_eq!(arr[1], "bob");
    assert_eq!(arr[2], "carol");
}

#[tokio::test]
async fn json_set_wildcard_array() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&[
                "JSON.SET",
                "wc4",
                "$",
                r#"{"items":[{"v":1},{"v":2},{"v":3}]}"#,
            ])
            .await,
    );

    // Set all v fields to 99
    let resp = client
        .command(&["JSON.SET", "wc4", "$.items[*].v", "99"])
        .await;
    assert_ok(&resp);

    let resp = client.command(&["JSON.GET", "wc4", "$.items[*].v"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    let arr = v.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    for item in arr {
        assert_eq!(item, &serde_json::json!(99));
    }
}

// ---------------------------------------------------------------------------
// Deep nesting tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_get_deep_nesting() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&[
                "JSON.SET",
                "deep",
                "$",
                r#"{"a":{"b":{"c":{"d":{"e":"found"}}}}}"#,
            ])
            .await,
    );

    let resp = client
        .command(&["JSON.GET", "deep", "$.a.b.c.d.e"])
        .await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(v, serde_json::json!(["found"]));
}

#[tokio::test]
async fn json_del_deep_nesting() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&[
                "JSON.SET",
                "deep2",
                "$",
                r#"{"a":{"b":{"c":1},"d":2}}"#,
            ])
            .await,
    );

    // Delete deeply nested field
    let resp = client.command(&["JSON.DEL", "deep2", "$.a.b.c"]).await;
    assert_integer_eq(&resp, 1);

    // Parent structure should remain
    let resp = client.command(&["JSON.GET", "deep2", "$.a.d"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(v, serde_json::json!([2]));

    // b should now be empty
    let resp = client.command(&["JSON.GET", "deep2", "$.a.b"]).await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(v, serde_json::json!([{}]));
}

// ---------------------------------------------------------------------------
// Multi-path JSON.GET response format
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_get_multi_path_format() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&[
                "JSON.SET",
                "mp",
                "$",
                r#"{"name":"alice","age":30}"#,
            ])
            .await,
    );

    // Two paths → response is {"$.name":[vals],"$.age":[vals]}
    let resp = client
        .command(&["JSON.GET", "mp", "$.name", "$.age"])
        .await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    let obj = v.as_object().unwrap();
    assert!(obj.contains_key("$.name"), "expected $.name key in response");
    assert!(obj.contains_key("$.age"), "expected $.age key in response");
    assert_eq!(obj["$.name"], serde_json::json!(["alice"]));
    assert_eq!(obj["$.age"], serde_json::json!([30]));
}

#[tokio::test]
async fn json_get_multi_path_with_wildcard() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&[
                "JSON.SET",
                "mpw",
                "$",
                r#"{"items":[{"n":"a"},{"n":"b"}],"count":2}"#,
            ])
            .await,
    );

    let resp = client
        .command(&["JSON.GET", "mpw", "$.items[*].n", "$.count"])
        .await;
    let body = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    let obj = v.as_object().unwrap();
    assert_eq!(obj["$.count"], serde_json::json!([2]));
    let names = obj["$.items[*].n"].as_array().unwrap();
    assert_eq!(names.len(), 2);
}

// ---------------------------------------------------------------------------
// Recursive descent error
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_get_recursive_descent_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&[
                "JSON.SET",
                "rd",
                "$",
                r#"{"a":{"name":"x"},"b":{"name":"y"}}"#,
            ])
            .await,
    );

    // Recursive descent $..name is not supported
    let resp = client.command(&["JSON.GET", "rd", "$..name"]).await;
    assert_error_prefix(&resp, "ERR");
}
