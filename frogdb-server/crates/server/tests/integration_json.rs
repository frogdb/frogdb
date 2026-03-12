//! Integration tests for JSON commands (JSON.SET, JSON.GET, JSON.DEL, etc.)

use crate::common::test_server::TestServer;
use bytes::Bytes;
use frogdb_protocol::Response;

// ============================================================================
// JSON Command Tests
// ============================================================================

#[tokio::test]
async fn test_json_set_get() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // JSON.SET creates a new key
    let response = client
        .command(&["JSON.SET", "doc", "$", r#"{"name":"test","count":0}"#])
        .await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // JSON.GET retrieves the value
    let response = client.command(&["JSON.GET", "doc", "$"]).await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("test") && s.contains("count"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_set_nx_xx() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // NX - only set if doesn't exist (should succeed)
    let response = client
        .command(&["JSON.SET", "doc", "$", r#"{"value":1}"#, "NX"])
        .await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // NX - should fail because key exists
    let response = client
        .command(&["JSON.SET", "doc", "$", r#"{"value":2}"#, "NX"])
        .await;
    assert_eq!(response, Response::Bulk(None));

    // XX - only set if exists (should succeed)
    let response = client
        .command(&["JSON.SET", "doc", "$", r#"{"value":3}"#, "XX"])
        .await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // XX on new key - should fail
    let response = client
        .command(&["JSON.SET", "newdoc", "$", r#"{"value":1}"#, "XX"])
        .await;
    assert_eq!(response, Response::Bulk(None));

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_del() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set up document
    client
        .command(&["JSON.SET", "doc", "$", r#"{"a":1,"b":{"c":2},"d":[1,2,3]}"#])
        .await;

    // Delete nested path
    let response = client.command(&["JSON.DEL", "doc", "$.b.c"]).await;
    assert_eq!(response, Response::Integer(1));

    // Verify deletion
    let response = client.command(&["JSON.GET", "doc", "$.b"]).await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(!s.contains("c\":2"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    // Delete non-existent path
    let response = client.command(&["JSON.DEL", "doc", "$.nonexistent"]).await;
    assert_eq!(response, Response::Integer(0));

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "JSON.SET",
            "doc",
            "$",
            r#"{"str":"hello","num":42,"bool":true,"null":null,"arr":[1],"obj":{}}"#,
        ])
        .await;

    // String type - single path returns scalar
    let response = client.command(&["JSON.TYPE", "doc", "$.str"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("string"))));

    // Number type (integer)
    let response = client.command(&["JSON.TYPE", "doc", "$.num"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("integer"))));

    // Boolean type
    let response = client.command(&["JSON.TYPE", "doc", "$.bool"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("boolean"))));

    // Null type
    let response = client.command(&["JSON.TYPE", "doc", "$.null"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("null"))));

    // Array type
    let response = client.command(&["JSON.TYPE", "doc", "$.arr"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("array"))));

    // Object type
    let response = client.command(&["JSON.TYPE", "doc", "$.obj"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("object"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_numincrby() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"counter":10}"#])
        .await;

    // Increment by 5
    let response = client
        .command(&["JSON.NUMINCRBY", "doc", "$.counter", "5"])
        .await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("15"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    // Decrement by 3
    let response = client
        .command(&["JSON.NUMINCRBY", "doc", "$.counter", "-3"])
        .await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("12"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    // Float increment
    let response = client
        .command(&["JSON.NUMINCRBY", "doc", "$.counter", "0.5"])
        .await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("12.5"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_nummultby() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"value":10}"#])
        .await;

    // Multiply by 3
    let response = client
        .command(&["JSON.NUMMULTBY", "doc", "$.value", "3"])
        .await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("30"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    // Multiply by 0.5
    let response = client
        .command(&["JSON.NUMMULTBY", "doc", "$.value", "0.5"])
        .await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("15"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_strappend() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"greeting":"Hello"}"#])
        .await;

    // Append string - single path returns scalar
    let response = client
        .command(&["JSON.STRAPPEND", "doc", "$.greeting", r#"" World""#])
        .await;
    assert_eq!(response, Response::Integer(11)); // "Hello World" length

    // Verify
    let response = client.command(&["JSON.GET", "doc", "$.greeting"]).await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("Hello World"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_strlen() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"name":"Hello"}"#])
        .await;

    // Single path returns scalar
    let response = client.command(&["JSON.STRLEN", "doc", "$.name"]).await;
    assert_eq!(response, Response::Integer(5));

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_arrappend() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"arr":[1,2]}"#])
        .await;

    // Append values - single path returns scalar
    let response = client
        .command(&["JSON.ARRAPPEND", "doc", "$.arr", "3", "4"])
        .await;
    assert_eq!(response, Response::Integer(4)); // New length

    // Verify
    let response = client.command(&["JSON.GET", "doc", "$.arr"]).await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("[1,2,3,4]") || s.contains("1, 2, 3, 4"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_arrindex() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"arr":["a","b","c","b","d"]}"#])
        .await;

    // Find first occurrence - single path returns scalar
    let response = client
        .command(&["JSON.ARRINDEX", "doc", "$.arr", r#""b""#])
        .await;
    assert_eq!(response, Response::Integer(1));

    // Find with start index
    let response = client
        .command(&["JSON.ARRINDEX", "doc", "$.arr", r#""b""#, "2"])
        .await;
    assert_eq!(response, Response::Integer(3));

    // Not found
    let response = client
        .command(&["JSON.ARRINDEX", "doc", "$.arr", r#""z""#])
        .await;
    assert_eq!(response, Response::Integer(-1));

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_arrinsert() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"arr":[1,3,4]}"#])
        .await;

    // Insert at index 1 - single path returns scalar
    let response = client
        .command(&["JSON.ARRINSERT", "doc", "$.arr", "1", "2"])
        .await;
    assert_eq!(response, Response::Integer(4)); // New length

    // Verify
    let response = client.command(&["JSON.GET", "doc", "$.arr"]).await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("[1,2,3,4]") || s.contains("1, 2, 3, 4"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_arrlen() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"arr":[1,2,3,4,5]}"#])
        .await;

    // Single path returns scalar
    let response = client.command(&["JSON.ARRLEN", "doc", "$.arr"]).await;
    assert_eq!(response, Response::Integer(5));

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_arrpop() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"arr":[1,2,3,4,5]}"#])
        .await;

    // Pop last element (default) - single path returns scalar
    let response = client.command(&["JSON.ARRPOP", "doc", "$.arr"]).await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("5"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    // Pop at index 0
    let response = client.command(&["JSON.ARRPOP", "doc", "$.arr", "0"]).await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("1"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    // Verify remaining [2,3,4]
    let response = client.command(&["JSON.ARRLEN", "doc", "$.arr"]).await;
    assert_eq!(response, Response::Integer(3));

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_arrtrim() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"arr":[0,1,2,3,4,5,6]}"#])
        .await;

    // Trim to indices 2-4 (inclusive) - single path returns scalar
    let response = client
        .command(&["JSON.ARRTRIM", "doc", "$.arr", "2", "4"])
        .await;
    assert_eq!(response, Response::Integer(3)); // New length

    // Verify [2,3,4]
    let response = client.command(&["JSON.GET", "doc", "$.arr"]).await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("[2,3,4]") || s.contains("2, 3, 4"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_objkeys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"a":1,"b":2,"c":3}"#])
        .await;

    // Single path returns array of keys directly
    let response = client.command(&["JSON.OBJKEYS", "doc", "$"]).await;
    match response {
        Response::Array(keys) => {
            assert_eq!(keys.len(), 3);
        }
        _ => panic!("Expected array, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_objlen() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"a":1,"b":2,"c":3}"#])
        .await;

    // Single path returns scalar
    let response = client.command(&["JSON.OBJLEN", "doc", "$"]).await;
    assert_eq!(response, Response::Integer(3));

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_clear() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"obj":{"a":1},"arr":[1,2,3]}"#])
        .await;

    // Clear object
    let response = client.command(&["JSON.CLEAR", "doc", "$.obj"]).await;
    assert_eq!(response, Response::Integer(1));

    // Clear array
    let response = client.command(&["JSON.CLEAR", "doc", "$.arr"]).await;
    assert_eq!(response, Response::Integer(1));

    // Verify
    let response = client.command(&["JSON.GET", "doc", "$"]).await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            // Object should be empty, array should be empty
            assert!(s.contains("\"obj\":{}") || s.contains("\"obj\": {}"));
            assert!(s.contains("\"arr\":[]") || s.contains("\"arr\": []"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_toggle() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"active":true}"#])
        .await;

    // Toggle true -> false - single path returns scalar
    let response = client.command(&["JSON.TOGGLE", "doc", "$.active"]).await;
    assert_eq!(response, Response::Integer(0)); // false = 0

    // Toggle false -> true
    let response = client.command(&["JSON.TOGGLE", "doc", "$.active"]).await;
    assert_eq!(response, Response::Integer(1)); // true = 1

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_merge() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"a":1,"b":{"c":2}}"#])
        .await;

    // Merge with patch
    let response = client
        .command(&["JSON.MERGE", "doc", "$", r#"{"a":10,"b":{"d":4},"e":5}"#])
        .await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Verify
    let response = client.command(&["JSON.GET", "doc", "$"]).await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("\"a\":10") || s.contains("\"a\": 10"));
            assert!(s.contains("\"e\":5") || s.contains("\"e\": 5"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_mget() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set multiple documents (use hash tags to colocate keys on same shard)
    client
        .command(&["JSON.SET", "{k}doc1", "$", r#"{"name":"one"}"#])
        .await;
    client
        .command(&["JSON.SET", "{k}doc2", "$", r#"{"name":"two"}"#])
        .await;
    client
        .command(&["JSON.SET", "{k}doc3", "$", r#"{"name":"three"}"#])
        .await;

    // MGET from multiple keys
    let response = client
        .command(&["JSON.MGET", "{k}doc1", "{k}doc2", "{k}doc3", "$.name"])
        .await;
    match response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 3);
        }
        _ => panic!("Expected array, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_wrongtype() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set a string key
    client.command(&["SET", "strkey", "value"]).await;

    // Try JSON operation on string key - should fail with WRONGTYPE
    let response = client.command(&["JSON.GET", "strkey", "$"]).await;
    assert!(matches!(response, Response::Error(_)));

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_nonexistent_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // GET on nonexistent key
    let response = client.command(&["JSON.GET", "nonexistent", "$"]).await;
    assert_eq!(response, Response::Bulk(None));

    // TYPE on nonexistent key
    let response = client.command(&["JSON.TYPE", "nonexistent", "$"]).await;
    assert_eq!(response, Response::Bulk(None));

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_nested_path() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "JSON.SET",
            "doc",
            "$",
            r#"{"level1":{"level2":{"level3":{"value":42}}}}"#,
        ])
        .await;

    // Access deeply nested value
    let response = client
        .command(&["JSON.GET", "doc", "$.level1.level2.level3.value"])
        .await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("42"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    // Set nested value
    let response = client
        .command(&["JSON.SET", "doc", "$.level1.level2.level3.value", "100"])
        .await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Verify
    let response = client
        .command(&["JSON.GET", "doc", "$.level1.level2.level3.value"])
        .await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("100"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_array_wildcard() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "JSON.SET",
            "doc",
            "$",
            r#"{"items":[{"id":1},{"id":2},{"id":3}]}"#,
        ])
        .await;

    // Access all ids using wildcard
    let response = client.command(&["JSON.GET", "doc", "$.items[*].id"]).await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("1") && s.contains("2") && s.contains("3"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_get_formatting() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"a":1,"b":2}"#])
        .await;

    // With INDENT
    let response = client
        .command(&["JSON.GET", "doc", "INDENT", "  ", "$"])
        .await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            // Should have some formatting structure
            assert!(!s.is_empty());
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_type_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set JSON document
    client
        .command(&["JSON.SET", "jsonkey", "$", r#"{"value":1}"#])
        .await;

    // TYPE command should return "ReJSON-RL"
    let response = client.command(&["TYPE", "jsonkey"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("ReJSON-RL")));

    server.shutdown().await;
}

// ============================================================================
// JSON.DEBUG tests
// ============================================================================

#[tokio::test]
async fn test_json_debug_help() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["JSON.DEBUG", "HELP"]).await;
    match response {
        Response::Array(entries) => {
            assert!(!entries.is_empty(), "HELP should return entries");
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_debug_memory_root() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set up a JSON document
    client
        .command(&[
            "JSON.SET",
            "{k}doc",
            "$",
            r#"{"a":1,"b":[1,2,3],"c":"hello"}"#,
        ])
        .await;

    // JSON.DEBUG MEMORY on root path
    let response = client
        .command(&["JSON.DEBUG", "MEMORY", "{k}doc", "$"])
        .await;
    match response {
        Response::Integer(size) => {
            assert!(size > 0, "Memory size should be positive, got {}", size);
        }
        _ => panic!("Expected integer response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_debug_memory_nested_path() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "JSON.SET",
            "{k}doc",
            "$",
            r#"{"a":1,"b":[1,2,3],"c":"hello"}"#,
        ])
        .await;

    // JSON.DEBUG MEMORY on nested array
    let response = client
        .command(&["JSON.DEBUG", "MEMORY", "{k}doc", "$.b"])
        .await;
    match response {
        Response::Integer(size) => {
            assert!(size > 0, "Memory size should be positive, got {}", size);
        }
        _ => panic!("Expected integer response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_debug_memory_default_path() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "{k}doc", "$", r#"{"x":42}"#])
        .await;

    // Omit path — should default to "$"
    let response = client.command(&["JSON.DEBUG", "MEMORY", "{k}doc"]).await;
    match response {
        Response::Integer(size) => {
            assert!(size > 0, "Memory size should be positive, got {}", size);
        }
        _ => panic!("Expected integer response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_debug_memory_nonexistent_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client
        .command(&["JSON.DEBUG", "MEMORY", "{k}nokey", "$"])
        .await;
    assert_eq!(
        response,
        Response::Bulk(None),
        "Non-existent key should return null"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_debug_memory_wrong_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set a string key, not JSON
    client.command(&["SET", "{k}strkey", "hello"]).await;

    let response = client
        .command(&["JSON.DEBUG", "MEMORY", "{k}strkey", "$"])
        .await;
    match response {
        Response::Error(_) => {} // Expected WRONGTYPE error
        _ => panic!("Expected error for wrong type, got {:?}", response),
    }

    server.shutdown().await;
}
