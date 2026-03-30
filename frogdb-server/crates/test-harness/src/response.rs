//! Assertion and unwrapping helpers for `Response` values.
//!
//! These complement the `Option`-returning parsers in `server.rs`
//! (`parse_simple_string`, `parse_bulk_string`, etc.) with panicking
//! variants that produce clear test-failure messages.

#![allow(dead_code)]

use frogdb_protocol::Response;

/// Panics unless `response` is `Response::Simple("OK")`.
pub fn assert_ok(response: &Response) {
    assert!(
        matches!(response, Response::Simple(s) if s == "OK"),
        "expected OK, got {response:?}",
    );
}

/// Panics unless `response` is `Response::Error` whose message starts with `prefix`.
pub fn assert_error_prefix(response: &Response, prefix: &str) {
    match response {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.starts_with(prefix),
                "expected error starting with {prefix:?}, got {msg:?}",
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

/// Panics unless `response` is `Response::Array`; returns the inner vec.
pub fn unwrap_array(response: Response) -> Vec<Response> {
    match response {
        Response::Array(items) => items,
        other => panic!("expected Array, got {other:?}"),
    }
}

/// Panics unless `response` is `Response::Bulk(Some(_))`; returns the bytes.
pub fn unwrap_bulk(response: &Response) -> &[u8] {
    match response {
        Response::Bulk(Some(b)) => b,
        other => panic!("expected Bulk(Some(_)), got {other:?}"),
    }
}

/// Unwraps a bulk response and asserts it equals `expected`.
pub fn assert_bulk_eq(response: &Response, expected: &[u8]) {
    let actual = unwrap_bulk(response);
    assert_eq!(actual, expected, "bulk value mismatch");
}

/// Panics unless `response` is `Response::Integer`; returns the value.
pub fn unwrap_integer(response: &Response) -> i64 {
    match response {
        Response::Integer(n) => *n,
        other => panic!("expected Integer, got {other:?}"),
    }
}

/// Panics unless `response` is `Response::Bulk(None)`.
pub fn assert_nil(response: &Response) {
    assert!(
        matches!(response, Response::Bulk(None)),
        "expected nil, got {response:?}",
    );
}

/// Unwraps an integer response and asserts it equals `expected`.
pub fn assert_integer_eq(response: &Response, expected: i64) {
    let actual = unwrap_integer(response);
    assert_eq!(actual, expected, "integer value mismatch");
}

/// Panics unless `response` is `Response::Array` of the expected length.
pub fn assert_array_len(response: &Response, expected: usize) {
    match response {
        Response::Array(items) => assert_eq!(
            items.len(),
            expected,
            "array length mismatch: got {}, expected {expected}",
            items.len()
        ),
        other => panic!("expected Array, got {other:?}"),
    }
}

/// Extracts UTF-8 bulk strings from a `Response::Array`, skipping non-bulk items.
pub fn extract_bulk_strings(response: &Response) -> Vec<String> {
    match response {
        Response::Array(items) => items
            .iter()
            .filter_map(|item| match item {
                Response::Bulk(Some(b)) => String::from_utf8(b.to_vec()).ok(),
                _ => None,
            })
            .collect(),
        other => panic!("expected Array, got {other:?}"),
    }
}
