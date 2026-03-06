//! Property-based tests for JSON parsing and operations.
//!
//! Tests that JSON operations never panic on arbitrary input and
//! maintain invariants.

use proptest::prelude::*;
use serde_json::json;

use frogdb_core::{JsonError, JsonLimits, JsonValue};

/// Configuration for proptest - run more cases than default for fuzzing
fn config() -> ProptestConfig {
    ProptestConfig::with_cases(1000)
}

proptest! {
    #![proptest_config(config())]

    /// JsonValue::parse should never panic on arbitrary bytes.
    #[test]
    fn json_parse_never_panics(data: Vec<u8>) {
        let _: Result<JsonValue, JsonError> = JsonValue::parse(&data);
    }

    /// JsonValue::parse_with_limits should never panic on arbitrary bytes.
    #[test]
    fn json_parse_with_limits_never_panics(data: Vec<u8>) {
        let limits = JsonLimits {
            max_depth: 128,
            max_size: 64 * 1024 * 1024,
        };
        let _: Result<JsonValue, JsonError> = JsonValue::parse_with_limits(&data, &limits);
    }

    /// Valid JSON string should parse and roundtrip.
    #[test]
    fn valid_json_string_roundtrip(s in "[a-zA-Z0-9 ]{0,100}") {
        let json_str = format!(r#""{}""#, s);
        let result = JsonValue::parse(json_str.as_bytes());
        prop_assert!(result.is_ok());
    }

    /// Valid JSON number should parse.
    #[test]
    fn valid_json_integer_parses(n: i64) {
        let json_str = format!("{}", n);
        let result = JsonValue::parse(json_str.as_bytes());
        prop_assert!(result.is_ok());
    }

    /// Valid JSON float should parse.
    #[test]
    fn valid_json_float_parses(n in -1e10f64..1e10f64) {
        if n.is_finite() {
            let json_str = format!("{}", n);
            let result = JsonValue::parse(json_str.as_bytes());
            prop_assert!(result.is_ok());
        }
    }

    /// Valid JSON boolean should parse.
    #[test]
    fn valid_json_boolean_parses(b: bool) {
        let json_str = if b { "true" } else { "false" };
        let result = JsonValue::parse(json_str.as_bytes());
        prop_assert!(result.is_ok());
    }

    /// JSON null should parse.
    #[test]
    fn json_null_parses(_dummy: bool) {
        let result = JsonValue::parse(b"null");
        prop_assert!(result.is_ok());
    }

    /// Valid JSON object should parse.
    #[test]
    fn valid_json_object_parses(key in "[a-zA-Z][a-zA-Z0-9]{0,20}", value: i32) {
        let json_str = format!(r#"{{"{}": {}}}"#, key, value);
        let result = JsonValue::parse(json_str.as_bytes());
        prop_assert!(result.is_ok());
    }

    /// Valid JSON array should parse.
    #[test]
    fn valid_json_array_parses(values in prop::collection::vec(any::<i32>(), 0..10)) {
        let elements: Vec<String> = values.iter().map(|v| v.to_string()).collect();
        let json_str = format!("[{}]", elements.join(","));
        let result = JsonValue::parse(json_str.as_bytes());
        prop_assert!(result.is_ok());
    }

    /// get() should never panic on arbitrary path.
    #[test]
    fn json_get_never_panics(path in "\\$[a-z\\.\\[\\]0-9\\*]{0,50}") {
        let json = JsonValue::parse(br#"{"a":{"b":[1,2,3]}}"#).unwrap();
        let _ = json.get(&path);
    }

    /// type_at() should never panic on arbitrary path.
    #[test]
    fn json_type_at_never_panics(path in "\\$[a-z\\.\\[\\]0-9\\*]{0,50}") {
        let json = JsonValue::parse(br#"{"a":{"b":[1,2,3]}}"#).unwrap();
        let _ = json.type_at(&path);
    }

    /// memory_size() should return consistent values.
    #[test]
    fn memory_size_is_consistent(n: i64) {
        let json = JsonValue::parse(format!("{}", n).as_bytes()).unwrap();
        let size1 = json.memory_size();
        let size2 = json.memory_size();
        prop_assert_eq!(size1, size2);
    }

    /// to_bytes() should produce valid JSON that can be re-parsed.
    #[test]
    fn to_bytes_produces_valid_json(key in "[a-zA-Z][a-zA-Z0-9]{0,10}", value: i32) {
        let json_str = format!(r#"{{"{}": {}}}"#, key, value);
        let json = JsonValue::parse(json_str.as_bytes()).unwrap();
        let bytes = json.to_bytes();
        let reparsed = JsonValue::parse(&bytes);
        prop_assert!(reparsed.is_ok());
    }

    /// set() should work for valid paths.
    #[test]
    fn json_set_at_root(value: i32) {
        let mut json = JsonValue::parse(br#"{"a":1}"#).unwrap();
        let new_value = json!(value);
        let result = json.set("$", new_value.clone(), false, false);
        prop_assert!(result.is_ok());
    }

    /// delete() should never panic.
    #[test]
    fn json_delete_never_panics(path in "\\$[a-z\\.\\[\\]0-9]{0,30}") {
        let mut json = JsonValue::parse(br#"{"a":{"b":{"c":1}}}"#).unwrap();
        let _ = json.delete(&path);
    }

    /// num_incr_by() on non-number should return error.
    #[test]
    fn num_incr_by_on_string_handles_gracefully(incr in -100.0f64..100.0) {
        let mut json = JsonValue::parse(br#"{"str":"hello"}"#).unwrap();
        let result = json.num_incr_by("$.str", incr);
        // Should error because type mismatch
        prop_assert!(result.is_err(), "num_incr_by on string should fail");
    }

    /// arr_len() should return correct length.
    #[test]
    fn arr_len_returns_correct_length(len in 0usize..20) {
        let arr: Vec<i32> = (0..len as i32).collect();
        let json_str = format!(r#"{{"arr":{}}}"#, serde_json::to_string(&arr).unwrap());
        let json = JsonValue::parse(json_str.as_bytes()).unwrap();
        let result = json.arr_len("$.arr");
        match result {
            Ok(lengths) => {
                prop_assert_eq!(lengths.len(), 1);
                prop_assert_eq!(lengths[0], Some(len));
            }
            Err(_) => prop_assert!(false, "arr_len should succeed"),
        }
    }

    /// obj_len() should return correct length.
    #[test]
    fn obj_len_returns_correct_length(num_keys in 0usize..10) {
        let mut obj = serde_json::Map::new();
        for i in 0..num_keys {
            obj.insert(format!("k{}", i), json!(i));
        }
        let json_str = format!(r#"{{"obj":{}}}"#, serde_json::Value::Object(obj));
        let json = JsonValue::parse(json_str.as_bytes()).unwrap();
        let result = json.obj_len("$.obj");
        match result {
            Ok(lengths) => {
                prop_assert_eq!(lengths.len(), 1);
                prop_assert_eq!(lengths[0], Some(num_keys));
            }
            Err(_) => prop_assert!(false, "obj_len should succeed"),
        }
    }

    /// Invalid UTF-8 should return parse error, not panic.
    #[test]
    fn invalid_utf8_returns_error(bytes in prop::collection::vec(128u8..=255u8, 1..20)) {
        let result = JsonValue::parse(&bytes);
        prop_assert!(result.is_err());
    }

    /// Empty input should return error.
    #[test]
    fn empty_input_returns_error(_dummy: bool) {
        let result = JsonValue::parse(b"");
        prop_assert!(result.is_err());
    }

    /// Depth limit should be enforced.
    #[test]
    fn depth_limit_enforced(depth in 2usize..10) {
        // Create deeply nested JSON: {"a":{"a":{"a":...}}}
        let mut json_str = "1".to_string();
        for _ in 0..depth {
            json_str = format!(r#"{{"a":{}}}"#, json_str);
        }

        let limits = JsonLimits {
            max_depth: depth - 1, // One less than actual depth
            max_size: 64 * 1024 * 1024,
        };
        let result = JsonValue::parse_with_limits(json_str.as_bytes(), &limits);
        prop_assert!(result.is_err());
    }

    /// Size limit should be enforced.
    #[test]
    fn size_limit_enforced(size in 10usize..100) {
        // Create JSON larger than limit
        let json_str = format!(r#""{}""#, "a".repeat(size + 10));
        let limits = JsonLimits {
            max_depth: 128,
            max_size: size,
        };
        let result = JsonValue::parse_with_limits(json_str.as_bytes(), &limits);
        prop_assert!(result.is_err());
    }

    /// merge() should not panic on arbitrary valid JSON.
    #[test]
    fn merge_never_panics(key1 in "[a-z]{1,5}", val1: i32, key2 in "[a-z]{1,5}", val2: i32) {
        let mut json = JsonValue::parse(format!(r#"{{"{}":{}}}"#, key1, val1).as_bytes()).unwrap();
        let patch = json!({key2: val2});
        let _ = json.merge("$", patch);
    }

    /// clear() should make containers empty.
    #[test]
    fn clear_empties_array(len in 1usize..10) {
        let arr: Vec<i32> = (0..len as i32).collect();
        let json_str = format!(r#"{{"arr":{}}}"#, serde_json::to_string(&arr).unwrap());
        let mut json = JsonValue::parse(json_str.as_bytes()).unwrap();
        let cleared = json.clear("$.arr").unwrap();
        prop_assert_eq!(cleared, 1);

        // Verify array is now empty
        let len_result = json.arr_len("$.arr").unwrap();
        prop_assert_eq!(len_result[0], Some(0));
    }

    /// toggle() should flip boolean values.
    #[test]
    fn toggle_flips_boolean(initial: bool) {
        let json_str = format!(r#"{{"flag":{}}}"#, initial);
        let mut json = JsonValue::parse(json_str.as_bytes()).unwrap();

        let result = json.toggle("$.flag").unwrap();
        prop_assert_eq!(result.len(), 1);
        prop_assert_eq!(result[0], !initial);

        // Toggle again
        let result = json.toggle("$.flag").unwrap();
        prop_assert_eq!(result[0], initial);
    }

    /// arr_index() should find correct index or -1.
    #[test]
    fn arr_index_finds_value(values in prop::collection::vec(0i32..100, 1..10), needle: i32) {
        let json_str = format!(r#"{{"arr":{}}}"#, serde_json::to_string(&values).unwrap());
        let json = JsonValue::parse(json_str.as_bytes()).unwrap();
        let search_value = json!(needle);
        let result = json.arr_index("$.arr", &search_value, 0, 0).unwrap();

        let expected_index = values.iter().position(|&v| v == needle).map(|i| i as i64).unwrap_or(-1);
        prop_assert_eq!(result.len(), 1);
        prop_assert_eq!(result[0], expected_index);
    }

    /// str_len() should return correct length.
    #[test]
    fn str_len_returns_correct_length(s in "[a-zA-Z0-9]{0,50}") {
        let json_str = format!(r#"{{"str":"{}"}}"#, s);
        let json = JsonValue::parse(json_str.as_bytes()).unwrap();
        let result = json.str_len("$.str").unwrap();
        prop_assert_eq!(result.len(), 1);
        prop_assert_eq!(result[0], Some(s.len()));
    }
}

#[cfg(test)]
mod edge_case_tests {
    use super::*;

    #[test]
    fn test_parse_special_values() {
        // Unicode
        let json = JsonValue::parse(r#"{"emoji":"🦊"}"#.as_bytes());
        assert!(json.is_ok());

        // Escaped characters
        let json = JsonValue::parse(r#"{"escaped":"hello\nworld\ttab"}"#.as_bytes());
        assert!(json.is_ok());

        // Very large number
        let json = JsonValue::parse(b"99999999999999999999999999999");
        assert!(json.is_ok());

        // Scientific notation
        let json = JsonValue::parse(b"1.23e10");
        assert!(json.is_ok());
    }

    #[test]
    fn test_deeply_nested_access() {
        let json = JsonValue::parse(br#"{"a":{"b":{"c":{"d":{"e":42}}}}}"#).unwrap();

        let result = json.get("$.a.b.c.d.e");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn test_array_negative_index() {
        let json = JsonValue::parse(br#"{"arr":[1,2,3,4,5]}"#).unwrap();

        // arr_pop with negative index
        let mut json_clone = json.clone();
        let result = json_clone.arr_pop("$.arr", Some(-1));
        assert!(result.is_ok());
    }

    #[test]
    fn test_wildcard_path() {
        let json = JsonValue::parse(br#"{"items":[{"id":1},{"id":2},{"id":3}]}"#).unwrap();

        let result = json.get("$.items[*].id");
        assert!(result.is_ok());
        let values = result.unwrap();
        assert_eq!(values.len(), 3);
    }

    #[test]
    fn test_nonexistent_path() {
        let json = JsonValue::parse(br#"{"a":1}"#).unwrap();

        let result = json.get("$.nonexistent");
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_empty_object_and_array() {
        // Empty object
        let json = JsonValue::parse(br#"{}"#).unwrap();
        let keys = json.obj_keys("$").unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].as_ref().unwrap().len(), 0);

        // Empty array
        let json = JsonValue::parse(br#"[]"#).unwrap();
        let len = json.arr_len("$").unwrap();
        assert_eq!(len[0], Some(0));
    }

    #[test]
    fn test_merge_delete_with_null() {
        let mut json = JsonValue::parse(br#"{"a":1,"b":2}"#).unwrap();
        let patch = json!({"a": null});

        json.merge("$", patch).unwrap();

        // "a" should be deleted
        let result = json.get("$.a").unwrap();
        assert!(result.is_empty());

        // "b" should still exist
        let result = json.get("$.b").unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_numeric_precision() {
        // Integer that fits in i64
        let json = JsonValue::parse(b"9007199254740992").unwrap();
        let result = json.get("$").unwrap();
        assert_eq!(result.len(), 1);

        // Float precision
        let mut json = JsonValue::parse(br#"{"n":0.1}"#).unwrap();
        let _ = json.num_incr_by("$.n", 0.2);
        // Result should be close to 0.3 (floating point precision)
    }

    #[test]
    fn test_arr_operations_bounds() {
        let mut json = JsonValue::parse(br#"{"arr":[1,2,3,4,5]}"#).unwrap();

        // arrtrim with out of bounds
        let result = json.arr_trim("$.arr", 10, 20);
        assert!(result.is_ok());

        // arr_insert at end
        let mut json = JsonValue::parse(br#"{"arr":[1,2,3]}"#).unwrap();
        let result = json.arr_insert("$.arr", 3, vec![json!(4)]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_type_detection() {
        let json = JsonValue::parse(
            br#"{"str":"hello","int":42,"float":3.14,"bool":true,"null":null,"arr":[],"obj":{}}"#,
        )
        .unwrap();

        let types: Vec<(&str, &str)> = vec![
            ("$.str", "string"),
            ("$.int", "integer"),
            ("$.float", "number"),
            ("$.bool", "boolean"),
            ("$.null", "null"),
            ("$.arr", "array"),
            ("$.obj", "object"),
        ];

        for (path, expected) in types {
            let result = json.type_at(path).unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].as_str(), expected);
        }
    }
}
