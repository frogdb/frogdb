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

/// Arbitrary JSON documents: scalars at the leaves, arrays and objects a few
/// levels deep. Object keys stay short so a generated document is also cheap to
/// address by JSONPath.
fn arb_json() -> impl Strategy<Value = serde_json::Value> {
    let leaf = prop_oneof![
        Just(serde_json::Value::Null),
        any::<bool>().prop_map(serde_json::Value::Bool),
        any::<i64>().prop_map(|n| json!(n)),
        any::<u64>().prop_map(|n| json!(n)),
        (-1e18f64..1e18f64).prop_map(|f| json!(f)),
        "(?s).{0,12}".prop_map(serde_json::Value::String),
        // Escape-heavy strings: quotes, backslashes and C0 controls exercise the
        // tape's escaping against serde_json's.
        "[\\x00-\\x1f\"\\\\ ]{0,8}".prop_map(serde_json::Value::String),
    ];
    leaf.prop_recursive(4, 48, 5, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..5).prop_map(serde_json::Value::Array),
            prop::collection::vec(("[a-z]{1,4}", inner), 0..5)
                .prop_map(|entries| serde_json::Value::Object(entries.into_iter().collect())),
        ]
    })
}

/// Where a mutation lands: a single top-level member, a member one level
/// deeper, or every top-level member at once. Nested targets push edits deeper
/// into `emit`'s recursion; the wildcard produces multi-entry edit maps.
#[derive(Debug, Clone)]
enum Target {
    Key(String),
    Nested(String, String),
    Wild,
}

impl Target {
    fn path(&self) -> String {
        match self {
            Target::Key(k) => format!("$.{k}"),
            Target::Nested(a, b) => format!("$.{a}.{b}"),
            Target::Wild => "$.*".to_string(),
        }
    }
}

/// Mutable references to every model value the target's JSONPath matches, in
/// document order — the model-side mirror of `match_offsets`. A nested target
/// matches only through an object, and a wildcard matches every top-level
/// member, like the evaluator.
fn model_matches<'a>(
    entries: &'a mut serde_json::Map<String, serde_json::Value>,
    target: &Target,
) -> Vec<&'a mut serde_json::Value> {
    match target {
        Target::Key(k) => entries.get_mut(k).into_iter().collect(),
        Target::Nested(a, b) => entries
            .get_mut(a)
            .and_then(|v| v.as_object_mut())
            .and_then(|o| o.get_mut(b))
            .into_iter()
            .collect(),
        Target::Wild => entries.values_mut().collect(),
    }
}

fn arb_target() -> impl Strategy<Value = Target> {
    prop_oneof![
        3 => "[a-e]{1,2}".prop_map(Target::Key),
        2 => ("[a-e]{1,2}", "[a-e]{1,2}").prop_map(|(a, b)| Target::Nested(a, b)),
        1 => Just(Target::Wild),
    ]
}

/// One mutation applied to both the stored document and the model. SET stays
/// top-level (a missing deeper path walks `create_path`, whose
/// invent-containers semantics belong to their own model); the array ops and
/// NUMINCRBY reach nested and wildcard targets, which is where the rebuild's
/// index arithmetic and multi-match edit maps live.
#[derive(Debug, Clone)]
enum Op {
    Set(String, serde_json::Value),
    Delete(Target),
    ArrAppend(Target, serde_json::Value),
    ArrInsert(Target, i64, serde_json::Value),
    ArrTrim(Target, i64, i64),
    ArrPop(Target, Option<i64>),
    NumIncrBy(Target, f64),
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        2 => ("[a-e]{1,2}", arb_json()).prop_map(|(k, v)| Op::Set(k, v)),
        2 => arb_target().prop_map(Op::Delete),
        2 => (arb_target(), arb_json()).prop_map(|(t, v)| Op::ArrAppend(t, v)),
        2 => (arb_target(), -6i64..6, arb_json()).prop_map(|(t, i, v)| Op::ArrInsert(t, i, v)),
        2 => (arb_target(), -6i64..6, -6i64..6).prop_map(|(t, a, b)| Op::ArrTrim(t, a, b)),
        2 => (arb_target(), prop::option::of(-6i64..6)).prop_map(|(t, i)| Op::ArrPop(t, i)),
        2 => (arb_target(), -1e6f64..1e6).prop_map(|(t, n)| Op::NumIncrBy(t, n)),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// The tape is a lossless encoding: parsing a document and serializing it
    /// again produces exactly what `serde_json` produces from the same text.
    ///
    /// The baseline is serde's *re-serialization*, not the original text:
    /// serde_json's default float parser is allowed to land one ULP off the
    /// nearest double, so `parse -> print` is not the identity for either
    /// representation. What must hold is that the tape does not add any loss of
    /// its own.
    #[test]
    fn tape_round_trips_serde_json(value in arb_json()) {
        let text = serde_json::to_string(&value).unwrap();
        let doc = JsonValue::parse(text.as_bytes()).unwrap();
        let canonical =
            serde_json::to_string(&serde_json::from_str::<serde_json::Value>(&text).unwrap())
                .unwrap();
        prop_assert_eq!(String::from_utf8(doc.to_bytes()).unwrap(), canonical);
    }

    /// `memory_size` is a pure function of the document, so MEMORY USAGE does
    /// not drift between two stores holding the same bytes.
    #[test]
    fn tape_memory_size_is_run_stable(value in arb_json()) {
        let text = serde_json::to_string(&value).unwrap();
        let a = JsonValue::parse(text.as_bytes()).unwrap();
        let b = JsonValue::parse(text.as_bytes()).unwrap();
        prop_assert_eq!(a.memory_size(), b.memory_size());
    }

    /// Path mutations agree with a `serde_json::Value` model applied the same
    /// way: after every step the tape serializes to the model's canonical text.
    #[test]
    fn tape_mutations_track_a_serde_json_model(
        base in prop::collection::vec(("[a-e]{1,2}", arb_json()), 0..5),
        ops in prop::collection::vec(arb_op(), 1..12),
    ) {
        // Both sides start from the same parsed document, so the model carries
        // the doubles serde's parser produced rather than the generator's.
        let text = serde_json::to_string(&serde_json::Value::Object(base.into_iter().collect()))
            .unwrap();
        let mut model: serde_json::Value = serde_json::from_str(&text).unwrap();
        let mut doc = JsonValue::parse(text.as_bytes()).unwrap();

        for op in ops {
            let entries = match &mut model {
                serde_json::Value::Object(entries) => entries,
                _ => unreachable!("the model document is an object"),
            };
            match op {
                Op::Set(key, value) => {
                    let path = format!("$.{key}");
                    prop_assert!(doc.set(&path, value.clone(), false, false).is_ok());
                    entries.insert(key, value);
                }
                Op::Delete(target) => {
                    let expected = match &target {
                        Target::Key(k) => usize::from(entries.contains_key(k)),
                        Target::Nested(a, b) => entries
                            .get(a)
                            .and_then(|v| v.as_object())
                            .map_or(0, |o| usize::from(o.contains_key(b))),
                        Target::Wild => entries.len(),
                    };
                    let deleted = doc.delete(&target.path()).unwrap();
                    prop_assert_eq!(deleted, expected);
                    // Not `Map::remove`: with serde_json's `preserve_order`
                    // feature unified on (it is, workspace-wide), that is a
                    // swap-remove and moves the last key into the hole.
                    // JSON.DEL leaves the surviving keys where they were.
                    match target {
                        Target::Key(k) => {
                            *entries = std::mem::take(entries)
                                .into_iter()
                                .filter(|(name, _)| name != &k)
                                .collect();
                        }
                        Target::Nested(a, b) => {
                            if let Some(o) = entries.get_mut(&a).and_then(|v| v.as_object_mut()) {
                                *o = std::mem::take(o)
                                    .into_iter()
                                    .filter(|(name, _)| name != &b)
                                    .collect();
                            }
                        }
                        Target::Wild => entries.clear(),
                    }
                }
                // A structural array op errors when the path matches nothing
                // or any match is not an array, and the whole command aborts
                // with the document untouched (the edit map is only applied
                // after every match has been sized).
                Op::ArrAppend(target, value) => {
                    let path = target.path();
                    let matches = model_matches(entries, &target);
                    if matches.is_empty() || matches.iter().any(|v| !v.is_array()) {
                        prop_assert!(doc.arr_append(&path, vec![value]).is_err());
                    } else {
                        prop_assert!(doc.arr_append(&path, vec![value.clone()]).is_ok());
                        for m in matches {
                            m.as_array_mut().unwrap().push(value.clone());
                        }
                    }
                }
                Op::ArrInsert(target, index, value) => {
                    let path = target.path();
                    let matches = model_matches(entries, &target);
                    if matches.is_empty() || matches.iter().any(|v| !v.is_array()) {
                        prop_assert!(doc.arr_insert(&path, index, vec![value]).is_err());
                    } else {
                        prop_assert!(doc.arr_insert(&path, index, vec![value.clone()]).is_ok());
                        for m in matches {
                            let arr = m.as_array_mut().unwrap();
                            let len = arr.len();
                            // JSON.ARRINSERT clamps: a negative index counts
                            // from the end (one past, so -1 appends), and a
                            // positive one saturates at the length.
                            let at = if index < 0 {
                                (len as i64 + index + 1).max(0) as usize
                            } else {
                                (index as usize).min(len)
                            };
                            arr.insert(at, value.clone());
                        }
                    }
                }
                Op::ArrTrim(target, start, stop) => {
                    let path = target.path();
                    let matches = model_matches(entries, &target);
                    if matches.is_empty() || matches.iter().any(|v| !v.is_array()) {
                        prop_assert!(doc.arr_trim(&path, start, stop).is_err());
                    } else {
                        prop_assert!(doc.arr_trim(&path, start, stop).is_ok());
                        for m in matches {
                            let arr = m.as_array_mut().unwrap();
                            let len = arr.len() as i64;
                            let norm = |i: i64| if i < 0 { (len + i).max(0) } else { i };
                            let start_idx = norm(start).max(0) as usize;
                            let stop_idx = (norm(stop) + 1).max(0) as usize;
                            let kept = if start_idx >= len as usize || start_idx >= stop_idx {
                                0..0
                            } else {
                                start_idx..stop_idx.min(len as usize)
                            };
                            *arr = arr[kept].to_vec();
                        }
                    }
                }
                // ARRPOP is the lenient one: a non-array, empty or
                // out-of-range match yields None for that match instead of
                // failing the command.
                Op::ArrPop(target, index) => {
                    let path = target.path();
                    let matches = model_matches(entries, &target);
                    if matches.is_empty() {
                        prop_assert!(doc.arr_pop(&path, index).is_err());
                    } else {
                        let popped = doc.arr_pop(&path, index).unwrap();
                        prop_assert_eq!(popped.len(), matches.len());
                        for (m, result) in matches.into_iter().zip(popped) {
                            let Some(arr) = m.as_array_mut() else {
                                prop_assert!(result.is_none());
                                continue;
                            };
                            if arr.is_empty() {
                                prop_assert!(result.is_none());
                                continue;
                            }
                            let len = arr.len() as i64;
                            let idx = match index {
                                Some(i) => {
                                    let n = if i < 0 { len + i } else { i };
                                    (0..len).contains(&n).then_some(n as usize)
                                }
                                None => Some(arr.len() - 1),
                            };
                            match idx {
                                None => prop_assert!(result.is_none()),
                                Some(i) => {
                                    let removed = arr.remove(i);
                                    prop_assert_eq!(result, Some(removed));
                                }
                            }
                        }
                    }
                }
                Op::NumIncrBy(target, incr) => {
                    let path = target.path();
                    let matches = model_matches(entries, &target);
                    if matches.is_empty() || matches.iter().any(|v| !v.is_number()) {
                        prop_assert!(doc.num_incr_by(&path, incr).is_err());
                    } else {
                        let results = doc.num_incr_by(&path, incr).unwrap();
                        prop_assert_eq!(results.len(), matches.len());
                        for m in matches {
                            let result = m.as_f64().unwrap() + incr;
                            // Mirror the command's classification: an integral
                            // in-range result is stored as an integer.
                            let number = if result.fract() == 0.0
                                && result >= i64::MIN as f64
                                && result <= i64::MAX as f64
                            {
                                serde_json::Number::from(result as i64)
                            } else {
                                serde_json::Number::from_f64(result).unwrap()
                            };
                            *m = serde_json::Value::Number(number);
                        }
                    }
                }
            }
            prop_assert_eq!(
                String::from_utf8(doc.to_bytes()).unwrap(),
                serde_json::to_string(&model).unwrap()
            );
        }
    }
}
