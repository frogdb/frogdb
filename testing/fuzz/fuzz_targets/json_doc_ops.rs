#![no_main]
use frogdb_types::json::JsonValue;
use libfuzzer_sys::arbitrary::{self, Arbitrary};
use libfuzzer_sys::fuzz_target;
use serde_json::Value as JsonData;

#[derive(Arbitrary, Debug)]
enum Op {
    Get { path: String },
    Set { path: String, value_idx: u8, nx: bool, xx: bool },
    Delete { path: String },
    NumIncrBy { path: String, incr_bits: u32 },
    NumMultBy { path: String, mult_bits: u32 },
    StrAppend { path: String, append: String },
    ArrAppend { path: String, value_idx: u8 },
    ArrInsert { path: String, index: i8, value_idx: u8 },
    ArrPop { path: String, index: i8 },
    ArrTrim { path: String, start: i8, stop: i8 },
    Toggle { path: String },
    Clear { path: String },
    Merge { path: String, value_idx: u8 },
}

#[derive(Arbitrary, Debug)]
struct JsonDocInput {
    ops: Vec<Op>,
}

/// Small set of preset JSON values for set/append/merge operations.
fn preset_value(idx: u8) -> JsonData {
    match idx % 7 {
        0 => JsonData::Null,
        1 => JsonData::Bool(true),
        2 => serde_json::json!(42),
        3 => serde_json::json!("str"),
        4 => serde_json::json!([1, 2]),
        5 => serde_json::json!({"x": 1}),
        _ => serde_json::json!(3.14),
    }
}

fuzz_target!(|input: JsonDocInput| {
    if input.ops.len() > 64 {
        return;
    }

    // Seed document with various types for interesting path exploration.
    let mut doc = JsonValue::new(serde_json::json!({
        "a": [1, 2, 3],
        "b": {"c": "hello", "d": true, "e": [10, 20]},
        "f": 42,
        "g": "world",
        "h": null
    }));

    for op in &input.ops {
        match op {
            Op::Get { path } => {
                if path.len() > 128 { continue; }
                let _ = doc.get(path);
            }
            Op::Set { path, value_idx, nx, xx } => {
                if path.len() > 128 { continue; }
                let _ = doc.set(path, preset_value(*value_idx), *nx, *xx);
            }
            Op::Delete { path } => {
                if path.len() > 128 { continue; }
                let _ = doc.delete(path);
            }
            Op::NumIncrBy { path, incr_bits } => {
                if path.len() > 128 { continue; }
                let incr = f64::from(*incr_bits as f32);
                if !incr.is_finite() { continue; }
                let _ = doc.num_incr_by(path, incr);
            }
            Op::NumMultBy { path, mult_bits } => {
                if path.len() > 128 { continue; }
                let mult = f64::from(*mult_bits as f32);
                if !mult.is_finite() { continue; }
                let _ = doc.num_mult_by(path, mult);
            }
            Op::StrAppend { path, append } => {
                if path.len() > 128 || append.len() > 64 { continue; }
                let _ = doc.str_append(path, append);
            }
            Op::ArrAppend { path, value_idx } => {
                if path.len() > 128 { continue; }
                let _ = doc.arr_append(path, vec![preset_value(*value_idx)]);
            }
            Op::ArrInsert { path, index, value_idx } => {
                if path.len() > 128 { continue; }
                let _ = doc.arr_insert(path, *index as i64, vec![preset_value(*value_idx)]);
            }
            Op::ArrPop { path, index } => {
                if path.len() > 128 { continue; }
                let _ = doc.arr_pop(path, Some(*index as i64));
            }
            Op::ArrTrim { path, start, stop } => {
                if path.len() > 128 { continue; }
                let _ = doc.arr_trim(path, *start as i64, *stop as i64);
            }
            Op::Toggle { path } => {
                if path.len() > 128 { continue; }
                let _ = doc.toggle(path);
            }
            Op::Clear { path } => {
                if path.len() > 128 { continue; }
                let _ = doc.clear(path);
            }
            Op::Merge { path, value_idx } => {
                if path.len() > 128 { continue; }
                let _ = doc.merge(path, preset_value(*value_idx));
            }
        }
    }
});
