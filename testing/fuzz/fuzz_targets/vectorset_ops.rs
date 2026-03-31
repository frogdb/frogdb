#![no_main]
use bytes::Bytes;
use frogdb_types::vectorset::{VectorDistanceMetric, VectorQuantization, VectorSetValue};
use libfuzzer_sys::arbitrary::{self, Arbitrary};
use libfuzzer_sys::fuzz_target;

#[derive(Arbitrary, Debug)]
enum Op {
    Add { name: u8, vector: [f32; 4] },
    Remove { name: u8 },
    Search { query: [f32; 4], count: u8 },
    SetAttr { name: u8, attr_idx: u8 },
    GetAttr { name: u8 },
    Range { cursor: u8, count: u8 },
    Links { name: u8 },
}

#[derive(Arbitrary, Debug)]
struct VectorSetInput {
    ops: Vec<Op>,
}

fn element_name(v: u8) -> Bytes {
    Bytes::from(format!("v{}", v % 16))
}

/// Small set of JSON attribute values for SetAttr.
fn attr_value(idx: u8) -> serde_json::Value {
    match idx % 6 {
        0 => serde_json::Value::Null,
        1 => serde_json::json!({"color": "red", "score": 42}),
        2 => serde_json::json!({"nested": {"x": 1, "y": 2}}),
        3 => serde_json::json!(true),
        4 => serde_json::json!(99),
        _ => serde_json::json!("hello"),
    }
}

/// Return true if all components are finite (not NaN or Inf).
fn is_finite(v: &[f32; 4]) -> bool {
    v.iter().all(|x| x.is_finite())
}

fuzz_target!(|input: VectorSetInput| {
    if input.ops.len() > 64 {
        return;
    }

    let mut vs = match VectorSetValue::new(
        VectorDistanceMetric::Cosine,
        VectorQuantization::NoQuant,
        4,  // dim
        8,  // m
        16, // ef_construction
    ) {
        Ok(vs) => vs,
        Err(_) => return,
    };

    let mut added = 0usize;

    for op in &input.ops {
        // Cap total elements to keep usearch fast under fuzzing.
        if added > 32 {
            break;
        }

        match op {
            Op::Add { name, vector } => {
                if !is_finite(vector) {
                    continue;
                }
                let n = element_name(*name);
                if let Ok(is_new) = vs.add(n, vector.to_vec()) {
                    if is_new {
                        added += 1;
                    }
                }
            }
            Op::Remove { name } => {
                let n = element_name(*name);
                vs.remove(&n);
            }
            Op::Search { query, count } => {
                if !is_finite(query) {
                    continue;
                }
                let c = (*count as usize).min(16);
                let _ = vs.search(&query[..], c);
            }
            Op::SetAttr { name, attr_idx } => {
                let n = element_name(*name);
                let attr = attr_value(*attr_idx);
                vs.set_attr(&n, attr);
            }
            Op::GetAttr { name } => {
                let n = element_name(*name);
                let _ = vs.get_attr(&n);
            }
            Op::Range { cursor, count } => {
                let c = element_name(*cursor);
                let cnt = (*count as usize).min(16);
                let _ = vs.range(&c, cnt);
            }
            Op::Links { name } => {
                let n = element_name(*name);
                let _ = vs.links(&n, None);
            }
        }
    }

    // Verify basic invariant: card() is consistent.
    let card = vs.card();
    assert!(card <= 32, "card {card} exceeded cap");
});
