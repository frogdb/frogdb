#![no_main]
use frogdb_types::FilterExpr;
use libfuzzer_sys::arbitrary::{self, Arbitrary};
use libfuzzer_sys::fuzz_target;

#[derive(Arbitrary, Debug)]
struct Input {
    filter: String,
    json_bytes: Vec<u8>,
}

fuzz_target!(|input: Input| {
    if input.filter.len() > 512 || input.json_bytes.len() > 4096 {
        return;
    }

    let expr = match FilterExpr::parse(&input.filter) {
        Ok(e) => e,
        Err(_) => return,
    };

    // Evaluate with no attributes.
    let _ = expr.evaluate(None);

    // Evaluate against fuzz-generated JSON.
    if let Ok(json_val) = serde_json::from_slice::<serde_json::Value>(&input.json_bytes) {
        let _ = expr.evaluate(Some(&json_val));
    }
});
