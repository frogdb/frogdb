#![no_main]
use frogdb_search::expression::{evaluate, parse_expression};
use libfuzzer_sys::arbitrary::{self, Arbitrary};
use libfuzzer_sys::fuzz_target;

#[derive(Arbitrary, Debug)]
struct Input {
    expr_str: String,
    row: Vec<(String, String)>,
}

fuzz_target!(|input: Input| {
    if input.expr_str.len() > 512 || input.row.len() > 16 {
        return;
    }
    if input.row.iter().any(|(k, v)| k.len() > 64 || v.len() > 64) {
        return;
    }

    let expr = match parse_expression(&input.expr_str) {
        Ok(e) => e,
        Err(_) => return,
    };

    let _ = evaluate(&expr, &input.row);
});
