#![no_main]
use frogdb_search::aggregate::{execute_shard_local, parse_aggregate_pipeline};
use libfuzzer_sys::arbitrary::{self, Arbitrary};
use libfuzzer_sys::fuzz_target;

#[derive(Arbitrary, Debug)]
struct Input {
    pipeline_str: String,
    rows: Vec<Vec<(String, String)>>,
}

fuzz_target!(|input: Input| {
    if input.pipeline_str.len() > 1024 || input.rows.len() > 32 {
        return;
    }
    if input.rows.iter().any(|r| r.len() > 8) {
        return;
    }
    if input
        .rows
        .iter()
        .flat_map(|r| r.iter())
        .any(|(k, v)| k.len() > 64 || v.len() > 64)
    {
        return;
    }

    let tokens: Vec<&str> = input.pipeline_str.split_whitespace().collect();
    if tokens.len() > 64 {
        return;
    }

    let steps = match parse_aggregate_pipeline(&tokens) {
        Ok(s) => s,
        Err(_) => return,
    };

    let _ = execute_shard_local(&input.rows, &steps);
});
