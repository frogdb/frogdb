#![no_main]
use frogdb_search::aggregate::parse_aggregate_pipeline;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        let tokens: Vec<&str> = s.split_whitespace().collect();
        if tokens.len() > 128 {
            return;
        }
        let _ = parse_aggregate_pipeline(&tokens);
    }
});
