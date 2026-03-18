#![no_main]
use frogdb_types::json::{JsonLimits, JsonValue};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let limits = JsonLimits {
        max_depth: 32,
        max_size: 64 * 1024,
    };
    let _ = JsonValue::parse_with_limits(data, &limits);
});
