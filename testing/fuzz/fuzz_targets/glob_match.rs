#![no_main]
use frogdb_types::glob_match;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    // Use first byte to split data into (pattern, key)
    let split = data[0] as usize % data.len();
    let (pattern, key) = data.split_at(split);
    let _ = glob_match(pattern, key);
});
