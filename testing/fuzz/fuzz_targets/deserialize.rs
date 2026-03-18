#![no_main]
use frogdb_persistence::deserialize;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = deserialize(data);
});
