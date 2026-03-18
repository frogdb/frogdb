#![no_main]
use frogdb_types::HyperLogLogValue;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = HyperLogLogValue::deserialize(data);
});
