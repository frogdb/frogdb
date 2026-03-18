#![no_main]
use frogdb_types::timeseries::decode_samples;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = decode_samples(data);
});
