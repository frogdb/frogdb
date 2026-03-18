#![no_main]
use frogdb_types::types::StreamId;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = StreamId::parse(data);
    let _ = StreamId::parse_range_bound(data);
    let _ = StreamId::parse_for_add(data);
});
