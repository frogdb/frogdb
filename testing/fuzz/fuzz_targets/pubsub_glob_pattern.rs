#![no_main]
use bytes::Bytes;
use frogdb_core::pubsub::GlobPattern;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Split fuzz data into pattern and input.
    if data.is_empty() {
        return;
    }
    let split = data[0] as usize % data.len().max(1);
    let pattern = &data[1..=split.min(data.len() - 1)];
    let input = &data[split.min(data.len() - 1) + 1..];

    // Cap sizes to avoid trivial timeouts on known-slow recursive paths
    // while still finding real bugs.
    if pattern.len() > 256 || input.len() > 1024 {
        return;
    }

    let glob = GlobPattern::new(Bytes::copy_from_slice(pattern));
    let _ = glob.matches(input);
});
