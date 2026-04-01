#![no_main]
use libfuzzer_sys::arbitrary::{self, Arbitrary};
use libfuzzer_sys::fuzz_target;

/// FNV-1a hash matching the private `compute_checksum` in frogdb-scripting.
fn compute_checksum(data: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in data {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[derive(Arbitrary, Debug)]
struct FunctionRestoreInput {
    /// Whether to wrap the payload in a valid envelope (magic + version + checksum).
    /// When false, raw bytes are fed directly to exercise early rejection paths.
    valid_envelope: bool,
    payload: Vec<u8>,
}

fuzz_target!(|input: FunctionRestoreInput| {
    // Cap payload to avoid trivial OOM.
    if input.payload.len() > 64 * 1024 {
        return;
    }

    if input.valid_envelope {
        // Build a valid envelope around the fuzz payload so the parser
        // gets past magic/version/checksum checks into field parsing.
        let mut buf = Vec::new();
        buf.extend_from_slice(b"FDBF"); // magic
        buf.push(1); // version
        buf.extend_from_slice(&input.payload);
        let checksum = compute_checksum(&buf);
        buf.extend_from_slice(&checksum.to_le_bytes());
        let _ = frogdb_scripting::restore_libraries(&buf);
    } else {
        let _ = frogdb_scripting::restore_libraries(&input.payload);
    }
});
