#![no_main]
use frogdb_persistence::deserialize;
use libfuzzer_sys::fuzz_target;

/// Number of known type markers (0..=16).
const NUM_TYPES: u8 = 17;

/// Header size expected by the deserializer.
const HEADER_SIZE: usize = 24;

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    // Construct a valid header so the fuzzer exercises type-specific deserializers
    // instead of always failing at header validation.
    let type_byte = data[0] % NUM_TYPES;
    let rest = &data[1..];

    let payload_len = rest.len().saturating_sub(HEADER_SIZE.saturating_sub(1));
    let mut header = [0u8; HEADER_SIZE];
    header[0] = type_byte;
    // flags = 0, expires_at_ms = 0, lfu = 0, padding = 0
    // payload_len as u64 little-endian at offset 16
    header[16..24].copy_from_slice(&(payload_len as u64).to_le_bytes());

    // Build the full buffer: header + payload
    let mut buf = Vec::with_capacity(HEADER_SIZE + payload_len);
    buf.extend_from_slice(&header);
    if rest.len() >= HEADER_SIZE - 1 {
        buf.extend_from_slice(&rest[HEADER_SIZE - 1..]);
    }

    let _ = deserialize(&buf);

    // Also try raw data directly (covers malformed headers)
    let _ = deserialize(data);
});
