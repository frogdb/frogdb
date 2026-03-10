#![no_main]
use bytes::Bytes;
use frogdb_protocol::ParsedCommand;
use libfuzzer_sys::fuzz_target;
use redis_protocol::resp2::decode::decode_bytes;
use std::convert::TryFrom;

fuzz_target!(|data: &[u8]| {
    let buf = Bytes::copy_from_slice(data);
    if let Ok(Some((frame, _))) = decode_bytes(&buf) {
        let _ = ParsedCommand::try_from(frame);
    }
});
