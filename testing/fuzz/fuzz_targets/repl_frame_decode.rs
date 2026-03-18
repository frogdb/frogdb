#![no_main]
use bytes::Bytes;
use frogdb_replication::ReplicationFrame;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let buf = Bytes::copy_from_slice(data);
    let _ = ReplicationFrame::decode(buf);
});
