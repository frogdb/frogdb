#![no_main]
use bytes::BytesMut;
use frogdb_replication::ReplicationFrameCodec;
use libfuzzer_sys::fuzz_target;
use tokio_util::codec::Decoder;

fuzz_target!(|data: &[u8]| {
    if data.len() > 64 * 1024 {
        return;
    }

    let mut buf = BytesMut::from(data);
    let mut codec = ReplicationFrameCodec::new();

    loop {
        match codec.decode(&mut buf) {
            Ok(Some(_frame)) => continue,
            Ok(None) => break,
            Err(_) => break,
        }
    }
});
