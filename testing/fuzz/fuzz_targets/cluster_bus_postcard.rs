#![no_main]
use frogdb_cluster::{ClusterRpcRequest, ClusterRpcResponse};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = postcard::from_bytes::<ClusterRpcRequest>(data);
    let _ = postcard::from_bytes::<ClusterRpcResponse>(data);
});
