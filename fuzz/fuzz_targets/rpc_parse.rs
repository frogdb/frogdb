#![no_main]
use frogdb_cluster::{ClusterRpcRequest, ClusterRpcResponse};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = serde_json::from_slice::<ClusterRpcRequest>(data);
    let _ = serde_json::from_slice::<ClusterRpcResponse>(data);
});
