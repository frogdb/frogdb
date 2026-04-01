#![no_main]
use frogdb_cluster::state::ClusterStateInner;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(state) = serde_json::from_slice::<ClusterStateInner>(data) {
        // Probe invariants that the rest of the system assumes hold.
        // Panics here reveal states that would crash production code.
        for (&slot, _) in &state.slot_assignment {
            assert!(slot < 16384, "slot {slot} out of range");
        }
        for (&slot, migration) in &state.migrations {
            assert!(slot < 16384, "migration slot {slot} out of range");
            // Migration references should point to known nodes.
            let _ = state.nodes.get(&migration.source_node);
            let _ = state.nodes.get(&migration.target_node);
        }
    }
});
