//! Process-global registry mapping shard id → its [`FakeWalLog`].
//!
//! Test/fake-only. turmoil runs single-threaded with one sim per test, so a
//! process-global keyed by shard id lets a test reach each shard's recorded WAL
//! effect log after a run without threading a handle through the server boot.
//!
//! Gated behind `cfg(any(test, feature = "fake-wal"))` so it never compiles
//! into a production binary.
use frogdb_persistence::FakeWalLog;
use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

static REGISTRY: LazyLock<Mutex<HashMap<usize, FakeWalLog>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// The process-global fake-WAL log registry.
pub struct FakeWalRegistry;

impl FakeWalRegistry {
    /// Register a shard's log. The builder calls this when it constructs a
    /// [`frogdb_persistence::FakeWalSink`].
    pub fn install(shard_id: usize, log: FakeWalLog) {
        REGISTRY
            .lock()
            .expect("FakeWalRegistry poisoned")
            .insert(shard_id, log);
    }

    /// Clear all registered logs. Call before a run for isolation.
    pub fn clear() {
        REGISTRY.lock().expect("FakeWalRegistry poisoned").clear();
    }

    /// The log for `shard_id`, if a fake sink was installed for it.
    pub fn log(shard_id: usize) -> Option<FakeWalLog> {
        REGISTRY
            .lock()
            .expect("FakeWalRegistry poisoned")
            .get(&shard_id)
            .cloned()
    }
}
