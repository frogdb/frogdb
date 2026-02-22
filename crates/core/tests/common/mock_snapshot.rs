use shuttle::sync::Mutex;
use shuttle::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Simulates snapshot mutual exclusion using CAS.
///
/// This models the RocksSnapshotCoordinator behavior where:
/// - start_snapshot() uses CAS to acquire the in_progress flag
/// - Only one snapshot can be in progress at a time
/// - Concurrent snapshot attempts get AlreadyInProgress error
pub struct MockSnapshotCoordinator {
    in_progress: AtomicBool,
    epoch: AtomicU64,
    completed_epochs: Mutex<Vec<u64>>,
}

impl MockSnapshotCoordinator {
    pub fn new() -> Self {
        Self {
            in_progress: AtomicBool::new(false),
            epoch: AtomicU64::new(0),
            completed_epochs: Mutex::new(Vec::new()),
        }
    }

    pub fn start_snapshot(&self) -> Result<u64, &'static str> {
        // CAS to acquire the in_progress lock (matches RocksSnapshotCoordinator)
        if self
            .in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err("AlreadyInProgress");
        }

        // Increment epoch atomically
        let epoch = self.epoch.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(epoch)
    }

    pub fn complete_snapshot(&self, epoch: u64) {
        self.completed_epochs.lock().unwrap().push(epoch);
        self.in_progress.store(false, Ordering::SeqCst);
    }

    pub fn in_progress(&self) -> bool {
        self.in_progress.load(Ordering::SeqCst)
    }

    pub fn completed_epochs(&self) -> Vec<u64> {
        self.completed_epochs.lock().unwrap().clone()
    }
}
