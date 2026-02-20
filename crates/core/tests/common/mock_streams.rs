use shuttle::sync::Mutex;

/// Models an XREAD BLOCK wait queue where waiters are registered
/// and satisfied concurrently.
pub struct MockStreamWaitQueue {
    /// Queue of waiting thread IDs (simulating connection IDs).
    waiters: Mutex<Vec<usize>>,
    /// Threads that have been satisfied (received their response).
    satisfied: Mutex<Vec<usize>>,
}

impl MockStreamWaitQueue {
    pub fn new() -> Self {
        Self {
            waiters: Mutex::new(Vec::new()),
            satisfied: Mutex::new(Vec::new()),
        }
    }

    /// Add a waiter to the queue (simulates XREAD BLOCK starting).
    pub fn add_waiter(&self, thread_id: usize) {
        let mut waiters = self.waiters.lock().unwrap();
        waiters.push(thread_id);
    }

    /// Check if there are waiters.
    pub fn has_waiters(&self) -> bool {
        let waiters = self.waiters.lock().unwrap();
        !waiters.is_empty()
    }

    /// Pop the oldest waiter and mark as satisfied (simulates XADD waking a waiter).
    pub fn satisfy_oldest(&self) -> Option<usize> {
        let mut waiters = self.waiters.lock().unwrap();
        if waiters.is_empty() {
            return None;
        }
        let thread_id = waiters.remove(0); // FIFO
        drop(waiters);

        let mut satisfied = self.satisfied.lock().unwrap();
        satisfied.push(thread_id);
        Some(thread_id)
    }

    /// Get the order in which waiters were satisfied.
    pub fn satisfied_order(&self) -> Vec<usize> {
        self.satisfied.lock().unwrap().clone()
    }
}
