//! Snapshot handle.

pub struct SnapshotHandle {
    epoch: u64,
    is_noop: bool,
    complete_fn: Option<Box<dyn FnOnce() + Send>>,
}
impl SnapshotHandle {
    pub fn noop() -> Self {
        Self {
            epoch: 0,
            is_noop: true,
            complete_fn: None,
        }
    }
    pub fn new(epoch: u64, complete_fn: impl FnOnce() + Send + 'static) -> Self {
        Self {
            epoch,
            is_noop: false,
            complete_fn: Some(Box::new(complete_fn)),
        }
    }
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    pub fn is_noop(&self) -> bool {
        self.is_noop
    }
    pub fn complete(mut self) {
        if let Some(f) = self.complete_fn.take() {
            f();
        }
    }
}
impl Drop for SnapshotHandle {
    fn drop(&mut self) {
        if let Some(f) = self.complete_fn.take() {
            f();
        }
    }
}
