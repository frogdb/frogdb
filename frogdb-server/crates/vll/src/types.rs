//! VLL type definitions.

/// Lock mode for key access.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    /// Read access - multiple readers allowed.
    Read,
    /// Write access - exclusive.
    Write,
}

/// State of a pending operation in the VLL queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingOpState {
    /// Waiting for locks to be acquired.
    Pending,
    /// Locks acquired, waiting for the coordinator's `VllExecute`.
    Ready,
    /// Operation is executing.
    Executing,
    /// Operation completed or aborted.
    Done,
}

/// VLL-specific errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VllError {
    /// Queue is full, cannot accept new operations.
    QueueFull,
    /// Lock acquisition timed out.
    LockTimeout,
    /// Operation was aborted.
    Aborted,
    /// Shard is busy with a continuation lock.
    ShardBusy,
    /// Internal error.
    Internal(String),
}

impl std::fmt::Display for VllError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VllError::QueueFull => write!(f, "VLL queue full"),
            VllError::LockTimeout => write!(f, "VLL lock acquisition timeout"),
            VllError::Aborted => write!(f, "VLL operation aborted"),
            VllError::ShardBusy => write!(f, "Shard busy with continuation lock"),
            VllError::Internal(msg) => write!(f, "VLL internal error: {}", msg),
        }
    }
}

impl std::error::Error for VllError {}

/// Result from a shard indicating readiness status.
#[derive(Debug, Clone)]
pub enum ShardReadyResult {
    /// Shard has acquired locks and is ready to execute.
    Ready,
    /// Shard failed to acquire locks.
    Failed(VllError),
}
