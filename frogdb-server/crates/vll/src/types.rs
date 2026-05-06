//! VLL type definitions.

use std::time::Duration;

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
    /// Locks acquired, waiting for execute signal.
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

/// Signal sent to shards to proceed with execution or abort.
#[derive(Debug, Clone, Copy)]
pub struct ExecuteSignal {
    /// Whether to proceed with execution.
    pub proceed: bool,
}

/// VLL configuration.
#[derive(Debug, Clone)]
pub struct VllConfig {
    /// Maximum queue depth per shard before rejecting new operations.
    pub max_queue_depth: usize,
    /// Timeout for acquiring locks on all shards (ms).
    pub lock_acquisition_timeout_ms: u64,
    /// Per-shard lock acquisition timeout (ms).
    pub per_shard_lock_timeout_ms: u64,
    /// Interval for checking/cleaning up expired operations (ms).
    pub timeout_check_interval_ms: u64,
    /// Maximum time a continuation lock can be held (ms).
    pub max_continuation_lock_ms: u64,
}

impl Default for VllConfig {
    fn default() -> Self {
        Self {
            max_queue_depth: 10000,
            lock_acquisition_timeout_ms: 4000,
            per_shard_lock_timeout_ms: 2000,
            timeout_check_interval_ms: 100,
            max_continuation_lock_ms: 65000,
        }
    }
}

impl VllConfig {
    /// Get lock acquisition timeout as Duration.
    pub fn lock_acquisition_timeout(&self) -> Duration {
        Duration::from_millis(self.lock_acquisition_timeout_ms)
    }

    /// Get per-shard lock timeout as Duration.
    pub fn per_shard_lock_timeout(&self) -> Duration {
        Duration::from_millis(self.per_shard_lock_timeout_ms)
    }

    /// Get timeout check interval as Duration.
    pub fn timeout_check_interval(&self) -> Duration {
        Duration::from_millis(self.timeout_check_interval_ms)
    }

    /// Get max continuation lock duration as Duration.
    pub fn max_continuation_lock(&self) -> Duration {
        Duration::from_millis(self.max_continuation_lock_ms)
    }
}
