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
    /// An older (lower-txid) transaction wounded this one so that the pair
    /// could not livelock on mutual refusal. Wound-wait's whole value is the
    /// progress guarantee, and that guarantee only holds if the wounded
    /// transaction retries under its *original* txid: a retry that minted a
    /// fresh (higher) txid could be wounded again by every younger arrival,
    /// forever. Retryable, and the retry keeps the txid.
    Wounded,
    /// The continuation lock was taken away from its holder for a reason that
    /// is not a wound: `SCRIPT KILL`/`FUNCTION KILL`, or the hold cap
    /// expiring. Not retryable — the caller's work was deliberately stopped.
    Revoked,
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
            VllError::Wounded => write!(f, "VLL transaction wounded by an older transaction"),
            VllError::Revoked => write!(f, "VLL continuation lock revoked"),
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
