//! Label enums for typed metrics.
//!
//! These enums provide compile-time safety for metric labels with a fixed
//! set of possible values. Each enum derives `MetricLabel` which generates
//! an `as_str()` method for Prometheus label values.

use frogdb_metrics_derive::MetricLabel;

/// Error types for command errors.
#[derive(MetricLabel, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorType {
    /// Timeout error (command took too long).
    #[label = "timeout"]
    Timeout,
    /// Out of memory error.
    #[label = "oom"]
    OutOfMemory,
    /// Wrong type error (operation on wrong data type).
    #[label = "wrongtype"]
    WrongType,
    /// Authentication failure.
    #[label = "auth"]
    AuthFailure,
    /// Syntax error in command.
    #[label = "syntax"]
    SyntaxError,
    /// Server is busy.
    #[label = "busy"]
    Busy,
    /// Cluster error (e.g., MOVED, ASK).
    #[label = "cluster"]
    ClusterError,
    /// Script error (Lua execution failure).
    #[label = "script"]
    ScriptError,
    /// No such key.
    #[label = "nokey"]
    NoKey,
    /// Invalid argument.
    #[label = "invalid_arg"]
    InvalidArg,
}

/// Transaction outcomes.
#[derive(MetricLabel, Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionOutcome {
    /// Transaction committed successfully.
    #[label = "committed"]
    Committed,
    /// Transaction discarded by client.
    #[label = "discarded"]
    Discarded,
    /// EXEC aborted due to error in queued command.
    #[label = "execabort"]
    ExecAbort,
    /// WATCH condition failed.
    #[label = "watchfailed"]
    WatchFailed,
}

/// Scatter-gather operation status.
#[derive(MetricLabel, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScatterGatherStatus {
    /// All shards responded successfully.
    #[label = "success"]
    Success,
    /// One or more shards returned an error.
    #[label = "error"]
    Error,
    /// Operation timed out.
    #[label = "timeout"]
    Timeout,
    /// Partial response (some shards failed).
    #[label = "partial"]
    Partial,
}

/// Server operating mode.
#[derive(MetricLabel, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServerMode {
    /// Standalone mode (no clustering).
    #[label = "standalone"]
    Standalone,
    /// Cluster mode (Raft consensus).
    #[label = "cluster"]
    Cluster,
}

/// Connection rejection reasons.
#[derive(MetricLabel, Clone, Copy, Debug, PartialEq, Eq)]
pub enum RejectionReason {
    /// Max clients limit reached.
    #[label = "maxclients"]
    MaxClients,
    /// Authentication required but not provided.
    #[label = "auth_required"]
    AuthRequired,
    /// Server is shutting down.
    #[label = "shutdown"]
    Shutdown,
}

/// Persistence error types.
#[derive(MetricLabel, Clone, Copy, Debug, PartialEq, Eq)]
pub enum PersistenceErrorType {
    /// WAL write error.
    #[label = "wal_write"]
    WalWrite,
    /// WAL flush/sync error.
    #[label = "wal_sync"]
    WalSync,
    /// Snapshot creation error.
    #[label = "snapshot"]
    Snapshot,
    /// Recovery/restore error.
    #[label = "recovery"]
    Recovery,
}

/// Blocking command resolution type.
#[derive(MetricLabel, Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockingResolution {
    /// Condition was satisfied (e.g., key became available).
    #[label = "satisfied"]
    Satisfied,
    /// Timed out waiting.
    #[label = "timeout"]
    Timeout,
    /// Client disconnected.
    #[label = "disconnect"]
    Disconnect,
}
