//! Connection-side MULTI/EXEC transaction orchestration.
//!
//! This crate owns the two halves of a transaction that live on the
//! *connection* side of FrogDB (the shard-side engine — WATCH version checks,
//! rollback, post-execution — stays in `frogdb-core`):
//!
//! - [`TransactionState`] — the per-connection MULTI queue, watch set, and
//!   slot/shard co-location accumulator, plus the lifecycle transitions
//!   (`begin` / `push_queued_command` / `abort` / `fold_keys` / `watch_key` /
//!   `unwatch_all` / `take` / `discard`).
//! - [`execute_transaction`] — the EXEC algorithm: abort and rate-limit gates,
//!   EXEC-time slot re-validation, the pause barrier, the
//!   deferred/shard partition, target resolution, the shard round-trip, and
//!   the deferred-command merge. Every exit names a [`TransactionOutcome`], and
//!   [`handle_exec`] is the single place the outcome metric is recorded.
//!
//! Everything the algorithm needs from the surrounding server — the command
//! registry, the cluster redirect seam, the rate limiter, the shard channels,
//! the connection-command dispatch machinery — is reached through the
//! [`TxnHost`] trait. That is what makes the EXEC failure modes forcible: a
//! test host can return a `-MOVED`, a dead shard channel, or a WATCH abort
//! without standing up a server.

mod exec;
mod host;
mod state;

pub use exec::{TransactionOutcome, execute_transaction, handle_exec, record_transaction_metrics};
pub use host::{Deferral, ShardTxnReply, TxnHost};
pub use state::{
    TransactionState, TransactionTarget, TxnError, TxnMetrics, TxnSummary, WatchedKey,
};

/// CI verification probe: deliberately untested so the `mutants-diff` job has
/// a mutant to miss. Never lands — this commit lives on the `[ci-verify]` branch only.
pub fn ci_verify_probe(n: u32) -> u32 {
    n + 1
}
