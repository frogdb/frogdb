//! The seam between the EXEC algorithm and the connection that runs it.
//!
//! [`TxnHost`] is deliberately all plain data in and plain data out: every
//! method is either a lookup the surrounding server owns (registry, rate
//! limiter, cluster redirect seam) or an effect it performs (shard round-trip,
//! deferred-command dispatch). Nothing in this crate names a connection, a
//! socket, or a registry, which is what lets a test host force every
//! [`TransactionOutcome`](crate::TransactionOutcome) in microseconds.

use async_trait::async_trait;
use bytes::Bytes;
use frogdb_core::{
    MetricsRecorder, RateLimitExceeded, ServerWideOp, TransactionResult, WatchEntry,
};
use frogdb_protocol::{ParsedCommand, Response};

/// Why a queued command cannot execute on the shard and must be deferred past
/// the shard transaction (its shard-side `Command::execute` is a placeholder).
///
/// Deferred commands are NOT atomic with the shard transaction — matching prior
/// FrogDB convention (and Redis semantics, where admin commands inside MULTI
/// take effect at EXEC time).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Deferral {
    /// `ConnectionLevel(_)` strategy: re-enters the connection dispatch
    /// machinery via [`TxnHost::run_connection_level`].
    ConnectionLevel,
    /// `ServerWide(_)` strategy: fans out to all shards via
    /// [`TxnHost::run_server_wide`]. Running these on the single transaction
    /// shard would execute the placeholder (or a one-shard subset: partial
    /// KEYS, one-shard FLUSHDB, stub FT.* replies).
    ServerWide(ServerWideOp),
}

/// The outcome of one EXEC shard round-trip, as the host observed it.
///
/// Both channel failures are named separately from the shard's own
/// [`TransactionResult`] so the algorithm — not the host — owns the mapping
/// onto wire replies.
#[derive(Debug)]
pub enum ShardTxnReply {
    /// The shard answered.
    Replied(TransactionResult),
    /// The shard's channel was closed: the request was never delivered.
    Unavailable,
    /// The shard took the request and dropped it without replying.
    Dropped,
}

/// Everything [`execute_transaction`](crate::execute_transaction) needs from the
/// connection that owns the transaction.
#[async_trait]
pub trait TxnHost {
    /// This connection's own shard — EXEC's fallback target when the queue
    /// folded neither keys nor watches.
    fn shard_id(&self) -> usize;

    /// This connection's id, for the transaction trace event.
    fn conn_id(&self) -> u64;

    /// Where the transaction outcome metrics go.
    fn metrics_recorder(&self) -> &dyn MetricsRecorder;

    /// Registry lookup: whether `name` must be deferred past the shard
    /// transaction, and how. `None` means "runs on the shard".
    fn deferral_of(&self, name: &str) -> Option<Deferral>;

    /// Whether the queued batch contains commands that `CLIENT PAUSE WRITE`
    /// must block.
    fn queue_has_writes(&self, queue: &[ParsedCommand]) -> bool;

    /// Charge the whole batch (command count + total bytes) against the
    /// authenticated user's rate limit, if one applies. `Ok(())` when the batch
    /// is admitted or no limit is in force.
    fn try_acquire_batch(&self, queue: &[ParsedCommand]) -> Result<(), RateLimitExceeded>;

    /// EXEC-time whole-batch slot re-validation (cluster mode only).
    ///
    /// `Some(reply)` is the bare `-MOVED` / `-ASK` / `-TRYAGAIN` /
    /// `-CROSSSLOT` / `-CLUSTERDOWN` that becomes EXEC's whole answer; `None`
    /// means "run the batch here". Standalone hosts always answer `None`.
    async fn validate_queued_batch(
        &mut self,
        queue: &[ParsedCommand],
        asking: bool,
    ) -> Option<Response>;

    /// Whether every watched key's slot is still served by this node (cluster
    /// mode only; standalone hosts always answer `true`).
    ///
    /// The queue's own verdict ([`Self::validate_queued_batch`]) covers the keys
    /// the *commands* name. A watch set can point somewhere else entirely — at a
    /// slot no queued command mentions, or at one the queue does not mention at
    /// all — and a watched key whose slot has changed hands is unobservable from
    /// here: the version WATCH recorded can never move again, however many
    /// writes the new owner takes. `false` means EXEC must fail the CAS rather
    /// than commit against a stale local copy.
    ///
    /// `asking` is the block-scoped ASKING flag, same as
    /// [`Self::validate_queued_batch`] takes.
    fn watched_slots_still_local(&mut self, watches: &[WatchEntry], asking: bool) -> bool;

    /// Block while a pause covering this batch is in force. Returns `true` only
    /// if the call actually blocked — EXEC uses that to decide whether its
    /// pre-pause cluster-slot verdict is still fresh.
    ///
    /// `queue` is the batch about to run, and it is the whole reason this takes
    /// an argument. A pause has two dimensions: the node-global `CLIENT PAUSE`,
    /// which covers everything, and the slot-scoped barrier the slot-migration
    /// handover arms, which covers exactly one hash slot. Without the queue a
    /// host can only answer the coarse question — "is *anything* paused?" — and
    /// must park every write EXEC on every barrier, including barriers on slots
    /// the batch never touches. With it, the host can resolve the batch to a
    /// slot and park only when the pause actually covers it.
    ///
    /// Hosts must stay fail-closed: a batch that cannot be pinned to a single
    /// slot (keyless commands, keys spanning slots) may reach the barriered slot
    /// and has to park. Standalone hosts ignore `queue` entirely.
    async fn wait_if_paused(&mut self, queue: &[ParsedCommand]) -> bool;

    /// One EXEC shard round-trip: hand `commands` + `watches` to `target_shard`
    /// and await its answer.
    async fn send_shard_transaction(
        &mut self,
        target_shard: usize,
        commands: Vec<ParsedCommand>,
        watches: Vec<WatchEntry>,
    ) -> ShardTxnReply;

    /// The reply for a batch whose routing generation refused to settle.
    ///
    /// A shard refuses an apply outright when the routing generation the batch
    /// was validated against is no longer the live one
    /// ([`TransactionResult::TopologyChanged`]). EXEC answers that by
    /// re-validating and re-sending — but only a bounded number of times, so a
    /// slot changing hands repeatedly cannot spin here. When the attempts run
    /// out the client is told to try again, a reply this crate cannot build
    /// itself: the redirect vocabulary lives behind the server's redirect seam
    /// (`frogdb-types::redirect`), which `frogdb-txn` does not depend on.
    ///
    /// Standalone hosts never see this call — they stamp no routing generation,
    /// so no shard can refuse one.
    fn routing_unsettled_reply(&self) -> Response;

    /// Run a deferred connection-level command (CLIENT, CONFIG, INFO, the
    /// pub/sub family, …).
    ///
    /// Returns `(exec_slot_response, push_confirmations)`: the first goes into
    /// the EXEC array at the command's queued position, the second carries any
    /// out-of-band Push frames to send after the EXEC reply (e.g. RESP3
    /// subscribe/unsubscribe confirmations).
    async fn run_connection_level(
        &mut self,
        name: &str,
        args: &[Bytes],
    ) -> (Response, Vec<Response>);

    /// Run a deferred server-wide command, fanning out to every shard.
    async fn run_server_wide(&mut self, op: ServerWideOp, args: &[Bytes]) -> Response;
}
