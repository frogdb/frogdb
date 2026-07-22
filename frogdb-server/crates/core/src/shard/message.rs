use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use frogdb_protocol::{ParsedCommand, ProtocolVersion, Response};
use tokio::sync::{mpsc, oneshot};

use crate::eviction::EvictionConfig;
use crate::latency::{LatencyEvent, LatencySample};
use crate::pubsub::{ConnId, IntrospectionRequest, IntrospectionResponse, PubSubSender};
use crate::slowlog::SlowLogEntry;
use crate::tracking::InvalidationSender;
use crate::types::StreamId;
use crate::vll::{LockMode, ShardReadyResult};

use super::counters::HotShardStatsResponse;
use super::types::{
    BigKeysScanResponse, InfoShardSnapshot, PartialResult, PubSubLimitsInfo, ShardMemoryStats,
    TransactionResult, VllQueueInfo,
};

/// A timestamped wrapper around [`ShardMessage`] for measuring queue latency.
pub struct Envelope {
    /// The inner message.
    pub message: ShardMessage,
    /// When the message was enqueued.
    pub enqueued_at: Instant,
}

/// Newtype sender that auto-timestamps messages on send.
#[derive(Debug, Clone)]
pub struct ShardSender {
    inner: mpsc::Sender<Envelope>,
}

impl ShardSender {
    /// Wrap a raw mpsc sender.
    pub fn new(inner: mpsc::Sender<Envelope>) -> Self {
        Self { inner }
    }

    /// Send a message, automatically recording the enqueue timestamp.
    ///
    /// Generic over `Into<ShardMessage>` so a send site can pass a category
    /// sub-enum value (`CoreMsg::Execute { .. }`) directly and have it wrapped,
    /// instead of naming the outer `ShardMessage::Core(..)` wrapper explicitly.
    pub async fn send<M: Into<ShardMessage>>(
        &self,
        message: M,
    ) -> Result<(), mpsc::error::SendError<ShardMessage>> {
        let envelope = Envelope {
            message: message.into(),
            enqueued_at: Instant::now(),
        };
        self.inner
            .send(envelope)
            .await
            .map_err(|e| mpsc::error::SendError(e.0.message))
    }

    /// Non-blocking send for use in synchronous contexts (e.g. Lua callbacks).
    #[allow(clippy::result_large_err)]
    pub fn try_send<M: Into<ShardMessage>>(
        &self,
        message: M,
    ) -> Result<(), mpsc::error::TrySendError<ShardMessage>> {
        let envelope = Envelope {
            message: message.into(),
            enqueued_at: Instant::now(),
        };
        self.inner.try_send(envelope).map_err(|e| match e {
            mpsc::error::TrySendError::Full(env) => mpsc::error::TrySendError::Full(env.message),
            mpsc::error::TrySendError::Closed(env) => {
                mpsc::error::TrySendError::Closed(env.message)
            }
        })
    }
}

/// Newtype receiver that yields [`Envelope`]s.
pub struct ShardReceiver {
    inner: mpsc::Receiver<Envelope>,
}

impl ShardReceiver {
    /// Wrap a raw mpsc receiver.
    pub fn new(inner: mpsc::Receiver<Envelope>) -> Self {
        Self { inner }
    }

    /// Receive the next envelope.
    pub async fn recv(&mut self) -> Option<Envelope> {
        self.inner.recv().await
    }

    /// Return the number of messages currently buffered.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if there are no messages buffered.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Non-blocking receive of the next buffered envelope, if any.
    ///
    /// Shard-driver harness seam for the deterministic pump loop: under a
    /// current-thread runtime a message already delivered by a spawned
    /// coordinator task is buffered and returned without yielding, so pumping
    /// one queued message never lets another task interleave mid-pump.
    #[cfg(any(test, feature = "shard-driver"))]
    #[doc(hidden)]
    pub fn try_recv(&mut self) -> Option<Envelope> {
        self.inner.try_recv().ok()
    }
}

/// One watched key carried by [`ShardMessage::ExecTransaction`].
///
/// Bundles the per-key state EXEC's watch validation needs: the key, the
/// per-shard `version` snapshotted at WATCH time, and `live_at_watch` — whether
/// the key was present-and-unexpired when it was watched (the inverse of Redis
/// `wk->expired`, PR #7920 / issue #7918).
///
/// `live_at_watch` lets EXEC distinguish a key watched **live** that then
/// expired during the window (must abort, even absent a version bump this
/// watcher observed) from one already expired/absent when watched (a
/// "nonexistent" watch that must NOT abort when it stays gone). The coarse
/// per-shard version alone cannot express that per-key discriminator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchEntry {
    /// The watched key.
    pub key: Bytes,
    /// The per-shard version snapshotted at WATCH time.
    pub version: u64,
    /// Whether the key was live (present and unexpired) at WATCH time.
    pub live_at_watch: bool,
}

/// Messages sent to shard workers.
///
/// A thin two-level wrapper: every variant except [`ShardMessage::Shutdown`]
/// carries a per-category sub-enum. The category partition the event loop
/// dispatches on is therefore enforced by the type system — a `dispatch_*`
/// method takes its category enum and matches it exhaustively (no wildcard), so
/// an unhandled variant is a compile error and a misrouted variant cannot type
/// check — rather than by hand-maintained agreement between `dispatch_message`
/// and each `dispatch_*` sub-match closed by `_ => unreachable!()`.
#[derive(Debug)]
pub enum ShardMessage {
    /// Core command execution — see [`CoreMsg`].
    Core(CoreMsg),
    /// Pub/Sub and connection lifecycle — see [`PubSubMsg`].
    PubSub(PubSubMsg),
    /// Client-side caching / tracking — see [`TrackingMsg`].
    Tracking(TrackingMsg),
    /// Scripting and functions — see [`ScriptingMsg`].
    Scripting(ScriptingMsg),
    /// Blocking command waiters — see [`BlockingMsg`].
    Blocking(BlockingMsg),
    /// Observability: slowlog, memory, latency, stats, config — see [`ObservabilityMsg`].
    Observability(ObservabilityMsg),
    /// VLL (Very Lightweight Locking) — see [`VllMsg`].
    Vll(VllMsg),
    /// Always-available DEBUG introspection probes — see [`DebugIntrospectionMsg`].
    DebugIntrospection(DebugIntrospectionMsg),
    /// Cluster / Raft — see [`ClusterMsg`].
    Cluster(ClusterMsg),
    /// Search index flush + pub/sub limits — see [`SearchMsg`].
    Search(SearchMsg),

    /// Shutdown signal.
    ///
    /// Handled directly in the event loop body (flushes WAL and breaks the
    /// loop); it is the only message that returns `true` from dispatch, so it
    /// stays a top-level variant rather than folding into a category.
    Shutdown,
}

/// Core command-execution messages.
#[derive(Debug)]
pub enum CoreMsg {
    /// Execute a command on this shard.
    Execute {
        command: Arc<ParsedCommand>,
        conn_id: u64,
        /// Transaction ID for VLL ordering (optional for single-shard operations).
        txid: Option<u64>,
        /// Protocol version for response encoding.
        protocol_version: ProtocolVersion,
        /// Whether to record key reads for client-side caching invalidation.
        track_reads: bool,
        /// Whether to suppress touch() calls (CLIENT NO-TOUCH mode).
        no_touch: bool,
        response_tx: oneshot::Sender<Response>,
    },

    /// Scatter-gather: partial request for multi-key operation.
    ScatterRequest {
        request_id: u64,
        keys: Vec<Bytes>,
        operation: ScatterOp,
        /// Connection ID for access control during continuation locks.
        conn_id: u64,
        response_tx: oneshot::Sender<PartialResult>,
    },

    /// Get the current shard version (for WATCH).
    ///
    /// `keys` are the keys being watched. Any that are *already* logically
    /// expired at watch time are lazily purged here (physical state aligned to
    /// logical state) WITHOUT bumping the version — so watching an already-stale
    /// key records a "nonexistent" watch that a later EXEC does not treat as
    /// modified. This is what lets `purge_expired_watches` at EXEC distinguish a
    /// key that was live-at-watch and expired during the window (F3: must abort)
    /// from one that was already gone when watched (must NOT abort), matching
    /// Redis's `wk->expired` flag (PR #7920). Pass an empty `keys` for a pure
    /// version probe (no purge).
    ///
    /// The reply is `(shard_version, live_at_watch)`: the current shard version
    /// plus, aligned with `keys`, one flag per key reporting whether it was live
    /// (present and unexpired) at watch time — the `wk->expired` discriminator
    /// (see [`WatchEntry::live_at_watch`]). The flags are computed via a
    /// non-destructive `exists_unexpired` probe before the no-bump lazy purge.
    GetVersion {
        keys: Vec<Bytes>,
        response_tx: oneshot::Sender<(u64, Vec<bool>)>,
    },

    /// Execute a transaction atomically.
    ExecTransaction {
        commands: Vec<ParsedCommand>,
        /// Watched keys with their watch-time version and liveness.
        watches: Vec<WatchEntry>,
        conn_id: u64,
        /// Protocol version for response encoding.
        protocol_version: ProtocolVersion,
        response_tx: oneshot::Sender<TransactionResult>,
    },
}

/// Pub/Sub and connection-lifecycle messages.
#[derive(Debug)]
pub enum PubSubMsg {
    /// Subscribe to broadcast channels.
    Subscribe {
        channels: Vec<Bytes>,
        conn_id: ConnId,
        sender: PubSubSender,
        response_tx: oneshot::Sender<Vec<usize>>,
    },

    /// Unsubscribe from broadcast channels.
    Unsubscribe {
        channels: Vec<Bytes>,
        conn_id: ConnId,
        response_tx: oneshot::Sender<Vec<usize>>,
    },

    /// Subscribe to patterns.
    PSubscribe {
        patterns: Vec<Bytes>,
        conn_id: ConnId,
        sender: PubSubSender,
        response_tx: oneshot::Sender<Vec<usize>>,
    },

    /// Unsubscribe from patterns.
    PUnsubscribe {
        patterns: Vec<Bytes>,
        conn_id: ConnId,
        response_tx: oneshot::Sender<Vec<usize>>,
    },

    /// Publish to a broadcast channel.
    Publish {
        channel: Bytes,
        message: Bytes,
        response_tx: oneshot::Sender<usize>,
    },

    /// Forwarded keyspace notification: publish into THIS (coordinator) shard's
    /// subscription table. Fire-and-forget — there is no `response_tx`, because
    /// the emitting shard does not (and must not) wait. This is the at-most-once
    /// contract expressed in the type: sent via [`ShardSender::try_send`] from a
    /// non-coordinator shard whose key-owner table has no broadcast subscribers.
    PublishKeyspace { channel: Bytes, payload: Bytes },

    /// Subscribe to sharded channels.
    ShardedSubscribe {
        channels: Vec<Bytes>,
        conn_id: ConnId,
        sender: PubSubSender,
        response_tx: oneshot::Sender<Vec<usize>>,
    },

    /// Unsubscribe from sharded channels.
    ShardedUnsubscribe {
        channels: Vec<Bytes>,
        conn_id: ConnId,
        response_tx: oneshot::Sender<Vec<usize>>,
    },

    /// Publish to a sharded channel.
    ShardedPublish {
        channel: Bytes,
        message: Bytes,
        response_tx: oneshot::Sender<usize>,
    },

    /// Pub/Sub introspection request.
    PubSubIntrospection {
        request: IntrospectionRequest,
        response_tx: oneshot::Sender<IntrospectionResponse>,
    },

    /// Connection closed - clean up subscriptions and tracking.
    ConnectionClosed { conn_id: ConnId },
}

/// Client-side caching / tracking messages.
#[derive(Debug)]
pub enum TrackingMsg {
    /// Register a connection for client-side caching invalidation.
    TrackingRegister {
        conn_id: ConnId,
        sender: InvalidationSender,
        noloop: bool,
    },

    /// Unregister a connection from client-side caching invalidation.
    TrackingUnregister { conn_id: ConnId },

    /// Register a connection for BCAST-mode client tracking.
    TrackingBroadcastRegister {
        conn_id: ConnId,
        sender: InvalidationSender,
        noloop: bool,
        prefixes: Vec<Bytes>,
    },
}

/// Scripting and function messages.
#[derive(Debug)]
pub enum ScriptingMsg {
    /// Execute a Lua script (EVAL / EVAL_RO).
    EvalScript {
        /// Script source code.
        script_source: Bytes,
        /// Keys passed to the script.
        keys: Vec<Bytes>,
        /// Additional arguments.
        argv: Vec<Bytes>,
        /// Connection ID.
        conn_id: u64,
        /// Protocol version for response encoding.
        protocol_version: ProtocolVersion,
        /// Read-only mode (EVAL_RO) — rejects write commands in the script.
        read_only: bool,
        /// Response channel.
        response_tx: oneshot::Sender<Response>,
    },

    /// Execute a cached Lua script (EVALSHA / EVALSHA_RO).
    EvalScriptSha {
        /// SHA1 hash of the script (hex string).
        script_sha: Bytes,
        /// Keys passed to the script.
        keys: Vec<Bytes>,
        /// Additional arguments.
        argv: Vec<Bytes>,
        /// Connection ID.
        conn_id: u64,
        /// Protocol version for response encoding.
        protocol_version: ProtocolVersion,
        /// Read-only mode (EVALSHA_RO) — rejects write commands in the script.
        read_only: bool,
        /// Response channel.
        response_tx: oneshot::Sender<Response>,
    },

    /// Load a script into the cache (SCRIPT LOAD).
    ScriptLoad {
        /// Script source code.
        script_source: Bytes,
        /// Response channel (returns SHA1 hex).
        response_tx: oneshot::Sender<String>,
    },

    /// Check if scripts exist (SCRIPT EXISTS).
    ScriptExists {
        /// SHA1 hashes to check (hex strings).
        shas: Vec<Bytes>,
        /// Response channel.
        response_tx: oneshot::Sender<Vec<bool>>,
    },

    /// Flush the script cache (SCRIPT FLUSH).
    ScriptFlush {
        /// Response channel.
        response_tx: oneshot::Sender<()>,
    },

    /// Kill the running script (SCRIPT KILL).
    ScriptKill {
        /// Response channel.
        response_tx: oneshot::Sender<Result<(), String>>,
    },

    /// Execute a sub-command from a Lua script on the correct shard.
    ///
    /// Used when `redis.call()` inside a Lua script accesses a key that
    /// belongs to a different shard than the one running the script.
    ScriptSubCommand {
        /// Full command parts (command name + args).
        command: Vec<Bytes>,
        /// Connection ID of the script's connection.
        conn_id: u64,
        /// Protocol version for response encoding.
        protocol_version: ProtocolVersion,
        /// Synchronous response channel (blocks the Lua thread).
        response_tx: std::sync::mpsc::SyncSender<Response>,
    },

    /// Execute a function (FCALL).
    FunctionCall {
        /// Function name.
        function_name: Bytes,
        /// Keys passed to the function.
        keys: Vec<Bytes>,
        /// Additional arguments.
        argv: Vec<Bytes>,
        /// Connection ID.
        conn_id: u64,
        /// Protocol version for response encoding.
        protocol_version: ProtocolVersion,
        /// Whether this is a read-only call (FCALL_RO).
        read_only: bool,
        /// Response channel.
        response_tx: oneshot::Sender<Response>,
    },
}

/// Blocking-command waiter messages.
#[derive(Debug)]
pub enum BlockingMsg {
    /// Register a blocking wait for keys.
    BlockWait {
        /// Connection ID of the blocked client.
        conn_id: u64,
        /// Keys to wait on.
        keys: Vec<Bytes>,
        /// The blocking operation type.
        op: crate::types::BlockingOp,
        /// Channel to send the response when data is available.
        response_tx: oneshot::Sender<Response>,
        /// Deadline for the blocking operation (None = indefinite).
        deadline: Option<Instant>,
        /// Protocol version of the blocked client (for RESP3-aware formatting).
        protocol_version: ProtocolVersion,
    },

    /// Cancel a blocking wait (timeout or disconnect).
    UnregisterWait {
        /// Connection ID to unregister.
        conn_id: u64,
    },
}

/// Observability messages: slowlog, memory, latency, stats, and runtime config.
#[derive(Debug)]
pub enum ObservabilityMsg {
    // =========================================================================
    // Slowlog messages
    // =========================================================================
    /// Get slow query log entries from this shard.
    SlowlogGet {
        /// Maximum number of entries to return.
        count: usize,
        /// Response channel.
        response_tx: oneshot::Sender<Vec<SlowLogEntry>>,
    },

    /// Get the number of slowlog entries in this shard.
    SlowlogLen {
        /// Response channel.
        response_tx: oneshot::Sender<usize>,
    },

    /// Reset (clear) the slowlog for this shard.
    SlowlogReset {
        /// Response channel.
        response_tx: oneshot::Sender<()>,
    },

    /// Add a slow query entry to this shard's log.
    SlowlogAdd {
        /// Duration in microseconds.
        duration_us: u64,
        /// Command name and arguments (pre-truncated).
        command: Vec<Bytes>,
        /// Client address.
        client_addr: String,
        /// Client name.
        client_name: String,
        /// Current max-len setting (to keep shard in sync with config).
        max_len: usize,
    },

    // =========================================================================
    // Memory messages
    // =========================================================================
    /// Get memory usage for a specific key.
    MemoryUsage {
        /// Key to check.
        key: Bytes,
        /// Number of nested samples for complex structures (optional).
        samples: Option<usize>,
        /// Response channel.
        response_tx: oneshot::Sender<Option<usize>>,
    },

    /// Get memory statistics from this shard.
    MemoryStats {
        /// Response channel.
        response_tx: oneshot::Sender<ShardMemoryStats>,
    },

    /// Get the combined INFO snapshot from this shard (memory, eviction,
    /// keysizes, tiered counters, WAL lag, and replica identity) in one reply.
    ///
    /// Used by INFO to gather all per-shard data in a single fleet scatter
    /// instead of separate `MemoryStats` and `KeysizesSnapshot` passes.
    InfoSnapshot {
        /// Response channel.
        response_tx: oneshot::Sender<InfoShardSnapshot>,
    },

    /// Scan for big keys (keys larger than threshold).
    ScanBigKeys {
        /// Minimum size in bytes to consider a key "big".
        threshold_bytes: usize,
        /// Maximum number of big keys to return.
        max_keys: usize,
        /// Response channel.
        response_tx: oneshot::Sender<BigKeysScanResponse>,
    },

    // =========================================================================
    // Latency messages
    // =========================================================================
    /// Get the latest latency sample for each event type.
    LatencyLatest {
        /// Response channel.
        response_tx: oneshot::Sender<Vec<(LatencyEvent, LatencySample)>>,
    },

    /// Get latency history for a specific event type.
    LatencyHistory {
        /// Event type to query.
        event: LatencyEvent,
        /// Response channel.
        response_tx: oneshot::Sender<Vec<LatencySample>>,
    },

    /// Reset latency data for specific events (or all if empty).
    LatencyReset {
        /// Events to reset (empty = all).
        events: Vec<LatencyEvent>,
        /// Response channel.
        response_tx: oneshot::Sender<()>,
    },

    // =========================================================================
    // Hot shard messages
    // =========================================================================
    /// Get hot shard statistics from this shard.
    HotShardStats {
        /// How many seconds of data to include (1-60).
        period_secs: u64,
        /// Response channel.
        response_tx: oneshot::Sender<HotShardStatsResponse>,
    },

    /// Reset statistics (CONFIG RESETSTAT).
    /// Clears latency monitor, slowlog, and peak memory.
    ResetStats {
        /// Response channel to acknowledge the reset.
        response_tx: oneshot::Sender<()>,
    },

    /// Update shard configuration at runtime.
    UpdateConfig {
        /// New eviction configuration (if changed).
        eviction_config: Option<EvictionConfig>,
        /// Response channel to acknowledge the update.
        response_tx: oneshot::Sender<()>,
    },

    /// Toggle active expiration (DEBUG SET-ACTIVE-EXPIRE).
    SetActiveExpire {
        /// Whether active expiry is enabled (true) or disabled (false).
        enabled: bool,
        /// Response channel to acknowledge the change.
        response_tx: oneshot::Sender<()>,
    },

    /// Toggle key-memory histogram tracking on this shard's store.
    SetKeyMemoryHistograms {
        /// Whether key-memory tracking is enabled.
        enabled: bool,
        /// Response channel to acknowledge the change.
        response_tx: oneshot::Sender<()>,
    },

    /// Get keysize histograms snapshot from this shard (DEBUG KEYSIZES-HIST-ASSERT).
    KeysizesSnapshot {
        /// Response channel returning cloned histograms.
        response_tx: oneshot::Sender<Option<crate::histogram::KeysizeHistograms>>,
    },

    /// Get total allocated memory for keys in a given slot (DEBUG ALLOCSIZE-SLOTS-ASSERT).
    AllocsizeInSlot {
        /// Hash slot to query.
        slot: u16,
        /// Response channel returning the total memory.
        response_tx: oneshot::Sender<usize>,
    },
}

/// VLL (Very Lightweight Locking) messages.
#[derive(Debug)]
pub enum VllMsg {
    /// VLL lock request - declare intents and acquire locks.
    VllLockRequest {
        /// Transaction ID for ordering.
        txid: u64,
        /// Keys to lock on this shard.
        keys: Vec<Bytes>,
        /// Lock mode (read or write).
        mode: LockMode,
        /// The operation to execute after locks are acquired.
        operation: ScatterOp,
        /// Channel to notify coordinator when ready.
        ready_tx: oneshot::Sender<ShardReadyResult>,
    },

    /// VLL execute - execute a previously locked operation.
    VllExecute {
        /// Transaction ID.
        txid: u64,
        /// Response channel for the result.
        response_tx: oneshot::Sender<PartialResult>,
    },

    /// VLL abort - release locks and cleanup for a failed operation.
    VllAbort {
        /// Transaction ID to abort.
        txid: u64,
    },

    /// VLL continuation lock - acquire full shard lock for MULTI/EXEC or Lua.
    VllContinuationLock {
        /// Transaction ID.
        txid: u64,
        /// Connection ID that owns this lock.
        conn_id: u64,
        /// Channel to notify coordinator when ready.
        ready_tx: oneshot::Sender<ShardReadyResult>,
        /// Channel to receive release signal.
        release_rx: oneshot::Receiver<()>,
    },

    /// Get VLL queue information from this shard.
    GetVllQueueInfo {
        /// Channel to send the response.
        response_tx: oneshot::Sender<VllQueueInfo>,
    },
}

/// Cluster / Raft messages.
#[derive(Debug)]
pub enum ClusterMsg {
    /// Notify shard that a slot has migrated to a new node.
    /// All blocked clients waiting on keys in this slot receive `-MOVED`.
    SlotMigrated {
        slot: u16,
        target_addr: std::net::SocketAddr,
    },

    /// Execute a Raft command asynchronously.
    /// Used by cluster commands (CLUSTER MEET, CLUSTER FORGET, etc.) that need
    /// to call async Raft operations from synchronous command handlers.
    /// The shard worker uses its own Raft reference (set via set_raft()).
    RaftCommand {
        /// The Raft command to execute.
        cmd: crate::cluster::ClusterCommand,
        /// Response channel for the result.
        response_tx: oneshot::Sender<Result<(), String>>,
    },
}

/// Always-available DEBUG introspection messages (LOCKTABLE / WAITQUEUE /
/// MEMORY-CHECK / EXPIRY-INDEX-CHECK).
#[derive(Debug)]
pub enum DebugIntrospectionMsg {
    /// Get the VLL lock-table snapshot from this shard (DEBUG LOCKTABLE).
    GetLockTableInfo {
        /// Channel to send the response.
        response_tx: oneshot::Sender<super::types::LockTableInfo>,
    },

    /// Get the blocking-waiter snapshot from this shard (DEBUG WAITQUEUE).
    GetWaitQueueInfo {
        /// Channel to send the response.
        response_tx: oneshot::Sender<super::types::WaitQueueInfo>,
    },

    /// Recompute live memory and report tracked vs recomputed (DEBUG MEMORY-CHECK).
    MemoryCheck {
        /// Channel to send the response.
        response_tx: oneshot::Sender<super::types::MemoryCheckInfo>,
    },

    /// Cross-check the expiry index against entry deadlines (DEBUG EXPIRY-INDEX-CHECK).
    ExpiryIndexCheck {
        /// Channel to send the response.
        response_tx: oneshot::Sender<super::types::ExpiryIndexCheckInfo>,
    },

    /// Backdate a key's expiry deadline into the past (DEBUG EXPIRE-BACKDATE),
    /// making it already-expired without a wall-clock wait. Rewrites only the
    /// deadline; the next read/sweep performs the actual expiry.
    ExpireBackdate {
        /// The key whose deadline is rewritten.
        key: Bytes,
        /// How many milliseconds into the past to move the deadline.
        ms: u64,
        /// Channel to report whether the key existed / had a TTL.
        response_tx: oneshot::Sender<crate::store::BackdateExpiryResult>,
    },
}

/// Search index flush + pub/sub limits messages.
#[derive(Debug)]
pub enum SearchMsg {
    /// Flush (commit) all dirty search indexes on this shard.
    /// Used by the snapshot coordinator to ensure search index consistency.
    FlushSearchIndexes { response_tx: oneshot::Sender<()> },

    /// Get pub/sub limits info from this shard.
    GetPubSubLimitsInfo {
        /// Channel to send the response.
        response_tx: oneshot::Sender<PubSubLimitsInfo>,
    },
}

impl From<CoreMsg> for ShardMessage {
    fn from(m: CoreMsg) -> Self {
        ShardMessage::Core(m)
    }
}
impl From<PubSubMsg> for ShardMessage {
    fn from(m: PubSubMsg) -> Self {
        ShardMessage::PubSub(m)
    }
}
impl From<TrackingMsg> for ShardMessage {
    fn from(m: TrackingMsg) -> Self {
        ShardMessage::Tracking(m)
    }
}
impl From<ScriptingMsg> for ShardMessage {
    fn from(m: ScriptingMsg) -> Self {
        ShardMessage::Scripting(m)
    }
}
impl From<BlockingMsg> for ShardMessage {
    fn from(m: BlockingMsg) -> Self {
        ShardMessage::Blocking(m)
    }
}
impl From<ObservabilityMsg> for ShardMessage {
    fn from(m: ObservabilityMsg) -> Self {
        ShardMessage::Observability(m)
    }
}
impl From<VllMsg> for ShardMessage {
    fn from(m: VllMsg) -> Self {
        ShardMessage::Vll(m)
    }
}
impl From<DebugIntrospectionMsg> for ShardMessage {
    fn from(m: DebugIntrospectionMsg) -> Self {
        ShardMessage::DebugIntrospection(m)
    }
}
impl From<ClusterMsg> for ShardMessage {
    fn from(m: ClusterMsg) -> Self {
        ShardMessage::Cluster(m)
    }
}
impl From<SearchMsg> for ShardMessage {
    fn from(m: SearchMsg) -> Self {
        ShardMessage::Search(m)
    }
}

impl ShardMessage {
    /// Return a static string identifying the message variant, for USDT probes.
    ///
    /// Delegates to a per-category `probe_type_str`; the produced strings are
    /// byte-for-byte identical to the pre-split flat variant names (e.g.
    /// `"VllAbort"`), which downstream USDT probe consumers depend on.
    pub fn probe_type_str(&self) -> &'static str {
        match self {
            ShardMessage::Core(m) => m.probe_type_str(),
            ShardMessage::PubSub(m) => m.probe_type_str(),
            ShardMessage::Tracking(m) => m.probe_type_str(),
            ShardMessage::Scripting(m) => m.probe_type_str(),
            ShardMessage::Blocking(m) => m.probe_type_str(),
            ShardMessage::Observability(m) => m.probe_type_str(),
            ShardMessage::Vll(m) => m.probe_type_str(),
            ShardMessage::DebugIntrospection(m) => m.probe_type_str(),
            ShardMessage::Cluster(m) => m.probe_type_str(),
            ShardMessage::Search(m) => m.probe_type_str(),
            ShardMessage::Shutdown => "Shutdown",
        }
    }
}

impl CoreMsg {
    /// USDT probe name for this variant (byte-stable with the pre-split names).
    pub fn probe_type_str(&self) -> &'static str {
        match self {
            CoreMsg::Execute { .. } => "Execute",
            CoreMsg::ScatterRequest { .. } => "ScatterRequest",
            CoreMsg::GetVersion { .. } => "GetVersion",
            CoreMsg::ExecTransaction { .. } => "ExecTransaction",
        }
    }
}

impl PubSubMsg {
    /// USDT probe name for this variant (byte-stable with the pre-split names).
    pub fn probe_type_str(&self) -> &'static str {
        match self {
            PubSubMsg::Subscribe { .. } => "Subscribe",
            PubSubMsg::Unsubscribe { .. } => "Unsubscribe",
            PubSubMsg::PSubscribe { .. } => "PSubscribe",
            PubSubMsg::PUnsubscribe { .. } => "PUnsubscribe",
            PubSubMsg::Publish { .. } => "Publish",
            PubSubMsg::PublishKeyspace { .. } => "PublishKeyspace",
            PubSubMsg::ShardedSubscribe { .. } => "ShardedSubscribe",
            PubSubMsg::ShardedUnsubscribe { .. } => "ShardedUnsubscribe",
            PubSubMsg::ShardedPublish { .. } => "ShardedPublish",
            PubSubMsg::PubSubIntrospection { .. } => "PubSubIntrospection",
            PubSubMsg::ConnectionClosed { .. } => "ConnectionClosed",
        }
    }
}

impl TrackingMsg {
    /// USDT probe name for this variant (byte-stable with the pre-split names).
    pub fn probe_type_str(&self) -> &'static str {
        match self {
            TrackingMsg::TrackingRegister { .. } => "TrackingRegister",
            TrackingMsg::TrackingUnregister { .. } => "TrackingUnregister",
            TrackingMsg::TrackingBroadcastRegister { .. } => "TrackingBroadcastRegister",
        }
    }
}

impl ScriptingMsg {
    /// USDT probe name for this variant (byte-stable with the pre-split names).
    pub fn probe_type_str(&self) -> &'static str {
        match self {
            ScriptingMsg::EvalScript { .. } => "EvalScript",
            ScriptingMsg::EvalScriptSha { .. } => "EvalScriptSha",
            ScriptingMsg::ScriptLoad { .. } => "ScriptLoad",
            ScriptingMsg::ScriptExists { .. } => "ScriptExists",
            ScriptingMsg::ScriptFlush { .. } => "ScriptFlush",
            ScriptingMsg::ScriptKill { .. } => "ScriptKill",
            ScriptingMsg::ScriptSubCommand { .. } => "ScriptSubCommand",
            ScriptingMsg::FunctionCall { .. } => "FunctionCall",
        }
    }
}

impl BlockingMsg {
    /// USDT probe name for this variant (byte-stable with the pre-split names).
    pub fn probe_type_str(&self) -> &'static str {
        match self {
            BlockingMsg::BlockWait { .. } => "BlockWait",
            BlockingMsg::UnregisterWait { .. } => "UnregisterWait",
        }
    }
}

impl ObservabilityMsg {
    /// USDT probe name for this variant (byte-stable with the pre-split names).
    pub fn probe_type_str(&self) -> &'static str {
        match self {
            ObservabilityMsg::SlowlogGet { .. } => "SlowlogGet",
            ObservabilityMsg::SlowlogLen { .. } => "SlowlogLen",
            ObservabilityMsg::SlowlogReset { .. } => "SlowlogReset",
            ObservabilityMsg::SlowlogAdd { .. } => "SlowlogAdd",
            ObservabilityMsg::MemoryUsage { .. } => "MemoryUsage",
            ObservabilityMsg::MemoryStats { .. } => "MemoryStats",
            ObservabilityMsg::InfoSnapshot { .. } => "InfoSnapshot",
            ObservabilityMsg::ScanBigKeys { .. } => "ScanBigKeys",
            ObservabilityMsg::LatencyLatest { .. } => "LatencyLatest",
            ObservabilityMsg::LatencyHistory { .. } => "LatencyHistory",
            ObservabilityMsg::LatencyReset { .. } => "LatencyReset",
            ObservabilityMsg::HotShardStats { .. } => "HotShardStats",
            ObservabilityMsg::ResetStats { .. } => "ResetStats",
            ObservabilityMsg::UpdateConfig { .. } => "UpdateConfig",
            ObservabilityMsg::SetActiveExpire { .. } => "SetActiveExpire",
            ObservabilityMsg::SetKeyMemoryHistograms { .. } => "SetKeyMemoryHistograms",
            ObservabilityMsg::KeysizesSnapshot { .. } => "KeysizesSnapshot",
            ObservabilityMsg::AllocsizeInSlot { .. } => "AllocsizeInSlot",
        }
    }
}

impl VllMsg {
    /// USDT probe name for this variant (byte-stable with the pre-split names).
    pub fn probe_type_str(&self) -> &'static str {
        match self {
            VllMsg::VllLockRequest { .. } => "VllLockRequest",
            VllMsg::VllExecute { .. } => "VllExecute",
            VllMsg::VllAbort { .. } => "VllAbort",
            VllMsg::VllContinuationLock { .. } => "VllContinuationLock",
            VllMsg::GetVllQueueInfo { .. } => "GetVllQueueInfo",
        }
    }
}

impl DebugIntrospectionMsg {
    /// USDT probe name for this variant (byte-stable with the pre-split names).
    pub fn probe_type_str(&self) -> &'static str {
        match self {
            DebugIntrospectionMsg::GetLockTableInfo { .. } => "GetLockTableInfo",
            DebugIntrospectionMsg::GetWaitQueueInfo { .. } => "GetWaitQueueInfo",
            DebugIntrospectionMsg::MemoryCheck { .. } => "MemoryCheck",
            DebugIntrospectionMsg::ExpiryIndexCheck { .. } => "ExpiryIndexCheck",
            DebugIntrospectionMsg::ExpireBackdate { .. } => "ExpireBackdate",
        }
    }
}

impl ClusterMsg {
    /// USDT probe name for this variant (byte-stable with the pre-split names).
    pub fn probe_type_str(&self) -> &'static str {
        match self {
            ClusterMsg::SlotMigrated { .. } => "SlotMigrated",
            ClusterMsg::RaftCommand { .. } => "RaftCommand",
        }
    }
}

impl SearchMsg {
    /// USDT probe name for this variant (byte-stable with the pre-split names).
    pub fn probe_type_str(&self) -> &'static str {
        match self {
            SearchMsg::FlushSearchIndexes { .. } => "FlushSearchIndexes",
            SearchMsg::GetPubSubLimitsInfo { .. } => "GetPubSubLimitsInfo",
        }
    }
}

/// Operation type for scatter-gather.
#[derive(Debug, Clone)]
pub enum ScatterOp {
    /// MGET operation - get multiple values.
    MGet,
    /// MSET operation - set multiple key-value pairs.
    MSet {
        /// Key-value pairs where keys align with the keys field in ScatterRequest.
        pairs: Vec<(Bytes, Bytes)>,
    },
    /// DELETE operation.
    Del,
    /// EXISTS operation.
    Exists,
    /// TOUCH operation.
    Touch,
    /// UNLINK operation (async delete, same as Del for now).
    Unlink,
    /// KEYS operation - get all keys matching a pattern.
    Keys {
        /// Pattern to match (glob syntax).
        pattern: Bytes,
    },
    /// DBSIZE operation - get total key count.
    DbSize,
    /// FLUSHDB operation - clear all keys.
    FlushDb,
    /// SCAN operation - scan keys with cursor.
    Scan {
        /// Position within this shard to start scanning.
        cursor: u64,
        /// Hint for number of keys to return.
        count: usize,
        /// Optional pattern to match.
        pattern: Option<Bytes>,
        /// Optional type filter.
        key_type: Option<crate::types::KeyType>,
    },
    /// COPY operation - retrieve value and expiry from source key for cross-shard copy.
    Copy {
        /// The source key to copy from.
        source_key: Bytes,
    },
    /// COPY set operation - write a value from cross-shard copy to destination key.
    CopySet {
        /// The destination key to write to.
        dest_key: Bytes,
        /// The serialized value data (a self-describing persistence frame).
        value_data: Bytes,
        /// TTL in milliseconds (None = no expiry).
        expiry_ms: Option<i64>,
        /// Whether to replace existing key.
        replace: bool,
    },
    /// RANDOMKEY operation - get a random key from the shard.
    RandomKey,
    /// DUMP operation for MIGRATE - serialize keys with full metadata.
    /// Returns serialized data compatible with Redis RESTORE command.
    Dump,
    /// TS.QUERYINDEX - query label index across all shards.
    TsQueryIndex { args: Vec<Bytes> },
    /// TS.MGET - label index query + get_last per match.
    TsMget { args: Vec<Bytes> },
    /// TS.MRANGE / TS.MREVRANGE - label index query + range per match.
    TsMrange { args: Vec<Bytes>, reverse: bool },
    /// FT.CREATE - create a search index on this shard.
    FtCreate { index_def_json: Bytes },
    /// FT.SEARCH - search the index on this shard. The request is the full
    /// FT.SEARCH grammar, parsed once at the coordinator.
    FtSearch {
        index_name: Bytes,
        request: Box<frogdb_search::FtSearchRequest>,
    },
    /// FT.DROPINDEX - drop a search index on this shard.
    FtDropIndex { index_name: Bytes },
    /// FT.INFO - get search index info from this shard.
    FtInfo { index_name: Bytes },
    /// FT._LIST - list all search indexes on this shard.
    FtList,
    /// FT.ALTER - add fields to an existing search index on this shard.
    FtAlter {
        index_name: Bytes,
        new_fields_json: Bytes,
    },
    /// FT.SYNUPDATE - update synonym group on this shard.
    FtSynupdate {
        index_name: Bytes,
        group_id: Bytes,
        terms: Vec<Bytes>,
    },
    /// FT.SYNDUMP - dump synonym groups from this shard.
    FtSyndump { index_name: Bytes },
    /// FT.AGGREGATE - aggregate search results on this shard. The request
    /// carries the pipeline parsed once at the coordinator.
    FtAggregate {
        index_name: Bytes,
        request: Box<frogdb_search::FtAggregateRequest>,
    },
    /// FT.HYBRID - hybrid search combining BM25 and vector search on this shard.
    FtHybrid {
        index_name: Bytes,
        query_args: Vec<Bytes>,
    },
    /// FT.ALIASADD - add an alias for a search index on this shard.
    FtAliasadd {
        alias_name: Bytes,
        index_name: Bytes,
    },
    /// FT.ALIASDEL - delete a search index alias on this shard.
    FtAliasdel { alias_name: Bytes },
    /// FT.ALIASUPDATE - add or update an alias on this shard.
    FtAliasupdate {
        alias_name: Bytes,
        index_name: Bytes,
    },
    /// FT.TAGVALS - get distinct tag values from this shard.
    FtTagvals {
        index_name: Bytes,
        field_name: Bytes,
    },
    /// FT.DICTADD - add terms to a dictionary on this shard.
    FtDictadd { dict_name: Bytes, terms: Vec<Bytes> },
    /// FT.DICTDEL - delete terms from a dictionary on this shard.
    FtDictdel { dict_name: Bytes, terms: Vec<Bytes> },
    /// FT.DICTDUMP - dump all terms from a dictionary on this shard.
    FtDictdump { dict_name: Bytes },
    /// FT.CONFIG - get/set search configuration on this shard.
    FtConfig { args: Vec<Bytes> },
    /// FT.SPELLCHECK - check spelling on this shard's index segment.
    FtSpellcheck {
        index_name: Bytes,
        query_args: Vec<Bytes>,
    },
    /// FT.EXPLAIN - return query execution plan from this shard.
    FtExplain { index_name: Bytes, query_str: Bytes },
    /// ES.ALL - read the per-shard `__frogdb:es:all` stream.
    EsAll {
        count: Option<usize>,
        after_id: Option<StreamId>,
    },
}
