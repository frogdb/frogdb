use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use frogdb_protocol::{ParsedCommand, ProtocolVersion, Response};
use tokio::sync::oneshot;

use crate::eviction::EvictionConfig;
use crate::latency::{LatencyEvent, LatencySample};
use crate::pubsub::{ConnId, IntrospectionRequest, IntrospectionResponse, PubSubSender};
use crate::slowlog::SlowLogEntry;
use crate::tracking::InvalidationSender;
use crate::vll::{ExecuteSignal, LockMode, ShardReadyResult};

use super::counters::HotShardStatsResponse;
use super::types::{
    BigKeysScanResponse, PartialResult, PubSubLimitsInfo, ShardMemoryStats, TransactionResult,
    VllQueueInfo, WalLagStatsResponse,
};

/// Messages sent to shard workers.
#[derive(Debug)]
pub enum ShardMessage {
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
    GetVersion { response_tx: oneshot::Sender<u64> },

    /// Execute a transaction atomically.
    ExecTransaction {
        commands: Vec<ParsedCommand>,
        /// Watched keys: (key, version_at_watch_time).
        watches: Vec<(Bytes, u64)>,
        conn_id: u64,
        /// Protocol version for response encoding.
        protocol_version: ProtocolVersion,
        response_tx: oneshot::Sender<TransactionResult>,
    },

    // =========================================================================
    // Pub/Sub messages
    // =========================================================================
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

    // =========================================================================
    // Client tracking messages
    // =========================================================================
    /// Register a connection for client-side caching invalidation.
    TrackingRegister {
        conn_id: ConnId,
        sender: InvalidationSender,
        noloop: bool,
    },

    /// Unregister a connection from client-side caching invalidation.
    TrackingUnregister { conn_id: ConnId },

    // =========================================================================
    // Scripting messages
    // =========================================================================
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

    // =========================================================================
    // Function messages
    // =========================================================================
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

    // =========================================================================
    // Blocking commands messages
    // =========================================================================
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
    },

    /// Cancel a blocking wait (timeout or disconnect).
    UnregisterWait {
        /// Connection ID to unregister.
        conn_id: u64,
    },

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

    /// Get WAL lag statistics from this shard.
    WalLagStats {
        /// Response channel.
        response_tx: oneshot::Sender<WalLagStatsResponse>,
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

    // =========================================================================
    // VLL (Very Lightweight Locking) messages
    // =========================================================================
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
        /// Channel to receive execute signal from coordinator.
        execute_rx: oneshot::Receiver<ExecuteSignal>,
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

    // =========================================================================
    // Cluster / Raft messages
    // =========================================================================
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

    /// Get VLL queue information from this shard.
    GetVllQueueInfo {
        /// Channel to send the response.
        response_tx: oneshot::Sender<VllQueueInfo>,
    },

    /// Get pub/sub limits info from this shard.
    GetPubSubLimitsInfo {
        /// Channel to send the response.
        response_tx: oneshot::Sender<PubSubLimitsInfo>,
    },

    /// Flush (commit) all dirty search indexes on this shard.
    /// Used by the snapshot coordinator to ensure search index consistency.
    FlushSearchIndexes { response_tx: oneshot::Sender<()> },

    /// Shutdown signal.
    Shutdown,
}

impl ShardMessage {
    /// Return a static string identifying the message variant, for USDT probes.
    pub fn probe_type_str(&self) -> &'static str {
        match self {
            ShardMessage::Execute { .. } => "Execute",
            ShardMessage::ScatterRequest { .. } => "ScatterRequest",
            ShardMessage::GetVersion { .. } => "GetVersion",
            ShardMessage::ExecTransaction { .. } => "ExecTransaction",
            ShardMessage::Subscribe { .. } => "Subscribe",
            ShardMessage::Unsubscribe { .. } => "Unsubscribe",
            ShardMessage::PSubscribe { .. } => "PSubscribe",
            ShardMessage::PUnsubscribe { .. } => "PUnsubscribe",
            ShardMessage::Publish { .. } => "Publish",
            ShardMessage::ShardedSubscribe { .. } => "ShardedSubscribe",
            ShardMessage::ShardedUnsubscribe { .. } => "ShardedUnsubscribe",
            ShardMessage::ShardedPublish { .. } => "ShardedPublish",
            ShardMessage::PubSubIntrospection { .. } => "PubSubIntrospection",
            ShardMessage::ConnectionClosed { .. } => "ConnectionClosed",
            ShardMessage::TrackingRegister { .. } => "TrackingRegister",
            ShardMessage::TrackingUnregister { .. } => "TrackingUnregister",
            ShardMessage::EvalScript { .. } => "EvalScript",
            ShardMessage::EvalScriptSha { .. } => "EvalScriptSha",
            ShardMessage::ScriptLoad { .. } => "ScriptLoad",
            ShardMessage::ScriptExists { .. } => "ScriptExists",
            ShardMessage::ScriptFlush { .. } => "ScriptFlush",
            ShardMessage::ScriptKill { .. } => "ScriptKill",
            ShardMessage::FunctionCall { .. } => "FunctionCall",
            ShardMessage::BlockWait { .. } => "BlockWait",
            ShardMessage::UnregisterWait { .. } => "UnregisterWait",
            ShardMessage::SlowlogGet { .. } => "SlowlogGet",
            ShardMessage::SlowlogLen { .. } => "SlowlogLen",
            ShardMessage::SlowlogReset { .. } => "SlowlogReset",
            ShardMessage::SlowlogAdd { .. } => "SlowlogAdd",
            ShardMessage::MemoryUsage { .. } => "MemoryUsage",
            ShardMessage::MemoryStats { .. } => "MemoryStats",
            ShardMessage::WalLagStats { .. } => "WalLagStats",
            ShardMessage::ScanBigKeys { .. } => "ScanBigKeys",
            ShardMessage::LatencyLatest { .. } => "LatencyLatest",
            ShardMessage::LatencyHistory { .. } => "LatencyHistory",
            ShardMessage::LatencyReset { .. } => "LatencyReset",
            ShardMessage::HotShardStats { .. } => "HotShardStats",
            ShardMessage::ResetStats { .. } => "ResetStats",
            ShardMessage::UpdateConfig { .. } => "UpdateConfig",
            ShardMessage::VllLockRequest { .. } => "VllLockRequest",
            ShardMessage::VllExecute { .. } => "VllExecute",
            ShardMessage::VllAbort { .. } => "VllAbort",
            ShardMessage::VllContinuationLock { .. } => "VllContinuationLock",
            ShardMessage::SlotMigrated { .. } => "SlotMigrated",
            ShardMessage::RaftCommand { .. } => "RaftCommand",
            ShardMessage::GetVllQueueInfo { .. } => "GetVllQueueInfo",
            ShardMessage::GetPubSubLimitsInfo { .. } => "GetPubSubLimitsInfo",
            ShardMessage::FlushSearchIndexes { .. } => "FlushSearchIndexes",
            ShardMessage::Shutdown => "Shutdown",
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
        /// The value type (e.g., "string", "hash", "list", "set", "zset", "hll", "json").
        value_type: Bytes,
        /// The serialized value data.
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
    /// FT.SEARCH - search the index on this shard.
    FtSearch {
        index_name: Bytes,
        query_args: Vec<Bytes>,
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
    /// FT.AGGREGATE - aggregate search results on this shard.
    FtAggregate {
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
    FtExplain {
        index_name: Bytes,
        query_str: Bytes,
    },
}
