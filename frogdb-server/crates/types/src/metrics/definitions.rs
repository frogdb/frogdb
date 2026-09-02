//! Typed metric definitions.
//!
//! All FrogDB metrics are defined here using the `define_metrics!` macro.
//! Each declaration generates both the typed emission handle and the
//! `ALL_METRICS` registry entry, so a metric's name, type, help text, and
//! label schema cannot drift apart.
//!
//! Label schemas here describe what the server *actually emits* — if a call
//! site needs a different label set, change the definition and every emitter
//! together, never just one side.
//!
//! # Example Usage
//! ```ignore
//! use frogdb_types::metrics::definitions::CommandsTotal;
//!
//! CommandsTotal::inc(&*recorder, "GET");
//! ```

use super::labels::*;
use super::{MetricDefinition, MetricType};
use crate::traits::metrics::MetricsRecorder;
use frogdb_metrics_derive::define_metrics;

define_metrics! {
    // ========================================================================
    // System Metrics
    // ========================================================================

    /// Server uptime in seconds
    gauge UptimeSeconds("frogdb_uptime_seconds") {}

    /// Server information (constant 1, labeled with version and mode)
    gauge Info("frogdb_info") {
        labels: [version: &str, mode: &str],
    }

    /// Resident set size memory in bytes
    gauge MemoryRssBytes("frogdb_memory_rss_bytes") {}

    /// Cumulative user CPU time in seconds (monotonic; sampled from getrusage)
    gauge CpuUserSeconds("frogdb_cpu_user_seconds_total") {}

    /// Cumulative system CPU time in seconds (monotonic; sampled from getrusage)
    gauge CpuSystemSeconds("frogdb_cpu_system_seconds_total") {}

    // ========================================================================
    // Connection Metrics
    // ========================================================================

    /// Total connections accepted
    counter ConnectionsTotal("frogdb_connections_total") {}

    /// Current number of connected clients
    gauge ConnectionsCurrent("frogdb_connections_current") {}

    /// Maximum configured connections
    gauge ConnectionsMax("frogdb_connections_max") {}

    /// Total rejected connections
    counter ConnectionsRejected("frogdb_connections_rejected_total") {
        labels: [reason: RejectionReason],
    }

    /// Total TLS handshake failures
    counter TlsHandshakeErrors("frogdb_tls_handshake_errors_total") {
        labels: [reason: TlsHandshakeError],
    }

    // ========================================================================
    // Command Metrics
    // ========================================================================

    /// Total commands executed
    counter CommandsTotal("frogdb_commands_total") {
        labels: [command: &str],
    }

    /// Command execution duration in seconds
    histogram CommandsDuration("frogdb_commands_duration_seconds") {
        labels: [command: &str],
    }

    /// Total command errors
    counter CommandsErrors("frogdb_commands_errors_total") {
        labels: [command: &str, error: &str],
    }

    // ========================================================================
    // Keyspace Metrics
    // ========================================================================

    /// Number of keys per shard
    gauge KeysTotal("frogdb_keys_total") {
        labels: [shard: &str],
    }

    /// Number of keys with expiry set per shard
    gauge KeysWithExpiry("frogdb_keys_with_expiry") {
        labels: [shard: &str],
    }

    /// Total keys expired
    counter KeysExpired("frogdb_keys_expired_total") {
        labels: [shard: &str],
    }

    /// Total hash fields expired
    counter FieldsExpired("frogdb_fields_expired_total") {
        labels: [shard: &str],
    }

    /// Total keyspace cache hits
    counter KeyspaceHits("frogdb_keyspace_hits_total") {}

    /// Total keyspace cache misses
    counter KeyspaceMisses("frogdb_keyspace_misses_total") {}

    /// Total keyspace notifications dropped (coordinator channel full)
    counter KeyspaceNotificationsDropped("frogdb_keyspace_notifications_dropped_total") {
        labels: [shard: &str],
    }

    // ========================================================================
    // Shard Metrics
    // ========================================================================

    /// Number of keys per shard
    gauge ShardKeys("frogdb_shard_keys") {
        labels: [shard: &str],
    }

    /// Memory usage per shard in bytes
    gauge ShardMemoryBytes("frogdb_shard_memory_bytes") {
        labels: [shard: &str],
    }

    /// Queue depth per shard
    gauge ShardQueueDepth("frogdb_shard_queue_depth") {
        labels: [shard: &str],
    }

    /// Shard queue latency in seconds
    histogram ShardQueueLatency("frogdb_shard_queue_latency_seconds") {
        labels: [shard: &str],
    }

    /// Total panics caught and isolated at the shard boundary, by isolation site.
    /// A non-zero value is always a bug: the shard survived, but a client saw
    /// `-ERR internal error` instead of an answer. Alert on the rate.
    counter ShardPanicsIsolated("frogdb_shard_panics_isolated_total") {
        labels: [shard: &str, site: &str],
    }

    // ========================================================================
    // Persistence Metrics (WAL)
    // ========================================================================

    /// Total WAL writes
    counter WalWrites("frogdb_wal_writes_total") {
        labels: [shard: &str],
    }

    /// Total WAL bytes written
    counter WalBytes("frogdb_wal_bytes_total") {
        labels: [shard: &str],
    }

    /// WAL flush duration in seconds
    histogram WalFlushDuration("frogdb_wal_flush_duration_seconds") {
        labels: [shard: &str],
    }

    /// Number of pending WAL operations
    gauge WalPendingOps("frogdb_wal_pending_ops") {
        labels: [shard: &str],
    }

    /// Number of pending WAL bytes
    gauge WalPendingBytes("frogdb_wal_pending_bytes") {
        labels: [shard: &str],
    }

    /// WAL durability lag in milliseconds
    gauge WalDurabilityLagMs("frogdb_wal_durability_lag_ms") {
        labels: [shard: &str],
    }

    /// Timestamp of last WAL flush (unix milliseconds)
    gauge WalLastFlushTimestamp("frogdb_wal_last_flush_timestamp") {
        labels: [shard: &str],
    }

    /// Total failed WAL flush attempts
    counter WalFlushFailures("frogdb_wal_flush_failures_total") {
        labels: [shard: &str],
    }

    /// Total WAL entries dropped in failed flushes (permanent losses)
    counter WalLostOps("frogdb_wal_lost_ops_total") {
        labels: [shard: &str],
    }

    /// Total estimated bytes dropped in failed WAL flushes
    counter WalLostBytes("frogdb_wal_lost_bytes_total") {
        labels: [shard: &str],
    }

    /// Whether the most recent WAL flush attempt succeeded (1 = ok)
    gauge WalLastFlushOk("frogdb_wal_last_flush_ok") {
        labels: [shard: &str],
    }

    /// Total store mutations rolled back after a WAL append failure
    counter WalRollbacks("frogdb_wal_rollbacks_total") {}

    // Non-zero means point-in-time WAL recovery hit a corrupt mid-log record and
    // truncated the durable suffix, silently dropping acknowledged writes. The
    // value is the gap between the durable-sync sequence watermark recorded
    // before the crash and the sequence RocksDB actually recovered to — see the
    // `rocks::wal_watermark` module in `frogdb-persistence`. Kept as an
    // implementation comment: the doc comment below is the operator-facing
    // Prometheus HELP string.
    /// Total committed records dropped by point-in-time WAL recovery on corruption
    counter WalRecoveryDroppedRecords("frogdb_wal_recovery_dropped_records_total") {}

    // Raised at boot by startup recovery when a stored value cannot be
    // deserialized. Skipping such a key is deliberate (one bad value must not
    // take the whole keyspace down), so this counter is the only positive
    // signal that a boot lost keys — the keyspace size alone cannot say whether
    // it shrank. A *wholly* undecodable database refuses to start instead, so a
    // running server with a non-zero value here decoded at least one key.
    /// Total keys skipped at startup because their stored value failed to deserialize
    counter RecoveryKeysFailed("frogdb_recovery_keys_failed_total") {}

    // Raised at boot when a persisted function library does not come back:
    // `functions.fdb` unreadable or corrupt (the whole file is downgraded to
    // "no functions"), or a single library that fails to parse or to register.
    // Startup tolerates all three deliberately — a stored script must not keep
    // the keyspace offline — so, exactly as with `RecoveryKeysFailed` above,
    // this counter is the only positive signal that `FUNCTION LIST` came back
    // smaller than what was saved.
    /// Total function libraries lost at startup because the file or the library failed to load
    counter RecoveryFunctionsFailed("frogdb_recovery_functions_failed_total") {}

    /// Total HyperLogLog register-delta operands persisted as WAL merges
    /// (dense-HLL PFADD writes that took the merge-delta path instead of a
    /// full value Put).
    counter WalMergeOperands("frogdb_wal_merge_operands_total") {}

    /// Total post-clear space-reclamation passes started (a FLUSHDB/FLUSHALL
    /// range tombstone was followed by an async DeleteFilesInRange + CompactRange
    /// over the cleared column family). Only counts passes that actually began;
    /// a reclamation coalesced away because one was already in flight for the
    /// same shard/tier does not increment this.
    counter FlushCompactStarted("frogdb_flush_compact_started_total") {
        labels: [shard: &str],
    }

    /// Total post-clear space-reclamation passes that finished executing
    /// (the compaction routine returned). Pairs with
    /// frogdb_flush_compact_started_total; a persistent gap between the two
    /// indicates reclamation passes still running or a stuck compaction.
    counter FlushCompactCompleted("frogdb_flush_compact_completed_total") {
        labels: [shard: &str],
    }

    // ========================================================================
    // Persistence Metrics (Snapshot)
    // ========================================================================

    /// Whether a snapshot is currently in progress
    gauge SnapshotInProgress("frogdb_snapshot_in_progress") {}

    /// Monotonic epoch of the most recent snapshot attempt
    gauge SnapshotEpoch("frogdb_snapshot_epoch") {}

    /// Timestamp of last successful snapshot (unix seconds)
    gauge SnapshotLastTimestamp("frogdb_snapshot_last_timestamp") {}

    /// Snapshot duration in seconds
    histogram SnapshotDuration("frogdb_snapshot_duration_seconds") {}

    /// Snapshot size in bytes
    gauge SnapshotSizeBytes("frogdb_snapshot_size_bytes") {}

    /// Total persistence errors
    counter PersistenceErrors("frogdb_persistence_errors_total") {
        labels: [error_type: PersistenceErrorType],
    }

    // ========================================================================
    // Pub/Sub Metrics
    // ========================================================================

    /// Number of active pub/sub channels per shard
    gauge PubsubChannels("frogdb_pubsub_channels") {
        labels: [shard: &str],
    }

    /// Number of active pub/sub patterns per shard
    gauge PubsubPatterns("frogdb_pubsub_patterns") {
        labels: [shard: &str],
    }

    /// Total pub/sub subscribers per shard
    gauge PubsubSubscribers("frogdb_pubsub_subscribers") {
        labels: [shard: &str],
    }

    /// Total pub/sub messages published
    counter PubsubMessages("frogdb_pubsub_messages_total") {
        labels: [shard: &str],
    }

    /// Total warnings for pub/sub resources approaching per-shard limits
    counter PubsubShardLimitWarnings("frogdb_pubsub_shard_limit_warnings_total") {
        labels: [resource: PubsubLimitResource],
    }

    /// Total subscriber connections disconnected for exceeding their pub/sub
    /// output-buffer hard limit (slow / non-reading subscribers).
    counter PubsubOutputBufferDisconnects("frogdb_pubsub_output_buffer_disconnects_total") {}

    /// Total client connections disconnected by the `client-output-buffer-limit`
    /// seam. `class` is the Redis limit class (`normal`, `replica`, `pubsub`);
    /// `reason` is `hard_limit`, `soft_limit`, or `budget_refused` (the core's
    /// `NetworkOutput` budget shed the connection).
    counter ClientOutputBufferDisconnects("frogdb_client_output_buffer_disconnects_total") {
        labels: [class: &str, reason: &str],
    }

    // ========================================================================
    // Memory/Eviction Metrics
    // ========================================================================

    /// Currently used memory in bytes per shard
    gauge MemoryUsedBytes("frogdb_memory_used_bytes") {
        labels: [shard: &str],
    }

    /// Maximum memory limit in bytes
    gauge MemoryMaxmemoryBytes("frogdb_memory_maxmemory_bytes") {}

    /// Peak memory usage in bytes per shard
    gauge MemoryPeakBytes("frogdb_memory_peak_bytes") {
        labels: [shard: &str],
    }

    /// Memory fragmentation ratio (RSS / used)
    gauge MemoryFragmentationRatio("frogdb_memory_fragmentation_ratio") {}

    /// jemalloc `stats.allocated`: bytes allocated by the application
    gauge AllocatorAllocatedBytes("frogdb_allocator_allocated_bytes") {}

    /// jemalloc `stats.active`: bytes in active pages allocated by the
    /// application
    gauge AllocatorActiveBytes("frogdb_allocator_active_bytes") {}

    /// jemalloc `stats.resident`: bytes physically resident (metadata +
    /// active pages + unused dirty pages)
    gauge AllocatorResidentBytes("frogdb_allocator_resident_bytes") {}

    /// Allocator fragmentation ratio (allocator active / allocator
    /// allocated)
    gauge AllocatorFragRatio("frogdb_allocator_frag_ratio") {}

    // ------------------------------------------------------------------
    // Per-core memory broker: the subsystem budget breakdown (ADR-0006 §2).
    // `subsystem` is `frogdb_memory::Subsystem::as_str()` — a closed set of
    // stable names, deliberately not derived from the Rust type that owns
    // the buffer.
    // ------------------------------------------------------------------

    /// Bytes a subsystem's `Budget` currently authorizes on a core. This is
    /// what the broker charged, not an allocator reading.
    gauge MemoryBudgetChargedBytes("frogdb_memory_budget_charged_bytes") {
        labels: [shard: &str, subsystem: &str],
    }

    /// A subsystem `Budget`'s limit on a core. Zero for a subsystem that has
    /// not been converted to charge yet.
    gauge MemoryBudgetLimitBytes("frogdb_memory_budget_limit_bytes") {
        labels: [shard: &str, subsystem: &str],
    }

    /// Charges a subsystem's `Budget` refused. Every refusal is handled at
    /// that subsystem's seam by shedding or backpressuring.
    counter MemoryBudgetRefusalsTotal("frogdb_memory_budget_refusals_total") {
        labels: [shard: &str, subsystem: &str],
    }

    // ------------------------------------------------------------------
    // RocksDB's own memory accounting (memory-architecture R10). These are
    // read from the engine — the block cache and write-buffer manager it
    // was given — not from the broker, so they are what a `Budget` charge
    // is checked *against*. One process-wide cache and one write-buffer
    // manager, hence no `shard` label.
    // ------------------------------------------------------------------

    /// Bytes RocksDB's process-wide `WriteBufferManager` currently accounts
    /// for across every column family's memtables.
    gauge RocksdbWriteBufferBytes("frogdb_rocksdb_write_buffer_bytes") {}

    /// The `WriteBufferManager`'s ceiling: `write_buffer_size *
    /// max_write_buffer_number`. Writes stall above it rather than
    /// allocating past it.
    gauge RocksdbWriteBufferLimitBytes("frogdb_rocksdb_write_buffer_limit_bytes") {}

    /// Bytes charged to RocksDB's process-wide block cache. With the
    /// write-buffer manager built against the cache, this covers memtables
    /// too, so it is the engine's whole tracked footprint.
    gauge RocksdbBlockCacheBytes("frogdb_rocksdb_block_cache_bytes") {}

    /// The block cache's capacity: `block_cache_size + write_buffer_size *
    /// max_write_buffer_number`. Zero when the operator left
    /// `block_cache_size` at 0 and RocksDB's own default cache is in use.
    gauge RocksdbBlockCacheCapacityBytes("frogdb_rocksdb_block_cache_capacity_bytes") {}

    /// Block-cache bytes pinned by live iterators and table readers —
    /// unevictable, so the gap to the capacity is the cache's real headroom.
    gauge RocksdbBlockCachePinnedBytes("frogdb_rocksdb_block_cache_pinned_bytes") {}

    /// Tracked keys the client-tracking table shed (evicted and invalidated)
    /// because its `Budget` refused a charge — the declared `shed`
    /// disposition, made observable.
    counter TrackingTableBudgetShedKeys("frogdb_tracking_table_budget_shed_keys_total") {
        labels: [shard: &str],
    }

    /// Reads the client-tracking table declined to record because even an
    /// empty table could not fit them (a budget smaller than one entry).
    counter TrackingTableBudgetDeclines("frogdb_tracking_table_budget_declines_total") {
        labels: [shard: &str],
    }

    /// Upper bound on the bytes one shard has allocated, from the jemalloc
    /// arena bound to that shard's thread (`stats.arenas.<i>.{small,large}`).
    ///
    /// An upper bound, not a live figure: freed bytes stay charged until the
    /// thread cache flushes them. Absent — not zero — for a shard with no
    /// arena of its own.
    gauge AllocatorShardAllocatedBytes("frogdb_allocator_shard_allocated_bytes") {
        labels: [shard: &str],
    }

    /// Bytes physically resident in one shard's jemalloc arena
    /// (`stats.arenas.<i>.resident`)
    gauge AllocatorShardResidentBytes("frogdb_allocator_shard_resident_bytes") {
        labels: [shard: &str],
    }

    /// Per-shard fragmentation ratio (arena resident / arena allocated)
    gauge AllocatorShardFragRatio("frogdb_allocator_shard_frag_ratio") {
        labels: [shard: &str],
    }

    /// Total keys evicted
    counter EvictionKeysTotal("frogdb_eviction_keys_total") {
        labels: [shard: &str, policy: &str],
    }

    /// Total bytes evicted
    counter EvictionBytesTotal("frogdb_eviction_bytes_total") {
        labels: [shard: &str],
    }

    /// Total keys sampled by the eviction policy
    counter EvictionSamplesTotal("frogdb_eviction_samples_total") {
        labels: [shard: &str, policy: &str],
    }

    /// Total out-of-memory rejections
    counter EvictionOomTotal("frogdb_eviction_oom_total") {
        labels: [shard: &str],
    }

    /// Total keys spilled to the cold tier instead of evicted
    counter TieredSpills("frogdb_tiered_spills_total") {
        labels: [shard: &str, policy: &str],
    }

    /// Total bytes freed by spilling keys to the cold tier
    counter TieredBytesSpilled("frogdb_tiered_bytes_spilled_total") {
        labels: [shard: &str],
    }

    // ========================================================================
    // Blocking Commands Metrics
    // ========================================================================

    /// Number of currently blocked clients per shard
    gauge BlockedClients("frogdb_blocked_clients") {
        labels: [shard: &str],
    }

    /// Number of keys being watched for blocking per shard
    gauge BlockedKeys("frogdb_blocked_keys") {
        labels: [shard: &str],
    }

    /// Total blocking commands timed out
    counter BlockedTimeoutTotal("frogdb_blocked_timeout_total") {
        labels: [shard: &str],
    }

    /// Total blocking commands satisfied
    counter BlockedSatisfiedTotal("frogdb_blocked_satisfied_total") {
        labels: [shard: &str],
    }

    /// Total blocked clients redirected with -MOVED after slot migration
    counter BlockedMigrationMoved("frogdb_blocked_migration_moved_total") {
        labels: [shard: &str],
    }

    // ========================================================================
    // Lua Scripting Metrics
    // ========================================================================

    /// Total Lua scripts executed
    counter LuaScriptsTotal("frogdb_lua_scripts_total") {
        labels: [shard: &str, kind: ScriptKind],
    }

    /// Lua script execution duration in seconds
    histogram LuaScriptsDuration("frogdb_lua_scripts_duration_seconds") {
        labels: [shard: &str, kind: ScriptKind],
    }

    /// Total Lua script errors
    counter LuaScriptsErrors("frogdb_lua_scripts_errors_total") {
        labels: [shard: &str, error: ScriptError],
    }

    /// Total Lua script cache hits
    counter LuaScriptsCacheHits("frogdb_lua_scripts_cache_hits_total") {
        labels: [shard: &str],
    }

    /// Total Lua script cache misses
    counter LuaScriptsCacheMisses("frogdb_lua_scripts_cache_misses_total") {
        labels: [shard: &str],
    }

    // ========================================================================
    // Transaction Metrics
    // ========================================================================

    /// Total transactions by outcome
    counter TransactionsTotal("frogdb_transactions_total") {
        labels: [outcome: &str],
    }

    /// Transaction duration in seconds
    histogram TransactionsDuration("frogdb_transactions_duration_seconds") {
        labels: [outcome: &str],
    }

    /// Number of queued commands in transactions
    histogram TransactionsQueuedCommands("frogdb_transactions_queued_commands") {
        labels: [outcome: &str],
    }

    /// Total WATCH-aborted transactions by what invalidated the watch:
    /// `watched-slot-write` (a write moved the watched key's Hash Slot version --
    /// a write to the key, to any key aliased onto its slot, or a keyless
    /// dirtying write such as FLUSHDB) or `expiry` (a key that was live at WATCH
    /// time is gone at EXEC). A CAS loop that never commits is diagnosed here.
    counter TransactionsWatchAborted("frogdb_transactions_watch_aborted_total") {
        labels: [reason: &str],
    }

    // ========================================================================
    // Scatter-Gather Metrics
    // ========================================================================

    /// Total scatter-gather operations
    counter ScatterGatherTotal("frogdb_scatter_gather_total") {
        labels: [command: &str, status: &str],
    }

    /// Scatter-gather operation duration in seconds
    histogram ScatterGatherDuration("frogdb_scatter_gather_duration_seconds") {
        labels: [command: &str],
    }

    /// Number of shards involved in scatter-gather
    histogram ScatterGatherShards("frogdb_scatter_gather_shards") {
        labels: [command: &str],
    }

    // ========================================================================
    // Split-Brain Metrics
    // ========================================================================

    /// Total split-brain events detected
    counter SplitBrainEventsTotal("frogdb_split_brain_events_total") {}

    /// Total operations discarded during split-brain recovery
    counter SplitBrainOpsDiscardedTotal("frogdb_split_brain_ops_discarded_total") {}

    /// Whether an unprocessed split-brain log file exists (1 = yes, 0 = no)
    gauge SplitBrainRecoveryPending("frogdb_split_brain_recovery_pending") {}

    /// Split-brain divergence records that could not be written to disk. The
    /// divergent writes are gone with no durable trace, so this is a data-loss
    /// signal, not a logging nit — and it is the only one, since
    /// `frogdb_split_brain_recovery_pending` deliberately stays down when there
    /// is no file for an operator to reconcile.
    counter SplitBrainLogWriteFailuresTotal("frogdb_split_brain_log_write_failures_total") {}

    // ========================================================================
    // Latency Metrics
    // ========================================================================

    /// Cumulative requests at or under each configured latency band, flushed
    /// from the band tracker at scrape time
    gauge LatencyBandRequests("frogdb_latency_band_requests_total") {
        labels: [le: &str],
    }

    // ========================================================================
    // Version / Rolling Upgrade Metrics
    // ========================================================================

    /// Binary version of this node (info gauge, always 1)
    gauge BinaryVersion("frogdb_binary_version") {
        labels: [version: &str],
    }

    /// Current active (finalized) version (info gauge, always 1)
    gauge ActiveVersion("frogdb_active_version") {
        labels: [version: &str],
    }

    /// Whether nodes report different binary versions (1 = mixed, 0 = uniform)
    gauge ClusterMixedVersion("frogdb_cluster_mixed_version") {}

    /// Whether a specific version gate is active (1) or suppressed (0)
    gauge VersionGateActive("frogdb_version_gate_active") {
        labels: [gate: &str],
    }

    // ========================================================================
    // Tokio Task Monitor Metrics
    // ========================================================================

    /// Number of tasks instrumented by this monitor in the last interval
    gauge TaskInstrumentedCount("frogdb_task_instrumented_count") {
        labels: [task: &str],
    }

    /// Number of instrumented tasks dropped in the last interval
    gauge TaskDroppedCount("frogdb_task_dropped_count") {
        labels: [task: &str],
    }

    /// Total task poll duration in seconds over the last interval
    gauge TaskTotalPollDuration("frogdb_task_total_poll_duration_seconds") {
        labels: [task: &str],
    }

    /// Total time tasks spent scheduled (runnable but waiting) in seconds over the last interval
    gauge TaskTotalScheduledDuration("frogdb_task_total_scheduled_duration_seconds") {
        labels: [task: &str],
    }

    /// Total time tasks spent idle in seconds over the last interval
    gauge TaskTotalIdleDuration("frogdb_task_total_idle_duration_seconds") {
        labels: [task: &str],
    }

    /// Mean task poll duration in seconds over the last interval
    gauge TaskMeanPollDuration("frogdb_task_mean_poll_duration_seconds") {
        labels: [task: &str],
    }
}
