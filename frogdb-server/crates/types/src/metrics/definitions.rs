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

    /// Total keys demoted to the cold tier instead of evicted
    counter TieredDemotions("frogdb_tiered_demotions_total") {
        labels: [shard: &str, policy: &str],
    }

    /// Total bytes freed by demoting keys to the cold tier
    counter TieredBytesDemoted("frogdb_tiered_bytes_demoted_total") {
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
