//! Typed metric definitions.
//!
//! All FrogDB metrics are defined here using the `define_metrics!` macro.
//! This provides:
//! - Compile-time type safety for metric names and labels
//! - Auto-generated `ALL_METRICS` registry for introspection
//! - Type-safe recording methods for each metric
//!
//! # Example Usage
//! ```ignore
//! use frogdb_telemetry::definitions::CommandsTotal;
//!
//! CommandsTotal::inc(&*recorder, "GET");
//! ```

use crate::labels::*;
use frogdb_metrics_derive::define_metrics;

define_metrics! {
    // ========================================================================
    // System Metrics
    // ========================================================================

    /// Server uptime in seconds
    gauge UptimeSeconds("frogdb_uptime_seconds") {}

    /// Server information
    gauge Info("frogdb_info") {
        labels: [version: &str, mode: ServerMode],
    }

    /// Resident set size memory in bytes
    gauge MemoryRssBytes("frogdb_memory_rss_bytes") {}

    /// Total user CPU time in seconds
    counter CpuUserSeconds("frogdb_cpu_user_seconds_total") {}

    /// Total system CPU time in seconds
    counter CpuSystemSeconds("frogdb_cpu_system_seconds_total") {}

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
        labels: [command: &str, error: ErrorType],
    }

    // ========================================================================
    // Keyspace Metrics
    // ========================================================================

    /// Total number of keys
    gauge KeysTotal("frogdb_keys_total") {}

    /// Number of keys with expiry set
    gauge KeysWithExpiry("frogdb_keys_with_expiry") {}

    /// Total keys expired
    counter KeysExpired("frogdb_keys_expired_total") {}

    /// Total keyspace cache hits
    counter KeyspaceHits("frogdb_keyspace_hits_total") {}

    /// Total keyspace cache misses
    counter KeyspaceMisses("frogdb_keyspace_misses_total") {}

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
    counter WalWrites("frogdb_wal_writes_total") {}

    /// Total WAL bytes written
    counter WalBytes("frogdb_wal_bytes_total") {}

    /// WAL flush duration in seconds
    histogram WalFlushDuration("frogdb_wal_flush_duration_seconds") {}

    /// Number of pending WAL operations
    gauge WalPendingOps("frogdb_wal_pending_ops") {}

    /// Number of pending WAL bytes
    gauge WalPendingBytes("frogdb_wal_pending_bytes") {}

    /// WAL durability lag in milliseconds
    gauge WalDurabilityLagMs("frogdb_wal_durability_lag_ms") {}

    /// WAL sync lag in milliseconds
    gauge WalSyncLagMs("frogdb_wal_sync_lag_ms") {}

    /// Timestamp of last WAL flush
    gauge WalLastFlushTimestamp("frogdb_wal_last_flush_timestamp") {}

    /// Timestamp of last WAL sync
    gauge WalLastSyncTimestamp("frogdb_wal_last_sync_timestamp") {}

    // ========================================================================
    // Persistence Metrics (Snapshot)
    // ========================================================================

    /// Whether a snapshot is currently in progress
    gauge SnapshotInProgress("frogdb_snapshot_in_progress") {}

    /// Timestamp of last successful snapshot
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

    /// Number of active pub/sub channels
    gauge PubsubChannels("frogdb_pubsub_channels") {}

    /// Number of active pub/sub patterns
    gauge PubsubPatterns("frogdb_pubsub_patterns") {}

    /// Total pub/sub subscribers
    gauge PubsubSubscribers("frogdb_pubsub_subscribers") {}

    /// Total pub/sub messages published
    counter PubsubMessages("frogdb_pubsub_messages_total") {}

    // ========================================================================
    // Network Metrics
    // ========================================================================

    /// Total network input bytes
    counter NetInputBytes("frogdb_net_input_bytes_total") {}

    /// Total network output bytes
    counter NetOutputBytes("frogdb_net_output_bytes_total") {}

    // ========================================================================
    // Memory/Eviction Metrics
    // ========================================================================

    /// Currently used memory in bytes
    gauge MemoryUsedBytes("frogdb_memory_used_bytes") {}

    /// Maximum memory limit in bytes
    gauge MemoryMaxmemoryBytes("frogdb_memory_maxmemory_bytes") {}

    /// Peak memory usage in bytes
    gauge MemoryPeakBytes("frogdb_memory_peak_bytes") {}

    /// Memory fragmentation ratio
    gauge MemoryFragmentationRatio("frogdb_memory_fragmentation_ratio") {}

    /// Total keys evicted
    counter EvictionKeysTotal("frogdb_eviction_keys_total") {}

    /// Total bytes evicted
    counter EvictionBytesTotal("frogdb_eviction_bytes_total") {}

    /// Total eviction samples
    counter EvictionSamplesTotal("frogdb_eviction_samples_total") {}

    /// Total out-of-memory rejections
    counter EvictionOomTotal("frogdb_eviction_oom_total") {}

    // ========================================================================
    // Blocking Commands Metrics
    // ========================================================================

    /// Number of currently blocked clients
    gauge BlockedClients("frogdb_blocked_clients") {}

    /// Number of keys being watched for blocking
    gauge BlockedKeys("frogdb_blocked_keys") {}

    /// Total blocking commands timed out
    counter BlockedTimeoutTotal("frogdb_blocked_timeout_total") {}

    /// Total blocking commands satisfied
    counter BlockedSatisfiedTotal("frogdb_blocked_satisfied_total") {}

    // ========================================================================
    // Lua Scripting Metrics
    // ========================================================================

    /// Total Lua scripts executed
    counter LuaScriptsTotal("frogdb_lua_scripts_total") {}

    /// Lua script execution duration in seconds
    histogram LuaScriptsDuration("frogdb_lua_scripts_duration_seconds") {}

    /// Total Lua script errors
    counter LuaScriptsErrors("frogdb_lua_scripts_errors_total") {}

    /// Total Lua script cache hits
    counter LuaScriptsCacheHits("frogdb_lua_scripts_cache_hits_total") {}

    /// Total Lua script cache misses
    counter LuaScriptsCacheMisses("frogdb_lua_scripts_cache_misses_total") {}

    // ========================================================================
    // Transaction Metrics
    // ========================================================================

    /// Total transactions by outcome
    counter TransactionsTotal("frogdb_transactions_total") {
        labels: [outcome: TransactionOutcome],
    }

    /// Transaction duration in seconds
    histogram TransactionsDuration("frogdb_transactions_duration_seconds") {}

    /// Number of queued commands in transactions
    histogram TransactionsQueuedCommands("frogdb_transactions_queued_commands") {}

    // ========================================================================
    // Scatter-Gather Metrics
    // ========================================================================

    /// Total scatter-gather operations
    counter ScatterGatherTotal("frogdb_scatter_gather_total") {
        labels: [status: ScatterGatherStatus],
    }

    /// Scatter-gather operation duration in seconds
    histogram ScatterGatherDuration("frogdb_scatter_gather_duration_seconds") {}

    /// Number of shards involved in scatter-gather
    histogram ScatterGatherShards("frogdb_scatter_gather_shards") {}

    // ========================================================================
    // Latency Metrics
    // ========================================================================

    /// Requests by latency band
    counter LatencyBandRequests("frogdb_latency_band_requests_total") {
        labels: [band: &str],
    }
}
