use frogdb_core::persistence::{CompressionType, DurabilityMode, WalConfig};
use frogdb_core::sync::{AtomicU64, Ordering};
use frogdb_core::{EvictionConfig, EvictionPolicy};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tokio::signal;

use crate::config::{MemoryConfig, PersistenceConfig};

/// Channel capacity for shard message queues.
pub const SHARD_CHANNEL_CAPACITY: usize = 1024;

/// Channel capacity for new connection queues.
pub const NEW_CONN_CHANNEL_CAPACITY: usize = 256;

/// Global connection ID counter.
static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

/// Global transaction ID counter for VLL (Very Lightweight Locking).
static NEXT_TXID: AtomicU64 = AtomicU64::new(1);

/// Generate a unique connection ID.
pub fn next_conn_id() -> u64 {
    NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed)
}

/// Generate a unique transaction ID for scatter-gather operations.
pub fn next_txid() -> u64 {
    NEXT_TXID.fetch_add(1, Ordering::SeqCst)
}

/// Generate a deterministic node_id from a socket address.
pub fn hash_addr_to_node_id(addr: &std::net::SocketAddr) -> u64 {
    let mut hasher = DefaultHasher::new();
    addr.hash(&mut hasher);
    hasher.finish()
}

/// Parse compression type from config string.
pub fn parse_compression(s: &str) -> CompressionType {
    match s.to_lowercase().as_str() {
        "none" => CompressionType::None,
        "snappy" => CompressionType::Snappy,
        "lz4" => CompressionType::Lz4,
        "zstd" => CompressionType::Zstd,
        _ => unreachable!(
            "Invalid compression '{}' should have been caught by validation",
            s
        ),
    }
}

/// Build WAL config from persistence config.
pub fn build_wal_config(config: &PersistenceConfig) -> WalConfig {
    let mode = match config.durability_mode.to_lowercase().as_str() {
        "async" => DurabilityMode::Async,
        "periodic" => DurabilityMode::Periodic {
            interval_ms: config.sync_interval_ms,
        },
        "sync" => DurabilityMode::Sync,
        _ => unreachable!(
            "Invalid durability_mode '{}' should have been caught by validation",
            config.durability_mode
        ),
    };

    // Config-consistency diagnostic. The *effective* WAL failure policy is owned
    // by the `ConfigManager` `Arc<AtomicU8>` (derived directly from
    // `config.persistence.wal_failure_policy` and mutable via
    // `CONFIG SET wal-failure-policy`); `WalConfig` no longer carries a duplicate
    // copy. This warning is a pure startup consistency check between the
    // configured failure policy and the durability mode — it does not compute or
    // store the policy.
    warn_if_rollback_without_sync(config, &mode);

    WalConfig {
        mode,
        batch_size_threshold: config.batch_size_threshold_kb * 1024,
        batch_timeout_ms: config.batch_timeout_ms,
        ..Default::default()
    }
}

/// Startup consistency check: `wal_failure_policy=rollback` only catches
/// flush-thread crashes in non-`sync` durability modes, so a `rollback` policy
/// paired with a non-`sync` mode is worth flagging. Reads the raw config string
/// (`config.wal_failure_policy`) against the resolved [`DurabilityMode`]; emits a
/// warning only, never affecting behavior.
fn warn_if_rollback_without_sync(config: &PersistenceConfig, mode: &DurabilityMode) {
    let is_rollback = config.wal_failure_policy.eq_ignore_ascii_case("rollback");
    if is_rollback && !matches!(mode, DurabilityMode::Sync) {
        let mode_str = match mode {
            DurabilityMode::Async => "async",
            DurabilityMode::Periodic { .. } => "periodic",
            DurabilityMode::Sync => "sync",
        };
        tracing::warn!(
            "wal_failure_policy=rollback with durability_mode={}: \
             rollback only catches flush-thread crashes in non-sync modes; \
             for full durability guarantees, use durability_mode=sync",
            mode_str
        );
    }
}

/// Build eviction config from memory config.
///
/// The policy string is validated against `EvictionPolicy` at startup (config
/// loader), so this parse always succeeds; it defaults to `NoEviction`
/// defensively otherwise — the same failure mode as the runtime propagation
/// path, rather than a divergent panic.
pub fn build_eviction_config(config: &MemoryConfig) -> EvictionConfig {
    let policy = config
        .maxmemory_policy
        .parse::<EvictionPolicy>()
        .unwrap_or_default();

    EvictionConfig {
        maxmemory: config.maxmemory,
        policy,
        maxmemory_samples: config.maxmemory_samples,
        lfu_log_factor: config.lfu_log_factor,
        lfu_decay_time: config.lfu_decay_time,
    }
}

/// Helper module for CPU count.
pub mod num_cpus {
    pub fn get() -> usize {
        std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1)
    }
}

/// Wait for a shutdown signal (SIGTERM or SIGINT).
pub async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
