use frogdb_core::persistence::{CompressionType, DurabilityMode, WalConfig};
use frogdb_core::sync::{Arc, AtomicU64, Ordering};
use frogdb_core::{EvictionConfig, EvictionPolicy, ExpiryIndex, HashMapStore};
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

/// Result type for persistence initialization.
pub type PersistenceInitResult = (
    Arc<frogdb_core::persistence::RocksStore>,
    Vec<(HashMapStore, ExpiryIndex)>,
    Option<crate::net::JoinHandle<()>>,
);

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

    WalConfig {
        mode,
        batch_size_threshold: config.batch_size_threshold_kb * 1024,
        batch_timeout_ms: config.batch_timeout_ms,
        ..Default::default()
    }
}

/// Build eviction config from memory config.
pub fn build_eviction_config(config: &MemoryConfig) -> EvictionConfig {
    let policy = config
        .maxmemory_policy
        .parse::<EvictionPolicy>()
        .unwrap_or_else(|_| {
            unreachable!(
                "Invalid eviction policy '{}' should have been caught by validation",
                config.maxmemory_policy
            )
        });

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
