//! WAL configuration types and policies.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WalFailurePolicy {
    #[default]
    Continue,
    Rollback,
}
#[derive(Debug, Clone)]
pub enum DurabilityMode {
    Async,
    Periodic { interval_ms: u64 },
    Sync,
}
impl Default for DurabilityMode {
    fn default() -> Self {
        Self::Periodic { interval_ms: 1000 }
    }
}
#[derive(Debug, Clone)]
pub struct WalConfig {
    pub mode: DurabilityMode,
    pub batch_size_threshold: usize,
    pub batch_timeout_ms: u64,
    pub channel_capacity: usize,
    pub failure_policy: WalFailurePolicy,
}
impl Default for WalConfig {
    fn default() -> Self {
        Self {
            mode: DurabilityMode::default(),
            batch_size_threshold: 4 * 1024 * 1024,
            batch_timeout_ms: 10,
            channel_capacity: 8192,
            failure_policy: WalFailurePolicy::Continue,
        }
    }
}
#[derive(Debug, Clone)]
pub struct WalLagStats {
    pub pending_ops: usize,
    pub pending_bytes: usize,
    pub durability_lag_ms: u64,
    pub sync_lag_ms: Option<u64>,
    pub sequence: u64,
    pub shard_id: usize,
    pub last_flush_timestamp_ms: u64,
    pub last_sync_timestamp_ms: Option<u64>,
}
