//! WAL configuration types and policies.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WalFailurePolicy {
    #[default]
    Continue,
    Rollback,
}

impl WalFailurePolicy {
    /// Stable numeric encoding, used for the shared `AtomicU8` runtime flag
    /// that shard workers and `CONFIG SET wal-failure-policy` both observe.
    /// This is the single owner of the mapping — do not hand-roll `0`/`1`
    /// comparisons at call sites.
    pub const fn as_u8(self) -> u8 {
        match self {
            WalFailurePolicy::Continue => 0,
            WalFailurePolicy::Rollback => 1,
        }
    }

    /// Decode the numeric encoding. Unknown values fall back to the default
    /// (`Continue`), mirroring the historical read-side behavior.
    pub const fn from_u8(v: u8) -> Self {
        match v {
            1 => WalFailurePolicy::Rollback,
            _ => WalFailurePolicy::Continue,
        }
    }

    /// Config-file / `CONFIG SET` string form (see
    /// `frogdb_config::persistence::WAL_FAILURE_POLICIES`).
    pub const fn as_config_str(self) -> &'static str {
        match self {
            WalFailurePolicy::Continue => "continue",
            WalFailurePolicy::Rollback => "rollback",
        }
    }

    /// Parse the (already-validated) config string; anything other than
    /// `"rollback"` is `Continue`.
    pub fn from_config_str(s: &str) -> Self {
        if s.eq_ignore_ascii_case("rollback") {
            WalFailurePolicy::Rollback
        } else {
            WalFailurePolicy::Continue
        }
    }
}

impl From<WalFailurePolicy> for u8 {
    fn from(p: WalFailurePolicy) -> u8 {
        p.as_u8()
    }
}

impl From<u8> for WalFailurePolicy {
    fn from(v: u8) -> Self {
        Self::from_u8(v)
    }
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
    /// Highest sequence assigned to a WAL entry.
    pub sequence: u64,
    /// Highest sequence confirmed durable in storage. Trails `sequence` by the
    /// buffered entries; a widening gap paired with `lost_ops > 0` means
    /// flushes are failing.
    pub durable_sequence: u64,
    /// Total failed flush attempts since startup.
    pub flush_failures: u64,
    /// Entries dropped in failed batches since startup. Losses are permanent:
    /// a later successful flush does not un-count them.
    pub lost_ops: u64,
    /// Estimated bytes dropped in failed batches since startup.
    pub lost_bytes: u64,
    /// Whether the most recent flush attempt succeeded (true when no flush
    /// has happened yet).
    pub last_flush_ok: bool,
    pub shard_id: usize,
    pub last_flush_timestamp_ms: u64,
}
