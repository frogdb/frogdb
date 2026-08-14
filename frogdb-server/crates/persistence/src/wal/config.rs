//! WAL configuration types and policies.

/// What a shard does when its WAL fails (FM-PERSISTENCE-005/006/055).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WalFailurePolicy {
    /// Availability over durability: acknowledge the write, account the loss.
    #[default]
    Continue,
    /// Undo the write and reply `-IOERR`, then refuse further writes while the
    /// shard is poisoned.
    Rollback,
    /// Fail-stop: `Rollback`'s undo plus a `-MISCONF` refusal of every write,
    /// with reads still served, until an operator resolves the failure.
    Readonly,
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
            WalFailurePolicy::Readonly => 2,
        }
    }

    /// Decode the numeric encoding. Unknown values fall back to the default
    /// (`Continue`), mirroring the historical read-side behavior.
    pub const fn from_u8(v: u8) -> Self {
        match v {
            1 => WalFailurePolicy::Rollback,
            2 => WalFailurePolicy::Readonly,
            _ => WalFailurePolicy::Continue,
        }
    }

    /// Config-file / `CONFIG SET` string form (see
    /// `frogdb_config::persistence::WAL_FAILURE_POLICIES`).
    pub const fn as_config_str(self) -> &'static str {
        match self {
            WalFailurePolicy::Continue => "continue",
            WalFailurePolicy::Rollback => "rollback",
            WalFailurePolicy::Readonly => "readonly",
        }
    }

    /// Parse the (already-validated) config string; anything unrecognised is
    /// `Continue`, the default.
    pub fn from_config_str(s: &str) -> Self {
        if s.eq_ignore_ascii_case("rollback") {
            WalFailurePolicy::Rollback
        } else if s.eq_ignore_ascii_case("readonly") {
            WalFailurePolicy::Readonly
        } else {
            WalFailurePolicy::Continue
        }
    }

    /// Whether a failed durability confirmation undoes the write and replies
    /// `-IOERR` (FM-PERSISTENCE-006). `readonly` is `rollback` plus the standing
    /// refusal below — a fail-stop that acknowledged the write it could not
    /// persist would be no fail-stop at all.
    pub const fn rolls_back(self) -> bool {
        matches!(
            self,
            WalFailurePolicy::Rollback | WalFailurePolicy::Readonly
        )
    }

    /// Whether writes are refused outright once the shard is poisoned
    /// (FM-PERSISTENCE-055). `continue` keeps acknowledging by design; the
    /// other two stop, because both were chosen to avoid acking a loss.
    pub const fn refuses_writes_when_poisoned(self) -> bool {
        self.rolls_back()
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
    /// Shared live handle for the batch-size flush threshold (bytes).
    ///
    /// When `Some`, [`super::RocksWalWriter::new`] *adopts* this atomic instead
    /// of minting its own from `batch_size_threshold`, so every shard's flush
    /// thread and `CONFIG SET batch-size-threshold-kb` read and write the same
    /// cell. `None` (tests, and any caller that does not care) keeps the old
    /// behaviour of seeding a private atomic from `batch_size_threshold`.
    pub batch_size_threshold_handle: Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>,
}
impl Default for WalConfig {
    fn default() -> Self {
        Self {
            mode: DurabilityMode::default(),
            batch_size_threshold: 4 * 1024 * 1024,
            batch_timeout_ms: 10,
            channel_capacity: 8192,
            batch_size_threshold_handle: None,
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
    /// Highest sequence committed to storage. Trails `sequence` by the buffered
    /// entries; a widening gap paired with `lost_ops > 0` means flushes are
    /// failing.
    ///
    /// Deliberately *committed*, not durable: whether a commit implies an fsync
    /// is the durability mode's decision, so a lag stat named "durable" would
    /// report `periodic`/`async` writes as on-device when they are only in
    /// RocksDB. The fsync watermark is
    /// `RocksWalWriter::durable_sequence`.
    pub committed_sequence: u64,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The numeric encoding is the wire between `CONFIG SET
    /// wal-failure-policy` and every shard's `AtomicU8`: a writer stores
    /// `as_u8`, a reader recovers it with `from_u8`, and the two must agree
    /// or a live policy change silently flips to the other policy.
    // FM-PERSISTENCE-006
    #[test]
    fn the_atomic_encoding_round_trips_and_distinguishes_the_policies() {
        for policy in [
            WalFailurePolicy::Continue,
            WalFailurePolicy::Rollback,
            WalFailurePolicy::Readonly,
        ] {
            assert_eq!(WalFailurePolicy::from_u8(policy.as_u8()), policy);
            assert_eq!(WalFailurePolicy::from(u8::from(policy)), policy);
        }
        assert_ne!(
            WalFailurePolicy::Continue.as_u8(),
            WalFailurePolicy::Rollback.as_u8(),
            "the two policies must not share an encoding"
        );
        assert_eq!(
            WalFailurePolicy::default(),
            WalFailurePolicy::Continue,
            "continue is the default policy"
        );
    }

    /// Unknown bytes decode to the default rather than to `Rollback`:
    /// a torn or uninitialised flag must not start refusing writes.
    // FM-PERSISTENCE-006
    #[test]
    fn an_unknown_encoding_decodes_to_continue() {
        for v in [0u8, 3, 17, 255] {
            assert_eq!(
                WalFailurePolicy::from_u8(v),
                WalFailurePolicy::Continue,
                "byte {v} must not decode to rollback"
            );
        }
    }

    /// The config strings are the operator-facing names, and
    /// `frogdb_config::persistence::WAL_FAILURE_POLICIES` validates against
    /// them, so the two directions must compose.
    // FM-PERSISTENCE-006
    #[test]
    fn config_strings_round_trip_and_only_rollback_selects_rollback() {
        for policy in [
            WalFailurePolicy::Continue,
            WalFailurePolicy::Rollback,
            WalFailurePolicy::Readonly,
        ] {
            assert_eq!(
                WalFailurePolicy::from_config_str(policy.as_config_str()),
                policy
            );
        }
        assert_eq!(WalFailurePolicy::Continue.as_config_str(), "continue");
        assert_eq!(WalFailurePolicy::Rollback.as_config_str(), "rollback");
        assert_eq!(
            WalFailurePolicy::from_config_str("ROLLBACK"),
            WalFailurePolicy::Rollback,
            "the config parse is case-insensitive"
        );
        for junk in ["", "continue ", "roll", "rollbacks", "1", "read-only"] {
            assert_eq!(
                WalFailurePolicy::from_config_str(junk),
                WalFailurePolicy::Continue,
                "{junk:?} must not select rollback"
            );
        }
    }

    /// `readonly` is a policy of its own, not an alias: it keeps `rollback`'s
    /// undo (a fail-stop that acked the write it could not persist would be no
    /// fail-stop) and adds the standing refusal `continue` must never grow.
    // FM-PERSISTENCE-055
    #[test]
    fn readonly_is_a_distinct_policy_that_rolls_back_and_refuses() {
        assert_ne!(
            WalFailurePolicy::Readonly,
            WalFailurePolicy::Rollback,
            "readonly must not collapse into rollback"
        );
        assert_eq!(
            WalFailurePolicy::from_config_str("READONLY"),
            WalFailurePolicy::Readonly
        );
        assert_eq!(
            WalFailurePolicy::from_u8(WalFailurePolicy::Readonly.as_u8()),
            WalFailurePolicy::Readonly
        );

        assert!(WalFailurePolicy::Readonly.rolls_back());
        assert!(WalFailurePolicy::Rollback.rolls_back());
        assert!(!WalFailurePolicy::Continue.rolls_back());

        assert!(WalFailurePolicy::Readonly.refuses_writes_when_poisoned());
        assert!(WalFailurePolicy::Rollback.refuses_writes_when_poisoned());
        assert!(
            !WalFailurePolicy::Continue.refuses_writes_when_poisoned(),
            "the default policy trades durability for availability — it never refuses"
        );
    }

    /// Defaults are a durability decision, not an arbitrary constant: an
    /// unconfigured WAL fsyncs on a timer rather than never (`Async`) or on
    /// every write (`Sync`).
    #[test]
    fn the_default_durability_mode_is_periodic_one_second() {
        let DurabilityMode::Periodic { interval_ms } = DurabilityMode::default() else {
            panic!("the default durability mode must be periodic");
        };
        assert_eq!(interval_ms, 1000);

        let cfg = WalConfig::default();
        assert!(matches!(cfg.mode, DurabilityMode::Periodic { .. }));
        assert_eq!(cfg.batch_size_threshold, 4 * 1024 * 1024);
        assert_eq!(cfg.batch_timeout_ms, 10);
        assert_eq!(cfg.channel_capacity, 8192);
        assert!(
            cfg.batch_size_threshold_handle.is_none(),
            "a default config owns no shared handle — the writer mints its own"
        );
    }
}
