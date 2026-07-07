//! Tiered warm-storage subsystem, extracted from [`HashMapStore`].
//!
//! FrogDB keeps hot values in RAM and can demote cold values to a per-shard
//! RocksDB column family (the "warm" tier). This module owns everything that
//! bookkeeping requires — the RocksDB handle, the warm-key count, and the
//! demotion / promotion / expired-on-promote counters — so the store body only
//! has to orchestrate entry-location changes and memory accounting.
//!
//! Keys and metadata always stay in RAM in the owning store; the warm tier
//! stores only serialized *values*, so this type never needs the key set. It is
//! a pure value cache plus counters.
//!
//! # Configuration ordering
//!
//! Recovery reinstates warm entries (bumping [`WarmTier::note_restored`]) before
//! the RocksDB handle is wired up via [`WarmTier::configure`]. The handle is
//! therefore an `Option`, mirroring the original `warm_store: Option<Arc<..>>`
//! field: warm-key bookkeeping is always live, while the RocksDB I/O is gated on
//! the handle being present.

use std::sync::Arc;

use frogdb_persistence::RocksStore;
use frogdb_persistence::rocks::RocksError;

/// The warm (RocksDB-backed) storage tier for a single shard.
#[derive(Default)]
pub struct WarmTier {
    /// RocksDB handle for cold values. `None` until [`configure`](Self::configure)
    /// wires it up — recovery populates `warm_keys` before that happens.
    store: Option<Arc<RocksStore>>,
    /// Shard ID for warm CF lookups.
    shard_id: usize,
    /// Number of keys whose value currently lives in the warm CF.
    warm_keys: usize,
    /// Total number of hot→warm demotions.
    total_demotions: u64,
    /// Total number of warm→hot promotions.
    total_promotions: u64,
    /// Keys that were found expired during a promotion attempt.
    expired_on_promote: u64,
}

// `RocksStore` is not `Debug`, so the derive can't apply; report the tier's
// configuration and counters without the handle itself.
impl std::fmt::Debug for WarmTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WarmTier")
            .field("configured", &self.store.is_some())
            .field("shard_id", &self.shard_id)
            .field("warm_keys", &self.warm_keys)
            .field("demotions", &self.total_demotions)
            .field("promotions", &self.total_promotions)
            .field("expired_on_promote", &self.expired_on_promote)
            .finish()
    }
}

impl WarmTier {
    /// Create an unconfigured warm tier (no backing store, zeroed counters).
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Wire up the RocksDB handle so demotion/promotion can perform I/O.
    ///
    /// Any `warm_keys` already counted (e.g. from recovery's `note_restored`)
    /// are preserved.
    pub(super) fn configure(&mut self, store: Arc<RocksStore>, shard_id: usize) {
        self.store = Some(store);
        self.shard_id = shard_id;
    }

    /// Whether a RocksDB handle is configured (tiered storage enabled).
    pub(super) fn is_configured(&self) -> bool {
        self.store.is_some()
    }

    // ------------------------------------------------------------------------
    // RocksDB value I/O (all gated on a configured handle)
    // ------------------------------------------------------------------------

    /// Persist a demoted value to the warm CF.
    ///
    /// Returns `None` if the tier has no backing store (the caller maps this to
    /// its own "warm tier not configured" error); otherwise the RocksDB result.
    /// Counters are left untouched — the caller records the demotion via
    /// [`record_demotion`](Self::record_demotion) only after the entry's
    /// location has been switched to warm.
    pub(super) fn try_put(&self, key: &[u8], serialized: &[u8]) -> Option<Result<(), RocksError>> {
        let store = self.store.as_ref()?;
        Some(store.put_warm(self.shard_id, key, serialized))
    }

    /// Read a warm value's serialized bytes.
    ///
    /// Returns `None` if the tier has no backing store; otherwise the RocksDB
    /// result (`Ok(None)` when the key is absent from the CF).
    pub(super) fn try_get(&self, key: &[u8]) -> Option<Result<Option<Vec<u8>>, RocksError>> {
        let store = self.store.as_ref()?;
        Some(store.get_warm(self.shard_id, key))
    }

    /// Read a warm value's bytes and delete its CF entry in one step (GETDEL).
    ///
    /// Best-effort: returns `None` if the tier is unconfigured or the read
    /// fails. Does not touch counters — the caller has already accounted for the
    /// removal.
    pub(super) fn take(&self, key: &[u8]) -> Option<Vec<u8>> {
        let store = self.store.as_ref()?;
        let data = store.get_warm(self.shard_id, key).ok().flatten();
        let _ = store.delete_warm(self.shard_id, key);
        data
    }

    /// Best-effort delete of a warm value's CF entry (no-op if unconfigured).
    /// Does not touch counters.
    pub(super) fn delete(&self, key: &[u8]) {
        if let Some(store) = &self.store {
            let _ = store.delete_warm(self.shard_id, key);
        }
    }

    // ------------------------------------------------------------------------
    // Counter bookkeeping
    // ------------------------------------------------------------------------

    /// Record a completed hot→warm demotion: one more warm key, one more
    /// demotion.
    pub(super) fn record_demotion(&mut self) {
        self.warm_keys += 1;
        self.total_demotions += 1;
    }

    /// Record a completed warm→hot promotion: one fewer warm key, one more
    /// promotion.
    pub(super) fn record_promotion(&mut self) {
        self.warm_keys = self.warm_keys.saturating_sub(1);
        self.total_promotions += 1;
    }

    /// Record a warm key found expired during a promotion attempt: bump the
    /// counter and remove the warm entry (CF delete + warm-key decrement).
    pub(super) fn record_expired_on_promote(&mut self, key: &[u8]) {
        self.expired_on_promote += 1;
        self.remove_warm(key);
    }

    /// A warm key is being removed wholesale (delete / overwrite / lazy
    /// expiry): delete its CF entry and drop the warm-key count.
    pub(super) fn remove_warm(&mut self, key: &[u8]) {
        self.delete(key);
        self.warm_keys = self.warm_keys.saturating_sub(1);
    }

    /// Drop the warm-key count by one without touching the CF (GETDEL of a warm
    /// key reads the value out itself via [`take`](Self::take)).
    pub(super) fn decrement_warm_keys(&mut self) {
        self.warm_keys = self.warm_keys.saturating_sub(1);
    }

    /// A warm entry was reinstated from a recovery snapshot (value already on
    /// disk): count it without any CF I/O.
    pub(super) fn note_restored(&mut self) {
        self.warm_keys += 1;
    }

    /// Reset the warm-key count to zero after the owner has flushed the CF
    /// (FLUSHDB / `clear`).
    pub(super) fn reset_keys(&mut self) {
        self.warm_keys = 0;
    }

    // ------------------------------------------------------------------------
    // Accessors
    // ------------------------------------------------------------------------

    /// Number of keys currently in the warm tier.
    pub fn warm_keys(&self) -> usize {
        self.warm_keys
    }

    /// Total hot→warm demotions.
    pub fn demotions(&self) -> u64 {
        self.total_demotions
    }

    /// Total warm→hot promotions.
    pub fn promotions(&self) -> u64 {
        self.total_promotions
    }

    /// Keys found expired during a promotion attempt.
    pub fn expired_on_promote(&self) -> u64 {
        self.expired_on_promote
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_tier_is_unconfigured_and_zeroed() {
        let tier = WarmTier::new();
        assert!(!tier.is_configured());
        assert_eq!(tier.warm_keys(), 0);
        assert_eq!(tier.demotions(), 0);
        assert_eq!(tier.promotions(), 0);
        assert_eq!(tier.expired_on_promote(), 0);
    }

    #[test]
    fn io_is_gated_on_configuration() {
        let tier = WarmTier::new();
        // Unconfigured: value I/O reports "no backing store" via `None`.
        assert!(tier.try_put(b"k", b"v").is_none());
        assert!(tier.try_get(b"k").is_none());
        assert!(tier.take(b"k").is_none());
        // delete() is a silent no-op when unconfigured.
        tier.delete(b"k");
    }

    #[test]
    fn demotion_and_promotion_counters_move_in_lockstep() {
        let mut tier = WarmTier::new();

        tier.record_demotion();
        tier.record_demotion();
        assert_eq!(tier.warm_keys(), 2);
        assert_eq!(tier.demotions(), 2);
        assert_eq!(tier.promotions(), 0);

        tier.record_promotion();
        assert_eq!(tier.warm_keys(), 1);
        assert_eq!(tier.promotions(), 1);
    }

    #[test]
    fn expired_on_promote_bumps_counter_and_drops_warm_key() {
        let mut tier = WarmTier::new();
        tier.record_demotion();
        assert_eq!(tier.warm_keys(), 1);

        // Unconfigured delete is a no-op, but the counters still settle.
        tier.record_expired_on_promote(b"k");
        assert_eq!(tier.expired_on_promote(), 1);
        assert_eq!(tier.warm_keys(), 0);
    }

    #[test]
    fn remove_and_decrement_saturate_at_zero() {
        let mut tier = WarmTier::new();
        tier.remove_warm(b"k");
        assert_eq!(tier.warm_keys(), 0, "remove_warm must not underflow");
        tier.decrement_warm_keys();
        assert_eq!(tier.warm_keys(), 0, "decrement must not underflow");
    }

    #[test]
    fn note_restored_counts_without_a_backing_store() {
        // Recovery reinstates warm entries before the RocksDB handle is wired
        // up: the count must still track.
        let mut tier = WarmTier::new();
        tier.note_restored();
        tier.note_restored();
        assert!(!tier.is_configured());
        assert_eq!(tier.warm_keys(), 2);
    }

    #[test]
    fn reset_keys_zeroes_the_count() {
        let mut tier = WarmTier::new();
        tier.record_demotion();
        tier.record_demotion();
        tier.reset_keys();
        assert_eq!(tier.warm_keys(), 0);
    }
}
