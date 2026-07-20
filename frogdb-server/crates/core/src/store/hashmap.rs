//! HashMap-based store implementation.

use bytes::Bytes;
use frogdb_persistence::{RocksStore, deserialize, serialize};
use griddle::HashMap;
use rand::seq::IteratorRandom;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

use crate::glob::glob_match;
use crate::histogram::KeysizeHistograms;
use crate::noop::{ExpiryIndex, FieldExpiryIndex};
use crate::shard::slot_for_key;
use crate::types::{KeyMetadata, KeyType, SetCondition, SetOptions, SetResult, Value};

use super::timeseries_labels::TimeSeriesLabels;
use super::warm_tier::WarmTier;
use super::{ExpiryIndexAnomaly, ExpiryIndexAnomalyKind, Store};

/// Where a key's value currently resides.
#[derive(Debug)]
enum ValueLocation {
    /// Value is in RAM (normal case).
    Hot(Arc<Value>),
    /// Value has been spilled to RocksDB warm tier.
    Warm,
}

/// Stable 48-bit content hash of a key, used to order the keyspace for SCAN.
///
/// SCAN's cursor is the hash of the resume point, not a table position, so the
/// ordering does not shift when griddle rehashes on insert. The result is masked
/// to 48 bits because it rides in the position field of the cross-shard SCAN
/// cursor, and remapped away from 0 (which the cross-shard driver reserves for
/// "shard exhausted").
fn scan_cursor_hash(key: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    const CURSOR_MASK: u64 = (1u64 << 48) - 1;
    let mut hasher = std::hash::DefaultHasher::new();
    key.hash(&mut hasher);
    let h = hasher.finish() & CURSOR_MASK;
    if h == 0 { 1 } else { h }
}

/// Entry in the store with value location and metadata.
///
/// Keys and metadata are ALWAYS in RAM. Only the value may be on disk.
///
/// Hot values are wrapped in `Arc` for copy-on-write semantics:
/// - Reads: cheap ref-count bump via `Arc::clone()`
/// - Writes with no readers: zero-copy (refcount == 1)
/// - Writes with outstanding readers: clone-on-write via `Arc::make_mut()`
#[derive(Debug)]
struct Entry {
    location: ValueLocation,
    metadata: KeyMetadata,
    /// Cached key type so TYPE/SCAN work without unspilling.
    key_type: KeyType,
}

impl Entry {
    /// Returns the in-memory value, or None if spilled to warm tier.
    fn hot_value(&self) -> Option<&Arc<Value>> {
        match &self.location {
            ValueLocation::Hot(v) => Some(v),
            ValueLocation::Warm => None,
        }
    }

    /// Returns true if the value is currently in RAM.
    fn is_hot(&self) -> bool {
        matches!(self.location, ValueLocation::Hot(_))
    }

    /// Memory size of this entry for accounting purposes.
    /// Warm entries contribute zero value bytes (value is on disk).
    fn memory_size(&self, key: &[u8]) -> usize {
        let value_size = match &self.location {
            ValueLocation::Hot(v) => v.memory_size(),
            ValueLocation::Warm => 0,
        };
        key.len() + value_size + std::mem::size_of::<KeyMetadata>() + std::mem::size_of::<Entry>()
    }
}

/// The histogram contribution of a single **hot** entry, captured before the
/// entry is removed or resized.
///
/// Every hot entry contributes a key-memory bin (`key_mem`, its accounted
/// `metadata.memory_size`); keysize-tracked types (string/list/…) additionally
/// contribute a per-type bin (`ks`). Untracked types (bloom/json/…) carry
/// `ks == None` but still a `key_mem`. A warm entry has no hot contribution, so
/// `HashMapStore::histogram_snapshot` returns `None` for it — the value it holds
/// lives on disk, not in RAM.
#[derive(Clone, Copy, Debug)]
struct HotHistoContribution {
    /// Per-type keysize bin: `(type, logical_size)`. `None` for untracked types.
    ks: Option<(crate::histogram::KeysizeType, Option<usize>)>,
    /// Accounted memory size — the key-memory bin and the `memory_used` charge.
    key_mem: usize,
}

/// Error returned by `spill_key()`.
#[derive(Debug, thiserror::Error)]
pub enum SpillError {
    #[error("key not found")]
    KeyNotFound,
    #[error("key is already warm")]
    AlreadyWarm,
    #[error("warm tier not configured")]
    NoWarmStore,
    #[error("RocksDB error: {0}")]
    Rocks(#[from] frogdb_persistence::rocks::RocksError),
}

/// Default store implementation using griddle::HashMap.
pub struct HashMapStore {
    data: HashMap<Bytes, Entry>,
    expiry_index: ExpiryIndex,
    field_expiry_index: FieldExpiryIndex,
    /// TimeSeries label index plus its keyspace-reconciliation logic.
    ts_labels: TimeSeriesLabels,
    memory_used: usize,
    /// Number of changes since last save (for INFO persistence rdb_changes_since_last_save).
    dirty: u64,
    /// Tiered warm-storage subsystem: the RocksDB handle (absent when tiered
    /// storage is disabled), the warm-key count, and the
    /// spill/unspill/expired-on-unspill counters.
    warm_tier: WarmTier,
    /// Total number of keys expired (lazy + active expiry).
    expired_keys: u64,
    /// Whether passive/lazy expiry is suppressed (set during CLIENT PAUSE).
    /// When true, expired keys are logically invisible (get returns None)
    /// but not physically deleted and the expired_keys counter is not
    /// incremented.
    expiry_suppressed: bool,
    /// Whether touch() calls should be suppressed (CLIENT NO-TOUCH mode).
    /// When true, `get_with_expiry_check()` and `get_mut()` skip updating
    /// the key's last access time. The explicit `touch()` method is NOT affected.
    suppress_touch: bool,
    /// Per-type keysize histograms and key-memory histogram.
    keysizes: KeysizeHistograms,
    /// Keys accessed via `get_mut()` that need a deferred size reconciliation
    /// after in-place mutation (histogram bins, `memory_used`, and the
    /// accounted `metadata.memory_size`). Each entry pairs the key with the
    /// pre-mutation hot contribution `resize` will retire against the live size.
    pending_keysizes_refreshes: Vec<(Bytes, HotHistoContribution)>,
}

impl std::fmt::Debug for HashMapStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashMapStore")
            .field("keys", &self.data.len())
            .field("memory_used", &self.memory_used)
            .field("warm_keys", &self.warm_tier.warm_keys())
            .field("warm_enabled", &self.warm_tier.is_configured())
            .finish()
    }
}

impl HashMapStore {
    /// Create a new empty store.
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            expiry_index: ExpiryIndex::new(),
            field_expiry_index: FieldExpiryIndex::new(),
            ts_labels: TimeSeriesLabels::new(),
            memory_used: 0,
            dirty: 0,
            warm_tier: WarmTier::new(),
            expired_keys: 0,
            expiry_suppressed: false,
            suppress_touch: false,
            keysizes: KeysizeHistograms::new(),
            pending_keysizes_refreshes: Vec::new(),
        }
    }

    /// Create a store with a pre-allocated expiry index.
    ///
    /// Used during recovery when we have a pre-built expiry index.
    pub fn with_expiry_index(expiry_index: ExpiryIndex) -> Self {
        Self {
            data: HashMap::new(),
            expiry_index,
            field_expiry_index: FieldExpiryIndex::new(),
            ts_labels: TimeSeriesLabels::new(),
            memory_used: 0,
            dirty: 0,
            warm_tier: WarmTier::new(),
            expired_keys: 0,
            expiry_suppressed: false,
            suppress_touch: false,
            keysizes: KeysizeHistograms::new(),
            pending_keysizes_refreshes: Vec::new(),
        }
    }

    /// Restore an entry during recovery.
    ///
    /// Inserts a key with its full recovered metadata (`last_access`,
    /// `lfu_counter`, `expires_at`). Routes through the same reconciliation
    /// seam as `set`, so replaying the same key twice (e.g. snapshot + WAL)
    /// retires the earlier incarnation's bookkeeping instead of leaking it.
    pub fn restore_entry(&mut self, key: Bytes, value: Value, metadata: KeyMetadata) {
        self.replace_entry(key, value, metadata);
    }

    /// Restore a warm entry during recovery.
    ///
    /// Inserts a key with metadata as a `Warm` entry — no value in RAM.
    /// Does NOT insert if the key already exists (hot copy wins).
    pub fn restore_warm_entry(&mut self, key: Bytes, mut metadata: KeyMetadata, key_type: KeyType) {
        // Hot copy wins — don't overwrite existing entries
        if self.data.contains_key(&key) {
            return;
        }

        // Warm entries: key + metadata in RAM, value on disk
        let size = key.len() + std::mem::size_of::<KeyMetadata>() + std::mem::size_of::<Entry>();
        metadata.memory_size = size;
        self.memory_used += size;

        // Update expiry index if key has expiry
        if let Some(expires_at) = metadata.expires_at {
            self.expiry_index.set(key.clone(), expires_at);
        }

        let entry = Entry {
            location: ValueLocation::Warm,
            metadata,
            key_type,
        };
        self.data.insert(key, entry);
        self.warm_tier.note_restored();
    }

    /// Calculate memory size for a new hot entry.
    fn hot_entry_memory_size(key: &[u8], value: &Value) -> usize {
        key.len()
            + value.memory_size()
            + std::mem::size_of::<KeyMetadata>()
            + std::mem::size_of::<Entry>()
    }

    /// Insert or overwrite an entry, reconciling **every** side structure.
    ///
    /// This is the single insert/overwrite seam: `set`, `set_with_options`,
    /// and `restore_entry` all route through it, so no write path can forget
    /// one of the derived structures (`expiry_index`, `field_expiry_index`,
    /// `label_index`, keysize histograms, `memory_used`). Before this seam
    /// existed, `set` skipped the expiry indexes — an overwritten key kept its
    /// stale index entry and active expiry would later delete the (now
    /// persistent) key: silent data loss.
    ///
    /// The caller supplies the new entry's `metadata`; its `expires_at` is the
    /// source of truth for the key-level expiry index (Some → indexed, None →
    /// removed). `metadata.memory_size` is overwritten with the computed entry
    /// size so the accounted size is always consistent with what `memory_used`
    /// was charged. Field-level expiries are re-derived from the value itself:
    /// stale entries from the previous incarnation are dropped and any field
    /// TTLs carried by a new `Value::Hash` are indexed (COPY/RESTORE of a hash
    /// with field TTLs).
    ///
    /// Returns the previous hot value, if any (a warm value being overwritten
    /// is deleted from the warm CF and reported as `None`).
    fn replace_entry(
        &mut self,
        key: Bytes,
        value: Value,
        metadata: KeyMetadata,
    ) -> Option<Arc<Value>> {
        self.install(key, value, metadata)
    }

    // ========================================================================
    // Entry-lifecycle seam: install / uninstall / resize
    //
    // These three private methods are the ONLY code permitted to reconcile the
    // derived side-structures (`memory_used`, the keysize histograms, both
    // expiry indexes, `ts_labels`, the warm-key count, and the deferred-refresh
    // queue) with the primary `data` map. Every mutation method delegates here,
    // so the "derived structures move atomically with the entry" invariant lives
    // in one place instead of being re-stated at nine call sites.
    // ========================================================================

    /// Insert or overwrite an entry, reconciling **every** side structure.
    ///
    /// The single insert/overwrite seam: `set`, `set_with_options`, and
    /// `restore_entry` all route here (via `replace_entry`), so no write path
    /// can forget one of the derived structures. Before this seam existed, `set`
    /// skipped the expiry indexes — an overwritten key kept its stale index
    /// entry and active expiry would later delete the (now persistent) key:
    /// silent data loss. An overwrite first fully `uninstall`s the previous
    /// incarnation, so its expiry/label/field bookkeeping cannot leak into the
    /// new one.
    ///
    /// The caller supplies the new entry's `metadata`; its `expires_at` is the
    /// source of truth for the key-level expiry index. `metadata.memory_size` is
    /// overwritten with the computed entry size so the accounted size is always
    /// consistent with what `memory_used` was charged. Field-level expiries are
    /// re-derived from the value itself.
    ///
    /// Returns the previous hot value, if any (a warm value being overwritten is
    /// deleted from the warm CF and reported as `None`).
    fn install(
        &mut self,
        key: Bytes,
        value: Value,
        mut metadata: KeyMetadata,
    ) -> Option<Arc<Value>> {
        // Retire any previous incarnation wholesale — memory, histograms, both
        // expiry indexes, ts_labels, warm-key count, and the pending-refresh
        // queue all settle inside `uninstall`. Field TTLs and labels are then
        // re-derived from the new value below.
        let old_value = self.uninstall(&key).and_then(|old| match old.location {
            ValueLocation::Hot(arc) => Some(arc),
            ValueLocation::Warm => None,
        });

        let new_size = Self::hot_entry_memory_size(&key, &value);

        // Reconcile the TimeSeries label index (indexes a time series's labels,
        // drops stale labels otherwise).
        self.ts_labels.reconcile(&key, &value);

        // Reconcile the field-expiry index from the new value.
        if let Value::Hash(ref hash) = value
            && let Some(expiries) = hash.field_expiries()
        {
            for (field, &expires_at) in expiries {
                self.field_expiry_index
                    .set(key.clone(), field.clone(), expires_at);
            }
        }

        // Reconcile the key-level expiry index from the new metadata. `uninstall`
        // already dropped any old index entry, so a `None` deadline needs no
        // further work.
        if let Some(expires_at) = metadata.expires_at {
            self.expiry_index.set(key.clone(), expires_at);
        }

        self.histogram_increment(&value, &key, new_size);
        self.memory_used += new_size;

        metadata.memory_size = new_size;
        let key_type = value.key_type();
        self.data.insert(
            key,
            Entry {
                location: ValueLocation::Hot(Arc::new(value)),
                metadata,
                key_type,
            },
        );

        old_value
    }

    /// Remove an entry, reconciling **every** side structure, and return it.
    ///
    /// The single removal seam: `delete`, `get_and_delete`,
    /// `check_and_delete_expired`, `install`'s overwrite path, and
    /// `unspill_key`'s expired branch all route here. It snapshots + decrements
    /// the keysize histograms, refunds the accounted memory, retires both expiry
    /// indexes + `ts_labels`, discards any pending refresh, and settles the warm
    /// tier from the entry's own location (`remove_warm` deletes the warm CF
    /// entry and drops the warm-key count).
    ///
    /// Returns the removed `Entry` (or `None` if absent) so callers that need
    /// the value (`get_and_delete`) or a different counter (expired-on-unspill)
    /// can finish their own residual. Callers needing the warm *bytes* must read
    /// them (`warm_tier.take`) before calling `uninstall`, whose best-effort CF
    /// delete then no-ops.
    fn uninstall(&mut self, key: &[u8]) -> Option<Entry> {
        let entry = self.data.remove(key)?;
        self.discard_pending_refresh(key);

        // Decrement the keysize histograms by the entry's hot contribution
        // (a no-op for warm entries — their hot bytes were retired at spill).
        let snap = Self::histogram_snapshot(&entry);
        self.histogram_decrement_snapshot(snap);

        // Refund the accounted size — exactly what `memory_used` was charged at
        // insert/refresh/spill time — never the recomputed live size, which can
        // exceed it after unflushed in-place growth.
        let size = entry.metadata.memory_size;
        debug_assert!(
            size <= self.memory_used,
            "memory accounting underflow during uninstall: accounted {} > memory_used {}",
            size,
            self.memory_used
        );
        if size > self.memory_used {
            warn!(
                key_len = key.len(),
                reported_size = size,
                "Memory accounting underflow during uninstall"
            );
        }
        self.memory_used = self.memory_used.saturating_sub(size);

        // Settle the warm side from the entry's own location.
        if !entry.is_hot() {
            self.warm_tier.remove_warm(key);
        }
        self.expiry_index.remove(key);
        self.ts_labels.remove(key);
        self.field_expiry_index.remove_key(key);
        Some(entry)
    }

    /// Check if a key is expired and delete it if so.
    ///
    /// Returns true if the key was expired (and deleted, unless expiry is suppressed).
    /// When `expiry_suppressed` is true (during CLIENT PAUSE), the key is logically
    /// treated as expired but not physically deleted and the counter is not incremented.
    fn check_and_delete_expired(&mut self, key: &[u8]) -> bool {
        if let Some(entry) = self.data.get(key)
            && entry.metadata.is_expired()
        {
            // During CLIENT PAUSE, suppress physical deletion but treat as expired
            if self.expiry_suppressed {
                return true;
            }
            debug!(key_len = key.len(), "Key expired via lazy deletion");
            self.uninstall(key);
            self.expired_keys += 1;
            return true;
        }
        false
    }

    /// Total number of expired keys (lazy + active).
    pub fn expired_keys(&self) -> u64 {
        self.expired_keys
    }

    /// Add to the expired keys counter (for active expiry which bypasses `check_and_delete_expired`).
    pub fn add_expired_keys(&mut self, count: u64) {
        self.expired_keys += count;
    }

    /// Reset the expired keys counter (for CONFIG RESETSTAT).
    pub fn reset_expired_keys(&mut self) {
        self.expired_keys = 0;
    }

    /// Set whether passive/lazy expiry is suppressed (during CLIENT PAUSE).
    pub fn set_expiry_suppressed(&mut self, suppressed: bool) {
        self.expiry_suppressed = suppressed;
    }

    // ========================================================================
    // Keysize histogram helpers
    // ========================================================================

    /// Increment keysize histograms for a newly inserted value.
    fn histogram_increment(&mut self, value: &Value, _key: &[u8], entry_memory: usize) {
        if let Some(ks_type) = value.keysize_type()
            && let Some(logical) = value.logical_size()
        {
            self.keysizes.get_mut(ks_type).increment(logical);
        }
        if self.keysizes.key_memory_enabled {
            self.keysizes.key_memory.increment(entry_memory);
        }
    }

    /// Snapshot the histogram contribution of a **hot** entry before it is
    /// removed or resized.
    ///
    /// Returns `None` for a warm or absent entry (no hot contribution). For a
    /// hot entry, `key_mem` is always its *accounted* size (`metadata.memory_size`,
    /// not the live size, so the decrement mirrors what the increment or last
    /// refresh charged), and `ks` is present only for keysize-tracked types.
    fn histogram_snapshot(entry: &Entry) -> Option<HotHistoContribution> {
        let v = entry.hot_value()?;
        Some(HotHistoContribution {
            ks: v.keysize_type().map(|t| (t, v.logical_size())),
            key_mem: entry.metadata.memory_size,
        })
    }

    /// Decrement keysize histograms using a pre-captured hot contribution.
    fn histogram_decrement_snapshot(&mut self, snapshot: Option<HotHistoContribution>) {
        if let Some(snap) = snapshot {
            if let Some((ks_type, Some(logical))) = snap.ks {
                self.keysizes.get_mut(ks_type).decrement(logical);
            }
            if self.keysizes.key_memory_enabled {
                self.keysizes.key_memory.decrement(snap.key_mem);
            }
        }
    }

    /// Flush pending size reconciliations from `get_mut()` calls.
    ///
    /// After commands mutate values in-place via `get_mut()`, the logical and
    /// memory sizes may have changed (e.g., RPUSH adding elements to a list).
    /// This method compares each pre-mutation snapshot with the current state
    /// and reconciles histogram bins, `memory_used`, and the accounted
    /// `metadata.memory_size`. Before it applied the memory delta, in-place
    /// growth was invisible to `memory_used` — a list grown to megabytes still
    /// counted as its creation size, so eviction/OOM checks fired late or
    /// never.
    pub fn flush_keysizes_refreshes(&mut self) {
        if self.pending_keysizes_refreshes.is_empty() {
            return;
        }
        let pending = std::mem::take(&mut self.pending_keysizes_refreshes);
        for (key, old_snapshot) in pending {
            // The queued snapshot was always taken from a hot entry, so its
            // `key_mem` is the previously-accounted size.
            self.resize(&key, Some(old_snapshot), old_snapshot.key_mem);
        }
    }

    /// Reconcile an existing entry's histograms, `memory_used`, and accounted
    /// `metadata.memory_size` against its current live size — the in-place-size
    /// arm of the lifecycle seam.
    ///
    /// Retires the pre-change hot contribution (`old_snapshot`, `None` when the
    /// entry was warm) and adds the post-change one read from the live entry, so
    /// a bin change is a symmetric decrement/increment rather than a migrate.
    /// This unifies four callers: deferred `get_mut` growth and hash-field
    /// shrink (hot→hot), plus `spill_key` (hot→warm) and `unspill_key`'s
    /// rehydrate (warm→hot). The hot↔warm transitions are why this also closes
    /// the key-memory histogram gap `spill_key`/`unspill_key` used to leave.
    ///
    /// `old_accounted` must be the previously-accounted size — what `memory_used`
    /// was last charged for this entry, valid even when `old_snapshot` is `None`
    /// (a warm entry) — so `memory_used` moves by exactly `new_live − old_accounted`.
    /// A key deleted since the snapshot is a no-op: the delete already settled it.
    fn resize(
        &mut self,
        key: &[u8],
        old_snapshot: Option<HotHistoContribution>,
        old_accounted: usize,
    ) {
        // Retire the pre-resize hot contribution (no-op if it was warm).
        self.histogram_decrement_snapshot(old_snapshot);

        let Some(entry) = self.data.get(key) else {
            return;
        };
        let new_accounted = entry.memory_size(key);
        let new_ks = entry
            .hot_value()
            .and_then(|v| v.keysize_type().map(|t| (t, v.logical_size())));
        let is_hot = entry.is_hot();

        // Add the post-resize hot contribution (no-op if the entry is now warm).
        if let Some((ks_type, Some(logical))) = new_ks {
            self.keysizes.get_mut(ks_type).increment(logical);
        }
        if is_hot && self.keysizes.key_memory_enabled {
            self.keysizes.key_memory.increment(new_accounted);
        }

        // Apply the memory delta and refresh the accounted size.
        if new_accounted >= old_accounted {
            self.memory_used += new_accounted - old_accounted;
        } else {
            self.memory_used = self
                .memory_used
                .saturating_sub(old_accounted - new_accounted);
        }
        if let Some(entry) = self.data.get_mut(key) {
            entry.metadata.memory_size = new_accounted;
        }
    }

    /// Drop any pending deferred refresh for `key`.
    ///
    /// Called by every path that fully settles the key's accounting itself
    /// (overwrite via `replace_entry`, the delete family): once the key's
    /// accounted size has been retired, a stale snapshot must not be replayed
    /// by a later flush against a new incarnation of the key.
    fn discard_pending_refresh(&mut self, key: &[u8]) {
        if !self.pending_keysizes_refreshes.is_empty() {
            self.pending_keysizes_refreshes
                .retain(|(k, ..)| k.as_ref() != key);
        }
    }

    /// Access the keysize histograms (read-only, for INFO).
    pub fn keysizes(&self) -> &KeysizeHistograms {
        &self.keysizes
    }

    /// Mutably access the keysize histograms (for CONFIG SET toggle).
    pub fn keysizes_mut(&mut self) -> &mut KeysizeHistograms {
        &mut self.keysizes
    }

    /// Set whether key-memory histograms are enabled at startup.
    pub fn set_key_memory_enabled(&mut self, enabled: bool) {
        self.keysizes.key_memory_enabled = enabled;
    }

    /// Calculate total allocated memory for keys in a given slot.
    pub fn allocsize_in_slot(&self, slot: u16) -> usize {
        self.data
            .iter()
            .filter(|(k, _)| slot_for_key(k) == slot)
            .map(|(k, e)| e.memory_size(k))
            .sum()
    }
}

impl Default for HashMapStore {
    fn default() -> Self {
        Self::new()
    }
}

impl HashMapStore {
    /// Get a value only if it's hot (in-memory). Does not unspill warm values.
    ///
    /// Use this for WAL persistence and diagnostics where unspilling is unnecessary.
    pub fn get_hot(&self, key: &[u8]) -> Option<Arc<Value>> {
        self.data.get(key).and_then(|e| e.hot_value().cloned())
    }

    /// Configure the warm (RocksDB) tier for this store.
    pub fn set_warm_store(&mut self, rocks: Arc<RocksStore>, shard_id: usize) {
        self.warm_tier.configure(rocks, shard_id);
    }

    /// Spill a key's value from hot (RAM) to warm (RocksDB).
    ///
    /// Returns the number of value bytes freed from RAM.
    pub fn spill_key(&mut self, key: &[u8]) -> Result<usize, SpillError> {
        // Settle any deferred size reconciliation first so the accounted size
        // reflects the live value about to be spilled (spill runs from the
        // event loop, so this is normally a no-op).
        self.flush_keysizes_refreshes();

        if !self.warm_tier.is_configured() {
            return Err(SpillError::NoWarmStore);
        }

        let entry = self.data.get(key).ok_or(SpillError::KeyNotFound)?;
        let value = entry.hot_value().ok_or(SpillError::AlreadyWarm)?;

        // Serialize using existing persistence format
        let serialized = serialize(value, &entry.metadata);
        let value_bytes = value.memory_size();
        // Capture the pre-spill hot contribution + accounted size before the
        // location flip, so `resize` can migrate the histograms + memory in the
        // one place that owns that reconciliation.
        let old_snapshot = Self::histogram_snapshot(entry);
        let old_accounted = entry.metadata.memory_size;

        // Write to warm CF. `try_put` returns `None` only when the tier is
        // unconfigured, already handled above.
        match self.warm_tier.try_put(key, &serialized) {
            Some(Ok(())) => {}
            Some(Err(e)) => return Err(SpillError::Rocks(e)),
            None => return Err(SpillError::NoWarmStore),
        }

        // Flip to Warm, then reconcile through the seam. `resize` frees the
        // value bytes from `memory_used` *and* migrates the key-memory /
        // per-type histograms to the metadata-only footprint — the histogram
        // update `spill_key` used to skip (the column-2 gap).
        self.data.get_mut(key).unwrap().location = ValueLocation::Warm;
        self.resize(key, old_snapshot, old_accounted);
        self.warm_tier.record_spill();

        Ok(value_bytes)
    }

    /// Unspill a key's value from warm (RocksDB) back to hot (RAM).
    ///
    /// Returns None if the key doesn't exist, isn't warm, or is expired.
    fn unspill_key(&mut self, key: &[u8]) -> Option<Arc<Value>> {
        // Check that entry exists and is warm
        let entry = self.data.get(key)?;
        if entry.is_hot() {
            return entry.hot_value().cloned();
        }

        // Check TTL before unspilling — don't waste work on expired keys
        if entry.metadata.is_expired() {
            debug!(key_len = key.len(), "Warm key expired during unspill");
            // Retire every side structure through the removal seam (its
            // `remove_warm` deletes the warm CF entry and drops the warm-key
            // count), then bump the expired-on-unspill counter — the residual
            // this branch owns, like `check_and_delete_expired`'s `expired_keys`.
            self.uninstall(key);
            self.warm_tier.note_expired_on_unspill();
            return None;
        }

        // Read from warm CF. `try_get` returns `None` only when tiered storage
        // is disabled — no warm value to unspill.
        let data = match self.warm_tier.try_get(key)? {
            Ok(Some(data)) => data,
            Ok(None) => {
                warn!(key_len = key.len(), "Warm key missing from RocksDB");
                return None;
            }
            Err(e) => {
                warn!(key_len = key.len(), error = %e, "Failed to read warm key");
                return None;
            }
        };

        // Deserialize
        let (value, _metadata) = match deserialize(&data) {
            Ok(v) => v,
            Err(e) => {
                warn!(key_len = key.len(), error = %e, "Failed to deserialize warm key");
                return None;
            }
        };

        // Replace location with Hot
        let value_arc = Arc::new(value);

        // Accounted (metadata-only) size while warm, before the flip; `resize`
        // reads the rehydrated full size from the live entry.
        let old_accounted = self.data.get(key).unwrap().metadata.memory_size;
        self.data.get_mut(key).unwrap().location = ValueLocation::Hot(value_arc.clone());

        // Reconcile through the seam: adds the value bytes back to `memory_used`
        // and re-installs the key-memory / per-type histogram bins the spill
        // removed. No old contribution to retire (the entry was warm).
        self.resize(key, None, old_accounted);
        // Drops the warm-key count and bumps the unspill counter.
        self.warm_tier.record_unspill();

        // Delete from warm CF
        self.warm_tier.delete(key);

        Some(value_arc)
    }

    /// Access the warm (tiered) storage subsystem and its counters.
    ///
    /// The cohesive accessor concrete callers use in place of the old
    /// `warm_key_count()` / `hot_key_count()` / `spill_count()` /
    /// `unspill_count()` / `expired_on_unspill_count()` reach-throughs. Hot-key
    /// count is `len() - warm_tier().warm_keys()`.
    pub fn warm_tier(&self) -> &WarmTier {
        &self.warm_tier
    }

    #[cfg(test)]
    fn force_index_entry_for_test(&mut self, key: Bytes, deadline: std::time::Instant) {
        self.expiry_index.set(key, deadline);
    }

    #[cfg(test)]
    fn force_drop_index_entry_for_test(&mut self, key: &[u8]) {
        self.expiry_index.remove(key);
    }

    /// Recompute the keysize histograms from scratch over the live keyspace —
    /// the ground truth the incremental counters must match. Only **hot** entries
    /// contribute (a spilled value is not in RAM); per-type bins use the live
    /// logical size, the key-memory bin uses the accounted `metadata.memory_size`.
    /// Valid only in a settled state (no pending deferred refreshes), just like
    /// `recompute_memory_used`.
    #[cfg(test)]
    fn recompute_keysizes(&self) -> KeysizeHistograms {
        let mut ks = if self.keysizes.key_memory_enabled {
            KeysizeHistograms::new()
        } else {
            KeysizeHistograms::new_without_memory()
        };
        for (_key, entry) in self.data.iter() {
            if let Some(v) = entry.hot_value() {
                if let (Some(t), Some(logical)) = (v.keysize_type(), v.logical_size()) {
                    ks.get_mut(t).increment(logical);
                }
                if ks.key_memory_enabled {
                    ks.key_memory.increment(entry.metadata.memory_size);
                }
            }
        }
        ks
    }

    /// Assert every derived side-structure agrees with a from-scratch recompute:
    /// `memory_used`, the expiry index (both directions), and every keysize +
    /// key-memory histogram. This is the belt-and-suspenders check the lifecycle
    /// seam is meant to make trivially true. Call only when settled (flush any
    /// pending `get_mut` refreshes first).
    #[cfg(test)]
    fn assert_consistent(&self) {
        assert_eq!(
            self.recompute_memory_used(),
            self.memory_used(),
            "memory_used drifted from the live recompute"
        );
        assert!(
            self.audit_expiry_index().is_empty(),
            "expiry index anomalies: {:?}",
            self.audit_expiry_index()
        );
        let expected = self.recompute_keysizes();
        for &t in crate::histogram::KeysizeType::ALL {
            assert_eq!(
                self.keysizes.get(t).bins(),
                expected.get(t).bins(),
                "keysize histogram {t:?} drifted from the live recompute"
            );
        }
        assert_eq!(
            self.keysizes.key_memory.bins(),
            expected.key_memory.bins(),
            "key_memory histogram drifted from the live recompute"
        );
    }
}

impl Store for HashMapStore {
    fn get(&mut self, key: &[u8]) -> Option<Arc<Value>> {
        // Fast path: hot value
        if let Some(entry) = self.data.get(key)
            && let Some(v) = entry.hot_value()
        {
            return Some(v.clone());
        }
        // Slow path: unspill from warm tier
        self.unspill_key(key)
    }

    fn set(&mut self, key: Bytes, value: Value) -> Option<Value> {
        // Fresh metadata with no expiry: a plain SET/MSET overwrite clears any
        // TTL (Redis semantics), and the seam clears the expiry indexes to
        // match — the metadata and the indexes can never disagree.
        self.replace_entry(key, value, KeyMetadata::new(0))
            .map(Arc::unwrap_or_clone)
    }

    fn delete(&mut self, key: &[u8]) -> bool {
        self.uninstall(key).is_some()
    }

    fn contains(&self, key: &[u8]) -> bool {
        self.data.contains_key(key)
    }

    fn exists_unexpired(&self, key: &[u8]) -> bool {
        // Non-mutating: a key past its deadline reads as absent (a keyspace
        // miss) even before lazy/active expiry removes it, matching what the
        // command's own `get_with_expiry_check` observes, without touching
        // access/LFU metadata.
        self.data
            .get(key)
            .is_some_and(|entry| !entry.metadata.is_expired())
    }

    fn key_type(&self, key: &[u8]) -> KeyType {
        self.data
            .get(key)
            .map(|e| e.key_type)
            .unwrap_or(KeyType::None)
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn memory_used(&self) -> usize {
        self.memory_used
    }

    fn recompute_memory_used(&self) -> usize {
        // `Entry::memory_size` returns the live size (key + value + metadata +
        // Entry overhead; warm values contribute zero value bytes). Summing it
        // over every entry is the ground truth the running counter tracks.
        self.data.iter().map(|(k, e)| e.memory_size(k)).sum()
    }

    fn audit_expiry_index(&self) -> Vec<ExpiryIndexAnomaly> {
        let mut anomalies = Vec::new();

        // Direction 1: every index entry must point at a key with a matching
        // deadline.
        for (key, index_deadline) in self.expiry_index.iter() {
            match self.data.get(key) {
                None => anomalies.push(ExpiryIndexAnomaly {
                    key: String::from_utf8_lossy(key).into_owned(),
                    kind: ExpiryIndexAnomalyKind::KeyMissing,
                }),
                Some(entry) => match entry.metadata.expires_at {
                    None => anomalies.push(ExpiryIndexAnomaly {
                        key: String::from_utf8_lossy(key).into_owned(),
                        kind: ExpiryIndexAnomalyKind::KeyPersistent,
                    }),
                    Some(actual) if actual != index_deadline => {
                        anomalies.push(ExpiryIndexAnomaly {
                            key: String::from_utf8_lossy(key).into_owned(),
                            kind: ExpiryIndexAnomalyKind::DeadlineMismatch,
                        })
                    }
                    Some(_) => {}
                },
            }
        }

        // Direction 2: every entry with a deadline must be in the index.
        for (key, entry) in self.data.iter() {
            if entry.metadata.expires_at.is_some() && self.expiry_index.get(key).is_none() {
                anomalies.push(ExpiryIndexAnomaly {
                    key: String::from_utf8_lossy(key).into_owned(),
                    kind: ExpiryIndexAnomalyKind::IndexMissing,
                });
            }
        }

        anomalies
    }

    fn scan(&self, cursor: u64, count: usize, pattern: Option<&[u8]>) -> (u64, Vec<Bytes>) {
        self.scan_filtered(cursor, count, pattern, None)
    }

    fn scan_filtered(
        &self,
        cursor: u64,
        count: usize,
        pattern: Option<&[u8]>,
        key_type: Option<KeyType>,
    ) -> (u64, Vec<Bytes>) {
        // Content-hash cursor: order the scannable keyspace by a stable hash of
        // each key rather than by griddle's iteration position. The position
        // order shifts whenever the table resizes (incremental rehash on
        // insert), so a positional cursor could skip keys that were present for
        // the whole scan. Hashing by key content makes the ordering independent
        // of table layout, so a key present throughout the scan is always
        // returned — the guarantee Redis provides via reverse-binary bucket
        // iteration (not available over griddle's SwissTable). MATCH/TYPE are
        // post-filters that never move the cursor, matching Redis.
        //
        // The per-shard cursor rides in the 48-bit position field of the
        // cross-shard SCAN cursor (see `frogdb_commands::scan::cursor`), so the
        // hash is masked to 48 bits. Collisions only ever cause a duplicate or a
        // (vanishingly rare) skip, never corruption; SCAN already permits dups.
        let mut ordered: Vec<(u64, &Bytes, &Entry)> = self
            .data
            .iter()
            .filter(|(_, entry)| !entry.metadata.is_expired())
            .map(|(key, entry)| (scan_cursor_hash(key), key, entry))
            .collect();
        ordered.sort_unstable_by_key(|(hash, _, _)| *hash);

        // Resume at the first key whose hash is >= the cursor. Cursor 0 starts
        // from the beginning; a returned cursor of 0 means the shard is done.
        let start = if cursor == 0 {
            0
        } else {
            ordered.partition_point(|(hash, _, _)| *hash < cursor)
        };

        let mut results = Vec::with_capacity(count);
        let mut next_cursor = 0u64;

        for (hash, key, entry) in ordered.into_iter().skip(start) {
            if results.len() >= count {
                // Stop before emitting this key; resume here next call.
                next_cursor = hash;
                break;
            }

            let pattern_matches = match pattern {
                Some(p) => glob_match(p, key),
                None => true,
            };
            let type_matches = match key_type {
                Some(filter_type) => entry.key_type == filter_type,
                None => true,
            };
            if pattern_matches && type_matches {
                results.push(key.clone());
            }
        }

        (next_cursor, results)
    }

    fn clear(&mut self) {
        // Clear every value in this shard's warm CF with a single range
        // tombstone (best-effort; a no-op when the tier is unconfigured). This
        // runs synchronously on the shard thread — the warm CF's only writer —
        // so it is ordered before any later spill. The primary CF is cleared
        // separately through the WAL flush pipeline (`WalStrategy::ClearShard`).
        // The two clears are NOT crash-atomic: a crash between them leaves the
        // warm CF cleared but the primary CF not yet (or vice versa never —
        // warm goes first). Recovery then resurrects only hot keys, matching
        // the pre-clear-shard behavior for exactly the un-cleared CF; no path
        // deletes data that should survive.
        if self.warm_tier.warm_keys() > 0 {
            self.warm_tier.clear_range();
        }
        self.data.clear();
        self.expiry_index = ExpiryIndex::new();
        self.ts_labels.clear();
        self.field_expiry_index = FieldExpiryIndex::new();
        self.memory_used = 0;
        self.warm_tier.reset_keys();
        self.keysizes.clear();
        self.pending_keysizes_refreshes.clear();
    }

    fn all_keys(&self) -> Vec<Bytes> {
        self.data.keys().cloned().collect()
    }

    // ========================================================================
    // Expiry-aware methods
    // ========================================================================

    fn get_with_expiry_check(&mut self, key: &[u8]) -> Option<Arc<Value>> {
        // Check for expiry and delete if expired
        if self.check_and_delete_expired(key) {
            return None;
        }

        // Fast path: hot value
        if let Some(entry) = self.data.get_mut(key) {
            if !self.suppress_touch {
                entry.metadata.touch();
                entry.metadata.lfu_counter = crate::eviction::lfu_log_incr(
                    entry.metadata.lfu_counter,
                    crate::eviction::DEFAULT_LFU_LOG_FACTOR,
                );
            }
            if let Some(v) = entry.hot_value() {
                return Some(v.clone());
            }
        }

        // Slow path: unspill from warm tier
        let value = self.unspill_key(key)?;
        if let Some(entry) = self.data.get_mut(key)
            && !self.suppress_touch
        {
            entry.metadata.touch();
            entry.metadata.lfu_counter = crate::eviction::lfu_log_incr(
                entry.metadata.lfu_counter,
                crate::eviction::DEFAULT_LFU_LOG_FACTOR,
            );
        }
        Some(value)
    }

    fn purge_if_expired(&mut self, key: &[u8]) -> bool {
        self.check_and_delete_expired(key)
    }

    fn set_with_options(&mut self, key: Bytes, value: Value, opts: SetOptions) -> SetResult {
        // Check condition (NX/XX)
        let key_exists = self.data.contains_key(&key) && !self.check_and_delete_expired(&key);

        match opts.condition {
            SetCondition::NX if key_exists => return SetResult::NotSet,
            SetCondition::XX if !key_exists => return SetResult::NotSet,
            _ => {}
        }

        // Determine the expiry for the new entry
        let new_expiry = if opts.keep_ttl && key_exists {
            // Preserve existing TTL
            self.get_expiry(&key)
        } else if let Some(expiry) = opts.expiry {
            expiry.to_instant()
        } else {
            None
        };

        let mut metadata = KeyMetadata::new(0);
        metadata.expires_at = new_expiry;

        // The seam reconciles memory, histograms, warm CF, and all three
        // expiry/label indexes; it also hands back the old hot value.
        let old_value = self.replace_entry(key, value, metadata);

        if opts.return_old {
            SetResult::OkWithOldValue(old_value.map(Arc::unwrap_or_clone))
        } else {
            SetResult::Ok
        }
    }

    fn set_expiry(&mut self, key: &[u8], expires_at: Instant) -> bool {
        // Check and delete if expired first
        if self.check_and_delete_expired(key) {
            return false;
        }

        if let Some(entry) = self.data.get_mut(key) {
            entry.metadata.expires_at = Some(expires_at);
            self.expiry_index
                .set(Bytes::copy_from_slice(key), expires_at);
            true
        } else {
            false
        }
    }

    fn get_expiry(&self, key: &[u8]) -> Option<Instant> {
        self.data.get(key).and_then(|e| e.metadata.expires_at)
    }

    fn persist(&mut self, key: &[u8]) -> bool {
        if let Some(entry) = self.data.get_mut(key)
            && entry.metadata.expires_at.is_some()
        {
            entry.metadata.expires_at = None;
            self.expiry_index.remove(key);
            return true;
        }
        false
    }

    fn touch(&mut self, key: &[u8]) -> bool {
        // Check and delete if expired first
        if self.check_and_delete_expired(key) {
            return false;
        }

        if let Some(entry) = self.data.get_mut(key) {
            entry.metadata.touch();
            true
        } else {
            false
        }
    }

    fn get_and_delete(&mut self, key: &[u8]) -> Option<Value> {
        // Check expiry first
        if self.check_and_delete_expired(key) {
            return None;
        }

        // A warm key holds its value on disk, not in the returned `Entry`, so
        // read (and delete) it from the CF *before* `uninstall` retires the
        // entry; `uninstall`'s own best-effort CF delete then no-ops.
        let warm_bytes = match self.data.get(key) {
            Some(e) if !e.is_hot() => self.warm_tier.take(key),
            _ => None,
        };

        // All six side structures (and the warm-key count) settle here.
        let entry = self.uninstall(key)?;
        match entry.location {
            ValueLocation::Hot(arc) => {
                Some(Arc::try_unwrap(arc).unwrap_or_else(|arc| (*arc).clone()))
            }
            ValueLocation::Warm => warm_bytes
                .and_then(|d| deserialize(&d).ok())
                .map(|(value, _metadata)| value),
        }
    }

    fn get_mut(&mut self, key: &[u8]) -> Option<&mut Value> {
        // Check expiry first
        if self.check_and_delete_expired(key) {
            return None;
        }

        // Unspill warm values before returning mutable reference
        if let Some(entry) = self.data.get(key)
            && !entry.is_hot()
        {
            self.unspill_key(key);
        }

        // Snapshot size state before mutation for the deferred refresh
        // (histograms + memory accounting). This must happen before the
        // mutable borrow below. Every hot value participates — types outside
        // the keysize histograms still need their memory delta applied.
        // The old-memory side of the snapshot is the *accounted* size, so the
        // flush moves `memory_used` by exactly `new_live − accounted`.
        // If the key is already pending, the first snapshot (the last-flushed
        // state) wins: pushing a mid-command state would double-apply the
        // delta between the two snapshots.
        if let Some(entry) = self.data.get(key)
            && let Some(snapshot) = Self::histogram_snapshot(entry)
            && !self
                .pending_keysizes_refreshes
                .iter()
                .any(|(k, ..)| k.as_ref() == key)
        {
            self.pending_keysizes_refreshes
                .push((Bytes::copy_from_slice(key), snapshot));
        }

        let suppress = self.suppress_touch;
        self.data.get_mut(key).and_then(|e| {
            if !suppress {
                e.metadata.touch();
                e.metadata.lfu_counter = crate::eviction::lfu_log_incr(
                    e.metadata.lfu_counter,
                    crate::eviction::DEFAULT_LFU_LOG_FACTOR,
                );
            }
            match &mut e.location {
                ValueLocation::Hot(arc) => Some(Arc::make_mut(arc)),
                ValueLocation::Warm => None,
            }
        })
    }

    #[allow(deprecated)]
    fn expiry_index_mut(&mut self) -> Option<&mut ExpiryIndex> {
        Some(&mut self.expiry_index)
    }

    fn get_expired_keys(&self, now: std::time::Instant) -> Vec<Bytes> {
        self.expiry_index.get_expired(now)
    }

    fn get_expired_keys_limited(&self, now: std::time::Instant, limit: usize) -> Vec<Bytes> {
        self.expiry_index.get_expired_limited(now, limit)
    }

    fn keys_with_expiry_count(&self) -> usize {
        self.expiry_index.len()
    }

    fn set_field_expiry(&mut self, key: &[u8], field: &[u8], expires_at: Instant) {
        self.field_expiry_index.set(
            Bytes::copy_from_slice(key),
            Bytes::copy_from_slice(field),
            expires_at,
        );
    }

    fn remove_field_expiry(&mut self, key: &[u8], field: &[u8]) {
        self.field_expiry_index.remove(key, field);
    }

    fn remove_all_field_expiries(&mut self, key: &[u8]) {
        self.field_expiry_index.remove_key(key);
    }

    fn get_field_expiry(&self, key: &[u8], field: &[u8]) -> Option<Instant> {
        self.field_expiry_index.get(key, field)
    }

    fn get_expired_fields(&self, now: Instant) -> Vec<(Bytes, Bytes)> {
        self.field_expiry_index.get_expired(now)
    }

    fn get_expired_fields_limited(&self, now: Instant, limit: usize) -> Vec<(Bytes, Bytes)> {
        self.field_expiry_index.get_expired_limited(now, limit)
    }

    fn purge_expired_hash_fields(&mut self, key: &[u8]) -> usize {
        // Get the hash value and remove expired fields
        let now = Instant::now();

        // Size snapshot for inline reconciliation: this path mutates the value
        // without going through `get_mut()`'s deferred-refresh mechanism. If a
        // deferred refresh is already pending for this key, skip — the
        // end-of-command flush will observe the post-purge state and settle
        // everything once.
        let pre_snapshot = if self
            .pending_keysizes_refreshes
            .iter()
            .any(|(k, ..)| k.as_ref() == key)
        {
            None
        } else {
            self.data.get(key).and_then(Self::histogram_snapshot)
        };

        // Get mutable access to the hash and remove expired fields
        let removed_fields = {
            let value = match self.data.get_mut(key) {
                Some(entry) => match &mut entry.location {
                    ValueLocation::Hot(arc) => Arc::make_mut(arc),
                    ValueLocation::Warm => return 0,
                },
                None => return 0,
            };
            let hash = match value.as_hash_mut() {
                Some(h) => h,
                None => return 0,
            };
            hash.remove_expired_fields(now)
        };

        let count = removed_fields.len();

        // Update field expiry index
        for field in &removed_fields {
            self.field_expiry_index.remove(key, field);
        }

        // Reconcile histograms + memory for the in-place shrink before any
        // delete below, so the delete refunds the freshly accounted size.
        if count > 0
            && let Some(snap) = pre_snapshot
        {
            self.resize(key, Some(snap), snap.key_mem);
        }

        // If hash is now empty, delete the key
        if count > 0
            && let Some(entry) = self.data.get(key)
            && let Some(v) = entry.hot_value()
            && let Some(hash) = v.as_hash()
            && hash.is_empty()
        {
            self.delete(key);
        }

        count
    }

    fn set_expiry_suppressed(&mut self, suppressed: bool) {
        self.expiry_suppressed = suppressed;
    }

    fn set_suppress_touch(&mut self, suppress: bool) {
        self.suppress_touch = suppress;
    }

    // ========================================================================
    // Eviction support methods
    // ========================================================================

    fn random_key(&self) -> Option<Bytes> {
        if self.data.is_empty() {
            return None;
        }

        let mut rng = rand::rng();
        // Skip warm entries — they're already spilled
        self.data
            .iter()
            .filter(|(_, e)| e.is_hot())
            .map(|(k, _)| k)
            .choose(&mut rng)
            .cloned()
    }

    fn sample_keys(&self, count: usize) -> Vec<Bytes> {
        if self.data.is_empty() || count == 0 {
            return vec![];
        }

        let mut rng = rand::rng();
        // Skip warm entries — they're already spilled
        self.data
            .iter()
            .filter(|(_, e)| e.is_hot())
            .map(|(k, _)| k)
            .sample(&mut rng, count)
            .into_iter()
            .cloned()
            .collect()
    }

    fn sample_volatile_keys(&self, count: usize) -> Vec<Bytes> {
        // Sample from the expiry index which only contains volatile keys
        self.expiry_index.sample(count)
    }

    fn get_metadata(&self, key: &[u8]) -> Option<KeyMetadata> {
        self.data.get(key).map(|e| e.metadata.clone())
    }

    fn idle_time(&self, key: &[u8]) -> Option<Duration> {
        self.data
            .get(key)
            .map(|e| Instant::now().duration_since(e.metadata.last_access))
    }

    fn update_lfu_counter(&mut self, key: &[u8], log_factor: u8) {
        if let Some(entry) = self.data.get_mut(key) {
            entry.metadata.lfu_counter =
                crate::eviction::lfu_log_incr(entry.metadata.lfu_counter, log_factor);
        }
    }

    fn get_lfu_value(&self, key: &[u8], decay_time: u64) -> Option<u8> {
        self.data.get(key).map(|e| {
            let minutes_since = Instant::now()
                .duration_since(e.metadata.last_access)
                .as_secs()
                / 60;
            crate::eviction::lfu_decay(e.metadata.lfu_counter, minutes_since, decay_time)
        })
    }

    // ========================================================================
    // Cluster slot support methods
    // ========================================================================

    fn keys_in_slot(&self, slot: u16, count: usize) -> Vec<Bytes> {
        self.data
            .keys()
            .filter(|k| slot_for_key(k) == slot)
            .take(count)
            .cloned()
            .collect()
    }

    fn count_keys_in_slot(&self, slot: u16) -> usize {
        self.data.keys().filter(|k| slot_for_key(k) == slot).count()
    }

    // ========================================================================
    // Dirty tracking
    // ========================================================================

    fn dirty(&self) -> u64 {
        self.dirty
    }

    fn increment_dirty(&mut self, count: u64) {
        self.dirty = self.dirty.wrapping_add(count);
    }

    fn ts_labels(&self) -> Option<&TimeSeriesLabels> {
        Some(&self.ts_labels)
    }

    fn ts_labels_mut(&mut self) -> Option<&mut TimeSeriesLabels> {
        Some(&mut self.ts_labels)
    }

    fn warm_tier(&self) -> Option<&WarmTier> {
        Some(&self.warm_tier)
    }

    fn keysizes(&self) -> Option<&KeysizeHistograms> {
        Some(&self.keysizes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::glob::glob_match;

    #[test]
    fn recompute_matches_tracked_after_inserts() {
        let mut store = HashMapStore::new();
        assert_eq!(store.recompute_memory_used(), store.memory_used());
        store.set(Bytes::from("a"), Value::string(Bytes::from("hello")));
        store.set(Bytes::from("b"), Value::string(Bytes::from("world")));
        assert_eq!(store.recompute_memory_used(), store.memory_used());
        store.delete(b"a");
        assert_eq!(store.recompute_memory_used(), store.memory_used());
    }

    #[test]
    fn recompute_reveals_unflushed_inplace_growth() {
        // In-place growth via get_mut is invisible to the tracked counter until
        // flush_keysizes_refreshes runs; recompute (live) reveals the drift, and
        // flushing reconciles it. This is exactly what DEBUG MEMORY-CHECK detects.
        let mut store = HashMapStore::new();
        store.set(Bytes::from("l"), Value::list());
        let before = store.recompute_memory_used();
        assert_eq!(before, store.memory_used());

        if let Some(v) = store.get_mut(b"l")
            && let Some(list) = v.as_list_mut()
        {
            for i in 0..100u32 {
                list.push_back(Bytes::from(format!("element-{i}")));
            }
        }
        // Live recompute now exceeds the still-unreconciled tracked counter.
        assert!(store.recompute_memory_used() > store.memory_used());

        store.flush_keysizes_refreshes();
        assert_eq!(store.recompute_memory_used(), store.memory_used());
    }

    #[test]
    fn expiry_index_audit_clean_when_consistent() {
        use std::time::{Duration, Instant};
        let mut store = HashMapStore::new();
        store.set(Bytes::from("persistent"), Value::string(Bytes::from("v")));
        store.set(Bytes::from("ttl"), Value::string(Bytes::from("v")));
        store.set_expiry(b"ttl", Instant::now() + Duration::from_secs(3600));
        // Persistent key not in index, ttl key in index with a matching deadline.
        assert!(store.audit_expiry_index().is_empty());
    }

    #[test]
    fn expiry_index_audit_flags_stale_entry_after_persist() {
        use std::time::{Duration, Instant};
        let mut store = HashMapStore::new();
        store.set(Bytes::from("k"), Value::string(Bytes::from("v")));
        store.set_expiry(b"k", Instant::now() + Duration::from_secs(3600));
        assert!(store.audit_expiry_index().is_empty());

        // Directly corrupt: leave a stale index entry for a now-persistent key.
        // (Simulates proposal-30's class of bug: a write path that forgot to
        // clear the index on overwrite/persist.)
        store.force_index_entry_for_test(Bytes::from("k"), Instant::now() + Duration::from_secs(1));
        store.persist(b"k"); // makes the entry persistent AND clears the index...
        // ...so re-corrupt after persist to model the leak:
        store.force_index_entry_for_test(Bytes::from("k"), Instant::now() + Duration::from_secs(1));

        let anomalies = store.audit_expiry_index();
        assert_eq!(anomalies.len(), 1);
        assert_eq!(anomalies[0].key, "k");
        assert!(matches!(
            anomalies[0].kind,
            ExpiryIndexAnomalyKind::KeyPersistent
        ));
    }

    #[test]
    fn expiry_index_audit_flags_index_missing() {
        use std::time::{Duration, Instant};
        let mut store = HashMapStore::new();
        store.set(Bytes::from("k"), Value::string(Bytes::from("v")));
        store.set_expiry(b"k", Instant::now() + Duration::from_secs(3600));
        // Drop only the index entry, leaving the entry's deadline in place.
        store.force_drop_index_entry_for_test(b"k");
        let anomalies = store.audit_expiry_index();
        assert_eq!(anomalies.len(), 1);
        assert!(matches!(
            anomalies[0].kind,
            ExpiryIndexAnomalyKind::IndexMissing
        ));
    }

    #[test]
    fn expiry_index_audit_flags_key_missing() {
        use std::time::{Duration, Instant};
        let mut store = HashMapStore::new();
        // An index entry for a key that was never stored: direction-1 KeyMissing.
        store.force_index_entry_for_test(
            Bytes::from("ghost"),
            Instant::now() + Duration::from_secs(60),
        );
        let anomalies = store.audit_expiry_index();
        assert_eq!(anomalies.len(), 1);
        assert_eq!(anomalies[0].key, "ghost");
        assert!(matches!(
            anomalies[0].kind,
            ExpiryIndexAnomalyKind::KeyMissing
        ));
    }

    #[test]
    fn expiry_index_audit_flags_deadline_mismatch() {
        use std::time::{Duration, Instant};
        let mut store = HashMapStore::new();
        store.set(Bytes::from("k"), Value::string(Bytes::from("v")));
        store.set_expiry(b"k", Instant::now() + Duration::from_secs(3600));
        assert!(store.audit_expiry_index().is_empty());
        // Overwrite the index entry's deadline so it disagrees with the entry's.
        store.force_index_entry_for_test(Bytes::from("k"), Instant::now() + Duration::from_secs(1));
        let anomalies = store.audit_expiry_index();
        assert_eq!(anomalies.len(), 1);
        assert_eq!(anomalies[0].key, "k");
        assert!(matches!(
            anomalies[0].kind,
            ExpiryIndexAnomalyKind::DeadlineMismatch
        ));
    }

    #[test]
    fn test_store_set_get() {
        let mut store = HashMapStore::new();

        store.set(Bytes::from("key1"), Value::string("value1"));
        let value = store.get(b"key1").unwrap();

        let sv = value.as_string().expect("expected string value");
        assert_eq!(sv.as_bytes().as_ref(), b"value1");
    }

    #[test]
    fn test_store_delete() {
        let mut store = HashMapStore::new();

        store.set(Bytes::from("key1"), Value::string("value1"));
        assert!(store.contains(b"key1"));

        assert!(store.delete(b"key1"));
        assert!(!store.contains(b"key1"));
        assert!(!store.delete(b"key1"));
    }

    #[test]
    fn test_store_key_type() {
        let mut store = HashMapStore::new();

        assert_eq!(store.key_type(b"missing"), KeyType::None);

        store.set(Bytes::from("string_key"), Value::string("value"));
        assert_eq!(store.key_type(b"string_key"), KeyType::String);
    }

    #[test]
    fn test_store_memory_tracking() {
        let mut store = HashMapStore::new();

        assert_eq!(store.memory_used(), 0);

        store.set(Bytes::from("key"), Value::string("value"));
        assert!(store.memory_used() > 0);

        store.delete(b"key");
        assert_eq!(store.memory_used(), 0);
    }

    // ========================================================================
    // Reconciliation seam: overwrite must retire stale expiry bookkeeping
    // ========================================================================

    #[test]
    fn set_overwrite_clears_stale_key_expiry_index() {
        // SET k v EX 100 → MSET-style plain overwrite. The overwrite clears the
        // TTL (fresh metadata), so the expiry index must forget the key too —
        // otherwise active expiry later deletes a persistent key (data loss).
        let mut store = HashMapStore::new();
        store.set(Bytes::from("k"), Value::string("v1"));
        assert!(store.set_expiry(b"k", Instant::now() + Duration::from_secs(100)));
        assert_eq!(store.keys_with_expiry_count(), 1);

        store.set(Bytes::from("k"), Value::string("v2"));

        assert_eq!(store.get_expiry(b"k"), None, "overwrite clears the TTL");
        assert_eq!(
            store.keys_with_expiry_count(),
            0,
            "expiry index must not retain the overwritten key"
        );
        // Even at a `now` past the old deadline, nothing is due.
        let far_future = Instant::now() + Duration::from_secs(1000);
        assert!(store.get_expired_keys(far_future).is_empty());
        assert!(store.contains(b"k"));
    }

    #[test]
    fn set_overwrite_clears_stale_field_expiry_index() {
        use crate::types::{HashValue, ListpackThresholds};

        let mut store = HashMapStore::new();
        let mut hash = HashValue::new();
        hash.set(
            Bytes::from("f"),
            Bytes::from("v"),
            ListpackThresholds::DEFAULT_HASH,
        );
        store.set(Bytes::from("h"), Value::Hash(hash));
        let deadline = Instant::now() + Duration::from_secs(100);
        store.set_field_expiry(b"h", b"f", deadline);
        assert_eq!(store.get_field_expiry(b"h", b"f"), Some(deadline));

        // Overwrite the hash with a plain string: the field TTL dies with the
        // old incarnation, so the field-expiry index must forget it.
        store.set(Bytes::from("h"), Value::string("s"));

        assert_eq!(store.get_field_expiry(b"h", b"f"), None);
        let far_future = Instant::now() + Duration::from_secs(1000);
        assert!(store.get_expired_fields(far_future).is_empty());
    }

    #[test]
    fn set_indexes_field_expiries_carried_by_the_value() {
        use crate::types::{HashValue, ListpackThresholds};

        // COPY/RESTORE hand `set` a hash that already carries field TTLs; the
        // seam must mirror them into the field-expiry index so active expiry
        // sees them.
        let mut store = HashMapStore::new();
        let deadline = Instant::now() + Duration::from_secs(100);
        let mut hash = HashValue::new();
        hash.set(
            Bytes::from("f"),
            Bytes::from("v"),
            ListpackThresholds::DEFAULT_HASH,
        );
        hash.set_field_expiry(b"f", deadline);

        store.set(Bytes::from("h"), Value::Hash(hash));

        assert_eq!(store.get_field_expiry(b"h", b"f"), Some(deadline));
    }

    #[test]
    fn set_with_options_overwrite_clears_stale_field_expiry_index() {
        use crate::types::{HashValue, ListpackThresholds};

        let mut store = HashMapStore::new();
        let mut hash = HashValue::new();
        hash.set(
            Bytes::from("f"),
            Bytes::from("v"),
            ListpackThresholds::DEFAULT_HASH,
        );
        store.set(Bytes::from("h"), Value::Hash(hash));
        store.set_field_expiry(b"h", b"f", Instant::now() + Duration::from_secs(100));

        let result =
            store.set_with_options(Bytes::from("h"), Value::string("s"), SetOptions::default());
        assert!(matches!(result, SetResult::Ok));

        assert_eq!(store.get_field_expiry(b"h", b"f"), None);
        let far_future = Instant::now() + Duration::from_secs(1000);
        assert!(store.get_expired_fields(far_future).is_empty());
    }

    #[test]
    fn restore_entry_overwrite_retires_previous_incarnation() {
        // Replaying the same key twice (snapshot + WAL) must not leak the
        // first incarnation's memory accounting or expiry-index entry.
        let mut store = HashMapStore::new();

        let mut meta1 = KeyMetadata::new(0);
        meta1.expires_at = Some(Instant::now() + Duration::from_secs(100));
        store.restore_entry(Bytes::from("k"), Value::string("first"), meta1);
        assert_eq!(store.keys_with_expiry_count(), 1);

        // Second replay: no expiry this time.
        store.restore_entry(
            Bytes::from("k"),
            Value::string("second"),
            KeyMetadata::new(0),
        );
        assert_eq!(store.keys_with_expiry_count(), 0);

        // memory_used equals exactly one fresh insert of the same entry.
        let mut fresh = HashMapStore::new();
        fresh.set(Bytes::from("k"), Value::string("second"));
        assert_eq!(store.memory_used(), fresh.memory_used());
    }

    // ========================================================================
    // Memory accounting: in-place mutation must reconcile memory_used
    // ========================================================================

    /// A list value with `n` copies of a 100-byte element.
    fn list_value(n: usize) -> Value {
        let mut list = crate::types::ListValue::new();
        for _ in 0..n {
            list.push_back(Bytes::from(vec![b'x'; 100]));
        }
        Value::List(list)
    }

    #[test]
    fn flush_applies_memory_delta_after_in_place_growth() {
        let mut store = HashMapStore::new();
        store.set(Bytes::from("k"), list_value(1));
        let before = store.memory_used();

        // Grow the list in place via get_mut (the RPUSH path).
        {
            let list = store.get_mut(b"k").unwrap().as_list_mut().unwrap();
            for _ in 0..100 {
                list.push_back(Bytes::from(vec![b'x'; 100]));
            }
        }
        store.flush_keysizes_refreshes();

        assert!(
            store.memory_used() > before,
            "in-place growth must be visible to memory_used after flush"
        );
        // The accounted state must equal a fresh store holding the same value.
        let mut fresh = HashMapStore::new();
        fresh.set(Bytes::from("k"), list_value(101));
        assert_eq!(store.memory_used(), fresh.memory_used());
        // The accounted per-key size follows too (feeds eviction's
        // memory_freed metric via get_metadata).
        assert_eq!(
            store.get_metadata(b"k").unwrap().memory_size,
            fresh.get_metadata(b"k").unwrap().memory_size
        );
    }

    #[test]
    fn grow_flush_delete_returns_to_zero() {
        let mut store = HashMapStore::new();
        store.set(Bytes::from("k"), list_value(1));
        {
            let list = store.get_mut(b"k").unwrap().as_list_mut().unwrap();
            for _ in 0..100 {
                list.push_back(Bytes::from(vec![b'x'; 100]));
            }
        }
        store.flush_keysizes_refreshes();

        assert!(store.delete(b"k"));
        assert_eq!(store.memory_used(), 0, "no drift after grow+flush+delete");
    }

    #[test]
    fn grow_then_delete_without_flush_does_not_underflow() {
        // The historical underflow: delete used to refund the *live* size of a
        // grown value while memory_used had only been charged the creation
        // size. With accounted-size refunds the pair is symmetric even when
        // the deferred flush never ran.
        let mut store = HashMapStore::new();
        store.set(Bytes::from("k"), list_value(1));
        store.set(Bytes::from("other"), Value::string("v"));
        let other_only = {
            let mut fresh = HashMapStore::new();
            fresh.set(Bytes::from("other"), Value::string("v"));
            fresh.memory_used()
        };

        {
            let list = store.get_mut(b"k").unwrap().as_list_mut().unwrap();
            for _ in 0..100 {
                list.push_back(Bytes::from(vec![b'x'; 100]));
            }
        }
        // No flush: delete immediately.
        assert!(store.delete(b"k"));

        assert_eq!(
            store.memory_used(),
            other_only,
            "grown-then-deleted key must refund exactly what was charged"
        );
        // The pending snapshot for the deleted key must not be replayed later.
        store.flush_keysizes_refreshes();
        assert_eq!(store.memory_used(), other_only);
    }

    #[test]
    fn shrink_then_flush_reduces_memory_used() {
        let mut store = HashMapStore::new();
        store.set(Bytes::from("k"), list_value(100));
        let before = store.memory_used();

        {
            let list = store.get_mut(b"k").unwrap().as_list_mut().unwrap();
            for _ in 0..99 {
                list.pop_back();
            }
        }
        store.flush_keysizes_refreshes();

        assert!(store.memory_used() < before);
        let mut fresh = HashMapStore::new();
        fresh.set(Bytes::from("k"), list_value(1));
        assert_eq!(store.memory_used(), fresh.memory_used());
        assert!(store.delete(b"k"));
        assert_eq!(store.memory_used(), 0);
    }

    #[test]
    fn repeated_get_mut_in_one_command_does_not_double_count() {
        // Two get_mut calls before one flush: only the first snapshot may be
        // applied, otherwise the delta between snapshots is charged twice.
        let mut store = HashMapStore::new();
        store.set(Bytes::from("k"), list_value(1));

        {
            let list = store.get_mut(b"k").unwrap().as_list_mut().unwrap();
            list.push_back(Bytes::from(vec![b'x'; 100]));
        }
        {
            let list = store.get_mut(b"k").unwrap().as_list_mut().unwrap();
            list.push_back(Bytes::from(vec![b'x'; 100]));
        }
        store.flush_keysizes_refreshes();

        let mut fresh = HashMapStore::new();
        fresh.set(Bytes::from("k"), list_value(3));
        assert_eq!(store.memory_used(), fresh.memory_used());
    }

    #[test]
    fn overwrite_after_unflushed_growth_settles_cleanly() {
        // get_mut growth, then the key is overwritten by set() before the
        // flush runs: the overwrite retires the accounted size and discards
        // the stale snapshot; the flush must not replay it against the new
        // incarnation.
        let mut store = HashMapStore::new();
        store.set(Bytes::from("k"), list_value(1));
        {
            let list = store.get_mut(b"k").unwrap().as_list_mut().unwrap();
            for _ in 0..100 {
                list.push_back(Bytes::from(vec![b'x'; 100]));
            }
        }
        store.set(Bytes::from("k"), Value::string("small"));
        store.flush_keysizes_refreshes();

        let mut fresh = HashMapStore::new();
        fresh.set(Bytes::from("k"), Value::string("small"));
        assert_eq!(store.memory_used(), fresh.memory_used());
    }

    #[test]
    fn purge_expired_hash_fields_reconciles_memory() {
        use crate::types::{HashValue, ListpackThresholds};

        let mut store = HashMapStore::new();
        let past = Instant::now() - Duration::from_secs(60);
        let mut hash = HashValue::new();
        hash.set(
            Bytes::from("gone"),
            Bytes::from(vec![b'x'; 1000]),
            ListpackThresholds::DEFAULT_HASH,
        );
        hash.set(
            Bytes::from("stays"),
            Bytes::from("v"),
            ListpackThresholds::DEFAULT_HASH,
        );
        hash.set_field_expiry(b"gone", past);
        store.set(Bytes::from("h"), Value::Hash(hash));
        store.set_field_expiry(b"h", b"gone", past);
        let before = store.memory_used();

        assert_eq!(store.purge_expired_hash_fields(b"h"), 1);

        assert!(
            store.memory_used() < before,
            "purging a large field must shrink memory_used"
        );
        assert!(store.delete(b"h"));
        assert_eq!(store.memory_used(), 0, "no drift after purge+delete");
    }

    #[test]
    fn hot_expired_key_get_vs_get_with_expiry_check_contract() {
        // Pins the contract difference the scatter-path fix (round-7 follow-up
        // item 3) depends on: `Store::get` is the raw hot fast-path with NO
        // expiry check, so a hot key past its TTL but not yet lazily purged
        // still comes back from `get`. `get_with_expiry_check` is the
        // expiry-aware accessor (same one `get_typed`/single-key GET use) —
        // it lazily purges the key and returns `None`. Callers that need
        // Redis's "expired reads as absent" semantics (scatter MGET, COPY,
        // DUMP) must use `get_with_expiry_check`, never raw `get`.
        let mut store = HashMapStore::new();
        store.set(Bytes::from("k"), Value::string("v"));
        let past = Instant::now() - Duration::from_secs(60);
        assert!(store.set_expiry(b"k", past));

        // The key is still hot and present in the map (no purge has run yet).
        assert!(store.contains(b"k"));

        // Raw `get` ignores expiry: the hot value is still handed back.
        assert!(
            store.get(b"k").is_some(),
            "raw get must not filter expired-but-unpurged hot keys"
        );

        // `get_with_expiry_check` lazily purges and reports absent.
        assert!(
            store.get_with_expiry_check(b"k").is_none(),
            "expiry-aware get must treat a hot expired key as absent"
        );
        assert!(
            !store.contains(b"k"),
            "expiry-aware get must have lazily deleted the expired key"
        );
    }

    #[test]
    fn warm_tier_accessor_exposes_counters() {
        let mut store = HashMapStore::new();
        store.set(Bytes::from("k"), Value::string("v"));

        // Inherent accessor (concrete callers).
        assert_eq!(store.warm_tier().warm_keys(), 0);
        assert_eq!(store.warm_tier().spills(), 0);
        assert_eq!(store.warm_tier().unspills(), 0);
        assert_eq!(store.warm_tier().expired_on_unspill(), 0);
        // Hot-key count is derived: len() - warm_keys().
        assert_eq!(store.len() - store.warm_tier().warm_keys(), 1);

        // Trait accessor (dyn Store callers) always yields the subsystem.
        let dyn_store: &dyn Store = &store;
        let warm = dyn_store
            .warm_tier()
            .expect("HashMapStore always has a warm tier");
        assert_eq!(warm.warm_keys(), 0);
    }

    #[test]
    fn ts_labels_accessor_reflects_indexed_timeseries() {
        use crate::TimeSeriesValue;

        let mut store = HashMapStore::new();
        assert!(
            store
                .ts_labels()
                .expect("HashMapStore has a label index")
                .index()
                .is_empty(),
            "fresh store has no indexed labels"
        );

        let mut ts = TimeSeriesValue::new();
        ts.set_labels(vec![("region".to_string(), "us".to_string())]);
        store.set(Bytes::from("ts:1"), Value::TimeSeries(ts));

        // set() reconciles labels into the index; the read accessor sees them.
        let labels = store.ts_labels().unwrap();
        assert_eq!(labels.index().label_values("region"), vec!["us"]);

        // The mutable accessor reaches the same index.
        assert!(
            store
                .ts_labels_mut()
                .unwrap()
                .index_mut()
                .get_labels(b"ts:1")
                .is_some()
        );
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"prefix*", b"prefix123"));
        assert!(!glob_match(b"prefix*", b"notprefix"));
        assert!(glob_match(b"*suffix", b"anysuffix"));
        assert!(!glob_match(b"*suffix", b"suffixnot"));
        assert!(glob_match(b"exact", b"exact"));
        assert!(!glob_match(b"exact", b"notexact"));
    }

    // ========================================================================
    // Active-expiry scan boundedness (TTL-avalanche regression)
    //
    // The active-expiry cycle relies on the `*_limited` scans to cap the
    // up-front clone of the due set: an unbounded scan clones every due entry
    // before any time budget is consulted, so a TTL avalanche stalls the shard.
    // These pin that the store never hands back more than `limit` entries, even
    // when the due set dwarfs the limit. They FAIL if the scan reverts to
    // cloning the whole due set (i.e. ignoring `limit`).
    // ========================================================================

    #[test]
    fn get_expired_keys_limited_caps_far_larger_due_set() {
        let mut store = HashMapStore::new();
        let past = Instant::now() - Duration::from_secs(60);
        for i in 0..10_000 {
            let key = format!("k{i}");
            store.set(Bytes::from(key.clone()), Value::string("v"));
            assert!(store.set_expiry(key.as_bytes(), past));
        }
        assert_eq!(store.keys_with_expiry_count(), 10_000);

        let now = Instant::now();
        // 10_000 due, limit 100 -> at most 100 collected, not the whole set.
        let batch = store.get_expired_keys_limited(now, 100);
        assert!(
            batch.len() <= 100,
            "bounded scan returned {} keys for a limit of 100",
            batch.len()
        );
        assert_eq!(batch.len(), 100, "a full limit's worth should be available");

        // limit == 0 collects nothing.
        assert!(store.get_expired_keys_limited(now, 0).is_empty());
    }

    #[test]
    fn get_expired_fields_limited_caps_far_larger_due_set() {
        use crate::types::{HashValue, ListpackThresholds};

        let mut store = HashMapStore::new();
        let past = Instant::now() - Duration::from_secs(60);
        // One hash carrying 10_000 already-expired fields.
        let mut hash = HashValue::new();
        for i in 0..10_000 {
            let field = format!("f{i}");
            hash.set(
                Bytes::from(field.clone()),
                Bytes::from("v"),
                ListpackThresholds::DEFAULT_HASH,
            );
            hash.set_field_expiry(field.as_bytes(), past);
        }
        store.set(Bytes::from("h"), Value::Hash(hash));
        for i in 0..10_000 {
            store.set_field_expiry(b"h", format!("f{i}").as_bytes(), past);
        }

        let now = Instant::now();
        // 10_000 due fields, limit 100 -> at most 100 collected.
        let batch = store.get_expired_fields_limited(now, 100);
        assert!(
            batch.len() <= 100,
            "bounded field scan returned {} pairs for a limit of 100",
            batch.len()
        );
        assert_eq!(batch.len(), 100, "a full limit's worth should be available");

        assert!(store.get_expired_fields_limited(now, 0).is_empty());
    }

    // ========================================================================
    // Entry-lifecycle seam (install / uninstall / resize) consistency
    //
    // Every one of these drives the store through a lifecycle op and then asserts
    // `assert_consistent()`: `recompute_memory_used`, `audit_expiry_index`, and a
    // from-scratch keysize/key-memory recompute all agree with the incremental
    // counters. That is the belt-and-suspenders guarantee the seam concentrates.
    // ========================================================================

    use crate::histogram::KeysizeType;

    /// One value of a few distinct keysize-tracked variants.
    fn sample_values() -> Vec<(Bytes, Value)> {
        use crate::types::{HashValue, ListpackThresholds};
        let mut hash = HashValue::new();
        hash.set(
            Bytes::from("f1"),
            Bytes::from("v1"),
            ListpackThresholds::DEFAULT_HASH,
        );
        hash.set(
            Bytes::from("f2"),
            Bytes::from("v2"),
            ListpackThresholds::DEFAULT_HASH,
        );
        let mut list = Value::list();
        if let Some(l) = list.as_list_mut() {
            for i in 0..8u32 {
                l.push_back(Bytes::from(format!("e{i}")));
            }
        }
        vec![
            (Bytes::from("s"), Value::string(Bytes::from("hello world"))),
            (Bytes::from("l"), list),
            (Bytes::from("h"), Value::Hash(hash)),
        ]
    }

    #[test]
    fn install_then_uninstall_returns_to_zero() {
        let mut store = HashMapStore::new();
        for (k, v) in sample_values() {
            let base_mem = store.memory_used();
            store.set(k.clone(), v);
            store.assert_consistent();
            assert!(store.memory_used() > base_mem);
            assert!(store.delete(&k));
            store.assert_consistent();
            assert_eq!(
                store.memory_used(),
                base_mem,
                "uninstall must refund exactly what install charged"
            );
        }
        assert_eq!(store.memory_used(), 0);
        assert!(store.keysizes().key_memory.is_empty());
        for &t in KeysizeType::ALL {
            assert!(
                store.keysizes().get(t).is_empty(),
                "{t:?} histogram not empty after full uninstall"
            );
        }
    }

    #[test]
    fn overwrite_reconciles_and_clears_old_indexes() {
        // Regression for the `replace_entry` silent-data-loss scar: overwriting a
        // volatile key must retire its stale expiry-index entry and every
        // histogram/memory counter of the old incarnation.
        let mut store = HashMapStore::new();
        store.set(Bytes::from("k"), Value::string(Bytes::from("original")));
        store.set_expiry(b"k", Instant::now() + Duration::from_secs(3600));
        store.assert_consistent();

        let mut list = Value::list();
        if let Some(l) = list.as_list_mut() {
            for i in 0..20u32 {
                l.push_back(Bytes::from(format!("e{i}")));
            }
        }
        store.set(Bytes::from("k"), list);
        store.assert_consistent();
        assert!(
            store.get_expiry(b"k").is_none(),
            "overwrite without a TTL must clear the expiry index"
        );
        assert!(store.keysizes().get(KeysizeType::Strings).is_empty());
        assert_eq!(store.keysizes().get(KeysizeType::Lists).total(), 1);
    }

    #[test]
    fn get_and_delete_reconciles() {
        let mut store = HashMapStore::new();
        store.set(Bytes::from("s"), Value::string(Bytes::from("payload")));
        store.set(Bytes::from("keep"), Value::string(Bytes::from("x")));
        store.assert_consistent();

        let got = store.get_and_delete(b"s").unwrap();
        assert_eq!(got.as_string().unwrap().as_bytes().as_ref(), b"payload");
        store.assert_consistent();
        assert!(!store.contains(b"s"));
        assert!(store.contains(b"keep"));
    }

    #[test]
    fn lazy_expiry_reconciles() {
        let mut store = HashMapStore::new();
        store.set(Bytes::from("k"), Value::string(Bytes::from("v")));
        store.set(Bytes::from("keep"), Value::string(Bytes::from("y")));
        // Give `k` a TTL and push it into the past; the check deletes it.
        store.set_expiry(b"k", Instant::now() - Duration::from_secs(60));
        assert!(store.check_and_delete_expired(b"k"));
        store.assert_consistent();
        assert!(!store.contains(b"k"));
        assert_eq!(store.expired_keys(), 1);
    }

    #[test]
    fn clear_reconciles() {
        let mut store = HashMapStore::new();
        for (k, v) in sample_values() {
            store.set(k, v);
        }
        store.set_expiry(b"s", Instant::now() + Duration::from_secs(100));
        store.assert_consistent();

        store.clear();
        store.assert_consistent();
        assert_eq!(store.memory_used(), 0);
        assert_eq!(store.len(), 0);
        assert!(store.keysizes().key_memory.is_empty());
    }

    #[test]
    fn resize_via_get_mut_growth_reconciles() {
        let mut store = HashMapStore::new();
        store.set(Bytes::from("l"), Value::list());
        if let Some(v) = store.get_mut(b"l")
            && let Some(list) = v.as_list_mut()
        {
            for i in 0..500u32 {
                list.push_back(Bytes::from(format!("element-{i}")));
            }
        }
        store.flush_keysizes_refreshes();
        store.assert_consistent();
        assert_eq!(store.keysizes().get(KeysizeType::Lists).total(), 1);
    }

    #[test]
    fn resize_grow_then_shrink_is_neutral() {
        let mut store = HashMapStore::new();
        store.set(Bytes::from("l"), Value::list());
        store.flush_keysizes_refreshes();
        let base_mem = store.memory_used();
        let base_list_bins = *store.keysizes().get(KeysizeType::Lists).bins();
        let base_key_mem = *store.keysizes().key_memory.bins();

        // Grow.
        if let Some(v) = store.get_mut(b"l")
            && let Some(list) = v.as_list_mut()
        {
            for i in 0..64u32 {
                list.push_back(Bytes::from(format!("element-{i}")));
            }
        }
        store.flush_keysizes_refreshes();
        store.assert_consistent();
        assert!(store.memory_used() > base_mem);

        // Shrink back to empty.
        if let Some(v) = store.get_mut(b"l")
            && let Some(list) = v.as_list_mut()
        {
            while list.pop_back().is_some() {}
        }
        store.flush_keysizes_refreshes();
        store.assert_consistent();
        assert_eq!(
            store.memory_used(),
            base_mem,
            "resize symmetry: a grow then an equivalent shrink is memory-neutral"
        );
        assert_eq!(
            *store.keysizes().get(KeysizeType::Lists).bins(),
            base_list_bins
        );
        assert_eq!(*store.keysizes().key_memory.bins(), base_key_mem);
    }

    // ------------------------------------------------------------------------
    // Warm-tier lifecycle (spill / unspill) — needs a configured warm store.
    // ------------------------------------------------------------------------

    fn store_with_warm() -> (HashMapStore, tempfile::TempDir) {
        use frogdb_persistence::{RocksConfig, RocksStore};
        let tmp = tempfile::TempDir::new().unwrap();
        let rocks = Arc::new(
            RocksStore::open_with_warm(tmp.path(), 1, &RocksConfig::default(), true).unwrap(),
        );
        let mut store = HashMapStore::new();
        store.set_warm_store(rocks, 0);
        (store, tmp)
    }

    #[test]
    fn spill_unspill_histograms_stay_consistent() {
        // Regression: `spill_key`/`unspill_key` used to skip the keysize
        // histograms, leaving a spilled key counted in the per-type and
        // key-memory bins even though its value bytes had left RAM. Against that
        // old behavior every `assert_consistent()` below fails (tracked bins
        // exceed the live recompute), and the `strings_before - 1` assertion is
        // false (the count would not move).
        let (mut store, _tmp) = store_with_warm();
        store.set(
            Bytes::from("s"),
            Value::string(Bytes::from("a reasonably sized string value")),
        );
        store.set(Bytes::from("keep"), Value::string(Bytes::from("y")));
        store.assert_consistent();
        let strings_before = store.keysizes().get(KeysizeType::Strings).total();

        store.spill_key(b"s").unwrap();
        store.assert_consistent();
        assert_eq!(store.warm_tier().warm_keys(), 1);
        assert_eq!(
            store.keysizes().get(KeysizeType::Strings).total(),
            strings_before - 1,
            "the spilled key must leave the per-type histogram"
        );

        // Unspill via GET: the value returns to RAM and its histogram bins with it.
        let v = store.get(b"s").unwrap();
        assert_eq!(
            v.as_string().unwrap().as_bytes().as_ref(),
            b"a reasonably sized string value"
        );
        store.assert_consistent();
        assert_eq!(store.warm_tier().warm_keys(), 0);
        assert_eq!(
            store.keysizes().get(KeysizeType::Strings).total(),
            strings_before
        );
    }

    #[test]
    fn delete_of_spilled_key_leaves_no_histogram_leak() {
        // With the spill decrementing the histograms, deleting the warm key must
        // NOT double-decrement, and no phantom count is left behind. Old code
        // (spill skips histograms, delete-of-warm also skips them) leaks the bin.
        let (mut store, _tmp) = store_with_warm();
        store.set(
            Bytes::from("s"),
            Value::string(Bytes::from("spillable string value")),
        );
        store.spill_key(b"s").unwrap();
        store.assert_consistent();

        assert!(store.delete(b"s"));
        store.assert_consistent();
        assert_eq!(store.memory_used(), 0);
        assert!(store.keysizes().get(KeysizeType::Strings).is_empty());
        assert!(store.keysizes().key_memory.is_empty());
        assert_eq!(store.warm_tier().warm_keys(), 0);
    }

    #[test]
    fn get_and_delete_warm_reconciles() {
        let (mut store, _tmp) = store_with_warm();
        store.set(
            Bytes::from("s"),
            Value::string(Bytes::from("warm payload here")),
        );
        store.spill_key(b"s").unwrap();
        store.assert_consistent();

        let got = store.get_and_delete(b"s").unwrap();
        assert_eq!(
            got.as_string().unwrap().as_bytes().as_ref(),
            b"warm payload here"
        );
        store.assert_consistent();
        assert_eq!(store.warm_tier().warm_keys(), 0);
        assert!(!store.contains(b"s"));
        assert_eq!(store.memory_used(), 0);
    }

    #[test]
    fn unspill_expired_warm_key_reconciles() {
        let (mut store, _tmp) = store_with_warm();
        store.set(Bytes::from("s"), Value::string(Bytes::from("doomed value")));
        store.set_expiry(b"s", Instant::now() + Duration::from_secs(3600));
        store.spill_key(b"s").unwrap();
        store.assert_consistent();

        // Move the deadline into the past, then trigger an unspill via GET: the
        // expired branch retires the entry through `uninstall` and only bumps the
        // expired-on-unspill counter.
        store.set_expiry(b"s", Instant::now() - Duration::from_secs(1));
        assert!(store.get(b"s").is_none());
        store.assert_consistent();
        assert_eq!(store.warm_tier().warm_keys(), 0);
        assert_eq!(store.warm_tier().expired_on_unspill(), 1);
        assert!(!store.contains(b"s"));
        assert_eq!(store.memory_used(), 0);
    }
}
