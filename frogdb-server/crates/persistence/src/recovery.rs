//! Recovery protocol for the on-disk format.
//!
//! This module owns the *sequencing* of turning persisted RocksDB state back
//! into live entries: column-family iteration, per-value deserialization,
//! expiry filtering, and warm-tier precedence (a hot copy always wins over a
//! warm copy of the same key). It does **not** know how an entry lands in the
//! in-memory store — that is the caller's concern, expressed through the
//! [`RestoreSink`] trait. The store crate implements `RestoreSink`; this crate
//! drives it, so the read half of the format lives next to the write half and
//! can be round-trip tested end-to-end against a mock sink.

use std::time::Instant;

use bytes::Bytes;
use frogdb_types::types::{KeyMetadata, KeyType, Value};

use crate::rocks::{RocksError, RocksStore};
use crate::serialization::{SerializationError, deserialize};

/// Statistics accumulated while recovering persisted state.
#[derive(Debug, Clone, Default)]
pub struct RecoveryStats {
    /// Number of keys successfully loaded.
    pub keys_loaded: u64,

    /// Number of expired keys skipped.
    pub keys_expired_skipped: u64,

    /// Total bytes loaded (serialized size).
    pub bytes_loaded: u64,

    /// Duration of recovery in milliseconds.
    pub duration_ms: u64,

    /// Number of keys that failed to deserialize.
    pub keys_failed: u64,

    /// Number of warm keys recovered from tiered storage.
    pub warm_keys_loaded: u64,

    /// Number of stale warm keys skipped (hot copy exists).
    pub warm_keys_stale: u64,
}

/// Error during recovery of the on-disk format.
#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error("RocksDB error: {0}")]
    RocksDb(#[from] RocksError),

    #[error("Serialization error: {0}")]
    Serialization(#[from] SerializationError),
}

/// Where recovered entries land.
///
/// The recovery protocol in this module drives a `RestoreSink` as it walks the
/// persisted format: it owns *what order things happen in and how the bytes are
/// decoded*, while the sink owns *how a decoded entry is applied to the store*.
/// Splitting the seam this way lets the persistence crate round-trip test its
/// own format against an in-memory mock sink without depending on the full
/// core store.
pub trait RestoreSink {
    /// Restore a hot entry with its full recovered metadata.
    ///
    /// Called once per non-expired hot-tier key, in iteration order.
    fn restore_entry(&mut self, key: Bytes, value: Value, metadata: KeyMetadata);

    /// Restore a warm entry (key + metadata resident, value stays on disk).
    ///
    /// Only called for warm-tier keys with no hot copy (see [`RestoreSink::contains`]).
    fn restore_warm_entry(&mut self, key: Bytes, metadata: KeyMetadata, key_type: KeyType);

    /// Whether the sink already holds `key` (from a prior hot restore).
    ///
    /// Drives warm-tier precedence: a hot copy always wins over a warm copy of
    /// the same key.
    fn contains(&self, key: &[u8]) -> bool;
}

/// Recover a shard's hot-tier entries into `sink`.
///
/// Walks the shard's primary column family, deserializes each value, filters
/// out already-expired keys, and hands the survivors to the sink. Returns the
/// per-shard statistics; warm-tier recovery (if enabled) is a separate step,
/// see [`recover_warm_shard_into`].
pub fn recover_shard_into<S: RestoreSink>(
    rocks: &RocksStore,
    shard_id: usize,
    sink: &mut S,
) -> Result<RecoveryStats, RecoveryError> {
    let start = Instant::now();
    let now = Instant::now();
    let mut stats = RecoveryStats::default();

    for (key, value) in rocks.iter_cf(shard_id)? {
        stats.bytes_loaded += value.len() as u64;

        match deserialize(&value) {
            Ok((val, metadata)) => {
                // Skip keys whose expiry has already passed.
                if let Some(expires_at) = metadata.expires_at
                    && expires_at <= now
                {
                    stats.keys_expired_skipped += 1;
                    continue;
                }

                let key_bytes = Bytes::copy_from_slice(&key);
                sink.restore_entry(key_bytes, val, metadata);
                stats.keys_loaded += 1;
            }
            Err(e) => {
                tracing::warn!(
                    key = ?String::from_utf8_lossy(&key),
                    error = %e,
                    "Failed to deserialize key during recovery"
                );
                stats.keys_failed += 1;
            }
        }
    }

    stats.duration_ms = start.elapsed().as_millis() as u64;

    tracing::info!(
        shard_id = shard_id,
        keys_loaded = stats.keys_loaded,
        keys_expired = stats.keys_expired_skipped,
        keys_failed = stats.keys_failed,
        bytes = stats.bytes_loaded,
        duration_ms = stats.duration_ms,
        "Shard recovery complete"
    );

    Ok(stats)
}

/// Recover a shard's warm-tier entries into `sink`.
///
/// Scans the warm column family and inserts each surviving entry as a warm
/// entry. Expired warm entries and stale warm entries (those whose key already
/// has a hot copy in the sink) are skipped and pruned from the warm CF. Updates
/// `stats` in place with warm-tier counters.
pub fn recover_warm_shard_into<S: RestoreSink>(
    rocks: &RocksStore,
    shard_id: usize,
    sink: &mut S,
    stats: &mut RecoveryStats,
) -> Result<(), RecoveryError> {
    let now = Instant::now();

    for (key, value) in rocks.iter_warm_cf(shard_id)? {
        match deserialize(&value) {
            Ok((val, metadata)) => {
                // Skip expired keys and clean up the stale warm entry.
                if let Some(expires_at) = metadata.expires_at
                    && expires_at <= now
                {
                    stats.keys_expired_skipped += 1;
                    let _ = rocks.delete_warm(shard_id, &key);
                    continue;
                }

                let key_bytes = Bytes::copy_from_slice(&key);

                // Hot copy wins — drop and prune the redundant warm entry.
                if sink.contains(&key_bytes) {
                    stats.warm_keys_stale += 1;
                    let _ = rocks.delete_warm(shard_id, &key);
                    continue;
                }

                let key_type = val.key_type();
                sink.restore_warm_entry(key_bytes, metadata, key_type);
                stats.warm_keys_loaded += 1;
            }
            Err(e) => {
                tracing::warn!(
                    shard_id,
                    key = ?String::from_utf8_lossy(&key),
                    error = %e,
                    "Failed to deserialize warm key during recovery"
                );
                stats.keys_failed += 1;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use frogdb_types::hyperloglog::HyperLogLogValue;
    use frogdb_types::types::{SortedSetValue, Value};
    use tempfile::TempDir;

    use super::*;
    use crate::rocks::RocksConfig;
    use crate::serialization::{serialize, serialize_hll_delta};

    /// In-memory restore sink used to round-trip the on-disk format without
    /// depending on the core store. Mirrors the store's hot-wins-over-warm rule
    /// so recovery precedence can be asserted directly.
    #[derive(Default)]
    struct MockSink {
        hot: HashMap<Vec<u8>, (Value, KeyMetadata)>,
        warm: HashMap<Vec<u8>, (KeyMetadata, KeyType)>,
    }

    impl RestoreSink for MockSink {
        fn restore_entry(&mut self, key: Bytes, value: Value, metadata: KeyMetadata) {
            self.hot.insert(key.to_vec(), (value, metadata));
        }

        fn restore_warm_entry(&mut self, key: Bytes, metadata: KeyMetadata, key_type: KeyType) {
            // Hot copy wins — mirror the real store's guard.
            if self.hot.contains_key(key.as_ref()) {
                return;
            }
            self.warm.insert(key.to_vec(), (metadata, key_type));
        }

        fn contains(&self, key: &[u8]) -> bool {
            self.hot.contains_key(key)
        }
    }

    /// Serialize a set of hot and warm entries, then recover them back through a
    /// mock sink and assert exact reconstruction — including expired-key
    /// filtering and hot-wins-over-warm precedence.
    #[test]
    fn round_trips_format_through_mock_sink() {
        let tmp = TempDir::new().unwrap();
        let rocks =
            RocksStore::open_with_warm(tmp.path(), 1, &RocksConfig::default(), true).unwrap();

        // --- Hot tier ---
        // A plain string.
        let hello = Value::string("hello");
        rocks
            .put(0, b"hot_str", &serialize(&hello, &KeyMetadata::new(5)))
            .unwrap();

        // A sorted set (exercises a richer value shape).
        let mut zset = SortedSetValue::new();
        zset.add(Bytes::from("m1"), 1.0);
        zset.add(Bytes::from("m2"), 2.0);
        let zset_val = Value::SortedSet(zset);
        rocks
            .put(0, b"hot_zset", &serialize(&zset_val, &KeyMetadata::new(2)))
            .unwrap();

        // A key with a future expiry — survives and stays.
        let mut future_meta = KeyMetadata::new(4);
        future_meta.expires_at = Some(Instant::now() + Duration::from_secs(3600));
        rocks
            .put(
                0,
                b"hot_future",
                &serialize(&Value::string("live"), &future_meta),
            )
            .unwrap();

        // A key whose expiry already passed — filtered during recovery.
        let mut past_meta = KeyMetadata::new(7);
        past_meta.expires_at = Some(Instant::now() - Duration::from_secs(1));
        rocks
            .put(
                0,
                b"hot_expired",
                &serialize(&Value::string("dead"), &past_meta),
            )
            .unwrap();

        // A key that ALSO has a warm copy — the hot copy must win.
        rocks
            .put(
                0,
                b"dup",
                &serialize(&Value::string("hot_wins"), &KeyMetadata::new(1)),
            )
            .unwrap();

        // A plain HyperLogLog written as a full value — round-trips like any
        // other value shape (recovered cardinality equals the reference).
        let mut hll_full = HyperLogLogValue::new();
        for i in 0..50u32 {
            hll_full.add(&i.to_le_bytes());
        }
        let hll_full_count = hll_full.count_no_cache();
        rocks
            .put(
                0,
                b"hot_hll_full",
                &serialize(&Value::HyperLogLog(hll_full), &KeyMetadata::new(6)),
            )
            .unwrap();

        // A HyperLogLog persisted as a base `put` plus two `merge` delta
        // operands (the Tier 2 dense-PFADD path). Recovery must fold the
        // operands onto the base so the recovered count matches an in-memory
        // reference that saw the exact same adds.
        let hll_meta = KeyMetadata::new(8);
        let mut hll_ref = HyperLogLogValue::new();
        for i in 0..20u32 {
            hll_ref.add(&i.to_le_bytes());
        }
        rocks
            .put(
                0,
                b"hot_hll_delta",
                &serialize(&Value::HyperLogLog(hll_ref.clone()), &hll_meta),
            )
            .unwrap();
        // First delta batch.
        let mut pairs1 = Vec::new();
        for i in 20..60u32 {
            if let Some(p) = hll_ref.add_tracked(&i.to_le_bytes()) {
                pairs1.push(p);
            }
        }
        rocks
            .merge(
                0,
                b"hot_hll_delta",
                &serialize_hll_delta(&pairs1, &hll_meta),
            )
            .unwrap();
        // Second delta batch.
        let mut pairs2 = Vec::new();
        for i in 60..120u32 {
            if let Some(p) = hll_ref.add_tracked(&i.to_le_bytes()) {
                pairs2.push(p);
            }
        }
        rocks
            .merge(
                0,
                b"hot_hll_delta",
                &serialize_hll_delta(&pairs2, &hll_meta),
            )
            .unwrap();
        let hll_delta_count = hll_ref.count_no_cache();

        // --- Warm tier ---
        // Unique warm key — recovered as warm.
        rocks
            .put_warm(
                0,
                b"warm_only",
                &serialize(&Value::string("w"), &KeyMetadata::new(9)),
            )
            .unwrap();

        // Stale warm copy of `dup` — must be skipped (hot wins) and pruned.
        rocks
            .put_warm(
                0,
                b"dup",
                &serialize(&Value::string("warm_loses"), &KeyMetadata::new(1)),
            )
            .unwrap();

        // Expired warm key — filtered and pruned.
        let mut warm_past = KeyMetadata::new(3);
        warm_past.expires_at = Some(Instant::now() - Duration::from_secs(1));
        rocks
            .put_warm(
                0,
                b"warm_expired",
                &serialize(&Value::string("gone"), &warm_past),
            )
            .unwrap();

        // --- Recover through the mock sink ---
        let mut sink = MockSink::default();
        let mut stats = recover_shard_into(&rocks, 0, &mut sink).unwrap();
        recover_warm_shard_into(&rocks, 0, &mut sink, &mut stats).unwrap();

        // --- Assert exact reconstruction ---
        // Hot: 6 live keys loaded, 1 expired skipped.
        assert_eq!(
            stats.keys_loaded, 6,
            "hot_str, hot_zset, hot_future, dup, hot_hll_full, hot_hll_delta"
        );
        assert_eq!(stats.keys_expired_skipped, 2, "hot_expired + warm_expired");
        assert_eq!(sink.hot.len(), 6);

        // Values reconstructed byte-for-byte.
        let (v, _) = sink.hot.get(b"hot_str".as_slice()).unwrap();
        assert_eq!(v.as_string().unwrap().as_bytes().as_ref(), b"hello");

        let (v, _) = sink.hot.get(b"hot_zset".as_slice()).unwrap();
        let z = v.as_sorted_set().unwrap();
        assert_eq!(z.len(), 2);
        assert_eq!(z.get_score(b"m1"), Some(1.0));
        assert_eq!(z.get_score(b"m2"), Some(2.0));

        assert!(sink.hot.contains_key(b"hot_future".as_slice()));
        assert!(!sink.hot.contains_key(b"hot_expired".as_slice()));

        // Plain HyperLogLog full value round-trips to the same cardinality.
        let (v, _) = sink.hot.get(b"hot_hll_full".as_slice()).unwrap();
        let Value::HyperLogLog(h) = v else {
            panic!("hot_hll_full recovered as the wrong type")
        };
        assert_eq!(h.count_no_cache(), hll_full_count);

        // Base put + two merge delta operands fold back to the reference count.
        let (v, _) = sink.hot.get(b"hot_hll_delta".as_slice()).unwrap();
        let Value::HyperLogLog(h) = v else {
            panic!("hot_hll_delta recovered as the wrong type")
        };
        assert_eq!(
            h.count_no_cache(),
            hll_delta_count,
            "recovered HLL must fold both merge operands onto the base"
        );

        // Hot-wins-over-warm precedence.
        let (v, _) = sink.hot.get(b"dup".as_slice()).unwrap();
        assert_eq!(v.as_string().unwrap().as_bytes().as_ref(), b"hot_wins");

        // Warm: only the unique, non-expired key is resident as warm.
        assert_eq!(stats.warm_keys_loaded, 1, "warm_only");
        assert_eq!(stats.warm_keys_stale, 1, "dup warm copy skipped");
        assert_eq!(sink.warm.len(), 1);
        assert!(sink.warm.contains_key(b"warm_only".as_slice()));
        assert!(!sink.warm.contains_key(b"dup".as_slice()));
        assert!(!sink.warm.contains_key(b"warm_expired".as_slice()));

        // Stale + expired warm entries pruned from the warm CF.
        assert!(rocks.get_warm(0, b"dup").unwrap().is_none());
        assert!(rocks.get_warm(0, b"warm_expired").unwrap().is_none());
    }
}
