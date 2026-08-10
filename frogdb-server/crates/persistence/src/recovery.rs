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

use frogdb_types::clock;

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

    /// Context for the **first** key that failed to deserialize, or `None` when
    /// none did.
    ///
    /// `keys_failed` is a count, and a count is not something an operator can
    /// act on. This carries the one example needed to start diagnosing: which
    /// shard, which tier, which key, and what the decoder said. First rather
    /// than last because the first failure in iteration order is the one a
    /// truncated or format-shifted column family produces at its boundary —
    /// and because "first" is stable across reruns where "last" is not.
    pub first_failure: Option<DecodeFailure>,
}

/// Where a single decode failure happened, and what the decoder said about it.
///
/// Recorded first-wins ([`RecoveryStats::record_first_failure`]) so this is one
/// concrete example, never a summary: the count in
/// [`RecoveryStats::keys_failed`] is what says how widespread the problem is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeFailure {
    /// Shard whose column family the key was read from.
    pub shard_id: usize,
    /// Tier the key was read from: `true` for the warm CF, `false` for the hot
    /// CF. Load-bearing for diagnosis — a warm-only failure points at tiered
    /// storage rather than at the primary dataset.
    pub warm: bool,
    /// The raw key bytes. Kept raw rather than pre-rendered: keys are arbitrary
    /// binary, and the rendering (lossy, truncated) belongs to whoever formats
    /// the message, not to the capture site.
    pub key: Bytes,
    /// The deserializer's own error text.
    pub error: String,
}

impl RecoveryStats {
    /// Record `failure` as the first decode failure, if none has been recorded.
    ///
    /// Idempotent after the first call — every later failure is counted in
    /// `keys_failed` and dropped here. Deliberately not `Option::get_or_insert`
    /// at the call sites: those would build the `DecodeFailure` (and its
    /// `String`) for every failing key just to throw it away.
    ///
    /// Public because the same first-wins rule has to hold when *per-shard*
    /// stats are folded into a whole-database total (`recover_all_shards` in
    /// the store crate); one shared method is what keeps the two levels from
    /// disagreeing about which failure is "first".
    pub fn record_first_failure(&mut self, failure: impl FnOnce() -> DecodeFailure) {
        if self.first_failure.is_none() {
            self.first_failure = Some(failure());
        }
    }
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
    let start = clock::now();
    let now = clock::now();
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
                stats.record_first_failure(|| DecodeFailure {
                    shard_id,
                    warm: false,
                    key: Bytes::copy_from_slice(&key),
                    error: e.to_string(),
                });
            }
        }
    }

    stats.duration_ms = clock::elapsed(start).as_millis() as u64;

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
    let now = clock::now();

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
                stats.record_first_failure(|| DecodeFailure {
                    shard_id,
                    warm: true,
                    key: Bytes::copy_from_slice(&key),
                    error: e.to_string(),
                });
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use frogdb_types::clock;
    use frogdb_types::hyperloglog::HyperLogLogValue;
    use frogdb_types::types::{
        SortedSetValue, StreamId, StreamIdSpec, StreamRangeBound, StreamValue, Value,
    };
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
        future_meta.expires_at = Some(clock::now() + Duration::from_secs(3600));
        rocks
            .put(
                0,
                b"hot_future",
                &serialize(&Value::string("live"), &future_meta),
            )
            .unwrap();

        // A key whose expiry already passed — filtered during recovery.
        let mut past_meta = KeyMetadata::new(7);
        past_meta.expires_at = Some(clock::now() - Duration::from_secs(1));
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

        // A stream carrying consumer-group state (proposal 45): a group with a
        // consumer and one pending entry must survive the full disk round-trip.
        let mut stream = StreamValue::new();
        let _ = stream.add(
            StreamIdSpec::Explicit(StreamId::new(1, 1)),
            vec![(Bytes::from("f"), Bytes::from("v"))],
        );
        stream
            .create_group(Bytes::from("grp"), StreamId::new(1, 1), Some(1))
            .unwrap();
        {
            let g = stream.get_group_mut(b"grp").unwrap();
            g.create_consumer(Bytes::from("worker"));
            g.add_pending(StreamId::new(1, 1), Bytes::from("worker"));
        }
        rocks
            .put(
                0,
                b"hot_stream",
                &serialize(&Value::Stream(stream), &KeyMetadata::new(10)),
            )
            .unwrap();

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
        warm_past.expires_at = Some(clock::now() - Duration::from_secs(1));
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
        // Hot: 7 live keys loaded, 1 expired skipped.
        assert_eq!(
            stats.keys_loaded, 7,
            "hot_str, hot_zset, hot_future, dup, hot_hll_full, hot_hll_delta, hot_stream"
        );
        assert_eq!(stats.keys_expired_skipped, 2, "hot_expired + warm_expired");
        assert_eq!(sink.hot.len(), 7);

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

        // Stream consumer-group state survives recovery: the group, its
        // last-delivered-id / entries-read, the consumer, and the PEL entry.
        let (v, _) = sink.hot.get(b"hot_stream".as_slice()).unwrap();
        let Value::Stream(s) = v else {
            panic!("hot_stream recovered as the wrong type")
        };
        assert_eq!(s.len(), 1);
        let g = s
            .get_group(b"grp")
            .expect("consumer group must survive recovery");
        assert_eq!(g.last_delivered_id(), StreamId::new(1, 1));
        assert_eq!(g.entries_read(), Some(1));
        assert_eq!(g.consumer_count(), 1);
        let pel = g.pending_entries(
            StreamRangeBound::Min,
            StreamRangeBound::Max,
            usize::MAX,
            None,
            None,
        );
        assert_eq!(pel.len(), 1, "PEL entry must survive recovery");
        assert_eq!(pel[0].id, StreamId::new(1, 1));
        assert_eq!(pel[0].consumer, Bytes::from("worker"));
        assert_eq!(pel[0].delivery_count, 1);

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

    // FM-PERSISTENCE-033
    /// An undecodable value is counted, not fatal — and counted on *both*
    /// tiers, since a tiered deployment can have bit-rot in either column
    /// family. `bytes_loaded` is asserted exactly because it is the only
    /// evidence the corrupt bytes were read at all: a counter that never
    /// accumulates would make a scan of unreadable data look like a scan of an
    /// empty database.
    #[test]
    fn undecodable_values_are_counted_on_both_tiers() {
        let tmp = TempDir::new().unwrap();
        let rocks =
            RocksStore::open_with_warm(tmp.path(), 1, &RocksConfig::default(), true).unwrap();

        let good = serialize(&Value::string("readable"), &KeyMetadata::new(8));
        rocks.put(0, b"good", &good).unwrap();
        // Not a serialized value: too short to even carry a header.
        rocks.put(0, b"hot_rot", b"garbage").unwrap();
        rocks.put_warm(0, b"warm_rot", b"also garbage").unwrap();

        let mut sink = MockSink::default();
        let mut stats = recover_shard_into(&rocks, 0, &mut sink).unwrap();
        assert_eq!(stats.keys_loaded, 1, "the one good key survives");
        assert_eq!(stats.keys_failed, 1, "the hot-tier corruption is counted");
        assert_eq!(
            stats.bytes_loaded,
            (good.len() + b"garbage".len()) as u64,
            "every byte the hot scan read is accounted for, corrupt ones included"
        );

        recover_warm_shard_into(&rocks, 0, &mut sink, &mut stats).unwrap();
        assert_eq!(
            stats.keys_failed, 2,
            "the warm-tier corruption adds to the same counter"
        );
        assert_eq!(stats.warm_keys_loaded, 0);
        assert_eq!(sink.hot.len(), 1, "recovery continued past both bad values");
    }

    // FM-PERSISTENCE-047
    /// The refusal message needs *one* concrete example, so the format layer
    /// captures the first failure's shard, tier, key, and decoder error — and
    /// keeps the first one. Asserted at this layer rather than only through the
    /// recovery orchestrator because this is where the tier is decided: the
    /// warm scan shares `keys_failed` with the hot scan, so a warm failure that
    /// overwrote the hot one would point an operator at tiered storage for a
    /// problem in the primary dataset.
    #[test]
    fn the_first_decode_failure_is_captured_with_its_tier_and_key() {
        let tmp = TempDir::new().unwrap();
        let rocks =
            RocksStore::open_with_warm(tmp.path(), 1, &RocksConfig::default(), true).unwrap();

        // Iteration is key-ordered, so `a_rot` is read before `b_rot`.
        rocks.put(0, b"a_rot", b"garbage").unwrap();
        rocks.put(0, b"b_rot", b"more garbage").unwrap();
        rocks.put_warm(0, b"c_warm_rot", b"warm garbage").unwrap();

        let mut sink = MockSink::default();
        let mut stats = recover_shard_into(&rocks, 0, &mut sink).unwrap();

        let first = stats
            .first_failure
            .clone()
            .expect("a decode failure must carry its context");
        assert_eq!(first.shard_id, 0);
        assert!(!first.warm, "the hot tier is where this one failed");
        assert_eq!(first.key, Bytes::from_static(b"a_rot"), "first, not last");
        assert!(!first.error.is_empty(), "the decoder's own words are kept");

        recover_warm_shard_into(&rocks, 0, &mut sink, &mut stats).unwrap();
        assert_eq!(stats.keys_failed, 3, "all three are still counted");
        assert_eq!(
            stats.first_failure.as_ref(),
            Some(&first),
            "a later failure, on either tier, does not displace the first"
        );
    }

    // FM-PERSISTENCE-047
    /// `record_first_failure` takes a closure so a database with a million bad
    /// keys allocates one `DecodeFailure`, not a million. That is only true if
    /// the closure is skipped once a failure is held — a property no
    /// caller-side assertion can see, so it is asserted here directly.
    #[test]
    fn recording_a_later_failure_does_not_even_build_it() {
        let mut stats = RecoveryStats::default();
        let mut builds = 0;
        let mut record = |key: &'static [u8], builds: &mut u32| {
            stats.record_first_failure(|| {
                *builds += 1;
                DecodeFailure {
                    shard_id: 3,
                    warm: false,
                    key: Bytes::from_static(key),
                    error: "bad header".to_string(),
                }
            });
        };

        record(b"first", &mut builds);
        record(b"second", &mut builds);

        assert_eq!(builds, 1, "the second failure never built a DecodeFailure");
        assert_eq!(
            stats.first_failure.as_ref().map(|f| f.key.clone()),
            Some(Bytes::from_static(b"first"))
        );
    }
}
