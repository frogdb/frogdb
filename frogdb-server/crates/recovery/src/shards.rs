//! Phases 2-3: open RocksDB and restore per-shard stores.
//!
//! Phase 2 opens RocksDB with the configured shard count; the open call itself
//! validates the persisted shard count against the configured one and fails
//! loudly on a mismatch (rather than silently misrouting or dropping data).
//! Phase 3 rebuilds the per-shard hash tables, expiry indexes, and warm-tier
//! entries from the opened store.

use anyhow::{Result, bail};
use frogdb_core::persistence::{RecoveryStats, RocksConfig, RocksStore, recover_all_shards};
use frogdb_core::sync::Arc;
use frogdb_core::{ExpiryIndex, HashMapStore};
use frogdb_types::metrics::definitions::RecoveryKeysFailed;
use tracing::{error, info};

use crate::RecoveryInputs;

/// Phase 2: open RocksDB (with optional warm-tier column families).
pub(crate) fn open_rocks(inputs: &RecoveryInputs<'_>) -> Result<Arc<RocksStore>> {
    let config = inputs.persistence;

    // The operator-vs-invariant knob partition lives in `RocksConfig::from_persistence`
    // (next to `RocksConfig::default()`); this site only supplies the two arguments
    // that come from `RecoveryInputs`, not `PersistenceConfig`: `num_shards` and
    // `warm_enabled`.
    let rocks_config = RocksConfig::from_persistence(config);

    let rocks = Arc::new(RocksStore::open_with_warm_metrics(
        &config.data_dir,
        inputs.num_shards,
        &rocks_config,
        inputs.warm_enabled,
        inputs.metrics_recorder.clone(),
    )?);

    Ok(rocks)
}

/// Phase 3: restore per-shard hash tables, expiry indexes, and warm-tier entries.
///
/// When the database has no existing data this returns `num_shards` fresh empty
/// stores; otherwise it replays every shard's persisted state.
pub(crate) fn restore(
    inputs: &RecoveryInputs<'_>,
    rocks: &Arc<RocksStore>,
) -> Result<(Vec<(HashMapStore, ExpiryIndex)>, RecoveryStats)> {
    if rocks.has_data() {
        info!("Recovering data from RocksDB...");
        let (stores, stats) = recover_all_shards(rocks)?;
        report_decode_failures(inputs, &stats)?;
        info!(
            keys_loaded = stats.keys_loaded,
            keys_expired = stats.keys_expired_skipped,
            bytes = stats.bytes_loaded,
            duration_ms = stats.duration_ms,
            "Recovery complete"
        );
        Ok((stores, stats))
    } else {
        info!("No existing data found, starting fresh");
        Ok((
            (0..inputs.num_shards).map(|_| Default::default()).collect(),
            RecoveryStats::default(),
        ))
    }
}

/// Turn skipped-because-undecodable keys into something an operator can see —
/// and refuse the boot outright when *nothing* decoded.
///
/// Skipping a key that will not deserialize is the right default: one bad value
/// must not cost the whole keyspace. But a skip is invisible from the outside,
/// because a keyspace that came back smaller cannot say whether it shrank. So
/// every boot that skipped anything raises one `ERROR` and increments
/// `frogdb_recovery_keys_failed_total` by the count (the per-key `WARN`s live in
/// the format layer; this is the aggregate, emitted once per boot rather than
/// once per shard).
///
/// The refusal is deliberately the *unambiguous* case only: data was found, and
/// not one value in it decoded — a format change, a truncated or bit-rotted
/// column family, or a directory that was never a FrogDB database. Booting that
/// as an empty keyspace is the dangerous outcome, because the server then
/// accepts writes and the WAL/snapshot cadence starts overwriting what is still
/// there. One decoded value (even one that was then dropped as expired, even a
/// warm-tier one) means the database is readable and takes the skip-and-count
/// path instead. A *partial*-corruption threshold ("refuse above N%") is policy,
/// needs a knob and an override, and is not decided here.
fn report_decode_failures(inputs: &RecoveryInputs<'_>, stats: &RecoveryStats) -> Result<()> {
    if stats.keys_failed == 0 {
        return Ok(());
    }

    RecoveryKeysFailed::inc_by(inputs.metrics_recorder.as_ref(), stats.keys_failed);

    let decoded = stats.keys_loaded
        + stats.keys_expired_skipped
        + stats.warm_keys_loaded
        + stats.warm_keys_stale;
    if decoded == 0 {
        bail!(
            "every value in {} failed to deserialize ({} key(s), none decoded): refusing to start \
             rather than come up as an empty database and overwrite it. Restore from a snapshot, \
             or point data-dir at the right directory.",
            inputs.persistence.data_dir.display(),
            stats.keys_failed
        );
    }

    error!(
        data_dir = %inputs.persistence.data_dir.display(),
        keys_failed = stats.keys_failed,
        keys_loaded = stats.keys_loaded,
        keys_expired = stats.keys_expired_skipped,
        "Recovery skipped keys whose stored values could not be deserialized; those keys are gone \
         from this boot. Check frogdb_recovery_keys_failed_total and INFO persistence \
         (rdb_last_load_keys_failed)."
    );
    Ok(())
}
