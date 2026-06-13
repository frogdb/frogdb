//! Seam-level tests for the recovery orchestrator.
//!
//! These exercise [`recover`] against a synthesized data-dir state with no
//! server boot: no listeners, no shard workers, no spawned tasks. They cover the
//! ordering invariant and the phases that previously had no test seam at all
//! (staged-checkpoint install in particular, which had zero tests).

use std::path::Path;

use frogdb_core::persistence::{RocksConfig, RocksStore};
use frogdb_core::{KeyMetadata, Store, Value, serialize};
use tempfile::TempDir;

use super::{RecoveryInputs, RecoveryPhase, recover};
use crate::config::PersistenceConfig;

/// Build a `PersistenceConfig` with serde defaults, overriding the two fields the
/// recovery seam cares about.
fn persistence_config(data_dir: &Path, enabled: bool) -> PersistenceConfig {
    let mut cfg: PersistenceConfig =
        serde_json::from_str("{}").expect("default persistence config from empty json");
    cfg.enabled = enabled;
    cfg.data_dir = data_dir.to_path_buf();
    cfg
}

/// Write a single string key into a freshly created RocksDB at `db_dir`, then
/// close it so the directory is a complete, reopenable database.
fn seed_db(db_dir: &Path, num_shards: usize, key: &[u8], val: &str) {
    let rocks = RocksStore::open(db_dir, num_shards, &RocksConfig::default()).unwrap();
    let value = Value::string(val.to_string());
    let metadata = KeyMetadata::new(val.len());
    rocks.put(0, key, &serialize(&value, &metadata)).unwrap();
    rocks.flush().unwrap();
    drop(rocks);
}

#[test]
fn fresh_boot_creates_empty_shards() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    let cfg = persistence_config(&db_dir, true);
    let inputs = RecoveryInputs {
        data_dir: &cfg.data_dir,
        persistence: &cfg,
        num_shards: 4,
        warm_enabled: false,
    };

    let recovered = recover(&inputs).expect("fresh boot recovers");

    assert!(recovered.rocks.is_some(), "rocks store opened");
    assert_eq!(recovered.shards.len(), 4, "one store per shard");
    assert!(!recovered.installed_staged_checkpoint);
    assert_eq!(recovered.stats.keys_loaded, 0);
    for (store, expiry) in &recovered.shards {
        assert_eq!(store.len(), 0);
        assert!(expiry.is_empty());
    }
}

#[test]
fn persistence_disabled_touches_nothing() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    let cfg = persistence_config(&db_dir, false);
    let inputs = RecoveryInputs {
        data_dir: &cfg.data_dir,
        persistence: &cfg,
        num_shards: 3,
        warm_enabled: false,
    };

    let recovered = recover(&inputs).expect("disabled persistence recovers");

    assert!(recovered.rocks.is_none(), "no store when disabled");
    assert_eq!(recovered.shards.len(), 3);
    assert!(!recovered.installed_staged_checkpoint);
    assert!(
        !db_dir.exists(),
        "disabled persistence must not create the data dir"
    );
}

#[test]
fn restart_with_data_restores_keys() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    seed_db(&db_dir, 2, b"greeting", "hello");

    let cfg = persistence_config(&db_dir, true);
    let inputs = RecoveryInputs {
        data_dir: &cfg.data_dir,
        persistence: &cfg,
        num_shards: 2,
        warm_enabled: false,
    };

    let mut recovered = recover(&inputs).expect("restart recovers");

    assert_eq!(recovered.shards.len(), 2);
    assert_eq!(recovered.stats.keys_loaded, 1);
    // key "greeting" hashes into some shard; find it.
    let found = recovered
        .shards
        .iter_mut()
        .filter_map(|(store, _)| store.get(b"greeting"))
        .next();
    let value = found.expect("recovered key present in some shard");
    assert_eq!(value.as_string().unwrap().as_bytes().as_ref(), b"hello");
}

#[test]
fn shard_count_mismatch_is_a_recovery_error() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    // Write the data dir with 2 shards.
    seed_db(&db_dir, 2, b"k", "v");

    // Recover configured for 4 shards: must fail loudly, not silently drop data.
    let cfg = persistence_config(&db_dir, true);
    let inputs = RecoveryInputs {
        data_dir: &cfg.data_dir,
        persistence: &cfg,
        num_shards: 4,
        warm_enabled: false,
    };

    let err = recover(&inputs)
        .err()
        .expect("shard-count mismatch must error");
    assert_eq!(err.phase, RecoveryPhase::OpenRocks);
}

#[test]
fn staged_checkpoint_is_installed() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    let checkpoint_dir = tmp.path().join("checkpoint_ready");

    // Live db has the old value; staged checkpoint has the new value.
    seed_db(&db_dir, 2, b"shared", "old");
    seed_db(&checkpoint_dir, 2, b"shared", "new");

    let cfg = persistence_config(&db_dir, true);
    let inputs = RecoveryInputs {
        data_dir: &cfg.data_dir,
        persistence: &cfg,
        num_shards: 2,
        warm_enabled: false,
    };

    let mut recovered = recover(&inputs).expect("staged checkpoint installs");

    assert!(
        recovered.installed_staged_checkpoint,
        "checkpoint should be reported as installed"
    );
    assert!(
        !checkpoint_dir.exists(),
        "checkpoint_ready renamed into the data dir"
    );
    // The checkpoint's value won.
    let value = recovered
        .shards
        .iter_mut()
        .filter_map(|(store, _)| store.get(b"shared"))
        .next()
        .expect("recovered key present");
    assert_eq!(value.as_string().unwrap().as_bytes().as_ref(), b"new");
    // The previous live db was backed up next to the data dir.
    let backed_up = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .any(|e| e.file_name().to_string_lossy().starts_with("db_backup_"));
    assert!(backed_up, "old database backed up to db_backup_*");
}

#[test]
fn incomplete_staged_checkpoint_is_refused_without_touching_live_db() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    let checkpoint_dir = tmp.path().join("checkpoint_ready");

    // A complete, valid live database.
    seed_db(&db_dir, 2, b"live", "data");
    // An incomplete staged checkpoint: directory exists but has no CURRENT manifest.
    std::fs::create_dir_all(&checkpoint_dir).unwrap();
    std::fs::write(checkpoint_dir.join("stray.sst"), b"garbage").unwrap();

    let cfg = persistence_config(&db_dir, true);
    let inputs = RecoveryInputs {
        data_dir: &cfg.data_dir,
        persistence: &cfg,
        num_shards: 2,
        warm_enabled: false,
    };

    let err = recover(&inputs)
        .err()
        .expect("incomplete checkpoint must be refused");
    assert_eq!(err.phase, RecoveryPhase::InstallStagedCheckpoint);
    // The live database must be untouched: not moved aside to a backup.
    assert!(db_dir.join("CURRENT").exists(), "live db left in place");
    let backed_up = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .any(|e| e.file_name().to_string_lossy().starts_with("db_backup_"));
    assert!(!backed_up, "live db must not be backed up on refusal");
}
