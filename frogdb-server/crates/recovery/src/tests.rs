//! Seam-level tests for the recovery orchestrator.
//!
//! These exercise [`recover`] against a synthesized data-dir state with no
//! server boot: no listeners, no shard workers, no spawned tasks. They cover the
//! ordering invariant and the phases that previously had no test seam at all
//! (staged-checkpoint install in particular, which had zero tests).

use std::path::Path;

use frogdb_config::{
    ClusterConfigSection, PersistenceConfig, RecoveryConfig, ReplicationConfigSection,
};
use frogdb_core::persistence::data_dir::{
    DATA_DIR_LAYOUT_VERSION, DataDirMarker, MARKER_FILE_NAME,
};
use frogdb_core::persistence::{RocksConfig, RocksStore};
use frogdb_core::sync::Arc;
use frogdb_core::{KeyMetadata, NoopMetricsRecorder, Store, Value, serialize};
use tempfile::TempDir;

use crate::{RecoveryInputs, RecoveryPhase, recover};

/// Build a `PersistenceConfig` with serde defaults, overriding the two fields the
/// recovery seam cares about.
fn persistence_config(data_dir: &Path, enabled: bool) -> PersistenceConfig {
    let mut cfg: PersistenceConfig =
        serde_json::from_str("{}").expect("default persistence config from empty json");
    cfg.enabled = enabled;
    cfg.data_dir = data_dir.to_path_buf();
    cfg
}

/// Build a `ReplicationConfigSection` with serde defaults and the given role.
fn replication_config(role: &str) -> ReplicationConfigSection {
    let mut cfg: ReplicationConfigSection =
        serde_json::from_str("{}").expect("default replication config from empty json");
    cfg.role = role.to_string();
    cfg
}

/// The default decode-failure policy (`continue`), borrowed as `'static` so
/// every `RecoveryInputs` in this file can name it without a per-test local.
///
/// Every test here except the policy tests below runs under it, which is the
/// point: `continue` is what a boot does when nobody configured anything.
fn continue_policy() -> &'static RecoveryConfig {
    static POLICY: std::sync::LazyLock<RecoveryConfig> =
        std::sync::LazyLock::new(RecoveryConfig::default);
    &POLICY
}

/// `recovery.on-decode-failure = refuse`, the opt-in strict policy.
fn refuse_policy() -> &'static RecoveryConfig {
    static POLICY: std::sync::LazyLock<RecoveryConfig> =
        std::sync::LazyLock::new(|| RecoveryConfig {
            on_decode_failure: "refuse".to_string(),
        });
    &POLICY
}

/// Build a `ClusterConfigSection` with serde defaults and the given enabled flag.
fn cluster_config(enabled: bool) -> ClusterConfigSection {
    let mut cfg: ClusterConfigSection =
        serde_json::from_str("{}").expect("default cluster config from empty json");
    cfg.enabled = enabled;
    cfg
}

/// Stamp the data-directory marker a previous FrogDB boot would have left
/// behind.
///
/// Every fixture below that hands recovery a directory which already holds
/// files is standing in for a *restart*, and a restarting node's data directory
/// carries a marker. Without one, phase 0 refuses the boot — correctly, and for
/// a reason that has nothing to do with what the test is about.
fn mark(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    if DataDirMarker::read(dir).expect("marker readable").is_none() {
        DataDirMarker::mint().stamp(dir).unwrap();
    }
}

/// The data directory a database directory belongs to — its parent, under the
/// layout of FM-PERSISTENCE-057.
fn data_dir_of(db_dir: &Path) -> &Path {
    db_dir
        .parent()
        .expect("a db directory has a data directory")
}

/// Whether the install left a `db_backup_*` directory under `<data-dir>/backup`.
fn has_backup(data_dir: &Path) -> bool {
    let Ok(entries) = std::fs::read_dir(data_dir.join("backup")) else {
        return false;
    };
    entries
        .filter_map(|e| e.ok())
        .any(|e| e.file_name().to_string_lossy().starts_with("db_backup_"))
}

/// The marker a data directory currently carries, for tests that assert the
/// directory's identity survives (or is created by) a boot.
fn marker_of(dir: &Path) -> DataDirMarker {
    DataDirMarker::read(dir)
        .expect("marker readable")
        .expect("marker present")
}

/// Write a single string key into a freshly created RocksDB at `db_dir`, then
/// close it so the directory is a complete, reopenable database.
///
/// Marks the *data* directory too — the one `db_dir` sits in: a database FrogDB
/// wrote is a database FrogDB stamped, and the marker is a sibling of `db/`
/// rather than a file inside it (FM-PERSISTENCE-057).
fn seed_db(db_dir: &Path, num_shards: usize, key: &[u8], val: &str) {
    seed_staged(db_dir, num_shards, key, val);
    mark(data_dir_of(db_dir));
}

/// The same database, unmarked: a staged full-sync payload is a RocksDB
/// checkpoint of *another* node's database and carries no identity of its own.
fn seed_staged(dir: &Path, num_shards: usize, key: &[u8], val: &str) {
    let rocks = RocksStore::open(dir, num_shards, &RocksConfig::default()).unwrap();
    let value = Value::string(val.to_string());
    let metadata = KeyMetadata::new(val.len());
    rocks.put(0, key, &serialize(&value, &metadata)).unwrap();
    rocks.flush().unwrap();
    drop(rocks);
}

// FM-PERSISTENCE-027
// FM-PERSISTENCE-029
// FM-PERSISTENCE-041
// FM-PERSISTENCE-048
#[test]
fn fresh_boot_creates_empty_shards() {
    let tmp = TempDir::new().unwrap();
    let cfg = persistence_config(tmp.path(), true);
    let repl_cfg = replication_config("standalone");
    let cluster_cfg = cluster_config(false);
    let inputs = RecoveryInputs {
        data_dir: &cfg.data_dir,
        persistence: &cfg,
        replication: &repl_cfg,
        cluster: &cluster_cfg,
        recovery: continue_policy(),
        num_shards: 4,
        warm_enabled: false,
        metrics_recorder: Arc::new(NoopMetricsRecorder::new()),
    };

    let recovered = recover(&inputs).expect("fresh boot recovers");

    assert!(recovered.rocks.is_some(), "rocks store opened");
    assert_eq!(recovered.shards.len(), 4, "one store per shard");
    assert!(recovered.functions.is_empty(), "no persisted functions");
    assert!(
        recovered.raft_storage.is_none(),
        "no raft storage in non-cluster mode"
    );
    assert!(!recovered.installed_staged_checkpoint);
    assert_eq!(recovered.stats.keys_loaded, 0);
    for (store, expiry) in &recovered.shards {
        assert_eq!(store.len(), 0);
        assert!(expiry.is_empty());
    }
}

// FM-PERSISTENCE-009
// FM-PERSISTENCE-028
#[test]
fn persistence_disabled_touches_nothing() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    let cfg = persistence_config(tmp.path(), false);
    let repl_cfg = replication_config("standalone");
    let cluster_cfg = cluster_config(false);
    let inputs = RecoveryInputs {
        data_dir: &cfg.data_dir,
        persistence: &cfg,
        replication: &repl_cfg,
        cluster: &cluster_cfg,
        recovery: continue_policy(),
        num_shards: 3,
        warm_enabled: false,
        metrics_recorder: Arc::new(NoopMetricsRecorder::new()),
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

// FM-PERSISTENCE-027
// FM-PERSISTENCE-041
#[test]
fn restart_with_data_restores_keys() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    seed_db(&db_dir, 2, b"greeting", "hello");

    let cfg = persistence_config(tmp.path(), true);
    let repl_cfg = replication_config("standalone");
    let cluster_cfg = cluster_config(false);
    let inputs = RecoveryInputs {
        data_dir: &cfg.data_dir,
        persistence: &cfg,
        replication: &repl_cfg,
        cluster: &cluster_cfg,
        recovery: continue_policy(),
        num_shards: 2,
        warm_enabled: false,
        metrics_recorder: Arc::new(NoopMetricsRecorder::new()),
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

// FM-PERSISTENCE-037
#[test]
fn corrupt_functions_file_is_tolerated() {
    let tmp = TempDir::new().unwrap();
    // A corrupt functions.fdb must not block startup.
    mark(tmp.path());
    std::fs::write(
        tmp.path().join("functions.fdb"),
        b"not a valid function dump",
    )
    .unwrap();

    let cfg = persistence_config(tmp.path(), true);
    let repl_cfg = replication_config("standalone");
    let cluster_cfg = cluster_config(false);
    let inputs = RecoveryInputs {
        data_dir: &cfg.data_dir,
        persistence: &cfg,
        replication: &repl_cfg,
        cluster: &cluster_cfg,
        recovery: continue_policy(),
        num_shards: 1,
        warm_enabled: false,
        metrics_recorder: Arc::new(NoopMetricsRecorder::new()),
    };

    let recovered = recover(&inputs).expect("corrupt functions.fdb is not fatal");
    assert!(
        recovered.functions.is_empty(),
        "corrupt function dump yields no functions"
    );
}

// FM-PERSISTENCE-037
/// The tolerance above is counted, not silent: a `functions.fdb` that will not
/// read is one lost library in `RecoveryStats` and one increment of
/// `frogdb_recovery_functions_failed_total`, so a `FUNCTION LIST` that came back
/// smaller than what was saved has something to be noticed by.
#[test]
fn a_corrupt_functions_file_is_counted_and_exported() {
    let tmp = TempDir::new().unwrap();
    mark(tmp.path());
    std::fs::write(
        tmp.path().join("functions.fdb"),
        b"not a valid function dump",
    )
    .unwrap();

    let cfg = persistence_config(tmp.path(), true);
    let repl_cfg = replication_config("standalone");
    let cluster_cfg = cluster_config(false);
    let metrics = Arc::new(RecordingMetricsRecorder::default());
    let inputs = inputs_for(
        &cfg,
        &repl_cfg,
        &cluster_cfg,
        continue_policy(),
        1,
        metrics.clone(),
    );

    let recovered = recover(&inputs).expect("corrupt functions.fdb is not fatal");
    assert_eq!(
        recovered.stats.functions_failed, 1,
        "the downgraded file must be counted once"
    );
    assert_eq!(
        metrics.total("frogdb_recovery_functions_failed_total"),
        1,
        "the count must reach the process metric, not only the boot stats"
    );
}

// FM-PERSISTENCE-037
/// The counter is a *failure* signal: a boot with no `functions.fdb` at all is
/// the ordinary fresh-boot case and must not look like a lost library.
#[test]
fn a_boot_with_no_functions_file_counts_no_failures() {
    let tmp = TempDir::new().unwrap();
    mark(tmp.path());

    let cfg = persistence_config(tmp.path(), true);
    let repl_cfg = replication_config("standalone");
    let cluster_cfg = cluster_config(false);
    let metrics = Arc::new(RecordingMetricsRecorder::default());
    let inputs = inputs_for(
        &cfg,
        &repl_cfg,
        &cluster_cfg,
        continue_policy(),
        1,
        metrics.clone(),
    );

    let recovered = recover(&inputs).expect("a fresh data dir recovers");
    assert!(recovered.functions.is_empty());
    assert_eq!(recovered.stats.functions_failed, 0);
    assert_eq!(metrics.total("frogdb_recovery_functions_failed_total"), 0);
}

// FM-PERSISTENCE-028
// FM-PERSISTENCE-038
#[test]
fn standalone_does_not_persist_replication_state() {
    let tmp = TempDir::new().unwrap();
    let cfg = persistence_config(tmp.path(), true);
    let repl_cfg = replication_config("standalone");
    let cluster_cfg = cluster_config(false);
    let inputs = RecoveryInputs {
        data_dir: &cfg.data_dir,
        persistence: &cfg,
        replication: &repl_cfg,
        cluster: &cluster_cfg,
        recovery: continue_policy(),
        num_shards: 1,
        warm_enabled: false,
        metrics_recorder: Arc::new(NoopMetricsRecorder::new()),
    };

    let recovered = recover(&inputs).expect("standalone recovers");

    // A fresh in-memory state, offset 0, and no state file written to disk.
    assert_eq!(recovered.replication.offset_at_save, 0);
    assert!(
        !tmp.path().join(&repl_cfg.state_file).exists(),
        "standalone must not write a replication state file"
    );
}

// FM-PERSISTENCE-038
#[test]
fn primary_loads_and_persists_replication_state() {
    let tmp = TempDir::new().unwrap();
    let cfg = persistence_config(tmp.path(), true);
    let repl_cfg = replication_config("primary");
    let cluster_cfg = cluster_config(false);
    let inputs = RecoveryInputs {
        data_dir: &cfg.data_dir,
        persistence: &cfg,
        replication: &repl_cfg,
        cluster: &cluster_cfg,
        recovery: continue_policy(),
        num_shards: 1,
        warm_enabled: false,
        metrics_recorder: Arc::new(NoopMetricsRecorder::new()),
    };

    let recovered = recover(&inputs).expect("primary recovers");

    assert_eq!(recovered.replication.offset_at_save, 0);
    assert_eq!(recovered.replication.replication_id.len(), 40);
    assert!(
        tmp.path().join(&repl_cfg.state_file).exists(),
        "primary creates a replication state file (load_or_create)"
    );
}

// FM-PERSISTENCE-027
// FM-PERSISTENCE-039
#[test]
fn staged_replication_metadata_is_adopted_and_consumed() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    std::fs::create_dir_all(&db_dir).unwrap();
    mark(tmp.path());

    // Stage replication metadata (as a replica full sync would, carried into the
    // data dir when the staged checkpoint is installed).
    let staged_id = "a".repeat(40);
    let staged = format!(
        "{{\"replication_id\":\"{}\",\"replication_offset\":4242}}",
        staged_id
    );
    std::fs::write(db_dir.join("replication_metadata.json"), staged).unwrap();

    let cfg = persistence_config(tmp.path(), true);
    let repl_cfg = replication_config("replica");
    let cluster_cfg = cluster_config(false);
    let inputs = RecoveryInputs {
        data_dir: &cfg.data_dir,
        persistence: &cfg,
        replication: &repl_cfg,
        cluster: &cluster_cfg,
        recovery: continue_policy(),
        num_shards: 1,
        warm_enabled: false,
        metrics_recorder: Arc::new(NoopMetricsRecorder::new()),
    };

    let recovered = recover(&inputs).expect("replica recovers");

    // Phase 5 returns the reconciled state: staged id + offset win.
    assert_eq!(recovered.replication.replication_id, staged_id);
    assert_eq!(recovered.replication.offset_at_save, 4242);
    // The staging file is consumed so later restarts use the state file.
    assert!(
        !db_dir.join("replication_metadata.json").exists(),
        "staged metadata consumed after adoption"
    );
    // The reconciled offset is persisted to the state file.
    assert!(tmp.path().join(&repl_cfg.state_file).exists());
}

/// A save that cannot land must not take the staging file with it.
///
/// The staged metadata is the only durable carrier of the offset that matches
/// the freshly installed snapshot; until the state file holds it, consuming it
/// destroys the only copy and the next boot resumes from the *pre*-full-sync
/// offset against post-full-sync data (issue 08).
///
/// The save is failed at its atomic-write step by parking a **directory** where
/// `ReplicationState::save` wants to put its `.tmp` file — deterministic, no
/// permission games, and it leaves the data dir writable so the consume step is
/// genuinely able to run (a read-only dir would fail the consume too, and the
/// test would pass for the wrong reason).
// FM-PERSISTENCE-039
#[test]
fn staged_metadata_survives_a_failed_state_save() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    std::fs::create_dir_all(&db_dir).unwrap();

    let repl_cfg = replication_config("replica");
    let state_path = tmp.path().join(&repl_cfg.state_file);

    // A pre-full-sync state file: the stale position that must not survive as
    // the node's durable answer.
    let stale_id = "b".repeat(40);
    std::fs::write(
        &state_path,
        format!(
            "{{\"replication_id\":\"{}\",\"offset_at_save\":100}}",
            stale_id
        ),
    )
    .unwrap();

    // Staged metadata from the installed checkpoint.
    let staged_id = "a".repeat(40);
    std::fs::write(
        db_dir.join("replication_metadata.json"),
        format!(
            "{{\"replication_id\":\"{}\",\"replication_offset\":4242}}",
            staged_id
        ),
    )
    .unwrap();

    // Block the atomic write: `save` writes `<state_file stem>.tmp` and renames.
    std::fs::create_dir_all(state_path.with_extension("tmp")).unwrap();

    // Persistence disabled so no other phase needs the data dir; phase 5 runs
    // either way (replication does not require persistence).
    let cfg = persistence_config(tmp.path(), false);
    let cluster_cfg = cluster_config(false);
    let inputs = RecoveryInputs {
        data_dir: tmp.path(),
        persistence: &cfg,
        replication: &repl_cfg,
        cluster: &cluster_cfg,
        recovery: continue_policy(),
        num_shards: 1,
        warm_enabled: false,
        metrics_recorder: Arc::new(NoopMetricsRecorder::new()),
    };

    let recovered = recover(&inputs).expect("a failed state save is not fatal");

    // This boot is still correct: the in-memory state carries the staged values.
    assert_eq!(recovered.replication.replication_id, staged_id);
    assert_eq!(recovered.replication.offset_at_save, 4242);

    // The save could not land, so the staging file is the only copy of the
    // post-full-sync offset and must still be there for the next boot.
    assert!(
        db_dir.join("replication_metadata.json").exists(),
        "staged metadata must survive a failed save so the next boot can re-adopt it"
    );

    // And the stale state file is untouched — nothing half-written.
    let on_disk = std::fs::read_to_string(&state_path).unwrap();
    assert!(
        on_disk.contains(&stale_id),
        "the unwritable state file keeps its previous contents: {on_disk}"
    );
}

// FM-PERSISTENCE-038
#[test]
fn corrupt_replication_state_is_regenerated() {
    // Both ways the persisted state can be unusable: unparseable bytes, and
    // well-formed JSON whose replication id fails validation. Neither may be
    // fatal, and neither may leave the bad file behind.
    for (label, contents) in [
        ("unparseable", "{ this is not json".to_string()),
        (
            "invalid replication id",
            "{\"replication_id\":\"nothex\",\"offset_at_save\":99}".to_string(),
        ),
    ] {
        let tmp = TempDir::new().unwrap();
        let repl_cfg = replication_config("primary");
        mark(tmp.path());
        let state_path = tmp.path().join(&repl_cfg.state_file);
        std::fs::write(&state_path, &contents).unwrap();

        let cfg = persistence_config(tmp.path(), true);
        let cluster_cfg = cluster_config(false);
        let inputs = RecoveryInputs {
            data_dir: &cfg.data_dir,
            persistence: &cfg,
            replication: &repl_cfg,
            cluster: &cluster_cfg,
            recovery: continue_policy(),
            num_shards: 1,
            warm_enabled: false,
            metrics_recorder: Arc::new(NoopMetricsRecorder::new()),
        };

        let recovered =
            recover(&inputs).unwrap_or_else(|e| panic!("{label} state must not be fatal: {e}"));

        // A fresh identity at offset 0 — which forces a full resync rather than
        // offering a peer a position neither side can honour.
        assert_eq!(
            recovered.replication.replication_id.len(),
            40,
            "{label}: regenerated replication id"
        );
        assert!(
            recovered
                .replication
                .replication_id
                .bytes()
                .all(|b| b.is_ascii_hexdigit()),
            "{label}: regenerated id is hex"
        );
        assert_eq!(
            recovered.replication.offset_at_save, 0,
            "{label}: offset 0 forces a full resync"
        );

        // The bad file is replaced, so the next boot is clean too.
        let rewritten = std::fs::read_to_string(&state_path).unwrap();
        assert!(
            rewritten.contains(&recovered.replication.replication_id),
            "{label}: regenerated state written back over the bad file"
        );
    }
}

// FM-PERSISTENCE-040
#[test]
fn cluster_storage_open_failure_is_a_recovery_error() {
    let tmp = TempDir::new().unwrap();
    mark(tmp.path());
    // A plain file where the Raft store's directory must be: the open fails, and
    // a cluster node must refuse to start rather than fall back to standalone.
    std::fs::write(tmp.path().join("raft"), b"not a directory").unwrap();

    let cfg = persistence_config(tmp.path(), true);
    let repl_cfg = replication_config("standalone");
    let cluster_cfg = cluster_config(true);
    let inputs = RecoveryInputs {
        data_dir: &cfg.data_dir,
        persistence: &cfg,
        replication: &repl_cfg,
        cluster: &cluster_cfg,
        recovery: continue_policy(),
        num_shards: 1,
        warm_enabled: false,
        metrics_recorder: Arc::new(NoopMetricsRecorder::new()),
    };

    let err = recover(&inputs)
        .err()
        .expect("unopenable raft storage must error");
    assert_eq!(err.phase, RecoveryPhase::OpenClusterStorage);
}

// FM-PERSISTENCE-040
#[test]
fn cluster_mode_opens_raft_storage() {
    let tmp = TempDir::new().unwrap();
    let cfg = persistence_config(tmp.path(), true);
    let repl_cfg = replication_config("standalone");
    let cluster_cfg = cluster_config(true);
    let inputs = RecoveryInputs {
        data_dir: &cfg.data_dir,
        persistence: &cfg,
        replication: &repl_cfg,
        cluster: &cluster_cfg,
        recovery: continue_policy(),
        num_shards: 1,
        warm_enabled: false,
        metrics_recorder: Arc::new(NoopMetricsRecorder::new()),
    };

    let recovered = recover(&inputs).expect("cluster recovers");

    assert!(
        recovered.raft_storage.is_some(),
        "cluster mode opens raft storage"
    );
    assert!(
        tmp.path().join("raft").exists(),
        "raft storage created under data_dir/raft"
    );
}

// FM-PERSISTENCE-027
// FM-PERSISTENCE-030
#[test]
fn shard_count_mismatch_is_a_recovery_error() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    // Write the data dir with 2 shards.
    seed_db(&db_dir, 2, b"k", "v");

    // Recover configured for 4 shards: must fail loudly, not silently drop data.
    let cfg = persistence_config(tmp.path(), true);
    let repl_cfg = replication_config("standalone");
    let cluster_cfg = cluster_config(false);
    let inputs = RecoveryInputs {
        data_dir: &cfg.data_dir,
        persistence: &cfg,
        replication: &repl_cfg,
        cluster: &cluster_cfg,
        recovery: continue_policy(),
        num_shards: 4,
        warm_enabled: false,
        metrics_recorder: Arc::new(NoopMetricsRecorder::new()),
    };

    let err = recover(&inputs)
        .err()
        .expect("shard-count mismatch must error");
    assert_eq!(err.phase, RecoveryPhase::OpenRocks);
}

// FM-PERSISTENCE-025
// FM-PERSISTENCE-027
#[test]
fn staged_checkpoint_is_installed() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    let checkpoint_dir = tmp.path().join("staging");

    // Live db has the old value; staged checkpoint has the new value.
    seed_db(&db_dir, 2, b"shared", "old");
    seed_staged(&checkpoint_dir, 2, b"shared", "new");

    let cfg = persistence_config(tmp.path(), true);
    let repl_cfg = replication_config("standalone");
    let cluster_cfg = cluster_config(false);
    let inputs = RecoveryInputs {
        data_dir: &cfg.data_dir,
        persistence: &cfg,
        replication: &repl_cfg,
        cluster: &cluster_cfg,
        recovery: continue_policy(),
        num_shards: 2,
        warm_enabled: false,
        metrics_recorder: Arc::new(NoopMetricsRecorder::new()),
    };

    let mut recovered = recover(&inputs).expect("staged checkpoint installs");

    assert!(
        recovered.installed_staged_checkpoint,
        "checkpoint should be reported as installed"
    );
    assert!(
        !checkpoint_dir.exists(),
        "the staged dir was renamed into place as <data-dir>/db"
    );
    // The checkpoint's value won.
    let value = recovered
        .shards
        .iter_mut()
        .filter_map(|(store, _)| store.get(b"shared"))
        .next()
        .expect("recovered key present");
    assert_eq!(value.as_string().unwrap().as_bytes().as_ref(), b"new");
    // The previous live db was backed up inside the data dir.
    assert!(
        has_backup(tmp.path()),
        "old database backed up to <data-dir>/backup/db_backup_*"
    );
}

// FM-PERSISTENCE-024
#[test]
fn incomplete_staged_checkpoint_is_refused_without_touching_live_db() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    let checkpoint_dir = tmp.path().join("staging");

    // A complete, valid live database.
    seed_db(&db_dir, 2, b"live", "data");
    // An incomplete staged checkpoint: directory exists but has no CURRENT manifest.
    std::fs::create_dir_all(&checkpoint_dir).unwrap();
    std::fs::write(checkpoint_dir.join("stray.sst"), b"garbage").unwrap();

    let cfg = persistence_config(tmp.path(), true);
    let repl_cfg = replication_config("standalone");
    let cluster_cfg = cluster_config(false);
    let inputs = RecoveryInputs {
        data_dir: &cfg.data_dir,
        persistence: &cfg,
        replication: &repl_cfg,
        cluster: &cluster_cfg,
        recovery: continue_policy(),
        num_shards: 2,
        warm_enabled: false,
        metrics_recorder: Arc::new(NoopMetricsRecorder::new()),
    };

    let err = recover(&inputs)
        .err()
        .expect("incomplete checkpoint must be refused");
    assert_eq!(err.phase, RecoveryPhase::InstallStagedCheckpoint);
    // The live database must be untouched: not moved aside to a backup.
    assert!(db_dir.join("CURRENT").exists(), "live db left in place");
    assert!(
        !has_backup(tmp.path()),
        "live db must not be backed up on refusal"
    );
}

/// A metrics recorder that keeps counter totals, so a test can assert what
/// recovery actually emitted rather than only what it returned.
#[derive(Default)]
struct RecordingMetricsRecorder {
    counters: std::sync::Mutex<std::collections::HashMap<String, u64>>,
}

impl RecordingMetricsRecorder {
    fn total(&self, name: &str) -> u64 {
        self.counters
            .lock()
            .unwrap()
            .get(name)
            .copied()
            .unwrap_or(0)
    }
}

impl frogdb_core::MetricsRecorder for RecordingMetricsRecorder {
    fn increment_counter(&self, name: &str, value: u64, _labels: &[(&str, &str)]) {
        *self
            .counters
            .lock()
            .unwrap()
            .entry(name.to_string())
            .or_default() += value;
    }
    fn record_gauge(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}
    fn record_histogram(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}
}

/// Write `value` under `key` in shard 0 without going through the serializer,
/// so recovery meets bytes it cannot decode.
fn seed_raw(db_dir: &Path, num_shards: usize, entries: &[(&[u8], &[u8])]) {
    let rocks = RocksStore::open(db_dir, num_shards, &RocksConfig::default()).unwrap();
    for (key, value) in entries {
        rocks.put(0, key, value).unwrap();
    }
    rocks.flush().unwrap();
    drop(rocks);
    mark(data_dir_of(db_dir));
}

/// Build recovery inputs for a data dir that already exists on disk.
fn inputs_for<'a>(
    cfg: &'a PersistenceConfig,
    repl: &'a ReplicationConfigSection,
    cluster: &'a ClusterConfigSection,
    recovery: &'a RecoveryConfig,
    num_shards: usize,
    metrics: Arc<dyn frogdb_core::MetricsRecorder>,
) -> RecoveryInputs<'a> {
    RecoveryInputs {
        data_dir: &cfg.data_dir,
        persistence: cfg,
        replication: repl,
        cluster,
        recovery,
        num_shards,
        warm_enabled: false,
        metrics_recorder: metrics,
    }
}

// FM-PERSISTENCE-045
/// A data directory holding data of which *nothing* decodes is a broken (or
/// foreign) database, not an empty one: recovery refuses rather than booting an
/// empty keyspace that the WAL and snapshot cadence would then overwrite.
#[test]
fn wholly_undecodable_database_refuses_to_start() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    seed_raw(
        &db_dir,
        2,
        &[
            (b"one", b"not a serialized value"),
            (b"two", b"nor is this one"),
        ],
    );

    let cfg = persistence_config(tmp.path(), true);
    let repl_cfg = replication_config("standalone");
    let cluster_cfg = cluster_config(false);
    let inputs = inputs_for(
        &cfg,
        &repl_cfg,
        &cluster_cfg,
        continue_policy(),
        2,
        Arc::new(NoopMetricsRecorder::new()),
    );

    let err = recover(&inputs)
        .err()
        .expect("a wholly undecodable database must not boot");
    assert_eq!(err.phase, RecoveryPhase::RestoreShards);
    let msg = err.to_string();
    assert!(
        msg.contains("2 key(s)") && msg.contains(&tmp.path().display().to_string()),
        "the refusal must name the scale and the directory: {msg}"
    );
}

// FM-PERSISTENCE-033
/// One bad value among good ones is skipped, counted, and metered — the boot
/// still succeeds, because a single corrupt key must not cost the keyspace.
#[test]
fn partial_decode_failure_is_counted_and_metered() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    seed_db(&db_dir, 2, b"good", "value");
    seed_raw(&db_dir, 2, &[(b"bad", b"not a serialized value")]);

    let cfg = persistence_config(tmp.path(), true);
    let repl_cfg = replication_config("standalone");
    let cluster_cfg = cluster_config(false);
    let metrics = Arc::new(RecordingMetricsRecorder::default());
    let inputs = inputs_for(
        &cfg,
        &repl_cfg,
        &cluster_cfg,
        continue_policy(),
        2,
        metrics.clone(),
    );

    let recovered = recover(&inputs).expect("one bad key must not fail the boot");
    assert_eq!(recovered.stats.keys_failed, 1);
    assert_eq!(recovered.stats.keys_loaded, 1);
    assert_eq!(
        metrics.total("frogdb_recovery_keys_failed_total"),
        1,
        "the skip must be visible to monitoring, not only in the log"
    );
}

// FM-PERSISTENCE-033
// FM-PERSISTENCE-045
/// A key that decoded and was then dropped for being expired still counts as
/// decoded: the database is readable, so a lone corrupt key beside it takes the
/// skip-and-count path rather than the refusal, even though the recovered
/// keyspace is empty.
#[test]
fn expired_keys_count_as_decoded_so_one_bad_key_does_not_refuse() {
    use std::time::Duration;

    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    {
        let rocks = RocksStore::open(&db_dir, 2, &RocksConfig::default()).unwrap();
        let value = Value::string("gone");
        let mut metadata = KeyMetadata::new(value.memory_size());
        metadata.expires_at = Some(std::time::Instant::now() + Duration::from_millis(1));
        rocks
            .put(0, b"expiring", &serialize(&value, &metadata))
            .unwrap();
        rocks.put(0, b"bad", b"not a serialized value").unwrap();
        rocks.flush().unwrap();
    }
    mark(tmp.path());
    std::thread::sleep(Duration::from_millis(10));

    let cfg = persistence_config(tmp.path(), true);
    let repl_cfg = replication_config("standalone");
    let cluster_cfg = cluster_config(false);
    let inputs = inputs_for(
        &cfg,
        &repl_cfg,
        &cluster_cfg,
        continue_policy(),
        2,
        Arc::new(NoopMetricsRecorder::new()),
    );

    let recovered = recover(&inputs).expect("a decodable-but-expired key means the db is readable");
    assert_eq!(recovered.stats.keys_loaded, 0);
    assert_eq!(recovered.stats.keys_expired_skipped, 1);
    assert_eq!(recovered.stats.keys_failed, 1);
}

/// Build recovery inputs with the warm tier switched on, for the tiered-storage
/// half of the "is this database readable at all" predicate.
fn warm_inputs_for<'a>(
    cfg: &'a PersistenceConfig,
    repl: &'a ReplicationConfigSection,
    cluster: &'a ClusterConfigSection,
    recovery: &'a RecoveryConfig,
    num_shards: usize,
) -> RecoveryInputs<'a> {
    RecoveryInputs {
        data_dir: &cfg.data_dir,
        persistence: cfg,
        replication: repl,
        cluster,
        recovery,
        num_shards,
        warm_enabled: true,
        metrics_recorder: Arc::new(NoopMetricsRecorder::new()),
    }
}

// FM-PERSISTENCE-045
/// A database whose *only* decodable value lives in the warm tier is still a
/// readable database. A tiered deployment can legitimately have a hot CF that
/// holds nothing this build can decode while the data itself sits warm, so the
/// refusal predicate counts warm-tier hits — dropping that term would condemn a
/// perfectly restorable database.
#[test]
fn warm_tier_only_decode_does_not_refuse_start() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    {
        let rocks = RocksStore::open_with_warm(&db_dir, 2, &RocksConfig::default(), true).unwrap();
        // Hot tier: nothing but bytes recovery cannot decode.
        rocks.put(0, b"bad", b"not a serialized value").unwrap();
        // Warm tier: one perfectly good value.
        let value = Value::string("tiered out");
        let metadata = KeyMetadata::new(value.memory_size());
        rocks
            .put_warm(0, b"warmkey", &serialize(&value, &metadata))
            .unwrap();
        rocks.flush().unwrap();
    }
    mark(tmp.path());

    let cfg = persistence_config(tmp.path(), true);
    let repl_cfg = replication_config("standalone");
    let cluster_cfg = cluster_config(false);
    let inputs = warm_inputs_for(&cfg, &repl_cfg, &cluster_cfg, continue_policy(), 2);

    let recovered = recover(&inputs).expect("a decodable warm-tier key means the db is readable");
    assert_eq!(recovered.stats.keys_loaded, 0, "hot tier decoded nothing");
    assert_eq!(recovered.stats.keys_failed, 1);
    assert_eq!(
        recovered.stats.warm_keys_loaded, 1,
        "the warm key is what makes this database readable"
    );
}

// FM-PERSISTENCE-045
/// A warm entry shadowed by a hot copy is dropped as stale — but it *decoded*,
/// so it counts toward the readability predicate just like a key that decoded
/// and was then dropped as expired. Losing that term would let a database with
/// one good key, its stale warm shadow, and one corrupt key refuse to boot.
#[test]
fn stale_warm_entry_counts_as_decoded() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    {
        let rocks = RocksStore::open_with_warm(&db_dir, 2, &RocksConfig::default(), true).unwrap();
        let value = Value::string("hot wins");
        let metadata = KeyMetadata::new(value.memory_size());
        let encoded = serialize(&value, &metadata);
        // Same key in both tiers: the hot copy wins, the warm one is stale.
        rocks.put(0, b"shadowed", &encoded).unwrap();
        rocks.put_warm(0, b"shadowed", &encoded).unwrap();
        rocks.put(0, b"bad", b"not a serialized value").unwrap();
        rocks.flush().unwrap();
    }
    mark(tmp.path());

    let cfg = persistence_config(tmp.path(), true);
    let repl_cfg = replication_config("standalone");
    let cluster_cfg = cluster_config(false);
    let inputs = warm_inputs_for(&cfg, &repl_cfg, &cluster_cfg, continue_policy(), 2);

    let recovered = recover(&inputs).expect("a stale-but-decoded warm key still counts");
    assert_eq!(recovered.stats.keys_loaded, 1);
    assert_eq!(recovered.stats.keys_failed, 1);
    assert_eq!(
        recovered.stats.warm_keys_stale, 1,
        "the warm shadow decoded and was then dropped for the hot copy"
    );
    assert_eq!(recovered.stats.warm_keys_loaded, 0);
}

// FM-PERSISTENCE-047
/// `refuse` means what it says: one undecodable key among perfectly good ones
/// stops the boot. The whole point of the policy is that it fires where the
/// nothing-decoded refusal deliberately does not, so this seeds a database that
/// boots fine under the default.
#[test]
fn refuse_policy_fails_the_boot_on_a_single_decode_failure() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    seed_db(&db_dir, 2, b"good", "value");
    seed_raw(&db_dir, 2, &[(b"bad", b"not a serialized value")]);

    let cfg = persistence_config(tmp.path(), true);
    let repl_cfg = replication_config("standalone");
    let cluster_cfg = cluster_config(false);
    let metrics = Arc::new(RecordingMetricsRecorder::default());
    let inputs = inputs_for(
        &cfg,
        &repl_cfg,
        &cluster_cfg,
        refuse_policy(),
        2,
        metrics.clone(),
    );

    let err = recover(&inputs)
        .err()
        .expect("refuse must not boot past a decode failure");
    assert_eq!(err.phase, RecoveryPhase::RestoreShards);
    let msg = err.to_string();
    assert!(
        msg.contains("recovery.on-decode-failure is 'refuse'"),
        "the refusal must name the policy that caused it, so the operator knows \
         it is their setting and not corruption triage: {msg}"
    );
    assert!(
        msg.contains("1 key(s)") && msg.contains(&tmp.path().display().to_string()),
        "the refusal must name the scale and the directory: {msg}"
    );
    assert!(
        msg.contains("key 'bad'") && msg.contains("shard 0") && msg.contains("hot tier"),
        "the refusal must point at a concrete failing key: {msg}"
    );
    assert!(
        msg.contains("recovery.on-decode-failure = continue"),
        "the refusal must name the way out of it: {msg}"
    );
    assert_eq!(
        metrics.total("frogdb_recovery_keys_failed_total"),
        1,
        "the metric is incremented before the refusal, so a monitored boot loop \
         shows what it is failing on"
    );
}

// FM-PERSISTENCE-047
/// The default is `continue`, and it is the *configured* default rather than a
/// property of the test fixture: this boots the same corrupt database as the
/// test above through a `RecoveryConfig` nobody touched.
#[test]
fn continue_policy_is_the_default_and_boots_past_a_decode_failure() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    seed_db(&db_dir, 2, b"good", "value");
    seed_raw(&db_dir, 2, &[(b"bad", b"not a serialized value")]);

    let cfg = persistence_config(tmp.path(), true);
    let repl_cfg = replication_config("standalone");
    let cluster_cfg = cluster_config(false);
    // Straight from serde defaults — the config an operator gets by writing
    // nothing at all.
    let recovery_cfg: RecoveryConfig =
        serde_json::from_str("{}").expect("default recovery config from empty json");
    let inputs = inputs_for(
        &cfg,
        &repl_cfg,
        &cluster_cfg,
        &recovery_cfg,
        2,
        Arc::new(NoopMetricsRecorder::new()),
    );

    let recovered = recover(&inputs).expect("the default policy skips and keeps going");
    assert_eq!(recovered.stats.keys_loaded, 1);
    assert_eq!(recovered.stats.keys_failed, 1);
}

// FM-PERSISTENCE-047
/// The policy covers the warm tier too. A tiered deployment holds most of its
/// bytes there, so a `refuse` that only watched the hot CF would quietly not be
/// the policy the operator asked for. The hot tier here decodes cleanly, so
/// nothing but the warm failure can be causing the refusal.
#[test]
fn refuse_policy_covers_warm_tier_decode_failures() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    {
        let rocks = RocksStore::open_with_warm(&db_dir, 2, &RocksConfig::default(), true).unwrap();
        let value = Value::string("hot and fine");
        let metadata = KeyMetadata::new(value.memory_size());
        rocks
            .put(0, b"hotkey", &serialize(&value, &metadata))
            .unwrap();
        rocks
            .put_warm(0, b"warmbad", b"not a serialized value")
            .unwrap();
        rocks.flush().unwrap();
    }
    mark(tmp.path());

    let cfg = persistence_config(tmp.path(), true);
    let repl_cfg = replication_config("standalone");
    let cluster_cfg = cluster_config(false);
    let inputs = warm_inputs_for(&cfg, &repl_cfg, &cluster_cfg, refuse_policy(), 2);

    let err = recover(&inputs)
        .err()
        .expect("a warm-tier decode failure must refuse under 'refuse'");
    assert_eq!(err.phase, RecoveryPhase::RestoreShards);
    let msg = err.to_string();
    assert!(
        msg.contains("key 'warmbad'") && msg.contains("warm tier"),
        "the refusal must name the tier, because a warm-only failure points at \
         tiered storage rather than at the primary dataset: {msg}"
    );
}

// FM-PERSISTENCE-047
/// The reported context is the *first* failure in iteration order, not the last
/// one seen. Two undecodable keys are seeded in the same shard with names that
/// fix their order, so a last-wins capture would name the other one.
#[test]
fn decode_failure_context_is_first_wins() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    seed_db(&db_dir, 2, b"good", "value");
    seed_raw(
        &db_dir,
        2,
        &[
            (b"aaa-bad", b"not a serialized value"),
            (b"zzz-bad", b"nor is this one"),
        ],
    );

    let cfg = persistence_config(tmp.path(), true);
    let repl_cfg = replication_config("standalone");
    let cluster_cfg = cluster_config(false);
    let inputs = inputs_for(
        &cfg,
        &repl_cfg,
        &cluster_cfg,
        refuse_policy(),
        2,
        Arc::new(NoopMetricsRecorder::new()),
    );

    let msg = recover(&inputs)
        .err()
        .expect("two decode failures must refuse under 'refuse'")
        .to_string();
    assert!(
        msg.contains("key 'aaa-bad'"),
        "the first failure in iteration order is the one to report: {msg}"
    );
    assert!(
        !msg.contains("zzz-bad"),
        "the context is one example, not a list: {msg}"
    );
    assert!(
        msg.contains("2 key(s)"),
        "the count is what says how widespread the problem is: {msg}"
    );
}

// FM-PERSISTENCE-047
/// The captured context records which tier it came from, and survives the fold
/// from per-shard stats into the whole-database total — so `continue` boots
/// carry it too, for the INFO/log surfaces rather than a refusal message.
#[test]
fn warm_decode_failure_context_records_the_warm_tier() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    {
        let rocks = RocksStore::open_with_warm(&db_dir, 2, &RocksConfig::default(), true).unwrap();
        let value = Value::string("hot and fine");
        let metadata = KeyMetadata::new(value.memory_size());
        rocks
            .put(0, b"hotkey", &serialize(&value, &metadata))
            .unwrap();
        rocks
            .put_warm(0, b"warmbad", b"not a serialized value")
            .unwrap();
        rocks.flush().unwrap();
    }
    mark(tmp.path());

    let cfg = persistence_config(tmp.path(), true);
    let repl_cfg = replication_config("standalone");
    let cluster_cfg = cluster_config(false);
    let inputs = warm_inputs_for(&cfg, &repl_cfg, &cluster_cfg, continue_policy(), 2);

    let recovered = recover(&inputs).expect("continue boots past a warm decode failure");
    let failure = recovered
        .stats
        .first_failure
        .as_ref()
        .expect("a decode failure must leave context behind, not only a count");
    assert!(failure.warm, "the failure came from the warm CF");
    assert_eq!(failure.shard_id, 0);
    assert_eq!(failure.key.as_ref(), b"warmbad");
    assert!(
        !failure.error.is_empty(),
        "the decoder's own words are what make the context actionable"
    );
}

// FM-PERSISTENCE-047
/// The refusal previews the failing key, and a key can be megabytes. The
/// preview is cut at 128 bytes — and the cut is announced with the real byte
/// length, so a truncated preview cannot be mistaken for a whole key name that
/// happens to end there. Both sides of the boundary are asserted: a key exactly
/// at the limit must come out whole, since a preview that says "… (128 bytes
/// total)" about a key it printed in full is a lie about the data.
#[test]
fn a_failing_key_is_previewed_whole_up_to_the_limit_and_marked_when_cut() {
    let refusal_for = |key: &[u8]| {
        let tmp = TempDir::new().unwrap();
        let db_dir = tmp.path().join("db");
        seed_raw(&db_dir, 2, &[(key, b"not a serialized value")]);

        let cfg = persistence_config(tmp.path(), true);
        let repl_cfg = replication_config("standalone");
        let cluster_cfg = cluster_config(false);
        let inputs = inputs_for(
            &cfg,
            &repl_cfg,
            &cluster_cfg,
            refuse_policy(),
            2,
            Arc::new(NoopMetricsRecorder::new()),
        );
        recover(&inputs)
            .err()
            .expect("a decode failure must refuse under 'refuse'")
            .to_string()
    };

    let at_limit = vec![b'k'; 128];
    let msg = refusal_for(&at_limit);
    assert!(
        msg.contains(&format!("key '{}'", String::from_utf8_lossy(&at_limit))),
        "a key exactly at the limit is printed whole: {msg}"
    );
    assert!(
        !msg.contains("bytes total"),
        "nothing was cut, so nothing announces a cut: {msg}"
    );

    let over_limit = vec![b'k'; 129];
    let msg = refusal_for(&over_limit);
    assert!(
        msg.contains("129 bytes total"),
        "one byte past the limit is cut, and says so with the true length: {msg}"
    );
    assert!(
        !msg.contains(&format!("key '{}'", String::from_utf8_lossy(&over_limit))),
        "the whole key is not printed once it is over the limit: {msg}"
    );
}

// FM-PERSISTENCE-037
/// The tolerant half of phase 4 has a positive half: a well-formed
/// `functions.fdb` comes back as the `(name, source)` pairs the wiring layer
/// registers. Downgrading every read to "no functions" would make a valid
/// library disappear as silently as a corrupt one.
#[test]
fn persisted_functions_are_restored() {
    let tmp = TempDir::new().unwrap();
    mark(tmp.path());

    let code = "#!lua name=greetlib\nredis.register_function('hi', function() return 'hi' end)";
    let mut registry = frogdb_core::FunctionRegistry::new();
    registry
        .load_library(
            frogdb_core::FunctionLibrary::new("greetlib".to_string(), code.to_string()),
            false,
        )
        .expect("library loads into a fresh registry");
    frogdb_core::save_to_file(&registry, &tmp.path().join("functions.fdb"))
        .expect("functions.fdb written");

    let cfg = persistence_config(tmp.path(), true);
    let repl_cfg = replication_config("standalone");
    let cluster_cfg = cluster_config(false);
    let inputs = inputs_for(
        &cfg,
        &repl_cfg,
        &cluster_cfg,
        continue_policy(),
        1,
        Arc::new(NoopMetricsRecorder::new()),
    );

    let recovered = recover(&inputs).expect("a valid functions.fdb is not fatal");
    assert_eq!(
        recovered.functions,
        vec![("greetlib".to_string(), code.to_string())],
        "the stored library must survive recovery, not be silently dropped"
    );
    assert_eq!(
        recovered.stats.functions_failed, 0,
        "a library that loaded is not a lost one"
    );
}

// ---------------------------------------------------------------------------
// Phase 0 — is this directory FrogDB's? (FM-PERSISTENCE-048..052)
// ---------------------------------------------------------------------------

/// How a refusal spells a path inside `<data-dir>/db`. Which of a RocksDB's
/// files the probe reaches first is directory-order, so tests that seed a real
/// database assert on the prefix rather than on one name.
fn db_prefix() -> String {
    format!("db{}", std::path::MAIN_SEPARATOR)
}

/// Boot a standalone, non-cluster node against `cfg`. Every data-directory test
/// below varies only the persistence config, so the rest of the inputs are noise.
fn boot_standalone(cfg: &PersistenceConfig) -> Result<crate::RecoveredState, crate::RecoveryError> {
    let repl_cfg = replication_config("standalone");
    let cluster_cfg = cluster_config(false);
    let inputs = inputs_for(
        cfg,
        &repl_cfg,
        &cluster_cfg,
        continue_policy(),
        2,
        Arc::new(NoopMetricsRecorder::new()),
    );
    recover(&inputs)
}

/// A directory holding somebody else's bytes and no marker is what a mistyped
/// `data-dir`, a lost bind mount, and a volume mounted elsewhere all look like
/// from the inside. The boot refuses instead of initializing a database on top,
/// and the refusal has to be actionable: the *resolved* path (the configured
/// spelling is the thing the operator already got wrong) and the way out.
// FM-PERSISTENCE-051
#[test]
fn an_unrelated_file_in_the_data_dir_refuses_the_boot() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    std::fs::create_dir_all(&db_dir).unwrap();
    std::fs::write(db_dir.join("important.txt"), b"somebody elses bytes").unwrap();

    let cfg = persistence_config(tmp.path(), true);
    let err = boot_standalone(&cfg)
        .err()
        .expect("a populated directory with no marker must not boot");
    assert_eq!(err.phase, RecoveryPhase::VerifyDataDir);

    let msg = err.to_string();
    let absolute = std::path::absolute(tmp.path()).unwrap();
    assert!(
        msg.contains(&absolute.display().to_string()),
        "the refusal must name the resolved absolute path: {msg}"
    );
    assert!(
        msg.contains(MARKER_FILE_NAME),
        "the refusal must name the marker it looked for: {msg}"
    );
    assert!(
        msg.contains(&Path::new("db").join("important.txt").display().to_string()),
        "the refusal must name the entry that is in the way: {msg}"
    );
    assert!(
        msg.contains("--force-fresh-data-dir does not override this"),
        "the refusal must not advertise the flag as an adopt path: {msg}"
    );

    // Refusing means refusing to *write*: the guard runs before anything can
    // initialize a database over the file it is protecting.
    assert_eq!(
        std::fs::read(db_dir.join("important.txt")).unwrap(),
        b"somebody elses bytes",
        "the file the refusal is about must be untouched"
    );
    assert!(
        !db_dir.join("CURRENT").exists(),
        "a refused boot must not leave a RocksDB behind"
    );
    assert!(
        !DataDirMarker::path(tmp.path()).exists(),
        "a refused boot must not stamp the directory it refused"
    );
}

/// The refusal names a handful of entries, then says the list is truncated —
/// and only then. Both halves matter: naming stops at a bound so a startup
/// error never renders somebody else's whole tree, and "and more" appears
/// exactly when something went unnamed, so an operator who moves the named
/// entries out knows whether they are done.
// FM-PERSISTENCE-051
#[test]
fn a_refusal_names_a_handful_of_entries_then_says_and_more() {
    let boot_message = |count: usize| {
        let tmp = TempDir::new().unwrap();
        for i in 0..count {
            std::fs::write(tmp.path().join(format!("part-{i}")), b"bytes").unwrap();
        }
        let cfg = persistence_config(tmp.path(), true);
        boot_standalone(&cfg)
            .err()
            .expect("foreign entries must refuse the boot")
            .to_string()
    };

    let at_the_bound = boot_message(8);
    assert_eq!(
        at_the_bound.matches("part-").count(),
        8,
        "a listing that fits the bound is named in full: {at_the_bound}"
    );
    assert!(
        !at_the_bound.contains("and more"),
        "a complete listing must not claim truncation: {at_the_bound}"
    );

    let past_the_bound = boot_message(9);
    assert_eq!(
        past_the_bound.matches("part-").count(),
        8,
        "the walk stops at the bound instead of rendering the tree: {past_the_bound}"
    );
    assert!(
        past_the_bound.contains("and more"),
        "an unnamed entry must be admitted to: {past_the_bound}"
    );
}

/// The other half of the guard: a directory FrogDB may have comes up silently
/// and is stamped, and the stamp is what makes the *next* boot silent too. If
/// this half were wrong, every restart would need the override.
// FM-PERSISTENCE-048
#[test]
fn a_fresh_data_dir_boots_and_stamps_the_marker() {
    let tmp = TempDir::new().unwrap();
    // Nothing is created: a first boot's data directory holds no `db/` yet.
    let cfg = persistence_config(tmp.path(), true);

    boot_standalone(&cfg).expect("a genuinely fresh directory is a first boot, not a refusal");

    let first = marker_of(tmp.path());
    assert_eq!(first.layout_version, DATA_DIR_LAYOUT_VERSION);
    assert_eq!(first.database_id.len(), 32);

    boot_standalone(&cfg).expect("the marker this boot stamped must let the next one in");
    assert_eq!(
        marker_of(tmp.path()).database_id,
        first.database_id,
        "a restart must not re-mint the directory's identity"
    );
}

/// Emptiness is about files. Container orchestration pre-creates mount points
/// and subdirectories (the cluster storage path among them), and a freshly
/// formatted ext4 volume arrives with `lost+found` — refusing those would refuse
/// the single most common production first boot.
// FM-PERSISTENCE-048
#[test]
fn pre_created_empty_subdirectories_are_still_a_first_boot() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    std::fs::create_dir_all(db_dir.join("cluster")).unwrap();
    std::fs::create_dir(db_dir.join("lost+found")).unwrap();

    let cfg = persistence_config(tmp.path(), true);
    boot_standalone(&cfg).expect("empty subdirectories are not evidence of a wrong directory");
    assert!(
        DataDirMarker::path(tmp.path()).exists(),
        "the first boot stamps the directory it initialized"
    );
}

/// An unreadable marker must not collapse into an absent one: that collapse is
/// the fail-*open* bug in miniature — a corrupt byte in the marker would license
/// initializing a fresh database over a real one.
// FM-PERSISTENCE-050
#[test]
fn a_corrupt_marker_refuses_the_boot() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    seed_db(&db_dir, 2, b"greeting", "hello");
    std::fs::write(DataDirMarker::path(tmp.path()), b"{ truncated mid-writ").unwrap();

    let cfg = persistence_config(tmp.path(), true);
    let err = boot_standalone(&cfg)
        .err()
        .expect("an unreadable marker must not boot");
    assert_eq!(err.phase, RecoveryPhase::VerifyDataDir);
    let msg = err.to_string();
    assert!(
        msg.contains("could not be read") && msg.contains("--force-fresh-data-dir"),
        "the refusal must say the marker is unreadable and name the way out: {msg}"
    );
}

/// The unmounted-volume caveat, opted into. An empty directory is what a failed
/// mount and a genuine first boot both look like, and nothing on disk can tell
/// them apart — so a deployment that knows it is past its first boot says so,
/// and an empty directory becomes a refusal instead of a fresh database.
// FM-PERSISTENCE-052
#[test]
fn require_existing_data_refuses_an_empty_data_dir() {
    let tmp = TempDir::new().unwrap();
    let mut cfg = persistence_config(tmp.path(), true);
    cfg.require_existing_data = true;

    let err = boot_standalone(&cfg)
        .err()
        .expect("an empty dir under require-existing-data must not boot");
    assert_eq!(err.phase, RecoveryPhase::VerifyDataDir);
    let msg = err.to_string();
    assert!(
        msg.contains("require-existing-data") && msg.contains("--force-fresh-data-dir"),
        "the refusal must name the setting that caused it and the way out: {msg}"
    );
    assert!(
        !DataDirMarker::path(tmp.path()).exists(),
        "a refused boot must not stamp"
    );

    // The setting is about mount failures, not about refusing every boot: the
    // same node with its data present comes up.
    mark(tmp.path());
    boot_standalone(&cfg).expect("a marked directory satisfies require-existing-data");
}

/// The flag is a *fresh-start tool, not an override* (R6). "This really is my
/// first boot" and "these bytes are not mine" are indistinguishable to a flag,
/// so the flag must not resolve both: beside entries FrogDB did not write it
/// refuses too, and the refusal names them so the operator knows what to move.
/// Adopting would mint a FrogDB identity over somebody else's volume.
// FM-PERSISTENCE-051
#[test]
fn force_fresh_data_dir_refuses_beside_foreign_entries() {
    let tmp = TempDir::new().unwrap();
    let stray = tmp.path().join("somebody-elses.txt");
    std::fs::write(&stray, b"not ours").unwrap();

    let mut cfg = persistence_config(tmp.path(), true);
    cfg.force_fresh_data_dir = true;
    let err = boot_standalone(&cfg)
        .err()
        .expect("the flag starts a fresh directory; it never claims somebody else's");
    assert_eq!(err.phase, RecoveryPhase::VerifyDataDir);

    let msg = err.to_string();
    assert!(
        msg.contains("somebody-elses.txt"),
        "the refusal must name the offending entry: {msg}"
    );
    assert!(
        msg.contains("--force-fresh-data-dir does not override this"),
        "the refusal must say the flag is not the way past it: {msg}"
    );

    // Refusing means refusing to write, flag or no flag.
    assert!(
        !DataDirMarker::path(tmp.path()).exists(),
        "a refused boot must not stamp the directory it refused"
    );
    assert_eq!(
        std::fs::read(&stray).unwrap(),
        b"not ours",
        "the entry the refusal is about must be untouched"
    );

    // The case the flag used to adopt: a whole database with no marker. Under R6
    // that is not a first boot either, and the flag no longer mints over it.
    std::fs::remove_file(&stray).unwrap();
    let db_dir = tmp.path().join("db");
    seed_db(&db_dir, 2, b"greeting", "hello");
    std::fs::remove_file(DataDirMarker::path(tmp.path())).unwrap();

    let err = boot_standalone(&cfg)
        .err()
        .expect("an unmarked database is bytes FrogDB did not write here either");
    assert_eq!(err.phase, RecoveryPhase::VerifyDataDir);
    assert!(
        err.to_string().contains(&db_prefix()),
        "the database that has no marker is itself the entry in the way: {err}"
    );
    assert!(
        !DataDirMarker::path(tmp.path()).exists(),
        "and it is still not stamped"
    );

    // The way out is the filesystem, not a second flag: with the foreign bytes
    // moved away the same command is the fresh start the flag advertises.
    std::fs::remove_dir_all(&db_dir).unwrap();
    boot_standalone(&cfg).expect("an excused-only directory is what the flag is for");
    assert!(
        DataDirMarker::path(tmp.path()).exists(),
        "the fresh start stamps the directory it initialized"
    );
}

/// The unreadable-marker arm follows the same fail-closed rule: re-stamping
/// rewrites FrogDB's own identity, which is only defensible on a directory that
/// is FrogDB's own. Beside foreign entries the flag refuses there too.
// FM-PERSISTENCE-050
#[test]
fn force_fresh_data_dir_refuses_to_re_stamp_beside_foreign_entries() {
    let tmp = TempDir::new().unwrap();
    seed_db(&tmp.path().join("db"), 2, b"greeting", "hello");
    std::fs::write(DataDirMarker::path(tmp.path()), b"{ truncated mid-writ").unwrap();

    let mut cfg = persistence_config(tmp.path(), true);
    cfg.force_fresh_data_dir = true;
    let err = boot_standalone(&cfg)
        .err()
        .expect("an unreadable marker beside a database FrogDB never stamped must not be claimed");
    assert_eq!(err.phase, RecoveryPhase::VerifyDataDir);

    let msg = err.to_string();
    assert!(
        msg.contains("could not be read"),
        "the refusal must still say why the marker failed: {msg}"
    );
    assert!(
        msg.contains(&db_prefix()),
        "and must name the entries that stop the re-stamp: {msg}"
    );
    assert_eq!(
        std::fs::read(DataDirMarker::path(tmp.path())).unwrap(),
        b"{ truncated mid-writ",
        "a refused boot must not re-stamp the marker it refused"
    );
}

/// The flag does cover the unreadable-marker refusal when the directory holds
/// nothing but FrogDB's own artifacts — otherwise a corrupt marker would leave
/// the directory permanently unbootable, which is the failure FM-PERSISTENCE-050
/// rules out.
// FM-PERSISTENCE-050
#[test]
fn force_fresh_data_dir_re_stamps_a_corrupt_marker() {
    let tmp = TempDir::new().unwrap();
    // Excused-only: the unreadable marker itself, plus install scratch. No `db/`
    // and nothing foreign, so there is no data an identity could be minted over.
    let discarded = tmp.path().join("staging.discarded");
    std::fs::create_dir_all(&discarded).unwrap();
    std::fs::write(discarded.join("CURRENT"), b"MANIFEST-000001\n").unwrap();
    std::fs::write(DataDirMarker::path(tmp.path()), b"{ truncated mid-writ").unwrap();

    let mut cfg = persistence_config(tmp.path(), true);
    cfg.force_fresh_data_dir = true;
    boot_standalone(&cfg).expect("the flag re-stamps a directory that is FrogDB's own");

    // Readable again — which is the whole point: the operator is not left with a
    // directory that needs the flag on every boot forever.
    cfg.force_fresh_data_dir = false;
    boot_standalone(&cfg).expect("the re-stamped directory boots on its own");
    assert_eq!(
        marker_of(tmp.path()).layout_version,
        DATA_DIR_LAYOUT_VERSION
    );
}

/// `require-existing-data` keeps its escape hatch, or provisioning a new node
/// into a deployment that sets it would be impossible. The two are not in
/// tension with R6: an empty directory — pre-created mount points and all — has
/// no foreign entries to refuse.
// FM-PERSISTENCE-052
#[test]
fn force_fresh_data_dir_overrides_require_existing_data() {
    let tmp = TempDir::new().unwrap();
    std::fs::create_dir(tmp.path().join("lost+found")).unwrap();
    let mut cfg = persistence_config(tmp.path(), true);
    cfg.require_existing_data = true;
    cfg.force_fresh_data_dir = true;

    boot_standalone(&cfg).expect("the override is how a require-existing-data node is provisioned");
    assert!(DataDirMarker::path(tmp.path()).exists());
}

/// The ordering that makes the guard worth having on a replica: a refusal that
/// arrives *after* a staged full sync has been installed has not protected
/// anything — it has replaced the operator's data with the primary's, which is a
/// different failure with the same lost bytes.
// FM-PERSISTENCE-027
// FM-PERSISTENCE-051
#[test]
fn the_data_dir_guard_runs_before_a_staged_checkpoint_can_install() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    let checkpoint_dir = tmp.path().join("staging");

    // The wrong directory: somebody else's file, no marker.
    std::fs::create_dir_all(&db_dir).unwrap();
    std::fs::write(db_dir.join("important.txt"), b"somebody elses bytes").unwrap();
    // A full-sync checkpoint from the primary, staged and ready to install.
    seed_staged(&checkpoint_dir, 2, b"from-primary", "new");

    let cfg = persistence_config(tmp.path(), true);
    let err = boot_standalone(&cfg)
        .err()
        .expect("a replica must refuse the wrong directory too");
    assert_eq!(err.phase, RecoveryPhase::VerifyDataDir);

    assert!(
        checkpoint_dir.join("CURRENT").exists(),
        "the checkpoint must still be staged: the install never ran"
    );
    assert!(
        db_dir.join("important.txt").exists(),
        "the directory the refusal is about must not have been renamed aside"
    );
    assert!(
        !has_backup(tmp.path()),
        "nothing was moved aside for an install that never ran"
    );
}

/// Installing a checkpoint renames `<data-dir>/db` away and a RocksDB checkpoint
/// into its place. The marker is a *sibling* of `db/` (FM-PERSISTENCE-057), so
/// the install cannot move it and the directory keeps the identity it had — a
/// full resync replaces the contents and leaves the next boot with the same
/// `database_id` rather than a refusal.
// FM-PERSISTENCE-049
#[test]
fn an_installed_checkpoint_leaves_the_data_dir_marked() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    let checkpoint_dir = tmp.path().join("staging");

    seed_db(&db_dir, 2, b"shared", "old");
    seed_staged(&checkpoint_dir, 2, b"shared", "new");
    let before = marker_of(tmp.path()).database_id;

    let cfg = persistence_config(tmp.path(), true);
    let recovered = boot_standalone(&cfg).expect("the staged checkpoint installs");
    assert!(recovered.installed_staged_checkpoint);
    // Release the installed database's LOCK: the boot below stands in for the
    // next process, not for a second one racing this test.
    drop(recovered);

    assert_eq!(
        marker_of(tmp.path()).database_id,
        before,
        "the directory's identity survives having its contents replaced"
    );
    boot_standalone(&cfg).expect("the boot after a full resync must not refuse");
}

/// Leave the data directory in the state a crash between the install's two
/// renames leaves it in: the previous database moved aside into `backup/`, the
/// new one still staged, nothing at `db/`.
fn crash_between_the_install_renames(data_dir: &Path, seq: u64) {
    let backup_root = data_dir.join("backup");
    std::fs::create_dir_all(&backup_root).unwrap();
    std::fs::rename(
        data_dir.join("db"),
        backup_root.join(format!("db_backup_{seq}")),
    )
    .unwrap();
}

/// The install's crash window, from the boot's side rather than the installer's:
/// power is cut between rename 1 and rename 2, so the directory holds a backup,
/// a staged payload, and no database at all. The next boot has to finish the
/// install *and* come up as the same database — the marker is a sibling of the
/// `db/` rename 1 moved (FM-PERSISTENCE-057) and it is stamped before the
/// install ever runs, so there is no state in which the identity has to be
/// re-derived from the contents.
// FM-PERSISTENCE-059
// FM-PERSISTENCE-025
#[test]
fn a_crash_between_the_install_renames_boots_to_the_same_database() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");

    seed_db(&db_dir, 2, b"shared", "old");
    let before = marker_of(tmp.path()).database_id;
    crash_between_the_install_renames(tmp.path(), 1);
    seed_staged(&tmp.path().join("staging"), 2, b"shared", "new");

    let cfg = persistence_config(tmp.path(), true);
    let mut recovered = boot_standalone(&cfg).expect("the next boot must finish the install");
    assert!(
        recovered.installed_staged_checkpoint,
        "the interrupted install is completed, not skipped"
    );

    let value = recovered
        .shards
        .iter_mut()
        .filter_map(|(store, _)| store.get(b"shared"))
        .next()
        .expect("the staged dataset must be the one that comes up");
    assert_eq!(value.as_string().unwrap().as_bytes().as_ref(), b"new");
    assert_eq!(
        marker_of(tmp.path()).database_id,
        before,
        "a crash mid-install must not turn this into a different database"
    );
    assert!(
        has_backup(tmp.path()),
        "the pre-crash database stays recoverable"
    );
}

/// The same crash under `persistence.require-existing-data`, and the operator
/// restore that has the same shape on disk: a replacement node provisioned onto
/// a fresh volume with a checkpoint copied into `<data-dir>/staging`. Neither
/// directory holds anything `contains_foreign_files` can see — `staging` and
/// `backup` are FrogDB's own names — so both used to refuse as "empty" with a
/// whole database sitting in them, and the only documented way past the refusal
/// (`--force-fresh-data-dir`) is the one that mints a fresh identity.
// FM-PERSISTENCE-059
// FM-PERSISTENCE-052
#[test]
fn an_install_waiting_to_finish_satisfies_require_existing_data() {
    let tmp = TempDir::new().unwrap();
    seed_staged(&tmp.path().join("staging"), 2, b"restored", "yes");

    let mut cfg = persistence_config(tmp.path(), true);
    cfg.require_existing_data = true;
    let mut recovered = boot_standalone(&cfg)
        .expect("a staged checkpoint is existing data, and needs no override to boot");
    let value = recovered
        .shards
        .iter_mut()
        .filter_map(|(store, _)| store.get(b"restored"))
        .next()
        .expect("the restore must be installed, not discarded");
    assert_eq!(value.as_string().unwrap().as_bytes().as_ref(), b"yes");
    assert!(
        DataDirMarker::path(tmp.path()).exists(),
        "and the directory is marked afterwards, so the next boot is ordinary"
    );
    drop(recovered);

    // The setting still does its job. A download that died mid-flight is not
    // data: an unmounted volume must not be excused by a directory of scratch.
    let half = TempDir::new().unwrap();
    std::fs::create_dir_all(half.path().join("staging")).unwrap();
    std::fs::write(half.path().join("staging/000123.sst"), b"partial").unwrap();
    let mut half_cfg = persistence_config(half.path(), true);
    half_cfg.require_existing_data = true;
    let err = boot_standalone(&half_cfg)
        .err()
        .expect("half a download is not existing data");
    assert_eq!(err.phase, RecoveryPhase::VerifyDataDir);
    assert!(
        err.to_string().contains("require-existing-data"),
        "the refusal must still name the setting that caused it: {err}"
    );
}

/// Identity is established before anything can be renamed into the directory,
/// not after. The install is what used to create the data directory, which is
/// why the stamp ran after it — and that ordering meant every install failure
/// and every crash inside the install left a directory whose database had no
/// name. Re-deriving one is not possible: the id is minted, so a second attempt
/// invents a *different* database.
// FM-PERSISTENCE-059
// FM-PERSISTENCE-049
#[test]
fn the_data_dir_is_stamped_before_the_install_can_touch_it() {
    let tmp = TempDir::new().unwrap();
    let staging = tmp.path().join("staging");

    // A payload that will not install: `CURRENT` names a MANIFEST that never
    // arrived, which is what a truncated copy leaves behind.
    std::fs::create_dir_all(&staging).unwrap();
    std::fs::write(staging.join("CURRENT"), b"MANIFEST-000001\n").unwrap();

    let cfg = persistence_config(tmp.path(), true);
    let err = boot_standalone(&cfg)
        .err()
        .expect("an unusable staged payload fails the boot");
    assert_eq!(err.phase, RecoveryPhase::InstallStagedCheckpoint);

    let minted = marker_of(tmp.path()).database_id;

    // The operator removes the bad payload and restarts. Same directory, same
    // database — the id was on disk before the install was attempted.
    std::fs::remove_dir_all(&staging).unwrap();
    boot_standalone(&cfg).expect("the directory boots once the bad payload is gone");
    assert_eq!(
        marker_of(tmp.path()).database_id,
        minted,
        "a failed install must not cost the directory its identity"
    );
}
