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
    DataDirMarker::mint().stamp(dir).unwrap();
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
/// Marks the directory too: a database FrogDB wrote is a database FrogDB
/// stamped.
fn seed_db(db_dir: &Path, num_shards: usize, key: &[u8], val: &str) {
    let rocks = RocksStore::open(db_dir, num_shards, &RocksConfig::default()).unwrap();
    let value = Value::string(val.to_string());
    let metadata = KeyMetadata::new(val.len());
    rocks.put(0, key, &serialize(&value, &metadata)).unwrap();
    rocks.flush().unwrap();
    drop(rocks);
    mark(db_dir);
}

// FM-PERSISTENCE-027
// FM-PERSISTENCE-029
// FM-PERSISTENCE-041
#[test]
fn fresh_boot_creates_empty_shards() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    let cfg = persistence_config(&db_dir, true);
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
    let cfg = persistence_config(&db_dir, false);
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

    let cfg = persistence_config(&db_dir, true);
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
    let db_dir = tmp.path().join("db");
    // A corrupt functions.fdb must not block startup.
    mark(&db_dir);
    std::fs::write(db_dir.join("functions.fdb"), b"not a valid function dump").unwrap();

    let cfg = persistence_config(&db_dir, true);
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

// FM-PERSISTENCE-028
// FM-PERSISTENCE-038
#[test]
fn standalone_does_not_persist_replication_state() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    let cfg = persistence_config(&db_dir, true);
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
        !db_dir.join(&repl_cfg.state_file).exists(),
        "standalone must not write a replication state file"
    );
}

// FM-PERSISTENCE-038
#[test]
fn primary_loads_and_persists_replication_state() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    let cfg = persistence_config(&db_dir, true);
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
        db_dir.join(&repl_cfg.state_file).exists(),
        "primary creates a replication state file (load_or_create)"
    );
}

// FM-PERSISTENCE-027
// FM-PERSISTENCE-039
#[test]
fn staged_replication_metadata_is_adopted_and_consumed() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    mark(&db_dir);

    // Stage replication metadata (as a replica full sync would, carried into the
    // data dir when the staged checkpoint is installed).
    let staged_id = "a".repeat(40);
    let staged = format!(
        "{{\"replication_id\":\"{}\",\"replication_offset\":4242}}",
        staged_id
    );
    std::fs::write(db_dir.join("replication_metadata.json"), staged).unwrap();

    let cfg = persistence_config(&db_dir, true);
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
    assert!(db_dir.join(&repl_cfg.state_file).exists());
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
    let state_path = db_dir.join(&repl_cfg.state_file);

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
    let cfg = persistence_config(&db_dir, false);
    let cluster_cfg = cluster_config(false);
    let inputs = RecoveryInputs {
        data_dir: &db_dir,
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
        let db_dir = tmp.path().join("db");
        let repl_cfg = replication_config("primary");
        mark(&db_dir);
        let state_path = db_dir.join(&repl_cfg.state_file);
        std::fs::write(&state_path, &contents).unwrap();

        let cfg = persistence_config(&db_dir, true);
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
    let db_dir = tmp.path().join("db");
    mark(&db_dir);
    // A plain file where the Raft store's directory must be: the open fails, and
    // a cluster node must refuse to start rather than fall back to standalone.
    std::fs::write(db_dir.join("raft"), b"not a directory").unwrap();

    let cfg = persistence_config(&db_dir, true);
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
    let db_dir = tmp.path().join("db");
    let cfg = persistence_config(&db_dir, true);
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
        db_dir.join("raft").exists(),
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
    let cfg = persistence_config(&db_dir, true);
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
    let checkpoint_dir = tmp.path().join("checkpoint_ready");

    // Live db has the old value; staged checkpoint has the new value.
    seed_db(&db_dir, 2, b"shared", "old");
    seed_db(&checkpoint_dir, 2, b"shared", "new");

    let cfg = persistence_config(&db_dir, true);
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

// FM-PERSISTENCE-024
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
    let backed_up = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .any(|e| e.file_name().to_string_lossy().starts_with("db_backup_"));
    assert!(!backed_up, "live db must not be backed up on refusal");
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
    mark(db_dir);
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

    let cfg = persistence_config(&db_dir, true);
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
        msg.contains("2 key(s)") && msg.contains(&db_dir.display().to_string()),
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

    let cfg = persistence_config(&db_dir, true);
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
    mark(&db_dir);
    std::thread::sleep(Duration::from_millis(10));

    let cfg = persistence_config(&db_dir, true);
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
    mark(&db_dir);

    let cfg = persistence_config(&db_dir, true);
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
    mark(&db_dir);

    let cfg = persistence_config(&db_dir, true);
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

    let cfg = persistence_config(&db_dir, true);
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
        msg.contains("1 key(s)") && msg.contains(&db_dir.display().to_string()),
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

    let cfg = persistence_config(&db_dir, true);
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
    mark(&db_dir);

    let cfg = persistence_config(&db_dir, true);
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

    let cfg = persistence_config(&db_dir, true);
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
    mark(&db_dir);

    let cfg = persistence_config(&db_dir, true);
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

        let cfg = persistence_config(&db_dir, true);
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
    let db_dir = tmp.path().join("db");
    mark(&db_dir);

    let code = "#!lua name=greetlib\nredis.register_function('hi', function() return 'hi' end)";
    let mut registry = frogdb_core::FunctionRegistry::new();
    registry
        .load_library(
            frogdb_core::FunctionLibrary::new("greetlib".to_string(), code.to_string()),
            false,
        )
        .expect("library loads into a fresh registry");
    frogdb_core::save_to_file(&registry, &db_dir.join("functions.fdb"))
        .expect("functions.fdb written");

    let cfg = persistence_config(&db_dir, true);
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
}

// ---------------------------------------------------------------------------
// Phase 0 — is this directory FrogDB's? (FM-PERSISTENCE-048..052)
// ---------------------------------------------------------------------------

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

    let cfg = persistence_config(&db_dir, true);
    let err = boot_standalone(&cfg)
        .err()
        .expect("a populated directory with no marker must not boot");
    assert_eq!(err.phase, RecoveryPhase::VerifyDataDir);

    let msg = err.to_string();
    let absolute = std::path::absolute(&db_dir).unwrap();
    assert!(
        msg.contains(&absolute.display().to_string()),
        "the refusal must name the resolved absolute path: {msg}"
    );
    assert!(
        msg.contains(MARKER_FILE_NAME),
        "the refusal must name the marker it looked for: {msg}"
    );
    assert!(
        msg.contains("--force-fresh-data-dir"),
        "the refusal must name the override: {msg}"
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
        !DataDirMarker::path(&db_dir).exists(),
        "a refused boot must not stamp the directory it refused"
    );
}

/// The other half of the guard: a directory FrogDB may have comes up silently
/// and is stamped, and the stamp is what makes the *next* boot silent too. If
/// this half were wrong, every restart would need the override.
// FM-PERSISTENCE-048
#[test]
fn a_fresh_data_dir_boots_and_stamps_the_marker() {
    let tmp = TempDir::new().unwrap();
    // Not created: a first boot's data dir does not necessarily exist yet.
    let db_dir = tmp.path().join("db");
    let cfg = persistence_config(&db_dir, true);

    boot_standalone(&cfg).expect("a genuinely fresh directory is a first boot, not a refusal");

    let first = marker_of(&db_dir);
    assert_eq!(first.layout_version, DATA_DIR_LAYOUT_VERSION);
    assert_eq!(first.database_id.len(), 32);

    boot_standalone(&cfg).expect("the marker this boot stamped must let the next one in");
    assert_eq!(
        marker_of(&db_dir).database_id,
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

    let cfg = persistence_config(&db_dir, true);
    boot_standalone(&cfg).expect("empty subdirectories are not evidence of a wrong directory");
    assert!(
        DataDirMarker::path(&db_dir).exists(),
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
    std::fs::write(DataDirMarker::path(&db_dir), b"{ truncated mid-writ").unwrap();

    let cfg = persistence_config(&db_dir, true);
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
    let db_dir = tmp.path().join("db");
    let mut cfg = persistence_config(&db_dir, true);
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
        !DataDirMarker::path(&db_dir).exists(),
        "a refused boot must not stamp"
    );

    // The setting is about mount failures, not about refusing every boot: the
    // same node with its data present comes up.
    mark(&db_dir);
    boot_standalone(&cfg).expect("a marked directory satisfies require-existing-data");
}

/// The override adopts, it does not wipe. A database written before markers
/// existed (or restored by hand) has to have a way in that keeps its data.
// FM-PERSISTENCE-051
#[test]
fn force_fresh_data_dir_adopts_an_unmarked_directory() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    seed_db(&db_dir, 2, b"greeting", "hello");
    std::fs::remove_file(DataDirMarker::path(&db_dir)).unwrap();

    let mut cfg = persistence_config(&db_dir, true);
    cfg.force_fresh_data_dir = true;
    let mut recovered = boot_standalone(&cfg).expect("the override adopts an unmarked directory");

    let value = recovered
        .shards
        .iter_mut()
        .filter_map(|(store, _)| store.get(b"greeting"))
        .next()
        .expect("adopting must recover the data normally, not start empty");
    assert_eq!(value.as_string().unwrap().as_bytes().as_ref(), b"hello");

    // One boot with the flag is enough: the directory is marked now. The first
    // boot's RocksDB has to go first — it holds the directory's LOCK, and the
    // second boot is standing in for the next process, not a second one.
    drop(recovered);

    let adopted = marker_of(&db_dir).database_id;
    cfg.force_fresh_data_dir = false;
    boot_standalone(&cfg).expect("an adopted directory boots on its own afterwards");
    assert_eq!(marker_of(&db_dir).database_id, adopted);
}

/// The override covers the unreadable-marker refusal too, by replacing the
/// marker rather than leaving the directory permanently unbootable.
// FM-PERSISTENCE-050
#[test]
fn force_fresh_data_dir_re_stamps_a_corrupt_marker() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    seed_db(&db_dir, 2, b"greeting", "hello");
    std::fs::write(DataDirMarker::path(&db_dir), b"{ truncated mid-writ").unwrap();

    let mut cfg = persistence_config(&db_dir, true);
    cfg.force_fresh_data_dir = true;
    boot_standalone(&cfg).expect("the override re-stamps an unreadable marker");

    // Readable again — which is the whole point: the operator is not left with a
    // directory that needs the flag on every boot forever.
    cfg.force_fresh_data_dir = false;
    boot_standalone(&cfg).expect("the re-stamped directory boots on its own");
    assert_eq!(marker_of(&db_dir).layout_version, DATA_DIR_LAYOUT_VERSION);
}

/// `require-existing-data` has to have the same escape hatch, or provisioning a
/// new node into a deployment that sets it would be impossible.
// FM-PERSISTENCE-052
#[test]
fn force_fresh_data_dir_overrides_require_existing_data() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    let mut cfg = persistence_config(&db_dir, true);
    cfg.require_existing_data = true;
    cfg.force_fresh_data_dir = true;

    boot_standalone(&cfg).expect("the override is how a require-existing-data node is provisioned");
    assert!(DataDirMarker::path(&db_dir).exists());
}

/// The ordering that makes the guard worth having on a replica: a refusal that
/// arrives *after* a staged full sync has been installed has not protected
/// anything — it has replaced the operator's data with the primary's, which is a
/// different failure with the same lost bytes.
// FM-PERSISTENCE-051
#[test]
fn the_data_dir_guard_runs_before_a_staged_checkpoint_can_install() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    let checkpoint_dir = tmp.path().join("checkpoint_ready");

    // The wrong directory: somebody else's file, no marker.
    std::fs::create_dir_all(&db_dir).unwrap();
    std::fs::write(db_dir.join("important.txt"), b"somebody elses bytes").unwrap();
    // A full-sync checkpoint from the primary, staged and ready to install.
    seed_db(&checkpoint_dir, 2, b"from-primary", "new");

    let cfg = persistence_config(&db_dir, true);
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
    let backed_up = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .any(|e| e.file_name().to_string_lossy().starts_with("db_backup_"));
    assert!(
        !backed_up,
        "nothing was moved aside for an install that never ran"
    );
}

/// Installing a checkpoint renames the whole data directory away, marker and
/// all, and the staged directory is a RocksDB checkpoint that carries none. The
/// marker is therefore rewritten after the install — with the *same* id, because
/// it names the directory rather than its contents — so a full resync does not
/// leave the next boot refusing the database this one just installed.
// FM-PERSISTENCE-049
#[test]
fn an_installed_checkpoint_leaves_the_data_dir_marked() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("db");
    let checkpoint_dir = tmp.path().join("checkpoint_ready");

    seed_db(&db_dir, 2, b"shared", "old");
    seed_db(&checkpoint_dir, 2, b"shared", "new");
    // A real staged checkpoint is a RocksDB checkpoint of the primary's db and
    // has no marker of its own.
    std::fs::remove_file(DataDirMarker::path(&checkpoint_dir)).unwrap();
    let before = marker_of(&db_dir).database_id;

    let cfg = persistence_config(&db_dir, true);
    let recovered = boot_standalone(&cfg).expect("the staged checkpoint installs");
    assert!(recovered.installed_staged_checkpoint);
    // Release the installed database's LOCK: the boot below stands in for the
    // next process, not for a second one racing this test.
    drop(recovered);

    assert_eq!(
        marker_of(&db_dir).database_id,
        before,
        "the directory's identity survives having its contents replaced"
    );
    boot_standalone(&cfg).expect("the boot after a full resync must not refuse");
}
