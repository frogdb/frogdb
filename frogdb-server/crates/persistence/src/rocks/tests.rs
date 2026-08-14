use super::*;
use rocksdb::{DBCompressionType, WriteBatch};
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;
// FM-PERSISTENCE-029
#[test]
fn test_open_and_write() {
    let t = TempDir::new().unwrap();
    let s = RocksStore::open(t.path(), 4, &RocksConfig::default()).unwrap();
    s.put(0, b"k1", b"v1").unwrap();
    assert_eq!(s.get(0, b"k1").unwrap(), Some(b"v1".to_vec()));
    s.put(3, b"k2", b"v2").unwrap();
    assert_eq!(s.get(3, b"k2").unwrap(), Some(b"v2".to_vec()));
    assert_eq!(s.get(1, b"k1").unwrap(), None);
}
#[test]
fn test_delete() {
    let t = TempDir::new().unwrap();
    let s = RocksStore::open(t.path(), 2, &RocksConfig::default()).unwrap();
    s.put(0, b"k", b"v").unwrap();
    assert!(s.get(0, b"k").unwrap().is_some());
    s.delete(0, b"k").unwrap();
    assert!(s.get(0, b"k").unwrap().is_none());
}
#[test]
fn test_write_batch() {
    let t = TempDir::new().unwrap();
    let s = RocksStore::open(t.path(), 2, &RocksConfig::default()).unwrap();
    let mut b = WriteBatch::default();
    s.batch_put(&mut b, 0, b"k1", b"v1").unwrap();
    s.batch_put(&mut b, 0, b"k2", b"v2").unwrap();
    s.batch_put(&mut b, 1, b"k3", b"v3").unwrap();
    s.write_batch(b).unwrap();
    assert_eq!(s.get(0, b"k1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(s.get(0, b"k2").unwrap(), Some(b"v2".to_vec()));
    assert_eq!(s.get(1, b"k3").unwrap(), Some(b"v3".to_vec()));
}
#[test]
fn test_iterate() {
    let t = TempDir::new().unwrap();
    let s = RocksStore::open(t.path(), 2, &RocksConfig::default()).unwrap();
    s.put(0, b"a", b"1").unwrap();
    s.put(0, b"b", b"2").unwrap();
    s.put(0, b"c", b"3").unwrap();
    assert_eq!(s.iter_cf(0).unwrap().count(), 3);
}
// FM-PERSISTENCE-029
#[test]
fn test_has_data() {
    let t = TempDir::new().unwrap();
    let s = RocksStore::open(t.path(), 2, &RocksConfig::default()).unwrap();
    assert!(!s.has_data());
    s.put(0, b"k", b"v").unwrap();
    assert!(s.has_data());
}
// FM-PERSISTENCE-029
#[test]
fn test_reopen() {
    let t = TempDir::new().unwrap();
    {
        RocksStore::open(t.path(), 2, &RocksConfig::default())
            .unwrap()
            .put(0, b"p", b"d")
            .unwrap();
    }
    assert_eq!(
        RocksStore::open(t.path(), 2, &RocksConfig::default())
            .unwrap()
            .get(0, b"p")
            .unwrap(),
        Some(b"d".to_vec())
    );
}
// FM-PERSISTENCE-014
#[test]
fn hll_merge_operand_folds_and_survives_reopen() {
    use crate::serialization::{deserialize, serialize, serialize_hll_delta};
    use frogdb_types::hyperloglog::HyperLogLogValue;
    use frogdb_types::types::{KeyMetadata, Value};

    let dir = TempDir::new().unwrap();
    let meta = KeyMetadata::new(1);
    let mut reference = HyperLogLogValue::new();
    for i in 0..10u32 {
        reference.add(&i.to_le_bytes());
    }
    let base = serialize(&Value::HyperLogLog(reference.clone()), &meta);
    let mut pairs = Vec::new();
    for i in 10..50u32 {
        if let Some(p) = reference.add_tracked(&i.to_le_bytes()) {
            pairs.push(p);
        }
    }
    let operand = serialize_hll_delta(&pairs, &meta);
    {
        let rocks = RocksStore::open(dir.path(), 1, &RocksConfig::default()).unwrap();
        rocks.put(0, b"hll", &base).unwrap();
        rocks.merge(0, b"hll", &operand).unwrap();
        let got = rocks.get(0, b"hll").unwrap().unwrap();
        let (value, _) = deserialize(&got).unwrap();
        let Value::HyperLogLog(h) = value else {
            panic!("wrong type")
        };
        assert_eq!(
            h.count_no_cache(),
            reference.count_no_cache(),
            "read-time merge"
        );
    }
    // Reopen: pending operands (or compacted state) still read merged.
    let rocks = RocksStore::open(dir.path(), 1, &RocksConfig::default()).unwrap();
    let got = rocks.get(0, b"hll").unwrap().unwrap();
    let (value, _) = deserialize(&got).unwrap();
    let Value::HyperLogLog(h) = value else {
        panic!("wrong type")
    };
    assert_eq!(h.count_no_cache(), reference.count_no_cache());
}

// FM-PERSISTENCE-014
#[test]
fn hll_batch_merge_folds_operand() {
    use crate::serialization::{deserialize, serialize, serialize_hll_delta};
    use frogdb_types::hyperloglog::HyperLogLogValue;
    use frogdb_types::types::{KeyMetadata, Value};

    let dir = TempDir::new().unwrap();
    let meta = KeyMetadata::new(1);
    let mut reference = HyperLogLogValue::new();
    for i in 0..10u32 {
        reference.add(&i.to_le_bytes());
    }
    let base = serialize(&Value::HyperLogLog(reference.clone()), &meta);
    let mut pairs = Vec::new();
    for i in 10..60u32 {
        if let Some(p) = reference.add_tracked(&i.to_le_bytes()) {
            pairs.push(p);
        }
    }
    let operand = serialize_hll_delta(&pairs, &meta);
    let rocks = RocksStore::open(dir.path(), 1, &RocksConfig::default()).unwrap();
    rocks.put(0, b"hll", &base).unwrap();
    let mut batch = WriteBatch::default();
    rocks.batch_merge(&mut batch, 0, b"hll", &operand).unwrap();
    rocks.write_batch(batch).unwrap();
    let got = rocks.get(0, b"hll").unwrap().unwrap();
    let (value, _) = deserialize(&got).unwrap();
    let Value::HyperLogLog(h) = value else {
        panic!("wrong type")
    };
    assert_eq!(h.count_no_cache(), reference.count_no_cache());
}

// FM-PERSISTENCE-030
#[test]
fn test_invalid_shard() {
    let t = TempDir::new().unwrap();
    let s = RocksStore::open(t.path(), 2, &RocksConfig::default()).unwrap();
    assert!(matches!(
        s.put(5, b"k", b"v"),
        Err(RocksError::InvalidShardId(5))
    ));
}
// FM-PERSISTENCE-031
#[test]
fn test_warm_cf_disabled() {
    let t = TempDir::new().unwrap();
    let s = RocksStore::open(t.path(), 2, &RocksConfig::default()).unwrap();
    assert!(!s.warm_enabled());
    assert!(s.put_warm(0, b"k", b"v").is_err());
}
#[test]
fn test_warm_cf_ops() {
    let t = TempDir::new().unwrap();
    let s = RocksStore::open_with_warm(t.path(), 2, &RocksConfig::default(), true).unwrap();
    assert!(s.warm_enabled());
    s.put_warm(0, b"k1", b"v1").unwrap();
    assert_eq!(s.get_warm(0, b"k1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(s.get_warm(1, b"k1").unwrap(), None);
    s.delete_warm(0, b"k1").unwrap();
    assert_eq!(s.get_warm(0, b"k1").unwrap(), None);
}
#[test]
fn test_warm_cf_iter() {
    let t = TempDir::new().unwrap();
    let s = RocksStore::open_with_warm(t.path(), 2, &RocksConfig::default(), true).unwrap();
    s.put_warm(0, b"a", b"1").unwrap();
    s.put_warm(0, b"b", b"2").unwrap();
    s.put_warm(0, b"c", b"3").unwrap();
    assert_eq!(s.iter_warm_cf(0).unwrap().count(), 3);
    assert_eq!(s.iter_warm_cf(1).unwrap().count(), 0);
}
// FM-PERSISTENCE-031
#[test]
fn test_warm_cf_reopen() {
    let t = TempDir::new().unwrap();
    {
        RocksStore::open_with_warm(t.path(), 2, &RocksConfig::default(), true)
            .unwrap()
            .put_warm(0, b"p", b"d")
            .unwrap();
    }
    assert_eq!(
        RocksStore::open_with_warm(t.path(), 2, &RocksConfig::default(), true)
            .unwrap()
            .get_warm(0, b"p")
            .unwrap(),
        Some(b"d".to_vec())
    );
}
// FM-PERSISTENCE-030
#[test]
fn test_warm_cf_invalid_shard() {
    let t = TempDir::new().unwrap();
    let s = RocksStore::open_with_warm(t.path(), 2, &RocksConfig::default(), true).unwrap();
    assert!(matches!(
        s.put_warm(5, b"k", b"v"),
        Err(RocksError::InvalidShardId(5))
    ));
}

// FM-PERSISTENCE-031
/// A data directory written with the warm tier enabled cannot reopen with it
/// disabled: the persisted `tiered_warm_*` column families would be left
/// unopened and RocksDB would reject the whole DB with a cryptic "column
/// families not opened" error. Guard it with a clear `WarmTierMismatch`. This is
/// the failing case that shipped untested before this proposal.
#[test]
fn test_warm_toggle_on_then_off_fails() {
    let t = TempDir::new().unwrap();
    {
        let s = RocksStore::open_with_warm(t.path(), 2, &RocksConfig::default(), true).unwrap();
        s.put_warm(0, b"k", b"v").unwrap();
    }
    match RocksStore::open_with_warm(t.path(), 2, &RocksConfig::default(), false) {
        Err(RocksError::WarmTierMismatch { path }) => {
            assert!(path.contains(&t.path().display().to_string()));
        }
        Ok(_) => panic!("expected WarmTierMismatch error, got Ok"),
        Err(other) => panic!("expected WarmTierMismatch, got {other}"),
    }
}

// FM-PERSISTENCE-031
/// Enabling the warm tier on a directory that never had it is a legitimate
/// first-enable: the warm CFs are created fresh and empty, the open succeeds,
/// warm ops work, and the pre-existing hot data is intact. Pins that the guard
/// does not over-rotate and reject this benign off -> on direction.
#[test]
fn test_warm_toggle_off_then_on_succeeds() {
    let t = TempDir::new().unwrap();
    {
        let s = RocksStore::open(t.path(), 2, &RocksConfig::default()).unwrap();
        s.put(0, b"hot", b"data").unwrap();
    }
    let s = RocksStore::open_with_warm(t.path(), 2, &RocksConfig::default(), true).unwrap();
    assert!(s.warm_enabled());
    // Pre-existing hot data survives the warm-enabling reopen.
    assert_eq!(s.get(0, b"hot").unwrap(), Some(b"data".to_vec()));
    // Warm ops now work against the freshly-created warm CFs.
    s.put_warm(0, b"w", b"v").unwrap();
    assert_eq!(s.get_warm(0, b"w").unwrap(), Some(b"v".to_vec()));
}

// FM-PERSISTENCE-030
/// Growing the shard count (4 → 8) must fail loudly. Without the guard this
/// silently "succeeds" but misroutes every key under the new hash space.
#[test]
fn test_reopen_with_more_shards_fails() {
    let t = TempDir::new().unwrap();
    {
        let s = RocksStore::open(t.path(), 4, &RocksConfig::default()).unwrap();
        s.put(0, b"k", b"v").unwrap();
    }
    match RocksStore::open(t.path(), 8, &RocksConfig::default()) {
        Err(RocksError::ShardCountMismatch {
            persisted,
            configured,
            path,
        }) => {
            assert_eq!(persisted, 4);
            assert_eq!(configured, 8);
            assert!(path.contains(&t.path().display().to_string()));
        }
        Ok(_) => panic!("expected ShardCountMismatch error, got Ok"),
        Err(other) => panic!("expected ShardCountMismatch, got {other}"),
    }
}

// FM-PERSISTENCE-030
/// Shrinking the shard count (8 → 2) must also fail loudly with our clear error
/// rather than RocksDB's cryptic "column families not opened".
#[test]
fn test_reopen_with_fewer_shards_fails() {
    let t = TempDir::new().unwrap();
    {
        let s = RocksStore::open(t.path(), 8, &RocksConfig::default()).unwrap();
        s.put(0, b"k", b"v").unwrap();
    }
    assert!(matches!(
        RocksStore::open(t.path(), 2, &RocksConfig::default()),
        Err(RocksError::ShardCountMismatch {
            persisted: 8,
            configured: 2,
            ..
        })
    ));
}

// FM-PERSISTENCE-030
/// Reopening with the matching shard count still succeeds with data intact.
#[test]
fn test_reopen_with_matching_shards_succeeds() {
    let t = TempDir::new().unwrap();
    {
        let s = RocksStore::open(t.path(), 4, &RocksConfig::default()).unwrap();
        s.put(3, b"k", b"v").unwrap();
    }
    let s = RocksStore::open(t.path(), 4, &RocksConfig::default()).unwrap();
    assert_eq!(s.num_shards(), 4);
    assert_eq!(s.get(3, b"k").unwrap(), Some(b"v".to_vec()));
}

// FM-PERSISTENCE-030
/// The warm-tier and search-meta column families must not be miscounted as data
/// shards. A warm-enabled store has 3 column families per shard, so without the
/// `shard_<n>`-only filter the persisted count would be inflated and a matching
/// reopen would be wrongly rejected.
#[test]
fn test_shard_count_validation_ignores_warm_cfs() {
    let t = TempDir::new().unwrap();
    {
        let s = RocksStore::open_with_warm(t.path(), 4, &RocksConfig::default(), true).unwrap();
        s.put(0, b"k", b"v").unwrap();
    }
    // Reopen warm-enabled with the same shard count: succeeds, data intact.
    let s = RocksStore::open_with_warm(t.path(), 4, &RocksConfig::default(), true).unwrap();
    assert_eq!(s.get(0, b"k").unwrap(), Some(b"v".to_vec()));
    // Reopen with a different shard count: rejected on the data-shard count alone.
    assert!(matches!(
        RocksStore::open_with_warm(t.path(), 2, &RocksConfig::default(), true),
        Err(RocksError::ShardCountMismatch {
            persisted: 4,
            configured: 2,
            ..
        })
    ));
}

// FM-PERSISTENCE-032
/// A failing column-family enumeration on an *existing* database must abort the
/// open, not be swallowed into an empty CF list. Swallowing it coerces the
/// reopen onto the fresh-open path, which silently skips BOTH reopen guards (the
/// shard-count and warm-tier invariants both trust `existing_cfs`) before
/// failing confusingly deep in RocksDB. This drives that branch via an injected
/// lister that fails, and asserts the enumeration error propagates verbatim and
/// leaves the on-disk data untouched. (Regression test for ff24a1a4.)
#[test]
fn test_cf_enumeration_failure_propagates_and_preserves_data() {
    let t = TempDir::new().unwrap();
    // An existing database with real data across two shards.
    {
        let s = RocksStore::open(t.path(), 2, &RocksConfig::default()).unwrap();
        s.put(0, b"k0", b"v0").unwrap();
        s.put(1, b"k1", b"v1").unwrap();
    }

    // Force CF enumeration to fail with a distinctive sentinel error. If the
    // error were swallowed into an empty CF list the open would instead surface
    // some other (downstream) RocksDB error or a wrongly-fresh success — either
    // way NOT this sentinel — so the assertion below is a genuine detector.
    let sentinel = "forced-enumeration-failure";
    let result = RocksStore::open_with_cf_lister(
        t.path(),
        2,
        &RocksConfig::default(),
        false,
        Arc::new(frogdb_types::traits::NoopMetricsRecorder),
        |_opts, _path| Err(RocksError::ColumnFamilyNotFound(sentinel.to_string())),
    );
    match result {
        Err(RocksError::ColumnFamilyNotFound(ref s)) if s == sentinel => {}
        Err(other) => panic!("enumeration error must propagate verbatim, got {other:?}"),
        Ok(_) => panic!("a failed CF enumeration on an existing db must abort the open"),
    }

    // The on-disk data is untouched: a normal reopen still reads both shards.
    let s = RocksStore::open(t.path(), 2, &RocksConfig::default()).unwrap();
    assert_eq!(s.get(0, b"k0").unwrap(), Some(b"v0".to_vec()));
    assert_eq!(s.get(1, b"k1").unwrap(), Some(b"v1".to_vec()));
}

// ---------------------------------------------------------------------------
// Staged checkpoint install (`load_staged_checkpoint`)
//
// Lifecycle: a replica full-sync writes a complete RocksDB directory to
// `<parent>/checkpoint_ready/`, then the next boot installs it by renaming the
// live db aside (`<parent>/<name>_backup_<unix_secs>`) and renaming the staged
// dir into place, then pruning backups beyond `staged::BACKUP_RETENTION`.
// These tests exercise that filesystem surgery directly — they
// construct the on-disk layouts (including crash-window intermediates) rather
// than killing a process, and assert no layout loses data or panics.
// ---------------------------------------------------------------------------

/// Create a complete, single-shard RocksDB directory holding `key -> val`.
fn write_db(path: &Path, key: &[u8], val: &[u8]) {
    let s = RocksStore::open(path, 1, &RocksConfig::default()).unwrap();
    s.put(0, key, val).unwrap();
}

/// Open a single-shard RocksDB directory and read `key` from shard 0.
fn read_db(path: &Path, key: &[u8]) -> Option<Vec<u8>> {
    let s = RocksStore::open(path, 1, &RocksConfig::default()).unwrap();
    s.get(0, key).unwrap()
}

/// All `<base>_backup_*` sibling directories under `parent`, in arbitrary order.
fn backup_dirs(parent: &Path, base: &str) -> Vec<PathBuf> {
    let prefix = format!("{base}_backup_");
    fs::read_dir(parent)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(&prefix))
        })
        .collect()
}

// FM-PERSISTENCE-025
/// No `checkpoint_ready` marker → nothing to install; the live db is untouched.
#[test]
fn test_load_staged_checkpoint_absent_marker_is_noop() {
    let t = TempDir::new().unwrap();
    let data = t.path().join("data");
    write_db(&data, b"k", b"v");

    assert!(!RocksStore::load_staged_checkpoint(&data).unwrap());
    assert_eq!(read_db(&data, b"k"), Some(b"v".to_vec()));
    assert!(backup_dirs(t.path(), "data").is_empty());
}

// FM-PERSISTENCE-025
/// A path with no parent (the staging area is a sibling of the db dir) can hold
/// no staged checkpoint: return `Ok(false)` rather than erroring.
#[test]
fn test_load_staged_checkpoint_no_parent_is_noop() {
    assert!(!RocksStore::load_staged_checkpoint(Path::new("")).unwrap());
}

// FM-PERSISTENCE-025
/// Happy path: a complete staged checkpoint wins, the previous live db is moved
/// aside into a `*_backup_*` dir (recoverable, not deleted), and the staging
/// marker is consumed.
#[test]
fn test_load_staged_checkpoint_installs_and_backs_up_old_db() {
    let t = TempDir::new().unwrap();
    let parent = t.path();
    let data = parent.join("data");
    let crd = parent.join("checkpoint_ready");
    write_db(&data, b"k", b"old");
    write_db(&crd, b"k", b"new");

    assert!(RocksStore::load_staged_checkpoint(&data).unwrap());

    // Checkpoint data is now live; staging marker consumed.
    assert_eq!(read_db(&data, b"k"), Some(b"new".to_vec()));
    assert!(
        !crd.exists(),
        "checkpoint_ready must be consumed after install"
    );

    // The previous live db survives in exactly one backup, fully readable.
    let backups = backup_dirs(parent, "data");
    assert_eq!(backups.len(), 1, "old db should be backed up once");
    assert_eq!(read_db(&backups[0], b"k"), Some(b"old".to_vec()));
}

// FM-PERSISTENCE-023
/// The install's crash-window reasoning ("crash between rename 1 and rename 2 is
/// recoverable") only holds if each rename is durable when the next one runs, so
/// the data dir's parent is fsynced after both. Asserted through the `SnapshotFs`
/// seam: an fsync reaching the platter is not observable from a unit test, but
/// the publisher issuing it in the right place is.
#[test]
fn staged_checkpoint_install_fsyncs_the_data_dir_parent() {
    let t = TempDir::new().unwrap();
    let parent = t.path();
    let data = parent.join("data");
    let crd = parent.join("checkpoint_ready");
    write_db(&data, b"k", b"old");
    write_db(&crd, b"k", b"new");
    let fs = crate::fs_seam::RecordingFs::new();

    assert!(RocksStore::load_staged_checkpoint_with(&data, &fs).unwrap());

    let trace = fs.trace(parent);
    assert_eq!(trace.len(), 4, "two renames, each with its sync: {trace:?}");
    assert!(
        trace[0].starts_with("rename data -> data_backup_"),
        "first the live db moves aside: {trace:?}"
    );
    assert_eq!(
        trace[1], "sync_dir .",
        "the backup rename must be durable \
         before the install rename can consume checkpoint_ready: {trace:?}"
    );
    assert_eq!(trace[2], "rename checkpoint_ready -> data");
    assert_eq!(
        trace[3], "sync_dir .",
        "the install rename is the commit point and must be durable: {trace:?}"
    );
}

// FM-PERSISTENCE-025
/// First full sync onto a node with no existing db: install with no backup.
#[test]
fn test_load_staged_checkpoint_first_sync_no_existing_db() {
    let t = TempDir::new().unwrap();
    let parent = t.path();
    let data = parent.join("data");
    let crd = parent.join("checkpoint_ready");
    write_db(&crd, b"k", b"fresh");

    assert!(RocksStore::load_staged_checkpoint(&data).unwrap());
    assert_eq!(read_db(&data, b"k"), Some(b"fresh".to_vec()));
    assert!(
        backup_dirs(parent, "data").is_empty(),
        "no live db existed, so no backup should be created"
    );
}

// FM-PERSISTENCE-024
/// A partially-staged checkpoint (no RocksDB `CURRENT` manifest) must be
/// refused with a clear error, leaving the original live db untouched. Without
/// the guard the live db is renamed aside and a fresh empty db opens in its
/// place — silent data loss. (Regression test for that bug.)
#[test]
fn test_load_staged_checkpoint_incomplete_dir_refuses_and_preserves_data() {
    let t = TempDir::new().unwrap();
    let parent = t.path();
    let data = parent.join("data");
    let crd = parent.join("checkpoint_ready");
    write_db(&data, b"k", b"keep");
    // A staged dir that is *not* a RocksDB database (no CURRENT manifest).
    fs::create_dir_all(&crd).unwrap();
    fs::write(crd.join("stray.txt"), b"not a database").unwrap();

    let err = RocksStore::load_staged_checkpoint(&data)
        .expect_err("install must refuse an incomplete staged checkpoint");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);

    // Original data is untouched: still live, not moved to a backup.
    assert_eq!(read_db(&data, b"k"), Some(b"keep".to_vec()));
    assert!(
        backup_dirs(parent, "data").is_empty(),
        "live db must not be moved aside when the staged dir is incomplete"
    );
    assert!(
        crd.exists(),
        "incomplete staged dir should be left for inspection"
    );
}

// FM-PERSISTENCE-025
/// Crash window: the install renamed the live db to `*_backup_*` but crashed
/// *before* renaming the staged dir into place. On reboot the on-disk layout is
/// {no live db, `checkpoint_ready` present, leftover backup present}. Recovery
/// must finish the install cleanly and the prior data must survive in the
/// leftover backup — no data loss in this window.
#[test]
fn test_load_staged_checkpoint_crash_after_backup_recovers() {
    let t = TempDir::new().unwrap();
    let parent = t.path();
    let data = parent.join("data");
    let crd = parent.join("checkpoint_ready");
    // Live db already renamed aside by the interrupted install.
    let leftover_backup = parent.join("data_backup_111");
    write_db(&leftover_backup, b"k", b"old");
    write_db(&crd, b"k", b"new");
    assert!(
        !data.exists(),
        "precondition: live db was already moved aside"
    );

    assert!(RocksStore::load_staged_checkpoint(&data).unwrap());

    // Staged checkpoint is now installed; the interrupted backup is untouched.
    assert_eq!(read_db(&data, b"k"), Some(b"new".to_vec()));
    assert!(!crd.exists(), "checkpoint_ready must be consumed");
    assert_eq!(
        read_db(&leftover_backup, b"k"),
        Some(b"old".to_vec()),
        "the pre-existing backup from the interrupted install must survive"
    );
}

// FM-PERSISTENCE-025
/// Crash window: the install completed (staged dir renamed into place) but the
/// process died before anything else. On reboot `checkpoint_ready` is gone, so
/// install is a no-op and re-running it is idempotent — the freshly installed
/// data stays intact and no spurious backup is produced.
#[test]
fn test_load_staged_checkpoint_idempotent_after_success() {
    let t = TempDir::new().unwrap();
    let parent = t.path();
    let data = parent.join("data");
    let crd = parent.join("checkpoint_ready");
    write_db(&data, b"k", b"old");
    write_db(&crd, b"k", b"new");

    assert!(RocksStore::load_staged_checkpoint(&data).unwrap());
    let backups_after_first = backup_dirs(parent, "data").len();

    // Second boot: nothing staged, so this is a no-op that preserves the data.
    assert!(!RocksStore::load_staged_checkpoint(&data).unwrap());
    assert_eq!(read_db(&data, b"k"), Some(b"new".to_vec()));
    assert_eq!(
        backup_dirs(parent, "data").len(),
        backups_after_first,
        "a no-op install must not create another backup"
    );
}

// FM-PERSISTENCE-026
/// A stale `*_backup_*` dir left by an earlier crash must not block a new
/// install: the new backup gets a distinct timestamped name and the install
/// succeeds. Retention (keep the newest `BACKUP_RETENTION = 1`) then prunes
/// the stale backup, so exactly one backup — the just-displaced live db —
/// survives. (Before retention existed, every full sync leaked a complete
/// database copy.)
#[test]
fn test_load_staged_checkpoint_prunes_older_backups() {
    let t = TempDir::new().unwrap();
    let parent = t.path();
    let data = parent.join("data");
    let crd = parent.join("checkpoint_ready");
    let stale_backup = parent.join("data_backup_111");
    write_db(&stale_backup, b"k", b"ancient");
    write_db(&data, b"k", b"current");
    write_db(&crd, b"k", b"staged");

    assert!(RocksStore::load_staged_checkpoint(&data).unwrap());

    // Install succeeded; retention kept only the newest backup, which holds
    // the just-displaced live db.
    assert_eq!(read_db(&data, b"k"), Some(b"staged".to_vec()));
    let backups = backup_dirs(parent, "data");
    assert_eq!(
        backups.len(),
        1,
        "retention must keep exactly the newest backup"
    );
    assert_eq!(
        read_db(&backups[0], b"k"),
        Some(b"current".to_vec()),
        "the surviving backup must be the just-displaced live db"
    );
    assert!(!stale_backup.exists(), "the stale backup must be pruned");
}

// FM-PERSISTENCE-026
/// Retention when the crash-after-backup window recovers: the only backup is
/// the leftover from the interrupted install (no new backup is created since
/// there is no live db), so retention keeps it — the previous data survives.
#[test]
fn test_load_staged_checkpoint_crash_recovery_keeps_lone_backup() {
    let t = TempDir::new().unwrap();
    let parent = t.path();
    let data = parent.join("data");
    let crd = parent.join("checkpoint_ready");
    let leftover_backup = parent.join("data_backup_111");
    write_db(&leftover_backup, b"k", b"old");
    write_db(&crd, b"k", b"new");

    assert!(RocksStore::load_staged_checkpoint(&data).unwrap());

    assert_eq!(read_db(&data, b"k"), Some(b"new".to_vec()));
    assert_eq!(backup_dirs(parent, "data").len(), 1);
    assert_eq!(
        read_db(&leftover_backup, b"k"),
        Some(b"old".to_vec()),
        "the lone leftover backup is the newest and must survive retention"
    );
}

// FM-PERSISTENCE-026
/// `prune_backups` picks "newest" by the numeric timestamp suffix — string
/// order would rank `_2` above `_10` and delete the wrong directory.
#[test]
fn test_prune_backups_orders_numerically_not_lexically() {
    let t = TempDir::new().unwrap();
    let parent = t.path();
    fs::create_dir_all(parent.join("data_backup_2")).unwrap();
    fs::create_dir_all(parent.join("data_backup_10")).unwrap();

    let removed = crate::rocks::staged::prune_backups(parent, "data", 1).unwrap();

    assert_eq!(removed, 1);
    assert!(
        parent.join("data_backup_10").exists(),
        "numerically newest (10) must be kept"
    );
    assert!(
        !parent.join("data_backup_2").exists(),
        "numerically older (2) must be pruned"
    );
}

// FM-PERSISTENCE-026
/// `prune_backups` with at most `keep` backups is a no-op; files that merely
/// share the backup prefix are ignored (only directories are backups).
#[test]
fn test_prune_backups_noop_within_retention() {
    let t = TempDir::new().unwrap();
    let parent = t.path();
    fs::create_dir_all(parent.join("data_backup_5")).unwrap();
    fs::write(parent.join("data_backup_9"), b"a stray file, not a backup").unwrap();

    let removed = crate::rocks::staged::prune_backups(parent, "data", 1).unwrap();

    assert_eq!(removed, 0);
    assert!(parent.join("data_backup_5").exists());
    assert!(parent.join("data_backup_9").exists());
}

// ============================================================================
// Post-clear space reclamation (proposal 48)
// ============================================================================

/// Counting [`frogdb_types::traits::MetricsRecorder`] so reclamation tests can
/// assert the started/completed counters without a real metrics backend.
#[derive(Default)]
struct CountingRecorder {
    counters: Mutex<std::collections::HashMap<String, u64>>,
}

impl frogdb_types::traits::MetricsRecorder for CountingRecorder {
    fn increment_counter(&self, name: &str, value: u64, _labels: &[(&str, &str)]) {
        *self
            .counters
            .lock()
            .unwrap()
            .entry(name.to_string())
            .or_insert(0) += value;
    }
    fn record_gauge(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}
    fn record_histogram(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}
    fn counter_value(&self, name: &str) -> Option<u64> {
        self.counters.lock().unwrap().get(name).copied()
    }
}

/// Total live SST bytes for a shard's primary CF (excludes memtables and the
/// DB's own WAL, so it is a precise "on-disk table bytes" measure).
fn sst_bytes(s: &RocksStore, sid: usize) -> u64 {
    let cf = s.cf_handle(sid).unwrap();
    s.db.property_int_value_cf(&cf, "rocksdb.total-sst-files-size")
        .unwrap()
        .unwrap_or(0)
}

/// Commit a full-shard range tombstone the way the WAL flush pipeline does
/// (batch_clear_shard + write_batch) and return the tombstone's upper bound.
fn commit_clear(s: &RocksStore, sid: usize) -> Option<Vec<u8>> {
    let mut batch = WriteBatch::default();
    let upper = s.batch_clear_shard(&mut batch, sid).unwrap();
    s.write_batch(batch).unwrap();
    upper
}

/// Block until no reclamation pass is in flight (async spawn path).
fn wait_reclaim_idle(s: &RocksStore) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while s.reclaim_guard.in_flight_count() > 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "reclamation did not finish within 60s"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

use frogdb_types::traits::MetricsRecorder as _;
use std::sync::Mutex;

// FM-PERSISTENCE-012
/// Proposal 48 test 1 (functional): after a clear plus the full reclamation
/// pass (DeleteFilesInRange + forced bottommost CompactRange), the data is
/// still gone across a restart — compaction must not resurrect anything —
/// and other shards are untouched.
#[test]
fn clear_reclamation_keeps_data_gone_after_reopen() {
    let t = TempDir::new().unwrap();
    {
        let s = Arc::new(RocksStore::open(t.path(), 2, &RocksConfig::default()).unwrap());
        for i in 0..500u32 {
            s.put(0, format!("key{i:04}").as_bytes(), &i.to_le_bytes())
                .unwrap();
        }
        s.put(1, b"other-shard", b"survives").unwrap();
        // Force the pre-clear data into SSTs so reclamation has files to act on.
        s.flush().unwrap();

        let upper = commit_clear(&s, 0).expect("non-empty CF stages a tombstone");
        super::reclaim::run_reclamation(&s, CfTier::Main, 0, &upper);

        assert_eq!(
            s.iter_cf(0).unwrap().count(),
            0,
            "cleared shard must stay empty"
        );
        assert_eq!(
            s.get(1, b"other-shard").unwrap(),
            Some(b"survives".to_vec())
        );
    }
    // Restart: compaction output must not resurrect cleared keys.
    let s = RocksStore::open(t.path(), 2, &RocksConfig::default()).unwrap();
    assert_eq!(
        s.iter_cf(0).unwrap().count(),
        0,
        "cleared data resurrected after reopen"
    );
    assert_eq!(
        s.get(1, b"other-shard").unwrap(),
        Some(b"survives".to_vec())
    );
}

/// Proposal 48 test 2 (disk shrink): reclamation must actually return SST
/// bytes, not just hide the keys behind the tombstone. Measured via the
/// `rocksdb.total-sst-files-size` CF property (precise: no WAL / memtable
/// noise), so this is deterministic rather than flaky-prone and does not need
/// `#[ignore]`.
#[test]
fn clear_reclamation_shrinks_sst_bytes() {
    let t = TempDir::new().unwrap();
    let s = Arc::new(RocksStore::open(t.path(), 1, &RocksConfig::default()).unwrap());

    // ~4 MB of pseudorandom (incompressible) values.
    let mut state = 0x9E3779B97F4A7C15u64;
    let mut value = vec![0u8; 4096];
    for i in 0..1000u32 {
        for b in value.iter_mut() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *b = state as u8;
        }
        s.put(0, format!("bulk{i:05}").as_bytes(), &value).unwrap();
    }
    s.flush().unwrap();
    let before = sst_bytes(&s, 0);
    assert!(
        before > 1024 * 1024,
        "setup: expected >1MB of SSTs before the clear, got {before}"
    );

    let upper = commit_clear(&s, 0).expect("non-empty CF stages a tombstone");
    super::reclaim::run_reclamation(&s, CfTier::Main, 0, &upper);

    let after = sst_bytes(&s, 0);
    assert!(
        after < before / 10,
        "reclamation should drop SST bytes materially: before={before} after={after}"
    );
}

// FM-PERSISTENCE-012
/// Proposal 48 test 3 (concurrency): writes accepted immediately after the
/// tombstone commit must survive the full asynchronous reclamation pass, and
/// the pre-clear keys must stay gone. Exercises the real spawn path.
#[test]
fn clear_reclamation_preserves_post_clear_writes() {
    let t = TempDir::new().unwrap();
    {
        let s = Arc::new(RocksStore::open(t.path(), 1, &RocksConfig::default()).unwrap());
        for i in 0..200u32 {
            s.put(0, format!("old{i:04}").as_bytes(), b"pre-clear")
                .unwrap();
        }
        s.flush().unwrap();

        let upper = commit_clear(&s, 0).expect("non-empty CF stages a tombstone");
        // Writes racing the (not yet started) reclamation, exactly like the WAL
        // flush thread committing post-clear entries while compaction runs.
        for i in 0..50u32 {
            s.put(0, format!("new{i:04}").as_bytes(), b"post-clear")
                .unwrap();
        }
        s.spawn_clear_reclamation(CfTier::Main, 0, upper);
        for i in 50..100u32 {
            s.put(0, format!("new{i:04}").as_bytes(), b"post-clear")
                .unwrap();
        }
        wait_reclaim_idle(&s);

        assert_eq!(
            s.get(0, b"old0000").unwrap(),
            None,
            "pre-clear key resurrected"
        );
        let live = s.iter_cf(0).unwrap().count();
        assert_eq!(live, 100, "all post-clear writes must survive reclamation");
    }
    let s = RocksStore::open(t.path(), 1, &RocksConfig::default()).unwrap();
    assert_eq!(s.iter_cf(0).unwrap().count(), 100);
    assert_eq!(s.get(0, b"new0099").unwrap(), Some(b"post-clear".to_vec()));
}

/// Warm-tier FLUSHDB path: `clear_tier_shard` both clears the CF and triggers
/// the same reclamation pass (observed via the started/completed counters).
#[test]
fn warm_clear_tier_shard_reclaims_and_counts() {
    let t = TempDir::new().unwrap();
    let recorder = Arc::new(CountingRecorder::default());
    let s = Arc::new(
        RocksStore::open_with_warm_metrics(
            t.path(),
            1,
            &RocksConfig::default(),
            true,
            recorder.clone(),
        )
        .unwrap(),
    );

    for i in 0..100u32 {
        s.put_warm(0, format!("warm{i:03}").as_bytes(), b"cold-value")
            .unwrap();
    }
    s.clear_tier_shard(CfTier::Warm, 0).unwrap();
    wait_reclaim_idle(&s);

    assert_eq!(s.iter_warm_cf(0).unwrap().count(), 0);
    assert_eq!(
        recorder.counter_value("frogdb_flush_compact_started_total"),
        Some(1)
    );
    assert_eq!(
        recorder.counter_value("frogdb_flush_compact_completed_total"),
        Some(1)
    );
}

/// With `flush-compact-range no`, the clear itself still works (tombstone
/// semantics are untouched) but no reclamation pass starts — the counters
/// stay unset and no pass is ever in flight.
#[test]
fn reclamation_disabled_by_config_knob() {
    let t = TempDir::new().unwrap();
    let config = RocksConfig {
        flush_compact_range: false,
        ..RocksConfig::default()
    };
    let recorder = Arc::new(CountingRecorder::default());
    let s =
        Arc::new(RocksStore::open_with_metrics(t.path(), 1, &config, recorder.clone()).unwrap());

    s.put(0, b"k", b"v").unwrap();
    let upper = commit_clear(&s, 0).expect("non-empty CF stages a tombstone");
    s.spawn_clear_reclamation(CfTier::Main, 0, upper);

    assert_eq!(
        s.reclaim_guard.in_flight_count(),
        0,
        "knob off must not start a pass"
    );
    assert_eq!(
        recorder.counter_value("frogdb_flush_compact_started_total"),
        None
    );
    assert_eq!(
        s.iter_cf(0).unwrap().count(),
        0,
        "tombstone still clears the data"
    );
}

/// The `open`/`open_with_warm` shims default to an *explicit*
/// `NoopMetricsRecorder` (no late install). Reclamation must still run through
/// that recorder — reading it every pass — without panicking and without any
/// observable count. This pins that the explicit-Noop default is intentional and
/// safe: a Store opened via a shim is fully functional, and the deleted
/// `set_metrics_recorder` install is not needed to make reclamation sound.
#[test]
fn noop_default_shim_reclaims_without_panicking() {
    let t = TempDir::new().unwrap();
    // Opened via the Noop-defaulting shim — no recorder is ever installed.
    let s = Arc::new(RocksStore::open(t.path(), 1, &RocksConfig::default()).unwrap());

    for i in 0..100u32 {
        s.put(0, format!("k{i:04}").as_bytes(), b"v").unwrap();
    }
    s.flush().unwrap();
    let upper = commit_clear(&s, 0).expect("non-empty CF stages a tombstone");
    // Drives `run_reclamation`, which reads `metrics_recorder()` on every pass.
    s.spawn_clear_reclamation(CfTier::Main, 0, upper);
    wait_reclaim_idle(&s);

    assert_eq!(
        s.iter_cf(0).unwrap().count(),
        0,
        "Noop-default reclamation still clears the shard"
    );
}

// --- Knob A: honor `compression` (proposal 19) ---

/// The curated per-level preset table is pinned cell-by-cell so the presets
/// cannot drift silently. Each `CompressionType` maps to a deliberate 7-level
/// schedule (not a mechanical single-codec fill).
#[test]
fn per_level_schedule_curated_table() {
    use DBCompressionType as D;
    assert_eq!(
        CompressionType::None.per_level_schedule(),
        [
            D::None,
            D::None,
            D::None,
            D::None,
            D::None,
            D::None,
            D::None
        ],
        "None preset compresses nothing"
    );
    assert_eq!(
        CompressionType::Lz4.per_level_schedule(),
        [D::None, D::None, D::Lz4, D::Lz4, D::Zstd, D::Zstd, D::Zstd],
        "Lz4 preset is the balanced historical mixed Lz4/Zstd schedule"
    );
    assert_eq!(
        CompressionType::Zstd.per_level_schedule(),
        [
            D::None,
            D::None,
            D::Zstd,
            D::Zstd,
            D::Zstd,
            D::Zstd,
            D::Zstd
        ],
        "Zstd preset is a uniform Zstd tail"
    );
    assert_eq!(
        CompressionType::Snappy.per_level_schedule(),
        [
            D::None,
            D::None,
            D::Snappy,
            D::Snappy,
            D::Snappy,
            D::Snappy,
            D::Snappy
        ],
        "Snappy preset is a uniform Snappy tail"
    );
}

/// Regression guard: the default compression (`Lz4`) must reproduce the exact
/// historical hard-coded per-level array so honoring the knob does not silently
/// change the default on-disk compression profile of existing data directories.
#[test]
fn default_compression_reproduces_historical_schedule() {
    let historical = [
        DBCompressionType::None,
        DBCompressionType::None,
        DBCompressionType::Lz4,
        DBCompressionType::Lz4,
        DBCompressionType::Zstd,
        DBCompressionType::Zstd,
        DBCompressionType::Zstd,
    ];
    assert_eq!(
        RocksConfig::default().compression.per_level_schedule(),
        historical,
        "default RocksConfig must keep the historical compression schedule"
    );
}

/// Open-time test: a store opened with a non-default `compression` opens, writes,
/// and round-trips across a reopen. This exercises the config→CF-open wiring for
/// `Zstd` (which now differs from `None`'s all-uncompressed schedule at the
/// schedule level, pinned by `per_level_schedule_curated_table`).
#[test]
fn open_with_zstd_compression_roundtrips() {
    let t = TempDir::new().unwrap();
    let config = RocksConfig {
        compression: CompressionType::Zstd,
        ..RocksConfig::default()
    };
    {
        let s = RocksStore::open(t.path(), 2, &config).unwrap();
        s.put(0, b"k1", b"v1").unwrap();
        s.put(1, b"k2", b"v2").unwrap();
    }
    // Reopen with the same compression and confirm data survives.
    let s = RocksStore::open(t.path(), 2, &config).unwrap();
    assert_eq!(s.get(0, b"k1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(s.get(1, b"k2").unwrap(), Some(b"v2".to_vec()));
}

/// Snappy build-support probe (proposal 19 Risks): `compression = "snappy"` was
/// validated-but-ignored before honoring, so this is the first value that routes
/// to `DBCompressionType::Snappy`. If the linked RocksDB build lacked Snappy this
/// would fail at CF open. Confirms the target build supports Snappy end-to-end.
#[test]
fn open_with_snappy_compression_succeeds() {
    let t = TempDir::new().unwrap();
    let config = RocksConfig {
        compression: CompressionType::Snappy,
        ..RocksConfig::default()
    };
    let s = RocksStore::open(t.path(), 1, &config)
        .expect("Snappy compression must be supported by the linked RocksDB build");
    s.put(0, b"k", b"v").unwrap();
    assert_eq!(s.get(0, b"k").unwrap(), Some(b"v".to_vec()));
}

// ============================================================================
// WAL recovery mode pin + mid-log corruption (issue 14)
// ============================================================================

/// Number of records written into the WAL by the corruption tests. Small enough
/// that all 50 stay in a single RocksDB WAL block (default 32 KiB) — so a byte
/// flipped partway through the file lands mid-log with valid records on both
/// sides of it, which is exactly the case `PointInTime` recovery must truncate.
const CORRUPT_TEST_RECORDS: usize = 50;

/// Deterministic key for record `i` (zero-padded so the on-disk order matches
/// the numeric order and the surviving prefix is easy to reason about).
fn corrupt_key(i: usize) -> Vec<u8> {
    format!("wal_key_{i:04}").into_bytes()
}

/// Return the path of the active WAL (`*.log`) — the largest one, which is the
/// live log the recent writes landed in.
fn active_wal_path(db_dir: &Path) -> PathBuf {
    let mut logs: Vec<(u64, PathBuf)> = fs::read_dir(db_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("log"))
        .map(|p| (fs::metadata(&p).unwrap().len(), p))
        .collect();
    assert!(
        !logs.is_empty(),
        "expected a RocksDB WAL (*.log) in {db_dir:?}"
    );
    logs.sort_by_key(|(len, _)| *len);
    logs.pop().unwrap().1
}

/// Write [`CORRUPT_TEST_RECORDS`] durably-synced records into shard 0, record
/// the WAL watermark (as the production sync path does), then drop the store
/// without a memtable flush — the `rocksdb` crate's `Drop` calls `rocksdb_close`
/// (the destructor), which does not flush memtables, so every record stays in
/// the WAL and reopen must replay it. Returns the WAL path for corruption.
fn seed_synced_wal(db_dir: &Path) -> PathBuf {
    let s = RocksStore::open(db_dir, 1, &RocksConfig::default()).unwrap();
    let mut wo = WriteOptions::default();
    wo.set_sync(true);
    for i in 0..CORRUPT_TEST_RECORDS {
        s.put_opt(0, &corrupt_key(i), format!("value_{i}").as_bytes(), &wo)
            .unwrap();
    }
    // The durable-sync watermark the production flush path maintains after an
    // fsync'd batch. Records the full sequence so a short recovery is
    // detectable. This is a single-writer setup with no racing shard, so
    // reading the sequence after every write lands (rather than the
    // pre-write snapshot the production sync path uses) is an accurate
    // covered sequence here, not an over-claim.
    s.record_wal_watermark(s.latest_sequence_number());
    assert_eq!(
        s.latest_sequence_number(),
        CORRUPT_TEST_RECORDS as u64,
        "each single-key put should advance the sequence by exactly one"
    );
    let wal = active_wal_path(db_dir);
    drop(s); // no flush → WAL is the sole copy, mimicking an unclean shutdown
    wal
}

/// Reopen with a counting recorder, recover the surviving keys, and return
/// (surviving_count, dropped_metric). Asserts the survivors form a contiguous
/// prefix — `PointInTime` recovery keeps the longest uninterrupted run of valid
/// records and discards everything from the first corruption onward, so the
/// survivors are always `wal_key_0000..wal_key_{m-1}` for some `m`.
fn reopen_and_measure(db_dir: &Path) -> (usize, u64) {
    let recorder = Arc::new(CountingRecorder::default());
    let s = RocksStore::open_with_metrics(db_dir, 1, &RocksConfig::default(), recorder.clone())
        .unwrap();

    let present: Vec<bool> = (0..CORRUPT_TEST_RECORDS)
        .map(|i| s.get(0, &corrupt_key(i)).unwrap().is_some())
        .collect();
    let survivors = present.iter().filter(|p| **p).count();

    // Contiguous-prefix invariant: no surviving key may appear after a dropped
    // one. This is the load-bearing `PointInTime` guarantee — recovered state is
    // always a real prefix of history, never an interleaving that resurrects a
    // record from beyond the corruption point.
    for (i, key_present) in present.iter().enumerate() {
        assert_eq!(
            *key_present,
            i < survivors,
            "survivors must be a contiguous prefix; key {i} present={key_present} but survivor count={survivors}",
        );
    }

    let dropped = recorder
        .counter_value("frogdb_wal_recovery_dropped_records_total")
        .unwrap_or(0);
    (survivors, dropped)
}

// FM-PERSISTENCE-034
/// A byte flipped in the *middle* of the WAL (not the tail) fails a record
/// checksum mid-log. `PointInTime` recovery — the mode FrogDB pins — truncates
/// at the first corrupt record and drops every valid record after it. Asserts
/// the exact surviving set (a strict, non-empty prefix) and that the drop is
/// signalled by the `frogdb_wal_recovery_dropped_records_total` metric.
#[test]
fn wal_mid_log_bitflip_drops_suffix_and_signals() {
    let t = TempDir::new().unwrap();
    let wal = seed_synced_wal(t.path());

    // Flip a byte two-thirds of the way in: safely past the early records
    // (which must survive) and safely before the tail (so valid records exist
    // after the corruption, proving the suffix is dropped, not just a torn tail).
    let mut bytes = fs::read(&wal).unwrap();
    let pos = bytes.len() * 2 / 3;
    bytes[pos] ^= 0xFF;
    fs::write(&wal, &bytes).unwrap();

    let (survivors, dropped) = reopen_and_measure(t.path());

    assert!(
        survivors > 0 && survivors < CORRUPT_TEST_RECORDS,
        "mid-log corruption must keep a non-empty prefix and drop a non-empty \
         suffix, got {survivors}/{CORRUPT_TEST_RECORDS} survivors"
    );
    assert_eq!(
        dropped,
        (CORRUPT_TEST_RECORDS - survivors) as u64,
        "the dropped-records metric must equal the number of truncated records \
         (watermark {CORRUPT_TEST_RECORDS} minus recovered sequence {survivors})"
    );
}

// FM-PERSISTENCE-034
/// Truncating the WAL mid-file cuts the log at an arbitrary point, dropping the
/// records past the cut. This is the documented "torn tail" case; `PointInTime`
/// recovers the valid prefix. Even though this truncation is *expected* on an
/// unclean shutdown, the dropped records must still raise the metric so an
/// operator can tell how much acknowledged data recovery discarded.
#[test]
fn wal_truncation_recovers_prefix_and_signals() {
    let t = TempDir::new().unwrap();
    let wal = seed_synced_wal(t.path());

    // Cut the log to 55% of its length — well before the final record, so a
    // meaningful suffix is lost while an early prefix survives.
    let orig = fs::metadata(&wal).unwrap().len();
    let truncated = orig * 55 / 100;
    let f = fs::OpenOptions::new().write(true).open(&wal).unwrap();
    f.set_len(truncated).unwrap();
    drop(f);

    let (survivors, dropped) = reopen_and_measure(t.path());

    assert!(
        survivors < CORRUPT_TEST_RECORDS,
        "truncation must drop at least the tail record"
    );
    assert_eq!(
        dropped,
        (CORRUPT_TEST_RECORDS - survivors) as u64,
        "the dropped-records metric must equal the number of truncated records"
    );
}

// FM-PERSISTENCE-035
/// A clean reopen of an intact WAL recovers every record and raises no
/// dropped-records signal: the watermark comparison must not false-alarm when
/// recovery reaches (or exceeds) the recorded durable sequence.
#[test]
fn wal_clean_reopen_recovers_all_without_signal() {
    let t = TempDir::new().unwrap();
    seed_synced_wal(t.path()); // intact WAL, no corruption

    let (survivors, dropped) = reopen_and_measure(t.path());

    assert_eq!(
        survivors, CORRUPT_TEST_RECORDS,
        "an intact WAL must recover every record"
    );
    assert_eq!(
        dropped, 0,
        "a clean recovery must not signal dropped records"
    );
}

// FM-PERSISTENCE-034
/// The pinned recovery mode is an explicit choice, not RocksDB's inherited
/// default. This guards against a silent library-default change: the acceptance
/// criterion is that the mode is *set in code*, and the corruption tests above
/// verify the behavior that setting produces.
#[test]
fn wal_recovery_mode_is_pinned_to_point_in_time() {
    // Compile-time proof the variant exists and is what we pin; the behavioral
    // proof lives in the corruption tests. Kept as a named anchor so a future
    // change to the pinned mode has to update this test deliberately.
    let pinned = rocksdb::DBRecoveryMode::PointInTime;
    assert!(matches!(pinned, rocksdb::DBRecoveryMode::PointInTime));
}

// FM-PERSISTENCE-034
/// `atomic_flush` is pinned at open, not left at RocksDB's `false` library
/// default (spec-gaps issue 03). Unlike the recovery-mode anchor above there
/// is no `Options` getter to assert against directly, so this reads RocksDB's
/// own record instead: a fresh checkpoint carries an `OPTIONS-*` file RocksDB
/// writes from the live options it opened with, which is a genuine behavioral
/// pin against a future accidental removal, not just a compile-time anchor.
#[test]
fn atomic_flush_is_pinned_on() {
    let t = TempDir::new().unwrap();
    let s = RocksStore::open(t.path(), 1, &RocksConfig::default()).unwrap();
    let ckpt_dir = t.path().join("checkpoint");
    s.create_checkpoint(&ckpt_dir).unwrap();

    let options_file = fs::read_dir(&ckpt_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("OPTIONS-"))
        })
        .expect("a checkpoint carries RocksDB's own OPTIONS-* record");
    let contents = fs::read_to_string(&options_file).unwrap();
    assert!(
        contents.contains("atomic_flush=true"),
        "atomic_flush must be pinned on; checkpoint OPTIONS recorded:\n{contents}"
    );
}

// FM-PERSISTENCE-034
/// A tiering demotion writes a warm copy then deletes the hot copy (`warm
/// put`, `hot delete`). Before this fix, `RocksStore::flush()` only ever
/// flushed the main tier — the warm tier's own memtable was never part of the
/// store's production flush surface at all, so a demoted key's warm copy
/// stayed WAL-dependent no matter how many `durable_sync` ticks ran. Losing
/// that WAL (e.g. total loss, not just truncation) after a `flush()` call
/// then drops the warm copy while the hot delete — durably flushed — sticks,
/// so the key vanishes outright: no hot, no warm, data loss for a demotion
/// `flush()` was supposed to have made durable.
///
/// `flush()` now names every CF it manages (main, warm when enabled,
/// search-meta) in one `flush_cfs_opt` call, so the warm copy is captured
/// right alongside the hot delete. The clearest proof `flush()` actually
/// reaches the warm CF: the WAL segment written before the call becomes
/// fully obsolete (RocksDB deletes it) once every CF it covers is fully on
/// SST, which only happens once the warm tier is included too.
#[test]
fn flush_covers_the_warm_tier_not_just_main() {
    let t = TempDir::new().unwrap();
    let key = b"demoted_key";

    let s = RocksStore::open_with_warm(t.path(), 1, &RocksConfig::default(), true).unwrap();
    let mut wo = WriteOptions::default();
    wo.set_sync(true);
    // The key starts out hot, durably.
    s.put_opt(0, key, b"orig", &wo).unwrap();

    // The demotion pair: warm put, then hot delete. Neither call is
    // individually synced.
    s.put_warm(0, key, b"orig").unwrap();
    s.delete(0, key).unwrap();

    let wal_before_flush = active_wal_path(t.path());
    s.flush().unwrap();
    assert!(
        !wal_before_flush.exists(),
        "the pre-flush WAL segment should be fully obsolete after flush() — it is not, so \
         some CF `flush()` is responsible for still depends on it (the warm tier, if the \
         fix regressed): {wal_before_flush:?}"
    );

    drop(s); // unclean shutdown: nothing written after the flush is durable

    let s2 = RocksStore::open_with_warm(t.path(), 1, &RocksConfig::default(), true).unwrap();
    assert_eq!(
        s2.get(0, key).unwrap(),
        None,
        "the hot delete must survive — it was always covered by flush()"
    );
    assert_eq!(
        s2.get_warm(0, key).unwrap(),
        Some(b"orig".to_vec()),
        "the warm put must survive flush() too, not just the main tier's delete"
    );
}

/// A stand-in for a WAL writer's `FlushOutcomes`: it reports a committed
/// sequence and records whatever `durable_sync` publishes back to it.
struct FakeSyncTarget {
    committed: std::sync::atomic::AtomicU64,
    synced: std::sync::atomic::AtomicU64,
}

impl FakeSyncTarget {
    fn new(committed: u64) -> Arc<Self> {
        Arc::new(Self {
            committed: std::sync::atomic::AtomicU64::new(committed),
            synced: std::sync::atomic::AtomicU64::new(0),
        })
    }
    fn synced(&self) -> u64 {
        self.synced.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl DurableSyncTarget for FakeSyncTarget {
    fn committed_sequence(&self) -> u64 {
        self.committed.load(std::sync::atomic::Ordering::SeqCst)
    }
    fn publish_synced_through(&self, seq: u64) {
        self.synced
            .fetch_max(seq, std::sync::atomic::Ordering::SeqCst);
    }
}

// FM-PERSISTENCE-043
/// An out-of-band `durable_sync` has to reach *every* registered writer, not
/// merely the most recent one: the store is the only thing that knows the fsync
/// covered all shards, so a watermark it fails to publish to is a shard that
/// under-reports its durability forever.
#[test]
fn durable_sync_publishes_the_flushed_sequence_to_every_registered_writer() {
    let t = TempDir::new().unwrap();
    let s = RocksStore::open(t.path(), 2, &RocksConfig::default()).unwrap();

    let a = FakeSyncTarget::new(7);
    let b = FakeSyncTarget::new(11);
    s.register_sync_target(&(a.clone() as Arc<dyn DurableSyncTarget>));
    s.register_sync_target(&(b.clone() as Arc<dyn DurableSyncTarget>));
    assert_eq!(a.synced(), 0, "nothing is durable before the sync");

    s.put(0, b"k", b"v").unwrap();
    s.durable_sync().unwrap();

    assert_eq!(a.synced(), 7, "the first writer's watermark must advance");
    assert_eq!(b.synced(), 11, "and so must the second's");
}

/// Registration holds `Weak` references, and both the register path and the
/// sync path prune the ones whose writer is gone — otherwise a store outliving
/// many short-lived writers accumulates them without bound.
#[test]
fn dead_sync_targets_are_reaped_on_register_and_on_sync() {
    let t = TempDir::new().unwrap();
    let s = RocksStore::open(t.path(), 1, &RocksConfig::default()).unwrap();

    {
        let gone = FakeSyncTarget::new(1);
        s.register_sync_target(&(gone.clone() as Arc<dyn DurableSyncTarget>));
    }
    let live = FakeSyncTarget::new(2);
    s.register_sync_target(&(live.clone() as Arc<dyn DurableSyncTarget>));
    assert_eq!(
        s.sync_targets.lock().unwrap().len(),
        1,
        "registering must drop the entry whose writer was dropped, and keep the live one"
    );

    drop(live);
    s.durable_sync().unwrap();
    assert_eq!(
        s.sync_targets.lock().unwrap().len(),
        0,
        "syncing must reap the now-dead entry too"
    );
}

/// Live entries in a shard's active memtable — the observable that separates
/// "handed to RocksDB" from "written out to an SST".
fn active_memtable_entries(s: &RocksStore, shard: usize) -> u64 {
    let cf = s.cf_handle(shard).unwrap();
    s.db.property_int_value_cf(&cf, "rocksdb.num-entries-active-mem-table")
        .unwrap()
        .unwrap()
}

/// The crash-recovery harness leans on `sync_wal` to force a durable point
/// before it kills the process; a version that returned `Ok(())` without
/// flushing would make every crash test pass for the wrong reason. The
/// post-condition is per shard, not just the first one.
#[test]
fn sync_wal_flushes_every_shard_memtable() {
    let t = TempDir::new().unwrap();
    let s = RocksStore::open(t.path(), 3, &RocksConfig::default()).unwrap();
    for shard in 0..3 {
        s.put(shard, b"k", b"v").unwrap();
        assert!(
            active_memtable_entries(&s, shard) > 0,
            "shard {shard} should hold an unflushed write"
        );
    }

    s.sync_wal().unwrap();

    for shard in 0..3 {
        assert_eq!(
            active_memtable_entries(&s, shard),
            0,
            "shard {shard} memtable must be flushed, not just the first"
        );
        assert_eq!(s.get(shard, b"k").unwrap(), Some(b"v".to_vec()));
    }
}

/// `commit_raw_batch` is the one seam the crash-atomicity tests write through,
/// and it must apply *both* op kinds to the shard each op names — a batch that
/// silently committed nothing would turn those tests into no-ops.
#[test]
fn commit_raw_batch_applies_puts_and_deletes_to_the_shard_each_op_names() {
    let t = TempDir::new().unwrap();
    let s = RocksStore::open(t.path(), 2, &RocksConfig::default()).unwrap();
    s.put(1, b"doomed", b"v").unwrap();

    s.commit_raw_batch(
        &[
            RawBatchOp::Put {
                shard: 0,
                key: b"fresh",
                value: b"v0",
            },
            RawBatchOp::Put {
                shard: 1,
                key: b"also_fresh",
                value: b"v1",
            },
            RawBatchOp::Delete {
                shard: 1,
                key: b"doomed",
            },
        ],
        &WriteOptions::default(),
    )
    .unwrap();

    assert_eq!(s.get(0, b"fresh").unwrap(), Some(b"v0".to_vec()));
    assert_eq!(s.get(1, b"also_fresh").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(s.get(1, b"doomed").unwrap(), None, "the delete applied too");
    assert_eq!(
        s.get(0, b"also_fresh").unwrap(),
        None,
        "ops land in the shard they name, not the first one"
    );
}

// FM-PERSISTENCE-014
/// Operands that reach an SST with no base value in the same memtable are
/// folded by the *partial* merge callback before the base is ever consulted.
/// The fold has to be a real HLL union: an operand that combines wrongly is
/// only visible after the base rejoins it at read time.
#[test]
fn hll_operands_flushed_without_their_base_still_read_back_merged() {
    use crate::serialization::{deserialize, serialize, serialize_hll_delta};
    use frogdb_types::hyperloglog::HyperLogLogValue;
    use frogdb_types::types::{KeyMetadata, Value};

    let dir = TempDir::new().unwrap();
    let meta = KeyMetadata::new(1);
    let mut reference = HyperLogLogValue::new();
    for i in 0..200u32 {
        reference.add(&i.to_le_bytes());
    }
    let rocks = RocksStore::open(dir.path(), 1, &RocksConfig::default()).unwrap();
    rocks
        .put(
            0,
            b"hll",
            &serialize(&Value::HyperLogLog(reference.clone()), &meta),
        )
        .unwrap();
    // Push the base out to an SST so the operands below flush on their own.
    rocks.flush().unwrap();

    for batch in 0..3u32 {
        let mut pairs = Vec::new();
        for i in 0..400u32 {
            let x = 1_000 + batch * 400 + i;
            if let Some(p) = reference.add_tracked(&x.to_le_bytes()) {
                pairs.push(p);
            }
        }
        rocks
            .merge(0, b"hll", &serialize_hll_delta(&pairs, &meta))
            .unwrap();
    }
    // A memtable holding only operands: this flush is what invokes the partial
    // merge callback.
    rocks.flush().unwrap();

    let got = rocks.get(0, b"hll").unwrap().unwrap();
    let (value, _) = deserialize(&got).unwrap();
    let Value::HyperLogLog(h) = value else {
        panic!("wrong type")
    };
    assert_eq!(
        h.count_no_cache(),
        reference.count_no_cache(),
        "partially merged operands must union, not overwrite or truncate"
    );
}

/// Capacity of the block cache the data column families were opened with.
fn block_cache_capacity(s: &RocksStore) -> u64 {
    let cf = s.cf_handle(0).unwrap();
    s.db.property_int_value_cf(&cf, "rocksdb.block-cache-capacity")
        .unwrap()
        .unwrap()
}

/// Total bloom-filter bytes across a shard's SSTs, parsed out of RocksDB's
/// aggregated table properties.
fn filter_bytes(s: &RocksStore) -> u64 {
    let cf = s.cf_handle(0).unwrap();
    let props =
        s.db.property_value_cf(&cf, "rocksdb.aggregated-table-properties")
            .unwrap()
            .unwrap();
    props
        .split(';')
        .find_map(|f| f.trim().strip_prefix("filter block size="))
        .unwrap_or_else(|| panic!("no filter block size in {props}"))
        .parse()
        .unwrap()
}

/// The `block_cache_size` knob has to reach RocksDB, and `0` has to mean "leave
/// RocksDB's own default alone" rather than "a cache that holds nothing" — a
/// zero-capacity cache would re-read a block from the SST on every lookup.
#[test]
fn block_cache_size_is_honoured_and_zero_leaves_the_rocksdb_default() {
    let t = TempDir::new().unwrap();
    let sized = RocksStore::open(
        t.path(),
        1,
        &RocksConfig {
            block_cache_size: 4 * 1024 * 1024,
            ..RocksConfig::default()
        },
    )
    .unwrap();
    assert_eq!(
        block_cache_capacity(&sized),
        4 * 1024 * 1024,
        "the configured cache size must be the cache's capacity"
    );
    drop(sized);

    let t2 = TempDir::new().unwrap();
    let unset = RocksStore::open(
        t2.path(),
        1,
        &RocksConfig {
            block_cache_size: 0,
            ..RocksConfig::default()
        },
    )
    .unwrap();
    assert!(
        block_cache_capacity(&unset) > 0,
        "0 means 'no override', never a cache with no room in it"
    );
}

/// The `bloom_filter_bits` knob has to reach the table builder: with it on, the
/// SSTs carry a filter block; with it at `0` they carry none. A guard that
/// fired on the wrong side would either drop the filters silently (every point
/// lookup pays a disk read) or build a zero-bit filter nobody asked for.
#[test]
fn bloom_filter_bits_is_honoured_and_zero_builds_no_filter() {
    fn write_and_flush(s: &RocksStore) {
        for i in 0..200u32 {
            s.put(0, format!("key{i:04}").as_bytes(), b"v").unwrap();
        }
        s.flush().unwrap();
    }

    let t = TempDir::new().unwrap();
    let with_bloom = RocksStore::open(
        t.path(),
        1,
        &RocksConfig {
            bloom_filter_bits: 10,
            ..RocksConfig::default()
        },
    )
    .unwrap();
    write_and_flush(&with_bloom);
    assert!(
        filter_bytes(&with_bloom) > 0,
        "a configured bloom filter must be built into the SST"
    );
    drop(with_bloom);

    let t2 = TempDir::new().unwrap();
    let without = RocksStore::open(
        t2.path(),
        1,
        &RocksConfig {
            bloom_filter_bits: 0,
            ..RocksConfig::default()
        },
    )
    .unwrap();
    write_and_flush(&without);
    assert_eq!(
        filter_bytes(&without),
        0,
        "0 bits means no filter block at all"
    );
}

/// The search-metadata sidecar is its own namespace: what the shims write there
/// is invisible to the main tier and to every other shard, and each shim
/// addresses the tier its name claims. Untagged — the sidecar's *contents* are
/// the search engine's business; what is pinned here is the addressing.
#[test]
fn search_meta_shims_address_their_own_tier_and_shard() {
    let t = TempDir::new().unwrap();
    let s = RocksStore::open(t.path(), 2, &RocksConfig::default()).unwrap();

    s.put_search_meta(0, b"idx", b"schema").unwrap();
    assert_eq!(
        s.get_search_meta(0, b"idx").unwrap(),
        Some(b"schema".to_vec()),
        "a sidecar write must read back byte for byte"
    );
    assert_eq!(
        s.get(0, b"idx").unwrap(),
        None,
        "and must not appear in the main tier under the same key"
    );
    assert_eq!(
        s.get_search_meta(1, b"idx").unwrap(),
        None,
        "nor in another shard's sidecar"
    );

    // The reverse direction: a main-tier write under the same key does not
    // shadow or overwrite the sidecar entry.
    s.put(0, b"idx", b"main").unwrap();
    assert_eq!(
        s.get_search_meta(0, b"idx").unwrap(),
        Some(b"schema".to_vec())
    );

    s.put_search_meta(0, b"other", b"x").unwrap();
    assert_eq!(s.iter_search_meta(0).unwrap().count(), 2);
    assert_eq!(s.iter_search_meta(1).unwrap().count(), 0);

    s.delete_search_meta(0, b"idx").unwrap();
    assert_eq!(
        s.get_search_meta(0, b"idx").unwrap(),
        None,
        "a sidecar delete must remove the entry"
    );
    assert_eq!(
        s.get_search_meta(0, b"other").unwrap(),
        Some(b"x".to_vec()),
        "and only that entry"
    );
    assert!(
        s.get_search_meta(2, b"idx").is_err(),
        "a shard id past the end is an error, not an empty read"
    );
}
