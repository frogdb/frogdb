use super::*;
use rocksdb::WriteBatch;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;
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
#[test]
fn test_has_data() {
    let t = TempDir::new().unwrap();
    let s = RocksStore::open(t.path(), 2, &RocksConfig::default()).unwrap();
    assert!(!s.has_data());
    s.put(0, b"k", b"v").unwrap();
    assert!(s.has_data());
}
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
#[test]
fn test_invalid_shard() {
    let t = TempDir::new().unwrap();
    let s = RocksStore::open(t.path(), 2, &RocksConfig::default()).unwrap();
    assert!(matches!(
        s.put(5, b"k", b"v"),
        Err(RocksError::InvalidShardId(5))
    ));
}
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
#[test]
fn test_warm_cf_invalid_shard() {
    let t = TempDir::new().unwrap();
    let s = RocksStore::open_with_warm(t.path(), 2, &RocksConfig::default(), true).unwrap();
    assert!(matches!(
        s.put_warm(5, b"k", b"v"),
        Err(RocksError::InvalidShardId(5))
    ));
}

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

/// A path with no parent (the staging area is a sibling of the db dir) can hold
/// no staged checkpoint: return `Ok(false)` rather than erroring.
#[test]
fn test_load_staged_checkpoint_no_parent_is_noop() {
    assert!(!RocksStore::load_staged_checkpoint(Path::new("")).unwrap());
}

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
