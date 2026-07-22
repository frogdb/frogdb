use super::stager::SnapshotStager;
use super::*;
use crate::rocks::{RocksConfig, RocksStore};
use std::path::Path;
use std::time::Duration;
use tempfile::TempDir;
#[test]
fn test_noop_coordinator() {
    let c = NoopSnapshotCoordinator::new();
    assert!(c.last_save_time().is_none());
    assert!(!c.in_progress());
    // Instant completion (proposal 21): the no-op is now backed by the real
    // `SnapshotScheduler` and has no background loop, so a started save claims
    // *and releases* the slot synchronously. `in_progress()` is therefore false
    // the moment `start_snapshot` returns; the observable contract is the
    // advancing epoch and the stamped `last_save`.
    let h = c.start_snapshot().unwrap();
    assert_eq!(h.epoch(), 1);
    assert!(c.last_save_time().is_some());
    assert!(!c.in_progress());
    // A second save advances the epoch again — no lingering in-progress window.
    let h2 = c.start_snapshot().unwrap();
    assert_eq!(h2.epoch(), 2);
    assert!(!c.in_progress());
}
#[test]
fn test_noop_start_advances_epoch_monotonically() {
    // Deliberate semantic flip (proposal 21): under instant completion there is
    // no genuinely concurrent in-flight save, so holding a handle no longer pins
    // the slot. Back-to-back `start_snapshot`s each succeed with a monotonically
    // advancing epoch — where the old copy returned `AlreadyInProgress`.
    let c = NoopSnapshotCoordinator::new();
    let h1 = c.start_snapshot().unwrap();
    let h2 = c.start_snapshot().unwrap();
    assert_eq!(h1.epoch(), 1);
    assert_eq!(h2.epoch(), 2);
}
#[test]
fn test_noop_request_always_starts_when_quiescent() {
    // Backing the no-op with the real scheduler + instant completion means a
    // quiescent coordinator always `Started`s a fresh save (never `Coalesced`/
    // `AlreadyRunning`), and the epoch advances every call. This is the
    // observable face of the delegation: `complete_instantly` releases the slot
    // before each call returns.
    let c = NoopSnapshotCoordinator::new();
    for expected in 1..=5 {
        assert_eq!(
            c.request_snapshot(SnapshotMode::Immediate),
            SnapshotRequest::Started(expected)
        );
        assert!(!c.in_progress(), "instant completion releases the slot");
    }
    // Schedule mode behaves identically when idle: nothing to coalesce with.
    assert_eq!(
        c.request_snapshot(SnapshotMode::Schedule),
        SnapshotRequest::Started(6)
    );
}
#[test]
fn test_noop_concurrent_request_storm_settles_idle() {
    use std::sync::Arc;
    use std::thread;
    // Concurrent BGSAVE SCHEDULE storm through the no-op coordinator. Because it
    // now *is* the `SnapshotScheduler`, it inherits the closed lost-wakeup
    // guarantee: whichever caller `Started`s owns the run and drains coalesced
    // follow-ups inline (`complete_instantly`); at quiescence the slot is
    // released and no follow-up is stranded. A non-draining `complete_instantly`
    // would leave `in_progress == true` forever — the primary guard here.
    for _ in 0..200 {
        let c = Arc::new(NoopSnapshotCoordinator::new());
        let threads: Vec<_> = (0..6)
            .map(|_| {
                let c = c.clone();
                thread::spawn(move || {
                    c.request_snapshot(SnapshotMode::Schedule);
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert!(
            !c.in_progress(),
            "no-op coordinator must settle idle at quiescence"
        );
        assert!(c.last_save_time().is_some(), "at least one save ran");
        // No stranded follow-up: two quiescent starts advance the epoch by
        // exactly 1 each. A dangling `scheduled` flag would drain into a phantom
        // run and jump the counter further.
        match (
            c.request_snapshot(SnapshotMode::Immediate),
            c.request_snapshot(SnapshotMode::Immediate),
        ) {
            (SnapshotRequest::Started(a), SnapshotRequest::Started(b)) => {
                assert_eq!(b, a + 1, "quiescent starts must advance by exactly 1");
            }
            other => panic!("expected two Started results, got {other:?}"),
        }
    }
}
#[test]
fn test_handle_is_bare_epoch_carrier() {
    // The handle is now always a bare epoch carrier (proposal 21): it holds no
    // completion state and implements no `Drop`. Completion is owned by the
    // coordinator's scheduler, not the handle.
    let h = SnapshotHandle::new(3);
    assert_eq!(h.epoch(), 3);
}
#[test]
fn test_metadata_file_new() {
    let m = SnapshotMetadataFile::new(1, 12345, 4);
    assert_eq!(m.version, 1);
    assert_eq!(m.epoch, 1);
    assert!(!m.is_complete());
}
#[test]
fn test_metadata_file_complete() {
    let mut m = SnapshotMetadataFile::new(1, 12345, 4);
    m.mark_complete(5_000_000);
    assert!(m.is_complete());
    assert_eq!(m.size_bytes, 5_000_000);
}
#[test]
fn test_metadata_to_metadata() {
    let mut mf = SnapshotMetadataFile::new(5, 99999, 8);
    mf.mark_complete(10_000_000);
    let m = mf.to_metadata();
    assert_eq!(m.epoch, 5);
    assert!(m.started_at.elapsed().unwrap() < Duration::from_secs(1));
}
#[test]
fn test_metadata_serialization() {
    let mut m = SnapshotMetadataFile::new(3, 54321, 2);
    m.mark_complete(1_000_000);
    let j = serde_json::to_string(&m).unwrap();
    let d: SnapshotMetadataFile = serde_json::from_str(&j).unwrap();
    assert_eq!(d.epoch, 3);
    assert!(d.is_complete());
}
/// Old metadata files written before `num_keys` was deleted still deserialize:
/// serde ignores the unknown field.
#[test]
fn test_metadata_deserializes_legacy_num_keys_field() {
    let legacy = r#"{
        "version": 1,
        "epoch": 7,
        "sequence_number": 42,
        "started_at_ms": 1000,
        "completed_at_ms": 2000,
        "num_shards": 4,
        "num_keys": 0,
        "size_bytes": 123,
        "completion_marker": "FROGDB_SNAPSHOT_COMPLETE_v1"
    }"#;
    let d: SnapshotMetadataFile = serde_json::from_str(legacy).unwrap();
    assert_eq!(d.epoch, 7);
    assert_eq!(d.size_bytes, 123);
    assert!(d.is_complete());
}
#[test]
fn test_config_default() {
    let c = SnapshotConfig::default();
    assert_eq!(c.snapshot_interval_secs, 3600);
    assert_eq!(c.max_snapshots, 5);
}
#[test]
fn test_cleanup_old_snapshots() {
    let td = std::env::temp_dir().join(format!("frogdb_snap_cleanup_{}", std::process::id()));
    std::fs::create_dir_all(&td).unwrap();
    for i in 1..=7 {
        std::fs::create_dir_all(td.join(format!("snapshot_{:05}", i))).unwrap();
    }
    SnapshotStager::cleanup_old_snapshots(&td, 3).unwrap();
    let c = std::fs::read_dir(&td)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("snapshot_"))
        .count();
    assert_eq!(c, 3);
    std::fs::remove_dir_all(&td).unwrap();
}
#[test]
fn test_cleanup_unlimited() {
    let td = std::env::temp_dir().join(format!("frogdb_snap_unlim_{}", std::process::id()));
    std::fs::create_dir_all(&td).unwrap();
    for i in 1..=5 {
        std::fs::create_dir_all(td.join(format!("snapshot_{:05}", i))).unwrap();
    }
    SnapshotStager::cleanup_old_snapshots(&td, 0).unwrap();
    assert_eq!(
        std::fs::read_dir(&td)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("snapshot_"))
            .count(),
        5
    );
    std::fs::remove_dir_all(&td).unwrap();
}

// ---------------------------------------------------------------------------
// Snapshot *creation* (`SnapshotStager::run`)
//
// The install side (`rocks/tests.rs`) has 8 crash-window tests that pin each
// intermediate on-disk state. These mirror that depth from the write direction:
// synthesize a small RocksStore + data dir, run a stager, and assert on the
// on-disk result — including failure injections that exercise the all-or-nothing
// cleanup invariant and the "never install an incomplete snapshot" contract.
// ---------------------------------------------------------------------------

/// A complete single-shard RocksDB to checkpoint from.
fn make_store(dir: &Path) -> RocksStore {
    let s = RocksStore::open(dir, 1, &RocksConfig::default()).unwrap();
    s.put(0, b"k", b"v").unwrap();
    s
}

/// Build a stager with the standard `<snapshot_dir>` path layout for `epoch`.
fn stager(
    snapshot_dir: &Path,
    data_dir: &Path,
    epoch: u64,
    max_snapshots: usize,
) -> SnapshotStager {
    SnapshotStager {
        snapshot_dir: snapshot_dir.to_path_buf(),
        tmp: snapshot_dir.join(format!(".snapshot_{epoch:05}.tmp")),
        final_dir: snapshot_dir.join(format!("snapshot_{epoch:05}")),
        name: format!("snapshot_{epoch:05}"),
        data_dir: data_dir.to_path_buf(),
        epoch,
        num_shards: 1,
        max_snapshots,
    }
}

/// Write a minimal `search/<index>/<shard>/<files>` sidecar under `data_dir`,
/// matching the layout `copy_search_indexes` walks.
fn write_search_sidecar(data_dir: &Path) {
    let shard = data_dir.join("search").join("idx").join("0");
    std::fs::create_dir_all(&shard).unwrap();
    std::fs::write(shard.join("segment.dat"), b"index-bytes").unwrap();
    std::fs::write(shard.join("meta.json"), b"{}").unwrap();
}

/// Happy path: a complete `snapshot_NNNNN/{checkpoint,search,metadata.json}` is
/// promoted, the staging dir is gone, and `latest` points at the new snapshot.
#[test]
fn test_stager_happy_path() {
    let db = TempDir::new().unwrap();
    let store = make_store(db.path());
    let snap = TempDir::new().unwrap();
    let data = TempDir::new().unwrap();
    write_search_sidecar(data.path());

    let md = stager(snap.path(), data.path(), 1, 5).run(&store).unwrap();

    assert!(md.is_complete());
    let dir = snap.path().join("snapshot_00001");
    assert!(dir.join("checkpoint").is_dir());
    assert!(
        dir.join("search")
            .join("idx")
            .join("0")
            .join("segment.dat")
            .exists()
    );
    assert!(dir.join("metadata.json").exists());
    assert!(
        !snap.path().join(".snapshot_00001.tmp").exists(),
        "staging dir must be gone after a successful promote"
    );
    assert!(md.size_bytes > 0, "size should reflect checkpoint + search");
    #[cfg(unix)]
    assert_eq!(
        std::fs::read_link(snap.path().join("latest")).unwrap(),
        Path::new("snapshot_00001")
    );
}

/// The checkpoint stage failing aborts cleanly: nothing is promoted and no
/// staging dir leaks. Here `snapshot_dir` is a regular file, so the staging
/// checkpoint dir cannot be created.
#[test]
fn test_stager_checkpoint_failure_aborts_cleanly() {
    let db = TempDir::new().unwrap();
    let store = make_store(db.path());
    let data = TempDir::new().unwrap();
    let base = TempDir::new().unwrap();
    let snap_dir = base.path().join("snap_is_a_file");
    std::fs::write(&snap_dir, b"not a dir").unwrap();

    let res = stager(&snap_dir, data.path(), 1, 5).run(&store);

    assert!(
        res.is_err(),
        "checkpoint stage must fail on an unusable snapshot dir"
    );
    let siblings: Vec<_> = std::fs::read_dir(base.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name())
        .collect();
    assert_eq!(
        siblings.len(),
        1,
        "nothing should be created, got {siblings:?}"
    );
}

/// Search-copy failure aborts (flag 1 regression): a snapshot missing its search
/// sidecar is never installed. The previous complete snapshot and its `latest`
/// pointer are left untouched as the recovery source.
#[test]
fn test_stager_search_copy_failure_aborts_preserving_previous() {
    let db = TempDir::new().unwrap();
    let store = make_store(db.path());
    let snap = TempDir::new().unwrap();
    let data = TempDir::new().unwrap();

    // First snapshot (no sidecar) succeeds and becomes the recovery source.
    stager(snap.path(), data.path(), 1, 5).run(&store).unwrap();
    assert!(
        snap.path()
            .join("snapshot_00001")
            .join("metadata.json")
            .exists()
    );

    // Make `data_dir/search` a *file* so `copy_search_indexes` fails.
    std::fs::write(data.path().join("search"), b"not a dir").unwrap();

    let res = stager(snap.path(), data.path(), 2, 5).run(&store);

    assert!(res.is_err(), "search-copy failure must abort the snapshot");
    assert!(
        !snap.path().join("snapshot_00002").exists(),
        "an incomplete snapshot must never be installed"
    );
    assert!(
        !snap.path().join(".snapshot_00002.tmp").exists(),
        "the staging dir must be reclaimed on abort"
    );
    assert!(
        snap.path()
            .join("snapshot_00001")
            .join("metadata.json")
            .exists(),
        "the previous good snapshot must survive"
    );
    #[cfg(unix)]
    assert_eq!(
        std::fs::read_link(snap.path().join("latest")).unwrap(),
        Path::new("snapshot_00001"),
        "latest must still point at the previous good snapshot"
    );
}

/// Promote-rename failure leaves no leak (flag 2 regression): the final
/// `tmp -> snapshot_NNNNN` rename fails onto a non-empty target, and the RAII
/// guard reclaims the checkpoint-sized staging dir. Before the fix this path
/// propagated with `?` and leaked the temp dir forever.
#[test]
fn test_stager_promote_rename_failure_leaves_no_leak() {
    let db = TempDir::new().unwrap();
    let store = make_store(db.path());
    let snap = TempDir::new().unwrap();
    let data = TempDir::new().unwrap();

    // Occupy the promotion target with a non-empty dir → rename fails (ENOTEMPTY).
    let blocker = snap.path().join("snapshot_00001");
    std::fs::create_dir_all(&blocker).unwrap();
    std::fs::write(blocker.join("occupied"), b"x").unwrap();

    let res = stager(snap.path(), data.path(), 1, 5).run(&store);

    assert!(
        res.is_err(),
        "promote rename must fail onto a non-empty target"
    );
    assert!(
        !snap.path().join(".snapshot_00001.tmp").exists(),
        "the staging dir must not leak on a rename failure"
    );
    assert!(
        blocker.join("occupied").exists(),
        "the blocker must be untouched"
    );
}

/// Crash window: a `.snapshot_NNNNN.tmp` left by a crashed prior run is reclaimed
/// rather than wedging the epoch with `Directory not empty`.
#[test]
fn test_stager_reclaims_stale_tmp() {
    let db = TempDir::new().unwrap();
    let store = make_store(db.path());
    let snap = TempDir::new().unwrap();
    let data = TempDir::new().unwrap();

    let stale = snap.path().join(".snapshot_00001.tmp");
    std::fs::create_dir_all(stale.join("checkpoint")).unwrap();
    std::fs::write(stale.join("garbage"), b"left over").unwrap();

    let md = stager(snap.path(), data.path(), 1, 5).run(&store).unwrap();

    assert!(md.is_complete());
    assert!(
        snap.path()
            .join("snapshot_00001")
            .join("metadata.json")
            .exists()
    );
    assert!(!stale.exists(), "the stale staging dir must be reclaimed");
}

/// Post-install non-fatal: a `latest` repoint failure (here `latest` is an
/// occupied directory) does not fail the snapshot — it is already durably
/// installed; only a warning is logged.
#[test]
fn test_stager_symlink_failure_is_nonfatal() {
    let db = TempDir::new().unwrap();
    let store = make_store(db.path());
    let snap = TempDir::new().unwrap();
    let data = TempDir::new().unwrap();

    // Make repointing `latest` fail (it is a non-empty directory).
    std::fs::create_dir_all(snap.path().join("latest").join("inner")).unwrap();

    let md = stager(snap.path(), data.path(), 1, 5).run(&store).unwrap();

    assert!(
        md.is_complete(),
        "snapshot must be installed despite the symlink failure"
    );
    assert!(
        snap.path()
            .join("snapshot_00001")
            .join("metadata.json")
            .exists()
    );
    assert!(!snap.path().join(".snapshot_00001.tmp").exists());
}

/// Consecutive epochs leave a consistent on-disk state: retention keeps the
/// newest `max_snapshots`, evicts the rest, and `latest` tracks the newest.
#[test]
fn test_stager_retention_across_epochs() {
    let db = TempDir::new().unwrap();
    let store = make_store(db.path());
    let snap = TempDir::new().unwrap();
    let data = TempDir::new().unwrap();

    for epoch in 1..=3 {
        stager(snap.path(), data.path(), epoch, 2)
            .run(&store)
            .unwrap();
    }

    assert!(
        !snap.path().join("snapshot_00001").exists(),
        "the oldest snapshot should be evicted by retention"
    );
    assert!(
        snap.path()
            .join("snapshot_00002")
            .join("metadata.json")
            .exists()
    );
    assert!(
        snap.path()
            .join("snapshot_00003")
            .join("metadata.json")
            .exists()
    );
    #[cfg(unix)]
    assert_eq!(
        std::fs::read_link(snap.path().join("latest")).unwrap(),
        Path::new("snapshot_00003")
    );
}

// ---------------------------------------------------------------------------
// SnapshotScheduler — the pure coalesce/reschedule state machine.
//
// Previously this handshake was reachable only through a live
// `RocksSnapshotCoordinator` (real RocksStore + Tokio + fs), so it had *no*
// tests. Extracted as a pure value type, every transition is a synchronous
// unit test — no runtime, no disk. The threaded storm test drives the two race
// windows of `finish_and_maybe_rebegin` (release↔swap, swap↔re-CAS).
// ---------------------------------------------------------------------------

use super::SnapshotRequest;
use super::scheduler::SnapshotScheduler;

/// Begin while idle claims the slot and mints epoch 1.
#[test]
fn test_scheduler_begin_while_idle() {
    let s = SnapshotScheduler::with_epoch(0);
    assert!(!s.in_progress());
    assert_eq!(s.try_begin(), Some(1));
    assert!(s.in_progress());
    assert_eq!(s.current_epoch(), 1);
}

/// `with_epoch` resumes the counter from a recovered epoch.
#[test]
fn test_scheduler_resumes_epoch() {
    let s = SnapshotScheduler::with_epoch(5);
    assert_eq!(s.current_epoch(), 5);
    assert_eq!(s.try_begin(), Some(6));
}

/// Begin while a save is running is rejected (the `AlreadyInProgress` guard).
#[test]
fn test_scheduler_begin_while_running_rejected() {
    let s = SnapshotScheduler::with_epoch(0);
    assert_eq!(s.try_begin(), Some(1));
    assert_eq!(s.try_begin(), None);
    // The rejected begin must not have bumped the epoch.
    assert_eq!(s.current_epoch(), 1);
}

/// A request during a run coalesces; any number of requests fold into one flag.
#[test]
fn test_scheduler_request_while_running_coalesces() {
    let s = SnapshotScheduler::with_epoch(0);
    assert_eq!(s.try_begin(), Some(1));
    assert_eq!(s.request(), SnapshotRequest::Coalesced);
    assert!(s.is_scheduled());
    assert_eq!(s.request(), SnapshotRequest::Coalesced);
    assert!(s.is_scheduled());
    // Coalesced requests never advance the epoch on their own.
    assert_eq!(s.current_epoch(), 1);
}

/// Pin the deliberate plain-BGSAVE vs BGSAVE-SCHEDULE distinction through the one
/// `request_mode` seam: `Immediate` while a save runs reports `AlreadyRunning`
/// WITHOUT arming a follow-up (the no-queue regression guard), whereas `Schedule`
/// reports `Coalesced` and DOES arm one. A naive unification that routed plain
/// BGSAVE through the coalescing path would silently queue an extra snapshot —
/// this test is the synchronous guard against exactly that.
#[test]
fn test_scheduler_request_mode_immediate_no_queue_vs_schedule_arms() {
    let s = SnapshotScheduler::with_epoch(0);
    // Idle: either mode starts a fresh save.
    assert_eq!(
        s.request_mode(SnapshotMode::Immediate),
        SnapshotRequest::Started(1)
    );

    // Immediate while running: already-running, and NOTHING is queued.
    assert_eq!(
        s.request_mode(SnapshotMode::Immediate),
        SnapshotRequest::AlreadyRunning
    );
    assert!(
        !s.is_scheduled(),
        "Immediate must not arm a follow-up — plain BGSAVE never queues"
    );

    // Schedule while running: coalesced, and a follow-up IS armed.
    assert_eq!(
        s.request_mode(SnapshotMode::Schedule),
        SnapshotRequest::Coalesced
    );
    assert!(
        s.is_scheduled(),
        "Schedule must arm the coalesced follow-up"
    );
}

/// Finish with a pending reschedule re-runs exactly once (the double-CAS loop).
#[test]
fn test_scheduler_finish_with_pending_reschedule_reruns_once() {
    let s = SnapshotScheduler::with_epoch(0);
    assert_eq!(s.try_begin(), Some(1));
    assert_eq!(s.request(), SnapshotRequest::Coalesced);
    assert!(s.is_scheduled());

    // Pending reschedule → rebegin at epoch 2, and the flag is consumed.
    assert_eq!(s.finish_and_maybe_rebegin(), Some(2));
    assert!(
        !s.is_scheduled(),
        "the schedule flag is cleared by the rebegin"
    );
    assert!(s.in_progress());

    // The follow-up run itself had no further requests → next finish idles.
    assert_eq!(s.finish_and_maybe_rebegin(), None);
    assert!(!s.in_progress());
    assert_eq!(s.current_epoch(), 2);
}

/// Finish with nothing scheduled goes idle (no phantom rerun).
#[test]
fn test_scheduler_finish_without_schedule_idles() {
    let s = SnapshotScheduler::with_epoch(0);
    assert_eq!(s.try_begin(), Some(1));
    assert_eq!(s.finish_and_maybe_rebegin(), None);
    assert!(!s.in_progress());
    assert_eq!(s.current_epoch(), 1);
}

/// A request after everything idles starts a fresh save (not a phantom coalesce).
#[test]
fn test_scheduler_request_after_finish_starts() {
    let s = SnapshotScheduler::with_epoch(0);
    assert_eq!(s.try_begin(), Some(1));
    assert_eq!(s.finish_and_maybe_rebegin(), None);
    assert_eq!(s.request(), SnapshotRequest::Started(2));
    assert!(s.in_progress());
}

/// Pin the exact former lost-wakeup window. A requester observed
/// `in_progress == true`, but by the time it arms the follow-up the runner has
/// already finished and consumed nothing (its `scheduled.swap` ran before the
/// arm). `arm_follow_up` is entered with the runner *already exited* — the
/// precise interleaving the black-box `request` re-load would hide.
///
/// Old behaviour: `arm_follow_up` would leave `scheduled == true` with no runner
/// and report `Coalesced` — a lost wakeup. Fixed behaviour: it detects the
/// exited runner, reclaims the flag, and `Started`s the guaranteed post-request
/// run, leaving the schedule flag clear.
#[test]
fn test_scheduler_arm_follow_up_after_runner_exit_starts() {
    let s = SnapshotScheduler::with_epoch(0);
    // A save ran and finished, consuming nothing (the window: our observe-running
    // happened-before, its finish landed before our arm).
    assert_eq!(s.try_begin(), Some(1));
    assert_eq!(s.finish_and_maybe_rebegin(), None);
    assert!(!s.in_progress());

    // Now the delayed arm lands. It must not lose the wakeup: it starts the run.
    assert_eq!(s.arm_follow_up(), SnapshotRequest::Started(2));
    assert!(s.in_progress(), "the reclaiming request owns the slot");
    assert!(
        !s.is_scheduled(),
        "no dangling schedule flag: the wakeup became a real run"
    );

    // And that run finishes cleanly with nothing left armed.
    assert_eq!(s.finish_and_maybe_rebegin(), None);
    assert!(!s.in_progress());
    assert!(!s.is_scheduled());
}

/// The legacy `schedule()` protocol only arms a follow-up while a save runs.
#[test]
fn test_scheduler_schedule_only_while_running() {
    let s = SnapshotScheduler::with_epoch(0);
    assert!(!s.schedule(), "schedule while idle must be refused");
    assert!(!s.is_scheduled());
    assert_eq!(s.try_begin(), Some(1));
    assert!(s.schedule());
    assert!(s.is_scheduled());
}

/// Relocated from the (deleted) `NoopSnapshotCoordinator::schedule_snapshot`
/// trait-surface tests: `schedule()` on an idle scheduler is refused.
#[test]
fn test_schedule_false() {
    let s = SnapshotScheduler::with_epoch(0);
    assert!(!s.schedule());
}

/// Relocated: `schedule()` while a save runs arms the coalesced follow-up.
#[test]
fn test_schedule_true() {
    let s = SnapshotScheduler::with_epoch(0);
    assert_eq!(s.try_begin(), Some(1));
    assert!(s.schedule());
    assert!(s.is_scheduled());
}

/// Epochs are monotonic across a full begin → coalesce → rebegin → begin cycle.
#[test]
fn test_scheduler_epoch_monotonic_across_cycle() {
    let s = SnapshotScheduler::with_epoch(0);
    assert_eq!(s.try_begin(), Some(1));
    assert_eq!(s.request(), SnapshotRequest::Coalesced);
    assert_eq!(s.finish_and_maybe_rebegin(), Some(2));
    assert_eq!(s.finish_and_maybe_rebegin(), None);
    assert_eq!(s.try_begin(), Some(3));
}

/// Concurrent request storm: whichever thread wins `Started` owns the run and
/// drains coalesced follow-ups; every other request folds in. Across many
/// randomized interleavings the invariants must hold:
///   * every epoch that runs is unique and contiguous from 1 (no skip/reuse),
///   * exactly one runner is ever active (the slot is never double-claimed),
///   * the scheduler is fully idle at quiescence — `in_progress` released *and*
///     no follow-up left armed (the closed lost-wakeup guarantee: a request that
///     coalesced never strands `scheduled == true` with no runner to drain it).
///
/// This is the only place the `finish_and_maybe_rebegin` re-CAS-failure branch
/// (another thread stole the slot in the release↔re-CAS window) is exercised.
#[test]
fn test_scheduler_concurrent_request_storm() {
    use std::sync::{Arc, Mutex};
    use std::thread;

    for _ in 0..500 {
        let sched = Arc::new(SnapshotScheduler::with_epoch(0));
        let ran = Arc::new(Mutex::new(Vec::<u64>::new()));

        let threads: Vec<_> = (0..6)
            .map(|_| {
                let sched = sched.clone();
                let ran = ran.clone();
                thread::spawn(move || {
                    // Model the production wiring: `Started` ⇒ this caller owns
                    // the run loop and drains reschedules; `Coalesced` ⇒ folded in.
                    if let SnapshotRequest::Started(mut epoch) = sched.request() {
                        loop {
                            ran.lock().unwrap().push(epoch);
                            match sched.finish_and_maybe_rebegin() {
                                None => break,
                                Some(next) => epoch = next,
                            }
                        }
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }

        assert!(!sched.in_progress(), "slot must be released at quiescence");
        assert!(
            !sched.is_scheduled(),
            "no follow-up may be left armed at quiescence: a coalesced request \
             must never strand `scheduled == true` with no runner (lost wakeup)"
        );
        let mut ran = ran.lock().unwrap().clone();
        assert!(!ran.is_empty(), "at least one run must happen");
        ran.sort_unstable();
        for (i, epoch) in ran.iter().enumerate() {
            assert_eq!(
                *epoch,
                i as u64 + 1,
                "epochs must be unique + contiguous from 1: {ran:?}"
            );
        }
        // Every allocated epoch corresponds to a completed run.
        assert_eq!(sched.current_epoch(), *ran.last().unwrap());
    }
}
