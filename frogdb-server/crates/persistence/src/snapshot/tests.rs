use super::*;
use std::sync::atomic::Ordering;
use std::time::Duration;
#[test]
fn test_noop_coordinator() {
    let c = NoopSnapshotCoordinator::new();
    assert!(c.last_save_time().is_none());
    assert!(!c.in_progress());
    let h = c.start_snapshot().unwrap();
    assert!(c.in_progress());
    assert!(c.last_save_time().is_some());
    assert_eq!(h.epoch(), 1);
    drop(h);
    assert!(!c.in_progress());
}
#[test]
fn test_noop_rejects_concurrent() {
    let c = NoopSnapshotCoordinator::new();
    let _h = c.start_snapshot().unwrap();
    assert!(matches!(
        c.start_snapshot(),
        Err(SnapshotError::AlreadyInProgress)
    ));
}
#[test]
fn test_handle_complete() {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    let d = Arc::new(AtomicBool::new(false));
    let d2 = d.clone();
    let h = SnapshotHandle::new(1, move || {
        d2.store(true, Ordering::SeqCst);
    });
    assert!(!d.load(Ordering::SeqCst));
    h.complete();
    assert!(d.load(Ordering::SeqCst));
}
#[test]
fn test_handle_noop() {
    let h = SnapshotHandle::noop();
    assert!(h.is_noop());
    assert_eq!(h.epoch(), 0);
    drop(h);
}
#[test]
fn test_noop_metadata() {
    let c = NoopSnapshotCoordinator::new();
    assert!(c.last_snapshot_metadata().is_none());
    drop(c.start_snapshot().unwrap());
    let m = c.last_snapshot_metadata().unwrap();
    assert_eq!(m.epoch, 1);
    assert!(m.completed_at.is_some());
}
#[test]
fn test_schedule_false() {
    let c = NoopSnapshotCoordinator::new();
    assert!(!c.schedule_snapshot());
}
#[test]
fn test_schedule_true() {
    let c = NoopSnapshotCoordinator::new();
    let _h = c.start_snapshot().unwrap();
    assert!(c.schedule_snapshot());
    assert!(c.is_scheduled());
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
    m.mark_complete(1000, 5_000_000);
    assert!(m.is_complete());
    assert_eq!(m.num_keys, 1000);
}
#[test]
fn test_metadata_to_metadata() {
    let mut mf = SnapshotMetadataFile::new(5, 99999, 8);
    mf.mark_complete(5000, 10_000_000);
    let m = mf.to_metadata();
    assert_eq!(m.epoch, 5);
    assert!(m.started_at.elapsed().unwrap() < Duration::from_secs(1));
}
#[test]
fn test_metadata_serialization() {
    let mut m = SnapshotMetadataFile::new(3, 54321, 2);
    m.mark_complete(100, 1_000_000);
    let j = serde_json::to_string(&m).unwrap();
    let d: SnapshotMetadataFile = serde_json::from_str(&j).unwrap();
    assert_eq!(d.epoch, 3);
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
    RocksSnapshotCoordinator::cleanup_old_snapshots(&td, 3).unwrap();
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
    RocksSnapshotCoordinator::cleanup_old_snapshots(&td, 0).unwrap();
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
