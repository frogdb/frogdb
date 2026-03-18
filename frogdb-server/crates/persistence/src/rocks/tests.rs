use super::*;
use rocksdb::WriteBatch;
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
