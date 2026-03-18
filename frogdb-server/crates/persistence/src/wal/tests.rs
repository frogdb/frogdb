//! Tests for WAL module.
use super::*;
use crate::rocks::RocksConfig;
use crate::serialization::serialize;
use frogdb_types::traits::NoopMetricsRecorder;
use frogdb_types::types::{KeyMetadata, Value};
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn test_wal_write_and_flush() {
    let tmp = TempDir::new().unwrap();
    let rocks =
        Arc::new(crate::rocks::RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
    let m = Arc::new(NoopMetricsRecorder::new());
    let wal = RocksWalWriter::new(
        rocks.clone(),
        0,
        WalConfig {
            mode: DurabilityMode::Async,
            batch_size_threshold: 1024 * 1024,
            batch_timeout_ms: 1000,
            ..Default::default()
        },
        m,
    );
    let v = Value::string("test_value");
    let md = KeyMetadata::new(10);
    assert_eq!(wal.write_set(b"key1", &v, &md).await.unwrap(), 1);
    wal.flush_async().await.unwrap();
    assert!(rocks.get(0, b"key1").unwrap().is_some());
}
#[tokio::test]
async fn test_wal_delete() {
    let tmp = TempDir::new().unwrap();
    let rocks =
        Arc::new(crate::rocks::RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
    let v = Value::string("test");
    let md = KeyMetadata::new(4);
    rocks.put(0, b"key", &serialize(&v, &md)).unwrap();
    let m = Arc::new(NoopMetricsRecorder::new());
    let wal = RocksWalWriter::new(rocks.clone(), 0, WalConfig::default(), m);
    assert_eq!(wal.write_delete(b"key").await.unwrap(), 1);
    wal.flush_async().await.unwrap();
    assert!(rocks.get(0, b"key").unwrap().is_none());
}
#[tokio::test]
async fn test_wal_batch_threshold() {
    let tmp = TempDir::new().unwrap();
    let rocks =
        Arc::new(crate::rocks::RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
    let m = Arc::new(NoopMetricsRecorder::new());
    let wal = RocksWalWriter::new(
        rocks.clone(),
        0,
        WalConfig {
            mode: DurabilityMode::Async,
            batch_size_threshold: 100,
            batch_timeout_ms: 60000,
            ..Default::default()
        },
        m,
    );
    wal.write_set(
        b"bigkey",
        &Value::string("x".repeat(200)),
        &KeyMetadata::new(200),
    )
    .await
    .unwrap();
    wal.flush_async().await.unwrap();
    assert!(rocks.get(0, b"bigkey").unwrap().is_some());
}
#[tokio::test]
async fn test_wal_sequence() {
    let tmp = TempDir::new().unwrap();
    let rocks =
        Arc::new(crate::rocks::RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
    let m = Arc::new(NoopMetricsRecorder::new());
    let wal = RocksWalWriter::new(rocks, 0, WalConfig::default(), m);
    assert_eq!(wal.sequence(), 0);
    let v = Value::string("v");
    let md = KeyMetadata::new(1);
    assert_eq!(wal.write_set(b"k1", &v, &md).await.unwrap(), 1);
    assert_eq!(wal.write_set(b"k2", &v, &md).await.unwrap(), 2);
    assert_eq!(wal.write_delete(b"k1").await.unwrap(), 3);
    assert_eq!(wal.sequence(), 3);
}
#[tokio::test]
async fn test_wal_drop_flushes_pending() {
    let tmp = TempDir::new().unwrap();
    let rocks =
        Arc::new(crate::rocks::RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
    let m = Arc::new(NoopMetricsRecorder::new());
    {
        let wal = RocksWalWriter::new(
            rocks.clone(),
            0,
            WalConfig {
                mode: DurabilityMode::Async,
                batch_size_threshold: 1024 * 1024,
                batch_timeout_ms: 60000,
                ..Default::default()
            },
            m,
        );
        wal.write_set(
            b"dropkey",
            &Value::string("drop_test"),
            &KeyMetadata::new(9),
        )
        .await
        .unwrap();
    }
    assert!(rocks.get(0, b"dropkey").unwrap().is_some());
}
#[tokio::test]
async fn test_wal_backpressure_no_data_loss() {
    let tmp = TempDir::new().unwrap();
    let rocks =
        Arc::new(crate::rocks::RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
    let m = Arc::new(NoopMetricsRecorder::new());
    let wal = RocksWalWriter::new(
        rocks.clone(),
        0,
        WalConfig {
            mode: DurabilityMode::Async,
            batch_size_threshold: 1024 * 1024,
            batch_timeout_ms: 60000,
            channel_capacity: 1,
            ..Default::default()
        },
        m,
    );
    let v = Value::string("bp");
    let md = KeyMetadata::new(2);
    for i in 0..50 {
        wal.write_set(format!("bpkey{i}").as_bytes(), &v, &md)
            .await
            .unwrap();
    }
    wal.flush_async().await.unwrap();
    for i in 0..50 {
        assert!(
            rocks
                .get(0, format!("bpkey{i}").as_bytes())
                .unwrap()
                .is_some()
        );
    }
}
#[tokio::test]
async fn test_wal_lag_stats_sync() {
    let tmp = TempDir::new().unwrap();
    let rocks =
        Arc::new(crate::rocks::RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap());
    let m = Arc::new(NoopMetricsRecorder::new());
    let wal = RocksWalWriter::new(rocks, 0, WalConfig::default(), m);
    let s = wal.lag_stats();
    assert_eq!(s.shard_id, 0);
    assert_eq!(s.sequence, 0);
}
