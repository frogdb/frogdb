//! Persistence micro-benchmarks.
//!
//! Benchmarks for:
//! - WAL write operations with different durability modes
//! - Value serialization/deserialization
//! - RocksDB operations

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use frogdb_core::persistence::{
    deserialize, serialize, DurabilityMode, RocksConfig, RocksStore, RocksWalWriter, WalConfig,
};
use frogdb_core::types::{HashValue, KeyMetadata, ListValue, SortedSetValue, StringValue, Value};
use frogdb_core::NoopMetricsRecorder;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::runtime::Runtime;

// ============================================================================
// Serialization Benchmarks
// ============================================================================

fn bench_serialize_string(c: &mut Criterion) {
    let mut group = c.benchmark_group("persistence/serialize/string");

    for value_size in [16, 128, 1024, 8192] {
        let data: Vec<u8> = (0..value_size).map(|i| (i % 256) as u8).collect();
        let value = Value::String(StringValue::new(Bytes::from(data)));
        let metadata = KeyMetadata::new(value_size);

        group.throughput(Throughput::Bytes(value_size as u64));
        group.bench_with_input(
            BenchmarkId::new("value_bytes", value_size),
            &value_size,
            |b, _| {
                b.iter(|| {
                    black_box(serialize(&value, &metadata));
                });
            },
        );
    }

    group.finish();
}

fn bench_deserialize_string(c: &mut Criterion) {
    let mut group = c.benchmark_group("persistence/deserialize/string");

    for value_size in [16, 128, 1024, 8192] {
        let data: Vec<u8> = (0..value_size).map(|i| (i % 256) as u8).collect();
        let value = Value::String(StringValue::new(Bytes::from(data)));
        let metadata = KeyMetadata::new(value_size);
        let serialized = serialize(&value, &metadata);

        group.throughput(Throughput::Bytes(value_size as u64));
        group.bench_with_input(
            BenchmarkId::new("value_bytes", value_size),
            &value_size,
            |b, _| {
                b.iter(|| {
                    black_box(deserialize(&serialized).unwrap());
                });
            },
        );
    }

    group.finish();
}

fn bench_serialize_sorted_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("persistence/serialize/sorted_set");

    for zset_size in [10, 100, 1000] {
        let mut zset = SortedSetValue::new();
        for i in 0..zset_size {
            zset.add(Bytes::from(format!("member:{:08}", i)), i as f64);
        }
        let value = Value::SortedSet(zset);
        let metadata = KeyMetadata::new(value.memory_size());

        group.throughput(Throughput::Elements(zset_size as u64));
        group.bench_with_input(
            BenchmarkId::new("members", zset_size),
            &zset_size,
            |b, _| {
                b.iter(|| {
                    black_box(serialize(&value, &metadata));
                });
            },
        );
    }

    group.finish();
}

fn bench_deserialize_sorted_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("persistence/deserialize/sorted_set");

    for zset_size in [10, 100, 1000] {
        let mut zset = SortedSetValue::new();
        for i in 0..zset_size {
            zset.add(Bytes::from(format!("member:{:08}", i)), i as f64);
        }
        let value = Value::SortedSet(zset);
        let metadata = KeyMetadata::new(value.memory_size());
        let serialized = serialize(&value, &metadata);

        group.throughput(Throughput::Elements(zset_size as u64));
        group.bench_with_input(
            BenchmarkId::new("members", zset_size),
            &zset_size,
            |b, _| {
                b.iter(|| {
                    black_box(deserialize(&serialized).unwrap());
                });
            },
        );
    }

    group.finish();
}

fn bench_serialize_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("persistence/serialize/hash");

    for hash_size in [10, 100, 1000] {
        let mut hash = HashValue::new();
        for i in 0..hash_size {
            hash.set(
                Bytes::from(format!("field:{}", i)),
                Bytes::from(format!("value:{}", i)),
            );
        }
        let value = Value::Hash(hash);
        let metadata = KeyMetadata::new(value.memory_size());

        group.throughput(Throughput::Elements(hash_size as u64));
        group.bench_with_input(BenchmarkId::new("fields", hash_size), &hash_size, |b, _| {
            b.iter(|| {
                black_box(serialize(&value, &metadata));
            });
        });
    }

    group.finish();
}

fn bench_serialize_list(c: &mut Criterion) {
    let mut group = c.benchmark_group("persistence/serialize/list");

    for list_size in [10, 100, 1000] {
        let mut list = ListValue::new();
        for i in 0..list_size {
            list.push_back(Bytes::from(format!("element:{}", i)));
        }
        let value = Value::List(list);
        let metadata = KeyMetadata::new(value.memory_size());

        group.throughput(Throughput::Elements(list_size as u64));
        group.bench_with_input(
            BenchmarkId::new("elements", list_size),
            &list_size,
            |b, _| {
                b.iter(|| {
                    black_box(serialize(&value, &metadata));
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// WAL Write Benchmarks
// ============================================================================

fn bench_wal_write_async(c: &mut Criterion) {
    let mut group = c.benchmark_group("persistence/wal/async");

    let rt = Runtime::new().unwrap();

    for value_size in [128, 1024, 8192] {
        let data: Vec<u8> = (0..value_size).map(|i| (i % 256) as u8).collect();
        let value = Value::String(StringValue::new(Bytes::from(data)));
        let metadata = KeyMetadata::new(value_size);

        group.throughput(Throughput::Bytes(value_size as u64));
        group.bench_with_input(
            BenchmarkId::new("value_bytes", value_size),
            &value_size,
            |b, _| {
                let tmp = TempDir::new().unwrap();
                let rocks =
                    Arc::new(RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap());
                let metrics = Arc::new(NoopMetricsRecorder::new());

                let wal = RocksWalWriter::new(
                    rocks.clone(),
                    0,
                    WalConfig {
                        mode: DurabilityMode::Async,
                        batch_size_threshold: 64 * 1024 * 1024, // Large threshold to avoid auto-flush
                        batch_timeout_ms: 60000,                // Long timeout
                    },
                    metrics,
                );

                let mut counter = 0u64;
                b.iter(|| {
                    let key = format!("key:{}", counter);
                    counter += 1;
                    rt.block_on(async {
                        black_box(wal.write_set(key.as_bytes(), &value, &metadata).await)
                    })
                    .unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_wal_write_sync(c: &mut Criterion) {
    let mut group = c.benchmark_group("persistence/wal/sync");
    group.sample_size(30); // Reduce sample size for slow sync operations

    let rt = Runtime::new().unwrap();

    for value_size in [128, 1024] {
        let data: Vec<u8> = (0..value_size).map(|i| (i % 256) as u8).collect();
        let value = Value::String(StringValue::new(Bytes::from(data)));
        let metadata = KeyMetadata::new(value_size);

        group.throughput(Throughput::Bytes(value_size as u64));
        group.bench_with_input(
            BenchmarkId::new("value_bytes", value_size),
            &value_size,
            |b, _| {
                let tmp = TempDir::new().unwrap();
                let rocks =
                    Arc::new(RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap());
                let metrics = Arc::new(NoopMetricsRecorder::new());

                let wal = RocksWalWriter::new(
                    rocks.clone(),
                    0,
                    WalConfig {
                        mode: DurabilityMode::Sync,
                        batch_size_threshold: 1, // Flush every write
                        batch_timeout_ms: 0,
                    },
                    metrics,
                );

                let mut counter = 0u64;
                b.iter(|| {
                    let key = format!("key:{}", counter);
                    counter += 1;
                    rt.block_on(async {
                        black_box(wal.write_set(key.as_bytes(), &value, &metadata).await)
                    })
                    .unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_wal_write_periodic(c: &mut Criterion) {
    let mut group = c.benchmark_group("persistence/wal/periodic");

    let rt = Runtime::new().unwrap();

    for value_size in [128, 1024, 8192] {
        let data: Vec<u8> = (0..value_size).map(|i| (i % 256) as u8).collect();
        let value = Value::String(StringValue::new(Bytes::from(data)));
        let metadata = KeyMetadata::new(value_size);

        group.throughput(Throughput::Bytes(value_size as u64));
        group.bench_with_input(
            BenchmarkId::new("value_bytes", value_size),
            &value_size,
            |b, _| {
                let tmp = TempDir::new().unwrap();
                let rocks =
                    Arc::new(RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap());
                let metrics = Arc::new(NoopMetricsRecorder::new());

                let wal = RocksWalWriter::new(
                    rocks.clone(),
                    0,
                    WalConfig {
                        mode: DurabilityMode::Periodic { interval_ms: 1000 },
                        batch_size_threshold: 64 * 1024 * 1024,
                        batch_timeout_ms: 10,
                    },
                    metrics,
                );

                let mut counter = 0u64;
                b.iter(|| {
                    let key = format!("key:{}", counter);
                    counter += 1;
                    rt.block_on(async {
                        black_box(wal.write_set(key.as_bytes(), &value, &metadata).await)
                    })
                    .unwrap();
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// RocksDB Direct Operations
// ============================================================================

fn bench_rocks_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("persistence/rocks/put");

    for value_size in [128, 1024, 8192] {
        let data: Vec<u8> = (0..value_size).map(|i| (i % 256) as u8).collect();

        group.throughput(Throughput::Bytes(value_size as u64));
        group.bench_with_input(
            BenchmarkId::new("value_bytes", value_size),
            &value_size,
            |b, _| {
                let tmp = TempDir::new().unwrap();
                let rocks = RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap();

                let mut counter = 0u64;
                b.iter(|| {
                    let key = format!("key:{}", counter);
                    counter += 1;
                    black_box(rocks.put(0, key.as_bytes(), &data)).unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_rocks_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("persistence/rocks/get");

    for num_keys in [100, 1000, 10000] {
        let data: Vec<u8> = (0..128).map(|i| (i % 256) as u8).collect();

        let tmp = TempDir::new().unwrap();
        let rocks = RocksStore::open(tmp.path(), 1, &RocksConfig::default()).unwrap();

        // Pre-populate
        for i in 0..num_keys {
            rocks
                .put(0, format!("key:{:08}", i).as_bytes(), &data)
                .unwrap();
        }

        let key = format!("key:{:08}", num_keys / 2);

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("keys", num_keys), &num_keys, |b, _| {
            b.iter(|| {
                black_box(rocks.get(0, key.as_bytes())).unwrap();
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    // Serialization
    bench_serialize_string,
    bench_deserialize_string,
    bench_serialize_sorted_set,
    bench_deserialize_sorted_set,
    bench_serialize_hash,
    bench_serialize_list,
    // WAL
    bench_wal_write_async,
    bench_wal_write_sync,
    bench_wal_write_periodic,
    // RocksDB direct
    bench_rocks_put,
    bench_rocks_get,
);

criterion_main!(benches);
