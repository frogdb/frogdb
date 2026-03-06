//! Store micro-benchmarks.
//!
//! Benchmarks for HashMapStore operations at various store sizes and value sizes.

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use frogdb_core::store::{HashMapStore, Store};
use frogdb_core::types::Value;
use rand::Rng;

/// Generate a random key of the given length.
#[allow(dead_code)]
fn random_key(len: usize) -> Bytes {
    let mut rng = rand::thread_rng();
    let key: Vec<u8> = (0..len).map(|_| rng.gen_range(b'a'..=b'z')).collect();
    Bytes::from(key)
}

/// Generate a random value of the given size.
fn random_value(size: usize) -> Value {
    let mut rng = rand::thread_rng();
    let data: Vec<u8> = (0..size).map(|_| rng.r#gen()).collect();
    Value::string(Bytes::from(data))
}

/// Pre-populate a store with N keys.
fn populate_store(n: usize, value_size: usize) -> (HashMapStore, Vec<Bytes>) {
    let mut store = HashMapStore::new();
    let mut keys = Vec::with_capacity(n);

    for i in 0..n {
        let key = Bytes::from(format!("key:{:08}", i));
        let value = random_value(value_size);
        store.set(key.clone(), value);
        keys.push(key);
    }

    (store, keys)
}

/// Benchmark GET operations at various store sizes.
fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("store/get");

    for store_size in [100, 1_000, 10_000, 100_000] {
        let (store, keys) = populate_store(store_size, 128);
        let key = &keys[store_size / 2]; // Middle key for consistent access

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("keys", store_size), &store_size, |b, _| {
            b.iter(|| {
                black_box(store.get(key));
            });
        });
    }

    group.finish();
}

/// Benchmark SET operations with various value sizes.
fn bench_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("store/set");

    for value_size in [16, 128, 1024, 8192] {
        group.throughput(Throughput::Bytes(value_size as u64));
        group.bench_with_input(
            BenchmarkId::new("value_bytes", value_size),
            &value_size,
            |b, &size| {
                let mut store = HashMapStore::new();
                let mut counter = 0u64;
                let value = random_value(size);

                b.iter(|| {
                    let key = Bytes::from(format!("key:{}", counter));
                    counter += 1;
                    black_box(store.set(key, value.clone()));
                });
            },
        );
    }

    group.finish();
}

/// Benchmark SET operations (update existing key).
fn bench_set_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("store/set_update");

    for store_size in [100, 1_000, 10_000] {
        let (mut store, keys) = populate_store(store_size, 128);
        let key = keys[store_size / 2].clone();
        let new_value = random_value(128);

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("keys", store_size), &store_size, |b, _| {
            b.iter(|| {
                black_box(store.set(key.clone(), new_value.clone()));
            });
        });
    }

    group.finish();
}

/// Benchmark DELETE operations.
fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("store/delete");

    for store_size in [1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("keys", store_size),
            &store_size,
            |b, &size| {
                b.iter_batched(
                    || {
                        // Setup: create store and pick a key to delete
                        let (store, keys) = populate_store(size, 128);
                        let key = keys[size / 2].clone();
                        (store, key)
                    },
                    |(mut store, key)| {
                        black_box(store.delete(&key));
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark CONTAINS operations.
fn bench_contains(c: &mut Criterion) {
    let mut group = c.benchmark_group("store/contains");

    for store_size in [100, 1_000, 10_000, 100_000] {
        let (store, keys) = populate_store(store_size, 128);
        let existing_key = &keys[store_size / 2];
        let missing_key = Bytes::from("nonexistent:key");

        group.throughput(Throughput::Elements(1));

        group.bench_with_input(
            BenchmarkId::new("hit/keys", store_size),
            &store_size,
            |b, _| {
                b.iter(|| {
                    black_box(store.contains(existing_key));
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("miss/keys", store_size),
            &store_size,
            |b, _| {
                b.iter(|| {
                    black_box(store.contains(&missing_key));
                });
            },
        );
    }

    group.finish();
}

/// Benchmark get_with_expiry_check (lazy expiry).
fn bench_get_with_expiry_check(c: &mut Criterion) {
    let mut group = c.benchmark_group("store/get_with_expiry_check");

    for store_size in [100, 1_000, 10_000] {
        let (mut store, keys) = populate_store(store_size, 128);
        let key = keys[store_size / 2].clone();

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("keys", store_size), &store_size, |b, _| {
            b.iter(|| {
                black_box(store.get_with_expiry_check(&key));
            });
        });
    }

    group.finish();
}

/// Benchmark SCAN operation.
fn bench_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("store/scan");

    for store_size in [1_000, 10_000, 100_000] {
        let (store, _) = populate_store(store_size, 128);

        group.throughput(Throughput::Elements(10)); // Scan returns 10 keys per call

        // Scan without pattern
        group.bench_with_input(
            BenchmarkId::new("no_pattern/keys", store_size),
            &store_size,
            |b, _| {
                b.iter(|| {
                    black_box(store.scan(0, 10, None));
                });
            },
        );

        // Scan with pattern
        group.bench_with_input(
            BenchmarkId::new("with_pattern/keys", store_size),
            &store_size,
            |b, _| {
                b.iter(|| {
                    black_box(store.scan(0, 10, Some(b"key:0000*")));
                });
            },
        );
    }

    group.finish();
}

/// Benchmark sample_keys (for eviction).
fn bench_sample_keys(c: &mut Criterion) {
    let mut group = c.benchmark_group("store/sample_keys");

    for store_size in [1_000, 10_000, 100_000] {
        let (store, _) = populate_store(store_size, 128);

        for sample_size in [5, 16] {
            group.throughput(Throughput::Elements(sample_size as u64));
            group.bench_with_input(
                BenchmarkId::new(format!("sample_{}/keys", sample_size), store_size),
                &store_size,
                |b, _| {
                    b.iter(|| {
                        black_box(store.sample_keys(sample_size));
                    });
                },
            );
        }
    }

    group.finish();
}

/// Benchmark mixed read/write workload.
fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("store/mixed");

    // 90% reads, 10% writes
    for store_size in [1_000, 10_000] {
        let (mut store, keys) = populate_store(store_size, 128);

        group.throughput(Throughput::Elements(100)); // 100 operations per iteration
        group.bench_with_input(
            BenchmarkId::new("90r_10w/keys", store_size),
            &store_size,
            |b, &size| {
                let mut rng = rand::thread_rng();
                let value = random_value(128);

                b.iter(|| {
                    for _ in 0..90 {
                        let idx = rng.gen_range(0..size);
                        black_box(store.get(&keys[idx]));
                    }
                    for _ in 0..10 {
                        let idx = rng.gen_range(0..size);
                        black_box(store.set(keys[idx].clone(), value.clone()));
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_get,
    bench_set,
    bench_set_update,
    bench_delete,
    bench_contains,
    bench_get_with_expiry_check,
    bench_scan,
    bench_sample_keys,
    bench_mixed_workload,
);

criterion_main!(benches);
