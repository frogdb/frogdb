//! Command/value type micro-benchmarks.
//!
//! Benchmarks for operations on different Redis data types:
//! - String: GET, SET, INCR
//! - Hash: HGET, HSET, HGETALL
//! - List: LPUSH, RPUSH, LPOP, LRANGE
//! - Set: SADD, SMEMBERS, SINTER
//! - Sorted Set: ZADD, ZRANGE, ZRANGEBYSCORE

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use frogdb_core::store::{HashMapStore, Store};
use frogdb_core::types::{
    HashValue, ListValue, ScoreBound, SetValue, SortedSetValue, StringValue, Value,
};
use rand::Rng;

// ============================================================================
// String Operations
// ============================================================================

fn bench_string_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("commands/string/get");

    for value_size in [16, 128, 1024, 8192] {
        let mut store = HashMapStore::new();
        let data: Vec<u8> = (0..value_size).map(|i| (i % 256) as u8).collect();
        let value = Value::String(StringValue::new(Bytes::from(data)));
        store.set(Bytes::from("key"), value);

        group.throughput(Throughput::Bytes(value_size as u64));
        group.bench_with_input(
            BenchmarkId::new("value_bytes", value_size),
            &value_size,
            |b, _| {
                b.iter(|| {
                    black_box(store.get(b"key"));
                });
            },
        );
    }

    group.finish();
}

fn bench_string_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("commands/string/set");

    for value_size in [16, 128, 1024, 8192] {
        let data: Vec<u8> = (0..value_size).map(|i| (i % 256) as u8).collect();
        let value = Value::String(StringValue::new(Bytes::from(data)));

        group.throughput(Throughput::Bytes(value_size as u64));
        group.bench_with_input(
            BenchmarkId::new("value_bytes", value_size),
            &value_size,
            |b, _| {
                let mut store = HashMapStore::new();
                let mut counter = 0u64;

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

fn bench_string_incr(c: &mut Criterion) {
    let mut group = c.benchmark_group("commands/string/incr");

    group.throughput(Throughput::Elements(1));
    group.bench_function("integer", |b| {
        let mut sv = StringValue::from_integer(0);

        b.iter(|| {
            black_box(sv.increment(1).unwrap());
        });
    });

    group.finish();
}

// ============================================================================
// Hash Operations
// ============================================================================

fn bench_hash_hset(c: &mut Criterion) {
    let mut group = c.benchmark_group("commands/hash/hset");

    for hash_size in [10, 100, 1000] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("fields", hash_size),
            &hash_size,
            |b, &size| {
                let mut hash = HashValue::new();
                // Pre-populate with some fields
                for i in 0..size {
                    hash.set(Bytes::from(format!("field:{}", i)), Bytes::from("value"));
                }

                let mut counter = 0u64;
                b.iter(|| {
                    let field = Bytes::from(format!("newfield:{}", counter));
                    counter += 1;
                    black_box(hash.set(field, Bytes::from("newvalue")));
                });
            },
        );
    }

    group.finish();
}

fn bench_hash_hget(c: &mut Criterion) {
    let mut group = c.benchmark_group("commands/hash/hget");

    for hash_size in [10, 100, 1000] {
        let mut hash = HashValue::new();
        for i in 0..hash_size {
            hash.set(Bytes::from(format!("field:{}", i)), Bytes::from("value"));
        }
        let field = Bytes::from(format!("field:{}", hash_size / 2));

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("fields", hash_size), &hash_size, |b, _| {
            b.iter(|| {
                black_box(hash.get(&field));
            });
        });
    }

    group.finish();
}

fn bench_hash_hgetall(c: &mut Criterion) {
    let mut group = c.benchmark_group("commands/hash/hgetall");

    for hash_size in [10, 100, 1000] {
        let mut hash = HashValue::new();
        for i in 0..hash_size {
            hash.set(Bytes::from(format!("field:{}", i)), Bytes::from("value"));
        }

        group.throughput(Throughput::Elements(hash_size as u64));
        group.bench_with_input(BenchmarkId::new("fields", hash_size), &hash_size, |b, _| {
            b.iter(|| {
                let result: Vec<_> = hash.iter().collect();
                black_box(result);
            });
        });
    }

    group.finish();
}

// ============================================================================
// List Operations
// ============================================================================

fn bench_list_lpush(c: &mut Criterion) {
    let mut group = c.benchmark_group("commands/list/lpush");

    for list_size in [0, 100, 10000] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("initial_size", list_size),
            &list_size,
            |b, &size| {
                let mut list = ListValue::new();
                for i in 0..size {
                    list.push_back(Bytes::from(format!("elem:{}", i)));
                }

                b.iter(|| {
                    list.push_front(Bytes::from("newelem"));
                    black_box(());
                });
            },
        );
    }

    group.finish();
}

fn bench_list_rpush(c: &mut Criterion) {
    let mut group = c.benchmark_group("commands/list/rpush");

    for list_size in [0, 100, 10000] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("initial_size", list_size),
            &list_size,
            |b, &size| {
                let mut list = ListValue::new();
                for i in 0..size {
                    list.push_back(Bytes::from(format!("elem:{}", i)));
                }

                b.iter(|| {
                    list.push_back(Bytes::from("newelem"));
                    black_box(());
                });
            },
        );
    }

    group.finish();
}

fn bench_list_lpop(c: &mut Criterion) {
    let mut group = c.benchmark_group("commands/list/lpop");

    for list_size in [100, 1000, 10000] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("initial_size", list_size),
            &list_size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let mut list = ListValue::new();
                        for i in 0..size {
                            list.push_back(Bytes::from(format!("elem:{}", i)));
                        }
                        list
                    },
                    |mut list| {
                        black_box(list.pop_front());
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_list_lrange(c: &mut Criterion) {
    let mut group = c.benchmark_group("commands/list/lrange");

    for list_size in [100, 1000, 10000] {
        let mut list = ListValue::new();
        for i in 0..list_size {
            list.push_back(Bytes::from(format!("elem:{}", i)));
        }

        // Fetch 10 elements from the middle
        let start = (list_size / 2) as i64;
        let end = start + 9;

        group.throughput(Throughput::Elements(10));
        group.bench_with_input(
            BenchmarkId::new("list_size", list_size),
            &list_size,
            |b, _| {
                b.iter(|| {
                    black_box(list.range(start, end));
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Set Operations
// ============================================================================

fn bench_set_sadd(c: &mut Criterion) {
    let mut group = c.benchmark_group("commands/set/sadd");

    for set_size in [0, 100, 10000] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("initial_size", set_size),
            &set_size,
            |b, &size| {
                let mut set = SetValue::new();
                for i in 0..size {
                    set.add(Bytes::from(format!("member:{}", i)));
                }

                let mut counter = 0u64;
                b.iter(|| {
                    let member = Bytes::from(format!("newmember:{}", counter));
                    counter += 1;
                    black_box(set.add(member));
                });
            },
        );
    }

    group.finish();
}

fn bench_set_smembers(c: &mut Criterion) {
    let mut group = c.benchmark_group("commands/set/smembers");

    for set_size in [10, 100, 1000] {
        let mut set = SetValue::new();
        for i in 0..set_size {
            set.add(Bytes::from(format!("member:{}", i)));
        }

        group.throughput(Throughput::Elements(set_size as u64));
        group.bench_with_input(BenchmarkId::new("members", set_size), &set_size, |b, _| {
            b.iter(|| {
                let result: Vec<_> = set.members().collect();
                black_box(result);
            });
        });
    }

    group.finish();
}

fn bench_set_sinter(c: &mut Criterion) {
    let mut group = c.benchmark_group("commands/set/sinter");

    for set_size in [100, 1000] {
        let mut set1 = SetValue::new();
        let mut set2 = SetValue::new();

        // Create overlapping sets (50% overlap)
        for i in 0..set_size {
            set1.add(Bytes::from(format!("member:{}", i)));
            set2.add(Bytes::from(format!("member:{}", i + set_size / 2)));
        }

        group.throughput(Throughput::Elements(set_size as u64 / 2));
        group.bench_with_input(BenchmarkId::new("set_size", set_size), &set_size, |b, _| {
            b.iter(|| {
                black_box(set1.intersection([&set2].into_iter()));
            });
        });
    }

    group.finish();
}

fn bench_set_sismember(c: &mut Criterion) {
    let mut group = c.benchmark_group("commands/set/sismember");

    for set_size in [100, 1000, 10000] {
        let mut set = SetValue::new();
        for i in 0..set_size {
            set.add(Bytes::from(format!("member:{}", i)));
        }
        let member = Bytes::from(format!("member:{}", set_size / 2));

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("set_size", set_size), &set_size, |b, _| {
            b.iter(|| {
                black_box(set.contains(&member));
            });
        });
    }

    group.finish();
}

// ============================================================================
// Sorted Set Operations
// ============================================================================

fn bench_zset_zadd(c: &mut Criterion) {
    let mut group = c.benchmark_group("commands/zset/zadd");

    for zset_size in [0, 100, 10000] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("initial_size", zset_size),
            &zset_size,
            |b, &size| {
                let mut zset = SortedSetValue::new();
                for i in 0..size {
                    zset.add(Bytes::from(format!("member:{}", i)), i as f64);
                }

                let mut counter = 0u64;
                let mut rng = rand::thread_rng();
                b.iter(|| {
                    let member = Bytes::from(format!("newmember:{}", counter));
                    let score: f64 = rng.r#gen();
                    counter += 1;
                    black_box(zset.add(member, score));
                });
            },
        );
    }

    group.finish();
}

fn bench_zset_zscore(c: &mut Criterion) {
    let mut group = c.benchmark_group("commands/zset/zscore");

    for zset_size in [100, 1000, 10000] {
        let mut zset = SortedSetValue::new();
        for i in 0..zset_size {
            zset.add(Bytes::from(format!("member:{}", i)), i as f64);
        }
        let member = Bytes::from(format!("member:{}", zset_size / 2));

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("zset_size", zset_size),
            &zset_size,
            |b, _| {
                b.iter(|| {
                    black_box(zset.get_score(&member));
                });
            },
        );
    }

    group.finish();
}

fn bench_zset_zrank(c: &mut Criterion) {
    let mut group = c.benchmark_group("commands/zset/zrank");

    for zset_size in [100, 1000, 10000] {
        let mut zset = SortedSetValue::new();
        for i in 0..zset_size {
            zset.add(Bytes::from(format!("member:{:08}", i)), i as f64);
        }
        let member = Bytes::from(format!("member:{:08}", zset_size / 2));

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("zset_size", zset_size),
            &zset_size,
            |b, _| {
                b.iter(|| {
                    black_box(zset.rank(&member));
                });
            },
        );
    }

    group.finish();
}

fn bench_zset_zrange(c: &mut Criterion) {
    let mut group = c.benchmark_group("commands/zset/zrange");

    for zset_size in [100, 1000, 10000] {
        let mut zset = SortedSetValue::new();
        for i in 0..zset_size {
            zset.add(Bytes::from(format!("member:{:08}", i)), i as f64);
        }

        // Fetch 10 elements from the middle
        let start = zset_size / 2;
        let stop = start + 9;

        group.throughput(Throughput::Elements(10));
        group.bench_with_input(
            BenchmarkId::new("zset_size", zset_size),
            &zset_size,
            |b, _| {
                b.iter(|| {
                    black_box(zset.range_by_rank(start, stop));
                });
            },
        );
    }

    group.finish();
}

fn bench_zset_zrangebyscore(c: &mut Criterion) {
    let mut group = c.benchmark_group("commands/zset/zrangebyscore");

    for zset_size in [100, 1000, 10000] {
        let mut zset = SortedSetValue::new();
        for i in 0..zset_size {
            zset.add(Bytes::from(format!("member:{:08}", i)), i as f64);
        }

        // Query for ~10% of elements
        let min = ScoreBound::Inclusive((zset_size / 2) as f64);
        let max = ScoreBound::Inclusive((zset_size / 2 + zset_size / 10) as f64);

        group.throughput(Throughput::Elements((zset_size / 10) as u64));
        group.bench_with_input(
            BenchmarkId::new("zset_size", zset_size),
            &zset_size,
            |b, _| {
                b.iter(|| {
                    black_box(zset.range_by_score(&min, &max, 0, None));
                });
            },
        );
    }

    group.finish();
}

fn bench_zset_zincrby(c: &mut Criterion) {
    let mut group = c.benchmark_group("commands/zset/zincrby");

    for zset_size in [100, 1000, 10000] {
        let mut zset = SortedSetValue::new();
        for i in 0..zset_size {
            zset.add(Bytes::from(format!("member:{}", i)), i as f64);
        }
        let member = Bytes::from(format!("member:{}", zset_size / 2));

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("zset_size", zset_size),
            &zset_size,
            |b, _| {
                b.iter(|| {
                    black_box(zset.incr(member.clone(), 1.0));
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    // String
    bench_string_get,
    bench_string_set,
    bench_string_incr,
    // Hash
    bench_hash_hset,
    bench_hash_hget,
    bench_hash_hgetall,
    // List
    bench_list_lpush,
    bench_list_rpush,
    bench_list_lpop,
    bench_list_lrange,
    // Set
    bench_set_sadd,
    bench_set_smembers,
    bench_set_sinter,
    bench_set_sismember,
    // Sorted Set
    bench_zset_zadd,
    bench_zset_zscore,
    bench_zset_zrank,
    bench_zset_zrange,
    bench_zset_zrangebyscore,
    bench_zset_zincrby,
);

criterion_main!(benches);
