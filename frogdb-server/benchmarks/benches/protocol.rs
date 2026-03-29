//! Protocol micro-benchmarks.
//!
//! Benchmarks for:
//! - RESP2/RESP3 response encoding
//! - Frame parsing
//! - Bulk array handling

use bytes::{Bytes, BytesMut};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use frogdb_protocol::Response;
use redis_protocol::resp2::encode::extend_encode as resp2_encode;
use redis_protocol::resp3::encode::complete::extend_encode as resp3_encode;
use std::hint::black_box;

// ============================================================================
// Response Encoding Benchmarks (RESP2)
// ============================================================================

fn bench_encode_simple_string(c: &mut Criterion) {
    let mut group = c.benchmark_group("protocol/resp2/encode/simple");

    let response = Response::ok();
    let frame = response.into_wire().unwrap().to_resp2_frame();

    group.throughput(Throughput::Elements(1));
    group.bench_function("ok", |b| {
        let mut buf = BytesMut::with_capacity(64);
        b.iter(|| {
            buf.clear();
            black_box(resp2_encode(&mut buf, &frame, false)).unwrap();
        });
    });

    group.finish();
}

fn bench_encode_integer(c: &mut Criterion) {
    let mut group = c.benchmark_group("protocol/resp2/encode/integer");

    for value in [0i64, 42, 1_000_000, i64::MAX] {
        let response = Response::Integer(value);
        let frame = response.into_wire().unwrap().to_resp2_frame();

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("value", value), &value, |b, _| {
            let mut buf = BytesMut::with_capacity(32);
            b.iter(|| {
                buf.clear();
                black_box(resp2_encode(&mut buf, &frame, false)).unwrap();
            });
        });
    }

    group.finish();
}

fn bench_encode_bulk_string(c: &mut Criterion) {
    let mut group = c.benchmark_group("protocol/resp2/encode/bulk");

    for size in [16, 128, 1024, 8192] {
        let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        let response = Response::bulk(Bytes::from(data));
        let frame = response.into_wire().unwrap().to_resp2_frame();

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("bytes", size), &size, |b, _| {
            let mut buf = BytesMut::with_capacity(size + 32);
            b.iter(|| {
                buf.clear();
                black_box(resp2_encode(&mut buf, &frame, false)).unwrap();
            });
        });
    }

    group.finish();
}

fn bench_encode_array(c: &mut Criterion) {
    let mut group = c.benchmark_group("protocol/resp2/encode/array");

    for array_size in [1, 10, 100, 1000] {
        let items: Vec<Response> = (0..array_size)
            .map(|i| Response::bulk(Bytes::from(format!("item:{}", i))))
            .collect();
        let response = Response::Array(items);
        let frame = response.into_wire().unwrap().to_resp2_frame();

        group.throughput(Throughput::Elements(array_size as u64));
        group.bench_with_input(
            BenchmarkId::new("elements", array_size),
            &array_size,
            |b, _| {
                let mut buf = BytesMut::with_capacity(array_size * 32);
                b.iter(|| {
                    buf.clear();
                    black_box(resp2_encode(&mut buf, &frame, false)).unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_encode_null(c: &mut Criterion) {
    let mut group = c.benchmark_group("protocol/resp2/encode/null");

    let response = Response::null();
    let frame = response.into_wire().unwrap().to_resp2_frame();

    group.throughput(Throughput::Elements(1));
    group.bench_function("null", |b| {
        let mut buf = BytesMut::with_capacity(16);
        b.iter(|| {
            buf.clear();
            black_box(resp2_encode(&mut buf, &frame, false)).unwrap();
        });
    });

    group.finish();
}

fn bench_encode_error(c: &mut Criterion) {
    let mut group = c.benchmark_group("protocol/resp2/encode/error");

    let response = Response::error("ERR unknown command 'FOO'");
    let frame = response.into_wire().unwrap().to_resp2_frame();

    group.throughput(Throughput::Elements(1));
    group.bench_function("error", |b| {
        let mut buf = BytesMut::with_capacity(64);
        b.iter(|| {
            buf.clear();
            black_box(resp2_encode(&mut buf, &frame, false)).unwrap();
        });
    });

    group.finish();
}

// ============================================================================
// Response Encoding Benchmarks (RESP3)
// ============================================================================

fn bench_encode_resp3_map(c: &mut Criterion) {
    let mut group = c.benchmark_group("protocol/resp3/encode/map");

    for map_size in [1, 10, 100] {
        let pairs: Vec<(Response, Response)> = (0..map_size)
            .map(|i| {
                (
                    Response::bulk(Bytes::from(format!("field:{}", i))),
                    Response::bulk(Bytes::from(format!("value:{}", i))),
                )
            })
            .collect();
        let response = Response::Map(pairs);
        let frame = response.into_wire().unwrap().to_resp3_frame();

        group.throughput(Throughput::Elements(map_size as u64));
        group.bench_with_input(BenchmarkId::new("pairs", map_size), &map_size, |b, _| {
            let mut buf = BytesMut::with_capacity(map_size * 64);
            b.iter(|| {
                buf.clear();
                black_box(resp3_encode(&mut buf, &frame, false)).unwrap();
            });
        });
    }

    group.finish();
}

fn bench_encode_resp3_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("protocol/resp3/encode/set");

    for set_size in [1, 10, 100] {
        let items: Vec<Response> = (0..set_size)
            .map(|i| Response::bulk(Bytes::from(format!("member:{}", i))))
            .collect();
        let response = Response::Set(items);
        let frame = response.into_wire().unwrap().to_resp3_frame();

        group.throughput(Throughput::Elements(set_size as u64));
        group.bench_with_input(BenchmarkId::new("members", set_size), &set_size, |b, _| {
            let mut buf = BytesMut::with_capacity(set_size * 32);
            b.iter(|| {
                buf.clear();
                black_box(resp3_encode(&mut buf, &frame, false)).unwrap();
            });
        });
    }

    group.finish();
}

fn bench_encode_resp3_double(c: &mut Criterion) {
    let mut group = c.benchmark_group("protocol/resp3/encode/double");

    for value in [0.0, std::f64::consts::PI, f64::MAX, f64::INFINITY] {
        let response = Response::Double(value);
        let frame = response.into_wire().unwrap().to_resp3_frame();

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("value", format!("{:.5}", value)),
            &value,
            |b, _| {
                let mut buf = BytesMut::with_capacity(32);
                b.iter(|| {
                    buf.clear();
                    black_box(resp3_encode(&mut buf, &frame, false)).unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_encode_resp3_boolean(c: &mut Criterion) {
    let mut group = c.benchmark_group("protocol/resp3/encode/boolean");

    for value in [true, false] {
        let response = Response::Boolean(value);
        let frame = response.into_wire().unwrap().to_resp3_frame();

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("value", value), &value, |b, _| {
            let mut buf = BytesMut::with_capacity(8);
            b.iter(|| {
                buf.clear();
                black_box(resp3_encode(&mut buf, &frame, false)).unwrap();
            });
        });
    }

    group.finish();
}

// ============================================================================
// Response to Frame Conversion
// ============================================================================

fn bench_response_to_frame_resp2(c: &mut Criterion) {
    let mut group = c.benchmark_group("protocol/response/to_resp2_frame");

    // Complex nested response
    let nested_response = Response::Array(vec![
        Response::bulk(Bytes::from("field1")),
        Response::Integer(42),
        Response::Array(vec![
            Response::bulk(Bytes::from("nested1")),
            Response::bulk(Bytes::from("nested2")),
        ]),
        Response::null(),
    ]);

    group.throughput(Throughput::Elements(1));
    group.bench_function("nested_array", |b| {
        b.iter(|| {
            black_box(
                nested_response
                    .clone()
                    .into_wire()
                    .unwrap()
                    .to_resp2_frame(),
            );
        });
    });

    // Map to flattened array (RESP2 doesn't have native maps)
    let map_response = Response::Map(
        (0..10)
            .map(|i| {
                (
                    Response::bulk(Bytes::from(format!("field:{}", i))),
                    Response::bulk(Bytes::from(format!("value:{}", i))),
                )
            })
            .collect(),
    );

    group.bench_function("map_flatten", |b| {
        b.iter(|| {
            black_box(map_response.clone().into_wire().unwrap().to_resp2_frame());
        });
    });

    group.finish();
}

fn bench_response_to_frame_resp3(c: &mut Criterion) {
    let mut group = c.benchmark_group("protocol/response/to_resp3_frame");

    // Complex nested response with RESP3-specific types
    let nested_response = Response::Map(vec![
        (
            Response::bulk(Bytes::from("string")),
            Response::bulk(Bytes::from("hello")),
        ),
        (
            Response::bulk(Bytes::from("integer")),
            Response::Integer(42),
        ),
        (
            Response::bulk(Bytes::from("double")),
            Response::Double(std::f64::consts::PI),
        ),
        (
            Response::bulk(Bytes::from("boolean")),
            Response::Boolean(true),
        ),
        (
            Response::bulk(Bytes::from("set")),
            Response::Set(vec![
                Response::bulk(Bytes::from("a")),
                Response::bulk(Bytes::from("b")),
                Response::bulk(Bytes::from("c")),
            ]),
        ),
    ]);

    group.throughput(Throughput::Elements(1));
    group.bench_function("complex_map", |b| {
        b.iter(|| {
            black_box(
                nested_response
                    .clone()
                    .into_wire()
                    .unwrap()
                    .to_resp3_frame(),
            );
        });
    });

    group.finish();
}

// ============================================================================
// RESP2 Parsing Benchmarks
// ============================================================================

fn bench_parse_resp2_bulk(c: &mut Criterion) {
    use redis_protocol::resp2::decode::decode;

    let mut group = c.benchmark_group("protocol/resp2/parse/bulk");

    for size in [16, 128, 1024, 8192] {
        // Create a valid RESP2 bulk string
        let data: String = (0..size).map(|i| ((i % 26) as u8 + b'a') as char).collect();
        let encoded = format!("${}\r\n{}\r\n", size, data);
        let bytes = Bytes::from(encoded);

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("bytes", size), &size, |b, _| {
            b.iter(|| {
                black_box(decode(&bytes)).unwrap();
            });
        });
    }

    group.finish();
}

fn bench_parse_resp2_array(c: &mut Criterion) {
    use redis_protocol::resp2::decode::decode;

    let mut group = c.benchmark_group("protocol/resp2/parse/array");

    for array_size in [1, 10, 100] {
        // Create a valid RESP2 array
        let mut encoded = format!("*{}\r\n", array_size);
        for i in 0..array_size {
            let item = format!("item:{}", i);
            encoded.push_str(&format!("${}\r\n{}\r\n", item.len(), item));
        }
        let bytes = Bytes::from(encoded);

        group.throughput(Throughput::Elements(array_size as u64));
        group.bench_with_input(
            BenchmarkId::new("elements", array_size),
            &array_size,
            |b, _| {
                b.iter(|| {
                    black_box(decode(&bytes)).unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_parse_resp2_command(c: &mut Criterion) {
    use redis_protocol::resp2::decode::decode;

    let mut group = c.benchmark_group("protocol/resp2/parse/command");

    // GET key
    let get_cmd = Bytes::from("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
    group.bench_function("GET", |b| {
        b.iter(|| {
            black_box(decode(&get_cmd)).unwrap();
        });
    });

    // SET key value
    let set_cmd = Bytes::from("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
    group.bench_function("SET", |b| {
        b.iter(|| {
            black_box(decode(&set_cmd)).unwrap();
        });
    });

    // MSET with 10 key-value pairs
    let mut mset_cmd = String::from("*21\r\n$4\r\nMSET\r\n");
    for i in 0..10 {
        let key = format!("key{}", i);
        let val = format!("val{}", i);
        mset_cmd.push_str(&format!(
            "${}\r\n{}\r\n${}\r\n{}\r\n",
            key.len(),
            key,
            val.len(),
            val
        ));
    }
    let mset_bytes = Bytes::from(mset_cmd);
    group.bench_function("MSET_10", |b| {
        b.iter(|| {
            black_box(decode(&mset_bytes)).unwrap();
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    // RESP2 encoding
    bench_encode_simple_string,
    bench_encode_integer,
    bench_encode_bulk_string,
    bench_encode_array,
    bench_encode_null,
    bench_encode_error,
    // RESP3 encoding
    bench_encode_resp3_map,
    bench_encode_resp3_set,
    bench_encode_resp3_double,
    bench_encode_resp3_boolean,
    // Response conversion
    bench_response_to_frame_resp2,
    bench_response_to_frame_resp3,
    // RESP2 parsing
    bench_parse_resp2_bulk,
    bench_parse_resp2_array,
    bench_parse_resp2_command,
);

criterion_main!(benches);
