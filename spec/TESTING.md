# FrogDB Testing Guide

This document describes FrogDB's testing strategy, how to run tests, and testing best practices.

## Test Categories

### 1. Unit Tests

Location: Inline with source code (`#[cfg(test)]` modules)

Purpose: Test individual functions, data structures, and algorithms in isolation.

```rust
// Example: Testing FrogSortedSet operations
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zadd_new_member() {
        let mut zset = FrogSortedSet::new();
        assert_eq!(zset.add("member1", 1.0, AddOptions::default()), 1);
        assert_eq!(zset.score("member1"), Some(1.0));
    }
}
```

Run unit tests:
```bash
cargo test --lib
```

### 2. Integration Tests

Location: `tests/` directory

Purpose: Test command execution end-to-end via real TCP connections.

```rust
// tests/string_commands.rs
use redis::Commands;

#[tokio::test]
async fn test_set_get() {
    let server = TestServer::start().await;
    let client = redis::Client::open(server.url()).unwrap();
    let mut con = client.get_connection().unwrap();

    let _: () = con.set("mykey", "myvalue").unwrap();
    let val: String = con.get("mykey").unwrap();
    assert_eq!(val, "myvalue");
}
```

Run integration tests:
```bash
cargo test --test '*'
```

### 3. Property-Based Tests

Location: `tests/` directory with `proptest` crate

Purpose: Generate random inputs to find edge cases.

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn incr_decr_roundtrip(start: i64, delta: i64) {
        // INCR by delta then DECR by delta should return to start
        let server = TestServer::start_sync();
        let mut con = server.connection();

        let _: () = con.set("counter", start).unwrap();
        let _: i64 = con.incr("counter", delta).unwrap();
        let result: i64 = con.decr("counter", delta).unwrap();
        assert_eq!(result, start);
    }
}
```

### 4. Stress Tests

Location: `tests/stress/` or `benches/`

Purpose: Verify behavior under load and find race conditions.

```bash
# Run with increased concurrency
cargo test stress -- --test-threads=1
```

### 5. Redis Compatibility Tests

Purpose: Verify behavior matches Redis for supported commands.

Approach:
1. Run same test against Redis and FrogDB
2. Compare responses for equivalence
3. Document known differences

```rust
#[tokio::test]
async fn test_zadd_options_match_redis() {
    let frogdb = FrogDBServer::start().await;
    let redis = RedisServer::start().await;

    // Test ZADD NX behavior
    for server in [&frogdb, &redis] {
        let mut con = server.connection();
        let _: () = con.zadd("zset", "member", 1.0).unwrap();
        // NX should not update existing member
        let added: i64 = con.zadd_nx("zset", "member", 2.0).unwrap();
        assert_eq!(added, 0);
        let score: f64 = con.zscore("zset", "member").unwrap();
        assert_eq!(score, 1.0);  // Original score preserved
    }
}
```

### 6. Persistence Tests

Purpose: Verify crash recovery and data durability.

```rust
#[tokio::test]
async fn test_crash_recovery() {
    let data_dir = tempdir().unwrap();

    // Start server, write data
    {
        let server = TestServer::with_config(Config {
            data_dir: data_dir.path().to_path_buf(),
            durability_mode: DurabilityMode::Sync,
            ..Default::default()
        }).await;
        let mut con = server.connection();
        let _: () = con.set("key", "value").unwrap();
    }
    // Server dropped (simulates crash)

    // Restart, verify data
    {
        let server = TestServer::with_config(Config {
            data_dir: data_dir.path().to_path_buf(),
            ..Default::default()
        }).await;
        let mut con = server.connection();
        let val: String = con.get("key").unwrap();
        assert_eq!(val, "value");
    }
}
```

### 7. Concurrency Tests

Purpose: Test multi-client and multi-shard behavior.

```rust
#[tokio::test]
async fn test_concurrent_incr() {
    let server = TestServer::start().await;
    let tasks: Vec<_> = (0..100).map(|_| {
        let url = server.url();
        tokio::spawn(async move {
            let client = redis::Client::open(url).unwrap();
            let mut con = client.get_async_connection().await.unwrap();
            let _: i64 = con.incr("counter", 1).await.unwrap();
        })
    }).collect();

    for task in tasks {
        task.await.unwrap();
    }

    let mut con = server.connection();
    let count: i64 = con.get("counter").unwrap();
    assert_eq!(count, 100);
}
```

---

## Running Tests

### All Tests

```bash
cargo test
```

### Specific Test Category

```bash
# Unit tests only
cargo test --lib

# Integration tests only
cargo test --test '*'

# Specific test file
cargo test --test string_commands

# Specific test function
cargo test test_set_get
```

### Test Output

```bash
# Show stdout for passing tests
cargo test -- --nocapture

# Run tests sequentially (useful for debugging)
cargo test -- --test-threads=1
```

### Release Mode

```bash
# Faster execution, catches release-only bugs
cargo test --release
```

---

## Test Utilities

### TestServer

Helper for starting FrogDB in tests:

```rust
struct TestServer {
    process: Child,
    port: u16,
    data_dir: TempDir,
}

impl TestServer {
    async fn start() -> Self;
    async fn with_config(config: Config) -> Self;
    fn url(&self) -> String;
    fn connection(&self) -> redis::Connection;
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.process.kill().unwrap();
    }
}
```

### Test Fixtures

Common test data generators:

```rust
fn random_key() -> String;
fn random_string(len: usize) -> String;
fn random_sorted_set(size: usize) -> Vec<(String, f64)>;
```

---

## Benchmarks

### Running Benchmarks

```bash
cargo bench
```

### Benchmark Categories

| Category | Command |
|----------|---------|
| Command throughput | `cargo bench -- throughput` |
| Latency percentiles | `cargo bench -- latency` |
| Memory usage | `cargo bench -- memory` |
| Persistence overhead | `cargo bench -- persistence` |

### External Benchmarking

Using `redis-benchmark`:

```bash
# Start FrogDB
./target/release/frogdb-server

# In another terminal
redis-benchmark -p 6379 -t set,get -n 100000 -c 50
```

Using `memtier_benchmark`:

```bash
memtier_benchmark -s 127.0.0.1 -p 6379 --protocol=redis \
    --clients=50 --threads=4 --requests=100000
```

---

## Coverage

### Generating Coverage Report

Using `cargo-llvm-cov`:

```bash
cargo install cargo-llvm-cov
cargo llvm-cov --html
open target/llvm-cov/html/index.html
```

### Coverage Targets

| Component | Target Coverage |
|-----------|-----------------|
| Command implementations | > 90% |
| Data structures | > 95% |
| Protocol parsing | > 90% |
| Persistence | > 80% |
| Error paths | > 70% |

---

## Continuous Integration

### CI Pipeline

```yaml
# .github/workflows/test.yml
name: Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: cargo test
      - run: cargo test --release

  clippy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: clippy
      - run: cargo clippy -- -D warnings

  format:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: rustfmt
      - run: cargo fmt --check
```

### Required Checks

Before merging:
1. All tests pass
2. No clippy warnings
3. Code formatted (`cargo fmt`)
4. No new unsafe code without justification

---

## Testing Best Practices

### Do

- Write tests for every new command
- Test error cases, not just happy paths
- Use property-based tests for complex logic
- Test concurrency with multiple clients
- Verify Redis compatibility for supported commands
- Clean up resources (use `Drop` or explicit cleanup)

### Don't

- Rely on test ordering
- Use fixed ports without checking availability
- Skip persistence tests (they catch real bugs)
- Ignore flaky tests (fix the flakiness)
- Mock too much (integration tests are valuable)

### Handling Flaky Tests

If a test is flaky:

1. **Identify root cause:**
   - Race condition?
   - Port conflict?
   - Timing assumption?

2. **Fix properly:**
   - Add proper synchronization
   - Use dynamic port allocation
   - Use explicit waits instead of sleeps

3. **If truly non-deterministic:**
   - Add `#[ignore]` with explanation
   - Run separately in CI with retries

---

## Future: Jepsen Testing

For distributed correctness verification:

1. Set up Jepsen test cluster
2. Define workload (concurrent writes, network partitions)
3. Verify linearizability of operations
4. Test failover correctness

See [Jepsen](https://jepsen.io/) for methodology.

---

## References

- [DESIGN.md Testing Strategy](../DESIGN.md#testing-strategy) - Test category overview
- [Rust Book: Testing](https://doc.rust-lang.org/book/ch11-00-testing.html) - Rust testing basics
- [proptest](https://proptest-rs.github.io/proptest/) - Property-based testing
- [cargo-llvm-cov](https://github.com/taiki-e/cargo-llvm-cov) - Coverage tooling
