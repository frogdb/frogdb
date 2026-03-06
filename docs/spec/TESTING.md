# FrogDB Testing Guide

This document describes FrogDB's testing strategy, how to run tests, and testing best practices.

---

## Testing Philosophy

### Start Simple, Build Incrementally

**First test**: A simple end-to-end test that:
1. Spawns a FrogDB server instance
2. Connects a client
3. Sets a string key (`SET foo bar`)
4. Queries it back (`GET foo`)
5. Asserts the value matches

Everything else builds from this foundation.

### Integration Tests First

During initial development, **prioritize integration tests over unit tests**. Rationale:
- Software design will evolve during early iterations
- Unit tests coupled to implementation details become maintenance burden
- Integration tests verify behavior, not implementation
- Easier to refactor internals when tests verify external contracts

Unit tests become valuable once:
- Core APIs stabilize
- Complex algorithms need edge-case coverage
- Performance-critical code needs isolation testing

### Testing Pyramid

```
                    ┌─────────────┐
                    │   Jepsen    │  ← Future: Distributed correctness
                    ├─────────────┤
                    │     DST     │  ← Later: Deterministic simulation
                    ├─────────────┤
                    │   Shuttle   │  ← Randomized concurrency scenarios
                    ├─────────────┤
                    │    Loom     │  ← Exhaustive primitive testing
                    ├─────────────┤
                    │ Integration │  ← Client-server interactions ★ START HERE
                    ├─────────────┤
                    │    Unit     │  ← Add later once APIs stabilize
                    └─────────────┘
```

**Initial focus**: Integration tests. Build up from there as design stabilizes.

---

## Test Categories

### 1. Unit Tests

Location: Inline with source code (`#[cfg(test)]` modules)

Purpose: Test individual functions, data structures, and algorithms in isolation.

```rust
// Example: Testing SortedSetValue operations
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zadd_new_member() {
        let mut zset = SortedSetValue::new();
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

#### Black-Box Testing

Test FrogDB as a client would see it—no knowledge of internals:
- Client connects via TCP, issues commands, observes responses
- Tests the public contract (RESP protocol behavior)
- Redis compatibility verification

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

#### White-Box Testing

Access internal state to verify implementation correctness:
- Inspect in-memory data structure state
- Verify RocksDB WAL contents after writes
- Check shard distribution of keys
- Validate eviction policy behavior

```rust
#[tokio::test]
async fn test_wal_persistence() {
    let server = TestServer::start().await;
    let mut con = server.connection();

    let _: () = con.set("key", "value").unwrap();

    // White-box: verify WAL contains the write
    server.flush_wal().await;
    let wal_entries = server.internal_state().read_wal_entries();
    assert!(wal_entries.iter().any(|e| e.key == "key"));
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

### 8. Loom Tests (Exhaustive Concurrency)

Purpose: Exhaustively test low-level synchronization primitives under all possible interleavings.

[Loom](https://github.com/tokio-rs/loom) is a testing tool that runs a test many times, permuting all possible concurrent executions under the C11 memory model. It uses state reduction techniques to avoid combinatorial explosion.

**When to use**:
- Lock-free data structures
- Custom synchronization primitives
- Atomic operations with memory ordering concerns

**Limitations**: Only feasible for small, isolated code units due to factorial growth of interleavings.

```rust
use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::sync::Arc;
use loom::thread;

#[test]
fn test_concurrent_counter() {
    loom::model(|| {
        let counter = Arc::new(AtomicUsize::new(0));

        let threads: Vec<_> = (0..2).map(|_| {
            let c = counter.clone();
            thread::spawn(move || {
                c.fetch_add(1, Ordering::SeqCst);
            })
        }).collect();

        for t in threads { t.join().unwrap(); }
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    });
}
```

**Cargo setup**:
```toml
[dev-dependencies]
loom = "0.7"

[lints.rust]
unexpected_cfgs = { level = "warn", check-cfg = ['cfg(loom)'] }
```

### 9. Shuttle Tests (Randomized Concurrency)

Purpose: Randomized exploration of concurrent executions. Scales to larger test cases than Loom.

[Shuttle](https://github.com/awslabs/shuttle) (by AWS Labs) is inspired by Loom but uses randomized testing rather than exhaustive exploration. This trades soundness for scalability—Shuttle can test much more complex scenarios.

**When to use**:
- Multi-client scenarios
- Cross-shard operations
- Higher-level concurrency (not just primitives)
- Integration-level concurrency testing

**Production usage**: AWS S3 uses Shuttle to verify correctness of concurrent code.

```rust
use shuttle::sync::Arc;
use shuttle::thread;

#[test]
fn test_concurrent_operations() {
    shuttle::check_random(|| {
        let server = TestServer::start_sync();
        let url = server.url();

        let threads: Vec<_> = (0..4).map(|i| {
            let u = url.clone();
            thread::spawn(move || {
                let mut con = connect(&u);
                con.set(format!("key{}", i), "value").unwrap();
            })
        }).collect();

        for t in threads { t.join().unwrap(); }

        // Verify all keys exist
        let mut con = connect(&url);
        for i in 0..4 {
            let v: String = con.get(format!("key{}", i)).unwrap();
            assert_eq!(v, "value");
        }
    }, 1000); // Run 1000 random schedules
}
```

**Cargo setup**:
```toml
[dev-dependencies]
shuttle = "0.7"
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

Helper for starting FrogDB in tests. Uses **in-process testing** (not subprocess) for:
- Faster test execution (no process spawn overhead)
- Easier debugging (single process, shared stack traces)
- White-box testing capability (access to internal state)

```rust
pub struct TestServer {
    /// Shutdown signal sender
    shutdown_tx: Option<oneshot::Sender<()>>,
    /// Server task handle
    server_handle: JoinHandle<()>,
    /// Bound address (with OS-assigned port)
    addr: SocketAddr,
    /// Temporary data directory (cleaned up on drop)
    _temp_dir: TempDir,
}

impl TestServer {
    /// Start server with default configuration
    pub async fn start() -> Self {
        Self::with_config(TestConfig::default()).await
    }

    /// Start server with custom configuration
    pub async fn with_config(config: TestConfig) -> Self {
        // 1. Create temp directory for data
        let temp_dir = TempDir::new().expect("failed to create temp dir");

        // 2. Bind to port 0 for OS-assigned available port
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind");
        let addr = listener.local_addr().expect("failed to get local addr");

        // 3. Create shutdown channel
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        // 4. Spawn server as Tokio task (runs in same runtime as test)
        let server_handle = tokio::spawn(async move {
            run_server_with_listener(listener, config, shutdown_rx).await
        });

        // Server is ready immediately after bind (no polling needed)
        Self {
            shutdown_tx: Some(shutdown_tx),
            server_handle,
            addr,
            _temp_dir: temp_dir,
        }
    }

    /// Get the server's bound address
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Get connection URL for redis client
    pub fn url(&self) -> String {
        format!("redis://{}", self.addr)
    }

    /// Create a new async connection to the server
    pub async fn connection(&self) -> redis::aio::MultiplexedConnection {
        let client = redis::Client::open(self.url()).expect("failed to create client");
        client
            .get_multiplexed_async_connection()
            .await
            .expect("failed to connect")
    }

    /// Create a new sync connection (for non-async test contexts)
    pub fn sync_connection(&self) -> redis::Connection {
        let client = redis::Client::open(self.url()).expect("failed to create client");
        client.get_connection().expect("failed to connect")
    }

    /// Graceful shutdown - waits for server to stop
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        let _ = self.server_handle.await;
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        // Send shutdown signal (ignore if already sent or receiver dropped)
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        // Note: JoinHandle is not awaited in Drop (would require async)
        // Tests should call shutdown().await for clean shutdown
        // Tokio will cancel the task when runtime shuts down
    }
}
```

**Port Allocation:** Using `TcpListener::bind("127.0.0.1:0")` lets the OS assign an available port, ensuring parallel tests don't conflict.

**Test Isolation:** Each test gets:
- Unique port (OS-assigned)
- Unique temp directory (cleaned up on drop)
- Independent server instance

**Parallel Test Safety:** Tests can run with `cargo test` default parallelism without port conflicts.

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

## Consistency Model Verification

Understanding and verifying consistency models is critical for database correctness. FrogDB should clearly define which guarantees it provides and verify them rigorously.

### Key Consistency Models

#### Linearizability (Single-Object Operations)

A correctness condition for **single operations on single objects**. Each operation appears to take effect atomically at some instant between its invocation and response. This is the gold standard for single-key operations.

**Applies to**: GET, SET, INCR, ZADD, and other single-key commands

**Verification approach**:
- Record operation history with real-time timestamps (invocation and response)
- Use a linearizability checker (e.g., [Porcupine](https://github.com/anishathalye/porcupine), [Knossos](https://github.com/jepsen-io/knossos), [Elle](https://github.com/jepsen-io/elle))
- Verify history admits a linearization

#### Serializability (Transactions)

A correctness condition for **transactions** (groups of operations). Transactions appear to execute in some serial order, as if run one-at-a-time. Does NOT require real-time ordering.

**Applies to**: MULTI/EXEC transactions, Lua scripts, multi-key operations

#### Strict Serializability

The combination of serializability and linearizability. Transactions appear to execute serially AND respect real-time ordering. The strongest common guarantee.

**Applies to**: FrogDB's eventual transaction support should aim for this within a shard.

### Redis Compatibility

Beyond formal models, verify behavior matches Redis documentation:
- Run identical tests against Redis and FrogDB
- Compare responses (exact match where applicable)
- Document intentional deviations explicitly

### Further Reading

- [Aphyr: Serializability, Linearizability, and Locality](https://aphyr.com/posts/333-serializability-linearizability-and-locality)
- [Kleppmann: Please stop calling databases CP or AP](https://martin.kleppmann.com/2015/05/11/please-stop-calling-databases-cp-or-ap.html)

---

## Deterministic Simulation Testing (Future)

Deterministic Simulation Testing (DST) is a technique pioneered by [FoundationDB](https://apple.github.io/foundationdb/testing.html) that enables exhaustive testing of distributed systems by controlling all sources of non-determinism.

### Current Approach

Start with **Shuttle** for randomized concurrency testing (see section 9). This provides meaningful concurrency coverage without the investment of full DST.

### Evolution Path

1. **Now**: Shuttle for concurrent scenario testing
2. **Later**: Consider MadSim for full deterministic simulation
3. **Future**: Potentially custom DST (like TigerBeetle's VOPR)

### MadSim (Future Option)

[MadSim](https://github.com/madsim-rs/madsim) is a Rust library from RisingWave that provides:
- Deterministic simulation of network, time, and I/O
- Reproducible failure injection
- Single-threaded simulation for debugging

```rust
// Example MadSim test (future)
#[madsim::test]
async fn test_with_network_partition() {
    let handle = madsim::runtime::Handle::current();
    let server1 = spawn_server(1).await;
    let server2 = spawn_server(2).await;

    // Inject network partition
    handle.net.partition(server1.addr(), server2.addr());

    // Test behavior under partition
    // ...

    // Heal partition
    handle.net.repair(server1.addr(), server2.addr());
}
```

### Antithesis (Commercial Option)

[Antithesis](https://antithesis.com/) is a commercial platform from FoundationDB founders that provides:
- Deterministic hypervisor for Docker containers
- Automated bug finding
- No implementation work required (but has cost)

Used by MongoDB, TigerBeetle, and others.

### TigerBeetle's VOPR (Inspiration)

[TigerBeetle](https://docs.tigerbeetle.com/concepts/safety/) built their own deterministic simulator called VOPR that achieves ~700x time acceleration. Their approach:
- All I/O mocked (network, storage)
- Aggressive fault injection (up to 8% storage corruption)
- Runs 24/7 on 1000 cores

---

## Future: Jepsen Testing

[Jepsen](https://jepsen.io/) is a framework for distributed systems verification with fault injection. It's used by CockroachDB, YugabyteDB, TigerBeetle, and many others.

### What Jepsen Provides

- Black-box distributed systems verification
- Fault injection (network partitions, node failures, clock skew)
- Linearizability and consistency checking via [Elle](https://github.com/jepsen-io/elle)
- Real cluster testing (not simulation)

### Prerequisites for FrogDB

Before investing in Jepsen:
- [ ] Clustering implementation complete
- [ ] Multi-node deployment working
- [ ] Basic fault tolerance implemented
- [ ] Replication operational

### Maelstrom (Learning Step)

[Maelstrom](https://github.com/jepsen-io/maelstrom) is Jepsen's learning workbench for testing distributed systems. It's useful for:
- Validating consensus algorithm implementations
- Learning distributed systems concepts
- Testing via simple JSON protocol (no cluster setup)

Consider using Maelstrom to validate FrogDB's consensus algorithms before full Jepsen integration.

---

## References

- [DESIGN.md Testing Strategy](INDEX.md#testing-strategy) - Test category overview
- [Rust Book: Testing](https://doc.rust-lang.org/book/ch11-00-testing.html) - Rust testing basics
- [proptest](https://proptest-rs.github.io/proptest/) - Property-based testing
- [cargo-llvm-cov](https://github.com/taiki-e/cargo-llvm-cov) - Coverage tooling

### Concurrency Testing

- [Loom](https://github.com/tokio-rs/loom) - Concurrency permutation testing
- [Shuttle](https://github.com/awslabs/shuttle) - Randomized concurrency testing (AWS)

### Consistency & Correctness

- [Jepsen](https://jepsen.io/) - Distributed systems verification
- [Maelstrom](https://github.com/jepsen-io/maelstrom) - Jepsen learning workbench
- [Elle](https://github.com/jepsen-io/elle) - Black-box transactional consistency checker

### Deterministic Simulation

- [MadSim](https://github.com/madsim-rs/madsim) - Deterministic simulation for Rust
- [FoundationDB Simulation Testing](https://apple.github.io/foundationdb/testing.html)
- [TigerBeetle Safety](https://docs.tigerbeetle.com/concepts/safety/)
- [Antithesis](https://antithesis.com/) - Commercial DST platform
