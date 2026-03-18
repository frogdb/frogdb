# FrogDB Testing Guide

Testing strategy, test categories, and testing best practices for FrogDB contributors.

---

## Testing Philosophy

### Integration Tests First

During initial development, **prioritize integration tests over unit tests**:
- Software design evolves during early iterations
- Unit tests coupled to implementation details become maintenance burden
- Integration tests verify behavior, not implementation

Unit tests become valuable once core APIs stabilize and complex algorithms need edge-case coverage.

### Testing Pyramid

```
                    +-------------+
                    |   Jepsen    |  <- Distributed correctness
                    +-------------+
                    |     DST     |  <- Deterministic simulation
                    +-------------+
                    |   Shuttle   |  <- Randomized concurrency scenarios
                    +-------------+
                    |    Loom     |  <- Exhaustive primitive testing
                    +-------------+
                    | Integration |  <- Client-server interactions
                    +-------------+
                    |    Unit     |  <- Individual functions/algorithms
                    +-------------+
```

---

## Test Categories

### 1. Unit Tests

Location: Inline with source code (`#[cfg(test)]` modules). Test individual functions, data structures, and algorithms in isolation.

### 2. Integration Tests

Location: `tests/` directory. Test command execution end-to-end via real TCP connections.

**Black-Box Testing:** Client connects via TCP, issues commands, observes responses. Tests the public contract (RESP protocol behavior).

**White-Box Testing:** Access internal state to verify implementation correctness -- inspect data structure state, verify WAL contents, check shard distribution.

### 3. Property-Based Tests

Use `proptest` crate to generate random inputs and find edge cases.

### 4. Stress Tests

Verify behavior under load and find race conditions.

### 5. Redis Compatibility Tests

Run identical tests against Redis and FrogDB, compare responses.

### 6. Persistence Tests

Verify crash recovery and data durability by starting a server, writing data, dropping it, restarting, and verifying data.

### 7. Concurrency Tests

Test multi-client and multi-shard behavior (e.g., 100 concurrent INCR operations).

### 8. Loom Tests (Exhaustive Concurrency)

[Loom](https://github.com/tokio-rs/loom) runs a test many times, permuting all possible concurrent executions under the C11 memory model.

**When to use:** Lock-free data structures, custom synchronization primitives, atomic operations with memory ordering concerns.

**Limitations:** Only feasible for small, isolated code units due to factorial growth of interleavings.

### 9. Shuttle Tests (Randomized Concurrency)

[Shuttle](https://github.com/awslabs/shuttle) (AWS Labs) uses randomized testing rather than exhaustive exploration. Trades soundness for scalability.

**When to use:** Multi-client scenarios, cross-shard operations, higher-level concurrency, integration-level concurrency testing.

---

## Test Utilities

### TestServer

In-process testing helper (not subprocess) for faster execution, easier debugging, and white-box capability.

```rust
pub struct TestServer {
    shutdown_tx: Option<oneshot::Sender<()>>,
    server_handle: JoinHandle<()>,
    addr: SocketAddr,
    _temp_dir: TempDir,
}
```

**Port Allocation:** `TcpListener::bind("127.0.0.1:0")` lets the OS assign an available port, ensuring parallel tests don't conflict.

**Test Isolation:** Each test gets a unique port, unique temp directory, and independent server instance.

---

## Consistency Model Verification

### Key Consistency Models

**Linearizability:** Each operation appears to take effect atomically at some instant between its invocation and response. Applies to single-key commands. Verify with [Porcupine](https://github.com/anishathalye/porcupine) or [Elle](https://github.com/jepsen-io/elle).

**Serializability:** Transactions appear to execute in some serial order. Applies to MULTI/EXEC, Lua scripts, multi-key operations.

**Strict Serializability:** The combination of serializability and linearizability. FrogDB's transaction support aims for this within a shard.

---

## Running Tests

```bash
just test                              # Run all tests
just test frogdb-core                  # Test specific crate
just test frogdb-server test_name      # Test matching a regex pattern
just concurrency                       # Run Shuttle + Turmoil concurrency tests
```

Tests are run via `cargo nextest` with a 15s hard timeout per test.

---

## Benchmarks

```bash
just bench                             # Run benchmarks
```

External benchmarking with `redis-benchmark` or `memtier_benchmark` is supported.

---

## Best Practices

### Do
- Write tests for every new command
- Test error cases, not just happy paths
- Use property-based tests for complex logic
- Test concurrency with multiple clients
- Verify Redis compatibility for supported commands

### Don't
- Rely on test ordering
- Use fixed ports without checking availability
- Skip persistence tests
- Ignore flaky tests (fix the flakiness)
- Mock too much (integration tests are valuable)
