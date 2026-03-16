# FrogDB Testing Patterns for Distributed Audits

Guide for selecting, running, and writing tests during correctness audits.

---

## When to Write New Tests vs. Run Existing

**Run existing tests when:**
- Change is within well-covered code paths
- Existing tests already exercise the affected invariants
- Change is a bug fix with a clear failure mode

**Write new tests when:**
- Change introduces a new code path not covered by existing tests
- Audit identifies a fault scenario with no test coverage
- Change modifies an invariant's enforcement mechanism
- A new edge case is discovered during audit

**Rule of thumb:** If the audit identifies a specific failure scenario and `grep` finds
no existing test for that scenario, write one.

---

## Test Type Selection Guide

### By Risk Domain

| Risk Domain | Primary Test Type | Tool | When to Add |
|-------------|-------------------|------|-------------|
| Shard-local concurrency | Shuttle deterministic test | `just concurrency` | Any shard worker change |
| Cross-shard atomicity | Shuttle + integration | `just concurrency` + `just test` | VLL or scatter-gather changes |
| WAL ordering | Integration test | `just test frogdb-persistence` | WAL write path changes |
| Snapshot consistency | Integration test | `just test frogdb-persistence` | Snapshot algorithm changes |
| Recovery | Jepsen crash tests | `just jepsen crash` | Persistence or recovery changes |
| Replication | Jepsen replication suite | `just jepsen-suite replication` | Replication crate changes |
| Failover | Jepsen raft suite | `just jepsen-suite raft` | Cluster state or promotion changes |
| Linearizability | Jepsen register + WGL | `just jepsen register` | Any change affecting op ordering |
| Pub/sub | Integration test | `just test frogdb-server` | Pub/sub routing changes |
| Network partition | Turmoil simulation | `just concurrency` | Cluster/replication networking |

### By Change Type

| Change Type | Recommended Tests |
|-------------|-------------------|
| New command (single-key, read-only) | Unit test + integration test |
| New command (single-key, write) | Unit + integration + Jepsen register (verify linearizability) |
| New command (multi-key) | Unit + integration + Shuttle (concurrency) |
| Bug fix (data corruption) | Regression test reproducing the bug + Jepsen crash |
| Replication protocol change | Jepsen replication suite + replication-chaos |
| Cluster topology change | Jepsen raft suite + raft-chaos |
| VLL change | Shuttle tests + Jepsen cross-slot |
| WAL format change | Integration + Jepsen append-crash + append-rapid |
| Snapshot algorithm change | Integration + Jepsen crash suite |
| Connection handling change | Integration tests with pipelining |

---

## Integration Test Templates

### Single-Shard Correctness

```rust
#[tokio::test]
async fn test_operation_single_shard() {
    let server = TestServer::start_default().await;
    let mut conn = server.connect().await;

    // Setup
    conn.send("SET", &["{tag}key1", "value1"]).await;

    // Operation under test
    let result = conn.send("COMMAND", &["{tag}key1"]).await;

    // Verify
    assert_eq!(result, expected);

    // Verify persistence (if write command)
    server.restart().await;
    let mut conn = server.connect().await;
    let result = conn.send("GET", &["{tag}key1"]).await;
    assert_eq!(result, expected_after_restart);
}
```

### Cross-Shard Atomicity

```rust
#[tokio::test]
async fn test_cross_shard_atomicity() {
    let server = TestServer::start_with_config(|c| {
        c.num_shards = Some(4);
        c.allow_cross_slot_standalone = true;
    }).await;
    let mut conn = server.connect().await;

    // Keys on different shards (no hash tags)
    conn.send("MSET", &["key1", "v1", "key2", "v2"]).await;

    // Verify all-or-nothing
    let r1 = conn.send("GET", &["key1"]).await;
    let r2 = conn.send("GET", &["key2"]).await;
    assert_eq!(r1, "v1");
    assert_eq!(r2, "v2");
}
```

### Crash Recovery

```rust
#[tokio::test]
async fn test_crash_recovery() {
    let server = TestServer::start_with_config(|c| {
        c.durability_mode = DurabilityMode::Sync;
    }).await;
    let mut conn = server.connect().await;

    // Write data
    conn.send("SET", &["key", "value"]).await;

    // Simulate crash (kill without graceful shutdown)
    server.kill().await;

    // Restart and verify
    server.restart().await;
    let mut conn = server.connect().await;
    let result = conn.send("GET", &["key"]).await;
    assert_eq!(result, "value");
}
```

---

## Shuttle Concurrency Test Templates

Shuttle tests explore all possible interleavings of concurrent operations.
Use for shard-local and cross-shard concurrency concerns.

### When to Add Shuttle Tests

- Shard worker event loop changes
- VLL lock ordering changes
- New message types between shards
- Blocking command changes
- Any code with multiple `.await` points that access shared state

### Basic Shuttle Test

```rust
#[cfg(test)]
mod shuttle_tests {
    use shuttle::*;

    #[test]
    fn test_concurrent_operations() {
        shuttle::check_random(|| {
            // Setup shared state (channels, etc.)

            let handle1 = shuttle::thread::spawn(|| {
                // Operation 1
            });

            let handle2 = shuttle::thread::spawn(|| {
                // Operation 2
            });

            handle1.join().unwrap();
            handle2.join().unwrap();

            // Verify invariants hold
        }, 1000); // 1000 random schedules
    }
}
```

### Shuttle Test for Lock Ordering

```rust
#[test]
fn test_vll_no_deadlock() {
    shuttle::check_random(|| {
        let shards = create_test_shards(4);

        // Two concurrent multi-shard operations
        let h1 = shuttle::thread::spawn({
            let shards = shards.clone();
            move || {
                // Operation touching shards [0, 2]
                acquire_locks_sorted(&shards, &[0, 2]);
                execute_on_shards(&shards, &[0, 2]);
                release_locks(&shards, &[0, 2]);
            }
        });

        let h2 = shuttle::thread::spawn({
            let shards = shards.clone();
            move || {
                // Operation touching shards [0, 2] (same shards, different order input)
                acquire_locks_sorted(&shards, &[2, 0]); // Should sort to [0, 2]
                execute_on_shards(&shards, &[0, 2]);
                release_locks(&shards, &[0, 2]);
            }
        });

        h1.join().unwrap();
        h2.join().unwrap();
        // If we reach here, no deadlock occurred
    }, 10000);
}
```

---

## Turmoil Simulation Test Templates

Turmoil simulates network conditions (partitions, latency, drops).
Use for replication, cluster, and failover concerns.

### When to Add Turmoil Tests

- Network timeout handling changes
- Replication reconnection logic
- Cluster topology propagation
- Failover detection and promotion
- Any code that handles `ConnectionReset` or timeout errors

---

## Jepsen Test Selection Guide

Cross-reference with `~/.claude/skills/jepsen-testing/references/test-catalog.md` for
the full catalog of 34+ tests.

### By Change Type → Recommended Jepsen Tests

| Change Type | Must Run | Should Run | Consider |
|-------------|----------|------------|----------|
| Shard worker | register, crash | counter-crash | concurrency tests |
| Persistence/WAL | append-crash, append-rapid | crash, counter-crash | All crash suite |
| Replication | replication, split-brain | replication-chaos, lag | zombie |
| Cluster/Raft | leader-election, key-routing | cluster-formation | raft-chaos |
| Failover | leader-election-partition | slot-migration-partition | raft extended |
| VLL/cross-shard | cross-slot | transaction | transaction-crash |
| Expiry | expiry, expiry-crash | expiry-rapid | clock-skew |
| Blocking | blocking, blocking-crash | — | — |

### Jepsen Commands

```bash
# Run individual test
just jepsen register --time-limit 30

# Run suite
just jepsen-suite single      # 10 basic tests
just jepsen-suite crash        # 19 tests with kill nemesis
just jepsen-suite replication  # 5 replication tests
just jepsen-suite raft         # 9 raft cluster tests
just jepsen-suite all          # All 33 tests (excludes raft-extended)

# Extended fault injection (resource-intensive)
just jepsen clock-skew
just jepsen disk-failure
just jepsen slow-network
just jepsen memory-pressure
```

### Interpreting Jepsen Results

- **`:valid? true`** — All checkers passed
- **`:valid? false`** — At least one checker found a violation. Read `:anomalies`
- **`:valid? :unknown`** — Couldn't determine (timeout/crash). Increase `--time-limit`
- High `:info-count` is usually timeouts, not real failures
- `:cycle-search-timeout` means Elle couldn't finish (not a failure)

See `~/.claude/skills/jepsen-testing/SKILL.md` for detailed result interpretation.

---

## WGL Linearizability Checker

The `frogdb-testing` crate provides a Wing-Gong-Luchangco (WGL) linearizability checker
for use in integration tests.

### When to Use

- Verifying that a new command implementation is linearizable
- Testing operation ordering under concurrent access
- Validating that pipeline responses match request order

### Usage Pattern

```rust
use frogdb_testing::{History, Model, WglChecker};

// Record operation history
let mut history = History::new();
history.invoke(thread_id, op);
history.ok(thread_id, result);

// Check linearizability against model
let model = RegisterModel::new();
let checker = WglChecker::new(model);
assert!(checker.check(&history).is_linearizable());
```

---

## Property-Based Testing with Proptest

Use proptest for generating random inputs to find edge cases in parsing,
serialization, and protocol handling.

### When to Use

- Serialization format changes
- Protocol parsing changes
- Command argument validation
- Hash tag parsing

### Example

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn roundtrip_serialization(value in arb_value()) {
        let bytes = serialize(&value);
        let recovered = deserialize(&bytes).unwrap();
        assert_eq!(value, recovered);
    }

    #[test]
    fn hash_tag_deterministic(key in ".*") {
        let shard1 = shard_for_key(key.as_bytes(), 4);
        let shard2 = shard_for_key(key.as_bytes(), 4);
        assert_eq!(shard1, shard2);
    }
}
```

---

## Test Verification Sequence

After an audit recommends tests, run them in this order:

```bash
# 1. Basic compilation and linting
just check
just lint

# 2. Unit and integration tests for affected crates
just test <crate-name>

# 3. Concurrency tests (if shard/VLL/channel changes)
just concurrency

# 4. Full test suite
just test

# 5. Jepsen tests (if cluster/replication/persistence changes)
just jepsen-suite <relevant-suite>
```

If any step fails, fix before proceeding. Jepsen tests are expensive — only run after
local tests pass.
