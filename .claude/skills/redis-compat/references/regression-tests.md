# Creating Redis Regression Tests

When a compatibility issue is fixed, you may be asked to create a Rust regression test
to prevent regressions in CI. These tests live in the `redis-regression` crate.

## Location

```
frogdb-server/crates/redis-regression/tests/
├── main.rs                      # Module declarations (one per file)
├── string_regression.rs         # String command regressions
├── list_regression.rs           # List command regressions
├── sort_regression.rs           # SORT command regressions
├── zset_regression.rs           # Sorted set regressions
├── ...                          # ~35 regression test files
```

## Crate Structure

`Cargo.toml` key points:
- Package name: `frogdb-redis-regression`
- `autotests = false` — single test binary via `[[test]] name = "main"`
- Dev dependencies: `frogdb-test-harness`, `frogdb-server`, `frogdb-protocol`,
  `redis-protocol`, `tokio`, `bytes`

## Test Harness API

### Starting a server

```rust
use frogdb_test_harness::server::{TestServer, TestServerConfig};

// Default: 4 shards, no persistence, port 0 (OS-assigned)
let server = TestServer::start_standalone().await;

// Custom config (e.g., single shard for SORT tests)
let server = TestServer::start_standalone_with_config(TestServerConfig {
    num_shards: Some(1),
    ..Default::default()
}).await;
```

The server automatically shuts down and cleans up temp data when dropped.

### Connecting a client

```rust
let mut client = server.connect().await;
```

Returns a `TestClient` with RESP2 framing.

### Sending commands

```rust
// Send command and get Response
let resp = client.command(&["SET", "mykey", "myvalue"]).await;
let resp = client.command(&["GET", "mykey"]).await;
let resp = client.command(&["ZADD", "myzset", "1.5", "member"]).await;
```

### Assertion helpers

Import from `frogdb_test_harness::response::*`:

```rust
use frogdb_test_harness::response::*;

// Assert OK response
assert_ok(&resp);

// Assert error with prefix
assert_error_prefix(&resp, "WRONGTYPE");
assert_error_prefix(&resp, "ERR");
assert_error_prefix(&resp, "CROSSSLOT");

// Assert bulk string value
assert_bulk_eq(&resp, b"expected_value");

// Unwrap typed responses (panics with clear message on mismatch)
let n: i64 = unwrap_integer(&resp);
let bytes: &[u8] = unwrap_bulk(&resp);
let items: Vec<Response> = unwrap_array(resp);

// Extract string values from array response
let strings: Vec<String> = extract_bulk_strings(&resp);
```

### Direct Response matching

```rust
use frogdb_protocol::Response;

// Nil check
assert!(matches!(resp, Response::Bulk(None)));

// Integer check
assert!(matches!(resp, Response::Integer(1)));
```

## Hash Tag Requirements

FrogDB always enforces cross-slot validation. Multi-key tests **must** use hash tags:

```rust
// WRONG — will get CROSSSLOT error
client.command(&["MSET", "key1", "v1", "key2", "v2"]).await;

// CORRECT — hash tags colocate keys
client.command(&["MSET", "{k}key1", "v1", "{k}key2", "v2"]).await;
```

Single-key tests don't need hash tags. SORT with BY/GET external keys needs hash tags
or a single-shard server.

## Template: New Regression Test

### 1. Create the test file

`frogdb-server/crates/redis-regression/tests/<category>_regression.rs`:

```rust
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn descriptive_test_name() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Setup
    assert_ok(&client.command(&["SET", "mykey", "myvalue"]).await);

    // Exercise the behavior being tested
    let resp = client.command(&["GET", "mykey"]).await;

    // Assert expected result
    assert_bulk_eq(&resp, b"myvalue");
}
```

### 2. Register in main.rs

Add to `frogdb-server/crates/redis-regression/tests/main.rs`:

```rust
mod category_regression;
```

Keep modules in alphabetical order.

### 3. Run the test

```bash
just test redis-regression descriptive_test_name
```

## Naming Conventions

- File: `<redis_suite_category>_regression.rs` (e.g., `sort_regression.rs`, `zset_regression.rs`)
- Test function: Descriptive snake_case that explains the behavior being verified
  - Good: `set_with_get_returns_previous_value`
  - Good: `sort_by_external_key_with_limit`
  - Bad: `test_issue_42`, `regression_1`

## When To Use Single-Shard Servers

Use `num_shards: Some(1)` when:
- SORT BY external key patterns (keys can't be hash-tagged since they're generated from
  pattern substitution)
- Tests where hash tags would make the test unreadable
- Reproducing Redis behavior that assumes single-threaded execution

```rust
async fn start_single_shard_server() -> TestServer {
    TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(1),
        ..Default::default()
    }).await
}
```

## Existing Regression Test Categories

| File | Covers |
|------|--------|
| `string_regression.rs` | SET GET, SUBSTR, STRLEN edge cases |
| `list_regression.rs` | LPUSH/RPUSH, LPOS, LMPOP |
| `list2_regression.rs` | Additional list operations |
| `list3_regression.rs` | List encoding stress tests |
| `set_regression.rs` | SADD, SMOVE, SRANDMEMBER |
| `hash_regression.rs` | HSET, HGETALL, HSCAN |
| `zset_regression.rs` | ZADD, ZSCORE, ZRANGEBYSCORE, ZPOPMIN |
| `sort_regression.rs` | SORT, SORT_RO, BY, GET, STORE |
| `expire_regression.rs` | EXPIRE, PEXPIRE, EXPIREAT, TTL |
| `scan_regression.rs` | SCAN, HSCAN, SSCAN, ZSCAN |
| `pubsub_regression.rs` | SUBSCRIBE, PUBLISH |
| `pubsubshard_regression.rs` | SSUBSCRIBE, SPUBLISH |
| `multi_regression.rs` | MULTI/EXEC/DISCARD/WATCH |
| `functions_regression.rs` | FUNCTION LOAD/CALL/LIST/DELETE |
| `hyperloglog_regression.rs` | PFADD, PFCOUNT, PFMERGE |
| `bitops_regression.rs` | BITOP, BITCOUNT, BITPOS |
| `geo_regression.rs` | GEOADD, GEODIST, GEOSEARCH |
| `incr_regression.rs` | INCR, INCRBY, INCRBYFLOAT, DECR |
| `keyspace_regression.rs` | DEL, EXISTS, RENAME, TYPE, OBJECT |
| `auth_regression.rs` | AUTH, requirepass |
| `acl_regression.rs` | ACL SETUSER/GETUSER/DELUSER |
| `acl_v2_regression.rs` | ACL v2 features |
| `info_regression.rs` | INFO sections |
| `info_command_regression.rs` | COMMAND INFO/COUNT/DOCS |
| `protocol_regression.rs` | RESP2/RESP3 protocol edge cases |
| `networking_regression.rs` | CLIENT SETNAME/GETNAME/ID |
| `other_regression.rs` | PING, ECHO, DBSIZE, RANDOMKEY |
| `quit_regression.rs` | QUIT command |
| `dump_regression.rs` | DUMP/RESTORE |
| `limits_regression.rs` | Key size, value size limits |
| `tracking_regression.rs` | CLIENT TRACKING |
| `stream_regression.rs` | XADD, XLEN, XRANGE, XREAD |
| `stream_cgroups_regression.rs` | XGROUP, XREADGROUP, XACK |
| `pause_regression.rs` | CLIENT PAUSE |
| `maxmemory_regression.rs` | MAXMEMORY, eviction |
| `cluster_scripting_regression.rs` | Cluster-mode scripting |
