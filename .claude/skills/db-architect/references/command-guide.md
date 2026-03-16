# Adding a New Command

Step-by-step guide for the most common FrogDB change: implementing a new Redis-compatible command.

## 1. Choose the Right Location

Data-structure commands go in `frogdb-server/crates/commands/src/`:

| Module | Data Type | Examples |
|--------|-----------|----------|
| `basic.rs` | Connection/keyless | PING, ECHO, QUIT |
| `string.rs` | Strings | SETNX, APPEND, INCR |
| `hash.rs` | Hashes | HSET, HGET, HDEL |
| `list.rs` | Lists | LPUSH, RPOP, LRANGE |
| `set.rs` | Sets | SADD, SREM, SMEMBERS |
| `sorted_set/` | Sorted sets | ZADD, ZRANGE, ZUNION |
| `stream/` | Streams | XADD, XREAD, XGROUP |
| `bitmap.rs` | Bitmaps | SETBIT, BITCOUNT |
| `geo.rs` | Geo | GEOADD, GEOSEARCH |
| `bloom.rs` | Bloom filters | BF.ADD, BF.EXISTS |
| `hyperloglog.rs` | HyperLogLog | PFADD, PFCOUNT |
| `json/` | JSON | JSON.SET, JSON.GET |
| `timeseries/` | TimeSeries | TS.ADD, TS.RANGE |
| `blocking.rs` | Blocking variants | BLPOP, BRPOP, BZPOPMIN |
| `expiry.rs` | TTL/expiry | EXPIRE, TTL, PERSIST |
| `generic.rs` | Key-agnostic | TYPE, RENAME, COPY |
| `scan.rs` | Iteration | SCAN, KEYS |
| `sort.rs` | Sorting | SORT, SORT_RO |

Server/admin commands go in `frogdb-server/crates/server/src/commands/`.

## 2. Choose ExecutionStrategy

```rust
// Most commands — single-key, route by hash
ExecutionStrategy::Standard

// Multi-key across shards — MGET, DEL, EXISTS, KEYS
ExecutionStrategy::ScatterGather { merge: MergeStrategy::OrderedArray }

// May block waiting for data — BLPOP, BRPOP
ExecutionStrategy::Blocking { default_timeout: None }

// All shards — SCAN, DBSIZE, FLUSHDB, RANDOMKEY
ExecutionStrategy::ServerWide(ServerWideOp::Scan)

// Handled by connection handler — pub/sub, auth, transactions
ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::PubSub)

// Cluster topology — CLUSTER ADDSLOTS, CLUSTER MEET
ExecutionStrategy::RaftConsensus

// External async I/O — MIGRATE, DEBUG SLEEP
ExecutionStrategy::AsyncExternal
```

**MergeStrategy** (for ScatterGather):

| Strategy | Use When | Examples |
|----------|----------|----------|
| `OrderedArray` | Response order must match key order | MGET |
| `SumIntegers` | Sum integer results from each shard | DEL, EXISTS, TOUCH, UNLINK |
| `CollectKeys` | Collect all keys into one array | KEYS |
| `CursoredScan` | Merge cursor-based iteration | SCAN |
| `AllOk` | All shards must succeed | MSET, FLUSHDB |
| `Custom` | Command implements own merge | Complex multi-shard ops |

## 3. Choose WalStrategy

```rust
// SET, APPEND, INCR, LPUSH, SADD, ZADD, HSET — persist the key's new value
WalStrategy::PersistFirstKey

// DEL, UNLINK, GETDEL — persist deletion for each key
WalStrategy::DeleteKeys

// LPOP, RPOP, SPOP, SREM, HDEL, LTRIM — persist or delete depending on result
WalStrategy::PersistOrDeleteFirstKey

// RENAME, RENAMENX — delete old key, persist new key
WalStrategy::RenameKeys

// RPOPLPUSH, LMOVE — persist-or-delete source, persist destination
WalStrategy::MoveKeys

// SINTERSTORE, COPY, ZRANGESTORE — persist destination at given arg index
WalStrategy::PersistDestination(0)  // arg index of destination key

// FLUSHDB, FLUSHALL — handled by RocksDB clear
WalStrategy::NoOp

// Read-only commands don't need WAL (default is fine)
// NEVER use WalStrategy::Infer for new commands
```

## 4. Set CommandFlags

Every command declares bitflags describing its behavior:

| Flag | When to Set |
|------|-------------|
| `WRITE` | Modifies data — triggers WAL, replication, memory check |
| `READONLY` | Pure read — safe on replicas |
| `FAST` | O(1) or O(log N) — used for latency classification |
| `BLOCKING` | May suspend waiting for data (BLPOP, BRPOP, etc.) |
| `MULTI_KEY` | Operates on multiple keys |
| `PUBSUB` | Pub/sub command |
| `SCRIPT` | Script execution command |
| `NOSCRIPT` | Cannot be called from Lua scripts |
| `LOADING` | Allowed while database is loading |
| `STALE` | Allowed on stale replica |
| `SKIP_SLOWLOG` | Should not be logged to slowlog |
| `RANDOM` | Involves random or non-deterministic data |
| `ADMIN` | Modifies server state |
| `NONDETERMINISTIC` | Returns time-varying data (e.g., TIME, RANDOMKEY) |
| `NO_PROPAGATE` | Not propagated to replicas |
| `TRACKS_KEYSPACE` | Tracks keyspace hit/miss metrics |

Combine with `|`: `CommandFlags::WRITE | CommandFlags::FAST`

## 5. Implement the Command

### Template: Standard Read Command

```rust
use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags};
use frogdb_protocol::Response;

pub struct FooCommand;

impl Command for FooCommand {
    fn name(&self) -> &'static str { "FOO" }

    fn arity(&self) -> Arity { Arity::Fixed(1) }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        match ctx.store.get_with_expiry_check(key) {
            Some(value) => {
                let sv = value.as_string().ok_or(CommandError::WrongType)?;
                Ok(Response::bulk(sv.as_bytes()))
            }
            None => Ok(Response::null()),
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() { vec![] } else { vec![&args[0]] }
    }
}
```

### Template: Standard Write Command

```rust
pub struct BarCommand;

impl Command for BarCommand {
    fn name(&self) -> &'static str { "BAR" }

    fn arity(&self) -> Arity { Arity::Fixed(2) }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy { WalStrategy::PersistFirstKey }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let value = &args[1];
        ctx.store.set(key.clone(), Value::string(value.clone()));
        Ok(Response::ok())
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() { vec![] } else { vec![&args[0]] }
    }
}
```

### Template: ScatterGather Command

```rust
pub struct BazCommand;

impl Command for BazCommand {
    fn name(&self) -> &'static str { "BAZ" }

    fn arity(&self) -> Arity { Arity::AtLeast(1) }

    fn flags(&self) -> CommandFlags { CommandFlags::READONLY | CommandFlags::FAST }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ScatterGather {
            merge: MergeStrategy::OrderedArray,
        }
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        // Execute on THIS shard's subset of keys only.
        // Connection handler splits keys by shard, sends to each, merges results.
        let results: Vec<Response> = args.iter()
            .map(|key| match ctx.store.get_with_expiry_check(key) {
                Some(v) => Response::bulk(v.as_string().map(|s| s.as_bytes()).unwrap_or_default()),
                None => Response::null(),
            })
            .collect();
        Ok(Response::Array(results))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        args.iter().map(|a| a.as_ref()).collect()
    }
}
```

### Template: Blocking Command

```rust
pub struct BquxCommand;

impl Command for BquxCommand {
    fn name(&self) -> &'static str { "BQUX" }

    fn arity(&self) -> Arity { Arity::AtLeast(2) } // key [key ...] timeout

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::BLOCKING | CommandFlags::FAST
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::Blocking { default_timeout: None }
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let timeout = parse_timeout(&args[args.len() - 1])?;
        let keys = &args[..args.len() - 1];

        // Try immediate operation
        for key in keys {
            if let Some(result) = try_pop(ctx, key)? {
                return Ok(result);
            }
        }

        // Signal blocking needed — connection handler manages the wait queue
        Ok(Response::BlockingNeeded {
            keys: keys.to_vec(),
            timeout,
            op: BlockingOp::BLPop, // or appropriate op
        })
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.len() < 2 { vec![] }
        else { args[..args.len() - 1].iter().map(|b| b.as_ref()).collect() }
    }

    fn requires_same_slot(&self) -> bool { true }
}
```

## 6. Register the Command

Add to `frogdb-server/crates/commands/src/lib.rs`:

```rust
// In register_all():
registry.register(module::FooCommand);
```

If adding a new module, also add `pub mod module;` at the top of `lib.rs`.

## 7. Write Integration Tests

Add tests in `frogdb-server/crates/server/tests/integration_{type}.rs`:

```rust
#[tokio::test]
async fn test_foo_basic() {
    let server = TestServer::start_standalone().await;
    let mut conn = server.connect().await;

    // Test the command
    conn.send_command(&["SET", "mykey", "hello"]).await;
    let resp = conn.send_command(&["FOO", "mykey"]).await;
    assert_eq!(resp, Response::bulk(Bytes::from("hello")));
}
```

For multi-key commands in the default 4-shard test server, use hash tags:

```rust
conn.send_command(&["MGET", "{t}key1", "{t}key2", "{t}key3"]).await;
```

## 8. Verification

```bash
just fmt frogdb-commands       # format
just lint frogdb-commands      # clippy
just test frogdb-commands      # unit tests
just test frogdb-server test_foo  # integration tests
```
