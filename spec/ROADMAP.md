# FrogDB Implementation Roadmap

This document tracks the implementation progress of FrogDB. Each phase has specific deliverables with checkboxes for progress tracking.

## Design Principles

1. **Build the skeleton first** - Establish correct abstractions from Phase 1, even as noops
2. **Avoid large refactors** - Include sharding infrastructure from day one
3. **Test as you go** - Each phase includes testing requirements
4. **Future features as noops** - WAL, ACL, replication hooks exist from Phase 1

---

## Current Status

**Phase**: 0 (Design)
**Next Milestone**: Phase 1 - Foundation

---

## Phase 1: Foundation

**Goal**: Complete architectural skeleton with working GET/SET. Includes sharding infrastructure (even with 1 shard) to avoid future refactoring.

### 1.1 Project Structure

- [ ] Create Cargo workspace with crates:
  - [ ] `frogdb-server/` - Main server binary
  - [ ] `frogdb-core/` - Core data structures and traits
  - [ ] `frogdb-protocol/` - RESP protocol handling
- [ ] Configure dependencies:
  - [ ] `tokio` (rt-multi-thread, net, sync, macros)
  - [ ] `bytes`
  - [ ] `redis-protocol` v5 with `bytes` and `codec` features
  - [ ] `tracing` + `tracing-subscriber` (with `json` feature)
  - [ ] `griddle` (HashMap without resize spikes)
  - [ ] `figment` with `toml` and `env` features (layered configuration)
  - [ ] `serde` + `serde_derive` (config deserialization)
  - [ ] `clap` with `derive` feature (CLI argument parsing)

### 1.2 Protocol Layer

- [ ] `ParsedCommand` struct (name: Bytes, args: Vec<Bytes>)
- [ ] `Response` enum:
  - [ ] RESP2 types: Simple, Error, Integer, Bulk, Array
  - [ ] RESP3 types: defined but `unimplemented!()` (future-proofing)
- [ ] `ProtocolVersion` enum (Resp2 default)
- [ ] Frame conversions (BytesFrame <-> ParsedCommand, Response -> BytesFrame)

### 1.3 Core Traits & Types

- [ ] `Command` trait:
  ```rust
  pub trait Command: Send + Sync {
      fn name(&self) -> &'static str;
      fn arity(&self) -> Arity;
      fn flags(&self) -> CommandFlags;
      fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError>;
      fn keys(&self, args: &[Bytes]) -> Vec<&[u8]>;
  }
  ```
- [ ] `Arity` enum (Fixed, AtLeast, Range)
- [ ] `CommandFlags` bitflags (WRITE, READONLY, FAST, MULTI_KEY, BLOCKING, PUBSUB, SCRIPT)
- [ ] `Store` trait (get, set, delete, contains, key_type, len, memory_used, scan)
- [ ] `HashMapStore` default implementation using `griddle::HashMap`
- [ ] `Value` enum in `crate::types` (String variant only for Phase 1)
- [ ] `StringValue` struct (avoids conflict with `std::String`):
  ```rust
  pub struct StringValue {
      data: StringData,
  }
  enum StringData {
      Raw(Bytes),
      Integer(i64),
  }
  ```
- [ ] `KeyMetadata` struct (expires_at, last_access, lfu_counter, memory_size)
- [ ] `CommandContext` struct
- [ ] `CommandError` enum with RESP error mappings
- [ ] Command registry (HashMap<&'static str, Arc<dyn Command>>)

### 1.4 Shard Infrastructure

- [ ] `ShardWorker` struct:
  - [ ] Local `Store` instance
  - [ ] `mpsc::Receiver<ShardMessage>` for cross-shard requests
  - [ ] `mpsc::Receiver<NewConnection>` for new connections
  - [ ] shard_id
- [ ] `ShardMessage` enum:
  - [ ] Execute { command, response_tx }
  - [ ] Shutdown
  - [ ] ScatterRequest (placeholder)
- [ ] `NewConnection` struct (socket, addr, conn_id)
- [ ] Key hashing: `shard_for_key(key, num_shards)` with hash tag support
- [ ] Channel topology: `Arc<Vec<mpsc::Sender<ShardMessage>>>`
- [ ] Shard worker event loop with `tokio::select!`

### 1.5 Connection Management

- [ ] `ConnectionState` struct:
  - [ ] id, addr, created_at
  - [ ] auth: AuthState (placeholder - always authenticated)
  - [ ] protocol_version: ProtocolVersion
  - [ ] tx_queue: Option<Vec<ParsedCommand>> (placeholder)
  - [ ] subscriptions, patterns, pubsub_mode (placeholders)
  - [ ] blocked: Option<BlockedState> (placeholder)
- [ ] `ConnectionAssigner` trait
- [ ] `RoundRobinAssigner` implementation
- [ ] Connection loop:
  - [ ] Read frame via Tokio codec
  - [ ] Parse to ParsedCommand
  - [ ] Auth check (noop - always pass)
  - [ ] Command lookup
  - [ ] Arity validation
  - [ ] Route and execute
  - [ ] Encode and send response

### 1.6 Configuration

Full layered configuration from Phase 1 (see [CONFIGURATION.md](CONFIGURATION.md) for details):

- [ ] `Config` struct with serde derive:
  ```rust
  #[derive(Debug, Deserialize)]
  pub struct Config {
      pub server: ServerConfig,
      pub logging: LoggingConfig,
      // Other sections as placeholders with defaults
  }
  ```
- [ ] Configuration loading via Figment (priority: CLI > env > TOML > defaults):
  ```rust
  Figment::new()
      .merge(Serialized::defaults(Config::default()))
      .merge(Toml::file("frogdb.toml").nested())
      .merge(Env::prefixed("FROGDB_").split("__"))
      .merge(Serialized::globals(cli_overrides))
  ```
- [ ] CLI arguments via clap:
  - [ ] `--config <FILE>` - Path to TOML config file
  - [ ] `--bind <ADDR>` - Override bind address
  - [ ] `--port <PORT>` - Override listen port
  - [ ] `--shards <N>` - Override number of shards
  - [ ] `--log-level <LEVEL>` - Override log level
  - [ ] `--log-format <FORMAT>` - Override log format (pretty/json)
- [ ] Default `frogdb.toml` generation on first run (optional, with `--generate-config`)
- [ ] Config validation at startup (fail fast on invalid values)

### 1.7 Server & Acceptor

- [ ] Acceptor task:
  - [ ] TCP listener
  - [ ] maxclients check (placeholder)
  - [ ] Round-robin shard assignment
  - [ ] Send NewConnection to shard
- [ ] Server startup:
  - [ ] Load configuration via Figment (TOML + env + CLI)
  - [ ] Initialize logging with configured format and level
  - [ ] Spawn shard workers (configurable, default: num_cpus)
  - [ ] Start acceptor
  - [ ] Log ready message with bound address
- [ ] Graceful shutdown (SIGTERM/SIGINT)

### 1.8 Routing

- [ ] `route_and_execute` function:
  - [ ] Extract keys via `command.keys(args)`
  - [ ] Keyless commands: execute locally
  - [ ] Single-key: route to owner shard
  - [ ] Multi-key: placeholder (error or single-shard only)
- [ ] `execute_local` function
- [ ] `execute_remote` function (send via channel, await oneshot response)

### 1.9 Initial Commands

- [ ] `PING` (keyless, returns PONG or echoes argument)
- [ ] `ECHO` (keyless)
- [ ] `QUIT` (close connection)
- [ ] `COMMAND` / `COMMAND DOCS` (placeholder - return empty)
- [ ] `SET` (key value, no options yet)
- [ ] `GET`
- [ ] `DEL` (single key)
- [ ] `EXISTS` (single key)

### 1.10 Noop Abstractions (Critical for Future)

These must exist as traits/stubs to avoid refactoring:

**Persistence & Replication:**
- [ ] `WalWriter` trait with noop implementation
- [ ] `ReplicationConfig` enum (Standalone/Primary/Replica)
- [ ] `ReplicationTracker` trait with noop implementation (for sync replication ACKs)
- [ ] Sequence number assignment hook in command flow (returns 0 for noop)

**Security:**
- [ ] `AclChecker` trait with `AlwaysAllow` implementation

**Expiry:**
- [ ] `ExpiryIndex` struct (empty, no-op methods)

**Observability (OpenTelemetry-ready):**
- [ ] `MetricsRecorder` trait with noop implementation:
  - `increment_counter(name, labels)`
  - `record_histogram(name, value, labels)`
  - `set_gauge(name, value, labels)`
- [ ] `Tracer` trait with noop implementation:
  - `start_span(name) -> Span`
  - `Span::set_attribute(key, value)`
  - `Span::end()`
- [ ] Structured logging setup with `tracing` crate (this IS implemented, not noop):
  - [ ] Configurable format via `logging.format` config:
    - `pretty` - Human-readable, colored output for development
    - `json` - Machine-parseable JSON lines for production
  - [ ] Configurable level via `logging.level` config (trace/debug/info/warn/error)
  - [ ] Log subscriber initialization based on config

### 1.11 Testing

**Integration Tests** (tests/ directory):

- [ ] `TestServer` helper:
  ```rust
  struct TestServer {
      addr: SocketAddr,
      shutdown_tx: oneshot::Sender<()>,
      handle: JoinHandle<()>,
  }
  impl TestServer {
      async fn start() -> Self;
      async fn start_with_config(config: Config) -> Self;
      fn addr(&self) -> SocketAddr;
      async fn connection(&self) -> redis::aio::Connection;
  }
  ```
- [ ] Integration test: connect, SET foo bar, GET foo, assert "bar"
- [ ] Test: PING returns PONG
- [ ] Test: unknown command returns error
- [ ] Test: GET nonexistent returns nil
- [ ] Test: wrong arity returns error

**Unit Tests** (inline #[cfg(test)] modules):

- [ ] Store trait implementation tests:
  - [ ] HashMapStore get/set/delete operations
  - [ ] Memory accounting accuracy
  - [ ] Key existence checks
- [ ] Command trait tests:
  - [ ] Arity validation (Fixed, AtLeast, Range)
  - [ ] CommandFlags bitflag operations
- [ ] Routing tests:
  - [ ] `shard_for_key()` hash distribution
  - [ ] Hash tag extraction `{...}` handling
- [ ] Configuration tests:
  - [ ] Config default values
  - [ ] Figment layering priority (CLI > env > TOML > defaults)
  - [ ] Invalid config rejection
- [ ] Protocol tests:
  - [ ] ParsedCommand construction
  - [ ] Response encoding for each variant

### 1.12 Documentation

- [ ] Update INDEX.md roadmap section to link here
- [ ] Add inline documentation to public types
- [ ] README with build/run instructions

---

## Phase 2: Complete String Commands & TTL

**Goal**: Full string command set including numeric operations and key expiration.

**Depends on**: Phase 1

### 2.1 Integer Encoding

- [ ] Implement `StringValue::as_integer()` -> Option<i64>
- [ ] Auto-detect integer on SET
- [ ] Maintain encoding through operations

### 2.2 String Commands

- [ ] `SET` options: EX, PX, EXAT, PXAT, NX, XX, KEEPTTL, GET, IFEQ, IFGT
- [ ] `SETNX`
- [ ] `SETEX`, `PSETEX`
- [ ] `APPEND`
- [ ] `STRLEN`
- [ ] `INCR`, `DECR`
- [ ] `INCRBY`, `DECRBY`
- [ ] `INCRBYFLOAT`
- [ ] `GETRANGE` (substring)
- [ ] `SETRANGE`
- [ ] `GETDEL`
- [ ] `GETEX`

### 2.3 Key Expiration

- [ ] Implement `ExpiryIndex`:
  ```rust
  pub struct ExpiryIndex {
      by_time: BTreeMap<(Instant, Bytes), ()>,
      by_key: HashMap<Bytes, Instant>,
  }
  ```
- [ ] Lazy expiry: check TTL on every read, delete if expired
- [ ] `EXPIRE`, `PEXPIRE`
- [ ] `EXPIREAT`, `PEXPIREAT`
- [ ] `TTL`, `PTTL`
- [ ] `PERSIST`
- [ ] `EXPIRETIME`, `PEXPIRETIME`

### 2.4 Active Expiry

- [ ] Background task per shard (~10Hz)
- [ ] Sample keys from ExpiryIndex
- [ ] Time-budgeted deletion (25ms default)
- [ ] Metrics: expired_keys counter

### 2.5 Generic Commands

- [ ] `TYPE`
- [ ] `RENAME` (same-shard only, CROSSSLOT error otherwise)
- [ ] `RENAMENX`
- [ ] `TOUCH`
- [ ] `UNLINK` (async delete)
- [ ] `OBJECT ENCODING`
- [ ] `OBJECT FREQ`
- [ ] `OBJECT IDLETIME`
- [ ] `DEBUG OBJECT` (basic)

### 2.6 Testing

- [ ] Test all SET options
- [ ] Test INCR overflow (return error, not wrap)
- [ ] Test INCR on non-integer string (error)
- [ ] Test TTL expiration (lazy)
- [ ] Test RENAME same-shard requirement
- [ ] Property tests: INCR/DECR roundtrip

---

## Phase 3: Sorted Sets

**Goal**: Complete sorted set implementation (moved earlier since it's a core data type).

**Depends on**: Phase 2

### 3.1 Data Structure

- [ ] `SortedSet` in `crate::types`:
  ```rust
  pub struct SortedSet {
      members: HashMap<Bytes, f64>,
      scores: BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
  }
  ```
- [ ] Add to `Value` enum

### 3.2 Basic Commands

- [ ] `ZADD` (NX, XX, GT, LT, CH, INCR options)
- [ ] `ZREM`
- [ ] `ZSCORE`
- [ ] `ZMSCORE`
- [ ] `ZCARD`
- [ ] `ZINCRBY`

### 3.3 Ranking

- [ ] `ZRANK`
- [ ] `ZREVRANK`

### 3.4 Range Queries

- [ ] `ZRANGE` (unified: BYSCORE, BYLEX, REV, LIMIT, WITHSCORES)
- [ ] `ZRANGEBYSCORE` / `ZREVRANGEBYSCORE` (legacy)
- [ ] `ZRANGEBYLEX` / `ZREVRANGEBYLEX` (legacy)
- [ ] `ZCOUNT`
- [ ] `ZLEXCOUNT`

### 3.5 Pop & Random

- [ ] `ZPOPMIN`, `ZPOPMAX`
- [ ] `ZMPOP`
- [ ] `ZRANDMEMBER`

### 3.6 Set Operations (Same-Slot Required)

- [ ] `ZUNION` / `ZUNIONSTORE`
- [ ] `ZINTER` / `ZINTERSTORE` / `ZINTERCARD`
- [ ] `ZDIFF` / `ZDIFFSTORE`

### 3.7 Other

- [ ] `ZSCAN`
- [ ] `ZRANGESTORE`
- [ ] `ZREMRANGEBYRANK`
- [ ] `ZREMRANGEBYSCORE`
- [ ] `ZREMRANGEBYLEX`

### 3.8 Testing

- [ ] Test ZADD options
- [ ] Test range queries (score bounds, lex bounds)
- [ ] Test set operations
- [ ] Property tests: score ordering consistency

---

## Phase 4: Multi-Shard Operations

**Goal**: Enable multiple shards with proper CROSSSLOT validation and all-shard scatter-gather.

**Depends on**: Phase 3

### 4.1 Multi-Shard Initialization

- [ ] `num_shards` config option (default: available_parallelism)
- [ ] Spawn N shard workers
- [ ] Verify channel topology

### 4.2 Hash Tags & Slot Validation

- [ ] `extract_hash_tag(key)` function
- [ ] Hash slot calculation: `crc16(tag) % 16384`
- [ ] Internal shard routing: `hash_slot % num_shards`
- [ ] Same hash tag = same slot = same internal shard
- [ ] Test: `{user:1}:profile` and `{user:1}:session` go to same shard

### 4.3 CROSSSLOT Validation

- [ ] `allow_cross_slot_standalone` config option (default: `false`)
- [ ] When disabled (default): validate multi-key commands, return `-CROSSSLOT` error
- [ ] When enabled: allow cross-shard scatter-gather for MGET/MSET (NOT atomic for MSET)
- [ ] Commands requiring same-slot by default: MGET, MSET, MSETNX, DEL (multi), EXISTS (multi)
- [ ] Note: MSETNX always requires same-slot (atomic semantics require it)

### 4.4 Multi-Key Commands

- [ ] `MGET`:
  - Default: same-slot required
  - With `allow_cross_slot_standalone`: scatter-gather across shards
- [ ] `MSET`:
  - Default: same-slot required (atomic)
  - With `allow_cross_slot_standalone`: scatter-gather (NOT atomic, document clearly)
- [ ] `MSETNX` (always same-slot required - atomic semantics)
- [ ] `DEL` multi-key:
  - Default: same-slot required
  - With config: scatter-gather
- [ ] `EXISTS` multi-key: same behavior as DEL
- [ ] `TOUCH` multi-key: same behavior as DEL
- [ ] `UNLINK` multi-key: same behavior as DEL

### 4.5 Scatter-Gather (All-Shard Operations Only)

Scatter-gather is for operations that inherently touch ALL shards:

- [ ] `ScatterRequest` message type
- [ ] `execute_scatter_gather` function:
  - [ ] Send request to all N shards
  - [ ] Await all responses
  - [ ] Aggregate results (sum, merge, etc.)
- [ ] Timeout handling
- [ ] Used by: SCAN, KEYS, DBSIZE, FLUSHDB, INFO (in later phases)

### 4.6 VLL Transaction Ordering (Foundation)

- [ ] Global `AtomicU64` transaction ID counter
- [ ] Include txid in Execute messages
- [ ] Per-shard pending queue (for future use)

### 4.7 Testing

- [ ] Test MGET with hash tags (same slot, works)
- [ ] Test MGET without hash tags (different slots, CROSSSLOT error)
- [ ] Test hash tag colocation
- [ ] Stress test: 100 concurrent clients, random keys
- [ ] Test CROSSSLOT error for various multi-key commands

---

## Phase 5: Persistence

**Goal**: Durable storage with RocksDB, WAL, and snapshots.

**Depends on**: Phase 4

### 5.1 RocksDB Setup

- [ ] Add `rust-rocksdb` dependency
- [ ] Data directory structure
- [ ] One column family per shard
- [ ] RocksDB options (write buffer, compression)

### 5.2 Value Serialization

- [ ] Binary format:
  ```
  [type: u8][flags: u8][expires_at: i64][lfu: u8][len: u32][data...]
  ```
- [ ] Serialize/deserialize StringValue, SortedSet
- [ ] (Future types added in later phases)

### 5.3 WAL Integration

- [ ] Real `WalWriter` implementation
- [ ] WriteBatch accumulator
- [ ] Batch triggers: size threshold, time interval
- [ ] Durability modes:
  - [ ] Async (batch, no fsync)
  - [ ] Periodic (fsync every N ms)
  - [ ] Sync (fsync every write)
- [ ] Hook into command execution (write commands append to WAL)

### 5.4 Recovery

- [ ] Startup recovery sequence:
  - [ ] Detect existing data directory
  - [ ] Load from RocksDB column families
  - [ ] Rebuild in-memory stores
  - [ ] Rebuild expiry indexes
- [ ] Skip expired keys on load
- [ ] Log recovery statistics

### 5.5 Snapshots

- [ ] Snapshot coordinator
- [ ] Forkless snapshot (iterate + COW for concurrent writes)
- [ ] Snapshot metadata (epoch, timestamp)
- [ ] `BGSAVE` command
- [ ] `LASTSAVE` command

### 5.6 Backup/Restore

- [ ] `DUMP` command (serialize single key)
- [ ] `RESTORE` command (deserialize single key)

### 5.7 Testing

- [ ] Test: write, kill, restart, verify data present
- [ ] Test: recovery with expired keys (should not load)
- [ ] Test: snapshot during writes
- [ ] Stress test: persistence under load

---

## Phase 6: Hash, List, Set Types

**Goal**: Remaining core data types.

**Depends on**: Phase 5 (Persistence)

### 6.1 Hash

- [ ] `HashValue` in `crate::types` (HashMap<Bytes, Bytes>)
- [ ] `HSET`, `HSETNX`, `HGET`, `HDEL`
- [ ] `HMSET`, `HMGET`
- [ ] `HGETALL`, `HKEYS`, `HVALS`
- [ ] `HEXISTS`, `HLEN`
- [ ] `HINCRBY`, `HINCRBYFLOAT`
- [ ] `HSTRLEN`
- [ ] `HSCAN`
- [ ] `HRANDFIELD`

### 6.2 List

- [ ] `List` in `crate::types` (VecDeque<Bytes>)
- [ ] `LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`
- [ ] `LPOP`, `RPOP`, `LMPOP`
- [ ] `LRANGE`, `LINDEX`, `LSET`
- [ ] `LLEN`, `LPOS`
- [ ] `LINSERT`
- [ ] `LREM`, `LTRIM`
- [ ] `LMOVE`, `LMPOP`

### 6.3 Set

- [ ] `SetValue` in `crate::types` (HashSet<Bytes>)
- [ ] `SADD`, `SREM`
- [ ] `SMEMBERS`, `SISMEMBER`, `SMISMEMBER`
- [ ] `SCARD`
- [ ] `SUNION`, `SINTER`, `SDIFF` (same-shard)
- [ ] `SUNIONSTORE`, `SINTERSTORE`, `SDIFFSTORE`, `SINTERCARD`
- [ ] `SRANDMEMBER`, `SPOP`
- [ ] `SMOVE`
- [ ] `SSCAN`

### 6.4 Persistence

- [ ] Serialization for Hash, List, Set
- [ ] Recovery tests

### 6.5 Testing

- [ ] Comprehensive command tests
- [ ] WRONGTYPE error tests
- [ ] Persistence tests

---

## Phase 7: Transactions & Pub/Sub

**Goal**: MULTI/EXEC, WATCH, and publish/subscribe.

**Depends on**: Phase 6

### 7.1 Transactions

- [ ] `MULTI` - start transaction
- [ ] Queue commands (return QUEUED)
- [ ] `EXEC` - execute atomically (same-shard requirement)
- [ ] `DISCARD` - abort transaction
- [ ] `WATCH` - optimistic locking
- [ ] `UNWATCH`
- [ ] Key versioning for WATCH detection

### 7.2 Pub/Sub - Broadcast

- [ ] `SUBSCRIBE`, `UNSUBSCRIBE`
- [ ] `PSUBSCRIBE`, `PUNSUBSCRIBE`
- [ ] `PUBLISH` (fan-out to all shards)
- [ ] Pub/sub mode restrictions

### 7.3 Pub/Sub - Sharded

- [ ] `SSUBSCRIBE`, `SUNSUBSCRIBE`
- [ ] `SPUBLISH` (route to channel's shard)

### 7.4 Pub/Sub - Introspection

- [ ] `PUBSUB CHANNELS`
- [ ] `PUBSUB NUMSUB`
- [ ] `PUBSUB NUMPAT`
- [ ] `PUBSUB SHARDCHANNELS`
- [ ] `PUBSUB SHARDNUMSUB`

### 7.5 Testing

- [ ] Test MULTI/EXEC flow
- [ ] Test WATCH conflict detection
- [ ] Test broadcast pub/sub
- [ ] Test sharded pub/sub
- [ ] Test pattern subscriptions

---

## Phase 8: Lua Scripting

**Goal**: Lua script execution with strict key validation.

**Depends on**: Phase 7

### 8.1 Lua VM

- [ ] Add `mlua` dependency
- [ ] Per-shard Lua VM instances
- [ ] Resource limits (execution time, memory)

### 8.2 Commands

- [ ] `EVAL`
- [ ] `EVALSHA`
- [ ] `SCRIPT LOAD`
- [ ] `SCRIPT EXISTS`
- [ ] `SCRIPT FLUSH`
- [ ] `SCRIPT KILL`

### 8.3 Bindings

- [ ] `redis.call()` - execute command, propagate errors
- [ ] `redis.pcall()` - execute command, return errors
- [ ] Strict key validation (keys must be in KEYS array)
- [ ] Script routing to owner shard

### 8.4 Caching

- [ ] Script cache (SHA1 -> bytecode)
- [ ] LRU eviction

### 8.5 Testing

- [ ] Test basic script execution
- [ ] Test strict key validation
- [ ] Test script caching
- [ ] Test timeout

---

## Phase 9: Key Iteration & Server Commands

**Goal**: SCAN, INFO, and server administration.

**Depends on**: Phase 8

### 9.1 SCAN

- [ ] Cursor format: `shard_id (16 bits) | position (48 bits)`
- [ ] `SCAN` (scatter to all shards, merge cursors)
- [ ] MATCH pattern filtering
- [ ] COUNT hint
- [ ] TYPE filter
- [ ] `SSCAN`, `HSCAN`, `ZSCAN` (already done with data types)

### 9.2 KEYS

- [ ] `KEYS pattern` (scatter-gather, warn about production use)

### 9.3 INFO

- [ ] `INFO [section]`
- [ ] Sections: server, clients, memory, persistence, stats, replication, cpu, keyspace

### 9.4 Server Commands

- [ ] `DBSIZE`
- [ ] `FLUSHDB`, `FLUSHALL`
- [ ] `TIME`
- [ ] `DEBUG SLEEP`
- [ ] `SHUTDOWN`
- [ ] `COMMAND`, `COMMAND COUNT`, `COMMAND DOCS`

### 9.5 Testing

- [ ] Test SCAN completeness (all keys returned exactly once)
- [ ] Test cursor handling across shards
- [ ] Test INFO sections

---

## Phase 10: Production Readiness

**Goal**: Metrics, configuration, ACL, and operational tooling.

**Depends on**: Phase 9

### 10.1 Metrics

- [ ] Prometheus endpoint (`:9090/metrics` or configurable)
- [ ] Connection metrics (total, current, rejected)
- [ ] Command metrics (per-command counters, latency histograms)
- [ ] Memory metrics (used, peak, fragmentation)
- [ ] Keyspace metrics (keys per DB/shard)
- [ ] Persistence metrics (WAL pending, last save time)

### 10.2 Configuration

- [ ] TOML config file support
- [ ] Environment variables (FROGDB_ prefix)
- [ ] `CONFIG GET`
- [ ] `CONFIG SET` (mutable parameters)
- [ ] Document mutable vs immutable parameters

### 10.3 Client Commands

- [ ] `CLIENT LIST`
- [ ] `CLIENT ID`
- [ ] `CLIENT SETNAME`, `CLIENT GETNAME`
- [ ] `CLIENT KILL`
- [ ] `CLIENT PAUSE`, `CLIENT UNPAUSE`
- [ ] `CLIENT INFO`

### 10.4 SLOWLOG

- [ ] Slow query threshold config
- [ ] `SLOWLOG GET [count]`
- [ ] `SLOWLOG LEN`
- [ ] `SLOWLOG RESET`

### 10.5 ACL

- [ ] `AclManager`
- [ ] `AUTH`
- [ ] `ACL SETUSER`, `ACL DELUSER`
- [ ] `ACL LIST`, `ACL GETUSER`
- [ ] `ACL CAT`
- [ ] `ACL WHOAMI`
- [ ] Permission checking hooks

### 10.6 Memory Management

- [ ] `max_memory` config
- [ ] OOM error on writes when exceeded
- [ ] Eviction policies: volatile-lru, allkeys-lru, volatile-lfu, allkeys-lfu, volatile-ttl
- [ ] Eviction sampling

### 10.7 Testing

- [ ] Test metrics accuracy
- [ ] Test ACL enforcement
- [ ] Test eviction behavior
- [ ] Load test with monitoring

---

## Phase 11: Blocking Commands

**Goal**: BLPOP, BRPOP, and blocking sorted set operations.

**Depends on**: Phase 10

### 11.1 Infrastructure

- [ ] `BlockedState` in ConnectionState
- [ ] Blocking key registry per shard
- [ ] Timeout handling

### 11.2 Commands

- [ ] `BLPOP`, `BRPOP`
- [ ] `BLMOVE`
- [ ] `BLMPOP`
- [ ] `BZPOPMIN`, `BZPOPMAX`
- [ ] `BZMPOP`

### 11.3 Unblocking

- [ ] Notify on key modification
- [ ] Timeout expiration
- [ ] Client disconnect handling

### 11.4 Testing

- [ ] Test basic blocking
- [ ] Test timeout
- [ ] Test concurrent blocking
- [ ] Test same-shard requirement

---

## Phase 12: RESP3 Protocol

**Goal**: RESP3 support for modern clients.

**Depends on**: Phase 11

### 12.1 Protocol Negotiation

- [ ] `HELLO` command
- [ ] Protocol version tracking per connection

### 12.2 RESP3 Types

- [ ] Null encoding
- [ ] Double encoding
- [ ] Boolean encoding
- [ ] Map encoding
- [ ] Set encoding
- [ ] Push encoding (pub/sub)

### 12.3 Response Updates

- [ ] HGETALL returns Map in RESP3
- [ ] SMEMBERS returns Set in RESP3
- [ ] Scores return Double in RESP3

### 12.4 Testing

- [ ] Test HELLO negotiation
- [ ] Test RESP3 responses
- [ ] Test RESP2 backwards compatibility

---

## Future Phases

### Phase 13: Streams
- [ ] `Stream` data type in `crate::types`
- [ ] XADD, XREAD, XRANGE, XLEN
- [ ] Consumer groups

### Phase 14: Clustering
- [ ] Replication via WAL streaming
- [ ] CLUSTER commands
- [ ] Hash slot migration
- [ ] Failover

### Phase 15: Advanced Testing
- [ ] Shuttle concurrency tests
- [ ] Loom primitive tests
- [ ] Jepsen integration
- [ ] Redis compatibility suite

### Phase 16: Performance
- [ ] io_uring (optional)
- [ ] Skip list for sorted sets (optional)
- [ ] Connection pooling optimizations

---

## Critical Abstractions

These must exist from Phase 1 to avoid refactoring:

| Abstraction | Phase 1 | Full Implementation |
|-------------|---------|---------------------|
| `Store` trait | HashMapStore | Same |
| `Command` trait | Full | Same |
| `Value` enum | StringValue only | SortedSet (Phase 3), Hash/List/Set (Phase 6) |
| `WalWriter` trait | Noop | RocksDB WAL (Phase 5) |
| `ReplicationConfig` | Standalone | Primary/Replica (Phase 14) |
| `ReplicationTracker` trait | Noop | WAL streaming (Phase 14) |
| `AclChecker` trait | AlwaysAllow | Full ACL (Phase 10) |
| `MetricsRecorder` trait | Noop | Prometheus (Phase 10) |
| `Tracer` trait | Noop | OpenTelemetry (Phase 10) |
| Shard channels | 1 shard | N shards (Phase 4) |
| `ExpiryIndex` | Empty | Functional (Phase 2) |
| `ProtocolVersion` | Resp2 only | Resp2 + Resp3 (Phase 12) |
| `Config` + Figment | Full (CLI + TOML + env) | CONFIG GET/SET (Phase 10) |
| Logging format | pretty + json | Same |

---

## References

- [INDEX.md](INDEX.md) - Architecture overview
- [EXECUTION.md](EXECUTION.md) - Command flow
- [STORAGE.md](STORAGE.md) - Data structures
- [CONCURRENCY.md](CONCURRENCY.md) - Threading model
- [PROTOCOL.md](PROTOCOL.md) - RESP handling
- [PERSISTENCE.md](PERSISTENCE.md) - RocksDB integration
- [CONFIGURATION.md](CONFIGURATION.md) - Configuration system
- [TESTING.md](TESTING.md) - Test strategy
