# FrogDB Implementation Roadmap

This document tracks the implementation progress of FrogDB. Each phase has specific deliverables with checkboxes for progress tracking.

## Design Principles

1. **Build the skeleton first** - Establish correct abstractions from Phase 1, even as noops
2. **Avoid large refactors** - Include sharding infrastructure from day one
3. **Test as you go** - Each phase includes testing requirements
4. **Future features as noops** - WAL, ACL, replication hooks exist from Phase 1

---

## Current Status

**Phase**: 10.1 (Metrics - partial) ✓
**Next Milestone**: Phase 10.1 completion (command metrics, keyspace metrics)

---

## Phase 1: Foundation

**Goal**: Complete architectural skeleton with working GET/SET. Includes sharding infrastructure (even with 1 shard) to avoid future refactoring.

### 1.1 Project Structure

- [x] Create Cargo workspace with crates:
  - [x] `frogdb-server/` - Main server binary
  - [x] `frogdb-core/` - Core data structures and traits
  - [x] `frogdb-protocol/` - RESP protocol handling
- [x] Configure dependencies:
  - [x] `tokio` (rt-multi-thread, net, sync, macros)
  - [x] `bytes`
  - [x] `redis-protocol` v5 with `bytes` and `codec` features
  - [x] `tracing` + `tracing-subscriber` (with `json` feature)
  - [x] `griddle` (HashMap without resize spikes)
  - [x] `figment` with `toml` and `env` features (layered configuration)
  - [x] `serde` + `serde_derive` (config deserialization)
  - [x] `clap` with `derive` feature (CLI argument parsing)

### 1.2 Protocol Layer

- [x] `ParsedCommand` struct (name: Bytes, args: Vec<Bytes>)
- [x] `Response` enum:
  - [x] RESP2 types: Simple, Error, Integer, Bulk, Array
  - [x] RESP3 types: defined but `unimplemented!()` (future-proofing)
- [x] `ProtocolVersion` enum (Resp2 default)
- [x] Frame conversions (BytesFrame <-> ParsedCommand, Response -> BytesFrame)

### 1.3 Core Traits & Types

- [x] `Command` trait:
  ```rust
  pub trait Command: Send + Sync {
      fn name(&self) -> &'static str;
      fn arity(&self) -> Arity;
      fn flags(&self) -> CommandFlags;
      fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError>;
      fn keys(&self, args: &[Bytes]) -> Vec<&[u8]>;
  }
  ```
- [x] `Arity` enum (Fixed, AtLeast, Range)
- [x] `CommandFlags` bitflags (WRITE, READONLY, FAST, MULTI_KEY, BLOCKING, PUBSUB, SCRIPT)
- [x] `Store` trait (get, set, delete, contains, key_type, len, memory_used, scan)
- [x] `HashMapStore` default implementation using `griddle::HashMap`
- [x] `Value` enum in `crate::types` (String variant only for Phase 1)
- [x] `StringValue` struct (avoids conflict with `std::String`):
  ```rust
  pub struct StringValue {
      data: StringData,
  }
  enum StringData {
      Raw(Bytes),
      Integer(i64),
  }
  ```
- [x] `KeyMetadata` struct (expires_at, last_access, lfu_counter, memory_size)
- [x] `CommandContext` struct
- [x] `CommandError` enum with RESP error mappings
- [x] Command registry (HashMap<&'static str, Arc<dyn Command>>)

### 1.4 Shard Infrastructure

- [x] `ShardWorker` struct:
  - [x] Local `Store` instance
  - [x] `mpsc::Receiver<ShardMessage>` for cross-shard requests
  - [x] `mpsc::Receiver<NewConnection>` for new connections
  - [x] shard_id
- [x] `ShardMessage` enum:
  - [x] Execute { command, response_tx }
  - [x] Shutdown
  - [x] ScatterRequest (placeholder)
- [x] `NewConnection` struct (socket, addr, conn_id)
- [x] Key hashing: `shard_for_key(key, num_shards)` with hash tag support
- [x] Channel topology: `Arc<Vec<mpsc::Sender<ShardMessage>>>`
- [x] Shard worker event loop with `tokio::select!`

### 1.5 Connection Management

- [x] `ConnectionState` struct:
  - [x] id, addr, created_at
  - [x] auth: AuthState (placeholder - always authenticated)
  - [x] protocol_version: ProtocolVersion
  - [x] tx_queue: Option<Vec<ParsedCommand>> (placeholder)
  - [x] subscriptions, patterns, pubsub_mode (placeholders)
  - [x] blocked: Option<BlockedState> (placeholder)
- [x] `ConnectionAssigner` trait
- [x] `RoundRobinAssigner` implementation
- [x] Connection loop:
  - [x] Read frame via Tokio codec
  - [x] Parse to ParsedCommand
  - [x] Auth check (noop - always pass)
  - [x] Command lookup
  - [x] Arity validation
  - [x] Route and execute
  - [x] Encode and send response

### 1.6 Configuration

Full layered configuration from Phase 1 (see [CONFIGURATION.md](CONFIGURATION.md) for details):

- [x] `Config` struct with serde derive:
  ```rust
  #[derive(Debug, Deserialize)]
  pub struct Config {
      pub server: ServerConfig,
      pub logging: LoggingConfig,
      // Other sections as placeholders with defaults
  }
  ```
- [x] Configuration loading via Figment (priority: CLI > env > TOML > defaults):
  ```rust
  Figment::new()
      .merge(Serialized::defaults(Config::default()))
      .merge(Toml::file("frogdb.toml").nested())
      .merge(Env::prefixed("FROGDB_").split("__"))
      .merge(Serialized::globals(cli_overrides))
  ```
- [x] CLI arguments via clap:
  - [x] `--config <FILE>` - Path to TOML config file
  - [x] `--bind <ADDR>` - Override bind address
  - [x] `--port <PORT>` - Override listen port
  - [x] `--shards <N>` - Override number of shards
  - [x] `--log-level <LEVEL>` - Override log level
  - [x] `--log-format <FORMAT>` - Override log format (pretty/json)
- [x] Default `frogdb.toml` generation on first run (optional, with `--generate-config`)
- [x] Config validation at startup (fail fast on invalid values)

### 1.7 Server & Acceptor

- [x] Acceptor task:
  - [x] TCP listener
  - [x] maxclients check (placeholder)
  - [x] Round-robin shard assignment
  - [x] Send NewConnection to shard
- [x] Server startup:
  - [x] Load configuration via Figment (TOML + env + CLI)
  - [x] Initialize logging with configured format and level
  - [x] Spawn shard workers (configurable, default: num_cpus)
  - [x] Start acceptor
  - [x] Log ready message with bound address
- [x] Graceful shutdown (SIGTERM/SIGINT)

### 1.8 Routing

- [x] `route_and_execute` function:
  - [x] Extract keys via `command.keys(args)`
  - [x] Keyless commands: execute locally
  - [x] Single-key: route to owner shard
  - [x] Multi-key: placeholder (error or single-shard only)
- [x] `execute_local` function
- [x] `execute_remote` function (send via channel, await oneshot response)

### 1.9 Initial Commands

- [x] `PING` (keyless, returns PONG or echoes argument)
- [x] `ECHO` (keyless)
- [x] `QUIT` (close connection)
- [x] `COMMAND` / `COMMAND DOCS` (placeholder - return empty)
- [x] `SET` (key value, no options yet)
- [x] `GET`
- [x] `DEL` (single key)
- [x] `EXISTS` (single key)

### 1.10 Noop Abstractions (Critical for Future)

These must exist as traits/stubs to avoid refactoring:

**Persistence & Replication:**
- [x] `WalWriter` trait with noop implementation
- [x] `ReplicationConfig` enum (Standalone/Primary/Replica)
- [x] `ReplicationTracker` trait with noop implementation (for sync replication ACKs)
- [x] Sequence number assignment hook in command flow (returns 0 for noop)

**Security:**
- [x] `AclChecker` trait with `AlwaysAllow` implementation

**Expiry:**
- [x] `ExpiryIndex` struct (empty, no-op methods)

**Observability (OpenTelemetry-ready):**
- [x] `MetricsRecorder` trait with noop implementation:
  - `increment_counter(name, labels)`
  - `record_histogram(name, value, labels)`
  - `set_gauge(name, value, labels)`
- [x] `Tracer` trait with noop implementation:
  - `start_span(name) -> Span`
  - `Span::set_attribute(key, value)`
  - `Span::end()`
- [x] Structured logging setup with `tracing` crate (this IS implemented, not noop):
  - [x] Configurable format via `logging.format` config:
    - `pretty` - Human-readable, colored output for development
    - `json` - Machine-parseable JSON lines for production
  - [x] Configurable level via `logging.level` config (trace/debug/info/warn/error)
  - [x] Log subscriber initialization based on config

### 1.11 Testing

**Integration Tests** (tests/ directory):

- [x] `TestServer` helper:
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
- [x] Integration test: connect, SET foo bar, GET foo, assert "bar"
- [x] Test: PING returns PONG
- [x] Test: unknown command returns error
- [x] Test: GET nonexistent returns nil
- [x] Test: wrong arity returns error

**Unit Tests** (inline #[cfg(test)] modules):

- [x] Store trait implementation tests:
  - [x] HashMapStore get/set/delete operations
  - [x] Memory accounting accuracy
  - [x] Key existence checks
- [x] Command trait tests:
  - [x] Arity validation (Fixed, AtLeast, Range)
  - [x] CommandFlags bitflag operations
- [x] Routing tests:
  - [x] `shard_for_key()` hash distribution
  - [x] Hash tag extraction `{...}` handling
- [x] Configuration tests:
  - [x] Config default values
  - [x] Figment layering priority (CLI > env > TOML > defaults)
  - [x] Invalid config rejection
- [x] Protocol tests:
  - [x] ParsedCommand construction
  - [x] Response encoding for each variant

### 1.12 Documentation

- [x] Update INDEX.md roadmap section to link here
- [x] Add inline documentation to public types
- [x] README with build/run instructions

---

## Phase 2: Complete String Commands & TTL

**Goal**: Full string command set including numeric operations and key expiration.

**Depends on**: Phase 1

### 2.1 Integer Encoding

- [x] Implement `StringValue::as_integer()` -> Option<i64>
- [x] Auto-detect integer on SET
- [x] Maintain encoding through operations

### 2.2 String Commands

- [x] `SET` options: EX, PX, EXAT, PXAT, NX, XX, KEEPTTL, GET, IFEQ, IFGT
- [x] `SETNX`
- [x] `SETEX`, `PSETEX`
- [x] `APPEND`
- [x] `STRLEN`
- [x] `INCR`, `DECR`
- [x] `INCRBY`, `DECRBY`
- [x] `INCRBYFLOAT`
- [x] `GETRANGE` (substring)
- [x] `SETRANGE`
- [x] `GETDEL`
- [x] `GETEX`

### 2.3 Key Expiration

- [x] Implement `ExpiryIndex`:
  ```rust
  pub struct ExpiryIndex {
      by_time: BTreeMap<(Instant, Bytes), ()>,
      by_key: HashMap<Bytes, Instant>,
  }
  ```
- [x] Lazy expiry: check TTL on every read, delete if expired
- [x] `EXPIRE`, `PEXPIRE`
- [x] `EXPIREAT`, `PEXPIREAT`
- [x] `TTL`, `PTTL`
- [x] `PERSIST`
- [x] `EXPIRETIME`, `PEXPIRETIME`

### 2.4 Active Expiry

- [x] Background task per shard (~10Hz)
- [x] Sample keys from ExpiryIndex
- [x] Time-budgeted deletion (25ms default)
- [ ] Metrics: expired_keys counter

### 2.5 Generic Commands

- [x] `TYPE`
- [x] `RENAME` (same-shard only, CROSSSLOT error otherwise)
- [x] `RENAMENX`
- [x] `TOUCH`
- [x] `UNLINK` (async delete)
- [x] `OBJECT ENCODING`
- [x] `OBJECT FREQ`
- [x] `OBJECT IDLETIME`
- [x] `DEBUG OBJECT` (basic)

### 2.6 Testing

- [x] Test all SET options
- [x] Test INCR overflow (return error, not wrap)
- [x] Test INCR on non-integer string (error)
- [x] Test TTL expiration (lazy)
- [x] Test RENAME same-shard requirement
- [ ] Property tests: INCR/DECR roundtrip

---

## Phase 3: Sorted Sets ✓

**Goal**: Complete sorted set implementation (moved earlier since it's a core data type).

**Depends on**: Phase 2

### 3.1 Data Structure

- [x] `SortedSetValue` in `crate::types`:
  ```rust
  pub struct SortedSetValue {
      members: HashMap<Bytes, f64>,
      scores: BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
  }
  ```
- [x] Add to `Value` enum
- [x] `ScoreBound` enum (Inclusive, Exclusive, NegInf, PosInf)
- [x] `LexBound` enum (Inclusive, Exclusive, Min, Max)

### 3.2 Basic Commands

- [x] `ZADD` (NX, XX, GT, LT, CH, INCR options)
- [x] `ZREM`
- [x] `ZSCORE`
- [x] `ZMSCORE`
- [x] `ZCARD`
- [x] `ZINCRBY`

### 3.3 Ranking

- [x] `ZRANK`
- [x] `ZREVRANK`

### 3.4 Range Queries

- [x] `ZRANGE` (unified: BYSCORE, BYLEX, REV, LIMIT, WITHSCORES)
- [x] `ZRANGEBYSCORE` / `ZREVRANGEBYSCORE` (legacy)
- [x] `ZRANGEBYLEX` / `ZREVRANGEBYLEX` (legacy)
- [x] `ZCOUNT`
- [x] `ZLEXCOUNT`

### 3.5 Pop & Random

- [x] `ZPOPMIN`, `ZPOPMAX`
- [x] `ZMPOP`
- [x] `ZRANDMEMBER`

### 3.6 Set Operations (Same-Slot Required)

- [x] `ZUNION` / `ZUNIONSTORE`
- [x] `ZINTER` / `ZINTERSTORE` / `ZINTERCARD`
- [x] `ZDIFF` / `ZDIFFSTORE`

### 3.7 Other

- [x] `ZSCAN`
- [x] `ZRANGESTORE`
- [x] `ZREMRANGEBYRANK`
- [x] `ZREMRANGEBYSCORE`
- [x] `ZREMRANGEBYLEX`

### 3.8 Testing

- [x] Test ZADD options
- [x] Test range queries (score bounds, lex bounds)
- [x] Test set operations
- [x] Integration tests (33 tests passing)

---

## Phase 4: Multi-Shard Operations ✓

**Goal**: Enable multiple shards with proper CROSSSLOT validation and all-shard scatter-gather.

**Depends on**: Phase 3

### 4.1 Multi-Shard Initialization

- [x] `num_shards` config option (default: available_parallelism)
- [x] Spawn N shard workers
- [x] Verify channel topology

### 4.2 Hash Tags & Slot Validation

- [x] `extract_hash_tag(key)` function
- [x] Hash slot calculation: `crc16(tag) % 16384`
- [x] Internal shard routing: `hash_slot % num_shards`
- [x] Same hash tag = same slot = same internal shard
- [x] Test: `{user:1}:profile` and `{user:1}:session` go to same shard

### 4.3 CROSSSLOT Validation

- [x] `allow_cross_slot_standalone` config option (default: `false`)
- [x] When disabled (default): validate multi-key commands, return `-CROSSSLOT` error (matches Redis Cluster)
- [x] When enabled: allow cross-shard operations via VLL (atomic for all operations including MSET)
- [x] Commands requiring same-slot by default: MGET, MSET, MSETNX, DEL (multi), EXISTS (multi)
- [x] Note: MSETNX always requires same-slot (atomic semantics require it)
- [x] Note: `allow_cross_slot_standalone` is only available in standalone mode, not cluster mode

### 4.4 Multi-Key Commands

- [x] `MGET`:
  - Default: same-slot required
  - With `allow_cross_slot_standalone`: scatter-gather across shards (atomic read via VLL)
- [x] `MSET`:
  - Default: same-slot required (atomic)
  - With `allow_cross_slot_standalone`: atomic via VLL (all-or-nothing semantics)
- [x] `MSETNX` (always same-slot required - atomic semantics)
- [x] `DEL` multi-key:
  - Default: same-slot required
  - With config: atomic via VLL
- [x] `EXISTS` multi-key: same behavior as DEL
- [x] `TOUCH` multi-key: same behavior as DEL
- [x] `UNLINK` multi-key: same behavior as DEL

### 4.5 Scatter-Gather

Scatter-gather supports two modes:

**All-Shard Operations** (always scatter to all shards):
- SCAN, KEYS, DBSIZE, FLUSHDB, INFO

**Key-Targeted Operations** (scatter only to shards owning the keys):
- MGET, MSET, DEL (multi-key) - when `allow_cross_slot_standalone` is enabled

Implementation:

- [x] `ScatterRequest` message type
- [x] `execute_scatter_gather` function:
  - [x] All-shard mode: Send request to all N shards
  - [x] Key-targeted mode: Hash keys, send only to owning shards
  - [x] Await all responses
  - [x] Aggregate results (sum, merge, etc.)
- [x] Timeout handling (`scatter_gather_timeout_ms`)
- [x] VLL integration for key-targeted write operations (atomic semantics)

### 4.6 [VLL](VLL.md) Transaction Ordering (Foundation)

- [x] Global `AtomicU64` transaction ID counter
- [x] Include txid in Execute messages
- [x] Per-shard pending queue (for future use)

### 4.7 Testing

- [x] Test MGET with hash tags (same slot, works)
- [x] Test MGET without hash tags (different slots, CROSSSLOT error)
- [x] Test hash tag colocation
- [x] Stress test: 100 concurrent clients, random keys
- [x] Test CROSSSLOT error for various multi-key commands

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

- [x] Binary format:
  ```
  [type: u8][flags: u8][expires_at: i64][lfu: u8][len: u32][data...]
  ```
- [x] Serialize/deserialize StringValue, SortedSet
- [x] Serialize/deserialize Hash, List, Set (added in Phase 6)

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

## Phase 6: Hash, List, Set Types ✓

**Goal**: Remaining core data types.

**Depends on**: Phase 5 (Persistence)

### 6.1 Hash

- [x] `HashValue` in `crate::types` (HashMap<Bytes, Bytes>)
- [x] `HSET`, `HSETNX`, `HGET`, `HDEL`
- [x] `HMSET`, `HMGET`
- [x] `HGETALL`, `HKEYS`, `HVALS`
- [x] `HEXISTS`, `HLEN`
- [x] `HINCRBY`, `HINCRBYFLOAT`
- [x] `HSTRLEN`
- [x] `HSCAN`
- [x] `HRANDFIELD`

### 6.2 List

- [x] `ListValue` in `crate::types` (VecDeque<Bytes>)
- [x] `LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`
- [x] `LPOP`, `RPOP`, `LMPOP`
- [x] `LRANGE`, `LINDEX`, `LSET`
- [x] `LLEN`, `LPOS`
- [x] `LINSERT`
- [x] `LREM`, `LTRIM`
- [x] `LMOVE`, `LMPOP`

### 6.3 Set

- [x] `SetValue` in `crate::types` (HashSet<Bytes>)
- [x] `SADD`, `SREM`
- [x] `SMEMBERS`, `SISMEMBER`, `SMISMEMBER`
- [x] `SCARD`
- [x] `SUNION`, `SINTER`, `SDIFF` (same-shard)
- [x] `SUNIONSTORE`, `SINTERSTORE`, `SDIFFSTORE`, `SINTERCARD`
- [x] `SRANDMEMBER`, `SPOP`
- [x] `SMOVE`
- [x] `SSCAN`

### 6.4 Persistence

- [x] Serialization for Hash, List, Set
- [x] Recovery tests

### 6.5 Testing

- [x] Comprehensive command tests
- [x] WRONGTYPE error tests
- [x] Persistence tests

### 6.6 Deferred Optimizations

The following optimizations are deferred to a future phase:
- Ziplist/listpack encoding for small hashes (< 64 entries)
- Intset encoding for small integer-only sets
- Quicklist optimization for lists

---

## Phase 7: Transactions & Pub/Sub

**Goal**: MULTI/EXEC, WATCH, and publish/subscribe.

**Depends on**: Phase 6

### 7.1 Transactions

- [x] `MULTI` - start transaction
- [x] Queue commands (return QUEUED)
- [x] `EXEC` - execute atomically (same-shard requirement)
- [x] `DISCARD` - abort transaction
- [x] `WATCH` - optimistic locking
- [x] `UNWATCH`
- [x] Key versioning for WATCH detection (per-shard)

### 7.2 Pub/Sub - Broadcast

- [x] `SUBSCRIBE`, `UNSUBSCRIBE`
- [x] `PSUBSCRIBE`, `PUNSUBSCRIBE`
- [x] `PUBLISH` (fan-out to all shards)
- [x] Pub/sub mode restrictions

### 7.3 Pub/Sub - Sharded

- [x] `SSUBSCRIBE`, `SUNSUBSCRIBE`
- [x] `SPUBLISH` (route to channel's shard)

### 7.4 Pub/Sub - Introspection

- [x] `PUBSUB CHANNELS`
- [x] `PUBSUB NUMSUB`
- [x] `PUBSUB NUMPAT`
- [x] `PUBSUB SHARDCHANNELS`
- [x] `PUBSUB SHARDNUMSUB`

### 7.5 Testing

- [x] Test MULTI/EXEC flow
- [x] Test WATCH conflict detection
- [x] Test broadcast pub/sub
- [x] Test sharded pub/sub
- [x] Test pattern subscriptions
- [x] Test namespace isolation (PUBLISH/SPUBLISH separation)
- [x] Test pub/sub mode restrictions

---

## Phase 8: Lua Scripting ✓

**Goal**: Lua script execution with strict key validation.

**Depends on**: Phase 7

### 8.1 Lua VM

- [x] Add `mlua` dependency (v0.10 with vendored Lua 5.4)
- [x] Per-shard Lua VM instances
- [x] Resource limits (execution time, memory)
- [x] Sandboxing (remove dangerous functions: loadfile, dofile, io, os, debug, etc.)
- [x] Timeout handling via Lua hooks (every 10k instructions)

### 8.2 Commands

- [x] `EVAL`
- [x] `EVALSHA`
- [x] `SCRIPT LOAD`
- [x] `SCRIPT EXISTS`
- [x] `SCRIPT FLUSH`
- [x] `SCRIPT KILL`

### 8.3 Bindings

- [x] `redis.call()` - execute command, propagate errors
- [x] `redis.pcall()` - execute command, return errors
- [x] Strict key validation (keys must be in KEYS array)
- [x] Script routing to owner shard (SingleShardRouter with CROSSSLOT error)
- [x] Forbidden command checking (MULTI, EXEC, WATCH, blocking commands, nested scripts)
- [x] KEYS and ARGV global tables

### 8.4 Caching

- [x] Script cache (SHA1 -> source)
- [x] LRU eviction (by count and bytes)

### 8.5 Testing

- [x] Test basic script execution
- [x] Test strict key validation
- [x] Test script caching
- [x] Test timeout
- [x] Unit tests (36 tests passing)

---

## Phase 9: Key Iteration & Server Commands ✓

**Goal**: SCAN, INFO, and server administration.

**Depends on**: Phase 8

### 9.1 SCAN

- [x] Cursor format: `shard_id (16 bits) | position (48 bits)`
- [x] `SCAN` (scatter to all shards, merge cursors)
- [x] MATCH pattern filtering (full glob support)
- [x] COUNT hint
- [x] TYPE filter
- [x] `SSCAN`, `HSCAN`, `ZSCAN` (already done with data types)

### 9.2 KEYS

- [x] `KEYS pattern` (scatter-gather, warn about production use)

### 9.3 INFO

- [x] `INFO [section]`
- [x] Sections: server, clients, memory, persistence, stats, replication, cpu, keyspace

### 9.4 Server Commands

- [x] `DBSIZE`
- [x] `FLUSHDB`, `FLUSHALL`
- [x] `TIME`
- [x] `DEBUG SLEEP`
- [x] `SHUTDOWN`
- [x] `COMMAND`, `COMMAND COUNT`, `COMMAND DOCS`, `COMMAND INFO`, `COMMAND HELP`

### 9.5 Testing

- [x] Test SCAN completeness (all keys returned exactly once)
- [x] Test cursor handling across shards
- [x] Test INFO sections
- [x] All 191 tests passing (169 unit + 6 protocol + 16 server)

---

## Phase 10: Production Readiness

**Goal**: Metrics, configuration, ACL, and operational tooling.

**Depends on**: Phase 9

### 10.1 Metrics

- [x] Prometheus endpoint (`:9090/metrics` or configurable)
- [x] Health check endpoints (`/health/live`, `/health/ready`)
- [x] Connection metrics (total, current)
- [x] System metrics (uptime, memory RSS)
- [x] Metrics configuration (bind, port, OTLP optional)
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
