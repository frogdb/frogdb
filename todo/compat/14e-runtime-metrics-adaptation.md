# 14e. Runtime Metrics Adaptation (10 tests — `introspection_tcl.rs`, `info_tcl.rs`, `other_tcl.rs`)

**Status**: Not implemented
**Scope**: Adapt Redis's internal runtime metrics (IO threads, eventloop, memory/dict
introspection) to FrogDB's tokio-based architecture and RocksDB storage engine.

## Architecture

These tests probe Redis internals that have no 1:1 equivalent in FrogDB but map to analogous
concepts. The approach is to implement adapted metrics that expose equivalent operational
insight.

### IO Thread Distribution (2 tests)

Redis 7+ has IO threads for read/write offloading. The tests verify:
- `IO threads client number` — clients are assigned to IO threads
- `Clients are evenly distributed among io threads` — fair distribution

**FrogDB equivalent**: Tokio task distribution across the runtime's worker threads. FrogDB can
expose per-worker-thread client counts via `tokio::runtime::RuntimeMetrics` (available with
`tokio_unstable`).

### Eventloop/Memory Metrics (5 tests)

- `stats: eventloop metrics` — cycle time, poll duration
- `stats: instantaneous metrics` — ops/sec, bandwidth sampling
- `stats: client input and output buffer limit disconnections` — disconnect tracking
- `memory: database and pubsub overhead and rehashing dict count` — memory breakdown
- `memory: used_memory_peak_time is updated when used_memory_peak_time is updated` — peak tracking

**FrogDB equivalent**:
- Eventloop metrics → tokio task poll durations, shard tick timings
- Instantaneous metrics → existing ops/sec counters (may already be partially implemented)
- Buffer limit disconnections → track in client eviction/disconnect paths
- Memory metrics → RocksDB block cache + memtable + connection memory
- Peak tracking → track high-water mark with timestamps

### Dict/Storage Introspection (3 tests)

- `Don't rehash if redis has child process` — Redis pauses rehashing during BGSAVE
- `Redis can trigger resizing` — dict grows when load factor exceeds threshold
- `Redis can rewind and trigger smaller slot resizing` — dict shrinks on delete

**FrogDB equivalent**: These map to RocksDB compaction behavior:
- Compaction pausing under memory pressure (analogous to rehash pause)
- Compaction triggering on write amplification thresholds
- Space reclamation after bulk deletes

## Implementation Steps

1. **IO thread metrics**:
   - Enable `tokio_unstable` for metrics access (if not already)
   - Expose worker thread count and per-worker task counts in INFO
   - Add connection-to-shard distribution metrics

2. **Eventloop metrics**:
   - Track shard tick duration (min/avg/max per reporting period)
   - Implement instantaneous ops/sec sampling (rolling window)
   - Track client disconnections by reason (buffer overflow, timeout, eviction)

3. **Memory metrics**:
   - Expose RocksDB memory usage (block cache, memtables, table readers)
   - Track pub/sub subscription memory overhead
   - Implement `used_memory_peak` with timestamp tracking

4. **Storage introspection**:
   - Expose RocksDB compaction state (running/pending)
   - Track LSM level sizes and compaction triggers
   - Implement equivalent "resize" semantics for test adaptation

## Integration Points

- `frogdb-server/crates/server/src/commands/info.rs` — INFO section generation
- `frogdb-server/crates/server/src/debug_providers.rs` — runtime debug metrics
- `frogdb-server/crates/core/src/` — shard tick timing infrastructure
- RocksDB statistics integration (via `frogdb-persistence` crate)
- Tokio runtime metrics API

## FrogDB Adaptations

These tests are **heavily adapted** — they test equivalent operational insight rather than
identical behavior:

| Redis Concept | FrogDB Equivalent |
|---------------|-------------------|
| IO threads | Tokio worker threads / shard distribution |
| Eventloop cycle | Shard tick duration |
| Dict rehashing | RocksDB compaction |
| Dict resize trigger | LSM level threshold |
| Dict slot count | SST file count / level sizes |
| used_memory (allocator) | RocksDB memory + connection memory |

## Tests

### IO Thread Distribution
- `IO threads client number`
- `Clients are evenly distributed among io threads`

### Eventloop / Memory Metrics
- `stats: eventloop metrics`
- `stats: instantaneous metrics`
- `stats: client input and output buffer limit disconnections`
- `memory: database and pubsub overhead and rehashing dict count`
- `memory: used_memory_peak_time is updated when used_memory_peak is updated`

### Storage Introspection
- `Don't rehash if redis has child process`
- `Redis can trigger resizing`
- `Redis can rewind and trigger smaller slot resizing`

## Verification

```bash
just test frogdb-server-redis-regression "IO threads"
just test frogdb-server-redis-regression "eventloop metrics"
just test frogdb-server-redis-regression "instantaneous metrics"
just test frogdb-server-redis-regression "rehash"
just test frogdb-server-redis-regression "trigger resizing"
```
