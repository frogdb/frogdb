---
title: "Persistence Internals"
description: "Contributor-facing documentation for FrogDB's persistence architecture: RocksDB topology, WAL design, key-value schema, and snapshot internals."
sidebar:
  order: 6
---
Contributor-facing documentation for FrogDB's persistence architecture: RocksDB topology, WAL design, key-value schema, and snapshot internals.

For operator-facing configuration and recovery procedures, see [Operations: Persistence](/operations/persistence/).

---

## RocksDB Topology

FrogDB uses a **single shared RocksDB instance** with one column family per shard (`shard_<n>`), plus a shared write-ahead log:

```
+----------------------------------------------------------+
|                    RocksDB Instance                       |
+----------------------------------------------------------+
|  +-----------+ +-----------+ +-----------+ +---------+   |
|  |  CF: s0   | |  CF: s1   | |  CF: s2   | | CF: sN  |  |
|  | (Shard 0) | | (Shard 1) | | (Shard 2) | |(Shard N)|  |
|  +-----------+ +-----------+ +-----------+ +---------+   |
|                                                           |
|  +---------------------------------------------------+   |
|  |              Shared WAL                            |   |
|  +---------------------------------------------------+   |
+----------------------------------------------------------+
```

### Design Rationale

**Benefits:**
- Single backup/restore operation for the entire database.
- A shared WAL simplifies recovery.
- Cross-shard writes can be committed atomically via a RocksDB `WriteBatch` spanning column families.

**Trade-off:**
- Potential contention on the shared WAL under write-heavy load (mitigated by batching).

Optional warm (`warm_<n>`) and search-metadata (`search_meta_<n>`) column-family tiers exist alongside the main tier when their features are enabled.

---

## Key-Value Schema

Each column family (shard) stores the user key directly and a serialized value frame (`frogdb-server/crates/persistence/src/serialization/`).

**Key Format:** the raw user key bytes.

**Value Format:** a fixed 24-byte header followed by the type-specific payload:

```
+---------------------------------------------------------+
| Header (fixed 24 bytes)                                  |
+---------------------------------------------------------+
| type: u8           | Type marker (see table below)       |
| flags: u8          | Reserved (currently 0)              |
| expires_at: i64    | Unix timestamp ms (-1 = no expiry)   |
| lfu_counter: u8    | LFU access counter                   |
| padding: [u8; 5]   | Alignment padding                    |
| payload_len: u64   | Length of the payload                |
+---------------------------------------------------------+
| Payload (variable)                                       |
+---------------------------------------------------------+
```

**Type Markers:** the one-byte type marker is the stable on-disk **and** replication wire byte and must not change (`serialization/marker.rs`, pinned by `marker_bytes_are_stable`):

| Marker | Byte | Notes |
|--------|------|-------|
| `StringRaw` | 0 | Raw bytes |
| `StringInt` | 1 | Integer-encoded string (i64) |
| `SortedSet` | 2 | Also backs Geospatial (geohash scores) |
| `Hash` | 3 | Field-value map |
| `List` | 4 | |
| `Set` | 5 | |
| `Stream` | 6 | Entries + consumer-group state |
| `Bloom` | 7 | |
| `HyperLogLog` | 8 | Sparse/dense encoding |
| `TimeSeries` | 9 | |
| `Json` | 10 | |
| `HashWithFieldExpiry` | 11 | Hash carrying per-field TTLs |
| `Cuckoo` | 12 | |
| `TopK` | 13 | |
| `TDigest` | 14 | |
| `Cms` | 15 | Count-Min Sketch |
| `VectorSet` | 16 | |

There is no distinct Bitmap or Geo marker: a Bitmap is stored as a `StringRaw`, and a Geospatial index is a `SortedSet`. These 17 markers cover the 15 [`Value` enum variants](/architecture/storage/#value-types) (String and Hash each have two markers).

**Byte Order:** all multi-byte integers are stored in **little-endian** format. Decoding goes through a bounds-checked cursor (`FrameReader`) that never reads past the buffer, however the length prefixes are corrupted.

### Metadata Persistence

- `lfu_counter` is persisted in the header.
- `last_access` (LRU) is **NOT persisted** -- keys read as fresh (idle time = 0) after recovery. This matches Redis behavior; eviction accuracy self-corrects as keys are accessed.
- **Expiry index:** NOT persisted separately. It is rebuilt during recovery from each value's `expires_at` field.
- **Recovery conversion:** persisted Unix millisecond timestamps are converted back to monotonic `std::time::Instant` values during recovery.

The [storage layer](/architecture/storage/#key-metadata) is the single source for the LRU/LFU metadata story.

---

## Write-Ahead Log (WAL)

Every write is appended to the WAL before acknowledgment:

```
Client Write (SET key value)
         |
         v
    Shard Worker
         |
         +-- 1. Apply to in-memory store
         |
         +-- 2. Append to WAL (RocksDB WriteBatch)
         |
         +-- 3. Return OK to client
```

### WAL Failure Handling

| Failure Point | In-Memory State | Client Response | Recovery |
|---------------|-----------------|-----------------|----------|
| Before in-memory apply | Unchanged | Error returned | None needed |
| After in-memory, WAL fails (`continue`) | **Write visible** | OK returned | May be lost on restart |
| After WAL, before fsync (Async) | Write visible | OK returned | May be lost on crash |
| After fsync (Sync) | Write visible | OK returned | Guaranteed durable |

**Design rationale:** in-memory is the source of truth during operation. The WAL provides durability, not correctness during normal operation — matching Redis AOF semantics. The alternative (rollback on every WAL failure) would require undo logs on the hot path.

### WAL Failure Policy

The `wal-failure-policy` (`WalFailurePolicy`) selects what happens when a WAL append fails:

| Policy | Behavior | Default |
|--------|----------|---------|
| `continue` | Log the error, return success (Redis/DragonflyDB semantics) | Yes |
| `rollback` | Undo the in-memory change, return `IOERR` to the client | No |

**Rollback mode details:**
- Before executing a write, the affected keys' current state is snapshotted (cheap `Arc<Value>` clones).
- If the WAL append fails, the snapshot is restored and `IOERR` is returned.
- Rollback applies to single-shard write commands only; scatter-gather paths always use `continue`.
- Rollback forces a synchronous flush per command, so it is materially slower than `continue` — it trades throughput for the guarantee that an acknowledged write is on disk.

### WAL Corruption Recovery

FrogDB **explicitly pins** the RocksDB WAL recovery mode to `PointInTime` (`rocksdb::DBRecoveryMode::PointInTime`, set in `frogdb-server/crates/persistence/src/rocks/mod.rs`). It is not left at RocksDB's implicit default — pinning it makes the recovery contract a deliberate, tested decision that a future RocksDB version cannot silently change.

| Corruption Type | Detection | Recovery under `PointInTime` |
|-----------------|-----------|------------------------------|
| **Truncated entry** (torn tail) | Entry length exceeds remaining file bytes | Recover the valid prefix; drop the torn tail |
| **Checksum mismatch** (mid-log) | Entry checksum does not match | Recover the valid prefix *up to the first bad record*, then stop — every record after the corruption is dropped, even if individually valid |
| **Invalid type marker** | Unknown operation type byte | Same as checksum mismatch: stop at the first bad record |

**Why `PointInTime`:** it recovers the longest uninterrupted prefix of valid records and stops at the first checksum failure — recovered state is therefore always a real *prefix* of history and never interleaves post-corruption records. This mirrors Redis point-in-time AOF loading (`aof-load-truncated`). The alternatives are worse for a database: `AbsoluteConsistency` would refuse to open at all on an ordinary torn tail, turning every unclean shutdown into a hard startup failure; `SkipAnyCorruptedRecord` keeps replaying *past* a corrupt record, which can resurrect stale values and reorder history.

**The sharp edge — and the signal for it:** on a mid-log corruption, `PointInTime` silently drops the (possibly still-valid) suffix behind the first bad record. To keep that from being invisible, FrogDB persists a durable-sync sequence watermark next to the database (the `rocks::wal_watermark` module). Each fsync'd WAL batch advances the watermark; on the next open, recovery compares the sequence it actually reached against the watermark. Landing **below** it means committed records were dropped, so FrogDB emits the `frogdb_wal_recovery_dropped_records_total` counter and a WARN log carrying the exact number of lost sequence numbers. The watermark is written best-effort (it can only lag the true durable sequence after a crash, never lead it), so this signal never false-alarms on a clean recovery — it can only under-report. The most recent snapshot provides a fallback for the dropped suffix.

---

## Durability Modes

The `DurabilityMode` (`frogdb-server/crates/persistence/src/wal/config.rs`) controls when data is flushed to disk. The default is `Periodic { interval_ms: 1000 }`.

| Mode | Durability | Trade-off |
|------|------------|-----------|
| `Async` | Best-effort; unflushed writes may be lost on crash | Acknowledges immediately after the in-memory append — the fastest, least durable mode |
| `Periodic { interval_ms }` | Bounded loss (up to one interval) | Flushes on a fixed timer (default 1000 ms); matches Redis `appendfsync everysec` |
| `Sync` | Every acknowledged write is durable | Waits for an fsync before acknowledging — the most durable, slowest mode |

FrogDB publishes no latency numbers for these modes yet; the ordering above (Async fastest, Sync slowest) is the qualitative trade-off, not a measured figure.

### Periodic Mode Timer Semantics

`Periodic` uses a **wall-clock timer** that fires on a fixed schedule (not reset-on-write). If a previous fsync is still in progress when the timer fires, that interval is skipped. This matches Redis `appendfsync everysec`.

### Write Visibility

In `Async` and `Periodic` modes, writes are visible to other clients **before** they are durably persisted. This is by design and matches Redis.

---

## Forkless Snapshot Algorithm

FrogDB uses a forkless snapshot instead of Redis's fork-based approach, built on RocksDB's own
checkpoint machinery rather than any in-memory copy-on-write buffer:

1. The `SnapshotScheduler` claims the next **snapshot epoch** (a monotonic counter that only
   numbers and names the run, e.g. `snapshot_00007`).
2. The coordinator's `pre_snapshot_hook` runs: it flushes each shard's search indexes and
   persists the current replication offset, so both land in the same snapshot.
3. `RocksStore::create_checkpoint` cuts a RocksDB checkpoint at the store's current
   `latest_sequence_number()`. RocksDB hard-links the checkpoint's SST files and copies only the
   still-mutable memtable/WAL data, giving a consistent point-in-time view without forking the
   process or freezing the keyspace.
4. The checkpoint and its `metadata.json` (recording the epoch and sequence number) are staged
   under a temp directory and atomically renamed into place.

Concurrent writes during the checkpoint are never diverted or buffered: they land in the
in-memory store and the WAL exactly as they would outside a snapshot. Nothing related to
snapshotting is counted toward `maxmemory` — eviction during a snapshot behaves the same as at
any other time.

---

## Sequence Number Assignment

Sequence numbers are assigned at WAL append time, not at command execution time. This gives:
- Monotonically increasing sequences for replication ordering.
- Gaps are possible if a batched write fails partially.
- Replicas can resume from a sequence number (see [Replication Internals](/architecture/replication/)).

The `WalWriter` trait (`frogdb-server/crates/types/src/traits/wal.rs`) is intentionally small:

```rust
pub trait WalWriter: Send + Sync {
    /// Append an operation; returns the sequence number assigned to it.
    fn append(&mut self, operation: &WalOperation) -> u64;
    /// Flush pending writes to disk.
    fn flush(&mut self) -> std::io::Result<()>;
    /// The current (latest assigned) sequence number.
    fn current_sequence(&self) -> u64;
}
```

`WalOperation` is the closed set of logged mutations: `Set`, `SetWithExpiry`, `Delete`, and `Expire`. The sequence returned by `append` is what replication uses to order and resume the stream.
