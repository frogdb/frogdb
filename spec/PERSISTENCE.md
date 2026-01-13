# FrogDB Persistence

This document details FrogDB's persistence architecture including RocksDB integration, write-ahead logging, durability modes, forkless snapshots, and backup/restore operations.

## RocksDB Topology

FrogDB uses a **single shared RocksDB instance** with one column family per shard:

```
┌─────────────────────────────────────────────────────────┐
│                    RocksDB Instance                      │
├─────────────────────────────────────────────────────────┤
│  ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌─────────┐ │
│  │  CF: s0   │ │  CF: s1   │ │  CF: s2   │ │ CF: sN  │ │
│  │ (Shard 0) │ │ (Shard 1) │ │ (Shard 2) │ │(Shard N)│ │
│  └───────────┘ └───────────┘ └───────────┘ └─────────┘ │
│                                                         │
│  ┌─────────────────────────────────────────────────┐   │
│  │              Shared WAL                          │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

### Benefits

- Single backup/restore operation for entire database
- Shared WAL simplifies recovery
- Atomic cross-shard operations possible via WriteBatch

### Trade-off

- Potential lock contention on WAL writes (mitigated by batching)

### Key-Value Schema

Each column family (shard) stores keys with this format:

**Key Format:**
```
[user_key_bytes]
```

**Value Format:**
```
┌─────────────────────────────────────────────────────────────┐
│ Header (fixed 24 bytes)                                      │
├─────────────────────────────────────────────────────────────┤
│ type: u8           │ Value type (0=String, 1=List, etc.)    │
│ flags: u8          │ Reserved for future use                 │
│ expires_at: i64    │ Unix timestamp ms (0 = no expiry)       │
│ lfu_counter: u8    │ LFU access counter                      │
│ padding: [u8; 5]   │ Alignment padding                       │
│ value_len: u64     │ Length of value data                    │
├─────────────────────────────────────────────────────────────┤
│ Value Data (variable)                                        │
│ - String: raw bytes                                          │
│ - List: length-prefixed elements                             │
│ - Set: length-prefixed members                               │
│ - Hash: length-prefixed key-value pairs                      │
│ - SortedSet: length-prefixed (score, member) pairs           │
└─────────────────────────────────────────────────────────────┘
```

**Type Encoding:**

| Type | Code | Serialization |
|------|------|---------------|
| String | 0 | Raw bytes |
| List | 1 | `[len:u32][elem1_len:u32][elem1]...` |
| Set | 2 | `[len:u32][member1_len:u32][member1]...` |
| Hash | 3 | `[len:u32][k1_len:u32][k1][v1_len:u32][v1]...` |
| SortedSet | 4 | `[len:u32][score:f64][member_len:u32][member]...` |

**Expiry Index:**
- NOT persisted separately
- Rebuilt during recovery from `expires_at` field in each value
- Active expiry index is in-memory only

**Recovery Conversion:** Unix timestamps (persisted as `i64` milliseconds) are converted to
`std::time::Instant` (monotonic clock) during recovery. See [STORAGE.md](STORAGE.md#key-metadata)
for the in-memory `KeyMetadata` structure and time handling details.

**LRU/LFU Metadata (Matches Redis Behavior):**
- `lfu_counter` persisted with value
- `last_access` (LRU) **NOT persisted** - reset to recovery time on startup

**Recovery Impact on Eviction:**
After recovery, all keys appear "fresh" for LRU purposes (idle time = 0). This matches Redis behavior:
- Redis intentionally does not persist LRU timestamps ([GitHub Issue #1261](https://github.com/redis/redis/issues/1261))
- Salvatore rejected persisting LRU due to limited benefit vs. implementation cost
- Eviction accuracy **self-corrects within minutes** as keys are accessed during normal operation
- The `RESTORE` command supports `IDLETIME` and `FREQ` modifiers for explicit migration tools

---

## Write-Ahead Log (WAL)

Every write operation is appended to RocksDB's WAL before acknowledgment:

```
Client Write (SET key value)
         │
         ▼
    Shard Worker
         │
         ├── 1. Apply to in-memory store
         │
         ├── 2. Append to WAL (async batch)
         │      └── RocksDB WriteBatch
         │
         └── 3. Return OK to client
```

### WAL Failure Handling

WAL writes can fail due to disk full, I/O errors, or RocksDB internal errors. FrogDB handles failures based on when they occur:

| Failure Point | In-Memory State | Client Response | Recovery |
|---------------|-----------------|-----------------|----------|
| Before in-memory apply | Unchanged | Error returned | None needed |
| After in-memory, WAL fails | **Write visible** | Error returned | May be lost on restart |
| After WAL, before fsync (Async) | Write visible | OK returned | May be lost on crash |
| After fsync (Sync) | Write visible | OK returned | Guaranteed durable |

> **Design Note: Why "Write Visible + Error Returned"?**
>
> This behavior matches Redis AOF semantics and is standard for in-memory databases:
> - **In-memory is the source of truth** during operation (for low latency)
> - **WAL provides durability**, not correctness during normal operation
> - **Error signals durability risk**, allowing the client to take action (retry, alert, etc.)
>
> The alternative (rollback on WAL failure) would require:
> - Maintaining undo logs for every write
> - Complex rollback logic for transactions
> - Significant performance overhead
>
> **Client Recommendations:**
> - For critical data: Use `sync_mode=sync` to guarantee durability before acknowledgment
> - Handle WAL errors by logging and alerting, potentially retrying the operation
> - Consider the write "at risk" until the next successful write confirms WAL health
>
> **Future Enhancement:** A configurable `wal_failure_policy: rollback` mode may be added
> to provide stricter consistency at the cost of performance.

**Critical Behavior:** In `Async` and `Periodic` modes, the in-memory write is applied **before** WAL durability is guaranteed. This matches Redis AOF behavior where:
- Writes are immediately visible to other clients
- Durability depends on fsync timing
- No rollback mechanism exists

**WAL Write Failure Response:**

```rust
match wal.append(&write_batch) {
    Ok(_) => Response::Ok,
    Err(e) => {
        // In-memory write already applied - cannot rollback
        // Log error, increment metric
        metrics.wal_errors.inc();
        error!("WAL append failed: {}", e);

        // Return error to client (write is visible but may not survive restart)
        Response::Error(format!("-ERR WAL write failed: {}", e).into())
    }
}
```

**Degraded State Handling:**

When WAL errors persist:
1. Log `WARN` on first failure, `ERROR` on repeated failures
2. Increment `frogdb_wal_errors_total` metric
3. Continue accepting writes (in-memory operations succeed)
4. **No automatic write rejection** - operational decision to stop traffic

**Recommendation:** Monitor `frogdb_wal_errors_total` and `frogdb_disk_usage_bytes`. Alert operators before disk fills. For critical data, use `Sync` durability mode.

---

## Durability Modes

```rust
pub enum DurabilityMode {
    /// Fastest: WAL write, no fsync (data loss on crash possible)
    Async,

    /// Balanced: fsync every N ms or M writes
    Periodic { interval_ms: u64, write_count: usize },

    /// Safest: fsync every write (slowest)
    Sync,
}
```

### Mode Comparison

| Mode | Durability | Latency |
|------|------------|---------|
| `Async` | Best-effort (may lose data) | ~1-10 μs |
| `Periodic(1000ms, 1000)` | Bounded loss (~1s or 1000 writes, matches Redis) | ~1-10 μs |
| `Sync` | Guaranteed (fsync per write) | ~100-500 μs |

### Periodic Mode Timer Semantics

The `Periodic` durability mode uses both a timer and a write counter. Understanding the exact behavior is important for durability guarantees.

**Timer Behavior:**
- Timer is **wall-clock based**, not write-event based
- Timer **does NOT reset** after fsync - it triggers on a fixed schedule
- Write counter **resets to zero** after each fsync

**Fsync Trigger Conditions:**
```rust
struct PeriodicFsync {
    interval_ms: u64,        // e.g., 100ms
    write_threshold: usize,  // e.g., 1000 writes
    last_fsync: Instant,
    writes_since_fsync: AtomicUsize,
}

impl PeriodicFsync {
    fn should_fsync(&self) -> bool {
        // Trigger fsync if EITHER condition is met:
        let time_trigger = self.last_fsync.elapsed().as_millis() >= self.interval_ms as u128;
        let write_trigger = self.writes_since_fsync.load(Ordering::Relaxed) >= self.write_threshold;

        time_trigger || write_trigger
    }

    fn on_fsync_complete(&mut self) {
        self.last_fsync = Instant::now();
        self.writes_since_fsync.store(0, Ordering::Relaxed);
    }
}
```

**Example Timeline (Periodic 1000ms, 1000 writes):**
```
T=0ms:     Server starts, timer begins
T=100ms:   500 writes (no fsync - neither threshold met)
T=500ms:   500 more writes → 1000 total → FSYNC triggered (write threshold)
T=500ms:   Counter reset to 0, timer continues
T=1000ms:  200 writes (no fsync yet)
T=1500ms:  Timer fires → FSYNC triggered (time threshold)
T=1500ms:  Counter reset to 0, timer reset
```

**What This Means for Durability:**
- In low-traffic scenarios: fsync every `interval_ms` (timer-driven)
- In high-traffic scenarios: fsync every `write_threshold` writes (write-driven)
- Worst-case data loss: min(interval_ms worth of writes, write_threshold writes)
- Default (1000ms, 1000) matches Redis `appendfsync everysec` behavior

### Write Visibility and Durability

**Important:** In `Async` and `Periodic` modes, writes are visible to other clients BEFORE they are durably persisted.

```
Client A: SET key value
  → In-memory: key = value (immediate)
  → WAL: Queued for batch write
  → Client A receives: +OK

Client B: GET key
  → Returns: value (from in-memory)

[Crash occurs before fsync]

After restart:
  → key may not exist (WAL entry was not fsynced)
```

**This matches Redis behavior.** The in-memory store is the source of truth during operation; WAL provides crash recovery, not read isolation.

**Implications:**
- A successful `GET` does NOT guarantee the value will survive a crash
- For guaranteed durability, use `Sync` mode or issue an explicit `PERSIST` (future command)
- Monitor `frogdb_wal_pending_bytes` to understand durability lag

---

## Forkless Snapshot Algorithm

Based on Dragonfly's approach - no fork(), no memory spike:

```
1. Coordinator signals all shards: "Start snapshot epoch N"

2. Each shard:
   a. Sets snapshot_epoch = N
   b. Continues processing commands normally
   c. In background, iterates owned keys:
      - For each key, serialize (key, value, expiry)
      - Send batch to snapshot writer
   d. For writes DURING snapshot:
      - If key not yet visited, serialize OLD value first (COW semantics)
      - Then apply new value

3. Snapshot writer:
   - Receives batches from all shards
   - Writes to RocksDB snapshot column family
   - On completion, updates metadata with epoch N

4. Recovery:
   - Load latest snapshot epoch
   - Replay WAL entries after snapshot LSN
```

### Advantages

- No 2x memory spike from fork() + copy-on-write
- Server continues processing during snapshot
- Consistent point-in-time capture across all shards

### Edge Cases During Snapshot

**Keys Modified During Snapshot:**
- If key not yet visited by iterator: Serialize OLD value first (COW), then apply new value
- If key already visited: Just apply new value (old value already in snapshot)

**Keys Deleted During Snapshot:**
- If not yet visited by iterator: **Skip** (key excluded from snapshot)
- If already visited: Already serialized, deletion is part of subsequent WAL entries

**Note on Point-in-Time Semantics:**
Unlike fork-based snapshots (Redis), our epoch-based approach does NOT capture a perfect point-in-time.
A key that exists at snapshot start may be excluded if deleted before the iterator visits it.
This is an acceptable trade-off vs. fork's 2x memory spike. DragonflyDB uses the same approach.

**Recovery Consistency Guarantees:**

> **Clarification: "Logical" vs "Physical" Point-in-Time**
>
> FrogDB's epoch-based snapshots provide **logical point-in-time** consistency:
> - All committed data at epoch start is captured
> - Keys deleted during snapshot are excluded (deletion goes to WAL)
> - Keys created during snapshot are excluded (creation goes to WAL)
>
> This differs from **physical point-in-time** (fork-based) where memory is frozen.
> The practical difference: a key deleted between epoch start and iterator visit
> will be absent from snapshot. WAL replay ensures correct final state.

| Scenario | Recovery State | Notes |
|----------|---------------|-------|
| Clean snapshot, clean shutdown | Logically consistent | All committed data at epoch start preserved |
| Snapshot + WAL replay | Fully consistent | WAL fills gaps from snapshot |
| Key deleted during snapshot | Key absent | Correct: delete captured in WAL |
| Key created during snapshot | Key present | Correct: create captured in WAL |
| Crash during snapshot | Previous snapshot | In-progress snapshot discarded |

**Cross-Shard Transaction Interaction:**

Multi-shard operations (MSET, MGET, DEL) interact with snapshots as follows:

| Timing | Behavior |
|--------|----------|
| **Transaction starts before snapshot epoch** | All changes included in snapshot (COW captures old values) |
| **Transaction starts during snapshot** | Changes go to WAL, not snapshot |
| **Transaction spans snapshot boundary** | Partial changes possible - WAL replay ensures consistency |

**Important:** Because snapshots are not perfect point-in-time, a multi-shard operation may appear "split" in the snapshot:
- Shard A: old value (not yet visited when write occurred)
- Shard B: new value (already visited, write went to COW)

WAL replay resolves this by re-applying the transaction, resulting in consistent final state. However, if the snapshot is loaded without WAL (e.g., WAL corrupted), inconsistent cross-shard state may be visible.

**Recommendation:** Always ensure WAL integrity. Use checksums and monitor `frogdb_wal_corruption_total` metric.

**Keys Created During Snapshot:**
- New keys are NOT part of this snapshot (snapshot captures point-in-time at epoch start)
- New key writes go to WAL and will be captured in next snapshot
- Recovery: Load snapshot, replay WAL (includes new keys)

**Concurrent Iteration State:**

```rust
struct SnapshotIterator {
    epoch: u64,
    visited: HashSet<Bytes>,   // Keys already serialized
    current_position: usize,   // Iterator position in shard's HashMap
}

fn on_write_during_snapshot(key: &Bytes, old_value: &Value, new_value: &Value) {
    if !self.visited.contains(key) {
        // COW: Serialize old value before overwriting
        self.batch_tx.send((key.clone(), old_value.clone())).await;
        self.visited.insert(key.clone());
    }
    // Now safe to apply new value
}
```

### Snapshot Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `snapshot_interval_s` | `3600` | Seconds between snapshots (0 = disabled) |
| `data_dir` | `./data` | Directory for RocksDB and snapshots |

### Snapshot Epoch vs RocksDB Sequence Number

Two different sequence concepts exist in FrogDB:

| Concept | Definition | Scope | Incremented When |
|---------|------------|-------|------------------|
| **Snapshot Epoch** | FrogDB-managed monotonic counter | Per-node | Each snapshot starts |
| **RocksDB Sequence Number (LSN)** | RocksDB-managed write sequence | Global (shared WAL) | Each WriteBatch committed |

**How They Relate:**

```rust
struct SnapshotMetadata {
    /// FrogDB epoch - incremented for each snapshot attempt
    epoch: u64,

    /// RocksDB sequence at snapshot start - used for WAL replay
    sequence_number: u64,

    /// Timestamp when snapshot started
    started_at: u64,

    /// Timestamp when snapshot completed (0 if in-progress)
    completed_at: u64,
}
```

**Why Both Exist:**
- **Epoch** identifies snapshot versions for FrogDB logic (which snapshot is newer, replication ID changes)
- **Sequence number** identifies the exact WAL position for recovery (replay WAL from this point)

**Example:**
```
Epoch 5 started at sequence 10000
  → Snapshot captures all data as of seq 10000
  → Writes during snapshot get seq 10001, 10002, ...
  → Snapshot completes
Epoch 6 started at sequence 15000
  → New snapshot captures all data as of seq 15000
```

**Recovery uses sequence number:**
1. Load snapshot (epoch 5, seq 10000)
2. Replay WAL from seq 10001 to current
3. Final state is consistent

### Snapshot Metadata Storage

Each snapshot has an associated metadata file that tracks its state and integrity.

**Metadata File Location:**
```
data/
├── rocksdb/
│   └── ...
└── snapshots/
    ├── snapshot_00005/           # Snapshot epoch 5
    │   ├── metadata.json         # Snapshot metadata
    │   ├── shard_0.sst           # Shard 0 data
    │   ├── shard_1.sst           # Shard 1 data
    │   └── ...
    ├── snapshot_00006/           # Snapshot epoch 6
    │   └── ...
    └── latest -> snapshot_00006  # Symlink to latest complete snapshot
```

**Metadata Format:**
```json
{
  "version": 1,
  "epoch": 6,
  "sequence_number": 15000,
  "started_at_ms": 1704825600000,
  "completed_at_ms": 1704825612000,
  "num_shards": 8,
  "num_keys": 1234567,
  "size_bytes": 536870912,
  "checksums": {
    "shard_0": "sha256:abc123...",
    "shard_1": "sha256:def456...",
    ...
  },
  "completion_marker": "FROGDB_SNAPSHOT_COMPLETE_v1"
}
```

**Metadata Fields:**

| Field | Purpose |
|-------|---------|
| `version` | Metadata format version (for future compatibility) |
| `epoch` | FrogDB snapshot epoch |
| `sequence_number` | RocksDB sequence at snapshot start |
| `started_at_ms` | Unix timestamp when snapshot began |
| `completed_at_ms` | Unix timestamp when snapshot finished (0 if incomplete) |
| `num_shards` | Number of shard files expected |
| `num_keys` | Total keys in snapshot |
| `size_bytes` | Total size of all shard files |
| `checksums` | SHA256 checksums of each shard file |
| `completion_marker` | Magic string indicating successful completion |

### Snapshot Completion Marker

The completion marker ensures partial or corrupted snapshots are not used.

**Completion Sequence:**
```rust
fn complete_snapshot(epoch: u64, metadata: &mut SnapshotMetadata) -> Result<()> {
    // 1. All shard files already written

    // 2. Calculate checksums for all files
    for shard_id in 0..num_shards {
        let checksum = sha256_file(&shard_file_path(epoch, shard_id))?;
        metadata.checksums.insert(format!("shard_{}", shard_id), checksum);
    }

    // 3. Set completion timestamp
    metadata.completed_at_ms = current_timestamp_ms();

    // 4. Add completion marker
    metadata.completion_marker = "FROGDB_SNAPSHOT_COMPLETE_v1".to_string();

    // 5. Write metadata file atomically (write to temp, rename)
    let temp_path = metadata_path(epoch).with_extension("tmp");
    write_json(&temp_path, metadata)?;
    std::fs::rename(&temp_path, &metadata_path(epoch))?;

    // 6. Update "latest" symlink atomically
    let latest_tmp = data_dir.join("latest.tmp");
    std::os::unix::fs::symlink(&snapshot_dir(epoch), &latest_tmp)?;
    std::fs::rename(&latest_tmp, data_dir.join("latest"))?;

    Ok(())
}
```

**Validation on Recovery:**
```rust
fn validate_snapshot(epoch: u64) -> Result<SnapshotMetadata> {
    let metadata = read_metadata(epoch)?;

    // 1. Check completion marker
    if metadata.completion_marker != "FROGDB_SNAPSHOT_COMPLETE_v1" {
        return Err(SnapshotError::Incomplete);
    }

    // 2. Check completed_at_ms is set
    if metadata.completed_at_ms == 0 {
        return Err(SnapshotError::Incomplete);
    }

    // 3. Verify all shard files exist with correct checksums
    for shard_id in 0..metadata.num_shards {
        let expected = metadata.checksums.get(&format!("shard_{}", shard_id))
            .ok_or(SnapshotError::MissingChecksum)?;
        let actual = sha256_file(&shard_file_path(epoch, shard_id))?;
        if expected != &actual {
            return Err(SnapshotError::ChecksumMismatch { shard_id });
        }
    }

    Ok(metadata)
}
```

**Failure Handling:**

| Failure Scenario | Detection | Recovery |
|------------------|-----------|----------|
| Crash during shard file write | Missing checksum entry or file | Use previous snapshot |
| Crash during metadata write | Missing or truncated metadata.json | Use previous snapshot |
| Crash during symlink update | "latest" points to previous snapshot | Use previous snapshot |
| Checksum mismatch | Validation fails | Use previous snapshot, log corruption |
| Metadata version mismatch | Unknown version | Use previous snapshot or fail |

---

## BGSAVE Command

Trigger a background snapshot without blocking the server:

```
BGSAVE
```

**Behavior:**
1. Returns `+Background saving started` immediately
2. Initiates forkless snapshot algorithm
3. Server continues processing commands
4. On completion, updates `last_save_time` and `rdb_changes_since_last_save`

**Monitoring:**
```
INFO persistence

# Persistence
rdb_changes_since_last_save:1234
rdb_bgsave_in_progress:1
rdb_last_save_time:1704825600
rdb_last_bgsave_status:ok
rdb_last_bgsave_time_sec:12
```

## LASTSAVE Command

Return timestamp of last successful snapshot:

```
LASTSAVE
```

Returns Unix timestamp (integer): `:1704825600`

---

## Backup Procedure

**Recommended Process:**

1. **Initiate snapshot:**
   ```
   BGSAVE
   ```

2. **Wait for completion:**
   ```bash
   while redis-cli INFO persistence | grep -q "rdb_bgsave_in_progress:1"; do
       sleep 1
   done
   ```

3. **Verify success:**
   ```
   redis-cli INFO persistence | grep rdb_last_bgsave_status
   # Should return: rdb_last_bgsave_status:ok
   ```

4. **Copy snapshot file:**
   ```bash
   cp /var/lib/frogdb/rocksdb/snapshot_latest /backup/frogdb_$(date +%Y%m%d).rdb
   ```

**Snapshot File Properties:**
- Immutable after completion (renamed atomically on finish)
- Contains all shard data at snapshot epoch
- Self-contained (no WAL needed for consistent restore)
- Portable across compatible FrogDB versions

---

## Point-in-Time Recovery

For recovery to specific point beyond snapshot:

1. **Restore snapshot** (provides base state)
2. **Replay WAL** up to desired timestamp/sequence

**Configuration:**
```toml
[persistence]
# Keep WAL files for point-in-time recovery
wal_retention_time_s = 86400  # 24 hours
```

**Recovery Command (future):**
```
RESTORE-PIT <timestamp_ms>
```

---

## Backup Consistency

| Backup Type | Consistency | Notes |
|-------------|-------------|-------|
| **Snapshot only** | Epoch point-in-time | Keys deleted during snapshot may be absent |
| **Snapshot + WAL** | True point-in-time | Full consistency with WAL replay |
| **WAL only** | Full history | Requires snapshot base for recovery |

---

## WAL Retention for Replication

When clustering is enabled, WAL files are retained beyond normal durability needs to support replica synchronization.

### Purpose

Replicas that disconnect briefly need to catch up without full resynchronization:

```
Primary                              Replica
   │                                    │
   │ ── WAL entries 1000-1500 ─────────▶│
   │                                    │
   │              [Replica disconnects] │
   │                                    │
   │  WAL entries 1501-2000 (buffered)  │
   │                                    │
   │              [Replica reconnects]  │
   │                                    │
   │◀──── PSYNC repl_id 1500 ──────────│
   │                                    │
   │ ── WAL entries 1501-2000 ─────────▶│  (Partial sync succeeds)
```

If WAL entries 1501-2000 were already purged, a full resync would be required.

### RocksDB WAL Archive

RocksDB moves obsolete WAL files to an archive directory before deletion:

```
data/
├── rocksdb/
│   ├── 000001.log      (current WAL)
│   └── archive/
│       ├── 000000.log  (archived for replication)
│       └── ...
```

### APIs for Replication

| API | Purpose |
|-----|---------|
| `GetLatestSequenceNumber()` | Get current WAL position |
| `GetUpdatesSince(seq)` | Iterate WAL entries from sequence |
| `GetSortedWalFiles()` | List available WAL files |

### WAL Retention Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `wal_retention_size` | `100MB` | Keep at least this much WAL for replicas |
| `wal_retention_time` | `3600s` | Keep WAL files for at least this duration |
| `repl_backlog_size` | `1MB` | In-memory buffer for fast reconnection |

### Trade-offs

| Aspect | Larger Retention | Smaller Retention |
|--------|------------------|-------------------|
| Disk usage | Higher | Lower |
| Partial sync success | More likely | Less likely |
| Stale replica recovery | Better | Worse (triggers full sync) |

See [CLUSTER.md](CLUSTER.md) for complete replication protocol details.

---

## Recovery Process

### Startup Sequence

1. **Open RocksDB** with all column families (one per shard)
2. **Find latest snapshot** from metadata
3. **Replay WAL** from snapshot's sequence number to end
4. **Rebuild expiry index** from `expires_at` field in each value
5. **Initialize shard workers** with recovered data

### Column Families and Sequence Numbers

RocksDB uses a **single global sequence number** across all column families:

```
Sequence 1000: CF:s0 PUT key1 value1
Sequence 1001: CF:s2 PUT key2 value2
Sequence 1002: CF:s0 DEL key3
...
```

**Recovery implications:**
- Snapshot captures global sequence number at start
- WAL replay begins from that global sequence
- Cross-shard operations in a WriteBatch are atomic (single sequence)

### Partial Snapshot Handling

If server crashes during snapshot:
- Incomplete snapshot is detected (missing completion marker)
- Previous complete snapshot is used instead
- WAL contains all operations since that older snapshot

### Expired Key Handling on Recovery

When loading a snapshot or replaying WAL, many keys may have `expires_at` timestamps in the past.

**Recovery Behavior (Lazy Expiry):**

```rust
fn recover_key(key: &[u8], value: &ValueHeader) -> KeyRecoveryAction {
    // Load ALL keys, including expired ones
    // Expiry is handled lazily after recovery completes

    KeyRecoveryAction::Load
}

fn post_recovery_cleanup(shard: &mut Shard) {
    // After recovery complete, schedule lazy cleanup

    // Option 1: Background expiry scan (recommended)
    spawn_expiry_scanner(shard);

    // Option 2: Expire on first access (always happens)
    // Keys are checked on read and deleted if expired
}
```

**Why Not Expire During Recovery?**
1. **Speed:** Recovery should be as fast as possible
2. **Simplicity:** Expiry logic is complex (active + passive), recovery is simpler without it
3. **Consistency:** Easier to reason about recovery state

**Memory Impact:**

Large numbers of expired keys may temporarily consume memory after recovery. Mitigation strategies:

| Strategy | Implementation | Trade-off |
|----------|----------------|-----------|
| **Lazy cleanup (default)** | Expire on access + background scan | Memory spike until first scan completes |
| **Immediate cleanup** | Scan all keys after recovery | Slower startup time |
| **Incremental recovery** | Expire during WAL replay | Complex, may miss snapshot keys |

**Configuration:**
```toml
[recovery]
# Strategy for expired keys
expired_key_strategy = "lazy"  # "lazy" (default), "immediate"

# Background scan interval after recovery (lazy mode)
expired_scan_delay_ms = 5000   # Start scan 5s after recovery

# Scan batch size to limit CPU impact
expired_scan_batch_size = 1000
```

**Metrics After Recovery:**
```
frogdb_recovery_keys_loaded        # Total keys loaded
frogdb_recovery_keys_expired       # Keys expired after recovery scan
frogdb_recovery_expired_bytes      # Memory reclaimed from expired keys
```

**Important:** Monitor memory usage after recovery. If expired keys consume significant memory, consider using `immediate` strategy or reducing `expired_scan_delay_ms`.

---

## Restore Procedure

1. **Stop FrogDB server**

2. **Replace data directory:**
   ```bash
   rm -rf /var/lib/frogdb/rocksdb
   cp -r /backup/snapshot /var/lib/frogdb/rocksdb
   ```

3. **Start server:**
   - Detects snapshot automatically
   - Replays any WAL files present
   - Becomes available after recovery

**Important:** Ensure sufficient disk space for both backup and running database during restore.

---

## Persistence Backpressure

When RocksDB can't keep up with write rate, backpressure propagates to clients.

### Write Pipeline

```
Client → Shard Worker → Persistence Queue → RocksDB
                              │
                              └── WriteBatch accumulator
```

### Backpressure Mechanism

```rust
struct PersistenceQueue {
    /// Bounded queue of pending WriteBatches
    queue: ArrayQueue<WriteBatch>,
    /// High watermark (80% full)
    high_watermark: usize,
}

impl PersistenceQueue {
    async fn enqueue(&self, batch: WriteBatch) {
        // If queue is at high watermark, block
        while self.queue.len() >= self.high_watermark {
            // Apply backpressure - shard worker blocks here
            tokio::task::yield_now().await;
        }
        self.queue.push(batch);
    }
}
```

### Backpressure Flow

```
RocksDB slow (disk I/O)
       │
       ▼
PersistenceQueue fills
       │
       ▼
Shard worker blocks on enqueue
       │
       ▼
Shard channel fills (1024 messages)
       │
       ▼
Connection blocks on send
       │
       ▼
Client TCP buffer fills
       │
       ▼
Client experiences latency
```

### Monitoring

| Metric | Description |
|--------|-------------|
| `frogdb_persistence_queue_depth` | Current queue size |
| `frogdb_persistence_write_latency_ms` | RocksDB write latency |
| `frogdb_persistence_batch_size` | Average WriteBatch size |
| `frogdb_persistence_backpressure_events` | Times backpressure applied |

### Backpressure Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `persistence_queue_size` | 64 | Max pending WriteBatches |
| `persistence_batch_max_size` | 4MB | Max size per WriteBatch |
| `persistence_batch_timeout_ms` | 10 | Max time to accumulate batch |

### WriteBatch Batching Timing

Individual writes are accumulated into WriteBatches before being sent to RocksDB. Understanding when batches are triggered is important for durability and performance.

**Batch Trigger Conditions:**

A WriteBatch is flushed to RocksDB when ANY of these conditions is met:

```rust
struct WriteBatchAccumulator {
    batch: WriteBatch,
    batch_start: Instant,
    batch_size: usize,

    // Configuration
    max_size: usize,      // persistence_batch_max_size (4MB default)
    timeout_ms: u64,      // persistence_batch_timeout_ms (10ms default)
}

impl WriteBatchAccumulator {
    fn add_write(&mut self, key: &[u8], value: &[u8]) -> Option<WriteBatch> {
        self.batch.put(key, value);
        self.batch_size += key.len() + value.len();

        // Trigger 1: Size threshold reached
        if self.batch_size >= self.max_size {
            return Some(self.take_batch());
        }

        // Trigger 2: Timeout (checked in background)
        // See timeout_check() below

        None
    }

    fn add_delete(&mut self, key: &[u8]) -> Option<WriteBatch> {
        self.batch.delete(key);
        self.batch_size += key.len();

        if self.batch_size >= self.max_size {
            return Some(self.take_batch());
        }

        None
    }

    /// Called periodically by background task
    fn timeout_check(&mut self) -> Option<WriteBatch> {
        if self.batch_size > 0 && self.batch_start.elapsed().as_millis() >= self.timeout_ms as u128 {
            return Some(self.take_batch());
        }
        None
    }

    /// Called on explicit flush request (e.g., BGSAVE, shutdown)
    fn explicit_flush(&mut self) -> Option<WriteBatch> {
        if self.batch_size > 0 {
            return Some(self.take_batch());
        }
        None
    }

    fn take_batch(&mut self) -> WriteBatch {
        let batch = std::mem::take(&mut self.batch);
        self.batch_size = 0;
        self.batch_start = Instant::now();
        batch
    }
}
```

**Batch Triggers Summary:**

| Trigger | Condition | Behavior |
|---------|-----------|----------|
| **Size** | `batch_size >= persistence_batch_max_size` | Immediate flush to RocksDB |
| **Timeout** | `elapsed >= persistence_batch_timeout_ms` AND `batch_size > 0` | Background task flushes |
| **Explicit** | `BGSAVE`, shutdown, or `SYNC` command | Immediate flush |

**Timing Diagram:**
```
T=0ms:    Write A (100 bytes) → batch_start = now, batch_size = 100
T=5ms:    Write B (200 bytes) → batch_size = 300
T=10ms:   Timeout fires → batch_size > 0 → FLUSH to RocksDB
T=10ms:   batch_start = now, batch_size = 0
T=15ms:   Write C (4MB) → batch_size >= max → FLUSH immediately
T=15ms:   batch_start = now, batch_size = 0
```

**Interaction with Durability Mode:**

| Durability Mode | Batch Behavior | Fsync Timing |
|-----------------|----------------|--------------|
| `Async` | Batch as configured | No fsync |
| `Periodic` | Batch as configured | Fsync on periodic timer or write count |
| `Sync` | Immediate flush (batch size = 1) | Fsync after each write |

**Note:** In `Sync` mode, `persistence_batch_max_size` and `persistence_batch_timeout_ms` are ignored. Each write is flushed and fsynced immediately.

**When Is a Write "In WAL"?**

A write enters the WAL when its batch is flushed to RocksDB:
- Before flush: Write is in-memory batch only
- After flush: Write is in RocksDB WAL (but may not be fsynced)
- After fsync: Write is durably on disk

**Client Acknowledgment vs WAL State:**

In `Async` and `Periodic` modes, the client receives `+OK` BEFORE the write is in WAL:

```
T=0:    Client: SET key value
T=0:    Server: Apply to in-memory store
T=0:    Server: Add to batch (batch_size = 100)
T=0:    Server: Return +OK to client  ← CLIENT SEES SUCCESS
T=10ms: Server: Batch timeout → flush to WAL
T=10ms: Server: Write is now in WAL (not fsynced)
T=100ms: Server: Periodic fsync → write is durable
```

---

## Automated Backups

### Scheduled Snapshots

Configure automatic background saves:

```toml
[persistence]
# Trigger BGSAVE after N seconds if at least M changes
save_intervals = [
    { seconds = 900, changes = 1 },      # 15 min if >= 1 change
    { seconds = 300, changes = 10 },     # 5 min if >= 10 changes
    { seconds = 60, changes = 10000 },   # 1 min if >= 10000 changes
]
```

### External Backup Tools

FrogDB snapshots are RocksDB-compatible. Use standard tools:

```bash
# Using RocksDB's backup tool
ldb backup --db=/var/lib/frogdb/rocksdb --backup_dir=/backup/

# Using filesystem snapshots (ZFS, LVM)
zfs snapshot tank/frogdb@backup-$(date +%Y%m%d)
```

---

## References

- [CLUSTER.md](CLUSTER.md) - Replication and cluster persistence
- [FAILURE_MODES.md](FAILURE_MODES.md) - Recovery from failures
- [CONFIGURATION.md](CONFIGURATION.md) - Server configuration
