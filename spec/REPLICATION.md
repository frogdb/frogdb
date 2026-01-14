# Replication

This document is the authoritative specification for FrogDB's primary-replica replication system.

## Overview

FrogDB supports primary-replica replication for high availability and read scaling.

**Key characteristics:**
- **Orchestrated model** - External orchestrator manages topology (not gossip)
- **Full dataset replication** - Replicas copy entire primary, not slot-specific
- **RocksDB WAL streaming** - Uses `GetUpdatesSince()` for incremental sync
- **PSYNC protocol** - Redis-compatible partial sync when possible
- **TCP backpressure** - No buffer limits, avoids "full sync loop" problems

For clustering and sharding, see [CLUSTER.md](CLUSTER.md).

## Architecture

### Data Flow

```
Primary: Write → In-Memory → WAL → Replication Stream → Replicas
                              ↓
                        WAL Archive (disk)
```

### Sync Types

| Type | Trigger | Mechanism |
|------|---------|-----------|
| FULLRESYNC | New replica, WAL gap too large | RocksDB checkpoint transfer |
| PSYNC | Reconnection within backlog | Resume from last sequence number |

---

## Replication ID

Each dataset history has a unique identifier:

- **40-character hex string** (like Redis)
- **Changes on failover**: New primary generates new ID
- **Secondary ID**: Promoted replicas remember old primary's ID

```
Primary A (repl_id: abc123...)
    │
    ├── Replica B (tracking abc123...)
    │
    [Primary A fails, B promoted]
    │
Primary B (repl_id: def456..., secondary_id: abc123...)
```

This allows replicas of A to connect to B and continue incrementally.

### Replication ID Generation

```rust
fn generate_replication_id() -> String {
    // 20 random bytes → 40 hex characters
    let mut bytes = [0u8; 20];
    getrandom::getrandom(&mut bytes).expect("random bytes");
    hex::encode(bytes)
}
```

**When a new ReplicationId is generated:**
- Fresh primary startup (no existing data)
- Replica promoted to primary (failover) - see [CLUSTER.md#failover](CLUSTER.md#failover)
- Manual `DEBUG RESET-REPLICATION-ID` command

**Collision prevention:** 20 random bytes = 160 bits of entropy. Collision probability is negligible (< 2^-80 for millions of IDs).

### Secondary ID Usage

When a replica is promoted:
```rust
fn on_promotion(&mut self) {
    // Preserve old primary's ID for PSYNC continuity
    self.secondary_repl_id = Some(self.repl_id.clone());
    self.secondary_repl_offset = self.repl_offset;

    // Generate new ID for this primary's history
    self.repl_id = generate_replication_id();
}
```

Other replicas can PSYNC using either the new primary's ID (if they connected after promotion) or the secondary ID (if they were replicating from the old primary).

---

## Full Synchronization (FULLRESYNC)

When incremental sync is not possible:

```
Primary                              Replica
   │                                    │
   │◀──── PSYNC ? -1 ──────────────────│  "I have no data"
   │                                    │
   │  Create RocksDB checkpoint         │
   │                                    │
   │───── FULLRESYNC <id> <seq> ──────▶│
   │                                    │
   │───── [checkpoint files] ─────────▶│  Transfer checkpoint
   │                                    │
   │                                    │  Load checkpoint
   │                                    │
   │◀──── PSYNC <id> <seq> ────────────│  Ready for incremental
   │                                    │
   │───── [WAL stream] ───────────────▶│  Continue streaming
```

### Concurrent FULLRESYNC Requests

If multiple replicas request FULLRESYNC simultaneously, FrogDB creates a **single RocksDB checkpoint** and streams it to all requesting replicas. This amortizes the cost of checkpoint creation across multiple sync operations.

| Scenario | Behavior |
|----------|----------|
| First FULLRESYNC request | Create checkpoint, begin streaming |
| Additional request during checkpoint creation | Wait for checkpoint, join streaming |
| Additional request during checkpoint transfer | Reuse existing checkpoint |
| All streams complete | Checkpoint eligible for cleanup |

### Checkpoint Streaming Protocol

During FULLRESYNC, the primary sends a RocksDB checkpoint to the replica. The checkpoint consists of multiple files.

**Checkpoint Transfer Format:**
```
┌────────────────────────────────────────────────────────────────────────┐
│                    FULLRESYNC Checkpoint Transfer                       │
├────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Primary sends:                                                        │
│                                                                         │
│  1. FULLRESYNC response:                                               │
│     +FULLRESYNC <repl_id> <sequence>\r\n                               │
│                                                                         │
│  2. Checkpoint header:                                                 │
│     $<total_size>\r\n     (total bytes in checkpoint, for progress)   │
│                                                                         │
│  3. File entries (repeated for each file):                             │
│     *3\r\n                                                             │
│     $<name_len>\r\n<filename>\r\n                                      │
│     :<file_size>\r\n                                                   │
│     $<file_size>\r\n<file_data>\r\n                                   │
│                                                                         │
│  4. End marker:                                                        │
│     *1\r\n                                                             │
│     $3\r\nEOF\r\n                                                      │
│                                                                         │
│  5. Checksum footer:                                                   │
│     $40\r\n<sha256_hex_of_all_files>\r\n                              │
│                                                                         │
└────────────────────────────────────────────────────────────────────────┘
```

**Checkpoint Integrity:**
- Primary computes SHA256 of all file contents concatenated in transfer order
- Replica verifies SHA256 after receiving all files
- Mismatch → abort, delete partial checkpoint, retry with PSYNC ? -1

**Interrupted Transfer Recovery:**

| Interruption Point | Recovery |
|--------------------|----------|
| Before $<total_size> | Replica retries PSYNC ? -1 |
| Mid-file | Delete partial file, retry PSYNC ? -1 |
| Before EOF marker | Delete all files, retry PSYNC ? -1 |
| Before checksum | Delete all files, retry PSYNC ? -1 |
| Checksum mismatch | Delete all files, retry PSYNC ? -1 |

**Checkpoint Transfer Timeout:**
```toml
[replication]
checkpoint_transfer_timeout_ms = 300000  # 5 minutes default
checkpoint_file_timeout_ms = 60000       # Per-file timeout
```

### Replica Memory Constraints

**Problem:** Primary dataset may exceed replica memory during full sync.

**Detection:**
- Replica monitors memory usage during checkpoint load
- If `used_memory` exceeds `max_memory * 0.9`, abort sync

**Behavior:**
```
Replica                              Primary
   │                                    │
   │◀──── FULLRESYNC <id> <seq> ───────│
   │                                    │
   │  Loading checkpoint...             │
   │  Memory: 80%... 85%... 90%         │
   │                                    │
   │───── SYNC_ABORTED OOM ───────────▶│
   │                                    │
   │  (Wait, retry with backoff)        │
```

**Recovery Options:**
1. Increase replica `max_memory`
2. Enable eviction on replica (when implemented)
3. Use replica with larger memory
4. Reduce primary dataset size

**Configuration:**
```toml
[replication]
# Abort full sync if memory exceeds this percentage
sync_memory_limit_pct = 90

# Retry sync after this delay (with exponential backoff)
sync_retry_delay_ms = 5000
```

**Checkpoint Cleanup on Abort:**

When replica aborts full sync due to OOM:

1. **Partial checkpoint discarded:** Incomplete RocksDB checkpoint deleted
2. **Local state unchanged:** Replica retains pre-sync data (if any)
3. **Availability:** Replica can serve READONLY from stale data during retry
4. **Backoff:** Wait `sync_retry_delay_ms * 2^attempt` before retry

---

## Partial Synchronization (PSYNC)

When replica reconnects with valid state:

```
Primary                              Replica
   │                                    │
   │◀──── PSYNC <repl_id> <seq> ───────│  "I have data up to seq"
   │                                    │
   │  Check: Is seq in WAL retention?   │
   │                                    │
   │───── CONTINUE ───────────────────▶│  "Yes, continuing"
   │                                    │
   │───── [WAL entries from seq] ─────▶│  Stream missing entries
```

### PSYNC Error Responses

| Response | Meaning | Replica Action |
|----------|---------|----------------|
| `+FULLRESYNC <id> <seq>` | Full sync required | Load checkpoint, stream WAL |
| `+CONTINUE` | Partial sync OK | Stream WAL from last seq |
| `-ERR unknown replication ID` | repl_id doesn't match | Retry PSYNC ? -1 (full sync) |
| `-ERR sequence too old` | seq outside WAL retention | Retry PSYNC ? -1 (full sync) |
| `-ERR primary shutting down` | Primary in shutdown | Wait, reconnect to new primary |
| `-ERR not primary` | Node is not a primary | Query orchestrator for primary |
| `-ERR too many replicas` | `max_replicas` exceeded (optional limit) | Alert operator, retry later |

**Example Error Handling:**
```
Replica: PSYNC abc123... 50000
Primary: -ERR sequence too old, last retained: 75000
Replica: PSYNC ? -1
Primary: +FULLRESYNC abc123... 80000
```

---

## WAL Streaming

Primary streams RocksDB WAL entries to replicas:

```rust
// Conceptual flow
fn stream_to_replica(replica: &mut Connection, from_seq: u64) {
    let mut current_seq = from_seq;

    loop {
        // Get updates since last sequence
        let updates = rocksdb.get_updates_since(current_seq);

        for batch in updates {
            // Convert WriteBatch to replication format
            let repl_data = encode_for_replication(batch);

            // Send to replica
            replica.send(repl_data);

            current_seq = batch.sequence_number();
        }

        // Wait for more WAL entries
        wait_for_wal_update();
    }
}
```

### Replication Data Format

WAL entries are streamed using RocksDB's native WriteBatch format with metadata header:

```
┌──────────────────────────────────────────────────────────────┐
│ Replication Frame                                             │
├──────────────────────────────────────────────────────────────┤
│ magic: u32        │ 0x46524F47 ("FROG")                      │
│ version: u8       │ Protocol version (1)                      │
│ flags: u8         │ 0x01 = compressed, 0x02 = has_checksum   │
│ sequence: u64     │ RocksDB sequence number                   │
│ timestamp_ms: u64 │ Wall clock time of write                  │
│ batch_len: u32    │ Length of WriteBatch data                 │
│ batch_data: [u8]  │ Raw RocksDB WriteBatch bytes             │
│ checksum: u32     │ CRC32 (if flags & 0x02)                  │
└──────────────────────────────────────────────────────────────┘
```

**Why include timestamp?**
- Enables lag calculation in seconds (not just bytes)
- Supports future PITR features
- Debugging and observability

**Compression:**
When `flags & 0x01`, batch_data is LZ4-compressed. Compression enabled when batch_len > 1KB.

### WAL Entry Stream Framing

Multiple replication frames are sent back-to-back on the TCP stream. Each frame is **self-delimiting** via the `batch_len` field:

```
┌────────────────────────────────────────────────────────────────────────┐
│                    Replication Stream Layout                            │
├────────────────────────────────────────────────────────────────────────┤
│ [Frame 1 Header][Frame 1 Data][Frame 2 Header][Frame 2 Data][...]      │
│                                                                         │
│ Each frame:                                                            │
│   - magic (4 bytes) + version (1) + flags (1) + seq (8) + ts (8)       │
│   - batch_len (4 bytes) tells exact size of batch_data                 │
│   - batch_data (batch_len bytes)                                        │
│   - checksum (4 bytes, if flags & 0x02)                                │
│                                                                         │
│ Parsing: Read 26-byte header, extract batch_len, read batch_len bytes  │
└────────────────────────────────────────────────────────────────────────┘
```

**Replica Parsing Algorithm:**
```rust
loop {
    // 1. Read fixed header (26 bytes)
    let header = read_exact(stream, 26)?;
    let magic = u32::from_le_bytes(&header[0..4]);
    if magic != 0x46524F47 {
        return Err(InvalidMagic);
    }

    let flags = header[5];
    let seq = u64::from_le_bytes(&header[6..14]);
    let timestamp = u64::from_le_bytes(&header[14..22]);
    let batch_len = u32::from_le_bytes(&header[22..26]);

    // 2. Read batch data
    let batch_data = read_exact(stream, batch_len as usize)?;

    // 3. Read checksum if present
    let checksum = if flags & 0x02 != 0 {
        Some(u32::from_le_bytes(read_exact(stream, 4)?))
    } else {
        None
    };

    // 4. Validate checksum
    if let Some(expected) = checksum {
        let actual = crc32(&batch_data);
        if actual != expected {
            return Err(ChecksumMismatch { seq, expected, actual });
        }
    }

    // 5. Decompress if needed
    let batch = if flags & 0x01 != 0 {
        lz4_decompress(&batch_data)?
    } else {
        batch_data
    };

    // 6. Apply WriteBatch
    apply_write_batch(&batch, seq)?;
}
```

### Sequence Number Scope

FrogDB uses RocksDB's **global sequence number** across all internal shards:

```
┌────────────────────────────────────────────────────────────────────────┐
│                    Single RocksDB Instance                              │
├────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Column Family 0     Column Family 1     Column Family N               │
│  (Internal Shard 0)  (Internal Shard 1)  (Internal Shard N)            │
│       │                   │                   │                        │
│       └───────────────────┴───────────────────┘                        │
│                           │                                            │
│                    Single WAL                                          │
│                    Global Sequence Numbers                             │
│                                                                         │
│  seq=100: Write to CF0 (key "user:1")                                  │
│  seq=101: Write to CF1 (key "user:2")                                  │
│  seq=102: Write to CF0 (key "user:3")                                  │
│  seq=103: Write to CF1 (key "user:4")                                  │
│                                                                         │
└────────────────────────────────────────────────────────────────────────┘
```

**Key Points:**
- One RocksDB instance per node, with one column family per internal shard
- All shards share a single WAL with monotonically increasing global sequence numbers
- Replication streams the global WAL - replica receives writes from ALL internal shards
- No per-shard sequence mapping needed
- `GetUpdatesSince(seq)` returns WriteBatches from all column families

---

## Replication Connection Management

### Authentication

Replicas authenticate with primaries using dedicated credentials:

| Config | Description |
|--------|-------------|
| `primary_auth` | Password for replica→primary authentication |
| `primary_user` | ACL username for replication (if using ACLs) |

**Configuration:**
```toml
[replication]
primary_auth = "replication-secret"
primary_user = "replicator"  # Optional, for ACL-based auth
max_replicas = 0             # Optional: 0 = unlimited (default), >0 = limit
```

**Behavior:**
- Replica sends AUTH before PSYNC handshake
- If `primary_user` set: `AUTH <user> <password>`
- If only `primary_auth` set: `AUTH <password>`
- Primary rejects PSYNC if authentication fails

**Best Practice:** Set `primary_auth` on all nodes (including primaries) since any node may become a replica after failover.

### PSYNC Handshake Sequence

```
Replica                              Primary
   │                                    │
   │───── AUTH [user] <password> ──────▶│  (if primary_auth configured)
   │◀──── +OK ──────────────────────────│
   │                                    │
   │───── REPLCONF listening-port 6379 ▶│  (optional, for INFO output)
   │◀──── +OK ──────────────────────────│
   │                                    │
   │───── REPLCONF capa eof psync2 ────▶│  (announce capabilities)
   │◀──── +OK ──────────────────────────│
   │                                    │
   │───── PSYNC <repl_id> <seq> ───────▶│  (initiate sync)
   │◀──── +FULLRESYNC or +CONTINUE ─────│
```

### Heartbeat Protocol

```
Primary                              Replica
   │                                    │
   │───── PING ────────────────────────▶│  Every `repl_ping_interval_ms`
   │◀──── PONG <last_seq> ─────────────│  Replica reports progress
   │                                    │
```

### Connection Timeouts

| Timeout | Default | Description |
|---------|---------|-------------|
| `repl_timeout_ms` | 60000 | No data received → close connection |
| `repl_ping_interval_ms` | 10000 | Heartbeat frequency |
| `repl_reconnect_base_ms` | 1000 | Initial reconnect delay |
| `repl_reconnect_max_ms` | 30000 | Maximum reconnect delay |

**Reconnection Backoff:**
```
delay = min(repl_reconnect_base_ms * 2^attempt, repl_reconnect_max_ms)
```

### Stream Interruption Recovery

| Scenario | Primary Behavior | Replica Behavior |
|----------|------------------|------------------|
| Network timeout | Close conn, free backlog slot | Reconnect with PSYNC |
| Replica sends invalid seq | Log error, trigger FULLRESYNC | Accept FULLRESYNC |
| WAL read error | Log, close conn, mark unhealthy | Reconnect, may need FULLRESYNC |
| Replica OOM during stream | Receive SYNC_ABORTED | Pause, retry after backoff |

---

## Replication Backlog

In-memory buffer of recent WAL entries for fast reconnection:

| Setting | Default | Description |
|---------|---------|-------------|
| `repl_backlog_size` | 1MB | Size of backlog buffer |
| `repl_backlog_ttl` | 3600s | How long to keep backlog after last replica disconnects |

### Backlog vs Disk WAL Retention

FrogDB maintains TWO mechanisms for partial sync support:

| Mechanism | Storage | Speed | Capacity |
|-----------|---------|-------|----------|
| **Replication backlog** | Memory | Fast | Small (1MB default) |
| **WAL archive** | Disk | Slower | Large (100MB default) |

**PSYNC Resolution Order:**
```
1. Check repl_backlog (memory)
   └── If seq found: Stream from backlog (fastest)
2. Check WAL archive (disk)
   └── If seq found: Stream from disk WAL (slower, larger window)
3. Neither has seq
   └── Trigger FULLRESYNC (slowest, full dataset)
```

**Configuration Guidance:**

| Workload | Backlog Size | WAL Retention | Rationale |
|----------|--------------|---------------|-----------|
| Low write rate | 1MB | 100MB | Backlog sufficient for brief disconnects |
| High write rate | 16MB | 500MB | Larger buffers to absorb write bursts |
| Unreliable network | 64MB | 1GB | Maximize partial sync window |

### Backlog Overflow Behavior

The replication backlog is a **ring buffer** (matching Redis behavior) - when full, oldest entries are overwritten:

```
┌─────────────────────────────────────────────────────┐
│  Replication Backlog (ring buffer, 1MB default)     │
├─────────────────────────────────────────────────────┤
│  [seq:100] [seq:101] [seq:102] ... [seq:999]        │
│     ↑                                    ↑          │
│   oldest                              newest        │
│   (overwritten                      (just added)    │
│   on overflow)                                      │
└─────────────────────────────────────────────────────┘
```

**On Overflow:**
1. **Oldest entries overwritten:** Ring buffer wraps around
2. **Writes never rejected:** Primary never blocks on backlog full
3. **Replica impact:** If replica needs discarded sequence, falls back to disk WAL or FULLRESYNC

**PSYNC After Overflow:**

| Replica's Last Sequence | Resolution |
|-------------------------|------------|
| In backlog (recent) | Stream from memory (fastest) |
| Not in backlog, in disk WAL | Stream from disk |
| Not in disk WAL either | FULLRESYNC required |

**Sizing Formula:**
```
backlog_size >= write_rate_bytes_per_second * expected_disconnect_seconds
```

---

## Replica Flow Control

FrogDB streams WAL directly to replica sockets without intermediate buffering (DragonflyDB model):

- **No output buffer limit** for replicas
- WAL entries written directly to TCP socket
- If replica socket blocks, streaming pauses (TCP backpressure)
- Replica disconnected only on network failure or timeout

**Advantages:**
- Stable under high write loads
- No memory spikes from buffering
- No "full sync loop" problem (unlike Redis)

**Replica Disconnect Conditions:**

| Condition | Action |
|-----------|--------|
| Network timeout (`repl_timeout_ms`) | Disconnect, replica reconnects |
| Socket write error | Disconnect, replica reconnects |
| Replica explicitly disconnects | Clean up connection state |

**Note:** Unlike Redis, FrogDB does NOT disconnect replicas for being "slow" - TCP backpressure handles flow control naturally. This avoids the pathological "full sync loop" where slow replicas repeatedly disconnect and trigger expensive FULLRESYNC operations.

---

## Cascading Replication

FrogDB does **not** support cascading replication (replica-of-replica). Replicas must connect directly to primaries.

**Rationale:**
- Simpler implementation and fewer edge cases
- Avoids propagation delay compounding
- Cluster mode already provides horizontal scaling

**If geo-distribution is needed:** Deploy multiple clusters with application-level data routing, or use an external replication tool.

---

## Synchronous Replication

When `min_replicas_to_write >= 1`, writes wait for replica acknowledgment before responding to client.

### Protocol Flow

```
Primary                              Replica
   │                                    │
   │◀──── Client: SET key value ───────│
   │                                    │
   │  1. Apply to local store           │
   │  2. Append to WAL                  │
   │                                    │
   │───── REPLICATE <seq> <data> ──────▶│
   │                                    │
   │                                    │ 3. Apply to replica store
   │                                    │ 4. Append to replica WAL
   │                                    │
   │◀──── ACK <seq> ───────────────────│
   │                                    │
   │  5. Ack count >= min_replicas      │
   │                                    │
   │───── +OK ─────────────────────────▶│ (to client)
```

### ACK Message Format

ACKs are sent via `REPLCONF ACK` command to distinguish from PONG heartbeat responses:

```
Replica → Primary: REPLCONF ACK <sequence_number>
```

**Wire Format:**
```
*3\r\n
$8\r\nREPLCONF\r\n
$3\r\nACK\r\n
$<len>\r\n<sequence_number>\r\n
```

**Distinguishing ACK from PONG:**

| Message | Purpose | Format | When Sent |
|---------|---------|--------|-----------|
| `REPLCONF ACK <seq>` | Acknowledge writes (sync mode) | RESP array command | After applying WAL entry |
| `PONG <seq>` | Heartbeat response | Simple string | In response to PING |

### Timeout Behavior

| Scenario | Behavior |
|----------|----------|
| Replica ACK within `sync_timeout_ms` | Write succeeds |
| Timeout before enough ACKs | Return `-NOREPL Not enough replicas` |
| Replica disconnects during wait | Attempt reconnect, timeout if exceeds |

### Configuration

```toml
[cluster]
min_replicas_to_write = 1        # Minimum ACKs needed (0 = async)
sync_timeout_ms = 1000           # Max wait for replica ACKs
sync_write_quorum = "any"        # "any" = any N replicas, "all" = specific replicas
```

### Client Handling

- `-NOREPL` error: Retry on different node or accept potential data loss
- Writes may still succeed on primary even if `-NOREPL` returned (partial durability)

### Edge Cases

| Edge Case | Behavior |
|-----------|----------|
| Primary has no replicas | Writes fail immediately with `-NOREPL` |
| Replica ACK lost (network) | Primary retries, may timeout |
| Replica crashes after ACK sent | Write durable on primary, may be lost on replica |

### ACK Failure Handling

**ACK Lost in Network:**

```
Primary                              Replica
   │                                    │
   │───── REPLICATE seq=100 ───────────▶│
   │                                    │ Applied to replica
   │◀──── ACK seq=100 ─────────────────│ (ACK sent)
   │         ✗ (ACK lost)               │
   │                                    │
   │  [ACK timeout, retry]              │
   │───── REPLICATE seq=100 (retry) ───▶│
   │                                    │ Already applied (idempotent)
   │◀──── ACK seq=100 ─────────────────│
   │                                    │
   │  [ACK received, respond to client] │
```

**Retry Configuration:**
```toml
[replication]
sync_ack_retry_count = 3         # Retries before giving up
sync_ack_retry_delay_ms = 100    # Delay between retries
```

**Timeout With Partial ACKs:**

When `min_replicas_to_write = 2` and only 1 replica ACKs:

| Condition | Behavior |
|-----------|----------|
| 1 ACK within timeout | Return `-NOREPL` (needed 2) |
| 0 ACKs within timeout | Return `-NOREPL` |
| Write on primary | **Already applied** - cannot roll back |

**Important:** When `-NOREPL` is returned, the write **is already on the primary**. The error indicates durability concern, not write failure.

**Client Recovery Options:**

| Strategy | Behavior | Trade-off |
|----------|----------|-----------|
| **Retry** | Re-send write (should be idempotent) | Simple, may duplicate non-idempotent ops |
| **Check** | Read key to verify write | Extra round-trip |
| **Accept** | Log warning, continue | Fast, accepts potential loss |

---

## WAIT Command

```
WAIT numreplicas timeout
```

Block until write propagated to N replicas or timeout.

| Behavior | Description |
|----------|-------------|
| Returns | Number of replicas that acknowledged |
| Timeout 0 | Block forever |
| numreplicas = 0 | Return immediately with current ack count |

**Example:**
```
SET mykey myvalue
WAIT 1 5000
:1
```

### WAIT vs min_replicas_to_write

These mechanisms are **complementary, not overlapping**:

| Mechanism | When Applied | Purpose |
|-----------|--------------|---------|
| `min_replicas_to_write` | **Before** write | Gate: reject writes if insufficient replicas connected |
| `WAIT` | **After** write | Confirm: block until write replicated to N replicas |

**Interaction:**
- `min_replicas_to_write` checks replica *connectivity* (based on ping lag, pre-write check)
- `WAIT` checks *replication progress* (specific offset acknowledged, post-write)
- Both can be used together for defense-in-depth
- `WAIT` can request more replicas than `min_replicas_to_write` requires

**Example - Combined Usage:**
```toml
[cluster]
min_replicas_to_write = 1  # Ensure at least 1 replica is connected
```
```
SET user:1 data
WAIT 2 5000  # Wait for 2 replicas to acknowledge this specific write
:2
```

This enables applications to selectively wait for stronger replication on critical writes while keeping general writes performant.

---

## Failure Modes

### Replica Streaming Stall

**Trigger:** TCP socket to replica blocks for extended period (slow replica, network congestion).

**Behavior:**
- Primary WAL streaming pauses for that replica (TCP backpressure)
- Other replicas unaffected (independent sockets)
- If stall exceeds `repl_timeout_ms`: connection closed, replica reconnects

**Detection:**
- `frogdb_replica_streaming_stall_total` metric increments
- Log entry: `"Replica streaming stalled for {duration}ms"`
- Lag metrics increase: `frogdb_replication_lag_seconds`

**Recovery:**
1. Replica automatically reconnects after timeout
2. PSYNC attempted (resume from last offset)
3. Falls back to FULLRESYNC if offset no longer available

**Note:** Unlike Redis, FrogDB does NOT proactively disconnect "slow" replicas. TCP backpressure naturally handles flow control, avoiding the pathological "full sync loop" where slow replicas repeatedly trigger expensive FULLRESYNC operations.

### Replication Stream Corruption Detection

FrogDB validates replication data integrity using checksums:

**Frame Validation:**
```
┌─────────────────────────────────────────────────────┐
│ Replication Frame                                    │
├─────────────────────────────────────────────────────┤
│ magic: u32        │ 0x46524F47 ("FROG")             │
│ version: u8       │ Protocol version                │
│ flags: u8         │ 0x02 = has_checksum             │
│ sequence: u64     │ RocksDB sequence number         │
│ batch_len: u32    │ Length of payload               │
│ batch_data: [u8]  │ WriteBatch bytes                │
│ checksum: u32     │ CRC32 of batch_data             │
└─────────────────────────────────────────────────────┘
```

**Corruption Detection Points:**

| Check | Location | Action on Failure |
|-------|----------|-------------------|
| Magic number | Frame start | Close connection, reconnect |
| CRC32 mismatch | Frame end | Log error, request retransmit from seq |
| Sequence gap | Frame sequence | Log warning, may trigger FULLRESYNC |
| Invalid WriteBatch | RocksDB parse | Close connection, FULLRESYNC |

**On Corruption Detected:**

```
Replica                                Primary
   │                                      │
   │◀── [Frame seq=100, CRC=bad] ────────│
   │                                      │
   │  CRC mismatch detected               │
   │  Log: "Replication corruption at     │
   │        seq=100, requesting resync"   │
   │                                      │
   │── PSYNC <repl_id> 99 ───────────────▶│  Request from last good seq
   │                                      │
   │◀── +CONTINUE ───────────────────────│
   │◀── [Frame seq=100, CRC=ok] ─────────│  Retransmit
```

**Configuration:**
```toml
[replication]
# Enable CRC32 validation (recommended, small CPU overhead)
repl_checksum_enabled = true

# Action on corruption
repl_corruption_action = "reconnect"  # "reconnect" or "fullresync"

# Max retries before FULLRESYNC
repl_corruption_max_retries = 3
```

### Primary OOM During Checkpoint

**Trigger:** Primary runs out of memory while creating a checkpoint for FULLRESYNC.

**Behavior:**

```
Primary (90% memory)                   Replica
      │                                   │
      │◀── PSYNC <repl_id> -1 ───────────│  (Request FULLRESYNC)
      │                                   │
      │  Creating checkpoint...           │
      │  Memory grows (snapshot buffers)  │
      │  OOM triggered!                   │
      │                                   │
      │── -ERR FULLRESYNC failed: OOM ──▶│
      │                                   │
      │                                   │  Replica logs error
      │                                   │  Retry with backoff
      │                                   │
      │◀── [Retry after 10s] ────────────│
```

**Recovery Behavior:**

| Primary State | Replica Action |
|---------------|----------------|
| Checkpoint fails (OOM) | Receives error, retries with exponential backoff |
| Memory freed (keys evicted/deleted) | Retry may succeed |
| Sustained OOM | Replica stays disconnected, serves stale reads |

**Configuration:**
```toml
[replication]
# Max memory for checkpoint creation (relative to max_memory)
checkpoint_memory_limit_percent = 10

# Retry timing
fullresync_retry_base_ms = 1000
fullresync_retry_max_ms = 60000
fullresync_retry_multiplier = 2.0
```

**Mitigation:**
- Ensure sufficient memory headroom for checkpoints (recommend 10-20%)
- Monitor `frogdb_fullresync_oom_total` metric
- Consider `checkpoint_memory_limit_percent` to abort early

### Graceful Shutdown During Replication

**Trigger:** Primary receives SHUTDOWN while replicas are connected.

**Behavior:**

```
Admin                 Primary                          Replica
  │                      │                                │
  │── SHUTDOWN ─────────▶│                                │
  │                      │                                │
  │                      │  1. Stop accepting new writes  │
  │                      │  2. Flush pending WAL          │
  │                      │  3. Notify replicas            │
  │                      │                                │
  │                      │── -ERR SHUTDOWN in progress ──▶│
  │                      │   (on next PSYNC heartbeat)    │
  │                      │                                │
  │                      │  4. Close connections          │
  │                      │  5. Persist final state        │
  │                      │  6. Exit                       │
  │                      │                                │
  │                      X                                │
  │                                                       │
  │                                   Replica detects disconnect
  │                                   Attempts reconnect (fails)
  │                                   Waits for orchestrator
```

**Replica Behavior on Graceful Shutdown:**
1. Receives `-ERR SHUTDOWN in progress` or connection close
2. Logs: `"Primary shutting down, disconnecting"`
3. Does NOT immediately seek new primary (waits for orchestrator)
4. Continues serving READONLY requests with stale data
5. If orchestrator promotes it: becomes primary

**In-Flight Checkpoint Handling:**

| Shutdown Phase | Checkpoint State | Action |
|----------------|------------------|--------|
| Before checkpoint | Not started | No cleanup needed |
| During checkpoint creation | Partial files | Abort checkpoint, delete temp files |
| Checkpoint streaming to replica | Partial transfer | Replica receives error, discards partial |
| After checkpoint complete | Streaming WAL | Clean disconnect, replica has valid base |

### Checkpoint I/O Errors

**Trigger:** I/O error during checkpoint creation (disk full, permission denied, hardware failure).

**Behavior:**

```
Primary                                Replica
   │                                      │
   │◀── PSYNC <repl_id> -1 ──────────────│
   │                                      │
   │  Creating checkpoint...              │
   │  write(shard_0.sst) → ENOSPC!        │
   │                                      │
   │── -ERR FULLRESYNC failed: ──────────▶│
   │      disk full                       │
   │                                      │
   │  Cleanup partial checkpoint          │
   │  Log error                           │
   │  Continue serving clients            │
```

**Primary Actions on I/O Error:**
1. Abort checkpoint immediately
2. Delete any partial checkpoint files
3. Return error to requesting replica
4. Increment `frogdb_checkpoint_io_error_total`
5. Continue normal operation (no data loss on primary)

**Replica Actions:**
1. Log error with details
2. Enter retry loop with exponential backoff
3. Report status via `INFO replication`
4. Serve stale READONLY requests if allowed

**I/O Error Types:**

| Error | Recovery |
|-------|----------|
| `ENOSPC` (disk full) | Clear space, retry |
| `EACCES` (permission) | Fix permissions, restart |
| `EIO` (hardware) | Replace disk, restore from other replica |
| `EROFS` (read-only FS) | Remount RW, retry |

### CPU Exhaustion Effects on Replication

**Trigger:** Primary CPU is saturated (100% utilization).

**Effects:**

| Component | Behavior |
|-----------|----------|
| WAL streaming | May fall behind (lower priority than client requests) |
| Heartbeats | May be delayed (PING/PONG) |
| Checkpoint creation | Slowed significantly |
| Connection handling | New replica connections may timeout |

**Replication Stream Starvation:**

```
Primary (CPU 100%)                    Replica
      │                                   │
      │  Processing client requests...    │
      │  Replication thread starved       │
      │                                   │
      │   [No WAL frames sent]            │
      │                                   │
      │                                   │  Lag increases
      │                                   │  Heartbeat timeout approaching
      │                                   │
      │◀── REPLCONF ACK seq ─────────────│  (Replica reports position)
      │                                   │
      │  [Still no bandwidth for          │
      │   replication]                    │
      │                                   │
      │                                   │  T=60s: Heartbeat timeout
      │                                   │  Replica disconnects
      │                                   │  Reconnects, PSYNC
```

**TCP Backpressure Behavior:**

When primary is CPU-bound:
1. Replication write() calls may block (TCP buffer full)
2. This is normal - TCP backpressure provides flow control
3. Replica lag increases but data is not lost
4. If lag exceeds WAL retention, FULLRESYNC required

**Configuration for High-CPU Scenarios:**
```toml
[replication]
# Increase timeout for high-CPU primaries
repl_timeout_ms = 120000  # 2 minutes

# Ensure sufficient WAL retention for lag
wal_retention_size = 500MB  # Larger than normal
```

### Asymmetric Network Failure

**Trigger:** Network failure where one direction works but the other doesn't.

**Scenario 1: Primary → Replica works, Replica → Primary broken**

```
Primary                              Replica
   │                                    │
   │── WAL frames ─────────────────────▶│  (Works)
   │                                    │
   │◀──────────────────── ACK ──────── X│  (Blocked)
   │                                    │
   │  No ACKs received                  │
   │  Sync-mode writes timeout          │
   │  Returns -NOREPL                   │
   │                                    │
   │  T=60s: Assume replica dead        │
   │  Close connection                  │
```

**Scenario 2: Replica → Primary works, Primary → Replica broken**

```
Primary                              Replica
   │                                    │
   │── WAL frames ─────────X            │  (Blocked)
   │                                    │
   │◀──────────────────── PING ────────│  (Works - heartbeat)
   │                                    │
   │  Heartbeats received               │
   │  But WAL not flowing               │
   │                                    │
   │                                    │  Replica sees no data
   │                                    │  But primary not timing out
   │                                    │  Lag grows indefinitely
```

**Detection and Recovery:**

| Failure Type | Detection | Recovery |
|--------------|-----------|----------|
| Primary → Replica broken | Replica heartbeat timeout | Replica reconnects |
| Replica → Primary broken | Primary ACK timeout | Primary closes, replica reconnects |
| Both directions broken | Both timeout | Both sides cleanup, reconnect when healed |

**Replica-Initiated Reconnect:**

The replica is responsible for reconnection:
```rust
fn replication_health_check(&self) {
    let last_data = self.last_received_timestamp;
    let last_heartbeat_sent = self.last_ping_sent;

    // No data received but heartbeats sent successfully
    // Indicates asymmetric failure (primary → replica broken)
    if last_data.elapsed() > self.config.data_timeout
        && last_heartbeat_sent.elapsed() < self.config.heartbeat_interval
    {
        warn!("Asymmetric network failure detected - no data but heartbeats OK");
        self.reconnect();
    }
}
```

**Configuration:**
```toml
[replication]
# Time without data before reconnect attempt
repl_data_timeout_ms = 30000

# Heartbeat interval (should be < data_timeout)
repl_heartbeat_interval_ms = 10000
```

---

## Configuration Reference

All replication-related configuration settings:

| Setting | Default | Description |
|---------|---------|-------------|
| **Authentication** | | |
| `primary_auth` | - | Password for replica→primary auth |
| `primary_user` | - | ACL user for replication (optional) |
| **Backlog & Retention** | | |
| `repl_backlog_size` | 1MB | In-memory buffer for partial sync |
| `repl_backlog_ttl` | 3600s | Keep backlog after last replica disconnects |
| `wal_retention_size` | 100MB | Disk WAL for slower catch-up |
| `wal_retention_time` | 3600s | Keep WAL files for this duration |
| **Timeouts** | | |
| `repl_timeout_ms` | 60000 | Connection timeout |
| `repl_ping_interval_ms` | 10000 | Heartbeat frequency |
| `repl_reconnect_base_ms` | 1000 | Initial reconnect delay |
| `repl_reconnect_max_ms` | 30000 | Maximum reconnect delay |
| `repl_data_timeout_ms` | 30000 | Time without data before reconnect |
| **Synchronous Replication** | | |
| `min_replicas_to_write` | 0 | Sync replication quorum (0 = async) |
| `sync_timeout_ms` | 1000 | Max wait for replica ACKs |
| `sync_ack_retry_count` | 3 | Retries before giving up |
| `sync_ack_retry_delay_ms` | 100 | Delay between retries |
| **FULLRESYNC** | | |
| `checkpoint_transfer_timeout_ms` | 300000 | Total checkpoint timeout |
| `checkpoint_file_timeout_ms` | 60000 | Per-file timeout |
| `checkpoint_memory_limit_percent` | 10 | Max memory for checkpoint |
| `sync_memory_limit_pct` | 90 | Abort sync if memory exceeds |
| `sync_retry_delay_ms` | 5000 | Retry delay after abort |
| `fullresync_retry_base_ms` | 1000 | Initial retry delay |
| `fullresync_retry_max_ms` | 60000 | Maximum retry delay |
| **Validation** | | |
| `repl_checksum_enabled` | true | Enable CRC32 validation |
| `repl_corruption_action` | "reconnect" | Action on corruption |
| `repl_corruption_max_retries` | 3 | Max retries before FULLRESYNC |
| **Limits** | | |
| `max_replicas` | 0 | Max connected replicas (0 = unlimited) |

---

## Metrics Reference

### Replication Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_replication_offset` | Gauge | Current replication offset (seq number) |
| `frogdb_replication_lag_seconds` | Gauge | Lag behind primary |
| `frogdb_replication_lag_bytes` | Gauge | Lag in bytes behind primary |
| `frogdb_connected_replicas` | Gauge | Number of connected replicas (primary only) |
| `frogdb_sync_full_count` | Counter | Full syncs performed |
| `frogdb_sync_partial_ok` | Counter | Successful partial syncs |
| `frogdb_sync_partial_err` | Counter | Failed partial syncs (triggered full) |
| `frogdb_replica_streaming_stall_total` | Counter | Streaming stalls detected |
| `frogdb_repl_checksum_failures_total` | Counter | CRC mismatches detected |
| `frogdb_repl_corruption_reconnects_total` | Counter | Reconnections due to corruption |
| `frogdb_fullresync_oom_total` | Counter | OOM during FULLRESYNC |
| `frogdb_checkpoint_io_error_total` | Counter | Checkpoint I/O errors |
| `frogdb_sync_aborted_oom_total` | Counter | OOM-aborted syncs |
| `frogdb_replica_state` | Gauge | 0=empty, 1=syncing, 2=stale, 3=connected |

### Backlog Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_repl_backlog_size_bytes` | Gauge | Current backlog usage |
| `frogdb_repl_backlog_oldest_seq` | Gauge | Earliest sequence in backlog |
| `frogdb_repl_backlog_evictions_total` | Counter | Entries overwritten due to wrap |

### Synchronous Replication Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_sync_ack_success_total` | Counter | Successful sync ACKs |
| `frogdb_sync_ack_timeout_total` | Counter | ACK timeouts |
| `frogdb_sync_ack_retry_total` | Counter | ACK retries |
| `frogdb_sync_norepl_errors_total` | Counter | -NOREPL errors returned |
| `frogdb_sync_partial_ack_total` | Counter | Some but not enough ACKs |

### Replication Lag Measurement

**Calculation (matching Redis):**

| Metric | Calculation | Description |
|--------|-------------|-------------|
| `lag_seconds` | `now - last_ack_time` | Seconds since last REPLCONF ACK from replica |
| `lag_bytes` | `primary_offset - replica_offset` | Bytes behind primary |

Replicas send `REPLCONF ACK <offset>` every `repl_ping_interval_ms` (default: 1000ms).

**Why seconds-since-ACK?**
- Simple and reliable (no throughput estimation needed)
- Matches Redis behavior exactly
- Works correctly even when write throughput is zero

**Lag Visibility:**

On primary (via INFO replication):
```
# Replication
role:master
connected_slaves:2
slave0:ip=10.0.0.2,port=6379,state=online,offset=12345678,lag=0
slave1:ip=10.0.0.3,port=6379,state=online,offset=12345600,lag=1
```

On replica:
```
# Replication
role:slave
master_link_status:up
master_last_io_seconds_ago:0
master_sync_in_progress:0
slave_repl_offset:12345678
slave_read_repl_offset:12345678
master_repl_offset:12345700
```

### Alerting Thresholds

| Threshold | Status | Action |
|-----------|--------|--------|
| < 1 second | Healthy | Normal operation |
| 1-5 seconds | Elevated | Monitor closely |
| 5-30 seconds | Warning | Investigate primary load or network |
| > 30 seconds | Critical | Risk of data loss on failover |

**Data Loss Bound:**

On failover, maximum data loss = replication lag at time of failure.
With `lag_seconds = 5`, up to 5 seconds of writes may be lost.

**Reducing Lag:**
- Ensure sufficient network bandwidth
- Monitor primary CPU and disk I/O
- Consider dedicated replication network
- Use synchronous replication (`WAIT` command) for critical writes

---

## See Also

- [CLUSTER.md](CLUSTER.md) - Cluster topology, failover, and slot migration
- [CONSISTENCY.md](CONSISTENCY.md) - Consistency guarantees with replication
- [PERSISTENCE.md](PERSISTENCE.md) - WAL and snapshot details
- [FAILURE_MODES.md](FAILURE_MODES.md) - General failure handling and recovery

### External References

- [Redis Replication](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/)
- [Valkey Replication](https://valkey.io/topics/replication/)
- [RocksDB Replication Helpers](https://github.com/facebook/rocksdb/wiki/Replication-Helpers)
