---
title: "Replication Internals"
description: "Contributor-facing documentation for FrogDB's primary-replica replication system: WAL streaming internals, PSYNC protocol details, and the replication state ..."
sidebar:
  order: 7
---
Contributor-facing documentation for FrogDB's primary-replica replication system: WAL streaming internals, PSYNC protocol details, and the replication state machine.

For operator-facing setup and failover procedures, see [Operations: Replication](/operations/replication/).

---

## Architecture

### Data Flow

```
Primary: Write -> In-Memory -> WAL -> Replication Stream -> Replicas
                                |
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
    |
    +-- Replica B (tracking abc123...)
    |
    [Primary A fails, B promoted]
    |
Primary B (repl_id: def456..., secondary_id: abc123...)
```

This allows replicas of A to connect to B and continue incrementally.

### Replication ID Generation

```rust
fn generate_replication_id() -> String {
    let mut bytes = [0u8; 20];
    getrandom::getrandom(&mut bytes).expect("random bytes");
    hex::encode(bytes)  // 20 random bytes -> 40 hex characters
}
```

### Secondary ID Usage

When a replica is promoted:
```rust
fn on_promotion(&mut self) {
    self.secondary_repl_id = Some(self.repl_id.clone());
    self.secondary_repl_offset = self.repl_offset;
    self.repl_id = generate_replication_id();
}
```

Other replicas can PSYNC using either the new primary's ID or the secondary ID.

---

## Full Synchronization (FULLRESYNC)

```
Primary                              Replica
   |                                    |
   |<---- PSYNC ? -1 ------------------| "I have no data"
   |                                    |
   |  Create RocksDB checkpoint         |
   |                                    |
   |----- FULLRESYNC <id> <seq> ------->|
   |                                    |
   |----- [checkpoint files] ---------->| Transfer checkpoint
   |                                    |
   |                                    | Load checkpoint
   |                                    |
   |<---- PSYNC <id> <seq> ------------|  Ready for incremental
   |                                    |
   |----- [WAL stream] --------------->|  Continue streaming
```

### Concurrent FULLRESYNC Requests

If multiple replicas request FULLRESYNC simultaneously, FrogDB creates a **single RocksDB checkpoint** and streams it to all requesting replicas, amortizing the cost.

### Checkpoint Transfer Format

```
1. FULLRESYNC response: +FULLRESYNC <repl_id> <sequence>\r\n
2. Checkpoint header: $<total_size>\r\n
3. File entries (repeated):
   *3\r\n
   $<name_len>\r\n<filename>\r\n
   :<file_size>\r\n
   $<file_size>\r\n<file_data>\r\n
4. End marker: *1\r\n $3\r\nEOF\r\n
5. Checksum footer: $40\r\n<sha256_hex_of_all_files>\r\n
```

**Checkpoint Integrity:** Primary computes SHA256 of all file contents. Replica verifies after receiving all files. Mismatch triggers retry.

---

## PSYNC Handshake Sequence

```
Replica                              Primary
   |                                    |
   |----- AUTH [user] <password> ------>| (if primary_auth configured)
   |<---- +OK -------------------------|
   |                                    |
   |----- REPLCONF listening-port 6379->| (optional, for INFO output)
   |<---- +OK -------------------------|
   |                                    |
   |----- REPLCONF capa eof psync2 --->| (announce capabilities)
   |<---- +OK -------------------------|
   |                                    |
   |----- PSYNC <repl_id> <seq> ------>| (initiate sync)
   |<---- +FULLRESYNC or +CONTINUE ----|
```

---

## WAL Streaming

After initial sync, the primary continuously streams WAL entries to replicas. FrogDB uses RocksDB's `GetUpdatesSince()` for incremental sync.

### TCP Backpressure Design

FrogDB uses **TCP backpressure** (no buffer limits) to avoid the "full sync loop" problem that affects Redis:

- If a replica is slow, TCP send buffer fills naturally
- Primary blocks on write (does not buffer unboundedly)
- No risk of exhausting memory with replication buffers
- Slow replicas cause primary to slow down rather than trigger full resync

This is a deliberate departure from Redis, which uses bounded output buffers and disconnects slow replicas, sometimes causing repeated full resyncs.

---

## Synchronous Replication

When `min_replicas_to_write >= 1`, writes wait for replica acknowledgment:

```rust
async fn execute_with_sync_replication(
    cmd: &ParsedCommand,
    handler: &dyn Command,
    ctx: &mut CommandContext<'_>,
) -> Response {
    let result = handler.execute(ctx, &cmd.args);
    let seq = ctx.wal.append(&result.operation)?;

    if ctx.config.min_replicas_to_write == 0 {
        return result.response;  // Async mode
    }

    let ack_future = ctx.replication_tracker.wait_for_acks(
        seq,
        ctx.config.min_replicas_to_write,
    );

    match tokio::time::timeout(ctx.config.replica_ack_timeout, ack_future).await {
        Ok(Ok(_)) => result.response,
        Ok(Err(_)) | Err(_) => {
            // Write ALREADY committed on primary
            Response::Error(Bytes::from_static(b"NOREPL Not enough replicas"))
        }
    }
}
```

When `-NOREPL` is returned, the write has already succeeded on the primary and is in the WAL. The error indicates replication durability was not confirmed, not that the write failed.

---

## Replication State Machine

### Node Roles

```rust
pub enum NodeRole {
    Primary,
    Replica { primary_addr: SocketAddr },
    Standalone,
}
```

### Replica Command Handling

| Command Type | Replica Behavior |
|--------------|------------------|
| Read (`READONLY` flag) | Execute locally, may return stale data |
| Write (`WRITE` flag) | Return `-READONLY` error |
| Replication commands | Execute (PSYNC, REPLCONF, etc.) |
| Admin commands | Depends on command |

### Replication Metrics

| Metric | Description |
|--------|-------------|
| `last_write_seq` | Sequence number of last write |
| `pending_sync_writes` | Writes waiting for replica ACK |
| `norepl_errors` | Writes that failed due to replica timeout |
| `replica_ack_wait_time` | Time spent waiting for replica ACKs |
