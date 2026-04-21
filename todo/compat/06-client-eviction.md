# 6. Client Eviction (`maxmemory-clients`)

**Tests**: 22 (15 from `client_eviction_tcl.rs`, 7 from `maxmemory_tcl.rs`)
**Source files**: `frogdb-server/crates/redis-regression/tests/client_eviction_tcl.rs`, `frogdb-server/crates/redis-regression/tests/maxmemory_tcl.rs`
**Status**: Not implemented. `CLIENT NO-EVICT` flag exists (`ClientFlags::NO_EVICT` in `client_registry/mod.rs`) and is settable via `CLIENT NO-EVICT ON|OFF` (handler in `connection/handlers/client.rs:520`), but no eviction logic consumes it. `CLIENT LIST` reports hardcoded zeroes for `qbuf`, `argv-mem`, `multi-mem`, `obl`, `oll`, `omem`, `tot-mem` (see `client_registry/info.rs:79`).

---

## Architecture

### Per-Client Memory Accounting

Redis tracks seven memory contributors per client. FrogDB must track the same:

| Contributor | Where it grows | Current FrogDB state |
|---|---|---|
| Query buffer (`qbuf`) | Codec read buffer in `Framed<ConnectionStream, FrogDbResp2>` | Not tracked. The `BytesMut` inside `tokio_util::codec::FramedRead` holds unprocessed input. |
| Argv memory (`argv-mem`) | Parsed command args held during execution | Not tracked. `ParsedCommand.args: Vec<Bytes>` exists transiently in the dispatch loop. |
| Multi buffer (`multi-mem`) | `TransactionState.queue: Option<Vec<ParsedCommand>>` | Queue length tracked (`multi_queue_len`) but byte size not computed. |
| Output buffer length (`obl`) | tokio write buffer + `resp3_buf: BytesMut` | Not tracked. |
| Output list length/memory (`oll`, `omem`) | Pending pub/sub messages in `mpsc::UnboundedReceiver` + pending invalidation messages | Not tracked. |
| Watched keys | `TransactionState.watches: HashMap<Bytes, (usize, u64)>` | Present but size not reported. |
| Pub/Sub subscriptions | `PubSubState.subscriptions/patterns/sharded_subscriptions` | Counts tracked, byte cost not. |
| Tracking prefixes | `TrackingState.prefixes: Vec<Bytes>` | Present but size not reported. |

The total (`tot-mem`) is the sum of all contributors plus a fixed per-connection overhead (struct sizes, channel buffers, TLS state if applicable).

### Eviction Loop Design

Client eviction is separate from key eviction. It runs at the **connection acceptor level** (not inside shard workers), because:
- Client memory is connection-local, not owned by any shard.
- The eviction decision needs a global view of all clients (via `ClientRegistry`).
- Key eviction (`ShardWorker::check_memory_for_write`) only considers per-shard store memory.

The eviction loop:
1. After every command dispatch (or on a timer), check if aggregate client memory exceeds `maxmemory-clients`.
2. If over limit, sort evictable clients by `tot-mem` descending.
3. Kill the largest client (send kill signal via `kill_tx`). Skip clients with `NO_EVICT` flag, master/replica connections.
4. Repeat until aggregate is below limit or no evictable clients remain.

Redis performs this check in `beforeSleep()` (event loop callback). FrogDB equivalent: a periodic async task or inline check in the connection dispatch loop.

### Interaction with Key Eviction

- `maxmemory-clients` is independent of `maxmemory`. Client memory does NOT count toward key eviction limits.
- However, when `maxmemory-clients` is a percentage (e.g., `5%`), it is resolved as a percentage of `maxmemory`.
- The `maxmemory_tcl.rs` tests verify that when `maxmemory-clients` is enabled, bloated client buffers trigger client eviction INSTEAD of key eviction, preventing key data loss from slow consumers.

---

## Implementation Steps

### Step 1: Add `maxmemory-clients` Config

**Files**:
- `frogdb-server/crates/config/src/memory.rs` — Add `maxmemory_clients: String` field (supports `"0"` disabled, `"100mb"` absolute, `"5%"` percentage).
- `frogdb-server/crates/config/src/params.rs` — Add `ConfigParamInfo` entry for `maxmemory-clients` (mutable, section `memory`).
- `frogdb-server/crates/server/src/runtime_config.rs` — Wire up `CONFIG GET/SET maxmemory-clients` with hot-reload support.

Parsing: accept bytes (raw number or with `kb`/`mb`/`gb` suffix) or a percentage string like `"5%"`. Zero or empty string disables.

### Step 2: Per-Client Memory Tracking in `ClientEntry`

**File**: `frogdb-server/crates/core/src/client_registry/mod.rs`

Add to `ClientEntry`:
```rust
/// Per-client memory usage breakdown (updated by connection handler).
query_buf_size: usize,
argv_mem: usize,
multi_mem: usize,
output_buf_len: usize,      // obl: bytes in write buffer
output_list_len: usize,     // oll: messages pending in channel
output_list_mem: usize,     // omem: bytes in output list
watched_keys_mem: usize,
subscriptions_mem: usize,
tracking_prefixes_mem: usize,
```

Add methods to `ClientRegistry`:
```rust
/// Update memory usage for a client.
pub fn update_memory(&self, id: u64, mem: ClientMemoryUpdate) { ... }

/// Get aggregate client memory across all connections.
pub fn total_client_memory(&self) -> u64 { ... }

/// Get clients sorted by total memory descending, excluding NO_EVICT.
pub fn eviction_candidates(&self) -> Vec<(u64, u64)> { ... } // (id, tot_mem)
```

### Step 3: Report Memory in `CLIENT LIST`

**File**: `frogdb-server/crates/core/src/client_registry/info.rs`

Replace hardcoded zeroes in `to_client_list_entry()` with actual values from the `ClientEntry` memory fields. Compute `tot-mem` as sum of all contributors plus a fixed `CLIENT_BASE_OVERHEAD` constant (size of `ConnectionHandler` + `ConnectionState` + channels).

### Step 4: Connection Handler Reports Memory Periodically

**File**: `frogdb-server/crates/server/src/connection/lifecycle.rs`

Extend the existing `maybe_sync_stats()` mechanism to also sync memory usage:

```rust
pub(super) fn compute_client_memory(&self) -> ClientMemoryUpdate {
    ClientMemoryUpdate {
        // Query buffer: access inner BytesMut length from Framed codec
        query_buf_size: self.framed.read_buffer().len(),
        // Argv: 0 between commands, computed during execution
        argv_mem: 0,
        // Multi buffer: sum of serialized sizes of queued commands
        multi_mem: self.state.transaction.queue.as_ref()
            .map(|q| q.iter().map(|cmd| cmd.estimated_memory()).sum())
            .unwrap_or(0),
        // Output buffer
        output_buf_len: self.resp3_buf.len(),
        // Output list (pub/sub + invalidation channel lengths)
        output_list_len: /* channel len */ 0,
        output_list_mem: /* estimated */ 0,
        // Watched keys
        watched_keys_mem: self.state.transaction.watches.keys()
            .map(|k| k.len() + 48 /* HashMap entry overhead */).sum(),
        // Subscriptions
        subscriptions_mem: self.state.pubsub.subscriptions.iter()
            .chain(self.state.pubsub.patterns.iter())
            .chain(self.state.pubsub.sharded_subscriptions.iter())
            .map(|b| b.len() + 48).sum(),
        // Tracking prefixes
        tracking_prefixes_mem: self.state.tracking.prefixes.iter()
            .map(|b| b.len() + 24).sum(),
    }
}
```

Call `compute_client_memory()` alongside `maybe_sync_stats()` in the dispatch loop. For argv memory, wrap the command execution to capture it transiently.

### Step 5: Client Eviction Task

**File**: New logic in `frogdb-server/crates/server/src/server/` (e.g., add to the server's background task set, or add a method to the accept loop).

Design options:
- **Option A (preferred)**: Inline check after each command in the connection dispatch loop. Each connection checks `total_client_memory() > limit` and, if true, calls into `ClientRegistry::try_evict_clients()`. Use a shared `AtomicU64` for aggregate memory to avoid lock contention on every command.
- **Option B**: Dedicated background tokio task polling every N ms.

Algorithm in `ClientRegistry`:
```rust
pub fn try_evict_clients(&self, limit: u64) -> usize {
    let total = self.total_client_memory();
    if total <= limit { return 0; }

    let mut evicted = 0;
    let candidates = self.eviction_candidates(); // sorted largest-first

    for (id, _mem) in candidates {
        if self.total_client_memory() <= limit { break; }
        // Send kill signal
        self.kill_by_id(id);
        evicted += 1;
    }
    evicted
}
```

The kill signal is already plumbed: `kill_tx.send(true)` causes `ClientHandle::is_killed()` to return true, and the connection dispatch loop checks this to break.

### Step 6: Respect `CLIENT NO-EVICT`

Already partially implemented. In `eviction_candidates()`, filter out clients where `entry.flags.contains(ClientFlags::NO_EVICT)` or `entry.flags.contains(ClientFlags::MASTER)` or `entry.flags.contains(ClientFlags::REPLICA)`.

### Step 7: Interaction with Output Buffer Limits

**Test**: `avoid client eviction when client is freed by output buffer limit`

When a client exceeds output buffer limits (hard limit), it is disconnected immediately. The client eviction loop must check whether the client is still alive before counting it toward the aggregate. Use the existing `kill_tx` closed state or a `disconnecting` flag.

Sequence: output buffer limit fires -> connection is dropped -> `ClientHandle::drop` calls `unregister()` -> memory automatically removed from aggregate. The eviction loop just needs to re-check `total_client_memory()` after each eviction to account for concurrent disconnections.

### Step 8: `CONFIG SET maxmemory-clients` Triggers Immediate Check

**Test**: `decrease maxmemory-clients causes client eviction`

When `CONFIG SET maxmemory-clients` reduces the limit, immediately run the eviction loop. Add a hook in `runtime_config.rs`'s CONFIG SET handler that calls `try_evict_clients(new_limit)` after updating the config.

---

## Integration Points

| Operation | Hook Location | What to track |
|---|---|---|
| Query buffer growth | `FrogDbResp2::decode()` — after each decode, buffer size changes | `query_buf_size` |
| Command args during execution | `route_and_execute_with_transaction()` — measure args before dispatch | `argv_mem` (transient) |
| MULTI queue growth | `TransactionState` push to queue | `multi_mem` |
| Output writes | `send_response()` / `feed_response()` — track resp3_buf growth | `output_buf_len` |
| Pub/Sub message delivery | `mpsc::UnboundedSender::send()` for pub/sub messages | `output_list_len`, `output_list_mem` |
| SUBSCRIBE / PSUBSCRIBE / SSUBSCRIBE | `connection/handlers/pubsub.rs` subscribe handlers | `subscriptions_mem` |
| WATCH | `connection/handlers/transaction.rs` WATCH handler | `watched_keys_mem` |
| CLIENT TRACKING ON with BCAST PREFIX | `connection/handlers/client.rs` tracking handler | `tracking_prefixes_mem` |

---

## FrogDB Adaptations

### Client Eviction and Raft

FrogDB uses Raft for cluster topology changes but NOT for per-client operations. Client connections are node-local (not replicated). Therefore:

- **No Raft interaction for client eviction.** Evicting a client is a local decision that does not need consensus.
- **Eviction during active Raft write**: If a client is killed while its command is in-flight through Raft consensus, the Raft command will still commit. The response is simply never delivered (connection closed). This is safe because:
  - Raft guarantees the write is durable once committed.
  - The client will reconnect and observe the committed state.
- **Replica connections**: Clients with `MASTER` or `REPLICA` flags must NEVER be evicted. Filter these out in `eviction_candidates()`.

### Aggregate Memory Tracking Without Per-Shard Allocation

Unlike key eviction which splits `maxmemory` across shards (`maxmemory / num_shards`), client memory is tracked globally. Use a shared `AtomicU64` for the aggregate to avoid expensive summation on every check:
- Each connection atomically adds/subtracts its delta when syncing memory stats.
- The eviction check reads the atomic — O(1) with no lock.

### Output Buffer Accounting for Unbounded Channels

FrogDB uses `mpsc::UnboundedChannel` for pub/sub delivery (unlike Redis's linked-list output buffer). This means:
- Cannot directly measure bytes in the channel without wrapper.
- Solution: maintain a per-client `AtomicUsize` counter incremented on send, decremented on receive. Each message adds its estimated serialized size.
- Alternative: use a bounded channel with explicit capacity tracking (larger refactor, consider for later).

---

## Tests

### client_eviction_tcl.rs (15 tests)

- `client evicted due to large argv`
- `client evicted due to large query buf`
- `client evicted due to large multi buf`
- `client evicted due to percentage of maxmemory`
- `client evicted due to watched key list`
- `client evicted due to pubsub subscriptions`
- `client evicted due to tracking redirection`
- `client evicted due to client tracking prefixes`
- `client evicted due to output buf`
- `client no-evict $no_evict`
- `avoid client eviction when client is freed by output buffer limit`
- `decrease maxmemory-clients causes client eviction`
- `evict clients only until below limit`
- `evict clients in right order (large to small)`
- `client total memory grows during $type`

### maxmemory_tcl.rs (7 tests)

- `eviction due to output buffers of many MGET clients, client eviction: false`
- `eviction due to output buffers of many MGET clients, client eviction: true`
- `eviction due to input buffer of a dead client, client eviction: false`
- `eviction due to input buffer of a dead client, client eviction: true`
- `eviction due to output buffers of pubsub, client eviction: false`
- `eviction due to output buffers of pubsub, client eviction: true`
- `client tracking don't cause eviction feedback loop` — also requires RESP3 (`HELLO 3`)

---

## Verification

1. **Unit tests** (in `client_registry/mod.rs`):
   - `test_memory_update_and_total` — verify `update_memory` / `total_client_memory` arithmetic.
   - `test_eviction_candidates_ordering` — verify largest-first sort, NO_EVICT exclusion.
   - `test_evict_until_below_limit` — verify loop stops once under threshold.

2. **Integration tests** (in `frogdb-server/crates/server/tests/`):
   - Connect multiple clients, inflate their buffers with large MGET / SUBSCRIBE, verify CONFIG SET triggers eviction.
   - Verify CLIENT NO-EVICT ON protects a client.
   - Verify master/replica connections are never evicted.

3. **Regression tests**: Run `just test frogdb-server client_eviction` and `just test frogdb-server maxmemory` — all 22 tests should pass after implementation.

4. **Manual smoke test**:
   ```bash
   # Start server with low client memory limit
   frogdb-server --maxmemory-clients 1mb

   # Connect clients and bloat buffers
   redis-cli SET bigkey $(python3 -c "print('x'*500000)")
   redis-cli SUBSCRIBE ch  # in another terminal, publish large messages

   # Observe eviction in logs and CLIENT LIST
   redis-cli CLIENT LIST
   redis-cli INFO clients  # check mem_clients_normal field
   ```
