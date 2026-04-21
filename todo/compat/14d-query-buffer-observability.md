# 14d. Query Buffer Observability (3 tests — `querybuf_tcl.rs`)

**Status**: Hardcoded zeros
**Scope**: Implement real `qbuf`, `qbuf-free`, and `argv-mem` fields in CLIENT LIST output,
plus a mechanism to pause background tasks for deterministic testing.

## Architecture

### Current State

The CLIENT LIST output is generated in
`frogdb-server/crates/core/src/client_registry/info.rs`. Currently, the `to_client_list_entry()`
method hardcodes `qbuf=0 qbuf-free=0 argv-mem=0`:

```
"... qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 obl=0 oll=0 omem=0 tot-mem=0 ..."
```

### What These Fields Mean

- **`qbuf`**: Current size of the client's input query buffer (bytes currently buffered but not
  yet parsed)
- **`qbuf-free`**: Free space in the allocated query buffer (allocated - used)
- **`argv-mem`**: Memory used by the parsed command arguments currently being processed
- **`tot-mem`**: Total memory usage for this client (qbuf + output buffers + overhead)

### Query Buffer Resize Tests

The Redis tests verify that:
1. `query buffer resized correctly` — buffer shrinks after a large command is processed
2. `query buffer resized correctly when not idle` — buffer behavior during active command stream
3. `query buffer resized correctly with fat argv` — large argument vectors are tracked

These tests use `DEBUG PAUSE-CRON` (or equivalent) to freeze background tasks so buffer sizes
can be measured deterministically.

### Implementation Approach

FrogDB uses tokio for async I/O. The "query buffer" is the read buffer on each connection's
codec. To expose real values:

1. Track read buffer capacity/usage in the connection state
2. Track parsed argument memory in command context
3. Expose these via `ClientInfo` fields
4. Format them in `to_client_list_entry()`

## Implementation Steps

1. Add `query_buffer_len`, `query_buffer_free`, `argv_mem` fields to `ClientInfo`
2. Update the connection's codec to report buffer metrics to `ClientInfo`
3. Compute `tot-mem` as sum of input buffer + output buffer + overhead
4. Replace hardcoded zeros in `to_client_list_entry()` format string
5. Implement `DEBUG PAUSE-CRON` equivalent (pause shard background ticks)
6. Port the three querybuf tests

## Integration Points

- `frogdb-server/crates/core/src/client_registry/info.rs` — `ClientInfo` struct, formatting
- `frogdb-server/crates/server/src/connection/` — codec buffer access
- `frogdb-server/crates/server/src/connection/handlers/debug.rs` — PAUSE-CRON subcommand
- `frogdb-server/crates/server/src/connection/handlers/client.rs` — CLIENT LIST handler

## FrogDB Adaptations

- **Buffer model**: Redis uses a single contiguous buffer per client. FrogDB's tokio codec may
  use `BytesMut` with different allocation semantics. Report the actual `BytesMut` capacity and
  remaining bytes.
- **Background task pausing**: Instead of Redis's `PAUSE-CRON`, pause the shard tick timer.
  This serves the same purpose — preventing background buffer shrinking during measurement.
- **tot-mem calculation**: Adapt to FrogDB's actual per-connection memory footprint (connection
  state struct size + buffers + pending responses).

## Tests

- `query buffer resized correctly`
- `query buffer resized correctly when not idle`
- `query buffer resized correctly with fat argv`

## Verification

```bash
just test frogdb-server-redis-regression "query buffer resized"
```
