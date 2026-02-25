# unit/querybuf — 4 errors

## Category
Query buffer management: CLIENT LIST field reporting and buffer resize behavior.

## Failures

### CLIENT LIST `qbuf` field reports 0 (~2 errors)
- **Expected:** `CLIENT LIST` includes `qbuf=<N>` and `qbuf-free=<M>` showing the current query buffer size and free space
- **Actual:** `qbuf=0 qbuf-free=0` because FrogDB doesn't track query buffer sizes
- **Root cause:** The CLIENT LIST formatting doesn't report actual buffer allocation — the query buffer size is not exposed from the connection's read buffer
- **Fix:** Track the tokio read buffer size (or the RESP parser's internal buffer) and expose it via CLIENT LIST

### Query buffer resize timing assertions (~2 errors)
- **Expected:** After sending large commands, the query buffer grows; after idle time, it shrinks back
- **Actual:** The buffer size is always reported as 0, so growth/shrink assertions fail
- **Root cause:** Same as above — no query buffer tracking
- **Fix:** Tied to the qbuf tracking implementation

## Source Files

| File | What to change |
|------|----------------|
| `crates/server/src/connection/handlers/client.rs` | CLIENT LIST format — include qbuf/qbuf-free |
| `crates/server/src/connection/mod.rs` | Track query buffer size from the read path |
| `crates/server/src/connection/state.rs` | Add qbuf tracking fields to connection state |

## Recommended Fix Order
1. Add query buffer size tracking to connection state
2. Expose qbuf/qbuf-free in CLIENT LIST output
3. Implement buffer resize/shrink behavior (to match Redis's adaptive buffer sizing)

## Cross-Suite Dependencies
None — query buffer tracking is self-contained.
