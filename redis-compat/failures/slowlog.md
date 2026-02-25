# unit/slowlog — 14 errors

## Category
SLOWLOG implementation gaps: entry limits, argument trimming, blocked client timeouts.

## Failures

### Max entries not correctly limited (~2 errors)
- **Expected:** `CONFIG SET slowlog-max-len N` limits the number of entries returned by `SLOWLOG GET`
- **Actual:** Always returns up to 10 entries regardless of configured max
- **Root cause:** The `SlowLog` struct may not enforce the max-len config when adding entries, or the scatter-gather across shards doesn't respect the per-shard limit
- **Fix:** Ensure `SlowLog::add()` evicts oldest entries when exceeding `slowlog-max-len`, and the scatter-gather in `handle_slowlog_get()` correctly aggregates

### SLOWLOG GET -1 doesn't return all entries (~1 error)
- **Expected:** `SLOWLOG GET -1` returns all recorded entries
- **Actual:** The -1 handling works in the handler (line 52: `usize::MAX`), but the underlying shard may not return all entries
- **Fix:** Verify that `ShardMessage::SlowlogGet { count: usize::MAX }` propagates correctly

### Argument trimming not implemented (~4 errors)
- **Expected:** Long arguments are truncated to `slowlog-max-arg-len` bytes with `"... (N more bytes)"` suffix; commands with more than 32 arguments show only 32 with `"... (N more arguments)"` appended
- **Actual:** `SlowLog::truncate_args()` exists but may not be fully implementing Redis's trimming behavior
- **Root cause:** Need to verify `truncate_args()` matches Redis's exact format: truncate individual args to `slowlog-max-arg-len`, and cap total args at 32
- **Fix:** Audit `SlowLog::truncate_args()` against Redis's `slowlogCreateEntry()` behavior

### SLOWLOG LEN count discrepancy (~2 errors)
- **Expected:** `SLOWLOG LEN` returns the total number of entries across all shards
- **Actual:** Count is off — may be related to entries being added during the scatter-gather window, or max-len not being enforced properly
- **Fix:** Tied to the max-entries fix above

### Rewritten commands logging — timeout (~3 errors)
- **Expected:** Tests that use blocked clients (BLPOP, etc.) check that rewritten commands are logged in SLOWLOG
- **Actual:** Timeout waiting for blocked clients to become visible
- **Root cause:** FrogDB doesn't expose `blocked_clients` count in INFO, so tests that poll for blocked state time out
- **Note:** This is a cross-suite blocked-client-tracking issue

### Blocking command reporting — timeout (~2 errors)
- **Expected:** Tests verify that blocking commands appear in SLOWLOG after they complete
- **Actual:** Timeout waiting for blocked client state
- **Note:** Same cross-suite issue as above

## Source Files

| File | What to change |
|------|----------------|
| `crates/server/src/connection/handlers/slowlog.rs` | SLOWLOG GET/LEN handler logic |
| `crates/core/src/slowlog.rs` | `SlowLog` struct — entry limits, `truncate_args()` |
| `crates/server/src/shard.rs` | Shard-side slowlog storage and max-len enforcement |
| `crates/server/src/connection/handlers/admin.rs` | CONFIG SET `slowlog-max-len` propagation |

## Recommended Fix Order
1. Fix max-entries enforcement in `SlowLog::add()` (2 errors)
2. Audit and fix `truncate_args()` (4 errors)
3. Fix SLOWLOG LEN aggregation (2 errors)
4. Blocked client tracking is a separate cross-suite effort (5 errors)

## Cross-Suite Dependencies
- Blocked client tracking: shared with `unit/type/zset`, `unit/pause`, `unit/multi`
- CONFIG SET `slowlog-max-len` / `slowlog-log-slower-than` propagation shared with admin handler
