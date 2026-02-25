# unit/multi — 5 errors

## Category
MULTI/EXEC edge cases: dirty key tracking and CONFIG SET support.

## Failures

### FLUSHALL/FLUSHDB inside MULTI — dirty tracking (~3 errors)
- **Expected:** After `MULTI; FLUSHALL; EXEC`, the EXEC response contains an `OK` for the FLUSHALL, and the dirty key counter is properly updated
- **Actual:** Returns `PONG` instead of the expected response for FLUSHALL/FLUSHDB within a transaction
- **Root cause:** FLUSHALL/FLUSHDB may be handled via a special code path that doesn't correctly enqueue the response when inside a MULTI block
- **Fix:** Ensure FLUSHALL and FLUSHDB are properly queued and their responses are correctly collected during EXEC

### CONFIG SET lua-time-limit not supported (~2 errors, cascading)
- **Expected:** `CONFIG SET lua-time-limit <value>` is accepted
- **Actual:** CONFIG SET doesn't recognize `lua-time-limit` as a valid parameter
- **Root cause:** The CONFIG SET handler doesn't include `lua-time-limit` in its parameter map
- **Fix:** Add `lua-time-limit` to the CONFIG SET/GET parameter handling. This parameter controls the maximum execution time for Lua scripts before they can be killed.
- **Cascade:** The test that sets lua-time-limit then tests MULTI + script timeout interaction fails because the config can't be set

## Source Files

| File | What to change |
|------|----------------|
| `crates/server/src/connection/handlers/transaction.rs` | EXEC response collection for FLUSHALL/FLUSHDB |
| `crates/server/src/connection/handlers/admin.rs` | CONFIG SET/GET `lua-time-limit` |
| `crates/server/src/config.rs` | Add `lua_time_limit` to config manager |

## Recommended Fix Order
1. Fix FLUSHALL/FLUSHDB response in MULTI (3 errors)
2. Add CONFIG SET/GET `lua-time-limit` (2 errors)

## Cross-Suite Dependencies
- Blocked client tracking may also affect some MULTI tests (shared issue)
- `lua-time-limit` config is also relevant to `unit/scripting` (currently skipped)
