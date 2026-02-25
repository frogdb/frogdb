# unit/pause — 6 errors

## Category
CLIENT PAUSE behavior: precedence, time preservation, write-pause mode.

## Failures

### Pause precedence — old pause-all over new pause-write (~1 error)
- **Expected:** If an existing pause-all is active, a new pause-write should not downgrade it — the pause-all takes precedence
- **Actual:** New pause-write overrides the existing pause-all
- **Root cause:** `CLIENT PAUSE` implementation replaces the current pause state rather than checking precedence
- **Fix:** When processing a new CLIENT PAUSE, if existing mode is ALL and new mode is WRITE, keep the ALL mode. Only upgrade (WRITE→ALL) or extend (same mode, longer time).

### Pause time preservation — smaller time should not override larger (~1 error)
- **Expected:** `CLIENT PAUSE 10000` followed by `CLIENT PAUSE 100` keeps the 10000ms pause
- **Actual:** The second call overrides with 100ms
- **Fix:** Keep the maximum of old and new pause times when the modes are compatible

### Write commands not correctly paused by WRITE mode (~2 errors)
- **Expected:** In WRITE pause mode, write commands are blocked while read commands proceed
- **Actual:** Write commands may not be correctly identified or paused
- **Root cause:** The command flag checking for write vs. read may not align with Redis's definition of "write commands" during pause
- **Fix:** Ensure the pause check uses `CommandFlags::WRITE` correctly and that all write commands are blocked

### Cascading timeout failures (~2 errors)
- **Expected:** Remaining tests depend on correct pause behavior
- **Actual:** Timeout or wrong results due to earlier pause behavior bugs
- **Note:** These should resolve once the above fixes are applied

## Source Files

| File | What to change |
|------|----------------|
| `crates/server/src/connection/handlers/client.rs` | CLIENT PAUSE/UNPAUSE handler |
| `crates/server/src/connection/mod.rs` | Pause state management, command dispatch pause check |
| `crates/core/src/types.rs` | `PauseMode` enum and precedence logic |

## Recommended Fix Order
1. Fix pause precedence (ALL > WRITE) (1 error)
2. Fix pause time preservation (max of old/new) (1 error)
3. Fix write command identification in WRITE mode (2 errors)
4. Remaining cascading errors should resolve (2 errors)

## Cross-Suite Dependencies
- Blocked client tracking: the pause tests may also depend on `blocked_clients` visibility (shared issue with `unit/type/zset`, `unit/slowlog`, `unit/multi`)
