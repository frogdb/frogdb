# unit/info-command — 6 errors

## Category
COMMAND INFO metadata mismatches and INFO output format.

## Failures

### INFO output has duplicate sections (~2 errors)
- **Expected:** Each INFO section key appears exactly once
- **Actual:** Some keys appear twice (e.g., `used_cpu_user_children`), causing the test to detect unexpected duplicates
- **Root cause:** Multiple code paths may append the same section key to the INFO output
- **Fix:** Audit INFO generation to deduplicate section entries

### COMMAND INFO response format (~4 errors)
- **Expected:** `COMMAND INFO <cmd>` returns an array with command metadata matching Redis's schema: `[name, arity, flags, first_key, last_key, step]`
- **Actual:** Response format differs — may have wrong arity values, missing flags, or incorrect key position metadata
- **Root cause:** The command metadata returned by COMMAND INFO is generated from the `Command` trait implementations, which may not match Redis's exact values for all commands
- **Fix:** Audit COMMAND INFO output for specific commands tested in the suite and align arity, flags, and key-spec fields with Redis 7.2.4

## Source Files

| File | What to change |
|------|----------------|
| `crates/commands/src/basic.rs` | COMMAND INFO response generation |
| `crates/server/src/connection/handlers/info.rs` | INFO output — deduplicate sections |
| `crates/server/src/commands/registry.rs` | Command metadata registration (arity, flags, key positions) |

## Recommended Fix Order
1. Fix INFO duplicate sections (2 errors)
2. Audit COMMAND INFO metadata for tested commands (4 errors)

## Cross-Suite Dependencies
None directly — INFO format is tested only in this suite.
