# 14c. Debug Commands & Expired Key Scanning (3 tests — `hyperloglog_tcl.rs`, `scan_tcl.rs`)

**Status**: Partial (PFDEBUG GETREG exists, DEBUG SET-ACTIVE-EXPIRE does not)
**Scope**: Verify PFDEBUG GETREG works correctly, implement DEBUG SET-ACTIVE-EXPIRE equivalent
for testing expired key visibility in SCAN.

## Architecture

### PFDEBUG GETREG

The `PFDEBUG` command already exists in `frogdb-server/crates/commands/src/hyperloglog.rs` with
subcommands ENCODING, DECODE, and GETREG. The `GETREG` subcommand returns an array of raw HLL
register values (16384 integers). The existing implementation iterates over all registers and
returns `Response::Array` of `Response::Integer` values.

**Current state**: Implemented. Need to verify the test passes or identify any format mismatch.

### DEBUG SET-ACTIVE-EXPIRE

Redis's `DEBUG SET-ACTIVE-EXPIRE 0` disables background expiration so tests can verify that
expired-but-not-yet-evicted keys appear (or don't appear) in SCAN results. FrogDB's active
expiration runs on shard ticks.

The debug handler is in `frogdb-server/crates/server/src/connection/handlers/debug.rs`. The
dispatch infrastructure already exists — need to add a `SET-ACTIVE-EXPIRE` subcommand that
toggles a flag on the shard's expiration subsystem.

**Key files**:
- `frogdb-server/crates/server/src/connection/handlers/debug.rs` — add subcommand
- `frogdb-server/crates/server/src/connection/dispatch.rs` — references `active_expire`
- `frogdb-server/crates/commands/src/scan.rs` — SCAN command (needs to filter expired keys)

### SCAN with Expired Keys

The `{$type} SCAN with expired keys` test sets keys with TTL, disables active expiration, waits
for expiry time to pass, then verifies SCAN behavior. The test validates that SCAN does NOT
return logically-expired keys (lazy expiration during SCAN iteration).

## Implementation Steps

1. **PFDEBUG GETREG**: Run the existing test to verify it passes. If it fails, check:
   - Register count (must be exactly 16384)
   - Register value range (0-63 for 6-bit registers)
   - Array response format

2. **DEBUG SET-ACTIVE-EXPIRE**:
   - Add a `set_active_expire_enabled` flag to the shard/expiration subsystem
   - Add `SET-ACTIVE-EXPIRE` subcommand to debug handler
   - When disabled, skip periodic expiration sweeps but still do lazy expiration on access

3. **SCAN expired key filtering**:
   - Ensure SCAN checks TTL and skips expired keys during iteration
   - This may already work via lazy expiration — verify behavior

## Integration Points

- `frogdb-server/crates/server/src/connection/handlers/debug.rs` — debug subcommand dispatch
- Shard tick/expiration loop — needs a disable flag
- `frogdb-server/crates/commands/src/scan.rs` — expired key filtering during iteration
- `frogdb-server/crates/commands/src/hyperloglog.rs` — PFDEBUG GETREG

## FrogDB Adaptations

- **Active expiration**: FrogDB's expiration is shard-tick-driven rather than Redis's `hz`-based
  sampling. The disable flag should suppress the shard tick expiration sweep.
- **Lazy expiration on SCAN**: FrogDB should already skip expired keys during SCAN iteration
  (check if `is_expired()` is called per key during scan).

## Tests

- `PFDEBUG GETREG returns the HyperLogLog raw registers`
- `{$type} SCAN with expired keys`
- `SCAN with expired keys`

## Verification

```bash
just test frogdb-server-redis-regression "PFDEBUG GETREG"
just test frogdb-server-redis-regression "SCAN with expired"
```
