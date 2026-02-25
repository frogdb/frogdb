# unit/expire — 15 errors

## Category
Expiry edge cases: missing NX/XX/GT/LT subcommands, overflow validation, error message format.

## Failures

### EXPIRE NX/XX/GT/LT subcommands not implemented (~8 errors, cascading)
- **Expected:** `EXPIRE key seconds NX|XX|GT|LT` accepted with 3 arguments
- **Actual:** Returns `ERR wrong number of arguments for 'expire' command` because arity is `Fixed(2)`
- **Root cause:** `ExpireCommand` declares `Arity::Fixed(2)`, so the third argument (NX/XX/GT/LT) is rejected before reaching `execute()`
- **Fix:** Change arity to `Arity::Range { min: 2, max: 3 }` and parse the optional subcommand. Same for PEXPIRE, EXPIREAT, PEXPIREAT.

### Error message format for extreme values (~4 errors)
- **Expected:** `ERR value is not an integer or out of range` (Redis generic message)
- **Actual:** `ERR invalid expire time in 'set' command` or `ERR invalid expire time in 'getex' command` (FrogDB-specific)
- **Root cause:** SET and GETEX use a different error path for expire validation that produces a command-specific error message
- **Fix:** Harmonize error messages to match Redis's generic format for overflow cases

### i64 overflow boundary validation (~2 errors)
- **Expected:** Expire values near `i64::MAX` are rejected (e.g., `EXPIRE key 9223372036854775808` should fail)
- **Actual:** `parse_i64` may overflow silently or produce unexpected behavior
- **Fix:** Ensure `parse_i64` and the expire-to-instant conversion properly reject values that would overflow when converted to `Duration`

### PEXPIRE overflow when added to basetime (~1 error)
- **Expected:** `PEXPIRE key <large_ms>` where `now_ms + large_ms` overflows should return an error
- **Actual:** The overflow is not detected; `Instant::now() + Duration::from_millis(ms as u64)` may panic or wrap
- **Fix:** Check for overflow before constructing the `Instant`

## Source Files

| File | What to change |
|------|----------------|
| `crates/commands/src/expiry.rs:94-132` | `ExpireCommand` — change arity, add NX/XX/GT/LT parsing |
| `crates/commands/src/expiry.rs:138-175` | `PexpireCommand` — same changes |
| `crates/commands/src/expiry.rs:183-225` | `ExpireatCommand` — same changes |
| `crates/commands/src/expiry.rs:231-275` | `PexpireatCommand` — same changes |
| `crates/commands/src/string.rs` | SET EX/PX/EXAT/PXAT error messages |
| `crates/commands/src/basic.rs` | GETEX EX/PX/EXAT/PXAT error messages |

## Recommended Fix Order
1. **Implement NX/XX/GT/LT subcommands** for EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT (highest impact, ~8 errors)
   - NX: Set expiry only if key has no expiry
   - XX: Set expiry only if key already has an expiry
   - GT: Set expiry only if new expiry is greater than current
   - LT: Set expiry only if new expiry is less than current
2. Harmonize error messages for extreme values (~4 errors)
3. Add overflow boundary validation (~3 errors)

## Cross-Suite Dependencies
None directly, but the error message format changes may affect other suites that validate error strings.
