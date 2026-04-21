# 14a. Error Message Fixes (3 tests — `set_tcl.rs`, `sort_tcl.rs`)

**Status**: Minor incompatibilities
**Scope**: Fix arity validation and error message formatting for SMISMEMBER and SORT to match
Redis-compatible output exactly.

## Architecture

### SMISMEMBER Arity Check

The SMISMEMBER command (`frogdb-server/crates/commands/src/set.rs`) is declared with
`Arity::AtLeast(2)` which means `SMISMEMBER key` (no members) is rejected at the arity layer.
Redis returns a specific error message: `wrong number of arguments for 'smismember' command` but
the test expects the message `SMISMEMBER requires one or more members` — a custom arity error
with a human-readable message rather than the generic arity format.

**Decision**: The Redis test (`SMISMEMBER requires one or more members`) validates that calling
`SMISMEMBER myset` (key only, zero members) returns an error. FrogDB's current arity
`AtLeast(2)` already rejects this, but the error message format may differ from Redis's. Verify
the exact error string matches Redis's output.

**Key file**: `frogdb-server/crates/commands/src/set.rs` — `SmismemberCommand`

### SORT Numeric Error Format

The SORT command (`frogdb-server/crates/commands/src/sort.rs`) returns
`"One or more scores can't be converted into double"` when encountering non-numeric values during
numeric sorting. Redis uses a specific format for this error that the test validates.

**Decision**: Compare the FrogDB error string against the Redis-expected string. The current
implementation at line ~225 of `sort.rs` already produces an error message — verify it matches
Redis verbatim.

**Key file**: `frogdb-server/crates/commands/src/sort.rs` — `get_sort_key()` function

## Implementation Steps

1. Run the skipped tests to capture the exact error mismatch
2. Compare FrogDB's error output with Redis's expected format
3. Adjust error messages to match exactly (typically a string literal change)
4. Consider whether the arity layer or the command's `execute()` produces the error

## Integration Points

- Arity checking infrastructure in `frogdb-server/crates/core/src/` (generic error format)
- Command-level error generation in individual command files
- Test assertion format in regression tests

## FrogDB Adaptations

None required — these are straightforward error string corrections. No architectural differences
from Redis for these cases.

## Tests

- `SMISMEMBER requires one or more members` — validate arity error message format
- `SORT will complain with numerical sorting and bad doubles (1)` — validate numeric parse error
- `SORT will complain with numerical sorting and bad doubles (2)` — validate numeric parse error

## Verification

```bash
just test frogdb-server-redis-regression "SMISMEMBER requires"
just test frogdb-server-redis-regression "SORT will complain"
```
