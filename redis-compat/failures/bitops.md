# unit/bitops — 12 errors

## Category
BITOP, BITCOUNT, BITPOS edge cases and type checking.

## Failures

### BITCOUNT with out-of-range indexes (~3 errors)
- **Expected:** `BITCOUNT key start end` returns 0 when start > end (after normalization)
- **Actual:** Returns non-zero count; negative index normalization and start>end detection is incorrect
- **Root cause:** `StringValue::bitcount()` in frogdb_core doesn't correctly handle the case where normalized `start > end`, which should return 0
- **Fix:** After normalizing negative indexes, if `start > end`, return 0 immediately

### BITCOUNT with 1 argument (~1 error)
- **Expected:** `BITCOUNT key start` (without end) returns a syntax error
- **Actual:** Accepted without error — the arity `Range { min: 1, max: 4 }` allows 2 args (key + start)
- **Root cause:** The argument count check in `BitcountCommand::execute()` only checks `args.len() >= 3` for range mode, but doesn't reject `args.len() == 2`
- **Fix:** Add explicit check: if `args.len() == 2`, return syntax error (must be 0 or 2 range args)

### BITOP with non-string source key (~2 errors)
- **Expected:** `BITOP AND dest listkey` returns `WRONGTYPE` error when a source key holds a non-string value
- **Actual:** Treats non-string keys as empty bytes (line 238: returns `Bytes::new()` for non-string types)
- **Root cause:** The source collection loop silently substitutes empty bytes for non-string types instead of returning an error
- **Fix:** Return `CommandError::WrongType` when any source key exists but is not a string

### BITPOS bit=0 with explicit end (~2 errors)
- **Expected:** When `end` is explicitly given, `BITPOS key 0 start end` limits the search to the specified range and returns -1 if no 0-bit is found within it
- **Actual:** The `end` parameter is passed through but `StringValue::bitpos()` doesn't correctly constrain the bit=0 search to the given range
- **Root cause:** Redis treats BITPOS differently when `end` is explicitly provided vs. omitted — with explicit end, it won't look beyond the range
- **Fix:** Track whether `end` was explicitly provided and pass that information to the `bitpos()` implementation

### SETBIT/BITFIELD dirty tracking (~4 errors)
- **Expected:** After SETBIT or BITFIELD SET/INCRBY, the server's dirty key counter is incremented
- **Actual:** The dirty counter is not updated because bitmap mutations don't signal dirtiness
- **Root cause:** The command execution path doesn't increment the dirty counter for bitmap writes
- **Fix:** Ensure SETBIT and BITFIELD write operations increment `ctx.dirty` (or equivalent mechanism)

## Source Files

| File | What to change |
|------|----------------|
| `crates/commands/src/bitmap.rs:135-194` | `BitcountCommand` — add 1-arg rejection, fix range handling |
| `crates/commands/src/bitmap.rs:200-274` | `BitopCommand` — add WRONGTYPE check for non-string sources |
| `crates/commands/src/bitmap.rs:280-358` | `BitposCommand` — fix end-given behavior for bit=0 |
| `crates/commands/src/bitmap.rs:22-81` | `SetbitCommand` — dirty tracking |
| `crates/commands/src/bitmap.rs:364-505` | `BitfieldCommand` — dirty tracking |
| `crates/core/src/types/string.rs` | `StringValue::bitcount()` and `StringValue::bitpos()` implementations |

## Recommended Fix Order
1. Fix BITOP WRONGTYPE check (quick, 2 errors)
2. Fix BITCOUNT 1-arg syntax error (quick, 1 error)
3. Fix BITCOUNT range normalization (3 errors)
4. Fix BITPOS end-given behavior (2 errors)
5. Add dirty tracking for bitmap writes (4 errors — may require plumbing through CommandContext)

## Cross-Suite Dependencies
Dirty tracking affects any suite that checks `INFO stats` dirty counters.
