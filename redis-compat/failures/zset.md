# unit/type/zset — 45 errors

## Category
Sorted set operations — cascading failures from ZSCORE/ZMSCORE response format.

## Failures

### ZSCORE/ZMSCORE response format (~30+ errors, cascading)
- **Expected:** `ZSCORE key member` returns integer reply (`:10\r\n`) for whole-number scores
- **Actual:** Returns bulk string reply (`$2\r\n10\r\n`)
- **Root cause:** `score_response()` in `utils.rs:565-570` always returns `Response::bulk(format_float(score))` in RESP2 mode. Redis returns `:10` for score 10 (integer reply), which Tcl can use directly in `expr` evaluation. The bulk string `"10"` breaks Tcl's `expr` because it treats it as a string, not a number.
- **Cascade:** The Redis test suite uses `assert_equal [r zscore key member] 10` which calls Tcl's `expr` to compare — the bulk string format causes comparison failures that cascade through every test that reads scores.
- **Fix:** In RESP2 mode, when the score is a whole number (no fractional part), return `Response::Integer(score as i64)`. For non-integer scores, continue using bulk string. This matches Redis behavior where ZSCORE returns the most compact representation.

### ZMSCORE with 0 member arguments (~1 error)
- **Expected:** `ZMSCORE key` (no members) returns arity error
- **Actual:** Returns empty array
- **Root cause:** `ZmscoreCommand` has `Arity::AtLeast(2)` which means key + at least 1 member, but the arity check may not be enforced correctly
- **Fix:** Verify the arity enforcement catches `ZMSCORE key` with no members

### ZADD with empty string score (~1 error)
- **Expected:** `ZADD key "" member` returns error (empty string is not a valid score)
- **Actual:** Accepted — `parse_f64("")` may return 0.0 or the empty check isn't triggered
- **Fix:** Add explicit empty-string check before `parse_f64` in ZADD's score parsing

### ZUNION/ZINTER/ZDIFF with numkeys=0 (~3 errors)
- **Expected:** `ZUNION 0` returns arity error
- **Actual:** Succeeds with empty result
- **Root cause:** The numkeys=0 case isn't validated — it should be rejected since at least 1 key is required
- **Fix:** Add `if numkeys == 0 { return Err(CommandError::SyntaxError) }` in ZUNION/ZINTER/ZDIFF

### Blocked client tracking (~5-8 errors)
- **Expected:** Tests that use BZPOPMIN/BZPOPMAX check `blocked_clients` in INFO or CLIENT LIST
- **Actual:** `blocked_clients` is always 0 because FrogDB doesn't track blocked client state in these outputs
- **Note:** This is a cross-suite issue — see STATUS.md "Known Cross-Suite Issues"

## Source Files

| File | What to change |
|------|----------------|
| `crates/commands/src/utils.rs:564-571` | `score_response()` — return Integer for whole-number scores in RESP2 |
| `crates/commands/src/sorted_set/basic.rs:26-56` | `ZaddCommand` — reject empty string scores |
| `crates/commands/src/sorted_set/basic.rs:255-293` | `ZmscoreCommand` — verify arity enforcement |
| `crates/commands/src/sorted_set/set_ops.rs` | ZUNION/ZINTER/ZDIFF — reject numkeys=0 |

## Recommended Fix Order
1. **Fix `score_response()` for RESP2 whole-number scores** (highest impact, fixes ~30 cascading errors)
   ```rust
   // In RESP2, return integer for whole-number scores
   if !is_resp3 && score.fract() == 0.0 && score.is_finite()
       && score >= i64::MIN as f64 && score <= i64::MAX as f64 {
       Response::Integer(score as i64)
   } else if is_resp3 && score.is_finite() {
       Response::Double(score)
   } else {
       Response::bulk(Bytes::from(format_float(score)))
   }
   ```
2. Reject empty string scores in ZADD (1 error)
3. Reject numkeys=0 in ZUNION/ZINTER/ZDIFF (3 errors)
4. Verify ZMSCORE arity (1 error)
5. Blocked client tracking is a separate cross-suite effort

## Cross-Suite Dependencies
- `score_response()` is used by ZSCORE, ZMSCORE, ZRANGE WITHSCORES, and all sorted set commands — changes here affect `unit/geo` score format as well
- Blocked client tracking: shared with `unit/pause`, `unit/multi`, `unit/slowlog`
