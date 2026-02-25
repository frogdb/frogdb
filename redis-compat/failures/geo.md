# unit/geo — 11 errors

## Category
GEO command edge cases: missing commands, error message format, score format.

## Failures

### GEORADIUS_RO not implemented (~3 errors, cascading)
- **Expected:** `GEORADIUS_RO key lon lat radius unit [options]` works as read-only variant
- **Actual:** Command not recognized
- **Fix:** Add `GeoradiusRoCommand` struct implementing the read-only variant (identical to `GeoradiusCommand` but with `CommandFlags::READONLY`)

### GEOSEARCHSTORE with non-existing source key (~2 errors)
- **Expected:** `GEOSEARCHSTORE dest nonexistent FROMLONLAT ... BYRADIUS ...` returns integer 0
- **Actual:** Returns empty array or error depending on the code path
- **Root cause:** `execute_geosearch()` returns `Ok(vec![])` for missing keys, but `GeosearchstoreCommand::execute()` handles empty results by deleting dest and returning `Integer(0)` — the issue may be in how `parse_geosearch_options()` handles FROMMEMBER on a non-existent key
- **Fix:** When source key doesn't exist, skip the FROMMEMBER lookup and return 0 directly

### GEOADD XX+NX conflict error message (~1 error)
- **Expected:** `ERR XX and NX options at the same time are not compatible` (Redis syntax error pattern)
- **Actual:** Error message matches but Redis test expects it to match `*syntax*` glob pattern
- **Root cause:** The `NxXxOptions::try_parse()` returns a specific incompatibility message, but Redis returns a generic syntax error
- **Fix:** Change the error to `CommandError::SyntaxError` for XX+NX conflict in GEOADD context

### GEOADD invalid option error message (~1 error)
- **Expected:** `ERR wrong number of arguments for 'geoadd' command`
- **Actual:** Returns syntax error for unrecognized options
- **Root cause:** After parsing options, if the remaining args aren't valid triplets, the error should be a wrong-arity error not a syntax error
- **Fix:** When remaining args after option parsing aren't divisible by 3, return `WrongArity` instead of `SyntaxError`

### ZRANGEBYSCORE on geo sorted set — float format (~2 errors)
- **Expected:** Scores are formatted with decimal point (e.g., `1.0`, `52.0`)
- **Actual:** Whole-number scores formatted as integers (e.g., `1`, `52`)
- **Root cause:** `format_float()` in `utils.rs:45-47` strips the decimal for integer-valued floats: `if f.fract() == 0.0 && f.abs() < 1e15 { format!("{:.0}", f) }`
- **Note:** This is the same root cause as the zset score format issue; fixing it there fixes it here

### GEOSEARCH BYLONLAT empty result for STORE (~2 errors)
- **Expected:** Returns integer 0 when no results found
- **Actual:** Returns empty array
- **Root cause:** The GEOSEARCHSTORE path correctly returns `Integer(0)` for empty results, but GEOSEARCH itself might be hit instead
- **Fix:** Verify the command routing distinguishes GEOSEARCH from GEOSEARCHSTORE correctly

## Source Files

| File | What to change |
|------|----------------|
| `crates/commands/src/geo.rs` | Add `GeoradiusRoCommand`, fix error messages |
| `crates/commands/src/geo.rs:37-125` | `GeoaddCommand` — fix XX+NX and invalid option error messages |
| `crates/commands/src/geo.rs:345-405` | `GeosearchstoreCommand` — fix non-existing source key handling |
| `crates/commands/src/utils.rs:34-58` | `format_float()` — score format (shared with zset) |
| `crates/server/src/commands/registry.rs` | Register `GEORADIUS_RO` command |

## Recommended Fix Order
1. **Add GEORADIUS_RO command** (quick, unblocks 3 cascading errors)
2. Fix GEOADD error messages for XX+NX and invalid options (2 errors)
3. Fix GEOSEARCHSTORE non-existing source key (2 errors)
4. Score format fix is shared with zset suite — see `zset.md`

## Cross-Suite Dependencies
- `format_float()` change affects `unit/type/zset` — see [zset.md](zset.md)
- GEORADIUS_RO also needs to be registered in the command registry
