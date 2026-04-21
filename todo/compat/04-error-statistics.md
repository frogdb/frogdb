# 4. Error Statistics (12 tests — `info_tcl.rs`)

**Status**: Not implemented (latency stats exist, error stats don't)
**Scope**: Implement per-error-type tracking in `INFO stats` (`errorstat_*` fields, `rejected_calls`,
`failed_calls`).

## Work

- Add atomic counters for `rejected_calls` and `failed_calls`
- Track per-error-prefix counts (e.g., `errorstat_ERR`, `errorstat_WRONGTYPE`)
- Classify errors: rejected (before execution) vs. failed (during execution)
- Cap error types to prevent unbounded growth
- Expose in INFO stats section
- Handle errors within MULTI/EXEC and Lua scripts

**Key files to modify**: `commands/info.rs`, command dispatch layer, error types

## Tests

- `errorstats: failed call authentication error`
- `errorstats: failed call NOSCRIPT error`
- `errorstats: failed call NOGROUP error`
- `errorstats: failed call within LUA`
- `errorstats: failed call within MULTI/EXEC`
- `errorstats: rejected call unknown command`
- `errorstats: rejected call within MULTI/EXEC`
- `errorstats: rejected call due to wrong arity`
- `errorstats: rejected call by OOM error`
- `errorstats: rejected call by authorization error`
- `errorstats: blocking commands`
- `errorstats: limit errors will not increase indefinitely`
