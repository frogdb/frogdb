# `INFO` counters report `0` while their live sources exist (`expires`, `tracking_clients`, script cache)

Status: ready-for-agent
Type: bug (introspection accuracy)
Area: info

## Problem

Distinct from [issue 25](../../../redis-feel/issues/) (static process facts): these are counters
whose subsystems are implemented and running, but whose INFO field is a literal `0`.

### `db0:…,expires=0,avg_ttl=0`

`frogdb-server/crates/server/src/info/sections.rs:590`, mirrored at
`frogdb-server/crates/server/src/commands/info.rs:576`. FrogDB tracks volatile keys — the store
exposes `sample_volatile_keys` (`core/src/store/mod.rs:729`) and active expiry walks an expiry
index (`core/src/shard/active_expiry.rs`) — so the count of keys with a TTL is obtainable per
shard and summable like every other shard aggregate. Redis's `avg_ttl` is an estimate and may
stay `0` (Redis itself reports `0` outside active-expire sampling); `expires` is exact and must
be real.

### `tracking_clients:0`

`sections.rs:150`. Client-side caching is implemented — tracking sessions with invalidation
channels are torn down per connection (`connection/dispatch.rs:310-323`), `CLIENT TRACKING`
/`TRACKINGINFO`/`CACHING`/`GETREDIR` all work (`client_conn_command.rs:98-99`). The count of
connections with tracking on is a registry scan.

### `client_recent_max_input_buffer` / `client_recent_max_output_buffer` = `0`

`sections.rs:147-148`. Per-client buffer sizes became real in
[issue 09](../../../redis-feel/issues/) (`ClientMemoryUsage::query_buf_size`, `query_buf_peak`,
`output_buf_len`, `output_list_mem`). These two are the max over recent clients of values the
registry already holds. Related deferred work:
[`.scratch/roadmap/compat/14d-query-buffer-observability.md`](../../../roadmap/compat/14d-query-buffer-observability.md)
(that file's "hardcoded zeros" description is now partly stale — `qbuf`/`argv-mem` landed).

### `used_memory_lua:0`, `used_memory_scripts:0`, `number_of_cached_scripts:0`

`sections.rs:195-199`, `commands/info.rs:274-277`. Scripting is implemented with a real per-shard
script cache (`SCRIPT LOAD` broadcasts to all shards,
`connection/scripting/script.rs:33-39`; `FUNCTION STATS` already reports real
`libraries_count`/`functions_count`, `connection/scripting/function.rs:213`). At minimum
`number_of_cached_scripts` is a `len()`. Byte counts follow whatever the cache can honestly
report; if it cannot size the Lua VM, leave `used_memory_lua` at `0` and record why in
`## Comments` rather than guessing.

### `clients_in_timeout_table:0`

`sections.rs:151`. Blocking clients with a timeout are tracked (the blocking subsystem owns a
timeout structure). Report the real count, or drop the field with the reasoning recorded if
FrogDB's blocking model has no equivalent table.

## Acceptance criteria

- [ ] `SET k v EX 60` then `INFO keyspace` → `expires=1`
- [ ] Two connections with `CLIENT TRACKING ON` → `tracking_clients:2`, back to `0` after both
      disconnect
- [ ] `SCRIPT LOAD` of 3 distinct scripts → `number_of_cached_scripts:3`; `SCRIPT FLUSH` → `0`
- [ ] `client_recent_max_input_buffer`/`output_buffer` are non-zero after a large pipelined write
- [ ] Any field that stays `0` has its reason recorded in `## Comments` (truthful-inert, per
      ADR-0005) rather than being left as an unexplained literal

Size: M
