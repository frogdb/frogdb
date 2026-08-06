# The scripting path's INFO persistence `rdb_last_load_keys_*` fields are still static

Status: needs-triage
Type: bug (observability accuracy)
Severity: likelihood 1/3 (only `redis.call('INFO')` from a Lua script reaches it), consequence 1/3
(the fields describe a one-time boot event, not ongoing health) — score 1
Area: persistence / INFO, scripting

## Problem

Split out of issue 10 while fixing it. `frogdb-server/crates/server/src/commands/info.rs`'s
`build_persistence_info` (the shard-local `# Persistence` section `redis.call('INFO')` reaches)
now reports the real `SnapshotCoordinator`-derived fields (`rdb_bgsave_in_progress`,
`rdb_last_save_time`, `rdb_last_bgsave_status`, `rdb_last_bgsave_error`,
`rdb_last_bgsave_time_sec`, `rdb_current_bgsave_time_sec`, `rdb_saves`, `rdb_bgsave_failures`).

Two fields in the same section are still hardcoded zero: `rdb_last_load_keys_expired` and
`rdb_last_load_keys_loaded` (plus `rdb_last_load_keys_failed`, a FrogDB extension, which the
client-facing renderer has and the shard-local one omits entirely). These describe what *this
boot's* recovery did — sourced from `RecoveryStats`, which `frogdb-recovery` (a locked crate)
aggregates node-wide in `AdminDeps.recovery_stats`, not per-shard. A shard's `CommandContext` has
no per-shard recovery counters to hand these from; the value is the same for every shard on a
node, but no plumbing currently carries it there.

Unlike issue 10's fields, these are boot-time constants once recovery finishes — not evolving save
health a script would poll for — so the misleading-data risk is lower, but they are still a
plausible-looking constant no more real than `rdb_last_bgsave_status:ok` was.

## Candidate fixes

1. **Plumb the node-wide `RecoveryStats` onto `CommandContext`**, same shape as issue 10's
   `snapshot_stats`/`bgsave_in_progress` fields — read once in `ShardWorker::command_context()`
   from wherever the node-wide value is threaded through to a shard, or via a new field on
   `ShardWorker` populated at construction (recovery is a one-time boot event, so this could be a
   plain `Arc`/copy set once rather than read live).
2. **Omit the three fields from the shard-local section** rather than emit zeros, matching the
   pattern the shard-local renderer already uses for fields it cannot know (e.g. `aof_*` are
   real Redis-shape constants because FrogDB has no AOF, but `connected_clients`-style
   per-connection fields are omitted entirely elsewhere in this file).

## Forcing test

A `frogdb-server` test that recovers a shard from a snapshot/WAL containing at least one expired
or undecodable key (reusing the fixtures behind `persistence_renders_the_real_load_stats` in
`info/sections.rs`), then runs `EVAL "return redis.call('INFO', 'persistence')" 0` and asserts the
returned bulk string's `rdb_last_load_keys_expired`/`loaded`/`failed` match the real counts —
i.e. that both INFO surfaces agree, the same check issue 10 forced for the save-outcome fields.
