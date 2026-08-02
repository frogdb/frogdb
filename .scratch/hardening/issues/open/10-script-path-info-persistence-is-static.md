# The scripting path's INFO persistence section is static placeholders

Status: ready-for-agent
Type: bug (observability accuracy)
Severity: likelihood 1/3 (only `redis.call('INFO')` from a Lua script reaches it), consequence 2/3
(the fields are plausible constants, so a script cannot tell a healthy server from a broken one) —
score 2
Area: persistence / INFO, scripting

## Problem

Split out of `.scratch/hardening/issues/03`. There are two INFO implementations:

- `frogdb-server/crates/server/src/info/` — the client-facing one. Since issue 03 it reports the
  snapshot coordinator's real state (`rdb_last_bgsave_status`, `rdb_last_bgsave_error`,
  `rdb_saves`, `rdb_bgsave_failures`, `rdb_last_save_time`).
- `frogdb-server/crates/server/src/commands/info.rs` — rendered inside a shard worker, which is
  what `redis.call('INFO')` from a Lua script gets. Its `# Persistence` section is a `format!` of
  constants: `rdb_last_bgsave_status:ok`, `rdb_last_save_time:0`, `rdb_bgsave_in_progress:0`, and
  the rest, with only `rdb_changes_since_last_save` carrying a real value.

The shard-local renderer has no handle on the `SnapshotCoordinator` — `CommandContext` carries
shard state, not the server's admin handles — so there is nothing behind those fields to report.
A script that polls INFO to decide whether saves are healthy always reads "healthy".

Recorded as a Redis deviation on the FM-PERSISTENCE-022 row of
`.scratch/hardening/specs/persistence-failure-modes.md`, and noted in a comment above
`build_persistence_info`.

## Candidate fixes

1. **Plumb a stats handle to the shards.** Give the shard context an
   `Arc<dyn SnapshotCoordinator>` (or a cheaper `Arc<RwLock<SnapshotStats>>` snapshot handle) and
   render from it, exactly as the client path does. Straightforward but widens the shard context
   for one section.
2. **Publish the stats into the existing shard-visible state.** The coordinator already writes to
   the metrics recorder, which shards can reach; a small shared `SnapshotStats` cell updated by
   `SnapshotRun::record` would let both renderers read one value without handing the shards the
   coordinator itself. Preferred: no new capability reaches a shard worker.
3. **Delete the section from the script path.** Omit `# Persistence` from the shard-local INFO
   rather than emit constants. Honest, but a client-visible removal and a Redis deviation of its
   own.

Whichever is chosen, the end state should be one source of truth for the fields — two renderers
reading one `SnapshotStats` — not two independently-maintained field lists.

## Forcing test

A `frogdb-server` e2e test that breaks a save (the `bgsave_failure_is_visible_in_info_persistence`
fixture makes the snapshot dir unwritable), then runs
`EVAL "return redis.call('INFO', 'persistence')" 0` and asserts the returned bulk string carries
`rdb_last_bgsave_status:err` — i.e. that both INFO surfaces agree.
