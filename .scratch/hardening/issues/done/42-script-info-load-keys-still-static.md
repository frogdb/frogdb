# The scripting path's INFO persistence `rdb_last_load_keys_*` fields are still static

Status: done
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

## Resolution

Took candidate fix 1 (plumb the node-wide `RecoveryStats` onto `CommandContext`), following the
exact shape issue 10 used for `snapshot_stats`/`bgsave_in_progress`. Recovery is a one-time boot
event rather than evolving state, so this is an `Arc<RecoveryStats>` captured once at
`ShardWorker` construction (via a `set_recovery_stats` setter, mirroring `set_keyspace_stats`) and
cheap-cloned (refcount bump only) into every `CommandContext` the worker builds — never re-read
live, unlike the `SnapshotCoordinator`-derived fields.

Plumbing, in dependency order:

- `frogdb-core/src/command.rs`: `CommandContext::recovery_stats: Arc<RecoveryStats>`.
- `frogdb-core/src/shard/types.rs`: `ShardPersistence` gains a `recovery_stats` field +
  accessor/setter; `ShardPersistenceDeps` gains a matching `Option` field.
- `frogdb-core/src/shard/builder.rs`: `ShardWorkerBuilder` gains `recovery_stats` (with a
  `with_recovery_stats` setter) defaulting to `RecoveryStats::default()` (all-zero) in
  `try_build()`, matching the `snapshot_coordinator` default-resolution pattern.
- `frogdb-core/src/shard/worker.rs`: `ShardWorker::set_recovery_stats()`, and
  `command_context()` now clones the `Arc` into every context it builds.
- `frogdb-server/src/server/shards.rs` / `server/mod.rs`: `ShardSpawnContext` carries an
  `Arc<RecoveryStats>` sourced from `InitResult::recovery_stats` (the same node-wide value
  `AdminDeps` already uses), and the spawn loop calls `worker.set_recovery_stats(...)` alongside
  the existing `set_keyspace_stats(...)` call.
- `frogdb-core/src/scripting/gate.rs`: `ScriptInvoker` gains a `recovery_stats` field, propagated
  in `from_context()` and re-applied to the fresh per-`redis.call` `CommandContext` in
  `run_local()` — the local (same-shard, synchronous) sub-command path that bypasses a real
  `command_context()` build. `run_remote()` needed no change: cross-shard sub-commands dispatch
  through the owning shard's own `command_context()`, which already carries the field once the
  above plumbing is in place.
- `frogdb-server/src/commands/info.rs`'s `build_persistence_info()`: replaced the static
  `rdb_last_load_keys_expired:0` / `rdb_last_load_keys_loaded:0` literals (and the previously
  *missing* `rdb_last_load_keys_failed` field) with real values from `ctx.recovery_stats`,
  matching the field names and order the connection-level renderer
  (`info/sections.rs::PersistenceSection::render`) already uses.

### Verification

- Forcing test added: `script_info_persistence_reports_the_real_load_stats`
  (`frogdb-server/crates/server/tests/integration_persistence.rs`, tagged `// FM-PERSISTENCE-033`)
  — pre-seeds a data directory directly (via `RocksStore`, gated behind the `test-support` feature
  now added as a `frogdb-server` dev-dependency) with one already-expired key and one undecodable
  key, stamps a `DataDirMarker` standing in for a prior boot, boots a real `TestServer` so genuine
  recovery runs, then asserts `INFO persistence` (connection-level) and
  `EVAL "return redis.call('INFO', 'persistence')" 0` (script-level) agree on
  `rdb_last_load_keys_expired:1`, `rdb_last_load_keys_loaded:0`, and `rdb_last_load_keys_failed:1`.
  Passes.
- `just check frogdb-core`, `just check frogdb-server`: clean.
- `just fmt frogdb-core`, `just fmt frogdb-server`: no diff.
- `just test frogdb-server` (targeted: the new test + the neighboring
  `persistence_renders_the_real_load_stats` and
  `script_info_persistence_reports_the_real_bgsave_outcome`): all pass.
- `just test frogdb-core` (targeted: `command_context_carries_replica_identity`,
  `command_context_reports_primary_by_default`, and the four `run_local_*` gate tests covering
  the same propagation seam): all pass.
- `just test frogdb-server` (full crate) and `just lint frogdb-server` / `just lint-failure-modes`:
  see the commit history / task report for full-suite results.
- FM-PERSISTENCE-033 row in `specs/persistence.md`: "Observable"
  extended to state the script surface now agrees with the connection-level one; "Forced by" lists
  the new test. FM-PERSISTENCE-022's "Bug refs" line updated to mark issue 42 fixed.
- No spec row changes were needed in a locked crate's own sense — this is plumbing through
  `frogdb-core`/`frogdb-server` only; `frogdb-persistence`/`frogdb-recovery` were not touched
  (aside from enabling their existing `test-support` feature as a new dev-dependency, itself an
  already-supported extension point used identically by `frogdb-recovery` and
  `frogdb-replication`), so `just mutants-diff` was not run.
