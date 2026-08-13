# The scripting path's INFO persistence section is static placeholders

Status: done
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
`specs/persistence.md`, and noted in a comment above
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

## Resolution

Took candidate fix 1 (plumb a stats handle to the shards), following the same pattern the
connection-level renderer already used. `frogdb_core::persistence::SnapshotStats` (the
`SnapshotCoordinator`'s existing stats value — no new type) now flows into `CommandContext`
itself, populated at every place a `CommandContext` is built for shard-local execution:

- `ShardWorker::command_context()` (`frogdb-server/crates/core/src/shard/worker.rs`) reads
  `self.persistence.snapshot_coordinator().stats()` / `.in_progress()` for the outer EVAL/direct-
  command context — this alone made the *client-issued* `EVAL ... redis.call('INFO')` see live
  data.
- The actual bug was one level deeper: each individual `redis.call()` sub-command inside a script
  runs against a **fresh** `CommandContext::new()` built by `ScriptInvoker::run_local`
  (`frogdb-server/crates/core/src/scripting/gate.rs`), which defaults `snapshot_stats` /
  `bgsave_in_progress` to empty — the exact same reason `is_replica`, `replication_tracker`, and
  `json_limits` are explicitly re-propagated onto that fresh context after construction. Snapshot
  stats had no such propagation line, so a script always saw the placeholder regardless of the
  worker-level fix. Added `snapshot_stats` / `bgsave_in_progress` to `ScriptInvoker`'s fields and
  to that propagation block, matching the existing pattern exactly.

Both renderers (`frogdb-server/crates/server/src/info/sections.rs` for the connection-level path,
`frogdb-server/crates/server/src/commands/info.rs` for the shard-local scripting path) now call
one shared function, `persistence_snapshot_fields` (`frogdb-server/crates/server/src/info/mod.rs`),
off the same `SnapshotStats` value — one source of truth, not two field lists.

### Per-field disposition

| Field | Before | After |
|---|---|---|
| `rdb_bgsave_in_progress` | hardcoded `0` | real, from `SnapshotCoordinator::in_progress()` |
| `rdb_last_save_time` | hardcoded `0` | real, from `SnapshotStats::last_save_time` |
| `rdb_last_bgsave_status` | hardcoded `ok` | real: `err` when `SnapshotStats::last_error.is_some()`, else `ok` |
| `rdb_last_bgsave_error` | absent | new field, present only on `err`, carrying the real cause (CR/LF folded to spaces) |
| `rdb_last_bgsave_time_sec` | hardcoded `-1` | real, from `SnapshotStats::last_duration` (`-1` sentinel only when never saved) |
| `rdb_current_bgsave_time_sec` | hardcoded `-1` | real, from `SnapshotStats::current_save_elapsed()` (`-1` when idle) |
| `rdb_saves` | hardcoded `0` | real, from `SnapshotStats::saves` |
| `rdb_bgsave_failures` | absent | new field, real, from `SnapshotStats::failures` |
| `rdb_changes_since_last_save` | already real (`ctx.store.dirty()`) | unchanged |
| `rdb_last_load_keys_expired` / `_loaded` / `_failed` | hardcoded `0` | **still hardcoded** — `RecoveryStats` is a node-wide-aggregated source, not per-shard, and issue 10's scope/forcing-test only covers the `SnapshotCoordinator`-derived save-outcome fields. Split out to new follow-up `.scratch/hardening/issues/open/42-script-info-load-keys-still-static.md` rather than silently left as-is. |
| `loading`, `async_loading`, `current_cow_*`, `current_save_keys_*`, `rdb_last_cow_size`, `aof_*`, `module_fork_*` | hardcoded `0` / sentinel | **left as-is, deliberately** — these describe Redis mechanisms FrogDB does not have (fork-based COW snapshotting, AOF, module fork hooks); the connection-level renderer already renders the same constants for the same reason pre-dating this issue, so leaving them keeps the two renderers agreeing rather than inventing a second Redis deviation |

### Verification

- Forcing test added: `script_info_persistence_reports_the_real_bgsave_outcome`
  (`frogdb-server/crates/server/tests/integration_persistence.rs`) — breaks a save via
  `fail_one_background_save`, then asserts `EVAL "return redis.call('INFO', 'persistence')" 0`
  returns `rdb_last_bgsave_status:err`, `rdb_bgsave_failures` incremented, `rdb_saves` unchanged,
  and (after a subsequent successful save) status flips back to `ok` with `rdb_bgsave_failures`
  retaining its cumulative count. Passes.
- `just check frogdb-core`, `just check frogdb-server`: clean.
- `just fmt frogdb-core`, `just fmt frogdb-server`: no diff.
- `just lint frogdb-core`, `just lint frogdb-server` (clippy `-D warnings` + `./scripts/failure-modes.py`): clean.
- `just test frogdb-server persistence`: 106/106 relevant tests pass except one known pre-existing
  flake unrelated to this change (`test_broadcast_lag_disconnect_and_resync`, already tracked as
  `.scratch/hardening/issues/done/01-broadcast-lag-resync-flake.md`; reproduced independently on
  the pre-fix commit at a similar ~20% rate, confirming it is not a regression from this change).
- FM-PERSISTENCE-022 row in `specs/persistence.md` updated:
  "NOT observable" now states the two renderers agree; "Forced by" lists the new test; "Bug refs"
  marks issue 10 fixed and cross-references issue 42.
