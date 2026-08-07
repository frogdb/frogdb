# Six hand-rolled durable writers at four levels of correctness

Status: ready-for-agent
Type: bug (durability) + refactor (chokepoint)
Severity: likelihood 2/3, consequence 3/3 (a crash loses replication identity, ACL state, or a
staged checkpoint's contents while the code reports success) — score 6
Area: cross-crate (persistence, replication, search, scripting, acl, server)

## Problem

`SnapshotFs` (`frogdb-server/crates/persistence/src/fs_seam.rs:32-93`) is the only correct
durable-write primitive in the repo — `write` / `rename` / `symlink` / `sync_file` / `sync_dir`,
with a `RecordingFs` test fake (`:104-187`) that records fsync-vs-rename *order*. It is
`pub(crate)`, so nothing outside `frogdb-persistence` can use it, and five other places have
re-implemented it, each stopping at a different point:

| file:line | protocol | file fsync | dir fsync | atomic |
|---|---|---|---|---|
| `persistence/src/data_dir.rs:191-194` | write tmp → sync_file → rename → sync_dir | yes | yes | yes |
| `search/src/vector.rs:452-462` (private re-impls at `:433-447`) | same | yes | yes | yes |
| `scripting/src/persistence.rs:175-200` (FUNCTION registry) | tmp → sync_all → rename | yes | no | yes |
| `server/src/config_persister.rs:98-128` (`CONFIG REWRITE`) | tmp → sync_all → rename | yes | no | yes |
| `replication/src/state.rs:277-291` (replid + `offset_at_save`) | `fs::write` → `fs::rename` | **no** | **no** | name only |
| `acl/src/manager.rs:267-277` (`ACL SAVE`) | `File::create` → `write_all` → drop | **no** | **no** | **no** |

Further unsynced writes on paths that are commit points:

- `replication/src/fullsync.rs:483-505` — every received SST/MANIFEST/CURRENT byte; `BufWriter`
  is flushed, the `File` is dropped unsynced.
- `replication/src/fullsync/stager.rs:129` — `fs::rename(incoming → staged)`, the writer's commit
  point in the staged-checkpoint contract; no fsync before, no dir sync after.
- `replication/src/fullsync/stager.rs:149` — the staged replication metadata; failure is `warn!`
  then `Ok`.
- `replication/src/state.rs:142` — the disarm rename, so a crash can resurrect a disarmed
  staged checkpoint.
- `replication/src/split_brain_log.rs:150-173` — file synced, directory not, while
  `has_pending_logs` (`:177-187`) decides `SplitBrainRecoveryPending` by scanning dirents.

## Fix

1. Promote `fs_seam.rs` into a zero-dependency `frogdb-fs` crate: `DurableFs`, `RealFs`, a
   composed `publish_file(fs, path, bytes)` implementing the full four-step protocol, and
   `RecordingFs` behind a `test-support` feature. (`search`/`scripting`/`acl` depend on
   `frogdb-types` only — a visibility change alone cannot reach them without a rocksdb
   dependency.)
2. Route all six writers and the replication commit points through it; delete the duplicates.
3. Give `StagedCheckpoint` a `publish(incoming, &dyn DurableFs)` so the full-sync writer uses the
   same protocol the installer is mutation-gated on.
4. Land the W1 durable-write chokepoint lint with this list as its burn-down baseline.

## Forcing tests

Per converted call site, a `RecordingFs`-based test asserting the operation order (bytes synced
before rename, parent synced after). The end-to-end crash proof belongs to the W2 harness.

## Comments

Found by the campaign-2 durability-extraction survey, 2026-08-07. This is the concrete work
behind PRD §4.2 candidates 1 and 2.
