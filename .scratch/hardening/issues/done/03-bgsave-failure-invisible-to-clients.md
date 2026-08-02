# A failed BGSAVE is invisible to every client-facing surface

Status: done
Type: bug (observability / Redis parity)
Severity: likelihood 2/3 (any snapshot failure — full disk, permissions, IO error), consequence 3/3
(an operator believes backups are running when none have succeeded for weeks) — score 6
Area: persistence / snapshot coordinator, INFO

## Problem

`BGSAVE` is fire-and-forget: the command replies `+Background saving started` the moment the
scheduler claims the epoch, and the run happens on a background task. Nothing that fails in that
task reaches a client.

1. **`INFO persistence` lies.** `rdb_last_bgsave_status` is the string literal `"ok"`
   (`frogdb-server/crates/server/src/info/sections.rs:256`) with no failure state plumbed behind
   it. There is no code path that can make it report `err`. Redis flips this field to `err` after
   a failed background save, and monitoring built against Redis reads it. The legacy handler has
   the same literal (`frogdb-server/crates/server/src/commands/info.rs:298`).
2. **No MISCONF.** Redis' `stop-writes-on-bgsave-error` (default `yes`) refuses every write with
   `-MISCONF Redis is configured to save RDB snapshots, but is currently not able to persist on
   disk` once a background save has failed. FrogDB has no equivalent: writes keep being
   acknowledged indefinitely against a server that cannot persist. The only refusal FrogDB offers
   is per-write `-IOERR` under the non-default `wal-failure-policy = rollback`, which covers
   failures the shard observes at write time — not a persistently broken disk observed by the
   snapshot task.
3. **No counters.** There is a `SnapshotFailures`-style log line, but no failure counter or
   timestamp reaches `INFO`, so even a careful operator has only the process log.

What *is* honest: `LASTSAVE` does not advance on a failed save (pinned by
`lastsave_tracks_real_bgsave_and_ignores_failed_saves`). So the only accurate signal is a
*negative* one — a timestamp that stops moving — while the positive signal next to it says `ok`.

Current behavior pinned by FM-PERSISTENCE-022 in
`.scratch/hardening/specs/persistence-failure-modes.md`.

## Candidate fixes

1. **Minimum: stop lying.** Track the last snapshot outcome in the coordinator (it already owns
   `last_save_time`), surface it through the existing `PersistenceSnapshot` struct the INFO handler
   builds, and report `ok`/`err`. Add `rdb_saves` and a failure counter while the plumbing is open.
   Small, no client-visible behavior change beyond a field telling the truth.
2. **Redis parity: a MISCONF gate.** A `stop-writes-on-snapshot-error` config (default off, to keep
   the current behavior until an operator opts in) that latches on a snapshot failure and makes the
   write path reply `-MISCONF ...`, clearing on the next successful save. Bigger: it needs a
   shared latch the write path reads on every write, which is exactly the shape of the existing
   `WalFailurePolicy` `AtomicU8`, so the mechanism exists.

Fix 1 is worth doing regardless of whether 2 is ever wanted.

## Resolution

Candidate fix 1 ("minimum: stop lying"), plus the counters. Fix 2 (the MISCONF gate) is
deliberately **not** implemented here — it changes when a server refuses writes, which is a
product decision, not an observability fix. It is now filed on its own as
`.scratch/hardening/issues/09`; this issue closes on the observability half.

The coordinator's write-only `last_metadata` and its `last_save_time` are replaced by one
`SnapshotStats` value (`frogdb-server/crates/persistence/src/snapshot/mod.rs`):

```rust
pub struct SnapshotStats {
    pub last_save_time: Option<SystemTime>,
    pub saves: u64,
    pub failures: u64,
    pub last_error: Option<String>,
}
```

`SnapshotRun::record` calls `record_success` / `record_failure` on it, `SnapshotCoordinator::stats`
publishes the whole value in one lock acquisition (and `last_save_time()` is now a provided method
derived from it, so `LASTSAVE` and `INFO` cannot disagree), and `PersistenceSnapshot` carries the
three new fields through to `PersistenceSection::render`:

```rust
.field("rdb_last_bgsave_status", if p.last_bgsave_error.is_some() { "err" } else { "ok" })
.field_opt("rdb_last_bgsave_error", p.last_bgsave_error.as_deref().map(|e| e.replace(['\r', '\n'], " ")))
.field("rdb_saves", p.saves)
.field("rdb_bgsave_failures", p.bgsave_failures)
```

`rdb_last_bgsave_error` and `rdb_bgsave_failures` are FrogDB additions (Redis reports only the
status, which says saves broke but not why); CR/LF in the cause are folded to spaces so one field
stays one line. Failures are cumulative — a later success clears the *cause* and the status but not
the count, so an operator sampling after a recovery still sees that saves were failing.

Forcing tests: `bgsave_failure_is_visible_in_info_persistence` (e2e: an unwritable snapshot dir →
`err` + cause + counters over the wire, then a recovery save flipping back to `ok`),
`test_coordinator_records_failed_then_recovered_save` (the coordinator's stats directly), and
`persistence_renders_the_real_bgsave_outcome` (the render, including the CRLF fold).

Spec: FM-PERSISTENCE-022 retitled to "…is reported as a failure, and a later success does not erase
it" and rewritten to the fixed behavior; the MISCONF gap moves to the Redis-deviations table
against issue 09.

### Follow-up in the same change: the save-duration fields

`rdb_last_bgsave_time_sec` and `rdb_current_bgsave_time_sec` were hardcoded `-1` next to the
status field. `-1` is Redis' "no such save" sentinel, so a permanent `-1` is the same class of
misleading field as the hardcoded `ok` this issue was filed about. Both are now real:
`SnapshotStats` carries `last_duration` (set on success only) and `current_started_at` (stamped by
`record_start` when `spawn_run` claims the scheduler slot, cleared when the outcome is recorded),
and the renderer maps `None` to `-1` at the edge. The current field is computed at *read* time, so
a hung save shows a rising number rather than a frozen one, and durations use a monotonic `Instant`
so a wall-clock step cannot invent a negative or hour-long save. `last_duration` is deliberately
per-process: the artifact records when a save finished, not how long it took, so a boot reports
`-1` there while `rdb_last_save_time` is set (stated in FM-PERSISTENCE-042).

Forcing tests: `test_coordinator_reports_last_and_current_save_duration`,
`test_failed_save_leaves_no_duration_and_nothing_in_flight`,
`persistence_renders_save_durations_with_redis_sentinels`, plus duration assertions added to
`bgsave_failure_is_visible_in_info_persistence`.

### Remaining scope, filed separately

- **MISCONF write refusal** — issue 09.
- **The scripting path's INFO** (`frogdb-server/crates/server/src/commands/info.rs`) still emits
  static `rdb_*` placeholders, because a shard worker has no handle on the snapshot coordinator.
  Only `redis.call('INFO')` sees it; the client-facing path is truthful. Filed as issue 10 and
  recorded as a Redis deviation.
