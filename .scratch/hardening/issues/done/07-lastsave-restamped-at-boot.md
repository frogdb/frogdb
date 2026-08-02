# LASTSAVE is restamped to the boot time, so a stale snapshot reports as fresh

Status: done
Type: bug (observability / Redis parity)
Severity: likelihood 3/3 (every restart of a node with an existing snapshot), consequence 2/3
(a "backup is stale" alarm can never fire after a restart) — score 6
Area: persistence / snapshot coordinator, LASTSAVE + INFO

## Problem

`RocksSnapshotCoordinator::new` seeds the last-save time from whether a complete snapshot exists,
not from when it was taken (`frogdb-server/crates/persistence/src/snapshot/rocks_coordinator.rs`):

```rust
let (ie, lm) = Self::load_latest_metadata(&config.snapshot_dir).unwrap_or((0, None));
let lst = if lm.is_some() { Some(Instant::now()) } else { None };
```

`Instant::now()` is the boot instant. `LASTSAVE` and `INFO`'s `rdb_last_save_time` both derive
their Unix timestamp by subtracting `instant.elapsed()` from the current wall clock
(`handle_lastsave`, `info_handler.rs:160`), so immediately after a restart both report *now* — for
a snapshot that may be days old.

The correct value is on disk and already parsed: `SnapshotMetadataFile::completed_at_ms` is read by
`load_latest_metadata` in the same expression that throws it away. The type is the obstacle —
`last_save_time` is an `Instant`, which is process-relative and cannot represent a time from before
the process started, so the seed has nowhere honest to go.

Consequences:

- Monitoring that alarms on "no successful save in N hours" is silenced by a restart, which is
  precisely when an operator most wants it. Combined with issue 03 (`rdb_last_bgsave_status` is
  hardcoded `ok`), *every* persistence health field is unreliable after a restart.
- Redis reports the RDB file's real save time across restarts, so parity is broken in the direction
  that hides staleness.
- The inverse case is honest by accident: with no complete snapshot, `lst` is `None` and `LASTSAVE`
  returns `0`, matching Redis' "never saved".

## Candidate fixes

1. **Store a Unix timestamp, not an `Instant`.** Change `last_save_time` to
   `Option<SystemTime>` (or a `u64` Unix seconds) so `completed_at_ms` from the loaded metadata can
   be used directly and `LASTSAVE`/`INFO` stop doing elapsed-time arithmetic. This also removes the
   double-truncation footgun `handle_lastsave` currently carries a comment about. Mechanical: the
   trait method `SnapshotCoordinator::last_save_time` and its three implementations
   (`rocks_coordinator`, `noop`, the `startup.rs` test stub) plus the two read sites.
2. **Keep `Instant` and reconstruct.** Convert `completed_at_ms` into an `Instant` by subtracting
   the delta from `Instant::now()`. Works, but is unrepresentable when the snapshot predates the
   process by more than the monotonic clock allows and re-introduces the same arithmetic. Not
   preferred.

Fix 1 is the same edit that issue 03's "track the last snapshot outcome" wants to make to this
struct, so the two are worth doing together.

## Forcing test

A coordinator test that writes a complete `metadata.json` with a `completed_at_ms` well in the past,
constructs a fresh `RocksSnapshotCoordinator` over that directory, and asserts `last_save_time()`
reflects the metadata rather than the construction time. The snapshot-dir fixtures for this already
exist in `frogdb-persistence`'s `snapshot::tests`.

## Resolution

Candidate fix 1 (store a wall-clock time, not an `Instant`), as filed and as issue 03 wanted for
the same struct — both were done in one change. `last_save_time` is now
`Option<SystemTime>` inside `SnapshotStats`, and the boot seed reads the value
`load_latest_metadata` used to parse and throw away:

```rust
let stats = SnapshotStats {
    last_save_time: lm.as_ref().and_then(|m| m.completed_at()),
    ..SnapshotStats::default()
};
```

**Truth source: `SnapshotMetadataFile::completed_at_ms`**, exposed as `completed_at()`, in
preference to the checkpoint directory's or manifest's mtime. It is written by `mark_complete` at
finalize time, lives *inside* the durable artifact (so it survives a copy that rewrites mtimes, and
a hand-touched directory cannot age or freshen it), and it is the same field the completing process
stamps into `SnapshotStats` — so the value reported right after a save and the value reported after
the next restart are the same bytes. A complete snapshot carrying no `completed_at_ms`, or an
incomplete one, seeds `None`: unknown is reported as "never saved" (`LASTSAVE` 0), never as "just
now". `SnapshotCoordinator::last_save_time` is now a provided method over `stats()`, so `LASTSAVE`
and `rdb_last_save_time` read one value; the elapsed-time arithmetic and its double-truncation
comment in `handle_lastsave` are gone.

`Instant` vs `SystemTime`: the reported time may predate the process, which a monotonic clock
cannot represent — that impossibility was the original bug. The metric
`SnapshotLastTimestamp` is now set from the same wall-clock value.

Forcing tests: `lastsave_reports_the_snapshot_time_after_restart` (e2e: save, shut down, age the
artifact's `completed_at_ms` by 30 days, restart, assert `LASTSAVE`/`rdb_last_save_time` report the
aged value and that `rdb_saves` stays 0 — a boot is not a save),
`test_coordinator_seeds_last_save_from_snapshot_metadata`, and
`test_coordinator_seeds_none_without_a_usable_completion_time`.

Spec: new row FM-PERSISTENCE-042, which documents the truth-source choice.
