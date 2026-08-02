# LASTSAVE is restamped to the boot time, so a stale snapshot reports as fresh

Status: needs-triage
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
