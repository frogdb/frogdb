# WAL recovery mode never explicitly configured; mid-log corruption untested

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 3/3 (score 6)
Area: persistence

## Context

Grepping `WalRecoveryMode`/`set_wal_recovery_mode`/`PointInTime`/`TolerateCorrupted` across every
crate finds nothing — the RocksDB WAL recovery mode is never explicitly set anywhere in FrogDB,
so it silently defaults to RocksDB's `kPointInTimeRecovery`. This default's actual behavior — on a
mid-log checksum failure, it silently truncates *all* subsequent valid records, not just the
corrupted one — is neither pinned in code nor tested against. The documented WAL-corruption
behavior table (`architecture/persistence.md`, describing truncated/checksum/invalid-marker cases
mapping to `Truncate`) describes what the RocksDB default does, but this is inherited default
behavior that has never been explicitly chosen or verified, not a deliberate configuration decision.

Existing corruption tests don't touch the WAL at all: `test_partial_recovery_on_corruption`
corrupts an invalid `VALUE` in a live column family, not the WAL, and just checks that the one bad
key is silently skipped (`crash_recovery_tests.rs:793-808`, `keys_failed=1`);
`test_truncated_metadata_recovery` truncates a snapshot's `metadata.json`, again not the WAL
(`:974-989`). Neither test flips a byte inside an actual RocksDB WAL record.

Per the documented policy (truncating only a torn tail is safe/expected), silently dropping a valid
*suffix* of committed data because an earlier record failed a checksum is not least-surprise
behavior for a database — the recovery mode should be an explicit, pinned choice, not an inherited
library default.

## What to build

- Explicitly pin the RocksDB `WalRecoveryMode` to the intended choice (rather than leaving it as an
  unset default) — the specific mode should be a deliberate decision documented alongside the
  existing WAL-corruption table in `architecture/persistence.md`.
- Unit test: build a WAL with N records, corrupt a single byte inside record `k` (not the tail),
  reopen, and assert the exact set of surviving records matches the chosen recovery mode's
  documented semantics — not "whatever RocksDB happens to default to."
- Assert that a value-skip (or record-truncation) event during recovery emits a metric and/or log
  line, so operators aren't silently missing data with no signal.

## Acceptance criteria

- [ ] `WalRecoveryMode` is explicitly set in code (not left at RocksDB's implicit default)
- [ ] Unit test with a mid-log (non-tail) corrupted WAL record asserts the exact surviving record
      set matches the pinned mode's documented behavior
- [ ] A metric or log line fires when recovery skips/truncates data due to corruption
- [ ] `architecture/persistence.md`'s WAL-corruption table reflects the pinned mode, not an assumed
      default

## Blocked by

None - can start immediately.

## References

- Grep for `WalRecoveryMode`/`set_wal_recovery_mode`/`PointInTime`/`TolerateCorrupted` across all
  crates — zero results (confirmed unset)
- `architecture/persistence.md:149-157` — WAL-corruption behavior table (describes default, not a
  pinned choice)
- `server/tests/crash_recovery_tests.rs:793-808` — `test_partial_recovery_on_corruption` (live CF
  value corruption, not WAL)
- `server/tests/crash_recovery_tests.rs:974-989` — `test_truncated_metadata_recovery` (snapshot
  metadata.json, not WAL)
- Source: `.scratch/testing-improvements/audit/D-persistence.md` gap #3,
  `.scratch/testing-improvements/audit/verdicts-D.md` #3 ("Grep confirms mode unpinned (RocksDB default
  kPointInTimeRecovery); corruption tests touch CF value + metadata.json only;
  persistence.md:149-157 tail-truncate table unverified")

## Resolution

**Mode pinned + why.** `RocksStore::open` now sets
`db_opts.set_wal_recovery_mode(DBRecoveryMode::PointInTime)` explicitly
(`persistence/src/rocks/mod.rs`), with an in-code justification. `PointInTime`
(which is also the RocksDB default — the point is that it is now a *pinned,
tested* decision, not an inherited default) recovers the longest uninterrupted
prefix of valid WAL records and stops at the first checksum failure, so recovered
state is always a real prefix of history. Rejected alternatives, documented in the
code and in `architecture/persistence.md`: `AbsoluteConsistency` refuses to open
on an ordinary torn tail (turns every unclean shutdown into a hard startup
failure); `SkipAnyCorruptedRecord` replays *past* corruption (resurrects stale
values / reorders history); `TolerateCorruptedTailRecords` treats a mid-log
checksum failure as fatal. This mirrors Redis point-in-time AOF loading
(`aof-load-truncated`).

**Corruption-test behavior observed.** New unit tests in
`persistence/src/rocks/tests.rs` write 50 durably-synced records into a single
shard's WAL, drop the store without a memtable flush (the `rocksdb` crate's
`Drop` = `rocksdb_close` destructor does not flush, so the WAL is the sole copy —
a genuine unclean-shutdown simulation), then reopen:
- `wal_mid_log_bitflip_drops_suffix_and_signals`: flips one byte 2/3 into the WAL.
  Observed: recovery keeps a strict, non-empty **prefix** and drops every record
  after the corruption (proving the valid-suffix is discarded, not just the torn
  tail). Asserts survivors are contiguous by index and `dropped == 50 - survivors`.
- `wal_truncation_recovers_prefix_and_signals`: truncates the WAL to 55% length.
  Observed: valid prefix recovered, tail dropped, metric equals the drop count.
- `wal_clean_reopen_recovers_all_without_signal`: intact WAL recovers all 50 with
  zero dropped-records signal (the watermark comparison never false-alarms).
- `wal_recovery_mode_is_pinned_to_point_in_time`: pins the mode choice as a named
  anchor. All 168 persistence tests pass.

**Metric added.** `frogdb_wal_recovery_dropped_records_total` (counter, defined in
`types/src/metrics/definitions.rs`, emitted via `WalRecoveryDroppedRecords`).
Mechanism: a durable-sync sequence high-watermark is persisted next to the DB
(`rocks/wal_watermark.rs`, file `frogdb_wal_watermark`), advanced after every
fsync'd WAL batch (the `RocksSink` sync-commit path and the periodic-sync loop
call `RocksStore::record_wal_watermark`). On open, `detect_and_reset` compares the
sequence RocksDB actually recovered to against the watermark; a shortfall emits
the counter (by the exact number of lost sequence numbers) plus a WARN log, then
re-baselines. Guarantee direction: the watermark is written best-effort
(atomic temp+rename, no self-fsync) so after a crash it can only *lag* the true
durable sequence — the signal can under-report but never false-alarm on a clean
recovery. `architecture/persistence.md`'s WAL-corruption table now documents the
pinned mode, the suffix-drop edge, and this metric.
