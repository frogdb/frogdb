# WAL recovery mode never explicitly configured; mid-log corruption untested

Status: needs-triage
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
