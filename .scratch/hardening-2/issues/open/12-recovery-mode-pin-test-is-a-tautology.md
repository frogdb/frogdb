# The WAL recovery-mode pin test is a tautology carrying an FM tag

Status: needs-triage
Type: weak witness (B3 assert-nothing class)
Severity: likelihood 1/3 (RocksDB open-path regression or library-default change), consequence
2/3 (recovery mode silently reverts to a RocksDB default while a named, FM-tagged test claims it
is pinned) — score 2
Area: persistence / WAL recovery

## Problem

`frogdb-server/crates/persistence/src/rocks/tests.rs:1312`
(`wal_recovery_mode_is_pinned_to_point_in_time`):

```rust
let pinned = rocksdb::DBRecoveryMode::PointInTime;
assert!(matches!(pinned, rocksdb::DBRecoveryMode::PointInTime));
```

A local literal matched against itself. The test passes if the open path stops setting
`set_wal_recovery_mode` entirely, or sets a different mode. Its own doc comment admits it is a
"compile-time proof the variant exists" — but it sits under an `// FM-PERSISTENCE-034` tag, so the
spec row counts it as a forcing test. That is exactly the B3 witness-inflation shape the W3 audit
scored: the row looks stronger than it is.

Found by the W3a re-witness agent (2026-08-07) while strengthening the crash-recovery rows; left
untouched because frogdb-persistence is LOCKED and the file was outside issue 08 step 3.

## Candidate fix

Assert against the actual configuration the open path builds — e.g. expose (test-only, or via a
`pub(crate)` options-builder seam) the `Options` used by `RocksStore::open` and assert
`wal_recovery_mode == PointInTime` there. If no seam is affordable, retag: drop the
FM-PERSISTENCE-034 tag from this test (the corruption tests carry the behavioral proof) and keep
it as an untagged named anchor, so the spec row stops counting it.

## Forcing test

The fixed test must fail when the `set_wal_recovery_mode` call in the open path is deleted.
Mutation check: frogdb-persistence is a locked crate — `just mutants-diff frogdb-persistence`
after the fix.
