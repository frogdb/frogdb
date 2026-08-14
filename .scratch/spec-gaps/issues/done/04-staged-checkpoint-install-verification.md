# Staged-checkpoint install verification: real completeness check, payload manifest, rollback

Status: done

## Parent

[spec-review-persistence.md](../../../formal-spec/reviews/2026-08-13-antipattern/spec-review-persistence.md)
— Finding H4 ("FM-PERSISTENCE-024/025: staged-checkpoint completeness is one `stat()`, and a bad
install has no rollback"), Finding A1 ("FM-PERSISTENCE-026: backup generation ordering is
wall-clock, and the clock is not a safe key"), Finding A4 ("FM-PERSISTENCE-017/042: the snapshot's
integrity story stops at 'a marker file exists'").

## What is wrong

**H4.** FM-PERSISTENCE-024's guard is `is_complete_db()`
(`frogdb-server/crates/persistence/src/rocks/staged.rs:83`):

```rust
pub fn is_complete_db(&self) -> bool {
    self.dir.join(ROCKSDB_CURRENT_MANIFEST).exists()
}
```

That is "the file named `CURRENT` exists" — it never reads `CURRENT`, opens the MANIFEST it points
at, or checks the SSTs the MANIFEST lists are present. `checkpoint_ready` is populated by an
operator copy (FM-PERSISTENCE-021) and a replica full-sync download, either of which can pass this
gate with a truncated payload. Install then renames the live database aside and moves the broken
checkpoint into its place; `OpenRocks` fails, and there is **no rollback** — `checkpoint_ready` has
been consumed, FM-PERSISTENCE-025's idempotence logic sees a completed install, and every
subsequent boot fails identically with an unrestored `<db>_backup_<ts>` sitting next to it.
FM-PERSISTENCE-026 compounds this: `BACKUP_RETENTION = 1`, so a second install (a retried full
sync) overwrites the only copy of the good database. The producing side is equally weak:
`SnapshotMetadataFile` (`frogdb-server/crates/persistence/src/snapshot/metadata.rs:25`) carries
`size_bytes`, `sequence_number`, `num_shards`, `completion_marker`, but nothing at restore time
compares the installed tree against any of them.

**A1.** `backup_dir_name` (`frogdb-server/crates/persistence/src/rocks/staged.rs:94`) is
`format!("{db_name}_backup_{unix_secs}")` — a wall-clock second. An NTP step backwards (or a
restored VM snapshot) between two installs makes the newer backup sort older, so retention-1
deletes the one it should keep; two installs inside the same second collide on the name with
undefined behavior.

**A4.** Adoption of a snapshot requires `metadata.json` carrying `FROGDB_SNAPSHOT_COMPLETE_v1`,
which proves the stager finished, not that the payload is intact. These artifacts are long-lived
backups on media that bit-rots, restored by an operator copy into `checkpoint_ready` where the only
gate is H4's `stat()` — the end-to-end backup path has no integrity check anywhere.

## What to build

1. Strengthen `is_complete_db()`: read `CURRENT`, parse the MANIFEST it names, confirm every
   referenced SST/blob file exists with the expected size.
2. Have the stager write a payload manifest (relative path, size, ideally checksum per file) into
   `metadata.json`; have install verify the staged tree against it when present.
3. Add a rollback row: if post-install `OpenRocks` fails, boot restores `<db>_backup_<ts>` over
   `<db>` and refuses with a message naming what happened. (Alternative: defer destroying the live
   db until after a trial open of the staged directory.)
4. Fix or weaken FM-PERSISTENCE-024's NOT observable ("the live database being renamed aside for a
   checkpoint that cannot replace it") to match what the strengthened check actually guarantees.
5. Make the backup suffix a monotone counter seeded from the highest existing suffix (matching
   FM-PERSISTENCE-015's snapshot-epoch pattern), keeping the timestamp only for human readability.
6. Add a `frogctl checkpoint-verify` command that runs the same payload-manifest check against a
   backup on demand, so an operator can validate before the outage they need it for.

## Acceptance criteria

- [ ] `is_complete_db()` parses `CURRENT`/MANIFEST and checks referenced files exist with expected
      sizes; forcing test: `CURRENT` present, SSTs missing/truncated → rejected
- [ ] Payload manifest written into `metadata.json` by the stager and checked at install
- [ ] Rollback-on-failed-open implemented (restore `<db>_backup_<ts>` or trial-open before replace);
      forcing test: corrupted staged db fails `OpenRocks`, node recovers on next boot
- [ ] Backup suffix changed to a monotone counter; forcing test for same-second collision and
      clock-step-backwards ordering
- [ ] `frogctl checkpoint-verify` command added
- [ ] FM-PERSISTENCE-024/025/026 rows updated to match; `just lint-spec` green
- [ ] `just mutants-diff frogdb-persistence` triaged on touched code

## Blocked by

None.

## Ruling (2026-08-13)

Full package. Strengthen is_complete_db(): parse CURRENT → MANIFEST and verify listed SSTs exist
with expected sizes (today it is CURRENT.exists()). Ship a payload manifest inside metadata.json
and verify at install. Trial-open the staged DB before destroying the old one, or add a rollback
row: restore the backup on failed post-install open (BACKUP_RETENTION=1 currently lets a retried
sync destroy the last good copy). Backup retention key becomes a monotone counter, not a
wall-clock second (A1). Add a frogctl checkpoint-verify command (A4).
