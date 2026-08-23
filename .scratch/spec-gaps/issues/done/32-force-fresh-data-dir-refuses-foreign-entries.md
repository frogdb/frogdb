# --force-fresh-data-dir refuses beside foreign entries (fresh-start tool, not override)

Status: done

Size: S

> **Ruling (2026-08-22, R6 in
> [work-item rulings](../../../cluster-correctness/2026-08-22-work-item-rulings.md)):**
> candidate rule (a) from the issue-31 design doc's V25-m6 verdict: the flag **refuses**
> when the data directory holds unexcused entries, and the error names them. The
> operator moves the foreign bytes out and retries. Fail-closed; matches CockroachDB's
> refusal to init into a directory that is not its own. Candidate rule (b) (a second
> confirmation flag) rejected. Breaking change to a shipped flag accepted — pre-release.

## Parent

Issue-31 migration design doc
(`.scratch/cluster-correctness/2026-08-14-issue31-migration-design.md`), V25-m6: the
force-fresh-beside-foreign hazard was flagged as an owed follow-up during the Quint rework review.

## What is wrong

`--force-fresh-data-dir` today is an *override*: in
`frogdb-server/crates/recovery/src/data_dir.rs` it turns the "files but no FrogDB marker" refusal
(`data_dir.rs:110-121`) into an adopt-and-restamp, and the unreadable-marker refusal
(`data_dir.rs:67-75`) into a warn-and-restamp. That means one flag, meant for "let a genuine first
boot proceed", will also silently claim a directory full of somebody else's bytes — a mistyped
`persistence.data-dir`, a lost bind mount, another database's volume — minting a FrogDB identity
over foreign data. The two situations ("this really is my first boot" and "these bytes are not
mine") are indistinguishable to the flag, so it must not resolve both.

## What to build

1. Re-scope the flag as a **fresh-start tool**: with `--force-fresh-data-dir`, a data directory
   that holds unexcused entries (same "foreign files" probe as `contains_foreign_files` — FrogDB's
   own `staging`/`backup`/marker artifacts stay excused) causes a refusal whose error **names the
   offending entries**. No override path remains for the foreign-files case; the operator moves
   the bytes out and retries.
2. Reconcile the flag's other arms spec-first with the same fail-closed principle:
   - unreadable-marker + force (`data_dir.rs:67-75`): decide whether restamping stays (directory
     is excused-only) or also refuses; row the outcome.
   - `require-existing-data` + force first-boot escape (`data_dir.rs:146-153`): unchanged in
     intent (empty dir has no foreign entries) — verify and keep forced.
3. Update the refusal message at `data_dir.rs:110-121` (it currently advertises the flag as the
   adopt path) and the flag's config/docs text (`frogdb-server/crates/config/src/persistence.rs`,
   website docs if the flag is documented there).
4. Spec-first: amend the FM-PERSISTENCE-048..052 family — the "adopt it" observable becomes
   "refuse and name entries"; forcing test: foreign file in data dir + `--force-fresh-data-dir` →
   boot refuses, error names the file.

## Acceptance criteria

- [x] `--force-fresh-data-dir` with unexcused entries present refuses; error names the entries
      (forcing test)
- [x] Excused-only directories (staging/backup/marker artifacts, pending install) still boot
      as today; existing FM-PERSISTENCE-057/059 tests stay green
- [x] FM rows updated; `just lint-spec` green
- [x] Flag help/docs updated to fresh-start-tool wording
- [ ] `just mutants-diff frogdb-recovery` triaged on touched code (run by the parent session
      before push)

## Blocked by

None - can start immediately.

## Resolution

Shipped on `worktree-agent-a7b3a543b2c66054d`.

**Probe.** `frogdb_persistence::data_dir::contains_foreign_files` became
`foreign_files(dir, limit) -> io::Result<Vec<PathBuf>>`: same walk, same excusals, but it
*names* what it finds (paths relative to the data directory) and stops at `limit`, so the
first-file-exists cost bound survives. The excusal predicate grew from
`DataDirLayout::is_install_scratch` to `is_own_artifact` — the install scratch *plus* the
marker file. The marker only ever reaches the probe on the unreadable-marker branch (a
readable one settles the verdict first), and counting FrogDB's own byte as foreign there
would have left no directory the flag could re-stamp. `frogdb_data_dir.tmp` stays
unexcused, as FM-PERSISTENCE-049 requires.

**Foreign-entries arm.** `verify` now bails whenever the probe returns a non-empty list,
with no `force` in the condition at all. The message names up to eight entries ("and more"
beyond that) and says in so many words that `--force-fresh-data-dir` does not override it.

**Unreadable-marker arm, ruled.** Restamp only when the directory is excused-only;
otherwise refuse, naming the entries and still reporting the marker's read error. Rowed in
FM-PERSISTENCE-050 (observable, NOT-observable, invariant) — an unreadable marker says
nothing about who wrote the rest of the directory, so the re-stamp is gated on the same
probe as FM-PERSISTENCE-051.

**`require-existing-data` arm.** Unchanged and re-verified: an empty directory (pre-created
mount points included) has no foreign entries, so the flag's provisioning escape never
collides with the new refusal. `force_fresh_data_dir_overrides_require_existing_data` now
seeds a `lost+found` to force the point.

**Forcing tests.** `force_fresh_data_dir_refuses_beside_foreign_entries` (FM-051),
`force_fresh_data_dir_refuses_to_re_stamp_beside_foreign_entries` (FM-050),
`force_fresh_data_dir_re_stamps_a_corrupt_marker` (FM-050, re-seeded excused-only),
`an_unrelated_file_in_the_data_dir_refuses_the_boot` (FM-051, now asserts the named entry),
`foreign_files_names_entries_up_to_the_limit` and
`foreign_files_ignores_the_install_scratch_but_not_the_database` in `frogdb-persistence`.
`force_fresh_data_dir_adopts_an_unmarked_directory` is gone — the behavior it forced no
longer exists.

**Docs.** CLI help, `PersistenceConfig` doc comments,
`website/src/content/docs/operations/persistence.md` (the "Adopting a directory FrogDB did
not stamp" section split into "A directory holding entries FrogDB did not write" and
"Repairing an unreadable marker"), `backup-restore.md`, and the regenerated
`config-reference.json` / `frogdb-server-cli.json` / website spec pages.
