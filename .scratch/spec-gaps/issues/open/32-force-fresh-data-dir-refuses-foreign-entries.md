# --force-fresh-data-dir refuses beside foreign entries (fresh-start tool, not override)

Status: ready-for-agent

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

- [ ] `--force-fresh-data-dir` with unexcused entries present refuses; error names the entries
      (forcing test)
- [ ] Excused-only directories (staging/backup/marker artifacts, pending install) still boot
      as today; existing FM-PERSISTENCE-057/059 tests stay green
- [ ] FM rows updated; `just lint-spec` green
- [ ] Flag help/docs updated to fresh-start-tool wording
- [ ] `just mutants-diff frogdb-recovery` triaged on touched code

## Blocked by

None - can start immediately.
