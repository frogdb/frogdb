# 33: Publishing renames share one fsync-bracketed helper — stager stops publishing undurable bytes

Status: ready-for-agent

## Origin

Distsys-review MAJ-19 (`.scratch/formal-spec/2026-08-13-independent-distsys-review.md`),
ruled accept-and-file by the user 2026-08-14
([rulings ledger](../../../formal-spec/2026-08-13-distsys-review-rulings.md)).

## What is wrong

FM-PERSISTENCE-023 states the global rule: every publishing rename is bracketed
(fsync contents before, fsync parent after). The full-sync stager breaks it:
`frogdb-server/crates/replication/src/fullsync/stager.rs:~110-135` does
`remove_dir_all` then `fs::rename` into `checkpoint_ready` with **no `sync_file` on
the payload and no `sync_dir` on the parent**. The staged checkpoint becomes
*visible* before its bytes are durable.

Failure: power loss after the rename's metadata is durable but before the SST and
CURRENT contents are → the next boot's `is_complete_db()` check passes (the names
are all there) and the install proceeds, writing a **partial database over the live
one**. Distinct from spec-gaps issue 04 (install-side verification): directory
metadata can be durable while file contents are not, so no install-side name check
detects this — only writer-side fsync-before-publish does.

The persistence crate already implements the discipline correctly
(`persistence/src/rocks/checkpoint.rs:110-118` brackets both install renames) — the
replication crate reimplemented the pattern and dropped the syncs. TigerBeetle
documents this discipline at length; FoundationDB enforces it in file-backed stores.

## What to build (spec-first; replication + persistence locked, gates 0.85)

1. Spec rows first:
   - FM row (replication or persistence spec, wherever the stager's contract lives):
     "the full-sync stager's publish rename is bracketed" — NOT observable: a
     `checkpoint_ready` whose name is durable but whose contents are not.
   - Cite FM-PERSISTENCE-023 as the parent rule; the row names the shared helper as
     the mechanism.
2. Code: extract ONE shared bracketed-rename helper — fsync every payload file,
   fsync the source dir, rename, fsync the parent — and route **both** call sites
   through it (`rocks/checkpoint.rs` install renames, `fullsync/stager.rs` publish
   rename), so the two crates cannot drift again. Place it where both crates can
   reach it without a dependency inversion (persistence crate exports it, or a
   shared fs-util module — follow the existing crate graph).
3. Forcing test: `RecordingFs` trace test on the full-sync staging path mirroring
   the existing `stamp_with` guard — asserts the fsync-rename-fsync sequence; fails
   pre-fix (no syncs recorded).
4. Seam-lint candidate (follow-up-sized, include if cheap): "every publishing
   rename goes through the helper" — grep-able chokepoint gate in the
   `agents/seam-lints.md` family; file a follow-up issue if not landed here.

## Cross-references

- [Spec-gaps issue 21](../../../spec-gaps/issues/open/21-staging-and-backup-live-inside-the-data-dir.md):
  moves the staging paths inside the data dir — build the helper against the new
  layout (or land the helper first and let 21 re-point paths; either order, one
  helper).
- [Spec-gaps issue 20](../../../spec-gaps/issues/open/20-mid-install-crash-recovery-preserves-database-id.md):
  the mid-install probe must be able to trust that a present `checkpoint_ready` is
  complete — this issue is what makes that trust sound.
- Spec-gaps issue 04 (install-side verification): complementary, not overlapping —
  writer-side durability here, reader-side verification there.

## Acceptance criteria

- [ ] FM row landed; `just lint-spec` green
- [ ] One shared helper; both call sites routed through it
- [ ] `RecordingFs` trace test fails pre-fix, passes post-fix
- [ ] `just mutants-diff` on touched locked crates (replication/persistence, 0.85)
      triaged

## Blocked by

None — coordinate ordering with spec-gaps issue 21.
