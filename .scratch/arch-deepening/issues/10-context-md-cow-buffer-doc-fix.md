# 10 — Fix CONTEXT.md Snapshot/COW Buffer entries to match the forkless-checkpoint implementation

Status: ready-for-human

## What to build

`frogdb-server/CONTEXT.md` describes Snapshots via an in-memory **COW Buffer** "holding
pre-snapshot values for keys written during a snapshot; counted toward `maxmemory`", and the
Relationships section says a Snapshot "diverts concurrent writes to the COW Buffer". The
implementation has no such buffer: Snapshots are forkless via a RocksDB checkpoint at a
sequence number plus `pre_snapshot_hook`; nothing snapshot-related is counted toward
`maxmemory`.

Doc-only change: rewrite the **COW Buffer** and **Snapshot**/**Snapshot Epoch** glossary
entries and the Relationships bullet to describe the checkpoint-at-sequence mechanism (either
retire the COW Buffer term or redefine it to what actually exists). Per repo doc rules, sync
the website glossary (`website/src/content/docs/architecture/glossary.md`) and fix any links —
follow `/doc-sync` conventions.

## Acceptance criteria

- [x] CONTEXT.md Snapshot/Snapshot Epoch/COW Buffer entries match the implementation (reviewed against `persistence/src/snapshot/`)
- [x] Relationships bullet corrected
- [x] Website glossary synced; no dangling links (run link check if available)
- [x] No implementation changes

## Blocked by

None - can start immediately

## Source

Round-8 P06 agent report; discrepancy verified against `rocks_coordinator.rs` during proposal 06.

## Comments

**2026-07-20**: Implemented. Verified the real mechanism against
`frogdb-server/crates/persistence/src/snapshot/rocks_coordinator.rs` (`RocksSnapshotCoordinator`,
`PreSnapshotHook`, `spawn_run`/`run_loop`), `.../snapshot/stager.rs` (`SnapshotStager::run` /
`stage_checkpoint`, which calls `rocks.latest_sequence_number()` then
`rocks.create_checkpoint()`), `.../snapshot/scheduler.rs` (`SnapshotScheduler` — the epoch is
just a generation counter for naming/coalescing runs), `.../snapshot/metadata.rs`
(`SnapshotConfig` has no COW/memory field), and
`frogdb-server/crates/persistence/src/rocks/checkpoint.rs` (`create_checkpoint` /
`latest_sequence_number` are thin wrappers over `rocksdb::checkpoint::Checkpoint`). Also
confirmed `set_pre_snapshot_hook` is wired in `frogdb-server/crates/server/src/server/init.rs`
to flush search indexes and persist the replication offset before the checkpoint is cut.
Grepped the whole repo for `total_memory_used` and `cow-buffer-max-bytes` /
`cow-memory-abort-threshold` config keys referenced by the old docs — none exist in code.

Rewrote:
- `frogdb-server/CONTEXT.md`: **Snapshot**, **Snapshot Epoch** entries rewritten; **COW Buffer**
  entry retired and replaced with a **Pre-Snapshot Hook** entry (documents the real
  `PreSnapshotHook` type); Relationships bullet corrected to describe
  epoch-claim → pre-snapshot-hook → checkpoint-at-sequence, with writes never diverted/buffered.
- `website/src/content/docs/architecture/glossary.md`: **Snapshot** and **Snapshot Epoch**
  entries rewritten to the same mechanism; the **COW (Copy-on-Write)** heading was removed
  (nothing in the repo linked to its anchor).
- `website/src/content/docs/architecture/persistence.md`: rewrote the "Forkless Snapshot
  Algorithm" section (previously described a 4-step epoch/COW-buffer/`total_memory_used()`
  algorithm that doesn't exist) to the real 4-step scheduler → hook → checkpoint → atomic-install
  sequence, and dropped the fictional "Eviction during snapshot" table (`cow-memory-abort-threshold`
  isn't a real config key).
- `website/src/content/docs/architecture/storage.md`: renamed "Total Memory (Including COW
  Buffers)" to "Memory During Snapshots" and rewrote it — no anchor to the old heading existed
  anywhere in the repo, so the rename is safe. Added a link to the persistence.md section.
- `website/src/content/docs/operations/persistence.md`: rewrote "How Snapshots Work" and
  "Memory During Snapshots" (dropped the fictional per-scenario COW-buffer-size table).

Grepped the whole repo for "COW Buffer" / "cow" doc references afterward. Two intentional
leftovers, both out of scope per the repo's `/doc-sync` skill (`todo/**` and historical
`.scratch/**` proposals are excluded from doc-sync's scope):
- `.scratch/arch-deepening/proposals/06-snapshot-scheduler.md` — the original proposal report
  that first flagged this exact discrepancy; left as an accurate historical record.
- `todo/**` (`regression-test-audit.md`, `proposals/{02,11,19}-*.md`, `proposals/INDEX.md`) —
  all "COW" hits there are an unrelated concept (typed-accessor "COW-before-check" value clones
  on wrong-type key access, e.g. TOPK.ADD), not the snapshot buffer this issue is about.
- `.claude/skills/distributed-audit/SKILL.md` line 132 mentions "snapshot COW semantics
  violations" in a generic test-idea list; left alone as out of scope (skills are maintained
  separately per `/doc-sync`) and vague enough not to assert the retired mechanism specifically.

No Rust/implementation changes were made. Did not run the full `just docs-link-check` (requires
a website build); instead manually verified no doc in the repo links to the anchors that
changed or were removed (`#cow-copy-on-write` in glossary.md, `#total-memory-including-cow-buffers`
in storage.md, `#how-snapshots-work` / `#memory-during-snapshots` in operations/persistence.md) —
none were referenced anywhere.
