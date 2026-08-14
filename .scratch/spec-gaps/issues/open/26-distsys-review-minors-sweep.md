# 26: Distsys-review minors sweep (persistence + txn)

Status: ready-for-agent

## Origin

Minor findings from the independent distsys review
(`.scratch/formal-spec/2026-08-13-independent-distsys-review.md`), ruled one at a
time by the user 2026-08-14
([rulings ledger](../../../formal-spec/2026-08-13-distsys-review-rulings.md)).
One sweep issue per area tracker; this one collects persistence and txn minors.
Locked gates apply per touched crate (persistence 0.85, txn 0.90) — spec edits run
`just lint-spec`; code changes get `just mutants-diff` triage.

## Ruled minors

### MIN-6 — `wal_watermark::write`'s no-torn-body reasoning silently depends on ext4 `auto_da_alloc`

Ruling: **add checksum** (filesystem-independent beats named assumption —
Kubernetes-primary deployment means operators pick filesystems; XFS/btrfs/
ext4-`data=writeback` all break the current argument). Watermark body gains a
checksum; a torn or corrupt body parses as **absent**, never as garbage. The
`wal_watermark` rows in `specs/persistence.md` restate the argument
filesystem-independently (the ext4 dependency disappears rather than being
documented). Forcing test: corrupted/truncated body → treated as absent
(fails pre-fix only on non-ext4 semantics, so the test corrupts bytes directly).

- [ ] Checksum in watermark body; torn/corrupt → absent
- [ ] Rows restated filesystem-independent; `just lint-spec` green
- [ ] Corruption forcing test landed

## Acceptance criteria

- [ ] Every checklist entry above resolved as ruled
- [ ] `just lint-spec` green after all spec edits
- [ ] `just mutants-diff` triaged for any code-touching entry

## Blocked by

None — can start immediately.
