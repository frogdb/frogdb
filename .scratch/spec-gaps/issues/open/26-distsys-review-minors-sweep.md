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

### MIN-7 — backup dirs wall-clock-named, `unwrap_or(0)` pruning

Ruling: **full counter redesign** — graduated to
[issue 27](27-backup-naming-monotonic-counter-prune-refuses-unparseable.md)
(persisted monotonic counter naming + prune refuses unparseable entries).
No checklist entry here; issue 27 carries the work.

### MIN-8 — TR-BLOCKING-003's current-code cell misdescribes the client-visible shape

Ruling: **correct cell now** (spec-only). The cell claims a dropped sender
resolves "exactly as an ordinary timeout would (TR-BLOCKING-009)" — false:
timeout yields `op.timeout_reply()` (`*-1` for BLPOP, `coordinator.rs:101`),
the drop yields RESP nil `$-1` (`coordinator.rs:40`, `response.rs:285`). State
the shape difference honestly and cross-ref
[issue 08](08-blocking-command-rows.md)'s MAJ-11 amendment (sender-drops
eliminated as signaling; drop then uniquely means channel death →
`-ERR shard unavailable`) as the behavioral fix that rewrites this row. Spec
never lies in the interim; this sentence was hiding the admission-limit bug's
severity (observability-accuracy principle).

- [ ] TR-BLOCKING-003 cell corrected ($-1 vs `*-1` stated, issue 08 cross-ref);
      `just lint-spec` green

## Acceptance criteria

- [ ] Every checklist entry above resolved as ruled
- [ ] `just lint-spec` green after all spec edits
- [ ] `just mutants-diff` triaged for any code-touching entry

## Blocked by

None — can start immediately.
