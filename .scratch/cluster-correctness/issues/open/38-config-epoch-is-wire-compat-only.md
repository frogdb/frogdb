# 38: `config_epoch` is wire-compat only — spec says so, dead arbitration code goes

Status: ready-for-agent

## Origin

Distsys-review MAJ-10 (`.scratch/formal-spec/2026-08-13-independent-distsys-review.md`),
ruled **wire-compat honesty** by the user 2026-08-14
([rulings ledger](../../../formal-spec/2026-08-13-distsys-review-rulings.md)).

## What is wrong

FM-CLUSTER-010/011/012 (locked) protect the monotonicity and collision behavior of
`config_epoch` — a value no routing, ownership, or admission path reads (no consumer
outside `types.rs`/`wire.rs` rendering and tests). The rows are true but vacuous, and
they mislead twice:

1. A reader (and the mutation gate) is given false confidence that ownership conflicts
   are epoch-arbitrated. They are not — and they need not be: FrogDB derives slot
   ownership and topology from **raft consensus**, a single linearized log. Redis needs
   epoch arbitration precisely because it has no consensus layer
   (`clusterUpdateSlotsConfigWith` genuinely arbitrates on epoch). Wiring a parallel
   epoch arbiter beside raft would be a second conflict resolver — redundant when raft
   is healthy, dangerous wherever the two could disagree.
2. The collision-resolution rule renumbers the *newcomer* upward — if ever wired into
   arbitration, a late-arriving node would beat an established owner, the opposite of a
   fencing epoch (Redis bumps the lexicographically smaller id, deterministically).
   Dead code whose semantics are wrong-direction invites future misuse.

## What to build (spec-first; cluster locked, gate 0.80 — locked-row edits go failure-mode row → forcing test → change)

1. Amend FM-CLUSTER-010/011/012: each row states plainly that slot ownership and
   topology authority derive from raft consensus; `config_epoch` exists solely for
   Redis wire compatibility (`CLUSTER NODES` / `CLUSTER SHARDS` / `CLUSTER SLOTS`
   rendering). Keep whatever monotonicity the wire rendering genuinely needs; drop or
   re-scope claims implying arbitration significance.
2. Redis deviations table row: FrogDB does not use `config_epoch` for ownership
   arbitration; conflicts cannot arise because topology is consensus-ordered
   (deviation-as-improvement rationale in the row).
3. Delete the newcomer-renumber collision machinery as dead arbitration code (it has
   no consumer; its direction is wrong if ever revived). If some collision handling
   must remain for wire realism, it follows Redis's smaller-id-bumps rule and the row
   says it is cosmetic.
4. Tests: re-point the existing epoch tests at the honest claims (rendering
   monotonicity, wire shape); forcing tests for the amended rows per spec-first flow.
5. Website/docs: cluster docs mentioning epochs updated to the raft-authority
   statement.

## Cross-references

- Testing-gap rework "epoch-fold" (done) touched this machinery; check its tests for
  assertions that pin arbitration-flavored behavior and re-derive them.
- [Issue 31](31-slot-migration-redesign-source-authoritative-until-commit.md):
  migration redesign restates ownership transfer; keep the raft-authority language
  consistent.

## Acceptance criteria

- [ ] FM-CLUSTER-010/011/012 amended; deviations row added; `just lint-spec` green
- [ ] Newcomer-renumber arbitration code deleted (or demoted to documented-cosmetic
      with Redis-direction semantics)
- [ ] Tests re-pointed; forcing tests for amended rows
- [ ] Docs updated
- [ ] `just mutants-diff` on frogdb-cluster (locked, 0.80) triaged

## Blocked by

None — can start immediately.
