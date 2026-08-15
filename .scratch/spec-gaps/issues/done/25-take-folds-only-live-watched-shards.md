# 25: `take` folds only *live* watched shards — dead watches stop forcing Multi

Status: done

## Origin

Distsys-review MAJ-23 (`.scratch/formal-spec/2026-08-13-independent-distsys-review.md`),
ruled **code matches spec** by the user 2026-08-14
([rulings ledger](../../../formal-spec/2026-08-13-distsys-review-rulings.md)).

## What is wrong

The locked spec says it twice — `specs/txn.md:30` (state-space `txn.slots.target`:
"**also** `fold_shard` on every *live* watched shard") and FM-TXN-020's Invariant
(`:554`: "`take` folds every *live* watched shard into the target") — but the code
folds unconditionally (`frogdb-server/crates/txn/src/state.rs:285-287`):

```rust
for &(shard_id, _, _) in self.watches.values() {
    self.slots.fold_shard(shard_id);
}
```

The `live_at_watch` component is destructured away; no `fold_shard` call site in
`crates/txn/src` filters on it.

Client-visible break, standalone included (FrogDB shards standalone): the canonical
create-if-absent CAS — `WATCH counter` (nonexistent, `live_at_watch = false`,
shard B), `MULTI; SET other 1; EXEC` (`other` on shard A) — should commit per spec
(dead watch contributes nothing); the code folds B anyway → target `Multi` →
`-CROSSSLOT` at EXEC.

Test gap: today's two tests (`cross_shard_watch_set_folds_to_multi_at_take`,
`take_transaction_folds_cross_shard_watch_set_to_multi`) use live watches only —
they cannot distinguish the two implementations, so no mutant on the filter is
killable.

## Safety hazard the fix must pin (dead → live)

Filtering a dead watch's shard from the fold is safe only if the EXEC-time watch
check still reaches that shard: a watched-nonexistent key *created* by another
client between WATCH and EXEC must abort the CAS. With shard B unfolded, EXEC may
take the single-shard path on A — and
[issue 11](11-exec-fast-path-ignores-watch-set.md) shows the fast path ignores the
watch set today. Landing this filter without that check converts a spurious
`-CROSSSLOT` into a **missed abort** — strictly worse. Sequencing: issue 11's
fast-path watch check lands first or together with this.

## What to build (spec-first; txn locked, gate 0.90)

1. Spec: FM-TXN-020's row gains forcing-test citations that distinguish live from
   dead cross-shard watches (row text already correct — no semantics change).
2. Code: `take` filters on `live_at_watch` — only live watched shards fold into
   the target. Verify (and pin with a test) that EXEC watch verification covers
   **all** watched shards, folded or not.
3. Forcing tests:
   - Create-if-absent CAS: dead watch on shard B + queued write on shard A →
     EXEC commits (fails pre-fix: `-CROSSSLOT`).
   - Live cross-shard watch still folds to Multi (existing behavior pinned —
     mutants on the filter now killable in both directions).
   - Safety: dead watch on shard B, key created by another client before EXEC →
     EXEC aborts (nil), even with shard B unfolded. Runs against the
     issue-11-fixed fast path.

## Cross-references

- [Issue 11](11-exec-fast-path-ignores-watch-set.md): **sequencing dependency** —
  fast-path watch check must land first or together; the safety test above is the
  shared witness.
- [Issue 24](24-batched-watch-fans-out-per-shard.md): same watch-set machinery
  (registration side); ideally one implementer takes 24 + 25.
- FM-TXN-033 gap-4 clause: the dead-stays-dead never-aborts guarantee this filter
  relies on.

## Acceptance criteria

- [ ] FM-TXN-020 forcing tests distinguish live vs dead; `just lint-spec` green
- [ ] `live_at_watch` filter in `take`; create-if-absent CAS commits cross-shard
- [ ] Dead→live safety test passes with the shard unfolded
- [ ] `just mutants-diff` on frogdb-txn (locked, 0.90) triaged — filter mutants die

## Blocked by

[Issue 11](11-exec-fast-path-ignores-watch-set.md) (fast-path watch check) — land
first or together.
