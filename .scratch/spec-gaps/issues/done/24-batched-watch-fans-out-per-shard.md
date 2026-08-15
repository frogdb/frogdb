# 24: Batched WATCH fans out per shard — argument packing stops changing semantics

Status: done

## Origin

Distsys-review MAJ-22 (`.scratch/formal-spec/2026-08-13-independent-distsys-review.md`),
ruled **code matches spec** by the user 2026-08-14
([rulings ledger](../../../formal-spec/2026-08-13-distsys-review-rulings.md)).

## What is wrong

`handle_watch`
(`frogdb-server/crates/server/src/connection/transaction_conn_command.rs:285-293`)
runs `SlotValidator::same_shard(args, num_shards)` before anything else and returns
`redirect::crossslot()` (`slot_migration/validator.rs:31-44`) when a multi-key
WATCH's keys land on different *internal shards* — in every mode, standalone
included, before any cluster snapshot is consulted.

This is a code defect against the locked spec, not a spec gap:

- FM-TXN-049 (`specs/txn.md:~897`) declares exactly this reply **NOT observable**:
  "a `-CROSSSLOT` for a watch set legitimately spanning two slots this node owns
  (only the *queue* is co-location-constrained, FM-TXN-019 — watch sets are not)".
- The two-call path proves cross-shard watch sets fully work: `WATCH {a}x` then
  `WATCH {b}y` builds the set and reaches EXEC — FM-TXN-020's own scenario.
  `WATCH {a}x {b}y` must mean the same thing; semantics cannot depend on argument
  packing (a batching client library fails where a one-key-per-call library
  succeeds).
- Standalone Redis allows multi-key WATCH freely — the refusal is a silent parity
  regression with no benefit.

Existing coverage misses it: `slot_migration/tests.rs:598`
(`watch_across_two_slots_is_crossslot`) exercises only the cluster
`route_watched_keys` path (not-owned slots — still correct behavior), not the
`same_shard` pre-check.

## What to build (spec-first; txn locked, gate 0.90)

1. Spec: FM-TXN-049 already states the contract — add/point its forcing test at
   the batched path (the row's test citations must include the new test).
   No row semantics change.
2. Code: `handle_watch` drops the `same_shard` pre-check; the `GetVersion`
   round-trip fans out per shard (one message per distinct shard in the key set),
   watch registrations land per key exactly as the sequential path produces.
   Cluster-mode not-owned-slot checks (`route_watched_keys`) are untouched.
3. Forcing tests:
   - Batched ≡ sequential: `WATCH {a}x {b}y` (keys on different internal shards)
     succeeds, builds the same watch set as two single-key WATCHes, EXEC reachable
     and CAS-correct (fails pre-fix: `-CROSSSLOT`).
   - Standalone multi-key WATCH across shards → `+OK` (parity).
   - Cluster mode, key in a slot the node does not own → existing redirect
     behavior unchanged (`slot_migration/tests.rs:598` stays green).

## Cross-references

- MAJ-23 / [issue 25](25-take-folds-only-live-watched-shards.md) (if filed): same
  watch-set machinery (`take` folding watched shards) — coordinate, ideally one
  implementer.
- [Issue 23](23-watch-epoch-bump-becomes-per-slot.md): WATCH version semantics —
  independent, but both touch watch registration paths.
- FM-TXN-019/021: the *queued batch* co-location constraint is separate and
  remains — only WATCH is freed.

## Acceptance criteria

- [ ] FM-TXN-049 forcing test covers the batched path; `just lint-spec` green
- [ ] `same_shard` pre-check gone from `handle_watch`; per-shard `GetVersion`
      fan-out
- [ ] Batched-≡-sequential forcing test fails pre-fix, passes post-fix
- [ ] Cluster not-owned redirect behavior unchanged
- [ ] `just mutants-diff` on frogdb-txn + touched server paths (gate 0.90) triaged

## Blocked by

None — coordinate with MAJ-23's issue if both ride the same wave.
