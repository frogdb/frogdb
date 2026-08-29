# 38: Replica stamps — floors unify into RocksDB, R15/R16 retire

Status: ready-for-agent

Blocked by issue 37.

Parent: [PRD 36](./36-offset-stamped-batches-restart-bias.md), ruling R22 (campaign
ledger 2026-08-22). Depends on [37](./37-mint-at-persist-and-primary-stamps.md) (the
stamp seam).

## Scope

1. **Replica per-frame stamping.** The replica knows a frame's end offset before applying
   it (issue-34/35 machinery). Plumb that offset into the persist seam 37 built — the
   primary mints there, the replica *supplies*. Each applied frame's batch carries the
   shard's stamp; the replica's exact applied coverage is now in RocksDB, atomic with the
   effects.
2. **Install adopts floors from the artifact.** An installed checkpoint already contains
   the primary's stamp keys, so the floors arrive with the data — crash-safe by
   construction. The trailer `ShardCoverage` stays the wire form: on the live-dataset
   path (no artifact) install writes the trailer's values as stamps; on the checkpoint
   path assert trailer == artifact stamps (a mismatch is a corrupt payload — refuse, same
   discipline as the 4-field-trailer refusal).
3. **R15 plumbing retires.** Delete the staged `replication_metadata.json` coverage
   vector and `ReplicationState.coverage_at_save` (`state.rs:216-223`) — floors are read
   from the CFs at boot. One source of truth.
4. **R16 refusal retires.** A crash-recovered replica stint reconstructs exact floors and
   an exact applied head from the stamps, so `OffsetProvenance::Recovered`'s unconditional
   window-grant refusal is deleted; `window_grant_verdict` keys on the reconstructed
   floors like any other stint.

## Spec / model

- FM-REPLICATION-066 rewritten: floors sourced from stamps; R15/R16 clauses replaced;
  forcing tests updated (the R16 tests invert — a recovered stint with stamps is granted
  what its floors allow).
- FM-PERSISTENCE-039 (staged metadata) row updated for the deleted vector.
- Model (R24): restart transitions on the replica — lose unflushed tail, recover stamps,
  reconstruct floors; invariant: recovered stint's refusal/grant decisions equal a
  never-crashed stint's at the same coverage. Battery rows.

## Acceptance

- [ ] Replica frame applies stamp their shard, batch-atomic; crash mid-stint recovers
      exact applied head + floors (forcing test: non-idempotent write applied, crash,
      recover, reconnect — replayed exactly once with no R16 refusal involved)
- [ ] Install adopts artifact stamps; live path writes trailer values as stamps;
      checkpoint-path trailer/artifact mismatch refused
- [ ] `coverage_at_save` + staged vector deleted; older state files still parse
      (serde default) and are treated as no-floors
- [ ] R16 refusal deleted; verdict seam tests updated
- [ ] Spec + model + battery; `just lint-spec` green; `just mutants-diff
      frogdb-replication` and `frogdb-persistence` before push
