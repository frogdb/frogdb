# 37: Mint at persist — log-first offsets and primary-side per-shard stamps

Status: ready-for-agent

Parent: [PRD 36](./36-offset-stamped-batches-restart-bias.md), rulings R18/R19/R23
(campaign ledger 2026-08-22). First of the four sub-issues; 38, 24, 39 depend on it.

## Scope

1. **Move the mint+enqueue to the persist point.** Today the offset `fetch_add` and the
   backlog append live in `broadcast_tagged` at effect 8 (`primary/mod.rs:946-963`,
   `offset_coordinator.rs:108`); WAL staging is effect 6 (`post_execution.rs:385-405`);
   under `should_confirm` the durable persist precedes all effects
   (`execution.rs:463-496`). After this issue: the replication form is computed and the
   offset minted + frame enqueued *before* staging, in one critical section (mint order
   must remain wire order — the fusion is load-bearing, spec it). Effect 8
   (`ReplicationBroadcast`) reduces to bookkeeping (waiter/ack plumbing that genuinely
   must follow effects); rename or restructure `WRITE_EFFECT_ORDER` so the doc matches
   reality.
2. **Count always, enqueue when active (R19).** Drop the `is_active()` gate on the
   *counter*: every replicable write advances the offset by its form length on every
   node; `ReplicationOverride::Suppress` / `CommandFlags::NO_PROPAGATE` forms advance by
   0. Backlog append and socket feed stay gated on activity. Assert: a standalone node
   allocates no backlog.
3. **Per-shard stamp, batch-atomic (R23).** Each shard's `WriteBatch` gains a stamp key —
   "max offset in this batch" — in a **reserved per-shard metadata CF** (new CF or
   existing reserved tier; `search_meta_<n>` precedent, `columns.rs:18-77`), written via
   the staging seam so it commits in the same batch (`RocksSink::commit`,
   `flush.rs:181-220`; WriteBatch atomicity spans CFs). The persist seam takes the offset
   as a parameter (issue 38 will have the replica supply the frame offset through the same
   seam). Under `should_confirm`, the pre-effect persist carries the stamp — ack covers
   data+stamp atomically.
4. **Recovery reads stamps.** RocksDB replays its own WAL to the last committed batch, so
   a per-CF get of each shard's stamp key after open yields exact flushed coverage. Wire
   this into recovery as the coverage source (the *claims* built on it — identity
   rotation, floors — are issues 24 and 38; this issue only has to surface the vector).

## Constraints

- The stamp value is "highest offset in whatever this batch held" — single-action writes
  are not group-bracketed (`persistence.rs:271`) and share batches; that is fine for a
  watermark, spec it as such.
- Commit failure drops the staged set and the stamp together (`flush.rs:112-126`) —
  atomicity holds in both directions; add the forcing test.
- Do not touch `wal_watermark.rs` (RocksDB sequence numbers, FM-PERSISTENCE-035 — a
  different quantity).
- Cross-shard groups: one batch per shard, each carrying only its own CF writes
  (`persistence.rs:253-308`) — per-shard stamps reflect exactly the per-shard halves;
  issue 35's torn-group mend argument depends on this, keep it true.

## Spec / model

- New FM row(s): stamp-data atomicity (both directions), mint-order = wire-order under the
  moved mint, count-always semantics. Forcing tests in the owning locked crates
  (frogdb-persistence for the batch atomicity, frogdb-replication for mint/wire order —
  `cargo mutants -p <crate>` runs only that package's own tests).
- `replication.md:360` clause flips to implemented.
- FM-REPLICATION-004's exact-pairing companion cell updated to name the stamps.
- Model: extend `replication_fullsync*.qnt` state with per-shard stamps advancing with
  applies/flushes; invariant `claim == coverage` at flush points. (Restart transitions are
  issue 24/38's model work per R24 — this issue lays the state.)
- Battery rows for new guards; `just quint-run` witnesses; `just lint-spec` green.

## Acceptance

- [ ] Mint+enqueue at persist, fused; effect-8 broadcast reduced to bookkeeping; wire
      order forced by test (two shards racing, frames arrive in mint order)
- [ ] Sync-durability path: ack covers data+stamp atomically (forcing test: crash after
      ack ⇒ recovered stamp ≥ acked write's offset)
- [ ] Count-always: standalone node advances offsets, allocates no backlog; suppressed
      forms advance 0
- [ ] Stamps in reserved CF, batch-atomic both directions; recovery surfaces exact
      per-shard coverage
- [ ] Spec rows + model state + battery; `just lint-spec` green;
      `just mutants-diff frogdb-persistence` and `frogdb-replication` (locked, 0.85)
      before push
