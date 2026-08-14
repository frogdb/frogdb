# 12: `wal_watermark` records the covered sequence, not a post-hoc global read

Status: ready-for-agent

## Origin

Distsys-review CRIT-3 (`.scratch/formal-spec/2026-08-13-independent-distsys-review.md`),
ruled accept-and-file by the user 2026-08-14
([rulings ledger](../../../formal-spec/2026-08-13-distsys-review-rulings.md)).

## What is wrong

The WAL watermark is supposed to be a **lower bound on what is durable**;
FM-PERSISTENCE-035 declares a false alarm (recovery reporting "lost acknowledged writes"
for writes never acknowledged as durable) impossible. `record_wal_watermark()`
(`persistence/src/rocks/mod.rs:345`) instead does
`let seq = self.db.latest_sequence_number();` — a post-hoc **global** read, not the
sequence the fsync actually covered. Two reachable paths make the watermark lead the
durable point:

- **`periodic` mode**: `durable_sync` runs `flush()` → `publish_synced_through` →
  `record_wal_watermark()`. `flush()` is a sequential per-CF `flush_cf` loop
  (`rocks/mod.rs:582-588`), so a commit landing in shard 0 *after* shard 0's `flush_cf`
  returned is WAL-only — yet the subsequent global `latest_sequence_number()` includes it.
- **`sync` mode, multi-shard**: `record_wal_watermark()` runs after `write_batch_opt` on
  one of N shard threads sharing one `RocksStore`; a concurrent shard's write — or a
  cluster-metadata write (`cluster/src/storage.rs:393` `put_cf_opt`) — bumps the global
  sequence between the fsync and the read.

On power loss or kernel panic, `detect_and_reset` compares the recovered sequence to the
inflated watermark, increments `frogdb_wal_recovery_dropped_records_total`, and logs
`WARN … sequence numbers of acknowledged writes were lost` — for writes `periodic` mode
never acknowledged as durable. That is the exact outcome FM-PERSISTENCE-035 rules out,
and it trains operators to ignore the one alarm that means real data loss
(observability-accuracy principle: misleading data is not ok). The window fires only on
power loss / kernel panic, never on `SIGKILL`, so no process-crash test in the suite can
catch it.

**The named forcing test pins the bug**:
`only_a_synced_rocks_sink_commit_records_the_durable_watermark`
(`persistence/src/wal/tests.rs:1655-1686`) asserts
`watermark == latest_sequence_number()`, so the spec↔test lint link is satisfied by a
test that encodes the wrong semantics.

Mature-system shape: RocksDB separates `FlushWAL(sync)` from `GetLatestSequenceNumber()`
and exposes `WriteBatchInternal::Sequence(batch)` so callers record the sequence the
batch carried; etcd/CRDB carry the durable index through the write path, never a post-hoc
global read. FrogDB's own `publish_synced_through` already does it correctly —
`record_wal_watermark` is the one place that doesn't.

## What to build (spec-first)

1. Amend FM-PERSISTENCE-035 (and TR-PERSISTENCE-010 if it restates the mechanism): the
   watermark is the sequence **covered by the sync that just completed**, carried through
   the write path — never `latest_sequence_number()` read after the fact.
2. `record_wal_watermark(covered_seq)` takes the covered sequence as an argument, from
   the same snapshot `publish_synced_through` uses; the store becomes a `fetch_max`
   rather than a blind store (concurrent shard syncs must not regress it).
3. Rewrite `only_a_synced_rocks_sink_commit_records_the_durable_watermark` — it must stop
   asserting equality with `latest_sequence_number()`.
4. New two-shard forcing test: write on shard 1, sync on shard 0, assert the watermark
   did not advance past shard 0's covered sequence.

## Acceptance criteria

- [ ] FM-PERSISTENCE-035 amended; `just lint-spec` green
- [ ] Watermark write is `fetch_max` of a carried covered-sequence, on both `sync` and
      `periodic` paths
- [ ] Two-shard forcing test fails on the pre-fix tree, passes post-fix
- [ ] Pinning test rewritten
- [ ] `just mutants-diff` on frogdb-persistence (locked, gate 0.85) triaged

## Blocked by

None — can start immediately.
