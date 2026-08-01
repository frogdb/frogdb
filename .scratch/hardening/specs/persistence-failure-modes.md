# Persistence — failure modes

Every way FrogDB's durable-write path can fail, refuse, or succeed, one table per mode. This is
the reference the mutation run is measured against: a mutant that survives is a row nothing forces.

Scope: the shard-to-storage path — `frogdb-core/src/shard/persistence.rs` (the `WalTarget` seam
and `persist_records`), `frogdb-persistence/src/wal/` (the per-shard flush engine, its batching
triggers and durability confirmation), and the snapshot/checkpoint machinery in
`frogdb-persistence/src/snapshot/` plus the pre-snapshot quiesce in
`frogdb-server/src/server/checkpoint_quiesce.rs`. Rows stop at what a *reader of storage* can
observe — a restored checkpoint or a crash-recovery replay. Client-visible transaction semantics
live in [Transactions — failure modes](txn-failure-modes.md).

## How to read a row

| Field | Meaning |
|---|---|
| Trigger | The concrete precondition or interleaving that puts the shard in this mode. |
| Observable | What a reader of storage sees: the restored checkpoint's contents, the recovered keyspace. |
| NOT observable | What must never appear in storage in this mode. This is the half mutation testing attacks. |
| Invariant | The internal guarantee, named at the mechanism that provides it. |
| Outcome variant | The enum variant / metric label the mode reports through, or `n/a` for storage-layer invariants with no client-visible outcome. |
| Forced by | The test(s) that fail if the behavior changes. Every one carries a `// FM-PERSISTENCE-NNN` tag at its definition site; `just lint-failure-modes` enforces both directions. |
| Bug refs | Known issues that touch this mode. |

Test names are bare function names, resolved against the crate list in
`scripts/failure-modes.py` (`NEXTEST_CRATES`).

---

## FM-PERSISTENCE-001 — a shard's write batch is never torn across storage batches

| Field | Value |
|---|---|
| Trigger | One shard-side `persist` call carrying more than one WAL entry — a single-shard `MULTI`/`EXEC`, a multi-key `MSET`, a `RENAME` (write + delete) — while the WAL flush engine's own batch triggers (size threshold, batch timeout) are live, and a `BGSAVE` checkpoint or a crash lands in the middle of the sequence. Load makes it likely: a descheduled shard task leaves the flush thread with an empty channel and an expired timeout. |
| Observable | A restored checkpoint, or a crash-recovered keyspace, shows **all** of the batch's writes or **none** of them. For the transaction test: every hash-tag group is all-absent or all-present at a single generation. |
| NOT observable | A committed *prefix* — some of a transaction's keys at the new generation and the rest at the old one (the shape issue 65 caught: `[Some("470"), Some("458"), Some("458"), Some("458"), Some("458")]`). Equally: a group that never commits because its trigger stayed suppressed, or entries lost because the producer died mid-group. |
| Invariant | `persist_records` resolves the batch's `WalAction`s once and brackets them in `WalTarget::begin_group` / `end_group` whenever there is more than one, closing the group on every path including the staging-error path. A one-action batch skips the markers — a single WAL entry is already indivisible, and the hot path should not pay for a group it cannot need; the count comes from `WriteRecord::wal_actions` itself, so no second guess at how many entries a command produces can drift low. The markers travel the same FIFO channel as the entries, so grouping is decided by *enqueue* order, not by timing. `FlushEngine::should_flush` returns false while `group_depth > 0`, suppressing both the size and the timeout trigger, so the group commits as one `WriteBatch`. Two deliberate exceptions: an explicit `WalCommand::Flush` is honored mid-group (a durability request must not deadlock behind a group), and a channel disconnect drains and commits an unclosed group (the producer is gone; losing the writes would be strictly worse). |
| Outcome variant | n/a (storage-layer invariant; a failure surfaces as a torn restore, not as a reply) |
| Forced by | `test_sink_ungrouped_entries_tear_across_batches`, `test_sink_write_group_survives_the_batch_timeout`, `test_sink_write_group_survives_the_size_threshold`, `persist_brackets_the_batch_in_one_write_group`, `single_action_persist_skips_the_write_group`, `write_group_closes_on_staging_failure`, `failed_group_open_skips_writes`, `test_checkpoint_preserves_single_shard_multi_atomicity_under_concurrent_bgsave` |
| Bug refs | `.scratch/testing-improvements/issues/65-checkpoint-multi-atomicity-testbox-flake.md` (the flake that exposed it), `.scratch/testing-improvements/issues/43-checkpoint-cross-shard-cut.md` (asserted the guarantee on reasoning that did not hold) |

Deliberate non-guarantees, so a future reader does not mistake them for gaps:

* **Cross-shard atomicity is not provided.** A cross-shard `MSET` is several independent
  `persist` calls on several shards, each its own group. The cut can capture one shard's half and
  not another's — FrogDB's documented model (execution atomicity via locking, not durability
  atomicity). `test_checkpoint_cross_shard_mset_contract_under_concurrent_bgsave` pins the
  contract as written, including its limits.
* **A group is not size-capped.** Atomicity and size-capping are mutually exclusive; an oversized
  transaction buys its atomicity with one oversized `WriteBatch`. The flush thread keeps draining
  the channel while a group is open, so backpressure cannot deadlock — the memory moves from the
  channel into the staged batch.
* **`WalEntry::Clear` remains a hard flush barrier** and cuts a group, because a range tombstone's
  bound is only correct against committed state. Unreachable in practice: core's
  `WalStrategy::ClearShard` yields exactly one action, so a clear is always a group of its own.
