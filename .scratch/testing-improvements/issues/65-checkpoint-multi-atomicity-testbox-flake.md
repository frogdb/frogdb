# 65 — checkpoint single-shard MULTI atomicity test flakes under full-suite load

Status: done — REAL BUG (not a test flake). Fixed by WAL write groups; see Resolution.

Observed 2026-07-28 on a Blacksmith testbox (8 vcpu, aarch64) during the config-mutability
round's final full-suite gate:

```
FAIL integration_persistence::test_checkpoint_preserves_single_shard_multi_atomicity_under_concurrent_bgsave
single-shard transaction cut captured mixed generations for tag g2: [Some("470"), Some("458"), Some("458"), Some("458"), Some("458")]
```

- Failed under full-suite parallelism **including its nextest retry**; passed **5/5
  consecutive `--retries 0` runs in isolation** on the same box at the same commit.
- The same tree passed the full suite earlier the same day (different load pattern), so it
  is load-dependent, same operational profile as the tracked
  `test_broadcast_lag_disconnect_and_resync` testbox-only failures.
- Test introduced by issue 43 (`204922bc`, BGSAVE checkpoint cut contract).

Why this deserves triage rather than a shrug: the asserted contract — a single-shard
MULTI must be captured atomically by a concurrent BGSAVE checkpoint cut — is a real
product guarantee. A load-only tear could be (a) a residual race in the checkpoint cut
the issue-43 work pinned, visible only when the BGSAVE thread is starved into a wider cut
window, or (b) a test-synchronization gap (e.g. the generation-writer loop overlapping the
cut in a way the assertion doesn't intend). Distinguishing (a) from (b) is the work.

## Resolution (2026-07-31) — (a): a real atomicity hole in the WAL flush engine

Verdict: **the test was right and the product was wrong.** The observation mechanism is sound;
the guarantee it asserts did not hold. The failure signature is exactly a committed one-entry
prefix: `k0` at generation 470 and `k1..k4` at 458, and 470 − 458 = `NUM_TAGS`, so both
generations map to tag `g2` — the checkpoint captured the first `SET` of one transaction plus
the last four of an earlier one.

### Root cause

A single-shard `MULTI`/`EXEC` reaches storage as N *independent* commands on the shard's WAL
channel:

- `frogdb-core/src/shard/persistence.rs` — `persist_records` loops over the batch's
  `WalAction`s, each becoming one `send_async(WalCommand::Write(..))`
  (`frogdb-persistence/src/wal/writer.rs`).
- `frogdb-persistence/src/wal/flush.rs` — `flush_thread_loop` decided batch boundaries purely
  by **size threshold** (default 4MB) and **batch timeout** (default 10ms), with no notion of
  which entries belong together.

So when the shard task was descheduled between two entries of one transaction — precisely what
full-suite CPU contention causes — the flush thread found the channel empty, the batch timeout
expired, and it committed a **partial** batch. The rest landed in a later `WriteBatch`. A
concurrent `create_checkpoint` (single RocksDB `Checkpoint`, one sequence-number image) then
captured the torn prefix. The same window tears crash recovery, not only checkpoints.

The reasoning in the issue-43 resolution and in `website/.../consistency.md` — "one event-loop
message enqueues all the writes before any later message, and RocksDB commits in sequence
order" — is true about *enqueue* order and says nothing about *commit* grouping. Enqueued is
not committed. Both have been corrected.

Eliminated along the way: partially-written or racing checkpoint artifacts (promotion is an
atomic `rename` of a fully-built tmp dir), an insufficient test polling gate
(`metadata.json` + `checkpoint/CURRENT` only exist post-rename), non-atomic RocksDB
checkpoints, and `FlushWal` interleaving *inside* a transaction (impossible — one shard task,
one message at a time).

### Fix — WAL write groups

`WalCommand::GroupBegin` / `GroupEnd` markers ride the same FIFO channel as the entries, so
grouping is decided by enqueue order rather than timing. `FlushEngine` tracks `group_depth`;
`should_flush` returns false while a group is open, suppressing both the size and the timeout
trigger, so the group commits as one `WriteBatch`. `persist_records` brackets every batch and
closes the group on every path, including staging errors. Exceptions, both deliberate and
tested: an explicit `Flush` is honored mid-group (a durability request must not deadlock), and
a channel disconnect drains and commits an unclosed group.

This also fixes multi-key single commands (`MSET`, `RENAME`, `BITOP`), which had the same hole.

Hot path pays nothing: `persist_records` resolves the batch's `WalAction`s once into a
`SmallVec<[_; 4]>` and emits the markers only when there is more than one — a lone WAL entry is
already indivisible. The count comes from `WriteRecord::wal_actions` itself, so there is no
second guess at how many entries a command produces that could drift low and reopen the tear.

Specified as `FM-PERSISTENCE-001` in
[`.scratch/hardening/specs/persistence-failure-modes.md`](../../hardening/specs/persistence-failure-modes.md),
with the deliberate non-guarantees (no cross-shard atomicity, groups are not size-capped, a
`Clear` still cuts a group) recorded there.

### Proof

Deterministic, no load required — `frogdb-persistence/src/wal/tests.rs`:

- `test_sink_ungrouped_entries_tear_across_batches` — forces the tear itself: a producer stall
  between entries yields two committed batches, the first a prefix.
- `test_sink_write_group_survives_the_batch_timeout` — the same stall inside a group commits as
  one batch.
- `test_sink_write_group_survives_the_size_threshold`,
  `test_sink_nested_write_groups_commit_as_one_batch`,
  `test_sink_explicit_flush_inside_a_group_is_honoured`,
  `test_sink_unclosed_group_commits_on_disconnect` — the trigger suppression, nesting, and the
  two exceptions.

`frogdb-core/src/shard/persistence.rs`: `persist_brackets_the_batch_in_one_write_group`,
`single_action_persist_skips_the_write_group`, `write_group_closes_on_staging_failure`,
`failed_group_open_skips_writes`.

The original integration test is unchanged apart from its `FM-PERSISTENCE-001` tag and a
corrected contract comment — it was never the problem.

### Cross-references

- Issue 43 (`.scratch/testing-improvements/issues/43-checkpoint-cross-shard-cut.md`): its
  Resolution states the single-shard guarantee with the reasoning that did not hold; annotated.
- `.scratch/hardening/issues/01-broadcast-lag-resync-flake.md`: the sibling load-dependent
  flake this issue cross-referenced. **Not** the same cause — that one is a WAIT-ack under a
  1s LagProxy timeout and remains open. The lesson transfers: a load-only failure is not
  evidence of a load-sensitive *test*.
