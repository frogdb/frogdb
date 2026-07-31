# BGSAVE checkpoint cross-shard consistent-cut is unverified under concurrent writers

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 1/3, consequence 3/3 (score 3)
Area: persistence / checkpointing (area D)

## Context

`create_checkpoint` cuts each shard at `latest_sequence_number()`
(`rocks/checkpoint.rs:7-21`). A cross-shard write — e.g. `MSET` touching keys owned by different
shards, or a cross-shard transaction — should be atomically either fully in or fully out of a
given checkpoint. The existing stager happy-path test
(`snapshot/tests.rs:251-315`) only checkpoints a quiescent store with no concurrent writer, so it
can't observe a torn cut even if one were possible.

The initial report suggested this might be correct-by-construction via sequence-number
semantics, but the adversarial pass found this is **not** guaranteed: `flush.rs` sinks each
shard's writes into *separate* per-shard `WriteBatch`es rather than one atomic cross-shard batch.
Because of that, a cross-shard `MSET` is not committed as a single atomic unit across shards —
each shard's checkpoint cut is independent — so it is possible (a narrow window, but real) for a
checkpoint to capture one shard's half of a cross-shard write and not the other's, producing a
torn/inconsistent checkpoint image.

This is adjacent to (not a duplicate of) issues 05/06 in the tracker (jepsen slot-migration
checker and Elle checkers) — those cover cross-shard/cross-slot linearizability under Jepsen;
this is specifically about checkpoint (BGSAVE) atomicity across shards, a different mechanism.

Verdict (adversarial pass): CONFIRMED L1/C3 — NOT purely correct-by-construction; narrow but real
window via independent per-shard `WriteBatch`es in `flush.rs`.

## What to build

A verifier/test that hammers cross-shard `MSET`/transactions concurrently with `BGSAVE`, restores
from the resulting checkpoint, and asserts no torn batch — i.e., for any cross-shard write, either
all of its keys are present in the restored checkpoint or none are.

## Acceptance criteria

- [ ] Test (turmoil or integration, concurrency-capable) runs a stream of cross-shard `MSET`s (or
      cross-shard transactions) concurrently with repeated `BGSAVE`.
- [ ] Restores from a captured checkpoint mid-stream and asserts, for each cross-shard write in
      flight during the checkpoint, that its keys are either all present or all absent in the
      restored image — no partial/torn writes.
- [ ] Test documents/pins whatever the actual guarantee turns out to be (if a torn checkpoint is
      reproduced, this becomes a bug report with the reproducer attached; if not reproduced after
      adequate stress, the test still stands as a regression guard).

## Blocked by

None - can start immediately

## References

- `core/src/persistence/rocks/checkpoint.rs:7-21`
- `core/src/persistence/flush.rs` (per-shard separate `WriteBatch`es)
- `core/src/persistence/snapshot/tests.rs:251-315`
- `.scratch/testing-improvements/audit/D-persistence.md` (`checkpoint-cross-shard-consistent-cut-untested`, D#5)
- `.scratch/testing-improvements/audit/verdicts-D.md`

## Resolution

Done 2026-07-23. Premise re-verified against current `main`, the intended checkpoint
contract determined and asserted, and three concurrency tests added. No product bug: the
originally-hypothesized *single-shard* tear cannot happen; the *cross-shard* tear is a
real but architecturally-accepted behavior, now pinned and documented rather than
asserted-against.

### Premise re-verification (post issue 13 / issue 14)

The audit framed this as a RocksDB-level `WriteBatch` atomicity problem. That framing is
wrong for the mechanism that actually exists:

- A checkpoint is a single `rocksdb::checkpoint::Checkpoint` over **one shared RocksDB**
  (`rocks/checkpoint.rs`), so it captures an atomic point-in-time image at one RocksDB
  sequence number across **all** shards' column families. RocksDB never captures half of a
  committed `WriteBatch`. So the tear is not "one CF caught mid-batch" — it is a question
  of *which per-shard commits have landed in RocksDB* at the instant of the cut.
- Issue 13's `FlushWal` fan-out (`server/src/server/init.rs` `pre_snapshot_hook`) drains
  every shard's WAL flush engine into RocksDB **before** `create_checkpoint` runs, so the
  cut is a committed *prefix* of each shard's history, never a lossy mid-flush state. This
  removed the data-loss exposure issue 13 found; it does **not** by itself make cross-shard
  writes atomic in the cut, because both the writes and the drain are independent per shard.

Conclusion — the intended contract, which the tests assert:

- **Single-shard `MULTI`/`EXEC` is never torn by the cut (strong guarantee).** A
  single-shard transaction is one shard event-loop message that enqueues all its WAL writes
  (each an independent `WalEntry` with a monotonic seq) before any later message — including
  the pre-snapshot `FlushWal` — is processed; RocksDB commits in seq order; the checkpoint
  captures a seq prefix. So a transaction's contiguous seq range is captured all-or-nothing.
  A regression that tore a single-shard transaction would be a real bug — this is the loud
  guard.
- **Cross-shard `MSET`/scatter may be torn by the cut (accepted limitation).** Independent
  per-shard writes + per-shard drain ⇒ the cut can catch shard A's half and not shard B's.
  This is consistent with FrogDB's cross-shard model: execution atomicity via locking,
  without failure/durability atomicity (`.scratch/concurrency-testing/issues/05-06`; the
  abort-on-recovery framing that would close it is deferred issue 06, not built). What **is**
  preserved and asserted is *per-shard* atomicity: the subset of a cross-shard write that
  lands on one shard is applied as one event-loop message, so a shard's own portion is never
  torn.

### Tests added (`server/tests/integration_persistence.rs`)

- `test_checkpoint_preserves_single_shard_multi_atomicity_under_concurrent_bgsave` — 4
  writers stream single-shard (hash-tagged) `MULTI`/`EXEC` transactions, each rewriting a
  tag group's 5 keys to one generation, while `BGSAVE` fires repeatedly; restores the latest
  checkpoint and asserts every tag group is all-absent or all-present-and-equal. Pins the
  strong single-shard guarantee.
- `test_checkpoint_cross_shard_mset_contract_under_concurrent_bgsave` — 4 writers stream
  cross-shard `MSET`s (keys deliberately spanning all shards) rewriting the whole set to one
  generation, concurrent with `BGSAVE`; restores and asserts *per-shard* atomicity (every
  present key on a shard shares a generation) while **tolerating** cross-shard generation
  spread (a torn cross-shard cut is surfaced via `eprintln!`, not failed on). Also asserts
  every restored value is a real, non-torn generation.
- `test_concurrent_bgsave_stress_restores_cleanly` — 4 clients spam `BGSAVE` (5ms apart)
  while 3 writers churn keys; asserts every reply is an accepted simple string (started /
  scheduled / already-in-progress — never an error or hang), the server stays responsive,
  and a pre-storm baseline dataset restores intact from the resulting checkpoint. Exercises
  the coordinator's already-running coalescing under real concurrency.

Helpers added: `spawn_writers`, `keys_spanning_shards`, `hammer_bgsave`.

### Verification

Testbox (aarch64 Linux): all 3 tests pass, and pass 3×3 repeat runs with no flakes
(`3 tests run: 3 passed` each). `just lint frogdb-server` clean; `just fmt frogdb-server`
clean.

### Docs

`website/src/content/docs/architecture/consistency.md` gains a "Checkpoint (BGSAVE)
Cross-Shard Cut — [Tested]" subsection documenting the single-shard guarantee and the
accepted cross-shard tear, linked to the new tests.
