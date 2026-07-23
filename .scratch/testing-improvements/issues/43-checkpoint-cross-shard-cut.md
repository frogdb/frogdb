# BGSAVE checkpoint cross-shard consistent-cut is unverified under concurrent writers

Status: needs-triage
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
