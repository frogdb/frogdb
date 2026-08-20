# 34: Replica-side skip for frames at or below the applied head

Status: ready-for-agent

## Origin

Ruling R7 (2026-08-20, [campaign ledger](../../../formal-spec/2026-08-19-quint-completeness-campaign.md)),
the residue of ruling R4. R4 assumed checkpoint-cut overlap was harmless by
construction via an offset-addressed skip; the Rust investigation (agent C,
`a2334b00`) found the premise half false.

## What is wrong

The dedup that exists is **sender-side only**, keyed on the offset the replica
*claimed*:

- `frogdb-server/crates/replication/src/primary/ring_buffer.rs:296-336`
  (`extract_backlog` filters `cmd.offset > start`)
- `frogdb-server/crates/replication/src/feed_sequencer.rs:251-256` (resume buffer
  holds only `frame.sequence > self.resume_offset`)

But `session_machine.rs:215-221` captures `snapshot_offset` *before* the checkpoint
cut, so the full-sync payload deliberately overships **above** the claim — exactly
the range the sender-side skips do not cover. On the replica:

- `apply.rs:361-397` (`consume_frames`) has **no** skip-at-or-below-applied rule —
  only a promotion flag and a history-epoch check.
- `core/src/shard/post_execution.rs:87-111`: propagation is verbatim,
  `debug_assert`ed deterministic but **not idempotent** — re-executing the
  overlapped range mutates the keyspace for `INCR`/`LPUSH`/`APPEND`
  (corroborated by FM-REPLICATION-015's NOT-observable and TR-REPLICATION-021).

`specs/replication.md` FM-REPLICATION-001's second non-guarantee bullet (corrected
in `a2334b00`) names the real sender-side mechanism and no longer claims idempotency;
the model's `inv_reapply_is_a_noop` is an abstraction guard, not a proof.

## What is needed (ruled: receiver-authoritative dedup)

- Replica ignores/skips any frame at or below its applied head. Raft-style:
  covers this hole and any future sender-side accounting bug.
- Spec-first: new FM-REPLICATION row (or upgrade FM-001's non-guarantee bullet to
  a guarantee) + failing forcing test in `frogdb-replication` (locked crate —
  forcing test lives in the mutated crate; `just mutants-diff frogdb-replication`
  before push).
- Model: `replication_fullsync.qnt`'s `inv_reapply_is_a_noop` graduates from
  abstraction guard to modeled skip behavior; the model header note recording the
  abstraction boundary (from `a2334b00`) is updated.
- Counting: skipped frames should be observable (counter/metric), not silent —
  consistent with the R-21 clamp→ignore+count precedent.

## Related

- R4/R5 rows landed in `a2334b00`; TR-REPLICATION-034 (truncate-above-claim,
  Pending) shares the "receiver defends its own history" principle.
- FM-REPLICATION-015, TR-REPLICATION-021 (non-idempotent propagation evidence).
