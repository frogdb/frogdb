# No end-to-end test of the split-brain divergence lifecycle (partition -> divergent writes -> heal -> discard)

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 3/3 (score 6)
Area: replication

## Context

Only individual mechanisms of split-brain handling are unit-tested; there is no test exercising the
full lifecycle end-to-end. Covered pieces: the divergence-record window
(`replication/src/primary/tests.rs:65-166`), the split-brain log's round-trip and
`has_pending_logs` (`split_brain_log.rs:194-317`), and the wiring in
(`cluster_init.rs:586-633`). What's missing is an integration or turmoil test of the complete
sequence: a demoted primary that accepted writes past its last-acked offset during a partition ->
correctly capturing those divergent writes -> writing the split-brain log file -> incrementing
`SplitBrainOpsDiscardedTotal` -> and, after the partition heals and a full resync occurs, the
divergent writes actually being discarded (not silently retained).

Additionally, `SplitBrainBufferConfig`'s overflow-truncation behavior
(`config/src/replication.rs:124-132`) is untested — what happens when the divergence buffer fills
up during an extended partition is unverified, and there's no rotation/retention test for the
split-brain log file itself.

## What to build

- Turmoil or Jepsen-style integration test: partition a primary from its replica(s), have the
  partitioned (soon-to-be-demoted) primary accept additional writes during the partition, heal the
  partition, and assert:
  - The split-brain log file is written and contains exactly the divergent operations (or a clear
    truncation marker if the buffer overflowed).
  - After full resync, the divergent writes are absent from the now-replica's data — i.e. actually
    discarded, not silently retained.
  - `SplitBrainOpsDiscardedTotal` telemetry matches the number of discarded operations.
- Separate test for `SplitBrainBufferConfig` overflow: fill the divergence buffer past its
  configured limit and assert truncation behavior is well-defined (not silent data corruption in
  the log itself) and observable (truncation marker or metric).

## Acceptance criteria

- [ ] End-to-end test: partition -> divergent writes on old primary -> heal -> promote/resync ->
      assert divergent writes discarded from final state
- [ ] Test asserts the split-brain log file contains exactly the discarded operations (or a
      truncation marker when overflowed)
- [ ] Test asserts `SplitBrainOpsDiscardedTotal` matches the actual discard count
- [ ] Separate test covers `SplitBrainBufferConfig` overflow/truncation behavior explicitly

## Blocked by

None - can start immediately.

## References

- `replication/src/primary/tests.rs:65-166` — divergence-record window (unit-tested only)
- `replication/src/primary/split_brain_log.rs:194-317` — log round-trip + `has_pending_logs`
  (unit-tested only)
- `server/src/cluster/cluster_init.rs:586-633` — wiring
- `config/src/replication.rs:124-132` — `SplitBrainBufferConfig` overflow behavior, untested
- `server/tests/integration_replication.rs` — grep for split-brain/divergen/discard returns nothing
- Source: `.scratch/testing-improvements/audit/E-replication.md` gap #5,
  `.scratch/testing-improvements/audit/verdicts-E.md` #5 ("grep split.brain/divergen/discard in
  integration_replication.rs -> nothing")
