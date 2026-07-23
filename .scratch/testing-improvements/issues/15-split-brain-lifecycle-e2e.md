# No end-to-end test of the split-brain divergence lifecycle (partition -> divergent writes -> heal -> discard)

Status: done
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

## Resolution

Resolved 2026-07-23. Added two end-to-end lifecycle tests driving the **real
production glue** — `SplitBrainLogger::log` / `DemotionConsumer::handle` on a real
`PrimaryReplicationHandler` that actually diverged — in
`server/src/server/cluster_init.rs` (`mod tests`). Prior coverage only ever built
a `noop_logger()` (`primary_handler: None`), so `divergence_record()` → `write_log`
→ telemetry was never exercised through the logger; the mechanisms were unit-tested
only in isolation. 6 runs, 0 flakes (tests are deterministic — no timing/network).

### `split_brain_lifecycle_captures_audit_and_initiates_discard`

Stages a real divergence: a streaming replica pinned at an acked floor, then
divergent `SET`s past that floor, then invokes the exact code the Raft metadata
plane runs (`consumer.handle(&DemotionEvent{..})`). Asserts:
- audit file written + `has_pending_logs` true;
- body contains **exactly** the divergent writes (`div_key_*`) and **excludes** the
  acknowledged ones (`acked_key_*`) — the new primary retains those;
- header window (`seq_diverge_start=acked`, `seq_diverge_end=current`,
  `ops_discarded=N`) + metadata (`old_primary`/`new_primary` hex, epoch old/new);
- `SplitBrainOpsDiscardedTotal == N`, `SplitBrainEventsTotal == 1`,
  `SplitBrainRecoveryPending == 1.0` (read back via a real `PrometheusRecorder`);
- the Role Demotion still fires (`request_demote(new_primary_addr)`) — the step
  that discards the divergent writes by resyncing this node from the new primary.

Covers acceptance criteria 1 (capture half + discard-*initiation*), 2, 3.

### `split_brain_buffer_overflow_truncates_audit_silently`

Fills the buffer past `split_brain_buffer_size` (CAP=8, 20 divergent writes, floor
0). Pins the **actual** designed behavior + boundary honestly (criterion 4):
- well-defined, no corruption: the newest CAP entries are retained intact and in
  order; oldest are FIFO-evicted;
- **NOT observable**: no truncation marker in the file, and `ops_discarded` counts
  only the retained tail — it **undercounts** the true divergence span
  `(seq_diverge_start=0, seq_diverge_end=current]`. The evicted oldest writes are
  silently absent; the header window and the body disagree on the true size.

### Boundaries documented honestly (not fought, per issues 34/23/61)

- **Discard-from-final-state (criterion 1 tail) is not asserted at the data path.**
  The `DemotionConsumer` *initiates* the discard via `request_demote`; the actual
  keyspace replacement is the resync path. Per issue 35 / the doc-comment on
  `test_broadcast_lag_disconnect_and_resync`, a *partial* resync converges the live
  store while a *full* resync stages a checkpoint that installs only on next boot
  (persistence) or ships an empty RDB (in-memory). A demoted old-primary whose data
  diverged needs a full resync (replid differs), so its divergent keys are not
  replaced live — that live-discard is issue 61's staged-not-installed boundary, not
  retested here.
- **The split-brain logger fires only via the Raft `DemotionConsumer`.** The
  `TestServer`/REPLICAOF harness in `integration_replication.rs` runs no Raft
  failover, so a LagProxy server-level e2e cannot produce the audit log/metric — it
  can only show data reconciliation. The tests therefore drive `consumer.handle`
  directly with the committed `DemotionEvent` (the real input the metadata plane
  feeds), which is the fullest deterministic reach of the production path.

### Follow-up candidate (finding, not fixed here — out of scope for a test task)

Buffer overflow silently truncates the discard **audit** with no marker and an
undercounting `ops_discarded`. An operator reading the log during an extended
partition cannot tell that older divergent ops were dropped. Consider emitting a
truncation marker line (e.g. `truncated_ops=<n>` derived from `end - start` vs
retained) or a dedicated metric so the truncation is observable.
