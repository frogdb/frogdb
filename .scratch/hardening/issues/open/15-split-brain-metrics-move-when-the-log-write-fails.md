# Split-brain metrics move even when the log write failed

Status: needs-triage
Type: bug (observability)
Severity: likelihood 2/3 (any unwritable or full log directory during a demotion), consequence 2/3
(`frogdb_split_brain_recovery_pending` points an operator at a recovery file that does not exist)
— score 4
Area: replication / split-brain

## Problem

`cluster_init.rs:805-815` reports the split-brain event unconditionally after matching on the
write result:

```rust
Err(e) => {
    tracing::error!(error = %e, "Failed to write split-brain log");
}
}

frogdb_telemetry::definitions::SplitBrainEventsTotal::inc(&*self.metrics);
frogdb_telemetry::definitions::SplitBrainOpsDiscardedTotal::inc_by(
    &*self.metrics,
    record.writes.len() as u64,
);
frogdb_telemetry::definitions::SplitBrainRecoveryPending::set(&*self.metrics, 1.0);
```

The three metric calls sit outside the match, so a failed `write_log` produces the same metric
state as a successful one. `SplitBrainRecoveryPending = 1` specifically means "a divergence record
is on disk waiting for an operator to reconcile it" — after a failed write there is no such record,
and the discarded writes are gone with no durable trace. The operator is told to go find a file
that was never created, and the counter of discarded operations claims those operations were
recorded when they were only counted.

This is the same class as the "lying INFO" bugs already closed in this campaign: the observability
surface must not report a stronger outcome than the code achieved.

## Candidate fix

Move `SplitBrainRecoveryPending::set(1.0)` into the `Ok` arm — it is a statement about a file, so
it belongs where the file exists. `SplitBrainEventsTotal` and `SplitBrainOpsDiscardedTotal` are
statements about the *event*, which did happen, so they can stay unconditional; but the failure
needs its own signal, so add a `SplitBrainLogWriteFailuresTotal` counter incremented in the `Err`
arm. That way the failure is visible as a failure rather than as a successful recovery-pending
state.

## Forcing tests

A test that drives a demotion with an unwritable log directory and asserts
`split_brain_recovery_pending` stays `0` while the new failure counter reads `1`, plus the mirror
case asserting the successful path still sets it. Note that `frogdb-telemetry` is not in
`NEXTEST_CRATES` in `scripts/failure-modes.py`, so a test placed there cannot be named in a
`Forced by` cell — put it in the server crate or add the crate to the list.
