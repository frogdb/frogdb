# Split-brain metrics move even when the log write failed

Status: done
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

## Resolution

Fixed exactly as proposed. `SplitBrainLogger::log` (`cluster_init.rs`) now matches on `write_log`
and each arm carries only the telemetry its outcome supports:

- `Ok(path)` logs the path and sets `SplitBrainRecoveryPending` to `1.0`. That gauge is a statement
  about a file on disk, and it now has exactly two writers that agree: this arm, and
  `Server::check_split_brain_logs` at boot, which raises it from `has_pending_logs(data_dir)`.
- `Err(e)` logs the error and increments the new
  `SplitBrainLogWriteFailuresTotal` (`frogdb_split_brain_log_write_failures_total`), added to the
  typed registry in `frogdb-server/crates/types/src/metrics/definitions.rs`.
- `SplitBrainEventsTotal` and `SplitBrainOpsDiscardedTotal` stay **outside** the match, with a
  comment saying why: they describe the demotion and the writes it discarded, both of which happened
  regardless of the write. Moving them into the `Ok` arm alongside the gauge would be the symmetric
  mistake — the case where the audit is lost would also be the case where nothing counts the data
  lost with it.

The demotion is deliberately not blocked or retried on a failed audit: it is a metadata-plane
decision the data path is reflecting, and refusing to reflect it because a diagnostic file could not
be written would leave two nodes claiming one history. The counter is the signal that the trade was
made.

Forcing test: `split_brain_telemetry_follows_the_log_write_outcome` (frogdb-server, unit — a
crate-level test rather than the integration suite, which `cargo mutants -p` never runs). It drives
both directions through a shared fixture: the success path asserts the gauge reads `Some(1.0)` and
the failure counter was never touched (`None`), and the failure path — a `data_dir` pointing at a
non-existent *nested* directory, so `File::create` fails without needing a chmod that a root test
runner would ignore — asserts the gauge stays untouched (`None`, not merely `0.0`) while the failure
counter reads `Some(1)`. Both directions also assert events/ops-discarded reached `Some(1)`, pinning
the unconditional half against a later "tidy-up" that moves them into the `Ok` arm. The existing
`split_brain_lifecycle_captures_audit_and_initiates_discard` gained a matching zero-failures
assertion. Spec row **FM-REPLICATION-048**; FM-REPLICATION-024 (which owns the audit itself) wants a
cross-reference to it.

**The note about crate placement is stale.** `frogdb-telemetry` and `frogdb-config` are both in
`NEXTEST_CRATES` now, so a test in either is citable. It made no difference here — the bug is in the
`cluster_init.rs` caller, so the test belongs in `frogdb-server` regardless.
