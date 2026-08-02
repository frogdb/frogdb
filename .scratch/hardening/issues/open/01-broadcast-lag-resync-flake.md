# test_broadcast_lag_disconnect_and_resync flakes under full-suite load

Status: needs-triage
Type: flaky-test
Severity: likelihood 2/3, consequence 2/3 (score 4) — masks real WAIT-ack regressions
Area: replication

## Symptom

`integration_replication::test_broadcast_lag_disconnect_and_resync::case_2_with_persistence`
fails intermittently under full-suite parallel load, twice on 2026-07-31 (different commits,
unrelated diffs — command feature-gating and the concurrency-harness drain fix):

```
assertion `left == right` failed: seed write should be acked by the healthy replica
  left: 0
  right: 1
```
at `integration_replication.rs:7238`. Passes in isolation and on full-suite rerun both times.

Reproduced again on 2026-07-31 during the issue-65 work, this time **locally on macOS** under
`just test frogdb-server` (1923/1924, this the only failure) — same assertion, same line. So it is
not testbox- or Linux-specific: any host running the full suite in parallel can hit it, which
raises its nuisance value and makes it cheap to iterate on locally.

## Context

Same load-dependent signature as `.scratch/testing-improvements/issues/65`, which already
cross-references this test's failure pattern. The assertion is a WAIT-ack count against a
healthy replica while a LagProxy throttles the other —
a 1s write timeout that presumably starves under CPU contention.

Hit twice more on 2026-08-02 during the phase-2c full-sync work (`just test frogdb-server`
1913/1914, and once in a 185-test `just core-test-e2e replication` run), same assertion, same
message. Passed in isolation and on two consecutive reruns of the same filter, so still
load-dependent and still unrelated to the diff under test.

## Next steps

- Root-cause whether the 1s LagProxy write timeout or the WAIT deadline is the load-sensitive
  edge; widen deterministically or restructure to condition-wait rather than fixed timeout.
- Belongs to the replication hardening phase (Phase 3); fold into its failure-mode spec work.
