# Promotion replication-ID semantics are unasserted; tests accept any outcome

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 2/3 (score 4)
Area: replication (area E)

## Context

`test_secondary_replication_id_failover` (`server/tests/integration_replication.rs:1515-1560`)
checks only that `role == master` after promotion; the presence of `master_replid2` is observed
via `eprintln!`, never asserted. The test never verifies that a *new* `replid` was minted on
promotion, nor that the *old* replid is preserved as `replid2` — both of which are required for
downstream replicas to correctly partial-resync (`+CONTINUE`) against a demoted-then-promoted
chain, matching Redis's PSYNC2 replid2 semantics.

`test_psync2_failover_partial_sync` (`:3637`) is the closest thing to cross-failover
partial-resync coverage but its assertions need confirming — the report notes it should be
checked for whether it actually asserts `+CONTINUE` specifically vs. accepting a looser outcome.
The verdicts pass confirms other PSYNC2 tests in this file accept "CONTINUE, FULLRESYNC, or OK"
loosely (`:1395,:1484`), which is exactly the kind of over-permissive assertion that would let a
partial-resync regression (silently falling back to full resync every time) go unnoticed.

Verdict (adversarial pass): CONFIRMED L2/C2.

## What to build

Tighten promotion tests to capture and assert the actual replid values, not just role. Add an
explicit test that an old-offset replica reconnecting after a primary promotion gets `+CONTINUE`
(partial resync), not `+FULLRESYNC`, when its offset is still within the backlog.

## Acceptance criteria

- [ ] Test captures `replid` before and after `REPLICAOF NO ONE` (promotion) and asserts it
      changed.
- [ ] Test asserts the new `replid2` equals the pre-promotion `replid`.
- [ ] Test with an old-offset replica reconnecting post-promotion asserts it receives
      `+CONTINUE`, not `+FULLRESYNC`.
- [ ] `test_psync2_failover_partial_sync` (`:3637`) and the loosely-asserting tests at `:1395`
      and `:1484` reviewed and tightened to assert the specific expected resync type rather than
      accepting any of CONTINUE/FULLRESYNC/OK.

## Blocked by

None - can start immediately

## References

- `server/tests/integration_replication.rs:1515-1560,1395,1484,3637`
- `.scratch/testing-improvements/audit/E-replication.md` (`promotion-replication-id-semantics-untested`, E#6)
- `.scratch/testing-improvements/audit/verdicts-E.md`
