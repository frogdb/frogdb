# Promotion replication-ID semantics are unasserted; tests accept any outcome

Status: done
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

## Resolution

Done. Rather than force Redis PSYNC2 parity, the tests now PIN FrogDB's actual
(deliberately-divergent) manual-promotion semantics, verified empirically on a
Linux testbox. All assertions are deterministic (structural, not timing-based);
flake rate 0/5 across repeated runs.

### Asserted semantics (actual FrogDB behavior on `REPLICAOF NO ONE`)

1. **replid is NOT rotated.** The promoted node keeps the exact `master_replid`
   it adopted from its former primary during FULLRESYNC. Redis mints a *new*
   replid here. Root cause: `ReplicationState::new_replication_id()` (which would
   rotate the id and freeze the secondary window) has **no production caller** —
   `RoleManager::promote()` only clears the read-only flag and tears down the
   inbound stream. The replid-rotation + `secondary_id`/`secondary_offset` window
   machinery exists and is unit-tested in the replication crate but is unwired.

2. **No secondary window is set.** `master_replid2` stays all-zero and
   `second_repl_offset` stays the `-1` sentinel after promotion (Redis would set
   replid2 = the prior replid). Consequence: the acceptance criteria "replid
   changed" and "replid2 == pre-promotion replid" are DIVERGENCES, not bugs to
   satisfy — pinned as-is with explanatory comments.

3. **A manually-promoted node rejects downstream `PSYNC` outright** — it replies
   `-ERR PSYNC not supported - server is not running as primary`, i.e. neither
   `+CONTINUE` nor `+FULLRESYNC`. The primary-side handler
   (`primary_replication_handler`, gated in `connection/dispatch.rs`) is wired
   only at boot for a node started as a primary; runtime promotion never
   establishes it. So the acceptance criterion "old-offset replica reconnecting
   gets +CONTINUE" cannot hold on the `REPLICAOF NO ONE` path — the promoted node
   cannot head a replication chain at all. The healthy `+CONTINUE` partial-resync
   path is still covered for boot-configured primaries by
   `test_partial_resync_after_brief_disconnect_grants_continue`.

### Parity-or-divergence outcome: DIVERGENCE (pinned, documented)

The ops doc already hedges this (`website/src/content/docs/operations/replication.md`:
"do not rely on a specific secondary-ID continuation behavior"), but the same doc
also claims surviving replicas "reconnect to it via PSYNC" — which does NOT hold
for the manual `REPLICAOF NO ONE` path. **Candidate follow-up (not filed):** wire
runtime promotion to (a) mint a new replid + freeze the secondary window via
`new_replication_id`, and (b) stand up a `PrimaryReplicationHandler` so a
promoted node can serve PSYNC. Until then the manual-promotion path produces a
writable standalone node, not a replication source.

### Tests (server/tests/integration_replication.rs)

- `test_secondary_replication_id_failover` — rewritten: asserts replica adopts
  primary replid, then pins replid UNCHANGED + replid2 all-zero + second=-1
  across promotion (was: role-only check, replid2 via `eprintln!`).
- `test_promoted_node_via_replicaof_no_one_rejects_downstream_psync` — NEW: pins
  the PSYNC rejection (no CONTINUE/FULLRESYNC) on a manually-promoted node.
- `test_psync2_failover_partial_sync` — tightened from `eprintln!`/soft `if let`
  to hard asserts: role master, replid unchanged, replid2 zero, PSYNC rejected,
  and all pre-promotion keys retained.
- `test_partial_sync_continue_response` — tightened from "CONTINUE/FULLRESYNC/OK
  or non-error" to assert `+CONTINUE` specifically (the audit's loose-assertion
  gap; stale line refs `:1395`/`:1484` in the issue now point elsewhere — the
  real loose sites were this test and the one below).
- `test_partial_sync_falls_back_to_full` — tightened from "FULLRESYNC or OK" to
  assert `+FULLRESYNC` specifically for an unknown replid.
