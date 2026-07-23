# Replica-side expiry / relative-TTL drift has no deterministic assertion

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 2/3 (score 4)
Area: replication (area E)

## Context

Expiry is replicated as `EffectScope::InternalRemoval` with `replicate:false`
(`post_execution.rs:141-150,371-377`) — replicas expire keys independently, on their own clock.
Relative-TTL commands (e.g. `PEXPIRE`) are shipped to replicas verbatim rather than rewritten to
an absolute deadline; `expire_tcl.rs:15` documents "TTL propagated as absolute" as an
intentional incompatibility that is NOT implemented. The practical effect: a replica's expiry
deadline for a given key can be later than the primary's by however much replication lag exists,
creating a window where the replica still serves a value the primary has already expired.

The only test that touches this, `test_expire_propagated_as_del_not_expire`
(`integration_replication.rs:2955-3012`), resolves all possible outcomes via `eprintln!` rather
than an assertion — it observes behavior but pins nothing. There is no test that deterministically
drives a key past expiry on the primary and then asserts what a replica read returns during the
drift window, and no test bounding replica `PTTL` against the primary's.

Verdict (adversarial pass): CONFIRMED L2/C2.

## What to build

A deterministic test harness (fake/controlled clock, as used elsewhere in the persistence test
harness) that: sets a key with a short TTL on the primary, advances past expiry, and asserts the
documented replica-read behavior during and after the drift window. Add a `PTTL` bound test
verifying replica `PTTL(key) <= primary PTTL(key) + lag_bound`.

## Acceptance criteria

- [ ] `SET`+`PEXPIRE` on primary, `WAIT` for replication, then deterministic (fake-clock-based,
      not wall-clock-timing-based) assertion of the replica's read behavior once the primary-side
      deadline has passed — pinning the actual (not aspirational) contract.
- [ ] Test asserting replica `PTTL` is bounded relative to primary `PTTL` plus a documented lag
      bound, not just "some value close to the primary's."
- [ ] `test_expire_propagated_as_del_not_expire` (`integration_replication.rs:2955-3012`)
      replaced or supplemented with a real assertion instead of `eprintln!`.
- [ ] Test explicitly documents/pins whether replica lazy-expiry-on-read is or isn't implemented.

## Blocked by

None - can start immediately

## References

- `core/src/shard/post_execution.rs:141-150,371-377`
- `redis-regression/tests/expire_tcl.rs:15`
- `server/tests/integration_replication.rs:2955-3012`
- `.scratch/testing-improvements/audit/E-replication.md` (`replica-independent-expiry-and-relative-ttl-drift-untested`, E#3)
- `.scratch/testing-improvements/audit/verdicts-E.md`
