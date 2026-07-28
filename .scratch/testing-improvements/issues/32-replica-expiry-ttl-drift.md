# Replica-side expiry / relative-TTL drift has no deterministic assertion

Status: done
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

- [x] `SET`+`PEXPIRE` on primary, `WAIT` for replication, then deterministic (fake-clock-based,
      not wall-clock-timing-based) assertion of the replica's read behavior once the primary-side
      deadline has passed — pinning the actual (not aspirational) contract.
- [x] Test asserting replica `PTTL` is bounded relative to primary `PTTL` plus a documented lag
      bound, not just "some value close to the primary's."
- [x] `test_expire_propagated_as_del_not_expire` (`integration_replication.rs:2955-3012`)
      replaced or supplemented with a real assertion instead of `eprintln!`.
- [x] Test explicitly documents/pins whether replica lazy-expiry-on-read is or isn't implemented.

## Blocked by

None - can start immediately

## References

- `core/src/shard/post_execution.rs:141-150,371-377`
- `redis-regression/tests/expire_tcl.rs:15`
- `server/tests/integration_replication.rs:2955-3012`
- `.scratch/testing-improvements/audit/E-replication.md` (`replica-independent-expiry-and-relative-ttl-drift-untested`, E#3)
- `.scratch/testing-improvements/audit/verdicts-E.md`

## Resolution

Confirmed the contract by code read + new deterministic tests; **no source-behavior change** —
this is a pre-existing, already-deliberate design choice (see "Decision" below for why it was not
flipped in this pass).

### What the code actually does (pinned by test)

- `PttlCommand`/`GetCommand` (`store::get_with_expiry_check`, `PttlCommand::execute`) are
  **role-agnostic**: a replica checks a key's deadline against its own `Instant::now()` on every
  read, independent of active expiry. So lazy-expiry-on-read **is implemented** on replicas, not
  just primaries.
- `PEXPIRE`/`EXPIRE` replicate **verbatim** (a relative duration), not rewritten to an absolute
  deadline (`PEXPIREAT`-style). The replica therefore computes its own deadline as
  `replica_apply_time + duration`, strictly later than the primary's
  `primary_apply_time + duration` by however long replication took to deliver the write.
- Organic (TTL-driven) expiry **never propagates a `DEL`/`UNLINK`** to replicas at all
  (`RemovalPropagation { replicate: false }` in `apply_expiry_effects`) — confirmed by asserting
  `master_repl_offset` is unchanged across a full expire-and-converge cycle.
- Net, measured behavior: after the primary's deadline elapses, there is a real (not
  hypothetical) window — bounded by replication lag, typically single-digit ms locally — during
  which a replica `GET` still returns the primary-already-expired value. The key disappears on
  the replica only once **its own** clock crosses **its own** (later) deadline; this was
  previously not asserted by the only test that touched the area
  (`test_expire_propagated_as_del_not_expire`, all outcomes resolved via `eprintln!`).

This is a genuine (bounded, not unbounded) incompatibility with Redis, where TTL commands are
rewritten to `PEXPIREAT` before replication so the replica's deadline is byte-identical to the
primary's and expiry-driven organic removal propagates as an explicit `DEL`.

### Tests added (`server/tests/integration_replication.rs`, Tier 9)

- `test_replica_expires_independently_not_via_del` (replaces
  `test_expire_propagated_as_del_not_expire`): measures the primary/replica `PTTL` drift via
  concurrent (`tokio::join!`) reads, sleeps *exactly* past the primary's measured deadline
  (deterministic — computed from the read PTTL, not a guessed knife-edge sleep), asserts the
  primary is expired, conditionally asserts the replica still serves the stale value when the
  measured drift exceeds the sleep margin, polls (generously, not knife-edge) for eventual
  replica convergence, and asserts `master_repl_offset` is unchanged to prove convergence happened
  via independent expiry, never a propagated `DEL`.
- `test_replica_pttl_bounded_by_replication_lag`: 5 concurrent-sample loop asserting
  `|replica_pttl - primary_pttl| <= 2000ms` (a documented, generous local-test lag bound) —
  pins "bounded by lag," not "unboundedly divergent" or "coincidentally close."

Both are `#[rstest]`-parameterized over `persistence` (in-memory / with-persistence), so 4 test
cases total.

### Test run results (issue 32 discipline: ≥5 runs, timing-sensitive)

- `just check frogdb-server` (all targets): clean.
- `just lint frogdb-server` (clippy, default + `--features turmoil --tests`): clean, 0 warnings.
- `just fmt frogdb-server`: no changes (already formatted).
- 6 total independent `just test frogdb-server` runs (1 initial + 5 repeat), each covering all 4
  new/rewritten test cases = 24 test executions total:
  ```
  4 tests run: 4 passed, 1922 skipped   (x6 runs)
  ```
  **Flake rate: 0/24 (0%).** No knife-edge timing failures observed; per-test wall time ~2.1-2.9s.

### Decision: documented, not fixed

The wrap-up guidance authorized a fix via the `ReplicationOverride` seam ("issue-01's
`ReplicationOverride` seam exists for a PEXPIREAT rewrite if needed and small") if this turned out
to be a live bug. Evaluated and **not implemented in this pass**, because:

1. It is already an explicit, deliberate, documented design choice in the code itself
   (`post_execution.rs` `RemovalPropagation` doc comment: "flipping it toward Redis-style
   primary-drives-expiry is a deliberate ADR, out of scope here") and in the TCL port
   (`expire_tcl.rs`: "TTL in commands are propagated as absolute timestamp in replication stream —
   intentional-incompatibility:replication").
2. The divergence is **bounded** by replication lag (now proven, not just asserted) — it is not
   an unbounded/runaway divergence, which is the bar the wrap-up guidance set for "real bug."
3. A full Redis-parity fix is **not small**: relative-TTL propagation affects `EXPIRE`, `PEXPIRE`,
   `SETEX`, `PSETEX`, `SET ... EX/PX`, `GETEX ... EX/PX`, and `MSETEX` — six-plus command
   handlers, several of which (`SET`/`GETEX`) would need a new "rewrite one option argument of a
   value-carrying command" pattern that has no precedent in the existing `ReplicationOverride`
   seam (today's only user, `SPOP`, rewrites a whole command to `SREM`/`DEL`, not one argument of
   itself). `EXPIRE`/`PEXPIRE` alone would be a small, isolated fix, but the issue text's own
   examples ("EXPIRE/SETEX") and the general Redis-parity goal call for the whole surface, not a
   partial one.

**Recommendation**: file a follow-up implementation issue ("rewrite relative-TTL propagation to
absolute PEXPIREAT/PXAT, Redis-parity") scoped to the full command surface above, using the tests
added here as the regression baseline (once implemented, `test_replica_pttl_bounded_by_replication_lag`'s
drift should shrink to near-zero and the conditional stale-read branch in
`test_replica_expires_independently_not_via_del` should stop firing).
