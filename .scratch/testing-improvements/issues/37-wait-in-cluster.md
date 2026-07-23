# WAIT has zero coverage in cluster mode

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 2/3 (score 4)
Area: cluster (area F)

## Context

Grepping `WAIT` in `integration_cluster.rs` returns zero hits (verdicts pass confirms the only
hits anywhere are comments containing "Wait for", not the command). There is no test asserting
which replicas count toward `WAIT`'s numreplicas target in cluster mode (all replicas of the
slot's owning shard? cluster-wide?), nor any test of `WAIT` behavior during or immediately after
a failover — a scenario where the ack-counting logic is most likely to have edge-case bugs
(replica changing shard ownership mid-wait, target replica becoming the new primary, etc.).

Verdict (adversarial pass): CONFIRMED L2/C2.

## What to build

Integration tests exercising `WAIT` in a 3+ node cluster: normal ack counting against the correct
shard-local replica set, and `WAIT` behavior spanning a failover of the shard being written to.

## Acceptance criteria

- [ ] Test: write a key in a 3-node cluster with the owning shard replicated, `WAIT <n> <timeout>`
      after the write, assert ack count matches the actual replica set for that shard (not
      cluster-wide replica count).
- [ ] Test: `WAIT` issued around a failover of the shard being written to — assert it does not
      hang indefinitely and resolves per a documented/pinned semantics (e.g., counts the new
      replica set, or errors, whichever is the intended contract).
- [ ] Test: `WAIT` with `numreplicas` exceeding actual replica count times out per configured
      timeout rather than hanging past it.

## Blocked by

None - can start immediately

## References

- `server/tests/integration_cluster.rs` (grep `WAIT` → 0 real hits)
- `.scratch/testing-improvements/audit/F-cluster.md` (`wait-in-cluster-untested`, F#10)
- `.scratch/testing-improvements/audit/verdicts-F.md`
