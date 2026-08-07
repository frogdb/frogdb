# Live-link fault primitive — fault injection only mangles recorded histories, never a running link

Status: needs-triage
Type: decision
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: INFRASTRUCTURE.md I6
LOE: 1–2 weeks (estimated)
Tier: C
Area: crates/testing / replication fault injection (turmoil hosts)
Asked by: 14 (item 2)
Unblocks: 14/F2, F7, F9, F10

## Context

Four replication findings need something to go wrong *while the link is live* — a checkpoint
transfer that stalls, a backlog that gets evicted mid-PSYNC, an ACK that arrives late. The
existing fault-injection module cannot do any of that: it edits histories after the run, so
the system under test never actually experiences the fault. Building the live version is a
multi-week job and needs an owner and a schedule before anyone starts.

## Evidence

- `testing/src/fault_injection.rs` mangles **recorded histories after the fact**. Nothing can
  stall a checkpoint transfer, evict a backlog, or delay an ACK on a *running* link.
- **Where**: the turmoil hosts (`real_frogdb_primary` / `real_frogdb_replica`) are the natural
  attachment point.

## What to build

1. A fault primitive attached to the turmoil hosts (`real_frogdb_primary` /
   `real_frogdb_replica`) that acts on a live replication link.
2. At minimum the three fault shapes the findings need: stall a checkpoint transfer, evict a
   backlog entry, delay an ACK.
3. Faults must be addressable to a specific link and bounded in duration, so a scenario can
   assert recovery after the fault clears.

## Decision needed

Tier C: 1–2 weeks (estimated), and it lands inside the turmoil host implementations, which
are shared. Confirm the owner and the schedule — and confirm the turmoil attachment point is
viable — before committing. See also issue 13, `.scratch/testing-improvements-round2/issues/`,
which hit an upstream turmoil port leak in round 1.

## Acceptance criteria

- [ ] The primitive attaches to `real_frogdb_primary` / `real_frogdb_replica` and acts on a
      link that is actually running, not on a recorded history.
- [ ] Stall-checkpoint, evict-backlog and delay-ACK are each expressible and each covered by
      one scenario.
- [ ] Faults are scoped to a named link and clear after a bounded interval; a scenario
      asserts the link recovers once cleared.
- [ ] `testing/src/fault_injection.rs` documents which module to use for live faults, so the
      after-the-fact mangler is not reached for by mistake.

## Test boundary

Level 5 — a live replication-link fault requires two real nodes and a controllable network,
which only the turmoil multi-node harness provides.

## Depends on

Nothing.

## Re-triage 2026-08-06

**Verdict: still-valid**

The primitive does not exist. `frogdb-server/crates/testing/src/fault_injection.rs` is still
"History-corruption helpers used by checker self-tests" (module doc, `:1`) — seven after-the-fact
manglers, nothing live. The turmoil hosts `real_frogdb_primary` / `real_frogdb_replica`
(`server/tests/common/sim_helpers.rs:250` / `:297`) still take no fault handle; the only live network
control in the suite is turmoil's own `sim.hold`/`sim.release`, used for cluster partitions
(`server/tests/simulation.rs:5367`, `:5384`), not for replication-link shapes. **But the motivation has
largely evaporated**: three of the four dependents were discharged during hardening at a much cheaper
boundary. 14/F2 and 14/F9 (backlog evicted between grant and stream; the full-sync handoff window) are
now FM-REPLICATION-012, forced by `a_resume_evicted_after_the_grant_is_abandoned_not_truncated` /
`a_full_sync_whose_handoff_window_is_evicted_abandons_the_link` in
`replication/src/replica_session.rs`; a checkpoint dying mid-transfer is FM-REPLICATION-001's
`a_checkpoint_that_dies_mid_transfer_leaves_the_old_history_alone`; 14/F7 (ACK on receipt, not apply) was
closed as round-2 issue 76 plus hardening 28's wire-only WAIT acks (90fefaf7). Only 14/F10 (replicas
expiring independently) is left. The decision this issue asks for should therefore probably be
**decline** — record that rather than schedule 1-2 weeks.
