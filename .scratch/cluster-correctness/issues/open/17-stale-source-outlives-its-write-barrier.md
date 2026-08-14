# 17 — A stale source outlives its write barrier and re-admits writes for a slot it no longer owns

Status: ready-for-agent

## Parent

[PRD](../../PRD.md) §3 W3 — found by the stateright handoff model (issue 10) when the
bounded-apply-lag assumption is withdrawn. Not covered by issue 14 (chained replicas) or
issue 15 (graceful failover), so it is filed on its own.

## What is wrong

The two-phase handoff fences the source with a *local, wall-clock* pause armed when the
source **applies** `PrepareSlotHandoff`, for `barrier_ms` from that instant
(`handoff_barrier.rs`, `plan_handoff_action` → `client_registry.pause_slot`). Ownership
moves when `CompleteSlotMigration` is applied, which the state machine admits only inside
`barrier_ms` of the *proposer's* `proposed_at_ms`.

`handoff_barrier.rs` argues the local pause is safe because it is armed later than the
proposer's timestamp and therefore expires *after* the admissible window closes. That
argument covers late **arming**. It says nothing about late **applying of `Complete`**: the
source's own view of `slot_assignment` only moves when the source applies that entry. So
whenever

    (source's apply of Complete) − (source's apply of Prepare) > barrier_ms

the source has a window in which its pause has lapsed *and* its replicated view still says
it owns the slot — while the target, which already applied `Complete`, also owns it. Both
nodes route and accept writes for the slot. Nothing in the state machine can see this: it
is purely a per-node apply-lag property.

`HANDOFF_BARRIER_MS` is 100 ms today, so the exposure needs a follower whose apply lags the
leader by >100 ms — a GC pause, a RocksDB stall, a snapshot install, or a node catching up
after a brief partition. That is not exotic.

## Evidence

Model: `frogdb-server/crates/cluster/src/model/mod.rs`, `unbounded_lag_scope()` — identical
to the smoke scope except that a node may apply the replicated log arbitrarily far behind
the leader. `model::tests::stale_source_admits_writes_after_ownership_moves` finds a
`single_writer_per_slot` violation in 1156 states:

```
[Tick, Coord(0), Apply(1), Apply(2), Coord(0), Drain(0), Apply(1), Apply(2), Coord(0), Apply(2), Tick, Tick]
```

Replayed against the real state machine with no model in the loop by
`model::replay::stale_source_keeps_serving_after_the_target_takes_the_slot`: prepare at
t=1 (source barrier armed to t=2), confirm, `Complete` applied by coordinator and target at
t=1 but *not* by the source; at t=2 the source's pause lapses, its snapshot still names it
the owner, and `admits_write` is true on both the source and the target.

Both tests assert the exposure is **still present** — they are characterization tests, so a
fix turns them red and they get flipped rather than silently passing.

The nightly `full_scope()` deliberately keeps `bounded_apply_lag: true`, i.e. it asserts the
handoff is correct *given* the assumption. This issue is about the assumption itself.

## What to build

Spec-first. Add an FM row for the residual (the existing FM-CLUSTER-084/085/086 rows cover
the replicated window, not the source's apply lag), add the forcing test, then fix.

Candidate rulings, to be decided and recorded in the row:

1. **Fence on applied index, not on wall clock.** The source keeps the pause armed until it
   has applied an entry at or beyond the index that carries `Complete`/`Abort` for that
   `handoff_seq` — i.e. release is event-driven (`SlotHandoffReleased`, which already
   exists) and the timeout is only a backstop against a coordinator that never finalizes.
   The current code already releases on the event; the bug is that the *timeout* can fire
   first. Making the pause outlive the timeout until a release arrives closes the window at
   the cost of a slot that stays fenced if the coordinator dies — bounded by the lease.
2. **Make the source's own liveness a precondition of `Complete`.** Require the source's
   applied index (or a lease/heartbeat) to be current before the state machine admits
   `CompleteSlotMigration`. Stronger, but it puts a liveness signal in the deterministic
   apply path, which the design has so far avoided.
3. **Route on a fence token, not on the local snapshot.** The source rejects writes for a
   slot whose `handoff_seq` it has seen prepared until it observes the resolution, making
   the check ownership-independent.

Option 1 is the smallest change and reuses machinery that is already there; the lease
already bounds the "coordinator died" case that the timeout was protecting against.

## Acceptance criteria

- [ ] FM row added for the source-apply-lag residual; `just lint-spec` green
- [ ] Forcing test in `frogdb-cluster` (or `frogdb-cluster-runtime`) fails first
- [ ] `model::tests::stale_source_admits_writes_after_ownership_moves` and
      `model::replay::stale_source_keeps_serving_after_the_target_takes_the_slot` flipped to
      assert a single writer, and `unbounded_lag_scope()` folded into the checked scopes
- [ ] `just mutants-diff` triaged on every touched locked crate

## Blocked by

None.

## Ruling (2026-08-13)

**Option: log-ordered fence (sharpened option 1; wall clock removed from correctness entirely).**
- Fence arms when the source applies `PrepareSlotHandoff`; it is released ONLY by applying `Complete` or `Abort` for that `handoff_seq` (through the existing `SlotHandoffReleased` path).
- The state machine enforces exactly-one-resolution per `handoff_seq`: first of Complete/Abort wins, the second is refused.
- The `barrier_ms` wall-clock admission window on Complete is DROPPED — replaced by the log-ordering rule.
- Orphaned handoffs are aborted by the level-triggered reconcile pass (issue 18's ruling): the leader proposes `Abort`. Time is only a liveness trigger to *propose*; release always flows through the log. Wall clock is not a correctness input anywhere in the handoff protocol.
- CRDB-style epoch leases were considered and rejected: wrong tool here (still needs bounded clock skew, adds hot-path cost, and FrogDB has no per-op consensus cost to amortize).
- Characterization tests `stale_source_admits_writes_after_ownership_moves` and `stale_source_keeps_serving_after_the_target_takes_the_slot` must flip to assert the fence.

## Amendment (2026-08-13)

**Feed hold bounded by bytes, not time.** The log-ordered fence stands (arm on Prepare apply, release only via Complete/Abort apply). The review found the node-wide hold and per-session `VecDeque` unbounded if Complete/Abort lags, falsifying FM-CLUSTER-097's "a feed cannot wedge". Amended: each held session's queue carries a **byte cap**; breaching it **disconnects that replica session** (it full-resyncs after the handoff resolves). Fence release stays purely log-ordered — no time-based release; liveness for orphaned handoffs is owned by issue 18's reconcile abort. The now-undefined `lease_expired` term in `admits_complete_at` is deleted.

## Cross-reference (2026-08-14)

The amendment above assigns orphaned-handoff liveness to "issue 18's reconcile abort" —
issue 18 never defined one (found by distsys-review MAJ-5). That trigger is now specified
in [issue 31](31-slot-migration-redesign-source-authoritative-until-commit.md): the
level-triggered reconcile pass proposes `AbortSlotMigration` for any open migration whose
counterparty carries the FAIL flag.
