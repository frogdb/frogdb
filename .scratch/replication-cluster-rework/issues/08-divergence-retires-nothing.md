# 08 — an admitted divergence keeps its claim and keeps applying

Status: ready-for-agent

## What to build

Two narrow cases where the applied offset claims data the node does not (or may not) hold. Both
weaken the same invariant the promotion boundary rests on — "the frozen offset is exactly what this
node holds" — so they belong together.

### 1. `apply_group` returned `Err` and the loop carries on

`consume_frames` (`frogdb-server/crates/replication/src/apply.rs`, the EXEC arm and the bare-command
arm) claims the group's stream bytes, calls `apply_group`, logs the error, counts a divergence — and
then keeps consuming. Neither arm breaks. The offset therefore advances over a write that never
landed, and the node keeps serving reads and, after a promotion, hands siblings `+CONTINUE` at an
offset that includes it.

The offset advance itself is deliberate and matches Redis (the replica offset counts stream *bytes
consumed*; stalling it would desynchronise every later frame). What is missing is any consequence:
an admitted divergence should retire the stint — stopping the applier and forcing the link to come
back through a full resync — rather than being logged and stepped over. Redis's equivalent
(`replicationHandleMasterDisconnection` / a panic on a failed master command) is deliberately
drastic for this reason: a replica that has provably diverged must not keep vouching for its
history.

Direction: on the first `Err` from `apply_group`, retire the stint (`AppliedOffset::retire_replica_applies`)
and drop the link so the reconnect full-resyncs; keep the divergence counter for observability.
Needs a decision on whether a repeated-failure loop should also refuse to serve reads.

This is **pre-existing** — the received/applied split and the apply gate did not change it — but it
is the one path on which `identity.rs`'s "the boundary is exactly what the applier has claimed"
claim is false. That doc comment has been softened to say so and to point here.

### 2. Crash between a claim and the shard write

The gate claims a group's bytes *before* `apply_group` dispatches it, which is what makes the
promotion boundary exact (a claimed group always completes; an unclaimed one never starts). The
residual: a crash in that window leaves a persisted `offset_at_save` covering a group whose write
never reached a shard, so the rebooted node resumes above data it does not hold and asks for a
`+CONTINUE` past the hole.

The trade is explicit and was taken knowingly:

- credit *after* the apply (commit `98d83a90`) → no crash window, but a live promotion can freeze
  the boundary between the apply and the credit, leaving applied data above the boundary in no
  backlog and outside every replication-id window. That is divergence during a **normal** failover.
- credit *before* the apply (commit `1eebf6ca`, current) → the promotion boundary is exact, and the
  hole needs a crash inside a window of microseconds, one group wide.

Closing it properly means making the claim and the shard write atomic with respect to a crash — i.e.
persisting the applied offset from the shard's own write path, not from the consume loop. That is a
much larger change (it couples replication accounting to shard commit), so it is filed rather than
attempted.

## Acceptance criteria

- [ ] A failed replicated apply stops the applier and forces the link back through a full resync
- [ ] The divergence counter/logging is preserved for observability
- [ ] Decision recorded on read-serving after an admitted divergence
- [ ] The crash window (case 2) is either closed or explicitly documented as accepted in the PRD

## Source

Adversarial review of `promotion-replid-psync.md`, round-3 residuals (2026-07-28).
