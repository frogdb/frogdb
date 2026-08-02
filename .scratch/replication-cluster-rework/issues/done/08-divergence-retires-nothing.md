# 08 — an admitted divergence keeps its claim and keeps applying

Status: done

## Resolution — an epoch-keyed divergence latch (2026-08-02)

**Deliberate deviation from this issue's own fix direction.** "Retire the stint
(`AppliedOffset::retire_replica_applies`) and drop the link" is wrong for this codebase and is
explicitly forbidden by FM-REPLICATION-007: `consume_frames` is a *long-lived* task that outlives
connections, so retiring its stint stops it for good — the replica would silently apply nothing for
the rest of the process, including after the full resync this fix is trying to force. That is a
worse bug than the one being fixed. The issue was written before issue 06 landed the history epoch,
which is what makes the correct version cheap.

What was built instead: a **divergence latch keyed by history epoch** on `AppliedOffset`
(`diverged: AtomicU64`, sentinel `u64::MAX` = none, plus a `Notify`).

- `apply_group` returning `Err` calls `ReplicaApplyStint::admit_divergence(epoch)`. That takes the
  gate, compares `epoch` against the live epoch, and stores only on a match — so a full resync that
  landed between the claim and the `Err` wins and the doomed history is simply forgotten (without
  this check a failure would latch against a history that no longer exists and force a second,
  pointless resync).
- `claim` checks the latch under the same gate, right after its epoch check, and returns
  `Claim::Stale` — **not** `Retired`. Later frames on that history are dropped without reaching a
  shard, the applied head stops, `land()` is never reached, so the ACK keeps reporting the last
  truly-applied offset and `WAIT` times out rather than lying. The consumer stays alive and resumes
  on the next epoch.
- The connection task parks on `AppliedOffset::divergence()` as a `select!` branch in
  `stream_replication` (and re-checks once before the pre-loop drain, for a divergence outstanding
  at connect). On wake it logs at `error`, `reset_to(0)`s the received head — the existing "rewind
  to force `PSYNC ? -1`" mechanism — and returns `Err`, so `start()` takes its exponential-backoff
  reconnect path rather than hot-looping a full resync every 100 ms.
- `reset_pair` stores the sentinel back under the gate, in the same critical section that bumps the
  epoch. So the latch clears if and only if a fresh dataset was installed: it survives reconnects,
  `+CONTINUE`s and promotion/demotion round trips.

Parse failures are deliberately **not** divergences — they reach no shard and break no keyspace, so
they stay counted-and-skipped as before.

**Criterion 3 — reads during the window: keep serving them.** Matches Redis's
`replica-serve-stale-data yes` default, and the FrogDB window is tighter than Redis's because the
link is dropped the instant the latch is seen rather than after a timeout. Refusing reads would
need a `-MASTERDOWN`-style gate on the read path plus a knob; not taken.

**Criterion 4 — case 2 (crash between a claim and its shard write) is accepted, not closed.** This
fix only covers failures the applier is alive to observe. Closing the crash window means persisting
the applied offset from the shard's own write path, which couples replication accounting to shard
commit. Recorded in FM-REPLICATION-010's "Not covered here" and in `identity.rs`'s `applied` field
doc, which now names this as the *only* remaining over-claim case. **Follow-up: keep this tracked
as issue 08's residual.**

Pre-fix evidence (latch wired out of `apply.rs`, everything else in place):
`a_failed_apply_stops_the_history_it_happened_on` FAILED,
`a_diverged_applier_resumes_on_the_history_a_resync_installs` timed out at 15 s (the consumer never
stops, so the test waited forever for a stalled head).

Spec: FM-REPLICATION-010 (`.scratch/hardening/specs/replication-failure-modes.md`). Tests:
`a_failed_apply_stops_the_history_it_happened_on`,
`a_diverged_applier_resumes_on_the_history_a_resync_installs` (apply.rs);
`a_diverged_history_is_refused_until_a_resync_replaces_it`,
`a_divergence_on_a_history_a_resync_already_replaced_is_ignored`,
`the_divergence_wait_resolves_however_it_races_the_latch` (replica/offset.rs);
`an_admitted_divergence_drops_the_link_and_rewinds_for_a_full_resync`,
`a_divergence_outstanding_at_connect_abandons_the_new_link_at_once` (replica/streaming.rs).

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

- [x] A failed replicated apply stops the applier and forces the link back through a full resync
- [x] The divergence counter/logging is preserved for observability
- [x] Decision recorded on read-serving after an admitted divergence
- [x] The crash window (case 2) is either closed or explicitly documented as accepted in the PRD

## Source

Adversarial review of `promotion-replid-psync.md`, round-3 residuals (2026-07-28).
