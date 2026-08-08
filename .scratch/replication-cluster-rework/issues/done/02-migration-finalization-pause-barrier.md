# Pause barrier at slot-migration finalization (Dragonfly parity)

Status: done

Disposition (2026-08-04, P3 lock review): cluster-scoped — the replication-area lock does not depend on this. Scheduled into Phase 4 cluster hardening (research task: Dragonfly source-side quiesce vs FrogDB's Raft apply). If adopted, subsumes rework 03.
Type: AFK
Origin: exec-slot-revalidation PRD, task T10(b) — deferred hardening
Severity: likelihood 2/3, consequence 2/3 (score 4)
Area: Cluster / Slot migration

## Context

The EXEC slot re-validation landed by the exec-slot-revalidation PRD closes the window in which a
`MULTI` queued on the old owner commits after the slot moved. It does so by re-reading the cluster
snapshot at `EXEC` entry, which leaves one residual window (risk 7 in that PRD): the interval
between the Raft entry that reassigns the slot being *committed* and it being *applied* on the node
that is about to lose the slot. During that interval the losing node's snapshot still names itself
as owner, so an `EXEC` there validates and commits. The window is bounded by Raft apply latency, not
by anything the transaction path controls.

Redis has the same window and accepts it. DragonflyDB does not: it quiesces the source before
finalizing a slot handoff, so no command — transactional or not — can be in flight across the
ownership change. That is a strictly stronger guarantee and it also covers command paths that
re-validation cannot reach, notably Lua scripts, which validate their key set once at invocation and
then execute an arbitrary number of writes afterwards.

## What to build

Research how Dragonfly implements the source-side quiesce (`cluster_family`/slot-migration
finalization) and evaluate it against FrogDB's Raft-driven control plane, where the pause has to be
coordinated with the Raft apply rather than with a gossip epoch bump. The mechanism FrogDB already
has is `CLIENT PAUSE` (`connection/transaction.rs` waits on it before the shard round-trip); the
work is deciding who arms it, on which nodes, and how the barrier is released if the finalizing node
dies mid-handoff.

Sketch:

1. Before committing the `SETSLOT NODE` entry, the control plane arms a write pause on the source
   node scoped to the migrating slot.
2. The source drains in-flight commands for that slot.
3. The reassignment is committed and applied.
4. The pause is released, and everything still queued gets the new snapshot.

## Acceptance criteria

- [x] A written comparison of Dragonfly's approach against FrogDB's Raft control plane, with the
      failure modes of a source that dies while paused. (Phase 2a comment; the lease and the
      abort-rather-than-proceed decision are the two places FrogDB deliberately differs.)
- [x] A decision recorded in the PRD directory: adopt, adopt-scoped (slot-scoped pause only), or
      reject with the residual window documented as intended behavior. (Adopt, Option A amended,
      on the 2026-08-05 measurement.)
- [x] If adopted: a test that a slot handoff completing concurrently with an in-flight Lua script
      cannot leave a write on the former owner (the property `EXEC` re-validation cannot provide).
      (`a_script_in_flight_across_a_handoff_leaves_no_write_on_the_former_owner`, phase 2b.)

## Interaction with the EXEC re-validation already in place

`connection/transaction.rs` validates a write batch *before* the `CLIENT PAUSE` wait (so an EXEC that
is already doomed fails fast instead of occupying a pause slot) and re-validates it afterwards
**only when the wait actually blocked** — `wait_if_paused_for_transaction` returns whether it parked.
Without that second pass an EXEC held across a finalization would resume on a stale verdict and
commit on the former owner, which would silently defeat this barrier the day it is armed. Anyone
implementing the barrier must keep the post-wait re-validation, or replace it with something
strictly stronger.

There is no integration test for the re-validation arm: driving it requires a `CLIENT PAUSE WRITE`
on the source that outlives a slot handover, and the handover's own `MIGRATE` runs through a client
connection on that same paused node. Adding the barrier gives the test a natural hook — the barrier
itself becomes the thing that holds the EXEC — so the coverage should land with this issue.

## Notes

Gated behind a decision, not a straight implementation task. The pause barrier costs availability at
every migration finalization; that trade may not be worth it if the residual window is measured and
found to be sub-millisecond in practice. Measure first.

## Comment — 2026-08-05: residual window measured

Sequencing step 1 of
[migration-pause-barrier-brief-2026-08-04.md](../../migration-pause-barrier-brief-2026-08-04.md) §7
is done. Full methodology, numbers, and recommendation:
[finalization-window-measurement-2026-08-05.md](../../finalization-window-measurement-2026-08-05.md).

Harness: `frogdb-server/crates/server/tests/cluster_finalization_window.rs` — test-only, every case
`#[ignore]`d, no production code instrumented. 3-node in-process cluster, 120 finalizations per
scenario, commit→apply observed by polling each node's `ClusterState` from a dedicated OS thread.

Residual window on the losing node (`t_source − t_leader`), microseconds:

| Scenario | p50 | p99 | max |
|---|---|---|---|
| follower-source, idle, shipped Raft timing | 248.8 | 541.0 | 887.3 |
| follower-source, loaded (32 writers + Raft churn) | 684.2 | 1927.4 | 2030.6 |
| leader-source (control) | −0.9 | 34.8 | 52.5 |

So the window **is** sub-millisecond typically (p99 0.54 ms) and ~2 ms under load. But it is not
benign: with a client writing the slot across the handover, the source acknowledged a write *after*
the cluster committed the handoff in **68/120** idle and **118/120** loaded finalizations. Raft
heartbeat interval does not affect it (100 ms vs 250 ms are indistinguishable) — openraft pushes the
commit index eagerly, so the window is follower apply *scheduling*, and it widens with node load.

**Recommendation: build the Option A barrier**, with two amendments the measurement forces:

1. It must also cover commands already past the routing guard — the leader-source control has a
   ~0 state window yet still shows ~150 µs p99 of post-flip acks, which a routing-admission gate
   cannot see. Option C (fencing token at the execute seam) is the cheap complement, not an
   alternative.
2. The prepare phase needs an explicit deadline so a source that never hears phase two resumes
   serving instead of blackholing the slot. Tens of milliseconds is 3–4 orders above the measured
   p99.

Key argument: the barrier's added unavailability is one extra Raft round trip on one slot — the same
0.2–2 ms it removes. The availability/correctness trade this issue was gated on is roughly 1:1, so
it is not a close call.

The harness doubles as the acceptance test: assert *"iterations with an acknowledged write after
commit = 0"* over ≥120 loaded iterations. It reports 118/120 today, so it is a live reproduction.

## Comment — 2026-08-06: phase 1 landed (slot-scoped pause), issue stays open

Sequencing step 2 of the brief §7 — the pause *mechanism*, landed independently and ahead of the
Raft two-phase work so it can be reviewed and mutation-tested on its own. **Nothing arms it in
production yet**: the barrier that will arm it is phase 2.

What exists now:

* **`PauseState` has two independent dimensions** (`core/src/client_registry/mod.rs`) — one
  node-global `PauseEntry` (today's `CLIENT PAUSE`, semantics unchanged) and a
  `HashMap<u16, PauseEntry>` of per-slot pauses. `PauseEntry::arm` implements the Redis merge rule
  (max mode, max deadline) once for both. New API: `pause_slot`, `unpause_slot`, `slot_pause`,
  `pause_overview`, `any_pause_active`; `check_pause` is gone. `unpause` clears only the node
  dimension, so `CLIENT UNPAUSE` can never disarm a migration barrier.
* **`should_pause_command` is slot-aware** (`server/src/connection/lifecycle.rs`). One lock answers
  the common unbarriered case; a command's slot is resolved only when a slot pause actually exists.
  The two decisions scoping needs are pure functions in the new
  `server/src/connection/pause_gate.rs`, so they are unit-testable without a socket.
* **Fail-closed on unpinnable commands.** A command naming no keys (`FLUSHALL`) or keys in more than
  one slot cannot be pinned, so it parks against the strongest pause armed on *any* slot. `FLUSHALL`
  erases the barriered slot along with everything else; "no slot" is not "no exposure".
* **Both brief §4 hazards are dodged by design and pinned by test.** `MIGRATE` (hazard 3) and
  `CLUSTER` (hazard 4, including the `SETSLOT … STABLE` escape hatch) are exempt from *slot-scoped*
  pauses only; a node-global `CLIENT PAUSE` still parks both, exactly as Redis does.
* **Test seam:** `DEBUG PAUSE-SLOT <slot> <timeout-ms> [WRITE|ALL]` (0 ms disarms).
  `CLIENT PAUSE` was deliberately **not** extended — Redis has no slot argument and the parity break
  would be visible to every client library.
* **Spec:** `FM-CLUSTER-079..083` in `.scratch/hardening/specs/cluster-failure-modes.md`, with the
  "pause barrier is out of scope" line replaced by a scope bullet and a pointer back to this issue
  for the phase-2 rows.

Tests: `frogdb-server/crates/server/tests/cluster_pause_barrier.rs` (5 integration witnesses),
plus unit tests in `client_registry/mod.rs` and `connection/pause_gate.rs`.

The `exec.rs` post-wait re-validation contract is untouched, and the previously-missing coverage
this issue called for now exists: `write_exec_parks_on_a_slot_barrier_and_commits_after_release`
pins that queuing is never parked, `EXEC` is, and the batch commits only after release.

**Known phase-1 limitation, deliberately unpinned.** `frogdb-txn` is LOCKED and
`TxnHost::wait_if_paused(&mut self)` is handed no queue, so the server cannot resolve a transaction's
slot at the gate. A write `EXEC` therefore parks on *any* armed pause, including a barrier on a slot
it never touches. That over-parks by at most the barrier's own lifetime and never under-parks.
Narrowing it means widening the `TxnHost` seam, which belongs with phase 2; FM-CLUSTER-083 records
the non-guarantee in prose rather than pinning it, so phase 2 can narrow it without a spec edit.

Still to build (phase 2, brief §6 Option A): `PrepareSlotHandoff` as a Raft op, the drain
(shard-mailbox round trip), the prepare deadline the measurement forced, the handoff lease, and the
fencing token at the execute seam that covers commands already past the routing guard. The Lua-script
acceptance criterion above is a phase-2 deliverable — a slot-scoped pause alone does not stop a
script that validated its key set before the barrier armed.

## Comment (2026-08-07): phase 2a landed — two-phase finalize, drain, deadlines

`CLUSTER SETSLOT <slot> NODE <target>` no longer moves ownership in one Raft entry. Brief §6
Option A is built: the finalization is a prepare/drain/complete saga carried entirely through the
replicated log, with three independent deadlines and a fencing sequence number.

**The saga.** `SlotMigrationHandler::complete` proposes `PrepareSlotHandoff { slot, source_node,
target_node, barrier_ms, lease_ms, proposed_at_ms }`. Applying it attaches a `SlotHandoff` to the
existing `SlotMigration` record and emits `ClusterEvent::SlotHandoffPrepared`. On the *source* node
only, that event arms a slot-scoped `PauseMode::Write` barrier through the phase-1
`ClientRegistry::pause_slot` and sends `ClusterMsg::DrainSlot { slot, ack }` to shard
`slot % num_shards`. The shard is single-threaded, so the mailbox round trip *is* the drain: when
the ack returns, every command that was already inside that shard has finished. The source then
proposes `ConfirmSlotHandoffDrained { slot, seq }`, which sets `handoff.drained`. The finalizer,
polling its own applied state, sees `drained`, proposes `CompleteSlotMigration` as before, and the
existing `SlotMigrationCompleted` event releases the barrier on the source.

**Ack transport: a follow-up Raft entry, not a point-to-point RPC** (brief hazard 1). The finalizer,
the source, and the Raft leader can all be three different nodes. A `BusRpc` ack would be visible
only to whoever received it; a replicated entry applies everywhere, so the finalizer reads the drain
out of its own local `ClusterState` regardless of topology. It costs one extra Raft round trip per
finalization — bounded, off the data path, and paid only at handoff. The integration witness
`a_source_that_cannot_drain_aborts_the_finalization` deliberately finalizes from the node that is
neither source nor leader, so this property is pinned rather than assumed.

**Determinism.** `apply_command` never reads a clock. Every deadline is proposer-minted data:
`handoff_now_ms()` (the `frogdb_core::clock` seam) stamps `prepared_at_ms` into the log entry, and
each node compares that stored instant against its own `system_now()`. Two nodes applying the same
entry at different instants therefore compute the same admissibility answer.

**Four bounds, deliberately staggered.** *Barrier* 100 ms (`HANDOFF_BARRIER_MS`) — how long the
source stops writes, and the window in which a `CompleteSlotMigration` is admissible at all. The
finalization-window measurement put loaded p99 apply at 1.9 ms, so 100 ms is ~50x headroom.
*Finalizer drain budget* 50 ms (`HANDOFF_DRAIN_WAIT_MS`, polled every 2 ms) — deliberately shorter
than the barrier, so whichever way the attempt resolves, the deciding entry lands inside the window
the source is still holding. *Drain timeout* 2 s (`HANDOFF_DRAIN_TIMEOUT_MS`, the
`CONTINUATION_DRAIN_TIMEOUT` shape) — the source's own bound on its shard round trip; on expiry it
simply never confirms. *Lease* 10 s (`HANDOFF_LEASE_MS`) — the consensus-level backstop. A prepared
record past its lease is invisible to every reader (`SlotMigration::live_handoff_at`), so a
finalizer that dies between prepare and complete cannot wedge a slot; a later attempt supersedes it.
The lease is visible in `snapshot().migrations` and is cleared by `SETSLOT … STABLE` along with the
migration.

**Dragonfly's drop-the-pause-and-proceed is explicitly not what happens.** A drain that never
arrives *aborts*: the finalizer proposes `AbortSlotHandoff { slot, seq }`, the migration record
survives untouched, and the caller gets `TRYAGAIN slot N handoff not ready: …`. Ownership does not
move. `SETSLOT … STABLE` remains the operator's way out. `ClusterError::HandoffNotReady` is the only
new error and it reports `is_retryable() == true`, which is what renders the `TRYAGAIN` prefix.

**Late `Complete` is refused, never half-applied.** The `CompleteSlotMigration` arm now requires the
migration to exist, its parameters to match, and `handoff.admits_complete_at(proposed_at_ms)` —
drained, inside the barrier window, inside the lease. A `Complete` that arrives after an abort or
after the barrier lapsed is rejected with `HandoffNotReady` and changes nothing, so both release
paths (complete and abort) are idempotent against a straggler.

**Stale attempts are fenced by `seq`.** `ClusterStateInner` carries a replicated `handoff_seq`
counter; each prepare mints the next value. `Confirm` and `Abort` must name the seq they belong to,
so an ack from an attempt that was already aborted cannot vouch for its replacement.

**FM-011 (concurrent second-slot finalization): compose, do not serialize.** Handoffs are keyed by
slot in `migrations`, and phase 1's `PauseState.slots` is already a per-slot map, so two
finalizations that happen to share a shard proceed independently — neither waits on the other's
barrier, and each drain ack is matched by slot and seq. Pinned by FM-CLUSTER-088.

The remaining hazard pins are inherited from phase 1 and re-verified rather than rebuilt: `MIGRATE`
stays exempt from slot pauses (FM-009), `CLUSTER` stays exempt so `SETSLOT NODE` is never parked by
the barrier it is about to end (FM-010), blocked readers still wake with `MOVED` because
`DispatchStage::PauseGate` precedes `DispatchStage::ClusterSlotValidation` (FM-012), operator
`CLIENT PAUSE` composes because it occupies the node dimension of `PauseState` and `CLIENT UNPAUSE`
cannot disarm a slot barrier (FM-013), and a leadership change during any of the three proposals
surfaces as `ProposeError::Redirect` through the ordinary `ClusterWriter` saga, which the caller
retries (FM-008).

**Shard-dispatch classification.** The new `ClusterMsg::DrainSlot` arm in
`core/src/shard/dispatch_cluster.rs` is **EXEMPT** from the C3 continuation-lock discipline: it
acquires no keys, holds no state across messages, and contains no await point. It sends the ack and
returns; the round trip *is* the drain, and taking a continuation lock would defeat its purpose.

**Spec.** `FM-CLUSTER-084..092` in `.scratch/hardening/specs/cluster-failure-modes.md`. Note the
namespace deviation from the brief: `FM-CLUSTER-BARRIER-NNN` is unparseable by
`scripts/failure-modes.py` (it derives the area from the filename and matches `FM-([A-Z]+)-(\d+)`,
so a hyphenated area matches neither the heading nor the tag regex) and the rows would have been
invisible in both directions. The rows continue phase 1's `FM-CLUSTER-NNN` series.

Tests: 16 unit tests in `cluster/src/commands.rs` (admissibility, seq fencing, lease expiry, release
unification), 7 in `cluster-runtime/src/handoff_barrier.rs` (arm/release planning, shard targeting,
source-only action, unanswered shard), and 2 integration witnesses in
`frogdb-server/crates/server/tests/cluster_handoff_barrier.rs`
(`a_write_parked_by_the_barrier_wakes_up_redirected`,
`a_source_that_cannot_drain_aborts_the_finalization`). The `exec.rs` post-wait re-validation
contract is untouched; `frogdb-txn` was not modified.

**Still to build (phase 2b).** The fencing token at the execute seam, for commands already past the
routing guard when the barrier arms — the types are ready for it: `SlotHandoff::seq` is the token,
and `live_handoff_at` is the lookup. Widening the `TxnHost` seam so a write `EXEC` parks only on the
slots it touches (the phase-1 over-park, still recorded in prose by FM-CLUSTER-083). The Lua
acceptance criterion, which needs the execute-seam token rather than the routing guard. And flipping
`cluster_finalization_window.rs` from a measurement harness to an assertion of 0/120.

## Comment (2026-08-07): phase 2b landed — fencing token, slot-scoped EXEC parking, 0/132 asserted

All four phase-2b deliverables are in. **Every acceptance criterion of this issue is met**, including
the third one, which was the only one still open.

**The fencing token (`slot_migration/slot_fence.rs`).** `stamp_fence` records
`(slot, owner, handoff_seq)` when `ClusterSlotValidation` decides to serve locally; `fence_verdict`
re-reads the snapshot at execution and refuses if either half moved. The carrier is the connection —
one `Option<SlotFence>` field, stamped by the validation stage and spent at the dispatch driver's
single `ShortCircuit` arm, which is where shard commands, bare scripts and `EXEC` all converge. It is
spent before `record_error_response`, so accounting sees the response the client sees.

Design decisions worth recording, because each one had a plausible-looking wrong answer:

- **`handoff_seq`, not "am I still the owner?".** The naive token cannot work: the source only
  learns ownership moved by *applying* `CompleteSlotMigration`, which is after the cluster committed
  it — that lag is precisely the residual window. `handoff_seq` moves at `PrepareSlotHandoff`, a
  full Raft round trip earlier, so the token is already stale when the window opens.
- **Reply class per case.** `TRYAGAIN` while this node is still the owner: a prepared handoff can
  still abort, and naming the target would bounce the client to a node that may never take the slot.
  `MOVED` once ownership actually moved. `CLUSTERDOWN` when the new owner has no addressable entry.
- **Per-slot, not a node-wide epoch.** The loaded measurement scenario fires `SETSLOT … STABLE` on
  an unrelated slot every 2 ms; a global epoch would refuse constantly. Pinned by
  `a_handoff_on_another_slot_is_invisible`.
- **The token ignores the lease.** Expiry is a function of the clock, and a lease-filtered token
  would read "unchanged" across a prepare whose lease had lapsed. The raw replicated `seq` is pure
  data, consistent with FM-CLUSTER-089.
- **Only the owner is stamped.** Fencing the importing target would have it answer `MOVED` naming
  itself.
- **Cluster awareness stays out of `frogdb-core`,** per the brief: the fence reads
  `snapshot.migrations[slot].handoff.seq` at the server seam. `frogdb-cluster` was not touched, so
  its lock is undisturbed.

The coordinator and the dispatcher hold the same `Arc<ClusterState>` (`cluster_init.rs` builds one
and passes it to both), so the stamp and the verdict are cut from the same published view.

**`TxnHost` widening.** `wait_if_paused` now takes the queued batch. `queue_pause_slot` folds the
batch's commands through the same `command_pause_slot` a single command uses and answers `None` —
fail-closed — on the first unpinnable command or the first disagreement; `pause_active_for_batch`
consults the node dimension first and the slot map only if a slot-scoped pause exists at all. The
`exec.rs` contract survives verbatim: validate before the wait, re-validate *only* if it parked. An
`EXEC` on an unbarriered slot now commits while a barrier is up, and a second `EXEC` on the
barriered slot in the same test still parks — a narrowing, not a disarm.

**Lua criterion.** `a_script_in_flight_across_a_handoff_leaves_no_write_on_the_former_owner`: an
`EVAL` whose body is `redis.call('SET', KEYS[1], …)` parks on the barrier, the handoff completes
underneath it, the reply is `MOVED`, and `DBSIZE` on the former owner is `0`. Every redirect
assertion in `cluster_handoff_barrier.rs`, including the pre-existing FM-CLUSTER-092 one, is now
paired with a `DBSIZE` check — being told `MOVED` is the client's half of the property; nothing
landing on the node that stopped owning the slot is the cluster's half.

**The acceptance harness now asserts.** `no_write_is_acknowledged_after_the_slot_is_handed_over_under_load`
runs the loaded scenario (32 writers + Raft churn, shipped timing, source is a follower) for 132
finalizations and asserts zero acked writes past the commit. **Result: 0/132, repeatedly, in ~3.5 s
of runtime**, so it is not `#[ignore]`d; the four measurement cases stay ignored. Baseline for the
same scenario on 2026-08-05 was 118/120. Two guards keep the pass from being vacuous: the sample
count is asserted (discards cannot pass the case by emptying it), and a supermajority of iterations
must have acked a write *before* the handoff (a source that refused everything would also score
zero, but that is an outage, not a fix).

Two honest caveats on that number:

- The harness measures `t_last_ok` at the *client*, after the reply is read, while `t_leader` comes
  straight off the leader's applied state. That biases towards false positives, never false
  negatives — the safe direction for an acceptance case, but it means a badly starved machine can
  turn a legitimately-early ack into a red run. The margin is the several milliseconds the drain
  round trip and two Raft commits take.
- **The residual window itself is not closed and is not claimed to be.** `t_source - t_leader` is
  still ~0.7 ms p50 / ~2.5 ms p99 under load; it is a replication lag. The claim is that nothing
  client-visible happens inside it. Relatedly, a fenced command's write may still have *applied*
  locally — the fence refuses the acknowledgement, not the work — so its status is in doubt exactly
  as a write whose connection dropped is, and the client's retry lands on the new owner. That is
  at-least-once, not the acknowledged-then-orphaned write of FM-CLUSTER-037.

**Spec.** New rows `FM-CLUSTER-093` (EXEC parked across a finalization), `094` (Lua), `095` (the
fencing token), `096` (slot-scoped EXEC parking). FM-CLUSTER-083's "accepted non-guarantee for
phase 1" is retired and its invariant rewritten; the phase-2a "still unpinned" paragraph is replaced
with a phase-2b one. The Redis-deviations row claiming script slot validation is absent was stale
since FM-CLUSTER-030 was fixed, and now describes the once-at-the-seam-plus-fence design against
Redis' per-`redis.call` `scriptVerifyClusterState`.

**Shard dispatch.** No dispatch arm changed, so no C3 classification and no
`continuation-lock-gate.py` pin edits; the gate still reports 65 arms pinned. `just lint-gates` and
`just lint-failure-modes` are green.

**Mutation testing (`frogdb-txn` is locked).** `just mutants-diff frogdb-txn` found 2 mutants and
both were unviable — the crate's diff is a trait signature (no body to mutate) and one call site
inside `execute_transaction`, whose only mutation is replacing the whole function, which does not
compile. A diff run with no viable mutants is no evidence, so the full crate run was used instead:
`just mutants frogdb-txn` → **48 mutants, 38 caught, 0 missed, 10 unviable, score 100%**,
`just mutants-gate frogdb-txn 0.90` PASS. Worth noting for the next agent who touches a locked
crate with a signature-only diff: `mutants-diff` will look green while measuring nothing.

**Still unpinned, deliberately.** The cross-shard script holding a VLL continuation across a handoff
has no row: the continuation-lock gate has two tracked bypasses of its own (`MULTI`/`EXEC`, `FCALL`),
so a row written now would pin barrier behaviour on top of a seam that is itself known-leaky. The
`PAUSE_ACTION_REPLICA` question (should the barrier stall a primary's replica feed?) remains
undecided.

## Resolution — 2026-08-07: Option A (amended) built, acceptance criterion met

All three acceptance boxes:

- [x] Written comparison: `migration-pause-barrier-brief-2026-08-04.md` + the measurement doc.
- [x] Decision recorded: adopt Option A slot-scoped, fail-closed, with the two measurement-forced
      amendments (fencing token at the execute seam, explicit prepare deadline). User-approved.
- [x] The Lua test: `a_script_in_flight_across_a_handoff_leaves_no_write_on_the_former_owner`
      (`cluster_handoff_barrier.rs`) — the property EXEC re-validation cannot provide.

Shipped in three merges: phase 1 slot-scoped pause (2026-08-06), phase 2a two-phase finalize saga
with Raft-carried drain-ack + staggered deadlines (4c632ea6), phase 2b execute-seam fencing +
slot-scoped EXEC parking + acceptance flip (0f5fcf49). Headline number: the loaded scenario in
`cluster_finalization_window.rs` went from **118/120 acked-after-commit to an asserted 0/132**,
running un-ignored in the default suite (~5 s). The `exec.rs:207` post-wait re-validation contract
survived verbatim and now has the genuine parked-EXEC-across-finalization coverage this issue
called for (`an_exec_parked_by_the_barrier_wakes_up_redirected`, FM-CLUSTER-093).

Spec: FM-CLUSTER-084..096. Residuals, tracked elsewhere: cross-shard script holding a VLL
continuation across a handoff has no row (blocked on the continuation-lock gate's own tracked
bypasses — round-2 #50, hardening-2 #05); single-shard Lua re-validation for issue 03 is subsumed,
its cross-shard half stays open on issue 03; the barrier-vs-replica-feed policy is issue 12.
