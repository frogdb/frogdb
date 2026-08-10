# 07 — Decision/IO seam extraction, tiers (i) and (ii)

Status: done

## Parent

[PRD](../../PRD.md) §3 W3 (the precondition deliverable); scope ruled in §8 D2 — "land (i) and
(ii) first as stepping stones".

## What to build

The models in issues 08 and 09 need a pure decision function, and today the decisions live inside
async methods that own sockets, shard senders, timers and files. W3's first deliverable is
therefore a **decision/IO split**: extract the decision half of each modelled transition into a
pure `fn(&ReplicationView, Action) -> Outcome` and leave the async method as the I/O half that
calls it. This is not a novel shape for the area — it is the shape
`PartialSyncReplay::handle_partial_sync_request` (`primary/replay.rs:350`) already has: state plus
a request plus the current offset in, a `ReplayDecision` out, no I/O.

**Tier (i) — what the two models need.** The promotion transition around
`PrimaryReplicationHandler::begin_primary_stint` (`primary/mod.rs:389`), which is already
synchronous and already transactional in character, so it is the first candidate; and the
feed-gate transition (`ReplicaFeedGate::publish`, `feed_gate.rs:75`). `AppliedOffset::{freeze,
claim, land, admit_divergence}` are synchronous too and entangled only with their own atomics, so
they come along cheaply. The session loop is untouched by this tier.

**Tier (ii) — the symmetry win.** Split the PSYNC arm selection out of `ReplicaConnection::psync`
(`replica/connection.rs:224`) into a pure function beside `handle_partial_sync_request`. This is
good architecture independent of stateright: it deletes the asymmetry ADR 0004 implicitly created,
where the primary side has a pure decision function and the replica side does not, and it is
bounded to one function.

Tier (iii) — the `replica_session.rs` restructure — is authorized by D2 but is **issue 10**, not
this one. Keep the boundary.

Locked-crate discipline applies (`frogdb-replication`, gate 0.85): every step spec-first against
`.scratch/hardening/specs/replication-failure-modes.md`, where rows may move their file:line
citations but not their meaning, and `just mutants-diff` on each touched locked crate before push.
This is a refactor, not a behavior change — no FM row's claim changes.

Soft coupling worth coordinating rather than blocking on: the extracted functions take
`&ReplicationView` from issue 02. If 02 has not landed, extract against the concrete component
state and re-type onto the view when it exists; the split itself is what matters.

## Acceptance criteria

- [x] Promotion decision extracted as a pure function; `begin_primary_stint` becomes the I/O half
      that calls it, and the W1 hook still fires on the seam
- [x] Feed-gate transition extracted the same way, with `ReplicaFeedGate::publish` reduced to the
      I/O half
- [x] PSYNC arm selection is a pure function beside `handle_partial_sync_request`;
      `ReplicaConnection::psync` selects nothing itself and does I/O only
- [x] No behavior change: every `replication-failure-modes.md` row keeps its meaning (citations
      may move), `just lint-failure-modes` green
- [x] `just test frogdb-replication` and `just test frogdb-server` green; `just mutants-diff` on
      each touched locked crate triaged before push

## Blocked by

None — can start immediately.

## Resolution (2026-08-10)

Three decisions extracted, each a plain-data function with no lock, file, socket, clock or
entropy inside, each with direct unit tests in the mutated crate.

**Tier (i), promotion** — `plan_primary_stint(previous: &ReplicationState, minted_id: String,
boundary: u64) -> StintPlan` (`frogdb-replication/src/primary/promotion.rs`). `StintPlan` carries
both outcomes of the transition as data: `minted` (the state to publish and persist), `rollback`
(what to restore if the persist fails — the previous state bit for bit) and `backlog_floor`.
`PrimaryReplicationHandler::begin_primary_stint` is the I/O half: it disarms the staged
checkpoint, settles the heads, **draws the entropy** (`generate_replication_id`), then takes the
state write lock, applies `plan.minted`, persists, restores `plan.rollback` on failure and arms
`plan.backlog_floor`. Hoisting the mint into the caller is what makes the planner a function of
its inputs; the `"Generated new replication ID"` log moved with it, verbatim, so the operator
surface is unchanged. `ReplicationState` gained `PartialEq, Eq` so the rollback can be asserted
as equality rather than field-by-field.

**Tier (i), feed gate** — `decide_publish(current: Option<Instant>, next: Option<Instant>) ->
FeedGatePublish` and `decide_hold(published: Option<Instant>, now: Instant) -> Option<Instant>`
(`frogdb-replication/src/feed_gate.rs`). `ReplicaFeedGate::publish` now owns only the mutex and
the `Notify` (wake iff `Store`), and `hold_deadline` only the mutex and the clock read. `now` is
a parameter for the same reason the mint is: a decision that reads the clock is not a decision a
model can drive.

**Tier (ii), PSYNC arm selection** — `frogdb-replication/src/replica/psync.rs`, the missing twin
of `PartialSyncReplay::handle_partial_sync_request`: `select_psync_arm(line: &str) ->
io::Result<PsyncArm>` (`FullResync { granted_id, granted_offset }` | `Continue { granted_id }` —
two variants for the same reason `ReplayDecision` has two), `select_full_resync_payload(line:
&str) -> io::Result<FullResyncPayload>` (`Checkpoint` | `LiveDataset`), and `psync_request_args`
moved over from `connection.rs` unchanged. `ReplicaConnection::psync` selects nothing: it writes
the request, reads the lines, moves the offset/state/link and reads the payload count. Order of
effects and every error fingerprint are byte-identical — the arm decision still fails before the
offset rewind, the payload decision still fails before the count line is read.

Behavior is unchanged throughout; spec edits are citations only (`FM-REPLICATION-019`/`020` name
`plan_primary_stint`, `FM-CLUSTER-097` names `decide_publish`/`decide_hold`,
`FM-REPLICATION-001` names `select_full_resync_payload` and `013` names `select_psync_arm`), with
the 18 new tagged tests added to the corresponding `Forced by` lists.

Results: `just test frogdb-replication` 427/427, `just test frogdb-replication-runtime` 36/36,
`just test frogdb-server 'psync|replic|fullresync|full_sync|failover'` 274/274,
`just lint-failure-modes` OK (279 rows, 1419 tags), `just mutants-diff frogdb-replication` 26
mutants — 19 caught, 7 unviable, **0 missed**. `frogdb-replication-runtime` was not touched.

**Not taken: the `AppliedOffset::{freeze, claim, land, admit_divergence}` ride-along** named in
tier (i). Those four are already synchronous, lock-local and directly unit-tested — no socket, no
clock, no async traps their decisions, which is the thing the split exists to undo. `claim`'s
decision is a comparison over three atomics read under the gate the method holds; extracting it
would produce a function whose arguments only the lock holder can assemble consistently, adding a
snapshot type without adding a testable decision. If issues 08/09 end up modelling the
applied-offset gate, the same split can be taken then, against a real consumer.
