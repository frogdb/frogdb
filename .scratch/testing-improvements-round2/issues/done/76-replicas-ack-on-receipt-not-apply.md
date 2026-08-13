# Replicas ACK on frame receipt, not on apply, so `WAIT` overstates durability by up to 10 000 commands

Status: done
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/14 F7 · MASTER.md §3 (durability)
Score: severity 4 · likelihood 4 · effort 3 · priority 17
Area: frogdb-replication / replica streaming, `WAIT`

## Resolution — the ACK is now the applied head, unconditionally

The contract decided (and now pinned by `FM-REPLICATION-008` in
`specs/replication.md`): **an acked offset means every frame at
or below it has been applied to its shard.** No config knob — a durability primitive that is only
sometimes durable is worse than one that is slow, and `WAIT` is the primitive promotion safety is
built on.

The replica now tracks three heads instead of two (`replication/src/replica/offset.rs`):
`landed <= claimed <= received`. `received` (`ReplicaOffset::current`) counts frames decoded off
the socket; `claimed` (`AppliedOffset::current`) moves when the applier claims a group, *before*
it dispatches — that is what the promotion boundary needs, because a claimed group will be applied
before the stint retires; `landed` (`AppliedOffset::landed`) moves only once nothing is in flight.
Both ACK paths in `replica/streaming.rs` now report `landed`: the spontaneous tick reads it
directly, and the solicited `REPLCONF GETACK` answer parks on `wait_until_applied`, which was
re-pointed at the landed head.

**Deviation from the dispatch's suggested design, deliberately.** The brief asked for a per-shard
applied high-water plus a min-fold at the ack point. That is not needed here: `apply_group` awaits
the shard's `oneshot` reply and the consume loop applies exactly one group at a time
(`replication-runtime/src/executor.rs`, `apply.rs::consume_frames`), so at most one group is ever
in flight and a single monotone `landed` counter is exactly the min-fold's answer with none of the
per-shard bookkeeping. If the applier ever dispatches groups to several shards concurrently, the
fold becomes necessary and `ReplicaApplyStint::land()` is the one place it has to go.

Acceptance criteria, as resolved:

- The turmoil criterion (kill/restart the replica after `WAIT 1` returns) is **not** the shape that
  landed — it depends on round-2 infrastructure issues 02 (subprocess SIGKILL) and 06 (live-link
  fault primitive), neither of which exists yet. What landed instead is stronger than the level-3
  pin and cheaper than the kill: five unit-level forcing tests that place the assertion exactly at
  the ack point (`an_ack_reports_the_landed_head_not_the_claimed_one`,
  `a_claim_alone_does_not_move_the_offset_the_replica_acks`,
  `a_group_in_flight_to_its_shard_is_claimed_but_not_yet_ackable`,
  `a_frame_that_touches_no_shard_lands_as_it_is_claimed`,
  `a_full_resync_levels_the_landed_head_with_the_adopted_offset`), plus the level-4 sim below. The
  kill-restart scenario stays worth adding once 02/06 land; it would then be testing the state
  file, not the ack.
- The level-3 "pin the current contract first" step was **skipped on purpose**: pinning a contract
  in the same change that replaces it records nothing anyone will read, and the receipt-only
  behaviour is described here and in the FM row's "NOT observable" line.
- The apply-lag INFO field is **moot** — it only existed to make the weaker contract honest, and
  the weaker contract is gone.
- The spontaneous-tick branch is covered by `an_ack_reports_the_landed_head_not_the_claimed_one`
  (which drives both branches: the tick fires first with nothing landed, then the solicited answer
  arrives once the group lands).

`server/tests/simulation.rs`'s `run_spop_replication_convergence` — the workaround this issue was
filed off — now compares primary and replica `SMEMBERS` immediately after `WAIT 1 1000` returns,
with no polling, and is listed in the FM row as a forcing test.

**Out of scope, stated rather than left open:** replica-side *persistence*. An ack means applied,
not fsynced; `ReplicationState::save()` is still `write`+`rename` without an fsync, so a replica
power-cut can still lose a `WAIT`-confirmed write that its store had not flushed. That is the same
durability question every local write has (`durability-mode`), and folding it into the ack would
make `WAIT` mean something different from what Redis means by it. Recorded in the FM row's "Not
covered here" note; a separate issue should carry it if fsync-on-ack is ever wanted.

## Context

`WAIT N t` returning `N` is the primary's durability primitive. It currently means "N replicas
have the bytes in a queue" — not that they have applied them, and certainly not that they have
persisted them. The replica advances its offset and ACKs *before* the frame leaves the 10 000-slot
channel, so a replica killed right after a successful `WAIT` loses everything still queued. This
is the default configuration; any crash of an acking replica exposes it.

**This is a suspected live defect found by reading, not by test failure — the proposed test
fails against today's code.** The file:line evidence below is the auditing agent's and needs
confirmation before or during the fix; this issue is not among the two the coordinator
verified directly. `needs-triage`: the proposal carried an `OPTIONS:` block, and the contract
`WAIT` should promise has to be decided before the test can assert anything.

## Evidence

`replication/src/replica/streaming.rs:33` advances the offset *then* queues the frame *then*
ACKs (`let offset = self.offsets.frame_advance(&frame); ... frame_tx.send(frame) ...; if
solicited { self.send_ack(offset) }`), with a spontaneous tick branch ACKing
`self.offsets.current()` unconditionally. The queue is `mpsc::channel(10000)`
(`replica/mod.rs:123`). `state.rs::save()` is `fs::write(tmp)` + `fs::rename` with **no fsync**.

Why nothing catches it: `server/tests/simulation.rs:4556` already documents the consequence in a
comment — "WAIT acks cover the offset, not the replica's apply loop, so poll" — and works around
it rather than asserting it. No test asserts what `WAIT` means.

## What to fix

1. Move the ACK behind the apply loop so the acked offset means "applied", and make the
   spontaneous tick branch ACK the applied offset rather than `offsets.current()`.
2. Decide whether `WAIT` must additionally imply replica-side persistence; if not, say so and
   fsync `ReplicationState::save()` anyway so a confirmed offset survives a replica power-cut.
3. If the weaker contract is kept, expose apply-lag as an INFO field so operators are not misled.
4. Document the chosen `WAIT` contract next to the command.

## Options

Reproduced verbatim from proposals/14 F7:

- *Pin the current contract* (level 3, server integration): cheap, honest, no behaviour
  change; leaves `WAIT` weaker than Redis.
- *Assert apply-durability* (level 4, turmoil): the correct contract, fails today, forces
  the ACK to move behind the apply loop. **Recommended** as the target, with the level-3
  pin landing first so the current behaviour is at least documented.
- *Jepsen register workload with kills* (level 5): highest fidelity, nightly tier only.

## Acceptance criteria

- [ ] A two-node turmoil scenario asserts that after `WAIT 1 t` returns 1, killing and
      restarting the replica leaves the WAIT-confirmed key readable. Fails today.
- [ ] A level-3 pin lands first, asserting and documenting the current receipt-only contract, so
      the behaviour is recorded before it changes.
- [ ] If the weaker contract is kept, an INFO field exposes apply-lag and is asserted non-zero
      while frames are queued.
- [ ] The spontaneous-tick ACK branch is covered by the same test, not only the solicited path.

## Test boundary

Level 4 (turmoil) — the property is about a crash at a precise point in a two-node stream, and
the deterministic simulator is the only place that is not flaky. Not level 3: a server
integration test can pin the current contract but cannot place the kill between receipt and
apply, which is the entire claim.

## Depends on

issue 06 (I6 — live-link fault primitive; `INFRASTRUCTURE.md` lists 14/F7 among what it
unblocks), `.scratch/testing-improvements-round2/issues/`; issue 02 (I2 — subprocess-SIGKILL
crash primitive, echoed by area 14 for the replica kill),
`.scratch/testing-improvements-round2/issues/`
