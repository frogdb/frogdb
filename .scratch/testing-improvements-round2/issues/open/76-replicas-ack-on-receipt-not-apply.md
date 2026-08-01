# Replicas ACK on frame receipt, not on apply, so `WAIT` overstates durability by up to 10 000 commands

Status: needs-triage
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/14 F7 · MASTER.md §3 (durability)
Score: severity 4 · likelihood 4 · effort 3 · priority 17
Area: frogdb-replication / replica streaming, `WAIT`

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
