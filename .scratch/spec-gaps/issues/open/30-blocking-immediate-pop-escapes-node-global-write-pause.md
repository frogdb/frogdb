# A blocking command's immediate pop escapes node-global CLIENT PAUSE WRITE

Status: needs-triage

## Parent

[issue 17](done/17-blocking-commands-during-client-pause.md) — its ruled deviation
("`CLIENT PAUSE` gates *execution*, not *parking*", distsys-review MAJ-14) is implemented as an
unconditional bypass in `wait_if_paused`
(`frogdb-server/crates/server/src/connection/lifecycle.rs`): a blocking-capable command skips the
node-global pause gate entirely, before anything knows whether it will park or pop.

## What is wrong

The bypass fires before `execute()` decides between parking and an immediate pop. When the data is
**already present** at dispatch time, the command never parks — it performs its pop synchronously.
A pop is a write. So during a node-global `CLIENT PAUSE WRITE` window, `BLPOP key 0` against a
non-empty list mutates the keyspace immediately, inside the exact drain window the pause exists to
protect (quiesce writes → let replicas catch up → fail over). Real Redis prevents this: its
`BLOCKED_POSTPONE` mechanism holds the whole command, so the data-present case cannot pop during a
pause.

The implementation's own doc comment defends only half the window: "The write that would satisfy
such a waiter (e.g. LPUSH) still goes through this same gate on its own connection" — true for
writes *arriving during* the pause, silent about data present *before* it. And the slot-scoped
analysis two paragraphs down names this exact hazard ("a blocking command that finds data
immediately available performs its pop synchronously, and letting that write cross an armed handoff
barrier would reopen the acknowledged-write hazard") — then exempts only the slot dimension,
leaving the node-global dimension open.

The MAJ-14 ruling itself draws the line correctly: pause gates execution. An immediate pop *is*
execution. The bypass over-reaches the ruling; parking (with a live deadline) is the only part that
should escape the gate.

## What to build

1. Keep the ruled deviation for the parking case: a blocking command that finds no data still
   bypasses the gate, registers a genuine waiter, and its deadline runs through the pause.
2. Close the data-present case: under an active node-global pause that covers the command
   (`PauseMode::Write` for the pop, or `PauseMode::All`), a blocking command whose data is already
   available must not pop. Park it instead (waiter with its normal deadline, satisfied when the
   pause lifts and the data is still there) — this keeps the deviation's own live-deadline
   semantics rather than reintroducing `BLOCKED_POSTPONE`.
3. The pause check belongs at the pop decision point (shard-side, where "data available" is
   actually known), not in `wait_if_paused` — connection-side cannot know availability without a
   race.
4. Spec-first: extend the issue-17 rows (TR-BLOCKING-025 area) with the data-present case as its
   own row, forcing test: arm `CLIENT PAUSE <t> WRITE`, `LPUSH k v`, then `BLPOP k 0` from another
   connection — assert the pop is not observable until the pause lifts, and that the waiter's
   deadline still ran during the pause.

## Acceptance criteria

- [ ] `BLPOP` with data present during node-global `CLIENT PAUSE WRITE` does not pop until the
      pause lifts (forcing test as above)
- [ ] `BLPOP` with no data during the same pause still parks with a live deadline (existing
      issue-17 behavior preserved, its tests stay green)
- [ ] Slot-scoped barrier behavior unchanged (`slot_pause_blocks_command` tests stay green)
- [ ] Row added for the data-present case; `just lint-spec` green

## Blocked by

None - can start immediately.
