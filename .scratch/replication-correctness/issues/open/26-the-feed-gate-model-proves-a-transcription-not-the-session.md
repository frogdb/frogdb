# 26 — the feed-gate model proves a transcription, not the session

Status: ready-for-agent

## Parent

Found by [issue 15](15-retro-validation-gate.md)'s retro-validation gate, revert (d), second
placement. [PRD](../PRD.md) §3 W3.

## What the gate found

Revert (d) — "the replica feed ran while a slot-handoff barrier was armed", fixed by `8d55cc4f` —
has two honest placements, and the layers see only one of them.

**(d-i), the derivation.** `decide_feed_hold_until` returns `None` instead of the latest armed
deadline (`frogdb-server/crates/replication/src/feed_gate.rs`). Caught immediately: the stateright
feed-gate model's smoke and all three of its replay cases go red, along with the decision seam's own
unit test. Five failures, nothing else in the tree moves.

**(d-ii), the consumer.** The derivation stays correct and the *session stops consulting it* — the
two places in `replica_session.rs` where the streaming path waits on the gate
(`handler.feed_gate.released().await` before the backlog tail, and the `while feed_gate.is_held()`
buffering loop) are bypassed. This is the same defect: frames ship inside an armed barrier window.
Every layer stayed green — `frogdb-replication` 529/529, `frogdb-replication-runtime` 45/45, the
seeded sweep 32/32, `frogdb-server` replication integration 236/236.

## Why the model cannot see it

`frogdb-server/crates/replication/src/model/feed_gate/mod.rs` model-checks a *transcription* of the
write task: the model's `Scope::honour_the_gate` reproduces the session's control flow inside the
model, and the replay tests check that transcription against the shipped decision functions. The
decision functions are the real ones — which is why (d-i) is caught — but the control flow around
them is a copy. A production session that stops calling `released()` leaves the copy untouched, so
the model keeps proving a property about code that is no longer what runs.

The other layers are blind for their own reasons. The proptests drive `LinkAction`s against the
registry and never run the streaming write task. The seeded sweep runs real servers but arms no
slot-handoff barrier — barriers are a cluster-topology fault, and the replication sweep's families
are link drops, promotions and full-sync interrupts. So the consumer side of the gate has exactly
one witness, `FM-CLUSTER-097`'s own integration test.

## What closing it takes

The gap is model fidelity, and there are two shapes of fix:

1. **Drive the real thing.** Have the model's step function call the production streaming loop's
   decision points rather than a transcription of them — the same move [issue
   07](../issues/done/07-decision-io-seam-extraction.md) made for the derivation. This is the
   general fix and the expensive one: it needs the session's hold/flush sequencing extracted as a
   pure step the model can drive, the way `decide_feed_hold_until` already is.
2. **Witness it end to end.** Add a barrier to the replication sweep's fault families (arm a
   handoff barrier on the primary, then assert no replica observes an offset past the barrier's
   floor until it releases), so the consumer is exercised by real servers even if the model keeps
   its transcription.

Option 1 is the one that generalises: every model in this campaign transcribes some control flow,
and (d-ii) is the first proof that the transcription can drift from the tree. Whichever is ruled,
the acceptance test is the revert: with the closing layer in place, bypassing the gate in
`replica_session.rs` must turn something red that is not `FM-CLUSTER-097`'s own forcing test.

## Note on scope

The *defect* (d) is caught — (d-i) is a faithful reintroduction and the model catches it in under a
second, so the PRD's exit criterion 8 holds for that row. What this issue records is narrower and
more durable: a layer that proves a property about a copy of the code cannot notice the copy going
stale.

## Ruling (2026-08-13)

**Option 2 now + option 1 via formal-spec phase 3.** The replication seeded sweep gains a slot-handoff-barrier fault family: arm the barrier on the primary, assert no replica observes offsets past the floor until release — closing finding d-ii and unblocking issue 15 / exit criterion 8. Acceptance: reverting the gate-bypass in `replica_session.rs` must turn something red that is NOT FM-CLUSTER-097's own forcing test. Drive-the-real-code fidelity (option 1) arrives structurally as phase 3's quint-connect feed-gate model — no stateright retrofit.

## Addendum (2026-08-13, anti-pattern review)

Review finding A5: option 2's single witness closes this instance but not the class — every
model in the campaign transcribes some control flow, and phase 3 (the structural fix) is an
external dependency that can slip. Add a cheap interim class-level guard alongside the sweep
fix: a seam lint (in the compile-free `just lint-gates` family) requiring the streaming path in
`replica_session.rs` to call `feed_gate.released()`/`feed_gate.is_held()` — generalizes to
every other transcribed control-flow model by adding one entry per model. Also broaden the
acceptance test: the issue names two consumption points (`released().await` before the backlog
tail, and the `while feed_gate.is_held()` buffering loop) — require that bypassing *each*
independently turns something red, not just the pair together.
