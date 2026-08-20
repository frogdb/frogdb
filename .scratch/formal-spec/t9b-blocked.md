# T9b — spec/seam divergence: a link that ends inside a barrier window drains past the floor

Status: **NEEDS A RULING.** Not blocking: the phase-3 feed-gate model
(`specs/quint/replication_feed_gate*.qnt`) is complete, green, and committed. It models the
seam's behaviour faithfully and *discloses* the divergence at the invariant rather than
picking a side silently. Nothing here should be "fixed" by weakening the invariant.

Filed 2026-08-20 by task T9b (phase-3 replica feed-gate / barrier session model).

## The two statements that cannot both be right

**The spec row.** `specs/cluster.md`, FM-CLUSTER-097 (node-wide feed hold), Observable:

> Nothing the node applies during the barrier window reaches the replica. [...] When the
> barrier ends — the handoff completes, aborts, or the pause simply lapses — the feed
> resumes and the replica converges on everything held, in offset order and with nothing
> dropped.

Unconditional. The window is defined by the barrier, not by the health of the link.

**The seam.** `frogdb-server/crates/replication/src/feed_sequencer.rs:221-230`:

```rust
// Both arms flush first: the link closing is not a reason to drop
// [the frames already buffered] on the floor.
(Stage::Receiving | Stage::Holding, FeedInput::SourceClosed) => {
    self.ending = Some(ReplicaDeparture::Graceful);
    self.flush()
}
(Stage::Receiving | Stage::Holding, FeedInput::SourceLagged) => {
    self.ending = Some(ReplicaDeparture::Lost);
    self.flush()
}
```

`flush()` pops the head of the held buffer and returns `Send`. It does **not** re-enter
`Stage::Consulting`, so the gate is never re-read. A session that is `Holding` because a
barrier is armed, and that then observes its source closing (or overrunning), ships its
entire held buffer onto the wire *while the barrier is still armed*. The module doc says so
in as many words (`feed_sequencer.rs:49`), and the seam's own tests assert it
(`feed_sequencer.rs:436`, `:452` — a close inside the window flushes before it ends the
link; a lagged receiver flushes then reports a lost link).

So: frames the node applied **during** the barrier window reach the replica, provided the
link happens to be on its way out. That is precisely what FM-CLUSTER-097 says cannot happen.

## Why it matters (i.e. why this is not a documentation nit)

The barrier exists so that a slot's contents stop moving to replicas while ownership is
handed off. A replica that receives the held tail anyway has applied writes the barrier was
meant to withhold. That the connection is closing does not undo the delivery: the replica
may have persisted the frames and may itself be a promotion candidate, and — on the
`SourceClosed` path — the departure is classified `Graceful`, which is the classification
that *disarms* the self-fence (FM-REPLICATION-062). The most permissive delivery and the
most permissive fence outcome are reached by the same arm.

## The two candidate rulings

1. **The row is too strong — amend FM-CLUSTER-097.** Add an explicit "unless the link is
   ending" clause: a session that has already classified its departure may drain what it
   already accepted, because dropping it is strictly worse for a replica that is about to
   reconnect and PSYNC from its own offset. If this is the ruling, FM-CLUSTER-097's
   Observable must say so, and the row should say what protects the handoff — presumably
   that a departing replica cannot be a promotion candidate for the slot under handoff.

2. **The seam is wrong — make the ending drain gate-aware.** The `SourceClosed` /
   `SourceLagged` arms set `ending` and then go to `Stage::Consulting` rather than calling
   `flush()` directly, so the drain waits out the barrier exactly like any other drain, and
   the departure is reported when the buffer finally empties. This costs a held connection
   for the remainder of the window and needs a story for "the peer already went away, so
   the drain will fail anyway" — but it is the reading the row currently states.

There is no third reading in which both the row and the code are correct as written.

## What the model does in the meantime

`specs/quint/replication_feed_gate.qnt`:

- `inv_no_ship_inside_barrier_window` is stated against each armed barrier's **floor** (the
  live offset at arm time), with an explicit carve-out for a session where `isEnding(ss)`
  holds. The carve-out is annotated at the invariant and in the model header, and points
  back at this file.
- `coverage.endingDrainedPastBarrierFloor` latches when the carve-out is actually taken, and
  `witnessEndingDrainedPastFloor` reports it, so the exemption cannot become vacuous
  without anyone noticing.
- `closeInsideWindowDrainsThenEndsGracefulTest` pins the exact sequence deterministically.

If ruling 2 lands, the fix in the model is to delete the `isEnding(ss)` disjunct from
`inv_no_ship_inside_barrier_window`, at which point that run test fails — which is the
intended alarm — and the seam change follows the usual spec-first order (row → failing test
→ fix). If ruling 1 lands, the carve-out stays and FM-CLUSTER-097's Observable gains the
matching clause.

## Secondary observation (no ruling needed, recorded for the record)

`FeedSequencer::step`'s transition table is total: every input/stage pair the explicit arms
do not cover falls through to `_ => self.end(ReplicaDeparture::Lost)`. That is a deliberate
fence in the safe direction for pairs the driver cannot produce, and the model does not
represent it — its guards simply disable those pairs. Worth knowing when a quint-connect
harness replays model traces through the real struct: the harness must not treat a
model-disabled transition as a seam transition to exercise, or it will see `End(Lost)` where
the model has no transition at all.
