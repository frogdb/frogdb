# 20 — `REPLCONF ACK` above the live head is admitted unclamped

Status: ready-for-agent

## Parent

Found by [issue 04](04-proptest-generator-and-r1.md)'s stateful proptest generator
(`frogdb-server/crates/replication/src/properties.rs`), filed under the campaign's rule that a
generator counterexample is pinned and filed rather than fixed inline.
[PRD](../../PRD.md) §3 W2.

## What the property found

Property R1 (`r1_every_action_leaves_the_catalog_clean`) reaches a node whose registry credits a
streaming replica with an offset the primary never wrote. The forcing sequence is
`pinned_issue_21_an_ack_above_the_live_head_is_admitted` in
`frogdb-server/crates/replication/src/properties.rs`: attach a replica, take it to `Streaming`
through the real PSYNC ladder, write once, then deliver a `REPLCONF ACK` naming `live + 65`.

The panic comes from the *ingesting seam's own hook* — `OffsetCoordinator::ingest_replica_ack`
(`frogdb-server/crates/replication/src/offset_coordinator.rs:260`) calls
`debug_assert_view_clean` on a view that already carries the inflated ack, so the HARD entry
`INV-OFFSET-3` ("replica N acked M past live L") fires on the very call that admitted it.

## Where the ceiling is missing

1. `ReplicaSession::record_ack` (`replica_session.rs:542`) stores any `sequence` strictly greater
   than the previous one. It is monotonicity-checked and nothing else — there is no comparison
   against the primary's live offset, which the session does not hold.
2. `OffsetCoordinator::ingest_replica_ack` (`offset_coordinator.rs:260`) *does* hold the live
   offset (`self.live`) and delegates to the tracker without consulting it.
3. The wire caller is the `REPLCONF ACK` arm of the replica session's reader
   (`replica_session.rs:1270`), which parses the peer's integer and passes it straight through.

So the offset is peer-controlled and never bounded. A replica that is buggy, a replica that is
mid-rollback after a partial resync it did not complete, or a peer that is simply hostile can name
any `u64`.

## Consequences in release builds

The `debug_assert_view_clean` hook is `#[cfg(any(test, debug_assertions))]`, so a release node does
not panic — it silently believes the number:

- `WAIT` counts the replica as having durably received writes it has not
  (`WaitCoordinator` reads the tracker's acked offsets), so `WAIT n 0` can return early. This is a
  durability claim the node cannot back.
- `INFO replication` reports a negative-lag replica (`offset` above `master_repl_offset`).
- `min_acked_offset` is inflated, which moves the split-brain divergence window's lower bound
  (`PrimaryReplicationHandler::divergence_record`) past writes that really did diverge — those
  writes are then omitted from the divergence log.

## Why this is filed rather than fixed

The fix looks like one `min(acked, live)`, but *where* it goes is a ruling:

1. **Clamp in `ingest_replica_ack`.** The coordinator is the only object holding both numbers.
   Cheap, and the session stays a dumb recorder. But a clamp silently rewrites what the peer said,
   which loses the signal that a peer is misbehaving.
2. **Reject and disconnect.** An ack above the live head is a protocol violation; treat it like any
   other malformed frame and drop the link (`ReplicaDeparture::Lost`). Matches how the campaign has
   handled other unvalidated wire input, and it surfaces the peer bug. Costs a link on a benign
   race — though there is no benign race here: the primary writes the offsets, so it cannot be
   behind its own stream.
3. **Clamp *and* count.** Clamp for correctness, and move a counter / log line so the misbehaving
   peer is observable without a disconnect.

Redis's `replconfCommand` takes the offset verbatim into `replica->repl_ack_off` with no ceiling,
so parity is not an argument either way — this is a place a deviation would be an improvement, per
the project's stated goal.

Whichever option is ruled, the fix needs a failure-mode row (a new `FM-REPLICATION-NNN` in
`specs/replication.md`) naming the forcing test, per the locked
crate's spec-first rule.

## Second, independent witness: the seeded turmoil sweep

[Issue 12](../done/12-replication-seeded-sweep.md)'s replication fault-scheduler
reaches this from the other end — three real servers over a simulated network,
no synthetic ack — at seeds 225 and 340 of the 500-seed nightly budget, where it
aborts the run as
`INV-OFFSET-3: replica N acked M past live L`. So the shape is not an artefact of
the generator handing a session a number no peer would send: ordinary faults
produce a replica whose position is above the primary's head.

The sweep reaches it two ways, and the ruling should say which it fixes:
crash-restart lineage reuse
([issue 24](24-a-restart-keeps-the-replication-id-it-lost-the-history-for.md) —
the rebooted primary is at offset 0 under the id its replicas still follow), and
a promotion that settles the new primary at its *applied* offset
(`settle_at_applied` discards received-but-unapplied frames and stores the lower
value into `live`) while a replica acks the higher number it already received.
Fixing 24 removes only the first. That second path is why option 2 — reject and
disconnect — has a cost the proptest cannot see: it would drop a replica that is
behaving correctly against a head that legitimately moved down under it.

Muzzled in the sweep by `known_panic_gap`
(`frogdb-server/crates/server/tests/simulation/replication_scheduler.rs`), which
matches this signature and only this one, pinned by
`test_known_panic_gap_matches_only_the_filed_signature`. Seeds that reach it are
counted and named on stderr rather than skipped, so the count is visible.
Deleting `known_panic_gap` is part of this issue's fix. Replay with:

```
REPLICATION_SEED_TRACE=1 REPLICATION_SEEDS_START=225 just replication-seeds 1
```

## Pin

`frogdb-server/crates/replication/src/properties.rs`:
`pinned_issue_21_an_ack_above_the_live_head_is_admitted`
(`#[should_panic(expected = "INV-OFFSET-3")]`).

The generator is muzzled for exactly the resolved shape "the ack would exceed the live head on a
streaming session" — `known_defect`'s first arm — and
`the_muzzle_only_covers_the_pinned_shapes` asserts the near misses (at-head, behind-head, zero, and
an overshoot against an empty slot) stay in the property. Fixing this issue turns the witness red.

## Ruling (2026-08-13)

**Option: clamp + count.** `ingest_replica_ack` clamps a `REPLCONF ACK` beyond the primary's live head down to the live head, incrementing a counter and logging for observability. NO disconnect — the promotion-settle path produces over-high acks innocently, and disconnecting would flap healthy replicas. Deliberate deviation from Redis (which ingests acks verbatim with no ceiling) as an improvement. New FM-REPLICATION row for the clamp. This ruling covers BOTH producing paths: restart-reuse (structurally closed by issue 24's ruling) and promotion-settle. Delete the sweep's `known_panic_gap` muzzle when fixed.

## Amendment (2026-08-13)

**Clamp reversed → ignore + count.** The anti-pattern review found clamping writes `min(wire, live)` into `acked_offset`, making the primary a writer of its own ack state — contradicting FM-REPLICATION-039's "wire ACK is the only writer" (the row issue 28 forced) and granting WAIT credit for bytes the replica never confirmed on this head's lineage. Amended behavior: an ack above the live head is **ignored entirely** (no `acked_offset` write) and counted via metric; the replica's next valid ack ≤ head updates normally. Raft shape: a leader never advances a follower's match index past its own log. The no-disconnect disposition stands. The FM row and forcing test target ignore-semantics, not clamp.
