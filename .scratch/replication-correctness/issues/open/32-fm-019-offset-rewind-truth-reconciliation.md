# 32: FM-REPLICATION-019 tells the truth about the promotion offset rewind

Status: ready-for-agent

## Origin

Distsys-review MAJ-3 (`.scratch/formal-spec/2026-08-13-independent-distsys-review.md`),
ruled accept-and-file by the user 2026-08-14
([rulings ledger](../../../formal-spec/2026-08-13-distsys-review-rulings.md)): the
rewind is the correct behavior — a node must never claim bytes it did not apply — and
FM-019's continuity cell is the lie.

## What is wrong

Two locked rows state opposite facts about the same observable. FM-REPLICATION-019's
Observable cell says `master_repl_offset` "is continuous across the identity change (it
does not reset, jump, or rewind)". The `LiveOffset` State-space row describes
`settle_at_applied_inner` as "(promotion boundary, `store(applied)` — a rewind)". The
code rewinds, and warns when it does (`offset_coordinator.rs:223-240`: if
`received > applied`, log "Promotion discarding replication frames received but never
applied" and `self.live.store(applied)`); `master_repl_offset` renders from
`tracker.current_offset()`, which is `LiveOffset`.

The rewind fires in the *normal* promotion case (any frames received but not yet
applied). Monitoring or a failover runbook built on FM-019's monotonicity guarantee
misreads a healthy promotion as corruption; a tool comparing offsets to judge catch-up
concludes wrongly. And a downstream that had received the un-applied bytes now holds a
*higher* offset than its new primary — the classic replica-ahead-of-master condition
(Redis PSYNC2 keeps the offset monotone precisely so downstreams can compute a
partial-resync point; FrogDB's rewind must instead handle the ahead case explicitly).
No FM row states the true behavior, so nothing forces it either way.

## What to build (spec-first; FM-019 is in a locked area — failure-mode row → forcing test → change)

1. Amend FM-REPLICATION-019's Observable cell: `master_repl_offset` "may rewind at the
   promotion boundary by the un-applied byte count; it never advances past the applied
   offset". Make the `LiveOffset` State-space row and FM-019 cite each other.
2. New FM row: downstream ahead of new primary — a downstream whose received offset
   exceeds the new primary's applied offset attempts partial resync → the primary
   refuses the PSYNC continuation and forces a full resync; NOT observable: the
   downstream silently keeping bytes the primary never applied (divergence).
3. Forcing tests:
   - Promotion with `received > applied` asserts `master_repl_offset` lands at
     `applied` (not `received`) and the warn fires — pins the rewind as intended
     behavior.
   - Downstream-ahead reconnect asserts full resync is forced and post-resync offsets
     agree.
4. Cross-check the offset-family issues so the amended cells stay consistent:
   [17](17-save-point-above-the-live-head.md),
   [21](21-ack-above-live-head.md),
   [24](24-a-restart-keeps-the-replication-id-it-lost-the-history-for.md) (atomic
   replid/offset pairing — the rewound offset must pair with the post-promotion
   replid shift).

## Acceptance criteria

- [ ] FM-REPLICATION-019 amended; State-space row and FM row cross-cite;
      `just lint-spec` green
- [ ] New downstream-ahead FM row with forcing test (full resync forced)
- [ ] Rewind-pinning forcing test names FM-019 in its tag
- [ ] `just mutants-diff` on frogdb-replication (locked, gate 0.85) triaged

## Blocked by

None — can start immediately. Coordinate wording with issue 24 if in flight
simultaneously (both touch the promotion-boundary offset cells).
