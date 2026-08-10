# 17 — the persisted save point survives a move to a shorter history

Status: needs-triage

## Parent

[PRD](../../PRD.md) §3 W1. Found by the invariant catalog's first pass
([issue 02](../), INV-OFFSET-2).

## What was found

`offset_at_save` is only ever raised, never lowered:

- `OffsetCoordinator::reconcile_for_persist` — `state.offset_at_save = state.offset_at_save.max(offset)`
- `ReplicaOffset::reconcile_for_persist` — same `max`
- `ReplicaOffset::reset_to` moves both live heads to the adopted full-sync offset and does
  **not** touch `offset_at_save`

So a node that ran to offset X, then took a `+FULLRESYNC` whose granted offset is below X
(repointing `REPLICAOF` at a fresh primary, or failing over onto a node with a shorter
history), holds data only up to the new head while its state file still claims X. Nothing
brings the two back together:

1. every later save re-persists `max(X, applied) == X`
2. a restart seeds the live head from the file — `ReplicationIdentity::recovering` does
   `live.fetch_max(state.offset_at_save, …)` — so the node comes back claiming X while
   holding data to the lower head, and `INFO master_repl_offset` reports X
3. a promotion from that state arms a backlog floor and a `+CONTINUE` window at a boundary
   above the data the node actually has

A live primary refuses a resume request above its own head, so the everyday reconnect
self-heals into a full resync. The reachable damage is the restart and promotion paths in
(2) and (3), where the fabricated head becomes this node's own history.

`primary::tests::a_promotion_persists_its_boundary_without_ever_rewinding_it` asserts the
current behaviour deliberately ("a lower boundary must never rewind the persisted offset"),
so this is a **ruling**, not an obvious bug: either the monotone save point is right and
the invariant over-claims, or Redis's semantics are right and a full resync must carry the
save point down with the heads.

## Precedent

Redis's `master_repl_offset` is monotone *within a replication id* only. A full resync
overwrites it wholesale (`readSyncBulkPayload` → `server.master_repl_offset = psync_offset`),
and the RDB aux field `repl-offset` is written from that value, so a Redis replica that
resyncs backwards persists the lower offset. Valkey inherits this.

## Ruling needed

- (a) `reset_to` (and the staged-checkpoint install) lowers `offset_at_save` with the heads,
  making INV-OFFSET-2 `Tier::Hard` — matches Redis, and the monotone guard keeps its meaning
  *within* a history
- (b) the save point stays monotone and INV-OFFSET-2 is retired or restated against something
  other than the live head

## Acceptance criteria

- [ ] Ruling recorded here with its reasoning
- [ ] Behaviour implemented, with a forcing test in `frogdb-replication` (locked crate)
- [ ] `save_point_follows_a_backwards_full_resync` un-ignored (or deleted with the ruling
      written into INV-OFFSET-2's comment if (b) wins)
- [ ] INV-OFFSET-2's tier updated: `Tier::Hard` under (a), removed/restated under (b)
- [ ] Restart-after-backwards-resync covered end to end in `integration_replication.rs`

## Witness

`frogdb-server/crates/replication/src/replica/offset.rs` —
`save_point_follows_a_backwards_full_resync`, `#[ignore]`d against this issue.
