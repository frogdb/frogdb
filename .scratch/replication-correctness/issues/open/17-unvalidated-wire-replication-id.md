# 17 — a replica adopts a replication id straight off the wire, unvalidated

Status: needs-triage

## Parent

[PRD](../../PRD.md) §3 W1. Found by the invariant catalog's first pass
([issue 02](../), INV-REPLID-3).

## What was found

`is_valid_replication_id` (40 lowercase-hex chars) exists and is applied on the **disk** path
— `read_staged_replication_metadata` refuses staged metadata whose id is malformed — but none
of the three **wire** paths that write the node's own `replication_id` checks anything:

| path | `frogdb-server/crates/replication/src/replica/connection.rs` | writes |
| ---- | ---- | ---- |
| `+CONTINUE <id>` | `psync`, ~`:340` | `state.shift_replication_id(new_repl_id, resumed_at)` |
| live-dataset trailer | `receive_snapshot`, ~`:425` | `adopt_replication_history(metadata.replication_id)` |
| checkpoint trailer | `receive_checkpoint`, ~`:499` | `adopt_replication_history(outcome.replication_id)` |

Whatever bytes the peer sends become this node's identity. Consequences, in order of how
much they hurt:

1. the garbage id is persisted, and `ReplicationState::validate()` refuses it on the next
   boot — a node that synced happily comes back refusing to start
2. `+CONTINUE` also *shifts* the old id into the failover window, so one malformed line
   rewrites both halves of the history this node advertises
3. every PSYNC against the malformed id misses, so the node full-resyncs forever without
   the reason being visible anywhere except a mismatched id in `INFO`
4. an id chosen by the peer is a handle on which resume windows this node will accept —
   a peer that echoes a *valid-looking* id it never owned gets `+CONTINUE` served against
   a history it does not have (the same shape as
   [issue 06](../)'s frozen-frame concerns, one layer up)

This is a peer-controlled write to persistent state, so it is a hardening issue as much as a
correctness one: the primary is trusted for its data, but "trusted" should still mean
"well-formed".

## Precedent

Redis validates the length and copies exactly `CONFIG_RUN_ID_SIZE` bytes
(`slaveTryPartialResynchronization` checks `strlen(replid) == CONFIG_RUN_ID_SIZE` before
`memcpy`), so a short or long id there is a protocol error, not an adopted identity.

## What to build

- Reject a malformed id at each of the three seams, with the same `is_valid_replication_id`
  the disk path uses. A refusal is an `io::Error` that drops the link — the next reconnect
  asks again — never a silent adopt.
- The refusal must leave the old history alone (same contract as
  `a_full_sync_that_never_delivers_a_dataset_leaves_the_old_history_alone`).
- Decide whether the check belongs *inside* `ReplicationState::shift_replication_id` /
  `adopt_replication_history` (one chokepoint, no seam can forget it — the seam-lint shape)
  or at each caller. The chokepoint form is preferred; it makes INV-REPLID-3 unfalsifiable by
  construction from the wire side.

## Acceptance criteria

- [ ] All three wire paths refuse a malformed id, leaving `replication_id`, `secondary_id`
      and `secondary_offset` untouched
- [ ] `a_continue_carrying_a_malformed_id_is_refused` un-ignored and passing
- [ ] Table test over the malformed shapes: empty, short, long, non-hex, uppercase
- [ ] The refusal is observable — a warn-level log naming the peer and the rejected id
- [ ] `integration_replication.rs` case proving the node survives a malformed grant and
      resyncs cleanly afterwards

## Witness

`frogdb-server/crates/replication/src/replica/connection.rs` —
`a_continue_carrying_a_malformed_id_is_refused`, `#[ignore]`d against this issue.
