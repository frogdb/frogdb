# 16 — `AssignSlots` ignores open migrations and hands a migrating slot to a third node (INV-MIG-1)

Status: needs-triage

## Parent

[PRD](../../PRD.md) §3 W2 — found by the P1 property (issue 03) on its first run, at 4 commands
after shrinking. Distinct from [issue 14](14-role-transitions-admit-malformed-parents.md) and
[issue 15](15-graceful-failover-leaves-migrations-sourced-at-the-old-primary.md): neither a role
transition nor a failover is involved.

## What is wrong

`ClusterCommand::AssignSlots` validates node membership and current slot ownership
(FM-CLUSTER-003, `commands.rs` `AssignSlots` arm) but never consults `inner.migrations`.
`BeginSlotMigration` deliberately accepts a slot with **no** recorded owner — the slot map can be
empty on a follower that was seeded locally, and the catalog documents that acceptance. The two
allowances compose into a hole: a slot that is *migrating but unassigned* can then be assigned to
any member, including a node that is neither the migration's source nor its target.

```
INV-MIG-1: slot 0 is migrating from 1 but is owned by 5
```

Shrunk P1 counterexample, verbatim (`p1_every_apply_leaves_the_catalog_clean`, 4 commands):

```rust
let state = ClusterState::new();
state.apply_local(ClusterCommand::AddNode { node: NodeInfo::new_primary(5, ..) }).unwrap();
state.apply_local(ClusterCommand::AddNode { node: NodeInfo::new_primary(1, ..) }).unwrap();
// legal: slot 0 has no recorded owner, so the follower-seed allowance applies
state.apply_local(ClusterCommand::BeginSlotMigration { slot: 0, source_node: 1, target_node: 1 }).unwrap();
// accepted: slot 0 is unassigned, so neither the membership nor the ownership check fires
state.apply_local(ClusterCommand::AssignSlots { node_id: 5, slots: vec![SlotRange::new(0, 0)] }).unwrap();
// panics in the post-apply invariant hook (commands.rs:108)
```

`RemoveSlots` reaches the same state on a longer path: it unassigns a slot the source owns
without touching the migration, after which the `AssignSlots` above is unconditionally
available. Both reduce to the same missing check.

Consequences beyond the shape: the redirect layer reads `migrations` to decide ASK/MOVED, so
the slot answers `MOVED`/`ASK` naming a node that does not hold the migration's data;
`CompleteSlotMigration` validates `source_node` against the recorded migration and would hand
the slot to the target, silently overwriting the third node's claim; and a prepared handoff's
barrier is armed at a node that no longer owns the slot.

## What to build

Spec-first: amend FM-CLUSTER-003 (and FM-CLUSTER-004 if the ruling covers the removal leg) in
`.scratch/hardening/specs/cluster-failure-modes.md`, add the failing forcing test, then fix.

Candidate rulings:

1. **Refuse** — `AssignSlots` rejects any slot with an open migration whose source is not
   `node_id`, as part of the same validate-all-then-apply pass, with a new error. Symmetric with
   `BeginSlotMigration`'s ownership check read in the other direction, and keeps the invariant
   structural rather than repaired after the fact.
2. **Refuse only the third-party case** — allow assignment to the migration's *source*
   (which is what a follower catching up on its own seed looks like) and refuse everything
   else. Narrower, and preserves the reason the unassigned-migrating state is legal at all.

Option 2 is the smaller claim; option 1 needs an argument for why re-asserting the source's own
ownership should fail.

## Acceptance criteria

- [ ] No command sequence assigns a migrating slot to a node other than the migration's source;
      forcing test in `frogdb-cluster` (fails first), reproducing the 4-command shape above
- [ ] The `RemoveSlots`-then-`AssignSlots` path is covered by the same ruling
- [ ] FM row amended; `just lint-spec` green
- [ ] The `known_defect` muzzle entry citing this issue is deleted from
      `frogdb-server/crates/cluster/src/properties.rs`, and
      `pinned_issue_16_assign_slots_ignores_an_open_migration` with it
- [ ] `just mutants-diff frogdb-cluster` triaged

## Blocked by

None.
