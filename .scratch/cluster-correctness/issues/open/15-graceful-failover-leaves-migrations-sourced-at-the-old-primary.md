# 15 — Graceful failover leaves migrations sourced at the demoted primary (INV-MIG-1 drift)

Status: needs-triage

## Parent

[PRD](../../PRD.md) §3 W1 — found by the invariant catalog (issue 02) while probing states no
existing test reaches. INV-MIG-1 is HARD; nothing in the suite forces this path today, so the
defect is latent rather than red.

## What is wrong

`ClusterCommand::Failover` transfers every slot the old primary owned to the successor
(mutation step 1) but only the `force: true` branch prunes migrations naming the old primary
(`prune_migrations_naming`). After a **graceful** failover, a slot that was mid-migration is
owned by the successor while its `SlotMigration.source_node` still names the demoted node:

```
INV-MIG-1: slot 5 is migrating from 1 but is owned by 2
```

Reproduced against this tree (`frogdb-cluster`, `commands.rs` test module):

```rust
let state = state_with_primaries(3);
assign(&state, 1, 0, 10);
state.apply_local(ClusterCommand::BeginSlotMigration { slot: 5, source_node: 1, target_node: 3 }).unwrap();
state.apply_local(ClusterCommand::Failover { old_primary_id: 1, new_primary_id: 2, force: false }).unwrap();
// panics in the post-apply invariant hook
```

Consequences beyond the shape: `CompleteSlotMigration` validates `source_node` against the
recorded migration, so the surviving migration can only be finished by a node that is now a
replica; the redirect layer reads `migrations` to decide ASK/MOVED for the slot; and a
prepared handoff's barrier is never released because nothing emits `SlotHandoffReleased` for
it. The forced path already treats all three as the reason it prunes.

## What to build

Spec-first: amend the failover FM row(s) in
`.scratch/hardening/specs/cluster-failure-modes.md`, add the failing forcing test, then fix.

Decide between the two candidate rulings and record it in the row:

1. **Prune** — graceful failover cancels migrations naming the old primary on either leg,
   exactly as `force: true` does, emitting the releases `prune_migrations_naming` returns.
   Simple, symmetric, and matches Redis, where the migrating/importing marks are node-local
   and vanish with the role change.
2. **Retarget** — rewrite `source_node` from the old primary to the successor, since the
   successor now owns the slot and holds the same data. Preserves in-flight work but has to
   decide what a prepared handoff's drain state means when the source changed under it.

Option 1 is the smaller claim and the one the force path already makes; option 2 needs an
argument about the handoff barrier before it can be taken.

## Acceptance criteria

- [ ] Graceful `Failover` leaves no migration whose source or target is the demoted node in a
      state INV-MIG-1 rejects; forcing test in `frogdb-cluster` (fails first)
- [ ] Any pruned prepared handoff emits its `SlotHandoffReleased`
- [ ] FM row amended; `just lint-failure-modes` green
- [ ] `just mutants-diff frogdb-cluster` triaged

## Blocked by

None.
