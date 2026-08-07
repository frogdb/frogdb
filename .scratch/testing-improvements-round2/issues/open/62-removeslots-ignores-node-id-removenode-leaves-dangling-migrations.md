# `RemoveSlots` ignores its `node_id`, and `RemoveNode` leaves dangling migrations that assign slots to a ghost node

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/11 F5 · proposals/11 F9 · MASTER.md §3 (consistency violations)
Score: severity 4 · likelihood 3 · effort 1 · priority 17
Area: frogdb-cluster / `apply_command` state machine

## Context

Two validation holes in the same pure state machine. `RemoveSlots` never checks the `node_id`
it is handed, so `CLUSTER DELSLOTS` issued against node A silently deletes slots owned by node
B. `RemoveNode` retains the removed node's migration records, and `CompleteSlotMigration` then
assigns the slot to a node that no longer exists — the slot's owner is absent from `nodes`, so
it vanishes from `CLUSTER SLOTS`/`SHARDS`, the health counter buckets it as **ok**, and
`CLUSTER ADDSLOTS` recovery fails with `SlotAlreadyAssigned(slot, ghost)`. Unavailability for
that key range behind a green health signal, unrecoverable short of `CLUSTER RESET`.

**This is a suspected live defect found by reading, not by test failure — the proposed tests
fail against today's code.** The file:line evidence below is the auditing agent's and needs
confirmation before or during the fix; this issue is not among the two the coordinator
verified directly.

## Evidence

**11/F5 — `RemoveNode` leaves dangling migrations** (severity 4 · likelihood 3 · effort 1 · priority 17)

`crates/cluster/src/commands.rs:82-93` — `RemoveNode` retains slot assignments and removes the
node but never touches `inner.migrations`. `commands.rs:353-385` — `CompleteSlotMigration`
validates only that the migration record matches; it then `slot_assignment.insert(slot,
target_node)` with **no** node-exists check, unlike `AssignSlots` (`:95-98`) and
`BeginSlotMigration` (`:323-328`), which both validate. The `Failover { force: true }` arm
*does* prune migrations touching the removed node — the asymmetry is the tell.

Why the existing test passes anyway: `integration_cluster.rs:10450
test_remove_node_during_active_migration` `SETSLOT ... STABLE`s the slot itself and then asserts
only that *some* write on *some* surviving node succeeds; it cannot catch this. The same test
demonstrates the sequence is reachable in ordinary ops (`CLUSTER FORGET` during a migration).

**11/F9 — `RemoveSlots` ignores `node_id`** (severity 4 · likelihood 2 · effort 1 · priority 15)

`crates/cluster/src/commands.rs:119-136` — `node_id` appears only in the `tracing::debug!` at
`:134`; the validation loop checks solely that each slot is assigned *to somebody*, and the
mutation loop `remove`s unconditionally. Compare `AssignSlots` (`:95-108`), which validates node
existence *and* rejects a conflicting owner. No test asserts a wrong-owner rejection.

## What to fix

1. `RemoveNode` must either prune migrations that reference the removed node (matching the
   `Failover { force: true }` arm) or `CompleteSlotMigration` must reject a target node that is
   absent from `nodes` — preferably both.
2. `RemoveSlots` must validate that each slot is owned by the `node_id` it was given and reject
   otherwise. Redis's `DELSLOTS` is deliberately owner-agnostic on the *local* node but never
   removes another node's assignment from the shared map, so rejecting is the parity-correct
   behaviour.
3. Add the post-state invariant `slot_assignment.values() ⊆ nodes.keys()` and assert it after
   every `apply_command` in the test suite.

## Acceptance criteria

- [ ] A pure `apply_command` unit test does `BeginSlotMigration{slot, A→B}` → `RemoveNode{B}`
      and asserts either the migration is gone or `CompleteSlotMigration{slot, A, B}` returns
      `NodeNotFound(B)`. Fails today.
- [ ] A unit test assigns slots 0–10 to A, applies `RemoveSlots { node_id: B, slots: 0..=10 }`,
      and asserts `Err(...)` plus that A still owns 0–10. Fails today.
- [ ] The invariant `slot_assignment.values() ⊆ nodes.keys()` is asserted as a post-state check.
- [ ] A proptest generalisation over `apply_command` sequences holds the same invariant.

## Test boundary

Level 1 — `apply_command` is pure, synchronous and `BTreeMap`-only; testing this through a
cluster would be the geohash anti-pattern the brief calls out. Not level 2 or above: no
storage, network or async behaviour participates in either defect.

## Depends on

nothing — `INFRASTRUCTURE.md` records that area 11 needs no new infrastructure; proptest,
tempfile and `tokio::test` are already dev-dependencies

## Re-triage 2026-08-06

**Verdict: partially-fixed**

**11/F9 (`RemoveSlots` ignores `node_id`) is FIXED** — commit `723b8114`
("fix(cluster): owner-checked DELSLOTS…"), specced as **FM-CLUSTER-004** and forced by
`remove_slots_rejects_a_slot_owned_by_another_node` and
`remove_slots_rejects_the_whole_batch_when_one_slot_is_foreign`. The arm now validates
`Some(&owner) if owner != node_id => InvalidOperation("slot N is owned by X, not Y")` before it
mutates anything (`frogdb-server/crates/cluster/src/commands.rs:150-178`; the issue's `:119-136` is
stale). Hardening tracker: `.scratch/hardening/issues/done/33-remove-slots-is-owner-blind.md`.

**11/F5 is half-discharged.** The *dangling migration* itself is now a deliberate, specced
non-guarantee — **FM-CLUSTER-002**, pinned by `remove_node_leaves_migrations_and_replicas_dangling`
(`commands.rs:621-658`), with the documented operator recovery being `SETSLOT … STABLE`
(FM-CLUSTER-035). But the **ghost-owner consequence is still live**: `CompleteSlotMigration`
(`commands.rs:459-490`, moved from `:353-385`) still does an unguarded
`inner.slot_assignment.insert(slot, target_node)` with **no** `nodes.contains_key(target_node)`
check, and nothing upstream adds one — `commands/cluster/admin.rs:627-635` only checks that a
migration record exists and its `target_node` matches, and `slot_migration/mod.rs::complete` →
`connection/cluster.rs:137` is a thin pass-through. FM-CLUSTER-033 explicitly blesses the unguarded
insert ("the parameter match … is what makes the unguarded `slot_assignment.insert` safe"), which
does not hold once FM-CLUSTER-002 lets the target be forgotten first. So
`BeginSlotMigration{s, A→B}` → `CLUSTER FORGET B` → `CLUSTER SETSLOT s NODE B` still yields
`slot_assignment[s] = B ∉ nodes`, and FM-CLUSTER-073 confirms the downstream symptom is specced
as-is ("A slot whose owner is unknown counts as `ok` rather than being dropped").

**Remaining work:** acceptance criteria 1, 3 and 4. There is no `slot_assignment.values() ⊆
nodes.keys()` post-state assertion and no proptest anywhere in `crates/cluster` (the crate has no
`proptest` dev-dependency). Either add the node-exists check to `CompleteSlotMigration` or prune
migrations in `RemoveNode` (matching the `Failover { force: true }` arm), and add an
FM-CLUSTER row for whichever is chosen. Criterion 2 is done.
