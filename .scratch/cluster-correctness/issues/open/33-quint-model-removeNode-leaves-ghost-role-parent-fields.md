# 33: Quint model's `removeNode` leaves ghost `role`/`parent` fields the real code has none of

Status: ready-for-agent

## Origin

Found by the Task 3 quint-connect conformance harness
(`frogdb-server/crates/cluster/tests/quint_conformance.rs`) while replaying
`specs/quint/cluster_migration_failover.qnt`'s named runs against real `ClusterState`
(coordinator review `.superpowers/sdd/2026-08-13-phase2-cluster-quint-plan/task-3-review.md`,
finding F5). Not one of issues 15/17/19/20/26 — this is a spec-representation artifact, not a
code defect.

## What is wrong

The model's `nodes` variable is a *total* map over the fixed `NODES` set. Its `removeNode`
action (`specs/quint/cluster_migration_failover.qnt:583-608`) flips only `member: false` and
leaves the removed node's `role`/`parent` fields at their last live value — a tombstone record.

`ClusterState`'s real `RemoveNode` arm (`frogdb-server/crates/cluster/src/commands.rs:215-236`)
does `inner.nodes.remove(&node_id)` — true deletion. This matches `specs/cluster.md`'s own
TR-CLUSTER-003 postcondition ("Node membership loses `node_id`"), so the model's own target
spec confirms real deletion is intended; nothing suggests the tombstone fields encode a
deliberate requirement. There is no live data anywhere in `ClusterState` after removal to
recover a removed node's last `role`/`parent`/`node_epoch` from, so the harness's projection of
an absent node necessarily defaults them (`RoleTagQ::Replica`, `parent: None`) rather than
replaying the model's stale values — and the two disagree, cited by the harness as a
divergence rather than resolved silently.

Reproduced (both hit this and only this): `removeNodeAfterDemotionSucceedsTest` (`force:
false`) and `removeNodeForceEvictsLiveOwnerTest` (`force: true`) — independent of which model
branch is replayed, both hit the identical ghost-field mismatch on node 1 after removal.

## What to build

Fix the model, not the code: null `role`/`parent`/`node_epoch` in the same `removeNode`
transition that flips `member: false`, so a removed node's tombstone carries the same
"nothing here" values the harness's projection already produces for a genuinely absent
`ClusterState` node record. This keeps `nodes` a total map (no Quint modeling change needed
beyond the transition body) while making the tombstone's non-`member` fields agree with real
deletion's information loss.

Until the model fix lands, `frogdb-server/crates/cluster/tests/quint_conformance.rs` carries a
harness-side `State::from_spec` normalization (any node the spec reports `member: false` has
its `role`/`parent`/`node_epoch` blanked to the same defaults `from_driver` produces for an
absent node) so `remove_node_after_demotion_succeeds_test` and
`remove_node_force_evicts_live_owner_test` can run un-ignored with their real slot-pruning
(INV-REF-1) and reparenting assertions live in the meantime. That normalization is a stopgap,
not a substitute for the spec fix — it hides exactly this one known-artifact mismatch and
nothing else.

## Acceptance criteria

- [ ] `cluster_migration_failover.qnt`'s `removeNode` action nulls `role`/`parent`/
      `node_epoch` alongside `member: false`
- [ ] `just quint-check` green
- [ ] `frogdb-server/crates/cluster/tests/quint_conformance.rs`'s harness-side `from_spec`
      normalization for the `member: false` ghost fields is removed (the model no longer needs
      it) and `remove_node_after_demotion_succeeds_test` /
      `remove_node_force_evicts_live_owner_test` still pass with the normalization gone
- [ ] `just quint-conformance-quarantine` (or `cargo nextest run -p frogdb-cluster
      --run-ignored ignored-only -E 'binary(quint_conformance)'`) reflects the change

## Blocked by

None — can start immediately. Independent of issues 15/17/19/20/26 (no shared root cause).
