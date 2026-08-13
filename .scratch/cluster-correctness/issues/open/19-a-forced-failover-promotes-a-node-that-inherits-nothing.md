# 19 — A forced failover naming a departed primary promotes its successor out of a live replication link

Status: ready-for-agent

## Parent

[PRD](../../PRD.md) §3 W3 — found by the stateright failover model (issue 11). Distinct from issue
15 (a *graceful* failover leaving migrations sourced at the old primary): this is the `force` path,
there are no migrations involved, and the damage is a promotion that moves nothing rather than a
dangling migration.

## What is wrong

`ClusterCommand::Failover` validates the successor but deliberately does **not** validate the old
primary when `force` is set (`commands.rs`):

```rust
if !old_exists && !force {
    return Err(ClusterError::NodeNotFound(old_primary_id));
}
```

That waiver is the point of `force` — the node being failed over is usually gone. But it is
unconditional, so the command cannot tell *"the old primary is absent because it died"* from
*"the old primary is absent because someone already failed it over"*. In the second case the
command still runs its full mutation:

1. transfer every slot owned by `old` — there are none, `old` is not a member;
2. promote `new`: `role = Primary`, `primary_id = None`, and since it *was* a replica, emit
   `NodePromoted`;
3. `force` → remove `old` (already absent) and prune migrations naming it (none);
4. reparent `old`'s children (none);
5. `config_epoch += 1`, and stamp it on `new`.

So a node that inherits nothing is promoted anyway. Three things are wrong with the result:

* the successor **detaches from the primary that is actually feeding it** — it was reparented onto
  the new primary by the earlier failover, and this command silently severs that link;
* the live slot owner **loses its only replica**, so the shard it is serving is now unreplicated
  and the next real failure of that node has nowhere to go — the exact scenario the failover
  machinery exists for;
* the cluster's **highest config epoch lands on a node that owns no slots**, which is the
  authority ordering the whole topology is arbitrated by.

Two callers can produce it, and neither is doing anything wrong:

* `trigger_auto_failover` scores a candidate off a snapshot read *before* another would-be leader's
  failover committed, then proposes it (`force: true`) after;
* `CLUSTER FAILOVER FORCE` on a replica reads that replica's local snapshot for the primary to take
  over from (`commands/cluster/admin.rs`), and a replica that has not yet applied its own
  reparenting names the departed primary.

## Evidence

Model: `frogdb-server/crates/cluster/src/model/failover/mod.rs`. Reachable inside the checked
configurations, so it is pinned there as the characterization property `a_promotion_moves_nothing`
— a `sometimes` property that goes unwitnessed, and therefore red, the day the exposure is closed.

`model::failover::tests::a_promotion_can_move_nothing` finds it in 1368 states.

Replayed against the real state machine with no model in the loop by
`model::failover::replay::a_forced_failover_promotes_a_node_that_inherits_nothing`: node 2 fails
node 1 over onto itself (node 3 is reparented onto node 2 as part of it); node 3, which has not
applied that entry, is issued `CLUSTER FAILOVER FORCE` and names node 1; the command is accepted
and node 3 is promoted, leaving node 2 serving slot 0 with no replica and node 3 holding the
highest epoch with no slots.

The test asserts the exposure is **still present** — a characterization test, so a fix turns it red
and it gets flipped rather than silently passing.

## What to build

Spec-first. Add an FM row for "a forced failover that inherits nothing is refused", add the forcing
test, then fix.

Candidate rulings, to be decided and recorded in the row:

1. **Refuse a failover that would move nothing.** If `old` is not a member *and* owns no slots
   (trivially true when absent) *and* `new` is not currently a replica of `old`, the command is a
   no-op proposal built on a stale view: return `InvalidOperation`. Precise — it rejects exactly
   the stale case and leaves the legitimate "old primary died and is already gone from the roster"
   case alone, because there `new` is still `old`'s child. Callers already handle a
   `ClusterResponse::Error` from a failover proposal.
2. **Fence the proposal with the epoch it was scored at.** Carry the proposer's `config_epoch` in
   the command and refuse if the cluster has moved on. General, and it would close a family of
   stale-proposal races rather than this one — but it changes the command's wire shape, and every
   caller has to learn to re-score on refusal.
3. **Make the promotion conditional rather than the command.** Accept the command but skip the
   promote step when nothing was inherited. Smallest diff, worst semantics: the caller is told its
   failover succeeded when nothing happened.

Option 1 is the smallest change that is still honest to the caller. Option 2 is worth recording as
the direction if a second stale-proposal defect shows up.

## Acceptance criteria

- [ ] FM row added for the inherit-nothing failover; `just lint-spec` green
- [ ] Forcing test in `frogdb-cluster` fails first
- [ ] `model::failover::tests::a_promotion_can_move_nothing` and
      `model::failover::replay::a_forced_failover_promotes_a_node_that_inherits_nothing` flipped to
      assert the refusal, and `a_promotion_moves_nothing` flipped from `sometimes` to
      `always(!unjustified_promotion)`
- [ ] `just mutants-diff` triaged on every touched locked crate

## Blocked by

None.

## Ruling (2026-08-13)

**Option: epoch-fenced proposals, GLOBAL epoch (user chose global over per-node: simpler, more spurious refusals, safe under retry).** `ClusterCommand::Failover` carries the cluster-wide config epoch observed at scoring time; apply refuses the command if the current epoch has advanced. Belt retained: also refuse when the command would move nothing (old primary not a member, owns no slots, or new primary not the old's replica). Retry is provided structurally by issue 18's level-triggered reconcile — a refused proposal is re-scored on the next pass. Wire-shape change accepted (pre-production). Flip the characterization test `a_promotion_can_move_nothing`.
