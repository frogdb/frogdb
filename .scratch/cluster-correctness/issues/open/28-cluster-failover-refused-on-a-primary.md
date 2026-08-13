# 28 — `CLUSTER FAILOVER FORCE` sent to a primary absorbs an arbitrary peer primary and evicts it

Status: ready-for-agent

## Parent

[Adversarial design review — `specs/cluster.md` + the 2026-08-13 rulings](../../../formal-spec/reviews/2026-08-13-antipattern/spec-review-cluster.md),
finding **H6**. Spec-gap finding, distinct from the amended rulings on issues 14–20 and 25: no
prior campaign issue examined the `FORCE`/`TAKEOVER`-on-a-primary arm at all — the review found it
on a direct code spot-check of `commands/cluster/admin.rs`.

## What is wrong

`commands/cluster/admin.rs:284-308`, reached only via the `FORCE`/`TAKEOVER` arm on a primary:

```rust
if this_node.is_primary() {
    if !force && !takeover {
        return Err(...); // "CLUSTER FAILOVER can only be run on a replica"
    }
    // FORCE/TAKEOVER on a Primary: find a target primary to absorb.
    // Prefer a failed primary, otherwise pick any other primary.
    let target_id = snapshot.nodes.iter()
        .find(|&(&id, n)| id != node_id && n.is_primary() && n.flags.fail)
        .or_else(|| snapshot.nodes.iter().find(|&(&id, n)| id != node_id && n.is_primary()))
        .map(|(&id, _)| id)
        .ok_or_else(...)?;
    return Ok(Response::RaftNeeded {
        op: RaftClusterOp::Failover { replica_id: node_id, primary_id: target_id, force: true },
        ...
    });
}
```

The victim is selected by `HashMap` iteration order — preferring a `fail`-flagged primary and
otherwise **any other primary** — and the resulting proposal is `force: true`, i.e. the victim is
removed from the topology (FM-CLUSTER-002-shaped pruning) and from the Raft voter set
(FM-CLUSTER-101). So a single mistyped or misrouted `CLUSTER FAILOVER FORCE` sent to the wrong node
takes a *healthy* primary's slots, moves them without their data (issue 20's evidence: "ownership
moved without the data — a promotion carries only what PSYNC already shipped"), and shrinks
quorum — against a target that is not deterministic run to run. FM-CLUSTER-040 acknowledges "the
primary-absorbs-primary case" as an observable but never states how the victim is chosen; "any
other primary, HashMap order" is not a rule.

Redis refuses this outright: `CLUSTER FAILOVER` (all grades — plain, `FORCE`, `TAKEOVER`) is
replica-only, and a master answers an error. A Redis-compatible operator script that sends
`FAILOVER FORCE` to the wrong node gets an error from Redis and data destruction from FrogDB.

## What to build

Spec-first. Redis parity: `CLUSTER FAILOVER` (all grades) is refused on a primary with an error;
the primary-absorbs-primary absorb mode is removed entirely, not merely made deterministic. An
operator who wants to move a primary's slots uses explicit resharding, or issues `CLUSTER FAILOVER`
to one of that primary's replicas instead.

- Remove the `FORCE`/`TAKEOVER`-on-a-primary absorb branch from `admin.rs`.
- The primary path always returns the "`CLUSTER FAILOVER` can only be run on a replica" error,
  regardless of `force`/`takeover`.
- Amend FM-CLUSTER-040's "primary-absorbs-primary case" language — that case no longer exists; note
  the removal and cross-reference this issue (also tracked by issue 29's row-edit sweep).

## Acceptance criteria

- [ ] FM row added (or FM-CLUSTER-040 amended in place) stating `CLUSTER FAILOVER` is refused on a
      primary regardless of grade, with the removed absorb mode noted as a deliberate Redis-parity
      change; `just lint-spec` green
- [ ] Forcing test in `frogdb-server`/`frogdb-cluster` sending `CLUSTER FAILOVER FORCE` (and
      `TAKEOVER`) to a primary and asserting a refusal, not an absorb, fails first against today's
      handler, then is fixed
- [ ] The primary-absorbs-primary code path and its `HashMap`-iteration victim selection are
      deleted, not merely made deterministic
- [ ] `just mutants-diff` triaged on every touched locked crate (`frogdb-cluster` if the FM row's
      invariant text or the `RaftClusterOp::Failover` construction site moves there; the handler
      itself lives in `frogdb-server`, which is not a locked crate, but the FM-row/test pair must
      still force the behavior from a locked-crate-adjacent test where possible)

## Blocked by

None.

## Ruling (2026-08-13)

**Redis parity: `CLUSTER FAILOVER` (all grades) is only valid on a replica; a primary answers an error. The current behavior — a primary absorbing an arbitrary peer primary chosen by HashMap iteration order and force-evicting it — is removed. An operator who wants to move a primary's slots uses explicit resharding or a failover issued to one of its replicas. FM row + forcing test.**
