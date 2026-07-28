# No epoch-collision detection/prevention exists anywhere in FrogDB cluster mode

Status: done
Type: AFK
Origin: Follow-up filed while implementing issue 47 (epoch-fold observability), per issue 47's
acceptance criterion to investigate epoch-collision detection and file a separate gap if absent.
Severity: likelihood 1/3, consequence 2/3 (score 2)
Area: Cluster

## Context

Redis's `redis-cli --cluster check`-style consistency tooling flags **epoch collisions** -- two
nodes independently claiming the same `configEpoch` -- as the actual invariant violation worth
detecting (drift/exceedance of `currentEpoch` over `configEpoch`s, which issue 47 covers, is not a
collision and is not a bug). FrogDB has **no equivalent detection or prevention anywhere**:

- **No prevention at node-join time.** `ClusterCommand::AddNode`
  (`frogdb-server/crates/cluster/src/commands.rs:25-53`) inserts the incoming `NodeInfo` verbatim,
  including whatever `config_epoch` it carries, with no check against the `config_epoch` already
  held by any existing node. A node rejoining after a network partition, a manually restored
  on-disk state, or a merge of two previously-independent sub-clusters (e.g. via repeated `CLUSTER
  MEET`) could introduce a node whose `config_epoch` collides with an existing primary's.
- **No detection/inspection command.** There is no `CLUSTER` subcommand or observability endpoint
  that scans `CLUSTER NODES`-equivalent data across the known node set and reports epoch
  collisions. `frogctl cluster check` is a stub that unconditionally errors: `frogctl/src/commands/
  cluster.rs:129` — `anyhow::bail!("frogctl cluster check: not yet implemented")`.
- **No test coverage.** `grep -rn "collision"` across `frogdb-server/crates/cluster/src` and
  `frogdb-server/crates/server/src/commands/cluster/` returns nothing.

Under FrogDB's *current* command paths, a collision is structurally hard to produce during normal
operation: every `config_epoch` bump (`IncrementEpoch`, `Failover`, `MarkNodeFailed`) goes through
the single Raft-replicated counter and is applied by one linearized state-machine transition, and
only `Failover` ever assigns a bumped value to a specific node -- so two live primaries cannot both
claim the same nonzero epoch through that path alone. The gap is real anyway, for two reasons: (1)
`AddNode` is the one path that bypasses the counter entirely (it takes whatever `config_epoch` the
incoming node reports), so a stale or manually-edited node rejoining, or two independently
bootstrapped sub-clusters merging via `CLUSTER MEET`, is not guarded against; (2) there is no
observability surface an operator or `redis-cli --cluster check`-equivalent tool could use to catch
it even if it happened via a bug elsewhere -- there is nothing to grep for, and no test exercises
the scenario.

## What to build

- Decide whether `AddNode` should validate/reject an incoming node whose `config_epoch` collides
  with an existing node's, and if so, define the resolution policy (reject the join, bump the
  cluster-wide counter and reassign, etc.) -- mirroring how Redis's gossip layer resolves competing
  `configEpoch` claims deterministically (lower node ID loses and re-claims a fresh epoch).
- Add a detection path: either a `CLUSTER` subcommand / observability field that reports the set of
  colliding node-id pairs (if any), or implement `frogctl cluster check` for real and give it this
  as its first real check.
- Add integration test(s): construct (or simulate) a collision -- e.g. two independently-started
  single-node clusters each promoted to `config_epoch == 1`, then `CLUSTER MEET`d together -- and
  assert the chosen resolution/detection behavior.

## Acceptance criteria

- [x] Documented (or implemented) resolution policy for a `config_epoch` collision introduced via
      `AddNode`/`CLUSTER MEET`.
- [x] A detection mechanism exists (subcommand, observability field, or `frogctl cluster check`)
      and is covered by a test that injects a real collision.
- [x] If the decision is "won't fix, structurally rare enough to accept" -- that must be an explicit,
      written decision (not silence), since it leaves operators with no tool to catch a collision
      that does occur. (N/A -- both prevention and detection were implemented.)

## Resolution

Both prevention and detection were implemented.

### Resolution policy (prevention at `AddNode`)

`ClusterStateInner::reconcile_incoming_epoch` (`frogdb-server/crates/cluster/src/state.rs`) runs
inside the `AddNode` state-machine transition, before the node lands in the table:

| Incoming claim | Resolution |
|----------------|------------|
| `config_epoch == 0` | Recorded as-is; 0 is the "unassigned" bootstrap value (`NodeInfo::new_primary` starts there, and `CLUSTER MEET`, bootstrap seeding, and Raft self-registration all use it), so any number of nodes may hold it. If the node is already known at a nonzero epoch, that epoch is **preserved** instead of being reset to 0. |
| Nonzero, already held by another primary | Collision: the joining node is admitted with a freshly minted epoch (`max(cluster counter, max per-node epoch) + 1`); the incumbent keeps the contested epoch. Logged at `warn` with `claimed_epoch`/`assigned_epoch`. |
| Nonzero, uncontested | Recorded as-is; the cluster-wide counter is raised to at least that value, preserving the `cluster_current_epoch >= max(per-node config_epoch)` invariant from issue 47. |

Only primaries are compared, matching Redis's `clusterHandleConfigEpochCollision`, which returns
early unless both nodes are masters -- only a primary's epoch arbitrates slot ownership.

The epoch-preservation rule is a small scope addition beyond the literal issue text. It closes a
coupled latent bug: a node re-registering itself after a restart claims epoch 0, which would have
reset its recorded nonzero epoch, freeing that epoch for another node to claim and manufacturing a
future collision. `AddNode` now never lowers a recorded epoch.

### Divergence from Redis's gossip rule

Redis breaks the tie by node ID (lexicographically smaller ID gives up its epoch and re-claims a
fresh one) because under gossip both nodes learn of the collision independently and need a rule
each can apply without coordination. FrogDB applies `AddNode` in a linearized Raft state-machine
transition where "which node was already there" is well defined, so the incumbent keeps the epoch
and the joiner takes the fresh one -- no ID comparison needed. The outcome matches Redis's: the two
nodes end at distinct epochs and no node's epoch decreases. Every replica derives the same
resolution from the same log entry, so nodes cannot disagree about the result.

### Detection surface

`frogctl cluster check` (`frogctl/src/commands/cluster.rs`) is implemented for real. It reads
`CLUSTER NODES`, parses the node table, and runs a list of independent checks; epoch collision is
the first. It reports every group of primaries sharing a nonzero `config_epoch` (naming node IDs and
addresses) and exits `1` on any finding, `0` otherwise. Detection is kept even though `AddNode`
prevents the collision, because it catches one introduced by a bug elsewhere or by an operator
editing on-disk state. Further checks (slot coverage, open migrations) plug into the same `CHECKS`
list.

Docs: `website/src/content/docs/architecture/clustering.md` gained a "Config Epoch collisions"
subsection replacing the inline gap reference added by issue 47 (which referred to this issue by the
wrong number, 63).

### Tests

- `frogdb-server/crates/cluster/src/state.rs` -- 7 unit tests on the reconciliation policy
  (reassignment, minting above the cluster counter, uncontested claims raising the counter, epoch-0
  exemption, epoch preservation, replica-sharing-primary-epoch, self-claim).
- `frogdb-server/crates/server/tests/integration_cluster.rs` -- 2 tests injecting a real collision
  through a live 3-node cluster's leader (`test_add_node_epoch_collision_resolved_by_state_machine`,
  `test_add_node_without_epoch_preserves_recorded_epoch`), asserting the resolution converges on
  every node and that `CLUSTER NODES` shows distinct nonzero primary epochs.
- `frogctl/src/commands/cluster.rs` -- 10 unit tests on parsing, detection, and rendering.
- `frogctl/tests/integration_cluster.rs` -- 3 end-to-end tests against a live server.

## Blocked by

None - can start immediately.

## References

- `.scratch/testing-improvements/issues/47-epoch-fold-observability.md` (originating task)
- `frogdb-server/crates/cluster/src/commands.rs:25` (`AddNode`, no epoch-collision check)
- `frogctl/src/commands/cluster.rs:129` (`cluster check` stub)
- `website/src/content/docs/architecture/clustering.md` ("`CLUSTER INFO`'s current epoch folds in
  the Raft term" subsection, added by issue 47, notes this gap inline)
