# No epoch-collision detection/prevention exists anywhere in FrogDB cluster mode

Status: needs-triage
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

- [ ] Documented (or implemented) resolution policy for a `config_epoch` collision introduced via
      `AddNode`/`CLUSTER MEET`.
- [ ] A detection mechanism exists (subcommand, observability field, or `frogctl cluster check`)
      and is covered by a test that injects a real collision.
- [ ] If the decision is "won't fix, structurally rare enough to accept" -- that must be an explicit,
      written decision (not silence), since it leaves operators with no tool to catch a collision
      that does occur.

## Blocked by

None - can start immediately.

## References

- `.scratch/testing-improvements/issues/47-epoch-fold-observability.md` (originating task)
- `frogdb-server/crates/cluster/src/commands.rs:25` (`AddNode`, no epoch-collision check)
- `frogctl/src/commands/cluster.rs:129` (`cluster check` stub)
- `website/src/content/docs/architecture/clustering.md` ("`CLUSTER INFO`'s current epoch folds in
  the Raft term" subsection, added by issue 47, notes this gap inline)
