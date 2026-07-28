# Pin INFO-vs-NODES cluster epoch relationship deliberately

Status: done
PRD: [replication-cluster-rework/epoch-fold-redesign.md](../../replication-cluster-rework/epoch-fold-redesign.md)
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 1/3 (score 2)
Area: Cluster

## Context

`cluster_current_epoch` returned by `CLUSTER INFO` is computed as
`max(config_epoch, raft_term)` (`commands/cluster/mod.rs:247`), while `CLUSTER NODES` reports each
node's raw `config_epoch` (`cluster/src/wire.rs:136`). Nothing asserts any relationship between the
two today; every Raft election bumps `raft_term`, so `INFO`'s folded epoch can legitimately exceed
the max `config_epoch` seen across `CLUSTER NODES`.

The original audit gap proposed asserting `INFO epoch <= max(NODES config_epoch)` after a
leader re-election with no topology change. **This assertion is wrong and must not be
implemented as written**: verification against Redis semantics shows `currentEpoch` (the Redis
analog) legitimately exceeds config epochs after elections that don't reassign slots — that's
normal, not a bug. `redis-cli --cluster check`-style tooling flags epoch **collisions** (two nodes
independently claiming the same `config_epoch`), not epoch drift/exceedance. Filing the originally
proposed assertion would create a test that fails on entirely correct behavior.

The corrected framing (per verdict, ADJUSTED L2/C1): this is an observability-only gap. The task is
to pin the actual INFO-vs-NODES epoch relationship deliberately as regression coverage — documenting
and testing what the fold legitimately does and does not guarantee — and separately verify whether
epoch-collision detection exists/works, since that is the actual tooling-relevant invariant.

## What to build

- Unit test on the epoch-folding helper (`commands/cluster/mod.rs:247`) pinning
  `max(config_epoch, raft_term)` output for representative input pairs (raft_term > config_epoch,
  raft_term < config_epoch, equal).
- Integration test: after a leader re-election with no topology change, capture `CLUSTER INFO`
  `cluster_current_epoch` and each node's `CLUSTER NODES` `config_epoch`; assert the actual,
  deliberate relationship (the fold, not a `<=` bound over NODES epochs). Test comment must
  explicitly document why the naive `<=` assertion is wrong, so nobody reintroduces it.
- Investigate and add coverage for epoch-**collision** detection specifically (two nodes with
  identical `config_epoch`) — the actual invariant relevant to `redis-cli --cluster check`-style
  consistency tooling. If collision detection doesn't exist, file that as an explicit follow-up gap
  rather than silently absorbing it into this task.

## Acceptance criteria

- [x] Unit test pins `commands/cluster/mod.rs:247` folding helper output for representative
      `(config_epoch, raft_term)` pairs.
- [x] Integration test captures INFO epoch + NODES per-node epochs after re-election without
      topology change; asserts the deliberate fold relationship — does **not** assert
      `INFO epoch <= max(NODES epoch)`.
- [x] Test comment explicitly documents that Redis `currentEpoch` may legitimately exceed config
      epochs post-election, citing this as the reason the naive bound is wrong.
- [x] Epoch-collision detection behavior investigated; either covered by a new test or filed as an
      explicit separate follow-up gap if absent.

## Resolution

The `max(config_epoch, raft_term)` fold was extracted into a named
`fold_current_epoch(config_epoch, raft_term)` helper in `commands/cluster/mod.rs` (previously an
inline `.max()` at the `CLUSTER INFO` call site), so it can be unit-tested and carries the
invariants in its doc comment.

Pinned behavior:

- **Unit** (`commands/cluster/mod.rs`, `#[cfg(test)]`): `test_fold_current_epoch_*` — five tests
  covering `raft_term > config_epoch`, `config_epoch > raft_term`, equal, both zero, and the lossy
  case below.
- **Integration** (`frogdb-server/crates/server/tests/integration_cluster.rs`):
  - `test_cluster_info_epoch_vs_nodes_epoch_after_reelection_no_topology_change` — kills the
    leader (a pure Raft event; the test issues no `CLUSTER FAILOVER`/`MEET`/`ADDSLOTS`), asserts
    the per-node `config_epoch` max is unchanged, and asserts `cluster_current_epoch` is `>=` it
    and in fact strictly `>` it. That last assertion is the direct counterexample to the refuted
    audit claim: the naive `INFO epoch <= max(NODES config_epoch)` bound fails here on entirely
    correct behavior.
  - `test_cluster_info_epoch_monotonic_across_failover` — graceful `CLUSTER FAILOVER` on a real
    replica (harness pattern borrowed from issue 16's `test_cluster_epoch_persists`); asserts the
    raw per-node `config_epoch` bumps, `cluster_current_epoch` is non-decreasing, and the `>=`
    bound survives the epoch-bumping event.

**Finding while implementing (the fold is lossy in both directions).** The first draft of the
failover test asserted strict monotonic increase of `cluster_current_epoch` and *failed*: `pre=1,
post=1`. On a freshly bootstrapped cluster the first election takes `raft_term` to 1 while
`config_epoch` is still 0, so the first failover moves `config_epoch` 0 → 1 and the `max` fold
reports 1 both before and after. `cluster_current_epoch` is therefore monotonic but **not** a
reliable topology-change detector — the raw per-node `config_epoch` from `CLUSTER NODES` is. This
is now pinned by `test_fold_current_epoch_masks_config_epoch_bump_under_higher_term`, asserted as
non-decrease (not strict `>`) in the failover integration test, and documented in both the
`fold_current_epoch` doc comment and the clustering docs.

**Docs**: `website/src/content/docs/architecture/clustering.md` gained the subsection
"`CLUSTER INFO`'s current epoch folds in the Raft term", stating honestly that FrogDB's reported
current epoch includes the Raft term and that this diverges from Redis's gossip-agreed
`currentEpoch` (Redis has no leader-election term to fold in), covering both lossy directions, and
naming the two regression tests.

**Collision detection**: investigated and found absent everywhere — `AddNode` inserts an incoming
node's `config_epoch` verbatim with no uniqueness check, there is no detection command, and
`frogctl cluster check` is a stub that unconditionally errors. Filed as
`.scratch/testing-improvements/issues/64-cluster-epoch-collision-detection-absent.md` rather than
absorbed here, per this issue's acceptance criteria.

## Blocked by

None - can start immediately

## References

- .scratch/testing-improvements/audit/F-cluster.md #1 (`info-current-epoch-folds-raft-term`)
- .scratch/testing-improvements/audit/verdicts-F.md #1 (ADJUSTED L2/C1 — reframe required)
- commands/cluster/mod.rs:247
- cluster/src/wire.rs:136
