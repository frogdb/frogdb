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

---

## Follow-up resolution: the fold itself is gone (2026-07-28)

This issue pinned the fold's behaviour and documented it honestly; it deliberately did not remove
it. The removal happened under
[PRD: Cluster epoch fold redesign](../../replication-cluster-rework/epoch-fold-redesign.md)
(branch `worktree-agent-a8eb890ba0f01c596`, commit `41c1727d` plus follow-ups).

What changed relative to what this issue left behind:

- `fold_current_epoch` and its five unit tests are deleted. `CLUSTER INFO` reports
  `cluster_current_epoch` as the replicated counter verbatim.
- The Raft term is now its own field, `cluster_raft_term`, documented as node-local and
  unreplicated. It is also exposed in the HTTP admin API (`raft_term`) and the debug web UI, so
  the term is observable without a folded field.
- The `current_epoch >= max(per-node config_epoch)` relationship this issue pinned still holds, but
  now at its source: issue 64's `reconcile_incoming_epoch` ratchets the cluster-wide counter on
  `AddNode` instead of a `max()` at the reporting site.
  `test_config_epoch_counter_dominates_every_node_epoch_across_command_sequence`
  (`crates/cluster/src/state.rs`) sweeps a mixed command sequence and asserts it after every step.
- The "monotonic but not a reliable topology-change detector" caveat this issue documented is
  **obsolete**: every Config Epoch bump is now visible in `cluster_current_epoch`. The remaining
  caveat is `CLUSTER RESET HARD`, which resets the counter by design.
- Docs: the architecture subsection this issue added ("`CLUSTER INFO`'s current epoch folds in the
  Raft term") is replaced by "Config Epoch vs. Raft term", a field-by-field table plus why the fold
  was wrong in both directions.

Two integration tests this issue created were rebased onto the new contract rather than deleted:
`test_cluster_info_epoch_vs_nodes_epoch_after_reelection_no_topology_change` now asserts a pure
re-election leaves `cluster_current_epoch` **unchanged** (it previously tolerated the term-driven
bump), and `test_cluster_info_epoch_monotonic_across_failover` asserts a strict increase where it
previously had to accept equality.

Rebasing `test_cluster_epoch_increases_after_failover` onto the real counter exposed a genuine
product bug the fold had been hiding end-to-end: `FailureDetector` propagated its one-shot FAIL
latch inline and leader-only, so a survivor that crossed `fail-threshold` while still a follower
never flagged the dead node after winning the election — no epoch bump, no auto-failover, which is
the shape of every leader-death failover. The detector now reconciles its local health view into
the replicated topology once per check interval. Details in the PRD's implementation notes.

### Review-fix round (2026-07-28)

One observability change relative to the notes above: **standalone mode omits the
`cluster_raft_term` line entirely** rather than reporting `cluster_raft_term:0`. A server with no
Raft has no term, and printing `0` states something untrue — the same bar this issue was filed
under. The other zeroed standalone fields (`cluster_current_epoch`, `cluster_my_epoch`) are
unchanged: `0` is the accurate value for a counter that exists and has never moved.
