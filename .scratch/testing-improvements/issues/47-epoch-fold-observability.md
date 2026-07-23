# Pin INFO-vs-NODES cluster epoch relationship deliberately

Status: needs-triage
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

- [ ] Unit test pins `commands/cluster/mod.rs:247` folding helper output for representative
      `(config_epoch, raft_term)` pairs.
- [ ] Integration test captures INFO epoch + NODES per-node epochs after re-election without
      topology change; asserts the deliberate fold relationship — does **not** assert
      `INFO epoch <= max(NODES epoch)`.
- [ ] Test comment explicitly documents that Redis `currentEpoch` may legitimately exceed config
      epochs post-election, citing this as the reason the naive bound is wrong.
- [ ] Epoch-collision detection behavior investigated; either covered by a new test or filed as an
      explicit separate follow-up gap if absent.

## Blocked by

None - can start immediately

## References

- .scratch/testing-improvements/audit/F-cluster.md #1 (`info-current-epoch-folds-raft-term`)
- .scratch/testing-improvements/audit/verdicts-F.md #1 (ADJUSTED L2/C1 — reframe required)
- commands/cluster/mod.rs:247
- cluster/src/wire.rs:136
