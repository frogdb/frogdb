# Cluster epoch persistence assertion is masked by raft term folding

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 3/3 (score 6)
Area: cluster

## Context

`test_cluster_epoch_persists` (`frogdb-server/crates/server/tests/integration_cluster.rs:8026-8067`) restarts a follower and asserts only `post_epoch >= pre_epoch` (line 8063), where `pre_epoch`/`post_epoch` come from `CLUSTER INFO`'s `cluster_current_epoch` (`cluster_current_epoch = max(config_epoch, raft_term)`, `frogdb-server/crates/server/src/commands/cluster/mod.rs:247`). A restart triggers a fresh Raft election, which bumps `raft_term` regardless of whether the persisted `config_epoch` survived. So the test passes even if the on-disk `config_epoch` reset to 0 — the composite epoch is folded and dominated by the term, hiding exactly the loss it's meant to guard against (the Config-Epoch invariant documented in `clustering.md:148-161`).

Expected behavior: `config_epoch` is a per-node persisted value used for conflict resolution during cluster topology changes; Redis Cluster requires it survive a restart unchanged (absent an explicit bump event). The test must read the raw per-node `config_epoch` — not the term-folded composite — to actually detect loss.

## What to build

- Parse per-node `config_epoch` from `CLUSTER NODES` output (raw value, `frogdb-server/crates/cluster/src/wire.rs:136`) before and after restart in `test_cluster_epoch_persists`, and assert equality (not `>=` on the folded `cluster_current_epoch`).
- Add a unit test round-tripping a nonzero `config_epoch` through the cluster persistence layer (`frogdb-server/crates/cluster/src/storage.rs`) without going through a full cluster harness restart, to isolate the storage layer from the election/term interaction.

## Acceptance criteria

- [ ] `test_cluster_epoch_persists` (or a renamed/added variant) asserts pre/post per-node `config_epoch` (parsed from `CLUSTER NODES`, not `CLUSTER INFO`'s composite) are equal after a follower restart.
- [ ] A unit test round-trips a nonzero `config_epoch` through `cluster/src/storage.rs` persistence and confirms it is read back unchanged.
- [ ] Manually reverting persistence (e.g. forcing `config_epoch` to reset to 0 on load) makes the strengthened test fail, confirming it actually detects the loss the old `>=` assertion could not.

## Blocked by

None - can start immediately.

## References

- `frogdb-server/crates/server/tests/integration_cluster.rs:8026-8067` (`test_cluster_epoch_persists`, weak `>=` assertion at :8063)
- `frogdb-server/crates/server/src/commands/cluster/mod.rs:247` (`cluster_current_epoch = max(config_epoch, raft_term)`)
- `frogdb-server/crates/cluster/src/wire.rs:136` (per-node raw `config_epoch` in `CLUSTER NODES`)
- `frogdb-server/crates/cluster/src/storage.rs` (persistence layer)
- `clustering.md:148-161` (Config-Epoch invariant)
