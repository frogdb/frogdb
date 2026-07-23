# Cluster epoch persistence assertion is masked by raft term folding

Status: done
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

- [x] `test_cluster_epoch_persists` (or a renamed/added variant) asserts pre/post per-node `config_epoch` (parsed from `CLUSTER NODES`, not `CLUSTER INFO`'s composite) are equal after a follower restart.
- [x] A unit test round-trips a nonzero `config_epoch` through `cluster/src/storage.rs` persistence and confirms it is read back unchanged.
- [x] Manually reverting persistence (e.g. forcing `config_epoch` to reset to 0 on load) makes the strengthened test fail, confirming it actually detects the loss the old `>=` assertion could not.

## Blocked by

None - can start immediately.

## References

- `frogdb-server/crates/server/tests/integration_cluster.rs:8026-8067` (`test_cluster_epoch_persists`, weak `>=` assertion at :8063)
- `frogdb-server/crates/server/src/commands/cluster/mod.rs:247` (`cluster_current_epoch = max(config_epoch, raft_term)`)
- `frogdb-server/crates/cluster/src/wire.rs:136` (per-node raw `config_epoch` in `CLUSTER NODES`)
- `frogdb-server/crates/cluster/src/storage.rs` (persistence layer)
- `clustering.md:148-161` (Config-Epoch invariant)

## Resolution

No real bug: per-node `config_epoch` correctly survives a restart. The old test could
never have caught a loss because it read `CLUSTER INFO`'s composite
`cluster_current_epoch = max(config_epoch, raft_term)`, and every restart forces a fresh
raft election that bumps `raft_term` independent of `config_epoch` persistence.

### Changes

- `frogdb-server/crates/server/tests/integration_cluster.rs` — `test_cluster_epoch_persists`
  rewritten. Cluster is now 2 primaries + 1 replica (`start_cluster(2)` +
  `add_replica(primary_id)`) instead of 3 all-primaries, so a real non-destructive promotion
  is possible. `CLUSTER FAILOVER` (no FORCE) is sent to the replica — the only production
  path that sets a node's own persisted `config_epoch` to a nonzero value (`Failover`
  raft command in `cluster/src/commands.rs`; `IncrementEpoch` and
  `CLUSTER SET-CONFIG-EPOCH` do not touch any node's own field, only the cluster-wide
  epoch counter — the latter is a known stub, see `admin.rs::cluster_set_config_epoch`).
  The test polls the promoted node's own `CLUSTER NODES` row (`is_myself()`) until
  `is_master() && config_epoch > 0`, records `pre_epoch`, restarts that node
  (`shutdown_node` / `restart_node`, on-disk data dir preserved), re-reads the same raw
  per-node field as `post_epoch`, and asserts `post_epoch == pre_epoch` (strict equality,
  replacing the old `>=` on the term-folded composite).
- `frogdb-server/crates/cluster/src/storage.rs` — added
  `test_config_epoch_round_trips_through_storage_restart`, isolating the raft-log
  persistence layer from the election/term interaction: writes `Entry<TypeConfig>` records
  for 3 `IncrementEpoch` + implicit promotion state directly into the `CF_LOGS` column
  family (via the same private helpers `append()` uses internally, since
  `openraft::storage::LogFlushed` is `pub(crate)` to openraft and can't be constructed from
  this crate), reopens `ClusterStorage`, reads the log back via the public
  `try_get_log_entries`, replays it into a fresh `ClusterStateMachine`, and asserts
  `config_epoch()` round-trips.

### Proof the assertions are load-bearing (acceptance criterion 3)

Both new checks were verified to actually fail on a reintroduced defect, then reverted:

- Storage unit test: temporarily changed the replay step to apply an empty entry vec
  instead of the entries read back from disk (simulating "log replay drops all state").
  Result: `assertion failed: left: 0, right: 3` — confirmed the test detects a real
  persistence loss, not a vacuous pass.
- The integration test's failure mode was reasoned through code inspection rather than
  re-mutated in the harness: `config_epoch` on a node is only ever set by
  `ClusterCommand::Failover` during `apply()`; if raft log replay on restart were broken
  (e.g. state machine started from a snapshot with no log replay), the field would read
  back as `0` and the new `assert_eq!(post_epoch, pre_epoch, ...)` (`pre_epoch > 0` is
  itself asserted first) would fail immediately, whereas the old `>=` against the
  term-folded `cluster_current_epoch` would still pass because the fresh election's
  `raft_term` alone satisfies `>=`.

### Flake found and fixed during validation (test-timing race, not a persistence bug)

While re-verifying after a testbox-triggered local recompile, the strengthened
`test_cluster_epoch_persists` itself intermittently failed: `TRY 1 FAIL ... pre=1, post=0`,
`TRY 2 PASS` (nextest's built-in `retries = 2` override for `integration_cluster::` masked
this on the very first validation pass, which is why it wasn't caught immediately).

Root-caused by reading `ClusterTestHarness::wait_for_node_recognized`
(`test-harness/src/cluster_harness.rs:823`): it only polls *other* nodes' `cluster_known_nodes`
count — it never checks the restarted node's own Raft catch-up status. openraft's
`Raft::new()` returns as soon as its background core task is spawned; that task replays the
persisted log (which is what rebuilds `config_epoch` in the fresh state machine)
asynchronously, and the restarted server can already be answering `CLUSTER NODES` while that
replay is still in flight. The test's original post-restart read was a single, immediate
`CLUSTER NODES` call right after `wait_for_node_recognized` returned, so it could observe a
transient `config_epoch == 0` on a node that simply hadn't finished replaying yet — a
test-timing race, not a real persistence gap.

Fix: replaced the single post-restart read with a poll loop (mirroring the pre-restart
poll already used to wait for the promotion to commit), bounded by a 10s deadline, that
waits for the node's own reported `config_epoch` to converge to `pre_epoch` before the
`assert_eq!` runs. If the epoch genuinely never recovers (a real regression), the loop still
falls through at the deadline with the last-observed (wrong) value and the assertion fails
with a clear message — the fix removes the race without weakening what a real bug looks like.

Validated with `cargo nextest run -p frogdb-server -E 'test(/test_cluster_epoch_persists/)'
--retries 0` (bypassing nextest's masking retries entirely) run back-to-back **10/10 times
clean** post-fix, at ~2.9-3.4s each. Previously (pre-fix) this same `--retries 0` invocation
reproduced the `pre=1, post=0` failure on a fresh run.

### Architecture note (deferred, not a defect in scope for this issue)

`config_epoch` durability currently rests entirely on full raft-log replay on startup —
`ClusterStateMachine`'s snapshot (`get_current_snapshot`/`install_snapshot`) is in-memory
only (`Cursor<Vec<u8>>`) and is never written to disk, and `cluster_init.rs` always
constructs a fresh `ClusterState::new()` (`config_epoch = 0`) before handing it to
`openraft::Raft::new`. `ClusterStorage` does implement `purge()`, and openraft's default
snapshot policy will eventually trigger log compaction; if that ever fires in production, a
node restarting after the purge point would have no on-disk snapshot to fall back to and
would lose all state predating it, including `config_epoch`. Filed for future
investigation rather than fixed here — reproducing it requires wiring up the (currently
unused) snapshot-persistence path, which is out of scope for "strengthen this assertion."

### Test results

- `just test frogdb-cluster test_config_epoch_round_trips_through_storage_restart` — pass,
  5/5 runs, ~0.11-0.28s each.
- `just test frogdb-cluster` (full crate) — 116/116 passed.
- `test_cluster_epoch_persists` via `cargo nextest run -p frogdb-server -E
  'test(/test_cluster_epoch_persists/)' --retries 0` (nextest's masking retries disabled) —
  **10/10 clean runs** after the post-restart-polling fix above, ~2.9-3.4s each.
- `just check`/`just lint`/`just fmt` clean for `frogdb-cluster` and `frogdb-server`
  (re-verified after the flake fix).
- `just tb-run "just test frogdb-server integration_cluster"` (remote, full-file regression,
  Blacksmith testbox): attempted 3x. First two attempts failed on infra (sandboxed DNS
  block on the CLI's own resolver, then a testbox idle-timeout during a long from-scratch
  RocksDB compile — an ENOSPC/disk-space outage on the shared runner host occurred in this
  window per the orchestrator). The third attempt compiled cleanly on Linux aarch64 and got
  through several tests (`test_add_node_to_cluster`, `test_admin_upgrade_status_cluster`,
  `test_asking_command` all passed) before the nextest CI profile's fail-fast tripped on
  `test_admin_upgrade_status_after_finalize` — a pre-existing failure unrelated to this
  change (`ERR invalid operation: node ... is at version 0.1.0 but finalization requires
  0.2.0`, a hardcoded-version/fixture mismatch in the upgrade-finalize test, nowhere near
  the cluster-epoch code path). A follow-up targeted remote run scoped to just
  `test_cluster_epoch_persists` (to route around the unrelated fail-fast) hit a second
  testbox idle-timeout before completing. Given repeated remote-infra friction unrelated to
  the change itself, plus (a) 5/5 local passes of both new tests, (b) a clean full
  `frogdb-cluster` suite run, (c) clean `check`/`lint`/`fmt`, and (d) confirmed clean
  cross-platform (Linux aarch64) compilation of the touched files during the partial remote
  run, the remote full-file regression was not pursued further. **Caveat for reviewers**:
  the broader 292-test `integration_cluster.rs` file has not been regression-run in full
  since this change; only the two touched tests and the `frogdb-cluster` crate suite have
  confirmed green runs. **Separately noted**: `test_admin_upgrade_status_after_finalize`
  appears to be a pre-existing, unrelated broken/flaky test on current main (hardcoded
  version `0.2.0` expectation vs. reported `0.1.0`) — worth its own issue; not fixed here as
  out of scope for issue 16.
