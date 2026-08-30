# 45: `FailureDetector::has_quorum` allocates to answer a boolean

Status: done

## Origin

Found while closing [issue 40](../done/40-read-consistency-contract-and-serve-stale-knob.md)
(FM-CLUSTER-107), which moved the self-fence verdict from once-per-write to
once-per-command on a fenced node.

## What is wrong

`FailureDetector::has_quorum` calls `ClusterState::get_all_nodes()`, which is

```rust
pub fn get_all_nodes(&self) -> Vec<NodeInfo> {
    self.read_inner().nodes.values().cloned().collect()
}
```

— a `Vec` allocation plus a deep clone of every `NodeInfo` (each carrying at least
one `String` id), to compute `reachable_count` and compare it against a majority.
`reachable_count` needs only `node.id`.

That cost was tolerable while the verdict was read once per *write*. FM-CLUSTER-107
made the read fence consult the same verdict, so on a fenced node it is now read once
per *command*. `SelfFenceGate::fences_stale_reads` short-circuits on
`cluster-self-fence-on-quorum-loss` before touching the node table, so a cluster
running with the fence disarmed pays nothing and the healthy path is unchanged — but a
node that is actually fenced allocates per rejected command, which is the worst moment
to be allocating.

## What to build

An allocation-free quorum read: iterate the node ids under the existing read lock
rather than materializing a `Vec<NodeInfo>`, or hold a maintained reachable count.

Not done under issue 40 deliberately. `reachable_count`'s signature and
`get_all_nodes()` are both load-bearing for FM-CLUSTER-055's forcing tests in a
**locked** crate (`frogdb-cluster`, gate 0.80), so changing them is its own spec-first
edit rather than a drive-by inside a behavior change.

## Ruling (2026-08-29)

- **Approach: iterate under lock.** A "maintained reachable count" is unsound here:
  reachability is time-based (`last_seen` vs the staleness window), so a node goes
  stale by clock passage with no event to update a counter on. A per-tick cached
  verdict would lag fence engage/release by up to one check interval and force
  spec-row amendments for no need. Instead, `ClusterState` gains an allocation-free
  reader exposing the node count and an id iterator under its existing read lock,
  and `HealthTable::has_quorum`/`reachable_count` consume ids instead of
  `&[NodeInfo]`. Semantics identical — no FM row changes expected.
- **Scope: convert both callers.** `FailureDetector::has_quorum` (per-command on a
  fenced node) and `reconcile_topology` (per-tick) both move to the new reader —
  no half-converted API.
- **`ClusterState::get_all_nodes` is deleted.** After conversion it has zero
  production callers; in-crate tests and any forcing tests that used it rewire
  onto the new reader (mechanical — invariants unchanged).
- **Lock order:** health lock first, then cluster-state read lock, matching
  `reconcile_topology`'s existing order.

## Acceptance criteria

- [ ] `has_quorum` performs no heap allocation on the hot path
- [ ] FM-CLUSTER-055 and FM-CLUSTER-059 forcing tests still pass unchanged, or the
      rows are amended spec-first if their invariants move
- [ ] `just mutants-diff frogdb-cluster` and `just mutants-diff frogdb-cluster-runtime` triaged
- [ ] `ClusterState::get_all_nodes` no longer exists

## Blocked by

None.

## Resolution

Landed as the ruling described: iterate under lock, both callers converted,
`get_all_nodes` deleted. No spec rows moved — the quorum arithmetic is
byte-for-byte what it was, so FM-CLUSTER-055 and FM-CLUSTER-059 keep their
invariants and their forcing tests changed shape only.

**New reader.** `ClusterState::with_nodes` (`cluster/src/state.rs`):

```rust
pub fn with_nodes<T>(&self, f: impl FnOnce(usize, &mut dyn Iterator<Item = &NodeInfo>) -> T) -> T
```

One reader rather than a family of siblings: the closure gets the node count
and a borrowed iterator under the existing read lock, and each caller projects
out what it needs (ids for the quorum verdict, `(id, flags.fail)` for
reconciliation, `(id, cluster_addr)` for the probe loop). Nothing escapes the
lock and no `NodeInfo` is cloned. The lock is held for the duration of `f`, so
`f` must not take another cluster-state lock — documented at the method.

**Consumers.** `HealthTable::reachable_count` and `HealthTable::has_quorum`
(`cluster-runtime/src/failure_detector.rs`) now take `impl Iterator<Item =
NodeId>`; `has_quorum` takes the total alongside it because the iterator is
consumed by the count and is not required to be sized. Self is still always
reachable and quorum is still `floor(total/2)+1`.

`get_all_nodes` had **three** production callers, not two — the issue text and
ruling named `has_quorum` and `reconcile_topology`, but the probe loop in
`spawn_failure_detector_task` was cloning the table once per check interval as
well. All three converted; the probe loop now collects
`Vec<(NodeId, SocketAddr)>` (it has to escape the lock — it spawns a task per
peer). Lock order in `has_quorum` is health first, then cluster state, matching
`reconcile_topology`.

**Verification.**

- `frogdb-cluster` 295 tests pass, `frogdb-cluster-runtime` 103 pass,
  `frogdb-server` self-fence/quorum selection 17 pass (includes
  `the_gauntlet_refuses_a_read_on_a_quorum_fenced_node`, the FM-CLUSTER-107
  read fence that motivated this issue).
- `just mutants-diff frogdb-cluster`: 1 mutant, 1 unviable, 0 missed. The only
  mutant is `with_nodes -> Default::default()`, which cannot compile against a
  generic `T`; the baseline built and its tests passed, so this is the
  function's shape, not a scratch-directory problem. The reader's node count is
  asserted by the FM-CLUSTER-078 test `state_readers_report_the_applied_table`.
- `just mutants-diff frogdb-cluster-runtime`: 12 mutants, 8 caught, 4 unviable,
  0 missed. The four unviable are `spawn_failure_detector_task`'s
  `JoinHandle::{new,from,from_iter}` substitutions.
- `just lint` (including `just lint-spec`) green.
