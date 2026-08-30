# 45: `FailureDetector::has_quorum` allocates to answer a boolean

Status: ready-for-agent

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
