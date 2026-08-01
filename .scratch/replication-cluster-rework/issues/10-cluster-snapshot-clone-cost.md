# `ClusterState::snapshot()` deep-clones the 16384-slot table on every read

Status: needs-triage
Type: AFK
Origin: analysis of [issue 01](01-exec-slot-table-version-fast-path.md) (2026-07-31)
Severity: likelihood 3/3, consequence 1/3 (score 3) — pure cost, no correctness risk
Area: Cluster

## Problem

`ClusterState::snapshot()` (`frogdb-server/crates/cluster/src/state.rs:129`) builds a fresh
`ClusterSnapshot` under the state read lock by cloning `nodes`, `slot_assignment` and `migrations`.
On a fully assigned cluster `slot_assignment` is a `BTreeMap<u16, NodeId>` with all 16384 entries,
so every call allocates and copies the whole table.

Measured on an M-series laptop (release build, throwaway harness):

```
clone of 16384-slot snapshot: 114.501µs per call
10 slot lookups:                  132ns per call
```

Callers are not rare. `SlotMigrationCoordinator::route`
(`server/src/slot_migration/routing.rs:119`) snapshots on **every keyed command** in cluster mode,
via `PreDispatchView::validate_cluster_slots` — including once per command while queuing a MULTI.
`check_migrating_multikey`, `validate_queued_batch`, `validate_watch_slots`,
`watched_slots_still_local`, the failure detector, `CLUSTER`/`INFO`/admin handlers and the debug
providers each snapshot again. Cluster-mode per-command latency is therefore dominated by copying a
table that changes only on a topology event.

## Fix direction

Publish the snapshot as an immutable value the readers share instead of rebuilding it:

- Keep an `arc_swap::ArcSwap<ClusterSnapshot>` (or `parking_lot::RwLock<Arc<_>>`) alongside the
  authoritative `ClusterStateInner`, rebuilt once inside the same critical section that applies a
  mutation (`apply_command`, `restore_from_snapshot`, membership changes).
- `snapshot()` becomes an `Arc` clone. Readers that need `&ClusterSnapshot` are unaffected; the few
  that mutate their copy must be found and changed to work off the shared value.
- Everything downstream keeps taking `&ClusterSnapshot`, so the routing seam does not move.

Once reads are pointer-cheap, the `slot_table_version` fast path proposed in issue 01 has nothing
left to save, which is why that issue is closed as `wontfix as specified`.

## Acceptance criteria

- [ ] `snapshot()` performs no per-slot copying; a test asserts two consecutive calls with no
      intervening mutation return `Arc`-identical values (`Arc::ptr_eq`).
- [ ] A mutation applied between two calls yields a different pointer with the new assignment
      visible (no stale publication).
- [ ] Existing cluster and migration integration tests pass unchanged — in particular the
      migration-window tests, which depend on a snapshot being internally consistent (one snapshot
      per decision is the invariant the EXEC re-validation PRD established; the shared `Arc`
      strengthens it).

## Notes

Correctness-neutral by construction *provided* the rebuild happens inside the same lock as the
mutation. Rebuilding outside it re-introduces the torn-verdict window the EXEC re-validation work
closed.
