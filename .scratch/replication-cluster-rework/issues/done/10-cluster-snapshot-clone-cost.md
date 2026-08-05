# `ClusterState::snapshot()` deep-clones the 16384-slot table on every read

Status: fixed
Type: AFK
Origin: analysis of [issue 01](../) (2026-07-31)
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

- [x] `snapshot()` performs no per-slot copying; a test asserts two consecutive calls with no
      intervening mutation return `Arc`-identical values (`Arc::ptr_eq`).
- [x] A mutation applied between two calls yields a different pointer with the new assignment
      visible (no stale publication).
- [x] Existing cluster and migration integration tests pass unchanged — in particular the
      migration-window tests, which depend on a snapshot being internally consistent (one snapshot
      per decision is the invariant the EXEC re-validation PRD established; the shared `Arc`
      strengthens it).

## Notes

Correctness-neutral by construction *provided* the rebuild happens inside the same lock as the
mutation. Rebuilding outside it re-introduces the torn-verdict window the EXEC re-validation work
closed.

## Resolution (2026-08-05)

Fixed as specified, with `parking_lot::RwLock<Arc<_>>` rather than `arc_swap` (`arc-swap` is not a
workspace dependency — it reaches the tree only as a transitive dep of `tantivy` — and adding one
for a value read under a lock that is otherwise uncontended was not worth it).

`ClusterState` now holds a private `StateCell { inner: ClusterStateInner, published:
Arc<ClusterSnapshot> }` under one lock, and `snapshot()` returns `Arc::clone(&published)`.
The single lock is the point: the published value cannot be rebuilt outside the critical section
that mutated the state, so the torn-verdict window stays closed.

Republication is structural rather than a convention. `PublishOnDrop` — the only handle that hands
out a `&mut ClusterStateInner`, since the lock field is private and the only other mutators are the
two no-republish setters below — rebuilds the snapshot in its `Drop`, so every mutation site
republishes whatever path it leaves by: `apply_command`'s early `return
Err`/`?` arms (including the new `SetConfigEpoch`), `restore_from_snapshot`, the two test-only
version overrides, and any site added later. The rebuild is unconditional, so a rejected command
republishes an identical value rather than relying on nothing having been touched yet. The two
fields no snapshot carries (`last_applied_log`, `last_membership`) get their own no-republish
setters, so openraft advancing per applied entry — including blank ones — does not rebuild the
slot table.

No caller mutated its copy: the whole workspace compiled against `Arc<ClusterSnapshot>` with only
two changes, both `match` arms whose other side builds a standalone snapshot
(`cluster_slots`/`cluster_shards` in `server/src/commands/cluster/mod.rs`, now `Arc::new(...)`).
Every other consumer already took `&ClusterSnapshot` and gets it by deref coercion. The
`ClusterSnapshot`s that *are* mutated in the tree are all locally built test fixtures.

Measured on an M-series laptop, release build, one node holding all 16384 slots, 10 000 iterations
each (`snapshot_cost_on_a_fully_assigned_cluster`, an `#[ignore]`d timing probe in
`state.rs`; run it with `cargo test --release -p frogdb-cluster snapshot_cost -- --ignored
--nocapture`):

```
rebuild per call:   63.608µs   (what snapshot() used to do)
published per call: 3ns        (what it does now)
```

~20 000x, and the remaining 3ns is an uncontended read lock plus a refcount bump. The rebuild
figure is 63.6µs here against the 114.5µs the original throwaway harness reported; the probe runs
in-process and rebuilds a one-node table, so treat the two as the same order of magnitude rather
than a regression or an improvement.

Spec: FM-CLUSTER-078 in `.scratch/hardening/specs/cluster-failure-modes.md`, forced by
`test_snapshot_observes_topology_applied_since_the_last_read`,
`test_repeated_snapshots_without_mutation_share_one_allocation`,
`test_rejected_command_leaves_snapshot_agreeing_with_state`, and
`test_snapshot_install_republishes_the_reader_view`.
