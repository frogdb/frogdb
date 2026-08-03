# A replica whose shard count or warm tier disagrees with the primary full-resyncs forever

Status: needs-triage
Type: bug (unbounded retry on an unsatisfiable operation)
Severity: likelihood 1/3 (requires a misconfigured pair, but that is exactly what happens during a
reshard or a tiered-storage rollout), consequence 3/3 (the replica never syncs, and each attempt
costs the primary a full checkpoint cut + transfer, indefinitely) — score 6 (weighted by
consequence: this burns the *primary* too)
Area: replication / full sync install

## Problem

`frogdb-server/crates/replication-runtime/src/install.rs` installs a `StagedCheckpoint` by opening
the staged directory as a RocksDB with **this** node's `cluster.shard_count` and
`tiered_storage.enabled`. `ColumnFamilyManifest::reconcile` refuses a DB whose persisted column
families disagree — `ShardCountMismatch` / `WarmTierMismatch`. The refusal is correct and loud, and
nothing is installed (forced by `a_checkpoint_this_node_cannot_read_is_refused_and_touches_no_shard`).

The problem is what happens next. The install error fails the sync, the replica drops the link and
reconnects, the primary cuts and ships **another** full checkpoint, and the install fails
identically. Nothing in the loop learns. So:

- the replica never serves data and never stops trying;
- the primary pays for a full checkpoint cut, a directory copy and a full transfer on every
  iteration, for a replica that cannot ever accept one;
- the operator sees a full-resync storm whose cause (a config disagreement between two nodes) is
  named only in a log line that scrolls past once per attempt.

The `LiveDataset` payload path does not have this problem: it routes each key through
`shard_for_key` against this node's shard count, so the geometries need not agree. Only the
persistent path is affected.

This was documented in the module header as a known limitation. Per `CLAUDE.md` ("if you need a
paragraph-long comment to justify why the workaround is OK, the code is wrong"), the paragraph is
now a pointer to this issue instead.

## Suggested remedy

Ordered by increasing ambition; (1) alone would close the operational dead-end.

1. **Fail once, visibly, and stop.** Distinguish a geometry mismatch from a transient install error
   at the seam (a distinct `InstallError` variant, not a stringly-typed check), and on that variant
   stop retrying: hold `master_link_status:down`, name both sides of the mismatch in the log and in
   an `INFO` field the operator will actually read, and require an explicit `REPLICAOF` (or a
   restart with corrected config) to try again. An unsatisfiable operation must not be retried on a
   timer.
2. **Back off even for the transient case.** The reconnect loop currently makes a full checkpoint a
   cheap thing to ask for repeatedly; it is not cheap for the primary.
3. **Repartition the staged checkpoint** the way the live-dataset path already repartitions, so the
   shard-count half of the mismatch stops being fatal at all. `route_dataset` /`install_per_shard`
   are the existing shape for this. The warm-tier half is a genuinely different DB layout and
   should stay a refusal.

## Tests that should exist

- `a_geometry_mismatch_is_refused_once_and_not_retried` — the install seam returns the terminal
  variant, and the replica does not issue a second `PSYNC`.
- `a_geometry_mismatch_is_named_in_the_operator_surface` — both the expected and the found value,
  in the field an operator reads, not only in a log line.
- `a_transient_install_failure_is_still_retried` — the terminal path must not swallow the ordinary
  case.

## Spec impact

FM-REPLICATION-053 covers the staged-checkpoint install and already records this refusal under
`Bug refs` as a known limitation. Closing this issue rewrites that row's Outcome variant (a
terminal error is a new observable) and its Bug refs.
