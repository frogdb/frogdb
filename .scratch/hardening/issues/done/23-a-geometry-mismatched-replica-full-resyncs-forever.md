# A replica whose shard count or warm tier disagrees with the primary full-resyncs forever

Status: done
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

## Resolution

Remedy (1) implemented; (2) was already true; (3) deliberately not taken (see Open question).

**The classification.** `SnapshotInstaller` now returns `Result<(), InstallError>` instead of
`io::Result<()>`, with `InstallError::Incompatible { detail }` (terminal) beside
`InstallError::Transient(io::Error)` (retry). `impl From<io::Error> for InstallError` yields
`Transient`, so the *default* is the pre-fix behaviour: an installer that has not classified a
failure keeps retrying. That direction is deliberate — mistaking a transient failure for a terminal
one strands a replica that would have recovered on its own, which is worse than the storm this issue
is about. `install::classify_open_failure` maps `RocksError::ShardCountMismatch` and
`WarmTierMismatch` to `Incompatible` with a detail naming **both** sides (`written with 2 shard(s)`
/ `configured for 1`; `this node has tiered-storage.enabled = false`), and everything else — a
staged directory that is not a database at all, a missing path — to `Transient`.

**The latch.** `ReplicaConnection::install_payload` writes an `Incompatible` detail into the
handler's shared `sync_refusal: Arc<RwLock<Option<String>>>` before converting the error for the
wire. It has to outlive the connection that learned it: the connection is torn down the moment the
sync fails, and the reconnect loop — which is what must give up — runs above it. The reconnect loop
gained one arm before the generic error arm:

```rust
Err(e) if self.sync_refusal().is_some() => { /* log and return */ }
```

so a refused node dials exactly once. Red proof: with that arm disabled,
`a_geometry_mismatch_is_refused_once_and_not_retried` fails by timing out mid-retry-storm
(`the loop must give up rather than keep retrying: Elapsed(())`).

**The operator surface.** `master_link_status:down` cannot express this: down means "no data
arriving right now", which is usually a restarting primary, and an operator has no way to tell
"reconnecting" from "will never connect again". So the refusal is a *separate*, FrogDB-specific
`INFO replication` field, `master_sync_error`, rendered **only when present** — its presence is the
alert. Plumbed `ReplicaReplicationHandler::sync_refusal()` → `ReplicaStream::sync_refusal` →
`RoleManager::sync_refusal` (same active-stream selection as `link_up`, `None` when not a replica)
→ `RoleController::sync_refusal` → `ShardIdentity::master_sync_error()` → `ShardDiagnostics` /
`NodeStateSnapshot` / `CommandContext` (and through the scripting gate, so a sub-command context
does not lose it).

**Both renderers, one list.** The two INFO renderers each had their own `master_link_status`
literal. Rather than adding a second duplicated field, both now go through
`info::replica_link_fields(link_up, sync_error)` — the same shape used for the sync counters
(issue 24) and the backlog geometry (issue 20), and for the same reason: this path has already
produced a renderer split three times.

**Unasked-for fix found while wiring it: a refused node could not be re-armed.**
`RoleManager::demote()` treats a re-demotion to the same target as a no-op ("already replicating").
That is exactly the command the issue's remedy tells the operator to use after correcting the
config — so the recovery path was a dead end and the node would have stayed down forever with its
configuration now correct. `demote()` now skips the no-op when `sync_refusal().is_some()`; the
no-op still holds for a healthy link, so a repeated `REPLICAOF` does not tear down a good stream and
force a resync. Forced by `re_demoting_to_the_same_primary_retries_a_refused_stream` and
`re_demoting_to_the_same_primary_is_still_a_no_op_when_nothing_was_refused`.

**Remedy (2) was already satisfied.** The reconnect loop backs off exponentially (100 ms → 30 s cap)
on every error path; only a clean close resets it to 100 ms. No change needed.

## Open question — repartitioning a mismatched checkpoint (remedy 3)

Not taken here. Recorded so the choice is visible rather than forgotten:

- **A. Refuse, name the cause, require operator action** (implemented). The two nodes' configs are
  a deployment fact; a mismatch is a mistake, and the fix belongs where the mistake is.
- **B. Repartition the staged checkpoint** through `shard_for_key`, the way the live-dataset path
  already does. Closes only the shard-count half. Cost: the node silently adopts a partitioning the
  operator did not configure — a resharded primary would quietly re-key its replicas, and the first
  visible symptom would be during a promotion. It also makes the checkpoint path pay a full decode
  it currently avoids.
- **C. Negotiate geometry in the handshake** (a `REPLCONF` option carrying shard count and warm-tier
  flag) so the primary refuses the `PSYNC` before cutting a checkpoint at all. Strictly better than
  A on the primary's cost — nothing is cut — and it composes with A rather than replacing it. The
  handshake already carries `frogdb-version` (issue 22), so the slot exists. Blocked on nothing
  except scope; worth a follow-up issue if the storm's cost on the primary matters more than the
  one wasted checkpoint A still permits.

The warm-tier half has no equivalent to B at all: the checkpoint's column families are a different
DB layout, not a different key routing.

## Tests

Names differ from the issue's suggestions where the behaviour split in two; the mapping:

- `a_geometry_mismatch_is_refused_once_and_not_retried` (`replication/src/replica/connection.rs`) —
  as suggested: exactly one dial, refusal readable from the handler afterwards, link down.
- `a_transient_install_failure_is_still_retried` (same file) — as suggested.
- `an_incompatible_install_latches_the_refusal_and_a_transient_one_does_not` (same file) — the
  latching half, asserted at the seam that does it.
- `a_geometry_mismatch_is_named_in_the_operator_surface` split into its two real halves:
  - `a_checkpoint_this_node_cannot_read_is_refused_and_touches_no_shard`
    (`replication-runtime/src/install.rs`, extended) — the *classification* and both sides of the
    mismatch, at the seam that produces the detail;
  - `a_replica_that_gave_up_names_the_mismatch_in_info` (`server/src/info/sections.rs`) — the same
    detail reaching the field an operator reads.
- `a_link_that_is_merely_down_renders_no_sync_error` — absence is what makes presence actionable.
- `both_renderers_report_the_same_replica_link_block` — the split this path keeps producing.
- `master_sync_error_derives_from_role_controller` (`core/src/shard/types.rs`) — live derivation,
  not a per-shard copy.
- `sync_refusal_reads_through_the_active_stream`,
  `re_demoting_to_the_same_primary_retries_a_refused_stream`,
  `re_demoting_to_the_same_primary_is_still_a_no_op_when_nothing_was_refused`
  (`server/src/role_manager.rs`).

Red proofs: the loop arm disabled → `a_geometry_mismatch_is_refused_once_and_not_retried` times out
retrying; `replica_link_fields` dropping the refusal → `a_replica_that_gave_up_names_the_mismatch_in_info`
fails; the `demote()` guard reverted → `re_demoting_to_the_same_primary_retries_a_refused_stream`
fails.

## Spec impact (done)

- **FM-REPLICATION-061** added — the terminal refusal, the operator surface, and the re-arm path.
- **FM-REPLICATION-053** rewritten where it was wrong: Observable now records that the *reason* for
  an unopenable checkpoint decides whether the sync is retried, the Invariant names
  `classify_open_failure` instead of the old blanket `failed to open staged checkpoint` mapping, the
  Outcome variant distinguishes the two, and the Bug refs paragraph no longer calls the retry storm
  a known limitation.
- A Redis-deviations row for `master_sync_error` (FrogDB-specific; Redis has no structural refusal
  to report, so its "always retry" is right for its failure set and wrong for this one).
