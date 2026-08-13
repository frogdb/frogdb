# Persistence-disabled primary serves a data-less full sync: replica keeps its stale keyspace

Status: done
Type: bug
Origin: Follow-up filed from issue 61 (live checkpoint install) — discovered while attempting to
strengthen the turmoil failover sim's failback convergence assertion.
Severity: likelihood 2/3, consequence 2/3 (score 4)
Area: replication

## Context

When a primary runs with `persistence.enabled = false`, its full-sync path (`handle_full`) takes
the *minimal-RDB* branch: it answers `PSYNC ? -1` with an empty envelope carrying no dataset. The
replica adopts the new replid/offset and flips to `Streaming`, but its pre-existing live keyspace
is left exactly as it was — a full "sync" that transfers nothing and deletes nothing.

Issue 61 made runtime full resync install the received checkpoint into the live store, which fixes
reconvergence for persistence-enabled primaries. But the persistence-disabled branch cannot benefit:
there is no checkpoint to install, and flushing the replica on that path would delete confirmed
data the primary never re-streams (the WAL tail starts at the new offset). The turmoil failover sim
(`frogdb-server/crates/server/tests/simulation.rs`, `test_replication_failover_wgl_linearizable`)
runs its hosts persistence-disabled, so its failback assertion is pinned to the durable subset —
forked post-failover keys on the demoted node survive failback in that configuration.

## Impact

- A replica (re)attaching to a persistence-disabled primary never converges to the primary's
  keyspace: stale keys survive, and keys that exist only on the replica persist indefinitely.
- Redis parity gap: a diskless Redis primary still streams a full RDB (`repl-diskless-sync`,
  fork + socket serialization); "no persistence configured" does not mean "no dataset in full sync".

## What to build

Decide and implement one of:

1. **Diskless-style live snapshot** (Redis parity): a persistence-disabled primary serializes its
   live keyspace (per-shard scan → snapshot entries) into the full-sync envelope, and the replica
   installs it via the issue-61 `InstallSnapshot` path. No RocksDB involvement required — the
   snapshot format already exists for the staged-checkpoint path.
2. **Reject the configuration**: refuse to serve PSYNC (or refuse REPLICAOF) when the primary has
   persistence disabled, making the operational contract explicit.

Option 1 matches Redis and makes the turmoil sim's full-convergence assertion honest without
persistence machinery in the sim.

## Acceptance criteria

- [x] A replica with pre-existing divergent keys that full-syncs from a persistence-disabled
      primary ends with a live keyspace identical to the primary's (or the configuration is
      explicitly rejected with a documented error).
- [x] Turmoil failover sim's failback assertion strengthened from durable-subset to full-keyspace
      equality (if option 1), deleting the boundary comment referencing issue 61/65.
- [x] Integration test covering the persistence-disabled full-sync path end-to-end.

## Blocked by

None — issue 61 (live install seam) is merged and provides the installer this would feed.

## References

- .scratch/testing-improvements/issues/61 (Resolution)
- frogdb-server/crates/replication/src/primary/ (`handle_full`, minimal-RDB branch)
- frogdb-server/crates/server/src/replication/install.rs (`LiveCheckpointInstaller`, issue 61)
- frogdb-server/crates/server/tests/simulation.rs (`test_replication_failover_wgl_linearizable`,
  durable-subset boundary)

## Resolution

Option 1 (diskless-style live snapshot, Redis `repl-diskless-sync` parity). A primary with
`persistence.enabled = false` now serializes its live keyspace into the full-sync envelope, and
the replica installs it through the issue-61 install seam. Spec:
[FM-REPLICATION-001](../../../../specs/replication.md) (with -002 for the
shard export/install seam and -003 for the blob framing).

What was built:

- **Primary side.** `replica_session::stream_live_dataset` replaces the minimal-RDB branch: it
  pulls one blob per shard from an injected `LiveSnapshotSource` and writes them under a new
  `$FROGDB_SNAPSHOT` marker, per-blob headers named `shard-<n>.dataset`, and the same
  `FullSyncMetadata` trailer the checkpoint path uses. `create_minimal_rdb` and the RDB opcode
  constants are gone.
- **Export.** `ReplicationMsg::ExportSnapshot` asks each shard for its keyspace on the shard's own
  task; expired keys are dropped, and a value that is not resident in memory fails the export
  rather than shipping a silent subset. Framing lives in
  `frogdb-persistence/src/serialization/dataset.rs` and is all-or-nothing on decode.
- **Install.** `CheckpointInstaller` became `SnapshotInstaller` over a `FullSyncPayload`
  (`StagedCheckpoint(PathBuf)` | `LiveDataset(Vec<Vec<u8>>)`); the live arm routes every decoded
  key through `shard_for_key` for the *receiving* node's shard count and installs into every
  shard, so no shard keeps a forked key.
- **Refusal, not a fake payload.** A primary that cannot produce a dataset (failed checkpoint cut,
  no live-snapshot source wired) now fails the sync; the replica's `psync` rejects any FULLRESYNC
  marker it cannot install, so an old primary's minimal RDB fails loudly instead of being adopted.
  `psync` also rewinds the offset to 0 on FULLRESYNC and adopts the granted offset only after the
  install succeeds.

Tests: `test_full_resync_from_a_persistence_disabled_primary_transfers_the_dataset`
(integration_replication), four wire-level tests in `replica/connection.rs` and two in
`replica_session.rs`, seven shard-level export/install tests, four framing tests. The turmoil
failback assertion in `run_replication_failover` is strengthened from the durable subset to
whole-keyspace convergence (split-brain keys gone from the demoted node, equal `DBSIZE`), and the
boundary note is deleted.
