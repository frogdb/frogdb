# Persistence-disabled primary serves a data-less full sync: replica keeps its stale keyspace

Status: needs-triage
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

- [ ] A replica with pre-existing divergent keys that full-syncs from a persistence-disabled
      primary ends with a live keyspace identical to the primary's (or the configuration is
      explicitly rejected with a documented error).
- [ ] Turmoil failover sim's failback assertion strengthened from durable-subset to full-keyspace
      equality (if option 1), deleting the boundary comment referencing issue 61/65.
- [ ] Integration test covering the persistence-disabled full-sync path end-to-end.

## Blocked by

None — issue 61 (live install seam) is merged and provides the installer this would feed.

## References

- .scratch/testing-improvements/issues/61 (Resolution)
- frogdb-server/crates/replication/src/primary/ (`handle_full`, minimal-RDB branch)
- frogdb-server/crates/server/src/replication/install.rs (`LiveCheckpointInstaller`, issue 61)
- frogdb-server/crates/server/tests/simulation.rs (`test_replication_failover_wgl_linearizable`,
  durable-subset boundary)
