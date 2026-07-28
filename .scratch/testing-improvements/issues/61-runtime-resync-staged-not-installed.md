# Runtime REPLICAOF full resync stages the checkpoint but does not install it into the live store

Status: done
Type: AFK
Origin: task 23 turmoil replication-failover sim (2026-07-23) — surfaced by the failback convergence assertion

## Context

While building the deterministic turmoil replication-failover simulation (issue 23,
`frogdb-server/crates/server/tests/simulation.rs`
`test_replication_failover_wgl_linearizable`), the failover + failback scenario exposed a
live-convergence gap.

Scenario: primary `P` + replica `R`. Confirmed writes (SET + `WAIT 1`) replicate to `R`. `P` is
isolated from `R` (`hold`). `R` is promoted (`REPLICAOF NO ONE`) and takes post-failover writes on
disjoint keys `p0..p4`. The isolation heals (`release`) and the operator fails back: `R` is demoted
back to a replica of the recovered `P` (`REPLICAOF <P> <port>`). `R` re-attaches and `WAIT 1` on `P`
returns `>= 1`, i.e. the replication link is up and acking.

Observed: after failback, `R` still serves its forked post-failover writes (`GET p0` -> `w..._0`)
even though `P` — the authoritative master — never had them. The two live keyspaces do not
reconverge; polling for ~8s does not clear the divergence.

## Root cause

`ReplicaConnection::receive_checkpoint`
(`frogdb-server/crates/replication/src/replica/connection.rs`, ~L263-296) is explicitly
*staged-not-installed*: the incoming full-sync checkpoint is written to a scratch dir and committed
as a staged checkpoint, the replica adopts the new replication id + offset and flips to `Streaming`,
but "the on-disk DB is unchanged until the next boot loads it" (doc-comment, verbatim). The live
in-memory/RocksDB store is never swapped for the new master's snapshot. A node that took a runtime
`REPLICAOF <new-master>` therefore keeps its entire pre-existing (possibly forked) live keyspace and
merely streams the new master's WAL tail on top; the staged snapshot only takes effect after a
restart.

Consequence: a demoted former-primary does not discard the writes it accepted while it was the
(temporary) master. Live-state reconvergence after a runtime role change requires a process restart.
For a clean single-key overwrite the WAL tail can paper over the divergence, but keys that exist only
on the deposed node (or diverged in value) persist until reboot.

## Impact

- Runtime failover/failback (`REPLICAOF` at runtime, no restart) does not achieve live keyspace
  convergence on the demoted node. Split-brain writes made during a promotion window survive a
  failback.
- Durable data (WAIT-confirmed writes present on both sides before the split) is unaffected and
  converges correctly — the turmoil test asserts exactly that and passes.

## What to verify / decide

- Confirm whether this is intended (restart-to-install by design, matching a documented operational
  runbook) or a gap. If intended, document the "restart required after runtime REPLICAOF to a new
  master" contract prominently (consistency.md / operator CONTEXT.md) and add a test asserting the
  staged-then-restart install path converges.
- If not intended, install the staged checkpoint into the live store at runtime (flush live keyspace
  + load snapshot) as part of the `receive_checkpoint` commit, then assert full live-keyspace
  reconvergence (all keys, not just the durable subset) after runtime failback.

## References

- `frogdb-server/crates/replication/src/replica/connection.rs` `receive_checkpoint` (staged-not-installed)
- `frogdb-server/crates/server/tests/simulation.rs` `test_replication_failover_wgl_linearizable`
  (failback convergence asserts the durable subset only; boundary noted inline referencing this issue)
- Sibling boundary: a promoted replica does not start a `PrimaryReplicationHandler` and so cannot
  serve PSYNC to a sub-replica (see issue 23 Resolution) — which is why the test fails back toward
  the recovered boot-primary rather than re-attaching the old primary under the promoted node.

## Resolution

Gap, not intended behavior — fixed. A received full-sync checkpoint is now installed into the **live**
keyspace before the replica adopts the snapshot's offset and resumes streaming, matching Redis (flush
the old dataset, load the master's snapshot, then apply the stream).

### Install mechanism

A new shard-message category carries the snapshot to the shards, which own the live store:

- `ShardMessage::Replication(ReplicationMsg::InstallSnapshot { entries, response_tx })`
  (`core/src/shard/message.rs`), handled by `ShardWorker::install_snapshot`
  (`core/src/shard/dispatch_replication.rs`): clear the store, `restore_entry` every snapshot key,
  then run the canonical write-effect pipeline once as a synthetic `FLUSHDB`, then WAL-persist the
  restored keys.
- The replication crate owns no store, so the install is injected the same way the connect factory
  is: `frogdb_replication::replica::CheckpointInstaller` (an
  `Arc<dyn Fn(PathBuf) -> Future<Output = io::Result<()>>>`), wired at both construction sites —
  boot-configured replica (`server/src/server/replication_init.rs`) and runtime `REPLICAOF`
  demotion (`RealReplicaStreamer::build_handler` in `server/src/role_manager.rs`) — from the single
  constructor `LiveCheckpointInstaller::for_config`.
- The server-side implementation (`server/src/replication/install.rs`) opens the staged checkpoint
  dir as a second RocksDB inside `spawn_blocking`, reads each shard with the existing
  `recover_shard_into` recovery protocol, and ships one `InstallSnapshot` per shard, awaiting each
  ack. The live RocksDB is **not** swapped: the live keyspace is the in-memory per-shard store, so a
  handle swap would leave exactly the bug being fixed. Durability converges through the shards' own
  WAL writes (a `ClearShard` range tombstone from the synthetic FLUSHDB, then a `Put` per restored
  key, in WAL sequence order).
- Ordering: stage → install → adopt offset/replid → `Streaming`. If the install fails the sync
  returns `Err` **and** rewinds the live offset to 0, so the next reconnect sends `PSYNC ? -1` and
  retries the whole full resync rather than streaming deltas onto a keyspace that never took the base
  snapshot.
- The staged dir is deliberately left on disk after the install, so the boot-time installer contract
  (`RocksStore::load_staged_checkpoint`) is untouched and a crash mid-install converges on the next
  boot (replaying the same snapshot is idempotent).

### Consistency window

**Per shard, not across shards.** Each shard's clear + restore runs to completion without an `.await`
inside its own single-threaded task, so no client can observe a half-installed *key* or a partially
replaced shard. The installer walks shards in order, so during the install a client can see shard 0
already swapped while shard 1 has not been — the same granularity every other cross-shard write
(FLUSHDB, MSET) already has. Clients are **not** blocked for the duration: reads are served from the
pre-install snapshot until their own shard swaps, which was chosen over quiescing the node for a
whole-dataset load. No *replicated* write can observe the intermediate state, because streaming does
not resume until every shard has acked.

### Side effects

Mirrors boot recovery: **no keyspace notifications** are emitted (the synthetic `FLUSHDB` the clear
routes through declares `EventSpec::Suppressed`, and restored keys are written straight into the
store rather than through per-key `SET` handlers). The install does bump WATCH versions and fire
flush-all client-tracking invalidation — a client whose watched key was replaced must abort, and a
tracking client's cache is stale. Replication broadcast is suppressed via
`REPLICA_INTERNAL_CONN_ID`, as for any replica-applied write. An install that neither clears nor
restores anything is a no-op (no version bump).

### Warm tier

A warm entry's value lives in the *staged* database, which is discarded after the install, so warm
keys are materialized as ordinary hot entries and re-demoted by the receiving node's own tiering
policy. Hot wins over warm for the same key, as at boot.

### Tests

- `core/src/shard/dispatch_replication.rs`: install replaces (not merges) the keyspace, bumps the
  WATCH version, an empty snapshot still clears, and an empty-into-empty install is a no-op.
- `replication/src/replica/connection.rs`: the installer is handed the committed staged dir and runs
  *before* the offset is adopted; an install failure returns `Err` and rewinds the offset so the next
  `PSYNC` asks `? -1`; with no installer wired the driver still stages + streams (degraded path).
- `server/tests/integration_replication.rs`
  `test_runtime_full_resync_installs_snapshot_into_live_store`: a RocksDB-backed primary with its own
  forked `p*` dataset takes a runtime `REPLICAOF` at a master holding a disjoint `m*` dataset; the
  `m*` keys become live and the `p*` keys are gone with **no restart**, `DBSIZE` proves it is a
  replace, and a subsequent streamed write still lands.

### Deferred edges

- **The turmoil failback sim (`test_replication_failover_wgl_linearizable`) still asserts only the
  durable subset**, and its inline boundary note has been rewritten to say why: those hosts run with
  persistence disabled, so the full sync ships the *minimal RDB* — an envelope carrying no dataset.
  There is nothing to install, and flushing on that path would delete confirmed data the primary
  never re-streams. Making the sim RocksDB-backed would inject `spawn_blocking` + real disk I/O into
  a seeded deterministic run, so the live-reconvergence claim is asserted in the integration test
  instead, which the sim comment now links to.
- **The data-less minimal-RDB full sync is itself a gap** (a persistence-disabled primary answers
  `PSYNC ? -1` with an empty envelope and relies on the replica keeping whatever it had). Out of
  scope here; worth its own issue.
- The install materializes the whole snapshot in memory (per shard) before handing it to the shards.
  Fine at current dataset sizes; a streaming/batched hand-off is the obvious next step if that stops
  being true.
