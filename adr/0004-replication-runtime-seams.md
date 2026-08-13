# The replication runtime is four seam implementations, not a host trait

`frogdb-replication` owns the protocol — frame encoding, the handshake, offset and `WAIT`
coordination, the primary/replica session state machines — and owns no shards and no store,
so every place the protocol has to touch this node's data is an injected seam. The
implementations of those seams used to sit in the 134K-LOC server crate, reachable only
through live two-node integration tests. They now live in `frogdb-replication-runtime`, and
there are exactly four. `ReplicaCommandExecutor` implements the `ReplicaApplier` trait: it
routes a replicated command group to the origin shard the *primary tagged the frame with*
(not a shard re-derived from `args[0]`), executes it under `REPLICA_INTERNAL_CONN_ID` so it is
not re-broadcast, applies a reconstructed `MULTI … EXEC` group atomically through
`CoreMsg::ExecTransaction`, and checks the answer — an error `Response` or a non-`Success`
`TransactionResult` becomes `ApplyError::Rejected` rather than being dropped on the floor.
`live_snapshot_source` builds the `LiveSnapshotSource` a primary with
`persistence.enabled = false` needs, serializing each shard's live keyspace in shard order
because there is no RocksDB to checkpoint. `LiveSnapshotInstaller` implements
`SnapshotInstaller` for both payload shapes a full resync can arrive in —
`FullSyncPayload::StagedCheckpoint`, scanned out of the staged DB, and
`FullSyncPayload::LiveDataset`, decoded and re-routed through `shard_for_key` so the two
nodes' shard counts need not agree — pushing entries into the shards rather than swapping a
RocksDB handle, because the live keyspace is the shards' in-memory store and a handle swap
would leave it untouched. `ReplicationQuorumChecker` is the replica-loss write fence, serving
both `QuorumChecker` (the write gate's verdict) and `WriteFenceReporter` (the reason string
that makes a `CLUSTERDOWN` attributable).

Unlike `frogdb_txn` (ADR 0002), none of this needs a host trait, and the reason is legible in
what the two seams actually ask their host for. `TxnHost` has twelve methods and exactly one
of them, `send_shard_transaction`, is a shard round-trip; the other eleven — `deferral_of`
against the command registry, `try_acquire_batch` against the authenticated user's rate
limiter, `validate_queued_batch` and `watched_slots_still_local` against the cluster
`SlotValidator`, `wait_if_paused` against the pause barrier, `run_connection_level` and
`run_server_wide` to execute the deferred commands back where they belong, plus the
connection's own id and shard — reach into per-connection and process-wide state that has no
message form at all, so EXEC could not be expressed as "send a value and await a reply" even
in principle. The replication runtime is the case that can be: its entire data surface is
`Arc<Vec<ShardSender>>`, and every operation it performs is one plain-data enum value carrying
its own `oneshot` reply channel —
`CoreMsg::Execute`, `CoreMsg::ExecTransaction`, `ReplicationMsg::ExportSnapshot`,
`ReplicationMsg::InstallSnapshot`, and nothing else. Nothing here reads `Server` or
`Subsystems`, nothing holds a connection, nothing spawns into the server's `net`/`cfg(turmoil)`
abstractions. That is the same shape `frogdb_recovery` has (ADR 0003), arrived at from the
other direction: recovery needs no trait because it returns data, this crate needs none
because it *sends* data. The two departures from pure message passing are worth naming rather
than glossing: `LiveSnapshotInstaller::for_config` reads `Config` once at construction (plain
data, not a callback), and `ReplicationQuorumChecker` holds an `Arc<ReplicationTrackerImpl>`
— a shared registry, still a `frogdb-core` type, still not a reach back into the server.

What stayed behind stayed for one reason: it is coupled to a live connection. The
`REPLICAOF`/`SLAVEOF` handler mutates the connection's own role view and drives the
`RoleManager` through `CommandContext`; `REPLCONF` is a handshake exchange whose state belongs
to the replica connection that sent it; and `PSYNC` never reaches a shard at all —
`DispatchStage::ReplicationHandshake` parses it into a `PsyncHandoff` and the connection task hands
the raw socket to the `PrimaryReplicationHandler`, so the registered `Command` executor exists
only to carry arity/flags and returns an internal error if it is ever actually run. Boot
wiring (`server::replication_init`) and `role_manager` stay for the same reason: they build
and re-point live components. `crate::replication` is now a facade that re-exports both
crates, so server call sites did not move.

Consequences: the four seams are unit-testable in a crate that builds without the server test
binary, and the failure-mode contract is `specs/replication.md`,
which now carries FM-REPLICATION-001..053 and names these files in its own scope statement —
including three rows (051-053) written directly against these seams, because the runtime crate's
own tests reached almost none of them.
The measured baseline is honest about how much of that is verified: `frogdb-replication`
scored 533 caught / 181 missed / 1 timeout / 84 unviable — 74.7% on viable mutants — and that
number is a floor rather than a measurement, because `cargo mutants -p frogdb-replication`
builds and runs only that package's tests. The forcing tests for most of these behaviors live
in `frogdb-server/crates/server/tests/integration_replication.rs`, which never runs against a
mutant, so a survivor there may well be killed by a test that was not given the chance to run;
raising the real score means moving forcing tests down into the crates, not tuning the gate.
The runtime crate itself is the sharper end of the same problem: it scored 28 caught / 28
missed / 6 unviable — 50.0% — because only `quorum.rs` carries tests today, and the
executor/export/install seams are exercised exclusively from the server's integration suite.
Every whole-function survivor there (`apply_single`, `apply_transaction`, `apply_group`,
`export_live_dataset`, `install`, `read_snapshot`) is a seam whose only caller is a live
two-node test. The other cost is that two of the four seams are type-erased closures
(`LiveSnapshotSource`, `SnapshotInstaller`) held as `Option` and set post-construction, so a
wiring site that forgets one gets a runtime warning, not a compile error —
`LiveSnapshotInstaller::for_config` exists as the single construction site for both wiring
points (boot-configured replica and runtime `REPLICAOF` demotion) precisely so those two
cannot drift, and the unwired paths degrade loudly: a staged checkpoint warns that the node
keeps its old keyspace until reboot, and a live dataset, which has no on-disk fallback, fails
the sync rather than let the offset advance over a keyspace that never adopted the snapshot.
