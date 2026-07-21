# Spec: architecture/request-flows.md
Status: update
Audiences: A2, A3, A5

Goal: A reader walks away with an accurate mental model of how a request moves
between FrogDB's major runtime components for each command shape — single-key,
scatter-gather, blocking, pub/sub, transaction, replication, and cluster
consensus — and understands *why* the boundaries fall where they do:
ConnectionHandler as a per-connection coordinator, ShardWorker as the sole owner
of a keyspace partition reached only by message-passing, ScatterGatherExecutor
driving the VLL lock/ready/execute handshake for multi-shard atomicity, and the
split between the data plane (PSYNC-style replication stream) and the metadata
plane (Raft). The channel-summary table lets the reader see, at a glance, every
inter-component channel and the message type it carries.

Not in scope: The mechanics behind each diagram — VLL internals (vll.md),
concurrency/backpressure model (concurrency.md), blocking wait-queue details
(blocking.md), persistence path (persistence.md). This page is the diagram
index; deeper pages own the rationale.

Sources of truth (author must open each and confirm the diagram matches):
- `frogdb-server/crates/server/src/acceptor.rs` — `Acceptor`, per-connection
  `tokio::spawn`.
- `frogdb-server/crates/server/src/connection.rs` — `ConnectionHandler`.
- `frogdb-server/crates/core/src/shard/` — `ShardWorker` (`worker.rs`,
  `event_loop.rs`), `ShardMessage` (`message.rs`), scatter op types
  (`types.rs`). NOTE: the module is the directory `core/src/shard/`, not a file
  `core/src/shard.rs`.
- `frogdb-server/crates/server/src/scatter/executor.rs` — `ScatterGatherExecutor`.
- `frogdb-server/crates/vll/src/` — `IntentTable` (`intent_table.rs`),
  transaction queue (`queue.rs`), coordinator (`coordinator.rs`),
  `ExecuteSignal`/ready types (`types.rs`). NOTE: VLL is its own crate
  (`crates/vll/`), not `core/src/vll/`.
- `frogdb-server/crates/core/src/pubsub.rs` — `PubSubMessage`,
  `ShardSubscriptions`.
- `frogdb-server/crates/server/src/replication/` — `PrimaryReplicationHandler`,
  `ReplicaReplicationHandler`, `ReplicaCommandExecutor`, replication frames.
- `frogdb-server/crates/cluster/src/` — `ClusterState`, `ClusterStateMachine`,
  cluster commands (`commands.rs`, `state.rs`, `types.rs`). NOTE: cluster is its
  own crate (`crates/cluster/`), not `core/src/cluster/`. Raft wiring
  (`ClusterRaft`) lives in the server; confirm its real location.

Existing content: `website/src/content/docs/architecture/request-flows.md`.
Eight sequence/flow diagrams plus a "Key source files" list and a channel
summary. Structure is good; correctness of the file paths and diagrams must be
re-verified.

Structure (keep the existing 8 numbered sections):
- Header "Key source files" list — must be corrected (see Drift guards) and
  becomes the primary S7 check surface for this page.
- 1. System Architecture Overview (flowchart) + Channel Summary table.
- 2. Single-Key Command sequence — verify routing note
  `CRC16(key) mod 16384 -> slot`, then `slot mod num_shards -> shard_id` against
  the actual `shard_for_key` logic; reconcile with concurrency.md, which
  describes internal routing as `xxhash64 % num_shards`. These two descriptions
  must be made consistent (see Drift guards).
- 3. Scatter-Gather (VLL lock/ready/execute/merge phases) — verify message names
  (`VllLockRequest`, `VllExecute`, `ShardReadyResult`, `PartialResult`,
  `ExecuteSignal`) against `shard/message.rs` and the vll crate.
- 4. Blocking Command — verify `BlockWait`, `UnregisterWait`, `BlockingNeeded`
  against `shard/message.rs` and `shard/wait_queue.rs`.
- 5. Pub/Sub — verify `Subscribe`/`Publish` message shapes and `PubSubMessage`
  channel type.
- 6. Transaction (MULTI/EXEC) — verify `ExecTransaction`, `TransactionResult`.
- 7. Replication — verify broadcaster/handler/executor chain.
- 8. Cluster Consensus (Raft) — verify `RaftNeeded`, `ClusterCommand`,
  read-only path bypassing Raft.

Generated data: None. The "Key source files" list is a hand-maintained index;
S7 turns it into a checked artifact.

Drift guards:
- **S7 code-path check (primary for this page).** Every path in the "Key source
  files" list and every path referenced in prose must exist. Current list has
  broken paths that S7 must catch and the author must fix:
  - `crates/core/src/shard.rs` → should be `crates/core/src/shard/` (directory).
  - `crates/core/src/vll/` → VLL is a separate crate: `crates/vll/`.
  - `crates/core/src/cluster/` → cluster is a separate crate: `crates/cluster/`
    (and confirm where `ClusterRaft` actually lives).
  Re-point every entry to a path that resolves in the current tree.
- **Re-verify all 8 diagrams** against the sources above. Message/variant names,
  channel kinds (mpsc vs oneshot vs broadcast vs unbounded), and the direction
  of each arrow must match the code. Do not carry forward a variant name that no
  longer exists in `ShardMessage`.
- **Routing description consistency.** Section 2 says internal shard =
  `slot mod num_shards` (derived from the CRC16 slot); concurrency.md says
  internal shard = `xxhash64(hash_tag) % num_shards`. Determine which the code
  actually does (`shard_for_key`) and make both pages state the same thing.
  Flag this explicitly for the author — it is a substantive inconsistency, not a
  wording nit.
- **No unbacked numbers.** No latency/throughput figures; timeouts shown must
  match real config defaults (cross-check with concurrency.md / config crate).
