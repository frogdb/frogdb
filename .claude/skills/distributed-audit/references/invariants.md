# FrogDB Invariants

Each invariant includes: formal statement, why it matters, enforcement mechanism,
key code paths, verification tests, and edge cases.

For the definitive source on consistency guarantees, see `website/src/content/docs/architecture/consistency.md`.

---

## Invariant 1: Per-Shard Linearizability

**Statement:** Within a single internal shard, all operations on a single key are totally
ordered and linearizable — each operation appears to take effect at some instant between
its invocation and completion.

**Why it matters:** This is the foundation of FrogDB's correctness model. Clients expect
that after a successful write, all subsequent reads on the same connection return the
written value. Violation means clients see stale or inconsistent data.

**Enforcement:**
- Each shard worker is a single Tokio task processing commands sequentially from its
  `mpsc::Receiver`
- No data races: shard data is accessed by exactly one async task
- Commands execute one at a time within each shard (no interleaving)

**Key code paths:**
- `crates/core/src/shard/` — shard worker event loop
- `crates/core/src/store/` — data access

**Verification:**
- `just jepsen register` — Knossos linearizability check on single key
- `just concurrency` — Shuttle tests for shard worker

**Edge cases:**
- Blocking commands (BLPOP) suspend but don't interleave — they yield the shard worker
  and resume in order
- Lua scripts execute atomically within a shard
- `.await` points within command execution must not allow interleaving of other commands
  on the same key

**How to violate:**
- Introduce shared mutable state accessible from multiple shard workers
- Allow multiple concurrent command executions within the same shard
- Hold a reference across an `.await` that allows another command to observe partial state

---

## Invariant 2: Cross-Shard Atomicity (VLL Sorted Lock Order)

**Statement:** Multi-shard operations acquire locks in sorted shard-ID order, preventing
deadlocks. All participating shards execute under their locks before any lock is released.

**Why it matters:** Without ordered locking, two concurrent multi-shard operations could
deadlock (A holds shard 0, wants shard 1; B holds shard 1, wants shard 0). VLL guarantees
progress.

**Enforcement:**
- Global atomic `NEXT_TXID` counter provides total ordering
- Per-shard `BTreeMap<u64, PendingOp>` queues operations by txid
- Lock acquisition in sorted shard order prevents circular waits
- Shards execute in lock-step: all acquire → all execute → all release

**Key code paths:**
- `crates/vll/src/` — VLL lock coordination
- `crates/core/src/shard/` — VllLockRequest, VllExecute message handling
- `crates/server/src/connection/` — scatter-gather coordination

**Verification:**
- `just concurrency` — Shuttle tests for VLL
- `just jepsen cross-slot` — Cross-slot hash tag transactions

**Edge cases:**
- Timeout during lock acquisition: all acquired locks must be released
- Coordinator crash mid-operation: locks expire after `scatter_gather_timeout_ms`
- VLL provides execution atomicity but NOT failure atomicity (no rollback on partial failure)

**How to violate:**
- Acquire locks in a different order than sorted shard ID
- Release locks before all shards have executed
- Introduce a new lock type that doesn't participate in the ordering
- Skip VLL for a multi-shard operation

---

## Invariant 3: WAL Sequence Ordering

**Statement:** WAL entries have monotonically increasing sequence numbers. Every write
operation that modifies data produces a WAL entry with a sequence number strictly greater
than all previous entries.

**Why it matters:** WAL sequence numbers are the foundation for:
1. Recovery: replay from a known sequence point
2. Replication: replicas track position by sequence number
3. PSYNC: partial sync requires contiguous sequence history

**Enforcement:**
- RocksDB's internal sequence numbering (monotonically increasing per WriteBatch)
- Single shared WAL across all column families (shards)
- WriteBatch provides atomic multi-key writes within a shard

**Key code paths:**
- `crates/persistence/src/wal.rs` — WAL append
- `crates/persistence/src/rocks.rs` — RocksDB WriteBatch
- `crates/replication/src/primary.rs` — WAL streaming uses `GetUpdatesSince()`

**Verification:**
- `just jepsen append` — Append-only durability
- `just jepsen append-crash` — Append durability through crashes
- Integration tests in `crates/persistence/`

**Edge cases:**
- Concurrent WriteBatch from different shards: RocksDB serializes internally
- WAL corruption recovery: truncation at corruption point may lose recent entries
- Sequence gap after recovery with `wal_corruption_policy = "truncate"`: gap is bounded
  by `wal_max_sequence_gap`

**How to violate:**
- Write to RocksDB outside of the normal WAL path (bypassing sequence numbering)
- Use multiple RocksDB instances (each has independent sequence space)
- Manipulate sequence numbers manually

---

## Invariant 4: Replication Consistency

**Statement:** WAL streaming to replicas preserves the operation order from the primary.
Replicas apply operations in the same sequence order as the primary.

**Why it matters:** If replicas apply operations out of order, they diverge from the primary.
This breaks read-your-writes for clients that fail over to a replica, and violates the
bounded-staleness guarantee.

**Enforcement:**
- Primary streams WAL entries via `GetUpdatesSince()` in sequence order
- Replication frames include sequence numbers for validation
- Replica validates frame sequence continuity (detects gaps)
- CRC32 checksums detect corruption in transit

**Key code paths:**
- `crates/replication/src/primary.rs` — WAL streaming
- `crates/replication/src/replica.rs` — frame application
- `crates/replication/src/frame.rs` — frame protocol with checksums

**Verification:**
- `just jepsen replication` — 3-node replication consistency
- `just jepsen split-brain` — Behavior under partition
- `just jepsen replication-chaos` — Combined replication faults

**Edge cases:**
- TCP reordering: TCP guarantees in-order delivery, so this is handled by the transport
- Sequence gap on replica: triggers PSYNC retry or FULLRESYNC
- Secondary replication ID after failover: enables PSYNC continuity from old primary's replicas
- FULLRESYNC integrity: SHA256 checksum over all checkpoint files

**How to violate:**
- Stream WAL entries out of sequence order
- Skip entries during streaming (gap in sequence)
- Apply frames on replica without validating sequence continuity
- Corrupt replication ID or sequence tracking during promotion

---

## Invariant 5: Failover Correctness (Epoch Monotonicity + Fencing)

**Statement:** Topology epochs are monotonically increasing. Nodes reject operations
associated with stale epochs. After failover, at most one primary accepts writes for
any given slot range.

**Why it matters:** Without epoch fencing, a stale primary could accept writes after
a new primary has been promoted, causing divergent histories that cannot be reconciled.

**Enforcement:**
- Orchestrator increments epoch on every topology change
- FROGDB.PROMOTE includes epoch; node rejects if local epoch is higher
- FROGDB.TOPOLOGY broadcast to all nodes; nodes update on higher epoch
- Self-fencing: primary self-demotes after `self_fence_timeout_ms` without orchestrator contact
- Demotion logs divergent writes to `data/split_brain_discarded.log`

**Key code paths:**
- `crates/cluster/src/state.rs` — epoch tracking, state transitions
- `crates/cluster/src/` — Raft consensus, topology management
- `crates/server/src/commands/cluster/` — CLUSTER commands

**Verification:**
- `just jepsen leader-election` — Raft leader election
- `just jepsen leader-election-partition` — Leader election under partitions
- `just jepsen split-brain` — Split-brain detection

**Edge cases:**
- Split-brain window: up to `fencing_timeout_ms` (default 10s) where both old and new
  primary may accept writes
- Network reordering: topology update may arrive before ROLE_CHANGED ack;
  node queues topology until promotion completes
- Multi-orchestrator: nodes validate epoch to prevent conflicting decisions
- Self-fence during temporary network blip: may cause unnecessary failover

**How to violate:**
- Compare epochs with `<` instead of `<=` (stale epoch accepted)
- Apply topology update before promotion completes (writes before ready)
- Skip epoch validation on any write path
- Allow writes during self-fence period

---

## Invariant 6: Recovery Consistency

**Statement:** After crash recovery (WAL replay + snapshot load), the database state is
equivalent to some prefix of the pre-crash operation sequence. No partial operations
are visible.

**Why it matters:** If recovery produces a state that never existed during normal operation,
clients may observe impossible values after restart.

**Enforcement:**
- RocksDB atomic WriteBatch: either all keys in a batch are persisted or none
- Snapshot written to temp file, atomically renamed on completion
- Partial snapshots ignored on recovery (use previous complete snapshot + WAL)
- WAL corruption recovery: truncate at corruption point, all preceding entries valid

**Key code paths:**
- `crates/persistence/src/rocks.rs` — RocksDB recovery
- `crates/persistence/src/snapshot.rs` — snapshot creation/loading
- `crates/persistence/src/wal.rs` — WAL replay
- `website/src/content/docs/operations/lifecycle.md` — recovery sequence

**Verification:**
- `just jepsen crash` — Linearizability through SIGKILL
- `just jepsen counter-crash` — Counter correctness through crashes
- `just jepsen append-crash` — Append durability through crashes

**Edge cases:**
- Mid-snapshot crash: partial snapshot file ignored, use previous snapshot
- WAL corruption mid-file: truncated at corruption point per `wal_corruption_policy`
- In-memory write applied but WAL failed (async/periodic mode): write lost on crash,
  this is expected behavior matching Redis
- LRU metadata not persisted: all keys appear "fresh" after recovery (by design)
- Expiry index rebuilt from persisted `expires_at` fields during recovery

**How to violate:**
- Write to RocksDB without using WriteBatch (partial multi-key update possible)
- Rename snapshot without completing write (corrupt snapshot)
- Skip WAL corruption validation during replay

---

## Invariant 7: Shared-Nothing (No Shared Mutable State Between Shards)

**Statement:** Shard workers do not share mutable state. All cross-shard communication
happens via message passing through bounded channels.

**Why it matters:** This is the foundation of FrogDB's thread-per-core architecture.
Shared mutable state would require locks, causing contention and eliminating the
performance benefits of the shared-nothing model.

**Enforcement:**
- Each shard worker owns its data exclusively
- `mpsc::channel` for inter-shard communication
- Global atomic `NEXT_TXID` is the only shared mutable state (intentional, lock-free)
- Connection state is per-connection, not shared

**Key code paths:**
- `crates/core/src/shard/` — shard worker, message handling
- `crates/server/src/connection/` — connection handler
- `crates/vll/src/` — VLL uses message passing, not shared state

**Verification:**
- `just concurrency` — Shuttle tests verify no data races
- Code review: search for `Arc<Mutex<...>>` or `Arc<RwLock<...>>` in data paths

**Edge cases:**
- Metrics/telemetry: may use atomic counters (read-only from shard perspective)
- Configuration: read-only after startup
- Global txid counter: `AtomicU64` with `SeqCst` ordering (intentionally shared)

**How to violate:**
- Add `Arc<Mutex<...>>` to data paths
- Share a mutable reference between shard workers
- Access another shard's data directly (bypass message passing)
- Use `static mut` or thread-local state that's accessed from multiple tasks

---

## Invariant 8: Pub/Sub Routing (Broadcast via Shard 0)

**Statement:** Broadcast pub/sub (SUBSCRIBE/PUBLISH) is coordinated through shard 0 only.
Sharded pub/sub (SSUBSCRIBE/SPUBLISH) uses hash-based routing.

**Why it matters:** Without single-coordinator routing, broadcast pub/sub fans out to all
shards, causing subscribers to receive N× messages (N = num_shards) and PUBLISH to return
N × subscriber_count instead of subscriber_count.

**Enforcement:**
- `handle_subscribe`, `handle_unsubscribe`, `handle_publish` route through shard 0
- `handle_ssubscribe`, `handle_spublish` use channel name hash for routing
- `handle_psubscribe`, `handle_punsubscribe` route through shard 0

**Key code paths:**
- `crates/server/src/connection/handlers/pubsub.rs` — pub/sub command handlers
- `crates/core/src/pubsub/` — pub/sub engine

**Verification:**
- Integration tests in `crates/server/tests/`
- Manual verification: PUBLISH returns subscriber count (not N× subscriber count)

**Edge cases:**
- Pattern subscriptions (PSUBSCRIBE): must also route via shard 0
- Sharded pub/sub with hash tags: uses channel name hash, not key hash

**How to violate:**
- Route broadcast pub/sub to all shards instead of shard 0
- Mix broadcast and sharded pub/sub routing logic

---

## Invariant 9: Channel Backpressure (Bounded Channels)

**Statement:** All inter-shard message channels are bounded. When a channel fills,
the sender blocks (async await), creating natural backpressure from overloaded shards
to clients.

**Why it matters:** Unbounded channels can cause OOM under load spikes. Bounded channels
ensure that a slow shard causes its clients to slow down rather than buffering indefinitely.

**Enforcement:**
- `mpsc::channel::<ShardMessage>(SHARD_CHANNEL_CAPACITY)` (default 1024)
- VLL queue has `vll_max_queue_depth` (default 10000)
- Backpressure propagates: channel full → sender blocks → connection stops reading →
  TCP buffer fills → client experiences latency

**Key code paths:**
- `crates/core/src/shard/` — channel creation
- `crates/server/src/connection/` — message sending

**Verification:**
- Load tests with `/benchmark` skill
- Monitor `channel_backpressure_events_total` metric

**Edge cases:**
- Deadlock risk: if shard A sends to shard B and B sends to A, and both channels are full,
  both block forever. VLL's sorted lock order prevents this for VLL operations.
- Non-VLL cross-shard messages (e.g., snapshot requests): must not create circular waits

**How to violate:**
- Use unbounded channels
- Increase channel capacity without understanding backpressure implications
- Create circular cross-shard message patterns outside VLL ordering

---

## Invariant 10: Connection Ordering (Pipeline Preserves Order)

**Statement:** For a single client connection, responses are returned in the same order
as requests, including when pipelining.

**Why it matters:** Redis protocol clients depend on response ordering to match responses
to requests. Violating this causes clients to misinterpret responses.

**Enforcement:**
- Connection handler processes commands sequentially per connection
- Pipeline commands executed in order, responses buffered and sent in order
- MULTI/EXEC: all commands queued, executed atomically, responses returned together

**Key code paths:**
- `crates/server/src/connection.rs` — connection event loop
- `crates/server/src/connection/dispatch.rs` — command dispatch
- `crates/server/src/connection/state.rs` — connection state (pipeline, transaction)

**Verification:**
- Integration tests with pipelined commands
- `just jepsen register` — register operations implicitly test ordering

**Edge cases:**
- Scatter-gather commands: coordinator must wait for all shard responses before returning
- Blocking commands: response arrives when data is available, but ordering within the
  connection is maintained
- PUB/SUB mode: push messages interleave with command responses (by protocol design)

**How to violate:**
- Process pipeline commands concurrently instead of sequentially
- Return scatter-gather partial results before all shards respond
- Send push messages (pub/sub) that break response ordering for pending commands
