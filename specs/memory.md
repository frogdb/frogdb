# Memory — failure modes

Status: DRAFT — rows land as the behavior they describe lands. This file is the scope
statement and the vocabulary the rows are written in, published ahead of them so the memory
work has one place to be spec-first *from*. It becomes the fifth locked area under
[`.scratch/memory-architecture/PRD.md`](../.scratch/memory-architecture/PRD.md) R15 —
`Status: LOCKED`, behavior changes spec-first — once the broker and table crates exist and
pass their mutation gate. The discipline holds from the first row on: **a row arrives with
the test that forces it, in the same commit**, or it is a wish rather than a contract.
`just lint-spec` checks both directions for every row present, so the file is never ahead
of the code.

## Scope

The memory contract owns **where bytes come from, who is charged for them, and what
happens when there are no more** — everywhere except the value encodings themselves.
Concretely, once the architecture lands:

* **The allocation substrate.** One jemalloc arena per shard thread, bound once at thread
  start — and a bind the allocator refuses is **fatal at boot**, not a shard that runs
  unattributed: once the broker takes `maxmemory` verdicts against the arena figure, a shard
  without one decides with nothing measuring its core, and there is no configuration that
  lets it serve anyway. A shard legitimately has no arena only where there are no arenas to
  bind at all — the simulation seam, or a build whose allocator has none — which is a
  configuration rather than a failure. Also here: the shard-placement seam that decides
  whether a shard is an OS thread or a task on
  the simulation's one thread; the `mallctl` reads that turn allocator state into numbers
  (`frogdb-server/crates/telemetry/src/jemalloc.rs`, today the single process-wide chokepoint
  for `INFO memory`, `MEMORY PURGE`, and the `frogdb_allocator_*` gauges — the per-arena
  reads extend it rather than growing a second caller).
* **The broker and its budgets.** The per-core memory broker, the `Budget` handle every
  non-keyspace subsystem holds, and the charge/release calls at each subsystem's growth
  seam: network output buffering, the replication backlog ring, the client tracking table,
  the WAL channel, full-sync staging, and transaction buffering.
* **The refusal and reclamation verdicts.** What `maxmemory` is measured against, which
  commands are refused when it is exceeded, what eviction selects and what it guarantees
  about what it did not select, and what a subsystem does at its own ceiling when eviction is
  not the answer.
* **The snapshot handle contract.** The copy-on-write guarantee that lets a full sync or a
  background export read a consistent keyspace out of a running shard in bounded memory.

Out of scope, owned elsewhere: the *layout* of a value (the keyspace table and its
encodings are a correctness contract of their own, not a memory one, and get their own
spec when they land); durability ordering ([persistence.md](persistence.md)); what a
replication link puts on the wire ([replication.md](replication.md)); slot ownership
([cluster.md](cluster.md)). Rows here stop at what an operator or a client can observe:
a refusal, an eviction, a shed connection, a metric, or a process that stayed inside its
configured ceiling.

## Invariant vocabulary

These are the nouns every future row uses. They are defined once here so no row has to
re-explain them and no two rows can mean different things by the same word.

| Term | Meaning |
|---|---|
| **Budget** | A handle, held by exactly one subsystem on exactly one core, carrying a limit and a current charge. It is the only thing that can authorize growth of a non-keyspace buffer. A `Budget` is not a counter a subsystem updates after the fact — the charge is the permission, taken *before* the bytes exist. |
| **broker** | The per-core owner of every `Budget` on that core, and the one component that reads allocator truth for the core. It arbitrates between subsystems (whose limits may sum to more than the core's share), answers the `maxmemory` question, and drives eviction. One broker per core; brokers do not share state, consistent with the shared-nothing topology (PRD R3). |
| **arena** | A jemalloc arena, one per shard thread, bound once at thread start via `arenas.create` + `thread.arena`. The arena is the *attribution* mechanism: bytes allocated by a shard thread are that shard's bytes, and no other shard's arena reports them. It is not an isolation boundary the code can rely on for safety — nothing stops a pointer crossing threads — which is why the no-cross-core-refcount rule (PRD R3) is a separate invariant rather than a consequence of arenas. |
| **charge** | The act of asking a `Budget` for `n` bytes and being told yes or no, before allocating them. A charge that succeeds is held until the matching release; a charge that fails is a refusal the caller must handle at that seam — never a charge taken anyway with a warning logged. |
| **shed** | The response to a failed charge when the growth belongs to a client the server may drop: the connection is closed, or its buffered output is discarded, and the server keeps running. Redis's `client-output-buffer-limit` classes are shed policy. Shedding is *observable* — it produces a metric and a log line naming the class — because a silently shed client is indistinguishable from a network fault. |
| **backpressure** | The response to a failed charge when the producer can be made to wait instead: the write is not accepted, the reader is not polled, the channel blocks. Backpressure preserves the work; shedding discards it. Every buffer must state which of the two it does, and no buffer may do neither. |
| **snapshot handle** | A reference to a value taken for reading while writers continue. Taking one is pointer work, not a copy; a subsequent write to that value copies (copy-on-write) rather than mutating what the handle sees. A snapshot handle bounds an export's memory to the entries it has not yet streamed plus whatever writers copied during the window — it is the in-process substitute for Redis's fork (PRD R6). |
| **ceiling** vs **budget** | A **ceiling** is a constant bound on one quantity, checked at one site, that exists to make an absurd value fail cleanly — a wire-supplied length that cannot be a real dataset. A **budget** is a live allowance derived from what this node actually has, shared between competing consumers, that can refuse a perfectly reasonable request because the node is full. A ceiling defends against a lie; a budget defends against the truth. The two are not substitutes, and a row must not claim a ceiling gives it a budget's guarantee. |

## Planned failure-mode groups

The rows will arrive in these groups. Each paragraph states the contract the group's rows
must make forceable — it is not itself a contract, and nothing below may be cited as one.

### Every buffer is bounded and budget-charged

The load-bearing claim of the whole area: **a structure that cannot charge cannot grow.**
For each non-keyspace buffer in the server, a row will name its `Budget`, its limit's
configuration key, and its behavior at the limit — shed or backpressure, never neither and
never both. The rows will have to be written against a seam the code actually funnels
through, because the invariant is only worth as much as its chokepoint: the plan is a seam
lint in the family described in [`agents/seam-lints.md`](../agents/seam-lints.md), pinning
that buffer growth goes through a `Budget` handle, landing with a ratcheted allowlist of
the buffers not yet converted. Two known members of this group today are the client
tracking table's insertion order list, which grows outside accounting entirely
([issue 66](../.scratch/testing-improvements-round2/issues/)), and the replica-feed hold
buffer, whose byte cap [cluster.md TR-CLUSTER-016](cluster.md#tr-cluster-016--replica-feed-hold-during-an-armed-slot-barrier)
rules but the code does not have.

### OOM verdicts

What `maxmemory` is measured against, and what a client sees when it is exceeded. Arena
statistics become the measurement, which makes the verdict *sampled*: allocator truth costs
roughly 2.5 µs per arena per epoch advance and overstates live bytes by whatever is parked
in a thread cache, so it is read at a low frequency and rides an exact per-thread counter in
between (see [spike-report.md](../.scratch/memory-architecture/spike-report.md) §(a) E4/E5).
Rows in this group must therefore be written against what a *sampled upper bound* can
promise — a refusal is eventually correct, and the bound direction is the safe one — rather
than against a per-command exact reading nothing can implement. The refusal itself is not a
new seam: command admission already has exactly one chokepoint that reads the
`DENYOOM` flag (`lint-command-admission`), and these rows attach to it.

### Eviction invariants

What eviction selects, what it guarantees about what it did not select, and what it
guarantees to the caller that triggered it. The interesting rows are the negative ones —
eviction never evicts a key a running command is holding, never evicts from a shard other
than the one over its budget, always makes progress or reports that it cannot rather than
spinning, and reports the same policy names Redis does even though the machinery underneath
is not Redis's sampling loop. Tiered spill, where a victim moves rather than dies, changes
the postcondition of every row in this group and so is rowed as its own variant rather than
smuggled in as a configuration detail.

### Snapshot-COW guarantees

What a reader holding a snapshot handle sees, for how long, and at what cost to the writers
who continue against the same shard. The rows must pin: the snapshot is a point in time and
never observes a write that landed after it was taken; a writer during the window is never
blocked by the reader, it copies; the export's peak memory is a function of what it has not
yet streamed, not of the dataset's size; and dropping the handle releases everything the
copies were protecting. This is the group that makes today's full-sync double
materialization a specified impossibility rather than a known cost.

### Transaction cap semantics

Transaction buffering — queued `MULTI` commands, the staged write batch, replica-side
pending groups — charges a `Budget` with a hard cap, and a transaction that would exceed it
aborts **before** `EXEC` applies anything (PRD R14). The rows own the boundary condition
that makes this safe: atomicity is preserved because the refusal happens on the way in, so
there is no partial application to undo, and the error is a distinct one the client can
tell apart from a failed command. This is a documented deviation from Redis, which has no
such limit, and it replaces the open-ended non-guarantee
[persistence.md FM-PERSISTENCE-001](persistence.md#fm-persistence-001--a-shards-write-batch-is-never-torn-across-storage-batches)
currently records for un-size-capped write groups.

## Failure modes

The rows that have landed. Each one arrived with the tests that force it, in the same
commit, per the DRAFT discipline at the top of this file.

## FM-MEMORY-001 — a connection past its `client-output-buffer-limit` class is disconnected and its buffered output released

| Field | Value |
|---|---|
| Trigger | A connection whose queued reply bytes reach the hard limit of its class, or stay at or above the soft limit continuously for `soft-seconds`. The class is the connection's role at the moment of the judgement: `replica` if it has announced a replica listening port, else `pubsub` if it holds a subscription channel, else `normal`. Limits come from `server.client-output-buffer-limit`, spelled as Redis spells it (`<class> <hard> <soft> <soft-seconds>`, repeated per class, `slave` an alias for `replica`, Redis's size suffixes and Redis's defaults `normal 0 0 0`, `replica 256mb 64mb 60`, `pubsub 32mb 8mb 60`; `0` disables a limit). |
| Observable | The connection is torn down rather than served — the client sees a close or a reset, never the reply that exceeded the allowance. Its buffered bytes are discarded and released from `Subsystem::NetworkOutput` before the error propagates, so a shed connection costs the shard nothing after the verdict. `frogdb_client_output_buffer_disconnects_total{class,reason}` counts it, with `reason` distinguishing `hard_limit`, `soft_limit` and `budget_refused`, and a `warn` log names class, reason and byte count. A connection whose output stays inside its class's allowance is served in full. |
| NOT observable | A limit applied to a class the operator did not configure — an unnamed class keeps its Redis default rather than inheriting another class's number. A malformed `client-output-buffer-limit` silently replaced by the default: the parse refuses the boot, because a limit the operator believes they set and does not have is worse than none. A shed connection whose bytes stay charged (the pre-fix behavior wrote the condemned buffer out on the way to the error). Also not observable: the soft window forced end to end — the reply path applies backpressure at the codec's boundary rather than accumulating, and a connection whose client has stopped reading is parked inside its write, so the window is forced at the housekeeping-tick seam that judges an idle connection rather than over a socket. **Also not observable: the `replica` class applied to a replication feed.** A connection is judged against `replica`'s limits only while it is still a client connection; once `PSYNC` hands the socket to the replication feed the connection's charge is dropped and the feed's own buffering is charged nowhere and judged by nothing. This is an accounting gap in this release, not a deferred enforcement decision: the class parses, the limits are stored and a pre-handoff replica connection is judged, but feed bytes are invisible to both the class limit and the `NetworkOutput` budget. Closing it means charging inside the replication crates, which is spec-first work under `specs/replication.md` and is filed separately. |
| Invariant | One seam decides. `OutputBufferAccount::judge` is the only place a class limit is compared against buffered bytes, `frame_io::account_buffered_output` is the only caller that measures them (codec write buffer + `resp3_buf` + the pub/sub delivery queue, both protocol versions), reached from the write path and from the connection's one-second housekeeping tick so an idle connection is still judged, and `frame_io::shed_output` is the only exit — it clears both buffers, releases the charge, emits the metric and returns the error. The pub/sub overflow path was migrated onto that exit rather than keeping its own disconnect. `OutputVerdict` is `#[must_use]`, so a caller cannot take a verdict and ignore it. |
| Outcome variant | `OutputVerdict::{Keep, Shed(ShedReason)}` / `ShedReason::{HardLimit, SoftLimit, BudgetRefused}` |
| Forced by | `the_hard_limit_sheds_at_once`, `the_soft_limit_sheds_only_after_the_window`, `dropping_under_the_soft_limit_restarts_the_window`, `changing_class_restarts_the_soft_window_and_switches_the_limit`, `test_normal_class_hard_limit_disconnects_an_oversized_reply`, `test_a_pipeline_within_the_limit_is_served_in_full`, `test_slow_subscriber_output_buffer_bound_disconnects`, `the_idle_tick_sheds_a_connection_that_stays_above_the_soft_mark`, `test_a_malformed_limit_spec_is_a_configuration_error` |
| Bug refs | — |

## FM-MEMORY-002 — buffered reply bytes are charged to a per-core `NetworkOutput` budget and released when they drain

| Field | Value |
|---|---|
| Trigger | Any reply buffered for a client, on either protocol version, and any drain of that buffer. |
| Observable | The shard broker publishes a `NetworkOutput` row: `frogdb_memory_budget_limit_bytes{shard,subsystem="network_output"}` carries the per-core ceiling, and the per-subsystem breakdown carries what is currently held. Charged bytes track the buffer exactly — a connection's charge is the delta against what it already held, and a drain releases every byte it was holding. A budget that refuses a charge sheds the connection with `ShedReason::BudgetRefused` (FM-MEMORY-001's exit), rather than the bytes being buffered anyway. |
| NOT observable | A budget shared between cores — the handle is thread-local, so a charge on one shard thread is invisible to another and no cross-core atomic is taken on the reply path. A charge that outlives its bytes: repeated accounting of the same buffer sets the charge rather than adding to it, so a connection cannot inflate the subsystem by being measured twice. A nonzero steady-state `charged_bytes` in a healthy sample is also not promised — the write path drains a batch before returning, so the observable that pins adoption is the published row and its limit, not a sampled charge. |
| Invariant | `frogdb_memory::network_output` owns one `Budget` per core in a `thread_local!`, opened against `Subsystem::NetworkOutput` on the shard's own thread; `OutputBufferAccount` holds the single `Charge` for a connection and `set_buffered`/`release_all` are the only mutations of it — a flush re-measures through `set_buffered` rather than claiming a drain, so bytes still queued for the client keep their soft window running. |
| Outcome variant | `Charge` / `OutputVerdict::Shed(ShedReason::BudgetRefused)` |
| Forced by | `buffered_bytes_are_charged_to_the_network_output_budget`, `a_refused_charge_sheds_the_connection`, `draining_releases_every_charged_byte`, `a_core_has_one_budget`, `budgets_do_not_cross_cores`, `test_network_output_budget_is_published_for_resp2_and_resp3`, `a_shed_connection_releases_its_charge` |
| Bug refs | — |

## FM-MEMORY-003 — nothing retained past a command's completion aliases the connection's pooled read buffer

| Field | Value |
|---|---|
| Trigger | A command parsed on the zero-copy path — its name and arguments are refcounted slices of the connection's pooled read buffer ([PRD](../.scratch/memory-architecture/PRD.md) R12) — whose bytes would be retained past the command's completion: a value installed into the keyspace, a blocking op parked on a shard, or a command hopped to a foreign core. (A scatter participant's per-op key batch is not one of these: it lives only for the one synchronous round trip.) |
| Observable | The retained copy is made exactly at the escape point, so the read buffer's allocation is releasable the moment the command's own slices drop: a `SET` leaves the source buffer unshared while the key stays stored, a parked `BLMOVE`/`XREADGROUP` holds no slice of it, a command hopped to a foreign core carries no slice of it, and a partitioned multi-key command's per-shard batches are still slices of it (one synchronous round trip; the shard's install seam makes the one copy of whatever it retains). The pool observes the discipline directly — a read buffer handed back (`recycle` on the idle tick, `release` at close) with every slice dropped parks for reuse; one handed back with a slice still live is freed to the slice rather than parked. Between hops, batching holds: keys spanning K shards cost K lock-request messages, not one per key (`frogdb_scatter_gather_shards` records K). |
| NOT observable | A stored value or parked op whose bytes still alias the read buffer past the command's completion — the next lease of that buffer would overwrite data the keyspace can still read, so no test may ever observe a `Bytes` retained past the round trip sharing the source allocation. A buffer pinned indefinitely by a retained slice: ten thousand stored keys must not hold ten thousand read buffers alive. A copy paid where nothing escapes — bytes already uniquely owned (replication apply, recovery) pass through `detach_bytes` unchanged. |
| Invariant | One helper decides: `frogdb_protocol::detach_bytes` copies iff the `Bytes` still shares its allocation (`is_unique`), and every escape point calls it — the keyspace install seam (`HashMapStore::install`, which also calls `Value::detach_network_aliases`), the blocking-park boundary (`detach_wait_inputs`, `BlockingOp::detach`), the foreign-core hop (`command_for_shard` → `ParsedCommand::detached`), the connection-local retention points (MULTI queue, WATCH set, tracking prefixes, client name and library info), the cross-connection ones (slow log — always a copy, its entries outlive the command on shard 0 — pub/sub publish, script cache), and the collection-internal ones (quicklist plain nodes, stream group/consumer/PEL names, vector-set element names, time-series compaction destinations). Scatter partitioning (`partition_keys`, MSET pairs) is deliberately *not* an escape point: the batches are per-op slices and the shard's install seam copies once. `detach_bytes` decides by `is_unique`, which is sound only because the socket is polled between commands — the one mid-command read-ahead (draining pipelined frames while a blocking command parks) copies each frame out via `detach_frame` before the next poll. A `reserve` during that read-ahead can still move the codec onto a fresh allocation and leave the parked command's own args holding the old one; that never pins it behind a short key because a `ParsedCommand` holds its name and every arg as slices of one allocation (so none is unique while the command is alive) and after the wait those args are only borrowed (the slow log copies, the hot-key sampler takes `&[u8]`), never moved into a retention point. The pool side is `BytesMut::try_reclaim` in `BufferPool::give`: a shared buffer is never parked; the read buffer itself is pool-seeded at accept (`lease`) and returned through `recycle`/`release`. |
| Outcome variant | `detach_bytes` (copy) / pass-through (already unique) / `PoolStats::freed_returns` (lease dropped while shared) |
| Forced by | `test_detach_bytes_copies_only_when_shared`, `test_detach_frame_releases_source_buffer`, `test_command_detach_releases_source_buffer`, `queued_commands_and_watches_detach_from_the_shared_read_buffer`, `install_detaches_args_that_alias_a_shared_read_buffer`, `blocking_op_detach_releases_the_shared_read_buffer`, `wait_inputs_detach_from_the_shared_read_buffer`, `read_ahead_realloc_leaves_parked_args_shared_and_parked_frames_detached`, `foreign_shard_hop_detaches_the_command_from_the_read_buffer`, `local_shard_keeps_the_zero_copy_command`, `tracking_prefixes_detach_from_the_shared_read_buffer`, `truncate_args_copies_a_slice_that_is_the_sole_holder_of_its_buffer`, `partition_batches_keys_per_shard_without_copying`, `mset_partition_sends_read_buffer_slices`, `a_shared_slice_keeps_the_lease_out_of_the_pool_until_it_drops`, `a_pipelined_burst_of_slices_releases_the_lease_only_when_the_last_drops`, `a_scatter_over_k_shards_sends_exactly_k_lock_messages` |
| Bug refs | — |

## What the simulation cannot force

**After the thread-per-core move, turmoil tests production logic, not production execution
shape.** Today those are the same thing — a shard is a tokio task and the simulation runs
it on the simulation's own thread, so a sim host is a faithful stand-in for a server
process. Once a shard is an OS thread with a pinned runtime and a bound arena, it is not:
the shard-placement seam has a simulation implementation that multiplexes every shard back
onto the one sim thread and makes arena binding a no-op, because a shard on its own thread
escapes the simulation's scheduler and virtual clock and takes determinism with it. The
seam is what keeps the existing suite alive across the change, and it is load-bearing enough
that it must exist from the first commit of the runtime work rather than being retrofitted
(see [spike-report.md](../.scratch/memory-architecture/spike-report.md) §(c)).

The consequence for this spec is a rule about forcing tests, and rows in every group above
are subject to it. **A row whose behavior depends on any of the following may not name a
turmoil test as its forcing test**, and must name a real-thread harness instead:

1. **Allocator behavior.** Per-arena statistics, fragmentation ratios, per-shard `maxmemory`
   verdicts, and eviction driven by allocator truth do not exist under simulation — every
   shard allocates from one thread's arena there.
2. **Real memory-ordering effects.** The simulation serializes everything onto one thread,
   so it cannot exhibit a race between two shard threads, a torn read, or false sharing.
   Those belong to the shuttle/loom-style tools the workspace already carries.
3. **Cross-core cost.** A cross-core hop is free in the simulation and roughly 8× a
   same-core request in reality. No performance claim is ever forced by a sim test.
4. **The absence of foreign-thread frees.** The no-cross-arena-bleed rule is trivially true
   under a single thread and therefore untested there; it needs a real-thread assertion.

What *is* preserved under simulation, and may be forced there: message ordering between
shards and connections, network latency and partitions, virtual time, the totally ordered
per-shard command queue, and every protocol- and consistency-level invariant the existing
simulations already assert. All of those are properties of the shard body, which is shared
code across both implementations of the seam.

## Rows this spec will inherit

Two contracts elsewhere are memory contracts written in another area's vocabulary because
this file did not exist when they were needed. Both migrate here rather than being
duplicated:

* [replication.md FM-REPLICATION-068](replication.md#fm-replication-068--a-live-dataset-blob-is-bounded-before-it-is-allocated-and-read-a-chunk-at-a-time)
  bounds a wire-supplied dataset blob size against a 16 GiB constant before allocating, and
  says so itself: *"A ceiling is not a budget."* It defends the receive path against one
  absurd claim; it does not size the receive against the node's actual memory. When the
  receive path holds a `Budget`, that row's ceiling becomes the degenerate case of this
  spec's budget vocabulary — the constant stays as the cheap early refusal for a header that
  cannot be honest, and the budget takes over the question the ceiling was never answering.
  The migration is a row here plus an amendment there, not a deletion.
* [cluster.md TR-CLUSTER-016](cluster.md#tr-cluster-016--replica-feed-hold-during-an-armed-slot-barrier)'s
  unimplemented byte cap on the replica-feed hold buffer is a budget in the
  every-buffer-bounded group above. It is deliberately not being implemented against the
  current row (the cluster campaign is rewriting that row to ordinary replica-feed
  backpressure), so it lands here, once, in whichever shape that rewrite settles on.

## Adjacent specs

[persistence.md](persistence.md) owns durability ordering and the write-group atomicity this
spec's transaction cap bounds. [replication.md](replication.md) owns what crosses the link;
this spec owns what holding it costs. [cluster.md](cluster.md) owns slot ownership; this
spec owns what a cross-slot hop copies and who is charged for the copy.
[blocking.md](blocking.md) owns the parked-wait contract, including the parked pipeline
buffer that is one of the buffers this spec's first group will charge.
