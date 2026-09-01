# 02: thread-per-core shards with connections pinned to their owning core

Status: ready-for-agent
Type: AFK
Origin: memory-architecture phase-1 spike, 2026-08-31
Area: frogdb-server / net + server::shards + connection accept path
Phase: 1 (runtime/topology) — [PRD.md](../../PRD.md) R2 + R3 + R4, shipped as one change

## Why the three rulings are one issue

[spike-report.md](../../spike-report.md) §(b) measured R4 taken literally — per-shard
current-thread runtimes with connections landing wherever — at **0.27× today's throughput
(3.7× worse) and 5.3× worse p99**, because every request then pays two cross-thread wakeups
that the work-stealing scheduler frequently avoids by co-scheduling the connection task and
the shard task on one worker. The same shape *with* connections pinned to their shard's core
is **2.27× throughput and 7.53× better p99 on half the OS threads**, and the
`mt + shard-affine keys` control shows the win is runtime shape, not the benchmark's key
locality (that control buys only +22 %).

So: **R2, R3 and R4 land in one commit or not at all.** A plan that ships current-thread
runtimes first and pins connections second ships a large regression in between. Recorded as
the PRD's spike amendment ("R4 must bundle connection→core pinning") and in
[adr/0006](../../../../adr/0006-memory-architecture-seams.md) "What this ADR does not rule".

## What to build

1. **Real `ShardExecutor` becomes thread-per-core.** Inside the seam issue 01 landed, the real
   implementation launches each shard on its own `std::thread` running a
   `tokio::runtime::Builder::new_current_thread()` runtime, pinned where the platform supports
   it (Linux `sched_setaffinity`; macOS has no strict affinity API — Apple silicon ignores
   `thread_policy_set` affinity tags — so pinning is best-effort there and its absence must not
   change correctness). The sim implementation is untouched and still multiplexes onto the sim
   thread.
2. **Connections are accepted onto a core and stay there.** An accepted connection runs on the
   runtime of the core that owns its keys, so a same-slot command is a zero-hop, same-thread
   call. Placement policy is part of this issue: for a single-key or same-slot command the
   owning core is known from the key; for a connection that has not issued a command yet, pick
   by a documented rule (round-robin over cores) and state whether a connection ever moves.
   Simplest defensible answer — it does not move — is acceptable; say so rather than leaving it
   implicit.
3. **A cross-slot hop protocol, sketched and implemented for the paths that need it.** A
   command touching a slot this core does not own sends a message to the owner and awaits a
   reply, copying its arguments into the target core's ownership at the boundary. No value,
   buffer, or refcount crosses cores: PRD R3's shared-nothing rule means no foreign-thread
   frees. **The cost framing in R3 is wrong and this issue must plan against the corrected
   one:** spike §(b) measured the hop at roughly **8×** a same-core request, and it is the
   thread hop, not the copy, that costs it. Size the multi-key fan-out (MGET/MSET, scatter)
   against that number and record the result in this issue's comments; it is the input to the
   follow-up spike on cross-slot fraction.
4. **`main.rs:123`'s multi-thread runtime keeps whatever is left** — the acceptor, the
   observability server, background tasks — sized down now that shards no longer live on it.

## Acceptance criteria

- [ ] Every shard runs on a dedicated OS thread with its own current-thread runtime, launched
      through the `ShardExecutor` from issue 01. No shard is a task on the shared runtime.
- [ ] On Linux, shard threads are pinned; a test or a startup log line reports the intended and
      the achieved CPU for each shard. On macOS the pinning call is skipped with a single
      documented reason, not a silent failure.
- [ ] A connection's command touching a slot its core owns crosses **no** thread boundary — a
      test asserts this structurally (the shard body observes the same thread id as the
      connection) rather than by timing.
- [ ] A cross-slot command produces exactly one hop per foreign core involved, and the
      arguments it delivers are owned by the target core (no pointer shared with the origin).
- [ ] `just test frogdb-server` and the full turmoil simulation suite pass. The sim suite
      passing is the load-bearing check: the sim implementation is what keeps it green, so a
      simulation failure here means the seam leaked.
- [ ] A determinism assertion still holds — the existing `assert_run_is_reproducible` path in
      `frogdb-server/crates/server/tests/concurrency_workload.rs` is unchanged and green.
- [ ] A before/after benchmark on the same machine, with the shape and client counts of
      spike §(b), recorded in this issue's comments. The bar is "not a regression"; the spike's
      2.27× is a microbenchmark on a `HashMap` shard body and real per-op work compresses every
      ratio.

## Test boundary

Level 4 for placement and hop behavior — thread identity, accept-path routing and cross-core
messaging are only observable with a real server on real threads. The turmoil suite deliberately
cannot force any of this (it has one thread); per [`specs/memory.md`](../../../../specs/memory.md)
"What the simulation cannot force", these assertions must name a real-thread harness. The
shard-harness crate (`frogdb-server/crates/shard-harness`) is the natural home for the
same-thread and hop-count assertions.

## Out of scope

Arena creation and binding, and anything that reads allocator stats (issue 03) — this issue's
real executor still reports `arena_of() -> None`. Budgets and the broker (issue 05). Value
encodings, the keyspace table, eviction.

## Depends on

[Issue 01](../) — the `ShardExecutor` seam. This issue only changes the real implementation
behind it and the accept path; if it also has to move call sites, 01 was incomplete.
