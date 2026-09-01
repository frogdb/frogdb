# 01: introduce the `ShardExecutor` seam over the existing spawn chokepoint

Status: done
Type: AFK
Origin: memory-architecture phase-1 spike, 2026-08-31
Area: frogdb-server / net + server::shards
Phase: 1 (runtime/topology) — [PRD.md](../../PRD.md) "Rough dependency order" item 1

## Why now

[spike-report.md](../../spike-report.md) §(c) verdict: the R2/R3/R4 architecture stays
turmoil-testable **if and only if** shard placement goes through a `ShardExecutor`-style seam
*from the first commit*, with the sim implementation multiplexing shards onto the sim thread
and eliding arena binding. Retrofitting it after shards are threads means changing the seam
and the execution shape simultaneously, in a tree where the simulation suite is already red.
The ruling is recorded in [adr/0006](../../../../adr/0006-memory-architecture-seams.md) §1.

This issue is the no-op half of that: land the abstraction over today's behavior, so the call
sites move once under a change that cannot break anything, and issue 02 grows the real
implementation inside it.

## What exists today

- `frogdb-server/crates/net/src/lib.rs:45` — `pub use tokio::spawn;`, the seam that already
  swaps tokio and turmoil types for the server (`TcpListener`/`TcpStream` at `:31-42`).
- `frogdb-server/crates/server/src/server/shards.rs:16` imports that `spawn`; `:313` is the
  one site that launches a shard worker (`let handle = spawn(monitor.instrument(...))`),
  pushing a `(shard_id, JoinHandle)` pair.
- `shards.rs:308` already carries a sim-only accommodation: under `feature = "turmoil"`,
  `worker.set_driven_ticks(true)` plus `spawn_shard_tick_pump` (`:339`) replaces the shard's
  `select!` timer branches with queued tick messages. Precedent for a sim-specific shard
  launch path, not a new concept.
- `frogdb-server/crates/server/src/main.rs:123` builds one `new_multi_thread()` runtime. This
  issue does not touch it.

## What to build

1. An object-safe `ShardExecutor` in the seam crate (`frogdb-net`, beside the existing
   tokio/turmoil swap — this is a widening of that seam, not a second one), shaped like the
   spike's prototype (`spike/src/lib.rs`):
   - `launch(&mut self, shard_id, worker) -> ShardHandle` — takes what `shards.rs:313`
     currently hands to `spawn`, returns whatever the caller needs to join or abort it.
   - `arena_of(&self, shard_id) -> Option<u32>` — `None` means arena binding is not modelled.
     It returns `None` from every implementation in this issue; issue 03 gives the real one a
     `Some`.
   - `kind(&self) -> &'static str` — for logs and for tests that assert which implementation
     is wired.
2. Two implementations, one shard-body call path:
   - **real** — `frogdb_net::spawn` on the ambient runtime. *Byte-for-byte today's behavior*:
     no `std::thread`, no `new_current_thread`, no `arenas.create`. Threads arrive in 02.
   - **sim** — `tokio::spawn` on the caller's runtime (under turmoil, the sim host's one
     thread), `arena_of` → `None`. Under today's build these two are the same call; the split
     exists so 02 can change one without touching the other.
3. `shards.rs` constructs the executor once and launches every shard through it. The driven
   ticks accommodation stays exactly where it is — it is a property of the shard *worker*, not
   of placement, and moving it is out of scope.
4. Selection: real by default, sim under `feature = "turmoil"`, following the existing
   `cfg` pattern in `net/src/lib.rs` rather than adding a runtime switch.

## Acceptance criteria

- [ ] `ShardExecutor` exists with the three methods, is object-safe, and both implementations
      compile under both feature configurations.
- [ ] `server/shards.rs` has **no** direct `spawn` call for a shard worker; the only remaining
      `spawn` uses in that file are the tick pump and any non-shard helpers.
- [ ] A unit test asserts `kind()` is the sim implementation under `feature = "turmoil"` and
      the real one without it — the wiring is what this issue delivers, so it is what gets
      pinned.
- [ ] A unit test asserts `arena_of()` is `None` for every shard under the sim implementation.
      This assertion is permanent: it is the executable form of "arena binding is deliberately
      not modelled under simulation" and must still hold after issue 03.
- [ ] No behavior change: `just test frogdb-server` and the turmoil simulation tests pass with
      no test edits beyond the two new ones. If a simulation test needed editing, the seam is
      drawn in the wrong place.
- [ ] `just lint-gates` clean.

## Test boundary

Level 1/2. The seam's contract is which implementation is wired and what it reports, which is
a crate-local property — a socket adds nothing. The *behavioral* no-op claim is carried by the
existing suites passing unedited, not by a new test.

## Out of scope

Threads, pinning, current-thread runtimes, arenas, connection placement (issue 02); arena
binding and stats (issue 03). A `ShardExecutor` that spawns a thread in this issue defeats the
purpose of splitting it out.

## Depends on

Nothing.

## Resolution

Landed in `562f8d9b` — `feat(net): introduce the ShardExecutor placement seam`.

**Shape.** `frogdb-net` grew the seam beside the existing tokio/turmoil type swap:
`ShardBody = Pin<Box<dyn Future<Output = ()> + Send + 'static>>`, `ShardHandle = JoinHandle<()>`,
and `trait ShardExecutor: Send` with `launch`/`arena_of`/`kind`. Two implementations,
`RealShardExecutor` (`frogdb_net::spawn` on the ambient runtime) and `SimShardExecutor`
(`tokio::spawn` on the caller's runtime); both are defined unconditionally so each compiles under
both feature configurations, and only the `shard_executor()` factory is `cfg`-selected on
`feature = "turmoil"`. Object safety is proved at compile time by a `const _` block next to the
existing type-identity assertions.

`server/shards.rs` constructs one executor before the shard loop, logs `executor.kind()`, and
launches every shard through `executor.launch(shard_id, Box::pin(monitor.instrument(...)))`. No
shard-worker `spawn` remains in the file; the file-level `use crate::net::spawn` moved into
`spawn_shard_tick_pump`, which — along with the driven-ticks accommodation — was left exactly
where it was.

**Deliberate choices.**

- `ShardHandle` is a type *alias* for `JoinHandle<()>`, not a new struct: the shard supervisor
  `select_all`s these handles, and a distinct type would have forced edits to
  `spawn_shard_supervisor` and its tests — which the acceptance criteria forbid. This alias is the
  single place issue 02 must widen to bridge an OS-thread join back into something the supervisor
  can await.
- `launch` takes `ShardBody` (a boxed `Send + 'static` future) rather than a generic future
  parameter — required for object safety, and exactly what `shards.rs` already handed to `spawn`.
- Behavior is byte-for-byte unchanged: `frogdb_net::spawn` is `pub use tokio::spawn` in *both*
  feature arms, so real and sim launch identically today. The seam exists for issue 02, not for a
  behavior difference now.

**Tests added** (no existing test was edited):

- `frogdb-net`: `sim_executor_reports_no_arena_for_any_shard` (permanent), `real_executor_reports_no_arena_yet`
  (the one issue 03 replaces), `kinds_are_distinct`, `selected_executor_matches_the_feature`.
- `server::shards::tests`: `selected_shard_executor_matches_the_build` (runs in both arms; also
  proves `frogdb-server/turmoil` forwards `frogdb-net/turmoil`) and, turmoil-only,
  `simulation_reports_no_arena_for_any_shard`.

**Verification** (local mode): `just check frogdb-net`; `cargo check -p frogdb-net --all-targets
--features turmoil`; `cargo nextest run -p frogdb-net` 4/4 and `--features turmoil` 4/4;
`just check frogdb-server`; `just lint-turmoil` clean; `just lint-gates` OK; `just test
frogdb-server` 2105 passed / 5 skipped. Turmoil sim suite: 176/178 passed. The two failures
(`simulation::client_pause_write_expiry_suppression_realpath`,
`simulation::test_cluster_wait_unblocked_across_failover`) reproduce identically — same assertions,
same payloads — at the parent commit `41ae1177` with this change absent, i.e. they are pre-existing
red, not regressions.
