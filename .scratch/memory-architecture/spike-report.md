# Memory architecture — phase-1 de-risk SPIKE report

Status: informational (spike, not a ruling)
Date: 2026-08-31
Prototype: [`spike/`](spike/) — throwaway crate, not a workspace member
Rules under test: [PRD.md](PRD.md) R2 (per-shard jemalloc arenas), R3 (shared-nothing
symmetric cores), R4 (tokio current-thread per core), and the open question
*"how turmoil tests model per-core pinning"*.

> **Validated on Linux.** Both benches were re-run on aarch64 Linux with hard
> `sched_setaffinity(2)` pinning and `narenas:1` —
> [spike-report-linux.md](spike-report-linux.md). **Every verdict below is confirmed**,
> and the colocated win *grows* under pinning (2.27× → 2.54×). Six numbers in this
> document are corrected there: the jemalloc config env var is `_RJEM_MALLOC_CONF` (not
> `MALLOC_CONF`), `narenas:1` buys 1.9× not 5×, E4's bleed is 0.32 % not 1.00 %, R4-alone
> is 2.5× not 3.7×, the hop is 6.35× not 8×, and the per-arena epoch cost is ~13 µs not
> 2.5 µs. Prefer the Linux numbers where they differ.

## Headline verdicts

| Ruling | Verdict | One-line reason |
| --- | --- | --- |
| **R2** — per-shard jemalloc arenas | **GO** | Attribution is byte-exact, binding is free (1.00× alloc cost), teardown purges clean. |
| **R3** — symmetric cores, connection pinned to its shard's core | **GO — and it is the load-bearing half** | Colocated shape: **2.3× throughput, 7.5× better p99**, on *half* the threads. |
| **R4** — one current-thread runtime per core | **GO WITH CONDITIONS** | R4 *without* R3 is a **3.7× regression**. Ship R4 only together with connection→core pinning. |
| Turmoil fidelity | **GO** — seam required | `ShardExecutor` with a sim impl is bit-deterministic (5/5 identical digests); arena behaviour is explicitly out of the sim. |

Machine: Apple M1-class, 10 cores (`hw.physicalcpu` 10), macOS 25.5.0, rustc 1.92.0,
jemalloc 5.3.0 via `tikv-jemalloc-sys 0.6.1` (`--enable-stats`), release + debug symbols.
Load average at the runtime-bench run: 3.3–3.8 (other agents were building on the box;
see [Caveats](#caveats)).

---

## (a) Per-shard arena binding from Rust — R2

### Method

`spike/src/bin/arena.rs`. jemalloc is the global allocator (as in
`frogdb-server/crates/server/src/main.rs:7`). Five experiments:

- **E1/E2** — 4 arenas via `arenas.create`; 4 threads, each `thread.arena`-bound to one
  arena, each allocating a *distinct* known volume of live small + large objects; then
  `epoch` advance and read `stats.arenas.<i>.{small,large}.allocated`.
- **E1c** — drop the data, `arena.<i>.purge`, re-read.
- **E4** — tcache composition: fill+free on arena A, rebind to B, refill, with and
  without `thread.tcache.flush`. Raw `malloc`/`free` into a pointer slab allocated
  *before* binding, so only the objects under test are charged to A/B.
- **E5** — cost of reading allocator truth (`epoch`, stats reads).
- **E3** — allocation microbench: default arena vs bound arena vs tcache disabled;
  min of 3 reps, fresh thread per rep, variants interleaved per size.

Two full runs were taken; numbers below are run 2, and run 1 agreed to within noise.

### E1/E2 — attribution and stats accuracy

| shard | arena | requested B | small.allocated | large.allocated | stats / requested |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 41 | 15,667,200 | 2,596,288 | 13,631,488 | 1.0358 |
| 1 | 42 | 62,668,800 | 10,250,240 | 53,477,376 | 1.0169 |
| 2 | 43 | 70,041,600 | 30,736,384 | 40,894,464 | 1.0227 |
| 3 | 44 | 291,635,200 | 81,940,480 | 211,812,352 | 1.0073 |

The plan uses exact size classes, so `requested == size-class-rounded`. The 0.7–3.6%
excess is **not** allocator overhead — it is the `Vec<Vec<u8>>` spines, which are
themselves heap objects charged to the same arena. Accounting for them:

| arena | observed | spines | expected | **delta** | resident |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 41 | 16,227,776 | 529,408 | 16,196,608 | **+0.192 %** | 19,939,328 |
| 42 | 63,727,616 | 1,058,816 | 63,727,616 | **0.000 %** | 70,942,720 |
| 43 | 71,630,848 | 1,589,248 | 71,630,848 | **0.000 %** | 82,673,664 |
| 44 | 293,752,832 | 2,117,632 | 293,752,832 | **0.000 %** | 309,428,224 |

Three of four arenas match the expected byte count **exactly**. Cross-bleed is zero:
no arena reports bytes belonging to another shard's thread. Arena 41's +31,168 B is
tcache residue (see E4).

`resident` runs 15–23 % above `allocated` — that gap *is* the per-shard fragmentation
metric R13 wants, and it is available per shard for free.

**E1c — teardown.** After the threads drop their data and `arena.<i>.purge`:

| arena | allocated after | resident after |
| ---: | ---: | ---: |
| 41 | 480 | 311,296 |
| 42 | 0 | 491,520 |
| 43 | 0 | 1,048,576 |
| 44 | 0 | 2,326,528 |

Purge returns 98.4–99.2 % of resident memory. `MEMORY PURGE` (today a documented no-op
at `frogdb-server/crates/server/src/connection/observability_conn_command.rs:405`) is
implementable for real, and shard teardown genuinely releases.

### E4 — does `thread.arena` compose with tcache?

20,000 × 128 B (exact size class) = 2,560,000 B under test.

| phase | arena A allocated | arena B allocated |
| --- | ---: | ---: |
| 1. fill + free N on A | 25,600 | 0 |
| 2. rebind → B, fill N, **no flush** | 25,600 | 2,534,400 |
| 3. free + explicit `thread.tcache.flush` | 0 | 48 |
| 4. A fill+free, rebind → B, **flush**, fill N | 0 | 2,560,048 |

**Two distinct findings, both actionable:**

1. **Freed-into-tcache objects still count as `allocated` on their arena.** Row 1: after
   allocating *and freeing* 20,000 objects on A, A still reports 25,600 B (200 regions
   parked in the thread cache). For R8, `stats.arenas.<i>.*.allocated` is an
   **upper bound** on live bytes, overstating by at most the tcache's capacity per bin.
   `thread.tcache.flush` on the owning thread drives it to 0 exactly (row 3).
2. **Rebinding `thread.arena` does *not* flush the tcache, so a rebind bleeds.** Row 2:
   B receives 2,534,400 B, exactly 200 regions (25,600 B, **1.00 %**) short — those came
   from A's stale tcache and stayed charged to A. With an explicit flush at the rebind
   (row 4), bleed is **0 B (0.0000 %)**.

Consequence for R2: **bind the arena once, at shard-thread start, before the thread
allocates anything** — which is exactly the thread-per-core design. If a rebind is ever
needed (shard migration, teardown), it must be `thread.arena` write **plus**
`thread.tcache.flush`, or the "no cross-arena bleed" invariant in R3 is violated by ~1 %.

### E3 — allocation cost under a bound arena

ns/op for alloc+free churn against a 64-block live ring; min of 3 reps.

| size (B) | default arena | bound arena | **bound / default** | tcache off | tcache-off / bound |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 64 | 7.88 | 7.67 | **0.973** | 22.27 | 2.90 |
| 256 | 7.80 | 7.81 | **1.002** | 21.90 | 2.80 |
| 1,024 | 12.06 | 12.09 | **1.002** | 28.24 | 2.33 |
| 8,192 | 112.21 | 111.01 | **0.989** | 114.09 | 1.03 |
| 16,384 | 189.15 | 185.82 | **0.982** | 253.00 | 1.36 |
| 65,536 | 1,374.81 | 1,355.87 | **0.986** | 1,310.68 | 0.97 |

**Binding a dedicated arena costs nothing** — every ratio is 0.97–1.00× across two runs.
This is expected: the thread cache is the fast path and it is per-thread either way.
Disabling tcache costs **2.3–2.9×** on small sizes, so R2 must keep tcache on; the
"per-shard arena" and "per-thread tcache" mechanisms are orthogonal and compose.

### E5 — cost of reading allocator truth (matters for R8)

`epoch` is what refreshes stats, and it **merges every arena**, so its cost is linear in
arena count:

| arenas live (`arenas.narenas`) | µs per `epoch` advance |
| ---: | ---: |
| 48 | 40.3 |
| 56 | 67.3 |
| 80 | 148.7 |
| 136 | 339.5 |

≈ **2.5 µs per arena per epoch**. Individual stat reads are cheap once the epoch has
advanced:

| operation | ns/call |
| --- | ---: |
| `stats.arenas.<i>.small.allocated` (by name) | 146 |
| `thread.allocated` (thread-local counter) | 23 |

Consequences for R8: allocator truth is a **sampled** quantity, not a per-command one.
A broker can afford an epoch advance at ~10–100 Hz, not per request; per-request
accounting should ride `thread.allocated`/`thread.deallocated` (23 ns, exact, per
thread) with periodic reconciliation against arena stats. Also: jemalloc auto-creates
`4 × ncpu` arenas by default (40 here); a thread-per-core FrogDB should set
`narenas:1` in `MALLOC_CONF` and create exactly N shard arenas, cutting epoch cost by
~5× at 8 shards.

> Minor: `mallctlnametomib` for `stats.arenas.<i>.small.allocated` returned an error
> under `tikv-jemalloc-ctl 0.6.1`, so the pre-resolved-MIB path was not measured. The
> by-name path at 146 ns is already far below the epoch cost, so this does not change
> any conclusion. Worth 20 minutes at implementation time.

### Verdict (a): **GO for R2**

Per-shard arenas do everything R2 and R8 claim: byte-exact per-shard attribution, zero
cross-bleed, free binding, per-shard fragmentation visibility, real purge on teardown.
Two conditions: **bind once at thread start** (a rebind without a tcache flush bleeds
1 %), and treat arena stats as a **sampled upper bound** (tcache residue), not a
per-command counter.

---

## (b) Pinned current-thread runtimes vs work-stealing — R3 / R4

### Method

`spike/src/bin/runtime.rs`. 4 shards, each owning a `HashMap<Vec<u8>, Vec<u8>>`
pre-populated from a 100,000-key space with 64 B values. Clients do 80 % GET / 20 % SET
round-trips through the same plumbing FrogDB uses (`ASYNC_RUNTIME.md` §2): bounded
`mpsc::channel(1024)` to the shard, `oneshot` back. 1,000,000 ops per run, median of 3
runs, three concurrency levels. Five shapes — the middle three are *controls*:

| shape | what it is |
| --- | --- |
| `mt work-stealing (today)` | one multi-thread runtime, 8 workers, shards as tasks — FrogDB today (`main.rs:123`, shards spawned as tasks at `server/shards.rs:313`) |
| `mt work-stealing, 4 wrk` | same, 4 workers — equal thread budget to the colocated shape |
| `mt + shard-affine keys` | same as today, but each client only touches one shard's keys — **isolates key locality from runtime shape** |
| `tpc, cross-thread (R4)` | one current-thread runtime per shard thread; clients on their own 4-worker runtime; every request crosses threads (R4 read literally) |
| `tpc colocated (R3+R4)` | one current-thread runtime per shard thread; each client lives on its own shard's runtime — zero cross-thread hops (R3 + R4 together) |

### Results

| shape | threads | clients | ops/sec | p50 µs | p99 µs | p99.9 µs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| mt work-stealing (today) | 8 | 32 | 3,361,318 | 4.83 | 38.50 | 231.17 |
| mt work-stealing, 4 wrk | 4 | 32 | 2,555,744 | 6.12 | 43.79 | 271.50 |
| mt + shard-affine keys | 8 | 32 | 4,013,981 | 4.04 | 13.67 | 199.92 |
| tpc, cross-thread (R4) | 8 | 32 | 1,009,304 | 24.46 | 124.08 | 348.21 |
| **tpc colocated (R3+R4)** | **4** | 32 | **8,601,602** | **3.50** | **5.62** | **39.88** |
| mt work-stealing (today) | 8 | 128 | 4,578,961 | 18.00 | 124.79 | 178.92 |
| mt work-stealing, 4 wrk | 4 | 128 | 3,695,666 | 31.75 | 108.21 | 151.58 |
| mt + shard-affine keys | 8 | 128 | 5,602,028 | 19.92 | 69.21 | 280.88 |
| tpc, cross-thread (R4) | 8 | 128 | 1,233,049 | 64.67 | 660.33 | 1,973.50 |
| **tpc colocated (R3+R4)** | **4** | 128 | **10,383,017** | **8.46** | **16.58** | **43.88** |
| mt work-stealing (today) | 8 | 512 | 4,772,591 | 43.25 | 501.62 | 1,019.00 |
| mt work-stealing, 4 wrk | 4 | 512 | 3,615,330 | 121.75 | 527.46 | 1,048.88 |
| mt + shard-affine keys | 8 | 512 | 5,173,688 | 41.54 | 376.12 | 2,395.08 |
| tpc, cross-thread (R4) | 8 | 512 | 1,443,323 | 269.79 | 1,456.79 | 3,148.21 |
| **tpc colocated (R3+R4)** | **4** | 512 | **10,583,942** | **35.38** | **75.38** | **244.29** |

### Reading the numbers

Ratios vs today's shape (8-worker work-stealing), at 128 clients:

| comparison | throughput | p99 |
| --- | ---: | ---: |
| tpc colocated / today | **2.27×** | **7.53× better** |
| tpc colocated / today, per thread | **4.54×** | — |
| tpc colocated / mt-4wrk (equal threads) | **2.81×** | 6.53× better |
| tpc cross-thread / today | **0.27× (3.7× worse)** | 5.29× worse |

The `mt + shard-affine keys` control is the decisive one. It gives the work-stealing
runtime *exactly* the key-locality advantage the colocated shape has (each client
touches one shard's 25,000-key working set) and it only buys **+22 %** (4.58M → 5.60M).
The colocated shape is **1.85× faster still**, on half the threads. So the win is a
**runtime-shape win — the elimination of cross-thread wakeups on the request path** —
not a cache-locality artifact of the benchmark.

The `tpc, cross-thread` row is the finding that matters most for planning. Taking R4 on
its own — per-shard current-thread runtimes, but connections landing wherever — makes
every single request pay two cross-thread wakeups (mpsc send parks/unparks the shard
thread; oneshot send parks/unparks the client thread). Today's work-stealing scheduler
frequently co-schedules the connection task and the shard task on one worker and skips
both. Result: **R4 alone is a 3.7× throughput regression and a 5× tail regression.**

### Verdict (b)

- **R3 (symmetric cores, connection pinned to its slot's core): GO.** It is where all the
  benefit lives — 2.3× throughput and 7.5× better p99 on half the OS threads, with the
  locality control ruling out the obvious confound.
- **R4 (current-thread runtime per core): GO WITH CONDITIONS.** R4 is the *mechanism*,
  not the benefit. It must land **together with** connection→core pinning (R3) and
  same-core command execution. A phased plan that ships "current-thread runtimes per
  shard" first and "pin connections to cores" second would ship a large regression in
  between. **The PRD's dependency order (§"Rough dependency order", item 1: R2+R3+R4
  together) is correct and must not be split.**
- Corollary for R3's cross-slot hop: every cross-slot key is a request that pays the
  cross-thread cost measured in the `tpc, cross-thread` row. The `tpc`/`colocated` gap
  (10.4M vs 1.2M) is the *price of a hop*, and it is ~8×. R3's "multi-key cross-slot ops
  pay a copy; that is the accepted trade" understates it — the copy is the cheap part;
  the thread hop is the expensive part. Worth sizing MGET/MSET fan-out behaviour against
  this before committing.

---

## (c) Turmoil fidelity for a thread-per-core architecture

### How the harness works today (grounded citations)

- **One sim host == one FrogDB server *process*.**
  `frogdb-server/crates/server/tests/simulation.rs:104` —
  `sim.host(SERVER_HOST, || real_frogdb_server(1))`; the closure at
  `frogdb-server/crates/server/tests/common/sim_helpers.rs:145` builds a whole
  `Server` (config, N shards, listener) and runs it to completion. Multi-node tests
  register one host per node
  (`tests/simulation/full_sync_payload.rs:104,122`; `tests/simulation/scheduler.rs:936`).
- **Turmoil owns the runtime, and it is single-threaded per host.** The host closure is
  a future; turmoil polls it on the simulation's one thread with virtual time.
- **Shards are tokio *tasks*, not threads, and they are spawned through a seam.**
  `frogdb-server/crates/server/src/server/shards.rs:313` — `let handle = spawn(...)`,
  where `spawn` is imported at `shards.rs:16` from `crate::net`, which is
  `frogdb-server/crates/net/src/lib.rs:45` — `pub use tokio::spawn;`. The same module
  swaps `TcpListener`/`TcpStream` between tokio and turmoil at `net/src/lib.rs:31-42`.
- **Production is work-stealing multi-thread.**
  `frogdb-server/crates/server/src/main.rs:123` —
  `tokio::runtime::Builder::new_multi_thread()`. No `core_affinity`, no `LocalSet`, no
  `new_current_thread` anywhere in the server data path.
- **The harness already carries sim-only determinism accommodations**, which is the
  precedent for a sim-only executor. `shards.rs:308` — under `feature = "turmoil"`,
  `worker.set_driven_ticks(true)` replaces the shard's `select!` timer branches with
  queued `DriveTick` messages pumped by `spawn_shard_tick_pump` (`shards.rs:339`), so the
  periodic sweeps take a definite place in the shard's totally-ordered queue.
  Determinism itself is already asserted:
  `frogdb-server/crates/server/tests/concurrency_workload.rs:492`
  (`assert_run_is_reproducible`), and the scheduler sims seed explicitly with
  `.rng_seed(seed).enable_random_order()` (`tests/simulation/scheduler.rs:823-834`).

### The problem, stated precisely

R2/R3/R4 turn each shard from a *task* into an *OS thread* with a pinned runtime and a
bound jemalloc arena. A turmoil sim host is one thread. Therefore **the production
placement cannot exist under simulation** — not because turmoil forbids threads, but
because a shard on its own OS thread escapes the sim's scheduler and its virtual clock,
and non-determinism returns immediately.

### The seam, prototyped

`spike/src/lib.rs` defines:

```rust
pub trait ShardExecutor {
    fn launch(&mut self, id: usize, trace: Trace) -> mpsc::Sender<ShardMsg>;
    fn arena_of(&self, id: usize) -> Option<u32>;   // None == arena binding not modelled
    fn kind(&self) -> &'static str;
}
```

with two implementations sharing one `shard_loop` body:

- `ThreadPerCore` — `std::thread::spawn` → `Builder::new_current_thread()` →
  `arenas.create` + `thread.arena` bind. This is R2+R4.
- `SimShards` — `tokio::spawn` on the caller's runtime (under turmoil, the sim host's
  single thread); `arena_of` returns `None`, i.e. **arena binding is deliberately a
  no-op under simulation**.

This slots directly into the existing structure: `shards.rs:313` already calls a
seamed `spawn` from `frogdb-net`, so `ShardExecutor` is a widening of the seam
`net/src/lib.rs:45` already established, not a new concept.

### Experiment and results

`spike/tests/sim_shard.rs` — 3 turmoil client hosts × 40 ops against a `server` host
running `SimShards` with 4 shards over `turmoil::net::TcpStream`. Each shard appends
`s<id> <VERB> <key>` to a shared trace; the run's digest is FNV-1a over that ordered
trace, so it is a witness for the *interleaving*, not just the outcome.

| test | result |
| --- | --- |
| same seed (42), 5 runs | `e1f79411cbf6daf5` × 5 — **bit-identical** |
| seeds 1..=6 | `7214cd04…`, `2dc098c9…`, `839ac9cf…`, `796bff3c…`, `41c433bd…`, `0d28a962…` — **6 distinct** |
| `ThreadPerCore`, 4 shards | arenas `[41, 42, 43, 44]` — distinct per shard; same results; **no interleaving guarantee** |

All three tests pass (`cargo test --release --test sim_shard`).

Determinism is preserved *and* non-degenerate: the same seed reproduces the schedule
exactly, and different seeds still explore different schedules.

### What is inside and outside the sim's fidelity envelope

**Preserved under simulation:** message ordering between shards and connections; network
latency, partitions, and failures; virtual time; the totally-ordered per-shard command
queue; every protocol- and consistency-level invariant the existing sims assert. All of
this is a property of the *shard body*, which is shared code across both executors.

**Outside the envelope — must not be asserted in turmoil tests:**

1. **Allocator behaviour.** Arena binding is a no-op under sim; every shard allocates
   from the one sim thread's arena. Per-arena stats, fragmentation ratios, per-shard
   `maxmemory` verdicts and eviction driven by allocator truth (R8) are **not**
   reproducible in a turmoil test and need their own harness — a real multi-threaded
   process test, in the style of the shard-harness crate.
2. **Allocation interleavings and real memory-ordering races.** The sim serialises
   everything onto one thread; it cannot exhibit a data race between two shard threads,
   a torn read, or a false-sharing effect. Those belong to shuttle/loom-style tools,
   which the workspace already uses (`shuttle` is a dev-dependency of `frogdb-server`).
3. **Cross-core wakeup cost and pinning effects.** Zero-cost in the sim, 8× in reality
   per (b). Performance conclusions never come from turmoil.
4. **Foreign-thread frees.** R3's "no cross-arena bleed, no foreign-thread frees"
   invariant is trivially true under sim (one thread) and therefore untested there. It
   needs a real-thread assertion — the E1b cross-bleed measurement generalises into one.

### Verdict (c): **GO — with the seam as a hard requirement**

The R2/R3 architecture stays turmoil-testable *if and only if* shard placement goes
through a `ShardExecutor`-style seam from day one, with the sim impl multiplexing shards
onto the sim thread and eliding arena binding. Retrofitting this after shards become
threads would be far more invasive. The precedent already exists in-tree
(`shards.rs:308` driven ticks, `net/src/lib.rs:45` seamed `spawn`), so this is an
extension of an established pattern rather than a new one.

The cost is a fidelity gap that must be written down: **turmoil will no longer be
testing the production execution shape, only the production logic.** Today those are the
same thing; after R2/R3/R4 they are not. `specs/memory.md` (R15) should state this
explicitly, and the memory contract's FM rows should name the *non-turmoil* harness that
forces each one.

---

## Caveats

- **No hard core pinning.** macOS has no strict affinity API — `thread_policy_set`
  affinity tags are advisory and are ignored on Apple silicon. Nothing in (b) is pinned.
  The comparison is therefore about *runtime shape*, which is the architectural
  question; absolute numbers need a Linux re-run.
- **Noisy machine.** Other agents were compiling on the box during the runtime bench
  (load average 3.3–3.8 on 10 cores). Medians of 3 were taken, and an earlier
  no-controls run reproduced the same ordering with larger absolute numbers
  (mt 5.26M, tpc 0.99M, colocated 16.3M at 128 clients — colocated/mt = 3.1× there vs
  2.3× here). Treat the ratios as directionally solid and the absolutes as soft.
- **The shard body is a `HashMap`, not FrogDB.** No RESP parsing, no persistence, no
  expiry sweep, no replication feed. Real per-op work is larger, which compresses every
  ratio in (b). The *sign* of each result is what this spike establishes.
- **The `colocated` shape assumes every request is same-slot.** Real workloads have
  cross-slot traffic; see the hop-cost note in (b).

## Follow-ups

1. **Cross-slot hop cost.** Extend the bench with a tunable cross-slot fraction
   (0 / 5 / 25 / 100 %) to find the point where R3's hop erases R3's win. Directly sizes
   the "copy at boundary" trade in R3 and the MGET/MSET fan-out design.
2. **io_uring interplay.** `.scratch/roadmap/optimizations/ASYNC_RUNTIME.md` §4 favours
   compio for a completion-based backend. compio is *natively* thread-per-core, so R4's
   "tokio current-thread per core" and a later io_uring move are the same shape — R4 does
   not foreclose it. But compio has no turmoil equivalent: the `ShardExecutor` seam from
   (c) plus the existing `frogdb-net` swap is what would keep the sim suite alive across
   that change. Check before committing to R4 that the seam is drawn at the executor,
   not at tokio types.
3. **`mallctlnametomib` on `stats.arenas.<i>.*`.** Returned an error under
   `tikv-jemalloc-ctl 0.6.1`; the by-name path works. Worth resolving so the broker can
   use the MIB fast path.
4. **`_RJEM_MALLOC_CONF` tuning study.** `narenas`, `dirty_decay_ms`, `muzzy_decay_ms`,
   `tcache_max` all move the R13 fragmentation/RSS trade. E1c shows purge recovers
   ~99 % of resident; a decay study should precede the R13 metrics work.
5. **Real-thread cross-bleed assertion.** Turn E1b into a non-turmoil regression test
   asserting zero cross-arena attribution across shard threads — the executable form of
   R3's "no cross-arena bleed" invariant, which the sim cannot test.

## Contradictions with the PRD as written

1. **R4 as stated is incomplete and, taken alone, harmful.** "One tokio current-thread
   runtime pinned per core" is a 3.7× regression without R3's connection pinning. R4
   should be reworded to make the connection→core pinning part of the ruling, not a
   separate ruling that happens to precede it.
2. **R3's cost framing understates the hop.** "Multi-key cross-slot ops pay a copy; that
   is the accepted trade" — the measured cost is dominated by the cross-thread wakeup
   (~8×), not the copy. The trade is real but larger than the wording implies.
3. **R8's "arena stats are ground truth for maxmemory" needs two qualifiers.** Arena
   `allocated` is an *upper bound* (tcache residue: 25.6 KB per bin per thread in E4,
   0 after a flush), and refreshing it costs ~2.5 µs per arena per `epoch`, so it is a
   sampled quantity. Neither invalidates R8; both change its implementation.
4. **The open question "how turmoil tests model per-core pinning" has an answer that is
   a constraint, not a detail**: the shard-executor seam must exist from the first
   commit of R2/R3/R4, and turmoil's fidelity envelope shrinks to "production logic, not
   production execution shape". That belongs in the PRD body, not the open-questions
   list.
