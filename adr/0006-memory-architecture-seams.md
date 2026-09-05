# Shard placement, buffer growth, and arena ownership are three seams, drawn before the first commit

The memory architecture ruled in
[`.scratch/memory-architecture/PRD.md`](../.scratch/memory-architecture/PRD.md) changes what a
shard *is*: today it is a tokio task on a work-stealing runtime, and it becomes an OS thread
with a pinned current-thread runtime and a dedicated jemalloc arena. Almost every consequence
of that change — deterministic simulation, per-shard accounting, the no-foreign-frees rule —
depends on decisions that are cheap to make now and expensive to retrofit, because they are
decisions about *where a boundary is*, not about what happens inside one. The phase-1 spike
([spike-report.md](../.scratch/memory-architecture/spike-report.md)) measured all three and
each came back with a condition attached. This ADR records the three boundaries and the
conditions.

## 1. Shard placement goes through a `ShardExecutor` seam, from the first commit

Shards are launched through an object-safe executor rather than a bare spawn, with two
implementations: a real one that puts each shard on its own OS thread with a
`Builder::new_current_thread()` runtime and a bound arena, and a simulation one that spawns
each shard as a task on the caller's runtime — under turmoil, the sim host's single thread —
and makes arena binding a **no-op**, reporting no arena rather than a fake one. The shard body
is one piece of code shared by both.

The seam is not an abstraction for its own sake; it is the only thing that keeps the
simulation suite alive. A turmoil host is one thread with a virtual clock, and a shard on its
own OS thread escapes both: it is no longer scheduled by the simulation and no longer sees
simulated time, so determinism goes immediately. The spike's simulation implementation
reproduced bit-identical execution traces across five runs of one seed while still exploring
six distinct schedules across six seeds — determinism preserved *and* non-degenerate
(spike-report §(c)).

**From the first commit** is the operative half of the ruling. The seam must land as the
introduction of the abstraction over today's behavior — real implementation spawns exactly
what `server/shards.rs` spawns today, no threads, no arenas, no behavior change — and only
then does the thread-per-core implementation grow inside it. This is the cheap order: the
call sites move once, under a no-op change, rather than being rewritten under a change that
also alters execution. Retrofitting the seam after shards are threads means doing both at
once in a tree where the simulation suite is already red.

There is precedent, which is why this is a widening of an established pattern rather than a
new one. `frogdb-net` already exists to swap tokio and turmoil types at one import
(`crates/net/src/lib.rs`, including the `spawn` that `server/shards.rs` uses), and the shard
worker already carries a simulation-only determinism accommodation: under the turmoil feature
its periodic-sweep timer branches are replaced by queued tick messages so the sweeps take a
definite place in the shard's totally ordered queue. The executor is the third member of that
family.

The cost is a fidelity gap, and it is real enough to be written down in the contract rather
than in a comment: after this lands, **turmoil tests production logic, not production
execution shape.** [`specs/memory.md`](../specs/memory.md) states it and derives the rule that
follows from it — allocator behavior, real memory-ordering effects, cross-core cost, and the
absence of foreign-thread frees may not be forced by a simulation test, and each needs a
real-thread harness instead. A later move to a completion-based backend (compio is natively
thread-per-core, and has no turmoil equivalent) survives only if the seam is drawn at the
executor rather than at tokio's types, which is a second reason to draw it exactly here.

## 2. Every non-keyspace buffer growth goes through a `Budget` handle

The per-core broker owns the budgets; each non-keyspace subsystem — network output, the
replication backlog, the tracking table, the WAL channel, full-sync staging, transaction
buffering — holds a handle and charges growth against it **before** the bytes exist. A charge
that fails is a refusal handled at that seam: the subsystem sheds (drops the client, discards
the buffered output) or backpressures (declines the write, stops reading), and it declares
which. A structure that cannot charge cannot grow.

The rule is worth exactly as much as its chokepoint, so it gets one, in the family documented
in [`agents/seam-lints.md`](../agents/seam-lints.md): an invariant stated so a violation is a
defect rather than a style opinion, a single type that satisfies it, a mechanical predicate
over source text, one of the two shipped suppression idioms, and a ratchet — the lint lands
with every unconverted buffer in a count-pinned allowlist so it can ship before the cleanup
does, and entries burn down in batches. Checking the allowlist in both directions is the
point: a buffer that gets converted has to leave the list, and a new violation inside an
already-listed file fails just like a new file would.

Why a lint rather than a type that makes the mistake unrepresentable: the buffers are
heterogeneous — a `Vec` in a ring, a codec's write half, a channel's queue depth, a staging
map — and no single wrapper type fits all of them without contorting each. What they *do*
share is a syntactic growth site, which is what a grep-shaped gate is good at. The precedent
in this repo is that the gates which stuck are the ones that pinned a call site, and the ones
that had to pin a type did so because the invariant was about constructibility
(`lint-status-sanitize`), which this one is not.

This ruling is what closes the issue-66 bug class by construction rather than one buffer at a
time: an unbounded structure is not a bug someone has to notice, it is a lint failure.

## 3. Arena ownership: one arena per shard thread, bound once, never crossed

Three rules, each measured:

**One arena per shard thread, created with `arenas.create` and bound with `thread.arena`.**
The spike's four-shard run attributed bytes to arenas exactly: three of four arenas matched
their expected byte count to the byte, the fourth to within 0.19 %, and no arena reported a
single byte belonging to another shard's thread. Binding costs nothing measurable — every
allocation-cost ratio against the default arena landed between 0.97× and 1.00× across sizes
from 64 B to 64 KB, because the thread cache is the fast path either way and is per-thread in
both shapes. Disabling the thread cache, by contrast, costs 2.3–2.9× on small sizes, so the
per-shard arena and the per-thread cache are orthogonal mechanisms that compose, and the cache
stays on (spike-report §(a) E1/E2/E3).

**Bound once, at thread start, before the thread allocates anything.** Rebinding
`thread.arena` does not flush the thread cache, so a rebind bleeds: in the spike, a rebind
charged 1.00 % of the objects allocated afterward to the *old* arena. With an explicit
`thread.tcache.flush` at the rebind the bleed is 0 bytes exactly. Bind-once is what the
thread-per-core design does naturally; if a rebind is ever needed — shard migration,
teardown — it is a `thread.arena` write **plus** a cache flush, or the no-cross-arena-bleed
invariant is quietly false by a percent (spike-report §(a) E4).

**No cross-core refcounts, no foreign-thread frees.** Values use a same-core, non-atomic
refcount; a cross-slot operation hops to the owning core and copies at the boundary rather
than sharing a pointer. Arenas do not enforce this — nothing stops a pointer crossing a
thread — so it is a separate invariant with its own real-thread assertion, and the spike's
cross-bleed measurement is the executable form of it. It cannot be tested under simulation,
where it is trivially true because there is one thread.

**Accounting is a sampled upper bound plus an exact per-request counter.** Arena statistics
are refreshed by a `mallctl` epoch advance that merges every arena, at a per-arena cost that
is a platform constant (~3.4 µs on the macOS spike box, ~13.4 µs on the Linux validation
box — budget from a measured sample), and they overstate live bytes by whatever is parked in
a thread cache — a residue bounded by the tcache's per-bin capacity, going to zero only on an
explicit cache flush. So the broker advances the epoch at 10–100 Hz, not per command, and
rides `thread.allocated` (tens of ns, exact, per thread) in between, with periodic
reconciliation. A thread-per-core FrogDB also sets `narenas:1` — via the compile-time
`_rjem_malloc_conf` symbol, since `tikv-jemalloc-sys` prefixes everything and plain
`MALLOC_CONF` is silently ignored (the env form is `_RJEM_MALLOC_CONF`) — and creates
exactly N shard arenas rather than accepting jemalloc's default of four per CPU, roughly
halving full-sample cost at eight shards (spike-report §(a) E4/E5, corrected by
spike-report-linux.md E5b). The direction of
the error is the safe one — an upper bound refuses slightly early — and
[`specs/memory.md`](../specs/memory.md)'s OOM rows are to be written against what a sampled
upper bound can promise, not against an exact per-command reading nothing can implement.

## What this ADR does not rule

The runtime shape itself is ruled in the PRD, not here, with one amendment the spike forced
and that belongs on the record next to these seams: **R4 does not stand alone.** Per-shard
current-thread runtimes *without* connection→core pinning measured 3.7× worse throughput and
5× worse tail latency than today's work-stealing runtime, because every request then pays two
cross-thread wakeups that work-stealing frequently avoids by co-scheduling the connection and
the shard on one worker. With pinning, the same shape is 2.3× throughput and 7.5× better p99
on *half* the OS threads, and a shard-affine-keys control rules out cache locality as the
explanation. So R2, R3 and R4 ship together or not at all, and a phase plan that splits them
ships a large regression in between (spike-report §(b)). The corollary sizes a design detail
these seams have to live with: a cross-slot hop costs roughly 8×, and it is the thread hop,
not the copy, that costs it.

## Addendum, 2026-09-05: memory is a locked area

[`specs/memory.md`](../specs/memory.md) is `Status: LOCKED`, making memory the fifth locked
core area beside txn, persistence, replication and cluster. The locked crates are
**`frogdb-memory`** — the per-core broker, the `Budget` handles §2 rules, and the
per-subsystem breakdown an operator reads — and **`frogdb-table`**, the keyspace structure
whose `memory_size()` the `maxmemory` verdict is measured against. Behaviour in either is
now spec-first: failure-mode row, then failing test, then code, with `just lint-spec`
enforcing that every row names live forcing tests and every tagged test names a row.

The gate is **0.85** for both crates (measured at lock: frogdb-memory MEMORY_SCORE,
frogdb-table TABLE_SCORE, caught / caught+missed).

Two of the rulings above need qualifying against what actually landed. Both narrow a claim;
neither reopens a seam.

**§2's chokepoint exists; §2's guarantee does not yet.** `lint-budget-growth` shipped with
its ratchet, as ruled — and the ratchet is still holding 83 unconverted growth sites across
32 files against 20 budgeted ones, with three of the seven declared subsystems
(`replication_backlog`, `wal_channel`, `fullsync_staging`) charged by nobody. *A structure
that cannot charge cannot grow* is therefore the direction of travel and not a contract, so
the locked spec deliberately writes no row claiming a buffer is bounded unless the row can
name that buffer's own `Budget`. The claim becomes a row when the allowlist is empty, one
converted buffer at a time.

**§3's "sampled upper bound" governs the arena figure, not the `maxmemory` verdict.** The
closing paragraph of §3 directs `specs/memory.md` to write its OOM rows against what a
sampled upper bound can promise. That is right about the arena statistics and wrong about
the verdict, and the code that landed draws the line the other way round: `is_over_memory_limit`
compares the shard's limit against `Store::memory_used()`, an exact running sum of
`Entry::memory_size()`, and never against the arena sample. Keeping them separate is the
point rather than an oversight — a verdict taken from the sampled bound would refuse writes
over thread-cache residue and over a neighbour's fragmentation, neither of which the shard
can evict its way out of. So FM-MEMORY-004 binds to the *accounted contents* and can state
an exact boundary (at the limit is inside it), while FM-MEMORY-008 owns the arena figure and
says in its own row that it is an upper bound and never drives a verdict. The honest caveat
runs the other way instead: `memory_size()` is a contents figure, deliberately run-stable, so
it excludes spare capacity and true footprint can exceed it — worst case about 2× for a
freshly doubled buffer.

**Fragmentation is measured, not fought.** Recorded here because it is a decision with no
row: there is no active defragmentation. Nothing walks the keyspace re-encoding values to
compact an arena. `MEMORY PURGE` returns dirty pages to the OS, the fragmentation ratio is
published, and re-encoding a value is a manual O(value) operation an operator triggers — never
a background task on a shard core, because an active defragmenter is a second allocator-load
generator on the one core the shard needs. It stays prose rather than a failure-mode row
because "nothing anywhere does this" over an open set of code is not something a test forces.
