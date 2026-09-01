# Memory architecture — phase-1 spike, **Linux validation run**

Status: informational (spike, not a ruling)
Date: 2026-09-01
Validates: [spike-report.md](spike-report.md) — every number there was taken on an
unpinnable, noisy macOS box. This run repeats both benches on aarch64 Linux with hard
`sched_setaffinity(2)` pinning and `narenas:1`.
Issue: [memory-architecture issue 04](issues/)

## Bottom line

| macOS conclusion | Linux verdict |
| --- | --- |
| **R2 GO** — attribution byte-exact, binding free, purge clean | **CONFIRMS** — identical `stats/requested` ratios, same 0.000 % deltas, purge recovers 98.5–99.3 % |
| **R3 GO** — colocated is 2.3× throughput / 7.5× better p99 on half the threads | **CONFIRMS, slightly stronger** — 2.54× throughput, 7.6× better p99, on half the threads, all pinned |
| **R4 GO WITH CONDITIONS** — R4 alone is a 3.7× regression | **CONFIRMS, weaker magnitude** — 2.5× regression (0.40× throughput), not 3.7× |
| Cross-slot hop costs ~8× | **CONFIRMS, weaker** — 6.35× on Linux |
| Arena stats are a sampled upper bound (tcache residue ~1 %) | **CONFIRMS direction, contradicts the magnitude** — residue is 0.32 % here, not 1.00 %; the number is platform-specific and must not be spec'd |
| `narenas:1` cuts epoch cost ~5× at 8 shards | **WEAKENS** — measured 1.87× at 8 shards, not 5× |
| Issue 03's 20 Hz default / 10–100 Hz clamp | **CONFIRMS** — full broker sample is 83 µs at 8 shards; 20 Hz = 0.17 % of one core, 100 Hz = 0.83 % |

**One new finding that changes shipping configuration** (see
[`_RJEM_MALLOC_CONF`](#finding-the-env-var-is-_rjem_malloc_conf-not-malloc_conf)):
`tikv-jemalloc-sys` builds jemalloc with `--with-jemalloc-prefix=_rjem_`, so jemalloc
reads **`_RJEM_MALLOC_CONF`**. `MALLOC_CONF=narenas:1` is silently ignored — the spike
report's follow-up #1 and the PRD's R2 note both name the wrong variable.

---

## Environment

| | |
| --- | --- |
| Machine | Blacksmith `blacksmith-8vcpu-ubuntu-2404-arm`, Ampere-1a aarch64 |
| CPUs | 8 vCPU, 1 thread/core, 1 NUMA node, no cpufreq governor exposed |
| Memory | 23 GB RAM + 10 GB swap |
| OS | Ubuntu 24.04.3 LTS, kernel 6.6.141 SMP |
| rustc / cargo | 1.92.0 (`ded5c06cf` 2025-12-08) / 1.92.0 |
| Profile | `release` + debug symbols (spike's own `[profile.release]`) |
| jemalloc | 5.3.0-1-ge13ca993e8ccb9ba9847cc330696e02839f328f7, via `tikv-jemalloc-sys 0.6` (`--enable-stats`) |
| `MALLOC_CONF` | `<unset>` — inert with this build, see finding below |
| `_RJEM_MALLOC_CONF` | `narenas:1` for the arena suite (`opt.narenas` readback = 1); `<unset>` for the runtime bench and the control runs |
| Load average | arena suite 0.87 → 1.07; runtime bench 0.09 → 2.21 (the bench itself is the load) |
| Pinning | `sched_setaffinity(2)`, verified per thread — every run ends with an evidence table |

Both binaries were built and run inside a single `just tb-run` invocation each; the spike
is its own cargo workspace, so it builds with its own `cargo build --release --bins`.

### Pinning: intent vs achieved

`spike/src/pin.rs` is new. It sets affinity, then *re-reads* `sched_getaffinity` for the
allowed mask and samples `sched_getcpu()` plus `CLOCK_THREAD_CPUTIME_ID` at least twice
per thread, and prints a table where `allowed` and `observed` must both equal the
intended `cpu`. Every run in this report ends with:

```
pinning verdict: OK — every thread stayed on its intended CPU
```

Achieved CPU per thread role in the runtime bench (aggregated over all 9 measured runs):

| role | CPUs | threads | allowed == observed == intended | cpu-sec / CPU | CPU % |
| --- | ---: | ---: | :---: | ---: | ---: |
| `mt/8w worker` | 0–7 | 10 | yes | 3.72–3.84 | 86.9–89.0 % |
| `mt/4w worker` | 0–3 | 9 | yes | 5.63–5.65 | 95.3–95.6 % |
| `mt/8w-affine worker` | 0–7 | 9 | yes | 3.34–3.40 | 89.5–90.6 % |
| `tpc-xthread shard` | 0–3 | 9 | yes | 7.57–7.92 | 76.7–80.3 % |
| `tpc-xthread client` | 4–7 | 9 | yes | 7.14–7.18 | 72.1–72.6 % |
| `colocated shard+clients` | 0–3 | 9 | yes | 1.57–1.59 | 99.9 % |

The colocated shape is the only one that saturates its cores (99.9 %); every
work-stealing shape leaves 10–13 % of each core idle, and the cross-thread shape leaves
20–28 % — that idle time *is* the park/unpark stall the shape is being blamed for.

> **Gotcha worth recording.** The first attempt sized the CPU space with
> `std::thread::available_parallelism()`, which reads the *caller's* affinity mask. Once
> the main thread was pinned it returned 1, and every `cpu % available_cpus()` collapsed
> onto CPU 0 — producing plausible-looking but meaningless numbers. `pin::available_cpus()`
> now uses `sysconf(_SC_NPROCESSORS_ONLN)`, a property of the machine rather than of the
> calling thread. Anything in FrogDB that sizes the shard grid must do the same.

---

## (b) Runtime shape — R3 / R4

Identical shapes and workload to the macOS run (4 shards, 100k-key space, 64 B values,
80 % GET / 20 % SET, 1,000,000 ops per run, **median of 3**), plus hard pinning:

```
mt work-stealing, W workers   workers -> CPUs 0..W          (shards are tasks)
tpc cross-thread              shards  -> CPUs 0..4, clients -> CPUs 4..8
tpc colocated                 shards  -> CPUs 0..4          (clients ride along)
```

`spread` is `(max − min) / median` across the 3 reps — the witness that the median means
something. `load1` is `/proc/loadavg` field 1 read before and after each shape's reps.

### Absolute results (aarch64 Linux, pinned)

| shape | threads | clients | ops/sec | p50 µs | p99 µs | p99.9 µs | spread | load1 pre→post |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| mt work-stealing (today) | 8 | 32 | 2,259,853 | 12.04 | 46.56 | 92.94 | 4.2 % | 0.09→0.09 |
| mt work-stealing, 4 wrk | 4 | 32 | 1,535,023 | 16.84 | 71.28 | 91.80 | 3.2 % | 0.09→0.09 |
| mt + shard-affine keys | 8 | 32 | 2,487,643 | 11.86 | 27.76 | 42.66 | 5.0 % | 0.09→0.73 |
| tpc, cross-thread (R4) | 8 | 32 | 892,884 | 31.14 | 91.14 | 124.84 | 10.0 % | 0.73→0.73 |
| **tpc colocated (R3+R4)** | **4** | 32 | **5,746,922** | **5.02** | **7.18** | **16.86** | 1.2 % | 0.73→0.73 |
| mt work-stealing (today) | 8 | 128 | 2,363,623 | 28.30 | 213.86 | 261.46 | 7.6 % | 0.73→1.31 |
| mt work-stealing, 4 wrk | 4 | 128 | 1,661,562 | 64.90 | 257.96 | 678.24 | 1.7 % | 1.31→1.31 |
| mt + shard-affine keys | 8 | 128 | 2,539,272 | 42.46 | 150.90 | 267.82 | 2.7 % | 1.31→1.31 |
| tpc, cross-thread (R4) | 8 | 128 | 946,154 | 72.70 | 452.66 | 501.52 | 10.4 % | 1.31→1.60 |
| **tpc colocated (R3+R4)** | **4** | 128 | **6,006,329** | **19.96** | **28.16** | **48.66** | 3.9 % | 1.60→1.60 |
| mt work-stealing (today) | 8 | 512 | 2,441,564 | 105.80 | 851.46 | 1,041.98 | 6.7 % | 1.60→1.60 |
| mt work-stealing, 4 wrk | 4 | 512 | 1,576,635 | 307.80 | 870.92 | 1,083.40 | 4.3 % | 1.80→1.80 |
| mt + shard-affine keys | 8 | 512 | 2,728,978 | 168.04 | 292.80 | 363.34 | 6.2 % | 1.80→1.80 |
| tpc, cross-thread (R4) | 8 | 512 | 921,254 | 204.68 | 2,054.78 | 4,571.78 | 3.3 % | 1.80→2.21 |
| **tpc colocated (R3+R4)** | **4** | 512 | **5,662,850** | **84.24** | **117.42** | **403.80** | 3.0 % | 2.21→2.21 |

Max spread across all 15 rows is 10.4 % (both `tpc, cross-thread` rows — the shape whose
cost *is* scheduler wakeup jitter); 13 of 15 rows are under 8 %.

### Linux vs macOS, side by side (128 clients)

| shape | macOS ops/sec | Linux ops/sec | Linux / macOS | macOS p99 | Linux p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| mt work-stealing (today) | 4,578,961 | 2,363,623 | 0.52× | 124.79 | 213.86 |
| mt work-stealing, 4 wrk | 3,695,666 | 1,661,562 | 0.45× | 108.21 | 257.96 |
| mt + shard-affine keys | 5,602,028 | 2,539,272 | 0.45× | 69.21 | 150.90 |
| tpc, cross-thread (R4) | 1,233,049 | 946,154 | 0.77× | 660.33 | 452.66 |
| tpc colocated (R3+R4) | 10,383,017 | 6,006,329 | 0.58× | 16.58 | 28.16 |

The Ampere-1a cores are roughly half an M1 core per-thread; every shape scales down by a
similar factor, so **the shape comparison is preserved**. The one shape that scales down
*least* is `tpc, cross-thread` (0.77×) — cross-thread wakeups were disproportionately
expensive on the noisy, unpinned macOS box, which is exactly the confound this run
existed to remove.

### Ratios (Linux, 128 clients — the numbers that carry the verdicts)

| comparison | macOS | **Linux** | verdict |
| --- | ---: | ---: | --- |
| colocated / today (throughput) | 2.27× | **2.54×** | confirms |
| colocated / today (per thread) | 4.54× | **5.08×** | confirms |
| colocated / today (p99) | 7.53× better | **7.59× better** | confirms |
| colocated / mt-4wrk (equal threads) | 2.81× | **3.61×** | confirms, stronger |
| tpc cross-thread / today | 0.27× (3.7× worse) | **0.40× (2.5× worse)** | confirms sign, **weakens magnitude** |
| shard-affine control / today | +22 % | **+7.4 %** | confirms, **stronger** (locality explains even less) |
| colocated / shard-affine control | 1.85× | **2.37×** | confirms, stronger |
| hop cost (colocated / cross-thread) | ~8.4× | **6.35×** | confirms, weakens |

### CPU efficiency per op (only measurable with pinning)

Total pinned CPU-seconds across the 9 measured runs, divided by ops retired:

| shape | CPUs used | total cpu-sec | **µs of CPU per op** | vs colocated |
| --- | ---: | ---: | ---: | ---: |
| tpc colocated (R3+R4) | 4 | 6.3 | **0.70** | 1.00× |
| mt work-stealing, 4 wrk | 4 | 22.5 | 2.50 | 3.6× |
| mt + shard-affine keys | 8 | 27.0 | 3.00 | 4.3× |
| mt work-stealing (today) | 8 | 30.2 | 3.27 | 4.7× |
| tpc, cross-thread (R4) | 8 | 59.6 | 6.63 | 9.5× |

This is the cleanest statement of the result and it was *not* available on macOS: the
colocated shape retires an op for **4.7× less CPU** than today's shape and **9.5× less**
than R4-without-R3. Throughput ratios understate the win because they are also bounded by
the client generator; CPU-per-op is not.

### Verdict (b) — Linux

- **R3 (symmetric cores, connection pinned to its slot's core): CONFIRMS the GO.** With
  every thread hard-pinned and the box quiet (load 0.09 at start, 4.2 % spread), colocated
  still wins 2.54× on throughput and 7.6× on p99 while using half the threads and 4.7×
  less CPU per op. **The macOS result was not a scheduler artifact.**
- **R4 (current-thread runtime per core): CONFIRMS the GO WITH CONDITIONS, with a
  softened number.** R4-without-R3 is a **2.5×** regression on Linux, not 3.7×. The
  dependency-order ruling is unchanged — shipping R4 before R3 still ships a large
  regression — but the spike report's "3.7×" should be quoted as "2.5–3.7× depending on
  platform" rather than as a constant.
- **Does colocated's win survive hard pinning? Yes, and it grows.** Pinning helps
  *every* shape (it is the ideal case for work-stealing too — no migration, no
  cross-socket traffic), and colocated still pulls further ahead: 2.27× → 2.54× vs
  today, 2.81× → 3.61× at equal threads, 1.85× → 2.37× vs the locality control. The
  shard-affine control giving only **+7.4 %** on Linux (vs +22 % on macOS) tightens the
  argument: key locality explains almost none of the gap. The win is the elimination of
  cross-thread wakeups.
- **Cross-slot hop cost: 6.35×, not 8×.** Still an order-of-magnitude-adjacent penalty
  per foreign-core hop. R3's "cross-slot ops pay a copy" framing remains an
  understatement.

---

## (a) Per-shard arenas — R2 / R8

Three full arena runs at `_RJEM_MALLOC_CONF=narenas:1` plus one default-`narenas`
control. **E1, E1b, E1c and E4 produced byte-identical output in all four runs** — the
attribution experiments are fully deterministic, so no median is needed. E3 and E5 are
timing-based and are reported as medians of the three `narenas:1` runs.

### Finding: the env var is `_RJEM_MALLOC_CONF`, not `MALLOC_CONF`

`tikv-jemalloc-sys` configures jemalloc with `--with-jemalloc-prefix=_rjem_`, which
also prefixes the config env var. Evidence, from the same binary in the same suite:

| run | env set | `opt.narenas` (readback) | `arenas.narenas` at start |
| --- | --- | ---: | ---: |
| control | *(nothing)* | 32 | 33 |
| earlier attempt | `MALLOC_CONF=narenas:1` | **32** | **33** — silently ignored |
| this suite | `_RJEM_MALLOC_CONF=narenas:1` | **1** | **2** |

The binary now prints `opt.narenas (effective)` on every run so the setting can never be
assumed again. **Action:** wherever FrogDB documents or sets `narenas:1`, it must use
`_RJEM_MALLOC_CONF` (or set it via `#[export_name = "_rjem_malloc_conf"]`, jemalloc's
compile-time alternative, which cannot be forgotten at deploy time and is the safer
choice for a shipped binary). The spike report's follow-up #1 and any PRD/R2 text naming
`MALLOC_CONF` are wrong as written.

### E1/E2 — attribution

| shard | arena | requested B | small.allocated | large.allocated | stats / requested | macOS |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 2 | 15,667,200 | 2,583,616 | 13,631,488 | 1.0350 | 1.0358 |
| 1 | 3 | 62,668,800 | 10,250,240 | 53,477,376 | 1.0169 | 1.0169 |
| 2 | 4 | 70,041,600 | 30,720,000 | 40,910,848 | 1.0227 | 1.0227 |
| 3 | 5 | 291,635,200 | 81,920,000 | 211,832,832 | 1.0073 | 1.0073 |

Three of four ratios match macOS to four decimal places.

### E1b — cross-bleed

| arena | observed | spines | expected | **delta** | macOS delta | resident |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2 | 16,215,104 | 529,408 | 16,196,608 | **+0.114 %** | +0.192 % | 17,276,928 |
| 3 | 63,727,616 | 1,058,816 | 63,727,616 | **0.000 %** | 0.000 % | 65,929,216 |
| 4 | 71,630,848 | 1,589,248 | 71,630,848 | **0.000 %** | 0.000 % | 75,227,136 |
| 5 | 293,752,832 | 2,117,632 | 293,752,832 | **0.000 %** | 0.000 % | 299,794,432 |

**Byte-exact attribution reproduces.** Zero cross-bleed. Arena 2's residual is tcache
residue and is *smaller* on Linux (+0.114 % vs +0.192 %), consistent with the smaller
tcache fill measured in E4.

`resident / allocated` is 1.021–1.066 here vs 1.15–1.23 on macOS — this box's jemalloc
holds far less dirty overhead. The R13 fragmentation metric is still per-shard and free;
its *typical value* is platform- and workload-dependent, so no threshold should be
calibrated from either run.

### E1c — teardown

| arena | allocated after | resident after | resident recovered |
| ---: | ---: | ---: | ---: |
| 2 | 1,904 | 233,472 | 98.65 % |
| 3 | 16 | 471,040 | 99.29 % |
| 4 | 16 | 1,134,592 | 98.49 % |
| 5 | 16 | 2,760,704 | 99.08 % |

Confirms macOS (98.4–99.2 %). `MEMORY PURGE` is implementable for real; shard teardown
genuinely releases.

### E4 — `thread.arena` × tcache

20,000 × 128 B (exact size class) = 2,560,000 B under test. Identical in all four runs.

| phase | arena A allocated | arena B allocated | macOS A / B |
| --- | ---: | ---: | --- |
| 1. fill + free N on A | 8,192 | 0 | 25,600 / 0 |
| 2. rebind → B, fill N, **no flush** | 12,288 | 2,551,808 | 25,600 / 2,534,400 |
| 3. free + explicit `thread.tcache.flush` | 0 | 48 | 0 / 48 |
| 4. A fill+free, rebind → B, **flush**, fill N | 0 | 2,560,048 | 0 / 2,560,048 |

- **no-flush bleed: 8,192 B of 2,560,000 B = 0.32 %** (macOS: 25,600 B = **1.00 %**)
- **with-flush bleed: 0 B = 0.0000 %** (exact match)
- residue after fill+free on A: **8,192 B** (macOS 25,600 B)

**Both findings reproduce; the magnitude does not.** The tcache bin fill for the 128 B
class is ~3× smaller in this jemalloc build, so the same mechanism leaks a third as much.
Consequences:

1. The *rules* are unchanged and confirmed: bind the arena once at shard-thread start
   before the thread allocates anything, and any rebind must be `thread.arena` **plus**
   `thread.tcache.flush` (which drives bleed to exactly 0 on both platforms).
2. **`specs/memory.md` and issue 03 must not encode "≈1 %" or "25.6 KB".** The overstatement
   is "bounded by the tcache's per-bin capacity", a build- and size-class-dependent
   quantity. A regression test should assert *zero bleed after a flush* (exact, portable)
   and *bounded, non-zero* bleed without one — never a specific byte count.

### E3 — allocation cost under a bound arena

Medians of the three `narenas:1` runs; each cell is itself the min of 3 reps on a fresh
pinned thread.

| size (B) | default ns/op | bound ns/op | **bound / default** | macOS ratio | tcache off | no-tcache / bound |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 | 13.53 | 13.53 | **1.001** | 0.973 | 85.54 | 6.32 |
| 256 | 15.99 | 15.99 | **1.000** | 1.002 | 88.44 | 5.53 |
| 1,024 | 23.00 | 23.21 | **0.997** | 1.002 | 98.96 | 4.36 |
| 8,192 | 120.95 | 123.57 | **0.971** | 0.989 | 415.10 | 3.30 |
| 16,384 | 246.32 | 274.09 | **0.952** | 0.982 | 5,766.64 | 21.19 |
| 65,536 | 5,871.84 | 5,866.79 | **1.011** | 0.986 | 6,079.03 | 1.02 |

**Binding a dedicated arena is free — confirmed, and more tightly than on macOS**
(0.95–1.01× here across three runs; the 64 B and 256 B rows are identical to two decimals,
which is the strongest form of "no cost"). Disabling tcache is *far* more expensive on
Linux than on macOS (6.3× vs 2.9× at 64 B; 21× vs 1.4× at 16 KB), so the "R2 must keep
tcache on" conclusion is strengthened, not weakened.

Two Linux-specific observations, neither affecting a verdict: the ≥16 KB rows cost 4×
what they do on macOS (large-class allocations go to `mmap`/`madvise` under this kernel),
and the 16,384 B tcache-off row is a 21× cliff because that size sits just above this
build's `tcache_max`.

### E5 — cost of reading allocator truth (R8)

Epoch cost is linear in **live arena count**. Medians of three `narenas:1` runs, with the
default-`narenas` control alongside:

| arenas live | µs per `epoch` (`narenas:1`) | arenas live | µs per `epoch` (default `narenas`) |
| ---: | ---: | ---: | ---: |
| 9 | 65.1 | 40 | 103.7 |
| 17 | 145.4 | 48 | 192.9 |
| 41 | 470.2 | 72 | 539.5 |
| 97 | 1,243.2 | 128 | 1,310.8 |

Marginal cost is **≈13.4 µs per arena per epoch** on both curves (macOS: ≈3.4 µs) — a
~4× more expensive epoch per arena on this machine. The two curves are the same line
offset by the 32 baseline arenas, which is precisely why `narenas:1` matters.

| operation | Linux ns/call | macOS ns/call |
| --- | ---: | ---: |
| `stats.arenas.<i>.small.allocated` (by name) | 285 | 146 |
| same, pre-resolved MIB | `NaN` — `mallctlnametomib` still errors | `NaN` |
| `thread.allocated` (thread-local counter) | 51.6 | 23 |

`mallctlnametomib` on `stats.arenas.<i>.*` fails on Linux too, so it is a
`tikv-jemalloc-ctl 0.6` issue, not a platform one. Spike follow-up #4 stands.

### E5b (new) — the actual broker sample at the shipping configuration

The macOS E5 curve started at 48 arenas because jemalloc had already auto-created
`4 × ncpu`. This experiment isolates the number issue 03 actually needs: a fresh process
at `narenas:1` creating **exactly N shard arenas**, each held live by a pinned thread with
~8 MB across four size classes, then timing a *complete broker sample* — one `epoch`
advance plus `2N` `small.allocated`/`large.allocated` reads.

| config | shard arenas | live arenas | epoch only | **full sample** | per-arena | 10 Hz | **20 Hz** | 50 Hz | 100 Hz |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `narenas:1` | 4 | 6 | 40.8 µs | **42.8 µs** | 6.8 µs | 0.043 % | **0.086 %** | 0.214 % | 0.428 % |
| `narenas:1` | 8 | 10 | 80.2 µs | **83.3 µs** | 8.0 µs | 0.083 % | **0.167 %** | 0.416 % | 0.833 % |
| `narenas:1` | 16 | 18 | 163.9 µs | **173.2 µs** | 9.1 µs | 0.173 % | **0.346 %** | 0.866 % | 1.732 % |
| default (control) | 8 | 41 | 165.7 µs | **156.0 µs** | 4.0 µs | 0.156 % | **0.312 %** | 0.780 % | 1.560 % |

Percentages are of **one** core, not of the machine.

Two results:

1. **`narenas:1` is worth taking, but it buys 1.87×, not 5×.** At 8 shards the full
   sample is 83.3 µs with `narenas:1` vs 156.0 µs with jemalloc's default 32 — the spike
   report's "cutting epoch cost by ~5× at 8 shards" **is an overestimate** and should be
   corrected to ~1.9×. (The estimate assumed cost scaled with `40/8`; the real curve has
   a fixed floor plus a per-arena term.)
2. **The stat reads are noise next to the epoch.** `2N` by-name reads add 2.0–9.3 µs to a
   40–164 µs epoch (5–6 %). Optimising the read path (MIB, follow-up #4) is not where the
   cost is; reducing arena count is.

---

## Consumers — what this run says to the three open issues

### 1. Issue 03 (shipped): sampler default 20 Hz, clamp 10–100 Hz — **supported**

The measured cost of one complete `ArenaSampler` pass at the realistic arena count:

| shards | at the 20 Hz default | at the 100 Hz clamp ceiling |
| ---: | ---: | ---: |
| 4 | 0.086 % of one core | 0.43 % |
| 8 | 0.167 % of one core | 0.83 % |
| 16 | 0.346 % of one core | 1.73 % |

**The 20 Hz default is comfortably right** — under a fifth of a percent of one core at
8 shards, on a machine whose epoch is ~4× more expensive per arena than the macOS box the
original estimate came from. **The 100 Hz ceiling is also defensible**: worst case in this
table is 1.73 % of one core at 16 shards. Two conditions on that ceiling:

- The cost is **linear in shard count**, and the clamp is not. At 64 shards, 100 Hz would
  cost roughly 6–7 % of one core. If the shard grid is ever allowed past ~32, the clamp
  should become a function of shard count rather than a constant, or the sampler should
  advance the epoch on a coarser cadence than it reads per-shard stats.
- The sampler must run on a **dedicated thread that is not a shard core**, since `epoch`
  serialises against all arenas; 83 µs of stall injected into a shard's request loop 20×
  a second is a p99.9 event, whereas 83 µs on a broker thread is bookkeeping.

The report's `allocated_upper_bound` / `is_sampled()` framing is confirmed by E4 — but
see the E4 note above: **do not encode the 1 % / 25.6 KB overstatement figure**; it is
0.32 % / 8,192 B on this platform.

Also for issue 03's configuration surface: `narenas:1` must be set through
`_RJEM_MALLOC_CONF` or a `_rjem_malloc_conf` symbol. Setting `MALLOC_CONF` does nothing,
and the failure is silent — worth a boot-time assertion that `opt.narenas` reads back as
expected.

### 2. Issue 02: cross-slot fan-out policy — **hop cost is 6.35×, budget accordingly**

The colocated-vs-cross-thread gap on Linux with everything pinned is **6.35×** throughput
(6,006,329 vs 946,154 at 128 clients) and **9.5× in CPU per op** (0.70 µs vs 6.63 µs).
That is the price of routing a request through a foreign core, measured with the
scheduler confound removed.

For the thread-per-core wiring this implies:

- A fan-out protocol that produces **exactly one hop per foreign core involved** is the
  correct target, and the CPU-per-op number is the right budget line: each foreign core
  touched adds ~6 µs of CPU, versus ~0.7 µs for the same work same-core. An MGET of *k*
  keys spread over *k* cores costs ~*k* × 6 µs of system-wide CPU; the same MGET batched
  into one hop per distinct core costs ~(distinct cores) × 6 µs. The saving is entirely
  in *deduplicating cores*, not in shrinking payloads.
- Tail latency is the harder constraint: the cross-thread shape's p99.9 at 512 clients is
  **4,571 µs** against colocated's **404 µs** (11×). A fan-out that waits on the slowest
  of *k* hops inherits that tail multiplied by a max-of-*k* order statistic. The policy
  should cap fan-out width or degrade to a bounded-concurrency scatter rather than
  issuing unbounded parallel hops.
- The pinning evidence explains the mechanism and rules out alternatives: the cross-thread
  shards ran at **76.7–80.3 %** CPU and their clients at **72.1–72.6 %**, all pinned and
  never migrating. The missing 20–28 % is park/unpark stall, not migration, not cache
  misses, not contention — so batching (fewer wakeups) is the lever that works, and
  affinity tuning is not.

### 3. Does tpc colocated's win survive hard pinning? — **yes, it grows**

Answered in [Verdict (b)](#verdict-b--linux). 2.27× → **2.54×** vs today's shape,
2.81× → **3.61×** at equal thread count, 1.85× → **2.37×** vs the shard-affine locality
control, p99 7.53× → **7.59×** better, and a new pinning-only measurement: **4.7× less
CPU per op**. The macOS caveat "nothing in (b) is pinned; absolute numbers need a Linux
re-run" is discharged.

---

## Corrections this run makes to [spike-report.md](spike-report.md)

1. **Follow-up #1 / R2 configuration:** the variable is **`_RJEM_MALLOC_CONF`**, not
   `MALLOC_CONF`, under `tikv-jemalloc-sys`. The plain name is silently inert.
2. **E5's "cutting epoch cost by ~5× at 8 shards"** overestimates. Measured: **1.87×**
   (83.3 µs vs 156.0 µs for a full sample). Still worth doing.
3. **E4's "1.00 % bleed / 25,600 B residue"** is platform-specific. On aarch64 Linux it
   is **0.32 % / 8,192 B**. The portable statements are "bounded by tcache per-bin
   capacity" and "exactly 0 after `thread.tcache.flush`".
4. **"R4 alone is a 3.7× regression"** is **2.5×** here. Quote it as a range; the ruling
   is unaffected.
5. **"~8× hop cost"** is **6.35×** here. Same conclusion, quote as 6–8×.
6. **Contradiction #3's "≈2.5 µs per arena per epoch"** is **≈13.4 µs** on this machine.
   Per-arena epoch cost is a 4×-variable platform constant; the broker's budget must be
   derived from a measured sample, not a hardcoded constant.

## Caveats

- **8 cores, 4 shards.** The colocated shape uses 4 CPUs and the work-stealing shapes use
  8, exactly as on macOS — the ratios are per-shape, not per-core-count. Nothing here
  measures behaviour at 32+ shards, which is where the E5b linearity note bites.
- **Single-socket, single NUMA node, no SMT.** The most favourable possible topology for
  work-stealing. On a 2-socket box the colocated advantage should be larger, not smaller.
- **The shard body is still a `HashMap`** — no RESP parsing, no persistence, no expiry
  sweep, no replication feed. Real per-op work compresses every ratio in (b). The sign and
  the CPU-per-op *gap* are what this run establishes.
- **The colocated shape still assumes every request is same-slot.** Sizing the cross-slot
  fraction is spike follow-up #2 and remains open (explicitly out of scope for this run).
- **E5b is a single run per configuration**, not a median of three; the epoch measurement
  itself is an internal best-of loop and the three full-suite E5 runs agree to within 8 %,
  so the sample-cost table is quoted to two significant figures only.
- **`_RJEM_MALLOC_CONF` was unset for the runtime bench.** Runtime shape and arena count
  are independent, and the arena suite is where `narenas` matters; nothing in (b) reads
  allocator stats.
