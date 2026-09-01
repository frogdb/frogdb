# 04: re-run the phase-1 spike benches on Linux with hard pinning and `narenas:1`

Status: done
Type: AFK
Origin: memory-architecture phase-1 spike, 2026-08-31 — [spike-report.md](../../spike-report.md)
"Follow-ups" item 1, forced by the "Caveats" section
Area: benchmarking / `.scratch/memory-architecture/spike/`

## Why

Every number in [spike-report.md](../../spike-report.md) was taken on an Apple M1-class macOS
box, and the report says plainly what that costs:

- **No hard core pinning.** macOS has no strict affinity API and Apple silicon ignores
  `thread_policy_set` affinity tags, so **nothing in §(b) was pinned**. The comparison is
  therefore about runtime *shape* — which is the architectural question, and is what the GO
  verdicts rest on — but the absolutes are unvalidated on the platform FrogDB ships to.
- **Noisy machine.** Other agents were compiling during the runtime bench (load average
  3.3–3.8 on 10 cores). Medians of 3 were taken and an earlier quieter run reproduced the same
  ordering with *larger* ratios (colocated/mt 3.1× vs 2.3×), so the ratios are directionally
  solid and the absolutes are soft.
- **jemalloc's default arena count.** §(a) E5's epoch-cost curve was measured against 48–136
  auto-created arenas, not the `narenas:1` + N-explicit-arenas configuration
  [issue 03](../) will actually ship. The per-sample cost that issue budgets against
  (~2.5 µs/arena) is an extrapolation until it is measured at a realistic arena count.

The verdicts do not depend on this run. The *sizing decisions* in issues 02, 03 and 05 do —
sampler frequency, cross-slot fan-out policy, and how much headroom a budget needs.

## What to run

The spike crate already exists at [`spike/`](../../spike/) (throwaway, deliberately not a
workspace member). Re-run it unchanged except for the platform affordances:

1. **`spike/src/bin/runtime.rs`** — the five shapes (today's 8-worker work-stealing, 4-worker
   work-stealing, shard-affine-keys control, `tpc` cross-thread, `tpc` colocated) at 32 / 128 /
   512 clients, with real `sched_setaffinity` pinning for the shard threads and the client
   threads. The `mt + shard-affine keys` control is not optional: it is the row that rules out
   cache locality as the explanation for the colocated win, and a Linux run without it proves
   less than the macOS run did.
2. **`spike/src/bin/arena.rs`** — E1/E1c/E3/E4/E5 under `MALLOC_CONF=narenas:1` with exactly N
   arenas created, so E5's epoch-cost curve is measured at the arena count the design will run
   at rather than at 48–136.
3. A **quiet box**: no concurrent builds, load average recorded before and after each run,
   medians of at least 3.

## Venue

The **aarch64 Linux testbox** (`just tb-warmup` / `just tb-run`, see the `blacksmith-testbox`
skill and CLAUDE.md "Execution mode") is the intended venue: it is the platform that matches
production, it has real affinity, and it keeps a multi-minute benchmark off a laptop that other
agents are sharing — which is the exact confound that softened the original numbers. A dedicated
CI runner is an acceptable substitute if it is not shared for the duration. **State the execution
mode in the dispatch prompt**; a session in local mode must not silently reach for a testbox, and
the `tb-*` recipes refuse to run in local mode anyway.

Note the spike crate is not in the workspace, so it needs its own build invocation on the box
(the `tb-run` command string, not a `just test` target). One `tb-run` at a time per worktree.

## Acceptance criteria

- [ ] Both benches run to completion on aarch64 Linux with shard threads verifiably pinned
      (report the achieved CPU per thread, not just the intent).
- [ ] The runtime bench reports all five shapes at all three concurrency levels, medians of ≥3,
      with load average recorded.
- [ ] The arena bench reports E5's epoch cost at the `narenas:1` + N-arena configuration, and
      E1/E4's attribution and rebind-bleed figures reproduce (or a discrepancy is written up —
      a *failure* to reproduce byte-exact attribution on Linux is a finding that changes
      issue 03, not a run to be retried until it agrees).
- [ ] A results section is appended to [spike-report.md](../../spike-report.md) — or a sibling
      `spike-report-linux.md` linked from it and from the feature README — reporting **absolute**
      ops/sec and percentiles alongside the macOS ratios, and stating for each of §(a)/§(b)'s
      verdicts whether Linux confirms, weakens, or contradicts it.
- [ ] Three consumers are named explicitly in that write-up, because they are what the run is
      for: the sampler frequency issue 03 should use, the cross-slot fan-out policy issue 02
      needs, and whether `tpc colocated`'s win survives hard pinning at all.
- [ ] Machine spec, kernel, rustc version, jemalloc version and `MALLOC_CONF` recorded, as the
      original report does.

## Out of scope

Changing the spike code's *shape* (adding a tunable cross-slot fraction is
spike-report "Follow-ups" item 2 and its own issue; a `MALLOC_CONF` decay study is item 5).
Fixing the `mallctlnametomib` error is folded into issue 03. This issue re-runs what exists on
a platform where the numbers mean something.

## Depends on

Nothing — the spike crate is standalone and independent of issues 01–03. It should ideally run
*before* 03 picks a sampler frequency, but nothing blocks it.

## Resolution (2026-09-01)

Run on a Blacksmith 8-vCPU Ampere-1a aarch64 testbox (Ubuntu 24.04, kernel 6.6.141,
jemalloc 5.3.0 via tikv-jemalloc-sys), hard `sched_setaffinity` pinning verified per thread
(new `spike/src/pin.rs` prints an allowed==observed==intended evidence table per run). Full
results: [spike-report-linux.md](../../spike-report-linux.md), linked from the macOS report
and the feature README. All acceptance criteria met; every §(a)/§(b) verdict addressed.

**Every GO verdict confirms.** R2 attribution byte-identical across 4 runs; R3's colocated
win *grows* under pinning (2.27×→2.54× vs today, 3.61× at equal threads, p99 7.59× better),
plus a pinning-only figure: 4.7× less CPU per op than today's shape. R4-alone softens to a
2.5× regression (was 3.7×) — ruling unchanged.

**Six corrections** to macOS figures (report §Corrections): env var is `_RJEM_MALLOC_CONF`
not `MALLOC_CONF` (plain name silently ignored — `opt.narenas` readback proves it);
`narenas:1` buys 1.87× not ~5×; E4 bleed 0.32 %/8,192 B not 1.00 %/25,600 B (bound is
"tcache per-bin capacity", never a byte count); hop 6.35× not ~8×; per-arena epoch ≈13.4 µs
not 2.5 µs (platform constant — measure, don't hardcode).

**Three consumers answered:** issue 03's 20 Hz default / 10–100 Hz clamp supported (0.167 %
of one core at 8 shards; conditions: clamp is not shard-count-aware — revisit past ~32
shards — and the sampler must stay off shard cores); issue 02's fan-out budget is ~6 µs CPU
per foreign core touched, one hop per distinct core, cap fan-out width (cross-thread p99.9
at 512 clients is 11× colocated's — park/unpark, not locality, per pinning evidence);
colocated survives hard pinning and grows.

Shipped code needed no change — issue 03 already sets `narenas:1` via the compile-time
`_rjem_` symbol with an `opt.narenas` readback (`malloc_conf::applied()`). Stale prose
corrected at merge: adr/0006 §3 accounting paragraph, `malloc_conf.rs` env-var note,
`telemetry/jemalloc.rs` comments that encoded macOS-specific byte/µs figures.

Deviations (all noted in the report's caveats): E5b added as a new isolated-process
experiment (single run per config, internal best-of, E5 medians agree within 8 %); runtime
bench ran with `_RJEM_MALLOC_CONF` unset (shape and arena count independent); pinning
instrumentation added to both binaries — no shape changes.
