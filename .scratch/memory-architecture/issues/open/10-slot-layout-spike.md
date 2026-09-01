# 10: R5 slot-layout spike — segmented-table prototype with inline values

Status: ready-for-agent
Type: AFK
Origin: memory-architecture PRD phase filing, 2026-09-01 — PRD.md R5/R6/R9; scoped by the
Dashtable grill (spike-first; table + inline word + reserved eviction-bit space; griddle
baseline on macOS; issues 11–12 filed after the report)
Area: benchmarking / a new throwaway crate under `.scratch/memory-architecture/`

## Why

R5 is the riskiest unbuilt piece of the architecture: an unsafe core structure whose layout
three rulings depend on. The PRD's open-questions list names the inline-value threshold as
"needs slot-layout prototype" — and the threshold *is* the slot word format, which sets
bucket capacity, which sets every occupancy and probe-length number the table produces. Two
other rulings are downstream of the same layout: R6's inline-small-values half is the slot
word's tag scheme, and R9's 2Q eviction bits live in the per-segment metadata this spike
lays out. Designing the layout inside a production crate means making those decisions twice.

Same discipline that de-risked R2–R4: throwaway prototype, measured verdicts, a report that
sizes the real issues (the [phase-1 spike](../../spike-report.md) and its
[Linux validation](../../spike-report-linux.md) are the precedent and the template).

## What to build

A throwaway crate (sibling of [`spike/`](../../spike/), **not** a workspace member) prototyping
the R5 shape and answering, with numbers:

1. **Slot word format + inline threshold.** Tagged 8-byte slot words; sweep the inline
   encoding candidates (at minimum: no inlining as control, small-int inlining, short-string
   inlining at the widths the tag scheme allows — including whether a wider slot buys enough
   inline coverage to pay for the halved bucket capacity). Measure against realistic
   key/value size distributions (short counters, session-token-sized strings, the
   redis-feel workload shapes), reporting bytes/entry, bucket occupancy at load, and probe
   lengths. This produces the threshold *ruling* the PRD left open.
2. **Segment + directory layout.** Cache-line-sized buckets, extendible-hash directory
   (~1 byte/entry overhead target), incremental segment splits. Measure split cost (worst
   single-operation stall, not just amortized) and directory growth behavior. Study
   Dragonfly's Dashtable (segment of regular + stash buckets, bucket-level metadata) as the
   reference implementation before inventing; deviations get a sentence each.
3. **SCAN cursor scheme.** Cursors map to segment ids and must survive splits — a
   Redis-compat hard requirement, so it gets an executable proof: a test that SCANs to
   completion while splits happen mid-iteration and shows every pre-existing key exactly
   once (the guarantee Redis's reverse-binary cursor gives), not an argument in prose.
4. **Reserved R9 space.** Per-segment metadata carries *sized, documented* space for the 2Q
   eviction bits (queue membership, per-segment counters) — layout only, no eviction logic.
   Issue 12 designs 2Q against this layout; the spike just proves the bits fit without
   growing the segment header past its cache-line budget.
5. **Baseline.** Every comparative number is against today's shipped
   `griddle::HashMap<Bytes, Entry>` under the same distributions: bytes/entry (including
   `Entry`'s overhead), lookup/insert cost, and iteration. macOS laptop is the venue —
   these are structural questions, not platform ones. A Linux re-run is a follow-up issue
   **only** if a perf verdict is close enough that absolutes matter (the issue-04 pattern).
6. **Report.** `spike-report-table.md` in the feature directory, linked from the README:
   verdicts (GO/NO-GO on the R5 shape), the inline-threshold ruling, the chosen cursor
   scheme, the segment-metadata layout with the reserved R9 bits, and the named consumers —
   issue 11 (table + inline crate implementation) and issue 12 (segment-integrated 2Q),
   which get drafted from this report's numbers.

## Out of scope

2Q eviction logic (issue 12, after the layout settles). The production crate, its miri/fuzz
budget, and the griddle swap-out (issue 11 — the spike is throwaway and may take shortcuts a
production unsafe core cannot). R6's heap-value refcount/COW/snapshot half (drafted at
issue-11 time; the spike only settles the inline half). Concurrency — the table is per-shard
single-threaded by R2/R3, so the spike is single-threaded too.

## Depends on

Nothing — the shipped table is untouched. Blocks issues 11 and 12, which are deliberately
not drafted until this reports.
