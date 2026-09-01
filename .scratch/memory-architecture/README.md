# Memory architecture — ideal end-state ruling

State: active

PRD: [PRD.md](PRD.md) — the ruled ideal memory-management architecture (rulings
R1–R15): thread-per-core with per-shard jemalloc arenas, shared-nothing symmetric
cores, Dashtable keyspace with inline small values, block encodings, per-core memory
broker with subsystem budgets, segment-integrated 2Q eviction, budget-charged
RocksDB, pooled network buffers, zero-copy parse path, and a future locked
`specs/memory.md`.

Survey report: https://claude.ai/code/artifact/952c39ae-b28f-40fe-bd34-9105f9537819

Phase-1 de-risk spike: [spike-report.md](spike-report.md) — measured answers for R2
(per-shard arenas), R3/R4 (runtime shape), and turmoil fidelity, with go/no-go verdicts.
Prototype code: [spike/](spike/) (throwaway, not a workspace member).
Linux validation: [spike-report-linux.md](spike-report-linux.md) — both benches re-run on
aarch64 Linux with hard `sched_setaffinity` pinning and `narenas:1`. Every verdict holds;
it corrects six numbers and finds that the jemalloc config env var is `_RJEM_MALLOC_CONF`.

Spec draft: [`specs/memory.md`](../../specs/memory.md) — `Status: DRAFT`, scope statement and
invariant vocabulary only, deliberately zero FM rows (a row arrives with its forcing test). It
becomes the fifth locked area under R15 once the broker/table crates exist and pass their
mutation gate.

Boundary ADR: [`adr/0006-memory-architecture-seams.md`](../../adr/0006-memory-architecture-seams.md)
— the three seams drawn before the first commit: the `ShardExecutor` (real thread-per-core vs
turmoil sim), the `Budget` chokepoint and its future seam lint, and arena ownership
(one per shard thread, bound once, sampled upper-bound accounting).

Issues: 01-06 filed (phases 1-2); phases 3-6 filed after these land.
