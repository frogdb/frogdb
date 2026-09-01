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

Issues: none yet — design-only ruling; issues get filed at implementation kickoff.
