# memarch-spike — THROWAWAY phase-1 de-risk prototype

**This is not production code.** It is a scratch prototype written to answer three
questions about `.scratch/memory-architecture/PRD.md` before any of R2/R3/R4 is
scheduled. Nothing here is meant to be merged into the server; it will be deleted
once the answers are folded into the implementation plan.

**Not a workspace member.** `Cargo.toml` carries an empty `[workspace]` table, so
cargo treats this directory as its own workspace root with its own `Cargo.lock`.
It does not participate in — or slow down — FrogDB workspace builds, and the
`just` recipes do not see it. Use plain `cargo` from inside this directory.

## What is here

| Target | Question | PRD ruling |
| --- | --- | --- |
| `src/bin/arena.rs` | Can per-shard jemalloc arenas be created, bound per thread, and read back accurately? What do they cost? | R2 |
| `src/bin/runtime.rs` | Pinned current-thread runtimes vs one work-stealing runtime | R3, R4 |
| `src/lib.rs` + `tests/sim_shard.rs` | Can a thread-per-core architecture still be driven deterministically under turmoil? | R2/R3 simulation fidelity |

## Running

```bash
cargo run --release --bin arena       # E1-E5 arena experiments
cargo run --release --bin runtime     # runtime-shape microbench
cargo test --release --test sim_shard -- --nocapture
```

Findings and verdicts: [`../spike-report.md`](../spike-report.md).
