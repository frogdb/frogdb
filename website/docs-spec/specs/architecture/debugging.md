# Spec: architecture/debugging.md
Status: update (remove nonexistent tools — loom, tokio-console, PortfolioRunner —
and correct DEBUG/LATENCY surfaces to what is implemented)
Audiences: A3 (architecture-curious), A5 (contributors)

Goal: The reader knows the real toolset for investigating FrogDB behavior: the
implemented `DEBUG` subcommands (and which dangerous ones are explicitly rejected),
the `LATENCY` framework with its actual event types, `MONITOR`, per-shard
diagnostics, the always-on USDT/DTrace probes, eBPF usage, the shuttle
deterministic-concurrency test harness, and the in-house `tokio-coz` causal
profiler (which deserves its own subsection). Every tool named on this page must
exist in the repo. The reader leaves able to reproduce each technique and knows
which are FrogDB-specific.

Not in scope:
- The debug web UI / HTTP JSON API / diagnostic bundles as an operator feature —
  Operations → Diagnostics (the merged debug-ui + performance page). This
  architecture page covers the developer/contributor debugging surface; link to
  Operations for the UI. (Note `DEBUG BUNDLE` exists as a command — mention it,
  but the UI is Operations' home.)
- Metrics catalog — Reference → Metrics (S3).
- The full testing pyramid — Compatibility & Correctness → Testing methodology
  (link). This page covers only the debugging-oriented tools.

Sources of truth (author MUST read; the current page names three tools that do not
exist and misstates the DEBUG/LATENCY surfaces — verify everything):
- `frogdb-server/crates/server/src/connection/dispatch.rs` (`dispatch_debug`) and
  `.../connection/handlers/debug.rs` — the real DEBUG subcommand set.
- `frogdb-server/crates/commands/src/generic.rs` — the registered `DebugCommand`
  for `OBJECT`.
- `frogdb-server/crates/server/src/connection/handlers/latency.rs` and
  `frogdb-server/crates/core/src/latency.rs` — `LatencyEvent` enum (the six real
  event types) and LATENCY subcommands (incl. FrogDB-specific `LATENCY BANDS`).
- `frogdb-server/crates/server/src/monitor.rs` + `frogdb-server/crates/config/src/monitor.rs`
  — MONITOR broadcast channel (default capacity 4096), AUTH redaction.
- `frogdb-server/crates/core/src/probes.rs` — USDT probe provider `frogdb`, gated
  by the `usdt-probes` cargo feature; the full probe list and their fire sites.
- root `Cargo.toml` — `shuttle = { version = "0.8" }`; `usdt = "0.6"`. Confirm
  there is **no** `console-subscriber` / `tokio-console` and **no** `loom`.
- `frogdb-server/crates/types/src/sync.rs` and
  `frogdb-server/crates/core/tests/concurrency.rs` — shuttle sync-type swapping and
  the real test entrypoints (`shuttle::check_pct`, `shuttle::check_random`; run via
  `cargo test -p frogdb-core --features shuttle --test concurrency`).
- `frogdb-server/crates/tokio-coz/` — README.md + `src/lib.rs` and modules
  (config, delay, experiment, hooks, progress, reporter, results, span_tracker,
  state) — the in-house causal profiler.

Existing content: current `architecture/debugging.md`. The USDT probe table,
MONITOR description, DTrace/eBPF sections, and build-flags are largely correct.
FACTUAL DISCREPANCIES to fix:

1. **Remove loom entirely.** loom is NOT a dependency and appears nowhere in the
   repo (Cargo.toml/Cargo.lock/source). The "Concurrency Testing with loom" block
   and the "scales better than loom" comparison must be deleted. The real tool for
   this purpose is **shuttle**; the section should describe shuttle only. (The word
   "loom" survives in the repo only as "Virtual Lock **Loom**" = VLL, unrelated.)
2. **Remove tokio-console.** There is no `tokio-console` cargo feature and no
   `console-subscriber` dependency. Delete the "tokio-console" subsection and its
   warning table (`lost waker`/`never yielded`/`very long poll`). If a runtime-
   task-introspection tool is ever added, document it then.
3. **Remove `PortfolioRunner`.** It does not exist. Describe the real shuttle usage:
   `check_pct` / `check_random`, sync-type swapping under `cfg(all(feature =
   "shuttle", test))`, and the `cargo test … --features shuttle` invocation.
4. **DEBUG subcommand list is wrong in specifics:**
   - `DEBUG DUMP-VLL-QUEUE` does not exist → the real command is **`DEBUG VLL
     [shard_id]`**.
   - `DEBUG DUMP-CONNECTIONS` does not exist → remove it.
   - `DEBUG SET-ACTIVE-EXPIRE` IS implemented → move it out of the "not
     implemented" list into the implemented set.
   - Implemented subcommands the page omits (add the relevant ones): `TRACING
     STATUS|RECENT`, `VLL`, `PUBSUB LIMITS`, `BUNDLE GENERATE|LIST`, `RESP3`,
     `SET-ACTIVE-EXPIRE`, `HELP`, and the assertion helpers
     `KEYSIZES-HIST-ASSERT` / `ALLOCSIZE-SLOTS-ASSERT` (contributor/test helpers —
     label as such). `OBJECT`, `STRUCTSIZE`, `SLEEP`, `HASHING` are real.
   - The truly-rejected dangerous commands: `SEGFAULT`, `RELOAD`,
     `CRASH-AND-RECOVER`, plus `OOM` and `PANIC` (the page omits the last two) —
     rejected with "not supported (unsafe command)". Correct this list.
   - Note `SLEEP` is gated behind `server.enable-debug-command`; state the gate.
   - REQUIREMENT (per PLAN §6 "no feature documented before implemented"): the
     DEBUG list MUST be implemented-only. Generate it from `dispatch_debug` at
     write time; do not carry over the current list.
5. **LATENCY event types are wrong.** The real `LatencyEvent` set is exactly:
   `command`, `fork`, `aof-fsync`, `expire-cycle`, `eviction-cycle`, `snapshot-io`.
   Remove the fabricated `fast-command`, `persistence-write`, `shard-message`,
   `scatter-gather`. LATENCY LATEST/HISTORY/DOCTOR/RESET are correct; add the
   FrogDB-specific `LATENCY BANDS`.
6. MONITOR: capacity 4096 (from config `default_channel_capacity`) and AUTH
   redaction are correct — keep. Note the redaction keys on `AUTH` only (a `HELLO
   … AUTH …` inline password is not specially redacted) if the page makes a broad
   redaction claim.

Structure (H2/H3 outline):

## Built-in diagnostic commands
### DEBUG
- Implemented-only subcommand list (generated from `dispatch_debug`), grouped:
  Redis-compatible (OBJECT, STRUCTSIZE, SLEEP, SET-ACTIVE-EXPIRE, …) and
  FrogDB-specific (HASHING, VLL, PUBSUB LIMITS, BUNDLE, TRACING, RESP3, assertion
  helpers). Note the `enable-debug-command` gate for SLEEP. List the explicitly-
  rejected dangerous commands and why.
### LATENCY
- LATEST/HISTORY/DOCTOR/RESET + `LATENCY BANDS`; the six real event types.
### MEMORY
- Keep MEMORY DOCTOR + per-shard memory analysis via INFO (verify the INFO field
  names against source before reproducing the sample output).
### MONITOR
- Bounded broadcast (4096), zero-overhead when no subscribers, AUTH redaction.

## Per-shard diagnostics
- Per-shard stats (queue depth, commands processed, scatter requests) and
  scatter-gather tracing via debug logging — verify field names/log targets.

## External introspection
### Build flags
- The debug/optimized/frame-pointer build lines (verify they are current).
### USDT / DTrace probes
- The `frogdb` provider, `usdt-probes` feature gate, always-on/zero-overhead when
  no tracer attached. The full real probe list (command-start/done,
  shard-message-sent/received, key-expired/evicted, memory-pressure, wal-write,
  scatter-start/done, pubsub-publish, connection-accept). FrogDB-specific.
### eBPF / bpftrace (Linux)
- uprobes, allocation probes, frame-pointer build requirement.

## Deterministic concurrency testing (shuttle)
- What shuttle does, sync-type swapping under the `shuttle` feature,
  `check_pct`/`check_random`, and the `cargo test … --features shuttle --test
  concurrency` entrypoint. Link Testing methodology. NO loom, NO PortfolioRunner.

## Causal profiling (tokio-coz) — FrogDB in-house
- Dedicated subsection. What causal profiling is (coz methodology: "what if this
  code were X% faster — how much does end-to-end improve?") and why stock coz does
  not fit tokio's M:N scheduling. What tokio-coz does differently: profiles at
  tracing-span / task-poll granularity; injects real delays into non-target task
  polls via `std::thread::sleep` (blocking the worker thread) to preserve the coz
  invariant; wires into the runtime via unstable poll hooks (`on_task_spawn`,
  `on_before/after_task_poll`, `on_task_terminate`) plus a tracing layer; requires
  `RUSTFLAGS="--cfg tokio_unstable"`. API: `CausalProfiler::new(ProfilerConfig)`,
  `.start()`, `.report()`; progress points, experiment loop over random speedups.
  State honestly that it is an experimental/research-grade standalone crate (its
  README is a design document; it is not currently wired into the running server,
  and the README flags an open question about causal validity under M:N
  scheduling) — do not overstate maturity.

## Debug logging
- The env-var and runtime `CONFIG SET loglevel` patterns (verify the env-var names
  and targets against source).

Generated data: none embedded. Probe/metric/config names should trace to source or
the Metrics reference (S3); no hardcoded counts.

Drift guards:
- S7 code-path check covers `dispatch.rs`, `handlers/debug.rs`, `handlers/latency.rs`,
  `core/src/latency.rs`, `monitor.rs`, `core/src/probes.rs`, `crates/tokio-coz/...`,
  `types/src/sync.rs`, `core/tests/concurrency.rs`.
- TOOL-EXISTENCE GUARD: every external tool/feature named here must correspond to a
  real cargo feature or dependency. Recommend a CI check (flag as S8-adjacent) that
  greps this page for tool/feature names (`--features X`, dependency names) and
  asserts each exists in a Cargo.toml — this exact class of drift produced the
  loom/tokio-console/PortfolioRunner errors.
- The DEBUG and LATENCY surfaces should ideally be generated from the command
  registry (S1 covers command names; DEBUG subcommands and LATENCY events are
  finer-grained) — until then, a reviewer regenerates the DEBUG list from
  `dispatch_debug` and the event list from `LatencyEvent` on each edit.
