---
title: "Debugging Guide"
description: "The contributor's toolset for investigating FrogDB behavior: DEBUG/LATENCY/MONITOR, USDT probes, shuttle, and the tokio-coz causal profiler."
sidebar:
  order: 15
---
The contributor's toolset for investigating FrogDB behavior. This page covers the
developer- and contributor-facing debugging surface. For the operator-facing debug
web UI, HTTP JSON API, and diagnostic bundles, see
[Operations → Diagnostics](/operations/diagnostics/); for the metric catalog, see
[Reference → Metrics](/reference/metrics/); for the full test pyramid, see
[Testing methodology](/compatibility/testing-methodology/).

---

## Built-in diagnostic commands

### DEBUG

The implemented `DEBUG` subcommands are dispatched in
`frogdb-server/crates/server/src/connection/debug_conn_command.rs`. Redis-compatible
subcommands:

| Subcommand | Purpose |
|------------|---------|
| `OBJECT <key>` | Internal representation of a key (encoding, refcount, idle time). Registered as the `OBJECT` command in `frogdb-server/crates/commands/src/generic.rs` |
| `STRUCTSIZE` | Sizes of internal data structures, for memory estimation |
| `SET-ACTIVE-EXPIRE 0\|1` | Toggle the active-expiry cycle |
| `SLEEP <seconds>` | Block the server briefly (gated — see below) |
| `HELP` | List subcommands |

FrogDB-specific subcommands, useful for diagnosing the shard/VLL machinery:

| Subcommand | Purpose |
|------------|---------|
| `HASHING <key>` | Which internal shard (and slot) a key maps to |
| `VLL [shard_id]` | Dump the VLL transaction queue for a shard |
| `LOCKTABLE` | Dump the per-shard VLL lock table |
| `WAITQUEUE` | Dump the per-shard blocking wait queue |
| `PUBSUB LIMITS` | Pub/sub subscription limits |
| `TRACING STATUS \| RECENT [count]` | Tracing state and recent spans |
| `BUNDLE GENERATE [DURATION <s>] \| LIST` | Generate or list diagnostic bundles |
| `RESP3 BIGNUMBER \| BOOLEAN \| VERBATIM` | Emit a RESP3 type, for client testing |
| `MEMORY-CHECK`, `EXPIRY-INDEX-CHECK` | Internal consistency checks |
| `EXPIRE-BACKDATE <key> <ms>` | Rewrite a key's TTL to `<ms>` in the past so it is already expired, with no wall-clock wait — deterministic, instant TTL elapse for tests (notably turmoil sims on a virtual clock). Rewrites only the deadline; the next read/sweep performs the actual expiry. Errors on a missing key or a key with no TTL |
| `KEYSIZES-HIST-ASSERT`, `ALLOCSIZE-SLOTS-ASSERT` | Contributor/test assertion helpers |

`DEBUG SLEEP` is gated behind the `server.enable-debug-command` config; when
disabled it returns
`ERR DEBUG SLEEP is disabled. Set server.enable-debug-command in the config to allow it.`

**Explicitly rejected.** The dangerous Redis DEBUG subcommands are refused so a
client cannot crash or corrupt the server: `SEGFAULT`, `RELOAD`,
`CRASH-AND-RECOVER`, `OOM`, and `PANIC` all return
`ERR DEBUG <NAME> is not supported (unsafe command)`.

### LATENCY

FrogDB implements Redis-compatible latency monitoring plus FrogDB-specific
extensions. Subcommands (in
`frogdb-server/crates/server/src/connection/observability_conn_command.rs`):
`LATEST`, `HISTORY`, `DOCTOR`, `RESET`, `HELP`, and the FrogDB-specific `BANDS`,
`GRAPH`, and `HISTOGRAM`. `LATENCY BANDS` requires `latency_bands.enabled` in
config.

The tracked event types are exactly those in the `LatencyEvent` enum
(`frogdb-server/crates/core/src/latency.rs`):

| Event | Description |
|-------|-------------|
| `command` | Command execution latency |
| `fork` | Fork operation (background save) |
| `aof-fsync` | AOF fsync latency |
| `expire-cycle` | Active-expiry sweep duration |
| `eviction-cycle` | Memory-eviction sweep duration |
| `snapshot-io` | Snapshot I/O latency |

### MEMORY

`MEMORY DOCTOR` returns a human-readable memory assessment. Per-shard memory and
key distribution are exposed through `INFO` sections and metrics — see
[Reference → Metrics](/reference/metrics/) for the exact field and metric names
rather than relying on a sample here.

### MONITOR

`MONITOR` streams every command the server processes over a bounded
`tokio::sync::broadcast` channel (default capacity 4096, from the monitor config's
`default_channel_capacity` in `frogdb-server/crates/config/src/monitor.rs`). It is
zero-overhead when no client is subscribed — a single atomic load of the receiver
count per command — and a slow subscriber is dropped forward rather than stalling
the server. `AUTH` command arguments are replaced with `(redacted)`; note this
special-cases the `AUTH` command only, so an inline password passed to
`HELLO … AUTH …` is not redacted.

---

## Per-shard diagnostics

The `DEBUG VLL`, `DEBUG LOCKTABLE`, and `DEBUG WAITQUEUE` subcommands above dump a
single shard's transaction queue, lock table, and blocking wait queue respectively
— the primary way to see what a specific shard worker is doing. Scatter-gather
execution across shards can be traced by raising the log level (see
[Debug logging](#debug-logging)); the trace records the per-shard fan-out of a
multi-shard operation.

---

## External introspection

### Build flags

```bash
cargo build                                                              # full symbols, no optimization
RUSTFLAGS="-C debuginfo=2" cargo build --release                         # symbols with optimizations
RUSTFLAGS="-C force-frame-pointers=yes -C debuginfo=2" cargo build --release  # frame pointers for profiling
```

### USDT / DTrace probes

FrogDB defines a `frogdb` USDT provider in
`frogdb-server/crates/core/src/probes.rs`, gated by the `usdt-probes` cargo feature
(`cargo build --features usdt-probes`). When the feature is off the probe
functions compile away entirely; when on, an attached tracer (DTrace, bpftrace) can
observe them with effectively zero overhead when no tracer is attached. The probes
and their arguments:

| Probe | Arguments |
|-------|-----------|
| `frogdb:::command-start` | (command, key, conn_id) |
| `frogdb:::command-done` | (command, latency_us, status) |
| `frogdb:::shard-message-sent` | (from_shard, to_shard, msg_type) |
| `frogdb:::shard-message-received` | (shard, msg_type, queue_depth) |
| `frogdb:::key-expired` | (key, shard_id) |
| `frogdb:::key-evicted` | (key, shard_id, policy) |
| `frogdb:::memory-pressure` | (used, max, action) |
| `frogdb:::wal-write` | (shard_id, key, bytes) |
| `frogdb:::scatter-start` | (command, shard_count, txid) |
| `frogdb:::scatter-done` | (command, latency_us, shard_count) |
| `frogdb:::pubsub-publish` | (channel, subscribers) |
| `frogdb:::connection-accept` | (conn_id, addr) |

### eBPF / bpftrace (Linux)

The same uprobe-able functions and the USDT probes are reachable from bpftrace;
allocation tracing works through the allocator's probes. Build with frame pointers
(`RUSTFLAGS="-C force-frame-pointers=yes"`) for accurate stacks.

---

## Deterministic concurrency testing (shuttle)

FrogDB uses [shuttle](https://docs.rs/shuttle) for randomized deterministic
concurrency testing (dependency `shuttle = "0.8"`). Under
`cfg(all(feature = "shuttle", test))`, the synchronization primitives in
`frogdb-server/crates/types/src/sync.rs` (`Arc`, `Mutex`, `RwLock`, `AtomicU64`,
`AtomicUsize`) are swapped from `std::sync` to `shuttle::sync`, so the same
production code runs under shuttle's controlled scheduler. The concurrency tests in
`frogdb-server/crates/core/tests/concurrency.rs` drive scenarios with
`shuttle::check_random` and `shuttle::check_pct`:

```bash
cargo test -p frogdb-core --features shuttle --test concurrency
```

shuttle explores many interleavings of a scenario per run and reports a minimal
failing schedule when an assertion breaks. It is the tool for this job in FrogDB —
there is no `loom` dependency and no `tokio-console` integration in the repo. See
[Testing methodology](/compatibility/testing-methodology/) for where this sits in
the overall test strategy.

---

## Causal profiling (tokio-coz) — FrogDB in-house

`frogdb-server/crates/tokio-coz/` is an in-house causal profiler. Causal profiling
answers "if this code were X% faster, how much would end-to-end performance
improve?" — the classic [coz](https://github.com/plasma-umass/coz) method, which
runs a *virtual speedup* experiment: instead of speeding the target up (impossible
mid-run), it delays everything else by the same fraction and measures the effect on
a progress point.

Stock coz operates at the OS-thread level, which does not fit tokio's M:N
scheduling — many tasks are multiplexed onto a few worker threads, so pausing
"other threads" also pauses unrelated tasks sharing those threads and confounds the
causal signal. tokio-coz instead works at tracing-span / task-poll granularity: it
injects a real delay into non-target task polls using `std::thread::sleep` (which
must block the worker thread, not yield, to preserve the coz invariant), driven by
the runtime's unstable poll hooks (`on_task_spawn`, `on_before_task_poll`,
`on_after_task_poll`, `on_task_terminate`) plus a tracing layer. The API is
`CausalProfiler::new(ProfilerConfig)`, then `.start()` / `.report()`, with progress
points registered via a macro; it requires `RUSTFLAGS="--cfg tokio_unstable"`.

Treat this as experimental and research-grade. It is a standalone crate whose
README is a design document, compiled into the server only under the non-default
`causal-profile` feature (and only when `tokio_unstable` is set) — it is off in
normal builds. The README itself flags an open research question: whether the coz
speedup invariant still holds under M:N scheduling with work-stealing is unproven.
Do not read its presence as a shipped, validated profiler.

---

## Debug logging

Log level is configured three ways (see the config loader in
`frogdb-server/crates/server/src/config/loader.rs`):

```bash
FROGDB_LOGGING__LEVEL=debug frogdb-server   # env var: FROGDB_ prefix, __ = section, _ -> -
RUST_LOG=info,frogdb_core=debug frogdb-server  # standard tracing EnvFilter, honored when set
```

```
CONFIG SET loglevel debug                   # at runtime (levels: trace, debug, info, warn, error)
```

`CONFIG SET loglevel` reloads the tracing filter live through a reload handle, so
the level can be raised to capture a problem and lowered again without a restart.
