# io_uring Integration Design Document

## 1. Goals

FrogDB's profiling under a 214K ops/sec write-heavy workload shows **86% of CPU time in kernel syscalls** — mostly epoll/kqueue event polling and network I/O (read/write/accept). io_uring can reduce this overhead by:

1. **Eliminating per-operation syscalls**: io_uring uses shared ring buffers between userspace and kernel. Operations are submitted to a Submission Queue (SQ) and completions arrive on a Completion Queue (CQ), both in shared memory — no syscall needed per I/O operation.
2. **Batching I/O operations**: Multiple operations can be submitted in a single `io_uring_enter()` call.
3. **Reducing context switches**: The kernel can process submissions without switching to userspace.
4. **Multishot operations**: A single SQE can serve multiple accept() or recv() completions.

**Expected improvement**: 30-50% throughput increase based on DragonflyDB benchmarks and [IO.md](IO.md) estimates. The optimizations spec already identifies this as a planned I/O optimization.

**Target platform**: Linux 5.10+ for production. macOS remains the development platform with a tokio-based fallback.

---

## 2. Current Architecture

### I/O Stack
- **Runtime**: Tokio multi-threaded (work-stealing, `#[tokio::main]`)
- **Network**: `Framed<TcpStream, Resp2>` (tokio-util codec with redis-protocol crate)
- **Event loop**: `tokio::select!` in both connection handler and shard worker
- **Channels**: `tokio::sync::mpsc` (bounded, 1024) for shard routing, `oneshot` for request-response
- **Disk I/O**: RocksDB sync calls, `spawn_blocking` in 3 files (rocks.rs, snapshot.rs, primary.rs)
- **Platform abstraction**: `net.rs` swaps `tokio::net` ↔ `turmoil::net` via feature flag

### Data Flow (Current)
```
Client ←TCP→ [tokio task: Framed codec + select!]
                ↓ ParsedCommand via mpsc
             [tokio task: shard event loop + select!]
                ↓ Response via oneshot
             [tokio task: Framed codec write]
                ↓ TCP
             Client
```

Each connection is a `tokio::spawn()` task. Commands are parsed from the Framed codec, routed to the owning shard via `mpsc::Sender<ShardMessage>`, and responses flow back via `oneshot`.

### Key Files
| File | Role |
|------|------|
| `crates/server/src/main.rs` | Tokio runtime entry point |
| `crates/server/src/acceptor.rs` | TCP accept loop, spawns per-connection tasks |
| `crates/server/src/connection.rs` | `Framed<TcpStream, Resp2>`, command loop with `tokio::select!` |
| `crates/server/src/connection/dispatch.rs` | Command parsing and dispatch to routing |
| `crates/server/src/connection/routing.rs` | Shard routing, `execute_on_shard()` |
| `crates/server/src/net.rs` | Platform abstraction (tokio/turmoil) |
| `crates/core/src/shard/event_loop.rs` | Shard worker `tokio::select!` loop |
| `crates/core/src/shard/message.rs` | `ShardMessage` enum |

---

## 3. The Core Problem: Readiness vs Completion I/O

**Tokio (readiness-based, epoll/kqueue):**
- OS notifies "socket X is readable"
- Application calls `read()` synchronously
- Application owns the buffer throughout
- `AsyncRead`/`AsyncWrite` traits, `Framed` codec built on this model

**io_uring (completion-based):**
- Application submits "read N bytes from socket X into buffer B"
- Kernel performs the read asynchronously
- Kernel writes completion to CQ when done
- **Buffer is owned by kernel** during the operation — cannot be touched

This buffer ownership difference means:
- `Framed<TcpStream, Resp2>` (tokio-util) cannot work with io_uring
- `AsyncRead`/`AsyncWrite` traits don't map to completion-based I/O
- A new RESP parser working with owned `Vec<u8>` buffers is required for any io_uring path

---

## 4. Rust io_uring Runtime Ecosystem

### Runtime Options (as of February 2026)

| Runtime | Model | io_uring | macOS | Windows | Ecosystem | Status | Used By |
|---------|-------|----------|-------|---------|-----------|--------|---------|
| **tokio** | Work-stealing, multi-threaded | No (readiness-based) | Yes (kqueue) | Yes (mio) | Massive (20k+ dependents) | Active | TiKV, SurrealDB, most Rust servers |
| **compio** | Thread-per-core | Yes | Yes (kqueue) | Yes (IOCP) | Growing (HTTP, TLS, QUIC, WS) | **Most active** (~300 commits/yr) | Apache Iggy |
| **monoio** | Thread-per-core | Yes (native) | Yes (kqueue fallback) | No | Small (ByteDance-internal focus) | Moderate (~70 commits/yr) | ByteDance (Monolake proxy) |
| **glommio** | Thread-per-core | Yes | No (Linux only) | No | Withered | **Effectively stalled** (~25 commits/yr) | DataDog (historical) |
| **tokio-uring** | Single-threaded on tokio | Yes | No | No | Small | **Dormant** (no releases since 2022) | Not recommended |
| **io-uring** (crate) | Low-level bindings | Yes (raw API) | No | No | N/A | Mature | Building block for other runtimes |

### Key Characteristics

**compio** (community, recommended for new thread-per-core projects):
- Cross-platform completion-based I/O: io_uring (Linux), IOCP (Windows), kqueue (macOS)
- Most actively developed thread-per-core runtime: ~300+ commits since Jan 2024, frequent releases (v0.18.0 Jan 2026)
- Richest ecosystem of any completion-based runtime: cyper (hyper-based HTTP), compio-tls (rustls/native-tls), compio-quic (quinn-synced), compio-ws (WebSocket)
- Apache Iggy rewrote their entire server from tokio to compio (v0.6.0, Dec 2025), documenting that poll-based and completion-based I/O models don't compose cleanly
- Modular crate structure: compio-driver, compio-runtime, compio-io, compio-net, compio-fs, compio-tls
- Risk: heavily dependent on a single maintainer (Berrysoft, ~1,137 of ~1,500 commits)

**monoio** (ByteDance):
- Thread-per-core: each thread has its own io_uring instance, no task migration
- Futures are `!Send` — no Arc/Mutex overhead for cross-thread safety
- ~20-26% faster than tokio in gateway/RPC benchmarks
- macOS fallback via kqueue (not io_uring, but same API)
- Production-proven at ByteDance scale via Monolake proxy
- No Windows support; ecosystem is narrow and ByteDance-internal-focused
- Moderate development pace: ~70 commits since Jan 2024, last release v0.2.4 (Aug 2024)

**glommio** (DataDog — not recommended for new projects):
- Created by Glauber Costa (Linux kernel developer, ex-ScyllaDB), inspired by Seastar
- Costa left to co-found Turso in 2021; no longer contributing
- Last release v0.9.0 (Mar 2024), only ~25 commits since Jan 2024
- Linux-only (io_uring exclusive, no fallback)
- Ecosystem has withered: no maintained HTTP server, TLS, or database drivers
- Pioneering work lives on conceptually in monoio and compio

### Why compio over monoio for FrogDB

If FrogDB were to replace tokio with a thread-per-core runtime, **compio is the recommended choice**:

1. **Development velocity**: compio has ~4x the commit rate of monoio (300+ vs 70 since 2024) and ships releases every 1-2 months vs monoio's 18-month gap
2. **Ecosystem breadth**: compio has HTTP, TLS, QUIC, and WebSocket support. Monoio's ecosystem is internal to ByteDance's CloudWeGo stack
3. **Production validation for our use case**: Apache Iggy (a message streaming server) did a full tokio→compio migration — a closer analog to FrogDB's architecture than ByteDance's proxy use case (Monolake)
4. **Cross-platform development**: compio supports macOS (kqueue) and Windows (IOCP) natively, preserving the macOS development workflow. Monoio supports macOS but not Windows
5. **Buffer model**: Both use owned-buffer completion-based I/O, but compio's modular crate design makes it easier to adopt incrementally

### DragonflyDB's Model (C++, for reference)

DragonflyDB — the highest-performing Redis-compatible server — uses an architecture very similar to what FrogDB would achieve:

- **Thread-per-core, shared-nothing**: Each CPU core runs one thread with its own event loop
- **io_uring via helio**: Custom C++ I/O library using io_uring for both network and disk I/O in a single polling loop per thread
- **Fibers for concurrency**: Boost.Fibers for cooperative multitasking within each thread (analogous to async/await)
- **Cross-shard messaging**: Connection fiber acts as coordinator, sends messages to other threads for multi-key operations
- **No locks on data**: Each shard exclusively owned by its thread

DragonflyDB claims 4.5x throughput over Valkey on identical hardware. FrogDB's shard model is already architecturally similar — the runtime is the main gap.

---

## 5. Potential Solutions

### Option A: Full Runtime Replacement (compio)

Replace tokio with compio. Each shard becomes one OS thread with its own completion-based I/O ring (io_uring on Linux, kqueue on macOS, IOCP on Windows).

**Data Flow:**
```
Client ←TCP→ [compio thread: io_uring accept + read]
                ↓ ParsedCommand (same thread, no channel)
             [compio thread: shard execution]
                ↓ Response (same thread)
             [compio thread: io_uring write]
                ↓ TCP
             Client
```

**What changes:**
- `#[tokio::main]` → `compio::runtime::RuntimeBuilder` per thread
- `Framed<TcpStream, Resp2>` → manual RESP parsing with owned buffers (compio-io)
- `tokio::select!` → compio equivalent
- `tokio::sync::mpsc`/`oneshot` → crossbeam channels (for cross-thread) or compio local channels (same-thread)
- `tokio::time::interval` → compio timer
- `tokio::spawn` → compio task spawn (thread-local, `!Send`)
- `net.rs` → compio-net `TcpListener`/`TcpStream`
- Connection handler runs on same thread as its shard — no channel hop for single-shard commands

**What stays the same:**
- Store data structures and command logic (pure Rust, no runtime dependency)
- Key hashing and shard routing logic
- RocksDB persistence (sync, runtime-agnostic)
- Jepsen tests (black-box)
- ACL, Lua scripting logic

**Considerations:**

| Pro | Con |
|-----|-----|
| Maximum performance (30-50% gain) | Largest rewrite (weeks) |
| Thread-per-core matches shard model perfectly | Lose Shuttle concurrency tests |
| No Send/Sync overhead | Lose Turmoil simulation tests |
| No channel hop for single-shard commands | Every tokio dependency must be replaced |
| Same architecture as DragonflyDB | io_uring safety concerns (buffer lifetime, cancellation) |
| `!Send` futures are more efficient | Single-maintainer risk on compio |
| Lower tail latency | |
| Cross-platform: io_uring (Linux), kqueue (macOS), IOCP (Windows) | |
| Richest completion-based ecosystem (HTTP, TLS, QUIC, WS) | |
| Apache Iggy proved this migration path viable | |

**Testing strategy post-migration:**
- Loom for low-level primitive correctness
- Jepsen for distributed correctness (unchanged)
- Property-based testing (proptest) for logic correctness
- OS-level fault injection (iptables, tc) for network chaos
- Custom in-process simulation would need to be built
- Consider MadSim (madsim-rs) for deterministic simulation testing — used by RisingWave, provides drop-in runtime replacement with deterministic execution

---

### Option B: Hybrid — io_uring I/O Threads + Tokio Business Logic

Keep tokio for shard event loops and business logic. Add dedicated io_uring I/O threads for network operations.

**Data Flow:**
```
Client ←TCP→ [io_uring I/O thread: accept + read + parse]
                ↓ ParsedCommand via crossbeam channel
             [tokio task: shard event loop (unchanged)]
                ↓ Response via crossbeam channel
             [io_uring I/O thread: encode + write]
                ↓ TCP
             Client
```

**What changes:**
- New `io_uring/` module in crates/server (feature-gated)
- New RESP parser for owned buffers (can reuse `redis_protocol::resp2::decode::decode_bytes_mut`)
- `acceptor.rs` delegates to I/O threads when feature enabled
- Cross-thread channels (flume/crossbeam) bridge I/O threads ↔ shard workers
- Feature flag: `#[cfg(feature = "io-uring")]` for Linux, falls back to tokio on macOS

**What stays the same:**
- Shard event loop (tokio `select!`)
- All command execution, routing, dispatch
- Shuttle and Turmoil tests (shard logic only, feature disabled)
- macOS development workflow
- Replication, Lua scripting, ACL

**Considerations:**

| Pro | Con |
|-----|-----|
| Preserves tokio ecosystem | Two code paths to maintain |
| Keeps Shuttle + Turmoil | Extra channel hops (~100-200ns) |
| Incremental adoption | Moderate gain (20-40%) vs full swap |
| macOS dev unaffected | RESP parser duplication (Framed + manual) |
| Clear rollback (disable feature) | PSYNC handoff more complex (fd ownership) |
| Smaller rewrite (1-2 weeks) | io_uring I/O path not testable via Turmoil |
| Feature-gated risk isolation | Debugging harder (strace can't see io_uring) |

---

### Option C: Targeted io_uring for Specific Syscalls

Use the low-level `io-uring` crate for high-frequency syscalls only. Keep everything else on tokio.

**Targets:**
- **Multishot accept**: One SQE serves many `accept()` completions
- **Multishot recv**: One SQE serves many `read()` completions on a socket
- **Batched fsync**: Submit WAL fsyncs through io_uring ring

**What changes:**
- New `uring_accept.rs` with multishot accept, feeds sockets to tokio tasks
- Bridge via `tokio::io::unix::AsyncFd` to wake tokio on io_uring completions
- WAL fsync path optionally uses io_uring

**What stays the same:**
- Everything except accept and fsync paths

**Considerations:**

| Pro | Con |
|-----|-----|
| Minimal code change (days) | Smallest gain (10-20%) |
| No architectural disruption | Doesn't address per-request read/write overhead |
| Easy to reason about | Still uses epoll for socket read/write |
| All tests preserved | Limited io_uring benefit (just accept + fsync) |

---

## 6. Safety Concerns with io_uring in Rust

Tonbo (a Rust database project) [documented safety issues](https://tonbo.io/blog/async-rust-is-not-safe-with-io-uring) with io_uring in async Rust:

**I/O Safety**: When a future is dropped (cancelled), the kernel may still be processing the submitted operation. The buffer and fd must remain valid until the kernel completes. If the future is dropped and the buffer is freed, the kernel writes to freed memory → UB.

**Halt Safety**: Unlike readiness-based I/O where cancellation means "stop polling" (safe), completion-based I/O cancellation means "tell kernel to cancel" (which may fail or complete anyway). Futures must be driven to completion even during cancellation.

**Mitigation strategies:**
- monoio provides `CancelableAsyncReadRent` — explicitly cancel and re-await to confirm
- compio handles this in its runtime layer
- Manual implementations need careful `Drop` handling to cancel pending SQEs and wait for completion
- Rust lacks linear types (values that must be consumed exactly once), which would solve this at the type level

These are engineering challenges, not blockers. Both monoio and compio handle them at the runtime level.

---

## 7. Testing Impact Matrix

| Test Type | Option A (compio) | Option B (hybrid) | Option C (targeted) |
|-----------|-------------------|-------------------|---------------------|
| **Shuttle** (randomized concurrency) | Lost | Preserved (shard logic) | Preserved |
| **Turmoil** (network simulation) | Lost | Preserved (shard logic only) | Preserved |
| **Jepsen** (distributed correctness) | Works (black-box) | Works | Works |
| **Loom** (primitive correctness) | Works | Works | Works |
| **Unit tests** | Rewrites needed | Preserved | Preserved |
| **Integration tests** | Rewrites needed | Mostly preserved | Preserved |
| **Redis compat tests** | Works (black-box) | Works | Works |

---

## 8. Effort Estimates

| Approach | Scope | Estimated Effort | Performance Gain |
|----------|-------|-----------------|------------------|
| Option A: Full compio | Rewrite network + runtime layer | 3-6 weeks | 30-50% |
| Option B: Hybrid | New I/O thread module + RESP parser | 1-2 weeks | 20-40% |
| Option C: Targeted | Accept + fsync wrappers | 2-3 days | 10-20% |

---

## 9. Recommendation

**Short-term (next milestone):** Focus on other optimizations from the [INDEX.md](INDEX.md) spec that don't require runtime changes — Arc<Value> for reads (30-50% read improvement), WAL batching (2-5x write throughput), response buffer pools. These compound with the jemalloc + TCP_NODELAY + Arc<ParsedCommand> changes already landed.

**Medium-term:** Option B (hybrid) as a feature-gated Linux optimization. Proves out the io_uring integration with minimal risk to existing code. Provides real benchmark data to justify further investment.

**Long-term:** If benchmarks confirm io_uring gains and FrogDB is Linux-primary in production, consider Option A (full compio swap) to match DragonflyDB's architecture. Compio is the recommended runtime for this migration: it has the most active development, richest ecosystem, cross-platform support (preserving macOS dev workflow), and Apache Iggy has already validated this exact migration path (tokio→compio for a thread-per-core shared-nothing server). This would be a major version milestone with a rebuilt testing strategy.

---

## 10. Open Questions for Future Work

1. **What compio version to target?** Releases are frequent (every 1-2 months). Pin to a stable release and track upstream closely given single-maintainer risk.
2. **How to handle cross-shard operations in thread-per-core?** Currently uses tokio mpsc. Compio has no built-in cross-thread channel — need crossbeam or flume.
3. **Can turmoil be adapted for compio?** Turmoil intercepts tokio's reactor. A compio equivalent would need to intercept compio's driver submission path. Alternatively, consider MadSim for deterministic simulation testing.
4. **RocksDB + io_uring**: RocksDB 7.x+ supports `io_uring` as a backend (`PosixIoUringRandomAccessFile`). Worth enabling?
5. **Kernel version requirements**: What's the minimum deployment kernel? 5.10 covers all stable io_uring features. 6.0+ adds multishot recv.
6. **Buffer pool design**: io_uring works best with pre-allocated fixed buffers registered with the ring. What buffer pool strategy? Per-thread arena? Slab allocator?
7. **Single-maintainer risk mitigation**: Compio is heavily dependent on Berrysoft (~75% of commits). Evaluate whether FrogDB should contribute upstream, maintain a fork, or have a fallback plan if the project stalls.

---

## References

- [IO.md](IO.md) — io_uring listed as an I/O optimization
- [DragonflyDB shared-nothing architecture](https://github.com/dragonflydb/dragonfly/blob/main/docs/df-share-nothing.md)
- [DragonflyDB vs Valkey threading comparison](https://www.dragonflydb.io/blog/why-threading-models-matter-dragonfly-vs-valkey)
- [Apache Iggy's compio migration](https://iggy.apache.org/blogs/2025/11/17/websocket-io-uring/) — Most relevant case study: tokio→compio for a thread-per-core server
- [Apache Iggy 0.6.0 release notes](https://iggy.apache.org/blogs/2025/12/09/release-0.6.0/) — Production results of the compio migration
- [Tonbo: Async Rust is not safe with io_uring](https://tonbo.io/blog/async-rust-is-not-safe-with-io-uring)
- [compio](https://github.com/compio-rs/compio) — Cross-platform completion-based runtime (recommended)
- [cyper](https://github.com/compio-rs/cyper) — HTTP client/server built on compio (hyper-based)
- [monoio](https://github.com/bytedance/monoio) — ByteDance's thread-per-core io_uring runtime
- [glommio](https://github.com/DataDog/glommio) — DataDog's thread-per-core runtime (stalled, not recommended)
- [tokio-uring status discussion](https://users.rust-lang.org/t/status-of-tokio-uring/114481)
- [io-uring crate](https://github.com/tokio-rs/io-uring) — Low-level Rust bindings
- [MadSim](https://github.com/madsim-rs/madsim) — Deterministic simulation testing (Tokio drop-in replacement)
- [DragonflyDB Redis threading analysis](https://www.dragonflydb.io/blog/redis-analysis-part-1-threading-model)
