# FrogDB Implementation Roadmap

This document tracks the implementation progress of FrogDB. Each phase has specific deliverables with checkboxes for progress tracking.

## Design Principles

1. **Build the skeleton first** - Establish correct abstractions from Phase 1, even as noops
2. **Avoid large refactors** - Include sharding infrastructure from day one
3. **Test as you go** - Each phase includes testing requirements
4. **Future features as noops** - WAL, ACL, replication hooks exist from Phase 1

---

## Current Status

**Completed**: Foundation, Sorted Sets, Multi-Shard Operations, Hash/List/Set Types, Transactions & Pub/Sub, Lua Scripting, Key Iteration & Server Commands, Blocking Commands, RESP3 Protocol, Streams, String Commands & TTL, Persistence, Production Readiness, Property Testing, Protocol Completion
**In Progress**: Phase 1 (SORT only remaining)
**Next Milestone**: Complete Phase 1, 3-4, then Phase 5 (Clustering)

---

### Part 3: Key migration

- [ ] Config prescedence: file, ENV variables, CLI flags, runtime changes with CONFIG SET
- [ ] Ability to "watch" or "listen" for config value changes for values adjustable at runtime
- [ ] Wire dynamic config values into code where they're used
- [ ] Validation + errors for invalid config
- [ ] Log the full configuration state on startup

### Observability audit

- [ ] What metrics + logs + traces are missing?
- [ ] Do we have per-command count metrics?
- [ ] Do we have per-command latency metrics?
- [ ] Any other metrics we're missing?
- [ ] Have we validated that open telemtry and prometheus can interact with frogdb?
- [ ] Could a skilled software reliability engineer be able to easily diagnose issues with frogdb?

### Operational Readiness

- [ ] Look up other modern databases like CockroachDB, DragonflyDB, FoundationDB, and identify features or tooling they provide for operating the database (migrations, disaster recovery, observability, debugging, etc)
- [ ] Determine if any of these apply to frogdb

---

## Phase 3: Benchmark Comparisons

**Goal**: Performance comparison with Redis/Valkey/Dragonfly.

- [x] Benchmark harness setup (Redis)
- [ ] Valkey comparison benchmarks
- [ ] Dragonfly comparison benchmarks
- [ ] Performance report generation

---

## Phase 4: Distributed Single-Node Testing

**Goal**: Correctness testing for single-node operation.

- [ ] Jepsen test harness integration

### Very Light Locking (VLL) Implementation

**Goal**: Provide same guarantees as Dragonflydb for multi-key operations

- [ ] MSET/MGET full atomicity (currently per-key atomic; should be fully atomic like Redis/DragonflyDB)
- [ ] transaction consistency behavior like dragonflydb
- [ ] Lua script consistency behavior like dragonflydb
- [ ] Check for potential performance bottlenecks.

### Clustering / Replication

**Goal**: Distributed operation support.

- [ ] Replication via WAL streaming
- [ ] CLUSTER commands
- [ ] Hash slot migration
- [ ] Failover
- [ ] `ROLE` - Report replication role (primary/replica)
- [ ] `BGREWRITEAOF` - Stub returning appropriate message (N/A for RocksDB)

### Distributed Cluster Testing

**Goal**: Correctness testing for clustered operation.

- [ ] Cluster partition/chaos tests (Jepsen)
- [ ] Cluster partition/chaos tests (Turmoil)
- [ ] Cluster failover tests
- [ ] Cluster linearizability tests

---

## Phase 7: Performance Optimizations

**Goal**: Comprehensive performance profiling and optimization.

See [OPTIMIZATIONS.md](OPTIMIZATIONS.md) for detailed profiling infrastructure, optimization strategies, and implementation guidance.

**Subsections:**

- Profiling Infrastructure
- Quick Wins
- Memory Optimizations
- I/O Optimizations
- Data Structure Optimizations
- Concurrency Optimizations
- Advanced Optimizations

---

## Phase 9: Redis Functions

**Goal**: Redis 7.0+ Functions support (alternative to Lua scripting).

- [ ] `FUNCTION` command (LOAD, DELETE, DUMP, FLUSH, KILL, LIST, RESTORE, STATS)
- [ ] `FCALL` - Execute function
- [ ] `FCALL_RO` - Execute read-only function
- [ ] Function library management and persistence

---

## Phase 10: Documentation & Polish

**Goal**: Documentation accuracy and completeness.

- [ ] Update COMPATIBILITY.md - Remove outdated "planned" status for Blocking Commands and Streams
- [ ] Audit all spec files for accuracy against implementation
- [ ] Add missing command documentation to types/\*.md files

---

## Critical Abstractions

These must exist from the initial foundation to avoid refactoring:

| Abstraction                | Initial                 | Full Implementation       |
| -------------------------- | ----------------------- | ------------------------- |
| `Store` trait              | HashMapStore            | Same                      |
| `Command` trait            | Full                    | Same                      |
| `Value` enum               | StringValue only        | All types ✓               |
| `WalWriter` trait          | Noop                    | RocksDB WAL ✓             |
| `ReplicationConfig`        | Standalone              | Primary/Replica (Phase 5) |
| `ReplicationTracker` trait | Noop                    | WAL streaming (Phase 5)   |
| `AclChecker` trait         | AlwaysAllow             | Full ACL ✓                |
| `MetricsRecorder` trait    | Noop                    | Prometheus ✓              |
| `Tracer` trait             | Noop                    | OpenTelemetry ✓           |
| Shard channels             | 1 shard                 | N shards ✓                |
| `ExpiryIndex`              | Empty                   | Functional ✓              |
| `ProtocolVersion`          | Resp2 only              | Resp2 + Resp3 ✓           |
| `Config` + Figment         | Full (CLI + TOML + env) | CONFIG GET/SET ✓          |
| Logging format             | pretty + json           | Same                      |

---

## References

- [INDEX.md](INDEX.md) - Architecture overview
- [EXECUTION.md](EXECUTION.md) - Command flow
- [STORAGE.md](STORAGE.md) - Data structures
- [CONCURRENCY.md](CONCURRENCY.md) - Threading model
- [PROTOCOL.md](PROTOCOL.md) - RESP handling
- [PERSISTENCE.md](PERSISTENCE.md) - RocksDB integration
- [CONFIGURATION.md](CONFIGURATION.md) - Configuration system
- [TESTING.md](TESTING.md) - Test strategy
- [OPTIMIZATIONS.md](OPTIMIZATIONS.md) - Performance profiling and optimization
