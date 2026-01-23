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

- [ ] `MIGRATE` - Migrate keys between instances

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
- [x] Turmoil test harness integration
- [x] Single-node linearizability tests
- [ ] Single-node crash recovery tests
- [ ] MSET/MGET full atomicity (currently per-key atomic; should be fully atomic like Redis/DragonflyDB)
- [ ] Lua script `redis.call()` integration with command execution (enables atomic cross-key operations)

---

## Phase 5: Clustering

**Goal**: Distributed operation support.

- [ ] Replication via WAL streaming
- [ ] CLUSTER commands
- [ ] Hash slot migration
- [ ] Failover
- [ ] `ROLE` - Report replication role (primary/replica)
- [ ] `BGREWRITEAOF` - Stub returning appropriate message (N/A for RocksDB)

---

## Phase 6: Distributed Cluster Testing

**Goal**: Correctness testing for clustered operation.

- [ ] Cluster partition tests (Jepsen)
- [ ] Cluster partition tests (Turmoil)
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

## <<<<<<< HEAD

## ||||||| parent of 2755a3c (update roadmap)

## Phase 8: ACL Completion

**Goal**: Full Redis ACL compatibility.

- [ ] Subcommand-level ACL rules (e.g., allow CONFIG GET but deny CONFIG SET)
- [ ] ACL selector syntax for granular key/channel permissions
- [ ] Per-command ACL category enforcement

---

=======

> > > > > > > 2755a3c (update roadmap)

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
