# FrogDB Implementation Roadmap

This document tracks the implementation progress of FrogDB. Each phase has specific deliverables with checkboxes for progress tracking.

## Design Principles

1. **Build the skeleton first** - Establish correct abstractions from Phase 1, even as noops
2. **Avoid large refactors** - Include sharding infrastructure from day one
3. **Test as you go** - Each phase includes testing requirements
4. **Future features as noops** - WAL, ACL, replication hooks exist from Phase 1

---

## Current Status

**Completed**: Foundation, Sorted Sets, Multi-Shard Operations, Hash/List/Set Types, Transactions & Pub/Sub, Lua Scripting, Key Iteration & Server Commands, Blocking Commands, RESP3 Protocol, Streams, String Commands & TTL, Persistence, Production Readiness
**In Progress**: Phase 1 (Remaining Commands)
**Next Milestone**: Complete Phase 1-4, then Phase 5 (Clustering)

---

## Phase 1: Remaining Commands

**Goal**: Complete missing non-cluster commands.

- [ ] `COPY` - Copy key to another key
- [ ] `SORT` / `SORT_RO` - Sort lists/sets/zsets
- [ ] `RANDOMKEY` - Random key selection
- [ ] `LCS` - Longest common subsequence
- [ ] `MEMORY` commands (DOCTOR, MALLOC-SIZE, PURGE, STATS, USAGE)
- [ ] `LATENCY` commands (DOCTOR, GRAPH, HISTOGRAM, HISTORY, LATEST, RESET)
- [ ] `OBJECT HELP`
- [ ] `DEBUG SLEEP`

---

## Phase 2: Property Testing

**Goal**: Property-based test coverage.

- [ ] Property tests: INCR/DECR roundtrip
- [ ] Test: snapshot during writes

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
- [ ] Turmoil test harness integration
- [ ] Single-node linearizability tests
- [ ] Single-node crash recovery tests

---

## Phase 5: Clustering

**Goal**: Distributed operation support.

- [ ] Replication via WAL streaming
- [ ] CLUSTER commands
- [ ] Hash slot migration
- [ ] Failover

---

## Phase 6: Distributed Cluster Testing

**Goal**: Correctness testing for clustered operation.

- [ ] Cluster partition tests (Jepsen)
- [ ] Cluster partition tests (Turmoil)
- [ ] Cluster failover tests
- [ ] Cluster linearizability tests

---

## Phase 7: Performance Optimizations

**Goal**: Optional performance enhancements.

- [ ] io_uring (optional)
- [ ] Skip list for sorted sets (optional)
- [ ] Connection pooling optimizations

---

## Critical Abstractions

These must exist from the initial foundation to avoid refactoring:

| Abstraction | Initial | Full Implementation |
|-------------|---------|---------------------|
| `Store` trait | HashMapStore | Same |
| `Command` trait | Full | Same |
| `Value` enum | StringValue only | All types ✓ |
| `WalWriter` trait | Noop | RocksDB WAL ✓ |
| `ReplicationConfig` | Standalone | Primary/Replica (Phase 5) |
| `ReplicationTracker` trait | Noop | WAL streaming (Phase 5) |
| `AclChecker` trait | AlwaysAllow | Full ACL ✓ |
| `MetricsRecorder` trait | Noop | Prometheus ✓ |
| `Tracer` trait | Noop | OpenTelemetry ✓ |
| Shard channels | 1 shard | N shards ✓ |
| `ExpiryIndex` | Empty | Functional ✓ |
| `ProtocolVersion` | Resp2 only | Resp2 + Resp3 ✓ |
| `Config` + Figment | Full (CLI + TOML + env) | CONFIG GET/SET ✓ |
| Logging format | pretty + json | Same |

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
