# FrogDB Implementation Roadmap

This document tracks the implementation progress of FrogDB. Each phase has specific deliverables with checkboxes for progress tracking.

## Distributed Cluster Testing

**Implementation Gaps:**
- [ ] Implement actual key movement during slot migration (CLUSTER SETSLOT updates metadata but doesn't move keys)
- [x] Implement auto full-resync trigger when replica lag exceeds threshold

**Goal**: Correctness testing for clustered operation.

- [ ] Turmoil deterministic simulation tests
- [ ] Cluster partition/chaos tests (Jepsen)
- [ ] Cluster failover tests
- [ ] Cluster linearizability tests

## Operational Readiness

Inspired by: **CockroachDB**, **DragonflyDB**, **FoundationDB**, **Redis**, **Valkey**

##### 5. Grafana Dashboard Templates

**Inspired by:** DragonflyDB Kubernetes dashboards

Ready-to-import JSON dashboards:

1. **Overview**: uptime, connections, ops/sec, memory
2. **Performance**: latency histograms, slow queries, command breakdown
3. **Shards**: per-shard memory/keys/queue depth
4. **Persistence**: WAL writes, snapshot status

**Why:** Reduces time-to-value; showcases FrogDB's metrics.
**Complexity:** Low (docs/config only)

- [ ] Create `deploy/grafana/overview.json` dashboard
- [ ] Create `deploy/grafana/performance.json` dashboard
- [ ] Create `deploy/grafana/shards.json` dashboard
- [ ] Create `deploy/grafana/persistence.json` dashboard
- [ ] Add dashboard import instructions to documentation

##### 8. Enhanced LATENCY DOCTOR

**Inspired by:** Redis LATENCY DOCTOR, CockroachDB diagnostics

Deeper analysis with:

- Correlation detection (latency spikes during expire-cycle, snapshots)
- Cross-reference with SLOWLOG
- FrogDB-specific scatter-gather analysis
- Specific remediation steps

**Complexity:** Medium

- [ ] Track latency correlation with background tasks
- [ ] Cross-reference high-latency periods with SLOWLOG entries
- [ ] Analyze scatter-gather overhead for multi-shard operations
- [ ] Generate actionable recommendations

##### 12. Automated Alert Rule Generation

**Inspired by:** CockroachDB alert generation

Generate Prometheus alerting rules from config:

```
GET /alerts/prometheus
```

**Why:** Alerts stay in sync with configuration; best-practice encoding.
**Complexity:** Medium

- [ ] Define alert templates based on FrogDB config
- [ ] Implement `/alerts/prometheus` endpoint
- [ ] Generate rules for memory, latency, connections, persistence
- [ ] Include severity levels and recommended thresholds

#### Summary Table

| Priority | Feature                      | Complexity | Inspired By       |
| -------- | ---------------------------- | ---------- | ----------------- |
| HIGH     | Machine-Readable Status JSON | Medium     | FoundationDB      |
| HIGH     | Hot Shard Detection          | Medium     | CockroachDB       |
| HIGH     | Latency Band Tracking        | Low        | FoundationDB      |
| HIGH     | Admin Port Separation        | Medium     | DragonflyDB       |
| HIGH     | Grafana Dashboard Templates  | Low        | DragonflyDB       |
| MEDIUM   | Enhanced MEMORY DOCTOR       | Medium     | Redis             |
| MEDIUM   | DEBUG HASHING                | Low        | FrogDB-specific   |
| MEDIUM   | Enhanced LATENCY DOCTOR      | Medium     | Redis/CockroachDB |
| MEDIUM   | Client Connection Stats      | Medium     | CockroachDB       |
| MEDIUM   | Persistence Lag Monitoring   | Medium     | FoundationDB      |
| NICE     | Debug Diagnostic Bundles     | High       | CockroachDB       |
| NICE     | Auto Alert Generation        | Medium     | CockroachDB       |
| NICE     | DEBUG DUMP-VLL-QUEUE         | Medium     | FrogDB-specific   |
| NICE     | Intrinsic Latency CLI        | Low        | Redis             |
| NICE     | Tracing Diagnostics          | Low        | CockroachDB       |

#### Key Implementation Files

- `crates/metrics/src/server.rs` - New HTTP endpoints
- `crates/server/src/connection.rs` - Command handling
- `crates/core/src/latency.rs` - Latency infrastructure
- `crates/metrics/src/prometheus_recorder.rs` - New metrics
- `spec/OBSERVABILITY.md` - Documentation updates

#### References

- [CockroachDB Metrics](https://www.cockroachlabs.com/docs/stable/metrics)
- [CockroachDB Hot Ranges](https://www.cockroachlabs.com/docs/stable/understand-hotspots)
- [CockroachDB Statement Diagnostics](https://www.cockroachlabs.com/docs/stable/cockroach-statement-diag)
- [DragonflyDB Monitoring](https://www.dragonflydb.io/docs/managing-dragonfly/monitoring)
- [FoundationDB Administration](https://apple.github.io/foundationdb/administration.html)
- [FoundationDB Machine-Readable Status](https://apple.github.io/foundationdb/mr-status.html)
- [Redis Latency Monitoring](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/latency-monitor/)
- [Redis MEMORY DOCTOR](https://redis.io/docs/latest/commands/memory-doctor/)
- [Valkey Administration](https://valkey.io/topics/admin/)

---

## Code Quality & Refactoring

##### Split connection.rs (DONE)

`connection.rs` has been decomposed into `connection/handlers/` with 17 handler modules plus routing, dispatch, state, and builder modules.

- [x] Extract auth handling to `connection/handlers/auth.rs`
- [x] Extract transaction handling to `connection/handlers/transaction.rs`
- [x] Extract pub/sub handling to `connection/handlers/pubsub.rs`
- [x] Extract blocking state to `connection/handlers/blocking.rs`
- [x] Extract routing logic to `connection/routing.rs`
- [x] Extract scatter-gather to `connection/handlers/scatter.rs`

##### Split shard.rs (DONE)

Eviction extracted to dedicated modules.

- [x] Extract eviction strategies to `shard/eviction.rs` and `crates/core/src/eviction/` (lfu, policy, pool)

##### Split types.rs (PARTIALLY DONE)

Types split into `crates/types/` crate. Specialized types extracted to individual files; core value types remain in `types.rs`.

- [ ] Extract string types to `types/string.rs`
- [ ] Extract list types to `types/list.rs`
- [ ] Extract set types to `types/set.rs`
- [ ] Extract hash types to `types/hash.rs`
- [ ] Extract sorted set types to `types/sorted_set.rs`
- [ ] Extract stream types to `types/stream.rs`
- [x] Extract specialized types (BloomFilter, HyperLogLog, Geo, Json, Bitmap) to individual files in `crates/types/src/`

##### Config magic numbers (LOW effort)

- [ ] Define named constants for timeout values and sizes in `crates/server/src/config.rs`

##### Sorted set parsing helpers (LOW effort)

- [ ] Extract `parse_score_bound()` and `parse_lex_bound()` to utils.rs (if not already covered)
- [ ] Extract `parse_set_op_options()` to utils.rs

---

## Phase 7: Performance Optimizations

**Goal**: Comprehensive performance profiling and optimization.

See [optimizations/](../todo/optimizations/INDEX.md) for detailed profiling infrastructure, optimization strategies, and implementation guidance.

**Subsections:**

- Profiling Infrastructure
- Async Task Profiling
- Quick Wins
- Memory Optimizations
- I/O Optimizations
- Data Structure Optimizations
- Concurrency Optimizations
- Advanced Optimizations

---

## Phase 10: Documentation & Polish

**Goal**: Documentation accuracy and completeness.

- [x] Update COMPATIBILITY.md - Remove outdated "planned" status for Blocking Commands, Streams, RESP3, MEMORY, LATENCY, Eviction
- [ ] Audit all spec files for accuracy against implementation
- [ ] Add missing command documentation to types/\*.md files

---

## Critical Abstractions

These must exist from the initial foundation to avoid refactoring:

| Abstraction                | Initial (Stub)          | Current Status            |
| -------------------------- | ----------------------- | ------------------------- |
| `Store` trait              | HashMapStore            | Same                      |
| `Command` trait            | Full                    | Same                      |
| `Value` enum               | StringValue only        | All types ✓               |
| `WalWriter` trait          | NoopWalWriter           | RocksDB WAL ✓             |
| `ReplicationConfig`        | Standalone              | Primary/Replica ✓         |
| `ReplicationTracker` trait | NoopTracker             | WAL streaming ✓           |
| `AclChecker` trait         | AllowAllChecker         | Full ACL ✓                |
| `MetricsRecorder` trait    | NoopRecorder            | Prometheus ✓              |
| `Tracer` trait             | NoopTracer              | OpenTelemetry ✓           |
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
- [optimizations/](../todo/optimizations/INDEX.md) - Performance profiling and optimization
