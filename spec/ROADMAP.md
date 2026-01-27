# FrogDB Implementation Roadmap

This document tracks the implementation progress of FrogDB. Each phase has specific deliverables with checkboxes for progress tracking.

## Distributed Cluster Testing

**Implementation Gaps:**
- [ ] Implement actual key movement during slot migration (CLUSTER SETSLOT updates metadata but doesn't move keys)
- [ ] Implement auto full-resync trigger when replica lag exceeds threshold (currently only logs warning at primary.rs:451)

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

##### Split connection.rs (HIGH effort)

`crates/server/src/connection.rs` is 5,932 lines handling multiple concerns.

- [ ] Extract auth handling to `connection/auth.rs`
- [ ] Extract transaction handling to `connection/transaction.rs`
- [ ] Extract pub/sub handling to `connection/pubsub.rs`
- [ ] Extract blocking state to `connection/blocking.rs`
- [ ] Extract routing logic to `connection/routing.rs`
- [ ] Extract scatter-gather to `connection/scatter_gather.rs`

##### Split shard.rs (MEDIUM effort)

`crates/core/src/shard.rs` is 3,720 lines with eviction logic mixed in.

- [ ] Extract eviction strategies to `shard/eviction.rs`

##### Split types.rs (MEDIUM-HIGH effort)

`crates/core/src/types.rs` is 3,668 lines containing all value types.

- [ ] Extract string types to `types/string.rs`
- [ ] Extract list types to `types/list.rs`
- [ ] Extract set types to `types/set.rs`
- [ ] Extract hash types to `types/hash.rs`
- [ ] Extract sorted set types to `types/sorted_set.rs`
- [ ] Extract stream types to `types/stream.rs`
- [ ] Extract specialized types (BloomFilter, HyperLogLog, TimeSeries, Json) to `types/specialized.rs`

##### Config magic numbers (LOW effort)

- [ ] Define named constants for timeout values and sizes in `crates/server/src/config.rs`

##### Sorted set parsing helpers (LOW effort)

- [ ] Extract `parse_score_bound()` and `parse_lex_bound()` to utils.rs (if not already covered)
- [ ] Extract `parse_set_op_options()` to utils.rs

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
