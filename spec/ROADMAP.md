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

## Benchmark Comparisons

**Goal**: Performance comparison with Redis/Valkey/Dragonfly.

- [x] Benchmark harness setup (Redis)
- [ ] Valkey comparison benchmarks
- [ ] Dragonfly comparison benchmarks
- [ ] Performance report generation

### Clustering / Replication

**Goal**: Distributed operation support with primary-replica replication and Redis Cluster protocol compatibility.

See [CLUSTER_PLAN.md](CLUSTER_PLAN.md) for the detailed implementation plan.

**Implementation Phases:**

| Phase | Description | Key Deliverables |
|-------|-------------|------------------|
| 1 | Primary-Replica Replication | PSYNC protocol, WAL streaming, WAIT command, ROLE/INFO |
| 2 | Admin API & Cluster Topology | HTTP admin port, slot assignment, cluster state management |
| 3 | Client Protocol & Redirects | MOVED/ASK redirects, CLUSTER commands, hash slot routing |
| 4 | Failover Support | Replica promotion, REPLICAOF NO ONE, self-fencing |
| 5 | Slot Migration | Live migration protocol, slot state machine |
| 6 | Testing & Validation | Turmoil chaos tests, Jepsen linearizability, integration tests |

**Phase 1 Tasks (First Milestone):**
- [ ] Replication via WAL streaming (RocksDB `GetUpdatesSince`)
- [ ] PSYNC/FULLRESYNC protocol implementation
- [ ] REPLCONF handshake and ACK tracking
- [ ] WAIT command with replica acknowledgment
- [ ] ROLE command - Report replication role (primary/replica)
- [ ] INFO replication section

**Later Phases:**
- [ ] Admin HTTP API (port 6380)
- [ ] CLUSTER commands (SLOTS, NODES, KEYSLOT, etc.)
- [ ] Hash slot routing and MOVED/ASK redirects
- [ ] Failover and replica promotion
- [ ] Live slot migration
- [ ] `BGREWRITEAOF` - Stub returning appropriate message (N/A for RocksDB)

### Distributed Cluster Testing

**Goal**: Correctness testing for clustered operation.

- [ ] Turmoil deterministic simulation tests
- [ ] Cluster partition/chaos tests (Jepsen)
- [ ] Cluster failover tests
- [ ] Cluster linearizability tests

## Distributed Single-Node Testing

**Goal**: Correctness testing for single-node operation.

- [ ] Jepsen test harness integration

### Config validation audit

- [ ] Validation + errors for invalid config

### Observability audit

- [ ] Could a skilled software reliability engineer be able to easily diagnose issues with frogdb?

### Operational Readiness

Inspired by: **CockroachDB**, **DragonflyDB**, **FoundationDB**, **Redis**, **Valkey**

#### HIGH PRIORITY - Essential for Production Readiness

##### 1. Machine-Readable Status JSON Endpoint

**Inspired by:** FoundationDB `status json`

Comprehensive JSON snapshot of server health accessible via HTTP or command:

```
GET /status/json
STATUS JSON  # Redis command
```

Returns: server info, memory stats, per-shard health, client counts, persistence status, health issues.

**Why:** Enables programmatic monitoring, automation decisions, custom dashboards.
**Complexity:** Medium

- [ ] Implement `/status/json` HTTP endpoint
- [ ] Implement `STATUS JSON` Redis-protocol command
- [ ] Include all health indicators in response

##### 2. Hot Shard Detection

**Inspired by:** CockroachDB Hot Ranges, Redis hot key detection

Identifies shards receiving disproportionate traffic:

```
DEBUG HOTSHARDS [PERIOD <seconds>]
INFO hotshards
```

Reports: ops/sec per shard, percentage distribution, queue depths, recommendations.

**Why:** Critical for FrogDB's shared-nothing architecture; identifies poor key distribution.
**Complexity:** Medium

- [ ] Track per-shard operation counts with windowed aggregation
- [ ] Implement `DEBUG HOTSHARDS` command
- [ ] Add `hotshards` section to INFO output
- [ ] Generate recommendations when imbalance detected

##### 3. Latency Band Tracking (SLO Monitoring)

**Inspired by:** FoundationDB latency band tracking

Server-side latency measurement against configurable thresholds:

```toml
[latency_bands]
bands = [1, 5, 10, 50, 100, 500]  # ms
```

```
LATENCY BANDS [command]
```

Reports: percentage of requests in each latency bucket.

**Why:** SLO-based alerting without external aggregation; maps directly to business requirements.
**Complexity:** Low

- [ ] Add `[latency_bands]` configuration section
- [ ] Track request counts per latency band
- [ ] Implement `LATENCY BANDS` command
- [ ] Export latency band metrics to Prometheus

##### 4. Admin Port Separation

**Inspired by:** DragonflyDB `--admin_bind`, `--admin_port`

Separate port for administrative commands:

```toml
[admin]
bind = "127.0.0.1"
port = 6380
```

**Why:** Security (network-level ACL), availability (no competition with client traffic), compliance.
**Complexity:** Medium

- [ ] Add `[admin]` configuration section
- [ ] Create separate listener for admin port
- [ ] Route admin-only commands (DEBUG, CONFIG SET, SHUTDOWN) to admin port
- [ ] Document recommended firewall rules

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

#### MEDIUM PRIORITY - Improves Debuggability

##### 6. Enhanced MEMORY DOCTOR

**Inspired by:** Redis MEMORY DOCTOR

Extend with actionable recommendations:

- Identify specific big keys (>1MB)
- Detect shard memory imbalance
- Provide specific remediation steps

**Complexity:** Medium

- [ ] Scan for big keys during MEMORY DOCTOR
- [ ] Calculate shard memory variance
- [ ] Generate specific remediation recommendations
- [ ] Add estimated memory savings per recommendation

##### 7. DEBUG HASHING Command

**Inspired by:** FrogDB-specific need

Show key-to-shard mapping:

```
DEBUG HASHING user:123
key:user:123 hash:0x7f1234... shard:3 num_shards:8
```

**Why:** Essential for debugging hotspots; understand hash tag behavior.
**Complexity:** Low

- [ ] Implement `DEBUG HASHING <key>` command
- [ ] Show hash value, shard assignment, hash tag detection
- [ ] Support multiple keys in single command

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

##### 9. Client Connection Statistics

**Inspired by:** CockroachDB connection diagnostics

Per-client command statistics:

```
CLIENT STATS [ID <client-id>]
```

Reports: commands processed, bytes sent/received, avg/p99 latency, command breakdown.

**Why:** Identify misbehaving clients, "noisy neighbor" debugging.
**Complexity:** Medium

- [ ] Track per-client command counts and latencies
- [ ] Implement `CLIENT STATS` command
- [ ] Add client-level bytes sent/received tracking
- [ ] Include command type breakdown per client

##### 10. Persistence Lag Monitoring

**Inspired by:** FoundationDB durability lag

Track how far behind persistence is:

```
INFO persistence
```

Reports: WAL lag (operations, bytes), last sync time/latency, durability lag in ms.

**Why:** Critical for understanding data durability guarantees.
**Complexity:** Medium

- [ ] Track uncommitted WAL operations count
- [ ] Calculate durability lag in milliseconds
- [ ] Add `persistence` section to INFO with lag metrics
- [ ] Export persistence lag metrics to Prometheus

#### NICE TO HAVE - Advanced Differentiation

##### 11. Debug Diagnostic Bundles

**Inspired by:** CockroachDB statement diagnostics

Generate downloadable ZIP with traces, slowlog, config, stats:

```
DEBUG BUNDLE GENERATE [DURATION <seconds>]
GET /debug/bundle
```

**Why:** Share with support teams; post-mortem analysis.
**Complexity:** High

- [ ] Implement diagnostic data collection
- [ ] Create ZIP archive generation
- [ ] Add `/debug/bundle` HTTP endpoint
- [ ] Include: config, INFO, SLOWLOG, recent traces, memory stats

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

##### 13. DEBUG DUMP-VLL-QUEUE

**Inspired by:** FrogDB VLL architecture

Inspect VLL transaction queues:

```
DEBUG DUMP-VLL-QUEUE [shard_id]
```

**Why:** Debug VLL-related latency; unique to FrogDB.
**Complexity:** Medium

- [ ] Expose VLL queue state safely
- [ ] Show pending transactions per shard
- [ ] Include lock contention information
- [ ] Add queue depth history

##### 14. Intrinsic Latency Testing CLI

**Inspired by:** Redis `redis-cli --intrinsic-latency`

Measure baseline system latency:

```bash
frogdb-cli --intrinsic-latency 100
```

**Why:** Distinguish system vs FrogDB issues; essential for cloud/containers.
**Complexity:** Low

- [ ] Add `--intrinsic-latency <seconds>` flag to CLI
- [ ] Measure scheduling/timer jitter
- [ ] Report min/max/avg/p99 system latency
- [ ] Provide interpretation guidance

##### 15. OpenTelemetry Tracing Diagnostics

**Inspired by:** CockroachDB trace bundles

Expose trace sampling status and recent trace IDs:

```
DEBUG TRACING STATUS
DEBUG TRACING RECENT [count]
```

**Why:** Correlate FrogDB traces with application traces.
**Complexity:** Low

- [ ] Implement `DEBUG TRACING STATUS` command
- [ ] Track recent trace IDs with timestamps
- [ ] Implement `DEBUG TRACING RECENT` command
- [ ] Show sampling rate and export status

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
