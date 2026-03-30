# FrogDB Roadmap

## Cross-Cutting

- [POTENTIAL.md](POTENTIAL.md) — Speculative features and future enhancements extracted from spec docs
- [NEW_FEATURES.md](NEW_FEATURES.md) — Competitive analysis + 6 unimplemented feature proposals
- [TOKIO_CAUSAL_PROFILER.md](../TOKIO_CAUSAL_PROFILER.md) — Research: causal profiler for async Rust

## Cluster & Reliability

- [CLUSTER_REBALANCING.md](CLUSTER_REBALANCING.md) — Atomic slot migration + auto-rebalancing (Valkey 9-style)
- [SPLIT_BRAIN_REPLAY.md](SPLIT_BRAIN_REPLAY.md) — Split-brain replay: `SPLITBRAIN` command, automatic recovery replay, CLI tool
- Cluster testing: key movement during slot migration, Turmoil/Jepsen deterministic simulation, failover & linearizability tests

## Security & Networking

- [TLS_PLAN.md](TLS_PLAN.md) — Full TLS: client TLS, mTLS, replication TLS, cluster bus TLS, hot-reload

## Features

- [CLI.md](CLI.md) — `frog` operational CLI tool

## Operational

- [METRICS.md](METRICS.md) — Unwired metrics instrumentation plan (22 metrics across 3 effort tiers)
- CONNECTION_LIMITS.md — Max clients feature + connection rejection metrics
- Grafana dashboard templates (overview, performance, shards, persistence — `dashboard-gen` exists)
- Automated alert rule generation (`/alerts/prometheus` endpoint)
- Enhanced `LATENCY DOCTOR` (correlation detection, SLOWLOG cross-reference, scatter-gather analysis)
- [ROLLING_UPGRADE.md](ROLLING_UPGRADE.md) — Rolling upgrades (not yet implemented)

## Performance

- [optimizations/INDEX.md](optimizations/INDEX.md) — io_uring, arena allocator, SIMD, single-shard mode

## Testing

- [FUZZ_FAILURES.md](../../todo/FUZZ_FAILURES.md) — Documented fuzz findings (OOM in deserialize, etc.)

## Code Quality

- `types.rs` decomposition — extract `stream` (~860 lines) and `sorted_set` (~1,202 lines) into own modules
- Sorted set parsing helper extraction (`parse_score_bound`, `parse_lex_bound`, `parse_set_op_options`)
- Spec file audit for accuracy against implementation

---

## Stub / Unimplemented Commands

These are intentional architectural decisions — FrogDB uses a single database per instance with no module system.

| Command         | Status        | Notes                                                 | Spec                                                                        |
| --------------- | ------------- | ----------------------------------------------------- | --------------------------------------------------------------------------- |
| MODULE commands | Not planned   | No modular architecture                               | COMPATIBILITY.md                                |
| SELECT          | Not supported | `SELECT 0` accepted as no-op; non-zero returns error  | COMPATIBILITY.md                |
| SWAPDB          | Not supported | Returns `DatabaseNotSupported` — single database      | COMPATIBILITY.md                 |
| MOVE            | Not supported | Returns `DatabaseNotSupported` — single database      | COMPATIBILITY.md                 |
