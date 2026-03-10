# FrogDB Roadmap

## Cluster & Reliability

- [CLUSTER_REBALANCING.md](CLUSTER_REBALANCING.md) — Atomic slot migration + auto-rebalancing (Valkey 9-style)
- [SPLIT_BRAIN_REPLAY.md](SPLIT_BRAIN_REPLAY.md) — Split-brain replay: `SPLITBRAIN` command, automatic recovery replay, CLI tool
- Cluster testing: key movement during slot migration, Turmoil/Jepsen deterministic simulation, failover & linearizability tests

## Security & Networking

- [TLS_PLAN.md](TLS_PLAN.md) — Full TLS: client TLS, mTLS, replication TLS, cluster bus TLS, hot-reload

## Features

- [CLIENT_TRACKING.md](CLIENT_TRACKING.md) — Client-side cache invalidation (`CLIENT TRACKING`)
- [CLI.md](CLI.md) — `frog` operational CLI tool

## Operational

- Grafana dashboard templates (overview, performance, shards, persistence — `dashboard-gen` exists)
- Automated alert rule generation (`/alerts/prometheus` endpoint)
- Enhanced `LATENCY DOCTOR` (correlation detection, SLOWLOG cross-reference, scatter-gather analysis)
- [ROLLING_UPGRADE.md](ROLLING_UPGRADE.md) — Rolling upgrades (not yet implemented)

## Performance

- [optimizations/INDEX.md](optimizations/INDEX.md) — io_uring, arena allocator, SIMD, single-shard mode

## Code Quality

- `types.rs` decomposition — extract `stream` (~860 lines) and `sorted_set` (~1,202 lines) into own modules
- Sorted set parsing helper extraction (`parse_score_bound`, `parse_lex_bound`, `parse_set_op_options`)
- Spec file audit for accuracy against implementation

---

## Stub / Unimplemented Commands

These are intentional architectural decisions — FrogDB uses a single database per instance with no module system.

| Command         | Status        | Notes                                                 | Spec                                                                        |
| --------------- | ------------- | ----------------------------------------------------- | --------------------------------------------------------------------------- |
| MODULE commands | Not planned   | No modular architecture                               | [COMPATIBILITY.md](../spec/COMPATIBILITY.md)                                |
| SELECT          | Not supported | `SELECT 0` accepted as no-op; non-zero returns error  | [COMPATIBILITY.md](../spec/COMPATIBILITY.md#single-database)                |
| SWAPDB          | Not supported | Returns `DatabaseNotSupported` — single database      | [COMPATIBILITY.md](../spec/COMPATIBILITY.md#database-model)                 |
| MOVE            | Not supported | Returns `DatabaseNotSupported` — single database      | [COMPATIBILITY.md](../spec/COMPATIBILITY.md#database-model)                 |
