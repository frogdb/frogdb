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
- Rolling upgrades — spec at `docs/spec/ROLLING_UPGRADE.md`

## Performance

- [optimizations/INDEX.md](optimizations/INDEX.md) — io_uring, arena allocator, lock-free WAL, SIMD, single-shard mode

## Code Quality

- `types.rs` decomposition (string, list, set, hash, sorted_set, stream modules)
- Config magic numbers → named constants
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
