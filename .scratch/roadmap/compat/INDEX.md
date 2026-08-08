# Redis 8.6.0 Compatibility — remaining work

The original audit (271 tests across 22 upstream files, 14 feature areas) is **complete**
except for two deferred adaptation areas:

| # | Feature | Tests | File | Status |
|---|---------|-------|------|--------|
| 14d | [Query Buffer Observability](14d-query-buffer-observability.md) | 3 | `querybuf_tcl.rs` | Deferred — `qbuf`/`qbuf-free`/`argv-mem` in CLIENT LIST are hardcoded zeros |
| 14e | [Runtime Metrics Adaptation](14e-runtime-metrics-adaptation.md) | 10 | mixed | Deferred — Redis runtime metrics (IO threads, eventloop, dict introspection) need tokio/RocksDB equivalents |

52 tests are permanently excluded (encoding, RDB/AOF, CLI, single-DB) — see
[excluded.md](excluded.md). File-level exclusions (upstream `.tcl` files that will never get a
port) are documented in the `redis-regression` crate's module doc
(`frogdb-server/crates/redis-regression/src/lib.rs`).

The per-area action-item breakdown this index used to carry is in git history.
