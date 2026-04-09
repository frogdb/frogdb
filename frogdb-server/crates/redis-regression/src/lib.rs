//! # redis-regression
//!
//! Rust regression tests for Redis 8.6.0 wire-protocol compatibility. Each
//! `*_tcl.rs` file under `tests/` ports a specific upstream `.tcl` file from
//! the Redis 8.6.0 test suite. Per-port intentional test-level exclusions
//! are documented in each file's own `## Intentional exclusions`
//! doc-comment section; this crate-level module doc captures the
//! **file-level** exclusions (upstream `.tcl` files that will never get a
//! Rust port at all).
//!
//! ## Out-of-scope upstream files
//!
//! These upstream files will **never** get Rust ports. The list is
//! maintained here as an explicit decision-record so that removing the
//! `testing/redis-compat/` TCL runner (Phase 5) doesn't silently drop
//! coverage. Each entry has a one-line reason. See
//! [`todo/COMPATIBILITY.md`] for the companion roadmap and the port
//! files under `tests/` for the tests that *are* implemented.
//!
//! [`todo/COMPATIBILITY.md`]: ../../../../todo/COMPATIBILITY.md
//!
//! ### AOF — FrogDB uses RocksDB instead of Redis AOF
//! - `integration/aof.tcl` — AOF file format and loading
//! - `integration/aof-multi-part.tcl` — AOF manifest / multi-part format
//! - `integration/aof-race.tcl` — AOF rewrite race conditions
//! - `unit/aofrw.tcl` — AOF rewrite internals
//!
//! ### RDB — FrogDB uses RocksDB snapshots instead of Redis RDB
//! - `integration/rdb.tcl` — RDB file format and encoding
//! - `integration/corrupt-dump.tcl` — Corrupt RDB payload handling
//! - `integration/corrupt-dump-fuzzer.tcl` — RDB corruption fuzzing
//! - `integration/convert-ziplist-hash-on-load.tcl` — Legacy ziplist hash encoding migration
//! - `integration/convert-ziplist-zset-on-load.tcl` — Legacy ziplist zset encoding migration
//! - `integration/convert-zipmap-hash-on-load.tcl` — Legacy zipmap hash encoding migration
//!
//! ### Replication / PSYNC — different replication model
//! - `integration/replication.tcl` — Base replication tests
//! - `integration/replication-2.tcl` — Replication follow-up tests
//! - `integration/replication-3.tcl` — Replication follow-up tests
//! - `integration/replication-4.tcl` — Replication follow-up tests
//! - `integration/replication-buffer.tcl` — Replication buffer memory management
//! - `integration/replication-iothreads.tcl` — I/O threaded replication
//! - `integration/replication-psync.tcl` — PSYNC protocol
//! - `integration/replication-rdbchannel.tcl` — RDB-channel replication
//! - `integration/psync2.tcl` — PSYNC2 protocol
//! - `integration/psync2-master-restart.tcl` — PSYNC2 master restart
//! - `integration/psync2-pingoff.tcl` — PSYNC2 with ping suppression
//! - `integration/psync2-reg.tcl` — PSYNC2 regression
//! - `integration/block-repl.tcl` — Blocked-command replication
//! - `integration/failover.tcl` — `FAILOVER` command
//!
//! ### Server lifecycle — different shutdown / logging model
//! - `integration/shutdown.tcl` — Shutdown with lagging replicas
//! - `integration/logging.tcl` — Server crash / stack-trace logging
//! - `integration/dismiss-mem.tcl` — Fork-child memory dismissal
//! - `unit/shutdown.tcl` — Redis shutdown / signal handling
//!
//! ### Tool integrations — require Redis-specific binaries
//! - `integration/redis-cli.tcl` — requires the `redis-cli` binary
//! - `integration/redis-benchmark.tcl` — requires the `redis-benchmark` binary
//!
//! ### Linux / platform-specific
//! - `unit/oom-score-adj.tcl` — Linux `/proc/self/oom_score_adj` tuning
//!
//! ### Redis-specific memory / buffer assertions
//! - `unit/limits.tcl` — Large-memory assertion test
//! - `unit/obuf-limits.tcl` — Redis-specific client output-buffer limits
//!
//! ### Trivial / informational
//! - `unit/printver.tcl` — 0 tests, just prints the Redis version
//!
//! ### Deferred features (may become in-scope later)
//! - `unit/tls.tcl` — TLS not yet implemented (tracked in `todo/TLS_PLAN.md`)
//!
//! ### Cluster internals — gossip and command-set differ from Redis
//! - `unit/cluster/cli.tcl` — requires the `redis-cli` binary
//! - `unit/cluster/cluster-response-tls.tcl` — TLS-dependent cluster tests
//! - `unit/cluster/failure-marking.tcl` — Cluster failure-detection internals
//! - `unit/cluster/human-announced-nodename.tcl` — Cluster gossip nodename format
//! - `unit/cluster/internal-secret.tcl` — Cluster shared-secret protocol
//! - `unit/cluster/links.tcl` — `CLUSTER LINKS` introspection internals
//! - `unit/cluster/slot-ownership.tcl` — Slot-ownership gossip internals
//!
//! ### Whole directories out of scope
//! - `tests/unit/moduleapi/` (45 files) — FrogDB has no module system
//! - `tests/sentinel/` (16 files) — Redis Sentinel out of scope
//! - `tests/cluster/` (28 files, legacy runner) — superseded by `tests/unit/cluster/`
//! - `tests/helpers/`, `tests/support/` — TCL helpers, not tests
//!
//! **Total: 42 individual files + 4 whole directories.**
