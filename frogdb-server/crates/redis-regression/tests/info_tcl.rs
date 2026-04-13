//! Rust port of Redis 8.6.0 `unit/info.tcl` test suite.
//!
//! All 27 upstream tests are currently excluded. FrogDB has a different
//! architecture than Redis (multi-threaded sharded vs single-threaded event
//! loop), so many metrics either don't apply or need different implementations.
//! This file categorizes each test as either permanently out of scope or as a
//! potential observability gap to revisit.
//!
//! ## Intentional exclusions
//!
//! ### Not applicable to FrogDB architecture
//!
//! These tests exercise Redis-internal metrics that have no meaningful
//! equivalent in FrogDB's multi-threaded, sharded architecture:
//!
//! - `stats: eventloop metrics` — redis-specific — Redis single-threaded event loop cycle tracking
//! - `stats: instantaneous metrics` — redis-specific — Redis event loop instantaneous sampling
//! - `stats: debug metrics` — redis-specific — Redis DEBUG info section (AOF/cron duration sums)
//! - `stats: client input and output buffer limit disconnections` — redis-specific — Redis buffer limit stats; also needs DEBUG
//! - `memory: database and pubsub overhead and rehashing dict count` — redis-specific — Redis dict/rehashing internals (MEMORY STATS)
//! - `memory: used_memory_peak_time is updated when used_memory_peak is updated` — redis-specific — Redis-specific peak timestamp tracking
//! - `Verify that LUT overhead is properly updated when dicts are emptied or reused` — intentional-incompatibility:cluster — cluster-specific Redis dict internals
//! - `errorstats: limit errors will not increase indefinitely` — intentional-incompatibility:observability — Redis-internal 128-error-type cap behavior
//! - `errorstats: blocking commands` — intentional-incompatibility:observability — CLIENT UNBLOCK error type tracking (UNBLOCKED error prefix)
//!
//! ### Observability gap: per-command latency tracking
//!
//! FrogDB does not yet implement per-command latency percentile tracking
//! (Redis `latency-tracking` config + `latencystats_*` INFO fields). If
//! per-command latency observability is desired, these tests define the
//! expected behavior:
//!
//! - `latencystats: disable/enable` — intentional-incompatibility:observability — CONFIG SET latency-tracking yes/no, p50/p99/p99.9 output
//! - `latencystats: configure percentiles` — intentional-incompatibility:observability — CONFIG SET latency-tracking-info-percentiles
//! - `latencystats: bad configure percentiles` — intentional-incompatibility:observability — config validation (non-numeric, >100)
//! - `latencystats: blocking commands` — intentional-incompatibility:observability — latency tracking for BLPOP and similar
//! - `latencystats: subcommands` — intentional-incompatibility:observability — per-subcommand latency (CLIENT|ID, CONFIG|SET)
//! - `latencystats: measure latency` — intentional-incompatibility:observability — verify latency magnitude (also needs:debug)
//!
//! ### Observability gap: error and command stats
//!
//! FrogDB tracks command call counts in `commandstats` but does not yet track
//! `rejected_calls`, `failed_calls`, or per-error-type `errorstat_*` counters.
//! `total_error_replies` is hardcoded to 0. These tests define the expected
//! error-tracking behavior:
//!
//! - `errorstats: failed call authentication error` — intentional-incompatibility:observability — AUTH failure → errorstat ERR count=1
//! - `errorstats: failed call within MULTI/EXEC` — intentional-incompatibility:observability — error tracking across transactions
//! - `errorstats: failed call within LUA` — intentional-incompatibility:observability — error tracking in EVAL/pcall
//! - `errorstats: failed call NOSCRIPT error` — intentional-incompatibility:observability — EVALSHA → errorstat NOSCRIPT
//! - `errorstats: failed call NOGROUP error` — intentional-incompatibility:observability — XGROUP CREATECONSUMER → errorstat NOGROUP
//! - `errorstats: rejected call unknown command` — intentional-incompatibility:observability — unknown command → errorstat ERR
//! - `errorstats: rejected call within MULTI/EXEC` — intentional-incompatibility:observability — arity error in MULTI queuing
//! - `errorstats: rejected call due to wrong arity` — intentional-incompatibility:observability — wrong arg count → rejected_calls=1
//! - `errorstats: rejected call by OOM error` — intentional-incompatibility:observability — maxmemory → errorstat OOM, rejected_calls=1
//! - `errorstats: rejected call by authorization error` — intentional-incompatibility:observability — ACL → errorstat NOPERM, rejected_calls=1
//!
//! ### Observability gap: client stats
//!
//! FrogDB does not yet expose `pubsub_clients`, `watching_clients`, or
//! `total_watched_keys` in INFO clients. These are trackable with current
//! architecture (connection state already knows pubsub/watch status):
//!
//! - `clients: pubsub clients` — intentional-incompatibility:observability — pubsub_clients count in INFO clients section
//! - `clients: watching clients` — intentional-incompatibility:observability — watching_clients, total_watched_keys in INFO clients; watch=N in CLIENT INFO
