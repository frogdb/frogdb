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
//! - `stats: eventloop metrics` — Redis single-threaded event loop cycle tracking
//! - `stats: instantaneous metrics` — Redis event loop instantaneous sampling
//! - `stats: debug metrics` — Redis DEBUG info section (AOF/cron duration sums)
//! - `stats: client input and output buffer limit disconnections` — Redis buffer limit stats; also needs DEBUG
//! - `memory: database and pubsub overhead and rehashing dict count` — Redis dict/rehashing internals (MEMORY STATS)
//! - `memory: used_memory_peak_time is updated when used_memory_peak is updated` — Redis-specific peak timestamp tracking
//! - `Verify that LUT overhead is properly updated when dicts are emptied or reused` — cluster-specific Redis dict internals
//! - `errorstats: limit errors will not increase indefinitely` — Redis-internal 128-error-type cap behavior
//! - `errorstats: blocking commands` — CLIENT UNBLOCK error type tracking (UNBLOCKED error prefix)
//!
//! ### Observability gap: per-command latency tracking
//!
//! FrogDB does not yet implement per-command latency percentile tracking
//! (Redis `latency-tracking` config + `latencystats_*` INFO fields). If
//! per-command latency observability is desired, these tests define the
//! expected behavior:
//!
//! - `latencystats: disable/enable` — CONFIG SET latency-tracking yes/no, p50/p99/p99.9 output
//! - `latencystats: configure percentiles` — CONFIG SET latency-tracking-info-percentiles
//! - `latencystats: bad configure percentiles` — config validation (non-numeric, >100)
//! - `latencystats: blocking commands` — latency tracking for BLPOP and similar
//! - `latencystats: subcommands` — per-subcommand latency (CLIENT|ID, CONFIG|SET)
//! - `latencystats: measure latency` — verify latency magnitude (also needs:debug)
//!
//! ### Observability gap: error and command stats
//!
//! FrogDB tracks command call counts in `commandstats` but does not yet track
//! `rejected_calls`, `failed_calls`, or per-error-type `errorstat_*` counters.
//! `total_error_replies` is hardcoded to 0. These tests define the expected
//! error-tracking behavior:
//!
//! - `errorstats: failed call authentication error` — AUTH failure → errorstat ERR count=1
//! - `errorstats: failed call within MULTI/EXEC` — error tracking across transactions
//! - `errorstats: failed call within LUA` — error tracking in EVAL/pcall
//! - `errorstats: failed call NOSCRIPT error` — EVALSHA → errorstat NOSCRIPT
//! - `errorstats: failed call NOGROUP error` — XGROUP CREATECONSUMER → errorstat NOGROUP
//! - `errorstats: rejected call unknown command` — unknown command → errorstat ERR
//! - `errorstats: rejected call within MULTI/EXEC` — arity error in MULTI queuing
//! - `errorstats: rejected call due to wrong arity` — wrong arg count → rejected_calls=1
//! - `errorstats: rejected call by OOM error` — maxmemory → errorstat OOM, rejected_calls=1
//! - `errorstats: rejected call by authorization error` — ACL → errorstat NOPERM, rejected_calls=1
//!
//! ### Observability gap: client stats
//!
//! FrogDB does not yet expose `pubsub_clients`, `watching_clients`, or
//! `total_watched_keys` in INFO clients. These are trackable with current
//! architecture (connection state already knows pubsub/watch status):
//!
//! - `clients: pubsub clients` — pubsub_clients count in INFO clients section
//! - `clients: watching clients` — watching_clients, total_watched_keys in INFO clients; watch=N in CLIENT INFO
