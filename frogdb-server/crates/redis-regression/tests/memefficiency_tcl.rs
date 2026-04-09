//! Rust port of Redis 8.6.0 `unit/memefficiency.tcl` test suite.
//!
//! The upstream file has two kinds of tests:
//!
//! 1. A "Memory efficiency with values in range $size_range" test that
//!    writes 10,000 keys and asserts `used_memory / written_bytes` is
//!    above a hand-tuned ratio per value-size bucket. These ratios are
//!    calibrated for jemalloc's size-class behavior and vary wildly
//!    across allocators. The whole `start_server` block is tagged
//!    `external:skip`, so upstream CI only runs it against a freshly
//!    launched Redis instance. FrogDB's memory accounting model is
//!    entirely different (we use mimalloc by default, accounted via
//!    `MEMORY STATS` / `MEMORY USAGE`), and we don't emit a
//!    `used_memory` figure that's comparable to the write workload in
//!    the same way — porting the assertions would require invention
//!    rather than translation.
//!
//! 2. The "Active defrag ..." family. These tests exercise Redis's
//!    jemalloc-integrated active-defragmenter, which walks the main
//!    dictionary / streams / hashes / lists and asks jemalloc to
//!    relocate objects into denser bins to reduce fragmentation. The
//!    implementation depends on jemalloc-specific APIs (`arenas.page`,
//!    `ctl`, etc.) exposed via `DEBUG MALLCTL`, and is controlled via
//!    the `active-defrag-*` config parameters, `activedefrag`, and the
//!    `MEMORY MALLOC-STATS` / `allocator_frag_ratio` INFO fields.
//!    FrogDB does NOT implement active defrag: `activedefrag`,
//!    `active-defrag-threshold-lower`, `active-defrag-cycle-min/max`,
//!    `active-defrag-ignore-bytes` are unknown CONFIG parameters,
//!    `DEBUG MALLCTL` is not a supported subcommand, and
//!    `MEMORY MALLOC-STATS` returns `ERR unknown subcommand`. None of
//!    these tests can run against FrogDB, and the task guidance calls
//!    out `jemalloc defrag not exposed in FrogDB` as the exclusion
//!    rationale.
//!
//! FrogDB does implement `MEMORY USAGE`, `MEMORY STATS`, `MEMORY
//! DOCTOR`, `MEMORY PURGE`, and `MEMORY MALLOC-SIZE` — those are
//! exercised in FrogDB's own regression suites rather than being
//! grafted onto this upstream port.
//!
//! ## Intentional exclusions
//!
//! Allocator-dependent efficiency ratios (upstream `external:skip`):
//! - `Memory efficiency with values in range $size_range` — allocator-calibrated ratio assertion, `external:skip`
//!
//! Active defragmentation (jemalloc defrag not exposed in FrogDB):
//! - `Active defrag main dictionary: $type` — jemalloc defrag not exposed in FrogDB
//! - `Active defrag - AOF loading` — jemalloc defrag not exposed in FrogDB
//! - `Active defrag eval scripts: $type` — jemalloc defrag not exposed in FrogDB
//! - `Active defrag big keys: $type` — jemalloc defrag not exposed in FrogDB
//! - `Active defrag pubsub: $type` — jemalloc defrag not exposed in FrogDB
//! - `Active defrag IDMP streams: $type` — jemalloc defrag not exposed in FrogDB
//! - `Active Defrag HFE with $eb_container: $type` — jemalloc defrag not exposed in FrogDB
//! - `Active defrag for argv retained by the main thread from IO thread: $type` — jemalloc defrag not exposed in FrogDB
//! - `Active defrag big list: $type` — jemalloc defrag not exposed in FrogDB
//! - `Active defrag edge case: $type` — jemalloc defrag not exposed in FrogDB
//! - `Active defrag can't be triggered during replicaof database flush. See issue #14267` — jemalloc defrag not exposed in FrogDB
