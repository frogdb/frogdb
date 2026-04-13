//! Rust port of Redis 8.6.0 `unit/client-eviction.tcl` test suite.
//!
//! The upstream suite exercises `CONFIG SET maxmemory-clients` together
//! with the `tot-mem=` / `qbuf=` / `omem=` fields in `CLIENT LIST` to
//! verify that clients whose buffers exceed the configured budget are
//! evicted in size order (largest first) and that the `CLIENT NO-EVICT`
//! flag protects clients from the reaper.
//!
//! FrogDB does not implement the `maxmemory-clients` configuration
//! parameter — attempting `CONFIG SET maxmemory-clients <v>` errors
//! with `ERR Unknown CONFIG parameter 'maxmemory-clients'`. FrogDB's
//! per-client memory accounting is also absent: `CLIENT LIST` always
//! reports `tot-mem=0`, `qbuf=0`, `qbuf-free=0`, `obl=0`, `oll=0`,
//! `omem=0`, `argv-mem=0`, `multi-mem=0`. Without those counters there
//! is no meaningful signal to drive the eviction logic off of, so no
//! test in the upstream file can run against FrogDB.
//!
//! `CLIENT NO-EVICT on|off` is accepted by FrogDB as a no-op for
//! protocol compatibility (see `unit/introspection.tcl`'s coverage in
//! `introspection_tcl.rs`), but without an underlying eviction loop
//! there's nothing to assert about its side-effects here.
//!
//! ## Intentional exclusions
//!
//! - `client evicted due to large argv` — intentional-incompatibility:memory — `maxmemory-clients` not implemented
//! - `client evicted due to large query buf` — intentional-incompatibility:memory — `maxmemory-clients` not implemented
//! - `client evicted due to percentage of maxmemory` — intentional-incompatibility:memory — `maxmemory-clients` not implemented
//! - `client evicted due to large multi buf` — intentional-incompatibility:memory — `maxmemory-clients` not implemented
//! - `client evicted due to watched key list` — intentional-incompatibility:memory — `maxmemory-clients` not implemented
//! - `client evicted due to pubsub subscriptions` — intentional-incompatibility:memory — `maxmemory-clients` not implemented
//! - `client evicted due to tracking redirection` — intentional-incompatibility:memory — `maxmemory-clients` not implemented
//! - `client evicted due to client tracking prefixes` — intentional-incompatibility:memory — `maxmemory-clients` not implemented
//! - `client evicted due to output buf` — intentional-incompatibility:memory — `maxmemory-clients` not implemented
//! - `client no-evict $no_evict` — intentional-incompatibility:memory — `maxmemory-clients` not implemented
//! - `avoid client eviction when client is freed by output buffer limit` — intentional-incompatibility:memory — `maxmemory-clients` not implemented
//! - `decrease maxmemory-clients causes client eviction` — intentional-incompatibility:memory — `maxmemory-clients` not implemented
//! - `evict clients only until below limit` — intentional-incompatibility:memory — `maxmemory-clients` not implemented
//! - `evict clients in right order (large to small)` — intentional-incompatibility:memory — `maxmemory-clients` not implemented
//! - `client total memory grows during $type` — intentional-incompatibility:memory — `maxmemory-clients` not implemented
