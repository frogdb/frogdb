# Potential Features & Future Enhancements

Ideas and designs not yet implemented. Extracted from spec docs during audit.

## Unimplemented Feature Proposals

- [NEW_FEATURES.md](NEW_FEATURES.md) — Competitive analysis + 6 feature proposals
  (tiered 3-tier, auto-rebalancing, feature store, vector search, event sourcing, migration)
- [TOKIO_CAUSAL_PROFILER.md](TOKIO_CAUSAL_PROFILER.md) — Research: causal profiler for async Rust

## Cluster Remaining Work

- DFLYMIGRATE streaming protocol (high-throughput slot migration)
- Jepsen/Turmoil chaos testing for cluster

## Storage Optimizations

- **Dashtable implementation** (from [STORAGE.md](../spec/STORAGE.md), [EVICTION.md](../spec/EVICTION.md)) —
  DragonflyDB's custom hash table based on "Dash: Scalable Hashing on Persistent Memory". Per-entry
  overhead ~20 bits (vs 64 bits in Redis), no resize spikes, 30-60% less memory. No existing Rust
  crate implements this algorithm.

## Eviction Enhancements

- **DragonflyDB 2Q LFRU algorithm** (from [EVICTION.md](../spec/EVICTION.md)) — 2Q algorithm
  (1994 paper) with probationary/protected buffers integrated with Dashtable segments. Zero per-key
  memory overhead, O(1) eviction at segment boundaries, naturally filters scan pollution. Becomes
  viable if/when a custom Dashtable is implemented.

## Tiered Storage Enhancements

- **Per-field tiering** (from [TIERED.md](../spec/TIERED.md)) — Keep collection scaffolding in
  RAM, demote individual field values to disk. More efficient for partial access to large
  collections (10K+ elements), but dramatically increases complexity: each collection type needs
  its own tiering strategy, RocksDB key scheme becomes `{key}\x00{field}`, sorted set range
  queries need score index in RAM with values on disk.
- **Future enhancements table** (from [TIERED.md](../spec/TIERED.md)):
  - Lazy promotion — read warm values without promoting (saves memory for one-off reads)
  - Compression — per-tier compression settings (heavier for warm)
  - Key patterns — route specific key patterns to always stay hot
  - Warm-only writes — write large values directly to warm tier
  - Cold tier — S3/DynamoDB backends for archival

## Security

- **mTLS CN/SAN to ACL user mapping** (from [TLS.md](../spec/TLS.md)) — Map client certificate
  identity to ACL users, enabling certificate-based authentication without passwords. CN could map
  to ACL username directly or via explicit mapping table. Would allow mTLS to serve as sole
  authentication mechanism. See [AUTH.md](../spec/AUTH.md) for the ACL system.

## Persistence

- **`wal_failure_policy: rollback` mode** (from [PERSISTENCE.md](../spec/PERSISTENCE.md)) — A
  configurable mode providing stricter consistency at the cost of performance. Current behavior
  matches Redis AOF: in-memory write applied before WAL durability is guaranteed, no rollback.

## Observability & Operations

- **Built-in rate limiting** (from [FAILURE_MODES.md](../spec/FAILURE_MODES.md),
  [CONNECTION.md](../spec/CONNECTION.md)) — Per-ACL-user rate limiting with
  `commands_per_second` and `bytes_per_second` controls. Current mitigation: client-side or
  proxy-based rate limiting (envoy, nginx).
- **`frogdb-admin diagnostic-bundle` CLI tool** (from
  [TROUBLESHOOTING.md](../spec/TROUBLESHOOTING.md)) — Generate diagnostic bundles containing
  server info, config, logs, metrics snapshots, and memory/latency state.

## Testing

- **Deterministic Simulation Testing / MadSim** (from [TESTING.md](../spec/TESTING.md)) —
  Full deterministic simulation of network, time, and I/O using MadSim or custom DST (like
  TigerBeetle's VOPR). Evolution path: Shuttle (current) → MadSim → potentially custom DST.
  Also Antithesis (commercial platform from FoundationDB founders).
- **Jepsen distributed correctness testing** (from [TESTING.md](../spec/TESTING.md)) —
  Black-box distributed systems verification with fault injection (network partitions, node
  failures, clock skew) and linearizability checking via Elle. Prerequisites: clustering
  complete, multi-node deployment working, basic fault tolerance implemented.
