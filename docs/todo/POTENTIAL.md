# Potential Features & Future Enhancements

Ideas and designs not yet implemented. Extracted from spec docs during audit.

## Cluster

- DFLYMIGRATE streaming protocol (high-throughput slot migration)

## Storage Optimizations

- **Dashtable implementation** (from [STORAGE.md](../spec/STORAGE.md),
  [EVICTION.md](../spec/EVICTION.md)) — DragonflyDB's custom hash table based on "Dash: Scalable
  Hashing on Persistent Memory". Per-entry overhead ~20 bits (vs 64 bits in Redis), no resize
  spikes, 30-60% less memory. No existing Rust crate implements this algorithm.

## Eviction Enhancements

- **DragonflyDB 2Q LFRU algorithm** (from [EVICTION.md](../spec/EVICTION.md)) — 2Q algorithm (1994
  paper) with probationary/protected buffers integrated with Dashtable segments. Zero per-key memory
  overhead, O(1) eviction at segment boundaries, naturally filters scan pollution. Becomes viable
  if/when a custom Dashtable is implemented.

## Tiered Storage Enhancements

- **Per-field tiering** (from [TIERED.md](../spec/TIERED.md)) — Keep collection scaffolding in RAM,
  demote individual field values to disk. More efficient for partial access to large collections
  (10K+ elements), but dramatically increases complexity: each collection type needs its own tiering
  strategy, RocksDB key scheme becomes `{key}\x00{field}`, sorted set range queries need score index
  in RAM with values on disk.
- **Future enhancements table** (from [TIERED.md](../spec/TIERED.md)):
  - Lazy promotion — read warm values without promoting (saves memory for one-off reads)
  - Compression — per-tier compression settings (heavier for warm)
  - Key patterns — route specific key patterns to always stay hot
  - Warm-only writes — write large values directly to warm tier

## Security

- **mTLS CN/SAN to ACL user mapping** (from [TLS.md](../spec/TLS.md)) — Map client certificate
  identity to ACL users, enabling certificate-based authentication without passwords. CN could map
  to ACL username directly or via explicit mapping table. Would allow mTLS to serve as sole
  authentication mechanism. See [AUTH.md](../spec/AUTH.md) for the ACL system.

## Persistence

- **Configurable replica WAL failure policy** — Allow replicas to use rollback mode with divergence
  detection and automatic re-sync. Requires Jepsen testing.