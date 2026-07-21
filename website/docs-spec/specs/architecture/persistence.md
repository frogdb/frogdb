# Spec: architecture/persistence.md
Status: update
Audiences: A2, A3, A5

Goal: A reader walks away understanding how FrogDB persists data and *why* the
design is what it is: a single shared RocksDB instance with one column family per
internal shard plus a shared WAL (so backup/restore is one operation and
cross-shard writes can be atomic via a WriteBatch), a compact per-value on-disk
encoding with a fixed header, an in-memory-is-source-of-truth WAL discipline
(the WAL provides durability, not correctness during normal operation — matching
Redis AOF), configurable durability modes (Async / Periodic / Sync) and a
configurable WAL-failure policy (continue vs rollback), corruption recovery by
truncation, and forkless epoch-based snapshots whose COW buffer memory is
explicitly tracked so maxmemory enforcement stays correct during a snapshot. The
reader also learns how sequence numbers are assigned at WAL-append time to give
replication a monotonic ordering.

Not in scope: In-memory storage layout and eviction (storage.md); replication
stream protocol (replication.md); operator-facing config/recovery procedures
(Operations: Persistence). This is the contributor-facing internals page; link
outward for the rest.

Sources of truth (author must read and reconcile every claim):
- `frogdb-server/crates/persistence/src/rocks/` — RocksDB setup, column-family
  topology (one CF per shard), WriteBatch usage.
- `frogdb-server/crates/persistence/src/wal/` — WAL writer, durability modes,
  failure policy, corruption handling.
- `frogdb-server/crates/persistence/src/serialization/` — the actual on-disk
  value encoding (header layout, type codes, byte order). Verify the header size
  (page claims fixed 24 bytes), field order, and the type-code table against
  this module — do not trust the current numbers.
- `frogdb-server/crates/persistence/src/snapshot/` — forkless snapshot:
  `handle.rs`, `metadata.rs`, `rocks_coordinator.rs` (epoch counter, COW).
- `frogdb-server/crates/types/src/traits/wal.rs` — the `WalWriter` trait
  (`Send + Sync`; `append(&mut self, &WalOperation) -> u64`,
  `flush -> io::Result<()>`, `current_sequence -> u64`) and `WalOperation` enum.
- `frogdb-server/crates/config/src/` — real defaults for durability mode,
  `wal-failure-policy`, `cow-buffer-max-bytes` / `cow-memory-abort-threshold`,
  Periodic interval.

Existing content: `website/src/content/docs/architecture/persistence.md`. Well
structured; several concrete numbers (header bytes, latency ranges) need
verification or removal.

Structure (keep existing H2s):
- ## RocksDB Topology — single instance, CF-per-shard, shared WAL; benefits and
  the WAL-contention trade-off. Verify against `rocks/`.
- ## Key-Value Schema — key format and the value header. Verify the 24-byte
  header, field layout, type-code table, and little-endian claim against
  `serialization/`; reconcile the type codes with the `Value` enum in
  storage.md.
- ## Metadata Persistence — lfu_counter persisted, last_access not persisted,
  expiry index rebuilt from `expires_at`, timestamp→Instant conversion on
  recovery. Cross-link storage.md (single source for the LRU/LFU metadata story).
- ## Write-Ahead Log — write path, failure-handling table, `wal-failure-policy`
  (continue/rollback), corruption recovery (truncation). Verify policy names and
  defaults.
- ## Durability Modes — Async / Periodic / Sync; Periodic wall-clock timer
  semantics; write visibility before durability. **Remove or cite the latency
  columns** (see Drift guards).
- ## Forkless Snapshot Algorithm — epoch-based COW; explicit COW memory tracking
  in `total_memory_used()`; eviction-during-snapshot table. Verify against
  `snapshot/` (epoch counter is real: `AtomicU64` in `rocks_coordinator.rs`).
- ## Sequence Number Assignment — assigned at WAL append; gaps possible; replicas
  resume from a sequence. Replace the illustrative `WalWriter::append` sample
  with one matching the real trait signature.

Generated data: None. Config defaults referenced here should track the config
crate (future S6/config-reference tie-in), not be hardcoded independently.

Drift guards:
- **On-disk encoding numbers.** The "fixed 24 bytes" header, the padding layout,
  and the type-code table are exact claims — verify every one against
  `serialization/`. If the encoding differs, correct it; a wrong schema here
  actively misleads contributors.
- **Latency figures must be cited or removed (PLAN §6).** Durability-modes table
  (`~1-10 us` / `~100-500 us`), the rollback-mode note (`~0.1-2ms vs ~1-10us`),
  and any other microsecond/millisecond claims are unbacked performance numbers.
  Replace with qualitative statements (e.g. "Sync waits for an fsync before
  acknowledging, so it is materially slower than Async, which acknowledges after
  an in-memory append") unless a published benchmark is cited.
- **WalWriter signature.** The sample `impl WalWriter { fn append(...) -> u64 }`
  must match `types/src/traits/wal.rs` (method name `append`, returns the
  sequence `u64`; the trait also has `flush` and `current_sequence`). Keep the
  architecture.md WalWriter block consistent with this page.
- **Config names/defaults.** `wal-failure-policy`, durability mode names, COW
  thresholds, and the Periodic interval must match the config crate; verify.
- **Type-code ↔ Value-enum consistency.** The persistence type codes must stay
  consistent with the `Value` enum documented in storage.md (15 variants; Bitmap
  and Geo are layered, not standalone variants).
- **S7 code-path check.** Any `crates/...` path named must exist; include this
  page in the S7 scan.
