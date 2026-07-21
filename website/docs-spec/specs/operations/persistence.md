# Spec: operations/persistence.md

Status: rewrite (the current page contains extensive fabricated content — see the
FABRICATED list below; treat existing prose as untrusted)
Audiences: A4 (primary), A3

Goal: The reader can choose a durability mode understanding the qualitative
trade-off (no fabricated latency numbers), configure the WAL failure policy,
understand how forkless snapshots work and what triggers them, understand what
startup recovery actually does, and know that tiered storage exists as an on/off
warm tier with LRU/LFU demotion. Every durability/persistence claim traces to
source.

Not in scope:
- Backup and restore procedures — that is
  [Backup & restore](/operations/backup-restore/); this page covers durability
  mechanics, that page covers copying data off-host and restoring it.
- Replication full-sync / checkpoint transfer — that is
  [Replication](/operations/replication/); mention that snapshots/checkpoints are
  the primitive, link for detail.
- Architecture-level storage-engine design — that is Architecture → Storage /
  Persistence; link.

Sources of truth (read before writing):
- `frogdb-server/crates/persistence/src/wal/config.rs` — `DurabilityMode` enum
  (`Async`, `Periodic { interval_ms }`, `Sync`; default `Periodic{1000}`) and
  `WalFailurePolicy` (`Continue` default / `Rollback`).
- `frogdb-server/crates/persistence/src/wal/flush.rs` — how each mode maps to
  `WriteOptions::set_sync`; per-shard flush thread; `spawn_periodic_sync`.
- `frogdb-server/crates/config/src/persistence.rs` — `[persistence]` and
  `[snapshot]` config keys + defaults. THE authoritative key names.
- `frogdb-server/crates/persistence/src/snapshot/rocks_coordinator.rs` — epoch,
  forkless-via-checkpoint, search-index copy.
- `frogdb-server/crates/persistence/src/rocks/checkpoint.rs` — RocksDB checkpoint
  primitive + `load_staged_checkpoint`.
- `frogdb-server/crates/server/src/server/startup.rs` and
  `.../persistence/src/.../recovery.rs` — the REAL recovery path.
- `frogdb-server/crates/config/src/tiered.rs` — `[tiered-storage] enabled` (only
  key) — and `crates/core/src/eviction/policy.rs` for `tiered-lru`/`tiered-lfu`.
- `frogdb-server/crates/config/src/params.rs` — runtime-mutability of
  `wal-failure-policy`, `durability-mode`, `sync-interval-ms`.

Existing content: `operations/persistence.md` — mine only the VERIFIED parts
(three modes, snapshot config keys, tiered storage existence); DELETE everything
in the FABRICATED list.

## Verified facts (authoritative)

**Durability modes** (`wal/config.rs`, keyed `durability-mode` in
`[persistence]`, validated in `persistence.rs`): `async`, `periodic` (default),
`sync`. The "WAL" is RocksDB's own write path; each mode sets
`WriteOptions.set_sync` accordingly (`flush.rs`). Writes batch on a per-shard
flush thread.
- `sync`: every batch write is fsynced before it returns.
- `async`: `sync=false`; OS decides when to fsync; no periodic sync task.
- `periodic`: `sync=false` on the write PLUS a `spawn_periodic_sync` task that
  calls `rocks.flush()` every `sync-interval-ms`.
Describe these **qualitatively only**. NO latency numbers (content policy §6).

**The interval key is `sync-interval-ms`** (default 1000), NOT `fsync-interval-ms`
(which does not exist). Fix this wherever it appears.

**`wal-failure-policy`** (`[persistence]`, default `continue`): `continue` logs
and returns success on a WAL write failure; `rollback` undoes the in-memory
change and returns an error. It is a shared atomic and is genuinely
runtime-mutable (`CONFIG SET wal-failure-policy rollback` works). Caveat the
build emits: `rollback` only catches flush-thread failures in non-`sync` modes —
document this honestly.
- VERIFY-BEFORE-WRITING: `params.rs` marks `durability-mode` and
  `sync-interval-ms` `mutable: true`, but I found no code that re-spawns the
  running flush thread when they change — a runtime `CONFIG SET` of those two may
  not re-plumb the live flush behavior. Do NOT claim they take effect live until
  verified; state only what's confirmed (`wal-failure-policy` is live).

**Snapshots** (`snapshot/rocks_coordinator.rs`): epoch-based (dirs
`snapshot_NNNNN`), forkless (no `fork()`) — implemented as a **RocksDB checkpoint**
(hard-links SST files at the filesystem level) plus a `metadata.json` and a copy
of the Tantivy search indexes. The low overhead comes from checkpoint hard-links,
NOT an in-memory per-shard copy-on-write buffer. Config keys (`[snapshot]`):
`snapshot-dir` (default `./snapshots`), `snapshot-interval-secs` (default 3600,
0=off), `max-snapshots` (default 5, 0=unlimited). `BGSAVE` (and `BGSAVE
SCHEDULE`) trigger a snapshot.

**Recovery** (`startup.rs`): on startup FrogDB (1) installs a staged replica
checkpoint if a `checkpoint_ready` sibling exists, else no-op; (2) opens RocksDB
at `data-dir` (RocksDB replays its own internal WAL here); (3) if data exists,
iterates every key in every column family into the in-memory store, skipping
expired keys and counting deserialize failures. There is **no snapshot-load step
at startup** — snapshots in `snapshot-dir` are for backup/off-host copy, not for
primary recovery. Recovery of the warm tier happens here too (`recover_warm_shard`).

**Data directory layout:** `data-dir` (default `./frogdb-data`) IS the RocksDB
directory (not a parent with a `data/` subdir). `snapshot-dir` is a sibling.
There are no separate FrogDB-managed WAL files — the WAL is RocksDB-internal.

**Tiered storage** (`config/src/tiered.rs`): real and wired end-to-end (warm
column families, per-shard warm store, warm recovery at startup), but the only
operator-facing config key is `[tiered-storage] enabled` (bool). Eviction
policies `tiered-lru` and `tiered-lfu` (`core/src/eviction/policy.rs`) demote
cold entries to the warm RocksDB CF instead of deleting. Do NOT invent tuning
knobs that don't exist; document the on/off flag and the two policies, and link
to Configuration/Memory for `maxmemory-policy`.

## FABRICATED / INACCURATE content to DELETE (do not carry forward)

| Current claim | Verdict |
|---|---|
| Latency columns `~1-10 us` / `~100-500 us` (durability table) | FABRICATED — remove per PLAN §6 |
| Config key `fsync-interval-ms` | WRONG — real key `sync-interval-ms` |
| "COW buffer per shard" / "~dataset size worst case" memory table | INACCURATE — mechanism is checkpoint hard-links, not in-mem COW |
| 4-step snapshot-then-WAL-replay recovery + "Recovery Scenarios" table | INACCURATE — no snapshot load at startup |
| "Recovery Time Estimates" table (1 GB=10-30 s … 1 TB=1-3 h) | FABRICATED — no benchmark |
| "WAL Corruption Policy" section (`wal-corruption-policy`, `truncate`/`fail`) | FABRICATED — key and enum do not exist |
| `frogctl debug wal-inspect` / `wal-truncate` | FABRICATED — commands do not exist |
| `[rocksdb] min-wal-retention-secs` / `min-wal-files-to-keep` | FABRICATED — no `[rocksdb]` section, keys do not exist |
| "PSYNC via WAL retention" / point-in-time WAL-replay narrative | UNVERIFIED — no backing config/code; cut or move to Replication with real detail |

Structure (H2/H3 — one line each):

- Intro: FrogDB persists to RocksDB; this page covers durability modes, the WAL
  failure policy, snapshots, recovery, and tiered storage. Link Backup & restore
  and Architecture → Storage.
- **## Durability modes** — the three modes, qualitative behavior each (sync
  fsyncs per batch; async lets the OS decide; periodic flushes every
  `sync-interval-ms`). A small "how to choose" list, framed by data-loss window,
  not by fabricated latency. Config snippet with `durability-mode` +
  `sync-interval-ms`.
- **### Write visibility** — writes are visible to other clients before they are
  durable in async/periodic (matches Redis); keep this (it's correct).
- **## WAL failure policy** — `continue` vs `rollback`, the runtime-mutable note,
  and the honest non-sync-mode caveat.
- **## Snapshots** — forkless/epoch-based via RocksDB checkpoint; what a snapshot
  dir contains; `[snapshot]` keys; `BGSAVE` trigger and retention via
  `max-snapshots`. Replace the bogus memory table with one honest sentence:
  checkpoints hard-link SST files, so a snapshot's marginal disk cost grows as
  post-snapshot compaction rewrites those SSTs (no memory-spike claim).
- **## Recovery** — the real 3-step startup path; RocksDB replays its own WAL;
  expired keys skipped; deserialize failures are logged/counted and recovery
  continues (this is the only "corruption" behavior that exists). No recovery-time
  table.
- **## Tiered storage** — what it is (warm RocksDB tier), the `enabled` flag, and
  `tiered-lru`/`tiered-lfu` demotion. Clearly labeled as an evolving feature with
  a single config knob.
- **## Persistence metrics** — keep the short list, but present it as a pointer to
  Reference → Metrics for the authoritative catalog (verify each named metric
  exists at write time; drop any that don't).
- **## See also** — Backup & restore, Replication, Configuration, Architecture →
  Storage.

Generated data:
- Links to Reference → Configuration reference (`config-reference.json`, for
  `[persistence]`/`[snapshot]`/`[tiered-storage]` keys) and Reference → Metrics
  (`metrics.json`, S3) for the metric catalog. No embedded generated component.

Drift guards:
- `just docs-gen-check` keeps the linked persistence/snapshot/tiered config keys
  honest against `config/src/`.
- S7 code-path check — the `crates/persistence/...` and `startup.rs` paths cited
  must exist.
- Metric names must come from / be validated against the generated metrics
  catalog (S3), not hand-listed.
- Content policy: no latency/throughput/recovery-time numbers without a cited
  benchmark; no config keys or CLI commands that don't exist in source (this page
  regressed badly on exactly that — the author must verify every key against
  `persistence.rs` before writing it).
