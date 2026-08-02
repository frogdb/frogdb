# Durability decisions are testable seams, not filesystem calls

Persistence bugs are the ones production notices last and forgives least: a checkpoint
published in the wrong order, a WAL batch torn in half, a point-in-time recovery that
silently drops acknowledged writes. All three are invisible to an ordinary integration
test — the process comes up, the data is *mostly* there — and all three used to be
expressed as bare `std::fs` calls and RocksDB options smeared across the server crate.
Three extractions made them addressable. **Startup recovery** moved into `frogdb-recovery`:
one deep seam, `recover(config, data_dir) -> RecoveredState`, over ordered phases (install
staged checkpoint → open RocksDB → restore shard stores → restore functions → restore
replication state → open cluster storage), with plain-data in and out. It needs no host
trait, unlike `frogdb_txn` (ADR 0002), because nothing in it reaches back into `Server`
state; it spawns nothing and returns data, so it sidesteps the server's `net`/`cfg(turmoil)`
abstractions and its tests build without the 130K-LOC server test binary. **Checkpoint
publication** writes through the `SnapshotFs` seam (`fs_seam.rs`) instead of `std::fs`:
whether an fsync reached the platter is not observable from a unit test, but whether the
publisher *issued* it, and in the right order relative to the rename it protects, is —
against a recording fake. The rule the seam exists to enforce: fsync what a rename
publishes *before* the rename, fsync the directory that gains the name *after* it.

Two WAL decisions are pinned by that same reasoning. **Write groups** (`begin_group` /
`end_group` on `WalSink`) make atomicity a property of the sink rather than of batch
timing: entries enqueued inside a group land in one committed storage batch, so neither a
crash nor a checkpoint can observe a prefix of a multi-key command. Groups nest and the
innermost close wins, which is what lets a command call into another without either one
knowing the other's grouping. **The durable WAL watermark** (`rocks/wal_watermark.rs`)
answers a question RocksDB refuses to: `PointInTime` recovery truncates at the first
corrupt record, discards every valid record after it, and returns a healthy `open`. We
persist the highest durably-synced sequence beside the database and compare it on the next
open, emitting `frogdb_wal_recovery_dropped_records_total` with the exact count when
recovery lands short. The watermark advances only *after* a sync and is written
best-effort, so it can only lag the truth — the comparison can under-report a truncation
but can never false-alarm on a clean boot, which is the only direction an operator-facing
data-loss alarm may fail in.

Consequences: both crates are small enough to mutation-test in one run, so the 85% gate is
enforceable — at the Phase 2 lock `frogdb-recovery` holds 100% (28 caught, 0 missed) and
`frogdb-persistence` 99.1% (864 caught, 8 missed) on caught / caught+missed. The failure-mode contract is
`.scratch/hardening/specs/persistence-failure-modes.md`, enforced two-directionally by
`just lint-failure-modes`. The costs are real and accepted: the `SnapshotFs` indirection
means a new publication path must be routed through the trait or it is untested by
construction; the recovery seam's plain-data boundary means a phase that genuinely needs a
live component has to be split, not smuggled through `RecoveredState` (per-shard search
index recovery is exactly that case and deliberately stayed in the server); and pre-sized
buffers in the serialization codecs carry `debug_assert_eq!` pins against the bytes
actually written, because a `Vec::with_capacity` figure is otherwise unobservable and a
wrong one rots silently as the wire format gains fields.
