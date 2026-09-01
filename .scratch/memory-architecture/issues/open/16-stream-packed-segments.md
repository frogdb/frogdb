# 16: stream entries in packed segments

Status: ready-for-agent
Type: AFK
Origin: memory-architecture PRD phase filing, 2026-09-01 — [PRD.md](../../PRD.md) R7
Area: frogdb-types (stream.rs) + commands (stream family, `stream` feature)
Phase: 4 — value representation

## Why

`StreamValue` (`types/src/types/stream.rs:1111`) stores
`entries: BTreeMap<StreamId, Vec<(Bytes, Bytes)>>` — every entry a separate `Vec`, every field
and value a separate `Bytes`, plus BTreeMap node overhead per entry. Consumer-group state adds
`pending: BTreeMap<StreamId, PendingEntry>` (~line 656) and per-group bookkeeping with its own
`order: VecDeque<Bytes>` (line 1044). Streams are append-mostly and ID-ordered — the single
easiest workload to pack, and currently the least packed value type in the tree.

Redis stores stream entries in a **rax (radix tree) of listpacks**: ~100 entries per listpack
node (`stream-node-max-entries`), master-entry field-name deduplication within a node, IDs
delta-encoded against the node's master ID. That design is the reference: ID-ordered segments
of packed entries, located by a sorted index on first-ID.

**Feature-gating note:** the stream family is behind the non-default `stream` cargo feature
(`frogdb-server/crates/commands/Cargo.toml`; `core-profile` excludes it, `full`/`cmd-full`
include it, ADR-0005 ruling 1). Consequences: (a) iteration builds with the dev default do not
compile this code — develop with `just test frogdb-server --features ...` per the Justfile's
pattern for feature crates, and do not alternate flags in a loop (build-cache thrash, per
CLAUDE.md); (b) CI covers it via the `full` jobs — confirm the stream regression suite actually
runs in CI under `full` before relying on it; (c) `StreamValue` itself lives in frogdb-types
which compiles unconditionally — the *type* changes build everywhere, only command tests need
the feature.

## What to build

### 1. Packed segments

Entries live in segments of ~128 entries: one buffer per segment holding delta-encoded IDs and
field/value bytes (reuse the [issue 13](../) listpack module for
the in-segment encoding). Segment index: `BTreeMap<StreamId, Segment>` keyed by first-ID —
keeps O(log segments) XRANGE seeks and preserves the existing code's BTreeMap idioms. Master
field-name dedup within a segment (Redis's trick — most streams append identical field sets)
is worth including; measure with and without in the memory-shape test.

### 2. Tombstones, trim, and the lifetime counters

XDEL marks in-segment tombstones (Redis does the same — deletion flags in the listpack);
XTRIM/MAXLEN drops whole segments from the front and rewrites the boundary segment.
`entries_added`, `max_deleted_id`, `total_appended` (the ES.* optimistic-concurrency counter)
are scalars on `StreamValue` and are unaffected — keep them exact.

### 3. Consumer groups and the PEL

The PEL (`pending: BTreeMap<StreamId, PendingEntry>`) stays a per-group BTreeMap — Redis also
keeps PELs as raxes separate from entry storage, and PEL entries are small fixed records
(consumer, delivery count, timestamp), not the fragmentation driver. Only change: `PendingEntry`
must not hold entry payload `Bytes` (verify it does not today — it should reference by ID).
XCLAIM/XAUTOCLAIM/XACK paths re-read entries from segments by ID.

### 4. Read paths

XRANGE/XREVRANGE/XREAD materialize reply `Bytes` by copying out of segments at reply time,
same stance as [issue 15](../) §4. Blocked XREAD consumers
wake on append and read from segments like anyone else.

## Acceptance criteria

- [ ] Entries stored in packed segments; no per-entry `Vec`/per-field `Bytes` in storage.
- [ ] Full stream + ES.* regression suites green under `--features stream` (or `full`).
- [ ] CI runs the stream suite under `full` (verify; add if missing — this is a pre-existing
      gap this issue would otherwise silently widen).
- [ ] Memory-shape test: 100k-entry stream with a fixed field set allocates O(segments); dedup
      benefit recorded in the issue resolution.
- [ ] XDEL/XTRIM churn test bounds dead space (tombstone compaction or boundary rewrite).
- [ ] `memory_size()` run-stable.
- [ ] Fuzz: segment encoding vs `BTreeMap<StreamId, Vec<(Bytes,Bytes)>>` model, ops including
      XADD auto-ID, XDEL, XTRIM, XRANGE.

## Test boundary

Level 1 fuzz vs model. Level 2 regression under the feature flag. The ES.* idempotency-dedup
state is lazy-allocated (`None` for non-ES streams) — keep that zero-overhead property.

## Spec rows at R15

None new. If ES.* durability interacts with segment rewrite (it should not — persistence
serializes logical entries, not segments), flag for issue 22 rather than solving here.

## Out of scope

PEL representation changes, `stream-node-max-entries` config knob (constant is fine),
persistence encoding changes (RDB/WAL serialize logical entries — storage format is invisible
to them; verify and assert with an existing round-trip test), rax-style radix index (BTreeMap
of segments is enough at FrogDB's scale until proven otherwise).

## Depends on

[Issue 13](../) — shared listpack/segment encoding module.
