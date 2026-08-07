# No on-disk format version or magic anywhere; the reserved `flags` byte is written 0 and discarded on read

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/13 F5 · MASTER.md §3 (durability)
Score: severity 5 · likelihood 3 · effort 1 · priority 20
Area: frogdb-persistence / serialization + RocksDB manifest

## Context

There is no mechanism by which an older binary can *refuse* to load a newer on-disk layout. The
one reserved byte in the frame header is written as zero and thrown away on read, and the column
family manifest stamps no format version at all. A rolling upgrade with a rollback — a normal
ops event for a pre-production DB that is about to add value types — will either drop keys or,
if a marker byte was reused, decode them as garbage. Silent corruption on downgrade.

**This is a suspected live defect found by reading, not by test failure — the proposed test
fails against today's code.** The file:line evidence below is the auditing agent's and needs
confirmation before or during the fix; this issue is not among the two the coordinator
verified directly.

## Evidence

`persistence/src/serialization/mod.rs:90-91` — `// Flags (1 byte) - reserved for future use` /
`result.push(0)`; `mod.rs:125` — `let _flags = data[1];`, discarded.
`persistence/src/rocks/manifest.rs` `ColumnFamilyManifest::reconcile` enforces shard count
(`ShardCountMismatch`) and warm-tier presence (`WarmTierMismatch`) but stamps **no
format/version** on disk. Only streams carry a per-type `STREAM_FORMAT_VERSION`.

Note from the same proposal's Deprioritised list: `marker.rs` already has four tests pinning wire
bytes, exhaustiveness, uniqueness and unknown rejection — it is the model for what this fix should
look like elsewhere.

## What to fix

1. Define the meaning of the reserved `flags` byte (or a version field) and **reject** a frame
   whose value the running binary does not understand, with a distinguishable error.
2. Stamp a format/version marker in `ColumnFamilyManifest` and make `reconcile` refuse an
   unrecognised version rather than opening as garbage.
3. Pin the current 24-byte header layout with a golden-bytes test so any future silent layout
   change fails loudly.

## Acceptance criteria

- [ ] A golden-bytes test pins the exact 24-byte header layout
      `[type][flags][expires_at_ms:i64][lfu][pad:5][payload_len:u64]` and fails on any silent
      layout change.
- [ ] A test asserts a frame with a non-zero `flags` byte (i.e. a future format) is **rejected
      with a distinguishable error**, not silently accepted. Fails today.
- [ ] A manifest test asserts an unrecognised on-disk version stamp refuses to open rather than
      opening as garbage. Fails today.

## Test boundary

Level 1 — header parsing and manifest reconciliation are both pure functions. Not level 2: no
engine, no RocksDB instance and no restart is needed to observe that an unknown format byte is
accepted.

## Depends on

nothing

## Re-triage 2026-08-06

**Verdict: still-valid**

Phase 2 locked persistence/recovery but never added a format stamp.
`frogdb-server/crates/persistence/src/serialization/mod.rs:93-94` (was `90-91`) still writes
`// Flags (1 byte) - reserved for future use` / `result.push(0)`, and `:133` (was `:125`) still
discards it as `let _flags = data[1];`. `rocks/manifest.rs` still stamps no version:
`ColumnFamilyManifest::reconcile` (`:46`) enforces only `ShardCountMismatch` (`:83`) and
`WarmTierMismatch` (`:100`), and `rg -n "version|magic|FORMAT"` over that file returns nothing.
No golden-bytes test pins the 24-byte header (`HEADER_SIZE` is referenced only by `lib.rs` and the
two serialization modules). What Phase 2 *did* add is adjacent but different: FM-PERSISTENCE-033
and FM-PERSISTENCE-045 govern how an *undecodable value / unknown type byte* is counted and when a
wholly-undecodable database refuses to boot — neither rejects a frame whose `flags` byte the
running binary does not understand, which is the downgrade hazard this issue is about.
