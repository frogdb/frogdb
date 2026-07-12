# 45 — Stream consumer groups are not persisted (groups, last-delivered, PEL lost on restart)

**Status:** implemented (2026-07-12) — stream codec format v1 persists groups, consumers, and the
PEL; both `#[ignore]`d restart tests unignored and green. PEL/consumer times are persisted as
wall-clock ms via the ClaimClock mapping; per-consumer seen/active times are persisted exactly
(open decision 1: matched Redis, it was cheap). Deferred follow-up: per-group delta/merge-operand
persistence to cut write amplification (design-sketch point 3; parity-first, not built here).
**Severity:** Critical (silent data loss for consumer-group workloads)
**Found:** 2026-07-12, by the regression-test wave: restart-survival tests for XGROUP CREATE and
XREADGROUP could not pass and are checked in `#[ignore]`d
(`frogdb-server/crates/server/tests/integration_persistence.rs` —
`test_xgroup_create_survives_restart`, `test_xreadgroup_pending_survives_restart`; both fail RED
if unignored: XINFO GROUPS returns `[]`, XPENDING returns NOGROUP).

## Problem

Stream serialization deliberately omits consumer groups
(`frogdb-server/crates/persistence/src/serialization/stream.rs:21`; the omission is documented as
a known limitation in [09-serialization-typecodec-registry.md](09-serialization-typecodec-registry.md)
— "stream consumer groups are deliberately not persisted"). Consequences after any restart with
persistence enabled:

- Every group vanishes: consumers must re-`XGROUP CREATE`, losing `last-delivered-id` — a group
  recreated at `0` re-delivers the entire stream; recreated at `$` silently skips everything
  delivered-but-unacked.
- The PEL (pending entries list) is lost wholesale: unacked messages are no longer claimable or
  re-deliverable. For at-least-once consumers this is silent message loss.

Redis persists all of it: RDB serializes groups, PEL entries (delivery time/count), and consumers
per stream (`rdbSaveStreamConsumerGroups` / `rdbLoadObject` in rdb.c); AOF reconstructs via
XGROUP CREATE + XCLAIM replay. This was an accepted scope cut when the codec registry landed;
with proposals 42/43 hardening the persistence layer, it is now the largest remaining durability
gap.

## Design sketch

Extend the stream codec (`serialization/stream.rs`) to encode, per group: name,
last-delivered-id, entries-read/lag counters if tracked, and the PEL as
`[(entry-id, consumer-name, delivery_count, idle-anchor)]`, plus the consumer table
(name → seen/active times).

Key design points to resolve (the implementer must check the actual types in
`frogdb-server/crates/types/src/types/stream.rs`):

1. **Format versioning.** FrogDB is pre-production — a clean breaking bump of the stream encoding
   byte is acceptable (precedent: HLL delta encoding `2` added alongside `0/1` in proposal 42).
   No migration path needed; note it in the codec docs.
2. **Monotonic-time PEL fields.** `PendingEntry` stores delivery time as `Instant` (monotonic),
   which cannot be serialized directly. Proposal-44-era work added `ClaimClock { now, unix_ms }`
   (commit `f03582ba`) mapping wall-clock ↔ monotonic for XCLAIM — reuse the same mapping to
   persist delivery time as unix ms and reconstruct an `Instant` anchor at load (idle time
   resumes from the persisted wall-clock value; matches Redis, which persists ms delivery time).
3. **Write amplification.** Consumer-group churn (every XREADGROUP/XACK/XCLAIM) rewrites the full
   stream value through the WAL. That is today's behavior for entry-appends too (XADD persists
   the whole stream), so parity-first: persist correctly now, optimize later. Note the follow-up
   (a per-group delta/merge-operand scheme like proposal 42's Tier 2) but do NOT build it here.
4. **WAL declarations already correct:** XGROUP/XREADGROUP/XACK/XCLAIM/XAUTOCLAIM declare
   key-persisting strategies (verified during the 01-40 audit) — once the codec includes groups,
   the existing write path persists them with no command changes.

## Tests

1. Unignore and green the two `#[ignore]`d restart tests (they are the RED baseline).
2. Round-trip unit tests in the codec: groups + PEL + consumers, empty group, group with
   consumers but empty PEL, delivery-count preservation.
3. Restart integration: XADD → XREADGROUP (pend 2 entries) → XACK 1 → restart → XPENDING shows
   exactly the 1 unacked entry with delivery count intact; XCLAIM works on it; XINFO GROUPS shows
   correct last-delivered-id.
4. Idle-time continuity: pend an entry, restart, assert XPENDING idle ≥ pre-restart idle (uses
   the ClaimClock mapping).
5. Recovery-path test in `persistence/src/recovery.rs` mock-sink round-trip (extend the existing
   `round_trips_format_through_mock_sink`).

## Open decisions

1. Whether to persist per-consumer `seen-time`/`active-time` exactly or coarsen to one timestamp
   (Redis persists both in newer RDB versions — match if cheap).
2. Update the stale "deliberately not persisted" note in proposal 09 when this lands.
