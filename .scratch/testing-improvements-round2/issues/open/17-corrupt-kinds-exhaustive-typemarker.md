# `CORRUPT_KINDS` omits four types and cannot detect the omission of a fifth

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: INFRASTRUCTURE.md I17
LOE: ~0.5 day (estimated)
Tier: A
Area: frogdb-server / DUMP-RESTORE corruption fixtures
Asked by: 07 (F12)

## Context

The DUMP/RESTORE corruption test enumerates the payload kinds it corrupts by hand. Four types
are already missing from that list, so their decoders are never fed a corrupted payload, and
the shape of the list guarantees the next new type will be missed too. The fix is to make the
list exhaustive over `TypeMarker` at compile time, so adding a marker without adding a
corruption case fails the build rather than silently reducing coverage.

## Evidence

- `server/tests/integration_dump_restore.rs:630-641` omits cms, topk, tdigest and vectorset.
- Wants a compile-time exhaustiveness link to `TypeMarker`
  (`persistence/src/serialization/marker.rs:36`) so the next type cannot be silently omitted.

## What to build

1. Add the four missing kinds — cms, topk, tdigest, vectorset — to `CORRUPT_KINDS` in
   `server/tests/integration_dump_restore.rs`.
2. Replace the hand-written list with a construction driven by an exhaustive `match` over
   `TypeMarker` (`persistence/src/serialization/marker.rs:36`), so a new marker without a
   corruption case is a compile error, not a silent gap.
3. If a type genuinely cannot be corruption-tested, it must be an explicit arm with a
   one-line reason, not an omission.

## Acceptance criteria

- [ ] `CORRUPT_KINDS` covers cms, topk, tdigest and vectorset in addition to the existing
      kinds.
- [ ] The list is derived from an exhaustive `match` over `TypeMarker`; adding a variant to
      `persistence/src/serialization/marker.rs` without updating the test fails to compile.
- [ ] Any excluded type is an explicit match arm carrying a one-line reason comment.
- [ ] The corruption test still passes for every newly added kind, or the failure is filed as
      a defect rather than the kind being excluded.

## Test boundary

Level 4 — the corruption fixture already lives in `server/tests/integration_dump_restore.rs`
and exercises RESTORE over RESP, which is where a rejected payload's observable behaviour is;
the exhaustiveness link itself is a compile-time (level 1) guarantee.

## Depends on

Nothing.
