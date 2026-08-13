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

## Re-triage 2026-08-06

**Verdict: still-valid**

The gap is unchanged and the list got *shorter*, not longer. `CORRUPT_KINDS` is no longer a
const: `cd78df2f` (exotic command families behind cargo features) turned it into
`fn corrupt_kinds() -> Vec<&'static str>` — so `server/tests/integration_dump_restore.rs:630-641`
→ `frogdb-server/crates/server/tests/integration_dump_restore.rs:638-655`. It is still
hand-written, still omits cms, topk, tdigest and vectorset, and now drops stream/hll/json out of
the matrix entirely under the default core profile (they are `#[cfg(feature = "cmd-…")]`
pushes), so the default `just test` run corrupts six kinds out of seventeen markers. One new
obstacle for the proposed fix: `TypeMarker` is now **crate-private** —
`frogdb-server/crates/persistence/src/serialization/marker.rs:19` is `pub(crate) enum TypeMarker`
and `serialization/mod.rs:36` re-exports it `pub(crate)` — so a server integration test cannot
match on it at all. Either the marker (or a purpose-built enumeration) must be exported from
`frogdb-persistence`, or the exhaustiveness check must live inside that crate alongside the
existing `TypeMarker::ALL`-driven `registry_covers_every_marker_once` /
`every_marker_round_trips` (`persistence/src/serialization/registry.rs:542,562`), which today
guarantee a decoder per marker but say nothing about corruption coverage. Nothing in
`specs/persistence.md` covers DUMP/RESTORE payload corruption
(its corruption rows are recovery-side: FM-PERSISTENCE-033/034/047).
